// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.transaction;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.stream.TableStreamUpdateInfo;
import org.apache.doris.cloud.proto.Cloud.CommitTxnResponse;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.QuotaExceedException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.BatchRemoveTransactionsOperation;
import org.apache.doris.persist.BatchRemoveTransactionsOperationV2;
import org.apache.doris.persist.EditLog;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TWaitingTxnStatusRequest;
import org.apache.doris.thrift.TWaitingTxnStatusResult;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;

import java.io.DataInput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

// 全局事务管理器的核心接口定义类
// 为整个 Doris 系统中的分布式事务（包括各种导入任务如 Broker Load、Stream Load、Insert 语句等）制定了统一的契约规范
// 主要承担以下职责：
// 定义分布式事务生命周期契约：规范了事务从开启（Begin）、预提交（Pre-Commit）、提交并发布（Commit & Publish）、中止（Abort）到最终完成（Finish）的全套标准操作。
// 多数据库与元数据管理：提供数据库事务管理器（Database Transaction Manager）的挂载、卸载及配额更新接口。
// 状态查询与运维监控：支持通过标签、状态、ID 等多种维度检索事务状态，为系统运维和排查提供数据接口。
// 日志重放（Replay）与高可用支持：定义了各类事务状态变更的重放方法，确保 FE 节点主备切换时事务状态的一致性。

public interface GlobalTransactionMgrIface extends Writable {
    // 校验事务或导入任务的超时时间是否合法
    // 检查超时秒数是否低于系统最小导入超时时间（minLoadTimeoutSecond），
    // 或高于最大导入超时时间（maxLoadTimeoutSecond，针对流式导入和普通导入有不同配置限制），若不合法则抛出 AnalysisException。
    default void checkValidTimeoutSecond(long timeoutSecond, int maxLoadTimeoutSecond,
                                         int minLoadTimeOutSecond, LoadJobSourceType sourceType)
            throws AnalysisException {
        if (timeoutSecond < minLoadTimeOutSecond) {
            throw new AnalysisException("Invalid timeout: " + timeoutSecond
                    + ". Timeout should be higher than Config.min_load_timeout_second: "
                    + minLoadTimeOutSecond);
        } else if (timeoutSecond > maxLoadTimeoutSecond) {
            switch (sourceType) {
                case BACKEND_STREAMING:
                    throw new AnalysisException("Invalid timeout: " + timeoutSecond
                            + ". Timeout should be lower than Config.max_stream_load_timeout_second: "
                            + maxLoadTimeoutSecond);
                default:
                    throw new AnalysisException("Invalid timeout: " + timeoutSecond
                            + ". Timeout should be lower than Config.max_load_timeout_second: "
                            + maxLoadTimeoutSecond);
            }
        }
    }
    // 设置编辑日志（EditLog）对象，用于将事务操作持久化到元数据日志中。
    public void setEditLog(EditLog editLog);
    // 获取事务状态变更回调工厂（TxnStateCallbackFactory），用于注册和管理如 LoadJob 这样的事务回调监听器。
    public TxnStateCallbackFactory getCallbackFactory();
    // 为指定的数据库 ID 添加对应的数据库级事务管理器。
    public void addDatabaseTransactionMgr(Long dbId);
    // 移除指定数据库 ID 的事务管理器（通常在数据库被 Dropped 时调用）
    public void removeDatabaseTransactionMgr(Long dbId);

    // 基础事务开启方法。
    // 根据数据库 ID、涉及表列表、Label、协调者信息、作业源类型及超时时间，向全局事务管理器申请并开启一个新事务，返回生成的 transactionId。
    public long beginTransaction(long dbId, List<Long> tableIdList, String label, TxnCoordinator coordinator,
            LoadJobSourceType sourceType, long timeoutSecond)
            throws AnalysisException, LabelAlreadyUsedException, BeginTransactionException, DuplicatedRequestException,
            QuotaExceedException, MetaNotFoundException;
    // 带有请求 ID（requestId）和监听器 ID 的扩展事务开启方法，用于支持幂等性检查和重复请求拦截。
    public long beginTransaction(long dbId, List<Long> tableIdList, String label, TUniqueId requestId,
            TxnCoordinator coordinator, LoadJobSourceType sourceType, long listenerId, long timeoutSecond)
            throws AnalysisException, LabelAlreadyUsedException, BeginTransactionException, DuplicatedRequestException,
            QuotaExceedException, MetaNotFoundException;
    // 两阶段提交（2PC）中的预提交阶段。在所有数据写入完成后锁定表并预提交事务，但不立即发布生效。
    public void preCommitTransaction2PC(Database db, List<Table> tableList, long transactionId,
            List<TabletCommitInfo> tabletCommitInfos, long timeoutMillis,
            TxnCommitAttachment txnCommitAttachment)
            throws UserException;
    // 在不加表锁的情况下提交事务。通常在外部已经由调用方显式加锁的安全上下文中使用。
    @Deprecated
    public void commitTransactionWithoutLock(long dbId, List<Table> tableList,
            long transactionId, List<TabletCommitInfo> tabletCommitInfos)
            throws UserException;

    public void commitTransactionWithoutLock(long dbId, List<Table> tableList, long transactionId,
            List<TabletCommitInfo> tabletCommitInfos, TxnCommitAttachment txnCommitAttachment)
            throws UserException;
    // 标准的事务提交方法。
    // 传入数据库对象、表列表、事务 ID、Tablet 提交信息及附件，执行常规提交。
    public void commitTransaction(DatabaseIf db, List<Table> tableList, long transactionId,
            List<TabletCommitInfo> tabletCommitInfos, long timeoutMillis,
            TxnCommitAttachment txnCommitAttachment)
            throws UserException;
    // 提交并发布事务（Commit & Publish）。
    // 这是 Doris 最常用的事务提交方式，不仅提交事务，还会触发版本发布（Publish Version），使导入的数据对用户立即可见。
    public boolean commitAndPublishTransaction(DatabaseIf db, List<Table> tableList, long transactionId,
            List<TabletCommitInfo> tabletCommitInfos, long timeoutMillis)
            throws UserException;

    public boolean commitAndPublishTransaction(DatabaseIf db, long transactionId,
            List<SubTransactionState> subTransactionStates, long timeoutMillis) throws UserException;

    public boolean commitAndPublishTransaction(DatabaseIf db, List<Table> tableList, long transactionId,
            List<TabletCommitInfo> tabletCommitInfos, long timeoutMillis,
            TxnCommitAttachment txnCommitAttachment)
            throws UserException;

    default boolean commitAndPublishTransaction(DatabaseIf db, List<Table> tableList, long transactionId,
            List<TabletCommitInfo> tabletCommitInfos, long timeoutMillis,
            TxnCommitAttachment txnCommitAttachment, List<TableStreamUpdateInfo> streamUpdateInfos)
            throws UserException {
        return commitAndPublishTransaction(db, tableList, transactionId, tabletCommitInfos, timeoutMillis,
                txnCommitAttachment);
    }
    // 两阶段提交中的正式提交阶段（针对已经预提交成功的 2PC 事务）
    public void commitTransaction2PC(Database db, List<Table> tableList, long transactionId, long timeoutMillis)
            throws UserException;
    // 根据事务 ID 中止（取消）指定的分布式事务，并记录中止原因。
    public void abortTransaction(Long dbId, Long transactionId, String reason) throws UserException;

    public void abortTransaction(Long dbId, Long txnId, String reason,
            TxnCommitAttachment txnCommitAttachment, List<Table> tableList) throws UserException;

    public void abortTransaction(Long dbId, String label, String reason) throws UserException;

    public void abortTransaction2PC(Long dbId, long transactionId, List<Table> tableList) throws UserException;
    // 获取所有准备好进行版本发布的事务状态列表。
    public List<TransactionState> getReadyToPublishTransactions();

    // 获取指定数据库下所有已提交（Committed）但尚未发布的事务列表。
    public List<TransactionState> getCommittedTransactions(long dbId) throws AnalysisException;
    // 检查指定表或分区是否存在已提交的事务。
    public boolean existCommittedTxns(Long dbId, Long tableId, Long partitionId);
    // 完成事务收尾。
    // 更新分区可见版本（Visible Versions）并清理后端分区映射关系。
    public void finishTransaction(long dbId, long transactionId, Map<Long, Long> partitionVisibleVersions,
            Map<Long, Set<Long>> backendPartitions) throws UserException;
    // 检查指定事务 ID 之前的历史事务是否全部完成，用于保证事务串行化或顺序一致性。
    public boolean isPreviousTransactionsFinished(long endTransactionId, long dbId, List<Long> tableIdList)
            throws AnalysisException;

    public boolean isPreviousTransactionsFinished(long endTransactionId, long dbId, long tableId,
            long partitionId) throws AnalysisException;
    // 后台定时清理过期和超时的悬挂事务。
    public void removeExpiredAndTimeoutTxns();

    public void cleanLabel(Long dbId, String label, boolean isReplay) throws Exception;
    // 动态更新多表运行中事务关联的表 ID 列表。
    public void updateMultiTableRunningTransactionTableIds(Long dbId, Long transactionId, List<Long> tableIds)
            throws UserException;

    public TWaitingTxnStatusResult getWaitingTxnStatus(TWaitingTxnStatusRequest request)
            throws AnalysisException, TimeoutException;

    public void updateDatabaseUsedQuotaData(long dbId, long usedQuotaDataBytes) throws AnalysisException;

    public void abortTxnWhenCoordinateBeRestart(long coordinateBeId, String coordinateHost, long beStartTime);

    public void abortTxnWhenCoordinateBeDown(long coordinateBeId, String coordinateHost, int limit);

    public TransactionStatus getLabelState(long dbId, String label) throws AnalysisException;

    public Long getTransactionId(Long dbId, String label) throws AnalysisException;

    public TransactionState getTransactionState(long dbId, long transactionId);

    public Long getTransactionIdByLabel(Long dbId, String label, List<TransactionStatus> statusList)
            throws UserException;

    public List<TransactionState> getPreCommittedTxnList(Long dbId) throws AnalysisException;

    public int getTransactionNum();

    public TransactionIdGenerator getTransactionIDGenerator() throws Exception;

    public List<List<String>> getDbTransInfoByStatus(Long dbId, TransactionStatus status) throws AnalysisException;

    public List<List<String>> getDbTransInfoByLabelMatch(long dbId, String label) throws AnalysisException;

    public List<List<String>> getDbTransStateInfo(Long dbId) throws AnalysisException;

    public List<List<String>> getDbTransInfo(Long dbId, boolean running, int limit) throws AnalysisException;

    public Map<Long, List<Long>> getDbRunningTransInfo(long dbId) throws AnalysisException;

    public List<List<Comparable>> getTableTransInfo(long dbId, long txnId) throws AnalysisException;

    public List<List<Comparable>> getPartitionTransInfo(long dbId, long tid, long tableId)
            throws AnalysisException;

    public List<List<String>> getDbInfo() throws AnalysisException;

    public long getTxnNumByStatus(TransactionStatus status);

    public List<List<String>> getSingleTranInfo(long dbId, long txnId) throws AnalysisException;

    public long getAllRunningTxnNum();

    public int getRunningTxnNums(Long dbId) throws AnalysisException;

    public long getAllPublishTxnNum();

    public Long getNextTransactionId() throws UserException;

    public void readFields(DataInput in) throws IOException;
    // 重放更新/插入事务状态的元数据日志。
    public void replayUpsertTransactionState(TransactionState transactionState) throws Exception;
    // 重放删除事务状态的元数据日志。
    public void replayDeleteTransactionState(TransactionState transactionState) throws Exception;

    public void replayBatchRemoveTransactions(BatchRemoveTransactionsOperation operation) throws Exception;

    public void replayBatchRemoveTransactionV2(BatchRemoveTransactionsOperationV2 operation) throws Exception;

    public void afterCommitTxnResp(CommitTxnResponse commitTxnResponse, List<TabletCommitInfo> tabletCommitInfos,
            List<Long> tabletIds);

    public void addSubTransaction(long dbId, long transactionId, long subTransactionId);

    public void removeSubTransaction(long dbId, long subTransactionId);

    public List<TransactionState> getUnFinishedPreviousLoad(long endTransactionId,
                long dbId, List<Long> tableIdList) throws UserException;

    public int getQueueLength();

    public AutoPartitionCacheManager getAutoPartitionCacheMgr();
}
