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

import org.apache.doris.alter.AlterJobV2;
import org.apache.doris.binlog.UpsertRecord;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Partition.PartitionState;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.catalog.stream.BaseTableStream;
import org.apache.doris.catalog.stream.TableStreamUpdateInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.QuotaExceedException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.lock.MonitoredReentrantReadWriteLock;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.InternalDatabaseUtil;
import org.apache.doris.common.util.MetaLockUtils;
import org.apache.doris.event.DataChangeEvent;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.persist.BatchRemoveTransactionsOperationV2;
import org.apache.doris.persist.CleanLabelOperationLog;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.OperationType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.AnalysisManager;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.ClearTransactionTask;
import org.apache.doris.task.PublishVersionTask;
import org.apache.doris.thrift.TTabletCommitInfo;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Transaction Manager in database level, as a component in GlobalTransactionMgr
 * DatabaseTransactionMgr mainly be responsible for the following content:
 * 1. provide read/write lock in database level
 * 2. provide basic txn infos interface in database level to GlobalTransactionMgr
 * 3. do some transaction management, such as add/update/delete transaction.
 * Attention: all api in DatabaseTransactionMgr should be only invoked by GlobalTransactionMgr
 */

public class DatabaseTransactionMgr {

    private enum PublishResult {
        FAILED,
        TIMEOUT_SUCC,  // each tablet has least one replica succ, and timeout
        QUORUM_SUCC,   // each tablet has least quorum replicas succ
    }

    private static final Logger LOG = LogManager.getLogger(DatabaseTransactionMgr.class);
    // the max number of txn that can be remove per round.
    // set it to avoid holding lock too long when removing too many txns per round.
    private static final int MAX_REMOVE_TXN_PER_ROUND = 10000;

    private final long dbId;

    // the lock is used to control the access to transaction states
    // no other locks should be inside this lock
    private final MonitoredReentrantReadWriteLock transactionLock = new MonitoredReentrantReadWriteLock(true);

    // transactionId -> running TransactionState
    private final Map<Long, TransactionState> idToRunningTransactionState = Maps.newConcurrentMap();

    // transactionId -> final status TransactionState
    private final Map<Long, TransactionState> idToFinalStatusTransactionState = Maps.newConcurrentMap();
    private final Map<Long, Long> subTxnIdToTxnId = new ConcurrentHashMap<>();

    // The following 2 queues are to store transactionStates with final status
    // These queues are mainly used to avoid traversing all txns and speed up the cleaning time
    // when cleaning up expired txs.
    // The "Short" queue is used to store the txns of the expire time
    // controlled by Config.streaming_label_keep_max_second.
    // The "Long" queue is used to store the txns of the expire time controlled by Config.label_keep_max_second.
    private final ConcurrentLinkedDeque<TransactionState> finalStatusTransactionStateDequeShort
            = new ConcurrentLinkedDeque<>();
    private final ConcurrentLinkedDeque<TransactionState> finalStatusTransactionStateDequeLong
            = new ConcurrentLinkedDeque<>();

    // label -> txn ids
    // this is used for checking if label already used. a label may correspond to multiple txns,
    // and only one is success.
    // this member should be consistent with idToTransactionState,
    // which means if a txn exist in idToRunningTransactionState or idToFinalStatusTransactionState
    // it must exists in dbIdToTxnLabels, and vice versa
    private final Map<String, Set<Long>> labelToTxnIds = Maps.newHashMap();

    private final Map<Long, Long> tableCommittedTxnCount = Maps.newConcurrentMap();

    private Long lastCommittedTxnCountUpdateTime = 0L;

    // count the number of running txns of database
    private final AtomicInteger runningTxnNums = new AtomicInteger(0);

    private final Env env;

    private final EditLog editLog;

    private final TransactionIdGenerator idGenerator;

    private final List<ClearTransactionTask> clearTransactionTasks = Lists.newArrayList();

    // not realtime usedQuota value to make a fast check for database data quota
    private volatile long usedQuotaDataBytes = -1;

    private long lockWriteStart;

    private long lockReportingThresholdMs = Config.lock_reporting_threshold_ms;

    private void readLock() {
        this.transactionLock.readLock().lock();
    }

    private void readUnlock() {
        this.transactionLock.readLock().unlock();
    }

    private void writeLock() {
        this.transactionLock.writeLock().lock();
        lockWriteStart = System.currentTimeMillis();
    }

    private void writeUnlock() {
        checkAndLogWriteLockDuration(lockWriteStart, System.currentTimeMillis());
        this.transactionLock.writeLock().unlock();
    }

    public DatabaseTransactionMgr(long dbId, Env env, TransactionIdGenerator idGenerator) {
        this.dbId = dbId;
        this.env = env;
        this.idGenerator = idGenerator;
        this.editLog = env.getEditLog();
    }

    public long getDbId() {
        return dbId;
    }

    protected TransactionState getTransactionState(Long transactionId) {
        return unprotectedGetTransactionState(transactionId);

    }
    // 核心职责是：根据给定的事务 ID，依次检索内存中的运行中事务容器、终态事务容器以及子事务映射表，最终返回对应的 TransactionState（事务状态对象）。
    private TransactionState unprotectedGetTransactionState(Long transactionId) {
        // 1. 检索运行中的事务（Running Transactions）
        // 从内存字典 idToRunningTransactionState（记录所有处于 PREPARE、PREPARED、COMMITTED 等运行中/未完全结束状态的事务 Map）中查找该 transactionId。
        TransactionState transactionState = idToRunningTransactionState.get(transactionId);
        // 如果查找到了，说明该事务属于当前正在运行的事务，直接返回对应的 TransactionState 对象。
        if (transactionState != null) {
            return transactionState;
        }
        // 2. 检索已结束/处于终态的事务（Final Status Transactions）
        // 若在运行中字典未命中，则从 idToFinalStatusTransactionState（记录已达到最终状态如 VISIBLE、ABORTED 的历史事务 LRU 缓存/Map）中继续查找。
        transactionState = idToFinalStatusTransactionState.get(transactionId);
        if (transactionState != null) {
            return transactionState;
        }
        // 3. 递归检索子事务（Sub-Transaction Routing）
        // 判断传入的 transactionId 是否其实是一个子事务 ID（SubTxnId）（例如多表导入或复合事务场景下的子任务）。
        if (subTxnIdToTxnId.containsKey(transactionId)) {
            // 如果匹配成功，根据子事务 ID 查出对应的主事务 ID（Parent TxnId）。
            // 递归调用自身，传入主事务 ID 重新检索，最终获取并返回主事务的 TransactionState。
            return unprotectedGetTransactionState(subTxnIdToTxnId.get(transactionId));
        }
        return null;
    }

    @VisibleForTesting
    protected Set<Long> unprotectedGetTxnIdsByLabel(String label) {
        return labelToTxnIds.get(label);
    }

    protected int getRunningTxnNums() {
        return runningTxnNums.get();
    }

    @VisibleForTesting
    protected int getFinishedTxnNums() {
        return idToFinalStatusTransactionState.size();
    }

    public List<List<String>> getTxnStateInfoList(boolean running, int limit) {
        List<List<String>> infos = Lists.newArrayList();
        Collection<TransactionState> transactionStateCollection = null;
        readLock();
        try {
            if (running) {
                transactionStateCollection = idToRunningTransactionState.values();
            } else {
                transactionStateCollection = idToFinalStatusTransactionState.values();
            }
            // get transaction order by txn id desc limit 'limit'
            transactionStateCollection.stream()
                    .sorted(TransactionState.TXN_ID_COMPARATOR)
                    .limit(limit)
                    .forEach(t -> {
                        infos.add(TransactionUtil.getTxnStateInfo(t, Lists.newArrayList()));
                    });
        } finally {
            readUnlock();
        }
        return infos;
    }

    public Map<Long, List<Long>> getDbRunningTransInfo() {
        Map<Long, List<Long>> infos = Maps.newHashMap();
        readLock();
        try {
            for (Entry<Long, TransactionState> info : idToRunningTransactionState.entrySet()) {
                infos.put(info.getKey(), info.getValue().getTableIdList());
            }
        } finally {
            readUnlock();
        }
        return infos;
    }

    public List<List<String>> getTxnStateInfoList(TransactionStatus status) {
        List<List<String>> infos = Lists.newArrayList();
        Collection<TransactionState> transactionStateCollection = null;
        readLock();
        try {
            if (status == TransactionStatus.VISIBLE || status == TransactionStatus.ABORTED) {
                transactionStateCollection = idToFinalStatusTransactionState.values();
            } else {
                transactionStateCollection = idToRunningTransactionState.values();
            }
            // get transaction order by txn id desc limit 'limit'
            transactionStateCollection.stream()
                    .filter(transactionState -> (transactionState.getTransactionStatus() == status))
                    .sorted(TransactionState.TXN_ID_COMPARATOR)
                    .forEach(t -> {
                        infos.add(TransactionUtil.getTxnStateInfo(t, Lists.newArrayList()));
                    });
        } finally {
            readUnlock();
        }
        return infos;
    }

    public List<List<String>> getTxnStateInfoList(String labelRegex) {
        List<List<String>> infos = Lists.newArrayList();
        List<TransactionState> transactionStateCollection = Lists.newArrayList();
        readLock();
        try {
            transactionStateCollection.addAll(idToFinalStatusTransactionState.values());
            transactionStateCollection.addAll(idToRunningTransactionState.values());
            // get transaction order by txn id desc
            transactionStateCollection.stream()
                    .filter(transactionState -> (transactionState.getLabel().matches(labelRegex)))
                    .sorted(TransactionState.TXN_ID_COMPARATOR)
                    .forEach(t -> {
                        infos.add(TransactionUtil.getTxnStateInfo(t, Lists.newArrayList()));
                    });
        } finally {
            readUnlock();
        }
        return infos;
    }


    public long beginTransaction(List<Long> tableIdList, String label, TUniqueId requestId,
            TransactionState.TxnCoordinator coordinator, TransactionState.LoadJobSourceType sourceType,
            long listenerId, long timeoutSecond)
            throws DuplicatedRequestException, LabelAlreadyUsedException, BeginTransactionException,
            AnalysisException, QuotaExceedException, MetaNotFoundException {

        if (!Env.getCurrentEnv().isMaster() && !FeConstants.runningUnitTest) {
            throw new BeginTransactionException("FE is not master");
        }

        Database db = env.getInternalCatalog().getDbOrMetaException(dbId);
        if (!coordinator.isFromInternal) {
            InternalDatabaseUtil.checkDatabase(db.getFullName(), ConnectContext.get());
        }
        MTMVUtil.checkModifyMTMVData(db, tableIdList, ConnectContext.get());
        checkDatabaseDataQuota();
        Preconditions.checkNotNull(coordinator);
        Preconditions.checkNotNull(label);
        FeNameFormat.checkLabel(label);

        long tid = 0L;
        TransactionState transactionState = null;
        EditLog.EditLogItem logItem = null;
        writeLock();
        try {
            /*
             * Check if label already used, by following steps
             * 1. get all existing transactions
             * 2. if there is a PREPARE transaction, check if this is a retry request. If yes, return the
             *    existing txn id.
             * 3. if there is a non-aborted transaction, throw label already used exception.
             */
            Set<Long> existingTxnIds = unprotectedGetTxnIdsByLabel(label);
            if (existingTxnIds != null && !existingTxnIds.isEmpty()) {
                List<TransactionState> notAbortedTxns = Lists.newArrayList();
                for (long txnId : existingTxnIds) {
                    TransactionState txn = unprotectedGetTransactionState(txnId);
                    Preconditions.checkNotNull(txn);
                    if (txn.getTransactionStatus() != TransactionStatus.ABORTED) {
                        notAbortedTxns.add(txn);
                    }
                }
                // there should be at most 1 txn in PREPARE/PRECOMMITTED/COMMITTED/VISIBLE status
                Preconditions.checkState(notAbortedTxns.size() <= 1, notAbortedTxns);
                if (!notAbortedTxns.isEmpty()) {
                    TransactionState notAbortedTxn = notAbortedTxns.get(0);
                    if (requestId != null && (notAbortedTxn.getTransactionStatus() == TransactionStatus.PREPARE
                            || notAbortedTxn.getTransactionStatus() == TransactionStatus.PRECOMMITTED)
                            && notAbortedTxn.getRequestId() != null && notAbortedTxn.getRequestId().equals(requestId)) {
                        // this may be a retry request for same job, just return existing txn id.
                        throw new DuplicatedRequestException(DebugUtil.printId(requestId),
                                notAbortedTxn.getTransactionId(), "");
                    }
                    throw new LabelAlreadyUsedException(notAbortedTxn);
                }
            }

            checkRunningTxnExceedLimit(tableIdList);

            tid = idGenerator.getNextTransactionId();
            transactionState = new TransactionState(dbId, tableIdList,
                    tid, label, requestId, sourceType, coordinator, listenerId, timeoutSecond * 1000);
            transactionState.setPrepareTime(System.currentTimeMillis());
            unprotectUpdateInMemoryState(transactionState, false);
            updateTxnLabels(transactionState);
            if (Config.enable_txn_log_outside_lock) {
                logItem = enqueueTransactionState(transactionState);
            } else {
                persistTransactionState(transactionState);
            }

            if (MetricRepo.isInit) {
                MetricRepo.COUNTER_TXN_BEGIN.increase(1L);
            }
        } finally {
            writeUnlock();
        }
        awaitTransactionState(logItem, transactionState);
        LOG.info("begin transaction: txn id {} with label {} from coordinator {}, listener id: {}",
                    tid, label, coordinator, listenerId);
        return tid;
    }

    private void checkDatabaseDataQuota() throws MetaNotFoundException, QuotaExceedException {
        Database db = env.getInternalCatalog().getDbOrMetaException(dbId);

        if (usedQuotaDataBytes == -1) {
            usedQuotaDataBytes = db.getUsedDataQuota();
        }

        long dataQuotaBytes = db.getDataQuota();
        if (usedQuotaDataBytes >= dataQuotaBytes) {
            throw new QuotaExceedException(db.getFullName(), dataQuotaBytes);
        }
    }

    public void updateDatabaseUsedQuotaData(long usedQuotaDataBytes) {
        this.usedQuotaDataBytes = usedQuotaDataBytes;
    }
    // Apache Doris 中数据库级别的事务管理器（DatabaseTransactionMgr）中处理导入事务两阶段提交（2PC）预提交（Pre-Commit）阶段的核心方法
    // 在完成了上层 FE/BE 的鉴权、数据库与表对象解析以及加锁之后，该方法在数据库维度执行事务状态校验、副本写入正确性检查（Quorum 校验）、更新事务状态为 PRECOMMITTED 以及元数据持久化（EditLog 持久化/异步入队）。
    public void preCommitTransaction2PC(List<Table> tableList, long transactionId,
            List<TabletCommitInfo> tabletCommitInfos, TxnCommitAttachment txnCommitAttachment)
            throws UserException {
        // check status
        // the caller method already own db lock, we do not obtain db lock here
        // 1. 查找目标事务状态对象
        // 从当前元数据目录中根据 dbId 获取目标数据库对象；若数据库不存在则直接抛出元数据异常。
        Database db = env.getInternalCatalog().getDbOrMetaException(dbId);
        TransactionState transactionState;
        // 给当前 DatabaseTransactionMgr 加上读锁，保护对内部事务状态容器（如内存中的事务 Map）的并发读取。
        readLock();
        try {
            // 在持有读锁的前提下，根据 transactionId 检索出内存中的事务状态对象 TransactionState。
            transactionState = unprotectedGetTransactionState(transactionId);
        } finally {
            readUnlock();
        }
        // 2. 事务合法性与当前状态校验
        // 如果事务不存在（null），或者已被回滚/终止（状态为 ABORTED），则抛出 TransactionCommitFailedException，并带上对应的异常原因（transactionState.getReason()）。
        if (transactionState == null
                || transactionState.getTransactionStatus() == TransactionStatus.ABORTED) {
            throw new TransactionCommitFailedException(
                    transactionState == null ? "transaction not found" : transactionState.getReason());
        }
        // 校验事务是否已经生效可见（VISIBLE）。如果是，说明事务早已彻底完成，不能重复执行预提交，抛出异常。
        if (transactionState.getTransactionStatus() == TransactionStatus.VISIBLE) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("transaction is already visible: {}", transactionId);
            }
            throw new TransactionCommitFailedException("transaction is already visible");
        }
        // 校验事务是否处于已提交状态（COMMITTED）。如果是，说明事务已完成 Commit 阶段，不可回退执行 Pre-Commit，抛出异常。
        if (transactionState.getTransactionStatus() == TransactionStatus.COMMITTED) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("transaction is already committed: {}", transactionId);
            }
            throw new TransactionCommitFailedException("transaction is already committed");
        }
        // 幂等性处理。如果事务状态已经是 PRECOMMITTED（预提交完成），说明之前已经成功调用过该接口（如网络重试场景）。
        // 此时打印日志并直接 return 成功，不再重复执行后续校验与写 EditLog 操作。
        if (transactionState.getTransactionStatus() == TransactionStatus.PRECOMMITTED) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("transaction is already pre-committed: {}", transactionId);
            }
            return;
        }
        // 3. 副本写入状态与分布式 Quorum 校验
        // 记录导入过程中写入失败或异常的副本（Replica）ID 集合。
        Set<Long> errorReplicaIds = Sets.newHashSet();
        // 记录参与本次事务写入的所有 Backend 节点 ID 集合。
        Set<Long> totalInvolvedBackends = Sets.newHashSet();
        // 记录本次事务影响的表 ID 到分区 ID 集合（TableID -> Set<PartitionID>）的映射。
        Map<Long, Set<Long>> tableToPartition = new HashMap<>();
        // 核心提交校验函数。
        // 遍历所有 Tablet，对比 BE 汇报的 tabletCommitInfos 与 FE 元数据。校验写入成功的副本数量是否达到了多数派要求（Quorum）；
        // 若有副本写入失败，将其放入 errorReplicaIds；同时整理出涉及的 Partition 和 Backend。如果成功副本数未达标，该方法内部会直接抛出异常。
        checkCommitStatus(tableList, transactionState, tabletCommitInfos, txnCommitAttachment, errorReplicaIds,
                          tableToPartition, totalInvolvedBackends);
        // 4. 事务状态内存更新与 EditLog 持久化
        EditLog.EditLogItem logItem = null;
        // 对当前事务状态对象加同步锁，保证该事务状态变更与日志写入过程的原子性。
        synchronized (transactionState) {
            // 更新内存中的 transactionState 属性，将其状态字段修改为 PRECOMMITTED，并记录错误副本、涉及的分区及 BE 列表等信息。
            unprotectedPreCommitTransaction2PC(transactionState, errorReplicaIds, tableToPartition,
                    totalInvolvedBackends, db);
            // true（开启同步/异步解耦）：调用 enqueueTransactionState(...) 将 transactionState 包装成日志项 logItem 放入 EditLog 异步写入队列，避免在长锁竞争下阻塞写日志。
            if (Config.enable_txn_log_outside_lock) {
                logItem = enqueueTransactionState(transactionState);
            } else {
            // false（传统模式）：调用 persistTransactionState(...) 直接将事务预提交状态同步持久化写入 Doris BDBJE / EditLog 中。
                persistTransactionState(transactionState);
            }
        }
        // 5. 等待日志落盘与完成返回
        // 等待元数据 EditLog 彻底持久化完成（如果在异步队列模式下，会在此处挂起等待 logItem 刷盘完成的 Signal）。这确保了返回给客户端成功之前，预提交状态已经安全落盘，防止 FE 宕机导致事务状态丢失。
        awaitTransactionState(logItem, transactionState);
        LOG.info("transaction:[{}] successfully pre-committed", transactionState);
    }
    // 核心职责是：核对 Backend（BE）上报的副本写入成果，判断每个 Tablet 成功写入数据的副本数量是否满足多数派（Quorum）法定要求。
    // 同时，它还负责检测表/分区是否处于恢复状态（Restore），并自动触发写失败 Tablet 的修复（Repair）。
    private void checkCommitStatus(List<Table> tableList, TransactionState transactionState,
                                   List<TabletCommitInfo> tabletCommitInfos, TxnCommitAttachment txnCommitAttachment,
                                   Set<Long> errorReplicaIds, Map<Long, Set<Long>> tableToPartition,
                                   Set<Long> totalInvolvedBackends) throws UserException {
        // 1. 基础元数据与提交附加信息绑定
        long transactionId = transactionState.getTransactionId();
        Database db = env.getInternalCatalog().getDbOrMetaException(dbId);

        // update transaction state extra if exists
        if (txnCommitAttachment != null) {
            transactionState.setTxnCommitAttachment(txnCommitAttachment);
        }
        // 2. 建立索引映射与初始化结构
        // 获取全局 Tablet 倒排索引（TabletInvertedIndex），用于快速反查 Tablet、Replica 与 Backend 的对应关系。
        TabletInvertedIndex tabletInvertedIndex = env.getTabletInvertedIndex();
        // 创建映射 TabletID -> Set<BackendID>，记录上报写入成功的 BE 节点集合。
        Map<Long, Set<Long>> tabletToBackends = new HashMap<>();
        // 建立 TableID -> Table 对象的快速映射字典。
        Map<Long, Table> idToTable = new HashMap<>();
        for (int i = 0; i < tableList.size(); i++) {
            idToTable.put(tableList.get(i).getId(), tableList.get(i));
        }
        // validate potential exists problem: db->table->partition
        // guarantee exist exception during a transaction
        // if index is dropped, it does not matter.
        // if table or partition is dropped during load, just ignore that tablet,
        // because we should allow dropping rollup or partition during load
        // 3. 校验表与分区的恢复（RESTORE）状态，整理成功上报信息
        // 通过 Stream 流提取 BE 上报的所有 tabletId
        List<Long> tabletIds = tabletCommitInfos.stream()
                .map(TabletCommitInfo::getTabletId).collect(Collectors.toList());
        // 从倒排索引中批量获取这些 Tablet 的元数据信息 TabletMeta（包含所属的 tableId 和 partitionId）
        List<TabletMeta> tabletMetaList = tabletInvertedIndex.getTabletMetaList(tabletIds);
        // 用于缓存表是否正在处于 Restore（备份恢复）状态的计算结果。
        HashMap<Long, Boolean> tableIdtoRestoring = new HashMap<>();
        for (int i = 0; i < tabletMetaList.size(); i++) {
            // get partition and table of this tablet
            TabletMeta tabletMeta = tabletMetaList.get(i);
            // 过滤已被删除的元数据：如果 Tablet 不存在（NOT_EXIST_TABLET_META）或对应的表已被删除/正在删除（tbl == null），则忽略该 Tablet。
            if (tabletMeta == TabletInvertedIndex.NOT_EXIST_TABLET_META) {
                continue;
            }
            long tabletId = tabletIds.get(i);
            long tableId = tabletMeta.getTableId();
            OlapTable tbl = (OlapTable) idToTable.get(tableId);
            if (tbl == null) {
                // this can happen when tableId == -1 (tablet being dropping) or table really not exist.
                continue;
            }

            // check relative partition restore here
            // 分区 RESTORE 校验：如果 Tablet 所属的分区正处于物理恢复阶段（PartitionState.RESTORE），直接抛出 LoadException 拒绝写入数据。
            long partitionId = tabletMeta.getPartitionId();
            if (tbl.getPartition(partitionId) == null) {
                // this can happen when partitionId == -1 (tablet being dropping) or partition really not exist.
                continue;
            }
            if (tbl.getPartition(partitionId).getState() == PartitionState.RESTORE) {
                // partition in restore process which can not load data
                throw new LoadException("Table [" + tbl.getName() + "], Partition ["
                        + tbl.getPartition(partitionId).getName() + "] is in restore process. Can not load into it");
            }

            // only do check when here's restore on this table now
            // 整表 RESTORE 校验：如果表状态为 OlapTableState.RESTORE，需要区分是“整表恢复”还是“部分分区恢复”。
            // 如果是整表恢复（没有任何单个分区处于 Restore 状态，即整表结构恢复中），抛出 LoadException 拒绝导入。
            if (tbl.getState() == OlapTableState.RESTORE) {
                boolean hasPartitionRestoring = false;
                if (tableIdtoRestoring.containsKey(tableId)) {
                    hasPartitionRestoring = tableIdtoRestoring.get(tableId);
                } else {
                    for (Partition partition : tbl.getPartitions()) {
                        if (partition.getState() == PartitionState.RESTORE) {
                            hasPartitionRestoring = true;
                            break;
                        }
                    }
                    tableIdtoRestoring.put(tableId, hasPartitionRestoring);
                }
                // tbl RESTORE && all partition NOT RESTORE -> whole table restore
                // tbl RESTORE && some partition RESTORE -> just partitions restore, NOT WHOLE TABLE
                // so check wether the whole table restore here
                if (!hasPartitionRestoring) {
                    throw new LoadException(
                            "Table " + tbl.getName() + " is in restore process. " + "Can not load into it");
                }
            }
            // 将 partitionId 加入 tableToPartition 集合中（记录哪些表和分区被真正写入了数据）；
            if (!tableToPartition.containsKey(tableId)) {
                tableToPartition.put(tableId, new HashSet<>());
            }
            // 将成功写入该 tabletId 的 backendId 加入 tabletToBackends 字典中。
            tableToPartition.get(tableId).add(partitionId);
            if (!tabletToBackends.containsKey(tabletId)) {
                tabletToBackends.put(tabletId, new HashSet<>());
            }
            tabletToBackends.get(tabletId).add(tabletCommitInfos.get(i).getBackendId());
        }
        // 4. 分布式多数派（Quorum）副本数量与健康状态校验
        // 分类收集器：针对每个 Tablet，定义三个集合分类保存副本：
        // 完全正常且成功写入数据的副本。
        List<Replica> tabletSuccReplicas = Lists.newArrayList();
        // 写入失败或未上报写入成功的副本。
        List<Replica> tabletWriteFailedReplicas = Lists.newArrayList();
        // 数据虽然写了，但版本落后/处于异常状态（getLastFailedVersion() >= 0）的副本。
        List<Replica> tabletVersionFailedReplicas = Lists.newArrayList();
        // 获取待校验的物化视图/索引（MaterializedIndex）：遍历涉及到的表和分区，根据 transactionState.getLoadedTblIndexes() 提取本次导入实际写入的 Index 列表（如 Base 表索引及 Rollup 索引）。
        for (long tableId : tableToPartition.keySet()) {
            OlapTable table = (OlapTable) db.getTableOrMetaException(tableId);
            for (Partition partition : table.getAllPartitions()) {
                if (!tableToPartition.get(tableId).contains(partition.getId())) {
                    continue;
                }

                List<MaterializedIndex> allIndices;
                if (transactionState.getLoadedTblIndexes().isEmpty()
                        || transactionState.getLoadedTblIndexes().get(tableId) == null) {
                    allIndices = partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL, true);
                } else {
                    allIndices = Lists.newArrayList();
                    for (long indexId : transactionState.getLoadedTblIndexes().get(tableId)) {
                        MaterializedIndex index = partition.getIndex(indexId);
                        if (index != null) {
                            allIndices.add(index);
                        }
                    }
                }
                // 延长 Publish 超时优化：如果表正处于 Rollup 或 Schema Change 变更状态，调用 prolongPublishTimeout() 延长后续版本发布阶段的超时时间，降低因为 Alter Job 期间冲突导致 Publish 失败的概率。
                if (table.getState() == OlapTable.OlapTableState.ROLLUP
                        || table.getState() == OlapTable.OlapTableState.SCHEMA_CHANGE) {
                    /*
                     * This is just a optimization that do our best to not let publish version tasks
                     * timeout if table is under rollup or schema change. Because with a short
                     * timeout, a replica's publish version task is more likely to fail. And if
                     * quorum replicas of a tablet fail to publish, the alter job will fail.
                     *
                     * If the table is not under rollup or schema change, the failure of a replica's
                     * publish version task has a minor effect because the replica can be repaired
                     * by tablet repair process very soon. But the tablet repair process will not
                     * repair rollup replicas.
                     *
                     * This a kind of best-effort-optimization, if FE restart after commit and
                     * before publish, this 'prolong' information will be lost.
                     */
                    transactionState.prolongPublishTimeout();
                }

                // (TODO): ignore the alter index if txn id is less than sc sched watermark
                // 计算该分区导入最少必须成功写入的副本数量（通常是多数派 $\lfloor \text{ReplicaNum} / 2 \rfloor + 1$）。
                int loadRequiredReplicaNum = table.getLoadRequiredReplicaNum(partition.getId());
                // 遍历属于该索引的每个 Tablet，清空上一个 Tablet 的临时列表，并拿到元数据中定义的所有期望 BE 节点 tabletBackends。
                for (MaterializedIndex index : allIndices) {
                    for (Tablet tablet : index.getTablets()) {
                        tabletSuccReplicas.clear();
                        tabletWriteFailedReplicas.clear();
                        tabletVersionFailedReplicas.clear();
                        long tabletId = tablet.getId();
                        Set<Long> tabletBackends = tablet.getBackendIds();
                        totalInvolvedBackends.addAll(tabletBackends);
                        Set<Long> commitBackends = tabletToBackends.get(tabletId);
                        // save the error replica ids for current tablet
                        // this param is used for log
                        for (long tabletBackend : tabletBackends) {
                            Replica replica = tabletInvertedIndex.getReplica(tabletId, tabletBackend);
                            if (replica == null) {
                                throw new TransactionCommitFailedException("could not find replica for tablet ["
                                        + tabletId + "], backend [" + tabletBackend + "]");
                            }

                            // if the tablet have no replica's to commit or the tablet is a rolling up tablet,
                            // the commit backends maybe null
                            // if the commit backends is null, set all replicas as error replicas
                            if (commitBackends != null && commitBackends.contains(tabletBackend)) {
                                // if the backend load success but the backend has some errors previously,
                                // then it is not a normal replica
                                // ignore it but not log it
                                // for example, a replica is in clone state
                                if (replica.getLastFailedVersion() < 0) {
                                    tabletSuccReplicas.add(replica);
                                } else {
                                    tabletVersionFailedReplicas.add(replica);
                                }
                            } else {
                                tabletWriteFailedReplicas.add(replica);
                                errorReplicaIds.add(replica.getId());
                            }
                        }
                        // 5. Quorum 多数派判断与自动修复（Tablet Repair）触发
                        int successReplicaNum = tabletSuccReplicas.size();
                        // 判断成功副本数是否达标：如果 successReplicaNum < loadRequiredReplicaNum（健康且成功的副本数小于法定要求数量）：
                        if (successReplicaNum < loadRequiredReplicaNum) {
                            long now = System.currentTimeMillis();
                            long lastLoadFailedTime = tablet.getLastLoadFailedTime();
                            tablet.setLastLoadFailedTime(now);
                            // 触发自动修复机制：更新 Tablet 的最后一次导入失败时间 lastLoadFailedTime。如果距离上次失败已超过 5 秒（now - lastLoadFailedTime >= 5000L），
                            // 调用 TabletScheduler.tryAddRepairTablet(...) 将该坏 Tablet 加入系统的后台修复调度队列，通知 FE 尽快发起副本补齐/克隆（Clone）。
                            if (now - lastLoadFailedTime >= 5000L) {
                                Env.getCurrentEnv().getTabletScheduler().tryAddRepairTablet(
                                        tablet, db.getId(), table, partition, index, 0);
                            }

                            String writeDetail = getTabletWriteDetail(tabletSuccReplicas, tabletWriteFailedReplicas,
                                    tabletVersionFailedReplicas);

                            String errMsg = String.format("Failed to commit txn %s, cause tablet %s succ replica num %s"
                                    + " < load required replica num %s. table %s, partition: [ id=%s, commit version %s"
                                    + ", visible version %s ], this tablet detail: %s. Please try again later.",
                                    transactionId, tablet.getId(), successReplicaNum, loadRequiredReplicaNum, tableId,
                                    partition.getId(), partition.getCommittedVersion(), partition.getVisibleVersion(),
                                    writeDetail);
                            LOG.info(errMsg);

                            throw new TabletQuorumFailedException(transactionId, errMsg);
                        }
                    }
                }
            }
        }
    }

    private String getTabletWriteDetail(List<Replica> tabletSuccReplicas, List<Replica> tabletWriteFailedReplicas,
            List<Replica> tabletVersionFailedReplicas) {
        String writeDetail = "";
        if (!tabletSuccReplicas.isEmpty()) {
            writeDetail += String.format("%s replicas final succ: { %s }; ",
                    tabletSuccReplicas.size(), Joiner.on(", ").join(
                            tabletSuccReplicas.stream().map(replica -> replica.toStringSimple(true))
                                    .collect(Collectors.toList())));
        }
        if (!tabletWriteFailedReplicas.isEmpty()) {
            writeDetail += String.format("%s replicas write data failed: { %s }, please check BE log for details; ",
                    tabletWriteFailedReplicas.size(), Joiner.on(", ").join(
                            tabletWriteFailedReplicas.stream().map(replica -> replica.toStringSimple(true))
                                    .collect(Collectors.toList())));
        }
        if (!tabletVersionFailedReplicas.isEmpty()) {
            writeDetail += String.format("%s replicas write data succ but miss previous "
                            + "version: { %s }, please check output of SHOW TABLET for details",
                    tabletVersionFailedReplicas.size(), Joiner.on(",").join(
                            tabletVersionFailedReplicas.stream().map(replica -> replica.toStringSimple(true))
                                    .collect(Collectors.toList())));
        }

        return writeDetail;
    }

    /**
     * @return true if the transaction need to commit, otherwise false
     */
    private boolean checkTransactionStateBeforeCommit(Database db, List<Table> tableList, long transactionId,
            boolean is2PC, TransactionState transactionState) throws UserException {
        if (transactionState == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("transaction not found: {}", transactionId);
            }
            throw new TransactionCommitFailedException("transaction [" + transactionId + "] not found.");
        }

        if (transactionState.getTransactionStatus() == TransactionStatus.ABORTED) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("transaction is already aborted: {}", transactionId);
            }
            throw new TransactionCommitFailedException("transaction [" + transactionId
                    + "] is already aborted. abort reason: " + transactionState.getReason());
        }

        if (transactionState.getTransactionStatus() == TransactionStatus.VISIBLE) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("transaction is already visible: {}", transactionId);
            }
            if (is2PC) {
                throw new TransactionCommitFailedException("transaction [" + transactionId
                        + "] is already visible, not pre-committed.");
            }
            return false;
        }
        if (transactionState.getTransactionStatus() == TransactionStatus.COMMITTED) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("transaction is already committed: {}", transactionId);
            }
            if (is2PC) {
                throw new TransactionCommitFailedException("transaction [" + transactionId
                        + "] is already committed, not pre-committed.");
            }
            return false;
        }

        if (is2PC && transactionState.getTransactionStatus() == TransactionStatus.PREPARE) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("transaction is prepare, not pre-committed: {}", transactionId);
            }
            throw new TransactionCommitFailedException("transaction [" + transactionId
                    + "] is prepare, not pre-committed.");
        }

        if (transactionState.isPartialUpdate()) {
            if (is2PC) {
                Iterator<TableCommitInfo> tableCommitInfoIterator
                        = transactionState.getIdToTableCommitInfos().values().iterator();
                while (tableCommitInfoIterator.hasNext()) {
                    TableCommitInfo tableCommitInfo = tableCommitInfoIterator.next();
                    long tableId = tableCommitInfo.getTableId();
                    OlapTable table = (OlapTable) db.getTableNullable(tableId);
                    if (table != null && table instanceof OlapTable) {
                        if (!transactionState.checkSchemaCompatibility((OlapTable) table)) {
                            throw new TransactionCommitFailedException("transaction [" + transactionId
                                    + "] check schema compatibility failed, partial update can't commit with"
                                            + " old schema sucessfully .");
                        }
                    }
                }
            } else {
                for (Table table : tableList) {
                    if (table instanceof OlapTable) {
                        if (!transactionState.checkSchemaCompatibility((OlapTable) table)) {
                            throw new TransactionCommitFailedException("transaction [" + transactionId
                                    + "] check schema compatibility failed, partial update can't commit with"
                                            + " old schema sucessfully .");
                        }
                    }
                }
            }
        }
        // check table stream offset if necessary
        checkStreamOffset(transactionState);
        return true;
    }

    /**
     * commit transaction process as follows：
     * 1. validate whether `Load` is cancelled
     * 2. validate whether `Table` is deleted
     * 3. validate replicas consistency
     * 4. update transaction state version
     * 5. persistent transactionState
     * 6. update nextVersion because of the failure of persistent transaction resulting in error version
     */
    public void commitTransaction(List<Table> tableList, long transactionId, List<TabletCommitInfo> tabletCommitInfos,
                                  TxnCommitAttachment txnCommitAttachment, Boolean is2PC)
            throws UserException {
        env.debugBlockAllOnGlobalLock("FE.BLOCK_IMPORT_LOCK");
        // check status
        // the caller method already own tables' write lock
        Database db = env.getInternalCatalog().getDbOrMetaException(dbId);
        TransactionState transactionState;
        readLock();
        try {
            transactionState = unprotectedGetTransactionState(transactionId);
        } finally {
            readUnlock();
        }

        if (!checkTransactionStateBeforeCommit(db, tableList, transactionId, is2PC, transactionState)) {
            return;
        }

        Set<Long> errorReplicaIds = Sets.newHashSet();
        Set<Long> totalInvolvedBackends = Sets.newHashSet();
        Map<Long, Set<Long>> tableToPartition = new HashMap<>();
        if (!is2PC) {
            checkCommitStatus(tableList, transactionState, tabletCommitInfos, txnCommitAttachment, errorReplicaIds,
                    tableToPartition, totalInvolvedBackends);
        }
        // before state transform
        transactionState.beforeStateTransform(TransactionStatus.COMMITTED);
        // transaction state transform
        boolean txnOperated = false;
        EditLog.EditLogItem logItem = null;
        synchronized (transactionState) {
            if (is2PC) {
                unprotectedCommitTransaction2PC(transactionState, db);
            } else {
                unprotectedCommitTransaction(transactionState, errorReplicaIds,
                        tableToPartition, totalInvolvedBackends, db);
            }
            if (Config.enable_txn_log_outside_lock) {
                logItem = enqueueTransactionState(transactionState);
            } else {
                persistTransactionState(transactionState);
            }
            txnOperated = true;
        }
        // after state transform
        try {
            transactionState.afterStateTransform(TransactionStatus.COMMITTED, txnOperated);
        } catch (Throwable e) {
            LOG.warn("afterStateTransform txn {} failed. exception: ", transactionState, e);
        }
        if (txnOperated) {
            awaitTransactionState(logItem, transactionState);
        }

        // update nextVersion because of the failure of persistent transaction resulting in error version
        updateCatalogAfterCommitted(transactionState, db, false);
        LOG.info("transaction:[{}] successfully committed", transactionState);
    }

    protected void commitTransaction(long transactionId, List<Table> tableList,
            List<SubTransactionState> subTransactionStates) throws UserException {
        // check status
        // the caller method already own tables' write lock
        Database db = env.getInternalCatalog().getDbOrMetaException(dbId);
        TransactionState transactionState;
        readLock();
        try {
            transactionState = unprotectedGetTransactionState(transactionId);
        } finally {
            readUnlock();
        }

        if (DebugPointUtil.isEnable("DatabaseTransactionMgr.commitTransaction.failed")) {
            throw new TabletQuorumFailedException(transactionId,
                    "DebugPoint: DatabaseTransactionMgr.commitTransaction.failed");
        }

        if (!checkTransactionStateBeforeCommit(db, tableList, transactionId, false, transactionState)) {
            return;
        }

        // error replica may be duplicated for different sub transaction, but it's ok
        Set<Long> errorReplicaIds = Sets.newHashSet();
        Map<Long, Set<Long>> subTxnToPartition = new HashMap<>();
        Set<Long> totalInvolvedBackends = Sets.newHashSet();
        for (SubTransactionState subTransactionState : subTransactionStates) {
            Map<Long, Set<Long>> tableToPartition = new HashMap<>();
            Table table = subTransactionState.getTable();
            List<TTabletCommitInfo> tabletCommitInfos = subTransactionState.getTabletCommitInfos();
            checkCommitStatus(Lists.newArrayList(table), transactionState,
                    TabletCommitInfo.fromThrift(tabletCommitInfos), null,
                    errorReplicaIds, tableToPartition, totalInvolvedBackends);
            Preconditions.checkState(tableToPartition.size() <= 1, "tableToPartition=" + tableToPartition);
            if (tableToPartition.size() > 0) {
                subTxnToPartition.put(subTransactionState.getSubTransactionId(), tableToPartition.get(table.getId()));
            }
        }

        // before state transform
        transactionState.beforeStateTransform(TransactionStatus.COMMITTED);
        // transaction state transform
        boolean txnOperated = false;
        EditLog.EditLogItem logItem = null;
        synchronized (transactionState) {
            unprotectedCommitTransaction(transactionState, errorReplicaIds, subTxnToPartition, totalInvolvedBackends,
                    subTransactionStates, db);
            if (Config.enable_txn_log_outside_lock) {
                logItem = enqueueTransactionState(transactionState);
            } else {
                persistTransactionState(transactionState);
            }
            txnOperated = true;
        }
        // after state transform
        try {
            transactionState.afterStateTransform(TransactionStatus.COMMITTED, txnOperated);
        } catch (Throwable e) {
            LOG.warn("afterStateTransform txn {} failed. exception: ", transactionState, e);
        }
        if (txnOperated) {
            awaitTransactionState(logItem, transactionState);
        }

        // update nextVersion because of the failure of persistent transaction resulting in error version
        updateCatalogAfterCommitted(transactionState, db, false);
        LOG.info("transaction:[{}] successfully committed", transactionState);
    }

    public boolean waitForTransactionFinished(DatabaseIf db, long transactionId, long timeoutMillis)
            throws TransactionCommitFailedException {
        TransactionState transactionState = null;
        readLock();
        try {
            transactionState = unprotectedGetTransactionState(transactionId);
        } finally {
            readUnlock();
        }

        switch (transactionState.getTransactionStatus()) {
            case COMMITTED:
            case VISIBLE:
                break;
            default:
                LOG.warn("transaction commit failed, db={}, txn={}", db.getFullName(), transactionId);
                throw new TransactionCommitFailedException("transaction commit failed");
        }

        long currentTimeMillis = System.currentTimeMillis();
        long timeoutTimeMillis = currentTimeMillis + timeoutMillis;
        while (currentTimeMillis < timeoutTimeMillis
                && transactionState.getTransactionStatus() == TransactionStatus.COMMITTED) {
            try {
                transactionState.waitTransactionVisible(timeoutMillis);
            } catch (InterruptedException e) {
                // CHECKSTYLE IGNORE THIS LINE
            }
            currentTimeMillis = System.currentTimeMillis();
        }
        return transactionState.getTransactionStatus() == TransactionStatus.VISIBLE;
    }

    @Deprecated
    // use replayBatchDeleteTransaction instead
    public void replayDeleteTransaction(TransactionState transactionState) {
        writeLock();
        try {
            // here we only delete the oldest element, so if element exist in finalStatusTransactionStateDeque,
            // it must at the front of the finalStatusTransactionStateDeque.
            // check both "short" and "long" queue.
            TransactionState shortHead = finalStatusTransactionStateDequeShort.peekFirst();
            TransactionState longHead = finalStatusTransactionStateDequeLong.peekFirst();
            if (shortHead != null
                    && transactionState.getTransactionId() == shortHead.getTransactionId()) {
                finalStatusTransactionStateDequeShort.pollFirst();
                clearTransactionState(transactionState.getTransactionId());
            } else if (longHead != null
                    && transactionState.getTransactionId() == longHead.getTransactionId()) {
                finalStatusTransactionStateDequeLong.pollFirst();
                clearTransactionState(transactionState.getTransactionId());
            }
        } finally {
            writeUnlock();
        }
    }

    public void replayBatchRemoveTransaction(List<Long> txnIds) {
        writeLock();
        try {
            for (Long txnId : txnIds) {
                // here we only delete the oldest element, so if element exist in finalStatusTransactionStateDeque,
                // it must at the front of the finalStatusTransactionStateDeque
                // check both "short" and "long" queue.
                TransactionState shortHead = finalStatusTransactionStateDequeShort.peekFirst();
                TransactionState longHead = finalStatusTransactionStateDequeLong.peekFirst();
                if (shortHead != null && txnId == shortHead.getTransactionId()) {
                    finalStatusTransactionStateDequeShort.pollFirst();
                    clearTransactionState(txnId);
                } else if (longHead != null && txnId == longHead.getTransactionId()) {
                    finalStatusTransactionStateDequeLong.pollFirst();
                    clearTransactionState(txnId);
                }
            }
        } finally {
            writeUnlock();
        }
    }

    public void replayBatchRemoveTransaction(BatchRemoveTransactionsOperationV2 operation) {
        writeLock();
        try {
            if (operation.getLatestTxnIdForShort() != -1) {
                TransactionState transactionState;
                while ((transactionState = finalStatusTransactionStateDequeShort.pollFirst()) != null) {
                    clearTransactionState(transactionState.getTransactionId());
                    if (operation.getLatestTxnIdForShort() == transactionState.getTransactionId()) {
                        break;
                    }
                }
            }

            if (operation.getLatestTxnIdForLong() != -1) {
                TransactionState transactionState;
                while ((transactionState = finalStatusTransactionStateDequeLong.pollFirst()) != null) {
                    clearTransactionState(transactionState.getTransactionId());
                    if (operation.getLatestTxnIdForLong() == transactionState.getTransactionId()) {
                        break;
                    }
                }
            }
        } finally {
            writeUnlock();
        }
    }

    public TransactionStatus getLabelState(String label) {
        readLock();
        try {
            Set<Long> existingTxnIds = unprotectedGetTxnIdsByLabel(label);
            if (existingTxnIds == null || existingTxnIds.isEmpty()) {
                return TransactionStatus.UNKNOWN;
            }
            // find the latest txn (which id is largest)
            long maxTxnId = existingTxnIds.stream().max(Comparator.comparingLong(Long::valueOf)).get();
            return unprotectedGetTransactionState(maxTxnId).getTransactionStatus();
        } finally {
            readUnlock();
        }
    }

    protected Long getTransactionIdByLabel(String label) {
        readLock();
        try {
            Set<Long> existingTxnIds = unprotectedGetTxnIdsByLabel(label);
            if (existingTxnIds == null || existingTxnIds.isEmpty()) {
                return null;
            }
            // find the latest txn (which id is largest)
            return existingTxnIds.stream().max(Comparator.comparingLong(Long::valueOf)).get();
        } finally {
            readUnlock();
        }
    }

    protected Long getTransactionIdByLabel(String label, List<TransactionStatus> statusList) throws UserException {
        readLock();
        try {
            TransactionState findTxn = null;
            for (TransactionStatus status : statusList) {
                Set<Long> existingTxns = unprotectedGetTxnIdsByLabel(label);
                if (existingTxns == null || existingTxns.isEmpty()) {
                    throw new TransactionNotFoundException("transaction not found, label=" + label);
                }
                for (Long txnId : existingTxns) {
                    TransactionState txn = unprotectedGetTransactionState(txnId);
                    if (txn.getTransactionStatus() == status) {
                        findTxn = txn;
                        break;
                    }
                }
            }

            if (findTxn == null) {
                throw new TransactionNotFoundException("running transaction not found, label=" + label);
            }

            return findTxn.getTransactionId();
        } finally {
            readUnlock();
        }
    }

    protected List<TransactionState> getPreCommittedTxnList() {
        readLock();
        try {
            // only send task to preCommitted transaction
            return idToRunningTransactionState.values().stream()
                    .filter(transactionState
                            -> (transactionState.getTransactionStatus() == TransactionStatus.PRECOMMITTED))
                    .sorted(Comparator.comparing(TransactionState::getPreCommitTime))
                    .collect(Collectors.toList());
        } finally {
            readUnlock();
        }
    }

    protected List<TransactionState> getCommittedTxnList() {
        List<TransactionState> committedTxnList = null;
        readLock();
        try {
            // only send task to committed transaction
            committedTxnList = idToRunningTransactionState.values().stream()
                                    .filter(transactionState ->
                                            (transactionState.getTransactionStatus() == TransactionStatus.COMMITTED))
                                    .sorted(Comparator.comparing(TransactionState::getCommitTime))
                                    .collect(Collectors.toList());
        } finally {
            readUnlock();
        }

        updateCommittedTxnCountPerTable(committedTxnList);

        return committedTxnList;
    }

    // given list of transactionstate, calculate committed trasactions for each table.
    protected void updateCommittedTxnCountPerTable(List<TransactionState> txnList) {
        // Control frequency by recording last update time
        long now = System.currentTimeMillis();
        // Only update if enough time has passed (e.g., 60 seconds)
        if (now - this.lastCommittedTxnCountUpdateTime < 60000 || Config.max_publishing_txn_num_per_table < 0) {
            return;
        }

        this.lastCommittedTxnCountUpdateTime = now;
        tableCommittedTxnCount.clear();
        for (TransactionState txn : txnList) {
            if (txn.getTransactionStatus() == TransactionStatus.COMMITTED) {
                List<Long> tableIds = txn.getTableIdList();
                if (tableIds != null) {
                    for (Long tableId : tableIds) {
                        tableCommittedTxnCount.put(tableId, tableCommittedTxnCount.getOrDefault(tableId, 0L) + 1);
                    }
                }
            }
        }
    }

    public void finishTransaction(long transactionId, Map<Long, Long> partitionVisibleVersions,
            Map<Long, Set<Long>> backendPartitions) throws UserException {
        if (DebugPointUtil.isEnable("DatabaseTransactionMgr.stop_finish_transaction")) {
            return;
        }

        TransactionState transactionState = null;
        readLock();
        try {
            transactionState = unprotectedGetTransactionState(transactionId);
        } finally {
            readUnlock();
        }

        // case 1 If database is dropped, then we just throw MetaNotFoundException, because all related tables are
        // already force dropped, we just ignore the transaction with all tables been force dropped.
        // case 2 If at least one table lock successfully, which means that the transaction should be finished for
        // the existed tables while just ignore tables which have been dropped forcefully.
        // case 3 Database exist and all tables already been dropped, this case is same with case1, just finish
        // the transaction with empty commit info only three cases mentioned above may happen, because user cannot
        // drop table without force while there are committed transactions on table and writeLockTablesIfExist is
        // a blocking function, the returned result would be the existed table list which hold write lock
        Database db = env.getInternalCatalog().getDbOrMetaException(transactionState.getDbId());
        List<Long> tableIdList;
        if (transactionState.getSubTxnIds() == null) {
            tableIdList = transactionState.getTableIdList();
        } else {
            tableIdList = transactionState.getSubTxnTableCommitInfos().stream().map(s -> s.getTableId()).distinct()
                    .collect(Collectors.toList());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("finish transaction {} with tables {}", transactionId, tableIdList);
        }
        List<? extends TableIf> tableList = db.getTablesOnIdOrderIfExist(tableIdList);
        if (!MetaLockUtils.tryWriteLockTablesIfExist(tableList, 10, TimeUnit.SECONDS)) {
            LOG.warn("finish transaction {} failed, get lock timeout with tables {}", transactionId, tableIdList);
            return;
        }
        PublishResult publishResult;
        try {
            // add all commit errors and publish errors to a single set
            Set<Long> errorReplicaIds = transactionState.getErrorReplicas();
            if (transactionState.getSubTxnIds() == null) {
                List<Pair<OlapTable, Partition>> relatedTblPartitions = Lists.newArrayList();
                if (!finishCheckPartitionVersion(transactionState, db, relatedTblPartitions)) {
                    return;
                }
                publishResult = finishCheckQuorumReplicas(transactionState, relatedTblPartitions, errorReplicaIds);
                if (publishResult == PublishResult.FAILED) {
                    return;
                }
            } else {
                if (!finishCheckPartitionVersionWithSubTxns(transactionState, db)) {
                    return;
                }
                publishResult = finishCheckQuorumReplicas(transactionState, errorReplicaIds);
                if (publishResult == PublishResult.FAILED) {
                    return;
                }
            }
            boolean txnOperated = false;
            EditLog.EditLogItem logItem = null;
            synchronized (transactionState) {
                transactionState.setErrorReplicas(errorReplicaIds);
                transactionState.setFinishTime(System.currentTimeMillis());
                transactionState.clearErrorMsg();
                transactionState.setTransactionStatus(TransactionStatus.VISIBLE);
                setTableVersion(transactionState, db);
                unprotectUpdateInMemoryState(transactionState, false);
                if (Config.enable_txn_log_outside_lock) {
                    logItem = enqueueTransactionState(transactionState);
                } else {
                    persistTransactionState(transactionState);
                }
                txnOperated = true;
                // TODO(cmy): We found a very strange problem. When delete-related transactions are processed here,
                // subsequent `updateCatalogAfterVisible()` is called, but it does not seem to be executed here
                // (because the relevant editlog does not see the log of visible transactions).
                // So I add a log here for observation.
                if (LOG.isDebugEnabled()) {
                    LOG.debug("after set transaction {} to visible", transactionState);
                }
            }
            try {
                transactionState.afterStateTransform(TransactionStatus.VISIBLE, txnOperated);
            } catch (Throwable e) {
                LOG.warn("afterStateTransform txn {} failed. exception: ", transactionState, e);
            }
            if (txnOperated) {
                awaitTransactionState(logItem, transactionState);
            }
            updateCatalogAfterVisible(transactionState, db, partitionVisibleVersions, backendPartitions);
        } finally {
            MetaLockUtils.writeUnlockTables(tableList);
        }
        // Here, we only wait for the EventProcessor to finish processing the event,
        // but regardless of the success or failure of the result,
        // it does not affect the logic of transaction
        try {
            produceEvent(transactionState, db);
        } catch (Throwable t) {
            // According to normal logic, no exceptions will be thrown,
            // but in order to avoid bugs affecting the original logic, all exceptions are caught
            LOG.warn("produceEvent failed: ", t);
        }

        // The visible latch should only be counted down after all things are done
        // (finish transaction, write edit log, etc).
        // Otherwise, there is no way for stream load to query the result right after loading finished,
        // even if we call "sync" before querying.
        transactionState.countdownVisibleLatch();
        LOG.info("finish transaction {} successfully, publish times {}, publish result {}",
                transactionState, transactionState.getPublishCount(), publishResult.name());
    }

    private void setTableVersion(TransactionState transactionState, Database db) {
        List<TableCommitInfo> tableCommitInfos;
        if (!transactionState.getSubTxnIdToTableCommitInfo().isEmpty()) {
            tableCommitInfos = transactionState.getSubTxnTableCommitInfos();
        } else {
            tableCommitInfos = Lists.newArrayList(transactionState.getIdToTableCommitInfos().values());
        }
        for (TableCommitInfo tableCommitInfo : tableCommitInfos) {
            long tableId = tableCommitInfo.getTableId();
            OlapTable table = (OlapTable) db.getTableNullable(tableId);
            if (table == null) {
                LOG.warn("table {} does not exist when setTableVersion. transaction: {}, db: {}",
                        tableId, transactionState.getTransactionId(), db.getId());
                continue;
            }
            tableCommitInfo.setVersion(table.getNextVersion());
            tableCommitInfo.setVersionTime(System.currentTimeMillis());
        }
    }

    private void produceEvent(TransactionState transactionState, Database db) throws AnalysisException {
        Collection<TableCommitInfo> tableCommitInfos;
        if (!transactionState.getSubTxnIdToTableCommitInfo().isEmpty()) {
            tableCommitInfos = transactionState.getSubTxnTableCommitInfos();
        } else {
            tableCommitInfos = transactionState.getIdToTableCommitInfos().values();
        }
        for (TableCommitInfo tableCommitInfo : tableCommitInfos) {
            long tableId = tableCommitInfo.getTableId();
            OlapTable table = (OlapTable) db.getTableNullable(tableId);
            if (table == null) {
                LOG.warn("table {} does not exist when produceEvent. transaction: {}, db: {}",
                        tableId, transactionState.getTransactionId(), db.getId());
                continue;
            }
            Env.getCurrentEnv().getEventProcessor().processEvent(
                    new DataChangeEvent(db.getCatalog().getId(), db.getId(), tableId));
        }
    }

    private boolean finishCheckPartitionVersion(TransactionState transactionState, Database db,
            List<Pair<OlapTable, Partition>> relatedTblPartitions) {
        Iterator<TableCommitInfo> tableCommitInfoIterator
                = transactionState.getIdToTableCommitInfos().values().iterator();
        while (tableCommitInfoIterator.hasNext()) {
            TableCommitInfo tableCommitInfo = tableCommitInfoIterator.next();
            long tableId = tableCommitInfo.getTableId();
            OlapTable table = (OlapTable) db.getTableNullable(tableId);
            // table maybe dropped between commit and publish, ignore this error
            if (table == null) {
                tableCommitInfoIterator.remove();
                LOG.warn("table {} is dropped, skip version check and remove it from transaction state {}",
                        tableId,
                        transactionState);
                continue;
            }

            Iterator<PartitionCommitInfo> partitionCommitInfoIterator
                    = tableCommitInfo.getIdToPartitionCommitInfo().values().iterator();
            while (partitionCommitInfoIterator.hasNext()) {
                PartitionCommitInfo partitionCommitInfo = partitionCommitInfoIterator.next();
                long partitionId = partitionCommitInfo.getPartitionId();
                Partition partition = table.getPartition(partitionId);
                // partition maybe dropped between commit and publish version, ignore this error
                if (partition == null) {
                    partitionCommitInfoIterator.remove();
                    LOG.warn("partition {} is dropped, skip version check"
                                    + " and remove it from transaction state {}", partitionId, transactionState);
                    continue;
                }
                if (partition.getVisibleVersion() != partitionCommitInfo.getVersion() - 1) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("for table {} partition {}, transactionId {} partition commitInfo version {} is not"
                                                + " equal with partition visible version {} plus one, need wait",
                                table.getId(), partition.getId(), transactionState.getTransactionId(),
                                partitionCommitInfo.getVersion(), partition.getVisibleVersion());
                    }
                    String errMsg = String.format("wait for publishing partition %d version %d."
                                    + " self version: %d. table %d", partitionId, partition.getVisibleVersion() + 1,
                            partitionCommitInfo.getVersion(), tableId);
                    transactionState.setErrorMsg(errMsg);
                    return false;
                }

                relatedTblPartitions.add(Pair.of(table, partition));
            }
        }

        return true;
    }

    private boolean finishCheckPartitionVersionWithSubTxns(TransactionState transactionState, Database db) {
        List<Pair<OlapTable, Partition>> relatedTblPartitions = new ArrayList<>();
        Map<Long, Long> partitionToVisibleVersion = new HashMap<>();
        Iterator<Long> iterator = transactionState.getSubTxnIds().iterator();
        while (iterator.hasNext()) {
            long subTxnId = iterator.next();
            TableCommitInfo tableCommitInfo = transactionState.getTableCommitInfoBySubTxnId(subTxnId);
            if (tableCommitInfo == null) {
                continue;
            }
            long tableId = tableCommitInfo.getTableId();
            OlapTable table = (OlapTable) db.getTableNullable(tableId);
            // table maybe dropped between commit and publish, ignore this error
            if (table == null) {
                iterator.remove();
                LOG.warn("table {} is dropped, skip version check and remove it from transaction state {}",
                        tableId,
                        transactionState);
                continue;
            }

            Iterator<PartitionCommitInfo> partitionCommitInfoIterator
                    = tableCommitInfo.getIdToPartitionCommitInfo().values().iterator();
            while (partitionCommitInfoIterator.hasNext()) {
                PartitionCommitInfo partitionCommitInfo = partitionCommitInfoIterator.next();
                long partitionId = partitionCommitInfo.getPartitionId();
                Partition partition = table.getPartition(partitionId);
                // partition maybe dropped between commit and publish version, ignore this error
                if (partition == null) {
                    partitionCommitInfoIterator.remove();
                    LOG.warn("partition {} is dropped, skip version check"
                            + " and remove it from transaction state {}", partitionId, transactionState);
                    continue;
                }
                long curPartitionVersion = partitionToVisibleVersion.getOrDefault(partitionId,
                        partition.getVisibleVersion());
                if (curPartitionVersion != partitionCommitInfo.getVersion() - 1) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("for table {} partition {}, transactionId {}, subTransactionId {},"
                                        + " partition commitInfo version {} is not"
                                        + " equal with partition expected version {}, visible version {}, need wait",
                                table.getId(), partition.getId(), transactionState.getTransactionId(), subTxnId,
                                partitionCommitInfo.getVersion(), curPartitionVersion + 1,
                                partition.getVisibleVersion());
                    }
                    String errMsg = String.format("wait for publishing partition %d version %d, visible version %d."
                                    + " self version: %d. table %d, transactionId %d, subTransactionId %d", partitionId,
                            curPartitionVersion + 1, partition.getVisibleVersion(), partitionCommitInfo.getVersion(),
                            tableId, transactionState.getTransactionId(), subTxnId);
                    transactionState.setErrorMsg(errMsg);
                    return false;
                }
                partitionToVisibleVersion.put(partitionId, partitionCommitInfo.getVersion());

                relatedTblPartitions.add(Pair.of(table, partition));
            }
        }

        return true;
    }

    private PublishResult finishCheckQuorumReplicas(TransactionState transactionState,
            List<Pair<OlapTable, Partition>> relatedTblPartitions,
            Set<Long> errorReplicaIds) {
        long now = System.currentTimeMillis();
        long firstPublishVersionTime = transactionState.getFirstPublishVersionTime();
        boolean allowPublishOneSucc = false;
        if (Config.publish_wait_time_second > 0 && firstPublishVersionTime > 0
                && now >= firstPublishVersionTime + Config.publish_wait_time_second * 1000L) {
            allowPublishOneSucc = true;
        }

        List<Replica> tabletSuccReplicas = Lists.newArrayList();
        List<Replica> tabletWriteFailedReplicas = Lists.newArrayList();
        List<Replica> tabletVersionFailedReplicas = Lists.newArrayList();
        TabletsPublishResultLogs logs = new TabletsPublishResultLogs();

        Map<Long, List<PublishVersionTask>> publishTasks = transactionState.getPublishVersionTasks();
        PublishResult publishResult = PublishResult.QUORUM_SUCC;
        for (Pair<OlapTable, Partition> pair : relatedTblPartitions) {
            OlapTable table = pair.key();
            Partition partition = pair.value();
            long tableId = table.getId();
            long partitionId = partition.getId();
            long newVersion = partition.getVisibleVersion() + 1;
            int loadRequiredReplicaNum = table.getLoadRequiredReplicaNum(partitionId);
            List<MaterializedIndex> allIndices;
            if (transactionState.getLoadedTblIndexes().isEmpty()) {
                allIndices = partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL, true);
            } else {
                allIndices = Lists.newArrayList();
                for (long indexId : transactionState.getLoadedTblIndexes().get(tableId)) {
                    MaterializedIndex index = partition.getIndex(indexId);
                    if (index != null) {
                        allIndices.add(index);
                    }
                }
            }

            long maxAlterWaterschedTxnId = getMaxAlterWaterschedTxnId(table);

            // check success replica number for each tablet.
            // a success replica means:
            //  1. Not in errorReplicaIds: succeed in both commit and publish phase
            //  2. last failed version < 0: is a health replica before
            //  3. version catch up: not with a stale version
            // Here we only check number, the replica version will be updated in updateCatalogAfterVisible()
            for (MaterializedIndex index : allIndices) {
                for (Tablet tablet : index.getTablets()) {
                    tabletSuccReplicas.clear();
                    tabletWriteFailedReplicas.clear();
                    tabletVersionFailedReplicas.clear();
                    for (Replica replica : tablet.getReplicas()) {
                        List<PublishVersionTask> publishVersionTasks
                                = publishTasks.get(replica.getBackendIdWithoutException());
                        Preconditions.checkState(publishVersionTasks == null || publishVersionTasks.size() == 1,
                                "publish tasks: " + publishVersionTasks);
                        PublishVersionTask publishVersionTask = null;
                        if (publishVersionTasks != null) {
                            publishVersionTask = publishVersionTasks.get(0);
                        }
                        checkReplicaContinuousVersionSucc(Lists.newArrayList(transactionState.getTransactionId()),
                                maxAlterWaterschedTxnId, tablet.getId(), replica, newVersion, newVersion,
                                Lists.newArrayList(publishVersionTask), errorReplicaIds, tabletSuccReplicas,
                                tabletWriteFailedReplicas, tabletVersionFailedReplicas);
                    }

                    publishResult = checkQuorumReplicas(transactionState, tableId, partition, tablet,
                            loadRequiredReplicaNum, allowPublishOneSucc, newVersion, tabletSuccReplicas,
                            tabletWriteFailedReplicas, tabletVersionFailedReplicas, publishResult, logs);
                    if (publishResult == PublishResult.QUORUM_SUCC) {
                        tablet.setLastLoadFailedTime(-1L);
                    }
                }
            }
        }

        boolean needLog = publishResult != PublishResult.FAILED
                || now - transactionState.getLastPublishLogTime() > Config.publish_fail_log_interval_second * 1000L;
        if (needLog) {
            transactionState.setLastPublishLogTime(now);
            logs.log();
        }

        return publishResult;
    }

    private long getMaxAlterWaterschedTxnId(OlapTable table) {
        List<AlterJobV2> unfinishedAlterJobs;
        if (table.getState() == OlapTable.OlapTableState.SCHEMA_CHANGE) {
            unfinishedAlterJobs = Env.getCurrentEnv().getAlterInstance().getSchemaChangeHandler()
                    .getUnfinishedAlterJobV2ByTableId(table.getId());
        } else if (table.getState() == OlapTable.OlapTableState.ROLLUP) {
            unfinishedAlterJobs = Env.getCurrentEnv().getAlterInstance().getMaterializedViewHandler()
                    .getUnfinishedAlterJobV2ByTableId(table.getId());
        } else {
            unfinishedAlterJobs = Lists.newArrayList();
        }
        return unfinishedAlterJobs.stream().mapToLong(AlterJobV2::getWatershedTxnId).max().orElse(-1);
    }

    private void checkReplicaContinuousVersionSucc(List<Long> subTxnIds, long alterWaterschedTxnId, long tabletId,
            Replica replica, long minReplicaVersion, long maxReplicaVersion,
            List<PublishVersionTask> replicaPublishTasks, Set<Long> errorReplicaIds, List<Replica> tabletSuccReplicas,
            List<Replica> tabletWriteFailedReplicas, List<Replica> tabletVersionFailedReplicas) {
        boolean success = true;
        for (int i = 0; i < subTxnIds.size(); i++) {
            PublishVersionTask task = replicaPublishTasks.get(i);
            // Defensive null guard: AgentTaskCleanupDaemon may force-finish a PublishVersionTask
            // without populating succTablets; MasterImpl.finishPublishVersion may also call
            // setSuccTablets(null) on a non-OK BE response.
            Map<Long, Long> succ = (task == null) ? null : task.getSuccTablets();
            success = (task != null && task.isFinished() && succ != null && succ.containsKey(tabletId)) || (
                    replica.getState() == Replica.ReplicaState.ALTER && (!Config.publish_version_check_alter_replica
                            || subTxnIds.get(i) < alterWaterschedTxnId || alterWaterschedTxnId == -1));
            if (!success) {
                break;
            }
        }
        if (success) {
            errorReplicaIds.remove(replica.getId());
        } else {
            errorReplicaIds.add(replica.getId());
        }

        if (!errorReplicaIds.contains(replica.getId())) {
            if (replica.checkVersionCatchUp(minReplicaVersion - 1, true)) {
                tabletSuccReplicas.add(replica);
            } else {
                tabletVersionFailedReplicas.add(replica);
            }
        } else if (replica.getVersion() >= maxReplicaVersion) {
            tabletSuccReplicas.add(replica);
            errorReplicaIds.remove(replica.getId());
        } else {
            tabletWriteFailedReplicas.add(replica);
        }
    }
    // 核心职责是：在完成了副本 Quorum 多数派校验后，正式将内存中的事务状态切换为预提交状态（PRECOMMITTED），
    // 构建并绑定本次事务涉及的表与分区提交元数据（TableCommitInfo / PartitionCommitInfo），更新事务管理器的内存索引状态。
    protected void unprotectedPreCommitTransaction2PC(TransactionState transactionState, Set<Long> errorReplicaIds,
                                                Map<Long, Set<Long>> tableToPartition, Set<Long> totalInvolvedBackends,
                                                Database db) {
        // transaction state is modified during check if the transaction could committed
        // 1. 事务状态幂等校验与防重
        if (transactionState.getTransactionStatus() != TransactionStatus.PREPARE) {
            return;
        }
        // update transaction state version
        // 2. 更新事务的核心预提交状态与异常副本
        // 记录当前系统时间作为本次事务的预提交时间戳（Pre-Commit Time），用于后续事务超时监控与统计。
        transactionState.setPreCommitTime(System.currentTimeMillis());
        // 将事务状态从 PREPARE 正式修改为 PRECOMMITTED（已预提交）。两阶段提交（2PC）的第一阶段在内存中完成标记。
        transactionState.setTransactionStatus(TransactionStatus.PRECOMMITTED);
        // 将写入失败的副本集合记录到事务状态中，后续在 Commit 阶段，事务管理器会将这些副本标记为版本落后，以便触发后续修复（Repair）。
        transactionState.setErrorReplicas(errorReplicaIds);
        // 3. 构建表与分区的提交元数据（TableCommitInfo & PartitionCommitInfo）
        for (long tableId : tableToPartition.keySet()) {
            // 从数据库中获取对应表的 OlapTable 元数据对象。
            OlapTable table = (OlapTable) db.getTableNullable(tableId);
            // 实例化表提交信息对象 TableCommitInfo，用于收集该表下所有变动分区的提交元数据。
            TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
            // 获取表的分区元数据定义（用于后续提取分区的 Range 范围描述等）。
            PartitionInfo tblPartitionInfo = table.getPartitionInfo();
            for (long partitionId : tableToPartition.get(tableId)) {
                // 遍历当前表下被实际写入数据的每一个分区 ID。
                String partitionRange = tblPartitionInfo.getPartitionRangeString(partitionId);
                PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(
                        partitionId, partitionRange, -1, -1,
                        table.isTemporaryPartition(partitionId));
                tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);
            }
            transactionState.putIdToTableCommitInfo(tableId, tableCommitInfo);
        }
        // Update in-memory state only; caller handles edit log persistence
        // 4. 更新内存索引与关联 Backend 节点
        // 更新 DatabaseTransactionMgr 内部的内存数据结构（例如维护表 ID 到运行中事务的映射表、更新事务超时定时器等）。
        // 注释强调：此步骤仅更新内存状态（In-memory state），元数据持久化 EditLog 的写入由外层调用方（preCommitTransaction2PC 主流程）统一处理。
        unprotectUpdateInMemoryState(transactionState, false);
        // 将参与本次事务的所有 BE 节点 ID 集合记录到 transactionState 中，用于后续 Publish Version（版本发布）阶段向这些 BE 节点下发最终的 Commit 命令。
        transactionState.setInvolvedBackends(totalInvolvedBackends);
    }

    private PartitionCommitInfo generatePartitionCommitInfo(OlapTable table, long partitionId, long partitionVersion) {
        PartitionInfo tblPartitionInfo = table.getPartitionInfo();
        String partitionRange = tblPartitionInfo.getPartitionRangeString(partitionId);
        return new PartitionCommitInfo(partitionId, partitionRange,
                partitionVersion, System.currentTimeMillis() /* use as partition visible time */,
                table.isTemporaryPartition(partitionId));
    }

    private PartitionCommitInfo generatePartitionCommitInfo(OlapTable table, long partitionId, long partitionVersion,
                                                            long commitTSO) {
        PartitionInfo tblPartitionInfo = table.getPartitionInfo();
        String partitionRange = tblPartitionInfo.getPartitionRangeString(partitionId);
        return new PartitionCommitInfo(partitionId, partitionRange,
                partitionVersion, System.currentTimeMillis(), table.isTemporaryPartition(partitionId), commitTSO);
    }

    protected void unprotectedCommitTransaction(TransactionState transactionState, Set<Long> errorReplicaIds,
                                                Map<Long, Set<Long>> tableToPartition, Set<Long> totalInvolvedBackends,
                                                Database db) throws TransactionCommitFailedException {
        // transaction state is modified during check if the transaction could committed
        if (transactionState.getTransactionStatus() != TransactionStatus.PREPARE) {
            return;
        }
        // update transaction state version
        long commitTime = System.currentTimeMillis();
        transactionState.setCommitTime(commitTime);
        long commitTSO = TransactionUtil.getCommitTSO(transactionState.getTransactionId(), db,
                tableToPartition.keySet());
        transactionState.setCommitTSO(commitTSO);

        if (MetricRepo.isInit) {
            MetricRepo.HISTO_TXN_EXEC_LATENCY.update(commitTime - transactionState.getPrepareTime());
        }
        transactionState.setErrorReplicas(errorReplicaIds);
        for (long tableId : tableToPartition.keySet()) {
            OlapTable table = (OlapTable) db.getTableNullable(tableId);
            TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
            if (Config.enable_feature_binlog && table.enableTso()) {
                tableCommitInfo.setCommitTSO(commitTSO);
            }

            for (long partitionId : tableToPartition.get(tableId)) {
                Partition partition = table.getPartition(partitionId);
                if (Config.enable_feature_binlog && table.enableTso()) {
                    tableCommitInfo.addPartitionCommitInfo(
                            generatePartitionCommitInfo(table, partitionId, partition.getNextVersion(), commitTSO));
                } else {
                    tableCommitInfo.addPartitionCommitInfo(
                            generatePartitionCommitInfo(table, partitionId, partition.getNextVersion()));
                }
            }
            transactionState.putIdToTableCommitInfo(tableId, tableCommitInfo);
        }
        // set COMMITTED only after idToTableCommitInfos is fully populated, so that
        // PublishVersionDaemon (which checks status without synchronized(transactionState))
        // will never see COMMITTED with empty commit info.
        transactionState.setTransactionStatus(TransactionStatus.COMMITTED);
        // Update in-memory state only; caller handles edit log persistence
        unprotectUpdateInMemoryState(transactionState, false);
        transactionState.setInvolvedBackends(totalInvolvedBackends);
    }

    protected void unprotectedCommitTransaction(TransactionState transactionState, Set<Long> errorReplicaIds,
            Map<Long, Set<Long>> subTxnToPartition, Set<Long> totalInvolvedBackends,
            List<SubTransactionState> subTransactionStates, Database db) throws TransactionCommitFailedException {
        // transaction state is modified during check if the transaction could committed
        if (transactionState.getTransactionStatus() != TransactionStatus.PREPARE) {
            return;
        }
        // update transaction state version
        long commitTime = System.currentTimeMillis();
        transactionState.setCommitTime(commitTime);
        Set<Long> tableIds = new HashSet<>();
        for (SubTransactionState subTransactionState : subTransactionStates) {
            long tableId = subTransactionState.getTable().getId();
            tableIds.add(tableId);
        }
        long commitTSO = TransactionUtil.getCommitTSO(transactionState.getTransactionId(), db, tableIds);
        transactionState.setCommitTSO(commitTSO);

        if (MetricRepo.isInit) {
            MetricRepo.HISTO_TXN_EXEC_LATENCY.update(commitTime - transactionState.getPrepareTime());
        }
        transactionState.setErrorReplicas(errorReplicaIds);

        Map<Long, List<SubTransactionState>> tableToSubTransactionState = new HashMap<>();
        for (SubTransactionState subTransactionState : subTransactionStates) {
            long tableId = subTransactionState.getTable().getId();
            tableToSubTransactionState.computeIfAbsent(tableId, k -> new ArrayList<>()).add(subTransactionState);
        }

        for (Entry<Long, List<SubTransactionState>> entry : tableToSubTransactionState.entrySet()) {
            long tableId = entry.getKey();
            List<SubTransactionState> subTransactionStateList = entry.getValue();

            OlapTable table = (OlapTable) db.getTableNullable(tableId);
            long tableNextVersion = table.getNextVersion();
            Map<Long, Long> partitionToVersion = new HashMap<>();

            for (SubTransactionState subTransactionState : subTransactionStateList) {
                Set<Long> partitionIds = subTxnToPartition.get(subTransactionState.getSubTransactionId());
                if (partitionIds == null) {
                    continue;
                }
                TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
                tableCommitInfo.setVersion(tableNextVersion);
                tableCommitInfo.setVersionTime(System.currentTimeMillis());
                if (Config.enable_feature_binlog && table.enableTso()) {
                    tableCommitInfo.setCommitTSO(commitTSO);
                }

                for (long partitionId : partitionIds) {
                    long partitionNextVersion = table.getPartition(partitionId).getNextVersion();
                    if (partitionToVersion.containsKey(partitionId)) {
                        partitionNextVersion = partitionToVersion.get(partitionId) + 1;
                    }
                    partitionToVersion.put(partitionId, partitionNextVersion);

                    PartitionCommitInfo partitionCommitInfo;
                    if (Config.enable_feature_binlog && table.enableTso()) {
                        partitionCommitInfo = generatePartitionCommitInfo(table, partitionId,
                                partitionNextVersion, commitTSO);
                    } else {
                        partitionCommitInfo = generatePartitionCommitInfo(table, partitionId,
                                partitionNextVersion);
                    }
                    tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);
                    LOG.info("commit txn_id={}, sub_txn_id={}, partition_id={}, version={}",
                            transactionState.getTransactionId(), subTransactionState.getSubTransactionId(),
                            partitionId, partitionNextVersion);
                }
                transactionState.addSubTxnTableCommitInfo(subTransactionState, tableCommitInfo);
            }
        }
        // set COMMITTED only after all commit info is fully populated, so that
        // PublishVersionDaemon (which checks status without synchronized(transactionState))
        // will never see COMMITTED with empty commit info.
        transactionState.setTransactionStatus(TransactionStatus.COMMITTED);
        // Update in-memory state only; caller handles edit log persistence
        unprotectUpdateInMemoryState(transactionState, false);
        transactionState.setInvolvedBackends(totalInvolvedBackends);
    }

    protected void unprotectedCommitTransaction2PC(TransactionState transactionState, Database db)
            throws TransactionCommitFailedException {
        // transaction state is modified during check if the transaction could committed
        if (transactionState.getTransactionStatus() != TransactionStatus.PRECOMMITTED) {
            LOG.warn("Unknown exception. state of transaction [{}] changed, failed to commit transaction",
                    transactionState.getTransactionId());
            return;
        }
        // update transaction state version
        transactionState.setCommitTime(System.currentTimeMillis());
        long commitTSO = TransactionUtil.getCommitTSO(transactionState.getTransactionId(), db,
                transactionState.getIdToTableCommitInfos().keySet());
        transactionState.setCommitTSO(commitTSO);

        transactionState.setTransactionStatus(TransactionStatus.COMMITTED);

        Iterator<TableCommitInfo> tableCommitInfoIterator
                = transactionState.getIdToTableCommitInfos().values().iterator();
        while (tableCommitInfoIterator.hasNext()) {
            TableCommitInfo tableCommitInfo = tableCommitInfoIterator.next();
            long tableId = tableCommitInfo.getTableId();
            OlapTable table = (OlapTable) db.getTableNullable(tableId);
            // table maybe dropped between commit and publish, ignore this error
            if (table == null) {
                tableCommitInfoIterator.remove();
                LOG.warn("table {} is dropped, skip and remove it from transaction state {}",
                        tableId,
                        transactionState);
                continue;
            }
            if (Config.enable_feature_binlog && table.enableTso()) {
                tableCommitInfo.setCommitTSO(commitTSO);
            }
            Iterator<PartitionCommitInfo> partitionCommitInfoIterator
                    = tableCommitInfo.getIdToPartitionCommitInfo().values().iterator();
            while (partitionCommitInfoIterator.hasNext()) {
                PartitionCommitInfo partitionCommitInfo = partitionCommitInfoIterator.next();
                long partitionId = partitionCommitInfo.getPartitionId();
                Partition partition = table.getPartition(partitionId);
                // partition maybe dropped between commit and publish version, ignore this error
                if (partition == null) {
                    partitionCommitInfoIterator.remove();
                    LOG.warn("partition {} is dropped, skip and remove it from transaction state {}",
                            partitionId,
                            transactionState);
                    continue;
                }
                partitionCommitInfo.setVersion(partition.getNextVersion());
                partitionCommitInfo.setVersionTime(System.currentTimeMillis());
                if (Config.enable_feature_binlog && table.enableTso()) {
                    partitionCommitInfo.setTso(commitTSO);
                }
            }
        }
        // Update in-memory state only; caller handles edit log persistence
        unprotectUpdateInMemoryState(transactionState, false);
    }

    /**
     * Replays a transaction state update from the edit log.
     * Only called during replay (follower sync or restart), so no edit log persistence is needed.
     *
     * @param transactionState the transaction state to replay
     * @param isReplay must be true (only used in replay path)
     */
    protected void unprotectUpsertTransactionState(TransactionState transactionState, boolean isReplay) {
        unprotectUpdateInMemoryState(transactionState, isReplay);
    }

    /**
     * Updates only in-memory transaction state without persisting to edit log.
     * This method must be called while holding the write lock.
     * Use this method in combination with {@link #persistTransactionState} to reduce lock contention
     * by moving edit log writes outside the lock.
     *
     * @param transactionState the transaction state to update
     * @param isReplay true if this is a replay operation (edit log already contains this state)
     */
    protected void unprotectUpdateInMemoryState(TransactionState transactionState, boolean isReplay) {
        if (!transactionState.getTransactionStatus().isFinalStatus()) {
            if (idToRunningTransactionState.put(transactionState.getTransactionId(), transactionState) == null) {
                runningTxnNums.incrementAndGet();
            }
            if (isReplay && transactionState.getSubTxnIds() != null) {
                LOG.info("add sub transactions for txn_id={}, status={}, sub_txn_ids={}",
                        transactionState.getTransactionId(), transactionState.getTransactionStatus(),
                        transactionState.getSubTxnIds());
                for (Long subTxnId : transactionState.getSubTxnIds()) {
                    addSubTransaction(transactionState.getTransactionId(), subTxnId);
                }
            }
        } else {
            if (idToRunningTransactionState.remove(transactionState.getTransactionId()) != null) {
                runningTxnNums.decrementAndGet();
            }
            idToFinalStatusTransactionState.put(transactionState.getTransactionId(), transactionState);
            if (transactionState.isShortTxn()) {
                finalStatusTransactionStateDequeShort.add(transactionState);
            } else {
                finalStatusTransactionStateDequeLong.add(transactionState);
            }
            if (transactionState.getSubTxnIds() != null) {
                LOG.info("clean sub transactions for txn_id={}, sub_txn_ids={}", transactionState.getTransactionId(),
                        transactionState.getSubTxnIds());
                cleanSubTransactions(transactionState.getTransactionId());
            }
        }
        if (isReplay) {
            updateTxnLabels(transactionState);
        }
    }

    /**
     * Persists the transaction state to edit log synchronously.
     * For PREPARE transactions with non-FRONTEND source type, persistence is skipped
     * because losing them only requires the client to retry.
     */
    // Apache Doris 中数据库事务管理器内部专门负责同步持久化事务状态（将 TransactionState 写入元数据日志 EditLog）的保护方法（protected）。
    // 核心价值在于通过精准判断事务的状态和来源类型，对无须落盘的事务跳过 EditLog 持久化，从而显著降低 FE（Frontend）在处理海量高频导入任务时的磁盘 I/O 压力与元数据日志膨胀速度。
    protected void persistTransactionState(TransactionState transactionState) {
        // 事务状态不处于 PREPARE 阶段（例如当前正在处理的 PRECOMMITTED、COMMITTED、VISIBLE、ABORTED 等关键状态）。
        // 一旦事务跨越了 PREPARE 阶段进入预提交或提交阶段，数据版本与状态必须强一致并持久化，因此必须写入 EditLog。
        // 事务的导入来源类型为 FRONTEND（例如由 FE 节点自身内部驱动并异步运行的 Insert Into语句、Broker Load、Routine Load 等后台任务）。
        if (transactionState.getTransactionStatus() != TransactionStatus.PREPARE
                || transactionState.getSourceType() == TransactionState.LoadJobSourceType.FRONTEND) {
            // if this is a prepare txn, and load source type is not FRONTEND
            // no need to persist it. if prepare txn lost, the following commit will just be failed.
            // user only need to retry this txn.
            // The FRONTEND type txn is committed and running asynchronously, so we have to persist it.
            // 当满足上述任一条件时（即：不是 PREPARE 状态，或者来源是 FRONTEND），调用 editLog 将当前 transactionState 序列化并同步写入元数据持久化日志中。
            editLog.logInsertTransactionState(transactionState);
        }
    }

    /**
     * Enqueue a transaction state edit log entry without waiting for persistence.
     * Must be called inside the write lock to preserve ordering via the FIFO queue.
     *
     * @return an {@link EditLog.EditLogItem} handle to await completion, or null if persistence is skipped
     */
    protected EditLog.EditLogItem enqueueTransactionState(TransactionState transactionState) {
        if (transactionState.getTransactionStatus() != TransactionStatus.PREPARE
                || transactionState.getSourceType() == TransactionState.LoadJobSourceType.FRONTEND) {
            return editLog.submitEdit(OperationType.OP_UPSERT_TRANSACTION_STATE, transactionState);
        }
        return null;
    }

    /**
     * Await completion of a previously enqueued transaction state edit log entry.
     * Should be called outside the write lock. Handles binlog and timing logic.
     *
     * @param item the handle returned by {@link #enqueueTransactionState}, may be null
     * @param transactionState the transaction state (for binlog and timing)
     */
    // 专门配合异步 EditLog 刷盘机制（即配置 Config.enable_txn_log_outside_lock = true 时）使用的等待与后处理辅助方法。
    // 事务状态已被封装为 EditLog.EditLogItem 并放入后台异步写日志队列。为了确保在向客户端返回“预提交（Pre-Commit）”或“提交（Commit）”成功之前，元数据已安全落盘并生成 Binlog，必须调用此方法在释放了事务/元数据锁之后阻塞等待 EditLog 持久化完成。
    // 必须在锁外部（outside the write lock）调用。因为等待 BDBJE / 磁盘 I/O 刷盘可能耗时较长，在锁外部等待可以避免阻塞其他线程对元数据的并发读写。同时，该方法内部还会处理 Binlog 生成以及写日志耗时的监控统计。
    protected void awaitTransactionState(EditLog.EditLogItem item, TransactionState transactionState) {
        // 如果传入的 item 为 null（例如系统关闭了 enable_txn_log_outside_lock 走同步写模式，或者由于某些无需持久化的场景未生成异步日志句柄），说明无需异步等待，直接返回。
        if (item == null) {
            return;
        }
        long start = System.currentTimeMillis();
        // 核心阻塞点。挂起当前线程，等待后台 EditLog 刷新线程将该事务日志写入 BDBJE 存储并返回分配的全局 EditLog ID（logId）。
        long logId = item.await();
        long logEditEnd = System.currentTimeMillis();
        long end = logEditEnd;
        // 3. Binlog 增量记录生成（针对交易终态 VISIBLE）
        // 判断事务状态是否已达到最终的 VISIBLE（版本可见/提交完成） 阶段。在预提交（Pre-Commit）阶段状态为 PRECOMMITTED，不会触发此分支。
        if (transactionState.getTransactionStatus() == TransactionStatus.VISIBLE) {
            UpsertRecord record = new UpsertRecord(logId, transactionState);
            // 将该记录添加到 Doris 的 BinlogManager 中，用于数据变更订阅、跨集群 CDC（Change Data Capture）同步或特征流管道。
            Env.getCurrentEnv().getBinlogManager().addUpsertRecord(record);
            end = System.currentTimeMillis();
        }
        // 若总耗时超标，打印 WARN 级别的日志，并细分输出 BDBJE 写磁盘耗时（logEditEnd - start） 与 Binlog 写入耗时（end - logEditEnd），便于定位到底是磁盘 I/O 瓶颈还是 Binlog 处理瓶颈。
        if (end - start > Config.lock_reporting_threshold_ms) {
            LOG.warn("edit log insert transaction take a lot time, write bdb {} ms, write binlog {} ms",
                    logEditEnd - start, end - logEditEnd);
        }
    }

    public int getRunningTxnNumsWithLock() {
        return runningTxnNums.get();
    }

    private void updateTxnLabels(TransactionState transactionState) {
        Set<Long> txnIds = labelToTxnIds.computeIfAbsent(transactionState.getLabel(), k -> Sets.newHashSet());
        txnIds.add(transactionState.getTransactionId());
    }

    public void abortTransaction(String label, String reason) throws UserException {
        Preconditions.checkNotNull(label);
        List<TransactionStatus> status = new ArrayList<>();
        status.add(TransactionStatus.PREPARE);
        long transactionId = getTransactionIdByLabel(label, status);
        abortTransaction(transactionId, reason, null);
    }

    public void abortTransaction(long transactionId, String reason, TxnCommitAttachment txnCommitAttachment)
            throws UserException {
        if (transactionId < 0) {
            LOG.info("transaction id is {}, less than 0, maybe this is an old type load job,"
                    + " ignore abort operation", transactionId);
            return;
        }
        TransactionState transactionState = null;
        readLock();
        try {
            transactionState = idToRunningTransactionState.get(transactionId);
        } finally {
            readUnlock();
        }
        if (transactionState == null) {
            throw new TransactionNotFoundException("transaction not found", transactionId);
        }

        // update transaction state extra if exists
        if (txnCommitAttachment != null) {
            transactionState.setTxnCommitAttachment(txnCommitAttachment);
        }

        // before state transform
        transactionState.beforeStateTransform(TransactionStatus.ABORTED);
        boolean txnOperated = false;
        EditLog.EditLogItem logItem = null;
        synchronized (transactionState) {
            txnOperated = unprotectAbortTransaction(transactionId, reason);
            if (txnOperated) {
                if (Config.enable_txn_log_outside_lock) {
                    logItem = enqueueTransactionState(transactionState);
                } else {
                    persistTransactionState(transactionState);
                }
            }
        }
        transactionState.afterStateTransform(TransactionStatus.ABORTED, txnOperated, reason);
        if (txnOperated) {
            awaitTransactionState(logItem, transactionState);
        }

        // send clear txn task to BE to clear the transactions on BE.
        // This is because parts of a txn may succeed in some BE, and these parts of txn should be cleared
        // explicitly, or it will be remained on BE forever
        // (However the report process will do the diff and send clear txn tasks to BE, but that is our
        // last defense)
        if (txnOperated && transactionState.getTransactionStatus() == TransactionStatus.ABORTED) {
            clearBackendTransactions(transactionState);
        }

        LOG.info("abort transaction: {} successfully", transactionState);
    }

    public void abortTransaction2PC(long transactionId) throws UserException {
        LOG.info("begin to abort txn {}", transactionId);
        if (transactionId < 0) {
            LOG.info("transaction id is {}, less than 0, maybe this is an old type load job,"
                    + " ignore abort operation", transactionId);
            return;
        }
        TransactionState transactionState;
        readLock();
        try {
            transactionState = unprotectedGetTransactionState(transactionId);
        } finally {
            readUnlock();
        }

        if (transactionState == null) {
            throw new TransactionNotFoundException("transaction [" + transactionId + "] not found");
        }

        // before state transform
        transactionState.beforeStateTransform(TransactionStatus.ABORTED);
        boolean txnOperated = false;
        EditLog.EditLogItem logItem = null;
        synchronized (transactionState) {
            txnOperated = unprotectAbortTransaction(transactionId, "User Abort");
            if (txnOperated) {
                if (Config.enable_txn_log_outside_lock) {
                    logItem = enqueueTransactionState(transactionState);
                } else {
                    persistTransactionState(transactionState);
                }
            }
        }
        transactionState.afterStateTransform(TransactionStatus.ABORTED, txnOperated, "User Abort");
        if (txnOperated) {
            awaitTransactionState(logItem, transactionState);
        }

        // send clear txn task to BE to clear the transactions on BE.
        // This is because parts of a txn may succeed in some BE, and these parts of txn should be cleared
        // explicitly, or it will be remained on BE forever
        // (However the report process will do the diff and send clear txn tasks to BE, but that is our
        // last defense)
        if (txnOperated && transactionState.getTransactionStatus() == TransactionStatus.ABORTED) {
            clearBackendTransactions(transactionState);
        }
        LOG.info("abort transaction: {} successfully", transactionState);
    }

    private boolean unprotectAbortTransaction(long transactionId, String reason)
            throws UserException {
        TransactionState transactionState = unprotectedGetTransactionState(transactionId);
        if (transactionState == null) {
            throw new TransactionNotFoundException("transaction [" + transactionId + "] not found.");
        }
        if (transactionState.getTransactionStatus() == TransactionStatus.ABORTED) {
            throw new TransactionNotFoundException("transaction [" + transactionId + "] is already aborted, "
                    + "abort reason: " + transactionState.getReason());
        }
        if (transactionState.getTransactionStatus() == TransactionStatus.COMMITTED
                || transactionState.getTransactionStatus() == TransactionStatus.VISIBLE) {
            throw new UserException("transaction [" + transactionId + "] is already "
                    + transactionState.getTransactionStatus() + ", could not abort.");
        }
        transactionState.setFinishTime(System.currentTimeMillis());
        transactionState.setReason(reason);
        transactionState.setTransactionStatus(TransactionStatus.ABORTED);
        // Update in-memory state only; caller handles edit log persistence
        unprotectUpdateInMemoryState(transactionState, false);
        return true;
    }

    private void clearBackendTransactions(TransactionState transactionState) {
        Preconditions.checkState(transactionState.getTransactionStatus() == TransactionStatus.ABORTED);
        // for aborted transaction, we don't know which backends are involved, so we have to send clear task
        // to all backends.
        List<Long> allBeIds = Env.getCurrentSystemInfo().getAllBackendIds(false);
        AgentBatchTask batchTask = null;
        synchronized (clearTransactionTasks) {
            for (Long beId : allBeIds) {
                ClearTransactionTask task = new ClearTransactionTask(
                        beId, transactionState.getTransactionId(), Lists.newArrayList());
                clearTransactionTasks.add(task);
            }
            if (transactionState.getSubTxnIds() != null) {
                for (long subTxnId : transactionState.getSubTxnIds()) {
                    for (Long beId : allBeIds) {
                        ClearTransactionTask task = new ClearTransactionTask(beId, subTxnId, Lists.newArrayList());
                        clearTransactionTasks.add(task);
                    }
                }
            }

            // try to group send tasks, not sending every time a txn is aborted. to avoid too many task rpc.
            if (clearTransactionTasks.size() > allBeIds.size() * 2) {
                batchTask = new AgentBatchTask();
                for (ClearTransactionTask clearTransactionTask : clearTransactionTasks) {
                    batchTask.addTask(clearTransactionTask);
                }
                clearTransactionTasks.clear();
            }
        }

        if (batchTask != null) {
            AgentTaskExecutor.submit(batchTask);
        }
    }


    protected List<List<Comparable>> getTableTransInfo(long txnId) throws AnalysisException {
        List<List<Comparable>> tableInfos = new ArrayList<>();
        readLock();
        try {
            TransactionState transactionState = unprotectedGetTransactionState(txnId);
            if (null == transactionState) {
                throw new AnalysisException("Transaction[" + txnId + "] does not exist.");
            }

            Iterator<TableCommitInfo> idToTableCommitInfos;
            if (transactionState.getSubTxnIds() != null) {
                idToTableCommitInfos = transactionState.getSubTxnTableCommitInfos().iterator();
            } else {
                idToTableCommitInfos = transactionState.getIdToTableCommitInfos().values().iterator();
            }
            while (idToTableCommitInfos.hasNext()) {
                TableCommitInfo tableCommitInfo = idToTableCommitInfos.next();
                List<Comparable> tableInfo = new ArrayList<>();
                tableInfo.add(tableCommitInfo.getTableId());
                tableInfo.add(Joiner.on(", ").join(tableCommitInfo.getIdToPartitionCommitInfo().values().stream().map(
                        PartitionCommitInfo::getPartitionId).collect(Collectors.toList())));
                tableInfos.add(tableInfo);
            }
        } finally {
            readUnlock();
        }
        return tableInfos;
    }

    protected List<List<Comparable>> getPartitionTransInfo(long txnId, long tableId) throws AnalysisException {
        List<List<Comparable>> partitionInfos = new ArrayList<List<Comparable>>();
        readLock();
        try {
            TransactionState transactionState = unprotectedGetTransactionState(txnId);
            if (null == transactionState) {
                throw new AnalysisException("Transaction[" + txnId + "] does not exist.");
            }

            List<PartitionCommitInfo> partitionCommitInfos = new ArrayList<>();
            if (transactionState.getSubTxnIds() != null) {
                for (TableCommitInfo tableCommitInfo : transactionState.getSubTxnTableCommitInfos()) {
                    if (tableCommitInfo.getTableId() == tableId) {
                        partitionCommitInfos.addAll(tableCommitInfo.getIdToPartitionCommitInfo().values());
                    }
                }
            } else {
                partitionCommitInfos.addAll(
                        transactionState.getIdToTableCommitInfos().get(tableId).getIdToPartitionCommitInfo().values());
            }
            for (PartitionCommitInfo partitionCommitInfo : partitionCommitInfos) {
                List<Comparable> partitionInfo = new ArrayList<>();
                partitionInfo.add(partitionCommitInfo.getPartitionId());
                partitionInfo.add(partitionCommitInfo.getVersion());
                partitionInfos.add(partitionInfo);
            }
        } finally {
            readUnlock();
        }
        return partitionInfos;
    }

    public void removeUselessTxns(long currentMillis) {
        // delete expired txns
        BatchRemoveTransactionsOperationV2 op = null;
        EditLog.EditLogItem logItem = null;
        int numOfClearedTransaction = 0;
        writeLock();
        try {
            Pair<Long, Integer> expiredTxnsInfoForShort = unprotectedRemoveUselessTxns(currentMillis,
                    finalStatusTransactionStateDequeShort, MAX_REMOVE_TXN_PER_ROUND);
            Pair<Long, Integer> expiredTxnsInfoForLong = unprotectedRemoveUselessTxns(currentMillis,
                    finalStatusTransactionStateDequeLong,
                    MAX_REMOVE_TXN_PER_ROUND - expiredTxnsInfoForShort.second);
            numOfClearedTransaction = expiredTxnsInfoForShort.second + expiredTxnsInfoForLong.second;
            if (numOfClearedTransaction > 0) {
                op = new BatchRemoveTransactionsOperationV2(dbId,
                        expiredTxnsInfoForShort.first, expiredTxnsInfoForLong.first);
                if (Config.enable_txn_log_outside_lock) {
                    logItem = editLog.submitEdit(OperationType.OP_BATCH_REMOVE_TXNS_V2, op);
                } else {
                    editLog.logBatchRemoveTransactions(op);
                }
            }
        } finally {
            writeUnlock();
        }
        if (logItem != null) {
            logItem.await();
        }
        if (op != null && LOG.isDebugEnabled()) {
            LOG.debug("Remove {} expired transactions", numOfClearedTransaction);
        }
    }

    private Pair<Long, Integer> unprotectedRemoveUselessTxns(long currentMillis,
            ConcurrentLinkedDeque<TransactionState> finalStatusTransactionStateDeque, int left) {
        long latestTxnId = -1;
        int numOfClearedTransaction = 0;
        TransactionState transactionState;
        while ((transactionState = finalStatusTransactionStateDeque.peekFirst()) != null
                && numOfClearedTransaction < left) {
            if (transactionState.isExpired(currentMillis)) {
                finalStatusTransactionStateDeque.pollFirst();
                clearTransactionState(transactionState.getTransactionId());
                latestTxnId = transactionState.getTransactionId();
                numOfClearedTransaction++;
            } else {
                break;
            }
        }
        while ((Config.label_num_threshold > 0 && finalStatusTransactionStateDeque.size() > Config.label_num_threshold)
                && numOfClearedTransaction < left) {
            transactionState = finalStatusTransactionStateDeque.peekFirst();
            if (transactionState != null && transactionState.getFinishTime() != -1) {
                finalStatusTransactionStateDeque.pollFirst();
                clearTransactionState(transactionState.getTransactionId());
                latestTxnId = transactionState.getTransactionId();
                numOfClearedTransaction++;
            } else {
                break;
            }
        }
        return Pair.of(latestTxnId, numOfClearedTransaction);
    }

    private void clearTransactionState(long txnId) {
        TransactionState transactionState = idToFinalStatusTransactionState.remove(txnId);
        if (transactionState != null) {
            Set<Long> txnIds = unprotectedGetTxnIdsByLabel(transactionState.getLabel());
            txnIds.remove(transactionState.getTransactionId());
            if (txnIds.isEmpty()) {
                labelToTxnIds.remove(transactionState.getLabel());
            }
            cleanSubTransactions(txnId);
            if (LOG.isDebugEnabled()) {
                LOG.debug("transaction [" + txnId + "] is expired, remove it from transaction manager");
            }
        } else {
            // should not happen, add a warn log to observer
            LOG.warn("transaction state is not found when clear transaction: " + txnId);
        }
    }

    public int getTransactionNum() {
        return idToRunningTransactionState.size() + finalStatusTransactionStateDequeShort.size()
                + finalStatusTransactionStateDequeLong.size();
    }


    public TransactionState getTransactionStateByCallbackIdAndStatus(long callbackId, Set<TransactionStatus> status) {
        readLock();
        try {
            for (TransactionState txn : idToRunningTransactionState.values()) {
                if (txn.getCallbackId() == callbackId && status.contains(txn.getTransactionStatus())) {
                    return txn;
                }
            }
            for (TransactionState txn : idToFinalStatusTransactionState.values()) {
                if (txn.getCallbackId() == callbackId && status.contains(txn.getTransactionStatus())) {
                    return txn;
                }
            }
        } finally {
            readUnlock();
        }
        return null;
    }

    public TransactionState getTransactionStateByCallbackId(long callbackId) {
        readLock();
        try {
            for (TransactionState txn : idToRunningTransactionState.values()) {
                if (txn.getCallbackId() == callbackId) {
                    return txn;
                }
            }
            for (TransactionState txn : idToFinalStatusTransactionState.values()) {
                if (txn.getCallbackId() == callbackId) {
                    return txn;
                }
            }
        } finally {
            readUnlock();
        }
        return null;
    }

    public List<Pair<Long, Long>> getPrepareTransactionIdByCoordinateBe(long coordinateBeId,
            String coordinateHost, int limit) {
        ArrayList<Pair<Long, Long>> txnInfos = new ArrayList<>();
        readLock();
        try {
            idToRunningTransactionState.values().stream()
                    .filter(t -> (t.getCoordinator().sourceType == TransactionState.TxnSourceType.BE
                            && t.getTransactionStatus() == TransactionStatus.PREPARE
                            && t.getCoordinator().ip.equals(coordinateHost)
                            && (t.getCoordinator().id == 0 || t.getCoordinator().id == coordinateBeId)))
                    .limit(limit)
                    .forEach(t -> txnInfos.add(Pair.of(t.getDbId(), t.getTransactionId())));
        } finally {
            readUnlock();
        }
        return txnInfos;
    }

    // get show info of a specified txnId
    public List<List<String>> getSingleTranInfo(long dbId, long txnId) throws AnalysisException {
        List<List<String>> infos = new ArrayList<List<String>>();
        readLock();
        try {
            TransactionState txnState = unprotectedGetTransactionState(txnId);
            if (txnState == null) {
                throw new AnalysisException("transaction with id " + txnId + " does not exist");
            }
            TransactionUtil.checkAuth(dbId, txnState);
            infos.add(TransactionUtil.getTxnStateInfo(txnState, Lists.newArrayList()));
        } finally {
            readUnlock();
        }
        return infos;
    }

    protected void checkRunningTxnExceedLimit(List<Long> tableIdList)
            throws BeginTransactionException, MetaNotFoundException {
        long txnQuota = env.getInternalCatalog().getDbOrMetaException(dbId).getTransactionQuotaSize();

        if (runningTxnNums.get() >= txnQuota) {
            throw new BeginTransactionException("current running txns on db " + dbId + " is "
                    + runningTxnNums.get() + ", larger than limit " + txnQuota);
        }

        // Check if committed txn count on any table exceeds the configured limit
        if (Config.max_publishing_txn_num_per_table >= 0) {
            for (Long tableId : tableIdList) {
                long committedTxnCount = tableCommittedTxnCount.getOrDefault(tableId, 0L);
                if (committedTxnCount >= Config.max_publishing_txn_num_per_table) {
                    throw new BeginTransactionException("current committed txns on table " + tableId
                            + " is " + committedTxnCount + ", larger than limit "
                            + Config.max_publishing_txn_num_per_table);
                }
            }
        }
    }

    private void updateCatalogAfterCommitted(TransactionState transactionState, Database db, boolean isReplay) {
        if (transactionState.getSubTxnIds() != null) {
            List<TableCommitInfo> tableCommitInfos = transactionState.getSubTxnTableCommitInfos();
            updatePartitionNextVersion(transactionState, db, isReplay, tableCommitInfos);
        } else {
            updatePartitionNextVersion(transactionState, db, isReplay,
                    Lists.newArrayList(transactionState.getIdToTableCommitInfos().values()));
        }
        // update table stream offset if necessary
        if (!CollectionUtils.isEmpty(transactionState.getStreamUpdateInfos())) {
            updateStreamOffset(transactionState, transactionState.getCommitTime());
        }
    }

    private void updatePartitionNextVersion(TransactionState transactionState, Database db, boolean isReplay,
            List<TableCommitInfo> tableCommitInfos) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("txn_id={}, table commit info={}", transactionState.getTransactionId(), tableCommitInfos);
        }
        Set<Long> errorReplicaIds = transactionState.getErrorReplicas();
        List<Replica> tabletSuccReplicas = Lists.newArrayList();
        List<Replica> tabletFailedReplicas = Lists.newArrayList();
        // one replica should set last_failed_version once
        Set<Long> failedVersionSetReplicas = new HashSet<>();

        Map<Partition, Long> partitionToVersionMap = Maps.newHashMap();
        for (TableCommitInfo tableCommitInfo : tableCommitInfos) {
            long tableId = tableCommitInfo.getTableId();
            OlapTable table = (OlapTable) db.getTableNullable(tableId);
            if (table == null) {
                LOG.warn("table {} does not exist when update catalog after committed. transaction: {}, db: {}",
                        tableId, transactionState.getTransactionId(), db.getId());
                continue;
            }
            for (PartitionCommitInfo partitionCommitInfo : tableCommitInfo.getIdToPartitionCommitInfo().values()) {
                long partitionId = partitionCommitInfo.getPartitionId();
                Partition partition = table.getPartition(partitionId);
                if (partition == null) {
                    LOG.warn("partition {} of table {} does not exist when update catalog after committed."
                                    + " transaction: {}, db: {}",
                            partitionId, tableId, transactionState.getTransactionId(), db.getId());
                    continue;
                }
                List<MaterializedIndex> allIndices = partition.getMaterializedIndices(
                        MaterializedIndex.IndexExtState.ALL, true);
                for (MaterializedIndex index : allIndices) {
                    List<Tablet> tablets = index.getTablets();
                    for (Tablet tablet : tablets) {
                        tabletFailedReplicas.clear();
                        tabletSuccReplicas.clear();
                        for (Replica replica : tablet.getReplicas()) {
                            if (errorReplicaIds.contains(replica.getId())) {
                                if (!failedVersionSetReplicas.contains(replica.getId())) {
                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug("txn_id={}, set replica={}, last_failed_version={}",
                                                transactionState.getTransactionId(), replica.getId(),
                                                partitionCommitInfo.getVersion());
                                    }
                                    // TODO(cmy): do we need to update last failed version here?
                                    // because in updateCatalogAfterVisible, it will be updated again.
                                    replica.updateLastFailedVersion(partitionCommitInfo.getVersion());
                                    failedVersionSetReplicas.add(replica.getId());
                                }
                                tabletFailedReplicas.add(replica);
                            } else {
                                tabletSuccReplicas.add(replica);
                            }
                        }
                        if (!isReplay && !tabletFailedReplicas.isEmpty()) {
                            LOG.info("some replicas load data failed for committed txn {} on version {}, table {}, "
                                            + "partition {}, tablet {}, {} replicas load data succ: {}, "
                                            + "{} replicas load data fail: {}",
                                    transactionState.getTransactionId(), partitionCommitInfo.getVersion(),
                                    tableId, partitionId, tablet.getId(), tabletSuccReplicas.size(),
                                    Joiner.on(", ").join(tabletSuccReplicas.stream()
                                            .map(replica -> replica.toStringSimple(true))
                                            .collect(Collectors.toList())),
                                    tabletFailedReplicas.size(),
                                    Joiner.on(", ").join(tabletFailedReplicas.stream()
                                            .map(replica -> replica.toStringSimple(true))
                                            .collect(Collectors.toList())));
                        }
                    }
                }
                if (!partitionToVersionMap.containsKey(partition)
                        || partitionToVersionMap.get(partition) < partitionCommitInfo.getVersion()) {
                    partitionToVersionMap.put(partition, partitionCommitInfo.getVersion());
                }
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("txn_id={}, partition to version={}", transactionState.getTransactionId(),
                    partitionToVersionMap);
        }
        for (Entry<Partition, Long> entry : partitionToVersionMap.entrySet()) {
            Partition partition = entry.getKey();
            long version = entry.getValue();
            partition.setNextVersion(version + 1);
            LOG.debug("set partition={}, next_version={}", partition.getId(), version + 1);
        }
    }

    private boolean updateCatalogAfterVisible(TransactionState transactionState, Database db,
            Map<Long, Long> partitionVisibleVersions, Map<Long, Set<Long>> backendPartitions) {
        Set<Long> errorReplicaIds = transactionState.getErrorReplicas();
        AnalysisManager analysisManager = Env.getCurrentEnv().getAnalysisManager();
        List<Long> newPartitionLoadedTableIds = new ArrayList<>();
        // one replica should set last_failed_version once
        Set<Long> failedVersionSetReplicas = new HashSet<>();

        Collection<TableCommitInfo> tableCommitInfos;
        if (!transactionState.getSubTxnIdToTableCommitInfo().isEmpty()) {
            tableCommitInfos = transactionState.getSubTxnTableCommitInfos();
        } else {
            tableCommitInfos = transactionState.getIdToTableCommitInfos().values();
        }
        Map<Long, Triple<Long, Pair<Long, Long>, Partition>> partitionMap = new HashMap<>();
        Map<Long, Triple<Long, Long, OlapTable>> tableMap = new HashMap<>();
        for (TableCommitInfo tableCommitInfo : tableCommitInfos) {
            long tableId = tableCommitInfo.getTableId();
            OlapTable table = (OlapTable) db.getTableNullable(tableId);
            if (table == null) {
                LOG.warn("table {} does not exist when update catalog after visible. transaction: {}, db: {}",
                        tableId, transactionState.getTransactionId(), db.getId());
                continue;
            }
            transactionState.addTableIndexes(table);
            for (PartitionCommitInfo partitionCommitInfo : tableCommitInfo.getIdToPartitionCommitInfo().values()) {
                long partitionId = partitionCommitInfo.getPartitionId();
                long newCommitVersion = partitionCommitInfo.getVersion();
                Partition partition = table.getPartition(partitionId);
                if (partition == null) {
                    LOG.warn("partition {} in table {} does not exist when update catalog after visible."
                                    + " transaction: {}, db: {}",
                            partitionId, tableId, transactionState.getTransactionId(), db.getId());
                    continue;
                }
                List<MaterializedIndex> allIndices = partition
                        .getMaterializedIndices(MaterializedIndex.IndexExtState.ALL, true);
                for (MaterializedIndex index : allIndices) {
                    for (Tablet tablet : index.getTablets()) {
                        for (Replica replica : tablet.getReplicas()) {
                            long lastFailedVersion = replica.getLastFailedVersion();
                            long newVersion = replica.getVersion();
                            long lastSuccessVersion = replica.getLastSuccessVersion();
                            if (!errorReplicaIds.contains(replica.getId())) {
                                newVersion = newCommitVersion;
                                if (!replica.checkVersionCatchUp(partition.getVisibleVersion(), true)) {
                                    // this means the replica has error in the past, but we did not observe it
                                    // during upgrade, one job maybe in quorum finished state, for example,
                                    // A,B,C 3 replica A,B 's version is 10, C's version is 10 but C' 10 is abnormal
                                    // should be rollback then we will detect this and set C's last failed version to
                                    // 10 and last success version to 11 this logic has to be replayed
                                    // in checkpoint thread
                                    lastFailedVersion = partition.getVisibleVersion();
                                    newVersion = replica.getVersion();
                                }

                                // success version always move forward
                                lastSuccessVersion = newCommitVersion;
                            } else {
                                if (!failedVersionSetReplicas.contains(replica.getId())) {
                                    // for example, A,B,C 3 replicas, B,C failed during publish version,
                                    // then B C will be set abnormal all loading will failed, B,C will have to recovery
                                    // by clone, it is very inefficient and maybe lost data Using this method, B,C will
                                    // publish failed, and fe will publish again, not update their last failed version
                                    // if B is publish successfully in next turn, then B is normal and C will be set
                                    // abnormal so that quorum is maintained and loading will go on.
                                    newVersion = replica.getVersion();
                                    if (newCommitVersion > lastFailedVersion) {
                                        lastFailedVersion = newCommitVersion;
                                    }
                                    failedVersionSetReplicas.add(replica.getId());
                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug("txn_id={}, set replica={}, last_failed_version={}",
                                                transactionState.getTransactionId(), replica.getId(),
                                                partitionCommitInfo.getVersion());
                                    }
                                }
                            }
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("txn_id={}, set replica={}, version={}, last_failed_version={}, "
                                                + "last_success_version={}", transactionState.getTransactionId(),
                                        replica.getId(), newVersion, lastFailedVersion, lastSuccessVersion);
                            }
                            replica.updateVersionWithFailed(newVersion, lastFailedVersion, lastSuccessVersion);
                            if (newVersion == Partition.PARTITION_INIT_VERSION + 1) {
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("{}.{}, index {} first loaded, set reported to false.",
                                            db.getName(), table.getName(), index.getId());
                                }
                                index.setRowCountReported(false);
                            }
                            long beId = replica.getBackendIdWithoutException();
                            Set<Long> partitionIds = backendPartitions.get(beId);
                            if (partitionIds == null) {
                                partitionIds = Sets.newHashSet();
                                backendPartitions.put(beId, partitionIds);
                            }
                            partitionIds.add(partitionId);
                        }
                    }
                } // end for indices
                long version = partitionCommitInfo.getVersion();
                long versionTime = partitionCommitInfo.getVersionTime();
                long tso = partitionCommitInfo.getTso();
                if (partition.getVisibleVersion() == Partition.PARTITION_INIT_VERSION
                        && version > Partition.PARTITION_INIT_VERSION) {
                    newPartitionLoadedTableIds.add(tableId);
                }
                partitionMap.compute(partitionId, (k, v) -> {
                    if (v == null || version > v.getLeft()) {
                        return Triple.of(version, Pair.of(versionTime, tso), partition);
                    }
                    return v;
                });
            }

            tableMap.compute(tableId, (k, v) -> {
                if (v == null || tableCommitInfo.getVersion() > v.getLeft()) {
                    return Triple.of(tableCommitInfo.getVersion(), tableCommitInfo.getVersionTime(), table);
                }
                return v;
            });
        }
        for (Entry<Long, Triple<Long, Pair<Long, Long>, Partition>> entry : partitionMap.entrySet()) {
            long version = entry.getValue().getLeft();
            long versionTime = entry.getValue().getMiddle().first;
            long tso = entry.getValue().getMiddle().second;
            Partition partition = entry.getValue().getRight();
            partition.updateVisibleVersionAndTime(version, versionTime, tso);
            partitionVisibleVersions.put(partition.getId(), version);
            if (LOG.isDebugEnabled()) {
                LOG.debug("transaction state {} set partition {}'s visible version to [{}]",
                        transactionState, partition.getId(), version);
            }
        }
        for (Entry<Long, Triple<Long, Long, OlapTable>> entry : tableMap.entrySet()) {
            long version = entry.getValue().getLeft();
            long versionTime = entry.getValue().getMiddle();
            OlapTable table = entry.getValue().getRight();
            table.updateVisibleVersionAndTime(version, versionTime);
        }
        analysisManager.setNewPartitionLoaded(newPartitionLoadedTableIds);
        analysisManager.updateUpdatedRows(transactionState.getTableIdToTabletDeltaRows(),
                db.getId(), transactionState.getTransactionId());
        return true;
    }

    public List<TransactionState> getUnFinishedPreviousLoad(long endTransactionId, List<Long> tableIdList) {
        readLock();
        List<TransactionState> unFishedTxns = new ArrayList<>();
        try {
            for (Map.Entry<Long, TransactionState> entry : idToRunningTransactionState.entrySet()) {
                if (entry.getValue().getDbId() != dbId || !isIntersectionNotEmpty(entry.getValue().getTableIdList(),
                        tableIdList) || entry.getValue().getTransactionStatus().isFinalStatus()) {
                    continue;
                }
                if (entry.getKey() <= endTransactionId) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("find a running txn with txn_id={} on db: {}, less than watermark txn_id {}",
                                entry.getKey(), dbId, endTransactionId);
                    }
                    unFishedTxns.add(entry.getValue());
                }
            }
        } finally {
            readUnlock();
        }
        return unFishedTxns;
    }

    public boolean isPreviousTransactionsFinished(long endTransactionId, List<Long> tableIdList) {
        readLock();
        try {
            for (Map.Entry<Long, TransactionState> entry : idToRunningTransactionState.entrySet()) {
                if (entry.getValue().getDbId() != dbId || !isIntersectionNotEmpty(entry.getValue().getTableIdList(),
                        tableIdList) || entry.getValue().getTransactionStatus().isFinalStatus()) {
                    continue;
                }
                if (entry.getKey() <= endTransactionId) {
                    LOG.info("find a running txn with txn_id={} on db: {}, less than watermark txn_id {}",
                            entry.getKey(), dbId, endTransactionId);
                    return false;
                }
            }
        } finally {
            readUnlock();
        }
        return true;
    }

    public boolean isPreviousTransactionsFinished(long endTransactionId, long tableId, long partitionId) {
        readLock();
        try {
            for (Map.Entry<Long, TransactionState> entry : idToRunningTransactionState.entrySet()) {
                TransactionState transactionState = entry.getValue();
                if (entry.getKey() > endTransactionId
                        || transactionState.getTransactionStatus().isFinalStatus()
                        || transactionState.getDbId() != dbId
                        || !transactionState.getTableIdList().contains(tableId)) {
                    continue;
                }

                if (transactionState.getTransactionStatus() == TransactionStatus.COMMITTED) {
                    TableCommitInfo tableCommitInfo = transactionState.getTableCommitInfo(tableId);
                    // txn not contains this partition
                    if (tableCommitInfo != null
                            && tableCommitInfo.getIdToPartitionCommitInfo().get(partitionId) == null) {
                        continue;
                    }
                }

                return false;
            }
        } finally {
            readUnlock();
        }
        return true;
    }

    /**
     * check if there exists a intersection between the source tableId list and target tableId list
     * if one of them is null or empty, that means that we don't know related tables in tableList,
     * we think the two lists may have intersection for right ordered txns
     */
    public boolean isIntersectionNotEmpty(List<Long> sourceTableIdList, List<Long> targetTableIdList) {
        if (CollectionUtils.isEmpty(sourceTableIdList) || CollectionUtils.isEmpty(targetTableIdList)) {
            return true;
        }
        for (Long srcValue : sourceTableIdList) {
            for (Long targetValue : targetTableIdList) {
                if (srcValue.equals(targetValue)) {
                    return true;
                }
            }
        }
        return false;
    }


    public List<Long> getTimeoutTxns(long currentMillis) {
        List<Long> timeoutTxns = Lists.newArrayList();
        readLock();
        try {
            for (TransactionState transactionState : idToRunningTransactionState.values()) {
                if (transactionState.isTimeout(currentMillis)) {
                    // txn is running but timeout, abort it.
                    timeoutTxns.add(transactionState.getTransactionId());
                }
            }
        } finally {
            readUnlock();
        }
        return timeoutTxns;
    }

    public void removeExpiredAndTimeoutTxns(long currentMillis) {
        removeUselessTxns(currentMillis);
        List<Long> timeoutTxns = getTimeoutTxns(currentMillis);
        // abort timeout txns
        for (Long txnId : timeoutTxns) {
            try {
                abortTransaction(txnId, "timeout by txn manager", null);
                LOG.info("transaction [" + txnId + "] is timeout, abort it by transaction manager");
            } catch (UserException e) {
                // abort may be failed. it is acceptable. just print a log
                LOG.warn("abort timeout txn {} failed. msg: {}", txnId, e.getMessage());
            }
        }
    }

    public void replayUpsertTransactionState(TransactionState transactionState) throws MetaNotFoundException {
        boolean shouldAddTableListLock  = transactionState.getTransactionStatus() == TransactionStatus.COMMITTED
                || transactionState.getTransactionStatus() == TransactionStatus.VISIBLE;
        Database db = null;
        List<? extends TableIf> tableList = null;
        if (shouldAddTableListLock) {
            db = env.getInternalCatalog().getDbOrMetaException(transactionState.getDbId());
            tableList = db.getTablesOnIdOrderIfExist(transactionState.getTableIdList());
            tableList = buildLockTableListNoException(tableList, transactionState);
            tableList = MetaLockUtils.writeLockTablesIfExist(tableList);
        }
        writeLock();
        try {
            // set transaction status will call txn state change listener
            transactionState.replaySetTransactionStatus();
            if (transactionState.getTransactionStatus() == TransactionStatus.COMMITTED) {
                updateCatalogAfterCommitted(transactionState, db, true);
            } else if (transactionState.getTransactionStatus() == TransactionStatus.VISIBLE) {
                updateCatalogAfterVisible(transactionState, db, Maps.newHashMap(), Maps.newHashMap());
            }
            unprotectUpsertTransactionState(transactionState, true);
        } finally {
            writeUnlock();
            LOG.info("replay a {} transaction {}",
                     transactionState.getTransactionStatus(), transactionState);
            if (shouldAddTableListLock) {
                MetaLockUtils.writeUnlockTables(tableList);
            }
        }
    }

    public List<List<String>> getDbTransStateInfo() {
        List<List<String>> infos = Lists.newArrayList();
        readLock();
        try {
            infos.add(Lists.newArrayList("running", String.valueOf(
                    runningTxnNums.get())));
            long finishedNum = getFinishedTxnNums();
            infos.add(Lists.newArrayList("finished", String.valueOf(finishedNum)));
        } finally {
            readUnlock();
        }
        return infos;
    }

    public void unprotectWriteAllTransactionStates(DataOutput out) throws IOException {
        for (Map.Entry<Long, TransactionState> entry : idToRunningTransactionState.entrySet()) {
            entry.getValue().write(out);
        }

        // Use 2 queues instead of idToFinalStatusTransactionState to keep the order in queues.
        for (TransactionState transactionState : finalStatusTransactionStateDequeShort) {
            transactionState.write(out);
        }

        for (TransactionState transactionState : finalStatusTransactionStateDequeLong) {
            transactionState.write(out);
        }
    }

    protected void cleanLabel(String label, boolean isReplay) {
        Set<Long> removedTxnIds = Sets.newHashSet();
        writeLock();
        try {
            if (Strings.isNullOrEmpty(label)) {
                Iterator<Map.Entry<String, Set<Long>>> iter = labelToTxnIds.entrySet().iterator();
                while (iter.hasNext()) {
                    Set<Long> txnIds = iter.next().getValue();
                    Iterator<Long> innerIter = txnIds.iterator();
                    while (innerIter.hasNext()) {
                        long txnId = innerIter.next();
                        if (idToFinalStatusTransactionState.remove(txnId) != null) {
                            cleanSubTransactions(txnId);
                            innerIter.remove();
                            removedTxnIds.add(txnId);
                        }
                    }
                    if (txnIds.isEmpty()) {
                        iter.remove();
                    }
                }
            } else {
                Set<Long> txnIds = labelToTxnIds.get(label);
                if (txnIds == null) {
                    return;
                }
                Iterator<Long> iter = txnIds.iterator();
                while (iter.hasNext()) {
                    long txnId = iter.next();
                    if (idToFinalStatusTransactionState.remove(txnId) != null) {
                        cleanSubTransactions(txnId);
                        iter.remove();
                        removedTxnIds.add(txnId);
                    }
                }
                if (txnIds.isEmpty()) {
                    labelToTxnIds.remove(label);
                }
            }
            // remove from finalStatusTransactionStateDequeShort and finalStatusTransactionStateDequeLong
            // So that we can keep consistency in meta image
            finalStatusTransactionStateDequeShort.removeIf(txn -> removedTxnIds.contains(txn.getTransactionId()));
            finalStatusTransactionStateDequeLong.removeIf(txn -> removedTxnIds.contains(txn.getTransactionId()));

            if (!isReplay) {
                CleanLabelOperationLog log = new CleanLabelOperationLog(dbId, label);
                Env.getCurrentEnv().getEditLog().logCleanLabel(log);
            }
        } finally {
            writeUnlock();
        }
    }

    public long getTxnNumByStatus(TransactionStatus status) {
        readLock();
        try {
            if (idToRunningTransactionState.size() > 10000) {
                return idToRunningTransactionState.values().parallelStream()
                        .filter(t -> t.getTransactionStatus() == status).count();
            } else {
                return idToRunningTransactionState.values().stream().filter(t -> t.getTransactionStatus() == status)
                        .count();
            }
        } finally {
            readUnlock();
        }
    }

    /**
     * Check write lock holding time, if it exceeds threshold, print this hint log.
     *
     * @param lockStart holing lock start time.
     * @param lockEnd release lock time.
     */
    private void checkAndLogWriteLockDuration(long lockStart, long lockEnd) {
        long duration = lockEnd - lockStart;
        if (duration > lockReportingThresholdMs) {
            StringBuilder msgBuilder = new StringBuilder();
            msgBuilder.append("lock is held at ")
                    .append(lockStart)
                    .append(".And release after ")
                    .append(duration)
                    .append(" ms.")
                    .append("Call stack is :\n")
                    .append(getStackTrace(Thread.currentThread()));
            LOG.info(msgBuilder.toString());
        }
    }

    private static String getStackTrace(Thread t) {
        final StackTraceElement[] stackTrace = t.getStackTrace();
        StringBuilder msgBuilder = new StringBuilder();
        for (StackTraceElement e : stackTrace) {
            msgBuilder.append(e.toString()).append("\n");
        }
        return msgBuilder.toString();
    }

    /**
     * Update transaction table ids by transaction id.
     * it's used for multi table transaction.
     */
    protected void updateMultiTableRunningTransactionTableIds(long transactionId, List<Long> tableIds) {
        if (CollectionUtils.isEmpty(tableIds)) {
            return;
        }
        //idToRunningTransactionState.get(transactionId).
        if (null == idToRunningTransactionState.get(transactionId)) {
            return;
        }
        idToRunningTransactionState.get(transactionId).setTableIdList(tableIds);
    }

    private PublishResult finishCheckQuorumReplicas(TransactionState transactionState, Set<Long> errorReplicaIds) {
        Database database = Env.getCurrentInternalCatalog().getDbNullable(transactionState.getDbId());
        if (database == null) {
            return PublishResult.QUORUM_SUCC;
        }
        long now = System.currentTimeMillis();
        long firstPublishVersionTime = transactionState.getFirstPublishVersionTime();
        boolean allowPublishOneSucc = false;
        if (Config.publish_wait_time_second > 0 && firstPublishVersionTime > 0
                && now >= firstPublishVersionTime + Config.publish_wait_time_second * 1000L) {
            allowPublishOneSucc = true;
        }
        TabletsPublishResultLogs logs = new TabletsPublishResultLogs();

        Map<Long, List<PublishVersionTask>> publishTasks = transactionState.getPublishVersionTasks();
        PublishResult publishResult = PublishResult.QUORUM_SUCC;
        Map<Long, List<Long>> partitionToSubTxns = new HashMap<>();
        for (Long subTxnId : transactionState.getSubTxnIds()) {
            TableCommitInfo tableCommitInfo = transactionState.getTableCommitInfoBySubTxnId(subTxnId);
            if (tableCommitInfo == null) {
                continue;
            }
            Table tableIf = database.getTableNullable(tableCommitInfo.getTableId());
            if (tableIf == null) {
                continue;
            }
            OlapTable table = (OlapTable) tableIf;
            for (Entry<Long, PartitionCommitInfo> entry : tableCommitInfo.getIdToPartitionCommitInfo()
                    .entrySet()) {
                long partitionId = entry.getKey();
                Partition partition = table.getPartition(partitionId);
                if (partition == null) {
                    continue;
                }
                partitionToSubTxns.compute(partitionId, (k, v) -> {
                    if (v == null) {
                        v = Lists.newArrayList();
                    }
                    v.add(subTxnId);
                    return v;
                });
            }
        }
        for (Entry<Long, List<Long>> entry : partitionToSubTxns.entrySet()) {
            long partitionId = entry.getKey();
            List<Long> subTxnIds = entry.getValue();
            if (subTxnIds.isEmpty()) {
                continue;
            }
            TableCommitInfo tableCommitInfo = transactionState.getTableCommitInfoBySubTxnId(subTxnIds.get(0));
            long tableId = tableCommitInfo.getTableId();
            Table tableIf = database.getTableNullable(tableCommitInfo.getTableId());
            if (tableIf == null) {
                continue;
            }
            OlapTable table = (OlapTable) tableIf;
            Partition partition = table.getPartition(partitionId);
            if (partition == null) {
                continue;
            }
            long maxAlterWaterschedTxnId = getMaxAlterWaterschedTxnId(table);
            int loadRequiredReplicaNum = table.getLoadRequiredReplicaNum(partitionId);
            // TODO should use sub transaction load indexes
            List<MaterializedIndex> allIndices;
            if (transactionState.getLoadedTblIndexes().isEmpty()
                    || transactionState.getLoadedTblIndexes().get(tableId) == null) {
                allIndices = partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL, true);
            } else {
                allIndices = Lists.newArrayList();
                for (long indexId : transactionState.getLoadedTblIndexes().get(tableId)) {
                    MaterializedIndex index = partition.getIndex(indexId);
                    if (index != null) {
                        allIndices.add(index);
                    }
                }
            }
            long minSubTxnId = subTxnIds.get(0);
            long minVersion = transactionState.getTableCommitInfoBySubTxnId(minSubTxnId).getIdToPartitionCommitInfo()
                    .get(partitionId).getVersion();
            long maxSubTxnId = subTxnIds.get(subTxnIds.size() - 1);
            long maxVersion = transactionState.getTableCommitInfoBySubTxnId(maxSubTxnId).getIdToPartitionCommitInfo()
                    .get(partitionId).getVersion();
            LOG.debug("txn_id={}, partition={}, sub_txn_ids={}, min_version={}, max_version={}",
                    transactionState.getTransactionId(), partitionId, subTxnIds, minVersion, maxVersion);
            // check success replica number for each tablet.
            // a success replica means:
            //  1. Not in errorReplicaIds: succeed in both commit and publish phase
            //  2. last failed version < 0: is a health replica before
            //  3. version catch up: not with a stale version
            // Here we only check number, the replica version will be updated in updateCatalogAfterVisible()
            for (MaterializedIndex index : allIndices) {
                for (Tablet tablet : partition.getIndex(index.getId()).getTablets()) {
                    List<Replica> tabletSuccReplicas = Lists.newArrayList();
                    List<Replica> tabletWriteFailedReplicas = Lists.newArrayList();
                    List<Replica> tabletVersionFailedReplicas = Lists.newArrayList();
                    for (Replica replica : tablet.getReplicas()) {
                        List<PublishVersionTask> publishVersionTasks
                                = publishTasks.get(replica.getBackendIdWithoutException());
                        List<PublishVersionTask> replicaTasks = new ArrayList<>();
                        for (Long subTransactionId : subTxnIds) {
                            PublishVersionTask publishVersionTask = null;
                            if (publishVersionTasks != null) {
                                List<PublishVersionTask> matchedTasks = publishVersionTasks.stream()
                                        .filter(t -> t != null && t.getTransactionId() == subTransactionId
                                                && t.getPartitionVersionInfos().stream()
                                                .anyMatch(s -> s.getPartitionId() == partitionId))
                                        .collect(Collectors.toList());
                                Preconditions.checkState(matchedTasks.size() <= 1,
                                        "matched publish tasks: " + matchedTasks);
                                if (matchedTasks.size() == 1) {
                                    publishVersionTask = matchedTasks.get(0);
                                }
                            }
                            replicaTasks.add(publishVersionTask);
                        }
                        checkReplicaContinuousVersionSucc(subTxnIds, maxAlterWaterschedTxnId, tablet.getId(), replica,
                                minVersion, maxVersion, replicaTasks, errorReplicaIds, tabletSuccReplicas,
                                tabletWriteFailedReplicas, tabletVersionFailedReplicas);
                        LOG.debug("after checkReplicaContinuousVersion for txn_id={}, sub_txn_ids={}, "
                                        + "partition={}, tablet_id={}, replica={}, min_version={}, max_version={}, "
                                        + "success_replicas={}, error_replicas={}, write_failed_replicas={}, "
                                        + "version_failed_replicas={}", transactionState.getTransactionId(), subTxnIds,
                                partition.getId(), tablet.getId(), replica.getId(), minVersion, maxVersion,
                                tabletSuccReplicas, errorReplicaIds, tabletWriteFailedReplicas,
                                tabletVersionFailedReplicas);
                    }

                    publishResult = checkQuorumReplicas(transactionState, tableId, partition, tablet,
                            loadRequiredReplicaNum, allowPublishOneSucc, maxVersion, tabletSuccReplicas,
                            tabletWriteFailedReplicas, tabletVersionFailedReplicas, publishResult, logs);
                    if (publishResult == PublishResult.QUORUM_SUCC) {
                        tablet.setLastLoadFailedTime(-1L);
                    }
                }
            }
        }

        boolean needLog = publishResult != PublishResult.FAILED
                || now - transactionState.getLastPublishLogTime() > Config.publish_fail_log_interval_second * 1000L
                || LOG.isDebugEnabled();
        if (needLog) {
            transactionState.setLastPublishLogTime(now);
            logs.log();
        }

        return publishResult;
    }

    private class TabletsPublishResultLogs {
        public List<String> quorumSuccLogs = Lists.newArrayList();
        public List<String> timeoutSuccLogs = Lists.newArrayList();
        public List<String> failedLogs = Lists.newArrayList();

        public void addQuorumSuccLog(String log) {
            if (quorumSuccLogs.size() < 16) {
                quorumSuccLogs.add(log);
            }
        }

        public void addTimeoutSuccLog(String log) {
            if (timeoutSuccLogs.size() < 16) {
                timeoutSuccLogs.add(log);
            }
        }

        public void addFailedLog(String log) {
            if (failedLogs.size() < 16) {
                failedLogs.add(log);
            }
        }

        public void log() {
            // log failed logs
            for (String log : failedLogs) {
                LOG.info(log);
            }
            // log timeout succ logs
            for (String log : timeoutSuccLogs) {
                LOG.info(log);
            }
            // log quorum succ logs
            for (String log : quorumSuccLogs) {
                LOG.info(log);
            }
        }
    }

    private PublishResult checkQuorumReplicas(TransactionState transactionState, long tableId, Partition partition,
            Tablet tablet, int loadRequiredReplicaNum, boolean allowPublishOneSucc, long newVersion,
            List<Replica> tabletSuccReplicas, List<Replica> tabletWriteFailedReplicas,
            List<Replica> tabletVersionFailedReplicas, PublishResult publishResult, TabletsPublishResultLogs logs) {
        long partitionId = partition.getId();
        int healthReplicaNum = tabletSuccReplicas.size();
        if (healthReplicaNum >= loadRequiredReplicaNum) {
            boolean hasFailedReplica = !tabletWriteFailedReplicas.isEmpty() || !tabletVersionFailedReplicas.isEmpty();
            if (hasFailedReplica) {
                String writeDetail = getTabletWriteDetail(tabletSuccReplicas, tabletWriteFailedReplicas,
                        tabletVersionFailedReplicas);
                logs.addQuorumSuccLog(String.format("publish version quorum succ for transaction %s on tablet %s"
                                + " with version %s, and has failed replicas, load require replica num %s. "
                                + "table %s, partition: [ id=%s, commit version=%s ], tablet detail: %s",
                                transactionState, tablet.getId(), newVersion, loadRequiredReplicaNum, tableId,
                                partitionId, partition.getCommittedVersion(), writeDetail));
            }
            return publishResult;
        }

        String writeDetail = getTabletWriteDetail(tabletSuccReplicas, tabletWriteFailedReplicas,
                tabletVersionFailedReplicas);
        if (allowPublishOneSucc && healthReplicaNum > 0) {
            if (publishResult == PublishResult.QUORUM_SUCC) {
                publishResult = PublishResult.TIMEOUT_SUCC;
            }
            // We can not do any thing except retrying,
            // because publish task is assigned a version,
            // and doris does not permit discontinuous
            // versions.
            //
            // If a timeout happens, it means that the rowset
            // that are being publised exists on a few replicas we should go
            // ahead, otherwise data may be lost and thre
            // publish task hangs forever.
            logs.addTimeoutSuccLog(String.format("publish version timeout succ for transaction %s on tablet %s "
                            + "with version %s, and has failed replicas, load require replica num %s. "
                            + "table %s, partition %s, tablet detail: %s", transactionState, tablet.getId(), newVersion,
                    loadRequiredReplicaNum, tableId, partitionId, writeDetail));
        } else {
            publishResult = PublishResult.FAILED;
            String errMsg = String.format(
                    "publish on tablet %d failed." + " succeed replica num %d < load required replica num %d."
                            + " table: %d, partition: %d, publish version: %d", tablet.getId(), healthReplicaNum,
                    loadRequiredReplicaNum, tableId, partitionId, newVersion);
            transactionState.setErrorMsg(errMsg);
            logs.addFailedLog(String.format("publish version failed for transaction %s on tablet %s with version"
                            + " %s, and has failed replicas, load required replica num %s. table %s, "
                            + "partition %s, tablet detail: %s", transactionState, tablet.getId(), newVersion,
                    loadRequiredReplicaNum, tableId, partitionId, writeDetail));
        }
        return publishResult;
    }

    protected void addSubTransaction(long transactionId, long subTransactionId) {
        subTxnIdToTxnId.put(subTransactionId, transactionId);
    }

    protected void removeSubTransaction(long subTransactionId) {
        subTxnIdToTxnId.remove(subTransactionId);
    }

    private void cleanSubTransactions(long transactionId) {
        Iterator<Entry<Long, Long>> iterator = subTxnIdToTxnId.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<Long, Long> entry = iterator.next();
            if (entry.getValue() == transactionId) {
                iterator.remove();
            }
        }
    }

    private List<? extends TableIf> buildLockTableListNoException(List<? extends TableIf> tableList,
                                                                 TransactionState transactionState) {
        if (transactionState == null || CollectionUtils.isEmpty(transactionState.getStreamUpdateInfos())) {
            return tableList;
        }
        TreeSet<Pair<Long, Long>> tableIfs = new TreeSet<>(new Pair.PairComparator<>());
        tableList.forEach(table -> tableIfs.add(Pair.of(table.getDatabase().getId(), table.getId())));
        transactionState.getStreamUpdateInfos()
                .forEach(info -> tableIfs.add(Pair.of(info.getDbId(), info.getStreamId())));
        List<Table> newTableList = new ArrayList<>(tableIfs.size());
        for (Pair<Long, Long> p : tableIfs) {
            Database db = env.getInternalCatalog().getDbNullable(p.first);
            if (db == null) {
                continue;
            }
            Table table = db.getTableNullable(p.second);
            if (table == null) {
                continue;
            }
            newTableList.add(table);
        }
        return newTableList;
    }

    private void checkStreamOffset(TransactionState transactionState) throws UserException {
        if (CollectionUtils.isEmpty(transactionState.getStreamUpdateInfos())) {
            return;
        }
        for (TableStreamUpdateInfo info : transactionState.getStreamUpdateInfos()) {
            Database db = env.getInternalCatalog().getDbOrMetaException(info.getDbId());
            TableIf tableIf = db.getTableOrMetaException(info.getStreamId());
            ((BaseTableStream) tableIf).unprotectedCheckStreamUpdate(info.getUpdate());
        }
    }

    private void updateStreamOffset(TransactionState transactionState, Long ts) {
        for (TableStreamUpdateInfo info : transactionState.getStreamUpdateInfos()) {
            Database db = env.getInternalCatalog().getDbNullable(info.getDbId());
            if (db == null) {
                continue;
            }
            TableIf tableIf = db.getTableNullable(info.getStreamId());
            if (tableIf == null) {
                continue;
            }
            ((BaseTableStream) tableIf).unprotectedUpdateStreamUpdate(info.getUpdate(), ts);
        }
    }
}
