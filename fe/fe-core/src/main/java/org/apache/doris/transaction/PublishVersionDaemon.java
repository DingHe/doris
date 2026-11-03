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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.Config;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.lock.MonitoredReentrantReadWriteLock;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.PublishVersionTask;
import org.apache.doris.task.UpdateVisibleVersionTask;
import org.apache.doris.thrift.TPartitionVersionInfo;
import org.apache.doris.thrift.TTaskType;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

// 核心的后台守护线程类
// 整个分布式事务从“提交（Committed）”阶段迈向“可见（Visible）”阶段的桥梁。
// 当事务完成数据写入并预提交/提交后，数据并不会立即对用户可见。PublishVersionDaemon 负责周期性地扫描这些准备发布的事务，并发地向所有涉及的 BE（Backend）节点下发版本发布任务（PublishVersionTask），并在所有 BE 节点成功生效后，推进事务状态并更新分区的可见版本。
// 周期性版本发布调度：按照固定的时间间隔（由 Config.publish_version_interval_ms 控制），自动扫描全局所有准备发布的事务（Ready-to-Publish Transactions）。
// 多线程并发任务分发：构建 PublishVersionTask，利用多线程线程池（publishExecutors）或 Agent 任务分发框架，将版本发布指令下发给各 BE 节点。
// 版本收尾与状态闭环：检查各 BE 节点的发布任务执行情况，当满足多数派或全部成功时，调用全局事务管理器完成事务收尾（finishTransaction），使数据正式对用户可见。
// 后端可见版本同步：收集并维护分区可见版本，异步向 BE 节点同步更新可见版本（UpdateVisibleVersionTask）。
public class PublishVersionDaemon extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(PublishVersionDaemon.class);
    // 根据配置项 Config.publish_thread_pool_num 初始化多个独立线程池，用于并行执行异步版本发布任务。
    private static ArrayList<ExecutorService> publishExecutors = new ArrayList(Config.publish_thread_pool_num);
    // 用于记录当前正在异步发布处理中的事务 ID，防止同一个事务被重复并发调度。
    private Set<Long> publishingTxnIds = Sets.newConcurrentHashSet();
    // 监控型可重入读写锁，用于保护 partitionVisibleVersions 和 backendPartitions 的并发读写安全。
    private final MonitoredReentrantReadWriteLock visibleVersionsLock = new MonitoredReentrantReadWriteLock(true);
    // 分区 ID 到最新可见版本号（Visible Version）的映射缓存。
    private Map<Long, Long> partitionVisibleVersions = Maps.newHashMap();
    // 后端 BE 节点 ID 到其托管的分区 ID 集合的映射，用于确定哪些 BE 需要同步更新可见版本。
    private Map<Long, Set<Long>> backendPartitions = Maps.newHashMap();

    public PublishVersionDaemon() {
        super("PUBLISH_VERSION", Config.publish_version_interval_ms);
        for (int i = 0; i < Config.publish_thread_pool_num; i++) {
            publishExecutors.add(ThreadPoolManager.newDaemonFixedThreadPool(1, Config.publish_queue_size,
                    "PUBLISH_VERSION_EXEC-" + i, true));
        }
    }
    // 只有当当前 FE 节点的 Catalog 准备就绪且处于 Master（主节点） 状态时，才会周期性触发执行
    @Override
    protected void runAfterCatalogReady() {
        try {
            publishVersion();
            sendBackendVisibleVersion();
        } catch (Throwable t) {
            LOG.error("errors while publish version to all backends", t);
        }
    }
    // 版本发布的总控入口。
    private void publishVersion() {
        if (DebugPointUtil.isEnable("PublishVersionDaemon.stop_publish")) {
            return;
        }
        // 从全局事务管理器获取所有准备发布的事务列表（readyTransactionStates），若为空则返回。
        GlobalTransactionMgrIface globalTransactionMgr = Env.getCurrentGlobalTransactionMgr();
        List<TransactionState> readyTransactionStates = globalTransactionMgr.getReadyToPublishTransactions();
        if (readyTransactionStates.isEmpty()) {
            return;
        }

        // ATTN, we publish transaction state to all backends including dead backend, if not publish to dead backend
        // then transaction manager will treat it as success
        // 获取集群所有在线和离线的 BE 节点列表
        SystemInfoService infoService = Env.getCurrentSystemInfo();
        List<Long> allBackends = infoService.getAllBackendIds(false);
        if (allBackends.isEmpty()) {
            LOG.warn("some transaction state need to publish, but no backend exists");
            return;
        }
        // 遍历并下发发布任务。
        traverseReadyTxnAndDispatchPublishVersionTask(readyTransactionStates, allBackends);
        // 检查并尝试完成已满足条件的事务
        tryFinishTxn(readyTransactionStates, infoService, globalTransactionMgr);
    }
    // 遍历所有准备好发布的事务（Ready-to-Publish Transactions），
    // 为尚未下发过任务的事务生成具体的版本发布任务（PublishVersionTask），
    // 并将这些任务批量提交给 Agent 任务执行器下发给后端 BE 节点。
    private void traverseReadyTxnAndDispatchPublishVersionTask(List<TransactionState> readyTransactionStates,
                                                               List<Long> allBackends) {
        // 1. 记录任务创建时间戳
        // 获取当前系统的毫秒级时间戳，作为这批版本发布任务的统一创建时间，用于后续计算耗时、超时判断或任务追踪。
        long createPublishVersionTaskTime = System.currentTimeMillis();
        // every backend-transaction identified a single task
        // 2. 初始化 Agent 批处理任务容器
        // 创建一个 AgentBatchTask 实例。在 Doris 中，“每一个 Backend 对应的每一个事务”都会被识别为一个独立的 Agent 任务，
        // AgentBatchTask 用于将这些零散的任务打包成批次，以便高效地通过 Agent 框架下发给多个 BE 节点。
        AgentBatchTask batchTask = new AgentBatchTask();
        // for delta rows statistics to exclude rollup tablets
        // 3. 初始化 BE 到基础分片 ID 的映射缓存
        // 创建一个 Map<Long, Set<Long>>（BE ID -> 基础 Tablet ID 集合），用于在统计增量数据行数时排除 Rollup 物化视图分片，
        // 避免重复计算行数指标。
        Map<Long, Set<Long>> beIdToBaseTabletIds = Maps.newHashMap();
        // traverse all ready transactions and dispatch the publish version task to all backends
        for (TransactionState transactionState : readyTransactionStates) {
            // 5. 幂等与防重复下发检查
            // 调用 transactionState.hasSendTask() 判断该事务是否已经下发过发布任务。
            // 如果已经下发过了（避免每一轮守护线程循环重复创建相同的任务），则通过 continue 跳过，防止任务堆积或重复发送。
            if (transactionState.hasSendTask()) {
                continue;
            }
            // 6. 异常保护与任务生成下发
            // 使用 try-catch 块包裹，调用 genPublishTask(...) 方法为当前的 transactionState 生成具体的发布任务，
            // 并将结果填充到 batchTask 和 beIdToBaseTabletIds 中。
            // 即使某一个事务在生成任务时发生异常，也不会中断整个守护线程对其他事务的处理。
            try {
                genPublishTask(allBackends, transactionState, createPublishVersionTaskTime, beIdToBaseTabletIds,
                        batchTask);
            } catch (Throwable t) {
                LOG.error("errors while generate publish task for transaction: {}", transactionState, t);
            }
        }
        // 7. 批量提交 Agent 任务
        // 检查 batchTask 中是否成功收集到了需要下发的任务（即任务列表不为空）。
        // 如果不为空，调用 AgentTaskExecutor.submit(batchTask)，将整批发布任务统一提交给系统的 Agent 线程池，
        // 通过 RPC 异步下发给各个后端 BE 节点执行。
        if (!batchTask.getAllTasks().isEmpty()) {
            AgentTaskExecutor.submit(batchTask);
        }
    }
    // 针对某一个具体的已提交事务，计算出哪些后端 BE 节点需要接收版本发布任务，并根据事务是否包含子事务（SubTxn），
    // 将对应的分区版本信息（TPartitionVersionInfo）打包并组装成具体的 PublishVersionTask 任务，最终放入批量下发队列中。
    private void genPublishTask(List<Long> allBackends, TransactionState transactionState,
            long createPublishVersionTaskTime, Map<Long, Set<Long>> beIdToBaseTabletIds, AgentBatchTask batchTask) {
        Set<Long> publishBackends = Sets.newHashSet(transactionState.getPublishVersionTasks().keySet());
        publishBackends.addAll(transactionState.getInvolvedBackends());
        // public version tasks are not persisted in catalog, so publishBackends may be empty.
        // so we have to try publish to all backends;
        if (publishBackends.isEmpty()) {
            // could not just add to it, should new a new object, or the back map will destroyed
            publishBackends = Sets.newHashSet();
            publishBackends.addAll(allBackends);
        }
        if (transactionState.getTransactionId() == DebugPointUtil.getDebugParamOrDefault(
                "PublishVersionDaemon.genPublishTask.failed", "txnId", -1L)) {
            throw new NullPointerException("genPublishTask failed for txnId: " + transactionState.getTransactionId());
        }

        if (transactionState.getSubTxnIds() != null) {
            for (Entry<Long, TableCommitInfo> entry : transactionState.getSubTxnIdToTableCommitInfo().entrySet()) {
                long subTxnId = entry.getKey();
                List<TPartitionVersionInfo> partitionVersionInfos = generatePartitionVersionInfos(entry.getValue(),
                        transactionState, beIdToBaseTabletIds);
                LOG.debug("add publish task, txnId={}, subTxnId={}, backends={}, partitionVersionInfos={}",
                        transactionState.getTransactionId(), subTxnId, publishBackends, partitionVersionInfos);
                addPublishVersionTask(publishBackends, subTxnId, transactionState, partitionVersionInfos,
                        beIdToBaseTabletIds, createPublishVersionTaskTime, batchTask);
            }
        } else {
            List<TPartitionVersionInfo> partitionVersionInfos = generatePartitionVersionInfos(
                    transactionState.getIdToTableCommitInfos().values(), transactionState, beIdToBaseTabletIds);
            addPublishVersionTask(publishBackends, transactionState.getTransactionId(), transactionState,
                    partitionVersionInfos, beIdToBaseTabletIds, createPublishVersionTaskTime, batchTask);
        }
        transactionState.setSendedTask();
        LOG.info("send publish tasks for transaction: {}, db: {}", transactionState.getTransactionId(),
                transactionState.getDbId());
    }

    private void tryFinishTxn(List<TransactionState> readyTransactionStates,
            SystemInfoService infoService, GlobalTransactionMgrIface globalTransactionMgr) {
        for (TransactionState transactionState : readyTransactionStates) {
            try {
                // try to finish the transaction, if failed just retry in next loop
                tryFinishOneTxn(transactionState, infoService, globalTransactionMgr);
            } catch (Throwable t) {
                LOG.error("errors while finish transaction: {}, publish tasks: {}", transactionState,
                        transactionState.getPublishVersionTasks(), t);
            }
        } // end for readyTransactionStates
    }
    // 检查某个已提交事务在各个后端（BE）节点上的版本发布任务执行进度、聚合统计增量行数、处理节点故障或发布缓慢等异常情况，并最终决定是否触发事务的收尾闭环（将事务状态推进为 VISIBLE）。
    private void tryFinishOneTxn(TransactionState transactionState, SystemInfoService infoService,
            GlobalTransactionMgrIface globalTransactionMgr) {
        // 1. 初始化增量行数统计映射
        // 创建一个嵌套的 Map（tableId -> (tabletId -> deltaRows)），用于记录本次事务中各个表及分片成功写入的数据行数。
        Map<Long, Map<Long, Long>> tableIdToTabletDeltaRows = Maps.newHashMap();
        // 2. 初始化存活且有未完成任务的原子布尔标记
        // 使用 AtomicBoolean 标记是否存在“某个后端节点处于存活状态，且其版本发布任务尚未完成”的情况。
        AtomicBoolean hasBackendAliveAndUnfinishedTask = new AtomicBoolean(false);
        // 3. 初始化未完成任务的 BE 集合
        // 记录所有尚未完成版本发布任务的后端 BE ID 列表，用于后续的超时和降级判断。
        Set<Long> notFinishTaskBe = Sets.newHashSet();
        // 4. 遍历当前事务所有的发布任务
        // transactionState.getPublishVersionTasks() 获取该事务下发给各个 BE 的所有发布任务映射（Map<BackendId, List<PublishVersionTask>>）
        transactionState.getPublishVersionTasks().forEach((key, tasks) -> {
            long beId = key;
            for (PublishVersionTask task : tasks) {
                if (task.isFinished()) {
                    // 如果任务已经成功执行完毕，调用 calculateTaskUpdateRows 聚合该任务统计的增量行数。
                    calculateTaskUpdateRows(tableIdToTabletDeltaRows, task);
                } else {
                    if (infoService.checkBackendAlive(task.getBackendId())) {
                        hasBackendAliveAndUnfinishedTask.set(true);
                    }
                    notFinishTaskBe.add(beId);
                }
            }
        });

        transactionState.setTableIdToTabletDeltaRows(tableIdToTabletDeltaRows);
        if (LOG.isDebugEnabled()) {
            LOG.debug("notFinishTaskBe {}, trans {}", notFinishTaskBe, transactionState);
        }
        boolean isPublishSlow = false;
        // 获取当前事务总共涉及的发布任务对应的 BE 节点总数。
        long totalNum = transactionState.getPublishVersionTasks().keySet().size();
        // 8. 检查未完成任务的 BE 是否都处于“队列拥堵/缓慢”状态
        boolean allUnFinishTaskIsSlow = notFinishTaskBe.stream().allMatch(beId -> {
            Backend be = infoService.getBackend(beId);
            if (be == null) {
                return false;
            }
            // 判断标准：未完成任务的 BE，其待处理发布任务堆积数（getPublishTaskLastTimeAccumulated()）是否超过了配置的阈值。
            return be.getPublishTaskLastTimeAccumulated() > Config.publish_version_queued_limit_number;
        });
        // 9. 判定是否整体发布缓慢（Slow Publishing）
        // 如果“已完成的节点数超过总数的一半” 并且 “所有未完成的节点全部处于队列拥堵的缓慢状态”，
        // 则判定当前事务发布变慢，开启容灾降级开关。
        if (totalNum - notFinishTaskBe.size() > totalNum / 2 && allUnFinishTaskIsSlow) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(" finishNum {}, txn publish tasks {}, notFinishTaskBe {}",
                        totalNum - notFinishTaskBe.size(), transactionState.getPublishVersionTasks().keySet(),
                        notFinishTaskBe);
            }
            isPublishSlow = true;
        }
        // 10. 核心决策：是否应该结束（收尾）该事务
        // 满足以下任意一个条件即认为可以结束事务：
        // (1) !hasBackendAliveAndUnfinishedTask.get()：没有存活且未完成任务的节点（即活着的节点都干完了，死的节点不用等）。
        // (2) transactionState.isPublishTimeout()：事务发布超时。
        // (3) isPublishSlow：发布过程被判定为整体缓慢。
        // (4) DebugPointUtil.isEnable(...)：通过动态 Debug 注入了跳过未完成任务的指令。
        boolean shouldFinishTxn = !hasBackendAliveAndUnfinishedTask.get() || transactionState.isPublishTimeout()
                || isPublishSlow
                || DebugPointUtil.isEnable("PublishVersionDaemon.not_wait_unfinished_tasks");
        // 11. 执行事务收尾动作
        if (shouldFinishTxn) {
            // 根据系统配置决定是走异步收尾还是同步收尾
            if (Config.enable_parallel_publish_version) {
                tryFinishTxnAsync(transactionState, globalTransactionMgr);
            } else {
                tryFinishTxnSync(transactionState, globalTransactionMgr);
            }
        }
    }

    private void tryFinishTxnAsync(TransactionState transactionState, GlobalTransactionMgrIface globalTransactionMgr) {
        if (publishingTxnIds.contains(transactionState.getTransactionId())) {
            return;
        }

        publishingTxnIds.add(transactionState.getTransactionId());
        LOG.info("try to finish transaction {}, dbId: {}, txnId: {}",
                transactionState.getTransactionId(), transactionState.getDbId(), transactionState.getTransactionId());
        try {
            // When enable_per_txn_publish is true, route by transactionId so different
            // transactions in the same database can finish in parallel across executor threads.
            // When false, route by dbId (old behavior) so transactions within a DB are sequential.
            long routingKey = Config.enable_per_txn_publish
                    ? transactionState.getTransactionId()
                    : transactionState.getDbId();
            publishExecutors.get((int) (routingKey % Config.publish_thread_pool_num)).execute(() -> {
                try {
                    tryFinishTxnSync(transactionState, globalTransactionMgr);
                } catch (Throwable e) {
                    LOG.warn("failed to finish dbId: {}, txnId: {}", transactionState.getDbId(),
                            transactionState.getTransactionId(), e);
                } finally {
                    publishingTxnIds.remove(transactionState.getTransactionId());
                }
            });
        } catch (Throwable e) {
            LOG.warn("failed to finish transaction {}, dbId: {}, txnId: {}, exception: {}",
                    transactionState.getTransactionId(), transactionState.getDbId(),
                    transactionState.getTransactionId(), e);
            publishingTxnIds.remove(transactionState.getTransactionId());
        }
    }

    private void tryFinishTxnSync(TransactionState transactionState, GlobalTransactionMgrIface globalTransactionMgr) {
        if (DebugPointUtil.isEnable("PublishVersionDaemon.tryFinishTxnSync.fail")) {
            throw new RuntimeException("finishTransaction failed for txnId: " + transactionState.getTransactionId());
        }

        try {
            Map<Long, Long> txnPartitionVersions = Maps.newHashMap();
            Map<Long, Set<Long>> txnBackendPartitions = Maps.newHashMap();
            // one transaction exception should not affect other transaction
            globalTransactionMgr.finishTransaction(transactionState.getDbId(),
                    transactionState.getTransactionId(), txnPartitionVersions, txnBackendPartitions);
            addBackendVisibleVersions(txnPartitionVersions, txnBackendPartitions);
        } catch (Exception e) {
            LOG.warn("error happens when finish transaction {}", transactionState.getTransactionId(), e);
        }
        if (transactionState.getTransactionStatus() != TransactionStatus.VISIBLE) {
            // if finish transaction state failed, then update publish version time, should check
            // to finish after some interval
            transactionState.updateSendTaskTime();
            if (LOG.isDebugEnabled()) {
                LOG.debug("publish version for transaction {} failed", transactionState);
            }
        }

        if (transactionState.getTransactionStatus() == TransactionStatus.VISIBLE) {
            transactionState.getPublishVersionTasks().values().forEach(tasks -> {
                for (PublishVersionTask task : tasks) {
                    AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.PUBLISH_VERSION,
                            task.getSignature());
                }
            });
            transactionState.pruneAfterVisible();
            if (MetricRepo.isInit) {
                long publishTime = transactionState.getLastPublishVersionTime()
                        - transactionState.getCommitTime();
                MetricRepo.HISTO_TXN_PUBLISH_LATENCY.update(publishTime);
            }
        }
    }

    private void addBackendVisibleVersions(Map<Long, Long> partitionVisibleVersions,
            Map<Long, Set<Long>> backendPartitions) {
        visibleVersionsLock.writeLock().lock();
        try {
            this.partitionVisibleVersions.putAll(partitionVisibleVersions);
            // merge backend partitions if exists merge value as set else add a new set
            for (Entry<Long, Set<Long>> entry : backendPartitions.entrySet()) {
                this.backendPartitions.computeIfPresent(entry.getKey(),
                        (backendId, existingPartitions) -> {
                            existingPartitions.addAll(entry.getValue());
                            return existingPartitions;
                        });
                this.backendPartitions.putIfAbsent(entry.getKey(), entry.getValue());
            }
        } finally {
            visibleVersionsLock.writeLock().unlock();
        }
    }

    // Merge task tablets update rows to tableToTabletsDelta.
    private void calculateTaskUpdateRows(Map<Long, Map<Long, Long>> tableIdToTabletDeltaRows, PublishVersionTask task) {
        if (CollectionUtils.isEmpty(task.getErrorTablets())) {
            LOG.debug("Task backend id {}, update rows info : [{}]",
                    task.getBackendId(), task.getTableIdToTabletDeltaRows());
            for (Entry<Long, Map<Long, Long>> tableEntry : task.getTableIdToTabletDeltaRows().entrySet()) {
                tableIdToTabletDeltaRows.putIfAbsent(tableEntry.getKey(), Maps.newHashMap());
                Map<Long, Long> tabletsDelta = tableIdToTabletDeltaRows.get(tableEntry.getKey());
                for (Entry<Long, Long> tabletEntry : tableEntry.getValue().entrySet()) {
                    tabletsDelta.computeIfPresent(tabletEntry.getKey(),
                            (tabletId, origRows) -> origRows + tabletEntry.getValue());
                    tabletsDelta.putIfAbsent(tabletEntry.getKey(), tabletEntry.getValue());
                }
            }
        }
    }

    private Map<Long, Set<Long>> getBaseTabletIdsForEachBe(TransactionState transactionState,
            TableCommitInfo tableCommitInfo) throws MetaNotFoundException {

        OlapTable table = (OlapTable) Env.getCurrentEnv()
                .getInternalCatalog()
                .getDb(transactionState.getDbId())
                .orElseThrow(() -> new MetaNotFoundException(String.format("could not get db by id=%s",
                        transactionState.getDbId())))
                .getTable(tableCommitInfo.getTableId())
                .orElseThrow(() -> new MetaNotFoundException(String.format("could not get tbl by id=%s",
                        tableCommitInfo)));

        return tableCommitInfo
                .getIdToPartitionCommitInfo()
                .values().stream()
                .map(PartitionCommitInfo::getPartitionId)
                .map(partitionId -> Optional.ofNullable(table.getPartition(partitionId)))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(Partition::getBaseIndex)
                .map(MaterializedIndex::getTablets)
                .flatMap(Collection::stream)
                .flatMap(tablet ->
                        tablet.getBackendIds()
                                .stream().map(backendId -> Pair.of(backendId, tablet.getId())))
                .collect(Collectors.groupingBy(p -> p.first,
                        Collectors.mapping(p -> p.second, Collectors.toSet())));
    }
    // 用于异步向后端（BE）同步分区可见版本（Visible Version）的后台维护方法
    // 当事务成功提交并进入 VISIBLE 状态后，前端（FE）需要将这些分区最新的可见版本号及时同步给存储它们的 BE 节点，以便 BE 在执行本地存储清理、Compaction（版本合并）或元数据校验时能够对齐版本视图。该方法负责把累积的可见版本信息打包并通过 Agent 批量下发给各个 BE。
    private void sendBackendVisibleVersion() {
        // 1. 加写锁保护共享缓存
        // 获取 visibleVersionsLock 的写锁。因为多线程并发收尾事务时会往 partitionVisibleVersions 和 backendPartitions 中写入数据，
        // 这里加写锁可以保证在读取、打包并清空缓存期间的线程安全。
        visibleVersionsLock.writeLock().lock();
        try {
            // 2. 检查缓存是否为空（提前返回优化）
            // 如果 partitionVisibleVersions（分区最新可见版本映射）或者 backendPartitions（BE 托管的分区集合）为空，
            // 说明近期没有新完成可见的事务产生版本变更，直接返回，避免做无用功。
            if (partitionVisibleVersions.isEmpty() || backendPartitions.isEmpty()) {
                return;
            }

            long createTime = System.currentTimeMillis();
            AgentBatchTask batchTask = new AgentBatchTask();
            // 5. 遍历后端 BE 及其对应的分区集合
            // backendPartitions 是一个 Map<Long, Set<Long>>（BackendId -> 该 BE 上更新过的分区 ID 集合）。
            backendPartitions.forEach((backendId, partitionIds) -> {
                Map<Long, Long> backendPartitionVersions = partitionVisibleVersions.entrySet().stream()
                        .filter(entry -> partitionIds.contains(entry.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                if (backendPartitionVersions.isEmpty()) {
                    return;
                }
                // 5.3 创建“更新可见版本任务”
                // 针对当前 BE 实例化一个 UpdateVisibleVersionTask，传入目标 backendId、对应的版本映射和创建时间戳。
                UpdateVisibleVersionTask task = new UpdateVisibleVersionTask(backendId, backendPartitionVersions,
                        createTime);
                batchTask.addTask(task);
            });
            AgentTaskExecutor.submit(batchTask);

            this.partitionVisibleVersions.clear();
            this.backendPartitions.clear();
        } finally {
            visibleVersionsLock.writeLock().unlock();
        }
    }

    private List<TPartitionVersionInfo> generatePartitionVersionInfos(Collection<TableCommitInfo> tableCommitInfos,
            TransactionState transactionState, Map<Long, Set<Long>> beIdToBaseTabletIds) {
        return tableCommitInfos.stream()
                .map(c -> generatePartitionVersionInfos(c, transactionState, beIdToBaseTabletIds)).flatMap(List::stream)
                .collect(Collectors.toList());
    }

    private List<TPartitionVersionInfo> generatePartitionVersionInfos(TableCommitInfo tableCommitInfo,
            TransactionState transactionState, Map<Long, Set<Long>> beIdToBaseTabletIds) {
        try {
            Map<Long, Set<Long>> map = getBaseTabletIdsForEachBe(transactionState, tableCommitInfo);
            map.forEach((beId, newSet) -> {
                beIdToBaseTabletIds.computeIfPresent(beId, (id, orgSet) -> {
                    orgSet.addAll(newSet);
                    return orgSet;
                });
                beIdToBaseTabletIds.putIfAbsent(beId, newSet);
            });
        } catch (MetaNotFoundException e) {
            LOG.warn("exception occur when trying to get rollup tablets info", e);
        }
        return tableCommitInfo.generateTPartitionVersionInfos();
    }

    private void addPublishVersionTask(Set<Long> publishBackends, long transactionId, TransactionState transactionState,
            List<TPartitionVersionInfo> partitionVersionInfos, Map<Long, Set<Long>> beIdToBaseTabletIds,
            long createPublishVersionTaskTime,
            AgentBatchTask batchTask) {
        for (Long backendId : publishBackends) {
            PublishVersionTask task = new PublishVersionTask(backendId,
                    transactionId,
                    transactionState.getDbId(),
                    partitionVersionInfos,
                    createPublishVersionTaskTime);
            task.setBaseTabletsIds(beIdToBaseTabletIds.getOrDefault(backendId, Collections.emptySet()));
            // add to AgentTaskQueue for handling finish report.
            // not check return value, because the add will success
            AgentTaskQueue.addTask(task);
            batchTask.addTask(task);
            transactionState.addPublishVersionTask(backendId, task);
        }
    }
}
