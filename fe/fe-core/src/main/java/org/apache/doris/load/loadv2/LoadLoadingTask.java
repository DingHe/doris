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

package org.apache.doris.load.loadv2;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Config;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.profile.Profile;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.FailMsg;
import org.apache.doris.nereids.load.NereidsBrokerFileGroup;
import org.apache.doris.nereids.load.NereidsLoadingTaskPlanner;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;
import org.apache.doris.thrift.TPipelineWorkloadGroup;
import org.apache.doris.thrift.TQueryType;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.ErrorTabletInfo;
import org.apache.doris.transaction.TabletCommitInfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

// LoadLoadingTask 是 Broker 导入（Broker Load）作业流程中的加载/执行任务（继承自 LoadTask，处于 LOADING 阶段）
// 在预处理（Pending 阶段文件扫描）完成后，负责将外部存储的数据文件生成分布式导入执行计划（Plan Fragments），并借助 Doris 的查询协调器（Coordinator）将任务下发到底层 BE 执行，最终收集导入结果（如导入计数、错误信息、Tablet 提交信息等）。
// 当 Pending 任务获取到所有待加载文件的元数据后，FE 会创建 LoadLoadingTask。它充当了导入作业与 Doris 分布式查询引擎之间的桥梁。
// 利用 NereidsLoadingTaskPlanner 构建执行计划，通过 Coordinator 驱动 BE 进行分布式扫描、ETL、分桶计算并写入 Tablet，同时支持超时控制、重试机制以及任务执行 Profiling。
public class LoadLoadingTask extends LoadTask {
    private static final Logger LOG = LogManager.getLogger(LoadLoadingTask.class);

    /*
     * load id is used for plan.
     * It should be changed each time we retry this load plan
     */
    // 当前导入任务的全局唯一标识（Query ID 形式）。每次重试该 Plan 时都会生成一个新的 loadId。
    private TUniqueId loadId;
    // 目标数据库对象（Database）。
    private final Database db;
    // 目标 OLAP 表对象。
    private final OlapTable table;
    // 外部存储/Broker 访问配置描述符。
    private final BrokerDesc brokerDesc;
    // 文件组列表，明确了数据源文件与目标分区/分桶的映射关系。
    private final List<BrokerFileGroup> fileGroups;
    // 作业的绝对截止时间戳（毫秒），用于计算超时。
    private final long jobDeadlineMs;
    // 单节点执行内存限制大小。
    private final long execMemLimit;
    // 是否开启严格模式（Strict Mode，用于校验列类型转换是否合法）。
    private final boolean strictMode;
    // 是否为唯一键（Unique Key）的部分更新（Partial Update）。
    private final boolean isPartialUpdate;
    // 部分更新中遇到新行时的处理策略。
    private final TPartialUpdateNewRowPolicy partialUpdateNewKeyPolicy;
    // 当前导入关联的分布式事务 ID（Transaction ID）。
    private final long txnId;
    // 执行导入时使用的时区。
    private final String timezone;
    // timeout of load job, in seconds
    // 作业超时时间（秒）。
    private final long timeoutS;
    // 导入并发度。
    private final int loadParallelism;
    // 发送数据 Batch 的并行度。
    private final int sendBatchParallelism;
    // 是否零容忍（Zero Tolerance，只要有一行错误即失败）。
    private final boolean loadZeroTolerance;
    // 每个 Sink 是否仅加载单个 Tablet 的优化控制项。
    private final boolean singleTabletLoadPerSink;
    // 是否在 Sink 节点启用 MemTable（针对特定导入架构优化）。
    private final boolean enableMemTableOnSinkNode;
    // 向量化执行时每批次处理的行数。
    private final int batchSize;
    // 基于 Nereids 优化器的导入任务规划器，负责生成分布式执行计划（Fragments 和 Scan Nodes）。
    private NereidsLoadingTaskPlanner planner;
    // 作业的性能 Profile 统计对象。
    private Profile jobProfile;
    // 任务开始执行的时间戳。
    private long beginTime;
    // 关联的 Pipeline 工作负载组配置。
    private List<TPipelineWorkloadGroup> tWorkloadGroups = null;
    // 触发该导入操作的用户身份信息。
    protected UserIdentity userInfo;

    public LoadLoadingTask(UserIdentity userInfo, Database db, OlapTable table,
            BrokerDesc brokerDesc, List<BrokerFileGroup> fileGroups,
            long jobDeadlineMs, long execMemLimit, boolean strictMode, boolean isPartialUpdate,
            TPartialUpdateNewRowPolicy partialUpdateNewKeyPolicy,
            long txnId, LoadTaskCallback callback, String timezone,
            long timeoutS, int loadParallelism, int sendBatchParallelism,
            boolean loadZeroTolerance, Profile jobProfile, boolean singleTabletLoadPerSink,
            Priority priority, boolean enableMemTableOnSinkNode, int batchSize) {
        super(callback, TaskType.LOADING, priority);
        this.userInfo = userInfo;
        this.db = db;
        this.table = table;
        this.brokerDesc = brokerDesc;
        this.fileGroups = fileGroups;
        this.jobDeadlineMs = jobDeadlineMs;
        this.execMemLimit = execMemLimit;
        this.strictMode = strictMode;
        this.isPartialUpdate = isPartialUpdate;
        this.partialUpdateNewKeyPolicy = partialUpdateNewKeyPolicy;
        this.txnId = txnId;
        this.failMsg = new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL);
        this.retryTime = 2; // 2 times is enough
        this.timezone = timezone;
        this.timeoutS = timeoutS;
        this.loadParallelism = loadParallelism;
        this.sendBatchParallelism = sendBatchParallelism;
        this.loadZeroTolerance = loadZeroTolerance;
        this.jobProfile = jobProfile;
        this.singleTabletLoadPerSink = singleTabletLoadPerSink;
        this.enableMemTableOnSinkNode = enableMemTableOnSinkNode;
        this.batchSize = batchSize;
    }
    // 在执行前初始化加载任务的计划（Plan）。
    public void init(TUniqueId loadId, List<List<TBrokerFileStatus>> fileStatusList,
                     int fileNum, UserIdentity userInfo) throws UserException {
        this.loadId = loadId;
        List<NereidsBrokerFileGroup> brokerFileGroups = new ArrayList<>(fileGroups.size());
        for (BrokerFileGroup fileGroup : fileGroups) {
            brokerFileGroups.add(fileGroup.toNereidsBrokerFileGroup());
        }
        planner = new NereidsLoadingTaskPlanner(callback.getCallbackId(), txnId, db.getId(), table, brokerDesc,
                brokerFileGroups, strictMode, isPartialUpdate, partialUpdateNewKeyPolicy, timezone, timeoutS,
                loadParallelism, sendBatchParallelism, userInfo, singleTabletLoadPerSink, enableMemTableOnSinkNode);
        planner.plan(loadId, fileStatusList, fileNum);
    }

    public TUniqueId getLoadId() {
        return loadId;
    }

    @Override
    protected void executeTask() throws Exception {
        LOG.info("begin to execute loading task. load id: {} job id: {}. db: {}, tbl: {}. left retry: {}",
                DebugUtil.printId(loadId), callback.getCallbackId(), db.getFullName(), table.getName(), retryTime);

        retryTime--;
        beginTime = System.currentTimeMillis();
        if (!((BrokerLoadJob) callback).updateState(JobState.LOADING)) {
            // job may already be cancelled
            return;
        }
        executeOnce();
    }
    // 负责配置 Coordinator 并注册查询执行。
    protected void executeOnce() throws Exception {
        final boolean enableProfile = this.jobProfile != null;
        // New one query id,
        // 创建协调器（Coordinator），传入 DescTable、Fragments、ScanNodes 等计划内容。
        Coordinator curCoordinator =  EnvFactory.getInstance().createCoordinator(callback.getCallbackId(),
                loadId, planner.getDescTable(),
                planner.getFragments(), planner.getScanNodes(), planner.getTimezone(), loadZeroTolerance,
                enableProfile);
        if (enableProfile) {
            this.jobProfile.addExecutionProfile(curCoordinator.getExecutionProfile());
        }
        curCoordinator.setQueryType(TQueryType.LOAD);
        curCoordinator.getQueryOptions().setEnableInsertStrict(strictMode);
        curCoordinator.setExecMemoryLimit(execMemLimit);

        curCoordinator.setMemTableOnSinkNode(enableMemTableOnSinkNode);
        if (enableMemTableOnSinkNode) {
            curCoordinator.setBatchSize(batchSize);
        }

        long leftTimeMs = getLeftTimeMs();
        if (leftTimeMs <= 0) {
            throw new LoadException("failed to execute loading task when timeout");
        }
        // 1 second is the minimum granularity of actual execution
        int timeoutS = Math.max((int) (leftTimeMs / 1000), 1);
        curCoordinator.setTimeout(timeoutS);

        // NOTE: currently broker load not enter workload group's queue
        // so we set tWorkloadGroup here directly.
        if (tWorkloadGroups != null) {
            curCoordinator.setTWorkloadGroups(tWorkloadGroups);
        }

        try {
            QeProcessorImpl.INSTANCE.registerQuery(loadId, new QeProcessorImpl.QueryInfo(curCoordinator));
            actualExecute(curCoordinator, timeoutS);
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(loadId);
        }
    }
    // 驱动 Coordinator 异步执行计划并阻塞等待其完成，收集执行结果或错误。
    private void actualExecute(Coordinator curCoordinator, int waitSecond) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug(new LogBuilder(LogKey.LOAD_JOB, callback.getCallbackId())
                    .add("task_id", signature)
                    .add("query_id", DebugUtil.printId(curCoordinator.getQueryId()))
                    .add("msg", "begin to execute plan")
                    .build());
        }
        curCoordinator.exec();
        if (curCoordinator.join(waitSecond)) {
            Status status = curCoordinator.getExecStatus();
            if (status.ok() || status.getErrorCode() == TStatusCode.DATA_QUALITY_ERROR) {
                attachment = new BrokerLoadingTaskAttachment(signature,
                        curCoordinator.getLoadCounters(),
                        curCoordinator.getTrackingUrl(),
                        curCoordinator.getFirstErrorMsg(),
                        TabletCommitInfo.fromThrift(curCoordinator.getCommitInfos()),
                        ErrorTabletInfo.fromThrift(curCoordinator.getErrorTabletInfos()
                                .stream().limit(Config.max_error_tablet_of_broker_load).collect(Collectors.toList())),
                        status);
                curCoordinator.getErrorTabletInfos().clear();
            } else {
                throw new LoadException(status.getErrorMsg());
            }
        } else {
            throw new LoadException("coordinator could not finished before job timeout");
        }
    }

    public long getLeftTimeMs() {
        return jobDeadlineMs - System.currentTimeMillis();
    }

    @Override
    public void updateRetryInfo() {
        super.updateRetryInfo();
        UUID uuid = UUID.randomUUID();
        this.loadId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        planner.updateLoadId(this.loadId);
        // reset progress on each retry, otherwise the finished/total num will be incorrect
        Env.getCurrentProgressManager().registerProgressSimple(String.valueOf(callback.getCallbackId()));
    }

    public void settWorkloadGroups(List<TPipelineWorkloadGroup> tWorkloadGroups) {
        this.tWorkloadGroups = tWorkloadGroups;
    }

}
