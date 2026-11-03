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
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.info.IndexType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DataQualityException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.QuotaExceedException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.Version;
import org.apache.doris.common.profile.Profile;
import org.apache.doris.common.profile.ProfileManager.ProfileType;
import org.apache.doris.common.profile.SummaryProfile.SummaryBuilder;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.common.util.MetaLockUtils;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.storage.S3ResourceCompat;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.BrokerFileGroupAggInfo.FileGroupAggKey;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.FailMsg;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.resource.computegroup.ComputeGroup;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.BeginTransactionException;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import java.util.stream.Collectors;

/**
 * There are 3 steps in BrokerLoadJob: BrokerPendingTask, LoadLoadingTask, CommitAndPublishTxn.
 * Step1: BrokerPendingTask will be created on method of unprotectedExecuteJob.
 * Step2: LoadLoadingTasks will be created by the method of onTaskFinished when BrokerPendingTask is finished.
 * Step3: CommitAndPublicTxn will be called by the method of onTaskFinished when all of LoadLoadingTasks are finished.
 */
// 专门用于管理和驱动基于 Broker（或兼容的 S3、HDFS 等对象存储）的批量导入作业（Broker Load）
// BrokerLoadJob 的核心生命周期分为三个主要步骤：
// Step1：在执行阶段（unprotectedExecuteJob）创建并提交 BrokerPendingTask（负责拉取或扫描远端文件元数据）。
// Step2：当 BrokerPendingTask 完成后，通过回调（onTaskFinished）根据表结构和文件切片创建并提交多个 LoadLoadingTask（负责真正的分布式数据并行加载）。
// Step3：当所有 LoadLoadingTask 全部完成后，收集提交信息并触发事务提交与发布（CommitAndPublishTxn）。
// BrokerLoadJob 主要承担以下职责：
// 两阶段任务调度：先通过 Pending 阶段解析远端文件并获取文件状态（File Status），再细分为多个 Loading 阶段分发给各个后端节点（BE）并行导入。
// 重试与事务安全交接：实现了智能的事务重用机制（当 Pending 任务重试时复用已开启的事务 ID，避免 LabelAlreadyUsedException 报错）。
// 性能指标与 Profile 集成：支持收集和汇总整个导入作业的 Profile 性能指标，并在作业成功后推送到 Profile 管理器。
// 资源与计算组绑定：根据当前用户和配置动态绑定计算组（Compute Group），并针对 MOW（Merge-on-Write）表或带倒排索引的表自动调整 Memtable 优化策略。
public class BrokerLoadJob extends BulkLoadJob {

    private static final Logger LOG = LogManager.getLogger(BrokerLoadJob.class);

    // Profile of this load job, including all tasks' profiles
    // 当前导入作业的 Profile 对象，用于汇总记录所有子任务的执行耗时和指标。
    protected Profile jobProfile;
    // If set to true, the profile of load job with be pushed to ProfileManager
    // 若设为 true，则会将导入作业的 Profile 推送到系统的 ProfileManager 中（默认值为 false）。
    protected boolean enableProfile = false;
    // 是否在 Sink 节点启用 Memtable 优化的标志位。
    private boolean enableMemTableOnSinkNode = false;
    // 批处理大小（Batch Size），用于控制 Broker 导入时的数据发送批次。
    private int batchSize = 0;

    // for log replay and unit test
    public BrokerLoadJob() {
        super(EtlJobType.BROKER);
    }

    protected BrokerLoadJob(EtlJobType type) {
        super(type);
    }

    public BrokerLoadJob(long dbId, String label, BrokerDesc brokerDesc,
                         OriginStatement originStmt, UserIdentity userInfo)
            throws MetaNotFoundException {
        super(EtlJobType.BROKER, dbId, label, originStmt, userInfo);
        this.brokerDesc = brokerDesc;
        if (ConnectContext.get() != null) {
            enableProfile = ConnectContext.get().getSessionVariable().enableProfile();
            enableMemTableOnSinkNode = ConnectContext.get().getSessionVariable().enableMemtableOnSinkNode;
            batchSize = ConnectContext.get().getSessionVariable().brokerLoadBatchSize;
        }
    }

    public BrokerLoadJob(EtlJobType type, long dbId, String label, BrokerDesc brokerDesc,
            OriginStatement originStmt, UserIdentity userInfo)
            throws MetaNotFoundException {
        super(type, dbId, label, originStmt, userInfo);
        this.brokerDesc = brokerDesc;
        if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable().enableProfile()) {
            enableProfile = true;
        }
    }
    // 非常关键的事务开启与重试容错方法
    // 主要作用是为当前的 Broker 导入作业向 Doris 的全局事务管理器（Global Transaction Manager）申请开启一个分布式事务，并巧妙地处理了由于任务重试可能导致的标签冲突（Label Already Used）问题。
    @Override
    public void beginTxn()
            throws LabelAlreadyUsedException, BeginTransactionException, AnalysisException, DuplicatedRequestException,
            QuotaExceedException, MetaNotFoundException {
        // 1. 检查是否已经存在有效的事务 ID
        // 背景：如果之前的 Pending 任务在尝试开启事务后由于某种原因失败并触发了重试（Retry），
        // 此时 transactionId 可能已经被赋过值（大于 0）。
        if (transactionId > 0) {
            // A previous attempt of the pending task already began our txn and failed afterwards;
            // the retried task must reuse it instead of failing LabelAlreadyUsedException
            // against the job's own txn.
            LOG.info("broker load job {} reuses already begun txn {} on pending task retry", id, transactionId);
            return;
        }
        try {
            // 2. 向全局事务管理器申请开启新事务
            // 调用 Env.getCurrentGlobalTransactionMgr().beginTransaction(...)，传入以下核心参数：
            // 目标数据库 ID
            // 本次导入涉及的所有目标表 ID 列表
            // 导入任务的唯一 Label（标签）
            transactionId = Env.getCurrentGlobalTransactionMgr()
                    .beginTransaction(dbId, Lists.newArrayList(fileGroupAggInfo.getAllTableIds()), label, null,
                            new TxnCoordinator(TxnSourceType.FE, 0,
                                    FrontendOptions.getLocalHostAddress(),
                                    ExecuteEnv.getInstance().getStartupTime()),
                            TransactionState.LoadJobSourceType.BATCH_LOAD_JOB, id,
                            getTimeout());
        } catch (LabelAlreadyUsedException e) {
            // The label may be occupied by our OWN txn: a previous attempt registered it but threw
            // before transactionId was assigned (e.g. edit log write failure), and the pending
            // task retry would otherwise burn all retries on this exception and cancel the job
            // with a misleading "Label has already been used".
            // 3. 捕获标签已被占用异常并进行容错处理（源码核心精髓）
            // 注释说明：该 Label 可能被我们“自己的”上一次事务尝试所占用：
            // 例如前一次尝试成功向事务管理器注册了 Label，但在将生成的 transactionId 写入元数据日志（Edit Log）时失败了，
            // 导致内存中没来得及记录 transactionId（表现为 transactionId 仍旧 <= 0）。
            // 如果不做处理，Pending 任务重试时会因为再次申请同名 Label 而耗尽重试次数，
            // 最终带上误导性的 "Label has already been used" 错误取消作业。

            // 尝试通过 Label 查找当前作业自己之前遗留且处于 PREPARE 状态的事务 ID
            Long ownTxnId = findSelfPreparedTxnByLabel();
            if (ownTxnId == null) {
                throw e;
            }
            LOG.info("broker load job {} adopts its own prepared txn {} for label {} on pending task retry",
                    id, ownTxnId, label);
            transactionId = ownTxnId;
        }
    }
    // 通过作业的 Label（标签）去全局事务管理器中查找当前作业是否已经存在一个处于 PREPARE（准备中）状态的事务，用于实现作业重试时的幂等性和事务复用。
    private Long findSelfPreparedTxnByLabel() {
        try {
            Long txnId = Env.getCurrentGlobalTransactionMgr().getTransactionIdByLabel(dbId, label,
                    Lists.newArrayList(TransactionStatus.PREPARE));
            if (txnId == null) {
                return null;
            }
            TransactionState existingTxn = Env.getCurrentGlobalTransactionMgr().getTransactionState(dbId, txnId);
            if (existingTxn != null && existingTxn.getCallbackId() == id
                    && existingTxn.getTransactionStatus() == TransactionStatus.PREPARE) {
                return txnId;
            }
        } catch (Exception lookupException) {
            LOG.warn("broker load job {} failed to look up txn by label {}", id, label, lookupException);
        }
        return null;
    }
    // 当 Broker 导入作业正式开始执行（从 PENDING 状态推进）时，该方法会被调用。
    // 核心职责是：创建作业的第一阶段任务（即 Pending 任务，负责文件路径解析与分片规划），将其注册到作业的任务映射中，并提交给全局的 Pending 任务调度器异步执行。
    @Override
    protected void unprotectedExecuteJob() {
        // 1. 创建 Pending 阶段任务
        // 调用内部的辅助方法 createPendingTask()，实例化一个专属于 Broker 导入的 Pending 任务对象。
        LoadTask task = createPendingTask();
        // 2. 注册任务到作业自身的任务管理器
        // 将新创建的任务放入 idToTasks 映射表中（Key 为任务签名 task.getSignature()，Value 为任务对象本身）。
        // 这样作业就能随时追踪该任务的执行状态、处理后续的状态回调或取消操作。
        idToTasks.put(task.getSignature(), task);
        // 3. 提交任务给全局 Pending 任务调度器
        // 通过 Env.getCurrentEnv().getPendingLoadTaskScheduler() 获取 Doris 全局的 Pending 任务调度器，
        // 并调用 submit(task) 将该任务提交进去。
        // 随后，调度器线程池会异步执行该任务（去远端 Broker 或 S3/HDFS 拉取文件列表、计算大小并进行切片规划）
        Env.getCurrentEnv().getPendingLoadTaskScheduler().submit(task);
    }

    protected LoadTask createPendingTask() {
        return new BrokerLoadPendingTask(this, fileGroupAggInfo.getAggKeyToFileGroups(), brokerDesc, getPriority());
    }

    /**
     * Situation1: When attachment is instance of BrokerPendingTaskAttachment,
     * this method is called by broker pending task.
     * LoadLoadingTask will be created after BrokerPendingTask is finished.
     * Situation2: When attachment is instance of BrokerLoadingTaskAttachment, this method is called by LoadLoadingTask.
     * CommitTxn will be called after all of LoadingTasks are finished.
     *
     * @param attachment
     */
    @Override
    public void onTaskFinished(TaskAttachment attachment) {
        if (attachment instanceof BrokerPendingTaskAttachment) {
            onPendingTaskFinished((BrokerPendingTaskAttachment) attachment);
        } else if (attachment instanceof BrokerLoadingTaskAttachment) {
            onLoadingTaskFinished((BrokerLoadingTaskAttachment) attachment);
        }
    }

    /**
     * step1: divide job into loading task
     * step2: init the plan of task
     * step3: submit tasks into loadingTaskExecutor
     * @param attachment BrokerPendingTaskAttachment
     */
    // 当 Broker 导入作业的第一个阶段（即 Pending 阶段，负责连接外部存储、解析文件路径、获取文件大小并进行切片规划）异步执行完成后，系统会触发该方法。
    // 核心职责是承上启下：
    // 第一步（Step 1）：根据 Pending 阶段解析出的文件切片信息，将作业划分为具体的导入任务（Loading Task）。
    // 第二步（Step 2）：初始化导入任务的执行计划（Plan）。
    // 第三步（Step 3）：将切分好的任务提交到导入任务执行器（Loading Task Executor）中并发执行。
    // BrokerPendingTaskAttachment attachment Pending 阶段任务执行完成后的附件对象（Attachment）。它携带了 Pending 任务在远端外部存储（如 HDFS、S3）上扫描和切分好的核心元数据，包括：
    // 任务唯一标识（taskId）。
    // 文件列表及各个文件的具体切片（Splits/Offset 范围）。
    // 文件大小、行数预估等统计信息，供后续生成导入计划使用。
    private void onPendingTaskFinished(BrokerPendingTaskAttachment attachment) {
        // 1. 获取写锁保护作业状态
        // 进入临界区前加写锁，确保对作业状态、已完成任务 ID 集合等元数据的修改是线程安全的。
        writeLock();
        try {
            // check if job has been cancelled
            // 2. 幂等与状态校验：检查作业是否已经终结（取消或完成）
            // 调用 isTxnDone() 检查当前导入作业是否已经处于取消（Cancelled）、完成（Finished）等终态。
            // 如果作业已经终结，说明该回调已经失去意义，打印警告日志并直接返回，忽略此回调。
            if (isTxnDone()) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                        .add("state", state)
                        .add("error_msg", "this task will be ignored when job is: " + state)
                        .build());
                return;
            }
            // 3. 防重校验：检查是否为重复回调
            // 检查 finishedTaskIds 集合中是否已经包含了当前 attachment 的 taskId。
            // 如果包含，说明这是一个重复的回调通知（例如网络重试导致任务完成消息被发送了两次），
            // 打印警告日志并直接返回，防止重复创建加载任务。
            if (finishedTaskIds.contains(attachment.getTaskId())) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                        .add("task_id", attachment.getTaskId())
                        .add("error_msg", "this is a duplicated callback of pending task "
                                + "when broker already has loading task")
                        .build());
                return;
            }

            // add task id into finishedTaskIds
            // 4. 记录已完成的任务 ID
            // 将当前的 taskId 放入 finishedTaskIds 集合中，标记该 Pending 任务已成功处理。
            finishedTaskIds.add(attachment.getTaskId());
        } finally {
            writeUnlock();
        }
        // 6. 核心业务逻辑：划分并创建加载任务 (Step 1, 2, 3)
        try {
            // 获取当前导入作业所属的 Database 对象
            Database db = getDb();
            // 调用 createLoadingTask 方法，利用 attachment 中的文件切片元数据，
            // 为每个数据分片生成具体的加载任务（Loading Task），初始化导入计划，并将其提交给 Loading 线程池执行。
            createLoadingTask(db, attachment);
        } catch (UserException | org.apache.doris.nereids.exceptions.AnalysisException e) {
            // Nereids analysis errors extend RuntimeException, not UserException; without this
            // branch a deterministic planning failure (e.g. "disk ... exceed limit usage") falls
            // into the generic pending-task retry path and the real cause is never reported.
            // 7. 异常捕获：规划失败或分析异常处理
            // 注释特别指出：Nereids 分析错误属于 RuntimeException 而非 UserException。
            // 如果不单独捕获，像“磁盘空间超出限制”这类确定的规划失败就会落入通用的 pending 重试路径，导致真实原因无法向用户报告。
            // 因此这里捕获后，打印警告日志，并直接调用 cancelJobWithoutCheck 取消作业，记录失败原因（ETL_RUN_FAIL）。
            LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                    .add("database_id", dbId)
                    .add("error_msg", "Failed to divide job into loading task.")
                    .build(), e);
            cancelJobWithoutCheck(new FailMsg(FailMsg.CancelType.ETL_RUN_FAIL, e.getMessage()), true, true);
            return;
        } catch (RejectedExecutionException e) {
            LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                    .add("database_id", dbId)
                    .add("error_msg", "the task queque is full.")
                    .build(), e);
            cancelJobWithoutCheck(new FailMsg(FailMsg.CancelType.ETL_RUN_FAIL, e.getMessage()), true, true);
            return;
        }

        loadStartTimestamp = System.currentTimeMillis();
    }

    // make public for UT
    public void setComputeGroup() {
        ComputeGroup computeGroup = null;
        ConnectContext context = ConnectContext.get();
        try {
            if (context == null) {
                context = new ConnectContext();
                context.setThreadLocalInfo();
            }
            if (context.getEnv() == null) {
                context.setEnv(Env.getCurrentEnv());
            }

            String currentUser = getUserInfo().getQualifiedUser();
            // user is null or get an invalid compute group should not be normal case,
            // broker load job can get all backends when meets it.
            if (StringUtils.isEmpty(currentUser)) {
                computeGroup = Env.getCurrentEnv().getComputeGroupMgr().getAllBackendComputeGroup();
                LOG.warn("can not find user in broker load, then skip compute group.");
            } else {
                computeGroup = Env.getCurrentEnv().getAuth().getComputeGroup(currentUser);
                if (ComputeGroup.INVALID_COMPUTE_GROUP.equals(computeGroup)) {
                    LOG.warn("get an invalid compute group in broker load job.");
                    computeGroup = Env.getCurrentEnv().getComputeGroupMgr().getAllBackendComputeGroup();
                }
            }
        } catch (Throwable t) {
            LOG.warn("error happens when set compute group for broker load", t);
            computeGroup = Env.getCurrentEnv().getComputeGroupMgr().getAllBackendComputeGroup();
        }

        context.setComputeGroup(computeGroup);
    }

    protected LoadLoadingTask createTask(Database db, OlapTable table, List<BrokerFileGroup> brokerFileGroups,
            boolean isEnableMemtableOnSinkNode, int batchSize, FileGroupAggKey aggKey,
            BrokerPendingTaskAttachment attachment) throws UserException {
        LoadLoadingTask task = new LoadLoadingTask(this.userInfo, db, table, brokerDesc,
                brokerFileGroups, getDeadlineMs(), getExecMemLimit(),
                isStrictMode(), isPartialUpdate(), getPartialUpdateNewKeyPolicy(),
                transactionId, this, getTimeZone(), getTimeout(),
                getLoadParallelism(), getSendBatchParallelism(),
                getMaxFilterRatio() <= 0, enableProfile ? jobProfile : null, isSingleTabletLoadPerSink(),
                getPriority(), isEnableMemtableOnSinkNode, batchSize);

        UUID uuid = UUID.randomUUID();
        TUniqueId loadId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());

        setComputeGroup();

        task.init(loadId, attachment.getFileStatusByTable(aggKey),
                attachment.getFileNumByTable(aggKey), getUserInfo());
        task.settWorkloadGroups(tWorkloadGroups);
        return task;
    }
    // Broker 导入流程中的核心计划与任务生成器
    // 当 Pending 阶段顺利完成后，该方法被触发。
    // 主要作用是：根据 Pending 阶段收集到的文件切片元数据，对目标表进行校验（如排除临时表、处理倒排索引和部分更新限制）、初始化导入执行计划（Plan）、
    // 为每个表生成对应的加载任务（LoadLoadingTask）、向全局事务中注册表和索引元数据，并将任务提交给加载调度器异步执行。
    private void createLoadingTask(Database db, BrokerPendingTaskAttachment attachment) throws UserException {
        // 1. 获取并排序目标表列表
        // 根据作业涉及的所有表 ID，从数据库中按 ID 顺序安全地获取对应的 Table 对象列表，若有表不存在则抛出异常。
        List<Table> tableList = db.getTablesOnIdOrderOrThrowException(
                Lists.newArrayList(fileGroupAggInfo.getAllTableIds()));
        // divide job into broker loading task by table
        // 2. 初始化新建的加载任务列表
        // 创建一个临时的列表，用于存放本次新创建的 LoadLoadingTask，以便在释放锁之后统一提交。
        List<LoadLoadingTask> newLoadingTasks = Lists.newArrayList();
        if (enableProfile) {
            this.jobProfile = new Profile(
                    true,
                    Integer.valueOf(sessionVariables.getOrDefault(SessionVariable.PROFILE_LEVEL, "1")),
                    Integer.valueOf(sessionVariables.getOrDefault(SessionVariable.AUTO_PROFILE_THRESHOLD_MS, "-1")));
            this.jobProfile.getSummaryProfile().setQueryBeginTime(TimeUtils.getStartTimeMs());
            // TODO: 怎么给这些 load job 设置 profile 记录时间
            // this.jobProfile.setId("BrokerLoadJob " + id + ". " + label);
        }
        // 4. 注册作业进度管理器
        // 获取全局的 ProgressManager，为当前作业 ID 注册一个简单的进度条跟踪器。
        ProgressManager progressManager = Env.getCurrentProgressManager();
        progressManager.registerProgressSimple(String.valueOf(id));
        // 5. 对目标表加读锁（Metadata Read Lock）
        // 在读取表结构、索引和生成导入计划前，对涉及的所有表加读锁，确保元数据在规划期间不会被修改。
        MetaLockUtils.readLockTables(tableList);
        try {
            // 6. 遍历文件组聚合信息（按表和文件组分组）
            for (Map.Entry<FileGroupAggKey, List<BrokerFileGroup>> entry
                    : fileGroupAggInfo.getAggKeyToFileGroups().entrySet()) {
                FileGroupAggKey aggKey = entry.getKey();
                List<BrokerFileGroup> brokerFileGroups = entry.getValue();
                long tableId = aggKey.getTableId();
                // 获取具体的 OlapTable 对象
                OlapTable table = (OlapTable) db.getTableNullable(tableId);
                // 7. 校验：不支持向临时表（Temporary Table）导入数据
                if (table.isTemporary())  {
                    throw new UserException("Do not support load into temporary table "
                        + table.getDisplayName());
                }
                // 8. 计算是否启用 Sink 节点的 MemTable 优化
                boolean isEnableMemtableOnSinkNode =
                        table.getTableProperty().getUseSchemaLightChange() && this.enableMemTableOnSinkNode;
                // 9. 检查表是否包含旧版倒排索引 (Inverted Index V1)
                boolean hasInvertedIndexV1 = false;
                if (table.getIndexes() != null) {
                    for (org.apache.doris.catalog.Index index : table.getIndexes()) {
                        if (index.getIndexType() == IndexType.INVERTED) {
                            if (table.getInvertedIndexFileStorageFormat()
                                    == org.apache.doris.thrift.TInvertedIndexFileStorageFormat.V1) {
                                hasInvertedIndexV1 = true;
                                break;
                            }
                        }
                    }
                }
                // 10. 降级策略：部分更新、旧版倒排索引或云模式下禁用 Sink 端 MemTable 优化
                if (isPartialUpdate() || hasInvertedIndexV1 || Config.isCloudMode()) {
                    isEnableMemtableOnSinkNode = false;
                }

                // Generate loading task and init the plan of task
                // 11. 创建具体的加载任务并初始化执行计划 (Plan)
                // 调用 createTask 生成 LoadLoadingTask，传入数据库、表、文件组、优化开关、批次大小及附件信息。
                LoadLoadingTask task = createTask(db, table, brokerFileGroups,
                        isEnableMemtableOnSinkNode, batchSize, aggKey, attachment);
                // 12. 注册任务到作业全局映射表
                // 放入 idToTasks 中（注意：此处 idToTasks 既包含之前的 Pending 任务，也包含新加的 Loading 任务）。
                idToTasks.put(task.getSignature(), task);
                // idToTasks contains previous LoadPendingTasks, so idToTasks is just used to save all tasks.
                // use newLoadingTasks to save new created loading tasks and submit them later.
                newLoadingTasks.add(task);
                // load id will be added to loadStatistic when executing this task
                // save all related tables and rollups in transaction state
                // 14. 关联事务状态与元数据注册
                // 从全局事务管理器中获取当前作业对应的 TransactionState。
                TransactionState txnState = Env.getCurrentGlobalTransactionMgr()
                        .getTransactionState(dbId, transactionId);
                if (txnState == null) {
                    throw new UserException("txn does not exist: " + transactionId);
                }
                // 将表及索引信息注册到事务状态中
                txnState.addTableIndexes(table);
                // 如果是部分更新（Partial Update），同时在事务状态中设置部分更新的 Schema
                if (isPartialUpdate()) {
                    txnState.setSchemaForPartialUpdate(table);
                }
            }
        } finally {
            MetaLockUtils.readUnlockTables(tableList);
        }
        // Submit task outside the database lock, cause it may take a while if task queue is full.
        // 16. 异步提交加载任务到调度器
        // 注意：提交动作放在数据库锁外面执行，因为如果任务队列满了，submit 可能会阻塞等待，避免长时间占有元数据读锁。
        for (LoadTask loadTask : newLoadingTasks) {
            Env.getCurrentEnv().getLoadingLoadTaskScheduler().submit(loadTask);
        }
    }
    // 当 Broker 导入作业的第二阶段（即 Loading 阶段，负责将外部文件解析并实际导入到各个 BE 分片中）中的某一个具体任务执行完成后，会触发该回调。
    // 的核心职责是：收集并更新各个分片提交的元数据（Commit Infos）、校验数据质量、并在所有加载任务全部完成后，自动驱动事务进入提交（Commit）阶段，使数据最终生效。
    private void onLoadingTaskFinished(BrokerLoadingTaskAttachment attachment) {
        writeLock();
        try {
            // check if job has been cancelled
            // 2. 检查作业是否已经终结（已取消或完成）
            // 如果作业已经终结，说明该回调失去意义，打印警告日志后直接返回。
            if (isTxnDone()) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                        .add("state", state)
                        .add("error_msg", "this task will be ignored when job is: " + state)
                        .build());
                return;
            }

            // check if task has been finished
            // 3. 防重校验：检查任务是否重复回调
            // 如果 finishedTaskIds 中已经包含了当前 attachment 的 taskId，
            // 说明这是一个重复的完成通知（网络重试导致），打印警告并直接返回。
            if (finishedTaskIds.contains(attachment.getTaskId())) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                        .add("task_id", attachment.getTaskId())
                        .add("error_msg", "this is a duplicated callback of loading task").build());
                return;
            }

            // update loading status
            // 4. 更新加载进度状态
            // 将当前的 taskId 加入已完成集合，并调用 updateLoadingStatus(attachment)
            // 把该任务带回来的 TabletCommitInfo 等提交信息合并累加到作业的全局 commitInfos 中。
            finishedTaskIds.add(attachment.getTaskId());
            updateLoadingStatus(attachment);

            // begin commit txn when all of loading tasks have been finished
            // 5. 检查是否所有加载任务均已完成
            // 对比已完成任务数（finishedTaskIds.size()）与总任务数（idToTasks.size()）。
            // 如果还有未完成的任务，直接返回，等待最后一个任务触发提交；
            // 如果大小相等，说明所有子任务全部成功搞定，继续往下执行提交事务的逻辑。
            if (finishedTaskIds.size() != idToTasks.size()) {
                return;
            }
        } finally {
            writeUnlock();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(new LogBuilder(LogKey.LOAD_JOB, id)
                    .add("commit_infos", Joiner.on(",").join(commitInfos))
                    .build());
        }

        // check data quality
        // 8. 数据质量校验
        // 检查全局的数据质量策略（如错误行数是否超标）以及当前任务附件中的状态码是否包含 DATA_QUALITY_ERROR。
        // 如果不满足数据质量要求，调用 cancelJobWithoutCheck 取消作业（取消类型为 ETL_QUALITY_UNSATISFIED），并直接返回
        if (!checkDataQuality() || attachment.getStatus().getErrorCode() == TStatusCode.DATA_QUALITY_ERROR) {
            cancelJobWithoutCheck(new FailMsg(FailMsg.CancelType.ETL_QUALITY_UNSATISFIED,
                            DataQualityException.QUALITY_FAIL_MSG), true, true);
            return;
        }
        // 3. 事务提交阶段（支持重试与锁控制）
        Database db = null;
        List<Table> tableList = null;
        int retryTimes = 0;
        while (true) {
            try {
                db = getDb();
                tableList = db.getTablesOnIdOrderOrThrowException(
                        Lists.newArrayList(fileGroupAggInfo.getAllTableIds()));
                // 10. 根据部署模式加表锁
                // 如果是云模式（Cloud Mode），调用 commitLockTables；如果是标准模式，调用 writeLockTablesOrMetaException，
                // 确保在提交事务前对涉及的目标表加写锁，防止元数据并发修改。
                if (Config.isCloudMode()) {
                    MetaLockUtils.commitLockTables(tableList);
                } else {
                    MetaLockUtils.writeLockTablesOrMetaException(tableList);
                }
            } catch (MetaNotFoundException e) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                        .add("database_id", dbId)
                        .add("error_msg", "db has been deleted when job is loading")
                        .build(), e);
                cancelJobWithoutCheck(new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL, e.getMessage()), true, true);
                return;
            }
            // 12. 正式提交全局事务
            try {
                LOG.info(new LogBuilder(LogKey.LOAD_JOB, id)
                        .add("txn_id", transactionId)
                        .add("msg", "Load job try to commit txn")
                        .build());
                // 调用全局事务管理器（GlobalTransactionMgr）的 commitTransactionWithoutLock 方法，
                // 传入 dbId、涉及的表列表、事务 ID、收集好的 commitInfos 以及作业最终操作配置，正式提交事务。
                Env.getCurrentGlobalTransactionMgr().commitTransactionWithoutLock(
                        dbId, tableList, transactionId, commitInfos, getLoadJobFinalOperation());
                // 13. 提交后的后续处理
                afterLoadingTaskCommitTransaction(tableList);
                afterCommit();
                return;
            } catch (UserException e) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                        .add("database_id", dbId)
                        .add("retry_times", retryTimes)
                        .add("error_msg", "Failed to commit txn with error:" + e.getMessage())
                        .build(), e);
                // 特别针对 UNIQUE KEY 存算分离/MOW 模型中的“删除位图锁冲突 (DELETE_BITMAP_LOCK_ERR)”进行重试控制
                if (e.getErrorCode() == InternalErrorCode.DELETE_BITMAP_LOCK_ERR) {
                    retryTimes++;
                    if (retryTimes >= Config.mow_calculate_delete_bitmap_retry_times) {
                        LOG.warn("cancelJob {} because up to max retry time,exception {}", id, e);
                        cancelJobWithoutCheck(new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL, e.getMessage()), true,
                                true);
                        return;
                    }
                } else {
                    cancelJobWithoutCheck(new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL, e.getMessage()), true, true);
                    return;
                }
            } finally {
                if (Config.isCloudMode()) {
                    MetaLockUtils.commitUnlockTables(tableList);
                } else {
                    MetaLockUtils.writeUnlockTables(tableList);
                }
            }
        }
    }

    // cloud override
    protected void afterLoadingTaskCommitTransaction(List<Table> tableList) {
    }

    protected void afterCommit() throws DdlException {}

    protected LoadJobFinalOperation getLoadJobFinalOperation() {
        return new LoadJobFinalOperation(id, loadingStatus, progress, loadStartTimestamp, finishTimestamp, state,
                failMsg);
    }

    private Map<String, String> getSummaryInfo(boolean isFinished) {
        long currentTimestamp = System.currentTimeMillis();
        SummaryBuilder builder = new SummaryBuilder();
        // Id of summary profile will be shown as the profile id in the web page
        builder.profileId(String.valueOf(id));
        if (Version.DORIS_BUILD_VERSION_MAJOR == 0) {
            builder.dorisVersion(Version.DORIS_BUILD_SHORT_HASH);
        } else {
            builder.dorisVersion(Version.DORIS_BUILD_VERSION + "-" + Version.DORIS_BUILD_SHORT_HASH);
        }
        builder.taskType(ProfileType.LOAD.name());
        builder.startTime(TimeUtils.longToTimeString(createTimestamp));
        if (isFinished) {
            builder.endTime(TimeUtils.longToTimeString(currentTimestamp));
            builder.totalTime(DebugUtil.getPrettyStringMs(currentTimestamp - createTimestamp));
        }
        builder.taskState(isFinished ? "FINISHED" : "RUNNING");
        builder.user(getUserInfo() != null ? getUserInfo().getQualifiedUser() : "N/A");
        builder.defaultCatalog(InternalCatalog.INTERNAL_CATALOG_NAME);
        builder.defaultDb(getDefaultDb());
        builder.sqlStatement(getOriginStmt().originStmt);
        return builder.build();
    }

    private String getDefaultDb() {
        Database database = Env.getCurrentEnv().getInternalCatalog().getDb(this.dbId).orElse(null);
        return database == null ? "N/A" : database.getFullName();
    }
    // 当 Broker 导入作业的某一个子加载任务（Loading Task）执行完成后，该方法会被调用。
    // 核心职责是：将子任务汇报上来的统计计数器（如正常行数、异常行数）、错误信息、追踪链接、副本提交元数据（Commit Infos）以及错误分片信息累加聚合到整个作业的全局状态中，并实时计算和更新作业的导入进度百分比。
    private void updateLoadingStatus(BrokerLoadingTaskAttachment attachment) {
        // 1. 累加并更新“异常行数”计数器
        // 获取子任务上报的异常行数（DPP_ABNORMAL_ALL，通常指格式错误、类型不匹配等不合规行），
        // 通过 increaseCounter 累加到作业全局的统计中，并用 replaceCounter 更新 loadingStatus。
        loadingStatus.replaceCounter(DPP_ABNORMAL_ALL,
                increaseCounter(DPP_ABNORMAL_ALL, attachment.getCounter(DPP_ABNORMAL_ALL)));
        // 2. 累加并更新“正常加载行数”计数器
        loadingStatus.replaceCounter(DPP_NORMAL_ALL,
                increaseCounter(DPP_NORMAL_ALL, attachment.getCounter(DPP_NORMAL_ALL)));
        // 3. 累加并更新“未选中行数”计数器
        loadingStatus.replaceCounter(UNSELECTED_ROWS,
                increaseCounter(UNSELECTED_ROWS, attachment.getCounter(UNSELECTED_ROWS)));
        // 4. 更新错误追踪 URL
        // 如果当前子任务附件中包含了非空的追踪 URL（用于下载或查看错误行详情），
        // 则将其设置到作业的全局 loadingStatus 中，供用户排查错误使用。
        if (attachment.getTrackingUrl() != null) {
            loadingStatus.setTrackingUrl(attachment.getTrackingUrl());
        }
        if (attachment.getFirstErrorMsg() != null) {
            loadingStatus.setFirstErrorMsg(attachment.getFirstErrorMsg());
        }
        // 6. 收集副本提交元数据
        // 将当前子任务带回来的所有 Tablet 提交信息（commitInfoList）批量追加到全局的 commitInfos 列表中。
        // 这些信息将在所有子任务完成后，用于向全局事务管理器提交事务。
        commitInfos.addAll(attachment.getCommitInfoList());
        errorTabletInfos.addAll(attachment.getErrorTabletInfos().stream().limit(Config.max_error_tablet_of_broker_load)
                .collect(Collectors.toList()));
        //  8. 计算当前作业的总体进度百分比
        progress = (int) ((double) finishedTaskIds.size() / idToTasks.size() * 100);
        if (progress == 100) {
            progress = 99;
        }
    }

    @Override
    public void updateProgress(Long beId, TUniqueId loadId, TUniqueId fragmentId, long scannedRows,
                               long scannedBytes, boolean isDone) {
        super.updateProgress(beId, loadId, fragmentId, scannedRows, scannedBytes, isDone);
        progress = (int) ((double) loadStatistic.getLoadBytes() / loadStatistic.totalFileSizeB * 100);
        if (progress >= 100) {
            progress = 99;
        }
    }

    private String increaseCounter(String key, String deltaValue) {
        long value = 0;
        if (loadingStatus.getCounters().containsKey(key)) {
            value = Long.valueOf(loadingStatus.getCounters().get(key));
        }
        if (deltaValue != null) {
            value += Long.valueOf(deltaValue);
        }
        return String.valueOf(value);
    }

    @Override
    public void afterVisible(TransactionState txnState, boolean txnOperated) {
        super.afterVisible(txnState, txnOperated);
        if (!enableProfile) {
            return;
        }
        jobProfile.updateSummary(getSummaryInfo(true), true, null);
        // jobProfile has been pushed into ProfileManager, remove reference in brokerLoadJob
        jobProfile = null;
    }

    @Override
    public String getResourceName() {
        StorageBackend.StorageType storageType = brokerDesc.getStorageType();
        if (storageType == StorageBackend.StorageType.BROKER) {
            return brokerDesc.getName();
        } else if (storageType == StorageBackend.StorageType.S3) {
            return Optional.ofNullable(brokerDesc.getProperties())
                .map(o -> o.get(S3ResourceCompat.Env.ENDPOINT))
                .orElse("s3_cluster");
        } else {
            return storageType.name().toLowerCase().concat("_cluster");
        }
    }
}
