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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.AuthorizationInfo;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.QuotaExceedException;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.EtlStatus;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.FailMsg.CancelType;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.privilege.Privilege;
import org.apache.doris.nereids.trees.plans.commands.LoadCommand;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.thrift.TEtlState;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;
import org.apache.doris.thrift.TPipelineWorkloadGroup;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.AbstractTxnStateChangeCallback;
import org.apache.doris.transaction.BeginTransactionException;
import org.apache.doris.transaction.ErrorTabletInfo;
import org.apache.doris.transaction.TransactionException;
import org.apache.doris.transaction.TransactionState;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
// LoadJob 是负责管理和调度数据导入任务（如 Broker Load、Insert、Copy 等）的核心抽象基类
// 定义了导入任务从创建、执行、加载、提交到完成或失败的整个生命周期管理框架，并与 Doris 的全局事务管理器（GlobalTransactionMgr）紧密结合，确保导入过程的一致性和可靠性。
// 主要承担以下职责
// 生命周期管理：维护导入任务的状态机（如 PENDING、LOADING、COMMITTED、FINISHED、CANCELLED 等）。
// 事务协同：作为事务状态变更回调类（继承自 AbstractTxnStateChangeCallback），监听并响应 Doris 内部全局事务的提交、中止等事件。
// 并发控制：利用读写锁（ReentrantReadWriteLock）保证多线程并发访问和修改任务状态时的线程安全性。
// 属性与配置管理：管理导入任务所需的各类参数（超时时间、内存限制、最大过滤比例、时区等）及鉴权信息。
// 统计与监控：记录任务进度、扫描行数/字节数、错误行信息（Error Tablets）、耗时等统计指标，支持通过 SQL 命令（如 SHOW LOAD）展示。
public abstract class LoadJob extends AbstractTxnStateChangeCallback
                implements LoadTaskCallback, Writable, GsonPostProcessable {

    private static final Logger LOG = LogManager.getLogger(LoadJob.class);
    // 正常行数
    public static final String DPP_NORMAL_ALL = "dpp.norm.ALL";
    // 异常行数
    public static final String DPP_ABNORMAL_ALL = "dpp.abnorm.ALL";
    // 未选中行数
    public static final String UNSELECTED_ROWS = "unselected.rows";
    // 导入作业的唯一标识 ID（全局唯一）
    @SerializedName("id")
    protected long id;
    // 目标数据库的 ID
    // input params
    @SerializedName("did")
    protected long dbId;
    // 导入作业的标签（Label），用于防止重复导入
    @SerializedName("lb")
    protected String label;
    // 作业当前所处的生命周期状态
    @SerializedName("st")
    protected JobState state = JobState.PENDING;
    // 导入任务类型（如 EtlJobType 定义的 Broker、Copy、Insert 等）
    @SerializedName("jt")
    protected EtlJobType jobType;
    // the auth info could be null when load job is created before commit named 'Persist auth info in load job'
    // 鉴权信息对象，用于检查用户是否有对目标表执行 LOAD 的权限。
    @SerializedName("ai")
    protected AuthorizationInfo authorizationInfo;
    // 作业创建的时间戳（毫秒）
    @SerializedName("ct")
    protected long createTimestamp = System.currentTimeMillis();
    // 开始加载数据的时间戳（默认 -1）
    @SerializedName("lst")
    protected long loadStartTimestamp = -1;
    // 作业完成（成功或失败）的时间戳（默认 -1）
    @SerializedName("ft")
    protected long finishTimestamp = -1;
    // 关联的 Doris 全局事务 ID
    @SerializedName("txid")
    protected long transactionId;
    // 失败信息对象（FailMsg），记录取消或失败的类型及具体原因。
    @SerializedName("fm")
    protected FailMsg failMsg;
    // 并发映射表（Map），保存了该作业包含的所有子任务（LoadTask ID 到任务对象的映射）。
    protected Map<Long, LoadTask> idToTasks = Maps.newConcurrentMap();
    // 记录已完成的子任务 ID 集合
    protected Set<Long> finishedTaskIds = Sets.newHashSet();
    // 加载状态对象（EtlStatus），包含详细的计数器、追踪链接和错误信息。
    @SerializedName("lsts")
    protected EtlStatus loadingStatus = new EtlStatus();
    // 0: the job status is pending
    // n/100: n is the number of task which has been finished
    // 99: all of tasks have been finished
    // 100: txn status is visible and load has been finished
    // 进度值（0-100）：0 表示 PENDING；n/100 表示已完成 n 个任务；99 表示所有任务完成；100 表示事务可见且导入结束。
    @SerializedName("pgs")
    protected int progress;

    // non-persistence
    // This param is set true during txn is committing.
    // During committing, the load job could not be cancelled.
    // 非持久化标志位。当事务正在提交时设为 true，此时作业不能被取消。
    protected boolean isCommitting = false;
    // 可重入读写锁（公平锁），用于保护作业内部状态和属性的并发修改。
    protected ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    // 请求唯一 ID，用于检查导入开始请求是否为重复请求。
    // this request id is only used for checking if a load begin request is a duplicate request.
    protected TUniqueId requestId;
    // 导入统计对象，用于追踪扫描的行数、字节数等明细。
    protected LoadStatistic loadStatistic = new LoadStatistic();

    // This map is used to save job property.
    // 保存作业属性参数的 Map（如超时时间、内存限制、过滤比例等）。
    @SerializedName("jp")
    private Map<String, Object> jobProperties = Maps.newHashMap();
    // 错误 Tablet 信息列表，记录导入过程中出错的 Tablet 及其错误原因。
    protected List<ErrorTabletInfo> errorTabletInfos = Lists.newArrayList();
    // 提交该导入作业的用户身份（UserIdentity）。
    @SerializedName("ui")
    protected UserIdentity userInfo = UserIdentity.UNKNOWN;
    // 导入作业的备注信息（默认为空字符串）。
    @SerializedName("cmt")
    protected String comment = "";
    // 关联的 Pipeline Workload Group 配置列表（用于资源隔离）。
    protected List<TPipelineWorkloadGroup> tWorkloadGroups = null;

    public LoadJob(EtlJobType jobType) {
        this.jobType = jobType;
        initDefaultJobProperties();
    }

    public LoadJob(EtlJobType jobType, long dbId, String label) {
        this(jobType);
        this.id = Env.getCurrentEnv().getNextId();
        this.dbId = dbId;
        this.label = label;
    }

    public LoadJob(EtlJobType jobType, long dbId, String label, long jobId) {
        this(jobType);
        this.id = jobId;
        this.dbId = dbId;
        this.label = label;
    }

    protected void readLock() {
        lock.readLock().lock();
    }

    protected void readUnlock() {
        lock.readLock().unlock();
    }

    protected void writeLock() {
        lock.writeLock().lock();
    }

    protected void writeUnlock() {
        lock.writeLock().unlock();
    }

    public long getId() {
        return id;
    }

    public Database getDb() throws MetaNotFoundException {
        return Env.getCurrentInternalCatalog().getDbOrMetaException(dbId);
    }

    public long getDbId() {
        return dbId;
    }

    public String getLabel() {
        return label;
    }

    public JobState getState() {
        return state;
    }

    public EtlJobType getJobType() {
        return jobType;
    }

    public long getCreateTimestamp() {
        return createTimestamp;
    }

    protected long getDeadlineMs() {
        return createTimestamp + getTimeout() * 1000;
    }

    private boolean isTimeout() {
        return System.currentTimeMillis() > getDeadlineMs();
    }

    public long getFinishTimestamp() {
        return finishTimestamp;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public void initLoadProgress(TUniqueId loadId, Set<TUniqueId> fragmentIds, List<Long> relatedBackendIds) {
        loadStatistic.initLoad(loadId, fragmentIds, relatedBackendIds);
    }

    public void updateProgress(Long beId, TUniqueId loadId, TUniqueId fragmentId, long scannedRows,
                               long scannedBytes, boolean isDone) {
        loadStatistic.updateLoadProgress(beId, loadId, fragmentId, scannedRows, scannedBytes, isDone);
    }

    public void setLoadFileInfo(int fileNum, long fileSize) {
        this.loadStatistic.fileNum = fileNum;
        this.loadStatistic.totalFileSizeB = fileSize;
    }

    public void addLoadFileInfo(int fileNum, long fileSize) {
        this.loadStatistic.fileNum += fileNum;
        this.loadStatistic.totalFileSizeB += fileSize;
    }

    /**
     * Show table names for frontend
     * If table name could not be found by id, the table id will be used instead.
     *
     * @return
     */
    abstract Set<String> getTableNamesForShow();

    /**
     * Return the real table names by table ids.
     * The method is invoked by 'checkAuth' when authorization info is null in job.
     * Also it is invoked by 'gatherAuthInfo' which saves the auth info in the constructor of job.
     * Throw MetaNofFoundException when table name could not be found.
     *
     * @return
     */
    public abstract Set<String> getTableNames() throws MetaNotFoundException;

    // return true if the corresponding transaction is done(COMMITTED, FINISHED, CANCELLED, RETRY)
    public boolean isTxnDone() {
        return state == JobState.COMMITTED || state == JobState.FINISHED
                || state == JobState.CANCELLED || state == JobState.RETRY;
    }

    // return true if job is done(FINISHED/CANCELLED/UNKNOWN)
    public boolean isCompleted() {
        return state == JobState.FINISHED || state == JobState.CANCELLED || state == JobState.UNKNOWN;
    }

    public void setJobProperties(Map<String, String> properties) throws DdlException {
        initDefaultJobProperties();

        // set property from session variables
        if (ConnectContext.get() != null) {
            jobProperties.put(LoadCommand.EXEC_MEM_LIMIT,
                    ConnectContext.get().getSessionVariable().getMaxExecMemByte());
            jobProperties.put(LoadCommand.TIMEZONE, ConnectContext.get().getSessionVariable().getTimeZone());
            jobProperties.put(LoadCommand.SEND_BATCH_PARALLELISM,
                    ConnectContext.get().getSessionVariable().getSendBatchParallelism());
        }

        if (properties == null || properties.isEmpty()) {
            return;
        }

        // set property from specified job properties
        for (String key : LoadCommand.PROPERTIES_MAP.keySet()) {
            if (!properties.containsKey(key)) {
                continue;
            }
            try {
                jobProperties.put(key, LoadCommand.PROPERTIES_MAP.get(key).apply(properties.get(key)));
            } catch (Exception e) {
                throw new DdlException("Failed to set property " + key + ". Error: " + e.getMessage());
            }
        }
    }

    public UserIdentity getUserInfo() {
        return userInfo;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    private void initDefaultJobProperties() {
        long timeout = Config.broker_load_default_timeout_second;
        switch (jobType) {
            case COPY:
            case BROKER:
                timeout = Config.broker_load_default_timeout_second;
                break;
            case INSERT:
                timeout = Optional.ofNullable(ConnectContext.get())
                                    .map(ConnectContext::getExecTimeoutS)
                                    .orElse(Config.insert_load_default_timeout_second);
                break;
            default:
                break;
        }
        jobProperties.put(LoadCommand.TIMEOUT_PROPERTY, timeout);
        jobProperties.put(LoadCommand.EXEC_MEM_LIMIT, 2 * 1024 * 1024 * 1024L);
        jobProperties.put(LoadCommand.MAX_FILTER_RATIO_PROPERTY, 0.0);
        jobProperties.put(LoadCommand.STRICT_MODE, false);
        jobProperties.put(LoadCommand.PARTIAL_COLUMNS, false);
        jobProperties.put(LoadCommand.PARTIAL_UPDATE_NEW_KEY_POLICY, TPartialUpdateNewRowPolicy.APPEND);
        jobProperties.put(LoadCommand.TIMEZONE, TimeUtils.DEFAULT_TIME_ZONE);
        jobProperties.put(LoadCommand.LOAD_PARALLELISM, Config.default_load_parallelism);
        jobProperties.put(LoadCommand.SEND_BATCH_PARALLELISM, 1);
        jobProperties.put(LoadCommand.LOAD_TO_SINGLE_TABLET, false);
        jobProperties.put(LoadCommand.PRIORITY, LoadTask.Priority.NORMAL);
    }

    public void beginTxn() throws LabelAlreadyUsedException, BeginTransactionException,
            AnalysisException, DuplicatedRequestException, QuotaExceedException, MetaNotFoundException {
    }

    /**
     * create pending task for load job and add pending task into pool
     * if job has been cancelled, this step will be ignored
     *
     * @throws LabelAlreadyUsedException  the job is duplicated
     * @throws BeginTransactionException  the limit of load job is exceeded
     * @throws AnalysisException          there are error params in job
     * @throws DuplicatedRequestException
     */
    public void execute() throws LoadException {
        writeLock();
        try {
            unprotectedExecute();
        } finally {
            writeUnlock();
        }
    }

    public void unprotectedExecute() throws LoadException {
        // check if job state is pending
        if (state != JobState.PENDING) {
            return;
        }

        unprotectedExecuteJob();
    }

    public void processTimeout() {
        // this is only for jobs which transaction is not started.
        // if transaction is started, global transaction manager will handle the timeout.
        writeLock();
        try {
            if (state != JobState.PENDING) {
                return;
            }

            if (!isTimeout()) {
                return;
            }

            unprotectedExecuteCancel(new FailMsg(FailMsg.CancelType.TIMEOUT, "loading timeout to cancel"), false);
            logFinalOperation();
        } finally {
            writeUnlock();
        }
    }

    protected void unprotectedExecuteJob() throws LoadException {
    }

    /**
     * This method only support update state to finished and loading.
     * It will not be persisted when desired state is finished because txn visible will edit the log.
     * If you want to update state to cancelled, please use the cancelJob function.
     *
     * @param jobState
     */
    public boolean updateState(JobState jobState) {
        writeLock();
        try {
            return unprotectedUpdateState(jobState);
        } finally {
            writeUnlock();
        }
    }
    // 内部状态更新核心逻辑。包含防重入保护（若已处于终态则拒绝更新），
    // 并根据目标状态分发到对应方法（UNKNOWN、LOADING、COMMITTED、FINISHED）。
    protected boolean unprotectedUpdateState(JobState jobState) {
        if (this.state.isFinalState()) {
            // This is a simple self-protection mechanism to prevent jobs
            // that have entered the final state from being placed in a non-terminating state again.
            // For example, when a LoadLoadingTask starts running, it tries to set the job state to LOADING,
            // but the job may have been cancelled (CANCELLED) due to a timeout.
            // At this point, the job state should not be set to LOADING again.
            // It is safe to return directly here without any processing,
            // and other processes will ensure that the job ends properly.
            LOG.warn("the load job {} is in final state: {}, should not update state to {} again",
                    id, this.state, jobState);
            return false;
        }
        switch (jobState) {
            case UNKNOWN:
                executeUnknown();
                return true;
            case LOADING:
                executeLoad();
                return true;
            case COMMITTED:
                executeCommitted();
                return true;
            case FINISHED:
                executeFinish();
                return true;
            default:
                return false;
        }
    }

    private void executeUnknown() {
        // set finished timestamp to create timestamp, so that this unknown job
        // can be remove due to label expiration so soon as possible
        finishTimestamp = createTimestamp;
        state = JobState.UNKNOWN;
    }

    private void executeLoad() {
        loadStartTimestamp = System.currentTimeMillis();
        state = JobState.LOADING;
    }

    private void executeCommitted() {
        state = JobState.COMMITTED;
    }

    // if needLog is false, no need to write edit log.
    public void cancelJobWithoutCheck(FailMsg failMsg, boolean abortTxn, boolean needLog) {
        writeLock();
        try {
            unprotectedExecuteCancel(failMsg, abortTxn);
            if (needLog) {
                logFinalOperation();
            }
        } finally {
            writeUnlock();
        }
    }

    public void cancelJob(FailMsg failMsg) throws DdlException {
        writeLock();
        try {
            checkAuth("CANCEL LOAD");
            if (isCommitting) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                        .add("error_msg", "The txn which belongs to job is committing. "
                                + "The job could not be cancelled in this step").build());
                throw new DdlException("Job could not be cancelled while txn is committing");
            }
            if (isTxnDone()) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                        .add("state", state)
                        .add("error_msg", "Job could not be cancelled when job is " + state)
                        .build());
                throw new DdlException("Job could not be cancelled when job is finished or cancelled");
            }

            unprotectedExecuteCancel(failMsg, true);
            logFinalOperation();
        } finally {
            writeUnlock();
        }
    }

    public void checkAuth(String command) throws DdlException {
        if (authorizationInfo == null) {
            // use the old method to check priv
            checkAuthWithoutAuthInfo(command);
            return;
        }
        if (!Env.getCurrentEnv().getAccessManager().checkPrivByAuthInfo(ConnectContext.get(), authorizationInfo,
                PrivPredicate.LOAD)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    Privilege.LOAD_PRIV);
        }
    }

    /**
     * This method is compatible with old load job without authorization info
     * If db or table name could not be found by id, it will throw the NOT_EXISTS_ERROR
     *
     * @throws DdlException
     */
    private void checkAuthWithoutAuthInfo(String command) throws DdlException {
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbId);

        // check auth
        try {
            Set<String> tableNames = getTableNames();
            if (tableNames.isEmpty()) {
                // forward compatibility
                if (!Env.getCurrentEnv().getAccessManager()
                        .checkDbPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, db.getFullName(),
                                PrivPredicate.LOAD)) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                            Privilege.LOAD_PRIV);
                }
            } else {
                for (String tblName : tableNames) {
                    if (!Env.getCurrentEnv().getAccessManager()
                            .checkTblPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, db.getFullName(),
                                    tblName, PrivPredicate.LOAD)) {
                        ErrorReport.reportDdlException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR,
                                command,
                                ConnectContext.get().getQualifiedUser(),
                                ConnectContext.get().getRemoteIP(), db.getFullName() + ": " + tblName);
                    }
                }
            }
        } catch (MetaNotFoundException e) {
            throw new DdlException(e.getMessage());
        }
    }

    protected void unprotectedExecuteRetry(FailMsg failMsg) {
        LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id).add("transaction_id", transactionId)
                .add("error_msg", "Failed to execute load with error: " + failMsg.getMsg()).build());

        // get load ids of all loading tasks, we will cancel their coordinator process later
        List<TUniqueId> loadIds = Lists.newArrayList();
        for (LoadTask loadTask : idToTasks.values()) {
            if (loadTask instanceof LoadLoadingTask) {
                loadIds.add(((LoadLoadingTask) loadTask).getLoadId());
            }
        }

        // set failMsg and state
        this.failMsg = failMsg;
        if (failMsg.getCancelType() == CancelType.TXN_UNKNOWN) {
            // for bug fix, see LoadManager's fixLoadJobMetaBugs() method
            finishTimestamp = createTimestamp;
        } else {
            finishTimestamp = System.currentTimeMillis();
        }

        // remove callback before abortTransaction(), so that the afterAborted() callback will not be called again
        Env.getCurrentGlobalTransactionMgr().getCallbackFactory().removeCallback(id);
        // abort txn by label, because transactionId here maybe -1
        try {
            LOG.debug(new LogBuilder(LogKey.LOAD_JOB, id)
                    .add("label", label)
                    .add("msg", "begin to abort txn")
                    .build());
            Env.getCurrentGlobalTransactionMgr().abortTransaction(dbId, label, failMsg.getMsg());
        } catch (UserException e) {
            LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                    .add("label", label)
                    .add("error_msg", "failed to abort txn when job is cancelled. " + e.getMessage())
                    .build());
        }

        // cancel all running coordinators, so that the scheduler's worker thread will be released
        for (TUniqueId loadId : loadIds) {
            Coordinator coordinator = QeProcessorImpl.INSTANCE.getCoordinator(loadId);
            if (coordinator != null) {
                coordinator.cancel(new Status(TStatusCode.CANCELLED, failMsg.getMsg()));
            }
        }

        // change state
        state = JobState.RETRY;
    }

    /**
     * This method will cancel job without edit log and lock
     *
     * @param failMsg
     * @param abortTxn true: abort txn when cancel job, false: only change the state of job and ignore abort txn
     */
    // 用于在无需加显式锁和不记录 EditLog（元数据日志）的情况下，强制取消 Broker 导入作业的核心清理方法。
    // 记录取消日志、更新加载状态与失败原因、清理和重置任务列表、终止底层所有运行中的 Coordinator（调度协调器）、清理事务回调工厂，并根据参数决定是否中止（Abort）关联的分布式事务，最终将作业状态变更为 CANCELLED。
    protected void unprotectedExecuteCancel(FailMsg failMsg, boolean abortTxn) {
        LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id).add("transaction_id", transactionId)
                .add("error_msg", "Failed to execute load with error: " + failMsg.getMsg()).build());

        // clean the loadingStatus
        // 2. 清理并更新加载状态详情
        // 将 loadingStatus（用于客户端展示的 ETL 状态）设置为 CANCELLED（已取消），
        // 并把具体的失败原因设置进去。
        loadingStatus.setState(TEtlState.CANCELLED);
        loadingStatus.setFailMsg(failMsg.getMsg());
        // get load ids of all loading tasks, we will cancel their coordinator process later
        // 3. 收集所有正在加载的子任务的 Load ID
        // 创建一个列表用来存放所有运行中任务的唯一标识（TUniqueId loadId）。
        List<TUniqueId> loadIds = Lists.newArrayList();
        for (LoadTask loadTask : idToTasks.values()) {
            // 如果当前任务属于 LoadLoadingTask（加载阶段的具体子任务），则提取出其对应的 loadId。
            if (loadTask instanceof LoadLoadingTask) {
                loadIds.add(((LoadLoadingTask) loadTask).getLoadId());
            }
        }
        idToTasks.clear();

        // set failMsg and state
        this.failMsg = failMsg;
        if (failMsg.getCancelType() == CancelType.TXN_UNKNOWN) {
            // for bug fix, see LoadManager's fixLoadJobMetaBugs() method
            finishTimestamp = createTimestamp;
        } else {
            finishTimestamp = System.currentTimeMillis();
        }

        // remove callback before abortTransaction(), so that the afterAborted() callback will not be called again
        Env.getCurrentGlobalTransactionMgr().getCallbackFactory().removeCallback(id);

        if (abortTxn) {
            // abort txn
            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(new LogBuilder(LogKey.LOAD_JOB, id)
                            .add("transaction_id", transactionId)
                            .add("msg", "begin to abort txn")
                            .build());
                }
                Env.getCurrentGlobalTransactionMgr().abortTransaction(dbId, transactionId, failMsg.getMsg());
            } catch (UserException e) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                        .add("transaction_id", transactionId)
                        .add("error_msg", "failed to abort txn when job is cancelled. " + e.getMessage())
                        .build());
            }
        }

        // cancel all running coordinators, so that the scheduler's worker thread will be released
        // 7. 取消所有运行中的协调器（Coordinator），释放调度器线程资源
        // 遍历前面收集到的所有 loadIds：
        // 通过 QeProcessorImpl 获取对应的 Coordinator 实例，如果不为空，则调用 coordinator.cancel(...) 发送取消指令，
        // 从而让底层正在向 BE 发送 RPC 或等待计算的调度器工作线程快速释放，避免资源泄漏。
        for (TUniqueId loadId : loadIds) {
            Coordinator coordinator = QeProcessorImpl.INSTANCE.getCoordinator(loadId);
            if (coordinator != null) {
                coordinator.cancel(new Status(TStatusCode.CANCELLED, failMsg.getMsg()));
            }
        }

        // change state
        state = JobState.CANCELLED;
    }

    protected void executeFinish() {
        progress = 100;
        finishTimestamp = System.currentTimeMillis();
        Env.getCurrentGlobalTransactionMgr().getCallbackFactory().removeCallback(id);
        state = JobState.FINISHED;
        // when load job finished, there is no need to hold the tasks which are the biggest memory consumers.
        idToTasks.clear();
    }

    protected boolean checkDataQuality() {
        Map<String, String> counters = loadingStatus.getCounters();
        if (!counters.containsKey(DPP_NORMAL_ALL) || !counters.containsKey(DPP_ABNORMAL_ALL)) {
            return true;
        }

        long normalNum = Long.parseLong(counters.get(DPP_NORMAL_ALL));
        long abnormalNum = Long.parseLong(counters.get(DPP_ABNORMAL_ALL));
        if (abnormalNum > (abnormalNum + normalNum) * getMaxFilterRatio()) {
            return false;
        }

        return true;
    }

    protected void logFinalOperation() {
        Env.getCurrentEnv().getEditLog().logEndLoadJob(
                new LoadJobFinalOperation(id, loadingStatus, progress, loadStartTimestamp, finishTimestamp,
                        state, failMsg));
    }

    public void unprotectReadEndOperation(LoadJobFinalOperation loadJobFinalOperation) {
        loadingStatus = loadJobFinalOperation.getLoadingStatus();
        progress = loadJobFinalOperation.getProgress();
        loadStartTimestamp = loadJobFinalOperation.getLoadStartTimestamp();
        finishTimestamp = loadJobFinalOperation.getFinishTimestamp();
        state = loadJobFinalOperation.getJobState();
        failMsg = loadJobFinalOperation.getFailMsg();
    }

    public List<TUniqueId> getLoadTaskIds() {
        readLock();
        try {
            List<TUniqueId> res = Lists.newArrayList();
            for (LoadTask task : idToTasks.values()) {
                if (task instanceof LoadLoadingTask) {
                    LoadLoadingTask loadLoadingTask = (LoadLoadingTask) task;
                    res.add(loadLoadingTask.getLoadId());
                }
            }
            return res;
        } finally {
            readUnlock();
        }
    }

    public List<Comparable> getShowInfo() throws DdlException {
        readLock();
        try {
            return getShowInfoUnderLock();
        } finally {
            readUnlock();
        }
    }

    protected List<Comparable> getShowInfoUnderLock() throws DdlException {
        // check auth
        List<Comparable> jobInfo = Lists.newArrayList();
        // jobId
        jobInfo.add(id);
        // label
        jobInfo.add(label);
        // state
        jobInfo.add(state.name());

        // progress
        // check null
        String progress = Env.getCurrentProgressManager()
                .getProgressInfo(String.valueOf(id), state == JobState.FINISHED);
        switch (state) {
            case PENDING:
                jobInfo.add("0%");
                break;
            case CANCELLED:
                jobInfo.add(progress);
                break;
            case ETL:
                jobInfo.add(progress);
                break;
            default:
                jobInfo.add(progress);
                break;
        }

        // type
        jobInfo.add(jobType);

        // etl info
        if (loadingStatus.getCounters().size() == 0) {
            jobInfo.add(FeConstants.null_string);
        } else {
            jobInfo.add(Joiner.on("; ").withKeyValueSeparator("=").join(loadingStatus.getCounters()));
        }

        // task info
        jobInfo.add("cluster:" + getResourceName() + "; timeout(s):" + getTimeout()
                + "; max_filter_ratio:" + getMaxFilterRatio());
        // error msg
        if (failMsg == null) {
            jobInfo.add(FeConstants.null_string);
        } else {
            jobInfo.add("type:" + failMsg.getCancelType() + "; msg:" + failMsg.getMsg());
        }

        // create time
        jobInfo.add(TimeUtils.longToTimeString(createTimestamp));
        // etl start time
        jobInfo.add(TimeUtils.longToTimeString(getEtlStartTimestamp()));
        // etl end time
        jobInfo.add(TimeUtils.longToTimeString(loadStartTimestamp));
        // load start time
        jobInfo.add(TimeUtils.longToTimeString(loadStartTimestamp));
        // load end time
        jobInfo.add(TimeUtils.longToTimeString(finishTimestamp));
        // tracking url
        jobInfo.add(loadingStatus.getTrackingUrl());
        jobInfo.add(getJobDetailsJson());
        // transaction id
        jobInfo.add(transactionId);
        // error tablets
        jobInfo.add(errorTabletsToJson());
        // user
        if (userInfo == null || userInfo.getQualifiedUser() == null) {
            jobInfo.add(FeConstants.null_string);
        } else {
            jobInfo.add(userInfo.getQualifiedUser());
        }
        // comment
        jobInfo.add(comment);
        // first error message
        jobInfo.add(StringUtils.abbreviate(loadingStatus.getFirstErrorMsg(), Config.first_error_msg_max_length));
        return jobInfo;
    }

    public String errorTabletsToJson() {
        Map<Long, String> map = Maps.newHashMap();
        errorTabletInfos.stream().limit(Config.max_error_tablet_of_broker_load)
            .forEach(p -> map.put(p.getTabletId(), p.getMsg()));
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(map);
    }

    public String getResourceName() {
        return "N/A";
    }

    protected String getJobDetailsJson() {
        return loadStatistic.toJson();
    }

    protected long getEtlStartTimestamp() {
        return loadStartTimestamp;
    }

    public static LoadJob read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), LoadJob.class);
    }

    @Override
    public long getCallbackId() {
        return id;
    }

    @Override
    public void beforeCommitted(TransactionState txnState) throws TransactionException {
        writeLock();
        try {
            if (isTxnDone()) {
                throw new TransactionException("txn could not be committed because job is: " + state);
            }
            isCommitting = true;
        } finally {
            writeUnlock();
        }
    }

    @Override
    public void afterCommitted(TransactionState txnState, boolean txnOperated) throws UserException {
        if (txnOperated) {
            return;
        }
        writeLock();
        try {
            isCommitting = false;
            state = JobState.COMMITTED;
        } finally {
            writeUnlock();
        }
    }

    @Override
    public void replayOnCommitted(TransactionState txnState) {
        writeLock();
        try {
            replayTxnAttachment(txnState);
            transactionId = txnState.getTransactionId();
            state = JobState.COMMITTED;
        } finally {
            writeUnlock();
        }
    }

    /**
     * This method will cancel job without edit log.
     * The job will be cancelled by replayOnAborted when journal replay
     *
     * @param txnState
     * @param txnOperated
     * @param txnStatusChangeReason
     * @throws UserException
     */
    @Override
    public void afterAborted(TransactionState txnState, boolean txnOperated, String txnStatusChangeReason)
            throws UserException {
        if (!txnOperated) {
            return;
        }
        writeLock();
        try {
            if (isTxnDone()) {
                return;
            }
            // record attachment in load job
            replayTxnAttachment(txnState);
            // cancel load job
            unprotectedExecuteCancel(new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL, txnStatusChangeReason), false);
        } finally {
            writeUnlock();
        }
    }

    /**
     * This method is used to replay the cancelled state of load job
     *
     * @param txnState
     */
    @Override
    public void replayOnAborted(TransactionState txnState) {
        writeLock();
        try {
            replayTxnAttachment(txnState);
            failMsg = new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL, txnState.getReason());
            finishTimestamp = txnState.getFinishTime();
            state = JobState.CANCELLED;
            Env.getCurrentGlobalTransactionMgr().getCallbackFactory().removeCallback(id);
        } finally {
            writeUnlock();
        }
    }

    /**
     * This method will finish the load job without edit log.
     * The job will be finished by replayOnVisible when txn journal replay
     *
     * @param txnState
     * @param txnOperated
     */
    @Override
    public void afterVisible(TransactionState txnState, boolean txnOperated) {
        if (!txnOperated) {
            return;
        }
        replayTxnAttachment(txnState);
        updateState(JobState.FINISHED);
        auditFinishedLoadJob();
    }

    @Override
    public void replayOnVisible(TransactionState txnState) {
        writeLock();
        try {
            replayTxnAttachment(txnState);
            progress = 100;
            finishTimestamp = txnState.getFinishTime();
            state = JobState.FINISHED;
            Env.getCurrentGlobalTransactionMgr().getCallbackFactory().removeCallback(id);
        } finally {
            writeUnlock();
        }
    }

    protected void replayTxnAttachment(TransactionState txnState) {
    }

    @Override
    public void onTaskFinished(TaskAttachment attachment) {
    }

    @Override
    public void onTaskFailed(long taskId, FailMsg failMsg) {
    }

    // This analyze will be invoked after the replay is finished.
    // The edit log of LoadJob saves the origin param which is not analyzed.
    // So, the re-analyze must be invoked between the replay is finished and LoadJobScheduler is started.
    // Only, the PENDING load job need to be analyzed.
    public void analyze() {
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        LoadJob other = (LoadJob) obj;

        return this.id == other.id
                && this.dbId == other.dbId
                && this.label.equals(other.label)
                && this.state.equals(other.state)
                && this.jobType.equals(other.jobType);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Override
    public void gsonPostProcess() throws IOException {
        Map<String, String> tmpProperties = Maps.newHashMap();
        for (Map.Entry<String, Object> entry : jobProperties.entrySet()) {
            tmpProperties.put(entry.getKey(), String.valueOf(entry.getValue()));
        }
        jobProperties = Maps.newHashMap();
        // init jobProperties
        try {
            setJobProperties(tmpProperties);
        } catch (Exception e) {
            // should not happen
            throw new IOException("failed to replay job property", e);
        }
    }

    public void replayUpdateStateInfo(LoadJobStateUpdateInfo info) {
        state = info.getState();
        transactionId = info.getTransactionId();
        loadStartTimestamp = info.getLoadStartTimestamp();
    }

    protected void auditFinishedLoadJob() {
    }

    public static class LoadJobStateUpdateInfo implements Writable {
        @SerializedName(value = "jobId")
        private long jobId;
        @SerializedName(value = "state")
        private JobState state;
        @SerializedName(value = "transactionId")
        private long transactionId;
        @SerializedName(value = "loadStartTimestamp")
        private long loadStartTimestamp;

        public LoadJobStateUpdateInfo(long jobId, JobState state, long transactionId, long loadStartTimestamp) {
            this.jobId = jobId;
            this.state = state;
            this.transactionId = transactionId;
            this.loadStartTimestamp = loadStartTimestamp;
        }

        public long getJobId() {
            return jobId;
        }

        public JobState getState() {
            return state;
        }

        public long getTransactionId() {
            return transactionId;
        }

        public long getLoadStartTimestamp() {
            return loadStartTimestamp;
        }

        @Override
        public String toString() {
            return GsonUtils.GSON.toJson(this);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            String json = GsonUtils.GSON.toJson(this);
            Text.writeString(out, json);
        }

        public static LoadJobStateUpdateInfo read(DataInput in) throws IOException {
            String json = Text.readString(in);
            return GsonUtils.GSON.fromJson(json, LoadJobStateUpdateInfo.class);
        }
    }

    // unit: second
    public long getTimeout() {
        return (long) jobProperties.get(LoadCommand.TIMEOUT_PROPERTY);
    }

    public void setTimeout(long timeout) {
        jobProperties.put(LoadCommand.TIMEOUT_PROPERTY, timeout);
    }

    protected long getExecMemLimit() {
        return (long) jobProperties.get(LoadCommand.EXEC_MEM_LIMIT);
    }

    public double getMaxFilterRatio() {
        return (double) jobProperties.get(LoadCommand.MAX_FILTER_RATIO_PROPERTY);
    }

    protected void setMaxFilterRatio(double maxFilterRatio) {
        jobProperties.put(LoadCommand.MAX_FILTER_RATIO_PROPERTY, maxFilterRatio);
    }

    protected boolean isStrictMode() {
        return (boolean) jobProperties.get(LoadCommand.STRICT_MODE);
    }

    protected boolean isPartialUpdate() {
        return (boolean) jobProperties.get(LoadCommand.PARTIAL_COLUMNS);
    }

    protected TPartialUpdateNewRowPolicy getPartialUpdateNewKeyPolicy() {
        return (TPartialUpdateNewRowPolicy) jobProperties.get(LoadCommand.PARTIAL_UPDATE_NEW_KEY_POLICY);
    }

    protected String getTimeZone() {
        return (String) jobProperties.get(LoadCommand.TIMEZONE);
    }

    public int getLoadParallelism() {
        if (jobProperties.get(LoadCommand.LOAD_PARALLELISM).getClass() == Integer.class) {
            return (int) jobProperties.get(LoadCommand.LOAD_PARALLELISM);
        } else {
            return ((Long) jobProperties.get(LoadCommand.LOAD_PARALLELISM)).intValue();
        }
    }

    public int getSendBatchParallelism() {
        if (jobProperties.get(LoadCommand.SEND_BATCH_PARALLELISM).getClass() == Integer.class) {
            return (int) jobProperties.get(LoadCommand.SEND_BATCH_PARALLELISM);
        } else {
            return ((Long) jobProperties.get(LoadCommand.SEND_BATCH_PARALLELISM)).intValue();
        }
    }

    public LoadTask.Priority getPriority() {
        return (LoadTask.Priority) jobProperties.get(LoadCommand.PRIORITY);
    }

    public boolean isSingleTabletLoadPerSink() {
        return (boolean) jobProperties.get(LoadCommand.LOAD_TO_SINGLE_TABLET);
    }

    // Return true if this job is finished for a long time
    public boolean isExpired(long currentTimeMs) {
        if (!isCompleted()) {
            return false;
        }
        long expireTime = Config.label_keep_max_second;
        if (jobType == EtlJobType.INSERT) {
            expireTime = Config.streaming_label_keep_max_second;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Job ID: {}, DB ID: {}, Label: {}, State: {}, Expire Time: {}, Current Time: {}, "
                      + "Finish Timestamp: {}", id, dbId, label, state, expireTime, currentTimeMs,
                      getFinishTimestamp());
        }
        return (currentTimeMs - getFinishTimestamp()) / 1000 > expireTime;
    }

    public FailMsg getFailMsg() {
        return failMsg;
    }

    public EtlStatus getLoadingStatus() {
        return loadingStatus;
    }

    public LoadStatistic getLoadStatistic() {
        return loadStatistic;
    }

    public void settWorkloadGroups(List<TPipelineWorkloadGroup> tWorkloadGroups) {
        this.tWorkloadGroups = tWorkloadGroups;
    }
}
