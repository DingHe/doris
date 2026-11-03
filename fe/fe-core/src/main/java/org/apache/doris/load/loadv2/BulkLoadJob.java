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
import org.apache.doris.analysis.DataDescription;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.AuthorizationInfo;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.BrokerFileGroupAggInfo;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.FailMsg;
import org.apache.doris.nereids.load.NereidsDataDescription;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.LoadCommand;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.plugin.audit.LoadAuditEvent;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectContextUtil;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TransactionState;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * parent class of BrokerLoadJob and SparkLoadJob from load stmt
 */
// 专门用于管理通过批量导入语句（如 Broker Load、Spark Load 等）创建的导入作业。
// 它在基类通用生命周期管理的基础上，扩展了对文件组聚合信息（BrokerFileGroupAggInfo）、Broker 描述配置、原始 SQL 语句（OriginStatement）、会话变量持久化、任务失败重试机制以及 Nereids 解析器的支持。
// 主要承担以下职责：
// 统一批量导入抽象：作为 Broker Load 等批量导入作业的共同父类，封装了数据源解析、文件路径规范化和多文件组聚合逻辑。
// SQL 原文持久化与重放分析：持久化保存原始 SQL 语句（OriginStatement），在系统重启或 FE 主备切换日志重放时能够重新解析和还原作业上下文。
// 会话变量捕获：在创建作业时捕获并持久化关键会话变量（如 sql_mode、profile_level 等），确保异步执行和日志重放时环境一致性。
// 智能失败重试：实现 onTaskFailed 接口，在子任务失败时根据剩余重试次数和超时状态，自动将任务重新提交给调度器进行重试。
// 审计日志集成：集成 Doris 的审计系统，在导入成功后自动生成并上报 LoadAuditEvent 审计事件。
public abstract class BulkLoadJob extends LoadJob implements GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(BulkLoadJob.class);

    // input params
    // 保存 Broker 的连接配置、凭证和访问属性（如 S3/BOS/OSS 的认证信息）。
    @SerializedName(value = "bd")
    protected BrokerDesc brokerDesc;
    // this param is used to persist the expr of columns
    // the origin stmt is persisted instead of columns expr
    // the expr of columns will be reanalyzed when the log is replayed
    // 记录提交该作业时的原始 SQL 语句。
    // Doris 持久化原始语句而非解析后的列表达式，以便日志重放时重新分析。
    @SerializedName(value = "os")
    private OriginStatement originStmt;

    // include broker desc and data desc
    // 管理聚合后的文件组信息，包含 Broker 描述与多个目标表的数据描述（Data Description）映射。
    protected BrokerFileGroupAggInfo fileGroupAggInfo = new BrokerFileGroupAggInfo();
    // 记录导入事务提交时的 Tablet 提交反馈信息。
    protected List<TabletCommitInfo> commitInfos = Lists.newArrayList();

    // sessionVariable's name -> sessionVariable's value
    // we persist these sessionVariables due to the session is not available when replaying the job.
    // 会话变量映射表
    @SerializedName(value = "svs")
    protected Map<String, String> sessionVariables = Maps.newHashMap();

    public BulkLoadJob(EtlJobType jobType) {
        super(jobType);
    }

    public BulkLoadJob(EtlJobType jobType, long dbId, String label,
            OriginStatement originStmt, UserIdentity userInfo) throws MetaNotFoundException {
        super(jobType, dbId, label);
        this.originStmt = originStmt;
        this.authorizationInfo = gatherAuthInfo();
        this.userInfo = userInfo;

        if (ConnectContext.get() != null) {
            SessionVariable var = ConnectContext.get().getSessionVariable();
            sessionVariables.put(SessionVariable.SQL_MODE, Long.toString(var.getSqlMode()));
            sessionVariables.put(SessionVariable.AUTO_PROFILE_THRESHOLD_MS,
                                    Long.toString(var.getAutoProfileThresholdMs()));
            sessionVariables.put(SessionVariable.PROFILE_LEVEL, Long.toString(var.getProfileLevel()));
        } else {
            sessionVariables.put(SessionVariable.SQL_MODE, String.valueOf(SqlModeHelper.MODE_DEFAULT));
            sessionVariables.put(SessionVariable.AUTO_PROFILE_THRESHOLD_MS, Long.toString(-1));
            sessionVariables.put(SessionVariable.PROFILE_LEVEL, Long.toString(1));
        }
    }
    // 主要作用是将用户提交并经过语法解析后的导入命令（LoadCommand）转化为具体的批量导入作业对象（如 BrokerLoadJob），并完成作业的初始化、属性设置、数据源校验及权限构建。
    // LoadCommand command 前端 SQL 解析器（基于 Nereids 或旧版解析器）解析导入 SQL 后生成的命令对象（AST 抽象语法树节点）。
    // 它包含了用户在 SQL 中指定的完整信息，如导入标签（Label）、Broker 描述、数据描述（Data Descriptions）、属性配置（Properties）以及备注（Comment）等。
    // StmtExecutor executor 当前 SQL 语句的执行器（Statement Executor）。在这里主要通过它获取提交该语句时的原始 SQL 语句对象
    public static BulkLoadJob fromLoadCommand(LoadCommand command, StmtExecutor executor, ConnectContext ctx)
            throws Exception {
        // get db id
        // 1. 获取目标数据库名称
        String dbName = command.getLabel().getDb();
        // 2. 根据数据库名称从内建目录中获取 Database 对象，若不存在则抛出 DDL 异常
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbName);

        // create job
        BulkLoadJob bulkLoadJob;
        try {
            switch (command.getEtlJobType()) {
            // 4. 根据命令中指定的 ETL 作业类型（EtlJobType）进行分支处理
                case BROKER:
                    // 5. 如果是 Broker 导入，通过环境变量工厂实例化一个 BrokerLoadJob 对象
                    // 传入：数据库 ID、Label 名称、Broker 配置、原始 SQL 语句、用户身份信息
                    bulkLoadJob = EnvFactory.getInstance().createBrokerLoadJob(db.getId(),
                            command.getLabel().getLabel(), command.getBrokerDesc(), executor.getOriginStmt(),
                            command.getUserIdentity());
                    break;
                case DELETE:
                case INSERT:
                    throw new DdlException("LoadManager only support create broker load job from stmt.");
                default:
                    throw new DdlException("Unknown load job type.");
            }
            // 8. 将 SQL 中用户指定的备注（Comment）设置到作业对象中
            bulkLoadJob.setComment(command.getComment());
            // 9. 解析并设置用户在 SQL 的 PROPERTIES 子句中指定的属性（如超时时间、最大过滤比例、内存限制等）
            bulkLoadJob.setJobProperties(command.getProperties());
            // 10. 使用 Nereids 解析器相关逻辑校验并设置数据源信息（检查文件合并类型、解析文件组、填充文件组聚合信息）
            bulkLoadJob.checkAndSetDataSourceInfoByNereids(db, command.getDataDescriptions(), ctx);
            // In the construction method, there may not be table information yet
            // 11. 重建鉴权信息（AuthorizationInfo）
            bulkLoadJob.rebuildAuthorizationInfo();
            return bulkLoadJob;
        } catch (MetaNotFoundException e) {
            throw new DdlException(e.getMessage());
        }
    }
    // 主要作用是在获取数据库读锁的安全环境下，遍历并校验用户提交的所有数据描述（Data Descriptions），
    // 确保它们的合并类型一致，将其转换为底层框架可识别的格式，并最终注册到作业的文件组聚合信息中。
    public void checkAndSetDataSourceInfoByNereids(Database db, List<NereidsDataDescription> dataDescriptions,
                                                       ConnectContext ctx) throws Exception {
        // check data source info
        db.readLock();
        try {
            LoadTask.MergeType mergeType = null;
            for (NereidsDataDescription dataDescription : dataDescriptions) {
                // 4. 记录基准合并类型
                // 如果是第一个数据描述，将其 MergeType 记为基准值。
                if (mergeType == null) {
                    mergeType = dataDescription.getMergeType();
                }
                // 5. 校验合并类型的一致性
                // Doris 要求在同一个导入作业中，所有数据源子句的合并类型必须完全一致。
                // 如果发现不一致（例如一部分是 APPEND，另一部分是 MERGE），则抛出 DDL 异常拒绝执行。
                if (mergeType != dataDescription.getMergeType()) {
                    throw new DdlException("merge type in all statement must be the same.");
                }

                DataDescription legacyDataDesc = dataDescription.toDataDescription(ctx);
                BrokerFileGroup brokerFileGroup = new BrokerFileGroup(legacyDataDesc);
                brokerFileGroup.parse(db, legacyDataDesc);
                // 9. 添加到文件组聚合信息中
                // 将解析校验通过的 brokerFileGroup 注册到作业的 fileGroupAggInfo（文件组聚合管理器）中，
                // 供后续任务调度和分片切分使用。
                fileGroupAggInfo.addFileGroup(brokerFileGroup);
            }
        } finally {
            db.readUnlock();
        }
    }

    public void checkAndSetDataSourceInfo(Database db, List<DataDescription> dataDescriptions) throws DdlException {
        // check data source info
        db.readLock();
        try {
            LoadTask.MergeType mergeType = null;
            for (DataDescription dataDescription : dataDescriptions) {
                if (mergeType == null) {
                    mergeType = dataDescription.getMergeType();
                }
                if (mergeType != dataDescription.getMergeType()) {
                    throw new DdlException("merge type in all statement must be the same.");
                }
                BrokerFileGroup fileGroup = new BrokerFileGroup(dataDescription);
                fileGroup.parse(db, dataDescription);
                fileGroupAggInfo.addFileGroup(fileGroup);
            }
        } finally {
            db.readUnlock();
        }
    }

    private AuthorizationInfo gatherAuthInfo() throws MetaNotFoundException {
        Database database = Env.getCurrentInternalCatalog().getDbOrMetaException(dbId);
        return new AuthorizationInfo(database.getFullName(), getTableNames());
    }

    public void rebuildAuthorizationInfo() throws MetaNotFoundException {
        this.authorizationInfo = gatherAuthInfo();
    }

    @Override
    public Set<String> getTableNamesForShow() {
        Optional<Database> db = Env.getCurrentInternalCatalog().getDb(dbId);
        return fileGroupAggInfo.getAllTableIds().stream()
                .map(tableId -> db.flatMap(d -> d.getTable(tableId)).map(TableIf::getName)
                        .orElse(String.valueOf(tableId)))
                .collect(Collectors.toSet());
    }

    @Override
    public Set<String> getTableNames() throws MetaNotFoundException {
        try {
            Set<String> result = Sets.newHashSet();
            Database database = Env.getCurrentInternalCatalog().getDbOrMetaException(dbId);
            // The database will not be locked in here.
            // The getTable is a thread-safe method called without read lock of database
            for (long tableId : fileGroupAggInfo.getAllTableIds()) {
                Table table = database.getTableOrMetaException(tableId);
                result.add(table.getName());
            }
            return result;
        } catch (Exception e) {
            LOG.warn(e);
            throw e;
        }
    }

    @Override
    public void onTaskFailed(long taskId, FailMsg failMsg) {
        List<LoadTask> retriedTasks = Lists.newArrayList();
        writeLock();
        try {
            // check if job has been completed
            if (isTxnDone()) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                        .add("state", state)
                        .add("error_msg", "this task will be ignored when job is: " + state)
                        .build());
                return;
            }
            LoadTask loadTask = idToTasks.get(taskId);
            if (loadTask == null) {
                return;
            }
            Predicate<LoadTask> isTaskTimeout =
                    (LoadTask task) -> task instanceof LoadLoadingTask
                            && ((LoadLoadingTask) task).getLeftTimeMs() <= 0;
            if (loadTask.getRetryTime() <= 0 || isTaskTimeout.test(loadTask)) {
                unprotectedExecuteCancel(failMsg, true);
                logFinalOperation();
                return;
            } else {
                // retry task
                idToTasks.remove(loadTask.getSignature());
                if (loadTask instanceof LoadLoadingTask) {
                    loadStatistic.removeLoad(((LoadLoadingTask) loadTask).getLoadId());
                }
                loadTask.updateRetryInfo();
                idToTasks.put(loadTask.getSignature(), loadTask);
                // load id will be added to loadStatistic when executing this task
                retriedTasks.add(loadTask);
            }
        } finally {
            writeUnlock();
        }

        // submit retried loading task outside the job's lock, cause task submitting may be block for a while
        for (LoadTask loadTask : retriedTasks) {
            try {
                if (loadTask.getTaskType() == LoadTask.TaskType.PENDING) {
                    Env.getCurrentEnv().getPendingLoadTaskScheduler().submit(loadTask);
                } else if (loadTask.getTaskType() == LoadTask.TaskType.LOADING) {
                    Env.getCurrentEnv().getLoadingLoadTaskScheduler().submit(loadTask);
                }
            } catch (RejectedExecutionException e) {
                writeLock();
                try {
                    unprotectedExecuteCancel(failMsg, true);
                    logFinalOperation();
                    return;
                } finally {
                    writeUnlock();
                }
            }
        }
    }

    /**
     * If the db or table could not be found, the Broker load job will be cancelled.
     */
    // 常关键的容灾与恢复方法
    // 当系统重启、FE 主备切换或进行日志重放（Journal Replay）时，内存中的导入作业需要被重新加载和初始化。
    // 该方法的作用就是重新解析和校验导入作业的原始 SQL 语句（originStmt），以恢复作业的数据源和元数据状态。
    // 如果在此期间发现目标数据库或表已被删除等异常，作业将被自动取消。
    @Override
    public void analyze() {
        // 1. 检查原始 SQL 语句是否存在或为空
        // 如果 originStmt 为空（说明可能是极老版本遗留或某些特殊未记录 SQL 的作业），则直接返回，不进行重新分析。
        if (originStmt == null || Strings.isNullOrEmpty(originStmt.originStmt)) {
            return;
        }
        // Reset dataSourceInfo, it will be re-created in analyze
        // 2. 重置数据源信息聚合对象
        // 在重新分析前清空旧的文件组信息，因为后续的 analyzeCommand 会重新创建和填充它们。
        fileGroupAggInfo = new BrokerFileGroupAggInfo();
        // 3. 实例化 Nereids 解析器
        // 创建一个 NereidsParser 对象，用于将原始 SQL 文本字符串重新解析为抽象语法树（AST）命令对象。
        NereidsParser nereidsParser = new NereidsParser();
        ConnectContext ctx = ConnectContext.get();
        try {
            // 5. 获取目标数据库对象
            // 根据保存的 dbId，从 Doris 的内建目录中获取 Database 对象。如果数据库已被删除，会抛出异常。
            Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbId);
            if (ctx == null) {
                ctx = ConnectContextUtil.getDummyCtx(db.getName());
            }
            // 7. 重新解析 SQL 语句
            // 调用 NereidsParser 将原汁原味的 SQL 字符串（originStmt.originStmt）解析并强转为 LoadCommand 对象。
            LoadCommand command = (LoadCommand) nereidsParser.parseSingle(originStmt.originStmt);
            // 8. 执行具体的命令分析与数据源设置
            // 调用内部的 analyzeCommand 方法，对解析出来的 command 进行语义分析、路径规范化，并重新绑定数据源。
            analyzeCommand(command, db, ctx);
        } catch (Exception e) {
            LOG.info(new LogBuilder(LogKey.LOAD_JOB, id)
                    .add("origin_stmt", originStmt)
                    .add("msg", "The failure happens in analyze, the load job will be cancelled with error:"
                            + e.getMessage())
                    .build(), e);
            cancelJobWithoutCheck(new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL, e.getMessage()), false, true);
        }
    }
    // 负责对解析出来的导入命令（LoadCommand）进行深度的语义分析、文件路径规范化（Scheme 转换），并最终完成数据源信息的填充与校验。
    protected void analyzeCommand(LoadCommand command, Database db, ConnectContext ctx) throws Exception {
        // 1. 防御性空值检查
        // 如果传入的命令、数据库或上下文对象任意一个为 null，则直接返回，不执行后续逻辑。
        if (command == null || db == null || ctx == null) {
            return;
        }
        // 2. 遍历并分析每一个数据描述（DataDescription）
        // 导入命令中可能包含多组数据源文件（例如：FROM files (...) INTO TABLE ...）。
        for (NereidsDataDescription dataDescription : command.getDataDescriptions()) {
            dataDescription.analyzeWithoutCheckPriv(db.getFullName());
        }
        // This replay path re-parses originStmt and skips LoadCommand.run(), which is where file
        // paths are normalized on fresh submission. Concrete filesystems only accept their native
        // schemes, so re-apply the same normalization (e.g. cos:// with s3.* properties -> s3://)
        // before the paths land in BrokerFileGroup, or a pending job rescheduled after an FE
        // restart / master failover would fail scheme validation and be cancelled.
        // 3. 路径规范化处理（源码自带的重要注释背景）
        // 背景说明：
        // 正常首次提交导入任务时，文件路径的规范化（比如将特定云存储的协议头做映射转换）是在 LoadCommand.run() 中完成的。
        // 但是，如果是 FE 重启恢复或主备切换（Replay 路径），系统会跳过 LoadCommand.run()，直接重新解析 originStmt。
        // 由于具体的文件系统（如 S3/COS 等底层客户端）只接受其原生识别的协议格式（Schema），
        // 如果不重新应用路径规范化（例如：将带有 s3 配置属性的 cos:// 路径转换为原生支持的 s3:// 路径），
        // 那么在 FE 重启后重新调度的作业在进行路径校验时就会报错并被取消。

        // 获取当前命令中的 Broker 描述信息
        BrokerDesc brokerDesc = command.getBrokerDesc();
        if (brokerDesc != null && !brokerDesc.isMultiLoadBroker()) {
            for (NereidsDataDescription dataDescription : command.getDataDescriptions()) {
                List<String> filePaths = dataDescription.getFilePaths();
                for (int i = 0; i < filePaths.size(); i++) {
                    filePaths.set(i, brokerDesc.getFileLocation(filePaths.get(i)));
                }
            }
        }
        // 4. 校验并设置数据源信息
        // 调用 BulkLoadJob 自身的 checkAndSetDataSourceInfoByNereids 方法：
        // 在数据库读锁保护下，进一步校验合并类型（MergeType），将规范化后的 Nereids 数据描述转换为底层的文件组聚合信息（BrokerFileGroupAggInfo）
        checkAndSetDataSourceInfoByNereids(db, command.getDataDescriptions(), ctx);
    }

    @Override
    protected void replayTxnAttachment(TransactionState txnState) {
        if (txnState.getTxnCommitAttachment() == null) {
            // The txn attachment maybe null when broker load has been cancelled without attachment.
            // The end log of broker load has been record but the callback id of txnState hasn't been removed
            // So the callback of txn is executed when log of txn aborted is replayed.
            return;
        }
        unprotectReadEndOperation((LoadJobFinalOperation) txnState.getTxnCommitAttachment());
    }

    public OriginStatement getOriginStmt() {
        return this.originStmt;
    }

    @Override
    public void gsonPostProcess() throws IOException {
        super.gsonPostProcess();
    }

    public UserIdentity getUserInfo() {
        return userInfo;
    }

    public void recycleProgress() {
        // Recycle memory occupied by Progress.
        Env.getCurrentProgressManager().removeProgress(String.valueOf(id));
    }

    @Override
    protected void auditFinishedLoadJob() {
        try {
            String dbName = getDb().getFullName();
            String tableListName = StringUtils.join(getTableNames(), ",");
            List<String> filePathList = Lists.newArrayList();
            for (List<BrokerFileGroup> brokerFileGroups : fileGroupAggInfo.getAggKeyToFileGroups().values()) {
                for (BrokerFileGroup brokerFileGroup : brokerFileGroups) {
                    filePathList.add("(" + StringUtils.join(brokerFileGroup.getFilePaths(), ",") + ")");
                }
            }
            String filePathListName = StringUtils.join(filePathList, ",");
            String brokerUserName = getBrokerUserName();
            AuditEvent auditEvent = new LoadAuditEvent.AuditEventBuilder()
                    .setEventType(AuditEvent.EventType.LOAD_SUCCEED)
                    .setJobId(id).setLabel(label).setLoadType(jobType.name()).setDb(dbName).setTableList(tableListName)
                    .setFilePathList(filePathListName).setBrokerUser(brokerUserName).setTimestamp(createTimestamp)
                    .setLoadStartTime(loadStartTimestamp).setLoadFinishTime(finishTimestamp)
                    .setScanRows(loadStatistic.getScannedRows()).setScanBytes(loadStatistic.totalFileSizeB)
                    .setFileNumber(loadStatistic.fileNum)
                    .build();
            Env.getCurrentEnv().getAuditEventProcessor().handleAuditEvent(auditEvent);
        } catch (Exception e) {
            LOG.warn("audit finished load job info failed", e);
        }
    }

    private String getBrokerUserName() {
        Map<String, String> properties = brokerDesc.getProperties();
        if (properties.containsKey("kerberos_principal")) {
            return properties.get("kerberos_principal");
        } else if (properties.containsKey("username")) {
            return properties.get("username");
        } else if (properties.containsKey("bos_accesskey")) {
            return properties.get("bos_accesskey");
        } else if (properties.containsKey("fs.s3a.access.key")) {
            return properties.get("fs.s3a.access.key");
        }
        return null;
    }
}
