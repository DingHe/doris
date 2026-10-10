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

package org.apache.doris.load.routineload;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.routineload.kafka.KafkaRoutineLoadJob;
import org.apache.doris.load.routineload.kinesis.KinesisRoutineLoadJob;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.AlterRoutineLoadCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateRoutineLoadInfo;
import org.apache.doris.nereids.trees.plans.commands.load.PauseRoutineLoadCommand;
import org.apache.doris.nereids.trees.plans.commands.load.ResumeRoutineLoadCommand;
import org.apache.doris.nereids.trees.plans.commands.load.StopRoutineLoadCommand;
import org.apache.doris.persist.AlterRoutineLoadJobOperationLog;
import org.apache.doris.persist.RoutineLoadOperation;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.Tag;
import org.apache.doris.resource.computegroup.ComputeGroup;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BeSelectionPolicy;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;

// RoutineLoadManager 是 Apache Doris 中用于统一管理例行导入作业（Routine Load Job）的核心元数据管理与调度决策组件。其主要职责包括：
// 生命周期与元数据管理：创建、存储、查询、暂停（Pause）、恢复（Resume）、停止（Stop）、修改（Alter）以及清理（Clean）各类 Routine Load 作业（如 Kafka、Kinesis 数据源）。
// BE 资源与并发分配：维护全集群 Backend（BE）节点的并发任务上限与实时负载情况，为任务调度（Task Scheduling）寻找合适的 BE 节点，并实现一定程度的负载均衡与 Cache 复用。
// 元数据持久化与回放：实现 Writable 接口，负责将创建、修改、操作日志通过 EditLog 写入二进制 Metadata，并在 FE 节点重启或主备同步时回放（Replay）日志。
// 权限与安全隔离：与 Doris 访问控制（AccessManager）结合，在提交、暂停、恢复、修改作业时检查用户对目标数据库及表的操作权限（LOAD 权限）。
// 多表/多任务事务映射：记录 Multi-Load 事务 ID（Txn ID）与 Routine Load Job ID 的关联，支持事务级别的回调与追踪。
public class RoutineLoadManager implements Writable {
    private static final Logger LOG = LogManager.getLogger(RoutineLoadManager.class);

    // Long is beId, integer is the size of tasks in be
    // 映射表（Key: beId, Value: maxConcurrentTasks），记录每个可用 BE 节点当前允许运行的最大 Routine Load 并发任务数。默认为 Config.max_routine_load_task_num_per_be。
    private Map<Long, Integer> beIdToMaxConcurrentTasks = Maps.newHashMap();

    // routine load job meta
    // 以全局唯一 jobId 为 Key 的核心 ConcurrentMap，保存当前 FE 内存中所有的 Routine Load 作业对象。
    private Map<Long, RoutineLoadJob> idToRoutineLoadJob = Maps.newConcurrentMap();
    // 二级映射表（Outer Key: dbId, Inner Key: jobName, Value: 具有该名称的 RoutineLoadJob 列表）。
    // 由于历史作业可能同名，因此 Value 为 List；用于支持按 dbName + jobName 快速检索作业。
    private Map<Long, Map<String, List<RoutineLoadJob>>> dbToNameToRoutineLoadJob = Maps.newConcurrentMap();
    // 映射表（Key: txnId, Value: routineLoadJobId），用于关联 Multi-Load 任务中的事务 ID 与对应的例行导入作业 ID。
    private ConcurrentHashMap<Long, Long> multiLoadTaskTxnIdToRoutineLoadJobId = new ConcurrentHashMap<>();
    // 公平的读写重入锁，保证并发查询（读锁）与修改/修改状态（写锁）时元数据一致性，防止 EditLog 乱序产生空指针或元数据冲突
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    // Map<beId, timestamp when added to blacklist>
    // 并发映射表（Key: beId, Value: Timestamp），记录被加入黑名单的 BE 节点及其加入时间，防止频繁异常的 BE 被持续调度。
    private Map<Long, Long> blacklist = new ConcurrentHashMap<>();

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

    public RoutineLoadManager() {
    }

    public Map<Long, Long> getBlacklist() {
        return blacklist;
    }
    // 获取当前系统内所有的 RoutineLoadJob 对象列表（包含已停止/历史作业）。
    public List<RoutineLoadJob> getAllRoutineLoadJobs() {
        return new ArrayList<>(idToRoutineLoadJob.values());
    }
    // 过滤并返回当前所有未处于终止状态（非 FINAL_STATE，即除 STOPPED/CANCELLED 外）的活动作业列表。
    public List<RoutineLoadJob> getActiveRoutineLoadJobs() {
        return idToRoutineLoadJob.values().stream()
                .filter(job -> !job.state.isFinalState())
                .collect(Collectors.toList());
    }

    // 向 multiLoadTaskTxnIdToRoutineLoadJobId 写入映射关系，将某个事务 ID 绑定到其所属的 Job ID。
    public void addMultiLoadTaskTxnIdToRoutineLoadJobId(long txnId, long routineLoadJobId) {
        multiLoadTaskTxnIdToRoutineLoadJobId.put(txnId, routineLoadJobId);
    }

    // 根据事务 ID 反查并返回对应的 RoutineLoadJob 实例。如果查不到或为 0 则返回 null。
    public RoutineLoadJob getRoutineLoadJobByMultiLoadTaskTxnId(long txnId) {
        long routineLoadJobId = multiLoadTaskTxnIdToRoutineLoadJobId.get(txnId);
        if (routineLoadJobId == 0) {
            return null;
        }
        return idToRoutineLoadJob.get(routineLoadJobId);
    }
    // 移除指定的事务 ID 映射记录，释放内存。
    public void removeMultiLoadTaskTxnIdToRoutineLoadJobId(long txnId) {
        multiLoadTaskTxnIdToRoutineLoadJobId.remove(txnId);
    }
    // 刷新 beIdToMaxConcurrentTasks 映射。通过 SystemInfoService 筛选出处于存活、未退役（Decommission）且导入可用（isLoadAvailable）的 BE 节点，
    // 并设置其最大并发任务数为配置项 Config.max_routine_load_task_num_per_be。
    public void updateBeIdToMaxConcurrentTasks() {
        beIdToMaxConcurrentTasks = Env.getCurrentSystemInfo().getAllBackendIds(true).stream()
                .filter(beId -> {
                    Backend backend = Env.getCurrentSystemInfo().getBackend(beId);
                    return backend != null && backend.isLoadAvailable()
                            && !backend.isDecommissioned() && !backend.isDecommissioning();
                })
                .collect(Collectors.toMap(beId -> beId, beId -> Config.max_routine_load_task_num_per_be));
    }

    // this is not real-time number
    // 计算并返回当前全集群所有可用 BE 节点能够承载的最大并发 Routine Load 任务总数（即各个 BE 上限之和）。
    public int getTotalMaxConcurrentTaskNum() {
        return beIdToMaxConcurrentTasks.values().stream().mapToInt(i -> i).sum();
    }

    // return the map of be id -> running tasks num
    // 遍历处于 RUNNING 状态的所有 Routine Load 作业，汇总并返回每个 BE 上当前正在运行的任务数量 Map（Map<beId, currentRunningTasks>）
    private Map<Long, Integer> getBeCurrentTasksNumMap() {
        Map<Long, Integer> beCurrentTaskNumMap = Maps.newHashMap();
        for (RoutineLoadJob routineLoadJob : getRoutineLoadJobByState(
                Sets.newHashSet(RoutineLoadJob.JobState.RUNNING))) {
            Map<Long, Integer> jobBeCurrentTasksNumMap = routineLoadJob.getBeCurrentTasksNumMap();
            for (Map.Entry<Long, Integer> entry : jobBeCurrentTasksNumMap.entrySet()) {
                if (beCurrentTaskNumMap.containsKey(entry.getKey())) {
                    beCurrentTaskNumMap.put(entry.getKey(), beCurrentTaskNumMap.get(entry.getKey()) + entry.getValue());
                } else {
                    beCurrentTaskNumMap.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return beCurrentTaskNumMap;

    }
    // 创建 Routine Load 作业的公共入口。
    public void createRoutineLoadJob(CreateRoutineLoadInfo info, ConnectContext ctx)
            throws UserException {
        // check load auth
        // 校验当前连接用户对目标表的 LOAD 权限；
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ConnectContext.get(),
                InternalCatalog.INTERNAL_CATALOG_NAME,
                info.getDBName(),
                info.getTableName(),
                PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    info.getDBName(),
                    info.getDBName() + ": " + info.getTableName());
        }
        // 根据 info.getTypeName() 判断数据源（Kafka / Kinesis），调用对应类（如 KafkaRoutineLoadJob.fromCreateInfo）生成 Job 实例；
        RoutineLoadJob routineLoadJob = null;
        LoadDataSourceType type = LoadDataSourceType.valueOf(info.getTypeName());
        switch (type) {
            case KAFKA:
                routineLoadJob = KafkaRoutineLoadJob.fromCreateInfo(info, ctx);
                break;
            case KINESIS:
                routineLoadJob = KinesisRoutineLoadJob.fromCreateInfo(info, ctx);
                break;
            default:
                throw new UserException("Unknown data source type: " + type);
        }
        // 附加原始 SQL 语句和 Comment，最后调用 addRoutineLoadJob 注册作业。
        routineLoadJob.setOrigStmt(ctx.getStatementContext().getOriginStatement());
        routineLoadJob.setComment(info.getComment());
        addRoutineLoadJob(routineLoadJob, info.getDBName(),
                info.getTableName());
    }
    // 将生成的 Job 注册到系统中，并持久化元数据。
    public void addRoutineLoadJob(RoutineLoadJob routineLoadJob, String dbName, String tableName)
                    throws UserException {
        // 加写锁；
        writeLock();
        try {
            // check if db.routineLoadName has been used
            // 校验作业名称在同 DB 下是否已被未结束的作业使用（isNameUsed）；
            if (isNameUsed(routineLoadJob.getDbId(), routineLoadJob.getName())) {
                throw new DdlException("Name " + routineLoadJob.getName() + " already used in db "
                        + dbName);
            }
            // 校验处于活跃状态（NEED_SCHEDULE, RUNNING, PAUSED）的作业数是否超过 Config.max_routine_load_job_num 上限；
            if (getRoutineLoadJobByState(Sets.newHashSet(RoutineLoadJob.JobState.NEED_SCHEDULE,
                    RoutineLoadJob.JobState.RUNNING, RoutineLoadJob.JobState.PAUSED)).size()
                    > Config.max_routine_load_job_num) {
                throw new DdlException("There are more than " + Config.max_routine_load_job_num
                        + " routine load jobs are running. exceed limit.");
            }
            // 调用 unprotectedAddJob 添加至内存结构；
            unprotectedAddJob(routineLoadJob);
            Env.getCurrentEnv().getEditLog().logCreateRoutineLoadJob(routineLoadJob);
        } finally {
            writeUnlock();
        }

        LOG.info("create routine load job: id: {}, job name: {}, db name: {}, table name: {}",
                 routineLoadJob.getId(), routineLoadJob.getName(), dbName, tableName);
    }
    // 无锁地将 Job 放置到 idToRoutineLoadJob 和 dbToNameToRoutineLoadJob 内存 Map 中，并将作业注册到全局事务管理器的 Callback 工厂中。
    private void unprotectedAddJob(RoutineLoadJob routineLoadJob) {
        idToRoutineLoadJob.put(routineLoadJob.getId(), routineLoadJob);

        Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob = dbToNameToRoutineLoadJob
                .computeIfAbsent(routineLoadJob.getDbId(), k -> Maps.newConcurrentMap());
        List<RoutineLoadJob> routineLoadJobList = nameToRoutineLoadJob
                .computeIfAbsent(routineLoadJob.getName(), k -> Lists.newArrayList());
        routineLoadJobList.add(routineLoadJob);
        // add txn state callback in factory
        Env.getCurrentGlobalTransactionMgr().getCallbackFactory().addCallback(routineLoadJob);
    }
    // 判断指定 DB 下该作业名称是否已被其他非终态（!isFinalState()）的 Routine Load 作业占用。
    // TODO(ml): Idempotency
    private boolean isNameUsed(Long dbId, String name) {
        if (dbToNameToRoutineLoadJob.containsKey(dbId)) {
            Map<String, List<RoutineLoadJob>> labelToRoutineLoadJob = dbToNameToRoutineLoadJob.get(dbId);
            if (labelToRoutineLoadJob.containsKey(name)) {
                List<RoutineLoadJob> routineLoadJobList = labelToRoutineLoadJob.get(name);
                Optional<RoutineLoadJob> optional = routineLoadJobList.stream()
                        .filter(entity -> entity.getName().equals(name))
                        .filter(entity -> !entity.getState().isFinalState()).findFirst();
                return optional.isPresent();
            }
        }
        return false;
    }
    // 根据 dbName 和 jobName 检索 Job 并校验当前上下文用户的权限。支持单表与多表（isMultiTable）权限检查。
    public RoutineLoadJob checkPrivAndGetJob(String dbName, String jobName)
            throws MetaNotFoundException, DdlException, AnalysisException {
        RoutineLoadJob routineLoadJob = getJob(dbName, jobName);
        if (routineLoadJob == null) {
            throw new DdlException("There is not operable routine load job with name " + jobName);
        }
        // check auth
        String dbFullName;
        String tableName;
        try {
            dbFullName = routineLoadJob.getDbFullName();
            tableName = routineLoadJob.getTableName();
        } catch (MetaNotFoundException e) {
            throw new DdlException("The metadata of job has been changed. The job will be cancelled automatically", e);
        }
        if (routineLoadJob.isMultiTable()) {
            if (!Env.getCurrentEnv().getAccessManager().checkDbPriv(ConnectContext.get(),
                    InternalCatalog.INTERNAL_CATALOG_NAME,
                    dbFullName,
                    PrivPredicate.LOAD)) {
                // todo add new error code
                ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                        ConnectContext.get().getQualifiedUser(),
                        ConnectContext.get().getRemoteIP(),
                        dbFullName);
            }
            return routineLoadJob;
        }
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ConnectContext.get(),
                InternalCatalog.INTERNAL_CATALOG_NAME,
                dbFullName,
                tableName,
                PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    dbFullName + ": " + tableName);
        }
        return routineLoadJob;
    }

    // get all jobs which state is not in final state from specified database
    // 获取指定 DB 下所有非终态的 Job，并自动过滤掉当前用户没有 LOAD 权限的单表作业。
    public List<RoutineLoadJob> checkPrivAndGetAllJobs(String dbName)
            throws MetaNotFoundException, DdlException {

        List<RoutineLoadJob> result = Lists.newArrayList();
        Database database = Env.getCurrentInternalCatalog().getDbOrDdlException(dbName);
        long dbId = database.getId();
        Map<String, List<RoutineLoadJob>> jobMap = dbToNameToRoutineLoadJob.get(dbId);
        if (jobMap == null) {
            // return empty result
            return result;
        }

        for (List<RoutineLoadJob> jobs : jobMap.values()) {
            for (RoutineLoadJob job : jobs) {
                if (!job.getState().isFinalState()) {
                    String tableName = job.getTableName();
                    if (!job.isMultiTable() && !Env.getCurrentEnv().getAccessManager()
                            .checkTblPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, dbName,
                                    tableName, PrivPredicate.LOAD)) {
                        continue;
                    }
                    result.add(job);
                }
            }
        }

        return result;
    }
    // 表示用户发起的“暂停例行导入（Routine Load）作业”的命令对象。它封装了执行该操作所需的所有元数据和请求参数，主要包括：
    // 目标数据库名 (dbFullName)：指示要操作的作业属于哪一个 Database。
    // 作业名称 / Label (label)：指示要暂停的具体作业名称（支持单个作业操作）。
    // 是否批量操作 (isAll())：一个布尔标志，指示用户是希望暂停指定数据库下的所有活动例行导入作业，还是仅仅暂停某一个特定的作业。
    public void pauseRoutineLoadJob(PauseRoutineLoadCommand pauseRoutineLoadCommand)
            throws UserException {
        // 初始化一个空的 RoutineLoadJob 列表。用于存放接下来需要被暂停的一个或多个作业实例。
        List<RoutineLoadJob> jobs = Lists.newArrayList();
        // it needs lock when getting routine load job,
        // otherwise, it may cause the editLog out of order in the following scenarios:
        // thread A: create job and record job meta
        // thread B: change job state and persist in editlog according to meta
        // thread A: persist in editlog
        // which will cause the null pointer exception when replaying editLog
        // 注释指出了加读锁的原因——防止在多线程并发场景下出现 EditLog 乱序。
        // 例如，线程 A 正在创建作业并准备写日志，而线程 B 获取了元数据并修改状态先持久化了 EditLog，这会导致后续 FE 重放（Replay）元数据时发生空指针异常（NullPointerException）。
        readLock();
        try {
            // 判断用户是否执行的是批量暂停命令（PAUSE ROUTINE LOAD FOR ALL ...）。
            if (pauseRoutineLoadCommand.isAll()) {
                jobs = checkPrivAndGetAllJobs(pauseRoutineLoadCommand.getDbFullName());
            } else {
                RoutineLoadJob routineLoadJob = checkPrivAndGetJob(pauseRoutineLoadCommand.getDbFullName(),
                        pauseRoutineLoadCommand.getLabel());
                jobs.add(routineLoadJob);
            }
        } finally {
            readUnlock();
        }
        // 遍历前面收集到的需要暂停的作业列表 jobs，针对每个作业逐一执行暂停逻辑，并使用 try-catch 捕获单个作业暂停失败的异常，保证“批量暂停时部分失败不影响其他作业”。
        for (RoutineLoadJob routineLoadJob : jobs) {
            try {
                // 更新单个作业的状态为暂停（PAUSED）。
                // 参数 1：目标状态 JobState.PAUSED。
                // 参数 2：错误原因（ErrorReason），其中包含内部错误码 MANUAL_PAUSE_ERR 以及详细描述信息（记录是由哪个登录用户手动暂停的）。
                // 参数 3：false 表示这不是在重放元数据日志（isReplay = false），而是由当前节点实时发起的操作（会触发内部写入 EditLog 持久化及向集群广播状态变更）。
                routineLoadJob.updateState(RoutineLoadJob.JobState.PAUSED,
                    new ErrorReason(InternalErrorCode.MANUAL_PAUSE_ERR,
                        "User " + ConnectContext.get().getQualifiedUser() + " pauses routine load job"),
                        false /* not replay */);
                LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId()).add("current_state",
                        routineLoadJob.getState()).add("user", ConnectContext.get().getQualifiedUser()).add("msg",
                        "routine load job has been paused by user").build());
            } catch (UserException e) {
                LOG.warn("failed to pause routine load job {}", routineLoadJob.getName(), e);
                // if user want to pause a certain job and failed, return error.
                // if user want to pause all possible jobs, skip error jobs.
                if (!pauseRoutineLoadCommand.isAll()) {
                    throw e;
                }
            }
        }
    }

    public void resumeRoutineLoadJob(ResumeRoutineLoadCommand resumeRoutineLoadCommand)
            throws UserException {
        List<RoutineLoadJob> jobs = Lists.newArrayList();
        if (resumeRoutineLoadCommand.isAll()) {
            jobs = checkPrivAndGetAllJobs(resumeRoutineLoadCommand.getDbFullName());
        } else {
            RoutineLoadJob routineLoadJob = checkPrivAndGetJob(resumeRoutineLoadCommand.getDbFullName(),
                    resumeRoutineLoadCommand.getLabel());
            jobs.add(routineLoadJob);
        }

        for (RoutineLoadJob routineLoadJob : jobs) {
            try {
                routineLoadJob.jobStatistic.errorRowsAfterResumed = 0;
                routineLoadJob.autoResumeCount = 0;
                routineLoadJob.latestResumeTimestamp = 0;
                routineLoadJob.updateState(RoutineLoadJob.JobState.NEED_SCHEDULE, null, false /* not replay */);
                LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId())
                        .add("current_state", routineLoadJob.getState())
                        .add("user", ConnectContext.get().getQualifiedUser())
                        .add("msg", "routine load job has been resumed by user")
                        .build());
            } catch (UserException e) {
                LOG.warn("failed to resume routine load job {}", routineLoadJob.getName(), e);
                // if user want to resume a certain job and failed, return error.
                // if user want to resume all possible jobs, skip error jobs.
                if (!resumeRoutineLoadCommand.isAll()) {
                    throw e;
                }
            }
        }
    }

    public void stopRoutineLoadJob(StopRoutineLoadCommand stopRoutineLoadCommand)
            throws UserException {
        RoutineLoadJob routineLoadJob;
        // it needs lock when getting routine load job,
        // otherwise, it may cause the editLog out of order in the following scenarios:
        // thread A: create job and record job meta
        // thread B: change job state and persist in editlog according to meta
        // thread A: persist in editlog
        // which will cause the null pointer exception when replaying editLog
        readLock();
        try {
            routineLoadJob = checkPrivAndGetJob(stopRoutineLoadCommand.getDbFullName(),
                stopRoutineLoadCommand.getLabel());
        } finally {
            readUnlock();
        }
        routineLoadJob.updateState(RoutineLoadJob.JobState.STOPPED,
            new ErrorReason(InternalErrorCode.MANUAL_STOP_ERR,
                "User  " + ConnectContext.get().getQualifiedUser() + " stop routine load job"),
                false /* not replay */);
        LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId())
                .add("current_state", routineLoadJob.getState())
                .add("user", ConnectContext.get().getQualifiedUser())
                .add("msg", "routine load job has been stopped by user")
                .build());
    }

    public int getSizeOfIdToRoutineLoadTask() {
        int sizeOfTasks = 0;
        for (RoutineLoadJob routineLoadJob : idToRoutineLoadJob.values()) {
            sizeOfTasks += routineLoadJob.getSizeOfRoutineLoadTaskInfoList();
        }
        return sizeOfTasks;
    }
    // 基于 RoutineLoadManager 内部维护的 BE 节点最大并发任务限制表（beIdToMaxConcurrentTasks）以及当前正在运行的任务情况（通过 getBeCurrentTasksNumMap() 获取），
    // 全局计算并返回当前整个集群中所有后端（BE）节点上剩余的例行导入空闲槽位（Idle Slots）总数。
    public int getClusterIdleSlotNum() {
        readLock();
        try {
            int result = 0;
            Map<Long, Integer> beIdToConcurrentTasks = getBeCurrentTasksNumMap();
            for (Map.Entry<Long, Integer> entry : beIdToMaxConcurrentTasks.entrySet()) {
                if (beIdToConcurrentTasks.containsKey(entry.getKey())) {
                    result += entry.getValue() - beIdToConcurrentTasks.get(entry.getKey());
                } else {
                    result += entry.getValue();
                }
            }
            return result;
        } finally {
            readUnlock();
        }
    }

    // get the BE id with minimum running task on it
    // return -1 if no BE is available.
    // throw exception if unrecoverable errors happen.
    // ATTN: this is only used for unit test now.
    public long getMinTaskBeId(String clusterName) throws LoadException {
        List<Long> beIdsInCluster = Env.getCurrentSystemInfo().getAllBackendIds(true);
        if (beIdsInCluster == null) {
            throw new LoadException("The " + clusterName + " has been deleted");
        }

        readLock();
        try {
            long result = -1L;
            int maxIdleSlotNum = 0;
            updateBeIdToMaxConcurrentTasks();
            Map<Long, Integer> beIdToConcurrentTasks = getBeCurrentTasksNumMap();
            for (Long beId : beIdsInCluster) {
                if (beIdToMaxConcurrentTasks.containsKey(beId)) {
                    int idleTaskNum = 0;
                    if (beIdToConcurrentTasks.containsKey(beId)) {
                        idleTaskNum = beIdToMaxConcurrentTasks.get(beId) - beIdToConcurrentTasks.get(beId);
                    } else {
                        idleTaskNum = Config.max_routine_load_task_num_per_be;
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("be {} has idle {}, concurrent task {}, max concurrent task {}", beId, idleTaskNum,
                                beIdToConcurrentTasks.get(beId), beIdToMaxConcurrentTasks.get(beId));
                    }
                    result = maxIdleSlotNum < idleTaskNum ? beId : result;
                    maxIdleSlotNum = Math.max(maxIdleSlotNum, idleTaskNum);
                }
            }
            return result;
        } finally {
            readUnlock();
        }
    }

    // check if the specified BE is available for running task
    // return true if it is available. return false if otherwise.
    // throw exception if unrecoverable errors happen.
    public long getAvailableBeForTask(long jobId, long previousBeId) throws UserException {
        List<Long> availableBeIds = getAvailableBackendIds(jobId);
        if (availableBeIds.isEmpty()) {
            RoutineLoadJob job = getJob(jobId);
            if (job != null) {
                String msg = "no available BE found for job " + jobId + ", cluster Name {}, " + job.getCloudCluster()
                        + "please check the BE status and user's cluster or tags";
                job.updateState(RoutineLoadJob.JobState.PAUSED,
                        new ErrorReason(InternalErrorCode.INTERNAL_ERR, msg), false /* not replay */);
            }
            return -1L;
        }

        // check if be has idle slot
        readLock();
        try {
            updateBeIdToMaxConcurrentTasks();
            Map<Long, Integer> beIdToConcurrentTasks = getBeCurrentTasksNumMap();
            int previousBeIdleTaskNum = 0;
            boolean previousBeAvailable = false;

            // 1. Find if the given BE id has more than half of available slots
            if (previousBeId != -1L && availableBeIds.contains(previousBeId)) {
                // get the previousBackend info
                Backend previousBackend = Env.getCurrentSystemInfo().getBackend(previousBeId);
                // check previousBackend is not null && load available
                if (previousBackend != null && previousBackend.isLoadAvailable()) {
                    previousBeAvailable = true;
                    if (!beIdToMaxConcurrentTasks.containsKey(previousBeId)) {
                        previousBeIdleTaskNum = 0;
                    } else if (beIdToConcurrentTasks.containsKey(previousBeId)) {
                        previousBeIdleTaskNum = beIdToMaxConcurrentTasks.get(previousBeId)
                                - beIdToConcurrentTasks.get(previousBeId);
                    } else {
                        previousBeIdleTaskNum = beIdToMaxConcurrentTasks.get(previousBeId);
                    }
                    if (previousBeIdleTaskNum > 0
                            && previousBeIdleTaskNum == Config.max_routine_load_task_num_per_be) {
                        return previousBeId;
                    }
                }
            }

            // 2. we believe that the benefits of load balance outweigh the benefits of object pool cache,
            //    so we try to find the one with the most idle slots as much as possible
            // 3. The previous BE is not in cluster && is not load available, find a new BE with min tasks
            int idleTaskNum = 0;
            long resultBeId = -1L;
            int maxIdleSlotNum = 0;
            for (Long beId : availableBeIds) {
                if (beIdToMaxConcurrentTasks.containsKey(beId)) {
                    if (beIdToConcurrentTasks.containsKey(beId)) {
                        idleTaskNum = beIdToMaxConcurrentTasks.get(beId) - beIdToConcurrentTasks.get(beId);
                    } else {
                        idleTaskNum = Config.max_routine_load_task_num_per_be;
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("be {} has idle {}, concurrent task {}, max concurrent task {}", beId, idleTaskNum,
                                beIdToConcurrentTasks.get(beId), beIdToMaxConcurrentTasks.get(beId));
                    }
                    resultBeId = maxIdleSlotNum < idleTaskNum ? beId : resultBeId;
                    maxIdleSlotNum = Math.max(maxIdleSlotNum, idleTaskNum);
                }
            }
            // 4. on the basis of selecting the maximum idle slot be,
            //    try to reuse the object cache as much as possible
            if (previousBeAvailable && previousBeIdleTaskNum > 0 && previousBeIdleTaskNum == maxIdleSlotNum) {
                return previousBeId;
            }
            return resultBeId;
        } finally {
            readUnlock();
        }
    }

    // just for UT
    public List<Long> getAvailableBackendIdsForUt(long jobId) throws LoadException {
        return getAvailableBackendIds(jobId);
    }

    /**
     * The routine load task can only be scheduled on backends which has proper resource tags.
     * The tags should be got from user property.
     * But in the old version, the routine load job does not have user info, so for compatibility,
     * if there is no user info, we will get tags from replica allocation of the first partition of the table.
     *
     * @param jobId
     * @param cluster
     * @return
     * @throws LoadException
     */
    protected List<Long> getAvailableBackendIds(long jobId) throws LoadException {
        // Usually Cloud node could not reach here(refer CloudRoutineLoadManager.getAvailableBackendIds),
        // check cloud mode here is just to be on the safe side.
        if (Config.isCloudMode()) {
            throw new LoadException("cloud mode should not reach here");
        }

        RoutineLoadJob job = getJob(jobId);
        if (job == null) {
            throw new LoadException("job " + jobId + " does not exist");
        }
        Set<Tag> tags = null;
        ComputeGroup computeGroup = null;
        if (job.getUserIdentity() == null) {
            // For old job, there may be no user info. So we have to use tags from replica allocation
            tags = getTagsFromReplicaAllocation(job.getDbId(), job.getTableId());
            BeSelectionPolicy policy = new BeSelectionPolicy.Builder().addTags(tags).needLoadAvailable().build();
            return Env.getCurrentSystemInfo()
                    .selectBackendIdsByPolicy(policy, -1 /* as many as possible */);
        } else {
            computeGroup = Env.getCurrentEnv().getAuth().getComputeGroup(job.getUserIdentity().getQualifiedUser());
            if (ComputeGroup.INVALID_COMPUTE_GROUP.equals(computeGroup)) {
                // user may be dropped, or may not set resource tag property.
                // Here we fall back to use replica tag
                tags = getTagsFromReplicaAllocation(job.getDbId(), job.getTableId());
            }

            if (computeGroup != null && !ComputeGroup.INVALID_COMPUTE_GROUP.equals(computeGroup)) {
                BeSelectionPolicy policy = new BeSelectionPolicy.Builder().needLoadAvailable().build();
                return Env.getCurrentSystemInfo()
                        .selectBackendIdsByPolicy(policy, -1 /* as many as possible */,
                                computeGroup.getBackendList());
            } else {
                BeSelectionPolicy policy = new BeSelectionPolicy.Builder().addTags(tags).needLoadAvailable().build();
                return Env.getCurrentSystemInfo()
                        .selectBackendIdsByPolicy(policy, -1 /* as many as possible */);
            }
        }
    }

    private Set<Tag> getTagsFromReplicaAllocation(long dbId, long tblId) throws LoadException {
        try {
            Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(dbId);
            OlapTable tbl = (OlapTable) db.getTableOrMetaException(tblId, Table.TableType.OLAP);
            tbl.readLock();
            try {
                PartitionInfo partitionInfo = tbl.getPartitionInfo();
                for (Partition partition : tbl.getPartitions()) {
                    ReplicaAllocation replicaAlloc = partitionInfo.getReplicaAllocation(partition.getId());
                    // just use the first one
                    return replicaAlloc.getAllocMap().keySet();
                }
                // Should not run into here. Just make compiler happy.
                return Sets.newHashSet();
            } finally {
                tbl.readUnlock();
            }
        } catch (MetaNotFoundException e) {
            throw new LoadException(e.getMessage());
        }
    }

    public RoutineLoadJob getJob(long jobId) {
        return idToRoutineLoadJob.get(jobId);
    }

    public RoutineLoadJob getJob(String dbFullName, String jobName) throws MetaNotFoundException {
        List<RoutineLoadJob> routineLoadJobList = getJob(dbFullName, jobName, false, null);
        if (CollectionUtils.isEmpty(routineLoadJobList)) {
            return null;
        } else {
            return routineLoadJobList.get(0);
        }
    }

    /*
      if dbFullName is null, result = all routine load job in all db
      else if jobName is null, result =  all routine load job in dbFullName

      if includeHistory is false, filter not running job in result
      else return all of result
     */
    public List<RoutineLoadJob> getJob(String dbFullName, String jobName,
            boolean includeHistory, PatternMatcher matcher)
            throws MetaNotFoundException {
        Preconditions.checkArgument(jobName == null || matcher == null,
                "jobName and matcher cannot be not null at the same time");
        // return all of routine load job
        List<RoutineLoadJob> result;
        RESULT:
        { // CHECKSTYLE IGNORE THIS LINE
            if (dbFullName == null) {
                result = new ArrayList<>(idToRoutineLoadJob.values());
                sortRoutineLoadJob(result);
                break RESULT;
            }

            Database database = Env.getCurrentInternalCatalog().getDbOrMetaException(dbFullName);
            long dbId = database.getId();
            if (!dbToNameToRoutineLoadJob.containsKey(dbId)) {
                result = new ArrayList<>();
                break RESULT;
            }
            if (jobName == null) {
                result = Lists.newArrayList();
                for (List<RoutineLoadJob> nameToRoutineLoadJob : dbToNameToRoutineLoadJob.get(dbId).values()) {
                    List<RoutineLoadJob> routineLoadJobList = new ArrayList<>(nameToRoutineLoadJob);
                    sortRoutineLoadJob(routineLoadJobList);
                    result.addAll(routineLoadJobList);
                }
                break RESULT;
            }
            if (dbToNameToRoutineLoadJob.get(dbId).containsKey(jobName)) {
                result = new ArrayList<>(dbToNameToRoutineLoadJob.get(dbId).get(jobName));
                sortRoutineLoadJob(result);
                break RESULT;
            }
            return null;
        } // CHECKSTYLE IGNORE THIS LINE

        if (!includeHistory) {
            result = result.stream().filter(entity -> !entity.getState().isFinalState()).collect(Collectors.toList());
        }
        if (matcher != null) {
            result = result.stream().filter(entity -> matcher.match(entity.getName())).collect(Collectors.toList());
        }
        return result;
    }

    // return all routine load job named jobName in all of db
    public List<RoutineLoadJob> getJobByName(String jobName) {
        List<RoutineLoadJob> result = Lists.newArrayList();
        for (Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob : dbToNameToRoutineLoadJob.values()) {
            if (nameToRoutineLoadJob.containsKey(jobName)) {
                List<RoutineLoadJob> routineLoadJobList = new ArrayList<>(nameToRoutineLoadJob.get(jobName));
                sortRoutineLoadJob(routineLoadJobList);
                result.addAll(routineLoadJobList);
            }
        }
        return result;
    }

    // put history job in the end
    private void sortRoutineLoadJob(List<RoutineLoadJob> routineLoadJobList) {
        if (routineLoadJobList == null) {
            return;
        }
        int i = 0;
        int j = routineLoadJobList.size() - 1;
        while (i < j) {
            while (!routineLoadJobList.get(i).isFinal() && (i < j)) {
                i++;
            }
            while (routineLoadJobList.get(j).isFinal() && (i < j)) {
                j--;
            }
            if (i < j) {
                RoutineLoadJob routineLoadJob = routineLoadJobList.get(i);
                routineLoadJobList.set(i, routineLoadJobList.get(j));
                routineLoadJobList.set(j, routineLoadJob);
            }
        }
    }

    public boolean checkTaskInJob(RoutineLoadTaskInfo task) {
        RoutineLoadJob routineLoadJob = idToRoutineLoadJob.get(task.getJobId());
        if (routineLoadJob == null) {
            return false;
        }
        return routineLoadJob.containsTask(task.getId());
    }

    public List<RoutineLoadJob> getRoutineLoadJobByState(Set<RoutineLoadJob.JobState> desiredStates) {
        List<RoutineLoadJob> stateJobs = idToRoutineLoadJob.values().stream()
                .filter(entity -> desiredStates.contains(entity.getState())).collect(Collectors.toList());
        return stateJobs;
    }

    // RoutineLoadScheduler will run this method at fixed interval, and renew the timeout tasks
    public void processTimeoutTasks() {
        for (RoutineLoadJob routineLoadJob : idToRoutineLoadJob.values()) {
            routineLoadJob.processTimeoutTasks();
        }
    }

    // Remove old routine load jobs from idToRoutineLoadJob
    // This function is called periodically.
    // Cancelled and stopped job will be removed after Configure.label_keep_max_second seconds
    public void cleanOldRoutineLoadJobs() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("begin to clean old routine load jobs ");
        }
        clearRoutineLoadJobIf(RoutineLoadJob::isExpired);
    }

    /**
     * Remove finished routine load jobs from idToRoutineLoadJob
     * This function is called periodically if Config.label_num_threshold is set.
     * Cancelled and stopped job will be removed.
     */
    public void cleanOverLimitRoutineLoadJobs() {
        if (Config.label_num_threshold < 0
                || idToRoutineLoadJob.size() <= Config.label_num_threshold) {
            return;
        }
        writeLock();
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("begin to clean routine load jobs");
            }
            Deque<RoutineLoadJob> finishedJobs = idToRoutineLoadJob
                    .values()
                    .stream()
                    .filter(RoutineLoadJob::isFinal)
                    .sorted(Comparator.comparingLong(o -> o.endTimestamp))
                    .collect(Collectors.toCollection(ArrayDeque::new));
            while (!finishedJobs.isEmpty()
                    && idToRoutineLoadJob.size() > Config.label_num_threshold) {
                RoutineLoadJob routineLoadJob = finishedJobs.pollFirst();
                unprotectedRemoveJobFromDb(routineLoadJob);
                idToRoutineLoadJob.remove(routineLoadJob.getId());
                RoutineLoadOperation operation = new RoutineLoadOperation(routineLoadJob.getId(),
                        routineLoadJob.getState());
                Env.getCurrentEnv().getEditLog().logRemoveRoutineLoadJob(operation);
            }
        } finally {
            writeUnlock();
        }
    }

    private void clearRoutineLoadJobIf(Predicate<RoutineLoadJob> pred) {
        writeLock();
        try {
            Iterator<Map.Entry<Long, RoutineLoadJob>> iterator = idToRoutineLoadJob.entrySet().iterator();
            long currentTimestamp = System.currentTimeMillis();
            while (iterator.hasNext()) {
                RoutineLoadJob routineLoadJob = iterator.next().getValue();
                if (pred.test(routineLoadJob)) {
                    unprotectedRemoveJobFromDb(routineLoadJob);
                    iterator.remove();
                    RoutineLoadOperation operation = new RoutineLoadOperation(routineLoadJob.getId(),
                            routineLoadJob.getState());
                    Env.getCurrentEnv().getEditLog().logRemoveRoutineLoadJob(operation);
                    LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId())
                            .add("end_timestamp", routineLoadJob.getEndTimestamp())
                            .add("current_timestamp", currentTimestamp)
                            .add("job_state", routineLoadJob.getState())
                            .add("msg", "old job has been cleaned")
                    );
                }
            }
        } finally {
            writeUnlock();
        }
    }

    public void replayRemoveOldRoutineLoad(RoutineLoadOperation operation) {
        writeLock();
        try {
            RoutineLoadJob job = idToRoutineLoadJob.remove(operation.getId());
            if (job != null) {
                unprotectedRemoveJobFromDb(job);
            }
        } finally {
            writeUnlock();
        }
        LOG.info("replay remove routine load job: {}", operation.getId());
    }

    private void unprotectedRemoveJobFromDb(RoutineLoadJob routineLoadJob) {
        dbToNameToRoutineLoadJob.get(routineLoadJob.getDbId()).get(routineLoadJob.getName()).remove(routineLoadJob);
        if (dbToNameToRoutineLoadJob.get(routineLoadJob.getDbId()).get(routineLoadJob.getName()).isEmpty()) {
            dbToNameToRoutineLoadJob.get(routineLoadJob.getDbId()).remove(routineLoadJob.getName());
        }
        if (dbToNameToRoutineLoadJob.get(routineLoadJob.getDbId()).isEmpty()) {
            dbToNameToRoutineLoadJob.remove(routineLoadJob.getDbId());
        }
    }

    public void updateRoutineLoadJob() throws UserException {
        for (RoutineLoadJob routineLoadJob : idToRoutineLoadJob.values()) {
            if (!routineLoadJob.state.isFinalState()) {
                routineLoadJob.update();
            }
        }
    }

    public void updateRoutineLoadJobLag() {
        for (RoutineLoadJob routineLoadJob : idToRoutineLoadJob.values()) {
            if (!routineLoadJob.state.isFinalState()) {
                try {
                    routineLoadJob.updateLag();
                } catch (UserException e) {
                    LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId())
                            .add("msg", "failed to update routine load lag")
                            .build(), e);
                }
            }
        }
    }

    public void replayCreateRoutineLoadJob(RoutineLoadJob routineLoadJob) {
        unprotectedAddJob(routineLoadJob);
        LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId())
                .add("msg", "replay create routine load job")
                .build());
    }

    public void replayChangeRoutineLoadJob(RoutineLoadOperation operation) {
        RoutineLoadJob job = getJob(operation.getId());
        try {
            job.updateState(operation.getJobState(), operation.getErrorReason(), true /* is replay */);
        } catch (UserException e) {
            LOG.error("should not happened", e);
        } catch (NullPointerException npe) {
            LOG.error("cannot get job when replaying state change job, which is unexpected, job id: "
                    + operation.getId());
        }
        LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, operation.getId())
                .add("current_state", operation.getJobState())
                .add("msg", "replay change routine load job")
                .build());
    }

    /**
     * Enter of altering a routine load job
     */
    public void alterRoutineLoadJob(AlterRoutineLoadCommand command) throws UserException {
        RoutineLoadJob job;
        // it needs lock when getting routine load job,
        // otherwise, it may cause the editLog out of order in the following scenarios:
        // thread A: create job and record job meta
        // thread B: change job state and persist in editlog according to meta
        // thread A: persist in editlog
        // which will cause the null pointer exception when replaying editLog
        readLock();
        try {
            job = checkPrivAndGetJob(command.getDbName(), command.getJobName());
        } finally {
            readUnlock();
        }
        if (command.hasDataSourceProperty()
                && !command.getDataSourceProperties().getDataSourceType().equalsIgnoreCase(job.dataSourceType.name())) {
            throw new DdlException("The specified job type is not: "
                + command.getDataSourceProperties().getDataSourceType());
        }
        job.modifyProperties(command);
        job.setRoutineLoadDesc(command.getRoutineLoadDesc());
    }

    public void replayAlterRoutineLoadJob(AlterRoutineLoadJobOperationLog log) {
        RoutineLoadJob job = getJob(log.getJobId());
        Preconditions.checkNotNull(job, log.getJobId());
        job.replayModifyProperties(log);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(idToRoutineLoadJob.size());
        for (RoutineLoadJob routineLoadJob : idToRoutineLoadJob.values()) {
            routineLoadJob.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            RoutineLoadJob routineLoadJob = RoutineLoadJob.read(in);
            idToRoutineLoadJob.put(routineLoadJob.getId(), routineLoadJob);
            Map<String, List<RoutineLoadJob>> map = dbToNameToRoutineLoadJob.get(routineLoadJob.getDbId());
            if (map == null) {
                map = Maps.newConcurrentMap();
                dbToNameToRoutineLoadJob.put(routineLoadJob.getDbId(), map);
            }

            List<RoutineLoadJob> jobs = map.get(routineLoadJob.getName());
            if (jobs == null) {
                jobs = Lists.newArrayList();
                map.put(routineLoadJob.getName(), jobs);
            }
            jobs.add(routineLoadJob);
            if (!routineLoadJob.getState().isFinalState()) {
                Env.getCurrentGlobalTransactionMgr().getCallbackFactory().addCallback(routineLoadJob);
            }
            if (Config.isCloudMode()) {
                routineLoadJob.setCloudCluster();
            }
        }
    }

    public void addToBlacklist(long beId) {
        blacklist.put(beId, System.currentTimeMillis());
    }

    public boolean isInBlacklist(long beId) {
        Long timestamp = blacklist.get(beId);
        if (timestamp == null) {
            return false;
        }

        if (System.currentTimeMillis() - timestamp > Config.routine_load_blacklist_expire_time_second * 1000) {
            blacklist.remove(beId);
            LOG.info("remove beId {} from blacklist, blacklist: {}", beId, blacklist);
            return false;
        }
        return true;
    }
}
