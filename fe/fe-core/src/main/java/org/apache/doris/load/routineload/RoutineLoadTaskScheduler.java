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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.load.routineload.RoutineLoadJob.JobState;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TRoutineLoadTask;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Routine load task scheduler is a function which allocate task to be.
 * Step1: update backend slot if interval more than BACKEND_SLOT_UPDATE_INTERVAL_MS
 * Step2: submit beIdToBatchTask when queue is empty
 * Step3: take a task from queue and schedule this task
 *
 * The scheduler will be blocked in step3 till the queue receive a new task
 */
// 负责例行导入任务调度与分发的核心后台守护线程类
// 主要职责是：
// 异步任务调度循环：从待调度任务队列（needScheduleTasksQueue）中获取例行导入子任务（RoutineLoadTaskInfo）。
// BE 节点资源匹配与分配：检查集群中各 Backend（BE）节点的空闲槽位（Slot），为任务寻找最合适的 BE 节点。
// 事务开启与 Thrift RPC 下发：为任务开启导入事务，通过 Thrift 客户端（BackendService.Client）将任务实际提交到目标 BE 节点执行。
// 异常容错与流控：处理任务提交失败、队列阻塞、资源过载（如 TOO_MANY_TASKS 或 MEM_LIMIT_EXCEEDED）以及 EOF 延迟调度，确保流式导入的高效稳定与高可用。
public class RoutineLoadTaskScheduler extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(RoutineLoadTaskScheduler.class);
    // BE 节点槽位信息的定期刷新时间间隔（10 秒）。
    private static final long BACKEND_SLOT_UPDATE_INTERVAL_MS = 10000; // 10s
    // 当集群没有空闲槽位时，调度线程暂停休眠的时间（10 秒），防止 CPU 空转。
    private static final long SLOT_FULL_SLEEP_MS = 10000; // 10s
    // 关联的例行导入管理器实例，用于获取作业元数据、查询 BE 槽位及状态更新。
    private RoutineLoadManager routineLoadManager;
    // 等待调度的任务阻塞双端队列。存放所有待分派的 RoutineLoadTaskInfo 任务。
    private LinkedBlockingDeque<RoutineLoadTaskInfo> needScheduleTasksQueue = new LinkedBlockingDeque<>();
    // 记录上一次刷新 BE 节点最大并发槽位的时间戳。
    private long lastBackendSlotUpdateTime = -1;
    // 调用父类 MasterDaemon，设置线程名称 "Routine load task scheduler"，轮询间隔为 0ms，并从当前全局环境获取 RoutineLoadManager。
    @VisibleForTesting
    public RoutineLoadTaskScheduler() {
        super("Routine load task scheduler", 0);
        this.routineLoadManager = Env.getCurrentEnv().getRoutineLoadManager();
    }

    public RoutineLoadTaskScheduler(RoutineLoadManager routineLoadManager) {
        //Set the polling interval to 1ms to avoid meaningless idling when there is no data, resulting in increased CPU.
        // The wait/notify mechanism should be used later
        super("Routine load task scheduler", 1);
        this.routineLoadManager = routineLoadManager;
    }
    // 当 FE 的 Catalog 准备就绪后，该方法被后台守护线程周期性调用。
    @Override
    protected void runAfterCatalogReady() {
        try {
            process();
        } catch (Throwable e) {
            LOG.warn("Failed to process one round of RoutineLoadTaskScheduler", e);
        }
    }
    // 单轮调度核心逻辑控制方法。
    private void process() throws UserException, InterruptedException {
        // update the max slot num of each backend periodically
        // 周期性刷新 BE 最大并发槽位
        updateBackendSlotIfNecessary();

        // if size of queue is zero, tasks will be submitted by batch
        // 获取集群当前的空闲槽位总数 (idleSlotNum)。若等于 0，则线程休眠 SLOT_FULL_SLEEP_MS（10秒）后返回。
        int idleSlotNum = routineLoadManager.getClusterIdleSlotNum();
        // scheduler will be blocked when there is no slot for task in cluster
        if (idleSlotNum == 0) {
            Thread.sleep(SLOT_FULL_SLEEP_MS);
            return;
        }

        try {
            // This step will be blocked when queue is empty
            // 阻塞式取出一个待调度任务。
            RoutineLoadTaskInfo routineLoadTaskInfo = needScheduleTasksQueue.take();
            // try to delay scheduling tasks that are perceived as Eof to MaxBatchInterval
            // to avoid to much small transaction
            // 判断该任务是否需要进行 EOF 延迟调度（needDelaySchedule()）。若时间未到批次间隔（maxBatchIntervalS），则将其重新放回队列尾部（addLast），避免产生过多的小事务。
            if (routineLoadTaskInfo.needDedalySchedule()) {
                RoutineLoadJob routineLoadJob = routineLoadManager.getJob(routineLoadTaskInfo.getJobId());
                if (System.currentTimeMillis() - routineLoadTaskInfo.getLastScheduledTime()
                        < routineLoadJob.getMaxBatchIntervalS() * 1000) {
                    needScheduleTasksQueue.addLast(routineLoadTaskInfo);
                    return;
                }
            }
            // 正式调度该任务。
            scheduleOneTask(routineLoadTaskInfo);
        } catch (Exception e) {
            LOG.warn("Taking routine load task from queue has been interrupted", e);
        }
    }
    // 调度单个具体任务的全流程执行方法。
    private void scheduleOneTask(RoutineLoadTaskInfo routineLoadTaskInfo) throws Exception {
        // 更新任务的最后调度时间。
        routineLoadTaskInfo.setLastScheduledTime(System.currentTimeMillis());
        if (LOG.isDebugEnabled()) {
            LOG.debug("schedule routine load task info {} for job {}",
                    routineLoadTaskInfo.id, routineLoadTaskInfo.getJobId());
        }
        // check if task has been abandoned
        // 检查任务是否已被放弃或所属数据库/表已被删除（checkTaskInJob），若是则放弃调度。
        if (!routineLoadManager.checkTaskInJob(routineLoadTaskInfo)) {
            // task has been abandoned while renew task has been added in queue
            // or database has been deleted
            LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_TASK, routineLoadTaskInfo.getId())
                             .add("error_msg", "task has been abandoned when scheduling task")
                             .build());
            return;
        }

        try {
            // 检查所属 Job 是否已进入终态（isFinal()），若是则直接返回。
            if (routineLoadManager.getJob(routineLoadTaskInfo.getJobId()).isFinal()) {
                return;
            }
            // check if topic has more data to consume
            // 检查 Kafka Topic 是否还有更多数据可消费（hasMoreDataToConsume()），无数据则延后调度。
            if (!routineLoadTaskInfo.hasMoreDataToConsume()) {
                needScheduleTasksQueue.addLast(routineLoadTaskInfo);
                return;
            }

            // allocate BE slot for this task.
            // this should be done before txn begin, or the txn may be begun successfully but failed to be allocated.
            // 为任务分配 BE 节点。分配失败则重新放回队列尾部等待。
            if (!allocateTaskToBe(routineLoadTaskInfo)) {
                // allocate failed, push it back to the queue to wait next scheduling
                needScheduleTasksQueue.addLast(routineLoadTaskInfo);
                return;
            }
        } catch (UserException e) {
            routineLoadManager.getJob(routineLoadTaskInfo.getJobId())
                    .updateState(JobState.PAUSED, new ErrorReason(e.getErrorCode(), e.getMessage()), false);
            throw e;
        } catch (Exception e) {
            // exception happens, PAUSE the job
            routineLoadManager.getJob(routineLoadTaskInfo.getJobId()).updateState(JobState.PAUSED,
                    new ErrorReason(InternalErrorCode.CREATE_TASKS_ERR,
                            "failed to allocate task: " + e.getMessage()), false);
            LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_TASK, routineLoadTaskInfo.getId()).add("error_msg",
                    "allocate task encounter exception: " + e.getMessage()).build(), e);
            throw e;
        }

        // update adaptive timeout before beginTxn to ensure transaction timeout matches task timeout
        RoutineLoadJob routineLoadJob = routineLoadManager.getJob(routineLoadTaskInfo.getJobId());
        routineLoadTaskInfo.updateAdaptiveTimeout(routineLoadJob);

        // begin txn
        try {
            // 开启导入事务
            if (!routineLoadTaskInfo.beginTxn()) {
                // begin txn failed. push it back to the queue to wait next scheduling
                // set BE id to -1 to release the BE slot
                routineLoadTaskInfo.setBeId(-1);
                needScheduleTasksQueue.addFirst(routineLoadTaskInfo);
                return;
            }
        } catch (Exception e) {
            // exception happens, PAUSE the job
            // set BE id to -1 to release the BE slot
            routineLoadTaskInfo.setBeId(-1);
            routineLoadManager.getJob(routineLoadTaskInfo.getJobId()).updateState(JobState.PAUSED,
                    new ErrorReason(InternalErrorCode.CREATE_TASKS_ERR,
                            "failed to begin txn: " + e.getMessage()), false);
            LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_TASK, routineLoadTaskInfo.getId()).add("error_msg",
                    "begin task txn encounter exception: " + e.getMessage()).build(), e);
            throw e;
        }

        // create thrift object
        // 生成 Thrift 结构体（TRoutineLoadTask）。若元数据找不到则设为 CANCELLED，其他异常设为 PAUSED。
        TRoutineLoadTask tRoutineLoadTask = null;
        try {
            long startTime = System.currentTimeMillis();
            tRoutineLoadTask = routineLoadTaskInfo.createRoutineLoadTask();
            if (DebugPointUtil.isEnable("FE.RoutineLoadTaskScheduler.createRoutineLoadTask.exception")) {
                throw new RuntimeException("debug point: createRoutineLoadTask.exception");
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("create routine load task cost(ms): {}, job id: {}",
                        (System.currentTimeMillis() - startTime), routineLoadTaskInfo.getJobId());
            }
        } catch (MetaNotFoundException e) {
            // this means database or table has been dropped, just stop this routine load job.
            // set BE id to -1 to release the BE slot
            routineLoadTaskInfo.setBeId(-1);
            routineLoadManager.getJob(routineLoadTaskInfo.getJobId())
                    .updateState(JobState.CANCELLED,
                            new ErrorReason(InternalErrorCode.META_NOT_FOUND_ERR, "meta not found: " + e.getMessage()),
                            false);
            throw e;
        } catch (Exception e) {
            // set BE id to -1 to release the BE slot
            routineLoadTaskInfo.setBeId(-1);
            routineLoadManager.getJob(routineLoadTaskInfo.getJobId())
                    .updateState(JobState.PAUSED,
                            new ErrorReason(InternalErrorCode.CREATE_TASKS_ERR,
                                    "failed to create task: " + e.getMessage()), false);
            throw e;
        }
        // 将 Thrift 任务发送给目标 BE。若发送失败调用 handleSubmitTaskFailure 处理。
        try {
            long startTime = System.currentTimeMillis();
            submitTask(routineLoadTaskInfo.getBeId(), tRoutineLoadTask);
            if (LOG.isDebugEnabled()) {
                LOG.debug("send routine load task cost(ms): {}, job id: {}",
                        (System.currentTimeMillis() - startTime), routineLoadTaskInfo.getJobId());
            }
            if (tRoutineLoadTask.isSetKafkaLoadInfo()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("send kafka routine load task {} with partition offset: {}, job: {}",
                            tRoutineLoadTask.label, tRoutineLoadTask.kafka_load_info.partition_begin_offset,
                            tRoutineLoadTask.getJobId());
                }
            }
        } catch (LoadException e) {
            handleSubmitTaskFailure(routineLoadTaskInfo, e.getMessage());
            return;
        }

        // set the executeStartTimeMs of task
        routineLoadTaskInfo.setExecuteStartTimeMs(System.currentTimeMillis());
    }

    private void handleSubmitTaskFailure(RoutineLoadTaskInfo routineLoadTaskInfo, String errorMsg) {
        LOG.warn("failed to submit routine load task {} to BE: {}, error: {}",
                DebugUtil.printId(routineLoadTaskInfo.getId()),
                routineLoadTaskInfo.getBeId(), errorMsg);
        routineLoadTaskInfo.setBeId(-1);
        RoutineLoadJob routineLoadJob = routineLoadManager.getJob(routineLoadTaskInfo.getJobId());
        RoutineLoadTaskInfo newTask;

        routineLoadJob.writeLock();
        try {
            routineLoadJob.setOtherMsg(errorMsg);

            // Check if this is a resource pressure error that should not be immediately rescheduled
            if (errorMsg.contains("TOO_MANY_TASKS") || errorMsg.contains("MEM_LIMIT_EXCEEDED")) {
                // submit task failed (such as TOO_MANY_TASKS/MEM_LIMIT_EXCEEDED error),
                // but txn has already begun. Here we will still set the ExecuteStartTime of
                // this task, which means we "assume" that this task has been successfully submitted.
                // And this task will then be aborted because of a timeout.
                // In this way, we can prevent the entire job from being paused due to submit errors,
                // and we can also relieve the pressure on BE by waiting for the timeout period.
                routineLoadTaskInfo.setExecuteStartTimeMs(System.currentTimeMillis());
                return;
            }

            if (routineLoadJob.getState() != JobState.RUNNING
                    || !routineLoadJob.containsTask(routineLoadTaskInfo.getId())) {
                return;
            }

            // for other errors (network issues, BE restart, etc.), reschedule immediately
            newTask = routineLoadJob.unprotectRenewTask(routineLoadTaskInfo, false);
        } finally {
            routineLoadJob.writeUnlock();
        }
        addTaskInQueue(newTask);
    }

    private void updateBackendSlotIfNecessary() {
        long currentTime = System.currentTimeMillis();
        if (lastBackendSlotUpdateTime == -1
                || (currentTime - lastBackendSlotUpdateTime > BACKEND_SLOT_UPDATE_INTERVAL_MS)) {
            routineLoadManager.updateBeIdToMaxConcurrentTasks();
            lastBackendSlotUpdateTime = currentTime;
            if (LOG.isDebugEnabled()) {
                LOG.debug("update backend max slot for routine load task scheduling. current task num per BE: {}",
                        Config.max_routine_load_task_num_per_be);
            }
        }
    }

    public void addTaskInQueue(RoutineLoadTaskInfo routineLoadTaskInfo) {
        needScheduleTasksQueue.add(routineLoadTaskInfo);
        if (LOG.isDebugEnabled()) {
            LOG.debug("total tasks num in routine load task queue: {}", needScheduleTasksQueue.size());
        }
    }

    public void addTasksInQueue(List<RoutineLoadTaskInfo> routineLoadTaskInfoList) {
        needScheduleTasksQueue.addAll(routineLoadTaskInfoList);
        if (LOG.isDebugEnabled()) {
            LOG.debug("total tasks num in routine load task queue: {}", needScheduleTasksQueue.size());
        }
    }
    // 负责将 Thrift 任务通过 RPC 发送给指定的 BE。如果发送失败或发生异常，会抛出 LoadException。
    private void submitTask(long beId, TRoutineLoadTask tTask) throws LoadException {
        Backend backend = Env.getCurrentSystemInfo().getBackend(beId);
        if (backend == null) {
            throw new LoadException("failed to send tasks to backend " + beId + " because not exist");
        }

        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBePort());

        boolean ok = false;
        BackendService.Client client = null;
        try {
            client = ClientPool.backendPool.borrowObject(address);
            TStatus tStatus = client.submitRoutineLoadTask(Lists.newArrayList(tTask));
            ok = true;

            if (DebugPointUtil.isEnable("FE.ROUTINE_LOAD_TASK_SUBMIT_FAILED")) {
                LOG.warn("debug point FE.ROUTINE_LOAD_TASK_SUBMIT_FAILED, routine load task submit failed");
                throw new LoadException("debug point FE.ROUTINE_LOAD_TASK_SUBMIT_FAILED");
            }

            if (DebugPointUtil.isEnable("FE.ROUTINE_LOAD_TASK_SUBMIT_FAILED.MEM_LIMIT_EXCEEDED")) {
                LOG.warn("debug point FE.ROUTINE_LOAD_TASK_SUBMIT_FAILED.MEM_LIMIT_EXCEEDED,"
                        + "routine load task submit failed");
                throw new LoadException("MEM_LIMIT_EXCEEDED");
            }

            if (DebugPointUtil.isEnable("FE.ROUTINE_LOAD_TASK_SUBMIT_FAILED.TOO_MANY_TASKS")) {
                LOG.warn("debug point FE.ROUTINE_LOAD_TASK_SUBMIT_FAILED.TOO_MANY_TASKS,"
                        + "routine load task submit failed");
                tStatus = new TStatus(TStatusCode.TOO_MANY_TASKS);
                tStatus.addToErrorMsgs("debug point: too many tasks");
            }

            if (tStatus.getStatusCode() != TStatusCode.OK) {
                throw new LoadException("failed to submit task. error code: " + tStatus.getStatusCode()
                        + ", msg: " + (tStatus.getErrorMsgsSize() > 0 ? tStatus.getErrorMsgs().get(0) : "NaN"));
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("send routine load task {} to BE: {}", DebugUtil.printId(tTask.id), beId);
            }
        } catch (Exception e) {
            throw new LoadException("failed to send task: " + e.getMessage(), e);
        } finally {
            if (ok) {
                ClientPool.backendPool.returnObject(address, client);
            } else {
                ClientPool.backendPool.invalidateObject(address, client);
            }
        }
    }

    // try to allocate a task to BE which has idle slot.
    // 1. First is to check if the previous allocated BE has more than half of available slots.
    //    If yes, allocate task to previous BE.
    // 2. If not, try to find a better one with most idle slots.
    // return true if allocate successfully. return false if failed.
    // throw exception if unrecoverable errors happen.
    private boolean allocateTaskToBe(RoutineLoadTaskInfo routineLoadTaskInfo) throws UserException {
        long beId = routineLoadManager.getAvailableBeForTask(routineLoadTaskInfo.getJobId(),
                routineLoadTaskInfo.getPreviousBeId());
        if (beId == -1L) {
            return false;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_TASK, routineLoadTaskInfo.getId())
                    .add("job_id", routineLoadTaskInfo.getJobId())
                    .add("previous_be_id", routineLoadTaskInfo.getPreviousBeId())
                    .add("assigned_be_id", beId)
                    .build());
        }
        routineLoadTaskInfo.setBeId(beId);
        return true;
    }
}
