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

import org.apache.doris.cloud.proto.Cloud.RLTaskTxnCommitAttachmentPB;
import org.apache.doris.common.Config;
import org.apache.doris.load.routineload.kafka.KafkaProgress;
import org.apache.doris.load.routineload.kinesis.KinesisProgress;
import org.apache.doris.thrift.TRLTaskTxnCommitAttachment;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TxnCommitAttachment;

import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;

// {"progress": "", "backendId": "", "taskSignature": "", "numOfErrorData": "",
// "numOfTotalData": "", "taskId": "", "jobId": ""}
// 当例行导入（Routine Load）的一个消费任务（Task）拉取并成功向后端（BE）写入一批数据、准备提交分布式事务时，
// BE 会将该任务的执行统计指标（如导入行数、接收字节数、错误信息等）以及最新的数据源消费进度（如 Kafka 的 Partition Offset 偏移量）通过该类打包并回传给前端（FE），
// 便 FE 更新作业状态、统计指标并推进消费进度。
// 例行导入指标上报：记录例行导入单次任务的执行统计数据（如成功行数、过滤行数、消耗时间、接收字节数等）。
// 消费进度同步（Offset 推进）：封装数据源的消费进度（支持 Kafka 或 Kinesis），供 FE 在事务提交成功后更新例行导入的消费位点。
public class RLTaskTxnCommitAttachment extends TxnCommitAttachment {
    // 例行导入作业（Routine Load Job）的唯一标识 ID。
    private long jobId;
    // 例行导入具体某个消费任务（Task）的唯一全局 ID（Thrift TUniqueId 结构）。
    private TUniqueId taskId;
    @SerializedName(value = "fr")
    // 被过滤掉的坏行数（例如数据格式不匹配、校验失败等被过滤的数据行数）。
    private long filteredRows;
    @SerializedName(value = "lr")
    // 成功加载并写入存储引擎的数据行数。
    private long loadedRows;
    @SerializedName(value = "ur")
    // 未被选中的行数（例如由于 SQL 的 WHERE 条件过滤而未参与导入的行数）。
    private long unselectedRows;
    // 当前任务接收到的原始数据总字节数（Bytes）。
    @SerializedName(value = "rb")
    private long receivedBytes;
    // 任务执行耗时（单位：毫秒）。
    @SerializedName(value = "tet")
    private long taskExecutionTimeMs;
    // 例行导入的消费进度对象。多态支持 Kafka 进度（KafkaProgress）或 Kinesis 进度（KinesisProgress），记录当前的 Partition 消费 Offset。
    @SerializedName(value = "pro")
    private RoutineLoadProgress progress;
    private String errorLogUrl;
    private String firstErrorMsg;

    public RLTaskTxnCommitAttachment() {
        super(TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK);
    }

    public RLTaskTxnCommitAttachment(TRLTaskTxnCommitAttachment rlTaskTxnCommitAttachment) {
        super(TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK);
        this.jobId = rlTaskTxnCommitAttachment.getJobId();
        this.taskId = rlTaskTxnCommitAttachment.getId();
        this.filteredRows = rlTaskTxnCommitAttachment.getFilteredRows();
        this.loadedRows = rlTaskTxnCommitAttachment.getLoadedRows();
        this.unselectedRows = rlTaskTxnCommitAttachment.getUnselectedRows();
        this.receivedBytes = rlTaskTxnCommitAttachment.getReceivedBytes();
        this.taskExecutionTimeMs = rlTaskTxnCommitAttachment.getLoadCostMs();

        switch (rlTaskTxnCommitAttachment.getLoadSourceType()) {
            case KAFKA:
                this.progress = new KafkaProgress(rlTaskTxnCommitAttachment.getKafkaRLTaskProgress());
                break;
            case KINESIS:
                this.progress = new KinesisProgress(rlTaskTxnCommitAttachment.getKinesisRLTaskProgress());
                break;
            default:
                break;
        }

        if (rlTaskTxnCommitAttachment.isSetErrorLogUrl()) {
            this.errorLogUrl = rlTaskTxnCommitAttachment.getErrorLogUrl();
        }
        if (rlTaskTxnCommitAttachment.isSetFirstErrorMsg()) {
            this.firstErrorMsg = abbreviateFirstErrorMsg(rlTaskTxnCommitAttachment.getFirstErrorMsg());
        }
    }

    public RLTaskTxnCommitAttachment(RLTaskTxnCommitAttachmentPB rlTaskTxnCommitAttachment) {
        super(TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK);
        this.jobId = rlTaskTxnCommitAttachment.getJobId();
        this.taskId = new TUniqueId(rlTaskTxnCommitAttachment.getTaskId().getHi(),
                                    rlTaskTxnCommitAttachment.getTaskId().getLo());
        this.filteredRows = rlTaskTxnCommitAttachment.getFilteredRows();
        this.loadedRows = rlTaskTxnCommitAttachment.getLoadedRows();
        this.unselectedRows = rlTaskTxnCommitAttachment.getUnselectedRows();
        this.receivedBytes = rlTaskTxnCommitAttachment.getReceivedBytes();
        this.taskExecutionTimeMs = rlTaskTxnCommitAttachment.getTaskExecutionTimeMs();

        KafkaProgress progress = new KafkaProgress(rlTaskTxnCommitAttachment.getProgress().getPartitionToOffset());

        this.progress = progress;
        this.errorLogUrl = rlTaskTxnCommitAttachment.getErrorLogUrl();
        this.firstErrorMsg = abbreviateFirstErrorMsg(rlTaskTxnCommitAttachment.getFirstErrorMsg());
    }

    private static String abbreviateFirstErrorMsg(String firstErrorMsg) {
        return StringUtils.abbreviate(firstErrorMsg, Config.first_error_msg_max_length);
    }

    public long getJobId() {
        return jobId;
    }

    public TUniqueId getTaskId() {
        return taskId;
    }

    public long getFilteredRows() {
        return filteredRows;
    }

    public long getLoadedRows() {
        return loadedRows;
    }

    public long getUnselectedRows() {
        return unselectedRows;
    }

    public long getTotalRows() {
        return filteredRows + loadedRows + unselectedRows;
    }

    public long getReceivedBytes() {
        return receivedBytes;
    }

    public long getTaskExecutionTimeMs() {
        return taskExecutionTimeMs;
    }

    public RoutineLoadProgress getProgress() {
        return progress;
    }

    public String getErrorLogUrl() {
        return errorLogUrl;
    }

    public String getFirstErrorMsg() {
        return firstErrorMsg;
    }

    @Override
    public String toString() {
        return "RLTaskTxnCommitAttachment [filteredRows=" + filteredRows
                + ", loadedRows=" + loadedRows
                + ", unselectedRows=" + unselectedRows
                + ", receivedBytes=" + receivedBytes
                + ", taskExecutionTimeMs=" + taskExecutionTimeMs
                + ", taskId=" + taskId
                + ", jobId=" + jobId
                + ", progress=" + progress.toString() + "]";
    }
}
