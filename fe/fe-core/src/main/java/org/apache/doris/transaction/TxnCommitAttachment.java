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

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.load.routineload.RLTaskTxnCommitAttachment;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TTxnCommitAttachment;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
// 用于表示事务提交时的附加元数据（Transaction Commit Attachment）
// 当不同类型的导入任务（如 Routine Load、Stream Load 等）向 FE 提交事务时，除了常规的 Tablet 写入信息外，往往还需要携带一些特有的元数据（例如例行导入的消费进度、Kafka 偏移量等）。该类就是所有这类导入特有附加信息的统一抽象载体
// 多态承载导入特有元数据：通过抽象基类配合具体子类（如 RLTaskTxnCommitAttachment），为不同导入类型的事务提交提供个性化的附加信息支持。
// RPC 与内部协议转换：提供 fromThrift 方法，将网络传输层（Thrift 协议）的 TTxnCommitAttachment 转换为 Doris 内部的对象实例。
// 元数据持久化与反序列化：实现 Writable 接口，支持将对象序列化为 JSON 字符串写入 EditLog 或从输入流中反序列化恢复。
public abstract class TxnCommitAttachment implements Writable, GsonPostProcessable {
    // 加载作业的源类型枚举（LoadJobSourceType）。用于标识该附加信息具体属于哪种导入类型（例如 ROUTINE_LOAD 等）。
    @SerializedName(value = "sourceType")
    protected TransactionState.LoadJobSourceType sourceType;
    // 标记类型是否已被读取或处理的布尔状态位。在 Gson 反序列化后处理钩子中被置为 true。
    protected boolean isTypeRead = false;

    public TxnCommitAttachment(TransactionState.LoadJobSourceType sourceType) {
        this.sourceType = sourceType;
    }

    public void setTypeRead(boolean isTypeRead) {
        this.isTypeRead = isTypeRead;
    }

    public static TxnCommitAttachment fromThrift(TTxnCommitAttachment txnCommitAttachment) {
        if (txnCommitAttachment != null) {
            switch (txnCommitAttachment.getLoadType()) {
                case ROUTINE_LOAD:
                    return new RLTaskTxnCommitAttachment(txnCommitAttachment.getRlTaskTxnCommitAttachment());
                default:
                    return null;
            }
        } else {
            return null;
        }
    }

    public static TxnCommitAttachment read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), TxnCommitAttachment.class);
    }

    public void gsonPostProcess() {
        setTypeRead(true);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }
}
