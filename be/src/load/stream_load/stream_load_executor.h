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

#pragma once

#include <gen_cpp/PaloInternalService_types.h>

#include <functional>
#include <memory>

#include "common/factory_creator.h"

namespace doris {

class ExecEnv;
class StreamLoadContext;
class Status;
class TTxnCommitAttachment;
class TLoadTxnCommitRequest;
// StreamLoadExecutor 是 Stream Load 导入任务的核心执行与事务管理类。
// StreamLoadExecutor 充当了 Stream Load 导入流程中 “协调器” 与 “事务/执行状态管理器” 的角色：
// 事务生命周期管理（Txn Management）：
// 通过 Thrift RPC 接口向 Frontend (FE) 交互，控制导入事务的开启 (begin)、预提交 (pre_commit，用于两阶段提交 2PC)、提交 (commit)、回滚 (rollback) 以及两阶段提交的具体操作（如 abort / commit 阶段）。
// 导入执行计划调度（Plan Execution）：
// 负责拉起并执行 FE 下发的 Fragment 执行计划（Pipeline Execution engine），异步/同步调度 Stream Load 数据流的写入。
// 导入数据统计与汇报（Metrics Collection）：
// 收集导入过程中的关键统计信息（如成功行数、过滤/异常行数、处理字节数、Loaded Bytes 等），在提交事务时将这些 Attachment 附件数据汇报给 FE，用于写元数据及在 HTTP Response 中呈现给用户。
class StreamLoadExecutor {
    ENABLE_FACTORY_CREATOR(StreamLoadExecutor);

public:
    StreamLoadExecutor(ExecEnv* exec_env) : _exec_env(exec_env) {}

    virtual ~StreamLoadExecutor() = default;
    // 向 Master FE 申请开启一个新的 Stream Load 事务。
    // 输出结果：若成功，FE 会返回一个全局唯一的 txn_id（事务 ID），此方法将其记录回 ctx->txn_id 中，后续的所有数据写入都将绑定此 txn_id。
    Status begin_txn(StreamLoadContext* ctx);
    // 执行两阶段提交（2PC）的第一阶段——预提交（Pre-Commit） 事务。
    virtual Status pre_commit_txn(StreamLoadContext* ctx);
    // 手动控制两阶段提交（2PC）事务的后续操作（二次提交或取消）
    // 当用户通过 HTTP 发起显式的 2PC 提交/取消请求（例如通过 /api/{db}/{txn_id}/_commit 或 _abort）时触发。
    virtual Status operate_txn_2pc(StreamLoadContext* ctx);
    // 执行普通（非 2PC）Stream Load 事务的正式提交。
    virtual Status commit_txn(StreamLoadContext* ctx);
    // 组装提交事务所需的 Thrift 请求对象 TLoadTxnCommitRequest。
    void get_commit_request(StreamLoadContext* ctx, TLoadTxnCommitRequest& request);
    // 回滚/取消（Rollback/Abort） 指定的事务。
    virtual void rollback_txn(StreamLoadContext* ctx);
    // 同步/常规模式拉起并执行 Stream Load 的执行计划 Fragment。
    Status execute_plan_fragment(std::shared_ptr<StreamLoadContext> ctx,
                                 const TPipelineFragmentParamsList& parent);
    // 带完成回调（Callback）的异步执行模式拉起并执行 Stream Load 执行计划。
    Status execute_plan_fragment(
            std::shared_ptr<StreamLoadContext> ctx, const TPipelineFragmentParamsList& parent,
            const std::function<void(std::shared_ptr<StreamLoadContext> ctx)>& cb);

protected:
    // collect the load statistics from context and set them to stat
    // return true if stat is set, otherwise, return false
    // 从 StreamLoadContext 中提取并汇总导入统计数据，组装为 Thrift 的事务提交附件 (TTxnCommitAttachment)。
    bool collect_load_stat(StreamLoadContext* ctx, TTxnCommitAttachment* attachment);
    // 指向 Doris BE 全局执行环境 (ExecEnv) 的单例指针。
    ExecEnv* _exec_env = nullptr;
};

} // namespace doris
