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

#include "load/stream_load/stream_load_executor.h"

#include <bvar/bvar.h>
#include <bvar/latency_recorder.h>
#include <gen_cpp/FrontendService.h>
#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/HeartbeatService_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <stdint.h>

#include <functional>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/metrics/doris_metrics.h"
#include "common/status.h"
#include "common/utils.h"
#include "load/message_body_sink.h"
#include "load/stream_load/new_load_stream_mgr.h"
#include "load/stream_load/stream_load_context.h"
#include "runtime/cluster_info.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/runtime_state.h"
#include "thrift/protocol/TDebugProtocol.h"
#include "util/client_cache.h"
#include "util/debug_points.h"
#include "util/thrift_rpc_helper.h"
#include "util/time.h"
#include "util/uid_util.h"

namespace doris {
using namespace ErrorCode;

#ifdef BE_TEST
TLoadTxnBeginResult k_stream_load_begin_result;
TLoadTxnCommitResult k_stream_load_commit_result;
TLoadTxnRollbackResult k_stream_load_rollback_result;
Status k_stream_load_plan_status;
#endif

bvar::LatencyRecorder g_stream_load_begin_txn_latency("stream_load", "begin_txn");
bvar::LatencyRecorder g_stream_load_precommit_txn_latency("stream_load", "precommit_txn");
bvar::LatencyRecorder g_stream_load_commit_txn_latency("stream_load", "commit_txn");

Status StreamLoadExecutor::execute_plan_fragment(std::shared_ptr<StreamLoadContext> ctx,
                                                 const TPipelineFragmentParamsList& parent) {
    return execute_plan_fragment(ctx, parent, [](std::shared_ptr<StreamLoadContext> ctx) {});
}
// 负责提交并执行 Stream Load（流式导入）任务的执行计划片段（Pipeline Execution Fragment）。
// 它通过异步回调机制处理数据导入的整个生命周期，包括状态统计、质量控制（Filter Ratio 过滤率校验）、事务提交/回滚以及结果通知
// std::shared_ptr<StreamLoadContext> ctx  包含了导入的元数据（label、txn_id、id）、配置参数（max_filter_ratio）、数据源信息（如 Kafka/Kinesis）、统计指标（导入行数、过滤行数）以及通信/同步句柄（promise、body_sink 等）。
// const TPipelineFragmentParamsList& parent 包含了要分发执行的 Fragment（片段）及其执行节点、数据源以及 Pipeline 调度所需的各种算子参数。
// const std::function<void(std::shared_ptr<StreamLoadContext> ctx)>& cb 数据导入完成（或失败）后的回调函数。
Status StreamLoadExecutor::execute_plan_fragment(
        std::shared_ptr<StreamLoadContext> ctx, const TPipelineFragmentParamsList& parent,
        const std::function<void(std::shared_ptr<StreamLoadContext> ctx)>& cb) {
// submit this params
#ifndef BE_TEST
    // 在查询选项中禁止严格类型转换（enable_strict_cast=false），容忍部分类型转换异常，以便后续通过过滤率机制统计错误行。
    ctx->put_result.pipeline_params.query_options.__set_enable_strict_cast(false);
    ctx->start_write_data_nanos = MonotonicNanos();
    // 打印 INFO 日志，输出本次 Stream Load 的三大核心标识：label（事务标签）、txn_id（事务ID）、query_id（查询ID/上下文ID）
    LOG(INFO) << "begin to execute stream load. label=" << ctx->label << ", txn_id=" << ctx->txn_id
              << ", query_id=" << ctx->id;
    Status st;
    std::shared_ptr<bool> is_prepare_success = std::make_shared<bool>(false);
    // 定义了一个 Lambda 闭包，作为 Fragment 执行完毕（完成或失败）时的完成回调（Finish Callback）
    auto exec_fragment = [ctx, cb, this, is_prepare_success](RuntimeState* state, Status* status) {
        // 如果开启了组提交（Group Commit），导入任务由后台合并处理，因此从 RuntimeState 中重新刷新分配的 import_label 和 WAL 事务 ID（wal_id）
        if (ctx->group_commit) {
            ctx->label = state->import_label();
            ctx->txn_id = state->wal_id();
        }
        // Fragment 运行结束，从全局流管理器（LoadStreamMgr）中移除该 ctx->id 对应的流通道，释放管道映射。
        ctx->exec_env()->new_load_stream_mgr()->remove(ctx->id);
        // 收集导入行数与质量指标
        // 统计总行数、成功加载行数、过滤行数（数据不规范导致）、未选中行数（Where条件过滤）和处理字节数。
        ctx->commit_infos = state->tablet_commit_infos();
        ctx->number_total_rows = state->num_rows_load_total();
        ctx->number_loaded_rows = state->num_rows_load_success();
        ctx->number_filtered_rows = state->num_rows_load_filtered();
        ctx->number_unselected_rows = state->num_rows_load_unselected();
        ctx->loaded_bytes = state->num_bytes_load_total();
        int64_t num_selected_rows = ctx->number_total_rows - ctx->number_unselected_rows;
        ctx->error_url = to_load_error_http_path(state->get_error_log_file_path());
        // 数据质量校验 (Max Filter Ratio)
        // 如果 Fragment 执行成功且非 Group Commit，计算实际错误行比例：filtered_rows / selected_rows。
        if (status->ok() && !ctx->group_commit && num_selected_rows > 0 &&
            (double)ctx->number_filtered_rows / num_selected_rows > ctx->max_filter_ratio) {
            // NOTE: Do not modify the error message here, for historical reasons,
            // some users may rely on this error message.
            if (ctx->need_commit_self) {
                *status =
                        Status::DataQualityError("too many filtered rows, url: {}", ctx->error_url);
            } else {
                *status = Status::DataQualityError("too many filtered rows");
            }
        }
        // 如果是例行导入（Routine Load）或发生执行错误，记录 RuntimeState 中捕获的第一条错误原因。
        if (ctx->load_type == TLoadType::ROUTINE_LOAD || !status->ok()) {
            ctx->first_error_msg = state->get_first_error_msg();
        }
        // 全局 Metrics 统计与异常重置
        if (status->ok()) {
            DorisMetrics::instance()->stream_receive_bytes_total->increment(ctx->receive_bytes);
            DorisMetrics::instance()->stream_load_rows_total->increment(ctx->number_loaded_rows);
        } else {
            LOG(WARNING) << "fragment execute failed"
                         << ", err_msg=" << status->to_string() << ", " << ctx->brief();
            ctx->number_loaded_rows = 0;
            // cancel body_sink, make sender known it
            if (ctx->body_sink != nullptr) {
                ctx->body_sink->cancel(status->to_string());
            }

            switch (ctx->load_src_type) {
            // reset the stream load ctx's kafka commit offset
            case TLoadSourceType::KAFKA:
                ctx->kafka_info->reset_offset();
                break;
            case TLoadSourceType::KINESIS:
                ctx->kinesis_info->reset_sequence_numbers();
                break;
            default:
                break;
            }
        }
        // 统计耗时与通知等待者
        ctx->write_data_cost_nanos = MonotonicNanos() - ctx->start_write_data_nanos;
        ctx->load_status_promise.set_value(*status);
        // 提前退出与 Pipe 取消清理
        if (!status->ok() && ctx->body_sink != nullptr) {
            // In some cases, the load execution is exited early.
            // For example, when max_filter_ratio is 0 and illegal data is encountered
            // during stream loading, the entire load process is terminated early.
            // However, the http connection may still be sending data to stream_load_pipe
            // and waiting for it to be consumed.
            // Therefore, we need to actively cancel to end the pipe.
            ctx->body_sink->cancel(status->to_string());
        }
        // 事务处理 (Commit 或 Rollback)
        // 若当前 BE 负责独立控制事务（need_commit_self == true）：
        // 如果管道被取消或执行状态非 OK，调用 rollback_txn 进行事务回滚；
        if (ctx->need_commit_self && ctx->body_sink != nullptr) {
            if (ctx->body_sink->cancelled() || !status->ok()) {
                ctx->status = *status;
                this->rollback_txn(ctx.get());
            } else {
                static_cast<void>(this->commit_txn(ctx.get()));
            }
        }
        // 判断 Fragment 的 Prepare 阶段是否成功。只有 Prepare 成功的任务才需要由 exec_fragment 触发回调 cb(ctx) 回写 HTTP 响应；
        if (*is_prepare_success) {
            // if prepare failed, on_header will send reply
            cb(ctx);
        }
    };
    // 提交任务到 FragmentMgr
    // is_prepare_success 作为出参被传入，用于同步标记准备阶段是否成功。
    st = _exec_env->fragment_mgr()->exec_plan_fragment(ctx->put_result.pipeline_params,
                                                       QuerySource::STREAM_LOAD, exec_fragment,
                                                       parent, is_prepare_success);

    if (!st.ok()) {
        // no need to check unref's return value
        return st;
    }
#else
    ctx->load_status_promise.set_value(k_stream_load_plan_status);
#endif
    return Status::OK();
}
// 负责向 FE (Frontend) 主节点（Master FE）发起 RPC 请求，开启一个导入事务（Transaction）。开启事务成功后，FE 会为本次 Stream Load 分配全局唯一的事务 ID（txn_id）。
// Stream Load 开启事务时必须向 FE 发送 RPC 请求（loadTxnBegin），主要有以下 4 个核心原因：
// 1. 全局事务 ID（Txn ID）的集中式分配
// Doris 支持跨多节点、多 Tablet 的分布式事务，必须确保所有数据写入操作在一个全局唯一且单调递增的事务下进行。
// 如果由 BE 自行生成 ID：多个 BE 节点独立并发处理导入时，极易发生事务 ID 冲突，且难以维护全局的逻辑时间顺序。
// 由 FE 统一分配：FE 的 TransactionMgr（事务管理器）作为权威中心，能确保分配给每次 Stream Load 的 txn_id 全局唯一，并与后续的写 WAL、Commit、Publish Version 过程无缝对接。
// 2. Label（标签）的幂等性与去重校验
// Stream Load 支持用户传入自定义的 Label，用于保障导入的精准一次（Exactly-Once） 语义。
// 3. 元数据校验与权限控制
// 在真正开始传输大体积数据前，必须先在 FE 端进行轻量级的“合法性门禁校验”：
// 拿到内部 ID：获取数据库对应的 db_id，为后续下发执行计划和写入元数据做准备。
// 4. 事务生命周期管控与超时泄露防护
// FE 的 TransactionMgr 维护着集群中所有活跃事务的状态机（PREPARE -> COMMITTED -> VISIBLE / ABORTED）：
// 超时自动回滚：FE 在收到 begin_txn 时会为该事务注册一个定时超时器（Based on timeout_second）。如果 BE 在规定时间内因宕机、网络中断等原因未向 FE 发送 commit_txn，FE 会自动将该事务标为 ABORTED（回滚），防止残余垃圾数据占用存储。
Status StreamLoadExecutor::begin_txn(StreamLoadContext* ctx) {
    DorisMetrics::instance()->stream_load_txn_begin_request_total->increment(1);
    // 构建 Thrift RPC 请求对象（TLoadTxnBeginRequest）
    TLoadTxnBeginRequest request;
    // 填充用户名、密码/Token 等认证授权信息
    set_request_auth(&request, ctx->auth);
    // 设置目标数据库名、表名以及本次导入的标识 Label。
    request.__set_db(ctx->db);
    request.__set_tbl(ctx->table);
    request.__set_label(ctx->label);
    // set timestamp
    // 获取当前的微秒级时间戳设置到请求中，用于记录事务发起的绝对时间。
    request.__set_timestamp(GetCurrentTimeMicros());
    // 如果用户显式指定了导入超时时间（不等于默认值 -1），将超时时间传递给 FE，FE 会根据该时间控制事务超时。
    if (ctx->timeout_second != -1) {
        request.__set_timeout(ctx->timeout_second);
    }
    // 将唯一标识本次 Stream Load 查询的 Unique ID（ctx->id）转换为 Thrift 格式并设置。
    request.__set_request_id(ctx->id.to_thrift());
    request.__set_backend_id(_exec_env->cluster_info()->backend_id);
    // 如果客户端请求中未显式设置 group_commit_mode 参数，则将 use_table_group_commit_mode 标识设为 true，告诉 FE 自动使用目标表在元数据中配置的组提交模式。
    if (ctx->group_commit_mode.empty()) {
        request.__set_use_table_group_commit_mode(true);
    }
    // 校验 FE Master 地址与发起 Thrift RPC 远程调用
    TLoadTxnBeginResult result;
    Status status;
    int64_t duration_ns = 0;
    auto master_addr_provider = [this]() { return _exec_env->cluster_info()->master_fe_addr; };
    TNetworkAddress master_addr = master_addr_provider();
    if (master_addr.hostname.empty() || master_addr.port == 0) {
        status = Status::Error<SERVICE_UNAVAILABLE>("Have not get FE Master heartbeat yet");
    } else {
        SCOPED_RAW_TIMER(&duration_ns);
#ifndef BE_TEST
        // 调用 FE 的 loadTxnBegin(result, request) 接口。若 RPC 网络传输或连接池异常，通过 RETURN_IF_ERROR 直接返回网络层错误。
        RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
                master_addr_provider, [&request, &result](FrontendServiceConnection& client) {
                    client->loadTxnBegin(result, request);
                }));
#else
        result = k_stream_load_begin_result;
#endif
        // 将 FE 端返回的 Thrift 状态结构体 result.status 转换为 Doris BE 内部的 Status 对象。
        status = Status::create<false>(result.status);
    }
    g_stream_load_begin_txn_latency << duration_ns / 1000;
    // RPC 结果校验与错误处理
    if (!status.ok()) {
        LOG(WARNING) << "begin transaction failed, errmsg=" << status << ctx->brief();
        if (result.__isset.job_status) {
            ctx->existing_job_status = result.job_status;
        }
        return status;
    }
    // 处理 Group Commit (组提交) 逻辑
    // 如果请求时没有手动指定 group_commit_mode，且 FE 返回了目标表默认配置的组提交模式（table_group_commit_mode）：
    if (ctx->group_commit_mode.empty() && result.__isset.table_group_commit_mode) {
        auto table_group_commit_mode = result.table_group_commit_mode;
        if (iequal(table_group_commit_mode, "async_mode") ||
            iequal(table_group_commit_mode, "sync_mode")) {
            // 若匹配，则在 ctx 中开启组提交标识 ctx->group_commit = true，保存组提交模式，并直接返回 Status::OK()。
            ctx->group_commit = true;
            ctx->group_commit_mode = table_group_commit_mode;
            return Status::OK();
        }
    }
    // 写回上下文与标志位设置
    ctx->txn_id = result.txnId;
    if (result.__isset.db_id) {
        ctx->db_id = result.db_id;
    }
    ctx->need_rollback = true;

    return Status::OK();
}
// 为了支持两阶段提交（Two-Phase Commit, 2PC）导入模式
// pre_commit_txn 函数负责执行第一阶段提交（Pre-Commit / Prepare 阶段）。
// 当客户端在 Stream Load 请求中开启了两阶段提交（例如设置了 two_phase_commit=true），数据写完后并不立即生效，而是先通过该方法向 Master FE 发起 loadTxnPreCommit RPC 请求，将事务状态置为 PRECOMMITTED（已预提交）。后续需要由客户端显式发起第二阶段的 Commit 或 Abort 指令。
Status StreamLoadExecutor::pre_commit_txn(StreamLoadContext* ctx) {
    // 构建 Commit 请求结构体
    TLoadTxnCommitRequest request;
    get_commit_request(ctx, request);

    TLoadTxnCommitResult result;
    int64_t duration_ns = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);
#ifndef BE_TEST
        // 发起 Thrift RPC 请求（预提交）
        auto master_addr_provider = [this]() { return _exec_env->cluster_info()->master_fe_addr; };
        RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
                master_addr_provider,
                [&request, &result](FrontendServiceConnection& client) {
                    client->loadTxnPreCommit(result, request);
                },
                config::txn_commit_rpc_timeout_ms));
#else
        result = k_stream_load_commit_result;
#endif
    }
    g_stream_load_precommit_txn_latency << duration_ns / 1000;
    // Return if this transaction is precommitted successful; otherwise, we need try
    // to
    // rollback this transaction
    // RPC 结果解析与错误处理
    Status status(Status::create(result.status));
    if (!status.ok()) {
        LOG(WARNING) << "precommit transaction failed, errmsg=" << status << ctx->brief();
        if (status.is<PUBLISH_TIMEOUT>()) {
            ctx->need_rollback = false;
        }
        ctx->status = status;
        return status;
    }
    // precommit success, set need_rollback to false
    ctx->need_rollback = false;
    return Status::OK();
}
// 为了支持两阶段提交（2PC）的完整闭环
// operate_txn_2pc 函数负责向 Master FE 发起 RPC 请求，执行两阶段提交的第二阶段指令（显式提交 Commit 或显式取消/回滚 Abort）。
// 当用户在第一阶段（pre_commit_txn）成功将事务置为 PRECOMMITTED 状态后，后续会单独发送 HTTP 请求或 API 指令来触发第二阶段的操作。该函数就是用来处理这第二次操作的。
Status StreamLoadExecutor::operate_txn_2pc(StreamLoadContext* ctx) {
    TLoadTxn2PCRequest request;
    set_request_auth(&request, ctx->auth);
    request.__set_db(ctx->db);
    request.__set_operation(ctx->txn_operation);
    request.__set_thrift_rpc_timeout_ms(config::txn_commit_rpc_timeout_ms);
    request.__set_label(ctx->label);
    if (ctx->txn_id != doris::StreamLoadContext::default_txn_id) {
        request.__set_txnId(ctx->txn_id);
    }

    TLoadTxn2PCResult result;
    int64_t duration_ns = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        auto master_addr_provider = [this]() { return _exec_env->cluster_info()->master_fe_addr; };
        RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
                master_addr_provider,
                [&request, &result](FrontendServiceConnection& client) {
                    client->loadTxn2PC(result, request);
                },
                config::txn_commit_rpc_timeout_ms));
    }
    g_stream_load_commit_txn_latency << duration_ns / 1000;
    Status status(Status::create(result.status));
    if (!status.ok()) {
        LOG(WARNING) << "2PC commit transaction failed, errmsg=" << status;
        return status;
    }
    return Status::OK();
}

// 在进行事务提交（commit_txn）或预提交（pre_commit_txn）之前，将 StreamLoadContext（Stream Load 上下文）中分散的元数据、认证信息、成功写入的 Tablet 统计信息以及导入质量报告等，打包提炼并填充到 Thrift RPC 结构体 TLoadTxnCommitRequest 中，
// 为后续向 Master FE 发起 RPC 请求做准备。
void StreamLoadExecutor::get_commit_request(StreamLoadContext* ctx,
                                            TLoadTxnCommitRequest& request) {
    set_request_auth(&request, ctx->auth);
    // 设置数据库与表信息
    request.__set_db(ctx->db);
    if (ctx->db_id > 0) {
        request.__set_db_id(ctx->db_id);
    }
    request.__set_tbl(ctx->table);
    // 设置事务核心标志与 Tablet 写入信息
    request.__set_txnId(ctx->txn_id);
    // 设置同步提交标识为 true。指示 FE 在处理提交请求时，尽量同步等待数据版本发布（Publish Version）完成或达到安全阈值后再返回结果给 BE。
    request.__set_sync(true);
    // 关键一步。将 Pipeline 算子/数据写入层收集到的 commit_infos（包含成功写入数据的各个 Tablet ID 以及对应的 Schema Hash、Version 等信息）附带上，告诉 FE 哪些 Tablet 的副本已经写成功。
    request.__set_commitInfos(ctx->commit_infos);
    request.__set_thrift_rpc_timeout_ms(config::txn_commit_rpc_timeout_ms);
    // 如果当前 Stream Load 涉及到多表导入（例如基于条件路由导入到不同的表），将 ctx->table_list（表名列表）设置到请求中。
    request.__set_tbls(ctx->table_list);

    VLOG_DEBUG << "commit txn request:" << apache::thrift::ThriftDebugString(request);

    // set attachment if has
    // 构建并挂载事务提交附件（Attachment）
    // 调用 collect_load_stat(ctx, &attachment) 收集本次导入的统计数据（例如：加载总行数、过滤行数、处理字节数、错误日志文件路径等）。
    TTxnCommitAttachment attachment;
    if (collect_load_stat(ctx, &attachment)) {
        request.__set_txnCommitAttachment(attachment);
    }
}

Status StreamLoadExecutor::commit_txn(StreamLoadContext* ctx) {
    DBUG_EXECUTE_IF("StreamLoadExecutor.commit_txn.block", DBUG_BLOCK);

    DorisMetrics::instance()->stream_load_txn_commit_request_total->increment(1);

    TLoadTxnCommitRequest request;
    get_commit_request(ctx, request);

    TLoadTxnCommitResult result;
#ifndef BE_TEST
    auto master_addr_provider = [this]() { return _exec_env->cluster_info()->master_fe_addr; };
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr_provider,
            [&request, &result](FrontendServiceConnection& client) {
                client->loadTxnCommit(result, request);
            },
            config::txn_commit_rpc_timeout_ms));
#else
    result = k_stream_load_commit_result;
#endif
    // Return if this transaction is committed successful; otherwise, we need try
    // to
    // rollback this transaction
    Status status(Status::create(result.status));
    if (!status.ok()) {
        LOG(WARNING) << "commit transaction failed, errmsg=" << status << ", " << ctx->brief();
        if (status.is<PUBLISH_TIMEOUT>()) {
            ctx->need_rollback = false;
        }
        ctx->status = status;
        return status;
    }
    // commit success, set need_rollback to false
    ctx->need_rollback = false;
    return Status::OK();
}

void StreamLoadExecutor::rollback_txn(StreamLoadContext* ctx) {
    DorisMetrics::instance()->stream_load_txn_rollback_request_total->increment(1);

    TLoadTxnRollbackRequest request;
    set_request_auth(&request, ctx->auth);
    request.__set_db(ctx->db);
    if (ctx->db_id > 0) {
        request.__set_db_id(ctx->db_id);
    }
    request.__set_tbl(ctx->table);
    request.__set_txnId(ctx->txn_id);
    request.__set_reason(ctx->status.to_string());
    request.__set_tbls(ctx->table_list);
    request.__set_label(ctx->label);

    // set attachment if has
    TTxnCommitAttachment attachment;
    if (collect_load_stat(ctx, &attachment)) {
        request.__set_txnCommitAttachment(attachment);
    }

    TLoadTxnRollbackResult result;
#ifndef BE_TEST
    auto master_addr_provider = [this]() { return _exec_env->cluster_info()->master_fe_addr; };
    auto rpc_st = ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr_provider, [&request, &result](FrontendServiceConnection& client) {
                client->loadTxnRollback(result, request);
            });
    if (!rpc_st.ok()) {
        LOG(WARNING) << "transaction rollback failed. errmsg=" << rpc_st << ctx->brief();
    }
#else
    result = k_stream_load_rollback_result;
#endif
}

bool StreamLoadExecutor::collect_load_stat(StreamLoadContext* ctx, TTxnCommitAttachment* attach) {
    if (ctx->load_type != TLoadType::ROUTINE_LOAD && ctx->load_type != TLoadType::MINI_LOAD) {
        // currently, only routine load and mini load need to be set attachment
        return false;
    }
    switch (ctx->load_type) {
    case TLoadType::MINI_LOAD: {
        throw Exception(Status::FatalError("mini load is not supported any more"));
    }
    case TLoadType::ROUTINE_LOAD: {
        attach->loadType = TLoadType::ROUTINE_LOAD;

        TRLTaskTxnCommitAttachment rl_attach;
        rl_attach.jobId = ctx->job_id;
        rl_attach.id = ctx->id.to_thrift();
        rl_attach.__set_loadedRows(ctx->number_loaded_rows);
        rl_attach.__set_filteredRows(ctx->number_filtered_rows);
        rl_attach.__set_unselectedRows(ctx->number_unselected_rows);
        rl_attach.__set_receivedBytes(ctx->receive_bytes);
        rl_attach.__set_loadedBytes(ctx->loaded_bytes);
        rl_attach.__set_loadCostMs(ctx->load_cost_millis);
        if (!ctx->first_error_msg.empty()) {
            rl_attach.__set_firstErrorMsg(ctx->first_error_msg);
        }

        attach->rlTaskTxnCommitAttachment = rl_attach;
        attach->__isset.rlTaskTxnCommitAttachment = true;
        break;
    }
    default:
        // unknown load type, should not happened
        return false;
    }

    switch (ctx->load_src_type) {
    case TLoadSourceType::KAFKA: {
        TRLTaskTxnCommitAttachment& rl_attach = attach->rlTaskTxnCommitAttachment;
        rl_attach.loadSourceType = TLoadSourceType::KAFKA;

        TKafkaRLTaskProgress kafka_progress;
        kafka_progress.partitionCmtOffset = ctx->kafka_info->cmt_offset;

        rl_attach.kafkaRLTaskProgress = kafka_progress;
        rl_attach.__isset.kafkaRLTaskProgress = true;
        if (!ctx->error_url.empty()) {
            rl_attach.__set_errorLogUrl(ctx->error_url);
        }
        return true;
    }
    case TLoadSourceType::KINESIS: {
        TRLTaskTxnCommitAttachment& rl_attach = attach->rlTaskTxnCommitAttachment;
        rl_attach.loadSourceType = TLoadSourceType::KINESIS;

        TKinesisRLTaskProgress kinesis_progress;
        kinesis_progress.shardCmtSeqNum = ctx->kinesis_info->cmt_sequence_number;
        if (!ctx->kinesis_info->millis_behind_latest.empty()) {
            kinesis_progress.__set_shardMillsBehindLatest(ctx->kinesis_info->millis_behind_latest);
        }
        if (!ctx->kinesis_info->closed_shard_ids.empty()) {
            kinesis_progress.__set_closedShardIds(ctx->kinesis_info->closed_shard_ids);
        }

        rl_attach.kinesisRLTaskProgress = kinesis_progress;
        rl_attach.__isset.kinesisRLTaskProgress = true;
        if (!ctx->error_url.empty()) {
            rl_attach.__set_errorLogUrl(ctx->error_url);
        }
        return true;
    }
    default:
        return true;
    }
    return false;
}

} // namespace doris
