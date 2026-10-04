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

#include "exec/pipeline/pipeline_fragment_context.h"

#include <gen_cpp/DataSinks_types.h>
#include <gen_cpp/FrontendService.h>
#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <pthread.h>

#include <algorithm>
#include <cstdlib>

// IWYU pragma: no_include <bits/chrono.h>
#include <fmt/format.h>
#include <thrift/Thrift.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <thrift/transport/TTransportException.h>

#include <chrono> // IWYU pragma: keep
#include <map>
#include <memory>
#include <ostream>
#include <utility>

#include "cloud/config.h"
#include "common/cast_set.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "exec/exchange/local_exchange_sink_operator.h"
#include "exec/exchange/local_exchange_source_operator.h"
#include "exec/exchange/local_exchanger.h"
#include "exec/exchange/vdata_stream_mgr.h"
#include "exec/operator/aggregation_sink_operator.h"
#include "exec/operator/aggregation_source_operator.h"
#include "exec/operator/analytic_sink_operator.h"
#include "exec/operator/analytic_source_operator.h"
#include "exec/operator/assert_num_rows_operator.h"
#include "exec/operator/blackhole_sink_operator.h"
#include "exec/operator/bucketed_aggregation_sink_operator.h"
#include "exec/operator/bucketed_aggregation_source_operator.h"
#include "exec/operator/cache_sink_operator.h"
#include "exec/operator/cache_source_operator.h"
#include "exec/operator/datagen_operator.h"
#include "exec/operator/dict_sink_operator.h"
#include "exec/operator/distinct_streaming_aggregation_operator.h"
#include "exec/operator/empty_set_operator.h"
#include "exec/operator/exchange_sink_operator.h"
#include "exec/operator/exchange_source_operator.h"
#include "exec/operator/file_scan_operator.h"
#include "exec/operator/group_commit_block_sink_operator.h"
#include "exec/operator/group_commit_scan_operator.h"
#include "exec/operator/hashjoin_build_sink.h"
#include "exec/operator/hashjoin_probe_operator.h"
#include "exec/operator/hive_table_sink_operator.h"
#include "exec/operator/iceberg_delete_sink_operator.h"
#include "exec/operator/iceberg_merge_sink_operator.h"
#include "exec/operator/iceberg_table_sink_operator.h"
#include "exec/operator/jdbc_scan_operator.h"
#include "exec/operator/jdbc_table_sink_operator.h"
#include "exec/operator/local_merge_sort_source_operator.h"
#include "exec/operator/materialization_opertor.h"
#include "exec/operator/maxcompute_table_sink_operator.h"
#include "exec/operator/memory_scratch_sink_operator.h"
#include "exec/operator/meta_scan_operator.h"
#include "exec/operator/multi_cast_data_stream_sink.h"
#include "exec/operator/multi_cast_data_stream_source.h"
#include "exec/operator/nested_loop_join_build_operator.h"
#include "exec/operator/nested_loop_join_probe_operator.h"
#include "exec/operator/olap_scan_operator.h"
#include "exec/operator/olap_table_sink_operator.h"
#include "exec/operator/olap_table_sink_v2_operator.h"
#include "exec/operator/partition_sort_sink_operator.h"
#include "exec/operator/partition_sort_source_operator.h"
#include "exec/operator/partitioned_aggregation_sink_operator.h"
#include "exec/operator/partitioned_aggregation_source_operator.h"
#include "exec/operator/partitioned_hash_join_probe_operator.h"
#include "exec/operator/partitioned_hash_join_sink_operator.h"
#include "exec/operator/rec_cte_anchor_sink_operator.h"
#include "exec/operator/rec_cte_scan_operator.h"
#include "exec/operator/rec_cte_sink_operator.h"
#include "exec/operator/rec_cte_source_operator.h"
#include "exec/operator/repeat_operator.h"
#include "exec/operator/result_file_sink_operator.h"
#include "exec/operator/result_sink_operator.h"
#include "exec/operator/schema_scan_operator.h"
#include "exec/operator/select_operator.h"
#include "exec/operator/set_probe_sink_operator.h"
#include "exec/operator/set_sink_operator.h"
#include "exec/operator/set_source_operator.h"
#include "exec/operator/sort_sink_operator.h"
#include "exec/operator/sort_source_operator.h"
#include "exec/operator/spill_iceberg_table_sink_operator.h"
#include "exec/operator/spill_sort_sink_operator.h"
#include "exec/operator/spill_sort_source_operator.h"
#include "exec/operator/streaming_aggregation_operator.h"
#include "exec/operator/table_function_operator.h"
#include "exec/operator/tvf_table_sink_operator.h"
#include "exec/operator/union_sink_operator.h"
#include "exec/operator/union_source_operator.h"
#include "exec/pipeline/dependency.h"
#include "exec/pipeline/pipeline_task.h"
#include "exec/pipeline/report_exec_status_size.h"
#include "exec/pipeline/task_scheduler.h"
#include "exec/runtime_filter/runtime_filter_mgr.h"
#include "exec/sort/topn_sorter.h"
#include "exec/spill/spill_file.h"
#include "io/fs/stream_load_pipe.h"
#include "load/stream_load/new_load_stream_mgr.h"
#include "runtime/cluster_info.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "service/backend_options.h"
#include "util/client_cache.h"
#include "util/countdown_latch.h"
#include "util/debug_util.h"
#include "util/network_util.h"
#include "util/uid_util.h"

namespace doris {
PipelineFragmentContext::PipelineFragmentContext(
        TUniqueId query_id, const TPipelineFragmentParams& request,
        std::shared_ptr<QueryContext> query_ctx, ExecEnv* exec_env,
        const std::function<void(RuntimeState*, Status*)>& call_back)
        : _query_id(std::move(query_id)),
          _fragment_id(request.fragment_id),
          _exec_env(exec_env),
          _query_ctx(std::move(query_ctx)),
          _call_back(call_back),
          _params(request),
          _parallel_instances(_params.__isset.parallel_instances ? _params.parallel_instances : 0),
          _need_notify_close(request.__isset.need_notify_close ? request.need_notify_close
                                                               : false) {
    _fragment_watcher.start();
}

PipelineFragmentContext::~PipelineFragmentContext() {
    LOG_INFO("PipelineFragmentContext::~PipelineFragmentContext")
            .tag("query_id", print_id(_query_id))
            .tag("fragment_id", _fragment_id);
    _release_resource();
    {
        // The memory released by the query end is recorded in the query mem tracker.
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_query_ctx->query_mem_tracker());
        _runtime_state.reset();
        _query_ctx.reset();
    }
}

bool PipelineFragmentContext::is_timeout(timespec now) const {
    if (_timeout <= 0) {
        return false;
    }
    return _fragment_watcher.elapsed_time_seconds(now) > _timeout;
}

// notify_close() transitions the PFC from "waiting for external close notification" to
// "self-managed close". A recursive CTE PFC normally remains registered until rerun_fragment()
// calls this for WAIT_FOR_DESTROY or FINAL_CLOSE; cancellation can also call it.
// Returns true if all tasks have already closed (i.e., the PFC can be safely destroyed).
bool PipelineFragmentContext::notify_close() {
    bool all_closed = false;
    bool need_remove = false;
    {
        std::lock_guard<std::mutex> l(_task_mutex);
        if (_closed_tasks >= _total_tasks) {
            if (_need_notify_close) {
                // The fragment finished while waiting for the external close notification.
                // Record that we need to remove from fragment mgr, but do it
                // after releasing _task_mutex to avoid ABBA deadlock with
                // dump_pipeline_tasks() (which acquires _pipeline_map lock
                // first, then _task_mutex via debug_string()).
                need_remove = true;
            }
            all_closed = true;
        }
        // Allow the fragment to be removed now or after its remaining tasks close.
        _need_notify_close = false;
    }
    if (need_remove) {
        _exec_env->fragment_mgr()->remove_pipeline_context({_query_id, _fragment_id});
    }
    return all_closed;
}

// Must not add lock in this method. Because it will call query ctx cancel. And
// QueryCtx cancel will call fragment ctx cancel. And Also Fragment ctx's running
// Method like exchange sink buffer will call query ctx cancel. If we add lock here
// There maybe dead lock.
void PipelineFragmentContext::cancel(const Status reason) {
    LOG_INFO("PipelineFragmentContext::cancel")
            .tag("query_id", print_id(_query_id))
            .tag("fragment_id", _fragment_id)
            .tag("reason", reason.to_string());
    if (notify_close()) {
        return;
    }
    // Timeout is a special error code, we need print current stack to debug timeout issue.
    if (reason.is<ErrorCode::TIMEOUT>()) {
        auto dbg_str = fmt::format("PipelineFragmentContext is cancelled due to timeout:\n{}",
                                   debug_string());
        LOG_LONG_STRING(WARNING, dbg_str);
    }

    // `ILLEGAL_STATE` means queries this fragment belongs to was not found in FE (maybe finished)
    if (reason.is<ErrorCode::ILLEGAL_STATE>()) {
        LOG_WARNING("PipelineFragmentContext is cancelled due to illegal state : {}",
                    debug_string());
    }

    if (reason.is<ErrorCode::MEM_LIMIT_EXCEEDED>() || reason.is<ErrorCode::MEM_ALLOC_FAILED>()) {
        print_profile("cancel pipeline, reason: " + reason.to_string());
    }

    if (auto error_url = get_load_error_url(); !error_url.empty()) {
        _query_ctx->set_load_error_url(error_url);
    }

    if (auto first_error_msg = get_first_error_msg(); !first_error_msg.empty()) {
        _query_ctx->set_first_error_msg(first_error_msg);
    }

    _query_ctx->cancel(reason, _fragment_id);
    if (!reason.is<ErrorCode::LIMIT_REACH>() && !reason.is<ErrorCode::FINISHED>()) {
        for (auto& id : _fragment_instance_ids) {
            LOG(WARNING) << "PipelineFragmentContext cancel instance: " << print_id(id);
        }
    }
    // Get pipe from new load stream manager and send cancel to it or the fragment may hang to wait read from pipe
    // For stream load the fragment's query_id == load id, it is set in FE.
    auto stream_load_ctx = _exec_env->new_load_stream_mgr()->get(_query_id);
    if (stream_load_ctx != nullptr) {
        stream_load_ctx->pipe->cancel(reason.to_string());
        // Set error URL here because after pipe is cancelled, stream load execution may return early.
        // We need to set the error URL at this point to ensure error information is properly
        // propagated to the client.
        stream_load_ctx->error_url = get_load_error_url();
        stream_load_ctx->first_error_msg = get_first_error_msg();
    }

    for (auto& tasks : _tasks) {
        for (auto& task : tasks) {
            task.first->unblock_all_dependencies();
        }
    }
}
// 在当前 Fragment 上下文中创建并注册一个新的 Pipeline 对象，建立父子 Pipeline 之间的拓扑依赖关系，并根据父 Pipeline 自动推导和初始化当前 Pipeline 的任务并发度（num_tasks）。
// PipelinePtr parent（输入参数，默认为 nullptr）
// 指定新创建 Pipeline 的父管道（Parent Pipeline）。如果为 nullptr，说明创建的是根管道（Root Pipeline）；如果非空，说明当前管道是因 Blocking 算子（如 HashJoin Build 端、Aggregation 聚合端等）切分出来的子管道。
// int idx（输入参数，默认值为 -1）
// 指定新 Pipeline 在当前 Fragment 的全局管道列表 _pipelines 中的插入位置索引。若 idx >= 0：将新 Pipeline 插入到 _pipelines 数组的指定位置（用于控制 Pipeline 的执行/准备顺序）；若 idx < 0（默认值通常为 -1）：直接追加到 _pipelines 数组的末尾。
PipelinePtr PipelineFragmentContext::add_pipeline(PipelinePtr parent, int idx) {
    // 分配自增 Pipeline ID
    PipelineId id = _next_pipeline_id++;
    // 实例化 Pipeline 并计算并发任务数
    // 如果存在父管道 parent，当前管道的初始任务数不能超过父管道的任务数，同时也不能超过当前 BE 节点分配的实例数 _num_instances（即取二者的较小值 std::min）；如果没有父管道，直接使用 _num_instances。
    // 第二个并发度参数（基准/最大 num_tasks）：作为并发度的参考上界（未裁剪前的父管道任务数或默认实例数）。
    auto pipeline = std::make_shared<Pipeline>(
            id, parent ? std::min(parent->num_tasks(), _num_instances) : _num_instances,
            parent ? parent->num_tasks() : _num_instances);
    // 将新 Pipeline 注册到 Fragment 的全局向量中
    // 若指定了插入位置 idx，使用 _pipelines.insert(...) 将新创建的 pipeline 插入到 _pipelines 迭代器的指定位置（如在某些特定依赖关系的算子构建中需要调整顺序）。
    if (idx >= 0) {
        _pipelines.insert(_pipelines.begin() + idx, pipeline);
    } else {
    // 若未指定索引（即 idx 为负数），调用 _pipelines.emplace_back(pipeline) 直接尾插到 _pipelines 容器末尾。
        _pipelines.emplace_back(pipeline);
    }
    // 建立父子 Pipeline 的拓扑关联
    if (parent) {
        // 调用 parent->set_children(pipeline)，将当前新创建的 pipeline 注册为 parent 的子管道（Child Pipeline）。这在后续依赖树构建、资源释放以及数据流依赖（SharedState 传递）时作为重要的拓扑边信息。
        parent->set_children(pipeline);
    }
    return pipeline;
}
// 主要职责是：将 FE (Frontend) 传递过来的逻辑执行计划树（ExecNode Tree）拆解转化为物理 Pipeline 拓扑结构，构建 Data Sink（数据接收/发送算子）、规划本地数据打散（Local Exchange），并最终为每一个 Instance 实例化出可被调度器执行的物理 PipelineTask。
// ThreadPool* thread_pool  传入用于并行构建和准备 PipelineTask 的线程池。当一个 Fragment 在当前 BE 节点上拥有多个 Instance（例如并行度为 8、16 时），利用该线程池可以并行并发地对各个 Instance 进行资源初始化和 Task 构建，从而加速大型 Fragment 的准备过程。
Status PipelineFragmentContext::_build_and_prepare_full_pipeline(ThreadPool* thread_pool) {
    {
        // 开启构建 Pipeline 阶段的计时，结果记录在 BuildPipelinesTime 指标中。
        SCOPED_TIMER(_build_pipelines_timer);
        // 2. Build pipelines with operators in this fragment.
        // 在当前 Fragment 上下文中新建一个空的 Pipeline 对象，并作为逻辑根管道 root_pipeline。
        auto root_pipeline = add_pipeline();
        // 深度优先递归遍历逻辑计划树
        // 遇到打断流水线的算子（Blocking Operator，如 Hash Join Build 端、Agg 聚合端）时，切分并创建子 Pipeline（Child Pipeline），并将生产端与消费端通过 SharedState 绑定；
        // _root_op 将接收构建出的根算子指针。
        RETURN_IF_ERROR(_build_pipelines(_runtime_state->obj_pool(), *_query_ctx->desc_tbl,
                                         &_root_op, root_pipeline));

        // Propagate _num_instances from LOCAL_EXCHANGE pipelines to ancestor pipelines
        // that inherited reduced num_tasks from a serial operator.
        // 本地并行度传播与延迟本地交换器构建
        // 将包含 LOCAL_EXCHANGE（本地数据交换）的 Pipeline 的真实并发度（_num_instances）向上游/祖先 Pipeline 传播。解决因为某些串行算子（Serial Operator，如单线程 Exchange/Gather）导致上游 Pipeline 任务数被错误缩减的问题。
        _propagate_local_exchange_num_tasks();

        // Create deferred local exchangers now that all pipelines have final num_tasks.
        // 在前一步确定了所有 Pipeline 的最终任务并发数（num_tasks）之后，延后（Deferred）实例化对应的本地数据交换器（Local Exchangers），确保数据通道两端的通道数量精确匹配。
        RETURN_IF_ERROR(_create_deferred_local_exchangers());

        // Raise num_tasks for pipelines whose serial non-scan operators (e.g.,
        // UNPARTITIONED Exchange) reduced num_tasks below _num_instances.
        // Without this, fragment instances 1+ have no task for these pipelines
        // and downstream operators fail with "must set shared state".
        //
        // This applies to ALL pipelines (not just deferred exchanger upstreams):
        // fragments with UNION/INTERSECT/EXCEPT + serial Exchange in child
        // pipelines also need the raise, even without FE-planned local exchange.
        //
        // Exception: serial scan sources (pooling scan) keep num_tasks=1 — the
        // PassthroughExchanger(1, N) handles the fan-out correctly.
        // NOTE: Do NOT raise pipelines whose source is a serial operator
        // (Exchange or scan) — they legitimately have 1 task, and raising
        // them causes crashes (e.g., 4 Exchange tasks but only 1 receives
        // data).  The correct fix for shared state injection across
        // instances is handled by the FE: it inserts local exchange nodes
        // between serial operators and their downstream consumers, creating
        // proper pipeline boundaries with _num_instances tasks.

        // 3. Create sink operator
        // 创建与绑定数据接收算子（Data Sink）
        // 校验 Sink：校验 Thrift 参数中是否定义了当前 Fragment 的输出 Sink（如 DataStreamSink 用于跨节点传输，或 ResultSink 用于向前端返回结果）。若没有则抛出内部错误。
        if (!_params.fragment.__isset.output_sink) {
            return Status::InternalError("No output sink in this fragment!");
        }
        // 在 obj_pool 内存池中根据 output_sink 的类型创建对应的物理 DataSinkOperatorBuilder。
        RETURN_IF_ERROR(_create_data_sink(_runtime_state->obj_pool(), _params.fragment.output_sink,
                                          _params.fragment.output_exprs, _params,
                                          root_pipeline->output_row_desc(), _runtime_state.get(),
                                          *_desc_tbl, root_pipeline->id()));

        // 初始化该 Data Sink（解析输出表达式 output_exprs、设置 Tuple 描述等）。
        RETURN_IF_ERROR(_sink->init(_params.fragment.output_sink));
        // 将创建好的 Data Sink 设置为根 Pipeline 的尾部输出节点。
        RETURN_IF_ERROR(root_pipeline->set_sink(_sink));
        // 建立算子与 Sink 的父子连接
        // 遍历当前 Fragment 管理的所有 Pipeline（包括根 Pipeline 及切分出的各个子 Pipeline）。
        for (PipelinePtr& pipeline : _pipelines) {
            // 断言确保每个 Pipeline 都已挂载了对应的 Sink 算子（子 Pipeline 会挂载类似 SinkLocalExchange 的中间 Sink）。
            DCHECK(pipeline->sink() != nullptr) << pipeline->operators().size();
            // 将当前 Pipeline 算子链的最后一个算子（operators().back()）作为孩子节点（数据提供方）绑定给当前 Pipeline 的 Sink 算子，完成 Pipeline 内部算子链的数据流闭环。
            RETURN_IF_ERROR(pipeline->sink()->set_child(pipeline->operators().back()));
        }
    }
    // 4. Build local exchanger
    // 构建本地 Shuffle (Local Exchange)
    // 检查当前查询是否启用了本地 Shuffle 优化（为了充分利用多核并发，在 BE 节点内部按 Bucket/Hash 重新打散数据）
    if (_runtime_state->plan_local_shuffle()) {
        // 开启 Local Exchanger 规划阶段的计时，记录在 PlanLocalLocalExchangerTime 指标中。
        SCOPED_TIMER(_plan_local_exchanger_timer);
        // 根据分片 Buckets 数量、Bucket 到 Instance 的映射关系表、Shuffle 索引映射表，
        // 向对应的 Pipeline 中插入 PassthroughExchanger、HashShuffleExchanger 或 BucketShuffleExchanger 等本地数据交换管道。
        RETURN_IF_ERROR(_plan_local_exchange(_params.num_buckets,
                                             _params.bucket_seq_to_instance_idx,
                                             _params.shuffle_idx_to_instance_idx));
    }

    // 5. Initialize global states in pipelines.
    // 初始化 Pipeline 全局状态（Pipeline Prepare）
    // 调用 Pipeline 自身的 prepare 方法。该方法会依次调用管道内所有算子（Operator）和 Sink 的 prepare()，分配全局数据结构、表达式编译和共享状态初始化（SharedState）。
    for (PipelinePtr& pipeline : _pipelines) {
        SCOPED_TIMER(_prepare_all_pipelines_timer);
        pipeline->children().clear();
        RETURN_IF_ERROR(pipeline->prepare(_runtime_state.get()));
    }
    // 构建物理 Pipeline Task 并初始化局部状态
    {
        SCOPED_TIMER(_build_tasks_timer);
        // 6. Build pipeline tasks and initialize local state.
        // 核心物理实例化步骤！
        // 根据并发度（_num_instances）和 Pipeline 拓扑，通过多线程池 thread_pool 并发为每个 Instance 实例化出最终的 PipelineTask；
        // 将构建好的 PipelineTask 提交给 Pipeline 调度器（PipelineTaskScheduler）等待被执行。
        RETURN_IF_ERROR(_build_pipeline_tasks(thread_pool));
    }

    return Status::OK();
}
// PipelineFragmentContext 负责管理单个 Fragment 在当前 BE 节点上的生命周期（包括 Pipeline 的构建、资源初始化、Task 任务准备以及执行状态上报等）。
// prepare 方法是 Fragment 真正开始物理执行前最核心的初始化准备阶段。
Status PipelineFragmentContext::prepare(ThreadPool* thread_pool) {
    // 防重入机制。检查该 Fragment Context 是否已经执行过 prepare，如果重复调用直接返回内部错误。
    if (_prepared) {
        return Status::InternalError("Already prepared");
    }
    // _timeout 设置：检查 Thrift 参数 _params 中是否包含了查询级别的 execution_timeout，若设置了则将其赋值给成员变量 _timeout，作为该 Fragment 执行的超时时间阈值。
    if (_params.__isset.query_options && _params.query_options.__isset.execution_timeout) {
        _timeout = _params.query_options.execution_timeout;
    }
    // 初始化 Profile 与计时器（Metrics 监控）
    _fragment_level_profile = std::make_unique<RuntimeProfile>("PipelineContext");
    _prepare_timer = ADD_TIMER(_fragment_level_profile, "PrepareTime");
    SCOPED_TIMER(_prepare_timer);
    _build_pipelines_timer = ADD_TIMER(_fragment_level_profile, "BuildPipelinesTime");
    _init_context_timer = ADD_TIMER(_fragment_level_profile, "InitContextTime");
    _plan_local_exchanger_timer = ADD_TIMER(_fragment_level_profile, "PlanLocalLocalExchangerTime");
    _build_tasks_timer = ADD_TIMER(_fragment_level_profile, "BuildTasksTime");
    _prepare_all_pipelines_timer = ADD_TIMER(_fragment_level_profile, "PrepareAllPipelinesTime");
    {
        SCOPED_TIMER(_init_context_timer);
        // 获取当前 BE 节点上即将调度的 Instance 数量（根据 local_params 的大小设置 _num_instances）
        cast_set(_num_instances, _params.local_params.size());
        // 计算整个集群中该 Fragment 的 Instance 总数。如果 params 中显式指定了 total_instances 则使用该值，否则回退为当前 BE 的 _num_instances。
        _total_instances =
                _params.__isset.total_instances ? _params.total_instances : _num_instances;

        auto* fragment_context = this;
        // 读取查询选项，设置在 Fragment 成功执行完毕后是否需要向 FE/Coordinator 上报 SUCCESS 状态。
        if (_params.query_options.__isset.is_report_success) {
            fragment_context->set_is_report_success(_params.query_options.is_report_success);
        }

        // 1. Set up the global runtime state.
        // 创建 Fragment 级别的全局 RuntimeState 实例，保存 query_id、fragment_id、全局配置（query_globals）、执行环境指针（_exec_env）以及所属的 QueryContext。
        _runtime_state = RuntimeState::create_unique(
                _params.query_id, _params.fragment_id, _params.query_options,
                _query_ctx->query_globals, _exec_env, _query_ctx.get());
        // 把当前 PipelineFragmentContext（继承自 TaskExecutionContext）绑定给 RuntimeState，使得全局逻辑能够回调 Fragment 上下文。
        _runtime_state->set_task_execution_context(shared_from_this());
        // 切换当前线程的内存分配统计挂载点到该 Query 的 query_mem_tracker 上，确保后续分配的内存正确计入内存配额。
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_runtime_state->query_mem_tracker());
        // 填充 RuntimeState 的运行期属性
        // 将外部传入的各类任务相关属性（如 backend_id、数据导入特有的 import_label、数据库名 db_name、导入作业 ID load_job_id）按需注入到 _runtime_state 中，以备执行或写日志时使用。
        if (_params.__isset.backend_id) {
            _runtime_state->set_backend_id(_params.backend_id);
        }
        if (_params.__isset.import_label) {
            _runtime_state->set_import_label(_params.import_label);
        }
        if (_params.__isset.db_name) {
            _runtime_state->set_db_name(_params.db_name);
        }
        if (_params.__isset.load_job_id) {
            _runtime_state->set_load_job_id(_params.load_job_id);
        }

        if (_params.is_simplified_param) {
            _desc_tbl = _query_ctx->desc_tbl;
        } else {
            DCHECK(_params.__isset.desc_tbl);
            RETURN_IF_ERROR(DescriptorTbl::create(_runtime_state->obj_pool(), _params.desc_tbl,
                                                  &_desc_tbl));
        }
        // 绑定描述符表（Descriptor Table）与并发参数
        _runtime_state->set_desc_tbl(_desc_tbl);
        _runtime_state->set_num_per_fragment_instances(_params.num_senders);
        _runtime_state->set_load_stream_per_node(_params.load_stream_per_node);
        _runtime_state->set_total_load_streams(_params.total_load_streams);
        _runtime_state->set_num_local_sink(_params.num_local_sink);

        // init fragment_instance_ids
        // 遍历 local_params，提取分配到当前 BE 节点的所有 Fragment Instance 的唯一 ID (fragment_instance_id)，记录在本地数组 _fragment_instance_ids 中。
        const auto target_size = _params.local_params.size();
        _fragment_instance_ids.resize(target_size);
        for (size_t i = 0; i < _params.local_params.size(); i++) {
            auto fragment_instance_id = _params.local_params[i].fragment_instance_id;
            _fragment_instance_ids[i] = fragment_instance_id;
        }
    }
    // 构建并准备完整的 Pipeline 拓扑与 Task
    RETURN_IF_ERROR(_build_and_prepare_full_pipeline(thread_pool));

    _init_next_report_time();

    _prepared = true;
    return Status::OK();
}

Status PipelineFragmentContext::_build_pipeline_tasks_for_instance(
        int instance_idx,
        const std::vector<std::shared_ptr<RuntimeProfile>>& pipeline_id_to_profile) {
    const auto& local_params = _params.local_params[instance_idx];
    auto fragment_instance_id = local_params.fragment_instance_id;
    auto runtime_filter_mgr = std::make_unique<RuntimeFilterMgr>(false);
    std::map<PipelineId, PipelineTask*> pipeline_id_to_task;
    auto get_shared_state = [&](PipelinePtr pipeline)
            -> std::map<int, std::pair<std::shared_ptr<BasicSharedState>,
                                       std::vector<std::shared_ptr<Dependency>>>> {
        std::map<int, std::pair<std::shared_ptr<BasicSharedState>,
                                std::vector<std::shared_ptr<Dependency>>>>
                shared_state_map;
        for (auto& op : pipeline->operators()) {
            auto source_id = op->operator_id();
            if (auto iter = _op_id_to_shared_state.find(source_id);
                iter != _op_id_to_shared_state.end()) {
                shared_state_map.insert({source_id, iter->second});
            }
        }
        for (auto sink_to_source_id : pipeline->sink()->dests_id()) {
            if (auto iter = _op_id_to_shared_state.find(sink_to_source_id);
                iter != _op_id_to_shared_state.end()) {
                shared_state_map.insert({sink_to_source_id, iter->second});
            }
        }
        return shared_state_map;
    };

    for (size_t pip_idx = 0; pip_idx < _pipelines.size(); pip_idx++) {
        auto& pipeline = _pipelines[pip_idx];
        if (pipeline->num_tasks() > 1 || instance_idx == 0) {
            auto task_runtime_state = RuntimeState::create_unique(
                    local_params.fragment_instance_id, _params.query_id, _params.fragment_id,
                    _params.query_options, _query_ctx->query_globals, _exec_env, _query_ctx.get());
            {
                // Initialize runtime state for this task
                task_runtime_state->set_external_file_report_state(
                        _runtime_state->external_file_report_state());
                task_runtime_state->set_query_mem_tracker(_query_ctx->query_mem_tracker());

                task_runtime_state->set_task_execution_context(shared_from_this());
                task_runtime_state->set_be_number(local_params.backend_num);

                if (_params.__isset.backend_id) {
                    task_runtime_state->set_backend_id(_params.backend_id);
                }
                if (_params.__isset.import_label) {
                    task_runtime_state->set_import_label(_params.import_label);
                }
                if (_params.__isset.db_name) {
                    task_runtime_state->set_db_name(_params.db_name);
                }
                if (_params.__isset.load_job_id) {
                    task_runtime_state->set_load_job_id(_params.load_job_id);
                }
                if (_params.__isset.wal_id) {
                    task_runtime_state->set_wal_id(_params.wal_id);
                }
                if (_params.__isset.content_length) {
                    task_runtime_state->set_content_length(_params.content_length);
                }

                task_runtime_state->set_desc_tbl(_desc_tbl);
                task_runtime_state->set_per_fragment_instance_idx(local_params.sender_id);
                task_runtime_state->set_num_per_fragment_instances(_params.num_senders);
                task_runtime_state->resize_op_id_to_local_state(max_operator_id());
                task_runtime_state->set_max_operator_id(max_operator_id());
                task_runtime_state->set_load_stream_per_node(_params.load_stream_per_node);
                task_runtime_state->set_total_load_streams(_params.total_load_streams);
                task_runtime_state->set_num_local_sink(_params.num_local_sink);

                task_runtime_state->set_runtime_filter_mgr(runtime_filter_mgr.get());
            }
            auto cur_task_id = _total_tasks++;
            task_runtime_state->set_task_id(cur_task_id);
            task_runtime_state->set_task_num(pipeline->num_tasks());
            auto task = std::make_shared<PipelineTask>(
                    pipeline, cur_task_id, task_runtime_state.get(),
                    std::dynamic_pointer_cast<PipelineFragmentContext>(shared_from_this()),
                    pipeline_id_to_profile[pip_idx].get(), get_shared_state(pipeline),
                    instance_idx);
            pipeline->incr_created_tasks(instance_idx, task.get());
            pipeline_id_to_task.insert({pipeline->id(), task.get()});
            _tasks[instance_idx].emplace_back(
                    std::pair<std::shared_ptr<PipelineTask>, std::unique_ptr<RuntimeState>> {
                            std::move(task), std::move(task_runtime_state)});
        }
    }

    /**
         * Build DAG for pipeline tasks.
         * For example, we have
         *
         *   ExchangeSink (Pipeline1)     JoinBuildSink (Pipeline2)
         *            \                      /
         *          JoinProbeOperator1 (Pipeline1)    JoinBuildSink (Pipeline3)
         *                 \                          /
         *               JoinProbeOperator2 (Pipeline1)
         *
         * In this fragment, we have three pipelines and pipeline 1 depends on pipeline 2 and pipeline 3.
         * To build this DAG, `_dag` manage dependencies between pipelines by pipeline ID and
         * `pipeline_id_to_task` is used to find the task by a unique pipeline ID.
         *
         * Finally, we have two upstream dependencies in Pipeline1 corresponding to JoinProbeOperator1
         * and JoinProbeOperator2.
         */
    for (auto& _pipeline : _pipelines) {
        if (pipeline_id_to_task.contains(_pipeline->id())) {
            auto* task = pipeline_id_to_task[_pipeline->id()];
            DCHECK(task != nullptr);

            // If this task has upstream dependency, then inject it into this task.
            if (_dag.contains(_pipeline->id())) {
                auto& deps = _dag[_pipeline->id()];
                for (auto& dep : deps) {
                    if (pipeline_id_to_task.contains(dep)) {
                        auto ss = pipeline_id_to_task[dep]->get_sink_shared_state();
                        if (ss) {
                            task->inject_shared_state(ss);
                        } else {
                            pipeline_id_to_task[dep]->inject_shared_state(
                                    task->get_source_shared_state());
                        }
                    }
                }
            }
        }
    }
    for (size_t pip_idx = 0; pip_idx < _pipelines.size(); pip_idx++) {
        if (pipeline_id_to_task.contains(_pipelines[pip_idx]->id())) {
            auto* task = pipeline_id_to_task[_pipelines[pip_idx]->id()];
            DCHECK(pipeline_id_to_profile[pip_idx]);
            std::vector<TScanRangeParams> scan_ranges;
            auto node_id = _pipelines[pip_idx]->operators().front()->node_id();
            if (local_params.per_node_scan_ranges.contains(node_id)) {
                scan_ranges = local_params.per_node_scan_ranges.find(node_id)->second;
            }
            RETURN_IF_ERROR_OR_CATCH_EXCEPTION(task->prepare(scan_ranges, local_params.sender_id,
                                                             _params.fragment.output_sink));
        }
    }
    {
        std::lock_guard<std::mutex> l(_state_map_lock);
        _runtime_filter_mgr_map[instance_idx] = std::move(runtime_filter_mgr);
    }
    return Status::OK();
}

Status PipelineFragmentContext::_build_pipeline_tasks(ThreadPool* thread_pool) {
    _total_tasks = 0;
    _closed_tasks = 0;
    const auto target_size = _params.local_params.size();
    _tasks.resize(target_size);
    _runtime_filter_mgr_map.resize(target_size);
    for (size_t pip_idx = 0; pip_idx < _pipelines.size(); pip_idx++) {
        _pip_id_to_pipeline[_pipelines[pip_idx]->id()] = _pipelines[pip_idx].get();
    }
    auto pipeline_id_to_profile = _runtime_state->build_pipeline_profile(_pipelines.size());

    if (target_size > 1 &&
        (_runtime_state->query_options().__isset.parallel_prepare_threshold &&
         target_size > _runtime_state->query_options().parallel_prepare_threshold)) {
        // If instances parallelism is big enough ( > parallel_prepare_threshold), we will prepare all tasks by multi-threads
        std::vector<Status> prepare_status(target_size);
        int submitted_tasks = 0;
        Status submit_status;
        CountDownLatch latch((int)target_size);
        for (int i = 0; i < target_size; i++) {
            submit_status = thread_pool->submit_func([&, i]() {
                SCOPED_ATTACH_TASK(_query_ctx.get());
                prepare_status[i] = _build_pipeline_tasks_for_instance(i, pipeline_id_to_profile);
                latch.count_down();
            });
            if (LIKELY(submit_status.ok())) {
                submitted_tasks++;
            } else {
                break;
            }
        }
        latch.arrive_and_wait(target_size - submitted_tasks);
        if (UNLIKELY(!submit_status.ok())) {
            return submit_status;
        }
        for (int i = 0; i < submitted_tasks; i++) {
            if (!prepare_status[i].ok()) {
                return prepare_status[i];
            }
        }
    } else {
        for (int i = 0; i < target_size; i++) {
            RETURN_IF_ERROR(_build_pipeline_tasks_for_instance(i, pipeline_id_to_profile));
        }
    }
    _pipeline_parent_map.clear();
    _op_id_to_shared_state.clear();
    // Record task cardinality once when this fragment context finishes task initialization.
    _query_ctx->add_total_task_num(_total_tasks.load(std::memory_order_relaxed));

    return Status::OK();
}

void PipelineFragmentContext::_init_next_report_time() {
    auto interval_s = config::pipeline_status_report_interval;
    if (_is_report_success && interval_s > 0 && _timeout > interval_s) {
        VLOG_FILE << "enable period report: fragment id=" << _fragment_id;
        uint64_t report_fragment_offset = (uint64_t)(rand() % interval_s) * NANOS_PER_SEC;
        // We don't want to wait longer than it takes to run the entire fragment.
        _previous_report_time =
                MonotonicNanos() + report_fragment_offset - (uint64_t)(interval_s)*NANOS_PER_SEC;
        _disable_period_report = false;
    }
}

void PipelineFragmentContext::refresh_next_report_time() {
    auto disable = _disable_period_report.load(std::memory_order_acquire);
    DCHECK(disable == true);
    _previous_report_time.store(MonotonicNanos(), std::memory_order_release);
    _disable_period_report.compare_exchange_strong(disable, false);
}

void PipelineFragmentContext::trigger_report_if_necessary() {
    if (!_is_report_success) {
        return;
    }
    auto disable = _disable_period_report.load(std::memory_order_acquire);
    if (disable) {
        return;
    }
    int32_t interval_s = config::pipeline_status_report_interval;
    if (interval_s <= 0) {
        LOG(WARNING) << "config::status_report_interval is equal to or less than zero, do not "
                        "trigger "
                        "report.";
    }
    uint64_t next_report_time = _previous_report_time.load(std::memory_order_acquire) +
                                (uint64_t)(interval_s)*NANOS_PER_SEC;
    if (MonotonicNanos() > next_report_time) {
        if (!_disable_period_report.compare_exchange_strong(disable, true,
                                                            std::memory_order_acq_rel)) {
            return;
        }
        if (VLOG_FILE_IS_ON) {
            VLOG_FILE << "Reporting "
                      << "profile for query_id " << print_id(_query_id)
                      << ", fragment id: " << _fragment_id;

            std::stringstream ss;
            _runtime_state->runtime_profile()->compute_time_in_profile();
            _runtime_state->runtime_profile()->pretty_print(&ss);
            if (_runtime_state->load_channel_profile()) {
                _runtime_state->load_channel_profile()->pretty_print(&ss);
            }

            VLOG_FILE << "Query " << print_id(get_query_id()) << " fragment " << get_fragment_id()
                      << " profile:\n"
                      << ss.str();
        }
        auto st = send_report(false);
        if (!st.ok()) {
            disable = true;
            _disable_period_report.compare_exchange_strong(disable, false,
                                                           std::memory_order_acq_rel);
        }
    }
}
// 将前端（FE）发送过来的 Thrift 逻辑执行计划节点树（plan.nodes）递归构建/拆解为 BE 物理算子链和 Pipeline 拓扑结构的核心入口函数。
// ObjectPool* pool（输入参数） 用于统一管理构建过程中动态创建的各种算子（Operator）、表达式以及辅助对象的生命周期，方便后续在 Fragment 销毁时统一释放资源。
// const DescriptorTbl& descs（输入参数） 包含当前查询用到的所有表、元组（Tuple）以及列（Slot）的描述信息（TupleDescriptor / SlotDescriptor），算子初始化时需要通过它来确定输入输出数据格式。
// OperatorPtr* root（输出参数） 用于接收并带回整棵物理执行计划树的根算子（Root Operator）指针。
// PipelinePtr cur_pipe（输入/输出参数） 传入当前正在构建的 Pipeline 对象（通常初始时是 _build_and_prepare_full_pipeline 中创建的根管道 root_pipeline）。在递归构建过程中，算子会被不断追加到该管道中；若遇到 Blocking 算子则以此为基础切分出新管道。
Status PipelineFragmentContext::_build_pipelines(ObjectPool* pool, const DescriptorTbl& descs,
                                                 OperatorPtr* root, PipelinePtr cur_pipe) {
    // 检查 Thrift 序列化结构 _params.fragment.plan.nodes（包含当前 Fragment 所有的 PlanNode 逻辑节点列表）是否为空。
    if (_params.fragment.plan.nodes.empty()) {
        throw Exception(ErrorCode::INTERNAL_ERROR, "Invalid plan which has no plan node!");
    }

    int node_idx = 0;
    // FE 在传递 plan.nodes 数组时，采用的是先序遍历（Pre-order Traversal） 方式展开的树形结构。
    // 在递归重建物理树时，node_idx 会作为传引用/指针参数，在递归过程中不断自增，从而按顺序消费 plan.nodes 中的每个逻辑节点。
    RETURN_IF_ERROR(_create_tree_helper(pool, _params.fragment.plan.nodes, descs, nullptr,
                                        &node_idx, root, cur_pipe, 0, false, false));
    // 计划树节点完整性校验（Completeness Check）
    if (node_idx + 1 != _params.fragment.plan.nodes.size()) {
        return Status::InternalError(
                "Plan tree only partially reconstructed. Not all thrift nodes were used.");
    }
    return Status::OK();
}

Status PipelineFragmentContext::_create_deferred_local_exchangers() {
    for (auto& info : _deferred_exchangers) {
        // DANGER ZONE — do not "fix" this line without reading the history.
        //
        // sender_count seeds Exchanger::_running_sink_operators, which the source side
        // waits to reach 0 via sub_running_sink_operators on each sink LocalState close.
        // The correct value is THIS pipeline-instance's sink task count, which is exactly
        // info.upstream_pipe->num_tasks() — one PipelineTask per task, one close per task.
        //
        // Tempting wrong fix #1: `std::max(num_tasks, _num_instances)` to mirror the
        //   BE-planned path in _add_local_exchange_impl (~line 1023).  THIS BREAKS the
        //   common FE-planned shape of `serial scan → LE(PT) → ...`: upstream_pipe
        //   genuinely has num_tasks=1, only 1 close arrives, but seed becomes
        //   _num_instances so _running_sink_operators never reaches 0 — downstream
        //   sources hang on SHUFFLE_DATA_DEPENDENCY (e.g. MTMV refresh from
        //   mtmv_up_down_job_p0/load.groovy stays at Status=RUNNING and regressed
        //   exactly this way).  BE-planned mode uses max() because its
        //   `cur_pipe` is the source-side pipeline (always raised to _num_instances by
        //   add_pipeline) — not analogous to our `upstream_pipe` here, which is the
        //   sink-side pipeline that may legitimately stay at 1 for serial sources.
        //
        // Tempting wrong fix #2: multiply by _num_instances on the theory shared_state
        //   is shared across all instances.  Same hang — each fragment-instance
        //   PipelineFragmentContext has its OWN _op_id_to_shared_state map, so the
        //   exchanger is per-instance, not per-BE.  num_tasks() is already the right
        //   close-count for one instance.
        //
        // If a hang shows up with `_running_sink_operators < 0`, the bug is upstream:
        // _propagate_local_exchange_num_tasks left num_tasks too low (or too high) for
        // this fragment shape.  Fix THAT pass, not this seed value.
        const int sender_count = info.upstream_pipe->num_tasks();
        switch (info.partition_type) {
        case TLocalPartitionType::LOCAL_EXECUTION_HASH_SHUFFLE:
        case TLocalPartitionType::GLOBAL_EXECUTION_HASH_SHUFFLE:
            info.shared_state->exchanger = ShuffleExchanger::create_unique(
                    sender_count, _num_instances, info.num_partitions, info.free_blocks_limit,
                    info.partition_type);
            break;
        case TLocalPartitionType::BUCKET_HASH_SHUFFLE:
            info.shared_state->exchanger = BucketShuffleExchanger::create_unique(
                    sender_count, _num_instances, info.num_partitions, info.free_blocks_limit);
            break;
        case TLocalPartitionType::PASSTHROUGH:
            info.shared_state->exchanger = PassthroughExchanger::create_unique(
                    sender_count, _num_instances, info.free_blocks_limit);
            break;
        case TLocalPartitionType::BROADCAST:
            info.shared_state->exchanger = BroadcastExchanger::create_unique(
                    sender_count, _num_instances, info.free_blocks_limit);
            break;
        case TLocalPartitionType::PASS_TO_ONE:
            if (_runtime_state->enable_share_hash_table_for_broadcast_join()) {
                info.shared_state->exchanger = PassToOneExchanger::create_unique(
                        sender_count, _num_instances, info.free_blocks_limit);
            } else {
                info.shared_state->exchanger = BroadcastExchanger::create_unique(
                        sender_count, _num_instances, info.free_blocks_limit);
            }
            break;
        case TLocalPartitionType::ADAPTIVE_PASSTHROUGH:
            info.shared_state->exchanger = AdaptivePassthroughExchanger::create_unique(
                    sender_count, _num_instances, info.free_blocks_limit);
            break;
        case TLocalPartitionType::NOOP:
        case TLocalPartitionType::LOCAL_MERGE_SORT:
            // FE-planned LocalExchangeNode currently never emits NOOP or LOCAL_MERGE_SORT
            // through the deferred-exchanger path.  NOOP means "no exchange needed" and
            // is filtered out before reaching here; LOCAL_MERGE_SORT is planned by the
            // legacy BE path only.  Crash in debug to surface the protocol violation if
            // that ever changes; return an error in release to avoid silently corrupting
            // execution.
            DCHECK(false) << "FE-planned local exchange should not emit partition_type="
                          << static_cast<int>(info.partition_type);
            return Status::InternalError("FE-planned local exchange emitted unsupported type: " +
                                         std::to_string(static_cast<int>(info.partition_type)));
        default:
            // New TLocalPartitionType added on FE side without a BE handler here.
            DCHECK(false) << "Unhandled TLocalPartitionType in deferred exchangers: "
                          << static_cast<int>(info.partition_type);
            return Status::InternalError("Unsupported FE-planned local exchange type: " +
                                         std::to_string(static_cast<int>(info.partition_type)));
        }
    }
    _deferred_exchangers.clear();
    return Status::OK();
}

void PipelineFragmentContext::_propagate_local_exchange_num_tasks() {
    // Only runs when FE has planned local exchanges and BE deferred their construction.
    // In legacy mode (enable_local_shuffle_planner=false) BE plans LE itself via
    // _plan_local_exchange and _deferred_exchangers stays empty — the legacy path
    // already gets its num_tasks right at construction time, so the propagate passes
    // would be no-ops and are skipped.  This is a transitional design: once the FE
    // planner is the only planner, the propagation logic itself should degrade into
    // a pure assertion that the FE plan already wired the right num_tasks everywhere.
    if (_deferred_exchangers.empty()) {
        return;
    }
    // Reconcile num_tasks across paired pipelines created by pipeline-splitting operators
    // (AGG, SORT, JOIN): they share state via inject_shared_state and must agree, or
    // instance 1+ tasks access null shared_state.  A pipeline's num_tasks is fully
    // determined by its source operator plus its upstreams:
    //   - LocalExchangeSource  -> _num_instances (the LE re-parallelizes)
    //   - serial source        -> its reduced count (kept as-is, typically 1)
    //   - otherwise (splitter) -> inherit from upstreams: raise to _num_instances if any
    //                             upstream was raised by an LE, then lower to a serial
    //                             upstream's count (lower wins).
    // Visiting each pipeline only after all its upstreams (topological order over _dag) lets
    // a single sweep reach the same fixpoint the previous two while-loops iterated to — those
    // only existed to reconcile the top-down build's parent-inherited num_tasks guesses.
    std::map<PipelineId, PipelinePtr> id_to_pipe;
    std::map<PipelineId, std::vector<PipelineId>> downstreams_of;
    std::map<PipelineId, int> in_degree;
    for (auto& p : _pipelines) {
        id_to_pipe[p->id()] = p;
        in_degree.try_emplace(p->id(), 0);
    }
    for (const auto& [downstream_id, upstream_ids] : _dag) {
        for (auto upstream_id : upstream_ids) {
            downstreams_of[upstream_id].push_back(downstream_id);
            in_degree[downstream_id]++;
        }
    }
    std::vector<PipelineId> ready;
    for (const auto& [id, deg] : in_degree) {
        if (deg == 0) {
            ready.push_back(id);
        }
    }
    size_t visited = 0;
    while (!ready.empty()) {
        const auto id = ready.back();
        ready.pop_back();
        visited++;
        auto pit = id_to_pipe.find(id);
        if (pit != id_to_pipe.end()) {
            auto& pipe = pit->second;
            const auto& ops = pipe->operators();
            const bool le_source =
                    !ops.empty() && dynamic_cast<LocalExchangeSourceOperatorX*>(ops.front().get());
            const bool serial_source = !ops.empty() && ops.front()->is_serial_operator();
            if (le_source) {
                pipe->set_num_tasks(_num_instances);
            } else if (!serial_source) {
                int target = pipe->num_tasks();
                const auto up_it = _dag.find(id);
                if (up_it != _dag.end()) {
                    // raise: any upstream already at _num_instances (e.g. an LE source)
                    for (auto upstream_id : up_it->second) {
                        auto uit = id_to_pipe.find(upstream_id);
                        if (uit != id_to_pipe.end() && uit->second->num_tasks() >= _num_instances) {
                            target = _num_instances;
                            break;
                        }
                    }
                    // lower: a serial upstream with fewer tasks (wins over the raise above)
                    for (auto upstream_id : up_it->second) {
                        auto uit = id_to_pipe.find(upstream_id);
                        if (uit != id_to_pipe.end() && uit->second->num_tasks() < target &&
                            !uit->second->operators().empty() &&
                            uit->second->operators().front()->is_serial_operator()) {
                            target = uit->second->num_tasks();
                        }
                    }
                }
                pipe->set_num_tasks(target);
            }
        }
        for (auto down : downstreams_of[id]) {
            if (--in_degree[down] == 0) {
                ready.push_back(down);
            }
        }
    }
    // The pipeline DAG is acyclic; if a future change introduces a back-edge, some pipelines
    // stay unvisited (in_degree never reaches 0) — fail loudly rather than silently leaving
    // their num_tasks unreconciled.
    DCHECK_EQ(visited, in_degree.size())
            << "pipeline num_tasks topological sweep visited " << visited << " of "
            << in_degree.size() << " pipelines (cycle in _dag?)";
}
// 根据前端（FE）发送的先序遍历（Pre-order Traversal）TPlanNode 节点数组，深度优先递归地创建物理算子（Operator）、建立父子算子层级关系，并向上向下传播数据分布约束（如 Shuffled 分布、Bucket/Colocated 分布）。
// ObjectPool* pool：内存对象池指针，用于管理动态创建的算子、表达式等对象的生命周期。
// const std::vector<TPlanNode>& tnodes：前端传递过来的当前 Fragment 的逻辑执行计划节点数组（按先序遍历排列）。
// const DescriptorTbl& descs：全局描述符表，包含元组（Tuple）和列（Slot）的 Schema 元数据。
// OperatorPtr parent：当前节点的父物理算子指针。若构建的是根节点，则为 nullptr。
// int* node_idx：指针类型，指向当前正在处理的 tnodes 数组的索引下标。在递归深度优先遍历过程中递增。
// OperatorPtr* root：输出参数，用于带回整棵算子树的根节点算子（仅在处理根节点时赋值）。
// PipelinePtr& cur_pipe：引用类型，指向当前正在构建的 Pipeline 对象。
// int child_idx：当前节点在其父算子的所有子节点中的下标位置（例如 Join 算子的左子树为 0，右子树为 1）。
// const bool followed_by_shuffled_operator：布尔标志，指示当前算子下游是否存在 Shuffle 类的算子（如 Shuffled Hash Join）。
// const bool require_bucket_distribution：布尔标志，指示当前算子下游是否要求 Bucket/Colocated 级别的特定数据分布。
// followed_by_shuffled_operator 解决的痛点问题
// 在多核/多线程并发执行的 Pipeline 架构中，为了充分利用多 CPU 核心，系统经常需要在管道内部插入 LocalExchange（本地数据重分配/本地 Shuffle），将数据均匀打散到不同的并行线程执行（如 PassThrough、Hash Shuffle 等）。
// 但是，如果下游存在一个 Shuffled Hash Join，它的左右两表必须使用完全一致的 Hash 函数和 Hash 槽位数进行数据路由（例如按照 Join Key 进行 Hash），Join 的 Build 端和 Probe 端才能在对应的线程里匹配到数据。
// 如果下游有 Shuffled Hash Join，但上游的 LocalExchange 不知道这个信息，随便选了一个普通的 Hash 函数或简单的轮询（Passthrough）打散数据，就会导致：
// 数据分布错位：数据被发到了错误的 Worker 线程，导致 Join 匹配不到正确的行，查询结果出错。
// 重复/无效 Shuffle：上游打散了一次，下游发现数据分布不对又被迫再做一次额外 Shuffle，产生性能开销。
Status PipelineFragmentContext::_create_tree_helper(
        ObjectPool* pool, const std::vector<TPlanNode>& tnodes, const DescriptorTbl& descs,
        OperatorPtr parent, int* node_idx, OperatorPtr* root, PipelinePtr& cur_pipe, int child_idx,
        const bool followed_by_shuffled_operator, const bool require_bucket_distribution) {
    // propagate error case
    // 数组边界防御性校验
    // 检查指针 *node_idx 是否超出了 tnodes 数组的范围。如果越界，说明 Thrift 结构损坏，直接抛出 InternalError；校验通过后获取当前待处理的 Thrift 逻辑节点 tnode。
    if (*node_idx >= tnodes.size()) {
        return Status::InternalError(
                "Failed to reconstruct plan tree from thrift. Node id: {}, number of nodes: {}",
                *node_idx, tnodes.size());
    }
    const TPlanNode& tnode = tnodes[*node_idx];
    // 获取当前逻辑节点的子节点数量（例如 Scan 为 0，Agg 为 1，Join 为 2）。
    int num_children = tnodes[*node_idx].num_children;
    // 初始化继承自上游/父节点的分布属性标志，后续会结合当前算子特性进行更新并传给子节点。
    bool current_followed_by_shuffled_operator = followed_by_shuffled_operator;
    bool current_require_bucket_distribution = require_bucket_distribution;
    // TODO: Create CacheOperator is confused now
    OperatorPtr op = nullptr;
    OperatorPtr cache_op = nullptr;
    // 调用工厂方法 _create_operator，根据 tnode 的类型（如 Agg、Join、Scan 等）实例化具体的物理算子对象 op（以及可能的缓存算子 cache_op），
    // 并根据算子特性（如 Blocking 算子）动态切分或调整 cur_pipe。
    RETURN_IF_ERROR(_create_operator(pool, tnodes[*node_idx], descs, op, cur_pipe,
                                     parent == nullptr ? -1 : parent->node_id(), child_idx,
                                     followed_by_shuffled_operator,
                                     current_require_bucket_distribution, cache_op));
    // Initialization must be done here. For example, group by expressions in agg will be used to
    // decide if a local shuffle should be planed, so it must be initialized here.
    // 调用算子的 init 方法解析表达式、配置参数等。
    RETURN_IF_ERROR(op->init(tnode, _runtime_state.get()));
    // assert(parent != nullptr || (node_idx == 0 && root_expr != nullptr));
    // 建立父子算子树状连接
    if (parent != nullptr) {
        // add to parent's child(s)
        RETURN_IF_ERROR(parent->set_child(cache_op ? cache_op : op));
    } else {
    // 如果 parent 为空，说明当前算子是整个 Fragment 执行计划树的根算子，将其赋值给输出参数 *root。
        *root = op;
    }
    /**
     * `TLocalPartitionType::GLOBAL_EXECUTION_HASH_SHUFFLE` should be used if an operator is followed by a shuffled operator (shuffled hash join, union operator followed by co-located operators).
     *
     * For plan:
     * LocalExchange(id=0) -> Aggregation(id=1) -> ShuffledHashJoin(id=2)
     *                           Exchange(id=3) -> ShuffledHashJoinBuild(id=2)
     * We must ensure data distribution of `LocalExchange(id=0)` is same as Exchange(id=3).
     *
     * If an operator's is followed by a local exchange without shuffle (e.g. passthrough), a
     * shuffled local exchanger will be used before join so it is not followed by shuffle join.
     */
    // 计算并向下传播数据分布标志（Shuffle & Bucket Distribution）
    // 获取当前位置要求的数据分布（如果当前管道内没有算子，取 Sink 的分布要求；否则取当前算子 op 的分布要求）。
    auto required_data_distribution =
            cur_pipe->operators().empty()
                    ? cur_pipe->sink()->required_data_distribution(_runtime_state.get())
                    : op->required_data_distribution(_runtime_state.get());
    // 判断当前算子/Sink 本身是否是 Shuffled 算子（如 Shuffled Hash Join），或者上游传递了该标志；
    // 同时结合 required_data_distribution 是否为 Hash Exchange 或 NOOP，确定数据在经过当前算子后，是否需要保持/满足全局 Hash Shuffle 的分布要求（防止上游 Local Exchange 误打散 Hash 分区数据）
    current_followed_by_shuffled_operator =
            ((followed_by_shuffled_operator ||
              (cur_pipe->operators().empty() ? cur_pipe->sink()->is_shuffled_operator()
                                             : op->is_shuffled_operator())) &&
             Pipeline::is_hash_exchange(required_data_distribution.distribution_type)) ||
            (followed_by_shuffled_operator &&
             required_data_distribution.distribution_type == TLocalPartitionType::NOOP);

    // 判断当前算子是否是 Colocated 算子（如 Bucket Shuffle Join），确定是否需要将特定的 Bucket 数据分布要求传递给子节点。
    current_require_bucket_distribution =
            ((require_bucket_distribution ||
              (cur_pipe->operators().empty() ? cur_pipe->sink()->is_colocated_operator()
                                             : op->is_colocated_operator())) &&
             Pipeline::is_hash_exchange(required_data_distribution.distribution_type)) ||
            (require_bucket_distribution &&
             required_data_distribution.distribution_type == TLocalPartitionType::NOOP);
    // 如果当前算子没有子节点（num_children == 0，即处于树的叶子节点，如 ScanOperator 或 ExchangeSourceOperator），
    // 将其是否为单线程串行算子（is_serial_operator()）记录到 Fragment 上下文变量 _use_serial_source 中。
    if (num_children == 0) {
        _use_serial_source = op->is_serial_operator();
    }
    // rely on that tnodes is preorder of the plan
    // 递归构建所有子节点（Children Traversal）
    for (int i = 0; i < num_children; i++) {
        // 由于 tnodes 是先序遍历数组，递增 *node_idx 指向下一个子节点的 Thrift 定义。
        ++*node_idx;
        RETURN_IF_ERROR(_create_tree_helper(pool, tnodes, descs, op, node_idx, nullptr, cur_pipe, i,
                                            current_followed_by_shuffled_operator,
                                            current_require_bucket_distribution));

        // we are expecting a child, but have used all nodes
        // this means we have been given a bad tree and must fail
        if (*node_idx >= tnodes.size()) {
            return Status::InternalError(
                    "Failed to reconstruct plan tree from thrift. Node id: {}, number of "
                    "nodes: {}",
                    *node_idx, tnodes.size());
        }
    }

    return Status::OK();
}

void PipelineFragmentContext::_inherit_pipeline_properties(
        const DataDistribution& data_distribution, PipelinePtr pipe_with_source,
        PipelinePtr pipe_with_sink) {
    pipe_with_sink->set_num_tasks(pipe_with_source->num_tasks());
    pipe_with_source->set_num_tasks(_num_instances);
    pipe_with_source->set_data_distribution(data_distribution);
}

Status PipelineFragmentContext::_add_local_exchange_impl(
        int idx, ObjectPool* pool, PipelinePtr cur_pipe, PipelinePtr new_pip,
        DataDistribution data_distribution, bool* do_local_exchange, int num_buckets,
        const std::map<int, int>& bucket_seq_to_instance_idx,
        const std::map<int, int>& shuffle_idx_to_instance_idx) {
    auto& operators = cur_pipe->operators();
    const auto downstream_pipeline_id = cur_pipe->id();
    auto local_exchange_id = next_operator_id();
    // 1. Create a new pipeline with local exchange sink.
    DataSinkOperatorPtr sink;
    auto sink_id = next_sink_operator_id();

    /**
     * `bucket_seq_to_instance_idx` is empty if no scan operator is contained in this fragment.
     * So co-located operators(e.g. Agg, Analytic) should use `HASH_SHUFFLE` instead of `BUCKET_HASH_SHUFFLE`.
     */
    const bool followed_by_shuffled_operator =
            operators.size() > idx ? operators[idx]->followed_by_shuffled_operator()
                                   : cur_pipe->sink()->followed_by_shuffled_operator();
    const bool use_global_hash_shuffle = bucket_seq_to_instance_idx.empty() &&
                                         !shuffle_idx_to_instance_idx.contains(-1) &&
                                         followed_by_shuffled_operator && !_use_serial_source;
    sink = std::make_shared<LocalExchangeSinkOperatorX>(
            sink_id, local_exchange_id, use_global_hash_shuffle ? _total_instances : _num_instances,
            data_distribution.partition_exprs, bucket_seq_to_instance_idx);
    if (bucket_seq_to_instance_idx.empty() &&
        data_distribution.distribution_type == TLocalPartitionType::BUCKET_HASH_SHUFFLE) {
        data_distribution.distribution_type =
                use_global_hash_shuffle ? TLocalPartitionType::GLOBAL_EXECUTION_HASH_SHUFFLE
                                        : TLocalPartitionType::LOCAL_EXECUTION_HASH_SHUFFLE;
    }
    if (!use_global_hash_shuffle &&
        data_distribution.distribution_type == TLocalPartitionType::GLOBAL_EXECUTION_HASH_SHUFFLE) {
        data_distribution.distribution_type = TLocalPartitionType::LOCAL_EXECUTION_HASH_SHUFFLE;
    }
    RETURN_IF_ERROR(new_pip->set_sink(sink));
    RETURN_IF_ERROR(new_pip->sink()->init(_runtime_state.get(), data_distribution.distribution_type,
                                          num_buckets, shuffle_idx_to_instance_idx));

    // 2. Create and initialize LocalExchangeSharedState.
    std::shared_ptr<LocalExchangeSharedState> shared_state =
            LocalExchangeSharedState::create_shared(_num_instances);
    switch (data_distribution.distribution_type) {
    case TLocalPartitionType::LOCAL_EXECUTION_HASH_SHUFFLE:
    case TLocalPartitionType::GLOBAL_EXECUTION_HASH_SHUFFLE:
        shared_state->exchanger = ShuffleExchanger::create_unique(
                std::max(cur_pipe->num_tasks(), _num_instances), _num_instances,
                use_global_hash_shuffle ? _total_instances : _num_instances,
                _runtime_state->query_options().__isset.local_exchange_free_blocks_limit
                        ? cast_set<int>(
                                  _runtime_state->query_options().local_exchange_free_blocks_limit)
                        : 0,
                data_distribution.distribution_type);
        break;
    case TLocalPartitionType::BUCKET_HASH_SHUFFLE:
        shared_state->exchanger = BucketShuffleExchanger::create_unique(
                std::max(cur_pipe->num_tasks(), _num_instances), _num_instances, num_buckets,
                _runtime_state->query_options().__isset.local_exchange_free_blocks_limit
                        ? cast_set<int>(
                                  _runtime_state->query_options().local_exchange_free_blocks_limit)
                        : 0);
        break;
    case TLocalPartitionType::PASSTHROUGH:
        shared_state->exchanger = PassthroughExchanger::create_unique(
                cur_pipe->num_tasks(), _num_instances,
                _runtime_state->query_options().__isset.local_exchange_free_blocks_limit
                        ? cast_set<int>(
                                  _runtime_state->query_options().local_exchange_free_blocks_limit)
                        : 0);
        break;
    case TLocalPartitionType::BROADCAST:
        shared_state->exchanger = BroadcastExchanger::create_unique(
                cur_pipe->num_tasks(), _num_instances,
                _runtime_state->query_options().__isset.local_exchange_free_blocks_limit
                        ? cast_set<int>(
                                  _runtime_state->query_options().local_exchange_free_blocks_limit)
                        : 0);
        break;
    case TLocalPartitionType::PASS_TO_ONE:
        if (_runtime_state->enable_share_hash_table_for_broadcast_join()) {
            // If shared hash table is enabled for BJ, hash table will be built by only one task
            shared_state->exchanger = PassToOneExchanger::create_unique(
                    cur_pipe->num_tasks(), _num_instances,
                    _runtime_state->query_options().__isset.local_exchange_free_blocks_limit
                            ? cast_set<int>(_runtime_state->query_options()
                                                    .local_exchange_free_blocks_limit)
                            : 0);
        } else {
            shared_state->exchanger = BroadcastExchanger::create_unique(
                    cur_pipe->num_tasks(), _num_instances,
                    _runtime_state->query_options().__isset.local_exchange_free_blocks_limit
                            ? cast_set<int>(_runtime_state->query_options()
                                                    .local_exchange_free_blocks_limit)
                            : 0);
        }
        break;
    case TLocalPartitionType::ADAPTIVE_PASSTHROUGH:
        shared_state->exchanger = AdaptivePassthroughExchanger::create_unique(
                std::max(cur_pipe->num_tasks(), _num_instances), _num_instances,
                _runtime_state->query_options().__isset.local_exchange_free_blocks_limit
                        ? cast_set<int>(
                                  _runtime_state->query_options().local_exchange_free_blocks_limit)
                        : 0);
        break;
    default:
        return Status::InternalError("Unsupported local exchange type : " +
                                     std::to_string((int)data_distribution.distribution_type));
    }
    shared_state->create_source_dependencies(_num_instances, local_exchange_id, local_exchange_id,
                                             "LOCAL_EXCHANGE_OPERATOR");
    shared_state->create_sink_dependency(sink_id, local_exchange_id, "LOCAL_EXCHANGE_SINK");
    _op_id_to_shared_state.insert({local_exchange_id, {shared_state, shared_state->sink_deps}});

    // 3. Set two pipelines' operator list. For example, split pipeline [Scan - AggSink] to
    // pipeline1 [Scan - LocalExchangeSink] and pipeline2 [LocalExchangeSource - AggSink].

    // 3.1 Initialize new pipeline's operator list.
    std::copy(operators.begin(), operators.begin() + idx,
              std::inserter(new_pip->operators(), new_pip->operators().end()));

    // 3.2 Erase unused operators in previous pipeline.
    operators.erase(operators.begin(), operators.begin() + idx);

    // 4. Initialize LocalExchangeSource and insert it into this pipeline.
    OperatorPtr source_op;
    source_op = std::make_shared<LocalExchangeSourceOperatorX>(pool, local_exchange_id);
    RETURN_IF_ERROR(source_op->set_child(new_pip->operators().back()));
    RETURN_IF_ERROR(source_op->init(data_distribution.distribution_type));
    if (!operators.empty()) {
        RETURN_IF_ERROR(operators.front()->set_child(nullptr));
        RETURN_IF_ERROR(operators.front()->set_child(source_op));
    }
    operators.insert(operators.begin(), source_op);

    // 5. Set children for two pipelines separately.
    std::vector<std::shared_ptr<Pipeline>> new_children;
    std::vector<PipelineId> edges_with_source;
    for (auto child : cur_pipe->children()) {
        bool found = false;
        for (auto op : new_pip->operators()) {
            if (child->sink()->node_id() == op->node_id()) {
                new_pip->set_children(child);
                found = true;
            };
        }
        if (!found) {
            new_children.push_back(child);
            edges_with_source.push_back(child->id());
        }
    }
    new_children.push_back(new_pip);
    edges_with_source.push_back(new_pip->id());

    // 6. Set DAG for new pipelines.
    if (!new_pip->children().empty()) {
        std::vector<PipelineId> edges_with_sink;
        for (auto child : new_pip->children()) {
            edges_with_sink.push_back(child->id());
        }
        _dag.insert({new_pip->id(), edges_with_sink});
    }
    cur_pipe->set_children(new_children);
    _dag[downstream_pipeline_id] = edges_with_source;
    RETURN_IF_ERROR(new_pip->sink()->set_child(new_pip->operators().back()));
    RETURN_IF_ERROR(cur_pipe->sink()->set_child(nullptr));
    RETURN_IF_ERROR(cur_pipe->sink()->set_child(cur_pipe->operators().back()));

    // 7. Inherit properties from current pipeline.
    _inherit_pipeline_properties(data_distribution, cur_pipe, new_pip);
    return Status::OK();
}

Status PipelineFragmentContext::_add_local_exchange(
        int pip_idx, int idx, int node_id, ObjectPool* pool, PipelinePtr cur_pipe,
        DataDistribution data_distribution, bool* do_local_exchange, int num_buckets,
        const std::map<int, int>& bucket_seq_to_instance_idx,
        const std::map<int, int>& shuffle_idx_to_instance_idx) {
    if (_num_instances <= 1 || cur_pipe->num_tasks_of_parent() <= 1) {
        return Status::OK();
    }

    if (!cur_pipe->need_to_local_exchange(data_distribution, idx)) {
        return Status::OK();
    }
    *do_local_exchange = true;

    auto& operators = cur_pipe->operators();
    auto total_op_num = operators.size();
    auto new_pip = add_pipeline(cur_pipe, pip_idx + 1);
    RETURN_IF_ERROR(_add_local_exchange_impl(
            idx, pool, cur_pipe, new_pip, data_distribution, do_local_exchange, num_buckets,
            bucket_seq_to_instance_idx, shuffle_idx_to_instance_idx));

    CHECK(total_op_num + 1 == cur_pipe->operators().size() + new_pip->operators().size())
            << "total_op_num: " << total_op_num
            << " cur_pipe->operators().size(): " << cur_pipe->operators().size()
            << " new_pip->operators().size(): " << new_pip->operators().size();

    // There are some local shuffles with relatively heavy operations on the sink.
    // If the local sink concurrency is 1 and the local source concurrency is n, the sink becomes a bottleneck.
    // Therefore, local passthrough is used to increase the concurrency of the sink.
    // op -> local sink(1) -> local source (n)
    // op -> local passthrough(1) -> local passthrough(n) ->  local sink(n) -> local source (n)
    if (cur_pipe->num_tasks() > 1 && new_pip->num_tasks() == 1 &&
        Pipeline::heavy_operations_on_the_sink(data_distribution.distribution_type)) {
        RETURN_IF_ERROR(_add_local_exchange_impl(
                cast_set<int>(new_pip->operators().size()), pool, new_pip,
                add_pipeline(new_pip, pip_idx + 2),
                DataDistribution(TLocalPartitionType::PASSTHROUGH), do_local_exchange, num_buckets,
                bucket_seq_to_instance_idx, shuffle_idx_to_instance_idx));
    }
    return Status::OK();
}

Status PipelineFragmentContext::_plan_local_exchange(
        int num_buckets, const std::map<int, int>& bucket_seq_to_instance_idx,
        const std::map<int, int>& shuffle_idx_to_instance_idx) {
    for (int pip_idx = cast_set<int>(_pipelines.size()) - 1; pip_idx >= 0; pip_idx--) {
        _pipelines[pip_idx]->init_data_distribution(_runtime_state.get());
        // Set property if child pipeline is not join operator's child.
        if (!_pipelines[pip_idx]->children().empty()) {
            for (auto& child : _pipelines[pip_idx]->children()) {
                if (child->sink()->node_id() ==
                    _pipelines[pip_idx]->operators().front()->node_id()) {
                    _pipelines[pip_idx]->set_data_distribution(child->data_distribution());
                }
            }
        }

        // if 'num_buckets == 0' means the fragment is colocated by exchange node not the
        // scan node. so here use `_num_instance` to replace the `num_buckets` to prevent dividing 0
        // still keep colocate plan after local shuffle
        RETURN_IF_ERROR(_plan_local_exchange(num_buckets, pip_idx, _pipelines[pip_idx],
                                             bucket_seq_to_instance_idx,
                                             shuffle_idx_to_instance_idx));
    }
    return Status::OK();
}

Status PipelineFragmentContext::_plan_local_exchange(
        int num_buckets, int pip_idx, PipelinePtr pip,
        const std::map<int, int>& bucket_seq_to_instance_idx,
        const std::map<int, int>& shuffle_idx_to_instance_idx) {
    int idx = 1;
    bool do_local_exchange = false;
    do {
        auto& ops = pip->operators();
        do_local_exchange = false;
        // Plan local exchange for each operator.
        for (; idx < ops.size();) {
            auto _le_req = ops[idx]->required_data_distribution(_runtime_state.get());
            if (_le_req.need_local_exchange()) {
                RETURN_IF_ERROR(_add_local_exchange(
                        pip_idx, idx, ops[idx]->node_id(), _runtime_state->obj_pool(), pip, _le_req,
                        &do_local_exchange, num_buckets, bucket_seq_to_instance_idx,
                        shuffle_idx_to_instance_idx));
            }
            if (do_local_exchange) {
                // If local exchange is needed for current operator, we will split this pipeline to
                // two pipelines by local exchange sink/source. And then we need to process remaining
                // operators in this pipeline so we set idx to 2 (0 is local exchange source and 1
                // is current operator was already processed) and continue to plan local exchange.
                idx = 2;
                break;
            }
            idx++;
        }
    } while (do_local_exchange);
    if (pip->sink()->required_data_distribution(_runtime_state.get()).need_local_exchange()) {
        RETURN_IF_ERROR(_add_local_exchange(
                pip_idx, idx, pip->sink()->node_id(), _runtime_state->obj_pool(), pip,
                pip->sink()->required_data_distribution(_runtime_state.get()), &do_local_exchange,
                num_buckets, bucket_seq_to_instance_idx, shuffle_idx_to_instance_idx));
    }
    return Status::OK();
}

Status PipelineFragmentContext::_create_data_sink(ObjectPool* pool, const TDataSink& thrift_sink,
                                                  const std::vector<TExpr>& output_exprs,
                                                  const TPipelineFragmentParams& params,
                                                  const RowDescriptor& row_desc,
                                                  RuntimeState* state, DescriptorTbl& desc_tbl,
                                                  PipelineId cur_pipeline_id) {
    switch (thrift_sink.type) {
    case TDataSinkType::DATA_STREAM_SINK: {
        if (!thrift_sink.__isset.stream_sink) {
            return Status::InternalError("Missing data stream sink.");
        }
        _sink = std::make_shared<ExchangeSinkOperatorX>(
                state, row_desc, next_sink_operator_id(), thrift_sink.stream_sink,
                params.destinations, _fragment_instance_ids);
        break;
    }
    case TDataSinkType::RESULT_SINK: {
        if (!thrift_sink.__isset.result_sink) {
            return Status::InternalError("Missing data buffer sink.");
        }

        auto& pipeline = _pipelines[cur_pipeline_id];
        int child_node_id = pipeline->operators().back()->node_id();
        _sink = std::make_shared<ResultSinkOperatorX>(next_sink_operator_id(), child_node_id + 1,
                                                      row_desc, output_exprs,
                                                      thrift_sink.result_sink);
        break;
    }
    case TDataSinkType::DICTIONARY_SINK: {
        if (!thrift_sink.__isset.dictionary_sink) {
            return Status::InternalError("Missing dict sink.");
        }

        _sink = std::make_shared<DictSinkOperatorX>(next_sink_operator_id(), row_desc, output_exprs,
                                                    thrift_sink.dictionary_sink);
        break;
    }
    case TDataSinkType::GROUP_COMMIT_OLAP_TABLE_SINK:
    case TDataSinkType::OLAP_TABLE_SINK: {
        auto& pipeline = _pipelines[cur_pipeline_id];
        int child_node_id = pipeline->operators().back()->node_id();
        if (state->query_options().enable_memtable_on_sink_node &&
            !_has_inverted_index_v1_or_partial_update(thrift_sink.olap_table_sink) &&
            !_has_row_binlog(thrift_sink.olap_table_sink) && !config::is_cloud_mode()) {
            _sink = std::make_shared<OlapTableSinkV2OperatorX>(
                    pool, next_sink_operator_id(), child_node_id + 1, row_desc, output_exprs);
        } else {
            _sink = std::make_shared<OlapTableSinkOperatorX>(
                    pool, next_sink_operator_id(), child_node_id + 1, row_desc, output_exprs);
        }
        break;
    }
    case TDataSinkType::GROUP_COMMIT_BLOCK_SINK: {
        DCHECK(thrift_sink.__isset.olap_table_sink);
        DCHECK(state->get_query_ctx() != nullptr);
        state->get_query_ctx()->query_mem_tracker()->is_group_commit_load = true;
        _sink = std::make_shared<GroupCommitBlockSinkOperatorX>(next_sink_operator_id(), row_desc,
                                                                output_exprs);
        break;
    }
    case TDataSinkType::HIVE_TABLE_SINK: {
        if (!thrift_sink.__isset.hive_table_sink) {
            return Status::InternalError("Missing hive table sink.");
        }
        _sink = std::make_shared<HiveTableSinkOperatorX>(pool, next_sink_operator_id(), row_desc,
                                                         output_exprs);
        break;
    }
    case TDataSinkType::ICEBERG_TABLE_SINK: {
        if (!thrift_sink.__isset.iceberg_table_sink) {
            return Status::InternalError("Missing iceberg table sink.");
        }
        if (thrift_sink.iceberg_table_sink.__isset.sort_info) {
            _sink = std::make_shared<SpillIcebergTableSinkOperatorX>(pool, next_sink_operator_id(),
                                                                     row_desc, output_exprs);
        } else {
            _sink = std::make_shared<IcebergTableSinkOperatorX>(pool, next_sink_operator_id(),
                                                                row_desc, output_exprs);
        }
        break;
    }
    case TDataSinkType::ICEBERG_DELETE_SINK: {
        if (!thrift_sink.__isset.iceberg_delete_sink) {
            return Status::InternalError("Missing iceberg delete sink.");
        }
        _sink = std::make_shared<IcebergDeleteSinkOperatorX>(pool, next_sink_operator_id(),
                                                             row_desc, output_exprs);
        break;
    }
    case TDataSinkType::ICEBERG_MERGE_SINK: {
        if (!thrift_sink.__isset.iceberg_merge_sink) {
            return Status::InternalError("Missing iceberg merge sink.");
        }
        _sink = std::make_shared<IcebergMergeSinkOperatorX>(pool, next_sink_operator_id(), row_desc,
                                                            output_exprs);
        break;
    }
    case TDataSinkType::MAXCOMPUTE_TABLE_SINK: {
        if (!thrift_sink.__isset.max_compute_table_sink) {
            return Status::InternalError("Missing max compute table sink.");
        }
        _sink = std::make_shared<MCTableSinkOperatorX>(pool, next_sink_operator_id(), row_desc,
                                                       output_exprs);
        break;
    }
    case TDataSinkType::JDBC_TABLE_SINK: {
        if (!thrift_sink.__isset.jdbc_table_sink) {
            return Status::InternalError("Missing data jdbc sink.");
        }
        if (config::enable_java_support) {
            _sink = std::make_shared<JdbcTableSinkOperatorX>(row_desc, next_sink_operator_id(),
                                                             output_exprs);
        } else {
            return Status::InternalError(
                    "Jdbc table sink is not enabled, you can change be config "
                    "enable_java_support to true and restart be.");
        }
        break;
    }
    case TDataSinkType::MEMORY_SCRATCH_SINK: {
        if (!thrift_sink.__isset.memory_scratch_sink) {
            return Status::InternalError("Missing data buffer sink.");
        }

        _sink = std::make_shared<MemoryScratchSinkOperatorX>(row_desc, next_sink_operator_id(),
                                                             output_exprs);
        break;
    }
    case TDataSinkType::RESULT_FILE_SINK: {
        if (!thrift_sink.__isset.result_file_sink) {
            return Status::InternalError("Missing result file sink.");
        }

        // Result file sink is not the top sink
        if (params.__isset.destinations && !params.destinations.empty()) {
            _sink = std::make_shared<ResultFileSinkOperatorX>(
                    next_sink_operator_id(), row_desc, thrift_sink.result_file_sink,
                    params.destinations, output_exprs, desc_tbl);
        } else {
            _sink = std::make_shared<ResultFileSinkOperatorX>(next_sink_operator_id(), row_desc,
                                                              output_exprs);
        }
        break;
    }
    case TDataSinkType::MULTI_CAST_DATA_STREAM_SINK: {
        DCHECK(thrift_sink.__isset.multi_cast_stream_sink);
        DCHECK_GT(thrift_sink.multi_cast_stream_sink.sinks.size(), 0);
        auto sink_id = next_sink_operator_id();
        const int multi_cast_node_id = sink_id;
        auto sender_size = thrift_sink.multi_cast_stream_sink.sinks.size();
        // one sink has multiple sources.
        std::vector<int> sources;
        for (int i = 0; i < sender_size; ++i) {
            auto source_id = next_operator_id();
            sources.push_back(source_id);
        }

        _sink = std::make_shared<MultiCastDataStreamSinkOperatorX>(
                sink_id, multi_cast_node_id, sources, pool, thrift_sink.multi_cast_stream_sink);
        for (int i = 0; i < sender_size; ++i) {
            auto new_pipeline = add_pipeline();
            // use to exchange sink
            RowDescriptor* exchange_row_desc = nullptr;
            {
                const auto& tmp_row_desc =
                        !thrift_sink.multi_cast_stream_sink.sinks[i].output_exprs.empty()
                                ? RowDescriptor(state->desc_tbl(),
                                                {thrift_sink.multi_cast_stream_sink.sinks[i]
                                                         .output_tuple_id})
                                : row_desc;
                exchange_row_desc = pool->add(new RowDescriptor(tmp_row_desc));
            }
            auto source_id = sources[i];
            OperatorPtr source_op;
            // 1. create and set the source operator of multi_cast_data_stream_source for new pipeline
            source_op = std::make_shared<MultiCastDataStreamerSourceOperatorX>(
                    /*node_id*/ source_id, /*consumer_id*/ i, pool,
                    thrift_sink.multi_cast_stream_sink.sinks[i], row_desc,
                    /*operator_id=*/source_id);
            RETURN_IF_ERROR(new_pipeline->add_operator(
                    source_op, params.__isset.parallel_instances ? params.parallel_instances : 0));
            // 2. create and set sink operator of data stream sender for new pipeline

            DataSinkOperatorPtr sink_op;
            sink_op = std::make_shared<ExchangeSinkOperatorX>(
                    state, *exchange_row_desc, next_sink_operator_id(),
                    thrift_sink.multi_cast_stream_sink.sinks[i],
                    thrift_sink.multi_cast_stream_sink.destinations[i], _fragment_instance_ids);

            RETURN_IF_ERROR(new_pipeline->set_sink(sink_op));
            {
                TDataSink* t = pool->add(new TDataSink());
                t->stream_sink = thrift_sink.multi_cast_stream_sink.sinks[i];
                RETURN_IF_ERROR(sink_op->init(*t));
            }

            // 3. set dependency dag
            _dag[new_pipeline->id()].push_back(cur_pipeline_id);
        }
        if (sources.empty()) {
            return Status::InternalError("size of sources must be greater than 0");
        }
        break;
    }
    case TDataSinkType::BLACKHOLE_SINK: {
        if (!thrift_sink.__isset.blackhole_sink) {
            return Status::InternalError("Missing blackhole sink.");
        }

        _sink.reset(new BlackholeSinkOperatorX(next_sink_operator_id()));
        break;
    }
    case TDataSinkType::TVF_TABLE_SINK: {
        if (!thrift_sink.__isset.tvf_table_sink) {
            return Status::InternalError("Missing TVF table sink.");
        }
        _sink = std::make_shared<TVFTableSinkOperatorX>(pool, next_sink_operator_id(), row_desc,
                                                        output_exprs);
        break;
    }
    default:
        return Status::InternalError("Unsuported sink type in pipeline: {}", thrift_sink.type);
    }
    return Status::OK();
}

// NOLINTBEGIN(readability-function-size)
// NOLINTBEGIN(readability-function-cognitive-
// Pipeline 架构（Pipeline Execution Engine） 构建的核心入口之一
// 主要职责是：读取 FE（FrontEnd）下发的 Thrift 逻辑计划树节点（TPlanNode），构建对应的 BE（BackEnd）执行算子（OperatorX 和 DataSinkOperatorX），并将其编排到流水线（Pipeline）及其依赖图（DAG）中。
// ObjectPool* pool   1. 内存池，用于管理算子对象的生命周期
// const TPlanNode& tnode  2. 当前待解析的 Thrift 逻辑计划节点（FE 传给 BE）
// const DescriptorTbl& descs 3. 元数据描述符表（包含 Schema, Tuple, Slot 等信息）
// OperatorPtr& op 4. [输出参数] 构建出的 Source/Transform 算子指针
// PipelinePtr& cur_pipe  5. [输入/输出] 当前正在构建的 Pipeline
// int parent_idx  6. 父算子在树中的索引（用于匹配前序遍历结构）
// int child_idx  7. 子算子在树中的索引
// const bool followed_by_shuffled_operator  8. 标记后续是否跟有 Shuffle 算子（影响并行度/数据分布）
// const bool require_bucket_distribution   9. 标记是否要求按 Bucket 分布数据
// OperatorPtr& cache_op  10. [输出参数] 若开启 Query Cache，指向 Cache 算子
Status PipelineFragmentContext::_create_operator(ObjectPool* pool, const TPlanNode& tnode,
                                                 const DescriptorTbl& descs, OperatorPtr& op,
                                                 PipelinePtr& cur_pipe, int parent_idx,
                                                 int child_idx,
                                                 const bool followed_by_shuffled_operator,
                                                 const bool require_bucket_distribution,
                                                 OperatorPtr& cache_op) {
    std::vector<DataSinkOperatorPtr> sink_ops;
    // 使用 RAII Defer 模式：确保在此函数退出（包括 return 退出）前，调用算子的 update_operator 方法
    // 更新算子属性（如并发度、Shuffle 及 Bucket 分布要求）
    Defer defer = Defer([&]() {
        if (op) {
            op->update_operator(tnode, followed_by_shuffled_operator, require_bucket_distribution);
        }
        for (auto& s : sink_ops) {
            s->update_operator(tnode, followed_by_shuffled_operator, require_bucket_distribution);
        }
    });
    // We directly construct the operator from Thrift because the given array is in the order of preorder traversal.
    // Therefore, here we need to use a stack-like structure.
    // FE 传过来的 TPlanNode 数组是按【先序遍历（Preorder Traversal）】排列的。
    // 这里通过内部栈结构弹出/调整父子 Pipeline 的对应关系，更新 cur_pipe。
    _pipeline_parent_map.pop(cur_pipe, parent_idx, child_idx);
    std::stringstream error_msg;
    bool enable_query_cache = _params.fragment.__isset.query_cache_param;

    bool fe_with_old_version = false;
    switch (tnode.node_type) {
    // Scan 类型算子构建 (以 OLAP Scan 为例)
    // OLAP_SCAN_NODE（Olap 表扫描节点） 在 Pipeline 执行引擎中构建对应 OperatorX 的核心实现，重点处理了 Query Cache（查询缓存）的联动校验与初始化 以及 Binlog 扫描场景下的缓存失效逻辑。
    // 背景原理：FE 生成的计划树是按先序遍历（Pre-order Traversal）顺序发给 BE 进行解析构建的。在逻辑计划树中，Query Cache 的 Source/Cache 节点位于 ScanNode 的上层（祖先节点）。
    // 因此，当遍历解析到底层的 OLAP_SCAN_NODE 时，上层的 Query Cache 节点必须已经被解析完毕，并且创建好了全局共享的 _query_cache_runtime。
    // 为什么必须 fail loudly（直接抛错）：正如注释所述，如果在此处 _query_cache_runtime 缺失而让 Scan 节点盲目使用自己的默认 Runtime 运行，一旦发生 Cache HIT（缓存命中），Scan 节点会因为不知道缓存命中的状态而主动跳过底层数据扫描（Skip Scanning），同时又没有上层的 Cache Source 节点来吐出缓存数据，会导致查询结果静默丢失（Silently Drop Data）。这里通过 Status::InternalError 快速失败，防止出现严重的正确性 Bug。
    case TPlanNodeType::OLAP_SCAN_NODE: {
        if (enable_query_cache) {
            if (_query_cache_runtime == nullptr) {
                // The plan tree is built in pre-order and the cache source
                // sits above the scan, so the runtime it created must already
                // exist here. Running the scan with its own runtime instead
                // would silently drop data on a HIT (the scan skips scanning
                // while no cache source emits the entry), so a malformed plan
                // shape must fail loudly.
                return Status::InternalError(
                        "query cache runtime is absent at the scan node, node_id={}, "
                        "cache node_id={}",
                        tnode.node_id, _params.fragment.query_cache_param.node_id);
            }
            // 如果当前 Olap Scan 被标记为读取 Row Binlog（如 CDC 数据变更捕获或主备同步场景），将调用 disable_for_binlog_scan() 禁用 Query Cache。
            // Binlog 扫描读取的是底层的变更日志流（Row-level Changes），其数据流形态和普通 SQL 查询的 Snapshot 扫描完全不同，既不能使用已有的 Query Cache，也不能将其扫描结果写入 Cache 覆盖正常的 Query 数据。
            if (tnode.olap_scan_node.__isset.read_row_binlog &&
                tnode.olap_scan_node.read_row_binlog) {
                // Row-binlog scans read a different data stream: they must
                // neither serve nor fill the query cache.
                _query_cache_runtime->disable_for_binlog_scan();
            }
        }
        // 创建物理算子 OlapScanOperatorX，并将 TQueryCacheParam 参数和刚才校验过的 _query_cache_runtime 指针透传给算子内部。
        op = std::make_shared<OlapScanOperatorX>(
                pool, tnode, next_operator_id(), descs, _num_instances,
                enable_query_cache ? _params.fragment.query_cache_param : TQueryCacheParam {},
                enable_query_cache ? _query_cache_runtime : nullptr);
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
        fe_with_old_version = !tnode.__isset.is_serial_operator;
        break;
    }
    // GROUP_COMMIT_SCAN_NODE（Group Commit 组提交导入节点） 在 Pipeline 执行引擎中构建物理算子 GroupCommitOperatorX 的入口
    // 与普通的 OLAP_SCAN_NODE 相比，GROUP_COMMIT_SCAN_NODE 的核心差异在于它并非用于普通的 SQL SELECT 查询，而是用于 Group Commit 导入模式（通过将多个小批量的 Stream Load / Insert 写入合并为一次大提交，大幅提升高并发小写场景下的吞吐量并减少 Tablet Header 版本分裂）。
    case TPlanNodeType::GROUP_COMMIT_SCAN_NODE: {
        DCHECK(_query_ctx != nullptr);
        // 标记 Query 内存跟踪器（MemTracker）的 Group Commit 属性
        // 将当前 Query 上下文（_query_ctx）对应的内存跟踪器（query_mem_tracker）标记为 is_group_commit_load = true。
        // 内存管控与 GC 策略隔离：Group Commit 导入涉及将数据在 BE 内存的 Block Queue 中暂存和攒批。内存管理器（MemTracker）在触发 Memory Limit 限制或系统 OOM 刷盘/Kill 查询时，需要识别出此类 Query 并应用特定的内存管控逻辑（如优先触发 Block 刷盘而非直接杀掉 Load Query）
        _query_ctx->query_mem_tracker()->is_group_commit_load = true;
        // 算子创建：创建物理算子 GroupCommitOperatorX。其内部不直接去存储引擎（OlapTable）中读 Tablet 数据，而是作为 Source 算子，从 FE / BE 预先分配好的 Group Commit 内存队列（GroupCommitBlockBuffer / LoadStream） 中消费攒批好的 RowBatch / Block 数据。
        op = std::make_shared<GroupCommitOperatorX>(pool, tnode, next_operator_id(), descs,
                                                    _num_instances);
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
        fe_with_old_version = !tnode.__isset.is_serial_operator;
        break;
    }
    // JDBC_SCAN_NODE（JDBC 外部表扫描节点） 在 Pipeline 执行引擎中构建物理算子 JDBCScanOperatorX 的入口
    // 主要负责处理外围异构数据库（如 MySQL、PostgreSQL、Oracle、SQL Server 等）的联邦查询/外表读取，核心逻辑聚焦于 Java 运行时（JNI / JVM）依赖的硬检查与拦截。
    case TPlanNodeType::JDBC_SCAN_NODE: {
        // Java 运行时支持校验 (config::enable_java_support)
        // 核心背景：Doris 的 BE 是基于 C++ 编写的，但 JDBC 驱动（JDBC Driver）及其生态是标准 Java 实现。因此，BE 读取 JDBC 外部表时，需要通过 JNI（Java Native Interface） 启动嵌入式 JVM，并在 JVM 中加载对应数据库的 .jar 包驱动来拉取数据。
        // 开启状态 (enable_java_support = true)：允许创建 JDBCScanOperatorX，并将算子加入当前 Pipeline。
        if (config::enable_java_support) {
            op = std::make_shared<JDBCScanOperatorX>(pool, tnode, next_operator_id(), descs,
                                                     _num_instances);
            RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
        } else {
            return Status::InternalError(
                    "Jdbc scan node is disabled, you can change be config enable_java_support "
                    "to true and restart be.");
        }
        fe_with_old_version = !tnode.__isset.is_serial_operator;
        break;
    }
    // FILE_SCAN_NODE（外表/文件系统扫描节点） 在 Pipeline 执行引擎中构建物理算子 FileScanOperatorX 的入口。
    // 用于解析与读取外部存储及湖仓架构中的数据文件（如 S3、HDFS、NAS/Local File 以及 Iceberg、Paimon、Hudi、Hive 等 Table Format），是 Doris 数据湖联邦查询（Data Lake Federation Query） 的核心 Source 算子。
    case TPlanNodeType::FILE_SCAN_NODE: {
        // 1. 实例化 FileScanOperatorX 物理算子
        op = std::make_shared<FileScanOperatorX>(pool, tnode, next_operator_id(), descs,
                                                 _num_instances);
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
        fe_with_old_version = !tnode.__isset.is_serial_operator;
        break;
    }
    // EXCHANGE_NODE（数据交换/网络接收节点） 在 Pipeline 执行引擎中构建物理算子 ExchangeSourceOperatorX 的入口。
    // 在 Doris 的分布式 MPR（Massively Parallel Processing）执行架构中，EXCHANGE_NODE 充当跨 BE 节点或跨 Fragment 数据传输的接收端（Receiver/DataStreamRecvr）。它负责从网络层接收上游上报的 DataStreamSender 发送过来的数据 Block，并向当前 Pipeline 吐出数据。
    case TPlanNodeType::EXCHANGE_NODE: {
        // 1. 计算与校验 Sender 数量
        // 从 Fragment 执行参数 _params.per_exch_num_senders 中，查找当前 EXCHANGE_NODE 节点对应的上游数据发送方（Sender Instance）的总数量。
        int num_senders = _params.per_exch_num_senders.contains(tnode.node_id)
                                  ? _params.per_exch_num_senders.find(tnode.node_id)->second
                                  : 0;
        // 校验：通过 DCHECK_GT(num_senders, 0) 确保必须存在至少一个 Sender。只有知道了 Sender 的确切数量，接收端的引用计数/状态机（EOS 机制）才知道需要等待多少个 Sender 发送结束标志，才能安全关闭当前 Recvr。
        DCHECK_GT(num_senders, 0);
        // 算子创建：创建 ExchangeSourceOperatorX 实例，将 num_senders 传入其内部。
        auto exchange_op = std::make_shared<ExchangeSourceOperatorX>(
                pool, tnode, next_operator_id(), descs, num_senders);
        // Bucket Shuffle 下的“孤立实例（Orphan Instance）”防死锁处理
        // 在按 Bucket（分桶路由） 进行 Shuffle 数据传输的场景下，FE 会根据 Bucket 的路由规则把 Bucket 分配给特定的 BE 实例。但可能会出现某种边界情况：某个 Receiver 实例（Instance/Task）没有被分配到任何一个 Bucket。这种实例被称为 Orphan Instance（孤立实例）。
        // 如果一个 Receiver 实例不拥有任何 Bucket，上游的 Sender 在根据 bucket_id 分发数据时，永远不会向该 Receiver 所在的 Channel 发送任何数据或 EOS（End Of Stream）信号。
        // 这会导致这个孤立的 Receiver 实例一直阻塞等待上游的 EOS，造成整个查询死锁（Deadlock）或超时挂起。
        // 通过 set_bucket_dest_instances 传入全局的 Bucket 到 Instance 的映射表。ExchangeSourceOperatorX 在初始化时会检查“自己是否拥有 Bucket”；如果不拥有，立刻将自己的 Receiver 状态直接设为 EOS（End Of Stream），不进行无意义的等待，从而安全地跳过执行。
        if (!_params.bucket_seq_to_instance_idx.empty()) {
            // Lets bucket-routed exchanges detect orphan instances (owning no bucket) that
            // no sender channel will ever address — their receivers must start at EOS.
            exchange_op->set_bucket_dest_instances(_params.bucket_seq_to_instance_idx);
        }
        op = exchange_op;
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
        fe_with_old_version = !tnode.__isset.is_serial_operator;
        break;
    }
    // 聚合算子是 OLAP 查询中最复杂的核心算子之一。这段代码体现了 Doris 在聚合计算上的四大核心机制：策略判定（Spill / Streaming）、Query Cache 动态插入、Pipeline 阻塞切分（Sink/Source 拆分） 以及 向量化优化（Distinct Streaming Agg）。
    case TPlanNodeType::AGGREGATION_NODE: {
        // 校验聚合节点合法性：GROUP BY 列和输出 Slot 不能同时为空（无基准无输出的非合法算子直接抛错）。
        if (tnode.agg_node.grouping_exprs.empty() &&
            descs.get_tuple_descriptor(tnode.agg_node.output_tuple_id)->slots().empty()) {
            return Status::InternalError("Illegal aggregate node " + std::to_string(tnode.node_id) +
                                         ": group by and output is empty");
        }

        bool need_create_cache_op =
                enable_query_cache && tnode.node_id == _params.fragment.query_cache_param.node_id;
        auto create_query_cache_operator = [&](PipelinePtr& new_pipe) {
            auto cache_node_id = _params.local_params[0].per_node_scan_ranges.begin()->first;
            auto cache_source_id = next_operator_id();
            if (_query_cache_runtime == nullptr) {
                _query_cache_runtime =
                        std::make_shared<QueryCacheRuntime>(_params.fragment.query_cache_param);
            }
            op = std::make_shared<CacheSourceOperatorX>(pool, cache_node_id, cache_source_id,
                                                        _params.fragment.query_cache_param,
                                                        _query_cache_runtime);
            RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));

            const auto downstream_pipeline_id = cur_pipe->id();
            if (!_dag.contains(downstream_pipeline_id)) {
                _dag.insert({downstream_pipeline_id, {}});
            }
            new_pipe = add_pipeline(cur_pipe);
            _dag[downstream_pipeline_id].push_back(new_pipe->id());

            DataSinkOperatorPtr cache_sink(new CacheSinkOperatorX(
                    next_sink_operator_id(), op->node_id(), op->operator_id()));
            RETURN_IF_ERROR(new_pipe->set_sink(cache_sink));
            return Status::OK();
        };
        // Group By Limit 优化 (group_by_limit_opt)：当存在 GROUP BY、按 Group Key 排序且带有 LIMIT 时，算子可以在 Hash 表达到 limit 数量后停止接收新 Key，大幅提速。
        const bool group_by_limit_opt =
                tnode.agg_node.__isset.agg_sort_info_by_group_key && tnode.limit > 0;

        /// PartitionedAggSourceOperatorX does not support "group by limit opt(#29641)" yet.
        /// If `group_by_limit_opt` is true, then it might not need to spill at all.
        // 溢写/落盘开关 (enable_spill)：当启用 Spill、且存在 GROUP BY 列、且未触发 group_by_limit_opt 时开启。因为 Group By Limit 优化通常内存占用极小，不需要落盘。
        const bool enable_spill = _runtime_state->enable_spill() &&
                                  !tnode.agg_node.grouping_exprs.empty() && !group_by_limit_opt;
        // 流式预聚合 (is_streaming_agg)：在两阶段聚合（Two-Phase Aggregation）的第一阶段，当数据 Hash 分布不均匀时，使用 Hash 表维护局部聚合会导致 Hash 表无限膨胀。
        // StreamingAgg 可以以较低内存开销将数据“流式”推向下游 Exchange 节点。
        const bool is_streaming_agg = tnode.agg_node.__isset.use_streaming_preaggregation &&
                                      tnode.agg_node.use_streaming_preaggregation &&
                                      !tnode.agg_node.grouping_exprs.empty();
        // TODO: distinct streaming agg does not support spill.
        // Distinct Streaming Agg (can_use_distinct_streaming_agg)：针对 SELECT DISTINCT x, y（无聚合函数 agg_functions.empty()）的场景进行极致优化的专有流式去重算子。
        const bool can_use_distinct_streaming_agg =
                (!enable_spill || is_streaming_agg) && tnode.agg_node.aggregate_functions.empty() &&
                !tnode.agg_node.__isset.agg_sort_info_by_group_key &&
                _params.query_options.__isset.enable_distinct_streaming_aggregation &&
                _params.query_options.enable_distinct_streaming_aggregation;
        // 分支 1：can_use_distinct_streaming_agg（Distinct 流式去重）
        // 原理：专用于无聚合函数的 DISTINCT 语句。算子维持一个固定大小的轻量 HashTable / Set，匹配到的新 Key 向上游吐出，过挤时直接流式 Bypass 吐出给二阶段处理。
        // 特点：非阻塞算子，数据随到随走，直接塞入当前 cur_pipe，无需切分 Pipeline。
        if (can_use_distinct_streaming_agg) {
            if (need_create_cache_op) {
                PipelinePtr new_pipe;
                RETURN_IF_ERROR(create_query_cache_operator(new_pipe));

                cache_op = op;
                op = std::make_shared<DistinctStreamingAggOperatorX>(pool, next_operator_id(),
                                                                     tnode, descs);
                RETURN_IF_ERROR(new_pipe->add_operator(op, _parallel_instances));
                RETURN_IF_ERROR(cur_pipe->operators().front()->set_child(op));
                cur_pipe = new_pipe;
            } else {
                op = std::make_shared<DistinctStreamingAggOperatorX>(pool, next_operator_id(),
                                                                     tnode, descs);
                RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
            }
        // 分支 2：is_streaming_agg（常规流式预聚合）
        // 原理：第一阶段 Pre-Aggregation 的流式模式。
        // 特点：非阻塞算子，实例化 StreamingAggOperatorX 直接放入 cur_pipe，不打断流水线。
        } else if (is_streaming_agg) {
            if (need_create_cache_op) {
                PipelinePtr new_pipe;
                RETURN_IF_ERROR(create_query_cache_operator(new_pipe));
                cache_op = op;
                op = std::make_shared<StreamingAggOperatorX>(pool, next_operator_id(), tnode,
                                                             descs);
                RETURN_IF_ERROR(cur_pipe->operators().front()->set_child(op));
                RETURN_IF_ERROR(new_pipe->add_operator(op, _parallel_instances));
                cur_pipe = new_pipe;
            } else {
                op = std::make_shared<StreamingAggOperatorX>(pool, next_operator_id(), tnode,
                                                             descs);
                RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
            }
        // 分支 3：Blocking Agg（阻塞式全量聚合 —— 最核心分支）
        // 当无法使用流式聚合时，聚合算子变成完全阻塞算子（Data-Blocking Operator）：必须等到上游所有数据全部写入 Hash 表（Sink 完成），才能开始向下游输出聚合结果（Source 读取）。
        } else {
            // create new pipeline to add query cache operator
            PipelinePtr new_pipe;
            if (need_create_cache_op) {
                RETURN_IF_ERROR(create_query_cache_operator(new_pipe));
                cache_op = op;
            }
            // 创建 Source 算子：
            // 根据 enable_spill 实例化 PartitionedAggSourceOperatorX 或 AggSourceOperatorX，加入下游 Pipeline（当前 cur_pipe）。
            if (enable_spill) {
                op = std::make_shared<PartitionedAggSourceOperatorX>(pool, tnode,
                                                                     next_operator_id(), descs);
            } else {
                op = std::make_shared<AggSourceOperatorX>(pool, tnode, next_operator_id(), descs);
            }
            if (need_create_cache_op) {
                RETURN_IF_ERROR(cur_pipe->operators().front()->set_child(op));
                RETURN_IF_ERROR(new_pipe->add_operator(op, _parallel_instances));
                cur_pipe = new_pipe;
            } else {
                RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
            }
            // 切分并建立 DAG 依赖：
            const auto downstream_pipeline_id = cur_pipe->id();
            if (!_dag.contains(downstream_pipeline_id)) {
                _dag.insert({downstream_pipeline_id, {}});
            }
            cur_pipe = add_pipeline(cur_pipe);
            _dag[downstream_pipeline_id].push_back(cur_pipe->id());
            // 挂载 Sink 算子：
            // 根据 enable_spill 实例化 PartitionedAggSinkOperatorX 或 AggSinkOperatorX，作为新建上游 Pipeline 的 Sink 算子。
            if (enable_spill) {
                sink_ops.push_back(std::make_shared<PartitionedAggSinkOperatorX>(
                        pool, next_sink_operator_id(), op->operator_id(), tnode, descs));
            } else {
                sink_ops.push_back(std::make_shared<AggSinkOperatorX>(
                        pool, next_sink_operator_id(), op->operator_id(), tnode, descs));
            }
            RETURN_IF_ERROR(cur_pipe->set_sink(sink_ops.back()));
            RETURN_IF_ERROR(cur_pipe->sink()->init(tnode, _runtime_state.get()));
        }
        break;
    }
    // BUCKETED_AGGREGATION_NODE（分桶/桶粒度并行聚合节点） 在 Pipeline 执行引擎中构建物理算子及其执行依赖的核心实现。
    // BUCKETED_AGGREGATION_NODE 是 Doris 针对按 Bucket Shuffle / 分桶数据分布的聚合场景推出的一项专有优化。它的核心思想是：充分利用数据在各个 Bucket 上的天然隔离/局部 Hash 特性，在多 Instance 节点间实现低锁竞争、高并发的分桶数据聚合与合并。
    case TPlanNodeType::BUCKETED_AGGREGATION_NODE: {
        // 核心语义：分桶聚合（Bucketed Aggregation）的前提是必须存在 GROUP BY Key，因为数据是依据 Key 的 Hash/Bucket 物理分布在不同的分桶或 Instance 中的。如果没有 GROUP BY（如标量聚合 COUNT(*)），则无法按照桶维度切分，属于 FE 优化的非合法状态，直接强拦截抛错
        if (tnode.bucketed_agg_node.grouping_exprs.empty()) {
            return Status::InternalError(
                    "Bucketed aggregation node {} should not be used without group by keys",
                    tnode.node_id);
        }

        // Create source operator (goes on the current / downstream pipeline).
        // 1. 创建 Source 算子，挂载到当前（下游）Pipeline
        op = std::make_shared<BucketedAggSourceOperatorX>(pool, tnode, next_operator_id(), descs);
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));

        // Create a new pipeline for the sink side.
        // 2. 切分 Pipeline：创建上游 Sink 侧管道并构建 DAG 依赖关系
        const auto downstream_pipeline_id = cur_pipe->id();
        if (!_dag.contains(downstream_pipeline_id)) {
            _dag.insert({downstream_pipeline_id, {}});
        }
        cur_pipe = add_pipeline(cur_pipe);
        _dag[downstream_pipeline_id].push_back(cur_pipe->id());

        // Create sink operator.
        // 3. 创建 Sink 算子并挂载到新建的上游 Pipeline
        // Pipeline 拆分：分桶聚合依然属于 Blocking（全阻塞）算子，因此被拆分为成对的 BucketedAggSinkOperatorX（负责攒批写 Hash 表）与 BucketedAggSourceOperatorX（负责读取合并结果吐给下游）。
        // DAG 拓扑：下游 Pipeline 依赖上游 Build Pipeline，只有上游所有的 Sink Task 全部完成后，下游 Source 才能解阻塞并开始读数据。
        sink_ops.push_back(std::make_shared<BucketedAggSinkOperatorX>(
                pool, next_sink_operator_id(), op->operator_id(), tnode, descs));
        RETURN_IF_ERROR(cur_pipe->set_sink(sink_ops.back()));
        RETURN_IF_ERROR(cur_pipe->sink()->init(tnode, _runtime_state.get()));

        // Pre-register a single shared state for ALL instances so that every
        // sink instance writes its per-instance hash table into the same
        // BucketedAggSharedState and every source instance can merge across
        // all of them.
        // 核心机制：共享状态 (BucketedAggSharedState) 与 Dependency 依赖预注册
        {
            // 1. 全局单例 SharedState：供当前 Fragment 下所有 Instance 共享
            auto shared_state = BucketedAggSharedState::create_shared();
            shared_state->id = op->operator_id();
            shared_state->related_op_ids.insert(op->operator_id());
            // 2. 为每个 Sink Instance 绑定对应的 Sink Dependency
            for (int i = 0; i < _num_instances; i++) {
                auto sink_dep = std::make_shared<Dependency>(op->operator_id(), op->node_id(),
                                                             "BUCKETED_AGG_SINK_DEPENDENCY");
                sink_dep->set_shared_state(shared_state.get());
                shared_state->sink_deps.push_back(sink_dep);
            }
            // 3. 为每个 Source Instance 创建对应的 Source Dependency 依赖
            shared_state->create_source_dependencies(_num_instances, op->operator_id(),
                                                     op->node_id(), "BUCKETED_AGG_SOURCE");
            // 4. 将 Shared State 注册到全局映射表，供 Runtime 调度引擎查询与状态变更通知
            _op_id_to_shared_state.insert(
                    {op->operator_id(), {shared_state, shared_state->sink_deps}});
        }
        break;
    }
    // 解析 Join 物理节点，区分 Spill（落盘/溢写）与内存执行模式，创建 Build 侧（Sink）和 Probe 侧（Source）物理算子，并配置 Broadcast Join 的跨 Instance 共享 Hash 表（SharedState）依赖。
    // 代码主要分为两个核心分支：Spill 分支（PartitionedHashJoin） 与 非 Spill 内存分支（HashJoin），并在最后统一处理 Broadcast Join 的内存共享机制。
    case TPlanNodeType::HASH_JOIN_NODE: {
        const auto is_broadcast_join = tnode.hash_join_node.__isset.is_broadcast_join &&
                                       tnode.hash_join_node.is_broadcast_join;
        const auto enable_spill = _runtime_state->enable_spill();
        // 1. Spill 溢写分支：PartitionedHashJoin
        // 为什么 Broadcast Join 不支持 Spill？ Broadcast Join 数据的 Build 侧通常小到可以完全放进内存，溢写开销反而远大于内存开销；此外 Broadcast 共享 Hash 表的结构难以进行 Partitioned Spill。
        if (enable_spill && !is_broadcast_join) {
            auto tnode_ = tnode;
            tnode_.runtime_filters.clear();
            auto inner_probe_operator =
                    std::make_shared<HashJoinProbeOperatorX>(pool, tnode_, 0, descs);

            // probe side inner sink operator is used to build hash table on probe side when data is spilled.
            // So here use `tnode_` which has no runtime filters.
            auto probe_side_inner_sink_operator =
                    std::make_shared<HashJoinBuildSinkOperatorX>(pool, 0, 0, tnode_, descs);

            RETURN_IF_ERROR(inner_probe_operator->init(tnode_, _runtime_state.get()));
            RETURN_IF_ERROR(probe_side_inner_sink_operator->init(tnode_, _runtime_state.get()));

            auto probe_operator = std::make_shared<PartitionedHashJoinProbeOperatorX>(
                    pool, tnode_, next_operator_id(), descs);
            probe_operator->set_inner_operators(probe_side_inner_sink_operator,
                                                inner_probe_operator);
            op = std::move(probe_operator);
            RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));

            const auto downstream_pipeline_id = cur_pipe->id();
            if (!_dag.contains(downstream_pipeline_id)) {
                _dag.insert({downstream_pipeline_id, {}});
            }
            PipelinePtr build_side_pipe = add_pipeline(cur_pipe);
            _dag[downstream_pipeline_id].push_back(build_side_pipe->id());

            auto inner_sink_operator =
                    std::make_shared<HashJoinBuildSinkOperatorX>(pool, 0, 0, tnode, descs);
            auto sink_operator = std::make_shared<PartitionedHashJoinSinkOperatorX>(
                    pool, next_sink_operator_id(), op->operator_id(), tnode_, descs);
            RETURN_IF_ERROR(inner_sink_operator->init(tnode, _runtime_state.get()));

            sink_operator->set_inner_operators(inner_sink_operator, inner_probe_operator);
            sink_ops.push_back(std::move(sink_operator));
            RETURN_IF_ERROR(build_side_pipe->set_sink(sink_ops.back()));
            RETURN_IF_ERROR(build_side_pipe->sink()->init(tnode_, _runtime_state.get()));

            _pipeline_parent_map.push(op->node_id(), cur_pipe);
            _pipeline_parent_map.push(op->node_id(), build_side_pipe);
        // 2. 非 Spill 内存分支：标准 Pipeline Hash Join
        // Pipeline 打断机制：Hash Join 是典型的 Data-Blocking（数据阻塞） 算子。Probe 侧（下游 Pipeline）必须等待 Build 侧（上游 Pipeline）将右表数据全量读取并成功构建 Hash 表（Sink 完成）后才能开始 Probe。
        } else {
            op = std::make_shared<HashJoinProbeOperatorX>(pool, tnode, next_operator_id(), descs);
            RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));

            const auto downstream_pipeline_id = cur_pipe->id();
            if (!_dag.contains(downstream_pipeline_id)) {
                _dag.insert({downstream_pipeline_id, {}});
            }
            PipelinePtr build_side_pipe = add_pipeline(cur_pipe);
            _dag[downstream_pipeline_id].push_back(build_side_pipe->id());

            sink_ops.push_back(std::make_shared<HashJoinBuildSinkOperatorX>(
                    pool, next_sink_operator_id(), op->operator_id(), tnode, descs));
            RETURN_IF_ERROR(build_side_pipe->set_sink(sink_ops.back()));
            RETURN_IF_ERROR(build_side_pipe->sink()->init(tnode, _runtime_state.get()));

            _pipeline_parent_map.push(op->node_id(), cur_pipe);
            _pipeline_parent_map.push(op->node_id(), build_side_pipe);
        }
        // 3. Broadcast Join 内存共享机制 (HashJoinSharedState)
        // 核心价值：在 Broadcast Join 场景下，同一个 BE 节点上的所有并发 Pipeline Instance 接收到的 Build 侧（右表）数据完全相同。
        // 共享机制：如果不做优化，每个 Instance 会各自构建一份一模一样的 Hash 表，造成严重内存浪费与 CPU 重复计算。
        if (is_broadcast_join && _runtime_state->enable_share_hash_table_for_broadcast_join()) {
            std::shared_ptr<HashJoinSharedState> shared_state =
                    HashJoinSharedState::create_shared(_num_instances);
            for (int i = 0; i < _num_instances; i++) {
                auto sink_dep = std::make_shared<Dependency>(op->operator_id(), op->node_id(),
                                                             "HASH_JOIN_BUILD_DEPENDENCY");
                sink_dep->set_shared_state(shared_state.get());
                shared_state->sink_deps.push_back(sink_dep);
            }
            shared_state->create_source_dependencies(_num_instances, op->operator_id(),
                                                     op->node_id(), "HASH_JOIN_PROBE");
            _op_id_to_shared_state.insert(
                    {op->operator_id(), {shared_state, shared_state->sink_deps}});
        }
        break;
    }
    // 执行引擎中解析与构建 CROSS_JOIN_NODE（交叉连接 / 笛卡尔积节点，以及带有非等值 Join 条件的 Nested Loop Join） 物理算子树的入口实现
    case TPlanNodeType::CROSS_JOIN_NODE: {
        // 1. 创建 Probe 侧算子并装载至当前 Pipeline
        // 算子选型：Doris 物理执行引擎将 CROSS_JOIN_NODE 实现为 NestedLoopJoin（嵌套循环连接）。
        op = std::make_shared<NestedLoopJoinProbeOperatorX>(pool, tnode, next_operator_id(), descs);
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
        // 2. 切分 Pipeline 并建立 DAG 依赖拓扑
        const auto downstream_pipeline_id = cur_pipe->id();
        if (!_dag.contains(downstream_pipeline_id)) {
            _dag.insert({downstream_pipeline_id, {}});
        }
        PipelinePtr build_side_pipe = add_pipeline(cur_pipe);
        _dag[downstream_pipeline_id].push_back(build_side_pipe->id());
        // 3. 创建 Build 侧 Sink 算子并挂载到新建 Pipeline
        sink_ops.push_back(std::make_shared<NestedLoopJoinBuildSinkOperatorX>(
                pool, next_sink_operator_id(), op->operator_id(), tnode, descs));
        RETURN_IF_ERROR(build_side_pipe->set_sink(sink_ops.back()));
        RETURN_IF_ERROR(build_side_pipe->sink()->init(tnode, _runtime_state.get()));
        _pipeline_parent_map.push(op->node_id(), cur_pipe);
        _pipeline_parent_map.push(op->node_id(), build_side_pipe);
        break;
    }
    // UNION_NODE（Union / 集合合并节点，通常对应 UNION ALL） 在 Pipeline 执行引擎中构建物理算子树及 N 选 1 多分支 DAG 拓扑的核心实现。
    case TPlanNodeType::UNION_NODE: {
        int child_count = tnode.num_children;
        // 1. 实例化 UnionSourceOperatorX 汇聚 Source
        // 算子职责：UnionSourceOperatorX 作为下游 Pipeline 的 Source 算子。
        // 数据流向：它的任务不是去存储引擎读数据，而是作为一个 M:N 共享 Buffer/队列的消费者，不断从中拉取各个上游分支 UnionSinkOperatorX 写入的数据块（Block），并吐给下游 Pipeline。
        op = std::make_shared<UnionSourceOperatorX>(pool, tnode, next_operator_id(), descs);
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));

        const auto downstream_pipeline_id = cur_pipe->id();
        if (!_dag.contains(downstream_pipeline_id)) {
            _dag.insert({downstream_pipeline_id, {}});
        }
        // 2. 循环构建 $N$ 个上游分支 Pipeline 与 Sink 算子
        for (int i = 0; i < child_count; i++) {
            //  1. 为第 i 个子分支创建独立的上游 Pipeline
            PipelinePtr build_side_pipe = add_pipeline(cur_pipe);
            _dag[downstream_pipeline_id].push_back(build_side_pipe->id());
            // 2. 为该 Pipeline 挂载 UnionSinkOperatorX
            sink_ops.push_back(std::make_shared<UnionSinkOperatorX>(
                    i, next_sink_operator_id(), op->operator_id(), pool, tnode, descs));
            RETURN_IF_ERROR(build_side_pipe->set_sink(sink_ops.back()));
            RETURN_IF_ERROR(build_side_pipe->sink()->init(tnode, _runtime_state.get()));
            // preset children pipelines. if any pipeline found this as its father, will use the prepared pipeline to build.
            // 3. 预先将创建好的子分支 Pipeline 注册到 _pipeline_parent_map
            _pipeline_parent_map.push(op->node_id(), build_side_pipe);
        }
        break;
    }
    case TPlanNodeType::SORT_NODE: {
        const auto should_spill = _runtime_state->enable_spill() &&
                                  tnode.sort_node.algorithm == TSortAlgorithm::FULL_SORT;
        const bool use_local_merge =
                tnode.sort_node.__isset.use_local_merge && tnode.sort_node.use_local_merge;
        if (should_spill) {
            op = std::make_shared<SpillSortSourceOperatorX>(pool, tnode, next_operator_id(), descs);
        } else if (use_local_merge) {
            op = std::make_shared<LocalMergeSortSourceOperatorX>(pool, tnode, next_operator_id(),
                                                                 descs);
        } else {
            op = std::make_shared<SortSourceOperatorX>(pool, tnode, next_operator_id(), descs);
        }
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));

        const auto downstream_pipeline_id = cur_pipe->id();
        if (!_dag.contains(downstream_pipeline_id)) {
            _dag.insert({downstream_pipeline_id, {}});
        }
        cur_pipe = add_pipeline(cur_pipe);
        _dag[downstream_pipeline_id].push_back(cur_pipe->id());

        if (should_spill) {
            sink_ops.push_back(std::make_shared<SpillSortSinkOperatorX>(
                    pool, next_sink_operator_id(), op->operator_id(), tnode, descs));
        } else {
            sink_ops.push_back(std::make_shared<SortSinkOperatorX>(
                    pool, next_sink_operator_id(), op->operator_id(), tnode, descs));
        }
        RETURN_IF_ERROR(cur_pipe->set_sink(sink_ops.back()));
        RETURN_IF_ERROR(cur_pipe->sink()->init(tnode, _runtime_state.get()));
        break;
    }
    case TPlanNodeType::PARTITION_SORT_NODE: {
        op = std::make_shared<PartitionSortSourceOperatorX>(pool, tnode, next_operator_id(), descs);
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));

        const auto downstream_pipeline_id = cur_pipe->id();
        if (!_dag.contains(downstream_pipeline_id)) {
            _dag.insert({downstream_pipeline_id, {}});
        }
        cur_pipe = add_pipeline(cur_pipe);
        _dag[downstream_pipeline_id].push_back(cur_pipe->id());

        sink_ops.push_back(std::make_shared<PartitionSortSinkOperatorX>(
                pool, next_sink_operator_id(), op->operator_id(), tnode, descs));
        RETURN_IF_ERROR(cur_pipe->set_sink(sink_ops.back()));
        RETURN_IF_ERROR(cur_pipe->sink()->init(tnode, _runtime_state.get()));
        break;
    }
    case TPlanNodeType::ANALYTIC_EVAL_NODE: {
        op = std::make_shared<AnalyticSourceOperatorX>(pool, tnode, next_operator_id(), descs);
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));

        const auto downstream_pipeline_id = cur_pipe->id();
        if (!_dag.contains(downstream_pipeline_id)) {
            _dag.insert({downstream_pipeline_id, {}});
        }
        cur_pipe = add_pipeline(cur_pipe);
        _dag[downstream_pipeline_id].push_back(cur_pipe->id());

        sink_ops.push_back(std::make_shared<AnalyticSinkOperatorX>(
                pool, next_sink_operator_id(), op->operator_id(), tnode, descs));
        RETURN_IF_ERROR(cur_pipe->set_sink(sink_ops.back()));
        RETURN_IF_ERROR(cur_pipe->sink()->init(tnode, _runtime_state.get()));
        break;
    }
    // MATERIALIZATION_NODE（物化/延迟物化节点） 在 Pipeline 执行引擎中构建物理算子 MaterializationOperator 的入口。
    // 它主要用于 Late Materialization（延迟物化） 优化技术，即在查询早期（如 Scan / Filter 阶段）只读取并传递行号（RowId）或少量的 Filter 列，待高选择性的过滤条件执行完毕后，再通过物化节点将需要的其余列数据拉取并拼接拼装出来，从而大幅减少昂贵的 I/O 和内存反序列化开销。
    case TPlanNodeType::MATERIALIZATION_NODE: {
        op = std::make_shared<MaterializationOperator>(pool, tnode, next_operator_id(), descs);
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
        break;
    }
    case TPlanNodeType::INTERSECT_NODE: {
        RETURN_IF_ERROR(_build_operators_for_set_operation_node<true>(pool, tnode, descs, op,
                                                                      cur_pipe, sink_ops));
        break;
    }
    case TPlanNodeType::EXCEPT_NODE: {
        RETURN_IF_ERROR(_build_operators_for_set_operation_node<false>(pool, tnode, descs, op,
                                                                       cur_pipe, sink_ops));
        break;
    }
    case TPlanNodeType::REPEAT_NODE: {
        op = std::make_shared<RepeatOperatorX>(pool, tnode, next_operator_id(), descs);
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
        break;
    }
    case TPlanNodeType::TABLE_FUNCTION_NODE: {
        op = std::make_shared<TableFunctionOperatorX>(pool, tnode, next_operator_id(), descs);
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
        break;
    }
    case TPlanNodeType::ASSERT_NUM_ROWS_NODE: {
        op = std::make_shared<AssertNumRowsOperatorX>(pool, tnode, next_operator_id(), descs);
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
        break;
    }
    case TPlanNodeType::EMPTY_SET_NODE: {
        op = std::make_shared<EmptySetSourceOperatorX>(pool, tnode, next_operator_id(), descs);
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
        break;
    }
    case TPlanNodeType::DATA_GEN_SCAN_NODE: {
        op = std::make_shared<DataGenSourceOperatorX>(pool, tnode, next_operator_id(), descs);
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
        fe_with_old_version = !tnode.__isset.is_serial_operator;
        break;
    }
    case TPlanNodeType::SCHEMA_SCAN_NODE: {
        op = std::make_shared<SchemaScanOperatorX>(pool, tnode, next_operator_id(), descs);
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
        break;
    }
    case TPlanNodeType::META_SCAN_NODE: {
        op = std::make_shared<MetaScanOperatorX>(pool, tnode, next_operator_id(), descs);
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
        break;
    }
    case TPlanNodeType::SELECT_NODE: {
        op = std::make_shared<SelectOperatorX>(pool, tnode, next_operator_id(), descs);
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
        break;
    }
    case TPlanNodeType::REC_CTE_NODE: {
        op = std::make_shared<RecCTESourceOperatorX>(pool, tnode, next_operator_id(), descs);
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));

        const auto downstream_pipeline_id = cur_pipe->id();
        if (!_dag.contains(downstream_pipeline_id)) {
            _dag.insert({downstream_pipeline_id, {}});
        }

        PipelinePtr anchor_side_pipe = add_pipeline(cur_pipe);
        _dag[downstream_pipeline_id].push_back(anchor_side_pipe->id());

        DataSinkOperatorPtr anchor_sink;
        anchor_sink = std::make_shared<RecCTEAnchorSinkOperatorX>(next_sink_operator_id(),
                                                                  op->operator_id(), tnode, descs);
        RETURN_IF_ERROR(anchor_side_pipe->set_sink(anchor_sink));
        RETURN_IF_ERROR(anchor_side_pipe->sink()->init(tnode, _runtime_state.get()));
        _pipeline_parent_map.push(op->node_id(), anchor_side_pipe);

        PipelinePtr rec_side_pipe = add_pipeline(cur_pipe);
        _dag[downstream_pipeline_id].push_back(rec_side_pipe->id());

        DataSinkOperatorPtr rec_sink;
        rec_sink = std::make_shared<RecCTESinkOperatorX>(next_sink_operator_id(), op->operator_id(),
                                                         tnode, descs);
        RETURN_IF_ERROR(rec_side_pipe->set_sink(rec_sink));
        RETURN_IF_ERROR(rec_side_pipe->sink()->init(tnode, _runtime_state.get()));
        _pipeline_parent_map.push(op->node_id(), rec_side_pipe);

        break;
    }
    case TPlanNodeType::REC_CTE_SCAN_NODE: {
        op = std::make_shared<RecCTEScanOperatorX>(pool, tnode, next_operator_id(), descs);
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
        break;
    }
    case TPlanNodeType::LOCAL_EXCHANGE_NODE: {
        op = std::make_shared<LocalExchangeSourceOperatorX>(pool, tnode, next_operator_id(), descs);
        // The downstream pipeline (containing LocalExchangeSource) must have
        // _num_instances tasks — matching BE-native _inherit_pipeline_properties
        // which sets pipe_with_source.set_num_tasks(_num_instances).
        // Without this, when the parent pipeline was reduced by a serial operator
        // (e.g., serial Exchange with use_serial_exchange=true, or UNPARTITIONED
        // Exchange), the downstream inherits the reduced num_tasks via
        // add_pipeline(parent).  The deferred exchanger creates _num_instances
        // channels but only fewer source tasks initialize mem_counters — the
        // sink round-robins to all channels and crashes on uninitialized ones.
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
        // Restore downstream pipeline's num_tasks (mirroring _inherit_pipeline_properties:
        // downstream keeps _num_instances, upstream gets the serial/reduced count)
        cur_pipe->set_num_tasks(_num_instances);

        const auto downstream_pipeline_id = cur_pipe->id();
        if (!_dag.contains(downstream_pipeline_id)) {
            _dag.insert({downstream_pipeline_id, {}});
        }
        cur_pipe = add_pipeline(cur_pipe);
        // If this local exchange was inserted because of a serial scan (is_serial_operator),
        // the upstream pipeline (cur_pipe) should have num_tasks=1 (only 1 scan task).
        // We set this now so the exchanger is created with the correct sender count.
        // Child operators added later (serial scan) will also set num_tasks=1, which is
        // consistent with this.
        if (op->is_serial_operator() && _parallel_instances > 0) {
            cur_pipe->set_num_tasks(_parallel_instances);
        }
        _dag[downstream_pipeline_id].push_back(cur_pipe->id());
        int num_partitions = 0;
        std::map<int, int> shuffle_id_to_instance_idx;
        auto partition_type = tnode.local_exchange_node.partition_type;
        switch (partition_type) {
        case TLocalPartitionType::BUCKET_HASH_SHUFFLE:
            num_partitions = _params.num_buckets;
            shuffle_id_to_instance_idx = _params.bucket_seq_to_instance_idx;
            break;
        case TLocalPartitionType::LOCAL_EXECUTION_HASH_SHUFFLE:
            for (int i = 0; i < _num_instances; i++) {
                shuffle_id_to_instance_idx[i] = i;
            }
            num_partitions = _num_instances;
            break;
        case TLocalPartitionType::GLOBAL_EXECUTION_HASH_SHUFFLE:
            num_partitions = _total_instances;
            shuffle_id_to_instance_idx = _params.shuffle_idx_to_instance_idx;
            break;
        default:
            break;
        }
        auto local_exchange_id = op->operator_id();
        auto sink_id = next_sink_operator_id();
        DataSinkOperatorPtr sink = std::make_shared<LocalExchangeSinkOperatorX>(
                sink_id, local_exchange_id, tnode, num_partitions, shuffle_id_to_instance_idx);
        sink_ops.push_back(sink);
        RETURN_IF_ERROR(cur_pipe->set_sink(sink));
        RETURN_IF_ERROR(cur_pipe->sink()->init(tnode, _runtime_state.get()));

        // For FE-planned local exchange, we need to:
        // 1. Initialize the partitioner for hash shuffle types
        // 2. Defer exchanger creation until after the full plan tree is built
        //    (child operators like serial ExchangeNode may change cur_pipe->num_tasks())
        // 3. Register shared state so pipeline tasks can find it
        RETURN_IF_ERROR(static_cast<LocalExchangeSinkOperatorX*>(cur_pipe->sink())
                                ->init_partitioner(_runtime_state.get()));

        int free_blocks_limit =
                _runtime_state->query_options().__isset.local_exchange_free_blocks_limit
                        ? cast_set<int>(
                                  _runtime_state->query_options().local_exchange_free_blocks_limit)
                        : 0;
        auto shared_state = LocalExchangeSharedState::create_shared(_num_instances);
        shared_state->create_source_dependencies(_num_instances, local_exchange_id,
                                                 local_exchange_id, "LOCAL_EXCHANGE_OPERATOR");
        shared_state->create_sink_dependency(sink_id, local_exchange_id, "LOCAL_EXCHANGE_SINK");
        _op_id_to_shared_state.insert({local_exchange_id, {shared_state, shared_state->sink_deps}});
        // Defer exchanger creation: sender count depends on final upstream num_tasks
        _deferred_exchangers.push_back({shared_state, cur_pipe, partition_type, num_partitions,
                                        free_blocks_limit, local_exchange_id, sink_id});
        break;
    }
    default:
        return Status::InternalError("Unsupported exec type in pipeline: {}",
                                     print_plan_node_type(tnode.node_type));
    }
    if (_params.__isset.parallel_instances && fe_with_old_version) {
        cur_pipe->set_num_tasks(_params.parallel_instances);
        op->set_serial_operator();
    }

    return Status::OK();
}
// NOLINTEND(readability-function-cognitive-complexity)
// NOLINTEND(readability-function-size)

template <bool is_intersect>
Status PipelineFragmentContext::_build_operators_for_set_operation_node(
        ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs, OperatorPtr& op,
        PipelinePtr& cur_pipe, std::vector<DataSinkOperatorPtr>& sink_ops) {
    op.reset(new SetSourceOperatorX<is_intersect>(pool, tnode, next_operator_id(), descs));
    RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));

    const auto downstream_pipeline_id = cur_pipe->id();
    if (!_dag.contains(downstream_pipeline_id)) {
        _dag.insert({downstream_pipeline_id, {}});
    }

    for (int child_id = 0; child_id < tnode.num_children; child_id++) {
        PipelinePtr probe_side_pipe = add_pipeline(cur_pipe);
        _dag[downstream_pipeline_id].push_back(probe_side_pipe->id());

        if (child_id == 0) {
            sink_ops.push_back(std::make_shared<SetSinkOperatorX<is_intersect>>(
                    child_id, next_sink_operator_id(), op->operator_id(), pool, tnode, descs));
        } else {
            sink_ops.push_back(std::make_shared<SetProbeSinkOperatorX<is_intersect>>(
                    child_id, next_sink_operator_id(), op->operator_id(), pool, tnode, descs));
        }
        RETURN_IF_ERROR(probe_side_pipe->set_sink(sink_ops.back()));
        RETURN_IF_ERROR(probe_side_pipe->sink()->init(tnode, _runtime_state.get()));
        // prepare children pipelines. if any pipeline found this as its father, will use the prepared pipeline to build.
        _pipeline_parent_map.push(op->node_id(), probe_side_pipe);
    }

    return Status::OK();
}

Status PipelineFragmentContext::submit() {
    if (_submitted) {
        return Status::InternalError("submitted");
    }
    _submitted = true;

    int submit_tasks = 0;
    Status st;
    auto* scheduler = _query_ctx->get_pipe_exec_scheduler();
    for (auto& task : _tasks) {
        for (auto& t : task) {
            st = scheduler->submit(t.first);
            DBUG_EXECUTE_IF("PipelineFragmentContext.submit.failed",
                            { st = Status::Aborted("PipelineFragmentContext.submit.failed"); });
            if (!st) {
                cancel(Status::InternalError("submit context to executor fail"));
                std::lock_guard<std::mutex> l(_task_mutex);
                _total_tasks = submit_tasks;
                break;
            }
            submit_tasks++;
        }
    }
    if (!st.ok()) {
        bool need_remove = false;
        {
            std::lock_guard<std::mutex> l(_task_mutex);
            if (_closed_tasks >= _total_tasks) {
                need_remove = _close_fragment_instance();
            }
        }
        // Call remove_pipeline_context() outside _task_mutex to avoid ABBA deadlock.
        if (need_remove) {
            _exec_env->fragment_mgr()->remove_pipeline_context({_query_id, _fragment_id});
        }
        return Status::InternalError("Submit pipeline failed. err = {}, BE: {}", st.to_string(),
                                     BackendOptions::get_localhost());
    } else {
        return st;
    }
}

void PipelineFragmentContext::print_profile(const std::string& extra_info) {
    if (_runtime_state->enable_profile()) {
        std::stringstream ss;
        for (auto runtime_profile_ptr : _runtime_state->pipeline_id_to_profile()) {
            runtime_profile_ptr->pretty_print(&ss);
        }

        if (_runtime_state->load_channel_profile()) {
            _runtime_state->load_channel_profile()->pretty_print(&ss);
        }

        auto profile_str =
                fmt::format("Query {} fragment {} {}, profile, {}", print_id(this->_query_id),
                            this->_fragment_id, extra_info, ss.str());
        LOG_LONG_STRING(INFO, profile_str);
    }
}
// If all pipeline tasks binded to the fragment instance are finished, then we could
// close the fragment instance.
// Returns true if the caller should call remove_pipeline_context() **after** releasing
// _task_mutex. We must not call remove_pipeline_context() here because it acquires
// _pipeline_map's shard lock, and this function is called while _task_mutex is held.
// Acquiring _pipeline_map while holding _task_mutex creates an ABBA deadlock with
// dump_pipeline_tasks(), which acquires _pipeline_map first and then _task_mutex
// (via debug_string()).
bool PipelineFragmentContext::_close_fragment_instance() {
    if (_is_fragment_instance_closed) {
        return false;
    }
    Defer defer_op {[&]() { _is_fragment_instance_closed = true; }};
    _fragment_level_profile->total_time_counter()->update(_fragment_watcher.elapsed_time());
    if (!_need_notify_close) {
        auto st = send_report(true);
        if (!st) {
            LOG(WARNING) << fmt::format("Failed to send report for query {}, fragment {}: {}",
                                        print_id(_query_id), _fragment_id, st.to_string());
        }
    }
    // Print profile content in info log is a tempoeray solution for stream load and external_connector.
    // Since stream load does not have someting like coordinator on FE, so
    // backend can not report profile to FE, ant its profile can not be shown
    // in the same way with other query. So we print the profile content to info log.

    if (_runtime_state->enable_profile() &&
        (_query_ctx->get_query_source() == QuerySource::STREAM_LOAD ||
         _query_ctx->get_query_source() == QuerySource::EXTERNAL_CONNECTOR ||
         _query_ctx->get_query_source() == QuerySource::GROUP_COMMIT_LOAD)) {
        std::stringstream ss;
        // Compute the _local_time_percent before pretty_print the runtime_profile
        // Before add this operation, the print out like that:
        // UNION_NODE (id=0):(Active: 56.720us, non-child: 00.00%)
        // After add the operation, the print out like that:
        // UNION_NODE (id=0):(Active: 56.720us, non-child: 82.53%)
        // We can easily know the exec node execute time without child time consumed.
        for (auto runtime_profile_ptr : _runtime_state->pipeline_id_to_profile()) {
            runtime_profile_ptr->pretty_print(&ss);
        }

        if (_runtime_state->load_channel_profile()) {
            _runtime_state->load_channel_profile()->pretty_print(&ss);
        }

        LOG_INFO("Query {} fragment {} profile:\n {}", print_id(_query_id), _fragment_id, ss.str());
    }

    if (_query_ctx->enable_profile()) {
        _query_ctx->add_fragment_profile(_fragment_id, collect_realtime_profile(),
                                         collect_realtime_load_channel_profile());
    }

    // Return whether the caller needs to remove from the pipeline map.
    // The caller must do this after releasing _task_mutex.
    return !_need_notify_close;
}

void PipelineFragmentContext::decrement_running_task(PipelineId pipeline_id) {
    // If all tasks of this pipeline has been closed, upstream tasks is never needed, and we just make those runnable here
    DCHECK(_pip_id_to_pipeline.contains(pipeline_id));
    if (_pip_id_to_pipeline[pipeline_id]->close_task()) {
        if (_dag.contains(pipeline_id)) {
            for (auto dep : _dag[pipeline_id]) {
                _pip_id_to_pipeline[dep]->make_all_runnable(pipeline_id);
            }
        }
    }
    bool need_remove = false;
    {
        std::lock_guard<std::mutex> l(_task_mutex);
        ++_closed_tasks;
        // Update query-level finished task progress in real time.
        _query_ctx->inc_finished_task_num();
        if (_closed_tasks >= _total_tasks) {
            need_remove = _close_fragment_instance();
        }
    }
    // Call remove_pipeline_context() outside _task_mutex to avoid ABBA deadlock.
    if (need_remove) {
        _exec_env->fragment_mgr()->remove_pipeline_context({_query_id, _fragment_id});
    }
}

std::string PipelineFragmentContext::get_load_error_url() {
    if (const auto& str = _runtime_state->get_error_log_file_path(); !str.empty()) {
        return to_load_error_http_path(str);
    }
    for (auto& tasks : _tasks) {
        for (auto& task : tasks) {
            if (const auto& str = task.second->get_error_log_file_path(); !str.empty()) {
                return to_load_error_http_path(str);
            }
        }
    }
    return "";
}

std::string PipelineFragmentContext::get_first_error_msg() {
    if (const auto& str = _runtime_state->get_first_error_msg(); !str.empty()) {
        return str;
    }
    for (auto& tasks : _tasks) {
        for (auto& task : tasks) {
            if (const auto& str = task.second->get_first_error_msg(); !str.empty()) {
                return str;
            }
        }
    }
    return "";
}

std::string PipelineFragmentContext::_to_http_path(const std::string& file_name) const {
    std::stringstream url;
    url << "http://" << BackendOptions::get_localhost() << ":" << config::webserver_port
        << "/api/_download_load?"
        << "token=" << _exec_env->token() << "&file=" << file_name;
    return url.str();
}

void PipelineFragmentContext::_append_external_file_commit_data(
        const ReportStatusRequest& req, TReportExecStatusParams* params) const {
    // External-file cleanup remains BE-owned until the final report transfers commit metadata.
    req.runtime_state->append_external_file_commit_data(params, req.done);
    for (auto* rs : req.runtime_states) {
        rs->append_external_file_commit_data(params, req.done);
    }
}

void PipelineFragmentContext::_coordinator_callback(const ReportStatusRequest& req) {
    DBUG_EXECUTE_IF("FragmentMgr::coordinator_callback.report_delay", {
        int random_seconds = req.status.is<ErrorCode::DATA_QUALITY_ERROR>() ? 8 : 2;
        LOG_INFO("sleep : ").tag("time", random_seconds).tag("query_id", print_id(req.query_id));
        std::this_thread::sleep_for(std::chrono::seconds(random_seconds));
        LOG_INFO("sleep done").tag("query_id", print_id(req.query_id));
    });

    DCHECK(req.status.ok() || req.done); // if !status.ok() => done
    if (req.coord_addr.hostname == "external") {
        // External query (flink/spark read tablets) not need to report to FE.
        if (req.done) {
            // Without a coordinator acknowledgement no external-write file may escape rollback.
            req.runtime_state->finalize_external_file_report_cleanup(
                    ExternalFileReportOutcome::REJECTED);
        }
        return;
    }
    int callback_retries = 10;
    const int sleep_ms = 1000;
    Status exec_status = req.status;
    Status coord_status;
    std::unique_ptr<FrontendServiceConnection> coord = nullptr;
    do {
        coord = std::make_unique<FrontendServiceConnection>(_exec_env->frontend_client_cache(),
                                                            req.coord_addr, &coord_status);
        if (!coord_status.ok()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
        }
    } while (!coord_status.ok() && callback_retries-- > 0);

    if (!coord_status.ok()) {
        UniqueId uid(req.query_id.hi, req.query_id.lo);
        static_cast<void>(req.cancel_fn(Status::InternalError(
                "query_id: {}, couldn't get a client for {}, reason is {}", uid.to_string(),
                PrintThriftNetworkAddress(req.coord_addr), coord_status.to_string())));
        if (req.done) {
            req.runtime_state->finalize_external_file_report_cleanup(
                    ExternalFileReportOutcome::REJECTED);
        }
        return;
    }

    TReportExecStatusParams params;
    params.protocol_version = FrontendServiceVersion::V1;
    params.__set_query_id(req.query_id);
    params.__set_backend_num(req.backend_num);
    params.__set_fragment_instance_id(req.fragment_instance_id);
    params.__set_fragment_id(req.fragment_id);
    params.__set_status(exec_status.to_thrift());
    params.__set_done(req.done);
    params.__set_query_type(req.runtime_state->query_type());
    params.__isset.profile = false;

    DCHECK(req.runtime_state != nullptr);

    if (req.runtime_state->query_type() == TQueryType::LOAD) {
        params.__set_loaded_rows(req.runtime_state->num_rows_load_total());
        params.__set_loaded_bytes(req.runtime_state->num_bytes_load_total());
    } else {
        DCHECK(!req.runtime_states.empty());
        if (!req.runtime_state->output_files().empty()) {
            params.__isset.delta_urls = true;
            for (auto& it : req.runtime_state->output_files()) {
                params.delta_urls.push_back(_to_http_path(it));
            }
        }
        if (!params.delta_urls.empty()) {
            params.__isset.delta_urls = true;
        }
    }

    static std::string s_dpp_normal_all = "dpp.norm.ALL";
    static std::string s_dpp_abnormal_all = "dpp.abnorm.ALL";
    static std::string s_unselected_rows = "unselected.rows";
    int64_t num_rows_load_success = 0;
    int64_t num_rows_load_filtered = 0;
    int64_t num_rows_load_unselected = 0;
    if (req.runtime_state->num_rows_load_total() > 0 ||
        req.runtime_state->num_rows_load_filtered() > 0 ||
        req.runtime_state->num_finished_range() > 0) {
        params.__isset.load_counters = true;

        num_rows_load_success = req.runtime_state->num_rows_load_success();
        num_rows_load_filtered = req.runtime_state->num_rows_load_filtered();
        num_rows_load_unselected = req.runtime_state->num_rows_load_unselected();
        params.__isset.fragment_instance_reports = true;
        TFragmentInstanceReport t;
        t.__set_fragment_instance_id(req.runtime_state->fragment_instance_id());
        t.__set_num_finished_range(cast_set<int>(req.runtime_state->num_finished_range()));
        t.__set_loaded_rows(req.runtime_state->num_rows_load_total());
        t.__set_loaded_bytes(req.runtime_state->num_bytes_load_total());
        params.fragment_instance_reports.push_back(t);
    } else if (!req.runtime_states.empty()) {
        for (auto* rs : req.runtime_states) {
            if (rs->num_rows_load_total() > 0 || rs->num_rows_load_filtered() > 0 ||
                rs->num_finished_range() > 0) {
                params.__isset.load_counters = true;
                num_rows_load_success += rs->num_rows_load_success();
                num_rows_load_filtered += rs->num_rows_load_filtered();
                num_rows_load_unselected += rs->num_rows_load_unselected();
                params.__isset.fragment_instance_reports = true;
                TFragmentInstanceReport t;
                t.__set_fragment_instance_id(rs->fragment_instance_id());
                t.__set_num_finished_range(cast_set<int>(rs->num_finished_range()));
                t.__set_loaded_rows(rs->num_rows_load_total());
                t.__set_loaded_bytes(rs->num_bytes_load_total());
                params.fragment_instance_reports.push_back(t);
            }
        }
    }
    params.load_counters.emplace(s_dpp_normal_all, std::to_string(num_rows_load_success));
    params.load_counters.emplace(s_dpp_abnormal_all, std::to_string(num_rows_load_filtered));
    params.load_counters.emplace(s_unselected_rows, std::to_string(num_rows_load_unselected));

    if (!req.load_error_url.empty()) {
        params.__set_tracking_url(req.load_error_url);
    }
    if (!req.first_error_msg.empty()) {
        params.__set_first_error_msg(req.first_error_msg);
    }
    for (auto* rs : req.runtime_states) {
        if (rs->wal_id() > 0) {
            params.__set_txn_id(rs->wal_id());
            params.__set_label(rs->import_label());
        }
    }
    if (!req.runtime_state->export_output_files().empty()) {
        params.__isset.export_files = true;
        params.export_files = req.runtime_state->export_output_files();
    } else if (!req.runtime_states.empty()) {
        for (auto* rs : req.runtime_states) {
            if (!rs->export_output_files().empty()) {
                params.__isset.export_files = true;
                params.export_files.insert(params.export_files.end(),
                                           rs->export_output_files().begin(),
                                           rs->export_output_files().end());
            }
        }
    }
    if (auto tci = req.runtime_state->tablet_commit_infos(); !tci.empty()) {
        params.__isset.commitInfos = true;
        params.commitInfos.insert(params.commitInfos.end(), tci.begin(), tci.end());
    } else if (!req.runtime_states.empty()) {
        for (auto* rs : req.runtime_states) {
            if (auto rs_tci = rs->tablet_commit_infos(); !rs_tci.empty()) {
                params.__isset.commitInfos = true;
                params.commitInfos.insert(params.commitInfos.end(), rs_tci.begin(), rs_tci.end());
            }
        }
    }
    if (auto eti = req.runtime_state->error_tablet_infos(); !eti.empty()) {
        params.__isset.errorTabletInfos = true;
        params.errorTabletInfos.insert(params.errorTabletInfos.end(), eti.begin(), eti.end());
    } else if (!req.runtime_states.empty()) {
        for (auto* rs : req.runtime_states) {
            if (auto rs_eti = rs->error_tablet_infos(); !rs_eti.empty()) {
                params.__isset.errorTabletInfos = true;
                params.errorTabletInfos.insert(params.errorTabletInfos.end(), rs_eti.begin(),
                                               rs_eti.end());
            }
        }
    }
    _append_external_file_commit_data(req, &params);

    req.runtime_state->get_unreported_errors(&(params.error_log));
    params.__isset.error_log = (!params.error_log.empty());

    if (_exec_env->cluster_info()->backend_id != 0) {
        params.__set_backend_id(_exec_env->cluster_info()->backend_id);
    }

    Status report_size_status = validate_report_exec_status_size(
            params, req.runtime_state->coordinator_thrift_message_limit());
    if (!report_size_status.ok()) {
        if (req.done) {
            req.runtime_state->finalize_external_file_report_cleanup(
                    ExternalFileReportOutcome::REJECTED);
        }
        req.cancel_fn(report_size_status);
        return;
    }

    TReportExecStatusResult res;
    Status rpc_status;
    bool report_outcome_ambiguous = false;

    VLOG_DEBUG << "reportExecStatus params is "
               << apache::thrift::ThriftDebugString(params).c_str();
    if (!exec_status.ok()) {
        LOG(WARNING) << "report error status: " << exec_status.msg()
                     << " to coordinator: " << req.coord_addr
                     << ", query id: " << print_id(req.query_id);
    }
    try {
        try {
            (*coord)->reportExecStatus(res, params);
        } catch (apache::thrift::transport::TTransportException& e) {
            report_outcome_ambiguous = true;
            LOG(WARNING) << "Retrying ReportExecStatus. query id: " << print_id(req.query_id)
                         << ", instance id: " << print_id(req.fragment_instance_id) << " to "
                         << req.coord_addr << ", err: " << e.what();
            rpc_status = coord->reopen();

            if (!rpc_status.ok()) {
                // The first request may have been consumed; keep files until metadata or orphan cleanup wins.
                report_outcome_ambiguous = true;
                if (req.done) {
                    req.runtime_state->finalize_external_file_report_cleanup(
                            ExternalFileReportOutcome::AMBIGUOUS);
                }
                req.cancel_fn(rpc_status);
                return;
            }
            (*coord)->reportExecStatus(res, params);
        }

        rpc_status = Status::create<false>(res.status);
    } catch (apache::thrift::TException& e) {
        report_outcome_ambiguous = true;
        rpc_status = Status::InternalError("ReportExecStatus() to {} failed: {}",
                                           PrintThriftNetworkAddress(req.coord_addr), e.what());
    }

    const bool requires_external_file_ack = params.__isset.iceberg_commit_datas;
    if (rpc_status.ok() && requires_external_file_ack &&
        (!res.__isset.external_file_commit_data_accepted ||
         !res.external_file_commit_data_accepted)) {
        rpc_status = Status::InternalError(
                "Coordinator did not accept ownership of the external-file report");
    }

    if (!rpc_status.ok()) {
        if (req.done && !report_outcome_ambiguous) {
            req.runtime_state->finalize_external_file_report_cleanup(
                    ExternalFileReportOutcome::REJECTED);
        } else if (req.done) {
            req.runtime_state->finalize_external_file_report_cleanup(
                    ExternalFileReportOutcome::AMBIGUOUS);
        }
        LOG_INFO("Going to cancel query {} since report exec status got rpc failed: {}",
                 print_id(req.query_id), rpc_status.to_string());
        req.cancel_fn(rpc_status);
    } else if (req.done && req.status.ok()) {
        // Files remain rollback-owned until the coordinator has acknowledged the final metadata report.
        req.runtime_state->finalize_external_file_report_cleanup(
                ExternalFileReportOutcome::ACKNOWLEDGED);
    } else if (req.done) {
        // An acknowledged error report confirms that FE will not publish this write's files.
        req.runtime_state->finalize_external_file_report_cleanup(
                ExternalFileReportOutcome::REJECTED);
    }
}

Status PipelineFragmentContext::send_report(bool done) {
    Status exec_status = _query_ctx->exec_status();

    if (!_is_report_success) {
        // _is_report_success means this is not a load job, do not need to report to fe periodically.
        if (exec_status.is<ErrorCode::LIMIT_REACH>() || exec_status.is<ErrorCode::FINISHED>() ||
            exec_status.ok()) {
            return Status::OK();
        } else {
            // else it means there is some error in processing the query, and we need to send report to FE to let FE know the error.
        }
    } else {
        // This is a load job, need report the process status to FE periodly, so that FE can know the process of the load job.
    }

    std::vector<RuntimeState*> runtime_states;

    for (auto& tasks : _tasks) {
        for (auto& task : tasks) {
            runtime_states.push_back(task.second.get());
        }
    }

    std::string load_eror_url = _query_ctx->get_load_error_url().empty()
                                        ? get_load_error_url()
                                        : _query_ctx->get_load_error_url();
    std::string first_error_msg = _query_ctx->get_first_error_msg().empty()
                                          ? get_first_error_msg()
                                          : _query_ctx->get_first_error_msg();

    ReportStatusRequest req {.status = exec_status,
                             .runtime_states = runtime_states,
                             .done = done || !exec_status.ok(),
                             .coord_addr = _query_ctx->coord_addr,
                             .query_id = _query_id,
                             .fragment_id = _fragment_id,
                             .fragment_instance_id = TUniqueId(),
                             .backend_num = -1,
                             .runtime_state = _runtime_state.get(),
                             .load_error_url = load_eror_url,
                             .first_error_msg = first_error_msg,
                             .cancel_fn = [this](const Status& reason) { cancel(reason); }};
    auto ctx = std::dynamic_pointer_cast<PipelineFragmentContext>(shared_from_this());
    Status submit_status =
            _exec_env->fragment_mgr()->get_thread_pool()->submit_func([this, req, ctx]() {
                SCOPED_ATTACH_TASK(ctx->get_query_ctx()->query_mem_tracker());
                _coordinator_callback(req);
                if (!req.done) {
                    ctx->refresh_next_report_time();
                }
            });
    if (!submit_status.ok() && req.done) {
        // A rejected final callback can never transfer ownership to the coordinator.
        req.runtime_state->finalize_external_file_report_cleanup(
                ExternalFileReportOutcome::REJECTED);
    }
    return submit_status;
}

size_t PipelineFragmentContext::get_revocable_size(bool* has_running_task) const {
    size_t res = 0;
    // _tasks will be cleared during ~PipelineFragmentContext, so that it's safe
    // here to traverse the vector.
    for (const auto& task_instances : _tasks) {
        for (const auto& task : task_instances) {
            if (task.first->is_running()) {
                LOG_EVERY_N(INFO, 50) << "Query: " << print_id(_query_id)
                                      << " is running, task: " << (void*)task.first.get()
                                      << ", is_running: " << task.first->is_running();
                *has_running_task = true;
                return 0;
            }

            size_t revocable_size = task.first->get_revocable_size();
            if (revocable_size >= SpillFile::MIN_SPILL_WRITE_BATCH_MEM) {
                res += revocable_size;
            }
        }
    }
    return res;
}

std::vector<PipelineTask*> PipelineFragmentContext::get_revocable_tasks() const {
    std::vector<PipelineTask*> revocable_tasks;
    for (const auto& task_instances : _tasks) {
        for (const auto& task : task_instances) {
            size_t revocable_size_ = task.first->get_revocable_size();

            if (revocable_size_ >= SpillFile::MIN_SPILL_WRITE_BATCH_MEM) {
                revocable_tasks.emplace_back(task.first.get());
            }
        }
    }
    return revocable_tasks;
}

std::string PipelineFragmentContext::debug_string() {
    std::lock_guard<std::mutex> l(_task_mutex);
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer,
                   "PipelineFragmentContext Info: _closed_tasks={}, _total_tasks={}, "
                   "need_notify_close={}, fragment_id={}, _rec_cte_stage={}\n",
                   _closed_tasks, _total_tasks, _need_notify_close, _fragment_id, _rec_cte_stage);
    for (size_t j = 0; j < _tasks.size(); j++) {
        fmt::format_to(debug_string_buffer, "Tasks in instance {}:\n", j);
        for (size_t i = 0; i < _tasks[j].size(); i++) {
            fmt::format_to(debug_string_buffer, "Task {}: {}\n", i,
                           _tasks[j][i].first->debug_string());
        }
    }

    return fmt::to_string(debug_string_buffer);
}

std::vector<std::shared_ptr<TRuntimeProfileTree>>
PipelineFragmentContext::collect_realtime_profile() const {
    std::vector<std::shared_ptr<TRuntimeProfileTree>> res;

    // we do not have mutex to protect pipeline_id_to_profile
    // so we need to make sure this funciton is invoked after fragment context
    // has already been prepared.
    if (!_prepared) {
        std::string msg =
                "Query " + print_id(_query_id) + " collecting profile, but its not prepared";
        DCHECK(false) << msg;
        LOG_ERROR(msg);
        return res;
    }

    // Make sure first profile is fragment level profile
    auto fragment_profile = std::make_shared<TRuntimeProfileTree>();
    _fragment_level_profile->to_thrift(fragment_profile.get(), _runtime_state->profile_level());
    res.push_back(fragment_profile);

    // pipeline_id_to_profile is initialized in prepare stage
    for (auto pipeline_profile : _runtime_state->pipeline_id_to_profile()) {
        auto profile_ptr = std::make_shared<TRuntimeProfileTree>();
        pipeline_profile->to_thrift(profile_ptr.get(), _runtime_state->profile_level());
        res.push_back(profile_ptr);
    }

    return res;
}

std::shared_ptr<TRuntimeProfileTree>
PipelineFragmentContext::collect_realtime_load_channel_profile() const {
    // we do not have mutex to protect pipeline_id_to_profile
    // so we need to make sure this funciton is invoked after fragment context
    // has already been prepared.
    if (!_prepared) {
        std::string msg =
                "Query " + print_id(_query_id) + " collecting profile, but its not prepared";
        DCHECK(false) << msg;
        LOG_ERROR(msg);
        return nullptr;
    }

    for (const auto& tasks : _tasks) {
        for (const auto& task : tasks) {
            if (task.second->load_channel_profile() == nullptr) {
                continue;
            }

            auto tmp_load_channel_profile = std::make_shared<TRuntimeProfileTree>();

            task.second->load_channel_profile()->to_thrift(tmp_load_channel_profile.get(),
                                                           _runtime_state->profile_level());
            _runtime_state->load_channel_profile()->update(*tmp_load_channel_profile);
        }
    }

    auto load_channel_profile = std::make_shared<TRuntimeProfileTree>();
    _runtime_state->load_channel_profile()->to_thrift(load_channel_profile.get(),
                                                      _runtime_state->profile_level());
    return load_channel_profile;
}

// Collect runtime filter IDs registered by all tasks in this PFC.
// Used during recursive CTE stage transitions to know which filters to deregister
// before creating the new PFC for the next recursion round.
// Called from rerun_fragment(wait_for_destroy) while tasks are still closing.
// Thread safety: safe because _tasks is structurally immutable after prepare() —
// the vector sizes do not change, and individual RuntimeState filter sets are
// written only during open() which has completed by the time we reach rerun.
std::set<int> PipelineFragmentContext::get_deregister_runtime_filter() const {
    std::set<int> result;
    for (const auto& _task : _tasks) {
        for (const auto& task : _task) {
            auto set = task.first->runtime_state()->get_deregister_runtime_filter();
            result.merge(set);
        }
    }
    if (_runtime_state) {
        auto set = _runtime_state->get_deregister_runtime_filter();
        result.merge(set);
    }
    return result;
}

void PipelineFragmentContext::_release_resource() {
    std::lock_guard<std::mutex> l(_task_mutex);
    // The memory released by the query end is recorded in the query mem tracker.
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_query_ctx->query_mem_tracker());
    auto st = _query_ctx->exec_status();
    for (auto& _task : _tasks) {
        if (!_task.empty()) {
            _call_back(_task.front().first->runtime_state(), &st);
        }
    }
    _tasks.clear();
    _dag.clear();
    _pip_id_to_pipeline.clear();
    _pipelines.clear();
    _sink.reset();
    _root_op.reset();
    _runtime_filter_mgr_map.clear();
    _op_id_to_shared_state.clear();
}

} // namespace doris
