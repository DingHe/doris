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

#include <brpc/closure_guard.h>
#include <gen_cpp/Partitions_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/types.pb.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include "common/status.h"
#include "exec/pipeline/pipeline.h"
#include "exec/pipeline/pipeline_task.h"
#include "runtime/query_context.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "runtime/task_execution_context.h"
#include "util/stopwatch.hpp"
#include "util/uid_util.h"

namespace doris {
struct ReportStatusRequest;
class ExecEnv;
class RuntimeFilterMergeControllerEntity;
class TDataSink;
class TPipelineFragmentParams;
class QueryCacheRuntime;

class Dependency;
struct LocalExchangeSharedState;
// Apache Doris BE 模块中的 PipelineFragmentContext 是 Pipeline 执行引擎中最核心的上下文管理类之一。它继承自 TaskExecutionContext，主要负责单个 Query Fragment（查询分片）从解析构建、调度提交到运行监控及资源销毁的全生命周期管理。
// 在 Apache Doris 的 PipelineX / Pipeline 执行引擎中，一个查询计划（Execution Plan）会被切分为多个 Fragment（如 Scan Fragment、Join Fragment）。每个 Fragment 在特定 BE 节点上的执行实例都对应一个 PipelineFragmentContext 对象。
// 其主要职责包括：
// Pipeline 算子树构建 (Plan to Pipeline Building)：将 FE（Frontend）下发的 Thrift 计划树（TPlanNode、TDataSink）翻译拆解为由多个 Pipeline 构成的有向无环图（DAG），并创建对应的 Operator（如 ScanOperator、HashJoinBuildOperator 等）和 DataSink。
// 并发任务生成 (PipelineTask Construction)：根据 Fragment 的并行度（Parallelism / Instances），将抽象的 Pipeline 实例化为具体的、可被调度器执行的 PipelineTask 矩阵（并行度 $N \times$ Pipeline 数 $M$）。
// 共享状态与依赖管理 (Shared State & Dependency Management)：管理 Pipeline 节点之间共享的状态（如 Local Exchange 缓冲区、Hash Join 构建好的 Hash 表等）以及上游算子对下游算子的依赖关系（Dependency）。
// 状态与 Profile 汇报 (Status Reporting & Profiling)：定时或在完成/失败时向 Coordinator（FE）汇报该 Fragment 的执行状态、错误信息（Error Log）、数据统计以及 RuntimeProfile 性能指标。
// 生命周期与资源清理 (Lifecycle & Resource Management)：通过继承 TaskExecutionContext 支持跨线程安全的弱引用管理；当所有 Task 完成或发生 Cancel（取消/超时）时，负责释放资源并通知关联的 QueryContext。

class PipelineFragmentContext : public TaskExecutionContext {
public:
    ENABLE_FACTORY_CREATOR(PipelineFragmentContext);
    PipelineFragmentContext(TUniqueId query_id, const TPipelineFragmentParams& request,
                            std::shared_ptr<QueryContext> query_ctx, ExecEnv* exec_env,
                            const std::function<void(RuntimeState*, Status*)>& call_back);

    ~PipelineFragmentContext() override;

    void print_profile(const std::string& extra_info);

    std::vector<std::shared_ptr<TRuntimeProfileTree>> collect_realtime_profile() const;
    std::shared_ptr<TRuntimeProfileTree> collect_realtime_load_channel_profile() const;

    bool is_timeout(timespec now) const;

    uint64_t elapsed_time() const { return _fragment_watcher.elapsed_time(); }

    int timeout_second() const { return _timeout; }
    // 向 FragmentContext 中添加一个新的 Pipeline 对象。
    PipelinePtr add_pipeline(PipelinePtr parent = nullptr, int idx = -1);

    QueryContext* get_query_ctx() { return _query_ctx.get(); }
    [[nodiscard]] bool is_canceled() const { return _query_ctx->is_cancelled(); }
    // 准备阶段核心入口。
    Status prepare(ThreadPool* thread_pool);
    // 将当前 Fragment 的所有 PipelineTask 提交给 TaskScheduler 执行。
    // 遍历 _tasks 矩阵，将可执行的任务提交到全局 Pipeline 调度线程池；若提交过程中出错，会安全地撤回已提交的任务并触发 Cancel。
    Status submit();

    void set_is_report_success(bool is_report_success) { _is_report_success = is_report_success; }
    // 取消当前 Fragment 的执行。
    // 设置取消状态原因（reason），通知关联的 QueryContext 发生取消，并将所有未完成的 PipelineTask 标记为取消状态。
    void cancel(const Status reason);
    // 当 Fragment 中所有任务完成或发生 Cancel 时被调用，标记并触发关闭逻辑。
    bool notify_close();

    TUniqueId get_query_id() const { return _query_id; }

    [[nodiscard]] int get_fragment_id() const { return _fragment_id; }
    // 当某个 PipelineTask 执行完成时，递减当前 Fragment 中正在运行的任务计数。
    void decrement_running_task(PipelineId pipeline_id);

    uint32_t rec_cte_stage() const { return _rec_cte_stage; }
    void set_rec_cte_stage(uint32_t stage) { _rec_cte_stage = stage; }
    // 向 FE（Coordinator）发送 RPC 状态汇报。
    // 构建 TReportExecStatusParams，填充当前 Fragment 执行状态（如是否完成、错误码、处理行数、Profile 结构等）。
    Status send_report(bool);

    void trigger_report_if_necessary();
    void refresh_next_report_time();

    std::string debug_string();

    [[nodiscard]] int next_operator_id() { return _operator_id--; }

    [[nodiscard]] int max_operator_id() const { return _operator_id; }

    [[nodiscard]] int next_sink_operator_id() { return _sink_operator_id--; }
    // 计算当前 Fragment 中所有算子可被释放/溢写（Spillable/Revocable）的内存总量。
    // 在内存紧张时被内存管理模块调用，确定能够通过溢写落盘腾出多少内存。
    [[nodiscard]] size_t get_revocable_size(bool* has_running_task) const;
    // 获取当前 Fragment 中包含可溢写内存算子的 PipelineTask 列表，用于触发内存落盘任务。
    [[nodiscard]] std::vector<PipelineTask*> get_revocable_tasks() const;
    // 清理已完成的 Task 资源。
    void clear_finished_tasks() {
        if (_need_notify_close) {
            return;
        }
        for (size_t j = 0; j < _tasks.size(); j++) {
            for (size_t i = 0; i < _tasks[j].size(); i++) {
                _tasks[j][i].first->stop_if_finished();
            }
        }
    }

    std::string get_load_error_url();
    std::string get_first_error_msg();

    std::set<int> get_deregister_runtime_filter() const;

    // Store the brpc ClosureGuard so the RPC response is deferred until this PFC is destroyed.
    // When need_send_report_on_destruction is true (final_close), send the report immediately
    // and do not store the guard (let it fire on return to complete the RPC).
    //
    // Thread safety: This method is NOT thread-safe. It reads/writes _wait_close_guard without
    // synchronization. Currently it is only called from rerun_fragment() which is invoked
    // sequentially by RecCTESourceOperatorX (a serial operator) — one opcode at a time per
    // fragment. Do NOT call this concurrently from multiple threads.
    Status listen_wait_close(const std::shared_ptr<brpc::ClosureGuard>& guard,
                             bool need_send_report_on_destruction) {
        if (_wait_close_guard) {
            return Status::InternalError("Already listening wait close");
        }
        if (need_send_report_on_destruction) {
            return send_report(true);
        } else {
            _wait_close_guard = guard;
        }
        return Status::OK();
    }

private:
    void _coordinator_callback(const ReportStatusRequest& req);
    void _append_external_file_commit_data(const ReportStatusRequest& req,
                                           TReportExecStatusParams* params) const;
    std::string _to_http_path(const std::string& file_name) const;

    void _release_resource();

    Status _build_and_prepare_full_pipeline(ThreadPool* thread_pool);

    Status _build_pipelines(ObjectPool* pool, const DescriptorTbl& descs, OperatorPtr* root,
                            PipelinePtr cur_pipe);
    Status _create_tree_helper(ObjectPool* pool, const std::vector<TPlanNode>& tnodes,
                               const DescriptorTbl& descs, OperatorPtr parent, int* node_idx,
                               OperatorPtr* root, PipelinePtr& cur_pipe, int child_idx,
                               const bool followed_by_shuffled_join,
                               const bool require_bucket_distribution);

    Status _create_operator(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs,
                            OperatorPtr& op, PipelinePtr& cur_pipe, int parent_idx, int child_idx,
                            const bool followed_by_shuffled_join,
                            const bool require_bucket_distribution, OperatorPtr& cache_op);
    template <bool is_intersect>
    Status _build_operators_for_set_operation_node(ObjectPool* pool, const TPlanNode& tnode,
                                                   const DescriptorTbl& descs, OperatorPtr& op,
                                                   PipelinePtr& cur_pipe,
                                                   std::vector<DataSinkOperatorPtr>& sink_ops);

    Status _create_data_sink(ObjectPool* pool, const TDataSink& thrift_sink,
                             const std::vector<TExpr>& output_exprs,
                             const TPipelineFragmentParams& params, const RowDescriptor& row_desc,
                             RuntimeState* state, DescriptorTbl& desc_tbl,
                             PipelineId cur_pipeline_id);


    // 规划并决定在哪些算子节点之间插入 LocalExchange（基于 Bucket Shuffle、Hash 分片或 Broadcast 重分配数据），提高多线程并行利用率。
    Status _plan_local_exchange(int num_buckets,
                                const std::map<int, int>& bucket_seq_to_instance_idx,
                                const std::map<int, int>& shuffle_idx_to_instance_idx);
    Status _plan_local_exchange(int num_buckets, int pip_idx, PipelinePtr pip,
                                const std::map<int, int>& bucket_seq_to_instance_idx,
                                const std::map<int, int>& shuffle_idx_to_instance_idx);
    void _inherit_pipeline_properties(const DataDistribution& data_distribution,
                                      PipelinePtr pipe_with_source, PipelinePtr pipe_with_sink);
    Status _add_local_exchange(int pip_idx, int idx, int node_id, ObjectPool* pool,
                               PipelinePtr cur_pipe, DataDistribution data_distribution,
                               bool* do_local_exchange, int num_buckets,
                               const std::map<int, int>& bucket_seq_to_instance_idx,
                               const std::map<int, int>& shuffle_idx_to_instance_idx);
    Status _add_local_exchange_impl(int idx, ObjectPool* pool, PipelinePtr cur_pipe,
                                    PipelinePtr new_pip, DataDistribution data_distribution,
                                    bool* do_local_exchange, int num_buckets,
                                    const std::map<int, int>& bucket_seq_to_instance_idx,
                                    const std::map<int, int>& shuffle_idx_to_instance_idx);
    // 根据构建好的抽象 Pipeline 列表，按照 Fragment 的并发度 _num_instances 批量实例化出具体的 PipelineTask 矩阵。
    Status _build_pipeline_tasks(ThreadPool* thread_pool);
    Status _build_pipeline_tasks_for_instance(
            int instance_idx,
            const std::vector<std::shared_ptr<RuntimeProfile>>& pipeline_id_to_profile);
    // Close the fragment instance and return true if the caller should call
    // remove_pipeline_context() **after** releasing _task_mutex. This avoids
    // holding _task_mutex while acquiring _pipeline_map's shard lock, which
    // would create an ABBA deadlock with dump_pipeline_tasks().
    bool _close_fragment_instance();
    void _init_next_report_time();

    // Id of this query
    // 当前 Fragment 所属查询的全局唯一 ID（Query ID）。
    TUniqueId _query_id;
    // 当前 Fragment 在整个 Query 逻辑计划树中的 ID（如 Fragment 0, Fragment 1）。
    int _fragment_id;
    // BE 节点的全局执行环境单例指针，用于获取线程池、内存池、网络传输等全局服务。
    ExecEnv* _exec_env = nullptr;
    // 标记当前 PipelineFragmentContext 是否已经成功完成了 prepare() 阶段（构造算子树与 Task）。
    std::atomic_bool _prepared = false;
    // 标记当前 Context 中的所有 PipelineTask 是否已被提交到 BE 的 Pipeline 调度器（Task Scheduler）中。
    bool _submitted = false;
    // 存储当前 Fragment 内构建出来的所有抽象 Pipeline 对象列表（std::vector<PipelinePtr>）。
    Pipelines _pipelines;
    // 生成 Pipeline ID 的递增计数器。
    PipelineId _next_pipeline_id = 0;
    // 保护任务状态更新（如 _closed_tasks、_tasks 的安全访问）的互斥锁。
    std::mutex _task_mutex;
    // 记录当前 Fragment 中已经执行完毕并 Close 的 PipelineTask 数量。当 _closed_tasks == _total_tasks 时，整个 Fragment 执行结束。
    int _closed_tasks = 0;
    // After prepared, `_total_tasks` is equal to the size of `_tasks`.
    // When submit fail, `_total_tasks` is equal to the number of tasks submitted.
    // 当前 Fragment 包含的 PipelineTask 总数。在 prepare 完成后等于矩阵的总任务数（instances * pipelines）；在提交失败时记录实际提交的数量。
    std::atomic<int> _total_tasks = 0;
    // Fragment 级别的总 RuntimeProfile 指标树，记录该 Fragment 的整体执行耗时和指标。
    std::unique_ptr<RuntimeProfile> _fragment_level_profile;
    // This is used by loading process to report Fragment exec status to FE, FE need fragment status to
    // check if the loading process is finished. And during the report, BE will send the loading message to FE,
    // for example the loading error, commit rows num etc.
    // 针对 Broker Load / Stream Load 等导入任务，标记是否需要在成功完成时向 FE 汇报导入状态（如导入行数、错误文件 URL 等）。
    bool _is_report_success = false;
    // Fragment 级别的全局 RuntimeState，包含查询参数、全局内存 Tracker、表达式上下文等。
    std::unique_ptr<RuntimeState> _runtime_state;
    // 指向当前 Query 级别上下文的共享指针。多个 FragmentContext 共享同一个 QueryContext，用于共享 Query 级别的资源、状态（如是否被 Cancel）。
    std::shared_ptr<QueryContext> _query_ctx;
    // 精确计时器，记录当前 Fragment 从创建/ Prepare 到当前时刻的累计运行时间。
    MonotonicStopWatch _fragment_watcher;
    // Profile 计数器，记录 prepare() 阶段的总耗时。
    RuntimeProfile::Counter* _prepare_timer = nullptr;
    // Profile 计数器，记录初始化 RuntimeState / Context 的耗时。
    RuntimeProfile::Counter* _init_context_timer = nullptr;
    // Profile 计数器，记录将 Thrift 计划翻译构建为 Pipeline 结构树的耗时。
    RuntimeProfile::Counter* _build_pipelines_timer = nullptr;
    // Profile 计数器，记录规划并插入 Local Exchange（线程间数据 Shuffle/重分配）算子的耗时。
    RuntimeProfile::Counter* _plan_local_exchanger_timer = nullptr;
    // Profile 计数器，记录对所有 Pipelines 及其算子进行 Prepare 初始化的耗时。
    RuntimeProfile::Counter* _prepare_all_pipelines_timer = nullptr;
    // Profile 计数器，记录由 Pipeline 实例化生成 PipelineTask 矩阵的耗时。
    RuntimeProfile::Counter* _build_tasks_timer = nullptr;
    // Fragment 完成时的回调函数，通常用于触发 Coordinator 汇报或通知上层管理模块。
    std::function<void(RuntimeState*, Status*)> _call_back;
    // 标记当前 Fragment Instance 是否已经调用过 _close_fragment_instance()，确保仅关闭一次。
    std::atomic_bool _is_fragment_instance_closed = false;

    // 0 indicates reporting is in progress or not required
    // 原子布尔值，为 true 时禁用定时向 FE 汇报状态（例如在汇报动作正在进行中或不需要定时汇报时置为 true）
    std::atomic_bool _disable_period_report = true;
    // 记录上一次向 FE 发送状态汇报的时间戳（纳秒/毫秒）。
    std::atomic_uint64_t _previous_report_time = 0;
    // 指向当前 Fragment 依赖的 Descriptor Table（Tuple/Slot 描述符表）指针。
    DescriptorTbl* _desc_tbl = nullptr;
    // 当前 BE 上运行的该 Fragment 的并行 Instance 数量。
    int _num_instances = 1;
    // Fragment 执行的超时时间（单位：秒）。
    int _timeout = -1;
    // 标记是否使用了单线程/串行 Source 算子（如包含 ORDER BY LIMIT 的合并算子）。
    bool _use_serial_source = false;
    // 当前 Fragment 算子树的根节点 Operator（如 DataSink 前的最上层 Operator）。
    OperatorPtr _root_op = nullptr;
    //
    /**
     * Matrix stores tasks with local runtime states.
     * This is a [n * m] matrix. n is parallelism of pipeline engine and m is the number of pipelines.
     *
     * 2-D matrix:
     * +-------------------------+------------+-------+
     * |            | Pipeline 0 | Pipeline 1 |  ...  |
     * +------------+------------+------------+-------+
     * | Instance 0 |  task 0-0  |  task 0-1  |  ...  |
     * +------------+------------+------------+-------+
     * | Instance 1 |  task 1-0  |  task 1-1  |  ...  |
     * +------------+------------+------------+-------+
     * | ...                                          |
     * +--------------------------------------+-------+
     */
    // 存储实例化后的 PipelineTask 和每个 Task 专属的 RuntimeState 构成的二维矩阵：尺寸为 [num_instances][num_pipelines]。通过该矩阵可以精确定位和管理每一个具体执行单元。
    std::vector<
            std::vector<std::pair<std::shared_ptr<PipelineTask>, std::unique_ptr<RuntimeState>>>>
            _tasks;

    // TODO: remove the _sink and _multi_cast_stream_sink_senders to set both
    // of it in pipeline task not the fragment_context
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshadow-field"
#endif
    // 当前 Fragment 的数据输出 DataSinkOperator（如 DataStreamSinkOperator、OlapTableSinkOperator）。
    DataSinkOperatorPtr _sink = nullptr;
#ifdef __clang__
#pragma clang diagnostic pop
#endif

    // `_dag` manage dependencies between pipelines by pipeline ID. the indices will be blocked by members
    // 维护 Pipeline 之间的依赖关系 DAG 图（Key 为下游 Pipeline ID，Value 为其依赖的上游 Pipeline ID 列表）。
    std::map<PipelineId, std::vector<PipelineId>> _dag;

    // We use preorder traversal to create an operator tree. When we meet a join node, we should
    // build probe operator and build operator in separate pipelines. To do this, we should build
    // ProbeSide first, and use `_pipelines_to_build` to store which pipeline the build operator
    // is in, so we can build BuildSide once we complete probe side.
    // 用于在先序遍历 Thrift 树构建算子时，记录 Join 节点等需要分支的 Build-side Pipelines 映射，帮助正确建立 Probe 与 Build 侧 Pipeline 的连接。
    // 核心背景：FE 计划树与 Pipeline 的映射矛盾
    // FE（Frontend）下发的执行计划树是以数组形式平铺传输的（先序遍历：根节点 -> 子节点0 -> 子节点0的子树 -> 子节点1 -> 子节点1的子树...）
    // 在 Pipeline 架构中：
    // 单输入算子（如 Filter、Project）可以继续留在当前 cur_pipe。
    // 多输入算子（如 HashJoinNode 有 Build/Probe 两侧，UnionNode 有 $N$ 个输入分支）在解析到父节点自身时，就会预先创建好对应分支的 Pipeline。
    // 当解析流程接着向后遍历解析到其子节点时，BE 需要知道当前的子节点应该属于父节点刚才创建的哪一条 Pipeline，这就是 _pipeline_parent_map 解决的核心问题。
    struct pipeline_parent_map {
        // Key: 父算子的 plan_node_id
        // Value: 该父算子为它的各个子分支创建的 Pipeline 列表
        std::map<int, std::vector<PipelinePtr>> _build_side_pipelines;
        // 1. 父节点解析时调用：记录为当前父节点创建的子分支 Pipeline
        void push(int parent_node_id, PipelinePtr pipeline) {
            if (!_build_side_pipelines.contains(parent_node_id)) {
                _build_side_pipelines.insert({parent_node_id, {pipeline}});
            } else {
                _build_side_pipelines[parent_node_id].push_back(pipeline);
            }
        }
        // 2. 子节点解析前调用：将 cur_pipe 切换为当前子节点对应的分支 Pipeline
        void pop(PipelinePtr& cur_pipe, int parent_node_id, int child_idx) {
            if (!_build_side_pipelines.contains(parent_node_id)) {
                return; // 如果父节点没有注册过多分支（例如单子节点算子），保持当前 cur_pipe 不变
            }
            DCHECK(_build_side_pipelines.contains(parent_node_id));
            auto& child_pipeline = _build_side_pipelines[parent_node_id];
            DCHECK(child_idx < child_pipeline.size());
            // 关键切换：将上下文中的 cur_pipe 重置为当前 child_idx 所指向的分支 Pipeline
            cur_pipe = child_pipeline[child_idx];
        }
        void clear() { _build_side_pipelines.clear(); }
    } _pipeline_parent_map;
    // 保护 _op_id_to_shared_state 映射表修改的互斥锁。
    std::mutex _state_map_lock;

    // Start from -1 so all operator IDs are negative. This avoids collision with
    // unpaired sinks (OlapTableSink etc.) whose hardcoded dest_id=0 would otherwise
    // match the first operator's ID when FE-planned LocalExchangeNode is the root.
    // 算子 ID 分配计数器（递减，起始为 -1，生成负数 ID），防止与 FE 规划的固定的正数 Node ID / Sink ID 冲突。
    int _operator_id = -1;
    // Sink 算子 ID 分配计数器（递减，起始为 -1）。
    int _sink_operator_id = -1;
    /**
     * Some states are shared by tasks in different pipeline task (e.g. local exchange , broadcast join).
     *
     * local exchange sink 0 ->                               -> local exchange source 0
     *                            LocalExchangeSharedState
     * local exchange sink 1 ->                               -> local exchange source 1
     *
     * hash join build sink 0 ->                               -> hash join build source 0
     *                              HashJoinSharedState
     * hash join build sink 1 ->                               -> hash join build source 1
     *
     * So we should keep states here.
     */
    // 维护 Operator ID 到其跨线程/跨 Task 共享状态（BasicSharedState）及事件依赖（Dependency）的映射关系。例如 Local Exchange 或 Hash Join Build/Probe 共享的状态。
    std::map<int,
             std::pair<std::shared_ptr<BasicSharedState>, std::vector<std::shared_ptr<Dependency>>>>
            _op_id_to_shared_state;
    // Pipeline ID 到 Pipeline 对象的快速 lookup 映射表。
    std::map<PipelineId, Pipeline*> _pip_id_to_pipeline;
    // 每个 Instance 专属的 RuntimeFilterMgr 管理器列表，用于处理该 Instance 上动态 Bloom Filter / MinMax Filter 的生成与合并。
    std::vector<std::unique_ptr<RuntimeFilterMgr>> _runtime_filter_mgr_map;

    // Deferred exchanger creation info for FE-planned local exchanges.
    // Exchanger sender count depends on the upstream pipeline's final num_tasks,
    // which is only known after the full plan tree is built (child operators like
    // serial ExchangeNode may reduce num_tasks). So we defer exchanger creation
    // until after _build_pipelines completes.
    struct DeferredExchangerInfo {
        std::shared_ptr<LocalExchangeSharedState> shared_state;
        PipelinePtr upstream_pipe;
        TLocalPartitionType::type partition_type;
        int num_partitions;
        int free_blocks_limit;
        int local_exchange_id;
        int sink_id;
    };
    // 延迟创建 Local Exchange 缓冲区的结构体与列表。由于 Exchange 的发送方数量依赖于上游 Pipeline 的最终 Task 数（可能会被串行算子改变），必须等整套 Pipelines 建立完毕后才能最终确定并创建 Exchanger。
    std::vector<DeferredExchangerInfo> _deferred_exchangers;
    Status _create_deferred_local_exchangers();
    // After _build_pipelines, propagate _num_instances from FE-planned LOCAL_EXCHANGE
    // pipelines upward through the DAG to ancestor pipelines that inherited reduced
    // num_tasks from a serial operator.
    void _propagate_local_exchange_num_tasks();

    //Here are two types of runtime states:
    //    - _runtime state is at the Fragment level.
    //    - _task_runtime_states is at the task level, unique to each task.
    // 当前 Fragment 在所有并发 Instance 上的唯一 Instance ID 列表。
    std::vector<TUniqueId> _fragment_instance_ids;

    // Total instance num running on all BEs
    // 分布式集群中该 Fragment 在所有 BE 上的总 Instance 数。
    int _total_instances = -1;
    // FE 下发的完整 Thrift 请求结构体 TPipelineFragmentParams，包含 Fragment 计划树、数据源分片信息等。
    TPipelineFragmentParams _params;
    // 并发 Instance 的数量。
    int32_t _parallel_instances = 0;

    // Query cache context of this fragment, shared by the olap scan operator
    // and the cache source operator so both consume the same per-instance
    // cache decision (HIT / INCREMENTAL / MISS). Created lazily when the
    // fragment carries a query_cache_param. See QueryCacheRuntime.
    // 当前 Fragment 级别的查询缓存上下文，由 OLAP Scan 算子和 Cache Source 算子共享，确保它们使用的是同一个缓存命中/未命中判定结果。
    std::shared_ptr<QueryCacheRuntime> _query_cache_runtime;
    // 标记是否需要通知 Close 状态。
    std::atomic<bool> _need_notify_close = false;
    // Holds the brpc ClosureGuard for async wait-close during recursive CTE rerun.
    // When the PFC finishes closing and is destroyed, the shared_ptr destructor fires
    // the ClosureGuard, which completes the brpc response to the RecCTESourceOperatorX.
    // Only written by listen_wait_close() from a single rerun_fragment RPC thread.
    // 持有 BRPC 异步响应的 Guard，在递归 CTE（Recursive CTE）重运行期间延迟发送 RPC 回复，直到当前 Context 真正析构清理完成。
    std::shared_ptr<brpc::ClosureGuard> _wait_close_guard = nullptr;

    // The recursion round number for recursive CTE fragments.
    // Incremented each time the fragment is rebuilt via rerun_fragment(rebuild).
    // Used to stamp runtime filter RPCs so stale messages from old rounds are discarded.
    // 递归 CTE 的递归轮次编号（Recursion Round）。每当重建/重新运行该 Fragment 时递增，用于给 Runtime Filter RPC 贴时间戳标记，防止过期轮次的消息污染新一轮计算。
    uint32_t _rec_cte_stage = 0;
};
} // namespace doris
