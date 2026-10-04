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
#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/QueryPlanExtra_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/types.pb.h>

#include <cstdint>
#include <functional>
#include <iosfwd>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/be_mock_util.h"
#include "common/metrics/metrics.h"
#include "common/status.h"
#include "exec/runtime_filter/runtime_filter_mgr.h"
#include "runtime/query_context.h"
#include "service/http/rest_monitor_iface.h"
#include "util/countdown_latch.h"
#include "util/hash_util.hpp" // IWYU pragma: keep

namespace butil {
class IOBufAsZeroCopyInputStream;
}

namespace doris {
extern bvar::Adder<uint64_t> g_fragment_executing_count;
extern bvar::Status<uint64_t> g_fragment_last_active_time;

class PipelineFragmentContext;
class QueryContext;
class ExecEnv;
struct FrontendInfo;
class ThreadPool;
class PExecPlanFragmentStartRequest;
class PMergeFilterRequest;
class RuntimeProfile;
class RuntimeState;
class TPipelineFragmentParams;
class TPipelineInstanceParams;
class TScanColumnDesc;
class TScanOpenParams;
class Thread;
class WorkloadQueryInfo;
// 根据文件名拼接并生成 Stream Load / Broker Load 导入失败时的错误日志 HTTP 下载 URL。
std::string to_load_error_http_path(const std::string& file_name);

// 为了避免单个全局大锁在并发 Prepare 或 Cancel 阶段导致死锁/性能瓶颈（如注释中所述的 prepare -> expr prepare -> memory allocate -> cancel -> lock 锁循环依赖），
// Doris 设计了分段/分桶并发 Map。
template <typename Key, typename Value, typename ValueType>
class ConcurrentContextMap {
public:
    using ApplyFunction = std::function<Status(phmap::flat_hash_map<Key, Value>&)>;
    ConcurrentContextMap();
    Value find(const Key& query_id);
    void insert(const Key& query_id, std::shared_ptr<ValueType>);
    void clear();
    bool erase(const Key& query_id);
    size_t num_items() const {
        size_t n = 0;
        for (auto& pair : _internal_map) {
            std::shared_lock lock(*pair.first);
            auto& map = pair.second;
            n += map.size();
        }
        return n;
    }
    void apply(ApplyFunction&& function) {
        for (auto& pair : _internal_map) {
            // TODO: Now only the cancel worker do the GC the _query_ctx_map. each query must
            // do erase the finish query unless in _query_ctx_map. Rethink the logic is ok
            std::unique_lock lock(*pair.first);
            static_cast<void>(function(pair.second));
        }
    }

    Status apply_if_not_exists(const Key& query_id, std::shared_ptr<ValueType>& query_ctx,
                               ApplyFunction&& function);

private:
    // The lock should only be used to protect the structures in fragment manager. Has to be
    // used in a very small scope because it may dead lock. For example, if the _lock is used
    // in prepare stage, the call path is  prepare --> expr prepare --> may call allocator
    // when allocate failed, allocator may call query_is_cancelled, query is callced will also
    // call _lock, so that there is dead lock.
    // 包含多个带读写锁的分桶 Map，通过哈希分散锁竞争。
    std::vector<std::pair<std::unique_ptr<std::shared_mutex>, phmap::flat_hash_map<Key, Value>>>
            _internal_map;
};

// This class used to manage all the fragment execute in this instance
// FragmentMgr（Fragment Manager）是 Apache Doris BE 节点中负责查询执行片段（Plan Fragment）与查询上下文（QueryContext）生命周期管理的的核心组件。
// 其核心作用可以概括为以下几点：
// Fragment 与 Query 级别的调度与管理：接收来自 FE（Frontend）或外部引擎下发的 Plan Fragment 执行请求，创建并管理 PipelineFragmentContext（Pipeline 执行上下文）和 QueryContext（查询全局上下文）。
// 生命周期与并发安全映射（Concurrent Mapping）：维护当前 BE 节点正在执行的所有 Query 以及 Pipeline Fragment 的映射关系（通过线程安全的 ConcurrentContextMap 容器管理），处理片段的准备、启动、取消、完成清理及延迟释放。
// 全局异步任务与后台轮询监控（Cancel Worker）：内置后台监控线程（cancel_thread），定期巡检并清理超时的查询、丢失 Coordinator 联系的孤儿查询（Orphan Queries）、以及执行资源泄露的异常任务。
// 运行时 Runtime Filter（动态过滤）与高级特性调度： 处理跨节点 Dynamic Runtime Filter 的合并、发布与同步；同时提供对递归 CTE（Recursive CTE）片段重跑（Rerun）等高级查询模式的支持。
class FragmentMgr : public RestMonitorIface {
public:
    using FinishCallback = std::function<void(RuntimeState*, Status*)>;

    FragmentMgr(ExecEnv* exec_env);
    ~FragmentMgr() override;
    // 停止 FragmentMgr 服务。触发 _stop_background_threads_latch 信号，停止后台 cancel_thread，并关闭/销毁专用线程池。
    void stop();

    // execute one plan fragment
    // 执行常规 Pipeline Plan Fragment 的重载入口。内部转调用带 FinishCallback 的重载版本。
    Status exec_plan_fragment(const TPipelineFragmentParams& params, const QuerySource query_type,
                              const TPipelineFragmentParamsList& parent);
    // 根据 (QueryID, FragmentID) 从 _pipeline_map 中移除对应的 PipelineFragmentContext，在 Fragment 执行完成或异常退出时被调用。
    void remove_pipeline_context(std::pair<TUniqueId, int> key);
    // 从 _query_ctx_map 及延迟清理表中彻底移除指定 Query 的 QueryContext，清理所有相关的全局资源。
    void remove_query_context(const TUniqueId& key);

    // `is_prepare_success` is used by invoker to ensure callback can be handle correctly (eg. stream_load_executor)
    // 执行 Plan Fragment 的核心入口。
    Status exec_plan_fragment(const TPipelineFragmentParams& params, const QuerySource query_type,
                              const FinishCallback& cb, const TPipelineFragmentParamsList& parent,
                              std::shared_ptr<bool> is_prepare_success = nullptr);
    // 接收并处理 FE 下发的 Start 异步指令，真正开始调度并运行指定 Query 的各个 Fragment Pipeline Task。
    Status start_query_execution(const PExecPlanFragmentStartRequest* request);

    // Can be used in both version.
    // 取消指定 query_id 的查询。
    MOCK_FUNCTION void cancel_query(const TUniqueId query_id, const Status reason);
    // 后台线程的主循环函数。定时轮询（通常每隔数百毫秒至数秒）：
    void cancel_worker();
    // 实现 RestMonitorIface 接口，向 HTTP /debug 诊断页面输出当前 FragmentMgr 内部状态的调试文本信息。
    void debug(std::stringstream& ss) override;

    // input: TQueryPlanInfo fragment_instance_id
    // output: selected_columns
    // execute external query, all query info are packed in TScanOpenParams
    // 执行来自外部系统（如 Doris Connector / External Scan）的 Plan Fragment 请求，解析 TScanOpenParams 并返回选中的列描述符信息（selected_columns）。
    Status exec_external_plan_fragment(const TScanOpenParams& params,
                                       const TQueryPlanInfo& t_query_plan_info,
                                       const TUniqueId& query_id,
                                       const TUniqueId& fragment_instance_id,
                                       std::vector<TScanColumnDesc>* selected_columns);
    // 接收并发布版本 2 的 Runtime Filter。将远程发送过来的全局过滤条件（Bloom Filter/MinMax 等数据）解码并应用到对应的 Fragment 上下文中。
    Status apply_filterv2(const PPublishFilterRequestV2* request,
                          butil::IOBufAsZeroCopyInputStream* attach_data);
    // 作为 Runtime Filter 合并节点（Coordinator/Merge Node），接收来自各个 HashJoin/Scan 节点发来的局部 Filter 并进行全局 Merge。
    Status merge_filter(const PMergeFilterRequest* request,
                        butil::IOBufAsZeroCopyInputStream* attach_data);
    // 发送 Runtime Filter 构建出的实际大小/基数统计信息，辅助动态调整 Filter 类型的决策。
    Status send_filter_size(const PSendFilterSizeRequest* request);
    // 同步各节点上 Runtime Filter 的大小分布信息。
    Status sync_filter_size(const PSyncFilterSizeRequest* request);
    // 获取 FragmentMgr 内部管理的异步线程池指针
    ThreadPool* get_thread_pool() { return _thread_pool.get(); }

    // When fragment mgr is going to stop, the _stop_background_threads_latch is set to 0
    // and other module that use fragment mgr's thread pool should get this signal and exit.
    // 检查 FragmentMgr 是否处于关闭状态（检查 _stop_background_threads_latch.count() == 0）。
    bool shutting_down() { return _stop_background_threads_latch.count() == 0; }
    // 获取当前 BE 节点上正在运行的查询总数（通过 _query_ctx_map.num_items() 统计）。
    int32_t running_query_num() { return cast_set<int32_t>(_query_ctx_map.num_items()); }
    // 导出当前运行时间超过 duration 毫秒的所有 Pipeline Task 的堆栈/状态 Dump 字符串，用于诊断查询卡死（Hang）。
    std::string dump_pipeline_tasks(int64_t duration = 0);
    // 仅导出指定 query_id 下的所有 Pipeline Task 的执行状态信息
    std::string dump_pipeline_tasks(TUniqueId& query_id);
    // 收集当前所有运行中查询的资源上下文（ResourceContext），用于 Workload Group 资源隔离与 CGroup 额度分配。
    void get_runtime_query_info(std::vector<std::weak_ptr<ResourceContext>>* _resource_ctx_list);
    // 实时拉取并组装指定 Query 当前的执行 Profile 与 Running Status，上报给 FE 或用于 HTTP 监控。
    Status get_realtime_exec_status(const TUniqueId& query_id,
                                    TReportExecStatusParams* exec_status);
    // get the query statistics of with a given query id
    // 获取指定 Query 的资源与数据量统计信息（如 Peak Memory、Scan Rows、Scan Bytes 等）。
    Status get_query_statistics(const TUniqueId& query_id, TQueryStatistics* query_stats);
    // 根据 query_id 从 _query_ctx_map 中查找并提升返回对应的 QueryContext 强引用指针；若查询不存在或已被释放则返回 nullptr。
    std::shared_ptr<QueryContext> get_query_ctx(const TUniqueId& query_id);
    // 传输递归 CTE（Recursive Common Table Expression）计算过程中产生的中间结果数据 Block。
    Status transmit_rec_cte_block(const TUniqueId& query_id, const TUniqueId& instance_id,
                                  int node_id,
                                  const google::protobuf::RepeatedPtrField<PBlock>& pblocks,
                                  bool eos);
    // 触发指定 Fragment 的重新运行（Rerun）。
    // 在递归 CTE 等迭代算法中，根据传入的 stage 阶段指令，清空/重建旧的 PipelineFragmentContext，利用保存的 RerunableFragmentInfo 再次拉起 Fragment 计算。
    Status rerun_fragment(const std::shared_ptr<brpc::ClosureGuard>& guard,
                          const TUniqueId& query_id, int fragment,
                          PRerunFragmentParams_Opcode stage);

    Status reset_global_rf(const TUniqueId& query_id,
                           const google::protobuf::RepeatedField<int32_t>& filter_ids);

private:
    // 用于后台线程批量检测与特定 Coordinator 节点的 bRPC 通信连通性。
    struct BrpcItem {
        // 对应 FE 或 Coordinator 节点的网络地址
        TNetworkAddress network_address;
        // 绑定到该 Coordinator 的查询上下文弱引用列表。
        std::vector<std::weak_ptr<QueryContext>> queries;
    };

    Status _get_or_create_query_ctx(const TPipelineFragmentParams& params,
                                    const TPipelineFragmentParamsList& parent,
                                    QuerySource query_type,
                                    std::shared_ptr<QueryContext>& query_ctx);
    // 检查所有运行中的 QueryContext，收集超时的 Query ID，同时收集与各 FE/Coordinator 节点的 bRPC 存活检测项（BrpcItem）。
    void _collect_timeout_queries_and_brpc_items(
            std::vector<TUniqueId>& queries_timeout,
            std::unordered_map<std::shared_ptr<PBackendService_Stub>, BrpcItem>&
                    brpc_stub_with_queries,
            timespec now);
    // 结合 FE 节点的存活状态（running_fes）与所有 FE 上的活跃查询列表（running_queries_on_all_fes），找出当前 BE 上失去 Coordinator 的“孤儿查询”以及 Pipeline Task 泄露的非法查询并予以清理。
    void _collect_invalid_queries(
            std::vector<TUniqueId>& queries_lost_coordinator,
            std::vector<TUniqueId>& queries_pipeline_task_leak,
            const std::map<int64_t, std::unordered_set<TUniqueId>>& running_queries_on_all_fes,
            const std::map<TNetworkAddress, FrontendInfo>& running_fes,
            timespec check_invalid_query_last_timestamp);
    // 探查与指定 Coordinator 的 bRPC 连接是否通畅，若连接已断开，则提前取消依赖该 Coordinator 的所有查询。
    void _check_brpc_available(const std::shared_ptr<PBackendService_Stub>& brpc_stub,
                               const BrpcItem& brpc_item);

    // This is input params
    // 指向 BE 全局执行环境的指针，提供线程池、Memory Tracker、RPC 客户端等全局服务。
    ExecEnv* _exec_env = nullptr;

    // (QueryID, FragmentID) -> PipelineFragmentContext
    // (QueryID, FragmentID) -> PipelineFragmentContext 的并发映射表，维护 BE 上所有激活的 Pipeline Fragment 上下文。
    ConcurrentContextMap<std::pair<TUniqueId, int>, std::shared_ptr<PipelineFragmentContext>,
                         PipelineFragmentContext>
            _pipeline_map;

    // Saved params and callback for rerunnable (recursive CTE) fragments.
    // Only populated when need_notify_close == true during exec_plan_fragment.
    // Lifecycle: created in exec_plan_fragment(), retained across wait/rebuild/submit rounds,
    // and removed after a successful final_close. remove_query_context() and stop() provide
    // fallback cleanup. Entries are detached under _rerunnable_params_lock and destroyed afterward;
    // releasing their last QueryContext reference can call FragmentMgr::remove_query_context().
    // 保存支持重跑的 Fragment（如递归 CTE）的上下文参数与状态，实现迭代计算。
    struct RerunableFragmentInfo {
        // Runtime filter IDs registered by the old PFC, collected during wait_for_destroy.
        // These are deregistered from the RuntimeFilterMgr before the new PFC is created.
        // 旧的 PipelineFragmentContext 注册的 Runtime Filter ID 集合，在片段重跑前需从全局注册表中注销。
        std::set<int> deregister_runtime_filter_ids;
        // Original params from FE, used to recreate the PFC each round.
        // FE 下发的原始 Fragment 执行参数，用于重新构建片段。
        TPipelineFragmentParams params;
        // 父 Fragment 参数列表。
        TPipelineFragmentParamsList parent;
        // 片段执行结束时的回调函数。
        FinishCallback finish_callback;
        // Hold query_ctx to prevent it from being destroyed while rerunnable fragments exist.
        // 持有的 QueryContext 强引用，防止重跑期间 QueryContext 被提前释放。
        std::shared_ptr<QueryContext> query_ctx;
        // Monotonically increasing stage counter, stamps runtime filter RPCs.
        // 单调递增的 Stage 计数器，用于标记和版本化 Runtime Filter RPC。
        uint32_t stage = 0;
    };
    // 保护 _rerunnable_params_map 读写互斥的互斥锁。
    std::mutex _rerunnable_params_lock;
    // 存储所有可重跑 Fragment 的配置与回调。
    std::map<std::pair<TUniqueId, int>, RerunableFragmentInfo> _rerunnable_params_map;

    // query id -> QueryContext
    // QueryID -> weak_ptr<QueryContext> 的并发映射表，管理 Query 级全局上下文（弱引用，防循环引用）。
    ConcurrentContextMap<TUniqueId, std::weak_ptr<QueryContext>, QueryContext> _query_ctx_map;
    // keep query ctx do not delete immediately to make rf coordinator merge filter work well after query eos
    // QueryID -> shared_ptr<QueryContext> 的延迟删除映射表。即使 Query 执行完 EOS，仍短期持有 QueryContext 强引用，确保 Coordinator 的 Merge Runtime Filter 等滞后 RPC 能正常处理。
    ConcurrentContextMap<TUniqueId, std::shared_ptr<QueryContext>, QueryContext>
            _query_ctx_map_delay_delete;
    // 倒计时门栓，用于通知和等待后台线程（如 cancel_thread）安全退出。
    CountDownLatch _stop_background_threads_latch;
    // 后台巡检线程句柄，运行 cancel_worker() 循环。
    std::shared_ptr<Thread> _cancel_thread;
    // This pool is used as global async task pool
    // FragmentMgr 专用的全局异步任务线程池（用于异步取消、执行异步 Plan Fragment 等）。
    std::unique_ptr<ThreadPool> _thread_pool;
    // Prometheus/bvar 监控指标实体。
    std::shared_ptr<MetricEntity> _entity;
    // 度量指标指针，记录因超时而被取消的 Fragment 数量。
    UIntGauge* timeout_canceled_fragment_count = nullptr;
};

uint64_t get_fragment_executing_count();
uint64_t get_fragment_last_active_time();
} // namespace doris
