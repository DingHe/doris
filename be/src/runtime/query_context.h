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
#include <gen_cpp/RuntimeProfile_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "common/config.h"
#include "common/factory_creator.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "exec/common/memory.h"
#include "exec/runtime_filter/runtime_filter_mgr.h"
#include "exec/scan/scanner_scheduler.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/runtime_predicate.h"
#include "runtime/workload_group/workload_group_fwd.h"
#include "runtime/workload_management/resource_context.h"
#include "util/hash_util.hpp"
#include "util/threadpool.h"

namespace doris {

namespace io {
class RemoteScanCacheWriteLimiter;
} // namespace io

class PipelineFragmentContext;
class PipelineTask;
class QueryTaskController;
class Dependency;
class RecCTEScanLocalState;
class SpillDataDir;

struct ReportStatusRequest {
    const Status status;
    std::vector<RuntimeState*> runtime_states;
    bool done;
    TNetworkAddress coord_addr;
    TUniqueId query_id;
    int fragment_id;
    TUniqueId fragment_instance_id;
    int backend_num;
    RuntimeState* runtime_state;
    std::string load_error_url;
    std::string first_error_msg;
    std::function<void(const Status&)> cancel_fn;
};
// 标识当前查询/作业的来源类型。
// Doris BE 在收到 Fragment 执行请求（如 exec_plan_fragment）时，通过传入 QuerySource 来对不同类型的作业进行资源隔离、内存追踪（MemTracker 分类）、优先级调度、审计日志统计以及监控度量（Metrics）。
enum class QuerySource {
    // 内部 FE 发起的 SQL 查询/作业
    // 最常见的来源类型。代表由 Doris 内部 Frontend（FE）节点经过 Parse、Analyze、Optimize 编译后直接下发的标准 SQL（如 SELECT 查询、INSERT INTO SELECT、Broker Load 等）。
    INTERNAL_FRONTEND,
    // Stream Load 导入作业
    // 代表用户通过 HTTP 协议直接向 BE 节点发起的 Stream Load 数据导入请求。通常拥有单独的线程池或内存管理管道。
    STREAM_LOAD,
    // Group Commit（组提交）导入作业
    // 针对高频小批量写入场景（如微批实时写入）。标识该作业属于 Group Commit 机制，数据会被合并入缓存 WAL 中，以提高并发写入性能并降低 FE 的调度开销。
    GROUP_COMMIT_LOAD,
    // Routine Load（例行导入）作业
    // 代表由 Doris 内部例行导入任务（如持续消费 Kafka 消息队列）所产生的流式数据导入 Task。
    ROUTINE_LOAD,
    // 外部 Connector（如 Spark/Flink/Presto Connector）直接导入/写入
    // 代表通过 Doris 暴露给外部计算框架的 Connector 接口发起的并发写入或读写任务（例如 Spark-Doris-Connector / Flink-Doris-Connector 数据写入）。
    EXTERNAL_CONNECTOR,
    // 外部 Frontend / 外部元数据/计算引擎发起的查询
    // 代表由外部交互引擎/计算系统（如结合第三方 Query Engine 或异构 Doris 集群架构）直接发起的查询片段。
    EXTERNAL_FRONTEND
};

const std::string toString(QuerySource query_source);

// Save the common components of fragments in a query.
// Some components like DescriptorTbl may be very large
// that will slow down each execution of fragments when DeSer them every time.
class DescriptorTbl;

// QueryContext 是当前 BE 节点上针对单个 SQL 查询（Query）生命周期最核心的上下文管理类。
// 单 Query 粒度的资源与上下文统筹：一个 SQL 在分布式执行时，FE 会将 Plan 拆分为多个 Fragment 发送到 BE。即使同一节点执行多个 Fragment，它们都共享同一个 QueryContext 实例。
// 生命周期与状态控制：统一管理查询在当前 BE 上的执行状态（正常、失败、取消）、超时检测（Timeout Watcher）以及两阶段执行（Prepare/Execute）的依赖解耦（Dependency）。
// 全局资源隔离与内存控制：维护查询绑定的 Workload Group、资源上下文（ResourceContext）、内存跟踪器（MemTrackerLimiter）以及 Low Memory 模式调度。
// 运行时协调与跨 Fragment 共享：跨 Fragment 共享全局描述符表（DescriptorTbl）、Runtime Filter 管理器（RuntimeFilterMgr）、bRPC 连接存根（RPC Stub Cache）、CTE 数据传输通道（Recursive CTE Scan）以及 Spill 落盘目录等。
// Profile 采集与状态上报：收集当前 BE 上各个 Pipeline/Fragment 的 Execution Profile，向 FE 定期汇报或最终汇报实时执行状态（TReportExecStatusParams）。
class QueryContext : public std::enable_shared_from_this<QueryContext> {
    ENABLE_FACTORY_CREATOR(QueryContext);

public:
    static std::shared_ptr<QueryContext> create(TUniqueId query_id, ExecEnv* exec_env,
                                                const TQueryOptions& query_options,
                                                TNetworkAddress coord_addr, bool is_nereids,
                                                TNetworkAddress current_connect_fe,
                                                QuerySource query_type);

    // use QueryContext::create, cannot be made private because of ENABLE_FACTORY_CREATOR::create_shared.
    QueryContext(TUniqueId query_id, ExecEnv* exec_env, const TQueryOptions& query_options,
                 TNetworkAddress coord_addr, bool is_nereids, TNetworkAddress current_connect_fe,
                 QuerySource query_type);

    ~QueryContext();

    void init_query_task_controller();

    ExecEnv* exec_env() const { return _exec_env; }

    bool is_timeout(timespec now) const {
        if (_timeout_second <= 0) {
            return false;
        }
        return _query_watcher.elapsed_time_seconds(now) > _timeout_second;
    }

    bool is_single_backend_query() const { return _is_single_backend_query; }

    void set_single_backend_query(bool is_single_backend_query) {
        _is_single_backend_query = is_single_backend_query;
    }
    // 计算并返回当前 Query 距离超时的剩余时间（秒）。若已超时则返回 -1。
    int64_t get_remaining_query_time_seconds() const {
        timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);
        if (is_timeout(now)) {
            return -1;
        }
        int64_t elapsed_seconds = _query_watcher.elapsed_time_seconds(now);
        return _timeout_second - elapsed_seconds;
    }
    // Pipeline 引擎为了防止“数据算子未准备好就收到数据”或实现跨节点同步，通过 _execution_dependency 阻塞所有 Pipeline Task。FE 发送二阶段执行指令后，该方法被调用，唤醒所有挂起的调度任务，决定整个 Query 在该 BE 上何时真正“开跑”。
    void set_ready_to_execute(Status reason);

    [[nodiscard]] bool is_cancelled() const { return !_exec_status.ok(); }

    void cancel_all_pipeline_context(const Status& reason, int fragment_id = -1);
    std::string print_all_pipeline_context();
    void set_pipeline_context(const int fragment_id,
                              std::shared_ptr<PipelineFragmentContext> pip_ctx);
    // 控制异常与级联终止的核心阀门。
    // 当任何一个 Pipeline Task（例如内存溢出、算子报错、RPC 超时或客户端断开）发生故障时，都会调用 cancel。它负责原子地更新全局状态 _exec_status，并向当前 BE 节点上该 Query 的所有 Fragment 广播取消信号，阻止无效计算继续浪费 CPU 和内存资源。
    void cancel(Status new_status, int fragment_id = -1);

    [[nodiscard]] Status exec_status() { return _exec_status.status(); }
    // Pipeline 引擎分布式协同与两阶段执行的驱动器。
    // Pipeline 引擎为了防止“数据算子未准备好就收到数据”或实现跨节点同步，通过 _execution_dependency 阻塞所有 Pipeline Task。FE 发送二阶段执行指令后，该方法被调用，唤醒所有挂起的调度任务，决定整个 Query 在该 BE 上何时真正“开跑”。
    void set_execution_dependency_ready();

    void set_memory_sufficient(bool sufficient);

    void set_ready_to_execute_only();

    bool has_runtime_predicate(int source_node_id) {
        return _runtime_predicates.contains(source_node_id);
    }

    RuntimePredicate& get_runtime_predicate(int source_node_id) {
        DCHECK(has_runtime_predicate(source_node_id));
        return _runtime_predicates.find(source_node_id)->second;
    }

    void init_runtime_predicates(const std::vector<TTopnFilterDesc>& topn_filter_descs) {
        for (auto desc : topn_filter_descs) {
            _runtime_predicates.try_emplace(desc.source_node_id, desc);
        }
    }
    // 将当前 Query 绑定到指定的 Workload Group（资源组），用于配额管制。
    Status set_workload_group(WorkloadGroupPtr& wg);
    // 获取实际执行超时时间（优先使用 execution_timeout，若未配置则使用 query_timeout）。
    int execution_timeout() const {
        return _query_options.__isset.execution_timeout ? _query_options.execution_timeout
                                                        : _query_options.query_timeout;
    }

    int32_t runtime_filter_wait_time_ms() const {
        return _query_options.runtime_filter_wait_time_ms;
    }

    int be_exec_version() const {
        if (!_query_options.__isset.be_exec_version) {
            return 0;
        }
        return _query_options.be_exec_version;
    }

    [[nodiscard]] int64_t get_fe_process_uuid() const {
        return _query_options.__isset.fe_process_uuid ? _query_options.fe_process_uuid : 0;
    }

    bool ignore_runtime_filter_error() const {
        return _query_options.__isset.ignore_runtime_filter_error
                       ? _query_options.ignore_runtime_filter_error
                       : false;
    }
    // 获取是否开启了强制数据落盘（Force Spill）控制。
    bool enable_force_spill() const {
        return _query_options.__isset.enable_force_spill && _query_options.enable_force_spill;
    }
    const TQueryOptions& query_options() const { return _query_options; }

    // global runtime filter mgr, the runtime filter have remote target or
    // need local merge should regist here. before publish() or push_to_remote()
    // the runtime filter should do the local merge work
    RuntimeFilterMgr* runtime_filter_mgr() { return _runtime_filter_mgr.get(); }

    TUniqueId query_id() const { return _query_id; }

    // Record a spill data directory before opening the first spill part so teardown only visits
    // touched roots.
    void record_spill_data_dir(SpillDataDir* data_dir);

    // Expose task-level query progress counters for runtime statistics reporting.
    void add_total_task_num(int delta);
    void inc_finished_task_num();

    ScannerScheduler* get_scan_scheduler() { return _scan_task_scheduler; }

    ScannerScheduler* get_remote_scan_scheduler() { return _remote_scan_task_scheduler; }

    Dependency* get_execution_dependency() { return _execution_dependency.get(); }
    Dependency* get_memory_sufficient_dependency() { return _memory_sufficient_dependency.get(); }

    doris::TaskScheduler* get_pipe_exec_scheduler();

    void set_merge_controller_handler(
            std::shared_ptr<RuntimeFilterMergeControllerEntity>& handler) {
        _merge_controller_handler = handler;
    }
    std::shared_ptr<RuntimeFilterMergeControllerEntity> get_merge_controller_handler() const {
        return _merge_controller_handler;
    }

    bool is_nereids() const { return _is_nereids; }
    std::shared_ptr<MemShareArbitrator> mem_arb() const { return _mem_arb; }
    // 获取当前 Query 关联的 WorkloadGroup 对象。
    WorkloadGroupPtr workload_group() const { return _resource_ctx->workload_group(); }
    // 获取当前 Query 顶层内存追踪器 MemTrackerLimiter 的共享指针。
    std::shared_ptr<MemTrackerLimiter> query_mem_tracker() const {
        DCHECK(_resource_ctx->memory_context()->mem_tracker() != nullptr);
        return _resource_ctx->memory_context()->mem_tracker();
    }

    int32_t get_slot_count() const {
        return _query_options.__isset.query_slot_count ? _query_options.query_slot_count : 1;
    }
    // 指向当前 Query 共享的描述符表（Tuple/Slot/Table Descriptor）。
    DescriptorTbl* desc_tbl = nullptr;
    // 标识是否已设置资源信息。
    bool set_rsc_info = false;
    // 提交当前查询的用户名称。
    std::string user;
    // 提交当前查询的用户组名称（或对应的资源组标签）。
    std::string group;
    // 当前 Query 的 Coordinator（协调者节点/FE）网络地址，用于状态汇报。
    TNetworkAddress coord_addr;
    // 发起当前 Session / Query 的 FE 客户端连接网络地址。
    TNetworkAddress current_connect_fe;
    // 查询全局静态信息（如系统当前时间 now()、时区 Timezone、Session ID 等）。
    TQueryGlobals query_globals;
    const TQueryGlobals get_query_globals() const { return query_globals; }
    // 当前 Query 上下文专用的对象池，管理生命周期与 Query 一致的 C++ 对象的内存分配与自动释放。
    ObjectPool obj_pool;

    std::shared_ptr<ResourceContext> resource_ctx() { return _resource_ctx; }

    io::RemoteScanCacheWriteLimiter* remote_scan_cache_write_limiter() const {
        return _remote_scan_cache_write_limiter.get();
    }

    // plan node id -> TFileScanRangeParams
    // only for file scan node
    // 存储 ScanNodeId -> TFileScanRangeParams 的映射，用于 Hive/Iceberg 等外表文件扫描节点的参数共享。
    std::map<int, TFileScanRangeParams> file_scan_range_params_map;
    // 安全地将使用中的网络节点 IP 地址及其对应的 bRPC Stub 句柄注册到缓存中，实现连接复用。
    void add_using_brpc_stub(const TNetworkAddress& network_address,
                             std::shared_ptr<PBackendService_Stub> brpc_stub) {
        if (network_address.port == 0) {
            return;
        }
        std::lock_guard<std::mutex> lock(_brpc_stubs_mutex);
        if (!_using_brpc_stubs.contains(network_address)) {
            _using_brpc_stubs.emplace(network_address, brpc_stub);
        }

        DCHECK_EQ(_using_brpc_stubs[network_address].get(), brpc_stub.get());
    }

    void set_ai_resources(std::map<std::string, TAIResource> ai_resources) {
        _ai_resources =
                std::make_shared<std::map<std::string, TAIResource>>(std::move(ai_resources));
    }

    const std::shared_ptr<std::map<std::string, TAIResource>>& get_ai_resources() const {
        return _ai_resources;
    }

    std::unordered_map<TNetworkAddress, std::shared_ptr<PBackendService_Stub>>
    get_using_brpc_stubs() {
        std::lock_guard<std::mutex> lock(_brpc_stubs_mutex);
        return _using_brpc_stubs;
    }
    // 强制当前 Query 进入 Low Memory 运行模式（只进不出），促使算子采取更保守的内存策略或触发 Spill。
    void set_low_memory_mode() {
        // will not return from low memory mode to non-low memory mode.
        _resource_ctx->task_controller()->set_low_memory_mode(true);
    }
    // 查询当前 Query 是否已开启 Low Memory 模式。
    bool low_memory_mode() { return _resource_ctx->task_controller()->low_memory_mode(); }

    bool is_pure_load_task() {
        return _query_source == QuerySource::STREAM_LOAD ||
               _query_source == QuerySource::ROUTINE_LOAD ||
               _query_source == QuerySource::GROUP_COMMIT_LOAD;
    }

    void set_load_error_url(std::string error_url);
    std::string get_load_error_url();
    void set_first_error_msg(std::string error_msg);
    std::string get_first_error_msg();

    Status send_block_to_cte_scan(const TUniqueId& instance_id, int node_id,
                                  const google::protobuf::RepeatedPtrField<doris::PBlock>& pblocks,
                                  bool eos);
    void registe_cte_scan(const TUniqueId& instance_id, int node_id, RecCTEScanLocalState* scan);
    void deregiste_cte_scan(const TUniqueId& instance_id, int node_id);

    std::vector<int> get_fragment_ids() {
        std::vector<int> fragment_ids;
        for (const auto& it : _fragment_id_to_pipeline_ctx) {
            fragment_ids.push_back(it.first);
        }
        return fragment_ids;
    }

    Status reset_global_rf(const google::protobuf::RepeatedField<int32_t>& filter_ids);

private:
    // Task-level progress counters for current query.
    friend class QueryTaskController;
    // 查询超时时间（秒）。
    int _timeout_second;
    // 查询的全局唯一标识（128位 UUID）。
    TUniqueId _query_id;
    // 指向 BE 进程全局执行环境的指针。
    ExecEnv* _exec_env = nullptr;
    // 单调递增计时器，记录 Query 到达 BE 后的运行时间，用于判断是否超时。
    MonotonicStopWatch _query_watcher;
    // 标识当前查询计划是否由 Doris 新优化器 Nereids 生成。
    bool _is_nereids = false;
    // 保护 Spill 数据目录集合的锁。
    std::mutex _spill_data_dirs_mutex;
    // 记录当前 Query 发生过数据 Spill 落盘的目录集合，便于清理。
    std::unordered_set<SpillDataDir*> _spill_data_dirs;
    // 管理当前 Query 的资源上下文（包含内存、CPU Cgroups、Workload Group、Task Controller 等）。
    std::shared_ptr<ResourceContext> _resource_ctx;

    void _init_resource_context();
    void _init_query_mem_tracker();
    // 保存按 source_node_id 映射的动态运行时谓词（如 Dynamic TopN Filter）。
    std::unordered_map<int, RuntimePredicate> _runtime_predicates;
    // 全局 Runtime Filter 管理器，处理局部构建、全局 Merge 与 Publish。
    std::unique_ptr<RuntimeFilterMgr> _runtime_filter_mgr;
    // 包含当前 Query 的所有 Control Options（如内存限制、并行度、超时设置等）。
    const TQueryOptions _query_options;

    // All pipeline tasks use the same query context to report status. So we need a `_exec_status`
    // to report the real message if failed.
    // 线程安全的原子执行状态（Status）。若某个 Pipeline Task 报错，错误信息会记录于此并扩散至全 Query。
    AtomicStatus _exec_status;
    // 指向 Pipeline 执行任务调度器。
    doris::TaskScheduler* _task_scheduler = nullptr;
    // 指向本地 I/O Scan 任务调度器。
    ScannerScheduler* _scan_task_scheduler = nullptr;
    // 指向远程 I/O Scan 任务调度器（如外表/S3/HDFS）。
    ScannerScheduler* _remote_scan_task_scheduler = nullptr;
    // This dependency indicates if the 2nd phase RPC received from FE.
    // Pipeline 系统的执行依赖。用于等待 FE 下发二阶段执行指令（如 Prepare 完成后等待 Ready）。
    std::unique_ptr<Dependency> _execution_dependency;
    // This dependency indicates if memory is sufficient to execute.
    // 内存充足性依赖。当 BE 整体内存紧张时阻塞任务，内存释放后唤醒。
    std::unique_ptr<Dependency> _memory_sufficient_dependency;

    // This shared ptr is never used. It is just a reference to hold the object.
    // There is a weak ptr in runtime filter manager to reference this object.
    // 若当前 BE 担任 RF Merge 协调节点，该句柄用于控制合并实体的生命周期。
    std::shared_ptr<RuntimeFilterMergeControllerEntity> _merge_controller_handler;
    // 保存 Fragment ID -> PipelineFragmentContext 的弱引用映射，用于管理和查找当前 Query 的所有 Fragment。
    std::map<int, std::weak_ptr<PipelineFragmentContext>> _fragment_id_to_pipeline_ctx;
    // 保护 _fragment_id_to_pipeline_ctx 写的并发锁。
    std::mutex _pipeline_map_write_lock;
    // 保护 Profile 结构映射的互斥锁。
    std::mutex _profile_mutex;
    // 记录 Query 报文到达当前 BE 节点的时间戳。
    timespec _query_arrival_timestamp;
    // Distinguish the query source, for query that comes from fe, we will have some memory structure on FE to
    // help us manage the query.
    // 标识 Query 来源（如 INTERNAL_QUERY、STREAM_LOAD、ROUTINE_LOAD 等）。
    QuerySource _query_source;
    // 保护 bRPC Stub 映射表的互斥锁。
    std::mutex _brpc_stubs_mutex;
    // 缓存当前 Query 正在向其他 BE/FE 发送 RPC 的 Stub 指针，复用 TCP 连接。
    std::unordered_map<TNetworkAddress, std::shared_ptr<PBackendService_Stub>> _using_brpc_stubs;

    // when fragment of pipeline is closed, it will register its profile to this map by using add_fragment_profile
    // flatten profile of one fragment:
    // Pipeline 0
    //      PipelineTask 0
    //              Operator 1
    //              Operator 2
    //              Scanner
    //      PipelineTask 1
    //              Operator 1
    //              Operator 2
    //              Scanner
    // Pipeline 1
    //      PipelineTask 2
    //              Operator 3
    //      PipelineTask 3
    //              Operator 3
    // fragment_id -> list<profile>
    // 按 fragment_id 收集各个 Pipeline Task 关闭时归档的 Runtime Profile 树。
    std::unordered_map<int, std::vector<std::shared_ptr<TRuntimeProfileTree>>> _profile_map;
    // 导入任务中按 fragment_id 保存 Load Channel 的 Profile。
    std::unordered_map<int, std::shared_ptr<TRuntimeProfileTree>> _load_channel_profile_map;
    // 存储与 AI 算子/模型推理相关的资源配置映射。
    std::shared_ptr<std::map<std::string, TAIResource>> _ai_resources;

    void _report_query_profile();

    std::unordered_map<int, std::vector<std::shared_ptr<TRuntimeProfileTree>>>
    _collect_realtime_query_profile();
    // 保护导入错误日志信息的互斥锁。
    std::mutex _error_url_lock;
    // 导入失败时生成的 Error Log URL 链接。
    std::string _load_error_url;
    // 记录 Query 执行过程中遇到的第一个错误详细信息。
    std::string _first_error_msg;
    // 标识当前查询是否仅在单个 BE 节点上执行（单节点查询可进行特定性能优化）。
    bool _is_single_backend_query = false;

    // file cache context holders
    // Block File Cache 上下文持有句柄列表。
    std::vector<io::BlockFileCache::QueryFileCacheContextHolderPtr> _query_context_holders;
    // instance id + node id -> cte scan
    // 映射 (Instance ID, Node ID) -> RecCTEScanLocalState，用于递归 CTE 的数据共享与本地消息传递。
    std::map<std::pair<TUniqueId, int>, RecCTEScanLocalState*> _cte_scan;
    // 保护 CTE Scan 注册表并发访问的锁。
    std::mutex _cte_scan_lock;
    // 内存共享仲裁器，用于在并发算子间动态分配/拆借内存。
    std::shared_ptr<MemShareArbitrator> _mem_arb = nullptr;
    // 远程 Scan 缓存写入限流器，防止远程数据缓存写入挤爆本地磁盘或内存。
    std::unique_ptr<io::RemoteScanCacheWriteLimiter> _remote_scan_cache_write_limiter;

public:
    // when fragment of pipeline is closed, it will register its profile to this map by using add_fragment_profile
    void add_fragment_profile(
            int fragment_id,
            const std::vector<std::shared_ptr<TRuntimeProfileTree>>& pipeline_profile,
            std::shared_ptr<TRuntimeProfileTree> load_channel_profile);
    // FE-BE 分布式协同与观测的桥梁。
    // 负责实时抓取和汇总当前 BE 节点上所有运行中及已结束 Fragment 的执行状态、错误日志以及 Runtime Profile 指标，打包成 TReportExecStatusParams 汇报给 Coordinator (FE)。FE 正是依靠这个方法返回的数据来感知查询进度、监控性能指标并在前端展示系统 Profiling。
    TReportExecStatusParams get_realtime_exec_status();

    bool enable_profile() const {
        return _query_options.__isset.enable_profile && _query_options.enable_profile;
    }

    timespec get_query_arrival_timestamp() const { return this->_query_arrival_timestamp; }
    QuerySource get_query_source() const { return this->_query_source; }

    TQueryOptions get_query_options() const { return _query_options; }
};

} // namespace doris
