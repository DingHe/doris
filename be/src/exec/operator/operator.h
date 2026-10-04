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

#include <fmt/format.h>
#include <glog/logging.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/be_mock_util.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "core/block/block.h"
#include "exec/exchange/local_exchanger.h"
#include "exec/exchange/vdata_stream_recvr.h"
#include "exec/operator/operator.h"
#include "exec/operator/spill_counters.h"
#include "exec/operator/spill_utils.h"
#include "exec/pipeline/dependency.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/query_context.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_profile_counter_names.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "util/block_budget.h"

namespace doris {
class RowDescriptor;
class RuntimeState;
class TDataSink;
class AsyncResultWriter;
class ScoreRuntime;
class AnnTopNRuntime;
class ParsedPartitionBoundaries;
} // namespace doris

namespace doris {

class OperatorBase;
class OperatorXBase;
class DataSinkOperatorXBase;

using OperatorPtr = std::shared_ptr<OperatorXBase>;
using Operators = std::vector<OperatorPtr>;

using DataSinkOperatorPtr = std::shared_ptr<DataSinkOperatorXBase>;

// This struct is used only for initializing local state.
struct LocalStateInfo {
    RuntimeProfile* parent_profile = nullptr;
    const std::vector<TScanRangeParams>& scan_ranges;
    BasicSharedState* shared_state;
    const std::map<int, std::pair<std::shared_ptr<BasicSharedState>,
                                  std::vector<std::shared_ptr<Dependency>>>>& shared_state_map;
    const int task_idx;
};

// This struct is used only for initializing local sink state.
struct LocalSinkStateInfo {
    const int task_idx;
    RuntimeProfile* parent_profile = nullptr;
    const int sender_id;
    BasicSharedState* shared_state;
    const std::map<int, std::pair<std::shared_ptr<BasicSharedState>,
                                  std::vector<std::shared_ptr<Dependency>>>>& shared_state_map;
    const TDataSink& tsink;
};

// OperatorBase 是 Pipeline 执行引擎中所有物理算子（Operator）的基类。
// OperatorBase 类的作用
// 算子通用抽象接口（Operator Abstraction）：作为 Pipeline 执行引擎中所有处理算子的根基类，声明了算子生命周期管理（init、prepare、terminate、close）、拓扑关系建立（设置/获取子算子 _child）、并行度计算以及算子标识（名字、Node ID）等统一标准接口。
// 数据分布属性与 Shuffle 特性识别（Data Distribution & Shuffle Properties）：定义了一套用于判断数据流特征的接口（如是否依赖分桶分布 is_colocated_operator、是否需要 Shuffle is_shuffled_operator、后续是否紧跟 Shuffle 算子等），为前端生成 Pipeline 任务拓扑图以及 BE 执行时数据路由提供了属性依据。
// 资源管理与阻塞调度（Resource Management & Scheduling）：提供了内存可回收（Spill to disk / Memory Revocation）接口（revocable_mem_size、revoke_memory）、低内存模式控制（set_low_memory_mode）以及 Pipeline 任务是否可阻塞判定（is_blockable），协助 Pipeline 调度器进行线程调度与内存调控。


class OperatorBase {
public:
    // 将 _child 初始化为 nullptr，_is_closed 初始化为 false，其他属性默认初始化。
    explicit OperatorBase() : _child(nullptr), _is_closed(false) {}
    // 允许指定算子串行特性的构造函数。
    explicit OperatorBase(bool is_serial_operator)
            : _child(nullptr), _is_closed(false), _is_serial_operator(is_serial_operator) {}
    virtual ~OperatorBase() = default;
    // 判断当前算子是否为 Sink 算子（即 Pipeline 链条的终点/数据接收端）
    // 默认返回 false；在派生类 DataSinkOperatorX 或 PipelineTaskSink 等中重写返回 true
    virtual bool is_sink() const { return false; }
    // 判断当前算子是否为 Source 算子（即 Pipeline 链条的起点/数据产生端，如 Scan 算子）
    // 默认返回 false；在派生类 OperatorXSource 等中重写返回 true。
    virtual bool is_source() const { return false; }
    // 获取当前算子输出数据的行描述符（RowDescriptor），用于描述输出 Block 中每一列的数据类型、列 ID 等 Schema 元数据。
    [[nodiscard]] virtual const RowDescriptor& row_desc() const;
    // 算子的初始化接口，通常在反序列化 Thrift 结构后执行。
    [[nodiscard]] virtual Status init(const TDataSink& tsink) { return Status::OK(); }

    [[nodiscard]] virtual std::string get_name() const = 0;
    // 算子执行前的准备阶段。
    // 负责分配资源、编译/初始化表达式（VExpr）、创建依赖对象等。
    [[nodiscard]] virtual Status prepare(RuntimeState* state) = 0;
    // 当 Query 发生异常、取消（Cancel）或提前结束时调用，用于强制终止算子当前正在进行的操作。
    [[nodiscard]] virtual Status terminate(RuntimeState* state) = 0;
    // 算子关闭与资源清理接口。
    // 释放算子占用的内存、关闭子算子等。基类中默认实现会设置 _is_closed = true。
    [[nodiscard]] virtual Status close(RuntimeState* state);
    [[nodiscard]] virtual int node_id() const = 0;
    // 计算并返回当前算子的并行执行实例（Instance/Parallel Task）数量。
    [[nodiscard]] virtual int parallelism(RuntimeState* state) const {
        return _is_serial_operator ? 1 : state->query_parallel_instance_num();
    }
    // 设置当前算子的子算子 _child。
    [[nodiscard]] virtual Status set_child(OperatorPtr child) {
        if (_child && child != nullptr) {
            return Status::InternalError("Child is already set in node name={}", get_name());
        }
        _child = child;
        return Status::OK();
    }

    // Operators need to be executed serially. (e.g. finalized agg without key)
    [[nodiscard]] virtual bool is_serial_operator() const { return _is_serial_operator; }

    [[nodiscard]] bool is_closed() const { return _is_closed; }
    // 查询当前算子中可被回收（或 Spill 落盘）的内存大小（单位字节
    // 默认返回 0（表示不可回收/不支持 Spill）；支持内存落盘的算子（如 Agg、Sort、Join）会返回各自 Cache/HashTable 占用的内存。
    virtual size_t revocable_mem_size(RuntimeState* state) const { return 0; }
    // 触发算子执行内存回收操作（如将内存中的数据刷写到磁盘临时文件）
    virtual Status revoke_memory(RuntimeState* state) { return Status::OK(); }
    // 判断当前算子是否是 Hash Join 的 Probe 阶段算子
    virtual bool is_hash_join_probe() const { return false; }

    /**
     * Pipeline task is blockable means it will be blocked in the next run. So we should put the
     * pipeline task into the blocking task scheduler.
     */
    // 判断当前算子在下一次执行时是否可能会被阻塞（例如等待数据异步加载、等待网络 RPC 等）
    virtual bool is_blockable(RuntimeState* state) const = 0;
    // 通知算子进入低内存运行模式（当系统整体内存紧张时调用）。算子可以采取更保守的内存分配策略（如减小 Batch Size、限制 HashTable 扩容等）。
    virtual void set_low_memory_mode(RuntimeState* state) {}

    OperatorPtr child() { return _child; }
    virtual Status reset(RuntimeState* state) {
        return Status::InternalError("Reset is not implemented in operator: {}", get_name());
    }

    /* -------------- Interfaces to determine the input data properties -------------- */
    /**
     * Return True if this operator relies on the bucket distribution (e.g. COLOCATE join, 1-phase AGG).
     * Data input to this kind of operators must have the same distribution with the table buckets.
     * It is also means `required_data_distribution` should be `BUCKET_HASH_SHUFFLE`.
     * @return
     */
    // 判断算子是否依赖 Colocate 分布（如 Colocate Join）。要求输入数据必须完全符合表的分桶分布。
    [[nodiscard]] virtual bool is_colocated_operator() const { return false; }
    /**
     * Return True if this operator relies on the bucket distribution or specific hash data distribution (e.g. SHUFFLED HASH join).
     * Data input to this kind of operators must be HASH distributed according to some rules.
     * All colocated operators are also shuffled operators.
     * It is also means `required_data_distribution` should be `BUCKET_HASH_SHUFFLE` or `HASH_SHUFFLE`.
     * @return
     */
    // 判断算子是否依赖 Hash Shuffle 分布（如常规的分散式 Hash Join）。所有 Colocate 算子默认也是 Shuffle 算子。
    [[nodiscard]] virtual bool is_shuffled_operator() const { return false; }
    /**
     * For multiple children's operators, return true if this is a shuffled operator or this is followed by a shuffled operator (HASH JOIN and SET OPERATION).
     *
     * For single child's operators, return true if this operator is followed by a shuffled operator.
     * For example, in the plan fragment:
     *   `UNION` -> `SHUFFLED HASH JOIN`
     * The `SHUFFLED HASH JOIN` is a shuffled operator so the UNION operator is followed by a shuffled operator.
     */
    // 判断当前算子之后是否紧跟了一个需要 Shuffle 的算子。
    [[nodiscard]] virtual bool followed_by_shuffled_operator() const {
        return _followed_by_shuffled_operator;
    }
    /**
     * Update the operator properties according to the plan node.
     * This is called before `prepare`.
     */
    // 在算子 prepare 之前被调用，根据执行计划节点（Thrift TPlanNode）更新算子的数据分布属性。
    virtual void update_operator(const TPlanNode& tnode, bool followed_by_shuffled_operator,
                                 bool require_bucket_distribution) {
        _followed_by_shuffled_operator = followed_by_shuffled_operator;
        _require_bucket_distribution = require_bucket_distribution;
    }
    /**
     * Return the required data distribution of this operator.
     */
    // 根据算子属性计算并返回当前算子所要求的输入数据分布类型（例如 UNSPECIFIED、HASH_SHUFFLE、BUCKET_HASH_SHUFFLE 等）。
    [[nodiscard]] virtual DataDistribution required_data_distribution(
            RuntimeState* /*state*/) const;

protected:
    [[nodiscard]] bool child_breaks_local_key_distribution(RuntimeState* state) const;
    // OperatorPtr（指向 OperatorBase 的智能指针，通常为 std::shared_ptr<OperatorBase>）
    // 指向当前算子的子算子（Child Operator）。Pipeline 算子通常以树状/链表拓扑组织，上游（Child）算子产生 Block 数据供给下游（当前算子）消费。
    OperatorPtr _child = nullptr;
    // 记录当前算子是否已经被关闭（即是否已成功执行过 close 方法）。用于防止重复关闭或清理资源。
    bool _is_closed;
    // 标记当前算子的后续流程中是否紧跟着一个需要数据 Shuffle 的算子（如 Hash Join、Set Operation 等）。用于推导数据分布要求与本地 Hash Shuffle 策略。
    bool _followed_by_shuffled_operator = false;
    // 标记当前算子是否要求输入数据按表的分桶（Bucket）规则进行 Hash 分布（例如 Colocate Join 或单阶段 Aggregating）
    bool _require_bucket_distribution = false;
    // 标记当前算子是否必须串行（单线程/单实例）执行（例如无 Group By Key 的全局 final 阶段聚合算子）。
    bool _is_serial_operator = false;
};

// PipelineX 执行引擎架构中，PipelineXLocalStateBase 是一个非常核心且至关重要的基类。
// 为了实现并发执行并避免多线程竞争，PipelineX 架构采用了 “全局算子对象（OperatorX） + 线程局部状态（LocalState）” 的设计模式：
// OperatorXBase（算子静态模板）：在节点级别的全局唯一定义，包含了跨并发线程共享的无状态元数据、逻辑配置及表达式（如 Join 条件、投影表达式等）。
// PipelineXLocalStateBase（并发实例局部状态）：对应每个具体的并发执行线程（Pipeline Task 实例）。它承载了该并发线程在运行期独享的私有状态、局部 Buffer、局部 RuntimeProfile 指标监控、表达式上下文（VExprContext 的 Clone 副本）以及运行期依赖（Dependency）。
// 状态隔离（State Isolation）：将算子执行时的状态从 OperatorXBase 中剥离，确保不同并发任务（Pipeline Task）独立的执行数据不发生线程冲突。
// 生命周期管理（Lifecycle Management）：定义了 Pipeline 任务内部算子从初始化（init）、准备（prepare）、开启（open）到关闭（close）以及强制终止（terminate）的标准流程。
// 性能监控（Performance & Profile Management）：内置了三层 Profile 结构（operator_profile, common_profile, custom_profile），精准收集单线程内的耗时、返回行数、Block 字节数大小、内存占用峰值等性能 Counter。
// 计算表达式执行与辅助（Expression Execution & Projection）：管理属于该并发实例的过滤条件（conjuncts）评估、列投影转换（projections）以及物理 Block 数据的剪裁。
// 异步调度依赖管理（Dependency Integration）：对接 PipelineX 的异步调度器，维护算子执行所依赖的同步/异步事件（Dependency）。
class PipelineXLocalStateBase {
public:
    // 构造函数。用指定的 RuntimeState 和对应的父级算子模板 parent 初始化 LocalState，并初始化基础属性（如 BlockBudget 等）。
    PipelineXLocalStateBase(RuntimeState* state, OperatorXBase* parent);
    virtual ~PipelineXLocalStateBase() = default;

    template <class TARGET>
    TARGET& cast() {
        DCHECK(dynamic_cast<TARGET*>(this))
                << " Mismatch type! Current type is " << typeid(*this).name()
                << " and expect type is" << typeid(TARGET).name();
        return reinterpret_cast<TARGET&>(*this);
    }
    template <class TARGET>
    const TARGET& cast() const {
        DCHECK(dynamic_cast<TARGET*>(this))
                << " Mismatch type! Current type is " << typeid(*this).name()
                << " and expect type is" << typeid(TARGET).name();
        return reinterpret_cast<const TARGET&>(*this);
    }

    // Do initialization. This step should be executed only once and in bthread, so we can do some
    // lightweight or non-idempotent operations (e.g. init profile, clone expr ctx from operatorX)
    // 初始化接口。只会被调用一次（通常在 bthread 轻量级初始化阶段）。用于进行非幂等的轻量初始化操作，例如创建和初始化 RuntimeProfile、从 OperatorXBase 中 Clone 表达式上下文 VExprContext 等。
    virtual Status init(RuntimeState* state, LocalStateInfo& info) = 0;
    // Make sure all resources are ready before execution. For example, remote tablets should be
    // loaded to local storage.
    // This is called by execution pthread and different from `Operator::prepare` which is called
    // by bthread.
    // 准备接口。在真正被 Pthread 执行线程调度执行前调用。用于确保执行所需资源准备就绪（例如确保远端存储的数据 Tablet/File 已经被加载到本地等）。
    virtual Status prepare(RuntimeState* state) = 0;
    // Do initialization. This step can be executed multiple times, so we should make sure it is
    // idempotent (e.g. wait for runtime filters).
    // 开启接口。在任务启动时执行，可以被重复执行（必须具备幂等性）。通常用于处理运行期依赖的开启操作，例如等待 Runtime Filter（运行时动态过滤条件）就绪。
    virtual Status open(RuntimeState* state) = 0;
    // 清理关闭接口。算子执行完毕或发生异常时调用。用于回收、关闭该 LocalState 持有的所有动态资源、解绑 Buffer 和 Counter。
    virtual Status close(RuntimeState* state) = 0;
    virtual Status terminate(RuntimeState* state) = 0;

    // If use projection, we should clear `_origin_block`.
    void clear_origin_block();

    void reached_limit(Block* block, bool* eos);
    RuntimeProfile* operator_profile() { return _operator_profile.get(); }
    RuntimeProfile* common_profile() { return _common_profile.get(); }
    RuntimeProfile* custom_profile() { return _custom_profile.get(); }

    RuntimeProfile::Counter* exec_time_counter() { return _exec_timer; }
    RuntimeProfile::Counter* memory_used_counter() { return _memory_used_counter; }
    OperatorXBase* parent() { return _parent; }
    RuntimeState* state() { return _state; }
    [[nodiscard]] const BlockBudget& block_budget() const { return _budget; }
    VExprContextSPtrs& conjuncts() { return _conjuncts; }
    VExprContextSPtrs& projections() { return _projections; }
    [[nodiscard]] int64_t num_rows_returned() const { return _num_rows_returned; }
    void add_num_rows_returned(int64_t delta) { _num_rows_returned += delta; }
    void set_num_rows_returned(int64_t value) { _num_rows_returned = value; }
    void update_output_block_counters(const Block& block) {
        if (auto rows = block.rows()) {
            COUNTER_UPDATE(_rows_returned_counter, rows);
            COUNTER_UPDATE(_blocks_returned_counter, 1);
            auto block_bytes = static_cast<int64_t>(block.bytes());
            COUNTER_UPDATE(_output_block_bytes_counter, block_bytes);
            if (block_bytes > _max_output_block_bytes) {
                _max_output_block_bytes = block_bytes;
                COUNTER_SET(_max_output_block_bytes_counter, block_bytes);
            }
            if (block_bytes < _min_output_block_bytes) {
                _min_output_block_bytes = block_bytes;
                COUNTER_SET(_min_output_block_bytes_counter, block_bytes);
            }
        }
    }

    [[nodiscard]] virtual std::string debug_string(int indentation_level = 0) const = 0;
    [[nodiscard]] virtual bool is_blockable() const;

    virtual std::vector<Dependency*> dependencies() const { return {nullptr}; }

    // override in Scan
    virtual Dependency* finishdependency() { return nullptr; }
    //  override in Scan  MultiCastSink
    virtual std::vector<Dependency*> execution_dependencies() { return {}; }

    Status filter_block(const VExprContextSPtrs& expr_contexts, Block* block);

    int64_t& estimate_memory_usage() { return _estimate_memory_usage; }

    void reset_estimate_memory_usage() { _estimate_memory_usage = 0; }

    bool low_memory_mode() {
#ifdef BE_TEST
        return false;
#else
        return _state->low_memory_mode();
#endif
    }

protected:
    friend class OperatorXBase;
    template <typename LocalStateType>
    friend class ScanOperatorX;
    // 对象的内存池指针。用于统一管理该 LocalState 生命周期内动态创建的 C++ 对象（如表达式上下文、局部 Counter 等），方便在 close 时统一释放。
    ObjectPool* _pool = nullptr;
    int64_t _num_rows_returned {0};
    int64_t _estimate_memory_usage {0};

    /*
    Each operator has its profile like this:
    XXXX_OPERATOR:
        CommonCounters:
            ...
        CustomCounters:
            ...
    */
    // Profile of this operator.
    // Should not modify this profile usually.
    std::unique_ptr<RuntimeProfile> _operator_profile;
    // CommonCounters of this operator.
    // CommonCounters are counters that will be used by all operators.
    std::unique_ptr<RuntimeProfile> _common_profile;
    // CustomCounters of this operator.
    // CustomCounters are counters that will be used by this operator only.
    std::unique_ptr<RuntimeProfile> _custom_profile;

    RuntimeProfile::Counter* _rows_returned_counter = nullptr;
    RuntimeProfile::Counter* _blocks_returned_counter = nullptr;
    RuntimeProfile::Counter* _output_block_bytes_counter = nullptr;
    RuntimeProfile::Counter* _max_output_block_bytes_counter = nullptr;
    RuntimeProfile::Counter* _min_output_block_bytes_counter = nullptr;
    int64_t _max_output_block_bytes = 0;
    int64_t _min_output_block_bytes = std::numeric_limits<int64_t>::max();
    RuntimeProfile::Counter* _wait_for_dependency_timer = nullptr;
    // Account for current memory and peak memory used by this node
    RuntimeProfile::HighWaterMarkCounter* _memory_used_counter = nullptr;
    RuntimeProfile::Counter* _projection_timer = nullptr;
    RuntimeProfile::Counter* _exec_timer = nullptr;
    RuntimeProfile::Counter* _init_timer = nullptr;
    RuntimeProfile::Counter* _open_timer = nullptr;
    RuntimeProfile::Counter* _close_timer = nullptr;
    // 指向该局部状态对应的全局静态算子模板对象（OperatorXBase）。LocalState 需要通过它获取算子的全局元数据。
    OperatorXBase* _parent = nullptr;
    // 指向当前的查询运行期状态对象（RuntimeState）。提供了全局配置、内存 Tracker、执行参数等信息。
    RuntimeState* _state = nullptr;
    // Execution-scoped row/byte budget derived from the session batch settings.
    const BlockBudget _budget;
    VExprContextSPtrs _conjuncts;
    VExprContextSPtrs _projections;
    std::shared_ptr<ScoreRuntime> _score_runtime;
    std::shared_ptr<segment_v2::AnnTopNRuntime> _ann_topn_runtime;
    // Used in common subexpression elimination to compute intermediate results.
    std::vector<VExprContextSPtrs> _intermediate_projections;
    // 标记当前 LocalState 是否已经执行过 close() 操作，防止重复清理或资源二次释放。
    bool _closed = false;
    // 原子的布尔标记，记录当前 LocalState 是否已被异常中断或强制终止（例如查询被 Cancel 时）。
    std::atomic<bool> _terminated = false;
    Block _origin_block;
};
// 在 Apache Doris 的 PipelineX 执行引擎中，PipelineXLocalState 是继承自 PipelineXLocalStateBase 的中间泛型模板抽象类。
// 如果说 PipelineXLocalStateBase 提供了所有算子本地状态的非泛型通用接口（如基础 Profile 统计、内存记录、通用生命周期定义等），
// 那么 PipelineXLocalState<SharedStateArg> 则是为大部分具体算子的 LocalState 实现（如 HashJoinProbeLocalState、AggregateLocalState 等）提供了共享状态（SharedState）类型绑定与依赖管理（Dependency）的标准泛型骨架。
// PipelineXLocalState 类的核心作用
// 共享状态绑定（SharedState Binding）：
// 在 Pipeline 执行模型中，许多跨 Pipeline（例如 HashJoin 的 Build 端与 Probe 端）或跨并发 Instance 的算子需要通过一个“共享数据/状态对象（SharedState）”进行通信与数据传递。PipelineXLocalState 引入了模板参数 SharedStateArg，使得具体的算子 LocalState 可以强类型地持有并访问其专用的 SharedState。
// 缺省占位与类型安全（Default Type Placeholders）：
// 默认模板参数为 FakeSharedState（虚构/占位共享状态）。对于那些不需要与上游/下游共享状态的简单单核算子（如普通的 Filter 或 Project），无需特殊指定模板参数，引擎即可自动匹配，避免编写冗余代码。
// 依赖（Dependency）生命周期挂载：
// 统一维护了指向算子运行期依赖 _dependency 的指针，并实现了基类的 dependencies() 虚函数，向调度器暴露当前任务的异步阻塞/就绪条件。
// 统一生命周期默认实现（Default Lifecycle Implementations）：
// 为 prepare、open、close、terminate 等虚函数提供了通用的基础处理逻辑（如初始化/释放 RuntimeProfile 耗时计时器、从 LocalStateInfo 中自动获取并绑定 SharedState 和 Dependency 等）。
template <typename SharedStateArg = FakeSharedState>
class PipelineXLocalState : public PipelineXLocalStateBase {
public:
    // 将模板参数 SharedStateArg 导出为类内部公开的类型别名 SharedStateType。
    using SharedStateType = SharedStateArg;
    PipelineXLocalState(RuntimeState* state, OperatorXBase* parent)
            : PipelineXLocalStateBase(state, parent) {}
    ~PipelineXLocalState() override = default;

    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status prepare(RuntimeState* state) override { return Status::OK(); }
    // 开启算子执行。
    // 重写了基类的 open 方法。默认实现通常包含开启 _open_timer 计时，以及对需要等待运行期动态条件（如等待 Runtime Filter 就绪）的操作进行开启和状态检查。
    Status open(RuntimeState* state) override;

    virtual std::string name_suffix() const;
    // 清理与关闭。
    // 重写了基类的 close 方法。处理当前 LocalState 的关闭与资源释放逻辑：启动 _close_timer，将 _closed 标记设为 true，解除对 _shared_state 和 _dependency 的引用或递减引用计数，防范资源泄漏。
    Status close(RuntimeState* state) override;
    Status terminate(RuntimeState* state) override;

    [[nodiscard]] std::string debug_string(int indentation_level = 0) const override;

    std::vector<Dependency*> dependencies() const override {
        return _dependency ? std::vector<Dependency*> {_dependency} : std::vector<Dependency*> {};
    }

    virtual bool must_set_shared_state() const {
        return !std::is_same_v<SharedStateArg, FakeSharedState>;
    }

protected:
    // 指向该算子局部状态绑定的核心依赖对象（Dependency）
    // 在 PipelineX 的异步调度模型中，当算子依赖某些上游条件（例如等待 HashJoin 的 HashTable 建立完成，或者等待异步 I/O 填充数据）时，调度器会通过检查 _dependency 的就绪状态（is_ready()）来决定是否唤醒或调度当前任务执行。
    Dependency* _dependency = nullptr;
    // 指向类型为 SharedStateArg 的共享状态对象的指针。
    SharedStateArg* _shared_state = nullptr;
};

template <typename SharedStateArg>
class PipelineXSpillLocalState : public PipelineXLocalState<SharedStateArg> {
public:
    using Base = PipelineXLocalState<SharedStateArg>;
    PipelineXSpillLocalState(RuntimeState* state, OperatorXBase* parent)
            : PipelineXLocalState<SharedStateArg>(state, parent) {}
    ~PipelineXSpillLocalState() override = default;

    Status init(RuntimeState* state, LocalStateInfo& info) override {
        RETURN_IF_ERROR(PipelineXLocalState<SharedStateArg>::init(state, info));

        init_spill_read_counters();

        return Status::OK();
    }

    void init_spill_write_counters() {
        _write_counters.init(Base::custom_profile());

        // Source-only extra write counters
        _spill_write_file_total_size = ADD_COUNTER_WITH_LEVEL(
                Base::custom_profile(), profile::SPILL_WRITE_FILE_BYTES, TUnit::BYTES, 1);
        _spill_file_total_count = ADD_COUNTER_WITH_LEVEL(
                Base::custom_profile(), profile::SPILL_WRITE_FILE_TOTAL_COUNT, TUnit::UNIT, 1);
    }

    void init_spill_read_counters() {
        _spill_total_timer =
                ADD_TIMER_WITH_LEVEL(Base::custom_profile(), profile::SPILL_TOTAL_TIME, 1);

        _read_counters.init(Base::custom_profile());

        _spill_file_current_size = ADD_COUNTER_WITH_LEVEL(
                Base::custom_profile(), profile::SPILL_WRITE_FILE_CURRENT_BYTES, TUnit::BYTES, 1);
    }

    // Total time of spill, including spill task scheduling time,
    // serialize block time, write disk file time,
    // and read disk file time, deserialize block time etc.
    RuntimeProfile::Counter* _spill_total_timer = nullptr;

    // Shared spill write counters
    SpillWriteCounters _write_counters;
    // Backward-compatible aliases for commonly accessed write counters
    RuntimeProfile::Counter*& _spill_write_file_timer = _write_counters.spill_write_file_timer;
    RuntimeProfile::Counter*& _spill_write_serialize_block_timer =
            _write_counters.spill_write_serialize_block_timer;
    RuntimeProfile::Counter*& _spill_write_block_count = _write_counters.spill_write_block_count;
    RuntimeProfile::Counter*& _spill_write_block_data_size =
            _write_counters.spill_write_block_data_size;
    RuntimeProfile::Counter*& _spill_write_rows_count = _write_counters.spill_write_rows_count;

    // Source-only write counters (not in SpillWriteCounters)
    // Total bytes of spill data written to disk file(after serialized)
    RuntimeProfile::Counter* _spill_write_file_total_size = nullptr;
    RuntimeProfile::Counter* _spill_file_total_count = nullptr;
    // Current spilled file size
    RuntimeProfile::Counter* _spill_file_current_size = nullptr;

    // Shared spill read counters
    SpillReadCounters _read_counters;
    // Backward-compatible aliases for commonly accessed read counters
    RuntimeProfile::Counter*& _spill_read_file_time = _read_counters.spill_read_file_time;
    RuntimeProfile::Counter*& _spill_read_deserialize_block_timer =
            _read_counters.spill_read_deserialize_block_timer;
    RuntimeProfile::Counter*& _spill_read_block_count = _read_counters.spill_read_block_count;
    RuntimeProfile::Counter*& _spill_read_block_data_size =
            _read_counters.spill_read_block_data_size;
    RuntimeProfile::Counter*& _spill_read_file_size = _read_counters.spill_read_file_size;
    RuntimeProfile::Counter*& _spill_read_rows_count = _read_counters.spill_read_rows_count;
    RuntimeProfile::Counter*& _spill_read_file_count = _read_counters.spill_read_file_count;
};

class DataSinkOperatorXBase;

class PipelineXSinkLocalStateBase {
public:
    PipelineXSinkLocalStateBase(DataSinkOperatorXBase* parent_, RuntimeState* state_);
    virtual ~PipelineXSinkLocalStateBase() = default;

    // Do initialization. This step should be executed only once and in bthread, so we can do some
    // lightweight or non-idempotent operations (e.g. init profile, clone expr ctx from operatorX)
    virtual Status init(RuntimeState* state, LocalSinkStateInfo& info) = 0;

    virtual Status prepare(RuntimeState* state) = 0;
    // Do initialization. This step can be executed multiple times, so we should make sure it is
    // idempotent (e.g. wait for runtime filters).
    virtual Status open(RuntimeState* state) = 0;
    virtual Status terminate(RuntimeState* state) = 0;
    virtual Status close(RuntimeState* state, Status exec_status) = 0;
    [[nodiscard]] virtual bool is_finished() const { return false; }
    [[nodiscard]] virtual bool is_blockable() const { return false; }

    [[nodiscard]] virtual std::string debug_string(int indentation_level) const = 0;

    template <class TARGET>
    TARGET& cast() {
        DCHECK(dynamic_cast<TARGET*>(this))
                << " Mismatch type! Current type is " << typeid(*this).name()
                << " and expect type is" << typeid(TARGET).name();
        return reinterpret_cast<TARGET&>(*this);
    }
    template <class TARGET>
    const TARGET& cast() const {
        DCHECK(dynamic_cast<const TARGET*>(this))
                << " Mismatch type! Current type is " << typeid(*this).name()
                << " and expect type is" << typeid(TARGET).name();
        return reinterpret_cast<const TARGET&>(*this);
    }

    DataSinkOperatorXBase* parent() { return _parent; }
    RuntimeState* state() { return _state; }
    RuntimeProfile* operator_profile() { return _operator_profile; }
    RuntimeProfile* common_profile() { return _common_profile; }
    RuntimeProfile* custom_profile() { return _custom_profile; }

    [[nodiscard]] RuntimeProfile* faker_runtime_profile() const {
        return _faker_runtime_profile.get();
    }

    RuntimeProfile::Counter* rows_input_counter() { return _rows_input_counter; }
    RuntimeProfile::Counter* exec_time_counter() { return _exec_timer; }
    RuntimeProfile::Counter* memory_used_counter() { return _memory_used_counter; }

    virtual std::vector<Dependency*> dependencies() const { return {nullptr}; }

    // override in exchange sink , AsyncWriterSink
    virtual Dependency* finishdependency() { return nullptr; }

    bool low_memory_mode() { return _state->low_memory_mode(); }

protected:
    DataSinkOperatorXBase* _parent = nullptr;
    RuntimeState* _state = nullptr;
    RuntimeProfile* _operator_profile = nullptr;
    RuntimeProfile* _common_profile = nullptr;
    RuntimeProfile* _custom_profile = nullptr;
    // Set to true after close() has been called. subclasses should check and set this in
    // close().
    bool _closed = false;
    bool _terminated = false;
    //NOTICE: now add a faker profile, because sometimes the profile record is useless
    //so we want remove some counters and timers, eg: in join node, if it's broadcast_join
    //and shared hash table, some counter/timer about build hash table is useless,
    //so we could add those counter/timer in faker profile, and those will not display in web profile.
    std::unique_ptr<RuntimeProfile> _faker_runtime_profile =
            std::make_unique<RuntimeProfile>(profile::FAKER_PROFILE);

    RuntimeProfile::Counter* _rows_input_counter = nullptr;
    RuntimeProfile::Counter* _init_timer = nullptr;
    RuntimeProfile::Counter* _open_timer = nullptr;
    RuntimeProfile::Counter* _close_timer = nullptr;
    RuntimeProfile::Counter* _wait_for_dependency_timer = nullptr;
    RuntimeProfile::Counter* _wait_for_finish_dependency_timer = nullptr;
    RuntimeProfile::Counter* _exec_timer = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _memory_used_counter = nullptr;
};

template <typename SharedStateArg = FakeSharedState>
class PipelineXSinkLocalState : public PipelineXSinkLocalStateBase {
public:
    using SharedStateType = SharedStateArg;
    PipelineXSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : PipelineXSinkLocalStateBase(parent, state) {}
    ~PipelineXSinkLocalState() override = default;

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;

    Status prepare(RuntimeState* state) override { return Status::OK(); }
    Status open(RuntimeState* state) override { return Status::OK(); }

    Status terminate(RuntimeState* state) override;
    Status close(RuntimeState* state, Status exec_status) override;

    [[nodiscard]] std::string debug_string(int indentation_level) const override;

    virtual std::string name_suffix();

    std::vector<Dependency*> dependencies() const override {
        return _dependency ? std::vector<Dependency*> {_dependency} : std::vector<Dependency*> {};
    }

    virtual bool must_set_shared_state() const {
        return !std::is_same_v<SharedStateArg, FakeSharedState>;
    }

protected:
    Dependency* _dependency = nullptr;
    SharedStateType* _shared_state = nullptr;
};

class DataSinkOperatorXBase : public OperatorBase {
public:
    DataSinkOperatorXBase(const int operator_id, const int node_id, const int dest_id)
            : _operator_id(operator_id), _node_id(node_id), _dests_id({dest_id}) {}
    DataSinkOperatorXBase(const int operator_id, const TPlanNode& tnode, const int dest_id)
            : OperatorBase(tnode.__isset.is_serial_operator && tnode.is_serial_operator),
              _operator_id(operator_id),
              _node_id(tnode.node_id),
              _dests_id({dest_id}) {}

    DataSinkOperatorXBase(const int operator_id, const int node_id, std::vector<int>& dests)
            : _operator_id(operator_id), _node_id(node_id), _dests_id(dests) {}

#ifdef BE_TEST
    DataSinkOperatorXBase() : _operator_id(-1), _node_id(0), _dests_id({-1}) {};
#endif

    ~DataSinkOperatorXBase() override = default;

    // For agg/sort/join sink.
    virtual Status init(const TPlanNode& tnode, RuntimeState* state);

    virtual bool reset_to_rerun(RuntimeState* state, OperatorXBase* root) const { return false; }

    Status init(const TDataSink& tsink) override;
    [[nodiscard]] virtual Status init(RuntimeState* state, TLocalPartitionType::type type,
                                      const int num_buckets,
                                      const std::map<int, int>& shuffle_idx_to_instance_idx) {
        return Status::InternalError("init() is only implemented in local exchange!");
    }

    Status prepare(RuntimeState* state) override { return Status::OK(); }
    Status terminate(RuntimeState* state) override;
    [[nodiscard]] bool is_finished(RuntimeState* state) const {
        auto result = state->get_sink_local_state_result();
        if (!result) {
            return result.error();
        }
        return result.value()->is_finished();
    }

    [[nodiscard]] Status sink(RuntimeState* state, Block* block, bool eos) {
        RETURN_IF_ERROR(block->check_column_and_type_not_null());
        RETURN_IF_ERROR(block->check_no_column_string64());
        RETURN_IF_ERROR(block->check_type_and_column());
        return sink_impl(state, block, eos);
    }

    [[nodiscard]] virtual Status sink_impl(RuntimeState* state, Block* block, bool eos) = 0;

    [[nodiscard]] virtual Status setup_local_state(RuntimeState* state,
                                                   LocalSinkStateInfo& info) = 0;

    // Returns the memory this sink operator expects to allocate in the next
    // execution round (sink only — pipeline task sums all operators + sink).
    [[nodiscard]] virtual size_t get_reserve_mem_size(RuntimeState* state, bool eos) {
        return state->minimum_operator_memory_required_bytes();
    }
    [[nodiscard]] virtual size_t get_reserve_mem_size(RuntimeState* state, bool eos,
                                                      const Block* block) {
        return get_reserve_mem_size(state, eos);
    }
    bool is_blockable(RuntimeState* state) const override {
        return state->get_sink_local_state()->is_blockable();
    }

    [[nodiscard]] bool is_spillable() const { return _spillable; }

    template <class TARGET>
    TARGET& cast() {
        DCHECK(dynamic_cast<TARGET*>(this))
                << " Mismatch type! Current type is " << typeid(*this).name()
                << " and expect type is" << typeid(TARGET).name();
        return reinterpret_cast<TARGET&>(*this);
    }
    template <class TARGET>
    const TARGET& cast() const {
        DCHECK(dynamic_cast<const TARGET*>(this))
                << " Mismatch type! Current type is " << typeid(*this).name()
                << " and expect type is" << typeid(TARGET).name();
        return reinterpret_cast<const TARGET&>(*this);
    }

    [[nodiscard]] virtual std::shared_ptr<BasicSharedState> create_shared_state() const = 0;

    Status close(RuntimeState* state) override {
        return Status::InternalError("Should not reach here!");
    }

    [[nodiscard]] virtual std::string debug_string(int indentation_level) const;

    [[nodiscard]] virtual std::string debug_string(RuntimeState* state,
                                                   int indentation_level) const;

    [[nodiscard]] bool is_sink() const override { return true; }

    static Status close(RuntimeState* state, Status exec_status) {
        auto result = state->get_sink_local_state_result();
        if (!result) {
            return result.error();
        }
        return result.value()->close(state, exec_status);
    }

    [[nodiscard]] int operator_id() const { return _operator_id; }

    [[nodiscard]] const std::vector<int>& dests_id() const { return _dests_id; }

    [[nodiscard]] int nereids_id() const { return _nereids_id; }

    [[nodiscard]] int node_id() const override { return _node_id; }

    [[nodiscard]] std::string get_name() const override { return _name; }

    virtual bool should_dry_run(RuntimeState* state) { return false; }

    [[nodiscard]] virtual bool count_down_destination() { return true; }

protected:
    template <typename Writer, typename Parent>
        requires(std::is_base_of_v<AsyncResultWriter, Writer>)
    friend class AsyncWriterSink;
    // _operator_id : the current Operator's ID, which is not visible to the user.
    // _node_id : the plan node ID corresponding to the Operator, which is visible on the profile.
    // _dests_id : the target _operator_id of the sink, for example, in the case of a multi-sink, there are multiple targets.
    const int _operator_id;
    const int _node_id;
    int _nereids_id = -1;
    bool _spillable = false;
    std::vector<int> _dests_id;
    std::string _name;
};

template <typename LocalStateType>
class DataSinkOperatorX : public DataSinkOperatorXBase {
public:
    DataSinkOperatorX(const int id, const int node_id, const int dest_id)
            : DataSinkOperatorXBase(id, node_id, dest_id) {}
    DataSinkOperatorX(const int id, const TPlanNode& tnode, const int dest_id)
            : DataSinkOperatorXBase(id, tnode, dest_id) {}

    DataSinkOperatorX(const int id, const int node_id, std::vector<int> dest_ids)
            : DataSinkOperatorXBase(id, node_id, dest_ids) {}
#ifdef BE_TEST
    DataSinkOperatorX() = default;
#endif
    ~DataSinkOperatorX() override = default;

    Status setup_local_state(RuntimeState* state, LocalSinkStateInfo& info) override;
    std::shared_ptr<BasicSharedState> create_shared_state() const override;

    using LocalState = LocalStateType;
    [[nodiscard]] LocalState& get_local_state(RuntimeState* state) const {
        return state->get_sink_local_state()->template cast<LocalState>();
    }
};

template <typename SharedStateArg>
class PipelineXSpillSinkLocalState : public PipelineXSinkLocalState<SharedStateArg> {
public:
    using Base = PipelineXSinkLocalState<SharedStateArg>;
    PipelineXSpillSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : Base(parent, state) {}
    ~PipelineXSpillSinkLocalState() override = default;

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override {
        RETURN_IF_ERROR(Base::init(state, info));
        init_spill_counters();
        return Status::OK();
    }

    void init_spill_counters() {
        _spill_total_timer =
                ADD_TIMER_WITH_LEVEL(Base::custom_profile(), profile::SPILL_TOTAL_TIME, 1);

        _write_counters.init(Base::custom_profile());

        // SpillFileWriter looks up these counters via get_counter() in its
        // constructor. They must be registered on the CustomCounters profile
        // before any SpillFileWriter is created, otherwise the lookups return
        // nullptr and COUNTER_UPDATE will SEGV.
        _spill_write_file_total_size = ADD_COUNTER_WITH_LEVEL(
                Base::custom_profile(), profile::SPILL_WRITE_FILE_BYTES, TUnit::BYTES, 1);
        _spill_file_total_count = ADD_COUNTER_WITH_LEVEL(
                Base::custom_profile(), profile::SPILL_WRITE_FILE_TOTAL_COUNT, TUnit::UNIT, 1);

        _spill_max_rows_of_partition = ADD_COUNTER_WITH_LEVEL(
                Base::custom_profile(), profile::SPILL_MAX_ROWS_OF_PARTITION, TUnit::UNIT, 1);
        _spill_min_rows_of_partition = ADD_COUNTER_WITH_LEVEL(
                Base::custom_profile(), profile::SPILL_MIN_ROWS_OF_PARTITION, TUnit::UNIT, 1);
    }

    std::vector<Dependency*> dependencies() const override {
        auto dependencies = Base::dependencies();
        return dependencies;
    }

    void update_max_min_rows_counter() {
        int64_t max_rows = 0;
        int64_t min_rows = std::numeric_limits<int64_t>::max();

        for (auto rows : _rows_in_partitions) {
            if (rows > max_rows) {
                max_rows = rows;
            }
            if (rows < min_rows) {
                min_rows = rows;
            }
        }

        COUNTER_SET(_spill_max_rows_of_partition, max_rows);
        COUNTER_SET(_spill_min_rows_of_partition, min_rows);
    }

    std::vector<int64_t> _rows_in_partitions;

    // Total time of spill, including spill task scheduling time,
    // serialize block time, write disk file time,
    // and read disk file time, deserialize block time etc.
    RuntimeProfile::Counter* _spill_total_timer = nullptr;

    // Shared spill write counters
    SpillWriteCounters _write_counters;
    // Backward-compatible aliases for commonly accessed write counters
    RuntimeProfile::Counter*& _spill_write_file_timer = _write_counters.spill_write_file_timer;
    RuntimeProfile::Counter*& _spill_write_serialize_block_timer =
            _write_counters.spill_write_serialize_block_timer;
    RuntimeProfile::Counter*& _spill_write_block_count = _write_counters.spill_write_block_count;
    RuntimeProfile::Counter*& _spill_write_block_data_size =
            _write_counters.spill_write_block_data_size;
    RuntimeProfile::Counter*& _spill_write_rows_count = _write_counters.spill_write_rows_count;

    // Sink-only counters
    // Total bytes written to spill files (required by SpillFileWriter)
    RuntimeProfile::Counter* _spill_write_file_total_size = nullptr;
    // Total number of spill files created (required by SpillFileWriter)
    RuntimeProfile::Counter* _spill_file_total_count = nullptr;
    RuntimeProfile::Counter* _spill_max_rows_of_partition = nullptr;
    RuntimeProfile::Counter* _spill_min_rows_of_partition = nullptr;
};

// OperatorXBase 是非模板化/无状态算子（Pipeline Operator）的核心基类。它继承自 OperatorBase，为所有 Pipeline 执行算子（如 HashJoinOperatorX、AggregateOperatorX、ScanOperatorX 等）提供了基础结构和默认实现。
// 在 Doris 的 Pipeline 执行引擎设计中，实现了执行逻辑与运行时状态的解耦（即 OperatorX 与 LocalState 分离）：
// 线程安全与共享元数据（Execution Logic & Metadata）：OperatorXBase 包含了算子在编译期/初始化时确定的元数据（如 PlanNode ID、谓词表达式 conjuncts、投影表达式 projections、输出/中间 RowDescriptor 等）。这些元数据在同一个 Pipeline Task 的多个并发实例（Parallel Instances）之间是共享且只读的。
// 实例状态隔离（LocalState Instantiation）：运行时每个算子实例拥有独立的 PipelineXLocalState（维护如线程局部 Buffer、计数器、动态 Runtime Filter 等状态）。OperatorXBase 负责定义初始化、预处理、数据处理（get_block）以及创建 LocalState 的规范接口。
// 表达式与投影处理：统一封装了中间表达式计算（公共子表达式消除 CSE）和最终输出列投影（Projection）的转换逻辑。
// 内存预留与阻塞控制：提供内存预留（Memory Reservation）和异步阻塞（Blockable check）机制的默认抽象。
class OperatorXBase : public OperatorBase {
public:
    // 从 Thrift 节点 tnode 构建算子的主构造函数。
    OperatorXBase(ObjectPool* pool, const TPlanNode& tnode, const int operator_id,
                  const DescriptorTbl& descs)
            : OperatorBase(tnode.__isset.is_serial_operator && tnode.is_serial_operator),
              _operator_id(operator_id),
              _node_id(tnode.node_id),
              _type(tnode.node_type),
              _pool(pool),
              _row_descriptor(descs, tnode.row_tuples),
              _resource_profile(tnode.resource_profile),
              _limit(tnode.limit) {
        if (tnode.__isset.output_tuple_id) {
            _output_row_descriptor =
                    std::make_unique<RowDescriptor>(descs, std::vector {tnode.output_tuple_id});
        }
        if (!tnode.intermediate_output_tuple_id_list.empty()) {
            // common subexpression elimination
            _intermediate_output_row_descriptor.reserve(
                    tnode.intermediate_output_tuple_id_list.size());
            for (auto output_tuple_id : tnode.intermediate_output_tuple_id_list) {
                _intermediate_output_row_descriptor.push_back(
                        RowDescriptor(descs, std::vector {output_tuple_id}));
            }
        }
    }

    OperatorXBase(ObjectPool* pool, int node_id, int operator_id)
            : OperatorBase(),
              _operator_id(operator_id),
              _node_id(node_id),
              _pool(pool),
              _limit(-1) {}

#ifdef BE_TEST
    OperatorXBase() : _operator_id(-1), _node_id(0), _limit(-1) {};
#endif
    // 基于 Thrift 计划节点初始化算子（如解析表达式、初始化谓词等）。子类通常会重写此方法以解析特定的 Thrift 字段。
    virtual Status init(const TPlanNode& tnode, RuntimeState* state);
    // 数据 Sink 初始化重载。在 OperatorXBase（作为 Source/Transform 算子）中直接抛出 Fatal 异常，说明算子不能作为 Sink 节点使用。
    Status init(const TDataSink& tsink) override {
        throw Exception(Status::FatalError("should not reach here!"));
    }
    // LocalPartition 初始化重载。默认抛出 Fatal 异常，仅专门的 Partition 算子会重写它。
    virtual Status init(TLocalPartitionType::type type) {
        throw Exception(Status::FatalError("should not reach here!"));
    }
    [[noreturn]] virtual const std::vector<TRuntimeFilterDesc>& runtime_filter_descs() {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, _op_name);
    }

    // Per-fragment shared partition-boundary parse result, used for
    // runtime-filter partition pruning. Returns nullptr for operators that
    // don't support this feature (default). Scan operators override to expose
    // their parsed boundaries; the per-instance pruning state lives on the
    // ScanLocalState. This sits on the generic OperatorXBase so non-templated
    // ScanLocalStateBase methods can fetch it without down-casting `_parent`
    // to a specific scan type.
    virtual const ParsedPartitionBoundaries* parsed_partition_boundaries() const { return nullptr; }
    [[nodiscard]] std::string get_name() const override { return _op_name; }
    // 判断算子当前是否处于阻塞状态或包含阻塞性算子。会结合 Sink LocalState 的状态以及自身的 _blockable 标志综合判断。
    [[nodiscard]] virtual bool need_more_input_data(RuntimeState* state) const { return true; }
    bool is_blockable(RuntimeState* state) const override {
        return state->get_sink_local_state()->is_blockable() || _blockable;
    }
    // 算子准备阶段。在执行阶段前被调用，用于准备相关资源或初始化表达式。
    Status prepare(RuntimeState* state) override;
    // 终止阶段。当 Query 发生异常、被 Cancel 或提前结束时调用，用于收尾清理。
    Status terminate(RuntimeState* state) override;
    // 上层调用的统一数据获取入口
    // 内部调用纯虚函数 get_block_impl 获取原始数据块，随后对获取到的 Block 强制进行合规性校验（校验列与类型非空、校验无 64 位字符串溢出列、校验数据列与 DataType 一致性）。
    [[nodiscard]] Status get_block(RuntimeState* state, Block* block, bool* eos) {
        RETURN_IF_ERROR(get_block_impl(state, block, eos));
        RETURN_IF_ERROR(block->check_column_and_type_not_null());
        RETURN_IF_ERROR(block->check_no_column_string64());
        RETURN_IF_ERROR(block->check_type_and_column());
        return Status::OK();
    }
    // 算子提取/生成数据的核心核心逻辑，由具体的子类算子（如 HashJoinBuildOperatorX::get_block_impl）实现。
    [[nodiscard]] virtual Status get_block_impl(RuntimeState* state, Block* block, bool* eos) = 0;
    // 算子关闭阶段。释放算子引用的静态资源（如上下文对象销毁）。
    Status close(RuntimeState* state) override;
    // 获取算子默认的中间行描述符，默认返回 _row_descriptor。
    [[nodiscard]] virtual const RowDescriptor& intermediate_row_desc() const {
        return _row_descriptor;
    }
    // 根据索引获取中间行描述符。idx == 0 时返回 _row_descriptor，idx > 0 时返回 _intermediate_output_row_descriptor[idx - 1]。
    [[nodiscard]] const RowDescriptor& intermediate_row_desc(int idx) {
        if (idx == 0) {
            return intermediate_row_desc();
        }
        DCHECK((idx - 1) < _intermediate_output_row_descriptor.size());
        return _intermediate_output_row_descriptor[idx - 1];
    }
    // 获取计算投影之前的最后一个中间行描述符。如果 _intermediate_output_row_descriptor 为空，则返回基础 intermediate_row_desc()，否则返回列表末尾元素。
    [[nodiscard]] const RowDescriptor& projections_row_desc() const {
        if (_intermediate_output_row_descriptor.empty()) {
            return intermediate_row_desc();
        } else {
            return _intermediate_output_row_descriptor.back();
        }
    }

    // Returns the memory this single operator expects to allocate in the next
    // execution round.  Each operator reports only its OWN requirement — the
    // pipeline task is responsible for summing all operators + sink.
    // After the value is consumed the caller should invoke
    // reset_reserve_mem_size() so the next round starts from zero.
    // If this method is not overridden by a subclass, its default value is the
    // minimum operator memory (typically 1 MB).
    // 获取该算子在下一轮执行计算中预估需要申请/保留的内存大小。
    // 返回系统配置的最小算子内存要求（minimum_operator_memory_required_bytes()，通常为 1MB）。需要大内存的算子（如 Join/Agg/Sort）会重写此方法。
    [[nodiscard]] virtual size_t get_reserve_mem_size(RuntimeState* state) {
        return state->minimum_operator_memory_required_bytes();
    }

    virtual std::string debug_string(int indentation_level = 0) const;

    virtual std::string debug_string(RuntimeState* state, int indentation_level = 0) const;
    // 为指定的执行线程/实例创建并设置属于该线程的 PipelineXLocalState。必须由具体的算子子类实现。
    virtual Status setup_local_state(RuntimeState* state, LocalStateInfo& info) = 0;

    template <class TARGET>
    TARGET& cast() {
        DCHECK(dynamic_cast<TARGET*>(this))
                << " Mismatch type! Current type is " << typeid(*this).name()
                << " and expect type is" << typeid(TARGET).name();
        return reinterpret_cast<TARGET&>(*this);
    }
    template <class TARGET>
    const TARGET& cast() const {
        DCHECK(dynamic_cast<const TARGET*>(this))
                << " Mismatch type! Current type is " << typeid(*this).name()
                << " and expect type is" << typeid(TARGET).name();
        return reinterpret_cast<const TARGET&>(*this);
    }

    [[nodiscard]] OperatorPtr get_child() { return _child; }

    [[nodiscard]] VExprContextSPtrs& conjuncts() { return _conjuncts; }
    [[nodiscard]] VExprContextSPtrs& projections() { return _projections; }
    [[nodiscard]] virtual RowDescriptor& row_descriptor() { return _row_descriptor; }

    [[nodiscard]] int operator_id() const { return _operator_id; }
    [[nodiscard]] int node_id() const override { return _node_id; }
    [[nodiscard]] int nereids_id() const { return _nereids_id; }

    [[nodiscard]] int64_t limit() const { return _limit; }

    [[nodiscard]] const RowDescriptor& row_desc() const override {
        return _output_row_descriptor ? *_output_row_descriptor : _row_descriptor;
    }

    [[nodiscard]] const RowDescriptor* output_row_descriptor() {
        return _output_row_descriptor.get();
    }

    bool has_output_row_desc() const { return _output_row_descriptor != nullptr; }

    // 先调用 get_block 获取原始数据，随后自动执行投影操作（Projection），将数据转换为最终需要的输出 Block 格式。
    [[nodiscard]] virtual Status get_block_after_projects(RuntimeState* state, Block* block,
                                                          bool* eos);

    /// Only use in vectorized exec engine try to do projections to trans _row_desc -> _output_row_desc
    // 具体执行 Projection（列转换/表达式计算）的辅助函数。
    // 把 origin_block 根据 _projections 中的表达式计算，填充输出到 output_block 中（实现行描述符 _row_desc 到 _output_row_desc 的映射）。
    Status do_projections(RuntimeState* state, Block* origin_block, Block* output_block) const;
    // 设置和获取当前算子执行的并行任务数（Parallel Instances count）。
    void set_parallel_tasks(int parallel_tasks) { _parallel_tasks = parallel_tasks; }
    int parallel_tasks() const { return _parallel_tasks; }

    // To keep compatibility with older FE
    // 将当前算子强行标记为串行算子（_is_serial_operator = true），用于保持与旧版本 FE 逻辑的兼容性。
    void set_serial_operator() { _is_serial_operator = true; }

    // Resets this operator's estimated memory usage to zero so that the next
    // call to get_reserve_mem_size() starts fresh.  The pipeline task calls
    // this after consuming the reserve size for all operators in a round.
    // 重置算子预留内存大小为 0。Pipeline 调度器在统计并分配完一轮内存后会调用此接口。
    virtual void reset_reserve_mem_size(RuntimeState* state) {}

protected:
    template <typename Dependency>
    friend class PipelineXLocalState;
    friend class PipelineXLocalStateBase;
    friend class Scanner;
    // 算子在整个 Pipeline / Query 中的唯一标志 ID
    const int _operator_id;
    // 对应 Frontend（FE）生成的逻辑执行计划节点 ID（TPlanNode.node_id），在一个 Plan Tree 内唯一。
    const int _node_id; // unique w/in single plan tree
    // 全新优化器 Nereids 对应的表达式或算子节点 ID，用于追踪和匹配新的 Planner 节点。
    int _nereids_id = -1;
    // 当前算子的逻辑类型（例如 AGGREGATION_NODE、HASH_JOIN_NODE 等）。
    TPlanNodeType::type _type;
    // 对象池指针，用于管理生命周期与 Query 绑定的一些动态分配对象（如 Expr 内存）。
    ObjectPool* _pool = nullptr;

private:
    // The expr of operator set to private permissions, as cannot be executed concurrently,
    // should use local state's expr.
    // 算子上绑定的过滤条件/谓词表达式上下文列表（Note: 算子对象上的上下文仅作为模板，实际并发执行时会 clone 到 LocalState 中执行）。
    VExprContextSPtrs _conjuncts;
    // 算子最终输出的投影表达式上下文列表。
    VExprContextSPtrs _projections;
    // Used in common subexpression elimination to compute intermediate results.
    // 中间投影表达式列表，对应公共子表达式消除（CSE）计算的不同阶段。
    std::vector<VExprContextSPtrs> _intermediate_projections;

protected:
    // 当前算子处理的基础行描述符（Row Descriptor），包含了算子输入或内部计算时包含的 Tuple 信息。
    RowDescriptor _row_descriptor;
    // 算子经过 Projection（列投影）过滤后，最终向上层算子输出的行描述符。如果无需 Projection 则为 nullptr。
    std::unique_ptr<RowDescriptor> _output_row_descriptor = nullptr;
    // 用于公共子表达式消除（Common Subexpression Elimination, CSE）或多阶段投影时的中间结果行描述符列表。
    std::vector<RowDescriptor> _intermediate_output_row_descriptor;

    /// Resource information sent from the frontend.
    // FE 下发的算子资源配置 profile（如预估内存、CPU 资源等）。
    const TBackendResourceProfile _resource_profile;
    // 当前算子限定返回的最大行数（-1 表示无 limit 限制）。
    int64_t _limit; // -1: no limit
    // 用于内部调试 / Debug Point 注入机制的计数器。
    uint32_t _debug_point_count = 0;
    // 算子处理单行数据的估算平均字节大小（原子变量，支持并发更新）。
    std::atomic_uint32_t _bytes_per_row = 0;
    // 算子的名称字符串（如 "HASH_JOIN_OPERATOR"），用于 Profile 显示和日志输出。
    std::string _op_name;
    // 该算子在当前 Execution Task 中的并发任务数量/并行度。
    int _parallel_tasks = 0;

    // _blockable is true if the operator contains expressions that may block execution
    // 标记该算子是否包含可能引发异步阻塞的复杂表达式/计算。
    bool _blockable = false;
};

// OperatorX 是基于 C++ 模板编程（CRTP / Policy Pattern）实现的通用 Pipeline 算子基类，继承自前面介绍的非模板基类 OperatorXBase。
// 在 Doris Pipeline 向量化执行引擎的设计中，OperatorXBase 提供了非模板的元数据和统一的基类虚接口，而 OperatorX<LocalStateType> 则通过模板参数绑定了该算子对应的运行时状态类（LocalState）。
// 其核心作用如下：
// 强类型绑定与解耦：将具体的算子类（如 AggOperatorX）与其专属的运行时状态类型（如 AggLocalState）在编译期强类型绑定，无需在运行时频繁进行类型转换。
// 快速获取 LocalState：提供便捷且高效的类型安全接口 get_local_state(state)，方便算子在执行时直接访问当前线程/并发实例绑定的局部状态（包含计数器、内存追踪、表达式执行上下文等）。
// 线程安全的 LocalState 创建与初始化：实现基类 OperatorXBase 留下的纯虚函数 setup_local_state，为每一个 Pipeline 执行 Task 动态实例化并挂载对应类型的 LocalStateType。
// 动态内存预留与状态重置的代理：重写基类的 get_reserve_mem_size 和 reset_reserve_mem_size 方法，将内存估算和重置的具体职责委托给 LocalState（因为实际消耗内存的数据结构如 Hash 表、Buffer 等都存储在 LocalState 中）。

template <typename LocalStateType>
class OperatorX : public OperatorXBase {
public:
    OperatorX(ObjectPool* pool, const TPlanNode& tnode, const int operator_id,
              const DescriptorTbl& descs)
            : OperatorXBase(pool, tnode, operator_id, descs) {}
    OperatorX(ObjectPool* pool, int node_id, int operator_id)
            : OperatorXBase(pool, node_id, operator_id) {};

#ifdef BE_TEST
    OperatorX() = default;
#endif

    ~OperatorX() override = default;

    Status setup_local_state(RuntimeState* state, LocalStateInfo& info) override;
    // 定义该算子对应的 LocalState 类型别名，方便子类和外部使用（例如通过 typename OperatorX<LocalState>::LocalState 直接引用具体的 LocalState 类型）。
    using LocalState = LocalStateType;
    [[nodiscard]] LocalState& get_local_state(RuntimeState* state) const {
        return state->get_local_state(operator_id())->template cast<LocalState>();
    }

    // Returns memory this single operator expects to allocate in the next round.
    // Does NOT include child operators — the pipeline task iterates all
    // operators itself.
    size_t get_reserve_mem_size(RuntimeState* state) override {
        auto& local_state = get_local_state(state);
        auto estimated_size = local_state.estimate_memory_usage();
        if (estimated_size < state->minimum_operator_memory_required_bytes()) {
            estimated_size = state->minimum_operator_memory_required_bytes();
        }
        return estimated_size;
    }

    void reset_reserve_mem_size(RuntimeState* state) override {
        auto& local_state = get_local_state(state);
        local_state.reset_estimate_memory_usage();
    }
};

/**
 * StreamingOperatorX indicates operators which always processes block in streaming way (one-in-one-out).
 */
template <typename LocalStateType>
class StreamingOperatorX : public OperatorX<LocalStateType> {
public:
    StreamingOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                       const DescriptorTbl& descs)
            : OperatorX<LocalStateType>(pool, tnode, operator_id, descs) {}

#ifdef BE_TEST
    StreamingOperatorX() = default;
#endif

    virtual ~StreamingOperatorX() = default;

    Status get_block_impl(RuntimeState* state, Block* block, bool* eos) override;

    virtual Status pull(RuntimeState* state, Block* block, bool* eos) = 0;
};

/**
 * StatefulOperatorX indicates the operators with some states inside.
 *
 * Specifically, we called an operator stateful if an operator can determine its output by itself.
 * For example, hash join probe operator is a typical StatefulOperator. When it gets a block from probe side, it will hold this block inside (e.g. _child_block).
 * If there are still remain rows in probe block, we can get output block by calling `get_block` without any data from its child.
 * In a nutshell, it is a one-to-many relation between input blocks and output blocks for StatefulOperator.
 */
template <typename LocalStateType>
class StatefulOperatorX : public OperatorX<LocalStateType> {
public:
    StatefulOperatorX(ObjectPool* pool, const TPlanNode& tnode, const int operator_id,
                      const DescriptorTbl& descs)
            : OperatorX<LocalStateType>(pool, tnode, operator_id, descs) {}
#ifdef BE_TEST
    StatefulOperatorX() = default;
#endif
    virtual ~StatefulOperatorX() = default;

    using OperatorX<LocalStateType>::get_local_state;

    [[nodiscard]] Status get_block_impl(RuntimeState* state, Block* block, bool* eos) override;

    [[nodiscard]] virtual Status pull(RuntimeState* state, Block* block, bool* eos) const = 0;
    [[nodiscard]] virtual Status push(RuntimeState* state, Block* input_block, bool eos) const = 0;
    // 判断算子当前是否还需要上游继续输入数据。默认返回 true；某些算子（如 Limit 算子满足条件后）可重写返回 false 以短路上游。
    bool need_more_input_data(RuntimeState* state) const override { return true; }
};

template <typename Writer, typename Parent>
    requires(std::is_base_of_v<AsyncResultWriter, Writer>)
class AsyncWriterSink : public PipelineXSinkLocalState<BasicSharedState> {
public:
    using Base = PipelineXSinkLocalState<BasicSharedState>;
    AsyncWriterSink(DataSinkOperatorXBase* parent, RuntimeState* state)
            : Base(parent, state), _async_writer_dependency(nullptr) {
        _finish_dependency =
                std::make_shared<Dependency>(parent->operator_id(), parent->node_id(),
                                             parent->get_name() + "_FINISH_DEPENDENCY", true);
    }

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;

    Status open(RuntimeState* state) override;

    Status sink(RuntimeState* state, Block* block, bool eos);

    std::vector<Dependency*> dependencies() const override {
        return {_async_writer_dependency.get()};
    }
    Status close(RuntimeState* state, Status exec_status) override;

    Dependency* finishdependency() override { return _finish_dependency.get(); }

protected:
    VExprContextSPtrs _output_vexpr_ctxs;
    std::unique_ptr<Writer> _writer;

    std::shared_ptr<Dependency> _async_writer_dependency;
    std::shared_ptr<Dependency> _finish_dependency;
};

#ifdef BE_TEST
class DummyOperatorLocalState final : public PipelineXLocalState<FakeSharedState> {
public:
    ENABLE_FACTORY_CREATOR(DummyOperatorLocalState);

    DummyOperatorLocalState(RuntimeState* state, OperatorXBase* parent)
            : PipelineXLocalState<FakeSharedState>(state, parent) {
        _tmp_dependency = Dependency::create_shared(_parent->operator_id(), _parent->node_id(),
                                                    "DummyOperatorDependency", true);
        _finish_dependency = Dependency::create_shared(_parent->operator_id(), _parent->node_id(),
                                                       "DummyOperatorDependency", true);
        _filter_dependency = Dependency::create_shared(_parent->operator_id(), _parent->node_id(),
                                                       "DummyOperatorDependency", true);
    }
    Dependency* finishdependency() override { return _finish_dependency.get(); }
    ~DummyOperatorLocalState() = default;

    std::vector<Dependency*> dependencies() const override { return {_tmp_dependency.get()}; }
    std::vector<Dependency*> execution_dependencies() override {
        return {_filter_dependency.get()};
    }

private:
    std::shared_ptr<Dependency> _tmp_dependency;
    std::shared_ptr<Dependency> _finish_dependency;
    std::shared_ptr<Dependency> _filter_dependency;
};

class DummyOperator final : public OperatorX<DummyOperatorLocalState> {
public:
    DummyOperator() : OperatorX<DummyOperatorLocalState>(nullptr, 0, 0) {}

    [[nodiscard]] bool is_source() const override { return true; }

    Status get_block_impl(RuntimeState* state, Block* block, bool* eos) override {
        *eos = _eos;
        return Status::OK();
    }
    void set_low_memory_mode(RuntimeState* state) override { _low_memory_mode = true; }
    Status terminate(RuntimeState* state) override {
        _terminated = true;
        return Status::OK();
    }
    size_t revocable_mem_size(RuntimeState* state) const override { return _revocable_mem_size; }
    size_t get_reserve_mem_size(RuntimeState* state) override {
        return _disable_reserve_mem
                       ? 0
                       : OperatorX<DummyOperatorLocalState>::get_reserve_mem_size(state);
    }
    Status revoke_memory(RuntimeState* state) override {
        _revoke_called = true;
        return Status::OK();
    }

private:
    friend class AssertNumRowsLocalState;
    bool _eos = false;
    bool _low_memory_mode = false;
    bool _terminated = false;
    size_t _revocable_mem_size = 0;
    bool _disable_reserve_mem = false;
    bool _revoke_called = false;
};

class DummySinkLocalState final : public PipelineXSinkLocalState<BasicSharedState> {
public:
    using Base = PipelineXSinkLocalState<BasicSharedState>;
    ENABLE_FACTORY_CREATOR(DummySinkLocalState);
    DummySinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state) : Base(parent, state) {
        _tmp_dependency = Dependency::create_shared(_parent->operator_id(), _parent->node_id(),
                                                    "DummyOperatorDependency", true);
        _finish_dependency = Dependency::create_shared(_parent->operator_id(), _parent->node_id(),
                                                       "DummyOperatorDependency", true);
    }

    std::vector<Dependency*> dependencies() const override { return {_tmp_dependency.get()}; }
    Dependency* finishdependency() override { return _finish_dependency.get(); }
    bool is_finished() const override { return _is_finished; }

private:
    std::shared_ptr<Dependency> _tmp_dependency;
    std::shared_ptr<Dependency> _finish_dependency;
    std::atomic_bool _is_finished = false;
};

class DummySinkOperatorX final : public DataSinkOperatorX<DummySinkLocalState> {
public:
    DummySinkOperatorX(int op_id, int node_id, int dest_id)
            : DataSinkOperatorX<DummySinkLocalState>(op_id, node_id, dest_id) {}
    Status sink_impl(RuntimeState* state, Block* in_block, bool eos) override {
        return _return_eof ? Status::Error<ErrorCode::END_OF_FILE>("source have closed")
                           : Status::OK();
    }
    void set_low_memory_mode(RuntimeState* state) override { _low_memory_mode = true; }
    Status terminate(RuntimeState* state) override {
        _terminated = true;
        return Status::OK();
    }
    size_t revocable_mem_size(RuntimeState* state) const override { return _revocable_mem_size; }
    size_t get_reserve_mem_size(RuntimeState* state, bool eos) override {
        return _disable_reserve_mem
                       ? 0
                       : DataSinkOperatorX<DummySinkLocalState>::get_reserve_mem_size(state, eos);
    }
    Status revoke_memory(RuntimeState* state) override {
        _revoke_called = true;
        return Status::OK();
    }

private:
    bool _low_memory_mode = false;
    bool _terminated = false;
    std::atomic_bool _return_eof = false;
    size_t _revocable_mem_size = 0;
    bool _disable_reserve_mem = false;
    bool _revoke_called = false;
};
#endif

} // namespace doris
