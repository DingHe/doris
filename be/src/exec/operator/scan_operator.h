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

#include <cstdint>
#include <optional>
#include <set>
#include <string>

#include "common/status.h"
#include "common/thread_safety_annotations.h"
#include "core/field.h"
#include "exec/common/util.hpp"
#include "exec/operator/operator.h"
#include "exec/pipeline/dependency.h"
#include "exec/runtime_filter/runtime_filter_consumer_helper.h"
#include "exec/runtime_filter/runtime_filter_partition_pruner.h"
#include "exec/scan/scan_node.h"
#include "exec/scan/scanner_context.h"
#include "exprs/function_filter.h"
#include "exprs/vectorized_fn_call.h"
#include "exprs/vin_predicate.h"
#include "runtime/descriptors.h"
#include "storage/predicate/filter_olap_param.h"

namespace doris {
class ScannerDelegate;
class OlapScanner;
} // namespace doris

namespace doris {

enum class PushDownType {
    // The predicate can not be pushed down to data source
    UNACCEPTABLE,
    // The predicate can be pushed down to data source
    // and the data source can fully evaludate it
    ACCEPTABLE,
    // The predicate can be pushed down to data source
    // but the data source can not fully evaluate it.
    PARTIAL_ACCEPTABLE
};
// ScanLocalStateBase 是 Apache Doris 执行引擎（ PipelineX 架构）中所有 Scan 算子（如 Hive Scan、Olap Scan、JDBC Scan、File Scan 等）的线程本地执行状态（Local State）基类。
// 在 Doris 的 PipelineX 执行引擎中，算子分为全局的 OperatorX（元数据/无状态）和运行在每个执行线程/PipelineTask 上的 PipelineXLocalState（本地运行状态）。
// 主要作用包括：
// 统一 Scan 算子的 LocalState 基类框架：提取所有存储介质/源表扫描共有的状态管理、Runtime Profile 统计指标、线程并发度控制与调度器策略。
// 谓词下推与规格化（Predicate Pushdown & Normalization）：提供一套统一且非模板化的谓词分析逻辑（如将 Expression AST 转换为 ColumnValueRange 或 ColumnPredicate），下推到底层存储引擎（如 Segment / Parquet reader）过滤数据。
// 动态 Runtime Filter 处理：支持运行期 Late-Arrival Runtime Filter 的接收、应用以及基于 Runtime Filter 的分区剪枝（Partition Pruning）。
// 降低模板编译膨胀（Code Bloat Reduction）：将不需要依赖特定模板参数 Derived 的公共成员变量与方法（尤其是复杂谓词处理和模板类型转换）从 ScanLocalState<Derived> 下移到基类 ScanLocalStateBase 中，大幅减少 C++ 模板实例化编译时间和生成的二进制文件体积。
class ScanLocalStateBase : public PipelineXLocalState<> {
public:
    ScanLocalStateBase(RuntimeState* state, OperatorXBase* parent)
            : PipelineXLocalState<>(state, parent), _helper(parent->runtime_filter_descs()) {}
    ~ScanLocalStateBase() override = default;
    // 返回当前 Scan 算子是否必须串行（单线程）运行（例如带有 TopN limit 的场景或极小表扫描）
    [[nodiscard]] virtual bool should_run_serial() const = 0;

    virtual RuntimeProfile* scanner_profile() = 0;

    [[nodiscard]] virtual const TupleDescriptor* output_tuple_desc() const = 0;

    virtual int64_t limit_per_scanner() = 0;
    virtual std::atomic<int64_t>* shared_scan_limit_ptr() = 0;
    // 为该 Scan 算子设置具体的分片/扫描范围数据（Scan Ranges）
    virtual void set_scan_ranges(RuntimeState* state,
                                 const std::vector<TScanRangeParams>& scan_ranges) = 0;
    // 获取下推到 Scan 阶段的聚合操作类型（例如 COUNT / MIN / MAX 的下推）。
    virtual TPushAggOp::type get_push_down_agg_type() = 0;
    // 获取下推的 COUNT(*) 或 COUNT(col) 操作对应的 Slot ID 集合。
    virtual const std::optional<std::vector<int32_t>>& get_push_down_count_slot_ids() const = 0;

    // If scan operator is serial operator(like topn), its real parallelism is 1.
    // Otherwise, its real parallelism is query_parallel_instance_num.
    // query_parallel_instance_num of olap table is usually equal to session var parallel_pipeline_task_num.
    // for file scan operator, its real parallelism will be 1 if it is in batch mode.
    // Related pr:
    // https://github.com/apache/doris/pull/42460
    // https://github.com/apache/doris/pull/44635
    // 计算并返回当前 Scan 算子的最大允许并发 Scanner 线程数（依据并行 Task 数、Batch 模式状态、Serial 属性等综合决定）
    [[nodiscard]] virtual int max_scanners_concurrency(RuntimeState* state) const;
    // 计算并返回保证当前 Scan 算子能够正常推进所需的最小并发 Scanner 线程数。
    [[nodiscard]] virtual int min_scanners_concurrency(RuntimeState* state) const;
    // 获取用于调度和执行当前 Scan 算子中 Scanner 任务的线程池调度器（ScannerScheduler）。
    [[nodiscard]] virtual ScannerScheduler* scan_scheduler(RuntimeState* state) const;

    // Thread-safe check whether a partition has been pruned by runtime filter.
    // Callable from any scan type's scanner in scheduling threads.
    // 线程安全地检查给定的 partition_id 是否已经被 Runtime Filter 成功剪枝。供各并发 Scanner 线程在调度时调用，避免读取无关分区。
    bool is_partition_pruned(int64_t partition_id) const;

    [[nodiscard]] std::string get_name() { return _parent->get_name(); }

    uint64_t get_condition_cache_digest() const { return _condition_cache_digest; }
    // 在查询执行过程中，检查并尝试更新迟到的（Late-Arrival）Runtime Filter，将最新到达的 RF 注入到当前 Scan 算子的表达式中，并触发新的分区剪枝。
    Status update_late_arrival_runtime_filter(RuntimeState* state, int& arrived_rf_num);

    Status clone_conjunct_ctxs(VExprContextSPtrs& scanner_conjuncts);

protected:
    friend class ScannerContext;
    friend class Scanner;

    virtual Status _init_profile() = 0;

    // Hook for subclasses to react after new runtime filters are appended.
    // Called inside update_late_arrival_runtime_filter() while _conjuncts_lock is held.
    // Default implementation runs partition pruning on the newly appended RFs.
    virtual Status _on_runtime_filter_update();

    Status _do_partition_pruning_by_rf();
    // 标记该 Scan 算子的 Local State 是否已经完成 open() 初始化，保证线程安全与幂等。
    std::atomic<bool> _opened {false};
    // Pipeline 引擎中的依赖对象（如等待 Runtime Filter 准备就绪的 Dependency），用于异步阻塞/唤醒 Pipeline 任务。
    DependencySPtr _scan_dependency = nullptr;

    std::shared_ptr<RuntimeProfile> _scanner_profile;
    RuntimeProfile::Counter* _scanner_wait_worker_timer = nullptr;
    // Num of newly created free blocks when running query
    RuntimeProfile::Counter* _newly_create_free_blocks_num = nullptr;
    // Max num of scanner thread
    RuntimeProfile::Counter* _max_scan_concurrency = nullptr;
    RuntimeProfile::Counter* _min_scan_concurrency = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _peak_running_scanner = nullptr;
    // time of get block from scanner
    RuntimeProfile::Counter* _scan_timer = nullptr;
    RuntimeProfile::Counter* _scan_cpu_timer = nullptr;
    // time of filter output block from scanner
    RuntimeProfile::Counter* _filter_timer = nullptr;
    // rows read from the scanner (including those discarded by (pre)filters)
    RuntimeProfile::Counter* _rows_read_counter = nullptr;

    RuntimeProfile::Counter* _num_scanners = nullptr;

    RuntimeProfile::Counter* _wait_for_rf_timer = nullptr;

    RuntimeProfile::Counter* _scan_rows = nullptr;
    RuntimeProfile::Counter* _scan_bytes = nullptr;
    // 互斥锁，用于保护谓词表达式（Conjuncts）在运行时被 Late-Arrival Runtime Filter 动态更新时的并发安全。
    AnnotatedMutex _conjuncts_lock;
    // 管理 Runtime Filter 消费的辅助类，负责注册和提取分配给当前算子的 Runtime Filter。
    RuntimeFilterConsumerHelper _helper;
    // magic number as seed to generate hash value for condition cache
    // 条件缓存的哈希摘要种子/Key，用于标识当前查询条件下下推谓词组合的唯一性。
    uint64_t _condition_cache_digest = 0;
    // condition cache filter stats
    // 条件缓存（Condition Cache）命中的次数统计。
    RuntimeProfile::Counter* _condition_cache_hit_counter = nullptr;
    // 通过条件缓存过滤掉的行数统计。
    RuntimeProfile::Counter* _condition_cache_filtered_rows_counter = nullptr;

    // ---- Runtime-filter partition pruning (scan-agnostic) ----
    RuntimeFilterPartitionPruner _rf_partition_pruner;
    RuntimeProfile::Counter* _partitions_pruned_by_rf_counter = nullptr;
    RuntimeProfile::Counter* _total_partitions_rf_counter = nullptr;

    // Moved from ScanLocalState<Derived> to avoid re-instantiation for each Derived type.
    // 标记该 Scan 算子数据流是否已经全部读取完毕（End Of Stream）。
    std::atomic<bool> _eos = false;
    // 单列支持下推的最大条件表达式个数（默认 1024），超过此阈值后不再继续下推该列的条件，防止生成过大的过滤条件（如极其庞大的 IN List）。
    int _max_pushdown_conditions_per_column = 1024;
    // Save all function predicates which may be pushed down to data source.
    // 收集并保存允许下推到底层数据源（如 External Storage/Lucene）执行的函数谓词列表（例如字符串 LIKE 或特定标量函数）。
    std::vector<FunctionFilter> _push_down_functions;

    // Virtual methods with default implementations; overridden by subclasses when supported.
    // Declared here so that the normalize methods below (non-Derived-template) can call them.
    virtual bool _push_down_topn(const RuntimePredicate& predicate) { return false; }
    virtual PushDownType _should_push_down_bloom_filter() const {
        return PushDownType::UNACCEPTABLE;
    }
    virtual PushDownType _should_push_down_topn_filter() const {
        return PushDownType::UNACCEPTABLE;
    }
    virtual PushDownType _should_push_down_is_null_predicate(VectorizedFnCall* fn_call) const {
        return PushDownType::UNACCEPTABLE;
    }
    virtual PushDownType _should_push_down_in_predicate() const {
        return PushDownType::UNACCEPTABLE;
    }
    virtual PushDownType _should_push_down_binary_predicate(
            VectorizedFnCall* fn_call, VExprContext* expr_ctx, Field& constant_val,
            const std::set<std::string> fn_name) const {
        return PushDownType::UNACCEPTABLE;
    }
    virtual Status _should_push_down_function_filter(VectorizedFnCall* fn_call,
                                                     VExprContext* expr_ctx,
                                                     StringRef* constant_str,
                                                     doris::FunctionContext** fn_ctx,
                                                     PushDownType& pdt) {
        pdt = PushDownType::UNACCEPTABLE;
        return Status::OK();
    }

    // Non-templated normalize methods, moved here to avoid re-compilation per Derived type.
    Status _eval_const_conjuncts(VExprContext* expr_ctx, PushDownType* pdt);
    Status _normalize_bloom_filter(VExprContext* expr_ctx, const VExprSPtr& root,
                                   SlotDescriptor* slot,
                                   std::vector<std::shared_ptr<ColumnPredicate>>& predicates,
                                   PushDownType* pdt);
    Status _normalize_topn_filter(VExprContext* expr_ctx, const VExprSPtr& root,
                                  SlotDescriptor* slot,
                                  std::vector<std::shared_ptr<ColumnPredicate>>& predicates,
                                  PushDownType* pdt);
    Status _normalize_function_filters(VExprContext* expr_ctx, SlotDescriptor* slot,
                                       PushDownType* pdt);

    // Inner PrimitiveType-template methods. Moved to base to avoid N(Derived)×M(PrimitiveType)
    // instantiation blowup: now instantiated M times total instead of N×M times.
    template <PrimitiveType T>
    Status _normalize_in_predicate(VExprContext* expr_ctx, const VExprSPtr& root,
                                   SlotDescriptor* slot,
                                   std::vector<std::shared_ptr<ColumnPredicate>>& predicates,
                                   ColumnValueRange<T>& range, PushDownType* pdt);
    template <PrimitiveType T>
    Status _normalize_binary_predicate(VExprContext* expr_ctx, const VExprSPtr& root,
                                       SlotDescriptor* slot,
                                       std::vector<std::shared_ptr<ColumnPredicate>>& predicates,
                                       ColumnValueRange<T>& range, PushDownType* pdt);
    template <PrimitiveType T>
    Status _normalize_is_null_predicate(VExprContext* expr_ctx, const VExprSPtr& root,
                                        SlotDescriptor* slot,
                                        std::vector<std::shared_ptr<ColumnPredicate>>& predicates,
                                        ColumnValueRange<T>& range, PushDownType* pdt);
    template <PrimitiveType PrimitiveType, typename ChangeFixedValueRangeFunc>
    Status _change_value_range(bool is_equal_op, ColumnValueRange<PrimitiveType>& range,
                               const Field& value, const ChangeFixedValueRangeFunc& func,
                               const std::string& fn_name);
};

template <typename LocalStateType>
class ScanOperatorX;
template <typename Derived>
class ScanLocalState : public ScanLocalStateBase {
    ENABLE_FACTORY_CREATOR(ScanLocalState);
    ScanLocalState(RuntimeState* state, OperatorXBase* parent)
            : ScanLocalStateBase(state, parent) {}
    ~ScanLocalState() override = default;

    Status init(RuntimeState* state, LocalStateInfo& info) override;

    Status open(RuntimeState* state) override;

    Status close(RuntimeState* state) override;
    std::string debug_string(int indentation_level) const final;

    [[nodiscard]] bool should_run_serial() const override;

    RuntimeProfile* scanner_profile() override { return _scanner_profile.get(); }

    [[nodiscard]] const TupleDescriptor* output_tuple_desc() const override;

    int64_t limit_per_scanner() override;
    std::atomic<int64_t>* shared_scan_limit_ptr() override;

    void set_scan_ranges(RuntimeState* state,
                         const std::vector<TScanRangeParams>& scan_ranges) override {}

    TPushAggOp::type get_push_down_agg_type() override;
    const std::optional<std::vector<int32_t>>& get_push_down_count_slot_ids() const override;

    std::vector<Dependency*> execution_dependencies() override {
        if (_filter_dependencies.empty()) {
            return {};
        }
        std::vector<Dependency*> res(_filter_dependencies.size());
        std::transform(_filter_dependencies.begin(), _filter_dependencies.end(), res.begin(),
                       [](DependencySPtr dep) { return dep.get(); });
        return res;
    }

    std::vector<Dependency*> dependencies() const override { return {_scan_dependency.get()}; }

    std::vector<int> get_topn_filter_source_node_ids(RuntimeState* state, bool push_down) {
        std::vector<int> result;
        for (int id : _parent->cast<typename Derived::Parent>()._topn_filter_source_node_ids) {
            const auto& pred = state->get_query_ctx()->get_runtime_predicate(id);
            if (!pred.enable()) {
                continue;
            }
            if (_push_down_topn(pred) == push_down) {
                result.push_back(id);
            }
        }
        return result;
    }

protected:
    template <typename LocalStateType>
    friend class ScanOperatorX;
    friend class ScannerContext;
    friend class Scanner;

    Status _init_profile() override;
    virtual Status _process_conjuncts(RuntimeState* state) {
        RETURN_IF_ERROR(_do_partition_pruning_by_rf());
        return _normalize_conjuncts(state);
    }
    virtual bool _should_push_down_common_expr(const VExprSPtr&) { return false; }

    virtual bool can_push_down_column_predicate(const SlotDescriptor* slot) {
        return _parent->cast<typename Derived::Parent>().can_push_down_column_predicate(slot);
    }

    virtual bool _storage_no_merge() { return false; }
    virtual bool _is_key_column(const std::string& col_name) { return false; }

    // Create a list of scanners.
    // The number of scanners is related to the implementation of the data source,
    // predicate conditions, and scheduling strategy.
    // So this method needs to be implemented separately by the subclass of ScanNode.
    // Finally, a set of scanners that have been prepared are returned.
    virtual Status _init_scanners(std::list<ScannerSPtr>* scanners) { return Status::OK(); }

    Status _normalize_conjuncts(RuntimeState* state);
    // Normalize a conjunct and try to convert it to column predicate recursively.
    Status _normalize_predicate(VExprContext* context, const VExprSPtr& root,
                                VExprSPtr& output_expr);
    bool _is_predicate_acting_on_slot(const VExprSPtrs& children, SlotDescriptor** slot_desc,
                                      ColumnValueRangeType** range);
    Status _prepare_scanners();

    // Submit the scanner to the thread pool and start execution
    Status _start_scanners(const std::list<std::shared_ptr<ScannerDelegate>>& scanners);

    // For some conjunct there is chance to elimate cast operator
    // Eg. Variant's sub column could eliminate cast in storage layer if
    // cast dst column type equals storage column type
    void get_cast_types_for_variants();
    void _filter_and_collect_cast_type_for_variant(
            const VExpr* expr,
            std::unordered_map<std::string, std::vector<DataTypePtr>>& colname_to_cast_types);

    Status _get_topn_filters(RuntimeState* state);

    // Stores conjuncts that have been fully pushed down to the storage layer as predicate columns.
    // These expr contexts are kept alive to prevent their FunctionContext and constant strings
    // from being freed prematurely.
    VExprContextSPtrs _stale_expr_ctxs;
    VExprContextSPtrs _common_expr_ctxs_push_down;

    atomic_shared_ptr<ScannerContext> _scanner_ctx;

    // colname -> cast dst type
    std::map<std::string, DataTypePtr> _cast_types_for_variants;

    // slot id -> ColumnValueRange
    // Parsed from conjuncts
    phmap::flat_hash_map<int, ColumnValueRangeType> _slot_id_to_value_range;
    phmap::flat_hash_map<int, std::vector<std::shared_ptr<ColumnPredicate>>> _slot_id_to_predicates;
    std::vector<std::shared_ptr<MutilColumnBlockPredicate>> _or_predicates;

    std::vector<std::shared_ptr<Dependency>> _filter_dependencies;

    // ScanLocalState owns the ownership of scanner, scanner context only has its weakptr
    std::list<std::shared_ptr<ScannerDelegate>> _scanners;
    Arena _arena;
    int _instance_idx = 0;
};

// 在 Apache Doris 的 Backend (BE) 执行引擎中，ScanOperatorX 是所有数据扫描类算子（Scan Operator，如 OlapScanOperatorX、FileScanOperatorX、JdbcScanOperatorX 等）的通用抽象模板基类。它继承自 OperatorX<LocalStateType>，专门针对数据源（Data Source）端的读取与下推优化（Push-down Optimization）进行了封装。
// 作为 Pipeline 框架中所有数据扫描源头算子的基类，ScanOperatorX 承担了以下核心职责：
// 定义数据源头（Source Operator）语义：重写 is_source() = true，标记该算子为 Pipeline 管道的最上游节点（不依赖上游输入算子，主动从存储引擎/外部数据源拉取数据）。
// 下推谓词与聚合管理（Push-down Management）：存储和管理从 FE（Frontend）下推到存储层/扫描层的各种优化条件（如下推谓词 _common_expr_ctxs_push_down、聚合下推类型 _push_down_agg_type、下推的 COUNT Slot ID 等）。
// 共享 Limit 与扫描并发控制：维护跨并发 Task 实例共享的剩余 Limit 行数（_shared_scan_limit）以及单 Scanner 的 Limit（_limit_per_scanner），实现高效的全局提前终止（Early Exit）。
// Runtime Filter 与分区剪枝：存储与该扫描算子相关的 Runtime Filter 描述符，并管理用于动态分区剪枝的边界解析数据（_parsed_partition_boundaries）。
// 内存感知与低内存模式（Low Memory Mode）：支持动态内存策略，响应系统的内存压力，提供在内存紧张时清理释放 Scanner 缓存 Block（clear_free_blocks）的机制。
template <typename LocalStateType>
class ScanOperatorX : public OperatorX<LocalStateType> {
public:
    // 基于 Thrift 结构 tnode（对应 TScanNode）初始化该扫描算子。
    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status get_block_impl(RuntimeState* state, Block* block, bool* eos) override;
    Status get_block_after_projects(RuntimeState* state, Block* block, bool* eos) override {
        Status status = OperatorX<LocalStateType>::get_block(state, block, eos);
        if (status.ok()) {
            state->get_local_state(operator_id())->update_output_block_counters(*block);
        }
        return status;
    }
    [[nodiscard]] bool is_source() const override { return true; }

    [[nodiscard]] size_t get_reserve_mem_size(RuntimeState* state) override;

    const std::vector<TRuntimeFilterDesc>& runtime_filter_descs() override {
        return _runtime_filter_descs;
    }

    // Expose this operator's per-fragment shared partition-boundary parse
    // result to the non-templated ScanLocalStateBase so it can drive runtime
    // filter partition pruning without down-casting to a specific scan type.
    // Subclasses are expected to populate `_parsed_partition_boundaries` from
    // their own partition-boundary thrift field inside their `prepare()`
    // override before any LocalState observes the result.
    const ParsedPartitionBoundaries* parsed_partition_boundaries() const override {
        return &_parsed_partition_boundaries;
    }

    [[nodiscard]] virtual int get_column_id(const std::string& col_name) const { return -1; }

    [[nodiscard]] virtual bool can_push_down_column_predicate(const SlotDescriptor*) const {
        return true;
    }

    TPushAggOp::type get_push_down_agg_type() { return _push_down_agg_type; }

    DataDistribution required_data_distribution(RuntimeState* /*state*/) const override {
        if (OperatorX<LocalStateType>::is_serial_operator()) {
            // `is_serial_operator()` returns true means we ignore the distribution.
            return {TLocalPartitionType::NOOP};
        }
        return {TLocalPartitionType::BUCKET_HASH_SHUFFLE};
    }

    void set_low_memory_mode(RuntimeState* state) override {
        auto& local_state = get_local_state(state);

        if (auto ctx = local_state._scanner_ctx.load()) {
            ctx->clear_free_blocks();
        }
    }

    using OperatorX<LocalStateType>::node_id;
    using OperatorX<LocalStateType>::operator_id;
    using OperatorX<LocalStateType>::get_local_state;

#ifdef BE_TEST
    ScanOperatorX() = default;
#endif

protected:
    using LocalState = LocalStateType;
    friend class OlapScanner;
    ScanOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                  const DescriptorTbl& descs, int parallel_tasks = 0);
    virtual ~ScanOperatorX() = default;
    template <typename Derived>
    friend class ScanLocalState;
    friend class OlapScanLocalState;

    // For load scan node, there should be both input and output tuple descriptor.
    // For query scan node, there is only output_tuple_desc.
    // 数据导入（Load Scan）场景下的输入 Tuple ID。在普通 Query 扫描中通常不使用（保持为 -1）。
    TupleId _input_tuple_id = -1;
    // 该扫描算子输出数据对应的 Tuple ID（在 FE 逻辑执行计划中定义）。
    TupleId _output_tuple_id = -1;
    // 输出 Tuple 的描述符指针，包含了该算子需要扫描并填充的所有列（Slot）元数据信息。
    const TupleDescriptor* _output_tuple_desc = nullptr;
    // 从 Slot ID 到其对应的 SlotDescriptor 指针的高效哈希映射表，用于快速根据 ID 查找列元数据。
    phmap::flat_hash_map<int, SlotDescriptor*> _slot_id_to_slot_desc;
    // 从列名（Column Name）到 Slot ID 的哈希映射表，用于根据列名查找对应的 Slot ID。
    std::unordered_map<std::string, int> _colname_to_slot_id;

    // These two values are from query_options
    // 从 FE 的 query_options 中获取的构建 Scan Key（主键/前缀索引查询条件）的最大数量限制，默认 48。
    int _max_scan_key_num = 48;
    // 从 FE 获得的单列允许下推的最大谓词条件/表达式数量限制，默认 1024。
    int _max_pushdown_conditions_per_column = 1024;

    // If the query like select * from table limit 10; then the query should run in
    // single scanner to avoid too many scanners which will cause lots of useless read.
    // 标记是否应当强制串行单线程运行。例如对于 SELECT * FROM table LIMIT 10 这类小 Limit 查询，设置为 true 可以避免启动过多并发 Scanner 造成无用的存储 IO 浪费。
    bool _should_run_serial = false;
    // 已成功下推到存储层/Scanner 层的普通表达式/谓词上下文列表。
    VExprContextSPtrs _common_expr_ctxs_push_down;

    // If sort info is set, push limit to each scanner;
    // 当下推了 TopN/Sort 信息时，下推给每个独立 Scanner 的 Limit 行数限制（-1 表示无限制）。
    int64_t _limit_per_scanner = -1;

    // Shared remaining limit across all parallel instances and their scanners.
    // Initialized to _limit (SQL LIMIT); -1 means no limit.
    // 跨所有并发 Pipeline Task 实例及其下属 Scanner 共享的全局剩余 Limit 计数器（原子变量）。初始化为 SQL 中的 LIMIT 值，每当有 Scanner 读取到 Block 时会原子扣减，降至 0 时通知所有并发 Scanner 快速结束。
    std::atomic<int64_t> _shared_scan_limit {-1};
    // 分配并下推到当前 Scan 节点上的所有 Runtime Filter 的 Thrift 描述符集合。
    std::vector<TRuntimeFilterDesc> _runtime_filter_descs;
    // 下推到扫描层的聚合函数类型（例如 COUNT、MIN、MAX、COUNT_ON_INDEX 等）。
    TPushAggOp::type _push_down_agg_type;

    // Semantic arguments of a pushed-down COUNT. This is deliberately optional because absence
    // and an empty list have different meanings during a BE-first rolling upgrade:
    //
    //  - nullopt: an old FE did not send the field, so the new BE must use the normal scan;
    //  - empty: the new FE explicitly planned COUNT(*)/COUNT(1);
    //  - non-empty: the new FE explicitly planned COUNT(col).
    //
    // Treating nullopt as empty would silently reinterpret an old plan as COUNT(*).
    // 下推的 COUNT(...) 聚合函数对应的输入列 Slot ID 集合。
    // 使用 std::optional 是为了兼容 FE 与 BE 跨版本滚动升级：nullopt 代表旧版 FE 未发送该字段（需回退到普通扫描）；empty（空向量）代表显式下推了 COUNT(*) 或 COUNT(1)；non-empty 代表显式下推了 COUNT(col)。
    std::optional<std::vector<int32_t>> _push_down_count_slot_ids;

    // Record the value of the aggregate function 'count' from doris's be
    // 若整个 COUNT(*) 能够完全通过存储层索引/元数据直接计算得出，则记录该预计算的行数结果（-1 代表未下推优化）。
    int64_t _push_down_count = -1;
    // 当前扫描算子的并行任务/实例数量。
    const int _parallel_tasks = 0;
    // 产生下推到当前 Scan 节点的 TopN 动态 Filter 的源节点 ID 列表。
    std::vector<int> _topn_filter_source_node_ids;
    // 多 Scanner 之间用于共享/仲裁内存配额的仲裁器指针。
    std::shared_ptr<MemShareArbitrator> _mem_arb = nullptr;
    // 当前扫描算子使用的内存限制器指针，用于跟踪与控制 Scanner 的内存申请。
    std::shared_ptr<MemLimiter> _mem_limiter = nullptr;

    // Shared parse result of partition boundaries for runtime-filter partition
    // pruning. Lives here (rather than on the Olap-specific subclass) so any
    // future scan type can populate it in its `prepare()` override and reuse
    // the generic pruning machinery in ScanLocalStateBase.
    // 当前 Fragment 内部共享的已解析分区边界信息，供非模板基类 ScanLocalStateBase 执行 Runtime Filter 动态分区剪枝使用。
    ParsedPartitionBoundaries _parsed_partition_boundaries;
};

} // namespace doris
