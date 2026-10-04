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

#include <stdint.h>

#include <shared_mutex>
#include <string>
#include <unordered_set>

#include "cloud/cloud_tablet.h"
#include "common/status.h"
#include "exec/operator/operator.h"
#include "exec/operator/scan_operator.h"
#include "runtime/runtime_profile.h"
#include "storage/olap_scan_common.h"
#include "storage/tablet/tablet_reader.h"

namespace doris {
class OlapScanner;
class QueryCacheRuntime;
struct QueryCacheInstanceDecision;
} // namespace doris

namespace doris {

class OlapScanOperatorX;

// OlapScanLocalState 是 Pipeline 执行引擎架构下，用于管理 内表（Olap Table/Doris 本地表）数据扫描算子（OlapScanOperatorX）在具体线程/执行实例（Execution Instance）级别运行时状态 的核心类
// OlapScanLocalState 类的作用
// 实例级状态隔离与存储：在 Pipeline 架构下，OlapScanOperatorX 作为全局共享的 Operator 模版，而 OlapScanLocalState 则存储了具体每个 Pipeline 执行线程/PipelineTask 独立的局部状态（包括分配给该实例的 scan_ranges / tablets、内部初始化产生的 Scanner 列表等）。
// 连接下推谓词与存储引擎：负责将上层执行引擎传入的 SQL 过滤谓词（Conjuncts、Runtime Filters、TopN 谓词等）进行校验与解析，判断能否下推（Push Down），并将其转换为存储引擎（OlapTable / Segment V2）可识别的 Key Range、Column Predicates 以及 Index Filter（如 ZoneMap、BloomFilter、倒排索引、向量索引 ANN 等）。
// 云原生/存算分离架构调度（Cloud Mode）：支持存算分离模式下的 Tablet 与 Rowset 元数据异步同步（Sync Cloud Tablets），并通过 Dependency 机制阻塞与唤醒 PipelineTask。
// 指标与 Profiling 统计：维护了极其详尽的性能监控 Counter（IO耗时、解压耗时、各类索引过滤行数、延迟读取 Lazy Read 耗时、Variant 动态列解析耗时等），用于生成 Doris Query Profile。
class OlapScanLocalState final : public ScanLocalState<OlapScanLocalState> {
public:
    // 指向其对应的全局 Pipeline Operator 类型。
    using Parent = OlapScanOperatorX;
    // 继承自扫描基类 ScanLocalState。
    using Base = ScanLocalState<OlapScanLocalState>;
    // Doris 内部工厂宏，用于提供安全的智能指针创建工厂方法。
    ENABLE_FACTORY_CREATOR(OlapScanLocalState);
    OlapScanLocalState(RuntimeState* state, OperatorXBase* parent) : Base(state, parent) {}
    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status prepare(RuntimeState* state) override;
    TOlapScanNode& olap_scan_node() const;

    std::string name_suffix() const override {
        if (_parent->nereids_id() == -1) {
            return fmt::format("(id={}, table_name={})", _parent->node_id(),
                               olap_scan_node().table_name);
        }
        return fmt::format("(nereids_id={}, id={}, table_name={})", _parent->nereids_id(),
                           _parent->node_id(), olap_scan_node().table_name);
    }
    std::vector<Dependency*> execution_dependencies() override {
        if (!_cloud_tablet_dependency) {
            return Base::execution_dependencies();
        }
        std::vector<Dependency*> res = Base::execution_dependencies();
        res.push_back(_cloud_tablet_dependency.get());
        return res;
    }

    Status open(RuntimeState* state) override;

private:
    friend class OlapScanner;

    Status _sync_cloud_tablets(RuntimeState* state);
    void set_scan_ranges(RuntimeState* state,
                         const std::vector<TScanRangeParams>& scan_ranges) override;
    Status _init_profile() override;
    Status _process_conjuncts(RuntimeState* state) override;
    bool _is_key_column(const std::string& col_name) override;

    bool can_push_down_column_predicate(const SlotDescriptor* slot) override;

    Status _should_push_down_function_filter(VectorizedFnCall* fn_call, VExprContext* expr_ctx,
                                             StringRef* constant_str,
                                             doris::FunctionContext** fn_ctx,
                                             PushDownType& pdt) override;

    PushDownType _should_push_down_bloom_filter() const override {
        return PushDownType::ACCEPTABLE;
    }

    PushDownType _should_push_down_topn_filter() const override { return PushDownType::ACCEPTABLE; }

    PushDownType _should_push_down_is_null_predicate(VectorizedFnCall* fn_call) const override {
        return fn_call->fn().name.function_name == "is_null_pred" ||
                               fn_call->fn().name.function_name == "is_not_null_pred"
                       ? PushDownType::ACCEPTABLE
                       : PushDownType::UNACCEPTABLE;
    }

    PushDownType _should_push_down_in_predicate() const override {
        return PushDownType::ACCEPTABLE;
    }
    PushDownType _should_push_down_binary_predicate(
            VectorizedFnCall* fn_call, VExprContext* expr_ctx, Field& constant_val,
            const std::set<std::string> fn_name) const override;

    bool _should_push_down_common_expr(const VExprSPtr& expr) override;

    enum class ExprStorageFilterCheckMode { HAS_SEGMENT_EVALUABLE_EXPR, HAS_NON_KEY_SLOT };
    bool _check_expr_storage_filter(const VExprSPtr& expr, ExprStorageFilterCheckMode mode);

    bool _storage_no_merge() override;

    bool _read_mor_as_dup();
    // True for MIN_DELTA / DETAIL binlog scans, which read through BlockReader's merge (op
    // synthesis + BEFORE/AFTER split) and thus must keep predicates above the reader. Returns bool
    // to avoid leaking the thrift binlog-scan-type enum into this header.
    bool _is_binlog_merge_scan() const;
    bool _push_down_topn(const RuntimePredicate& predicate) override {
        if (!predicate.target_is_slot(_parent->node_id())) {
            return false;
        }
        if (!olap_scan_node().__isset.columns_desc || olap_scan_node().columns_desc.empty() ||
            olap_scan_node().columns_desc[0].col_unique_id < 0) {
            // Disable topN filter if there is no schema info
            return false;
        }
        return _is_key_column(predicate.get_col_name(_parent->node_id()));
    }

    Status _init_scanners(std::list<ScannerSPtr>* scanners) override;

    Status _build_key_ranges_and_filters();
    // 存储分配给当前 LocalState 的原始 Thrift 扫描范围列表（包含 Tablet ID、 Version 等信息）
    std::vector<std::unique_ptr<TPaloScanRange>> _scan_ranges;
    // 存算分离下同步 Rowset 元数据的统计信息。
    std::vector<SyncRowsetStats> _sync_statistics;
    // 用于测量异步同步 Cloud Tablet 元数据耗时的单向计时器。
    MonotonicStopWatch _sync_cloud_tablets_watcher;
    // Pipeline 依赖项。当 Tablet 元数据尚未异步同步完成时，该 Dependency 会阻塞 Pipeline Task 的执行。
    std::shared_ptr<Dependency> _cloud_tablet_dependency;
    // 当前正在等待异步同步完成的 Tablet 数量计数器。
    std::atomic<size_t> _pending_tablets_num = 0;
    // 标记 LocalState 是否已准备就绪。
    bool _prepared = false;
    // 异步获取/同步云端 Tablet 元数据的 Future 异步句柄。
    std::future<Status> _cloud_tablet_future;
    // 标记当前 LocalState 是否正在或已触发 Cloud Tablet 同步。
    std::atomic_bool _sync_tablet = false;
    // 根据条件下推计算出的扫描条件范围（如 key >= 100 AND key <= 200）
    std::vector<std::unique_ptr<doris::OlapScanRange>> _cond_ranges;
    // 存储根据谓词下推构建的主键/排序键查找 Range（Key Ranges）
    OlapScanKeys _scan_keys;
    // If column id in this set, indicate that we need to read data after index filtering
    // 需要从存储层读取并输出的 Column Unique ID 集合。
    std::set<int32_t> _output_column_ids;

    std::unique_ptr<RuntimeProfile> _segment_profile;
    std::unique_ptr<RuntimeProfile> _index_filter_profile;

    RuntimeProfile::Counter* _tablet_counter = nullptr;
    RuntimeProfile::Counter* _key_range_counter = nullptr;
    RuntimeProfile::Counter* _reader_init_timer = nullptr;
    RuntimeProfile::Counter* _scanner_init_timer = nullptr;
    RuntimeProfile::Counter* _process_conjunct_timer = nullptr;

    RuntimeProfile::Counter* _io_timer = nullptr;
    RuntimeProfile::Counter* _read_compressed_counter = nullptr;
    RuntimeProfile::Counter* _decompressor_timer = nullptr;
    RuntimeProfile::Counter* _read_uncompressed_counter = nullptr;

    RuntimeProfile::Counter* _rows_vec_cond_filtered_counter = nullptr;
    RuntimeProfile::Counter* _rows_short_circuit_cond_filtered_counter = nullptr;
    RuntimeProfile::Counter* _rows_expr_cond_filtered_counter = nullptr;
    RuntimeProfile::Counter* _rows_vec_cond_input_counter = nullptr;
    RuntimeProfile::Counter* _rows_short_circuit_cond_input_counter = nullptr;
    RuntimeProfile::Counter* _rows_expr_cond_input_counter = nullptr;
    RuntimeProfile::Counter* _vec_cond_timer = nullptr;
    RuntimeProfile::Counter* _short_cond_timer = nullptr;
    RuntimeProfile::Counter* _expr_filter_timer = nullptr;
    RuntimeProfile::Counter* _output_col_timer = nullptr;

    RuntimeProfile::Counter* _stats_filtered_counter = nullptr;
    RuntimeProfile::Counter* _stats_rp_filtered_counter = nullptr;
    // Number of whole segments skipped by expression ZoneMap evaluation.
    RuntimeProfile::Counter* _expr_zonemap_filtered_segment_counter = nullptr;
    // Number of pages skipped by expression ZoneMap evaluation after page index ranges are built.
    RuntimeProfile::Counter* _expr_zonemap_filtered_page_counter = nullptr;
    // Number of expression ZoneMap evaluations that reached the evaluator but could not use the
    // current ZoneMap context, such as missing slot/page ZoneMap or unusable range statistics.
    RuntimeProfile::Counter* _expr_zonemap_unusable_counter = nullptr;
    // Number of IN-predicate ZoneMap evaluations that used per-value point checks.
    RuntimeProfile::Counter* _in_zonemap_point_check_counter = nullptr;
    // Number of IN-predicate ZoneMap evaluations that fell back to min/max range overlap only.
    RuntimeProfile::Counter* _in_zonemap_range_only_counter = nullptr;
    RuntimeProfile::Counter* _bf_filtered_counter = nullptr;
    RuntimeProfile::Counter* _dict_filtered_counter = nullptr;
    RuntimeProfile::Counter* _del_filtered_counter = nullptr;
    RuntimeProfile::Counter* _conditions_filtered_counter = nullptr;
    RuntimeProfile::Counter* _key_range_filtered_counter = nullptr;

    RuntimeProfile::Counter* _block_fetch_timer = nullptr;
    RuntimeProfile::Counter* _delete_bitmap_get_agg_timer = nullptr;
    RuntimeProfile::Counter* _sync_rowset_timer = nullptr;
    RuntimeProfile::Counter* _sync_rowset_get_remote_tablet_meta_rpc_timer = nullptr;
    RuntimeProfile::Counter* _sync_rowset_tablet_meta_cache_hit = nullptr;
    RuntimeProfile::Counter* _sync_rowset_tablet_meta_cache_miss = nullptr;
    RuntimeProfile::Counter* _sync_rowset_tablets_rowsets_total_num = nullptr;
    RuntimeProfile::Counter* _sync_rowset_get_remote_rowsets_num = nullptr;
    RuntimeProfile::Counter* _sync_rowset_get_remote_rowsets_rpc_timer = nullptr;
    RuntimeProfile::Counter* _sync_rowset_get_local_delete_bitmap_rowsets_num = nullptr;
    RuntimeProfile::Counter* _sync_rowset_get_remote_delete_bitmap_rowsets_num = nullptr;
    RuntimeProfile::Counter* _sync_rowset_get_remote_delete_bitmap_key_count = nullptr;
    RuntimeProfile::Counter* _sync_rowset_get_remote_delete_bitmap_bytes = nullptr;
    RuntimeProfile::Counter* _sync_rowset_get_remote_delete_bitmap_rpc_timer = nullptr;
    RuntimeProfile::Counter* _sync_rowset_bthread_schedule_wait_timer = nullptr;
    RuntimeProfile::Counter* _sync_rowset_meta_lock_wait_timer = nullptr;
    RuntimeProfile::Counter* _sync_rowset_sync_meta_lock_wait_timer = nullptr;
    RuntimeProfile::Counter* _block_load_timer = nullptr;
    RuntimeProfile::Counter* _block_load_counter = nullptr;
    // Add more detail seek timer and counter profile
    // Read process is split into 3 stages: init, first read, lazy read
    RuntimeProfile::Counter* _block_init_timer = nullptr;
    RuntimeProfile::Counter* _block_init_seek_timer = nullptr;
    RuntimeProfile::Counter* _block_init_seek_counter = nullptr;
    RuntimeProfile::Counter* _segment_generate_row_range_by_keys_timer = nullptr;
    RuntimeProfile::Counter* _segment_generate_row_range_by_column_conditions_timer = nullptr;
    RuntimeProfile::Counter* _segment_generate_row_range_by_bf_timer = nullptr;
    RuntimeProfile::Counter* _collect_iterator_merge_next_timer = nullptr;
    RuntimeProfile::Counter* _segment_generate_row_range_by_zonemap_timer = nullptr;
    RuntimeProfile::Counter* _segment_generate_row_range_by_dict_timer = nullptr;
    RuntimeProfile::Counter* _predicate_column_read_timer = nullptr;
    RuntimeProfile::Counter* _non_predicate_column_read_timer = nullptr;
    RuntimeProfile::Counter* _predicate_column_read_seek_timer = nullptr;
    RuntimeProfile::Counter* _predicate_column_read_seek_counter = nullptr;
    RuntimeProfile::Counter* _lazy_read_timer = nullptr;
    RuntimeProfile::Counter* _lazy_read_seek_timer = nullptr;
    RuntimeProfile::Counter* _lazy_read_seek_counter = nullptr;
    RuntimeProfile::Counter* _lazy_read_pruned_timer = nullptr;

    // total pages read
    // used by segment v2
    RuntimeProfile::Counter* _total_pages_num_counter = nullptr;
    // page read from cache
    // used by segment v2
    RuntimeProfile::Counter* _cached_pages_num_counter = nullptr;

    RuntimeProfile::Counter* _statistics_collect_timer = nullptr;
    RuntimeProfile::Counter* _inverted_index_filter_counter = nullptr;
    RuntimeProfile::Counter* _inverted_index_filter_timer = nullptr;
    RuntimeProfile::Counter* _inverted_index_query_null_bitmap_timer = nullptr;
    RuntimeProfile::Counter* _inverted_index_query_cache_hit_counter = nullptr;
    RuntimeProfile::Counter* _inverted_index_query_cache_miss_counter = nullptr;
    RuntimeProfile::Counter* _inverted_index_query_timer = nullptr;
    RuntimeProfile::Counter* _inverted_index_query_bitmap_copy_timer = nullptr;
    RuntimeProfile::Counter* _inverted_index_searcher_open_timer = nullptr;
    RuntimeProfile::Counter* _inverted_index_searcher_search_timer = nullptr;
    RuntimeProfile::Counter* _inverted_index_searcher_search_init_timer = nullptr;
    RuntimeProfile::Counter* _inverted_index_searcher_search_exec_timer = nullptr;
    RuntimeProfile::Counter* _inverted_index_searcher_cache_hit_counter = nullptr;
    RuntimeProfile::Counter* _inverted_index_searcher_cache_miss_counter = nullptr;
    RuntimeProfile::Counter* _inverted_index_downgrade_count_counter = nullptr;
    RuntimeProfile::Counter* _inverted_index_analyzer_timer = nullptr;
    RuntimeProfile::Counter* _inverted_index_lookup_timer = nullptr;

    RuntimeProfile::Counter* _ann_topn_filter_counter = nullptr;
    // topn_search_costs = index_load_costs + engine_search_costs + pre_process_costs + post_process_costs
    RuntimeProfile::Counter* _ann_topn_search_costs = nullptr;
    RuntimeProfile::Counter* _ann_topn_search_cnt = nullptr;
    RuntimeProfile::Counter* _ann_cache_hit_cnt = nullptr;
    RuntimeProfile::Counter* _ann_range_cache_hit_cnt = nullptr;

    RuntimeProfile::Counter* _ann_index_load_costs = nullptr;
    RuntimeProfile::Counter* _ann_ivf_on_disk_load_costs = nullptr;
    RuntimeProfile::Counter* _ann_ivf_on_disk_cache_hit_cnt = nullptr;
    RuntimeProfile::Counter* _ann_ivf_on_disk_cache_miss_cnt = nullptr;
    RuntimeProfile::Counter* _ann_topn_pre_process_costs = nullptr;
    RuntimeProfile::Counter* _ann_topn_engine_search_costs = nullptr;
    RuntimeProfile::Counter* _ann_topn_post_process_costs = nullptr;
    // post_process_costs = engine_convert_costs + result_convert_costs
    RuntimeProfile::Counter* _ann_topn_engine_convert_costs = nullptr;
    RuntimeProfile::Counter* _ann_topn_result_convert_costs = nullptr;

    RuntimeProfile::Counter* _ann_range_search_filter_counter = nullptr;
    // range_Search_costs = index_load_costs + engine_search_costs + pre_process_costs + post_process_costs
    RuntimeProfile::Counter* _ann_range_search_costs = nullptr;
    RuntimeProfile::Counter* _ann_range_search_cnt = nullptr;

    RuntimeProfile::Counter* _ann_range_pre_process_costs = nullptr;
    RuntimeProfile::Counter* _ann_range_engine_search_costs = nullptr;
    RuntimeProfile::Counter* _ann_range_post_process_costs = nullptr;

    RuntimeProfile::Counter* _ann_range_engine_convert_costs = nullptr;
    RuntimeProfile::Counter* _ann_range_result_convert_costs = nullptr;

    RuntimeProfile::Counter* _ann_fallback_brute_force_cnt = nullptr;
    RuntimeProfile::Counter* _ann_topn_fallback_by_small_candidate_cnt = nullptr;
    RuntimeProfile::Counter* _ann_topn_fallback_small_candidate_rows = nullptr;
    RuntimeProfile::Counter* _ann_range_fallback_by_small_candidate_cnt = nullptr;
    RuntimeProfile::Counter* _ann_range_fallback_small_candidate_rows = nullptr;

    RuntimeProfile::Counter* _output_index_result_column_timer = nullptr;

    // number of segment filtered by column stat when creating seg iterator
    RuntimeProfile::Counter* _filtered_segment_counter = nullptr;
    // total number of segment related to this scan node
    RuntimeProfile::Counter* _total_segment_counter = nullptr;

    // timer about tablet reader
    RuntimeProfile::Counter* _tablet_reader_init_timer = nullptr;
    RuntimeProfile::Counter* _tablet_reader_capture_rs_readers_timer = nullptr;
    RuntimeProfile::Counter* _tablet_reader_init_return_columns_timer = nullptr;
    RuntimeProfile::Counter* _tablet_reader_init_keys_param_timer = nullptr;
    RuntimeProfile::Counter* _tablet_reader_init_orderby_keys_param_timer = nullptr;
    RuntimeProfile::Counter* _tablet_reader_init_conditions_param_timer = nullptr;
    RuntimeProfile::Counter* _tablet_reader_init_delete_condition_param_timer = nullptr;

    // timer about block reader
    RuntimeProfile::Counter* _block_reader_vcollect_iter_init_timer = nullptr;
    RuntimeProfile::Counter* _block_reader_rs_readers_init_timer = nullptr;
    RuntimeProfile::Counter* _block_reader_build_heap_init_timer = nullptr;

    RuntimeProfile::Counter* _rowset_reader_get_segment_iterators_timer = nullptr;
    RuntimeProfile::Counter* _rowset_reader_create_iterators_timer = nullptr;
    RuntimeProfile::Counter* _rowset_reader_init_iterators_timer = nullptr;
    RuntimeProfile::Counter* _rowset_reader_load_segments_timer = nullptr;

    RuntimeProfile::Counter* _segment_iterator_init_timer = nullptr;
    RuntimeProfile::Counter* _segment_iterator_init_return_column_iterators_timer = nullptr;
    RuntimeProfile::Counter* _segment_iterator_init_index_iterators_timer = nullptr;
    RuntimeProfile::Counter* _segment_iterator_init_segment_prefetchers_timer = nullptr;

    RuntimeProfile::Counter* _segment_create_column_readers_timer = nullptr;
    RuntimeProfile::Counter* _segment_load_index_timer = nullptr;

    // total uncompressed bytes read when scanning sparse columns in variant
    RuntimeProfile::Counter* _variant_scan_sparse_column_bytes = nullptr;

    // total time spent scanning sparse subcolumns
    RuntimeProfile::Counter* _variant_scan_sparse_column_timer = nullptr;
    // time to build/resolve subcolumn paths from the sparse column
    RuntimeProfile::Counter* _variant_fill_path_from_sparse_column_timer = nullptr;
    // Variant subtree: times falling back to default iterator due to missing path
    RuntimeProfile::Counter* _variant_subtree_default_iter_count = nullptr;
    // Variant subtree: times selecting leaf iterator (target subcolumn is a leaf)
    RuntimeProfile::Counter* _variant_subtree_leaf_iter_count = nullptr;
    // Variant subtree: times selecting hierarchical iterator (node has children and sparse columns)
    RuntimeProfile::Counter* _variant_subtree_hierarchical_iter_count = nullptr;
    // Variant subtree: times selecting sparse iterator (iterate over sparse subcolumn)
    RuntimeProfile::Counter* _variant_subtree_sparse_iter_count = nullptr;
    // Variant subtree: times selecting doc snapshot all iterator (merge doc snapshot into root)
    RuntimeProfile::Counter* _variant_doc_value_column_iter_count = nullptr;

    RuntimeProfile::Counter* _adaptive_batch_predict_min_rows_counter = nullptr;
    RuntimeProfile::Counter* _adaptive_batch_predict_max_rows_counter = nullptr;
    // 解析并构建后的Tablet及其版本号集合。
    std::vector<TabletWithVersion> _tablets;
    // 存储引擎层读取的数据源描述（封装了 Tablet、Rowset Reader 等）。
    std::vector<TabletReadSource> _read_sources;

    // The per-instance query cache decision shared with the cache source
    // operator of the same fragment. Null when the query cache is disabled.
    // HIT: leave _scan_ranges empty so nothing is scanned; INCREMENTAL: scan
    // only the pre-captured delta read sources in (cached, current] version.
    // Query Cache 判定结果共享决策对象（区分全中 HIT、增量读 INCREMENTAL 或 未命中）。
    std::shared_ptr<QueryCacheInstanceDecision> _query_cache_decision;
    // 虚拟列/隐藏列（如 __DORIS_ROW_STORE_COL__ 或 Variant 字段路径）对应的表达表达式上下文映射。
    std::map<SlotId, VExprContextSPtr> _slot_id_to_virtual_column_expr;

    // ---- Runtime-filter partition pruning ----
    // Attaches this per-instance pruner to the shared parse result owned by
    // OlapScanOperatorX (parsed once in OperatorX::prepare()). Cheap: pointer
    // assignment plus a counter set, no parsing work.
    void _attach_partition_boundaries();

    RuntimeProfile::Counter* _tablets_pruned_by_rf_counter = nullptr;
};
// OlapScanOperatorX 是专门用于扫描和读取 Doris 本地 OLAP 存储引擎（即 Segment 文件、Tablet 数据）的具体扫描算子。它继承自 ScanOperatorX<OlapScanLocalState>，且被声明为 final（不可再被继承）
// 作为针对 Doris 自研 OLAP 存储引擎的叶子节点算子，OlapScanOperatorX 承担了以下核心职责：
// 对接 Doris OLAP 存储元数据：解析并持有从 FE（Frontend）下发的针对 OLAP 表特有的扫描节点元数据结构（TOlapScanNode），如 Tablet 列表、读取的 Version、Key 范围等。
// 管理 TabletSchema 列映射：持有并管理表的物理 Schema 元数据（TabletSchemaSPtr），用于将上层 SQL 逻辑列名（Column Name）精准映射为存储引擎内部的列物理索引 ID（Field Index），以支持下推谓词与索引过滤。
// 支持 Query Cache 缓存机制：持有 Query Cache 相关的配置与运行时控制对象（TQueryCacheParam 和 QueryCacheRuntime），与同一个 Fragment 内的 Cache Source 算子协同共享缓存决策，以实现查询结果/中间 Block 的复用。
// 准备扫描上下文（Pipeline Preparation）：重写 prepare 生命周期方法，在算子真正执行前对 OLAP 引擎专用的谓词下推、引擎配置及 Schema 进行校验和预处理。
class OlapScanOperatorX final : public ScanOperatorX<OlapScanLocalState> {
public:
    OlapScanOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                      const DescriptorTbl& descs, int parallel_tasks,
                      const TQueryCacheParam& cache_param,
                      std::shared_ptr<QueryCacheRuntime> query_cache_runtime = nullptr);

    Status prepare(RuntimeState* state) override;

    int get_column_id(const std::string& col_name) const override {
        if (!_tablet_schema) {
            return -1;
        }
        const auto& column = *DORIS_TRY(_tablet_schema->column(col_name));
        return _tablet_schema->field_index(column.unique_id());
    }

private:
    friend class OlapScanLocalState;
    // 储存 FE 下发的 OLAP 扫描节点 Thrift 结构体。
    // 包含了该 OLAP 表扫描所需的所有关键元数据，例如涉及的 key_ranges（查询主键区间）、olap_filter（存储层过滤条件）、is_preaggregation（是否开启前置聚合）、Tablet 分布信息以及 Schema Version 等。
    TOlapScanNode _olap_scan_node;
    // 存储 Query Cache（查询缓存）的参数配置。
    // 包含当前查询是否使能 Query Cache、缓存 Key、缓存切片/TTL 策略等控制信息。
    TQueryCacheParam _cache_param;
    // Shared with the cache source operator of the same fragment so both
    // consume the same per-instance cache decision (see QueryCacheRuntime).
    // Null when the query cache is disabled.
    // 控制 Query Cache 的运行时状态共享对象。
    // 与同一个 Fragment 中的 Cache Source 算子共享。通过该对象，OlapScanOperatorX 和 Cache 算子可以在同一个并发实例（Per-instance）内部使用相同的缓存命中判断决策（若缓存命中则直接读取缓存 Block，避开存储层 I/O 扫描；若未命中则回退到真实 OLAP 存储扫描并写回缓存）。当 Query Cache 被禁用时，该指针为 nullptr
    std::shared_ptr<QueryCacheRuntime> _query_cache_runtime;
    // 指向当前扫描表物理元数据 Schema（TabletSchema）的共享指针。
    // 描述了 Segment 文件中的物理列定义，包括每一列的 Unique ID、Column Name、数据类型、编码格式、索引信息等。用于在算子层将列名快速转化为存储层所需的 Field Index。
    TabletSchemaSPtr _tablet_schema;
};

} // namespace doris
