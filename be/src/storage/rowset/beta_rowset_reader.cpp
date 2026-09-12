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

#include "storage/rowset/beta_rowset_reader.h"

#include <stddef.h>

#include <algorithm>
#include <memory>
#include <ostream>
#include <roaring/roaring.hh>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>

#include "common/logging.h"
#include "common/status.h"
#include "core/block/block.h"
#include "core/data_type/data_type_number.h"
#include "io/io_common.h"
#include "runtime/descriptors.h"
#include "runtime/query_context.h"
#include "runtime/runtime_profile.h"
#include "storage/binlog.h"
#include "storage/delete/delete_handler.h"
#include "storage/iterator/vgeneric_iterators.h"
#include "storage/olap_define.h"
#include "storage/predicate/block_column_predicate.h"
#include "storage/predicate/column_predicate.h"
#include "storage/predicate/predicate_creator.h"
#include "storage/row_cursor.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/rowset/rowset_reader_context.h"
#include "storage/schema.h"
#include "storage/segment/lazy_init_segment_iterator.h"
#include "storage/segment/segment.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {
using namespace ErrorCode;

BetaRowsetReader::BetaRowsetReader(BetaRowsetSharedPtr rowset)
        : _read_context(nullptr), _rowset(std::move(rowset)), _stats(&_owned_stats) {
    _rowset->acquire();
}

void BetaRowsetReader::reset_read_options() {
    _read_options.delete_condition_predicates = AndBlockColumnPredicate::create_shared();
    _read_options.column_predicates.clear();
    _read_options.col_id_to_predicates.clear();
    _read_options.del_predicates_for_zone_map.clear();
    _read_options.key_ranges.clear();
}

RowsetReaderSharedPtr BetaRowsetReader::clone() {
    return RowsetReaderSharedPtr(new BetaRowsetReader(_rowset));
}

void BetaRowsetReader::update_profile(RuntimeProfile* profile) {
    if (_iterator != nullptr) {
        _iterator->update_profile(profile);
    }
}

// 根据传入的读取上下文，将上层的查询/Compaction 请求转化为控制每个 Segment 文件粒度的底层迭代器（LazyInitSegmentIterator），
// 并完成索引过滤、删除位图处理、TSO 谓词推导等各种读取选项（StorageReadOptions）的准备。
// RowsetReaderContext* read_context（输入参数） 读取上下文指针。包含来自上层（如 VScanNode 或 Compaction 任务）传入的所有读取控制参数，包括：需要读取的列（return_columns）、下推的下界/上界 Key 范围、下推的谓词（predicates）、Delete Bitmap 删除位图、Runtime State 以及 IO 控制选项等。
// std::vector<RowwiseIteratorUPtr>* out_iters  （输出参数）迭代器输出容器。函数会在内部创建每个 Segment 对应的 LazyInitSegmentIterator，并将其智能指针追加存入该 vector 中，供外层的 MergeIterator 或 UnionIterator 消费。
// bool use_cache（输入参数） Segment 缓存开关。指示在加载 Segment 元数据或数据时是否优先使用 Segment Cache。
Status BetaRowsetReader::get_segment_iterators(RowsetReaderContext* read_context,
                                               std::vector<RowwiseIteratorUPtr>* out_iters,
                                               bool use_cache) {
	// 保存上下文指针至成员变量。
    _read_context = read_context;
    // The segment iterator is created with its own statistics,
    // and the member variable '_stats'  is initialized by '_stats(&owned_stats)'.
    // The choice of statistics used depends on the workload of the rowset reader.
    // For instance, if it's for query, the get_segment_iterators function
    // will receive one valid read_context with corresponding valid statistics,
    // and we will use those statistics.
    // However, for compaction or schema change workloads,
    // the read_context passed to the function will have null statistics,
    // and in such cases we will try to use the beta rowset reader's own statistics.
    // 如果 read_context 携带了统计指标对象（通常是查询 Query 传进来的），则使用该对象记录 Profile
    // 如果是 Compaction 或 Schema Change 任务（stats 为空），则保留使用 RowsetReader 自带的成员变量统计。
    if (_read_context->stats != nullptr) {
        _stats = _read_context->stats;
    }
    // 记录当前函数执行消耗的纳秒数，用于性能 Profiling。
    SCOPED_RAW_TIMER(&_stats->rowset_reader_get_segment_iterators_timer_ns);
    // 确保当前 Rowset 的元数据（Meta）已被完全加载到内存中；若加载失败直接退出。
    RETURN_IF_ERROR(_rowset->load());

    // convert RowsetReaderContext to StorageReadOptions
    // 构造基础读取选项 StorageReadOptions
    // 将 read_context 中的批量大小、内存限制、预过滤下推表达式、索引访问路径（如倒排索引 AccessPath）、
    // 向量检索（ANN TopN）、Rowset ID/Version 等元数据转换填充到底层 Segment 读选项 _read_options 中。
    // 单次 next_batch 预期望拉取的行数（默认 1024 行）。
    _read_options.block_row_max = read_context->batch_size;
    // 自适应 Block 内存预算大小（默认 8MB）
    // 配合 batch_size 动态调整单批次返回的真实行数，防止大宽表导致内存爆炸。
    _read_options.preferred_block_size_bytes = read_context->preferred_block_size_bytes;
    _read_options.stats = _stats;
    // 下推的聚合操作类型（如 COUNT、MIN、MAX 等）。当整个 Segment 满足全表 Count 或索引聚合时，可直接读取元数据返回而无需逐行扫描。
    _read_options.push_down_agg_type_opt = _read_context->push_down_agg_type_opt;
    // 下推到存储层的复杂通用向量化表达式（VExpr）上下文列表，用于短路过滤或高级计算。
    _read_options.common_expr_ctxs_push_down = _read_context->common_expr_ctxs_push_down;
    _read_options.virtual_column_exprs = _read_context->virtual_column_exprs;

    _read_options.all_access_paths = _read_context->all_access_paths;
    _read_options.predicate_access_paths = _read_context->predicate_access_paths;

    _read_options.ann_topn_runtime = _read_context->ann_topn_runtime;
    _read_options.score_runtime = _read_context->score_runtime;
    _read_options.collection_statistics = _read_context->collection_statistics;
    _read_options.rowset_id = _rowset->rowset_id();
    _read_options.version = _rowset->version();
    _read_options.commit_tso = _rowset->rowset_meta()->commit_tso();
    _read_options.tablet_id = _rowset->rowset_meta()->tablet_id();
    _read_options.read_limit = _topn_limit;
    // 处理按 Key 查询的物理范围（Key Ranges）
    // 如果查询包含主键范围过滤（比如点查或 Range 查询），将传入的上限/下限 Key（及其开闭区间标志）封装成 key_ranges，后续利用 Prefix Index 进行 Segment 快速定位。
    // lower_bound_keys 主键/前缀索引查询的下界 Key 列表（起点）。
    if (_read_context->lower_bound_keys != nullptr) {
        for (int i = 0; i < _read_context->lower_bound_keys->size(); ++i) {
            _read_options.key_ranges.emplace_back(&_read_context->lower_bound_keys->at(i),
                                                  _read_context->is_lower_keys_included->at(i),
                                                  &_read_context->upper_bound_keys->at(i),
                                                  _read_context->is_upper_keys_included->at(i));
        }
    }

    // delete_hanlder is always set, but it maybe not init, so that it will return empty conditions
    // or predicates when it is not inited.
    // 处理 Delete Handler 关联的删除条件
    // 从 delete_handler 中获取版本号晚于当前 Rowset end_version() 的历史 Delete 语句谓词（即旧版本数据的删除条件）
    // 将它们注入到 delete_condition_predicates 中，并提取用于 ZoneMap 索引过滤的谓词。
    if (_read_context->delete_handler != nullptr) {
        _read_context->delete_handler->get_delete_conditions_after_version(
                _rowset->end_version(), _read_options.delete_condition_predicates.get(),
                &_read_options.del_predicates_for_zone_map);
    }
    // 补充物理读取列与处理 Schema
    std::vector<uint32_t> read_columns;
    std::set<uint32_t> read_columns_set;
    std::set<uint32_t> delete_columns_set;
    // 首先将 SQL 要求的返回列（return_columns）放入。
    // return_columns 列投影集合。指针指向存储引擎真正需要返回给上层的列 Column ID 列表（非列表中的列尽量不进行磁盘 IO）。
    for (int i = 0; i < _read_context->return_columns->size(); ++i) {
        read_columns.push_back(_read_context->return_columns->at(i));
        read_columns_set.insert(_read_context->return_columns->at(i));
    }
    // 重要拓展：Delete 条件中用到的列可能不在 SQL 返回列中，但过滤时必须读取，因此需要把删除条件用到的列加入 read_columns。
    _read_options.delete_condition_predicates->get_all_column_ids(delete_columns_set);
    for (auto cid : delete_columns_set) {
        if (read_columns_set.find(cid) == read_columns_set.end()) {
            read_columns.push_back(cid);
        }
    }
    // 如果指定了 TSO（时间戳）谓词过滤列且不在返回列中，也一并加入。
    if (_read_context->tso_predicate_column_id.has_value() &&
        read_columns_set.find(*_read_context->tso_predicate_column_id) == read_columns_set.end()) {
        read_columns.push_back(*_read_context->tso_predicate_column_id);
    }
    // disable condition cache if you have delete condition or forced pushed tso predicate
    // 如果存在 Delete 动态条件或强下推的 TSO 谓词，为了避免过滤条件冲突导致缓存数据错乱，将条件缓存摘要（condition_cache_digest）清零（禁用 Cache）。
    _read_context->condition_cache_digest =
            (delete_columns_set.empty() && !_read_context->tso_predicate_column_id.has_value())
                    ? _read_context->condition_cache_digest
                    : 0;
    // create segment iterators
    VLOG_NOTICE << "read columns size: " << read_columns.size();
    // 真正从存储文件磁盘读取数据时的 Schema（包含返回列 + Delete 列 + TSO 列等额外物理过滤列）
    _input_schema = std::make_shared<Schema>(_read_context->tablet_schema->columns(), read_columns);
    _read_options.extra_columns = _read_context->extra_columns;
    // output_schema only contains return_columns (excludes extra columns like delete-predicate
    // columns and the TSO predicate-only column).
    // It is used by merge/union iterators to determine how many columns to copy to the output block.
    // 最终投射给上层执行引擎的 Block Schema（仅包含用户请求的 return_columns）。
    _output_schema = std::make_shared<Schema>(_read_context->tablet_schema->columns(),
                                              *(_read_context->return_columns));

    // 将上层传入的列谓词（Column Predicates）解析并组织到底层读取选项（StorageReadOptions）中，为后续 Segment 层的索引过滤和向量化数据裁剪提供基础
    // 另一方面按 column_id 分组整理到 col_id_to_predicates Map 中，构建 AND 逻辑链，供向量化 BlockPredicate 进行快速行裁剪。
    // 校验上下文中的谓词指针是否有效。
    if (_read_context->predicates != nullptr) {
        // 将 _read_context->predicates 中的所有谓词指针一维平铺（Vector Append）追加到 _read_options.column_predicates 数组的末尾。
        // 主要用途：供 Segment 级别的整体索引（如 ZoneMap 索引、BloomFilter 索引、Bitmap 索引、倒排索引）进行粗粒度的 Block/Segment 级文件裁剪评估。
        _read_options.column_predicates.insert(_read_options.column_predicates.end(),
                                               _read_context->predicates->begin(),
                                               _read_context->predicates->end());
        // 按列分组并构造向量化 Block 谓词树
        for (auto pred : *(_read_context->predicates)) {
            //  步骤 A: 如果当前列 ID 还没有对应的复合谓词容器，先初始化创建一个 AndBlockColumnPredicate
            if (_read_options.col_id_to_predicates.count(pred->column_id()) < 1) {
                _read_options.col_id_to_predicates.insert(
                        {pred->column_id(), AndBlockColumnPredicate::create_shared()});
            }
            // 步骤 B: 将单列谓词包装成 SingleColumnBlockPredicate，并加入到对应列的 AND 逻辑链中
            _read_options.col_id_to_predicates[pred->column_id()]->add_column_predicate(
                    SingleColumnBlockPredicate::create_unique(pred));
        }
    }

    // Forced TSO range pushdown for binlog/snapshot incremental read. Build the (start_tso,
    // end_tso] comparison predicates here and inject them directly, so they always reach the
    // SegmentIterator for row-level filtering instead of going through the value/key predicate
    // split in TabletReader::_init_conditions_param (where they may be dropped). This is a
    // correctness requirement for MIN_DELTA, which groups consecutive same-key rows and would
    // produce wrong results if out-of-range rows leaked into a group.
    // 主要作用是为增量读取（Incremental Read）或 Binlog / 快照查询场景，强行构建基于 TSO（Timestamp Oracle / 时间戳）的范围过滤谓词，并下推到底层 _read_options 中。
    // 在 Apache Doris 的 Binlog、数据变更追踪（CDC）或基于时间戳的增量/快照查询 场景中，存储引擎需要按版本范围筛选数据。
    // 为了避免将不在目标时间戳区间的数据透传到上层（造成不必要的解压、归并和 CPU 消耗），Doris 在最底层的 SegmentIterator 阶段直接构造开闭区间为 (start_tso, end_tso] 的物理列谓词：
    // 条件触发：必须指定了 TSO 所在的列 ID（tso_predicate_column_id），且至少指定了起始时间戳（start_tso）或结束时间戳（end_tso）中的一个。
    if (_read_context->tso_predicate_column_id.has_value() &&
        (_read_context->start_tso.has_value() || _read_context->end_tso.has_value())) {
        // 参数提取：获取 TSO 对应的物理列 ID（tso_cid）、设置数据类型为 Int64/BIGINT，并从 tablet_schema 中获取对应的列名（如 __DORIS_DELETE_SIGN__ 或 Binlog TSO 列）。
        ColumnId tso_cid = *_read_context->tso_predicate_column_id;
        auto tso_data_type = std::make_shared<DataTypeInt64>();
        const std::string& tso_col_name = _read_context->tablet_schema->column(tso_cid).name();
        // Lambda 帮助函数：同时更新两种谓词数据结构
        // 确保将构建出来的 TSO 谓词同步更新到底层读取所需的两个结构中：
        // col_id_to_predicates：按列 ID 分组的向量化 Block 谓词树（用于延迟物化和按列向量化过滤）。
        // column_predicates：平铺的谓词数组（用于 ZoneMap 索引、Page 级过滤等）。
        auto add_tso_predicate = [&](std::shared_ptr<ColumnPredicate> pred) {
            if (_read_options.col_id_to_predicates.count(pred->column_id()) < 1) {
                _read_options.col_id_to_predicates.insert(
                        {pred->column_id(), AndBlockColumnPredicate::create_shared()});
            }
            _read_options.col_id_to_predicates[pred->column_id()]->add_column_predicate(
                    SingleColumnBlockPredicate::create_unique(pred));
            _read_options.column_predicates.push_back(std::move(pred));
        };
        // 构造范围谓词 (start_tso, end_tso]
        // 构造 TSO_COL > start_tso（GT 为 Greater Than），过滤掉早于或等于 start_tso 的历史变更。
        if (_read_context->start_tso.has_value()) {
            add_tso_predicate(create_comparison_predicate<PredicateType::GT>(
                    tso_cid, tso_col_name, tso_data_type,
                    Field::create_field<TYPE_BIGINT>(*_read_context->start_tso), false));
        }
        // end_tso 处理：构造 TSO_COL <= end_tso（LE 为 Less Equal），过滤掉晚于 end_tso 的未提交/未来变更。
        if (_read_context->end_tso.has_value()) {
            add_tso_predicate(create_comparison_predicate<PredicateType::LE>(
                    tso_cid, tso_col_name, tso_data_type,
                    Field::create_field<TYPE_BIGINT>(*_read_context->end_tso), false));
        }
    }

    // Take a delete-bitmap for each segment, the bitmap contains all deletes
    // until the max read version, which is read_context->version.second
    // 在 Unique Key 模型（Merge-on-Write 写时合并）下，为当前 Rowset 的每个 Segment 加载并聚合截至当前读取版本（Max Version）的所有删除标记（Delete Bitmap），并注入到底层读取选项中。
    // 在 Apache Doris 的 Unique Key 主键写时合并（Merge-on-Write, MoW） 模式下：
    // 写时标记删除：当写入或更新一条主键相同的记录时，存储引擎不会直接修改原数据，而是会在 DeleteBitmap 中把旧版本数据对应的物理行号（RowID）标记为“已删除”。
    // 读时直接跳过：在读取数据时，SegmentIterator 会拿当前 Segment 对应的 DeleteBitmap，在读取数据 Page 时直接跳过这些已被标记删除的 RowID，从而避免了传统 Merge-on-Read 模式下的多路归并排序，大幅提升读取性能。
    // 判断当前查询/任务上下文是否带有 DeleteBitmap 索引。非 Unique Key 模型或非 MoW 模式下，该指针通常为空。
    if (_read_context->delete_bitmap != nullptr) {
        {
            // 记录从 DeleteBitmap 查找和合并 Bitmap 所消耗的时间（纳秒），用于 Query Profile 中的性能分析。
            SCOPED_RAW_TIMER(&_stats->delete_bitmap_get_agg_ns);
            RowsetId rowset_id = rowset()->rowset_id();
            // 遍历 Rowset 下的所有 Segment 并聚合 DeleteBitmap
            // rowset_id & seg_id：唯一定位当前 Rowset 下的具体 Segment 文件。
            for (uint32_t seg_id = 0; seg_id < rowset()->num_segments(); ++seg_id) {
                // 根据 key 三元组 {rowset_id, seg_id, max_version} 获取聚合后的 Bitmap。
                auto d = _read_context->delete_bitmap->get_agg(
                        {rowset_id, seg_id, _read_context->version.second});
                // 如果该 Segment 在此版本下没有被删除的行，则直接跳过，节省内存和后续计算。
                if (d->isEmpty()) {
                    continue; // Empty delete bitmap for the segment
                }
                VLOG_TRACE << "Get the delete bitmap for rowset: " << rowset_id.to_string()
                           << ", segment id:" << seg_id << ", size:" << d->cardinality();
                // 将该 Segment 专属的 DeleteBitmap（以 seg_id 为 Key）存入 StorageReadOptions 中。
                _read_options.delete_bitmap.emplace(seg_id, std::move(d));
            }
        }
    }
    // 在满足特定模型约束的前提下，尝试将 Value 列（非 Key 列/指标列）的过滤谓词下推（Push-down）到存储引擎的底层，从而在读取 Segment 数据时提前过滤掉不满足条件的行。
    // 在 Doris 存储引擎中，数据列分为 Key 列（维度列） 和 Value 列（指标列/非主键列）：
    // Key 列谓词：天然可以下推，利用 Prefix Index（前缀索引）、ZoneMap 等快速定位数据。
    // Value 列谓词：在 Aggregate / Unique (Merge-on-Read) / Duplicate 等不同数据模型下，Value 列的处理机制不同。
    // 例如在聚合表中，只有在多路归并聚合之后才能确定最终的 Value 值，因此某些情况下不能提前在 Segment 粒度下推 Value 谓词，否则会导致多版本聚合结果出错。
    // 判断当前的数据模型和查询类型是否允许下推 Value 谓词
    // 明细模型（Duplicate Key）：数据无聚合，Value 谓词总是可以安全下推。
    // 主键写时合并（Unique Key MoW）：数据已在写入时去重， Value 谓词可以安全下推。
    // 聚合模型 / 读时合并（Merge-on-Read）：通常不允许下推 Value 谓词（除非开启了某些特定优化），防止未聚合前的数据行被错误过滤。
    if (_should_push_down_value_predicates()) {
        // sequence mapping currently only support merge on read, so can not push down value predicates
        // Sequence Column（顺序列/序列映射）：用于 Unique Key 表中指定按哪一列（如 sequence_col 或 updated_time）来决定相同 Key 的覆盖顺序。
        // 限制原因：目前 Sequence 逻辑主要依赖读时合并（Merge-on-Read）来判定最新版本。如果下推了 Value 谓词，可能会在归并之前把某个 Key 的“旧版本”过滤掉，导致该 Key 的最新版本在对比时缺失，进而算错最新值。因此如果表 Schema 包含 seq_map，强制禁止下推 Value 谓词。
        if (_read_context->value_predicates != nullptr &&
            !read_context->tablet_schema->has_seq_map()) {
            // 将 Value 谓词注入到底层读取选项
            _read_options.column_predicates.insert(_read_options.column_predicates.end(),
                                                   _read_context->value_predicates->begin(),
                                                   _read_context->value_predicates->end());
            // 按 Column ID 分组的向量化 Block 谓词树
            for (auto pred : *(_read_context->value_predicates)) {
                if (_read_options.col_id_to_predicates.count(pred->column_id()) < 1) {
                    // 按 column_id 归类并创建 AndBlockColumnPredicate
                    _read_options.col_id_to_predicates.insert(
                            {pred->column_id(), AndBlockColumnPredicate::create_shared()});
                }
                _read_options.col_id_to_predicates[pred->column_id()]->add_column_predicate(
                        SingleColumnBlockPredicate::create_unique(pred));
            }
        }
    }
    _read_options.use_page_cache = _read_context->use_page_cache;
    _read_options.tablet_schema = _read_context->tablet_schema;
    _read_options.enable_unique_key_merge_on_write =
            _read_context->enable_unique_key_merge_on_write;
    _read_options.record_rowids = _read_context->record_rowids;
    _read_options.topn_filter_source_node_ids = _read_context->topn_filter_source_node_ids;
    _read_options.topn_filter_target_node_id = _read_context->topn_filter_target_node_id;
    _read_options.read_orderby_key_reverse = _read_context->read_orderby_key_reverse;
    _read_options.use_insert_order_when_same = _read_context->use_insert_order_when_same;
    _read_options.read_row_binlog = _read_context->read_row_binlog;
    // 定位 Binlog TSO（时间戳）物理列在用户请求返回列（return_columns）中的相对索引位置，并将该索引记录到读取选项（_read_options.binlog_tso_idx）中。
    // 每条变更数据（Row）都会包含一个隐式或显式的 TSO（Timestamp Oracle）列，用于标识该条 Binlog 产生的全局时间戳。
    int32_t tso_col_id = _read_context->tablet_schema->binlog_tso_col_idx();
    if (tso_col_id >= 0) {
        for (size_t i = 0; i < _read_context->return_columns->size(); ++i) {
            if (_read_context->return_columns->at(i) == static_cast<uint32_t>(tso_col_id)) {
                _read_options.binlog_tso_idx = static_cast<int>(i);
                break;
            }
        }
    }
    _read_options.read_orderby_key_columns = _read_context->read_orderby_key_columns;
    _read_options.io_ctx.reader_type = _read_context->reader_type;
    _read_options.io_ctx.file_cache_stats = &_stats->file_cache_stats;
    _read_options.runtime_state = _read_context->runtime_state;
    _read_options.output_columns = _read_context->output_columns;
    _read_options.io_ctx.reader_type = _read_context->reader_type;
    _read_options.io_ctx.is_disposable = _read_context->reader_type != ReaderType::READER_QUERY;
    _read_options.target_cast_type_for_variants = _read_context->target_cast_type_for_variants;
    // 基于 RuntimeState 的高级 File Cache 与限速控制
    if (_read_context->runtime_state != nullptr) {
        // 绑定当前查询的 Unique ID，便于缓存日志追踪与资源隔离
        _read_options.io_ctx.query_id = &_read_context->runtime_state->query_id();
        // 根据 Session 变量 enable_file_cache 决定本次读取是否走本地文件缓存。
        _read_options.io_ctx.read_file_cache =
                _read_context->runtime_state->query_options().enable_file_cache;
        _read_options.io_ctx.is_disposable =
                _read_context->runtime_state->query_options().disable_file_cache;
        // 存算分离/远程 Scan 的 Cache 写入限流（Write Limiter）
        // 背景：在存算分离（Cloud-Native / Shared Storage）架构下，从 S3/HDFS 远程拉取数据时，如果并发很高且全部写入本地 Disk Cache，极易引发本地磁盘 I/O 堵塞
        // 机制：仅对用户查询（READER_QUERY），从全局 QueryContext 获取 remote_scan_cache_write_limiter 令牌桶/限流器，限制当前查询向本地磁盘写入远程 Cache 的吞吐速率（MB/s），防止单大查询拉垮整个 BE 节点的磁盘 I/O
        auto* query_ctx = _read_context->runtime_state->get_query_ctx();
        if (_read_context->reader_type == ReaderType::READER_QUERY && query_ctx != nullptr) {
            _read_options.io_ctx.remote_scan_cache_write_limiter =
                    query_ctx->remote_scan_cache_write_limiter();
        }
    }
    // 为存储层的条件缓存（Condition Cache / Filter Cache）计算并生成一个全局唯一的 Digest（摘要/哈希值）。
    // 通过将 数据主键查询范围（key_ranges） 的特征叠加融入到已有的查询条件摘要（_read_context->condition_cache_digest）中，确保生成的 Cache Key 能够精确唯一地标识 “特定的过滤条件 + 特定的数据主键扫描范围”。
    if (_read_context->condition_cache_digest) {
        for (const auto& key_range : _read_options.key_ranges) {
            _read_context->condition_cache_digest =
                    key_range.get_digest(_read_context->condition_cache_digest);
        }
        _read_options.condition_cache_digest = _read_context->condition_cache_digest;
    }

    _read_options.io_ctx.expiration_time = read_context->ttl_seconds;
    // 判定是否启用 Segment Cache 与确定扫描的 Segment 范围
    bool enable_segment_cache = true;
    auto* state = read_context->runtime_state;
    if (state != nullptr) {
        enable_segment_cache = state->query_options().__isset.enable_segment_cache
                                       ? state->query_options().enable_segment_cache
                                       : true;
    }
    // When reader type is for query, session variable `enable_segment_cache` should be respected.
    bool should_use_cache = use_cache || (_read_context->reader_type == ReaderType::READER_QUERY &&
                                          enable_segment_cache);

    // 处理 Rowset 中 Segment 的切分扫描（Split Scan / Parallel Scan）逻辑，确定当前 Reader 需要负责读取的 Segment 物理下标区间 [seg_start, seg_end)
    // 在 Doris 执行查询时，为了最大化并行度（Parallelism），一个大 Rowset 包含的多个 Segment 文件可能不会只由一个 Scanner 处理，而是会被切分成不同的 Segment 子区间（Split），分配给多个并发的 Scan/Read Task 执行：
    // 全量场景：如果不开启并发切分，或者该 Task 需要单线程读取整个 Rowset，上层传入的 _segment_offsets 通常会是一个默认的占位值（如 (0, 0)）。
    // 获取当前 Rowset 物理上包含的所有 Segment 文件数量（如 5 个）
    auto segment_count = _rowset->num_segments();
    // 解包上层指定的读取切分区间 [seg_start, seg_end)
    auto [seg_start, seg_end] = _segment_offsets;
    // If seg_start == seg_end, it means that the segments of a rowset is not
    // split scanned by multiple scanners, and the rowset reader is used to read the whole rowset.
    // 如果 seg_start == seg_end（例如默认的 (0, 0)），表示上层调度器没有对该 Rowset 进行多线程切分，或者要求当前 Rowset Reader 独立处理整块 Rowset。
    if (seg_start == seg_end) {
        seg_start = 0;
        seg_end = segment_count;
    }
    // 在 Apache Doris 的存储引擎中，数据行的物理地址由 {RowsetId, SegmentId, RowId} 唯一标识。
    // 当系统在后台执行 Compaction（数据合并/重排） 或 Schema Change 时，旧 Rowset 中的数据行会被读取并写入到新的 Rowset 中，其物理 RowID 会发生改变。
    // 如果此时有并发的删除操作（Delete）或者内存中存有指向旧 RowID 的 DeleteBitmap / 索引，就需要通过 RowidConversion 映射表把“旧 RowID”精准转换/重定向为“新 Rowset 中的新 RowID”。
    // 要建立这个映射关系，第一步就是必须获取当前 Rowset 下每个 Segment 的总行数（segment_rows），从而计算出物理行号的前缀偏移量（Offset）。
    // record_rowids：标识当前读取操作是否需要记录/追踪每一行的物理 RowID
    // rowid_conversion：指向 RowID 转换管理器的指针。只有当上层（如 Cumulate/Base Compaction Task）显式传入并开启了 RowID 转换功能时，才进入初始化逻辑。
    if (_read_context->record_rowids && _read_context->rowid_conversion) {
        // init segment rowid map for rowid conversion
        std::vector<uint32_t> segment_rows;
        RETURN_IF_ERROR(_rowset->get_segment_num_rows(&segment_rows, should_use_cache, _stats));
        RETURN_IF_ERROR(_read_context->rowid_conversion->init_segment_map(rowset()->rowset_id(),
                                                                          segment_rows));
    }
    // 按 Segment 逐个创建迭代器（Iterator），并应用延迟初始化（Lazy Initialization）和按 Segment 级的行范围（Row Ranges）裁剪，过滤掉空 Segment 后将有效迭代器放入结果集中。
    // 问题背景：如果一个 Rowset 包含几十个 Segment，且表非常宽（数百列），如果一次性为所有 Segment 创建并初始化 Iterator，会立即打开大量 Column Reader，加载大量 Index Page / Dictionary Page，产生很高的内存峰值。
    // 解决方案：使用 LazyInitSegmentIterator。创建时仅仅是一个轻量级占位符，只有真正 Iterator 到该 Segment 读数据时（Call init()），才会分配和加载具体的列读取资源，读完后即可释放，极大降低了宽表查询的内存占用。
    // 循环遍历指定的 Segment 范围
    for (int64_t i = seg_start; i < seg_end; i++) {
        SCOPED_RAW_TIMER(&_stats->rowset_reader_create_iterators_timer_ns);
        std::unique_ptr<RowwiseIterator> iter;

        /// For iterators, we don't need to initialize them all at once when creating them.
        /// Instead, we should initialize each iterator separately when really using them.
        /// This optimization minimizes the lifecycle of resources like column readers
        /// and prevents excessive memory consumption, especially for wide tables.
        // 分支一：没有行范围限制（全量 Segment 读取）
        // 场景：没有进行 Segment 内部的 RowID 过滤（如前缀索引过滤、Bitmap 索引过滤未产生精细 Row Range）。
        if (_segment_row_ranges.empty()) {
            _read_options.row_ranges.clear();
            iter = std::make_unique<LazyInitSegmentIterator>(_rowset, i, should_use_cache,
                                                             _input_schema, _read_options);
        } else {
        // 分支二：存在精细的行范围限制（Segment 内部剪枝）
        // _segment_row_ranges：上层通过索引过滤（如 Bitmap 索引、ZoneMap 索引、二级索引）算出的 每个 Segment 专属的待读取行号区间集合（RowRanges）
            DCHECK_EQ(seg_end - seg_start, _segment_row_ranges.size());
            // local_options：由于每个 Segment 对应的 row_ranges 不同，这里复制一份 _read_options 局部变量，只把第 i - seg_start 个 Segment 的 row_ranges 赋给它。
            auto local_options = _read_options;
            local_options.row_ranges = _segment_row_ranges[i - seg_start];
            if (local_options.condition_cache_digest) {
                local_options.condition_cache_digest =
                        local_options.row_ranges.get_digest(local_options.condition_cache_digest);
            }
            iter = std::make_unique<LazyInitSegmentIterator>(_rowset, i, should_use_cache,
                                                             _input_schema, local_options);
        }
	// 若通过索引预过滤判定该 Segment 必定无数据（例如 ZoneMap 匹配全过滤），则不加入 out_iters。
        if (iter->empty()) {
            continue;
        }
	// 将最终的 iter 存入 out_iters，返回 Status::OK() 完成整个 Segment 迭代器的创建阶段。
        out_iters->push_back(std::move(iter));
    }

    return Status::OK();
}

Status BetaRowsetReader::init(RowsetReaderContext* read_context, const RowSetSplits& rs_splits) {
    // 将上层（如 TabletReader 或查询引擎）传入的全局读取上下文保存到成员变量中。该上下文包含了谓词、Schema、PageCache 策略、合并规则等关键信息。
    _read_context = read_context;
    // 反向填充上下文。将当前 Reader 持有的物理 Rowset 的唯一 ID（rowset_id）回写到 read_context 中，
    // 供后续做日志追踪或针对该 Rowset 的专有处理（例如 Unique Key MoW 查找 DeleteBitmap）。
    _read_context->rowset_id = _rowset->rowset_id();
    // 记录并发切分信息。保存当前 Reader 负责读取的 Segment 文件索引区间 [start_seg_id, end_seg_id)。
    // 如果是 Pipeline 并行读取，不同的 Reader 实例会拿到不同的 Segment 偏移区间。
    _segment_offsets = rs_splits.segment_offsets;
    // 记录行号裁减信息。保存经过上层索引过滤后，各个 Segment 内需要读取的实际物理行号集合（RowRanges），避免全表扫描。
    _segment_row_ranges = rs_splits.segment_row_ranges;
    return Status::OK();
}

Status BetaRowsetReader::_init_iterator_once() {
    return _init_iter_once.call([this] { return _init_iterator(); });
}
// 根据当前的读取上下文（数据模型、排序要求、Sequence 列等），将一个 Rowset 内部包含的多个 Segment 迭代器（SegmentIterator）组装/包裹成一个统一的全局迭代器（MergeIterator 或 UnionIterator）。
Status BetaRowsetReader::_init_iterator() {
	// 根据读取上下文 _read_context，为当前 Rowset 下的每一个文件（Segment）分别创建一个物理读取迭代器（通常是 SegmentIterator），并存入 iterators 向量中。
    std::vector<RowwiseIteratorUPtr> iterators;
    RETURN_IF_ERROR(get_segment_iterators(_read_context, &iterators));
	// 记录初始化迭代器阶段所消耗的纳秒数，更新到系统 Profiler 的 rowset_reader_init_iterators_timer_ns 指标中
    SCOPED_RAW_TIMER(&_stats->rowset_reader_init_iterators_timer_ns);

    if (_read_context->merged_rows == nullptr) {
        _read_context->merged_rows = &_merged_rows;
    }
    // merge or union segment iterator
   // 分支一：组装多路归并迭代器（MergeIterator）
   // 通常在 Aggregate Key / Unique Key 模型，或者涉及跨 Segment 主键去重/有序合并时
    if (is_merge_iterator()) {
	// 如果表定义了 Sequence Column（用于在 Key 相同时判定哪个版本的数据更新）
	// 在返回列集合 return_columns 中遍历匹配 sequence_id_idx 的物理列偏移索引。
        auto sequence_loc = -1;
        if (_read_context->sequence_id_idx != -1) {
            for (int loc = 0; loc < _read_context->return_columns->size(); loc++) {
                if (_read_context->return_columns->at(loc) == _read_context->sequence_id_idx) {
                    sequence_loc = loc;
                    break;
                }
            }
        }
	// 如果启用了 Binlog 且存在 binlog_tso_idx（TSO 事务时间戳索引），强制使用 TSO 索引作为版本判定依据。
        if (_read_options.binlog_tso_idx != -1) {
            sequence_loc = _read_options.binlog_tso_idx;
        }
	// 将所有 iterators 移交（std::move）给 new_merge_iterator
	// 核心参数：传入 Sequence 列位置、是否为 Unique Key 模型、是否倒序读取（read_orderby_key_reverse）、合并行统计指针、输出 Schema，以及 Binlog 标志。
        _iterator = new_merge_iterator(std::move(iterators), sequence_loc, _read_context->is_unique,
                                       _read_context->read_orderby_key_reverse,
                                       _read_context->merged_rows, _output_schema,
                                       _read_options.binlog_tso_idx != -1);
    } else {
	// 分支二：组装拼接迭代器（UnionIterator）
	// 当不需要按 Key 进行版本合并时（通常在 Duplicate 模型，或者已被全量 Compaction 过的单 Segment/无需去重的场景）
	// 倒序适配：若外部查询指定了倒序输出（read_orderby_key_reverse），将 iterators 向量翻转，优先读取后面的 Segment 文件。
        if (_read_context->read_orderby_key_reverse) {
            // reverse iterators to read backward for ORDER BY key DESC
            std::reverse(iterators.begin(), iterators.end());
        }
        _iterator = new_union_iterator(std::move(iterators), _output_schema);
    }
    // 初始化最终迭代器并返回
    auto s = _iterator->init(_read_options);
    if (!s.ok()) {
        LOG(WARNING) << "failed to init iterator: " << s.to_string();
        _iterator.reset();
        return Status::Error<ROWSET_READER_INIT>(s.to_string());
    }
    return Status::OK();
}

bool BetaRowsetReader::_should_push_down_value_predicates() const {
    // if unique table with rowset [0-x] or [0-1] [2-y] [...],
    // value column predicates can be pushdown on rowset [0-x] or [2-y], [2-y]
    // must be compaction, not overlapping and don't have sequence column
    return _rowset->keys_type() == UNIQUE_KEYS &&
           (((_rowset->start_version() == 0 || _rowset->start_version() == 2) &&
             !_rowset->_rowset_meta->is_segments_overlapping() &&
             _read_context->sequence_id_idx == -1) ||
            _read_context->enable_unique_key_merge_on_write ||
            _read_context->enable_mor_value_predicate_pushdown);
}
} // namespace doris
