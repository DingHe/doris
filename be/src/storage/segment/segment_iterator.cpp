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

#include "storage/segment/segment_iterator.h"

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Opcodes_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <glog/logging.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <cassert>
#include <cstdint>
#include <memory>
#include <numeric>
#include <optional>
#include <set>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cloud/config.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/consts.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/metrics/doris_metrics.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_const.h"
#include "core/column/column_nothing.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_variant.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_number.h"
#include "core/data_type/define_primitive_type.h"
#include "core/field.h"
#include "core/string_ref.h"
#include "core/typeid_cast.h"
#include "core/types.h"
#include "exprs/expr_zonemap_filter.h"
#include "exprs/function/array/function_array_index.h"
#include "exprs/runtime_filter_expr.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/virtual_slot_ref.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "io/cache/cached_remote_file_reader.h"
#include "io/fs/file_reader.h"
#include "io/io_common.h"
#include "runtime/query_context.h"
#include "runtime/runtime_predicate.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "service/backend_options.h"
#include "storage/binlog.h"
#include "storage/compaction/collection_similarity.h"
#include "storage/id_manager.h"
#include "storage/index/ann/ann_index.h"
#include "storage/index/ann/ann_index_iterator.h"
#include "storage/index/ann/ann_index_reader.h"
#include "storage/index/ann/ann_topn_runtime.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_iterator.h"
#include "storage/index/index_query_context.h"
#include "storage/index/index_reader_helper.h"
#include "storage/index/indexed_column_reader.h"
#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/index/ordinal_page_index.h"
#include "storage/index/primary_key_index.h"
#include "storage/index/short_key_index.h"
#include "storage/index/zone_map/zone_map_index.h"
#include "storage/index/zone_map/zonemap_eval_context.h"
#include "storage/iterators.h"
#include "storage/olap_common.h"
#include "storage/predicate/bloom_filter_predicate.h"
#include "storage/predicate/column_predicate.h"
#include "storage/predicate/like_column_predicate.h"
#include "storage/schema.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/column_reader_cache.h"
#include "storage/segment/condition_cache.h"
#include "storage/segment/row_ranges.h"
#include "storage/segment/segment.h"
#include "storage/segment/segment_prefetcher.h"
#include "storage/segment/variant/variant_column_reader.h"
#include "storage/segment/virtual_column_iterator.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/types.h"
#include "storage/utils.h"
#include "util/concurrency_stats.h"
#include "util/defer_op.h"
#include "util/simd/bits.h"

namespace doris {
using namespace ErrorCode;
namespace segment_v2 {

class ScopedColumnIteratorReadPhase {
public:
    ScopedColumnIteratorReadPhase(ColumnIterator* column_iter, ColumnIterator::ReadPhase mode)
            : _column_iter(column_iter) {
        DORIS_CHECK(_column_iter != nullptr);
        _column_iter->set_read_phase(mode);
    }

    ScopedColumnIteratorReadPhase(const ScopedColumnIteratorReadPhase&) = delete;
    ScopedColumnIteratorReadPhase& operator=(const ScopedColumnIteratorReadPhase&) = delete;

    ~ScopedColumnIteratorReadPhase() {
        // ReadPhase is a per-read phase knob. SegmentIterator only needs a
        // temporary PREDICATE/LAZY mode while reading one column in one phase; it
        // must be restored before the next column or later normal reads reuse the
        // same ColumnIterator. Keep the restoration in one scoped helper instead
        // of open-coding the same Defer block at every call site.
        _column_iter->set_read_phase(ColumnIterator::ReadPhase::NORMAL);
    }

private:
    ColumnIterator* _column_iter = nullptr;
};

SegmentIterator::~SegmentIterator() = default;

void SegmentIterator::_init_row_bitmap_by_condition_cache() {
    // Only dispose need column predicate and expr cal in condition cache
    if (!_col_predicates.empty() || !_common_expr_ctxs_push_down.empty()) {
        if (_opts.condition_cache_digest) {
            auto* condition_cache = ConditionCache::instance();
            ConditionCache::CacheKey cache_key(_opts.rowset_id, _segment->id(),
                                               _opts.condition_cache_digest);

            // Increment search count when digest != 0
            DorisMetrics::instance()->condition_cache_search_count->increment(1);

            ConditionCacheHandle handle;
            _find_condition_cache = condition_cache->lookup(cache_key, &handle);

            // Increment hit count if cache lookup is successful
            if (_find_condition_cache) {
                DorisMetrics::instance()->condition_cache_hit_count->increment(1);
                if (_opts.runtime_state) {
                    VLOG_DEBUG << "Condition cache hit, query id: "
                               << print_id(_opts.runtime_state->query_id())
                               << ", segment id: " << _segment->id()
                               << ", cache digest: " << _opts.condition_cache_digest
                               << ", rowset id: " << _opts.rowset_id.to_string();
                }
            }

            auto num_rows = _segment->num_rows();
            if (_find_condition_cache) {
                const auto& filter_result = *(handle.get_filter_result());
                int64_t filtered_blocks = 0;
                for (int i = 0; i < filter_result.size(); i++) {
                    if (!filter_result[i]) {
                        _row_bitmap.removeRange(
                                i * CONDITION_CACHE_OFFSET,
                                i * CONDITION_CACHE_OFFSET + CONDITION_CACHE_OFFSET);
                        filtered_blocks++;
                    }
                }
                // Record condition_cache hit segment number
                _opts.stats->condition_cache_hit_seg_nums++;
                // Record rows filtered by condition cache hit
                _opts.stats->condition_cache_filtered_rows +=
                        filtered_blocks * SegmentIterator::CONDITION_CACHE_OFFSET;
            } else {
                _condition_cache = std::make_shared<std::vector<bool>>(
                        num_rows / CONDITION_CACHE_OFFSET + 1, false);
            }
        }
    } else {
        _opts.condition_cache_digest = 0;
    }
}

// A fast range iterator for roaring bitmap. Output ranges use closed-open form, like [from, to).
// Example:
//   input bitmap:  [0 1 4 5 6 7 10 15 16 17 18 19]
//   output ranges: [0,2), [4,8), [10,11), [15,20) (when max_range_size=10)
//   output ranges: [0,2), [4,7), [7,8), [10,11), [15,18), [18,20) (when max_range_size=3)
class SegmentIterator::BitmapRangeIterator {
public:
    BitmapRangeIterator() = default;
    virtual ~BitmapRangeIterator() = default;

    explicit BitmapRangeIterator(const roaring::Roaring& bitmap) {
        roaring_init_iterator(&bitmap.roaring, &_iter);
    }

    bool has_more_range() const { return !_eof; }

    [[nodiscard]] static uint32_t get_batch_size() { return kBatchSize; }

    // read next range into [*from, *to) whose size <= max_range_size.
    // return false when there is no more range.
    virtual bool next_range(const uint32_t max_range_size, uint32_t* from, uint32_t* to) {
        if (_eof) {
            return false;
        }

        *from = _buf[_buf_pos];
        uint32_t range_size = 0;
        uint32_t expect_val = _buf[_buf_pos]; // this initial value just make first batch valid

        // if array is contiguous sequence then the following conditions need to be met :
        // a_0: x
        // a_1: x+1
        // a_2: x+2
        // ...
        // a_p: x+p
        // so we can just use (a_p-a_0)-p to check conditions
        // and should notice the previous batch needs to be continuous with the current batch
        while (!_eof && range_size + _buf_size - _buf_pos <= max_range_size &&
               expect_val == _buf[_buf_pos] &&
               _buf[_buf_size - 1] - _buf[_buf_pos] == _buf_size - 1 - _buf_pos) {
            range_size += _buf_size - _buf_pos;
            expect_val = _buf[_buf_size - 1] + 1;
            _read_next_batch();
        }

        // promise remain range not will reach next batch
        if (!_eof && range_size < max_range_size && expect_val == _buf[_buf_pos]) {
            do {
                _buf_pos++;
                range_size++;
            } while (range_size < max_range_size && _buf[_buf_pos] == _buf[_buf_pos - 1] + 1);
        }
        *to = *from + range_size;
        return true;
    }

    // read batch_size of rowids from roaring bitmap into buf array
    virtual uint32_t read_batch_rowids(rowid_t* buf, uint32_t batch_size) {
        return roaring::api::roaring_read_uint32_iterator(&_iter, buf, batch_size);
    }

private:
    void _read_next_batch() {
        _buf_pos = 0;
        _buf_size = roaring::api::roaring_read_uint32_iterator(&_iter, _buf, kBatchSize);
        _eof = (_buf_size == 0);
    }

    static const uint32_t kBatchSize = 256;
    roaring::api::roaring_uint32_iterator_t _iter;
    uint32_t _buf[kBatchSize];
    uint32_t _buf_pos = 0;
    uint32_t _buf_size = 0;
    bool _eof = false;
};

// A backward range iterator for roaring bitmap. Output ranges use closed-open form, like [from, to).
// Example:
//   input bitmap:  [0 1 4 5 6 7 10 15 16 17 18 19]
//   output ranges: , [15,20), [10,11), [4,8), [0,2) (when max_range_size=10)
//   output ranges: [17,20), [15,17), [10,11), [5,8), [4, 5), [0,2) (when max_range_size=3)
class SegmentIterator::BackwardBitmapRangeIterator : public SegmentIterator::BitmapRangeIterator {
public:
    explicit BackwardBitmapRangeIterator(const roaring::Roaring& bitmap) {
        roaring_init_iterator_last(&bitmap.roaring, &_riter);
        _rowid_count = cast_set<uint32_t>(roaring_bitmap_get_cardinality(&bitmap.roaring));
        _rowid_left = _rowid_count;
    }

    bool has_more_range() const { return !_riter.has_value; }

    // read next range into [*from, *to) whose size <= max_range_size.
    // return false when there is no more range.
    bool next_range(const uint32_t max_range_size, uint32_t* from, uint32_t* to) override {
        if (!_riter.has_value) {
            return false;
        }

        uint32_t range_size = 0;
        *to = _riter.current_value + 1;

        do {
            *from = _riter.current_value;
            range_size++;
            roaring_previous_uint32_iterator(&_riter);
        } while (range_size < max_range_size && _riter.has_value &&
                 _riter.current_value + 1 == *from);

        return true;
    }
    /**
     * Reads a batch of row IDs from a roaring bitmap, starting from the end and moving backwards.
     * This function retrieves the last `batch_size` row IDs from the bitmap and stores them in the provided buffer.
     * It updates the internal state to track how many row IDs are left to read in subsequent calls.
     *
     * The row IDs are read in reverse order, but stored in the buffer maintaining their original order in the bitmap.
     *
     * Example:
     *   input bitmap: [0 1 4 5 6 7 10 15 16 17 18 19]
     *   If the bitmap has 12 elements and batch_size is set to 5, the function will first read [15, 16, 17, 18, 19]
     *   into the buffer, leaving 7 elements left. In the next call with batch_size 5, it will read [4, 5, 6, 7, 10].
     *
     */
    uint32_t read_batch_rowids(rowid_t* buf, uint32_t batch_size) override {
        if (!_riter.has_value || _rowid_left == 0) {
            return 0;
        }

        if (_rowid_count <= batch_size) {
            roaring_bitmap_to_uint32_array(_riter.parent,
                                           buf); // Fill 'buf' with '_rowid_count' elements.
            uint32_t num_read = _rowid_left;     // Save the number of row IDs read.
            _rowid_left = 0;                     // No row IDs left after this operation.
            return num_read;                     // Return the number of row IDs read.
        }

        uint32_t read_size = std::min(batch_size, _rowid_left);
        uint32_t num_read = 0; // Counter for the number of row IDs read.

        // Read row IDs into the buffer in reverse order.
        while (num_read < read_size && _riter.has_value) {
            buf[read_size - num_read - 1] = _riter.current_value;
            num_read++;
            _rowid_left--; // Decrement the count of remaining row IDs.
            roaring_previous_uint32_iterator(&_riter);
        }

        // Return the actual number of row IDs read.
        return num_read;
    }

private:
    roaring::api::roaring_uint32_iterator_t _riter;
    uint32_t _rowid_count;
    uint32_t _rowid_left;
};

SegmentIterator::SegmentIterator(std::shared_ptr<Segment> segment, SchemaSPtr schema)
        : _segment(std::move(segment)),
          _schema(schema),
          _column_iterators(_schema->num_columns()),
          _index_iterators(_schema->num_columns()),
          _cur_rowid(0),
          _lazy_materialization_read(false),
          _lazy_inited(false),
          _inited(false),
          _pool(new ObjectPool) {}

// 初始化的入口包装函数
Status SegmentIterator::init(const StorageReadOptions& opts) {
    // 1. 调用实际的初始化实现函数 _init_impl
    auto status = _init_impl(opts);
    // 2. 错误处理与健康状态更新（Health Status Feedback）
    if (!status.ok()) {
        _segment->update_healthy_status(status);
    }
    return status;
}

std::unique_ptr<AdaptiveBlockSizePredictor> SegmentIterator::_make_block_size_predictor() const {
    if (!config::enable_adaptive_batch_size || _opts.preferred_block_size_bytes == 0) {
        return nullptr;
    }

    // Collect per-column raw byte metadata from the segment footer for the columns
    // this iterator will actually output (defined by _schema, which is built from
    // _opts.return_columns).
    uint32_t seg_rows = _segment->num_rows();
    uint64_t total_raw_bytes = 0;
    double metadata_hint_bytes_per_row = 0.0;
    if (seg_rows > 0) {
        const auto& ts = _segment->tablet_schema();
        if (ts) {
            for (ColumnId cid : _schema->column_ids()) {
                if (static_cast<size_t>(cid) < ts->num_columns()) {
                    int32_t uid = ts->column(cid).unique_id();
                    uint64_t raw_bytes = _segment->column_raw_data_bytes(uid);
                    if (uid >= 0 && raw_bytes > 0) {
                        total_raw_bytes += raw_bytes;
                    }
                }
            }
            metadata_hint_bytes_per_row = total_raw_bytes / static_cast<double>(seg_rows);
        }
    }

    return std::make_unique<AdaptiveBlockSizePredictor>(
            _opts.preferred_block_size_bytes, metadata_hint_bytes_per_row,
            AdaptiveBlockSizePredictor::kDefaultProbeRows, _opts.block_row_max);
}
// Apache Doris BE 中 SegmentIterator 的真正物理初始化实现逻辑。
// 责在进行任何物理 Page 读取或索引裁剪之前，完成参数快照、安全谓词过滤、动态 Batch 预测器构建、Schema 字段与倒排索引字段映射绑定、列/索引迭代器实例化（init_iterators()）以及复合表达式上下文（_construct_compound_expr_context()）的构建。
Status SegmentIterator::_init_impl(const StorageReadOptions& opts) {
    // get file handle from file descriptor of segment
    // 初始化状态幂等与耗时统计
    // 通过 _inited 标记避免重复初始化。
    if (_inited) {
        return Status::OK();
    }
    _opts = opts;
    SCOPED_RAW_TIMER(&_opts.stats->segment_iterator_init_timer_ns);
    _inited = true;
    // 文件句柄赋值：将 _segment 底层的 FileReader 赋值给迭代器作为后续 Data Page IO 读取句柄。
    _file_reader = _segment->_file_reader;
    _col_predicates.clear();
    // 安全谓词下推筛选 (can_apply_predicate_safely)
    // 原理：并非所有上层传入的谓词都能安全地在存储引擎（Segment 物理层）下推执行。
    for (const auto& predicate : opts.column_predicates) {
        if (!_segment->can_apply_predicate_safely(predicate->column_id(), *_schema,
                                                  _opts.target_cast_type_for_variants, _opts)) {
            continue;
        }
        _col_predicates.emplace_back(predicate);
    }
    _tablet_id = opts.tablet_id;
    // Read options will not change, so that just resize here
    // 动态/自适应 Batch Size 预测器
    // 背景：Doris 采用向量化 Block 批处理机制。
    // 自适应调节：除了预分配固定最大行数（block_row_max）的 RowID 缓存数组，还通过 _make_block_size_predictor() 创建动态 Batch 预测器。
    // 在后续读取变长数据（如 String, Array, JSON Variant）时，根据列的内存开销动态调整每次返回的 Block 行数，防止产生超大 Block 挤爆内存。
    _block_rowids.resize(_opts.block_row_max);

    // Adaptive batch size: snapshot the initial row limit and create predictor if enabled.
    _initial_block_row_max = _opts.block_row_max;
    _block_size_predictor = _make_block_size_predictor();
    // RowID 追踪：如果 Schema 包含 RowID 列，开启 _record_rowids，记录读取行的物理行号（用于 Delete/Update/Unique Key 检查）。
    if (_schema->rowid_col_idx() > 0) {
        _record_rowids = true;
    }
    // 算子下推句柄：保存虚拟列表达式（virtual_column_exprs）、倒排/全文检索打分（score_runtime）和向量近似近邻检索（ann_topn_runtime）。
    _virtual_column_exprs = _opts.virtual_column_exprs;
    _score_runtime = _opts.score_runtime;
    _ann_topn_runtime = _opts.ann_topn_runtime;
    // Nested 列剪枝：如果当前的 Reader 类型是普通 Query 且 RuntimeState 中开启了嵌套列剪枝，则标记 _enable_prune_nested_column = true，减少 Complex/Nested 类型的无效数据 Page 读取。
    _enable_prune_nested_column = _opts.io_ctx.reader_type == ReaderType::READER_QUERY &&
                                  _opts.runtime_state &&
                                  _opts.runtime_state->enable_prune_nested_column();

    if (opts.output_columns != nullptr) {
        _output_columns = *(opts.output_columns);
    }
    // 倒排索引字段映射构造与 Variant 稀疏列 Cache (_storage_name_and_type)
    _storage_name_and_type.resize(_schema->columns().size());
    auto storage_format = _opts.tablet_schema->get_inverted_index_storage_format();
    for (int i = 0; i < _schema->columns().size(); ++i) {
        const TabletColumn* col = _schema->column(i);
        if (col) {
            // 数据类型确定：优先从 Segment 拿底层真实的物理持久化数据类型 get_data_type_of，拿不到则按 Schema 创建。
            auto storage_type = _segment->get_data_type_of(*col, _opts);
            if (storage_type == nullptr) {
                storage_type =
                        DataTypeFactory::instance().create_data_type(*col, col->is_nullable());
            }
            // Currently, when writing a lucene index, the field of the document is column_name, and the column name is
            // bound to the index field. Since version 1.2, the data file storage has been changed from column_name to
            // column_unique_id, allowing the column name to be changed. Due to current limitations, previous inverted
            // index data cannot be used after Doris changes the column name. Column names also support Unicode
            // characters, which may cause other problems with indexing in non-ASCII characters.
            // After consideration, it was decided to change the field name from column_name to column_unique_id in
            // format V2, while format V1 continues to use column_name.
            std::string field_name;
            // V1 格式：以列名 col->name() 作为 Lucene/Inverted Index 的 Document Field 名称。缺点是 Alter Table 改列名后旧索引失效。
            if (storage_format == InvertedIndexStorageFormatPB::V1) {
                field_name = col->name();
            } else {
            // V2 格式：使用全局唯一的 Column Unique ID。
                if (col->is_extracted_column()) {
                    // Variant 抽取子列：parent_unique_id.sub_col_name。
                    // variant sub col
                    // field_name format: parent_unique_id.sub_col_name
                    field_name = std::to_string(col->parent_unique_id()) + "." + col->name();
                } else {
                    //普通列：std::to_string(col->unique_id())。
                    field_name = std::to_string(col->unique_id());
                }
            }
            _storage_name_and_type[i] = std::make_pair(field_name, storage_type);
            if (int32_t uid =
                        col->is_extracted_column() ? col->parent_unique_id() : col->unique_id();
                !_variant_sparse_column_cache.contains(uid)) {
                DCHECK(uid >= 0);
                _variant_sparse_column_cache.emplace(uid,
                                                     std::make_unique<PathToBinaryColumnCache>());
            }
        }
    }
    // 物理迭代器与下推表达式初始化
    // 依次实例化物理列迭代器（ColumnIterator）与索引迭代器（IndexIterator）
    RETURN_IF_ERROR(init_iterators());
    // 构建复合表达式（如 AND/OR/复杂的 Function Expr）下推计算上下文，供后续做更复杂的复杂谓词评估。
    RETURN_IF_ERROR(_construct_compound_expr_context());
    VLOG_DEBUG << fmt::format(
            "Segment iterator init, virtual_column_exprs size: {}, common_expr_pushdown size: {}",
            _opts.virtual_column_exprs.size(), _common_expr_ctxs_push_down.size());
    // 预先初始化谓词过滤结果位图/缓存容器，准备迎接真正的 Scan 数据提取。
    _initialize_predicate_results();
    return Status::OK();
}

void SegmentIterator::_initialize_predicate_results() {
    // Initialize from _col_predicates
    for (auto pred : _col_predicates) {
        int cid = pred->column_id();
        _column_predicate_index_exec_status[cid][pred] = false;
    }

    _calculate_common_expr_index_exec_status();
}
// 用于实例化并初始化当前 Segment 中所有被引用的物理列迭代器（ColumnIterator）和二级/倒排索引迭代器（IndexIterator）
Status SegmentIterator::init_iterators() {
    // 1. 初始化所有物理数据列/返回列的 ColumnIterator
    RETURN_IF_ERROR(_init_return_column_iterators());
    // 2. 初始化所有配置了二级索引/倒排索引列的 IndexIterator
    RETURN_IF_ERROR(_init_index_iterators());
    return Status::OK();
}
// 在首次真正触发数据读取（Scan）时调用的延迟初始化函数（Lazy Initialization）。
// _lazy_init 集中处理物理索引剪枝（Index Pruning）、Delete Bitmap 行裁剪、向量化延迟物化结构初始化、向量/ANN/TopN 索引生效，以及列 Column Buffer 预分配，是确定“哪些物理行最终需要被读取”的最关键关卡。
Status SegmentIterator::_lazy_init(Block* block) {
    if (_lazy_inited) {
        return Status::OK();
    }
    SCOPED_RAW_TIMER(&_opts.stats->block_init_ns);
    DorisMetrics::instance()->segment_read_total->increment(1);
    // 全量位图设定：最开始将当前 Segment 的所有行（0 到 num_rows - 1）加入 _row_bitmap（Roaring Bitmap）。
    _row_bitmap.addRange(0, _segment->num_rows());
    // Condition Cache 过滤：如果命中了历史 Query 留下的条件缓存，直接对 _row_bitmap 进行初步快速过滤。
    _init_row_bitmap_by_condition_cache();

    // z-order can not use prefix index
    // 2. 多级索引物理裁剪（Prefix Key / ZoneMap / Inverted Index）
    // 前缀 Key 索引剪枝（_get_row_ranges_by_keys）：对普通排序列，利用 Short Key 索引二分查找确定大致的 Row Range。（针对 Z-ORDER 或 Cluster Key 则跳过此步骤）。
    if (_segment->_tablet_schema->sort_type() != SortType::ZORDER &&
        _segment->_tablet_schema->cluster_key_uids().empty()) {
        RETURN_IF_ERROR(_get_row_ranges_by_keys());
    }
    // 列条件与高级索引剪枝（_get_row_ranges_by_column_conditions）：触发 ZoneMap、Bloom Filter、Inverted Index（倒排索引）计算，大幅收缩 _row_bitmap 包含的绝对行号。
    RETURN_IF_ERROR(_get_row_ranges_by_column_conditions());
    // 延迟物化数据结构初始化（_vec_init_lazy_materialization）：划分哪些列是第一阶段过滤用的 _predicate_column_ids，哪些是第二阶段才读取的数据列 _non_predicate_columns。
    RETURN_IF_ERROR(_vec_init_lazy_materialization());
    // Remove rows that have been marked deleted
    // Delete Bitmap 逻辑删除裁剪与上层 RowRanges 求交
    if (_opts.delete_bitmap.count(segment_id()) > 0 &&
        _opts.delete_bitmap.at(segment_id()) != nullptr) {
        size_t pre_size = _row_bitmap.cardinality();
        // 执行位图减法 _row_bitmap -= delete_bitmap，被删除的行在此彻底被物理过滤，永远不会发起磁盘 Page 读取！
        _row_bitmap -= *(_opts.delete_bitmap.at(segment_id()));
        _opts.stats->rows_del_by_bitmap += (pre_size - _row_bitmap.cardinality());
        VLOG_DEBUG << "read on segment: " << segment_id() << ", delete bitmap cardinality: "
                   << _opts.delete_bitmap.at(segment_id())->cardinality() << ", "
                   << _opts.stats->rows_del_by_bitmap << " rows deleted by bitmap";
    }
    // RowRanges 求交集：与上层（如 RowsetReader 侧传入）进一步限定的物理行号范围取交集。
    if (!_opts.row_ranges.is_empty()) {
        _row_bitmap &= RowRanges::ranges_to_roaring(_opts.row_ranges);
    }
    // 在执行向量相似度搜索（如 SELECT *, distance(...) FROM tbl ORDER BY distance LIMIT 10）时，为上层物化并返回“向量距离/相似度 Score 列”做准备。
    _prepare_score_column_materialization();
    // 调用底层 Segment 绑定的 ANN 向量索引（如 HNSW / FAISS）
    // 结合索引在全局直接检索出 TopN 的相似行，并再次对 _row_bitmap 进行 AND 求交操作，大幅裁剪物理行，将非 TopN 的行号直接过滤掉。
    RETURN_IF_ERROR(_apply_ann_topn_predicate());
    // 按需反向扫描：如果 SQL 包含倒序扫描需求（例如 ORDER BY pk DESC），创建 BackwardBitmapRangeIterator，
    // 会从 _row_bitmap 的高位物理行号向低位倒序产生连续的 Block 范围；反之则构造正向的 BitmapRangeIterator。
    if (_opts.read_orderby_key_reverse) {
        _range_iter.reset(new BackwardBitmapRangeIterator(_row_bitmap));
    } else {
        _range_iter.reset(new BitmapRangeIterator(_row_bitmap));
    }

    // Reserve columns for _initial_block_row_max (the original max before any adaptive
    // prediction) because the predictor may increase block_row_max on subsequent batches
    // up to this ceiling. Using the current (possibly reduced) _opts.block_row_max would
    // cause heap-buffer-overflow if a later prediction is larger.
    // Doris 内部存在自适应 Batch Size 预测器（Adaptive Predictor），会在扫描过程中根据过滤率动态增减后续批次的 block_row_max（最高可扩展至初始最大上限 _initial_block_row_max）。
    auto nrows_reserve_limit =
            std::min(_row_bitmap.cardinality(), uint64_t(_initial_block_row_max));
    // 在启用了延迟物化、RowID 记录或复杂表达式计算时，系统需要用 _block_rowids 记录当前 Batch 读出的物理行号（以备第二阶段做稀疏离散读取）。此处直接将其一次性扩容至最大安全的 _initial_block_row_max。
    if (_lazy_materialization_read || _opts.record_rowids || _is_need_expr_eval) {
        _block_rowids.resize(_initial_block_row_max);
    }
    // 将其大小调整为 Schema 中的物理列数，为随后循环创建具体的 ColumnPtr（如 ColumnVector, ColumnString）提供容器空间。
    _current_return_columns.resize(_schema->columns().size());

    for (size_t i = 0; i < _schema->column_ids().size(); i++) {
        ColumnId cid = _schema->column_ids()[i];
        const auto* column_desc = _schema->column(cid);
        // 谓词过滤列初始化
        if (_is_pred_column[cid]) {
            auto storage_column_type = _storage_name_and_type[cid].second;
            RETURN_IF_CATCH_EXCEPTION(
                    // Here, cid will not go out of bounds
                    // because the size of _current_return_columns equals _schema->tablet_columns().size()
                    _current_return_columns[cid] = Schema::get_predicate_column_ptr(
                            storage_column_type, _opts.io_ctx.reader_type));
            // 唯一元数据绑定：调用 set_rowset_segment_id 注入当前 Segment 所在的 Rowset ID 与 Segment ID。这在计算包含全表唯一行标识（RowID/Global Dict/MVCC 追踪）时至关重要。
            _current_return_columns[cid]->set_rowset_segment_id(
                    {_segment->rowset_id(), _segment->id()});
            // 内存预分配（Reserve）：按上一阶段算出的 nrows_reserve_limit 进行容量预留，确保后续批次提取时无需频繁重新分配堆内存（Realloc）。
            _current_return_columns[cid]->reserve(nrows_reserve_limit);
        // 未生效隐藏 Delete 条件列的处理
        // 在 Doris 中执行带条件的删除（如 DELETE FROM tbl WHERE status = 0）时，系统会生成包含该删除条件的 Segment。
        // 对于后续导入的新数据 Segment（如版本 C），该 Segment 物理上并没有定义这个 Delete 过滤条件，但底层 Schema 为了版本对齐仍会包含 status 列。
        } else if (i >= block->columns()) {
            // This column needs to be scanned, but doesn't need to be returned upward. (delete sign)
            // if i >= block->columns means the column and not the pred_column means `column i` is
            // a delete condition column. but the column is not effective in the segment. so we just
            // create a column to hold the data.
            // a. origin data -> b. delete condition -> c. new load data
            // the segment of c do not effective delete condition, but it still need read the column
            // to match the schema.
            // TODO: skip read the not effective delete column to speed up segment read.
            _current_return_columns[cid] = Schema::get_data_type_ptr(*column_desc)->create_column();
            _current_return_columns[cid]->reserve(nrows_reserve_limit);
        }
    }

    // Additional deleted filter condition will be materialized column be at the end of the block,
    // after _output_column_by_sel_idx  will be erase, we not need to filter it,
    // so erase it from _columns_to_filter in the first next_batch.
    // Eg:
    //      `delete from table where a = 10;`
    //      `select b from table;`
    // a column only effective in segment iterator, the block from query engine only contain the b column,
    // so no need to filter a column by expr.
    // 在存储层内部，可能存在一些仅用于 Delete 条件筛选或内部 MVCC 计算的隐藏谓词列。这些列的索引号（*it）可能大于或等于上层 Query Engine 传入的 block 所包含的物理列数（block->columns()）。
    // 如果将越界的列索引留存在 _columns_to_filter 中，后续对 block 执行列过滤/擦除时，就会导致严重的数组越界（Out-of-bound Access）或无效的 Column 操作崩溃。此处通过标准的 C++ iterator 安全擦除模式（it = _columns_to_filter.erase(it)），安全地清理掉这些仅在存储层内部生效的列。
    for (auto it = _columns_to_filter.begin(); it != _columns_to_filter.end();) {
        if (*it >= block->columns()) {
            it = _columns_to_filter.erase(it);
        } else {
            ++it;
        }
    }
    // 将标志位置为 true。
    // 当后续上层算子不断循环调用 _next_batch_internal 读取数据批次时，开头检查到 if (_lazy_inited) 会直接返回 Status::OK()，避免重复执行开销昂贵的索引剪枝和 Buffer 分配逻辑。
    _lazy_inited = true;
    // 根据前面的 _row_bitmap 计算出即将需要读取的数据 Page/Column Range，并向 IO 线程池或操作系统 Page Cache 提交异步预取请求（Prefetch Request）。
    _init_segment_prefetchers();

    return Status::OK();
}

void SegmentIterator::_init_segment_prefetchers() {
    SCOPED_RAW_TIMER(&_opts.stats->segment_iterator_init_segment_prefetchers_timer_ns);
    if (!config::is_cloud_mode()) {
        return;
    }
    static std::vector<ReaderType> supported_reader_types {
            ReaderType::READER_QUERY, ReaderType::READER_BASE_COMPACTION,
            ReaderType::READER_CUMULATIVE_COMPACTION, ReaderType::READER_FULL_COMPACTION};
    if (std::ranges::none_of(supported_reader_types,
                             [&](ReaderType t) { return _opts.io_ctx.reader_type == t; })) {
        return;
    }
    // Initialize segment prefetcher for predicate and non-predicate columns
    bool is_query = (_opts.io_ctx.reader_type == ReaderType::READER_QUERY);
    bool enable_prefetch = is_query ? config::enable_query_segment_file_cache_prefetch
                                    : config::enable_compaction_segment_file_cache_prefetch;
    LOG_IF(INFO, config::enable_segment_prefetch_verbose_log) << fmt::format(
            "[verbose] SegmentIterator _init_segment_prefetchers, is_query={}, "
            "enable_prefetch={}, "
            "_row_bitmap.isEmpty()={}, row_bitmap.cardinality()={}, tablet={}, rowset={}, "
            "segment={}, predicate_column_ids={}, common_expr_column_ids={}",
            is_query, enable_prefetch, _row_bitmap.isEmpty(), _row_bitmap.cardinality(),
            _opts.tablet_id, _opts.rowset_id.to_string(), segment_id(),
            fmt::join(_predicate_column_ids, ","), fmt::join(_common_expr_column_ids, ","));
    if (enable_prefetch && !_row_bitmap.isEmpty()) {
        int window_size =
                1 + (is_query ? config::query_segment_file_cache_prefetch_block_size
                              : config::compaction_segment_file_cache_prefetch_block_size);
        LOG_IF(INFO, config::enable_segment_prefetch_verbose_log) << fmt::format(
                "[verbose] SegmentIterator prefetch config: window_size={}", window_size);
        if (window_size > 0 &&
            !_column_iterators.empty()) { // ensure init_iterators has been called
            SegmentPrefetcherConfig prefetch_config(window_size,
                                                    config::file_cache_each_block_size);
            for (auto cid : _schema->column_ids()) {
                auto& column_iter = _column_iterators[cid];
                if (column_iter == nullptr) {
                    continue;
                }
                const auto* tablet_column = _schema->column(cid);
                SegmentPrefetchParams params {
                        .config = prefetch_config,
                        .read_options = _opts,
                };
                LOG_IF(INFO, config::enable_segment_prefetch_verbose_log) << fmt::format(
                        "[verbose] SegmentIterator init_segment_prefetchers, "
                        "tablet={}, rowset={}, segment={}, column_id={}, col_name={}, type={}",
                        _opts.tablet_id, _opts.rowset_id.to_string(), segment_id(), cid,
                        tablet_column->name(), tablet_column->type());
                Status st = column_iter->init_prefetcher(params);
                if (!st.ok()) {
                    LOG_IF(WARNING, config::enable_segment_prefetch_verbose_log) << fmt::format(
                            "[verbose] failed to init prefetcher for column_id={}, "
                            "tablet={}, rowset={}, segment={}, error={}",
                            cid, _opts.tablet_id, _opts.rowset_id.to_string(), segment_id(),
                            st.to_string());
                }
            }

            // for compaction, it's guaranteed that all rows are read, so we can prefetch all data blocks
            PrefetcherInitMethod init_method = (is_query && _row_bitmap.cardinality() < num_rows())
                                                       ? PrefetcherInitMethod::FROM_ROWIDS
                                                       : PrefetcherInitMethod::ALL_DATA_BLOCKS;
            std::map<PrefetcherInitMethod, std::vector<SegmentPrefetcher*>> prefetchers;
            for (size_t idx = 0; idx < _column_iterators.size(); ++idx) {
                auto cid = cast_set<ColumnId>(idx);
                auto* column_iter = _column_iterators[cid].get();
                if (column_iter != nullptr) {
                    ScopedColumnIteratorReadPhase scoped_read_phase {
                            column_iter, _support_lazy_read_pruned_columns.contains(cid)
                                                 ? ColumnIterator::ReadPhase::PREDICATE
                                                 : ColumnIterator::ReadPhase::NORMAL};
                    column_iter->collect_prefetchers(prefetchers, init_method);
                }
            }
            for (auto& [method, prefetcher_vec] : prefetchers) {
                if (method == PrefetcherInitMethod::ALL_DATA_BLOCKS) {
                    for (auto* prefetcher : prefetcher_vec) {
                        prefetcher->build_all_data_blocks();
                    }
                } else if (method == PrefetcherInitMethod::FROM_ROWIDS && !prefetcher_vec.empty()) {
                    SegmentPrefetcher::build_blocks_by_rowids(_row_bitmap, prefetcher_vec);
                }
            }
        }
    }
}
// 利用物理存储中的 Short Key（前缀稀疏索引），根据 SQL 查询传入的主键/排序键范围（_opts.key_ranges），二分查找计算出符合条件的数据行区间（RowRanges），并对 _row_bitmap 进行求交集剪枝。
Status SegmentIterator::_get_row_ranges_by_keys() {
    SCOPED_RAW_TIMER(&_opts.stats->generate_row_ranges_by_keys_ns);
    DorisMetrics::instance()->segment_row_total->increment(num_rows());

    // fast path for empty segment or empty key ranges
    // 性能优化：如果之前的步骤已经把 _row_bitmap 裁剪为空（无存活行），或者上层 Optimizer 根本没有下推 key 范围过滤，直接提前返回 Status::OK()，避免做无用功。
    if (_row_bitmap.isEmpty() || _opts.key_ranges.empty()) {
        return Status::OK();
    }

    // Read & seek key columns is a waste of time when no key column in _schema
    // Short Key 索引的 Seek 需要解析 Key 列的数据格式。
    // 如果当前 SQL 查询的列（_schema）中完全没有包含任何物理 Key 列（例如 SELECT val_col FROM tbl WHERE key_col > 10，其中 key_col 并不在选中的输出列或谓词读取列表中），去解析并 Seek Key 列反而会带来额外开销，因此此处直接跳过 Seek 流程。
    if (std::none_of(_schema->columns().begin(), _schema->columns().end(),
                     [&](const TabletColumnPtr& col) {
                         return col &&
                                _opts.tablet_schema->column_by_uid(col->unique_id()).is_key();
                     })) {
        return Status::OK();
    }

    RowRanges result_ranges;
    // 基于前缀索引二分查找 RowID 范围（Upper/Lower Lookup）
    for (auto& key_range : _opts.key_ranges) {
        rowid_t lower_rowid = 0;
        rowid_t upper_rowid = num_rows();
        // 准备 Short Key Index 的 Block 数据
        RETURN_IF_ERROR(_prepare_seek(key_range));
        // 先定位上限 upper_rowid
        if (key_range.upper_key != nullptr) {
            // If client want to read upper_bound, the include_upper is true. So we
            // should get the first ordinal at which key is larger than upper_bound.
            // So we call _lookup_ordinal with include_upper's negate
            RETURN_IF_ERROR(_lookup_ordinal(*key_range.upper_key, !key_range.include_upper,
                                            num_rows(), &upper_rowid));
        }
        // Lower Bound Lookup（寻找下限行号）
        if (upper_rowid > 0 && key_range.lower_key != nullptr) {
            RETURN_IF_ERROR(_lookup_ordinal(*key_range.lower_key, key_range.include_lower,
                                            upper_rowid, &lower_rowid));
        }
        // 区间合并：将每一个 [lower_rowid, upper_rowid) 行区间通过 ranges_union 累加到最终的 result_ranges 中（支持多个点查或范围查的并集）
        auto row_range = RowRanges::create_single(lower_rowid, upper_rowid);
        RowRanges::ranges_union(result_ranges, row_range, &result_ranges);
    }
    // 位图二次裁剪与过滤指标累加
    size_t pre_size = _row_bitmap.cardinality();
    // 位图求交（Bitwise AND）：将二分查找计算出的 Short Key 结果区间转换为 Roaring Bitmap，并与当前 _row_bitmap 做交集计算（&=）。
    _row_bitmap &= RowRanges::ranges_to_roaring(result_ranges);
    // 统计数据更新：记录经过 Short Key 索引过滤掉的物理行数，方便在 EXPLAIN ANALYZE 中追踪 Key 索引的过滤效率。
    _opts.stats->rows_key_range_filtered += (pre_size - _row_bitmap.cardinality());

    return Status::OK();
}

// Set up environment for the following seek.
Status SegmentIterator::_prepare_seek(const StorageReadOptions::KeyRange& key_range) {
    std::vector<const TabletColumn*> key_columns;
    std::set<uint32_t> column_set;
    if (key_range.lower_key != nullptr) {
        for (auto cid : key_range.lower_key->schema()->column_ids()) {
            column_set.emplace(cid);
            key_columns.emplace_back(key_range.lower_key->column(cid));
        }
    }
    if (key_range.upper_key != nullptr) {
        for (auto cid : key_range.upper_key->schema()->column_ids()) {
            if (column_set.count(cid) == 0) {
                key_columns.emplace_back(key_range.upper_key->column(cid));
                column_set.emplace(cid);
            }
        }
    }
    if (!_seek_schema) {
        std::vector<TabletColumnPtr> cols;
        cols.reserve(key_columns.size());
        for (const TabletColumn* col : key_columns) {
            cols.emplace_back(std::make_shared<TabletColumn>(*col));
        }
        std::vector<uint32_t> column_ids(cols.size());
        std::iota(column_ids.begin(), column_ids.end(), 0);
        _seek_schema = std::make_unique<Schema>(cols, column_ids);
    }
    // todo(wb) need refactor here, when using pk to search, _seek_block is useless
    if (_seek_block.size() == 0) {
        _seek_block.resize(_seek_schema->num_column_ids());
        int i = 0;
        for (auto cid : _seek_schema->column_ids()) {
            auto column_desc = _seek_schema->column(cid);
            _seek_block[i] = Schema::get_data_type_ptr(*column_desc)->create_column();
            i++;
        }
    }

    // create used column iterator
    for (auto cid : _seek_schema->column_ids()) {
        if (_column_iterators[cid] == nullptr) {
            // TODO: Do we need this?
            if (_virtual_column_exprs.contains(cid)) {
                _column_iterators[cid] = std::make_unique<VirtualColumnIterator>();
                continue;
            }

            RETURN_IF_ERROR(_segment->new_column_iterator(_opts.tablet_schema->column(cid),
                                                          &_column_iterators[cid], &_opts,
                                                          &_variant_sparse_column_cache));
            ColumnIteratorOptions iter_opts {
                    .use_page_cache = _opts.use_page_cache,
                    .file_reader = _file_reader.get(),
                    .stats = _opts.stats,
                    .io_ctx = _opts.io_ctx,
            };
            RETURN_IF_ERROR(_column_iterators[cid]->init(iter_opts));
        }
    }

    return Status::OK();
}
// Doris 实现高效列谓词索引剪枝（Inverted Index / ZoneMap / Bloom Filter）的聚合枢纽
// 核心目标是在读取实际数据 Page 之前，利用倒排索引和各类统计索引大幅收缩存活行号位图（_row_bitmap），并根据过滤情况优化后续的列读取路径。
Status SegmentIterator::_get_row_ranges_by_column_conditions() {
    SCOPED_RAW_TIMER(&_opts.stats->generate_row_ranges_by_column_conditions_ns);
    if (_row_bitmap.isEmpty()) {
        return Status::OK();
    }

    {   // 1. 倒排索引评估与位图裁剪（Inverted Index Pipeline）
        if (_opts.runtime_state &&
            _opts.runtime_state->query_options().enable_inverted_index_query &&
            (has_index_in_iterators() || !_common_expr_ctxs_push_down.empty())) {
            SCOPED_RAW_TIMER(&_opts.stats->inverted_index_filter_timer);
            size_t input_rows = _row_bitmap.cardinality();
            // Only apply column-level inverted index if we have iterators
            // a. 应用列级倒排索引
            if (has_index_in_iterators()) {
                RETURN_IF_ERROR(_apply_inverted_index());
            }
            // Always apply expr-level index (e.g., search expressions) if we have common_expr_pushdown
            // This allows search expressions with variant subcolumns to be evaluated even when
            // the segment doesn't have all subcolumns
            // b. 应用表达式级倒排索引 (支持 Variant 动态子列与复合表达式)
            RETURN_IF_ERROR(_apply_index_expr());
            // c. 提取已完全评估的倒排索引结果 Bitmap
            for (auto it = _common_expr_ctxs_push_down.begin();
                 it != _common_expr_ctxs_push_down.end();) {
                if ((*it)->all_expr_inverted_index_evaluated()) {
                    const auto* result = (*it)->get_index_context()->get_index_result_for_expr(
                            (*it)->root().get());
                    if (result != nullptr) {
                        // 位图求交
                        _row_bitmap &= *result->get_data_bitmap();
                        // 完全计算过的表达式直接移除
                        it = _common_expr_ctxs_push_down.erase(it);
                    }
                } else {
                    ++it;
                }
            }
            _opts.condition_cache_digest =
                    _common_expr_ctxs_push_down.empty() ? 0 : _opts.condition_cache_digest;
            _opts.stats->rows_inverted_index_filtered += (input_rows - _row_bitmap.cardinality());
            // 2. IO 读取优化：跳过全满足列的索引读取
            // 核心优化：检查某个列 cid 上的所有谓词条件是否均已被倒排索引 100% 精确过滤。
            // 效果：如果是，说明剩余在 _row_bitmap 中的所有行已经绝对满足该列的谓词条件，因此将 _need_read_data_indices[cid] 标记为 false，后续在第一阶段谓词列过滤读取时，可以直接跳过读取该列的磁盘数据 Page！
            for (auto cid : _schema->column_ids()) {
                bool result_true = _check_all_conditions_passed_inverted_index_for_column(cid);
                if (result_true) {
                    _need_read_data_indices[cid] = false;
                }
            }
        }
    }

    DBUG_EXECUTE_IF("segment_iterator.inverted_index.filtered_rows", {
        LOG(INFO) << "Debug Point: segment_iterator.inverted_index.filtered_rows: "
                  << _opts.stats->rows_inverted_index_filtered;
        auto filtered_rows = DebugPoints::instance()->get_debug_param_or_default<int32_t>(
                "segment_iterator.inverted_index.filtered_rows", "filtered_rows", -1);
        if (filtered_rows != _opts.stats->rows_inverted_index_filtered) {
            return Status::Error<ErrorCode::INTERNAL_ERROR>(
                    "filtered_rows: {} not equal to expected: {}",
                    _opts.stats->rows_inverted_index_filtered, filtered_rows);
        }
    })

    DBUG_EXECUTE_IF("segment_iterator.apply_inverted_index", {
        LOG(INFO) << "Debug Point: segment_iterator.apply_inverted_index";
        if (!_common_expr_ctxs_push_down.empty() || !_col_predicates.empty()) {
            return Status::Error<ErrorCode::INTERNAL_ERROR>(
                    "it is failed to apply inverted index, common_expr_ctxs_push_down: {}, "
                    "col_predicates: {}",
                    _common_expr_ctxs_push_down.size(), _col_predicates.size());
        }
    })
    // 3. 统计索引与谓词过滤（ZoneMap, Bloom Filter, Dict Filter）
    // 触发基于 Segment 块级/Page 级统计索引的过滤，包含：
    // ZoneMap 索引：按 Min/Max 范围裁剪 Page。
    // Bloom Filter 索引：快速排除不包含目标 Key 的 Page。
    // Runtime Filter / TopN Filter：运行时下推的动态过滤条件。
    // Delete Condition：物理级 Delete 过滤条件。
    if (!_row_bitmap.isEmpty() &&
        (!_opts.topn_filter_source_node_ids.empty() || !_opts.col_id_to_predicates.empty() ||
         _opts.delete_condition_predicates->num_of_column_predicate() > 0 ||
         !_common_expr_ctxs_push_down.empty())) {
        RowRanges condition_row_ranges = RowRanges::create_single(_segment->num_rows());
        RETURN_IF_ERROR(_get_row_ranges_from_conditions(&condition_row_ranges));
        size_t pre_size = _row_bitmap.cardinality();
        // 将计算出的 condition_row_ranges 转换为位图后，再次与 _row_bitmap 执行 &= 求交集，并更新统计计数
        _row_bitmap &= RowRanges::ranges_to_roaring(condition_row_ranges);
        _opts.stats->rows_conditions_filtered += (pre_size - _row_bitmap.cardinality());
    }

    DBUG_EXECUTE_IF("bloom_filter_must_filter_data", {
        if (_opts.stats->rows_bf_filtered == 0) {
            return Status::Error<ErrorCode::INTERNAL_ERROR>(
                    "Bloom filter did not filter the data.");
        }
    })

    // TODO(hkp): calculate filter rate to decide whether to
    // use zone map/bloom filter/secondary index or not.
    return Status::OK();
}

bool SegmentIterator::_column_has_ann_index(int32_t cid) {
    bool has_ann_index = _index_iterators[cid] != nullptr &&
                         _index_iterators[cid]->get_reader(AnnIndexReaderType::ANN);

    return has_ann_index;
}

Status SegmentIterator::_apply_ann_topn_predicate() {
    if (_ann_topn_runtime == nullptr) {
        return Status::OK();
    }

    VLOG_DEBUG << fmt::format("Try apply ann topn: {}", _ann_topn_runtime->debug_string());
    size_t src_col_idx = _ann_topn_runtime->get_src_column_idx();
    // AnnTopNRuntime keeps VSlotRef::column_id(), which is the scan schema ordinal.
    ColumnId src_cid = _schema->column_id(src_col_idx);
    IndexIterator* ann_index_iterator = _index_iterators[src_cid].get();
    bool has_ann_index = _column_has_ann_index(src_cid);
    bool has_common_expr_push_down = !_common_expr_ctxs_push_down.empty();
    bool has_column_predicate = std::any_of(_is_pred_column.begin(), _is_pred_column.end(),
                                            [](bool is_pred) { return is_pred; });
    if (!has_ann_index || has_common_expr_push_down || has_column_predicate) {
        VLOG_DEBUG << fmt::format(
                "Ann topn can not be evaluated by ann index, has_ann_index: {}, "
                "has_common_expr_push_down: {}, has_column_predicate: {}",
                has_ann_index, has_common_expr_push_down, has_column_predicate);
        // Disable index-only scan on ann indexed column.
        _need_read_data_indices[src_cid] = true;
        _opts.stats->ann_fall_back_brute_force_cnt += 1;
        return Status::OK();
    }

    // Process asc & desc according to the type of metric
    auto index_reader = ann_index_iterator->get_reader(AnnIndexReaderType::ANN);
    auto ann_index_reader = dynamic_cast<AnnIndexReader*>(index_reader.get());
    DCHECK(ann_index_reader != nullptr);
    if (ann_index_reader->get_metric_type() == AnnIndexMetric::IP) {
        if (_ann_topn_runtime->is_asc()) {
            VLOG_DEBUG << fmt::format(
                    "Asc topn for inner product can not be evaluated by ann index");
            // Disable index-only scan on ann indexed column.
            _need_read_data_indices[src_cid] = true;
            _opts.stats->ann_fall_back_brute_force_cnt += 1;
            return Status::OK();
        }
    } else {
        if (!_ann_topn_runtime->is_asc()) {
            VLOG_DEBUG << fmt::format("Desc topn for l2/cosine can not be evaluated by ann index");
            // Disable index-only scan on ann indexed column.
            _need_read_data_indices[src_cid] = true;
            _opts.stats->ann_fall_back_brute_force_cnt += 1;
            return Status::OK();
        }
    }

    if (ann_index_reader->get_metric_type() != _ann_topn_runtime->get_metric_type()) {
        VLOG_DEBUG << fmt::format(
                "Ann topn metric type {} not match index metric type {}, can not be evaluated "
                "by "
                "ann index",
                metric_to_string(_ann_topn_runtime->get_metric_type()),
                metric_to_string(ann_index_reader->get_metric_type()));
        // Disable index-only scan on ann indexed column.
        _need_read_data_indices[src_cid] = true;
        _opts.stats->ann_fall_back_brute_force_cnt += 1;
        return Status::OK();
    }

    size_t pre_size = _row_bitmap.cardinality();
    size_t rows_of_segment = _segment->num_rows();
    const auto& user_params = _ann_topn_runtime->user_params();
    if (user_params.should_fallback_ann_index_by_small_candidate(pre_size, rows_of_segment)) {
        VLOG_DEBUG << fmt::format(
                "Ann topn predicate input rows {} reach small candidate threshold, "
                "rows_of_segment: {}, absolute_threshold: {}, percent_threshold: {}, "
                "will not use ann index to filter",
                pre_size, rows_of_segment, user_params.ann_index_candidate_rows_threshold,
                user_params.ann_index_candidate_rows_percent_threshold);
        // Disable index-only scan on ann indexed column.
        _need_read_data_indices[src_cid] = true;
        _opts.stats->ann_fall_back_brute_force_cnt += 1;
        _opts.stats->ann_topn_fallback_by_small_candidate_cnt += 1;
        _opts.stats->ann_topn_fallback_small_candidate_rows += pre_size;
        return Status::OK();
    }
    IColumn::MutablePtr result_column;
    std::shared_ptr<std::vector<uint64_t>> result_row_ids;
    segment_v2::AnnIndexStats ann_index_stats;

    // Try to load ANN index before search
    auto ann_index_iterator_casted =
            dynamic_cast<segment_v2::AnnIndexIterator*>(ann_index_iterator);
    if (ann_index_iterator_casted == nullptr) {
        VLOG_DEBUG << "Failed to cast index iterator to AnnIndexIterator, fallback to brute force";
        _need_read_data_indices[src_cid] = true;
        _opts.stats->ann_fall_back_brute_force_cnt += 1;
        return Status::OK();
    }

    // Track load index timing
    {
        SCOPED_TIMER(&(ann_index_stats.load_index_costs_ns));
        if (!ann_index_iterator_casted->try_load_index()) {
            VLOG_DEBUG << "Failed to load ANN index, fallback to brute force search";
            _need_read_data_indices[src_cid] = true;
            _opts.stats->ann_fall_back_brute_force_cnt += 1;
            return Status::OK();
        }
        double load_costs_ms =
                static_cast<double>(ann_index_stats.load_index_costs_ns.value()) / 1000000.0;
        DorisMetrics::instance()->ann_index_load_costs_ms->increment(
                static_cast<int64_t>(load_costs_ms));
    }

    bool enable_ann_index_result_cache =
            !_opts.runtime_state ||
            !_opts.runtime_state->query_options().__isset.enable_ann_index_result_cache ||
            _opts.runtime_state->query_options().enable_ann_index_result_cache;
    RETURN_IF_ERROR(_ann_topn_runtime->evaluate_vector_ann_search(
            ann_index_iterator_casted, &_row_bitmap, rows_of_segment, enable_ann_index_result_cache,
            result_column, result_row_ids, ann_index_stats));

    VLOG_DEBUG << fmt::format("Ann topn filtered {} - {} = {} rows", pre_size,
                              _row_bitmap.cardinality(), pre_size - _row_bitmap.cardinality());

    int64_t rows_filterd = pre_size - _row_bitmap.cardinality();
    _opts.stats->rows_ann_index_topn_filtered += rows_filterd;
    _opts.stats->ann_index_load_ns += ann_index_stats.load_index_costs_ns.value();
    _opts.stats->ann_topn_search_ns += ann_index_stats.search_costs_ns.value();
    _opts.stats->ann_ivf_on_disk_load_ns += ann_index_stats.ivf_on_disk_load_costs_ns.value();
    _opts.stats->ann_ivf_on_disk_cache_hit_cnt += ann_index_stats.ivf_on_disk_cache_hit_cnt.value();
    _opts.stats->ann_ivf_on_disk_cache_miss_cnt +=
            ann_index_stats.ivf_on_disk_cache_miss_cnt.value();
    _opts.stats->ann_index_topn_engine_search_ns += ann_index_stats.engine_search_ns.value();
    _opts.stats->ann_index_topn_result_process_ns +=
            ann_index_stats.result_process_costs_ns.value();
    _opts.stats->ann_index_topn_engine_convert_ns += ann_index_stats.engine_convert_ns.value();
    _opts.stats->ann_index_topn_engine_prepare_ns += ann_index_stats.engine_prepare_ns.value();
    _opts.stats->ann_index_topn_search_cnt += 1;
    _opts.stats->ann_index_cache_hits += ann_index_stats.topn_cache_hits.value();
    const size_t dst_col_idx = _ann_topn_runtime->get_dest_column_idx();
    ColumnIterator* column_iter = _column_iterators[_schema->column_id(dst_col_idx)].get();
    DCHECK(column_iter != nullptr);
    VirtualColumnIterator* virtual_column_iter = dynamic_cast<VirtualColumnIterator*>(column_iter);
    DCHECK(virtual_column_iter != nullptr);
    VLOG_DEBUG << fmt::format(
            "Virtual column iterator, column_idx {}, is materialized with {} rows", dst_col_idx,
            result_row_ids->size());
    // reference count of result_column should be 1, so move will not issue any data copy.
    virtual_column_iter->prepare_materialization(std::move(result_column), result_row_ids);

    _need_read_data_indices[src_cid] = false;
    VLOG_DEBUG << fmt::format(
            "Enable ANN index-only scan for src column cid {} (skip reading data pages)", src_cid);

    return Status::OK();
}

Status SegmentIterator::_get_row_ranges_from_conditions(RowRanges* condition_row_ranges) {
    std::set<int32_t> cids;
    for (auto& entry : _opts.col_id_to_predicates) {
        cids.insert(entry.first);
    }

    {
        SCOPED_RAW_TIMER(&_opts.stats->generate_row_ranges_by_dict_ns);
        /// Low cardinality optimization is currently not very stable, so to prevent data corruption,
        /// we are temporarily disabling its use in data compaction.
        // TODO: enable it in not only ReaderTyper::READER_QUERY but also other reader types.
        if (_opts.io_ctx.reader_type == ReaderType::READER_QUERY) {
            RowRanges dict_row_ranges = RowRanges::create_single(num_rows());
            for (auto cid : cids) {
                if (!_segment->can_apply_predicate_safely(
                            cid, *_schema, _opts.target_cast_type_for_variants, _opts)) {
                    continue;
                }
                DCHECK(_opts.col_id_to_predicates.count(cid) > 0);
                RETURN_IF_ERROR(_column_iterators[cid]->get_row_ranges_by_dict(
                        _opts.col_id_to_predicates.at(cid).get(), &dict_row_ranges));

                if (dict_row_ranges.is_empty()) {
                    break;
                }
            }

            if (dict_row_ranges.is_empty()) {
                RowRanges::ranges_intersection(*condition_row_ranges, dict_row_ranges,
                                               condition_row_ranges);
                _opts.stats->segment_dict_filtered++;
                _opts.stats->filtered_segment_number++;
                return Status::OK();
            }
        }
    }

    size_t pre_size = 0;
    {
        SCOPED_RAW_TIMER(&_opts.stats->generate_row_ranges_by_bf_ns);
        // first filter data by bloom filter index
        // bloom filter index only use CondColumn
        RowRanges bf_row_ranges = RowRanges::create_single(num_rows());
        for (auto& cid : cids) {
            DCHECK(_opts.col_id_to_predicates.count(cid) > 0);
            if (!_segment->can_apply_predicate_safely(cid, *_schema,
                                                      _opts.target_cast_type_for_variants, _opts)) {
                continue;
            }
            // get row ranges by bf index of this column,
            RowRanges column_bf_row_ranges = RowRanges::create_single(num_rows());
            RETURN_IF_ERROR(_column_iterators[cid]->get_row_ranges_by_bloom_filter(
                    _opts.col_id_to_predicates.at(cid).get(), &column_bf_row_ranges));
            RowRanges::ranges_intersection(bf_row_ranges, column_bf_row_ranges, &bf_row_ranges);
        }

        pre_size = condition_row_ranges->count();
        RowRanges::ranges_intersection(*condition_row_ranges, bf_row_ranges, condition_row_ranges);
        _opts.stats->rows_bf_filtered += (pre_size - condition_row_ranges->count());
    }

    {
        SCOPED_RAW_TIMER(&_opts.stats->generate_row_ranges_by_zonemap_ns);
        RowRanges zone_map_row_ranges = RowRanges::create_single(num_rows());
        // second filter data by zone map
        for (const auto& cid : cids) {
            DCHECK(_opts.col_id_to_predicates.count(cid) > 0);
            if (!_segment->can_apply_predicate_safely(cid, *_schema,
                                                      _opts.target_cast_type_for_variants, _opts)) {
                continue;
            }
            if (_segment->is_tso_placeholder_col(cid, *_schema, _opts)) {
                // skip untrustworthy tso placeholder zonemap
                // if possible already be pruned as a whole before,
                // so just skip
                continue;
            }
            // do not check zonemap if predicate does not support zonemap
            if (!_opts.col_id_to_predicates.at(cid)->support_zonemap()) {
                VLOG_DEBUG << "skip zonemap for column " << cid;
                continue;
            }
            // get row ranges by zone map of this column,
            RowRanges column_row_ranges = RowRanges::create_single(num_rows());
            RETURN_IF_ERROR(_column_iterators[cid]->get_row_ranges_by_zone_map(
                    _opts.col_id_to_predicates.at(cid).get(),
                    _opts.del_predicates_for_zone_map.count(cid) > 0
                            ? &(_opts.del_predicates_for_zone_map.at(cid))
                            : nullptr,
                    &column_row_ranges));
            // intersect different columns's row ranges to get final row ranges by zone map
            RowRanges::ranges_intersection(zone_map_row_ranges, column_row_ranges,
                                           &zone_map_row_ranges);
        }

        pre_size = condition_row_ranges->count();
        RowRanges::ranges_intersection(*condition_row_ranges, zone_map_row_ranges,
                                       condition_row_ranges);

        size_t pre_size2 = condition_row_ranges->count();
        RowRanges::ranges_intersection(*condition_row_ranges, zone_map_row_ranges,
                                       condition_row_ranges);
        _opts.stats->rows_stats_rp_filtered += (pre_size2 - condition_row_ranges->count());
        _opts.stats->rows_stats_filtered += (pre_size - condition_row_ranges->count());
    }

    {
        SCOPED_RAW_TIMER(&_opts.stats->generate_row_ranges_by_zonemap_ns);
        if (!_common_expr_ctxs_push_down.empty()) {
            const auto pre_expr_zonemap_size = condition_row_ranges->count();
            RETURN_IF_ERROR(_apply_expr_zonemap_to_row_ranges(_common_expr_ctxs_push_down, 0,
                                                              condition_row_ranges));
            _opts.stats->rows_stats_filtered +=
                    (pre_expr_zonemap_size - condition_row_ranges->count());
        }
    }

    return Status::OK();
}

bool SegmentIterator::_is_literal_node(const TExprNodeType::type& node_type) {
    switch (node_type) {
    case TExprNodeType::BOOL_LITERAL:
    case TExprNodeType::INT_LITERAL:
    case TExprNodeType::LARGE_INT_LITERAL:
    case TExprNodeType::FLOAT_LITERAL:
    case TExprNodeType::DECIMAL_LITERAL:
    case TExprNodeType::STRING_LITERAL:
    case TExprNodeType::DATE_LITERAL:
    case TExprNodeType::TIMEV2_LITERAL:
        return true;
    default:
        return false;
    }
}

Status SegmentIterator::_extract_common_expr_columns(const VExprSPtr& expr) {
    auto& children = expr->children();
    for (int i = 0; i < children.size(); ++i) {
        RETURN_IF_ERROR(_extract_common_expr_columns(children[i]));
    }

    auto node_type = expr->node_type();
    if (node_type == TExprNodeType::SLOT_REF) {
        auto slot_expr = std::dynamic_pointer_cast<doris::VSlotRef>(expr);
        auto cid = _schema->column_id(slot_expr->column_id());
        _is_common_expr_column[cid] = true;
        _common_expr_columns.insert(cid);
    } else if (node_type == TExprNodeType::VIRTUAL_SLOT_REF) {
        std::shared_ptr<VirtualSlotRef> virtual_slot_ref =
                std::dynamic_pointer_cast<VirtualSlotRef>(expr);
        RETURN_IF_ERROR(_extract_common_expr_columns(virtual_slot_ref->get_virtual_column_expr()));
    }

    return Status::OK();
}

bool SegmentIterator::_check_apply_by_inverted_index(std::shared_ptr<ColumnPredicate> pred) {
    if (_opts.runtime_state && !_opts.runtime_state->query_options().enable_inverted_index_query) {
        return false;
    }
    auto pred_column_id = pred->column_id();
    if (_index_iterators[pred_column_id] == nullptr) {
        //this column without inverted index
        return false;
    }

    if (_inverted_index_not_support_pred_type(pred->type())) {
        return false;
    }

    if (pred->type() == PredicateType::IN_LIST || pred->type() == PredicateType::NOT_IN_LIST) {
        // in_list or not_in_list predicate produced by runtime filter
        if (pred->is_runtime_filter()) {
            return false;
        }
    }

    // UNTOKENIZED strings exceed ignore_above, they are written as null, causing range query errors
    if (PredicateTypeTraits::is_range(pred->type()) &&
        !IndexReaderHelper::has_bkd_index(_index_iterators[pred_column_id].get())) {
        return false;
    }

    // Function filter no apply inverted index
    if (dynamic_cast<LikeColumnPredicate*>(pred.get()) != nullptr) {
        return false;
    }

    bool handle_by_fulltext = _column_has_fulltext_index(pred_column_id);
    if (handle_by_fulltext) {
        // when predicate is leafNode of andNode,
        // can apply 'match query' and 'equal query' and 'list query' for fulltext index.
        return pred->type() == PredicateType::MATCH || pred->type() == PredicateType::IS_NULL ||
               pred->type() == PredicateType::IS_NOT_NULL ||
               PredicateTypeTraits::is_equal_or_list(pred->type());
    }

    return true;
}

// TODO: optimization when all expr can not evaluate by inverted/ann index,
Status SegmentIterator::_apply_index_expr() {
    bool enable_ann_index_result_cache =
            !_opts.runtime_state ||
            !_opts.runtime_state->query_options().__isset.enable_ann_index_result_cache ||
            _opts.runtime_state->query_options().enable_ann_index_result_cache;

    for (const auto& expr_ctx : _common_expr_ctxs_push_down) {
        if (Status st = expr_ctx->evaluate_inverted_index(num_rows()); !st.ok()) {
            if (_downgrade_without_index(st) || st.code() == ErrorCode::NOT_IMPLEMENTED_ERROR) {
                continue;
            } else {
                // other code is not to be handled, we should just break
                LOG(WARNING) << "failed to evaluate inverted index for expr_ctx: "
                             << expr_ctx->root()->debug_string()
                             << ", error msg: " << st.to_string();
                return st;
            }
        }
    }

    // Evaluate inverted index for virtual column MATCH expressions (projections).
    // Unlike common exprs which filter rows, these only compute index result bitmaps
    // for later materialization via fast_execute().
    for (auto& [cid, expr_ctx] : _virtual_column_exprs) {
        if (expr_ctx->get_index_context() == nullptr) {
            continue;
        }
        if (Status st = expr_ctx->evaluate_inverted_index(num_rows()); !st.ok()) {
            if (_downgrade_without_index(st) || st.code() == ErrorCode::NOT_IMPLEMENTED_ERROR) {
                continue;
            } else {
                LOG(WARNING) << "failed to evaluate inverted index for virtual column expr: "
                             << expr_ctx->root()->debug_string()
                             << ", error msg: " << st.to_string();
                return st;
            }
        }
    }

    // Apply ann range search
    for (const auto& expr_ctx : _common_expr_ctxs_push_down) {
        segment_v2::AnnIndexStats ann_index_stats;
        size_t origin_rows = _row_bitmap.cardinality();
        bool ann_range_search_executed = false;
        RETURN_IF_ERROR(expr_ctx->evaluate_ann_range_search(
                _index_iterators, _schema->column_ids(), _column_iterators,
                _common_expr_to_slotref_map, num_rows(), _row_bitmap, ann_index_stats,
                enable_ann_index_result_cache, &ann_range_search_executed));
        if (ann_range_search_executed) {
            _opts.stats->ann_index_range_search_cnt++;
        }
        _opts.stats->rows_ann_index_range_filtered += (origin_rows - _row_bitmap.cardinality());
        _opts.stats->ann_index_load_ns += ann_index_stats.load_index_costs_ns.value();
        _opts.stats->ann_index_range_search_ns += ann_index_stats.search_costs_ns.value();
        _opts.stats->ann_ivf_on_disk_load_ns += ann_index_stats.ivf_on_disk_load_costs_ns.value();
        _opts.stats->ann_ivf_on_disk_cache_hit_cnt +=
                ann_index_stats.ivf_on_disk_cache_hit_cnt.value();
        _opts.stats->ann_ivf_on_disk_cache_miss_cnt +=
                ann_index_stats.ivf_on_disk_cache_miss_cnt.value();
        _opts.stats->ann_range_engine_search_ns += ann_index_stats.engine_search_ns.value();
        _opts.stats->ann_range_result_convert_ns += ann_index_stats.result_process_costs_ns.value();
        _opts.stats->ann_range_engine_convert_ns += ann_index_stats.engine_convert_ns.value();
        _opts.stats->ann_range_pre_process_ns += ann_index_stats.engine_prepare_ns.value();
        _opts.stats->ann_fall_back_brute_force_cnt += ann_index_stats.fall_back_brute_force_cnt;
        _opts.stats->ann_range_fallback_by_small_candidate_cnt +=
                ann_index_stats.range_fallback_by_small_candidate_cnt;
        _opts.stats->ann_range_fallback_small_candidate_rows +=
                ann_index_stats.range_fallback_small_candidate_rows;
        _opts.stats->ann_index_range_cache_hits += ann_index_stats.range_cache_hits.value();
    }

    return Status::OK();
}

bool SegmentIterator::_downgrade_without_index(Status res, bool need_remaining) {
    bool is_fallback =
            _opts.runtime_state->query_options().enable_fallback_on_missing_inverted_index;
    if ((res.code() == ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND && is_fallback) ||
        res.code() == ErrorCode::INVERTED_INDEX_BYPASS ||
        res.code() == ErrorCode::INVERTED_INDEX_EVALUATE_SKIPPED ||
        (res.code() == ErrorCode::INVERTED_INDEX_NO_TERMS && need_remaining) ||
        res.code() == ErrorCode::INVERTED_INDEX_FILE_CORRUPTED) {
        // 1. INVERTED_INDEX_FILE_NOT_FOUND means index file has not been built,
        //    usually occurs when creating a new index, queries can be downgraded
        //    without index.
        // 2. INVERTED_INDEX_BYPASS means the hit of condition by index
        //    has reached the optimal limit, downgrade without index query can
        //    improve query performance.
        // 3. INVERTED_INDEX_EVALUATE_SKIPPED means the inverted index is not
        //    suitable for executing this predicate, skipped it and filter data
        //    by function later.
        // 4. INVERTED_INDEX_NO_TERMS means the column has fulltext index,
        //    but the column condition value no terms in specified parser,
        //    such as: where A = '' and B = ','
        //    the predicate of A and B need downgrade without index query.
        // 5. INVERTED_INDEX_FILE_CORRUPTED means the index file is corrupted,
        //    such as when index segment files are not generated
        // above case can downgrade without index query
        _opts.stats->inverted_index_downgrade_count++;
        if (!res.is<ErrorCode::INVERTED_INDEX_BYPASS>()) {
            LOG(INFO) << "will downgrade without index to evaluate predicate, because of res: "
                      << res;
        } else {
            VLOG_DEBUG << "will downgrade without index to evaluate predicate, because of res: "
                       << res;
        }
        return true;
    }
    return false;
}

bool SegmentIterator::_column_has_fulltext_index(int32_t cid) {
    bool has_fulltext_index =
            _index_iterators[cid] != nullptr &&
            _index_iterators[cid]->get_reader(InvertedIndexReaderType::FULLTEXT) &&
            _index_iterators[cid]->get_reader(InvertedIndexReaderType::STRING_TYPE) == nullptr;

    return has_fulltext_index;
}

inline bool SegmentIterator::_inverted_index_not_support_pred_type(const PredicateType& type) {
    return type == PredicateType::BF;
}

Status SegmentIterator::_apply_inverted_index_on_column_predicate(
        std::shared_ptr<ColumnPredicate> pred,
        std::vector<std::shared_ptr<ColumnPredicate>>& remaining_predicates, bool* continue_apply) {
    if (!_check_apply_by_inverted_index(pred)) {
        remaining_predicates.emplace_back(pred);
    } else {
        bool need_remaining_after_evaluate = _column_has_fulltext_index(pred->column_id()) &&
                                             PredicateTypeTraits::is_equal_or_list(pred->type());
        Status res =
                pred->evaluate(_storage_name_and_type[pred->column_id()],
                               _index_iterators[pred->column_id()].get(), num_rows(), &_row_bitmap);
        if (!res.ok()) {
            if (_downgrade_without_index(res, need_remaining_after_evaluate)) {
                remaining_predicates.emplace_back(pred);
                return Status::OK();
            }
            LOG(WARNING) << "failed to evaluate index"
                         << ", column predicate type: " << pred->pred_type_string(pred->type())
                         << ", error msg: " << res;
            return res;
        }

        if (_row_bitmap.isEmpty()) {
            // all rows have been pruned, no need to process further predicates
            *continue_apply = false;
        }

        if (need_remaining_after_evaluate) {
            remaining_predicates.emplace_back(pred);
            return Status::OK();
        }
        if (!pred->is_runtime_filter()) {
            _column_predicate_index_exec_status[pred->column_id()][pred] = true;
        }
    }
    return Status::OK();
}

bool SegmentIterator::_need_read_data(ColumnId cid) {
    if (_opts.runtime_state && !_opts.runtime_state->query_options().enable_no_need_read_data_opt) {
        return true;
    }
    if (_can_skip_reading_extra_column(cid)) {
        return false;
    }
    // only support DUP_KEYS and UNIQUE_KEYS with MOW
    if (!((_opts.tablet_schema->keys_type() == KeysType::DUP_KEYS ||
           (_opts.tablet_schema->keys_type() == KeysType::UNIQUE_KEYS &&
            _opts.enable_unique_key_merge_on_write)))) {
        return true;
    }
    // this is a virtual column, we always need to read data
    if (_virtual_column_exprs.contains(cid)) {
        return true;
    }

    // if there is a delete predicate, we always need to read data
    if (_has_delete_predicate(cid)) {
        return true;
    }
    if (_output_columns.count(-1)) {
        // if _output_columns contains -1, it means that the light
        // weight schema change may not be enabled or other reasons
        // caused the column unique_id not be set, to prevent errors
        // occurring, return true here that column data needs to be read
        return true;
    }
    const auto& column = _opts.tablet_schema->column(cid);
    // Different subcolumns may share the same parent_unique_id, so we choose to abandon this optimization.
    if (column.is_extracted_column() &&
        _opts.push_down_agg_type_opt != TPushAggOp::COUNT_ON_INDEX) {
        return true;
    }
    int32_t unique_id = column.unique_id();
    if (unique_id < 0) {
        unique_id = column.parent_unique_id();
    }
    // A column can skip data reads when its predicates have already been fully resolved.
    // zonemap_always_true_pred_cols is produced only for non-key columns because key columns
    // must remain readable for short-key range seeks.
    const bool used_by_common_expr =
            cid < _is_common_expr_column.size() && _is_common_expr_column[cid];
    const bool zonemap_always_true_filter_column =
            _opts.zonemap_always_true_pred_cols.contains(cid);
    DCHECK(!zonemap_always_true_filter_column || !column.is_key());
    const bool no_need_read_filter_column =
            (_need_read_data_indices.contains(cid) && !_need_read_data_indices[cid]) ||
            (zonemap_always_true_filter_column && !used_by_common_expr);
    if ((no_need_read_filter_column && !_output_columns.contains(unique_id)) ||
        (no_need_read_filter_column && _output_columns.count(unique_id) == 1 &&
         _opts.push_down_agg_type_opt == TPushAggOp::COUNT_ON_INDEX)) {
        VLOG_DEBUG << "SegmentIterator no need read data for column: "
                   << _opts.tablet_schema->column_by_uid(unique_id).name();
        return false;
    }
    return true;
}

Status SegmentIterator::_apply_inverted_index() {
    std::vector<std::shared_ptr<ColumnPredicate>> remaining_predicates;
    std::set<std::shared_ptr<ColumnPredicate>> no_need_to_pass_column_predicate_set;

    for (auto pred : _col_predicates) {
        if (no_need_to_pass_column_predicate_set.count(pred) > 0) {
            continue;
        } else {
            bool continue_apply = true;
            RETURN_IF_ERROR(_apply_inverted_index_on_column_predicate(pred, remaining_predicates,
                                                                      &continue_apply));
            if (!continue_apply) {
                break;
            }
        }
    }

    _col_predicates = std::move(remaining_predicates);
    return Status::OK();
}

/**
 * @brief Checks if all conditions related to a specific column have passed in both
 * `_column_predicate_inverted_index_status` and `_common_expr_inverted_index_status`.
 *
 * This function first checks the conditions in `_column_predicate_inverted_index_status`
 * for the given `ColumnId`. If all conditions pass, it sets `default_return` to `true`.
 * It then checks the conditions in `_common_expr_inverted_index_status` for the same column.
 *
 * The function returns `true` if all conditions in both maps pass. If any condition fails
 * in either map, the function immediately returns `false`. If the column does not exist
 * in one of the maps, the function returns `default_return`.
 *
 * @param cid The ColumnId of the column to check.
 * @param default_return The default value to return if the column is not found in the status maps.
 * @return true if all conditions in both status maps pass, or if the column is not found
 *         and `default_return` is true.
 * @return false if any condition in either status map fails, or if the column is not found
 *         and `default_return` is false.
 */
bool SegmentIterator::_check_all_conditions_passed_inverted_index_for_column(ColumnId cid,
                                                                             bool default_return) {
    auto pred_it = _column_predicate_index_exec_status.find(cid);
    if (pred_it != _column_predicate_index_exec_status.end()) {
        const auto& pred_map = pred_it->second;
        bool pred_passed = std::all_of(pred_map.begin(), pred_map.end(),
                                       [](const auto& pred_entry) { return pred_entry.second; });
        if (!pred_passed) {
            return false;
        } else {
            default_return = true;
        }
    }

    auto expr_it = _common_expr_index_exec_status.find(cid);
    if (expr_it != _common_expr_index_exec_status.end()) {
        const auto& expr_map = expr_it->second;
        return std::all_of(expr_map.begin(), expr_map.end(),
                           [](const auto& expr_entry) { return expr_entry.second; });
    }
    return default_return;
}
// Apache Doris BE 中负责按 Schema 初始化所有列迭代器（ColumnIterator）的物理构建逻辑。
// 根据请求的 Schema 遍历每一列，识别该列是常规物理列还是各种特殊/虚拟列，并为其创建并初始化正确的迭代器句柄。
Status SegmentIterator::_init_return_column_iterators() {
    // 耗时统计：使用 SCOPED_RAW_TIMER 将当前初始化的 CPU/IO 耗时记录到 Profile 中，方便分析 Query 性能。
    SCOPED_RAW_TIMER(&_opts.stats->segment_iterator_init_return_column_iterators_timer_ns);
    // 空数据熔断：如果当前 Segment 的起始游标已经超过总行数，说明当前 Segment 为空或无有效数据，直接返回，避免不必要的对象初始化开销。
    if (_cur_rowid >= num_rows()) {
        return Status::OK();
    }

    for (auto cid : _schema->column_ids()) {
        // 特殊列 Handling：单 Tablet 内物理 RowID 列 (ROWID_COL)
        // 作用：当查询需要显式读取物理行号（如 Update / Delete / Unique Key Merge-on-Write 机制）时，触发生成 RowIdColumnIterator。
        // 底层特点：该迭代器并不对应物理磁盘上存储的真实 Page 列，而是在读取时根据 (tablet_id, rowset_id, segment_id, ordinal_id) 内存动态计算组装出 64 位全局唯一的 RowID 字段。
        if (_schema->column(cid)->name() == BeConsts::ROWID_COL) {
            _column_iterators[cid].reset(
                    new RowIdColumnIterator(_opts.tablet_id, _opts.rowset_id, _segment->id()));
            continue;
        }
        // 特殊列 Handling：跨节点全局 RowID 列 (GLOBAL_ROWID_COL)
        // 作用：分布式/全球唯一 RowID（常用在分布式索引、点查或全局位图追溯场景）。
        // 从 runtime_state 的 id_file_map 中获取 (tablet_id, rowset_id, segment_id) 到短整型 file_id 的压缩映射，实例化 RowIdColumnIteratorV2，减少全局标识符占用的内存与传输带宽。
        if (_schema->column(cid)->name().starts_with(BeConsts::GLOBAL_ROWID_COL)) {
            auto& id_file_map = _opts.runtime_state->get_id_file_map();
            uint32_t file_id = id_file_map->get_file_mapping_id(std::make_shared<FileMapping>(
                    _opts.tablet_id, _opts.rowset_id, _segment->id()));
            _column_iterators[cid].reset(new RowIdColumnIteratorV2(
                    IdManager::ID_VERSION, BackendOptions::get_backend_id(), file_id));
            continue;
        }
        // 特殊列 Handling：虚拟列/计算列 (VIRTUAL_COLUMN_PREFIX)
        // 作用：处理类似全文检索相关性得分（SCORE 列）、Variant 类型动态展开的虚列或高阶函数生成的虚拟列。
        if (_schema->column(cid)->name().starts_with(BeConsts::VIRTUAL_COLUMN_PREFIX)) {
            _column_iterators[cid] = std::make_unique<VirtualColumnIterator>();
            continue;
        }
        // 常规物理列初始化与谓词标记
        // 目的：统计哪些列参与了谓词条件（Filter Predicate）或删除条件（Delete Condition），标记在 tmp_is_pred_column 位图数组中。
        std::set<ColumnId> del_cond_id_set;
        _opts.delete_condition_predicates->get_all_column_ids(del_cond_id_set);
        std::vector<bool> tmp_is_pred_column;
        tmp_is_pred_column.resize(_schema->columns().size(), false);
        // 在 Doris 的列式存储设计中，如果一列属于“谓词列”，下层 ColumnIterator 可能会触发特殊的优化策略
        // （例如：优先读取该列的最后一个 Page，检查该列在整个 Segment 内是否达到了全字典编码 full dict encoding。如果达到了全字典编码，过滤谓词就能直接转化为针对 Integer Code 的极速数值比较）。
        for (auto predicate : _col_predicates) {
            auto p_cid = predicate->column_id();
            tmp_is_pred_column[p_cid] = true;
        }
        // handle delete_condition
        for (auto d_cid : del_cond_id_set) {
            tmp_is_pred_column[d_cid] = true;
        }
        // 物理列迭代器创建与物理 init
        // 惰性创建（Lazy Reset）：只有未初始化的列（nullptr）才真正实例化，避免重复创建。
        if (_column_iterators[cid] == nullptr) {
            // new_column_iterator：通过 Segment 工厂方法根据列的数据类型（Scalar、Array、Map、Struct、Variant 等）选择并构造对应的物理 ColumnIterator（如 FileColumnIterator 或嵌套类型的 ArrayColumnIterator）。
            // 同时支持 Variant 稀疏列缓存（_variant_sparse_column_cache）。
            RETURN_IF_ERROR(_segment->new_column_iterator(_opts.tablet_schema->column(cid),
                                                          &_column_iterators[cid], &_opts,
                                                          &_variant_sparse_column_cache));
            ColumnIteratorOptions iter_opts {
                    .use_page_cache = _opts.use_page_cache,
                    // If the col is predicate column, then should read the last page to check
                    // if the column is full dict encoding
                    .is_predicate_column = tmp_is_pred_column[cid],
                    .file_reader = _file_reader.get(),
                    .stats = _opts.stats,
                    .io_ctx = _opts.io_ctx,
            };
            // 传递 ColumnIteratorOptions：将 use_page_cache（PageCache 开关）、is_predicate_column、底层 file_reader 以及 IO 上下文传给列迭代器
            // 并调用 init(iter_opts) 完成物理 Page Index、Dict Page 的加载与解压准备。
            RETURN_IF_ERROR(_column_iterators[cid]->init(iter_opts));
        }
    }
    // Debug 模式下的安全性校验
#ifndef NDEBUG
    for (const auto& entry : _virtual_column_exprs) {
        ColumnId vir_col_cid = entry.first;
        DCHECK(_column_iterators[vir_col_cid] != nullptr)
                << "Virtual column iterator for " << vir_col_cid << " should not be null";
        ColumnIterator* column_iter = _column_iterators[vir_col_cid].get();
        DCHECK(dynamic_cast<VirtualColumnIterator*>(column_iter) != nullptr)
                << "Virtual column iterator for " << vir_col_cid
                << " should be VirtualColumnIterator";
    }
#endif
    return Status::OK();
}
// Apache Doris BE 存储引擎中负责初始化二级索引/倒排索引（Inverted Index）与 ANN 向量索引迭代器的物理构建函数。
Status SegmentIterator::_init_index_iterators() {
    SCOPED_RAW_TIMER(&_opts.stats->segment_iterator_init_index_iterators_timer_ns);
    if (_cur_rowid >= num_rows()) {
        return Status::OK();
    }
    // 创建一个在当前 Segment 索引查询生命周期内共享的 IndexQueryContext 容器。
    _index_query_context = std::make_shared<IndexQueryContext>();
    _index_query_context->io_ctx = &_opts.io_ctx;
    _index_query_context->stats = _opts.stats;
    _index_query_context->runtime_state = _opts.runtime_state;
    // 打分与相关性计算：如果存在 _score_runtime（例如 BM25 全文检索打分或向量 TopN 搜索），
    // 会额外注入文档集合统计信息（collection_statistics）、相似度模型（CollectionSimilarity）、LIMIT 限制和排序方向（升序/降序），供底层的倒排/向量索引在检索时直接计算相关性分数。
    if (_score_runtime) {
        _index_query_context->collection_statistics = _opts.collection_statistics;
        _index_query_context->collection_similarity = std::make_shared<CollectionSimilarity>();
        _index_query_context->query_limit = _score_runtime->get_limit();
        _index_query_context->is_asc = _score_runtime->is_asc();
    }

    // Inverted index iterators
    // 第一阶段：倒排索引迭代器构建 (Inverted Index Iterators)
    // 重点处理了 普通物理列 与 半结构化 Variant 类型的提取子列（Extracted Columns） 两种情况：
    for (auto cid : _schema->column_ids()) {
        // Use segment’s own index_meta, for compatibility with future indexing needs to default to lowercase.
        // 解决的核心痛苦问题是：Variant 类型的 JSON 子路径（例如 a.b.c）在 Schema 中可能只是一个占位/抽取列（Extracted Column），其真实的倒排索引元数据（Index Metadata）并不直接挂在普通的 TabletSchema 顶层，
        // 而是绑定在 Variant 的父节点及其内部动态抽取的子列物理结构中。
        if (_index_iterators[cid] == nullptr) {
            // Scan-time Variant path placeholders retain the Variant storage type. Use their
            // parent unique id and path to locate the extracted column's inverted-index metadata.
            const auto& column = _opts.tablet_schema->column(cid);
            std::vector<const TabletIndex*> inverted_indexs;
            // Keep shared_ptr alive to prevent use-after-free when accessing raw pointers
            // 存放 std::shared_ptr<const TabletIndex> 的容器。
            TabletIndexes inverted_indexs_holder;
            // If the column is an extracted column, we need to find the sub-column in the parent column reader.
            std::shared_ptr<ColumnReader> column_reader;
            // 判断当前列是否为从 Variant 扩展/抽取出来的 JSON 路径列。
            // parent_unique_id()：Variant 列在磁盘上以根列（Parent Column）的形式存在，子路径列记录了父列的 Unique ID。
            if (column.is_extracted_column()) {
                // _column_reader_cache：从 Segment 的列读取器缓存中获取父列的 ColumnReader。获取失败则直接 continue（说明底层物理数据不存在，跳过索引读取）。
                if (!_segment->_column_reader_cache->get_column_reader(
                            column.parent_unique_id(), &column_reader, _opts.stats) ||
                    column_reader == nullptr) {
                    continue;
                }
                // 在 DEBUG 模式下断言并安全地转型为专用的 VariantColumnReader 指针。
                auto* variant_reader = assert_cast<VariantColumnReader*>(column_reader.get());
                // 动态数据类型推导 (infer_data_type_for_path)
                // 背景：JSON 里的某个 Field（如 a.b）在扫描初期（Scan Time）占位符类型可能默认还是 TYPE_VARIANT（未指定具体类型）。
                // 类型推导：索引的匹配与查找强依赖具体的数据类型（比如 Int32 的倒排索引与 String 的倒排索引物理结构完全不同）。
                // 此处调用 variant_reader->infer_data_type_for_path 结合当前 Segment 的实际物理 Page 元数据，推导出该路径真实的物理类型（例如推导出实际存的是 TYPE_STRING 或 TYPE_BIGINT），更新 data_type。
                DataTypePtr data_type = _storage_name_and_type[cid].second;
                if (data_type != nullptr &&
                    data_type->get_primitive_type() == PrimitiveType::TYPE_VARIANT) {
                    DataTypePtr inferred_type;
                    Status st = variant_reader->infer_data_type_for_path(
                            &inferred_type, column, _opts, _segment->_column_reader_cache.get());
                    if (st.ok() && inferred_type != nullptr) {
                        data_type = inferred_type;
                    }
                }
                // 带着子列的 Path 信息以及推导出的 data_type，在 VariantColumnReader 维护的子列索引映射表中查找匹配的倒排索引（TabletIndex）。
                inverted_indexs_holder = variant_reader->find_subcolumn_tablet_indexes(
                        column, data_type, _opts.stats);
                // Extract raw pointers from shared_ptr for iteration
                // 将 inverted_indexs_holder 中智能指针引用的物理索引元数据地址装入 inverted_indexs 列表中，供后续代码统一调用
                for (const auto& index_ptr : inverted_indexs_holder) {
                    inverted_indexs.push_back(index_ptr.get());
                }
            }
            // If the column is not an extracted column, we can directly get the inverted index metadata from the tablet schema.
            // 非 Variant 扩展列直接从当前 Segment 的 TabletSchema 中拉取挂载在该列上的倒排索引元数据列表。
            else {
                inverted_indexs = _segment->_tablet_schema->inverted_indexs(column);
            }
            // 针对 Variant 类型抽取列（Extracted Sub-column）进行倒排索引绑定时的“未命中/无候选索引”诊断与 Trace 逻辑
            if (column.is_extracted_column() && inverted_indexs.empty() && _opts.stats != nullptr) {
                const auto relative_path = column.path_info_ptr()->copy_pop_front().get_path();
                const auto diagnostic = fmt::format(
                        "[VariantSearchBinding] phase=init_index_iterators "
                        "result=no_candidate tablet_id={} rowset_id={} segment_id={} cid={} "
                        "logical_path={} relative_path={} materialized_column={}",
                        _tablet_id, _segment->rowset_id().to_string(), _segment->id(), cid,
                        column.path_info_ptr()->get_path(), relative_path, column.name());
                VLOG_DEBUG << diagnostic;
                _opts.stats->inverted_index_stats.add_binding_diagnostic(diagnostic);
            }
            // 遍历候选倒排索引元数据（inverted_indexs）、实例化物理 InvertedIndexIterator，并记录 Variant/半结构化列索引绑定结果（Accepted/No Iterator）的核心执行与可观测性（Diagnostic）逻辑。
            for (const auto& inverted_index : inverted_indexs) {
                const bool had_iterator = _index_iterators[cid] != nullptr;
                // 物理层的工厂函数。它会根据 inverted_index 元数据（如索引类型为 Fulltext/Inverted，使用的分词器，索引文件路径等），在底层打开对应的物理索引文件（如 .idx 文件），
                // 并实例化具体的 InvertedIndexIterator 赋值给 _index_iterators[cid]。若遇到文件损坏或 IO 错误，直接通过 RETURN_IF_ERROR 宏向上传播错误状态。
                RETURN_IF_ERROR(_segment->new_index_iterator(column, inverted_index, _opts,
                                                             &_index_iterators[cid]));
                // 触发条件：只有当当前列属于 Variant 类型抽取出来的子路径列（Extracted Column） 或 原生 Variant 根类型列，且开启了 Query Profile 统计（_opts.stats != nullptr）时，才会进入诊断记录逻辑。
                if ((column.is_extracted_column() || column.is_variant_type()) &&
                    _opts.stats != nullptr) {
                    const auto diagnostic = fmt::format(
                            "[VariantSearchBinding] phase=init_index_iterators "
                            "result={} tablet_id={} rowset_id={} segment_id={} cid={} "
                            "logical_path={} materialized_column={} index_id={} suffix={} "
                            "field_pattern={} iterator_state={}",
                            _index_iterators[cid] == nullptr ? "no_iterator" : "accepted",
                            _tablet_id, _segment->rowset_id().to_string(), _segment->id(), cid,
                            column.has_path_info() ? column.path_info_ptr()->get_path()
                                                   : column.name(),
                            column.name(), inverted_index->index_id(),
                            inverted_index->get_index_suffix(), inverted_index->field_pattern(),
                            had_iterator ? "preserved" : "created");
                    VLOG_DEBUG << diagnostic;
                    // 将这段诊断字符串塞入当前查询的 InvertedIndexStats 中。
                    _opts.stats->inverted_index_stats.add_binding_diagnostic(diagnostic);
                }
            }
            if (_index_iterators[cid] != nullptr) {
                _index_iterators[cid]->set_context(_index_query_context);
            }
        }
    }

    // Ann index iterators
    // 负责初始化 ANN（Approximate Nearest Neighbor，近似最近邻/向量）索引迭代器（AnnIndexIterator） 的循环处理逻辑
    // 为倒排索引（Inverted Index）初始化之后的补全阶段，专门为存储高维向量数据（如 HNSW、IVF-Flat 等向量索引） 的列创建物理索引检索句柄，并注入全局查询上下文。
    for (auto cid : _schema->column_ids()) {
        if (_index_iterators[cid] == nullptr) {
            const auto& column = _opts.tablet_schema->column(cid);
            const auto* index_meta = _segment->_tablet_schema->ann_index(column);
            if (index_meta) {
                RETURN_IF_ERROR(_segment->new_index_iterator(column, index_meta, _opts,
                                                             &_index_iterators[cid]));

                if (_index_iterators[cid] != nullptr) {
                    _index_iterators[cid]->set_context(_index_query_context);
                }
            }
        }
    }

    return Status::OK();
}
// 根据当前表的主键模型类型（Keys Type）和物理索引配置，决定是使用 Merge-on-Write 模型下的 Primary Key Index（主键索引/PK Index） 还是普通的 Short Key Index（前缀稀疏索引/SK Index） 来检索物理行号。

Status SegmentIterator::_lookup_ordinal(const RowCursor& key, bool is_include, rowid_t upper_bound,
                                        rowid_t* rowid) {
    // Unique Key 模型（Merge-on-Write）分支：_lookup_ordinal_from_pk_index
    // 表模型为 UNIQUE_KEYS（主键唯一模型）
    // 启用并生成了 Primary Key Index（通常是 Doris Merge-on-Write/MoW 开启时构建的有序索引树或 BloomFilter 结合结构）。
    if (_segment->_tablet_schema->keys_type() == UNIQUE_KEYS &&
        _segment->get_primary_key_index() != nullptr) {
        return _lookup_ordinal_from_pk_index(key, is_include, rowid);
    }
    // 通用/Dup/Agg模型分支：_lookup_ordinal_from_sk_index
    return _lookup_ordinal_from_sk_index(key, is_include, upper_bound, rowid);
}

// look up one key to get its ordinal at which can get data by using short key index.
// 'upper_bound' is defined the max ordinal the function will search.
// We use upper_bound to reduce search times.
// If we find a valid ordinal, it will be set in rowid and with Status::OK()
// If we can not find a valid key in this segment, we will set rowid to upper_bound
// Otherwise return error.
// 1. get [start, end) ordinal through short key index
// 2. binary search to find exact ordinal that match the input condition
// Make is_include template to reduce branch
// BE 存储引擎中利用 Short Key Index（前缀稀疏索引） 定位物理行号（RowID/Ordinal）的核心实现。
// 采用了 “两阶段查找（Two-phase Search）” 架构：先利用内存中的稀疏索引块进行粗粒度 Index Page 范围锁定，再通过磁盘数据块的 二分查找（Binary Search） 进行精细化 RowID 定位。
Status SegmentIterator::_lookup_ordinal_from_sk_index(const RowCursor& key, bool is_include,
                                                      rowid_t upper_bound, rowid_t* rowid) {
    // Doris 的 Short Key 索引截取表 Schema 前 N 个 Key 列（最多 36 字节）进行连续二进制编码。
    const ShortKeyIndexDecoder* sk_index_decoder = _segment->get_short_key_index();
    DCHECK(sk_index_decoder != nullptr);
    // 将传入的 RowCursor 按照 Tablet 的 Short Key 规则编码为字节流 index_key
    std::string index_key;
    key.encode_key_with_padding(&index_key, _segment->_tablet_schema->num_short_key_columns(),
                                is_include);

    const auto& key_col_ids = key.schema()->column_ids();
    // 阶段一：利用 Short Key 稀疏索引定位起始 Block（粗粒度定位）
    ssize_t start_block_id = 0;
    auto start_iter = sk_index_decoder->lower_bound(index_key);
    if (start_iter.valid()) {
        // Because previous block may contain this key, so we should set rowid to
        // last block's first row.
        start_block_id = start_iter.ordinal();
        // 回退一块（start_block_id--）的原因
        // Doris 的 Short Key Index 是稀疏索引，每个 Index Item 记录的是对应 Block（默认 1024 行）第一行的 Key。
        // 即使 lower_bound 匹配到了 Block $N$，目标 Key 依然可能落位于 Block $N-1$ 的后半部分。为了不漏过边界数据，起始块强制往回推退 1 个 Block。
        if (start_block_id > 0) {
            start_block_id--;
        }
    } else {
        // When we don't find a valid index item, which means all short key is
        // smaller than input key, this means that this key may exist in the last
        // row block. so we set the rowid to first row of last row block.
        start_block_id = sk_index_decoder->num_items() - 1;
    }
    rowid_t start = cast_set<rowid_t>(start_block_id) * sk_index_decoder->num_rows_per_block();
    // 阶段一（续）：确定终止范围上限 end
    rowid_t end = upper_bound;
    auto end_iter = sk_index_decoder->upper_bound(index_key);
    if (end_iter.valid()) {
        end = cast_set<rowid_t>(end_iter.ordinal()) * sk_index_decoder->num_rows_per_block();
    }

    // binary search to find the exact key
    // 阶段二：针对精确行号的磁盘数据二分查找（精细粒度定位）
    while (start < end) {
        rowid_t mid = (start + end) / 2;
        // 解压并定位到物理第 mid 行的数据 Page
        RETURN_IF_ERROR(_seek_and_peek(mid));
        // 将待查 key 与 mid 行真实存储的前缀 Key 列进行逐字节比较。
        int cmp = _compare_short_key_with_seek_block(key, key_col_ids);
        if (cmp > 0) {
            start = mid + 1;
        } else if (cmp == 0) {
            if (is_include) {
                // lower bound
                end = mid;
            } else {
                // upper bound
                start = mid + 1;
            }
        } else {
            end = mid;
        }
    }

    *rowid = start;
    return Status::OK();
}

Status SegmentIterator::_lookup_ordinal_from_pk_index(const RowCursor& key, bool is_include,
                                                      rowid_t* rowid) {
    DCHECK(_segment->_tablet_schema->keys_type() == UNIQUE_KEYS);
    const PrimaryKeyIndexReader* pk_index_reader = _segment->get_primary_key_index();
    DCHECK(pk_index_reader != nullptr);

    std::string index_key;
    key.encode_key_with_padding<true>(&index_key, _segment->_tablet_schema->num_key_columns(),
                                      is_include);
    if (index_key < _segment->min_key()) {
        *rowid = 0;
        return Status::OK();
    } else if (index_key > _segment->max_key()) {
        *rowid = num_rows();
        return Status::OK();
    }
    bool exact_match = false;

    std::unique_ptr<segment_v2::IndexedColumnIterator> index_iterator;
    RETURN_IF_ERROR(pk_index_reader->new_iterator(&index_iterator, _opts.stats, &_opts.io_ctx));

    Status status = index_iterator->seek_at_or_after(&index_key, &exact_match);
    if (UNLIKELY(!status.ok())) {
        *rowid = num_rows();
        if (status.is<ENTRY_NOT_FOUND>()) {
            return Status::OK();
        }
        return status;
    }
    *rowid = cast_set<rowid_t>(index_iterator->get_current_ordinal());

    // The sequence column needs to be removed from primary key index when comparing key
    bool has_seq_col = _segment->_tablet_schema->has_sequence_col();
    // Used to get key range from primary key index,
    // for mow with cluster key table, we should get key range from short key index.
    DCHECK(_segment->_tablet_schema->cluster_key_uids().empty());

    // if full key is exact_match, the primary key without sequence column should also the same
    if (has_seq_col && !exact_match) {
        size_t seq_col_length =
                _segment->_tablet_schema->column(_segment->_tablet_schema->sequence_col_idx())
                        .length() +
                1;
        auto index_type = DataTypeFactory::instance().create_data_type(
                _segment->_pk_index_reader->type(), 1, 0);
        auto index_column = index_type->create_column();
        size_t num_to_read = 1;
        size_t num_read = num_to_read;
        RETURN_IF_ERROR(index_iterator->next_batch(&num_read, index_column));
        DCHECK(num_to_read == num_read);

        Slice sought_key =
                Slice(index_column->get_data_at(0).data, index_column->get_data_at(0).size);
        Slice sought_key_without_seq =
                Slice(sought_key.get_data(), sought_key.get_size() - seq_col_length);

        // compare key
        if (Slice(index_key).compare(sought_key_without_seq) == 0) {
            exact_match = true;
        }
    }

    // find the key in primary key index, and the is_include is false, so move
    // to the next row.
    if (exact_match && !is_include) {
        *rowid += 1;
    }
    return Status::OK();
}

// seek to the row and load that row to _key_cursor
Status SegmentIterator::_seek_and_peek(rowid_t rowid) {
    {
        _opts.stats->block_init_seek_num += 1;
        SCOPED_RAW_TIMER(&_opts.stats->block_init_seek_ns);
        RETURN_IF_ERROR(_seek_columns(_seek_schema->column_ids(), rowid));
    }
    size_t num_rows = 1;

    //note(wb) reset _seek_block for memory reuse
    // it is easier to use row based memory layout for clear memory
    for (int i = 0; i < _seek_block.size(); i++) {
        _seek_block[i]->clear();
    }
    RETURN_IF_ERROR(_read_columns(_seek_schema->column_ids(), _seek_block, num_rows));
    return Status::OK();
}

Status SegmentIterator::_seek_columns(const std::vector<ColumnId>& column_ids, rowid_t pos) {
    for (auto cid : column_ids) {
        if (!_need_read_data(cid)) {
            continue;
        }
        RETURN_IF_ERROR(_column_iterators[cid]->seek_to_ordinal(pos));
    }
    return Status::OK();
}

/* ---------------------- for vectorization implementation  ---------------------- */

/**
 *  For storage layer data type, can be measured from two perspectives:
 *  1 Whether the type can be read in a fast way(batch read using SIMD)
 *    Such as integer type and float type, this type can be read in SIMD way.
 *    For the type string/bitmap/hll, they can not be read in batch way, so read this type data is slow.
 *   If a type can be read fast, we can try to eliminate Lazy Materialization, because we think for this type, seek cost > read cost.
 *   This is an estimate, if we want more precise cost, statistics collection is necessary(this is a todo).
 *   In short, when returned non-pred columns contains string/hll/bitmap, we using Lazy Materialization.
 *   Otherwise, we disable it.
 *
 *   When Lazy Materialization enable, we need to read column at least two times.
 *   First time to read Pred col, second time to read non-pred.
 *   Here's an interesting question to research, whether read Pred col once is the best plan.
 *   (why not read Pred col twice or more?)
 *
 *   When Lazy Materialization disable, we just need to read once.
 *
 *
 *  2 Whether the predicate type can be evaluate in a fast way(using SIMD to eval pred)
 *    Such as integer type and float type, they can be eval fast.
 *    But for BloomFilter/string/date, they eval slow.
 *    If a type can be eval fast, we use vectorization to eval it.
 *    Otherwise, we use short-circuit to eval it.
 *
 *
 */

// todo(wb) need a UT here
Status SegmentIterator::_vec_init_lazy_materialization() {
    _is_pred_column.resize(_schema->columns().size(), false);

    // including short/vec/delete pred
    std::set<ColumnId> pred_column_ids;
    _lazy_materialization_read = false;

    std::set<ColumnId> del_cond_id_set;
    _opts.delete_condition_predicates->get_all_column_ids(del_cond_id_set);

    std::set<std::shared_ptr<const ColumnPredicate>> delete_predicate_set {};
    _opts.delete_condition_predicates->get_all_column_predicate(delete_predicate_set);
    for (auto predicate : delete_predicate_set) {
        if (PredicateTypeTraits::is_range(predicate->type())) {
            _delete_range_column_ids.push_back(predicate->column_id());
        } else if (PredicateTypeTraits::is_bloom_filter(predicate->type())) {
            _delete_bloom_filter_column_ids.push_back(predicate->column_id());
        }
    }

    // Step1: extract columns that can be lazy materialization
    if (!_col_predicates.empty() || !del_cond_id_set.empty()) {
        std::set<ColumnId> short_cir_pred_col_id_set; // using set for distinct cid
        std::set<ColumnId> vec_pred_col_id_set;

        for (auto predicate : _col_predicates) {
            auto cid = predicate->column_id();
            _is_pred_column[cid] = true;
            pred_column_ids.insert(cid);

            // check pred using short eval or vec eval
            if (_can_evaluated_by_vectorized(predicate)) {
                vec_pred_col_id_set.insert(cid);
                _pre_eval_block_predicate.push_back(predicate);
            } else {
                short_cir_pred_col_id_set.insert(cid);
                _short_cir_eval_predicate.push_back(predicate);
            }
            if (predicate->is_runtime_filter()) {
                _filter_info_id.push_back(predicate);
            }
        }

        // handle delete_condition
        if (!del_cond_id_set.empty()) {
            short_cir_pred_col_id_set.insert(del_cond_id_set.begin(), del_cond_id_set.end());
            pred_column_ids.insert(del_cond_id_set.begin(), del_cond_id_set.end());

            for (auto cid : del_cond_id_set) {
                _is_pred_column[cid] = true;
            }
        }

        _vec_pred_column_ids.assign(vec_pred_col_id_set.cbegin(), vec_pred_col_id_set.cend());
        _short_cir_pred_column_ids.assign(short_cir_pred_col_id_set.cbegin(),
                                          short_cir_pred_col_id_set.cend());
    }

    if (!_vec_pred_column_ids.empty()) {
        _is_need_vec_eval = true;
    }
    if (!_short_cir_pred_column_ids.empty()) {
        _is_need_short_eval = true;
    }

    // Step2: extract columns that can execute expr context
    _is_common_expr_column.resize(_schema->columns().size(), false);
    if (!_common_expr_ctxs_push_down.empty()) {
        for (const auto& expr_ctx : _common_expr_ctxs_push_down) {
            RETURN_IF_ERROR(_extract_common_expr_columns(expr_ctx->root()));
        }
        if (!_common_expr_columns.empty()) {
            _is_need_expr_eval = true;
            for (auto cid : _schema->column_ids()) {
                // pred column also needs to be filtered by expr, exclude additional delete condition column.
                // if delete condition column not in the block, no filter is needed
                // and will be removed from _columns_to_filter in the first next_batch.
                if (_is_common_expr_column[cid] || _is_pred_column[cid]) {
                    auto loc = _schema->column_index(cid);
                    _columns_to_filter.push_back(loc);

                    const auto field_type = _schema->column(cid)->type();
                    if (_is_common_expr_column[cid] && _enable_prune_nested_column &&
                        (field_type == FieldType::OLAP_FIELD_TYPE_STRUCT ||
                         field_type == FieldType::OLAP_FIELD_TYPE_ARRAY ||
                         field_type == FieldType::OLAP_FIELD_TYPE_MAP)) {
                        DCHECK(_column_iterators[cid]);
                        if (_column_iterators[cid]->read_requirement() ==
                                    ColumnIterator::ReadRequirement::PREDICATE &&
                            _column_iterators[cid]->has_lazy_read_target()) {
                            // Only split lazy recovery for complex common expr columns that have
                            // both predicate-only and non-predicate nested targets. The two requirement
                            // checks already imply that nested-column pruning happened: without an
                            // explicit predicate sub-path the parent would not be
                            // PREDICATE, and without a pruned non-predicate child there
                            // would be no lazy target to recover after filtering.
                            _support_lazy_read_pruned_columns.emplace(cid);
                        }
                    }
                }
            }

            for (const auto& entry : _virtual_column_exprs) {
                _columns_to_filter.push_back(_schema->column_index(entry.first));
            }
        }
    }

    // Step 3: fill non predicate columns and second read column
    // if _schema columns size equal to pred_column_ids size, lazy_materialization_read is false,
    // all columns are lazy materialization columns without non predicte column.
    // If common expr pushdown exists, and expr column is not contained in lazy materialization columns,
    // add to second read column, which will be read after lazy materialization
    if (_schema->column_ids().size() > pred_column_ids.size()) {
        // pred_column_ids maybe empty, so that could not set _lazy_materialization_read = true here
        // has to check there is at least one predicate column
        for (auto cid : _schema->column_ids()) {
            if (!_is_pred_column[cid]) {
                if (_is_need_vec_eval || _is_need_short_eval) {
                    _lazy_materialization_read = true;
                }
                if (_is_common_expr_column[cid]) {
                    _common_expr_column_ids.push_back(cid);
                } else {
                    _non_predicate_columns.push_back(cid);
                }
            }
        }
    }

    // Step 4: fill first read columns
    if (_lazy_materialization_read) {
        // insert pred cid to first_read_columns
        for (auto cid : pred_column_ids) {
            _predicate_column_ids.push_back(cid);
        }
    } else if (!_is_need_vec_eval && !_is_need_short_eval && !_is_need_expr_eval) {
        for (int i = 0; i < _schema->num_column_ids(); i++) {
            auto cid = _schema->column_id(i);
            _predicate_column_ids.push_back(cid);
        }
    } else {
        if (_is_need_vec_eval || _is_need_short_eval) {
            // TODO To refactor, because we suppose lazy materialization is better performance.
            // pred exits, but we can eliminate lazy materialization
            // insert pred/non-pred cid to first read columns
            std::set<ColumnId> pred_id_set;
            pred_id_set.insert(_short_cir_pred_column_ids.begin(),
                               _short_cir_pred_column_ids.end());
            pred_id_set.insert(_vec_pred_column_ids.begin(), _vec_pred_column_ids.end());

            DCHECK(_common_expr_column_ids.empty());
            // _non_predicate_column_ids must be empty. Otherwise _lazy_materialization_read must not false.
            for (int i = 0; i < _schema->num_column_ids(); i++) {
                auto cid = _schema->column_id(i);
                if (pred_id_set.find(cid) != pred_id_set.end()) {
                    _predicate_column_ids.push_back(cid);
                }
            }
        } else if (_is_need_expr_eval) {
            DCHECK(!_is_need_vec_eval && !_is_need_short_eval);
            for (auto cid : _common_expr_columns) {
                _predicate_column_ids.push_back(cid);
            }
        }
    }

    VLOG_DEBUG << fmt::format(
            "Laze materialization init end. "
            "lazy_materialization_read: {}, "
            "_col_predicates size: {}, "
            "_cols_read_by_column_predicate: [{}], "
            "_non_predicate_columns: [{}], "
            "_cols_read_by_common_expr: [{}], "
            "columns_to_filter: [{}], "
            "schema_column_id_to_index: [{}]",
            _lazy_materialization_read, _col_predicates.size(),
            fmt::join(_predicate_column_ids, ","), fmt::join(_non_predicate_columns, ","),
            fmt::join(_common_expr_column_ids, ","), fmt::join(_columns_to_filter, ","),
            fmt::join(_schema->column_id_to_index(), ","));
    return Status::OK();
}

bool SegmentIterator::_can_evaluated_by_vectorized(std::shared_ptr<ColumnPredicate> predicate) {
    auto cid = predicate->column_id();
    FieldType field_type = _schema->column(cid)->type();
    if (field_type == FieldType::OLAP_FIELD_TYPE_VARIANT) {
        // Use variant cast dst type
        field_type = _opts.target_cast_type_for_variants[_schema->column(cid)->name()]
                             ->get_storage_field_type();
    }
    switch (predicate->type()) {
    case PredicateType::EQ:
    case PredicateType::NE:
    case PredicateType::LE:
    case PredicateType::LT:
    case PredicateType::GE:
    case PredicateType::GT: {
        if (field_type == FieldType::OLAP_FIELD_TYPE_VARCHAR ||
            field_type == FieldType::OLAP_FIELD_TYPE_CHAR ||
            field_type == FieldType::OLAP_FIELD_TYPE_STRING) {
            return config::enable_low_cardinality_optimize &&
                   _opts.io_ctx.reader_type == ReaderType::READER_QUERY &&
                   _column_iterators[cid]->is_all_dict_encoding();
        } else if (field_type == FieldType::OLAP_FIELD_TYPE_DECIMAL) {
            return false;
        }
        return true;
    }
    default:
        return false;
    }
}

// These placeholders are used only when the real column data is skipped after
// index/count pushdown has already identified the matching rows. The value is
// irrelevant, but nullable columns must stay non-NULL so COUNT(col) can count
// the matched rows instead of treating every placeholder as NULL.
static void insert_many_not_null_defaults(MutableColumnPtr& column, size_t num) {
    if (auto* nullable_column = check_and_get_column<ColumnNullable>(column.get())) {
        nullable_column->insert_not_null_elements(num);
        return;
    }
    column->insert_many_defaults(num);
}

bool SegmentIterator::_prune_column(ColumnId cid, MutableColumnPtr& column,
                                    size_t num_of_defaults) {
    if (_need_read_data(cid)) {
        return false;
    }
    insert_many_not_null_defaults(column, num_of_defaults);
    return true;
}

bool SegmentIterator::_can_skip_reading_extra_column(ColumnId cid) {
    if (!_opts.extra_columns.contains(cid) || _is_pred_column.empty()) {
        return false;
    }
    DCHECK_EQ(_is_pred_column.size(), _is_common_expr_column.size());
    DCHECK_LT(cid, _is_pred_column.size());

    // extra_columns is only an optimization hint. The real value is still
    // required when the column participates in expression materialization or
    // any predicate path.
    return !_virtual_column_exprs.contains(cid) && !_has_delete_predicate(cid) &&
           !_is_pred_column[cid] && !_is_common_expr_column[cid];
}

Status SegmentIterator::_read_columns(const std::vector<ColumnId>& column_ids,
                                      MutableColumns& column_block, size_t nrows) {
    for (auto cid : column_ids) {
        auto& column = column_block[cid];
        size_t rows_read = nrows;
        if (_prune_column(cid, column, rows_read)) {
            continue;
        }
        RETURN_IF_ERROR(_column_iterators[cid]->next_batch(&rows_read, column));
        if (nrows != rows_read) {
            return Status::Error<ErrorCode::INTERNAL_ERROR>("nrows({}) != rows_read({})", nrows,
                                                            rows_read);
        }
    }
    return Status::OK();
}

Status SegmentIterator::_init_current_block(Block* block,
                                            std::vector<MutableColumnPtr>& current_columns,
                                            uint32_t nrows_read_limit) {
    block->clear_column_data(_schema->num_column_ids());

    for (size_t i = 0; i < _schema->num_column_ids(); i++) {
        auto cid = _schema->column_id(i);
        const auto* column_desc = _schema->column(cid);

        auto file_column_type = _storage_name_and_type[cid].second;
        auto expected_type = Schema::get_data_type_ptr(*column_desc);
        if (!_is_pred_column[cid] && !file_column_type->equals(*expected_type)) {
            // The storage layer type is different from schema needed type, so we use storage
            // type to read columns instead of schema type for safety
            VLOG_DEBUG << fmt::format(
                    "Recreate column with expected type {}, file column type {}, col_name {}, "
                    "col_path {}",
                    block->get_by_position(i).type->get_name(), file_column_type->get_name(),
                    column_desc->name(),
                    column_desc->path_info_ptr() == nullptr
                            ? ""
                            : column_desc->path_info_ptr()->get_path());
            // TODO reuse
            current_columns[cid] = file_column_type->create_column();
            current_columns[cid]->reserve(nrows_read_limit);
        } else {
            // the column in block must clear() here to insert new data
            if (_is_pred_column[cid] ||
                i >= block->columns()) { //todo(wb) maybe we can release it after output block
                if (current_columns[cid].get() == nullptr) {
                    return Status::InternalError(
                            "SegmentIterator meet invalid column, id={}, name={}", cid,
                            _schema->column(cid)->name());
                }
                current_columns[cid]->clear();
            } else { // non-predicate column
                current_columns[cid] = std::move(*block->get_by_position(i).column).mutate();
                current_columns[cid]->reserve(nrows_read_limit);
            }
        }
    }

    for (const auto& entry : _virtual_column_exprs) {
        auto cid = entry.first;
        current_columns[cid] = ColumnNothing::create(0);
        current_columns[cid]->reserve(nrows_read_limit);
    }

    return Status::OK();
}

Status SegmentIterator::_output_non_pred_columns(Block* block) {
    SCOPED_RAW_TIMER(&_opts.stats->output_col_ns);
    VLOG_DEBUG << fmt::format(
            "Output non-predicate columns, _non_predicate_columns: [{}], "
            "schema_column_id_to_index: [{}]",
            fmt::join(_non_predicate_columns, ","), fmt::join(_schema->column_id_to_index(), ","));
    RETURN_IF_ERROR(_convert_to_expected_type(_non_predicate_columns));
    for (auto cid : _non_predicate_columns) {
        auto loc = _schema->column_index(cid);
        // Whether a delete predicate column gets output depends on how the caller builds
        // the block passed to next_batch(). Both calling paths now build the block with
        // only the output schema (return_columns), so delete predicate columns are skipped:
        //
        // 1) VMergeIterator path: block_reset() builds _block using the output schema
        //    (return_columns only), e.g. block has 2 columns {c1, c2}.
        //    Here loc=2 for delete predicate c3, block->columns()=2, so loc < block->columns()
        //    is false, and c3 is skipped.
        //
        // 2) VUnionIterator path: the caller's block is built with only return_columns
        //    (output schema), e.g. block has 2 columns {c1, c2}.
        //    Here loc=2 for c3, block->columns()=2, so loc < block->columns() is false,
        //    and c3 is skipped — same behavior as the VMergeIterator path.
        if (loc < block->columns()) {
            bool column_in_block_is_nothing = check_and_get_column<const ColumnNothing>(
                    block->get_by_position(loc).column.get());
            bool column_is_normal = !_virtual_column_exprs.contains(cid);
            bool return_column_is_nothing =
                    check_and_get_column<const ColumnNothing>(_current_return_columns[cid].get());
            VLOG_DEBUG << fmt::format(
                    "Cid {} loc {}, column_in_block_is_nothing {}, column_is_normal {}, "
                    "return_column_is_nothing {}",
                    cid, loc, column_in_block_is_nothing, column_is_normal,
                    return_column_is_nothing);

            if (column_in_block_is_nothing || column_is_normal) {
                block->replace_by_position(loc, std::move(_current_return_columns[cid]));
                VLOG_DEBUG << fmt::format(
                        "Output non-predicate column, cid: {}, loc: {}, col_name: {}, rows {}", cid,
                        loc, _schema->column(cid)->name(),
                        block->get_by_position(loc).column->size());
            }
            // Means virtual column in block has been materialized(maybe by common expr).
            // so do nothing here.
        }
    }
    return Status::OK();
}

/**
 * Reads columns by their index, handling both continuous and discontinuous rowid scenarios.
 *
 * This function is designed to read a specified number of rows (up to nrows_read_limit)
 * from the segment iterator, dealing with both continuous and discontinuous rowid arrays.
 * It operates as follows:
 *
 * 1. Reads a batch of rowids (up to the specified limit), and checks if they are continuous.
 *    Continuous here means that the rowids form an unbroken sequence (e.g., 1, 2, 3, 4...).
 *
 * 2. For each column that needs to be read (identified by _predicate_column_ids):
 *    - If the rowids are continuous, the function uses seek_to_ordinal and next_batch
 *      for efficient reading.
 *    - If the rowids are not continuous, the function processes them in smaller batches
 *      (each of size up to 256). Each batch is checked for internal continuity:
 *        a. If a batch is continuous, uses seek_to_ordinal and next_batch for that batch.
 *        b. If a batch is not continuous, uses read_by_rowids for individual rowids in the batch.
 *
 * This approach optimizes reading performance by leveraging batch processing for continuous
 * rowid sequences and handling discontinuities gracefully in smaller chunks.
 */
Status SegmentIterator::_read_columns_by_index(uint32_t nrows_read_limit, uint16_t& nrows_read) {
    SCOPED_RAW_TIMER(&_opts.stats->predicate_column_read_ns);

    nrows_read = (uint16_t)_range_iter->read_batch_rowids(_block_rowids.data(), nrows_read_limit);
    bool is_continuous = (nrows_read > 1) &&
                         (_block_rowids[nrows_read - 1] - _block_rowids[0] == nrows_read - 1);
    VLOG_DEBUG << fmt::format(
            "nrows_read from range iterator: {}, is_continus {}, "
            "_cols_read_by_column_predicate "
            "[{}]",
            nrows_read, is_continuous, fmt::join(_predicate_column_ids, ","));

    LOG_IF(INFO, config::enable_segment_prefetch_verbose_log) << fmt::format(
            "[verbose] SegmentIterator::_read_columns_by_index read {} rowids, continuous: {}, "
            "rowids: [{}...{}]",
            nrows_read, is_continuous, nrows_read > 0 ? _block_rowids[0] : 0,
            nrows_read > 0 ? _block_rowids[nrows_read - 1] : 0);
    for (auto cid : _predicate_column_ids) {
        auto& column = _current_return_columns[cid];
        VLOG_DEBUG << fmt::format("Reading column {}, col_name {}", cid,
                                  _schema->column(cid)->name());
        if (!_virtual_column_exprs.contains(cid)) {
            if (_no_need_read_key_data(cid, column, nrows_read)) {
                VLOG_DEBUG << fmt::format("Column {} no need to read.", cid);
                continue;
            }
            if (_prune_column(cid, column, nrows_read)) {
                VLOG_DEBUG << fmt::format("Column {} is pruned. No need to read data.", cid);
                continue;
            }
            DBUG_EXECUTE_IF("segment_iterator._read_columns_by_index", {
                auto col_name = _opts.tablet_schema->column(cid).name();
                auto debug_col_name =
                        DebugPoints::instance()->get_debug_param_or_default<std::string>(
                                "segment_iterator._read_columns_by_index", "column_name", "");
                if (debug_col_name.empty() && col_name != "__DORIS_DELETE_SIGN__") {
                    return Status::Error<ErrorCode::INTERNAL_ERROR>(
                            "does not need to read data, {}", col_name);
                }
                if (debug_col_name.find(col_name) != std::string::npos) {
                    return Status::Error<ErrorCode::INTERNAL_ERROR>(
                            "does not need to read data, {}", col_name);
                }
            })
        }

        auto* column_iter = _column_iterators[cid].get();
        ScopedColumnIteratorReadPhase scoped_read_phase {
                column_iter, _support_lazy_read_pruned_columns.contains(cid)
                                     ? ColumnIterator::ReadPhase::PREDICATE
                                     : ColumnIterator::ReadPhase::NORMAL};

        if (is_continuous) {
            size_t rows_read = nrows_read;
            _opts.stats->predicate_column_read_seek_num += 1;
            if (_opts.runtime_state && _opts.runtime_state->enable_profile()) {
                SCOPED_RAW_TIMER(&_opts.stats->predicate_column_read_seek_ns);
                RETURN_IF_ERROR(column_iter->seek_to_ordinal(_block_rowids[0]));
            } else {
                RETURN_IF_ERROR(column_iter->seek_to_ordinal(_block_rowids[0]));
            }
            RETURN_IF_ERROR(column_iter->next_batch(&rows_read, column));
            if (rows_read != nrows_read) {
                return Status::Error<ErrorCode::INTERNAL_ERROR>("nrows({}) != rows_read({})",
                                                                nrows_read, rows_read);
            }
        } else {
            const uint32_t batch_size = _range_iter->get_batch_size();
            uint32_t processed = 0;
            while (processed < nrows_read) {
                uint32_t current_batch_size = std::min(batch_size, nrows_read - processed);
                bool batch_continuous = (current_batch_size > 1) &&
                                        (_block_rowids[processed + current_batch_size - 1] -
                                                 _block_rowids[processed] ==
                                         current_batch_size - 1);

                if (batch_continuous) {
                    size_t rows_read = current_batch_size;
                    _opts.stats->predicate_column_read_seek_num += 1;
                    if (_opts.runtime_state && _opts.runtime_state->enable_profile()) {
                        SCOPED_RAW_TIMER(&_opts.stats->predicate_column_read_seek_ns);
                        RETURN_IF_ERROR(column_iter->seek_to_ordinal(_block_rowids[processed]));
                    } else {
                        RETURN_IF_ERROR(column_iter->seek_to_ordinal(_block_rowids[processed]));
                    }
                    RETURN_IF_ERROR(column_iter->next_batch(&rows_read, column));
                    if (rows_read != current_batch_size) {
                        return Status::Error<ErrorCode::INTERNAL_ERROR>(
                                "batch nrows({}) != rows_read({})", current_batch_size, rows_read);
                    }
                } else {
                    RETURN_IF_ERROR(column_iter->read_by_rowids(&_block_rowids[processed],
                                                                current_batch_size, column));
                }
                processed += current_batch_size;
            }
        }
    }

    return Status::OK();
}
void SegmentIterator::_replace_version_col_if_needed(const std::vector<ColumnId>& column_ids,
                                                     size_t num_rows) {
    // Only the rowset with single version need to replace the version column.
    // Doris can't determine the version before publish_version finished, so
    // we can't write data to __DORIS_VERSION_COL__ in segment writer, the value
    // is 0 by default.
    // So we need to replace the value to real version while reading.
    if (_opts.version.first != _opts.version.second) {
        return;
    }
    int32_t version_idx = _schema->version_col_idx();
    if (std::ranges::find(column_ids, version_idx) == column_ids.end()) {
        return;
    }

    const auto* column_desc = _schema->column(version_idx);
    auto column = Schema::get_data_type_ptr(*column_desc)->create_column();
    DCHECK(_schema->column(version_idx)->type() == FieldType::OLAP_FIELD_TYPE_BIGINT);
    auto* col_ptr = assert_cast<ColumnInt64*>(column.get());
    for (size_t j = 0; j < num_rows; j++) {
        col_ptr->insert_value(_opts.version.second);
    }
    _current_return_columns[version_idx] = std::move(column);
    VLOG_DEBUG << "replaced version column in segment iterator, version_col_idx:" << version_idx;
}

void SegmentIterator::_update_tso_col_if_needed(const std::vector<ColumnId>& column_ids,
                                                size_t num_rows) {
    // use physical time part of commit tso to replace timestamp col
    if (_opts.version.first != _opts.version.second) {
        return;
    }

    if (!_opts.read_row_binlog) {
        return;
    }

    int32_t tso_col_idx = _schema->tso_col_idx();
    if (tso_col_idx < 0 || std::ranges::find(column_ids, tso_col_idx) == column_ids.end()) {
        return;
    }

    DCHECK_EQ(_opts.commit_tso.start_tso(), _opts.commit_tso.end_tso());
    Int64 commit_tso = _opts.commit_tso.end_tso() == -1 ? 0 : _opts.commit_tso.end_tso();

    if (_is_pred_column[tso_col_idx]) {
        // Nullable predicate column is represented as ColumnNullable(predicate_col)
        if (auto* tso_nullable = check_and_get_column<ColumnNullable>(
                    _current_return_columns[tso_col_idx].get())) {
            _current_return_columns[tso_col_idx]->clear();
            auto value = commit_tso;
            for (size_t j = 0; j < num_rows; j++) {
                tso_nullable->get_nested_column_ptr()->insert_data(
                        reinterpret_cast<const char*>(&value), 0);
                tso_nullable->get_null_map_data().emplace_back(0);
            }
            return;
        }

        auto* tso_column = assert_cast<ColumnInt64*>(_current_return_columns[tso_col_idx].get());
        tso_column->clear();
        auto value = commit_tso;
        for (size_t j = 0; j < num_rows; j++) {
            tso_column->insert_data(reinterpret_cast<const char*>(&value), 0);
        }
        return;
    }

    const auto* column_desc = _schema->column(tso_col_idx);
    auto column = Schema::get_data_type_ptr(*column_desc)->create_column();
    DCHECK(column_desc->type() == FieldType::OLAP_FIELD_TYPE_BIGINT);

    if (auto* tso_nullable = check_and_get_column<ColumnNullable>(column.get())) {
        auto* col_ptr = assert_cast<ColumnInt64*>(&tso_nullable->get_nested_column());
        for (size_t j = 0; j < num_rows; j++) {
            col_ptr->insert_value(commit_tso);
            tso_nullable->get_null_map_data().emplace_back(0);
        }
    } else {
        auto* col_ptr = assert_cast<ColumnInt64*>(column.get());
        for (size_t j = 0; j < num_rows; j++) {
            col_ptr->insert_value(commit_tso);
        }
    }
    _current_return_columns[tso_col_idx] = std::move(column);
}

uint16_t SegmentIterator::_evaluate_vectorization_predicate(uint16_t* sel_rowid_idx,
                                                            uint16_t selected_size) {
    SCOPED_RAW_TIMER(&_opts.stats->vec_cond_ns);
    bool all_pred_always_true = true;
    for (const auto& pred : _pre_eval_block_predicate) {
        if (!pred->always_true()) {
            all_pred_always_true = false;
        } else {
            pred->update_filter_info(0, 0, selected_size);
        }
    }

    const uint16_t original_size = selected_size;
    //If all predicates are always_true, then return directly.
    if (all_pred_always_true || !_is_need_vec_eval) {
        for (uint16_t i = 0; i < original_size; ++i) {
            sel_rowid_idx[i] = i;
        }
        // All preds are always_true, so return immediately and update the profile statistics here.
        _opts.stats->vec_cond_input_rows += original_size;
        return original_size;
    }

    _ret_flags.resize(original_size);
    DCHECK(!_pre_eval_block_predicate.empty());
    bool is_first = true;
    for (auto& pred : _pre_eval_block_predicate) {
        if (pred->always_true()) {
            continue;
        }
        auto column_id = pred->column_id();
        auto& column = _current_return_columns[column_id];
        if (is_first) {
            pred->evaluate_vec(*column, original_size, (bool*)_ret_flags.data());
            is_first = false;
        } else {
            pred->evaluate_and_vec(*column, original_size, (bool*)_ret_flags.data());
        }
    }

    uint16_t new_size = 0;

    uint16_t sel_pos = 0;
    const uint16_t sel_end = sel_pos + selected_size;
    static constexpr size_t SIMD_BYTES = simd::bits_mask_length();
    const uint16_t sel_end_simd = sel_pos + selected_size / SIMD_BYTES * SIMD_BYTES;

    while (sel_pos < sel_end_simd) {
        auto mask = simd::bytes_mask_to_bits_mask(_ret_flags.data() + sel_pos);
        if (0 == mask) {
            //pass
        } else if (simd::bits_mask_all() == mask) {
            for (uint16_t i = 0; i < SIMD_BYTES; i++) {
                sel_rowid_idx[new_size++] = sel_pos + i;
            }
        } else {
            simd::iterate_through_bits_mask(
                    [&](const int bit_pos) {
                        sel_rowid_idx[new_size++] = sel_pos + (uint16_t)bit_pos;
                    },
                    mask);
        }
        sel_pos += SIMD_BYTES;
    }

    for (; sel_pos < sel_end; sel_pos++) {
        if (_ret_flags[sel_pos]) {
            sel_rowid_idx[new_size++] = sel_pos;
        }
    }

    _opts.stats->vec_cond_input_rows += original_size;
    _opts.stats->rows_vec_cond_filtered += original_size - new_size;
    return new_size;
}

uint16_t SegmentIterator::_evaluate_short_circuit_predicate(uint16_t* vec_sel_rowid_idx,
                                                            uint16_t selected_size) {
    SCOPED_RAW_TIMER(&_opts.stats->short_cond_ns);
    if (!_is_need_short_eval) {
        return selected_size;
    }

    uint16_t original_size = selected_size;
    for (auto predicate : _short_cir_eval_predicate) {
        auto column_id = predicate->column_id();
        auto& short_cir_column = _current_return_columns[column_id];
        selected_size = predicate->evaluate(*short_cir_column, vec_sel_rowid_idx, selected_size);
    }

    _opts.stats->short_circuit_cond_input_rows += original_size;
    _opts.stats->rows_short_circuit_cond_filtered += original_size - selected_size;

    // evaluate delete condition
    original_size = selected_size;
    selected_size = _opts.delete_condition_predicates->evaluate(_current_return_columns,
                                                                vec_sel_rowid_idx, selected_size);
    _opts.stats->rows_vec_del_cond_filtered += original_size - selected_size;
    return selected_size;
}

static void shrink_materialized_block_columns(Block* block, size_t rows) {
    for (auto& entry : *block) {
        if (entry.column && entry.column->size() > rows) {
            entry.column = entry.column->shrink(rows);
        }
    }
}

static void slice_materialized_block_columns(Block* block, size_t offset, size_t rows,
                                             size_t original_rows) {
    for (auto& entry : *block) {
        if (!entry.column || entry.column->size() == 0) {
            continue;
        }
        DORIS_CHECK(entry.column->size() == original_rows);
        entry.column = entry.column->cut(offset, rows);
    }
}

Status SegmentIterator::_apply_read_limit_to_selected_rows(Block* block, uint16_t& selected_size) {
    if (_opts.read_limit == 0) {
        return Status::OK();
    }
    DORIS_CHECK(_rows_returned <= _opts.read_limit);
    size_t remaining = _opts.read_limit - _rows_returned;
    if (remaining == 0) {
        selected_size = 0;
        shrink_materialized_block_columns(block, 0);
        return Status::OK();
    }
    if (selected_size > remaining) {
        if (_opts.read_orderby_key_reverse) {
            const auto original_size = selected_size;
            const auto offset = original_size - remaining;
            for (size_t i = 0; i < remaining; ++i) {
                _sel_rowid_idx[i] = _sel_rowid_idx[offset + i];
            }
            selected_size = cast_set<uint16_t>(remaining);
            slice_materialized_block_columns(block, offset, remaining, original_size);
            return Status::OK();
        }
        selected_size = cast_set<uint16_t>(remaining);
        shrink_materialized_block_columns(block, selected_size);
    }
    return Status::OK();
}

Status SegmentIterator::_read_columns_by_rowids(std::vector<ColumnId>& read_column_ids,
                                                std::vector<rowid_t>& rowid_vector,
                                                uint16_t* sel_rowid_idx, size_t select_size,
                                                MutableColumns* mutable_columns,
                                                bool init_condition_cache,
                                                bool read_for_predicate) {
    SCOPED_RAW_TIMER(&_opts.stats->lazy_read_ns);
    std::vector<rowid_t> rowids(select_size);

    if (init_condition_cache) {
        DCHECK(_condition_cache);
        auto& condition_cache = *_condition_cache;
        for (size_t i = 0; i < select_size; ++i) {
            rowids[i] = rowid_vector[sel_rowid_idx[i]];
            condition_cache[rowids[i] / SegmentIterator::CONDITION_CACHE_OFFSET] = true;
        }
    } else {
        for (size_t i = 0; i < select_size; ++i) {
            rowids[i] = rowid_vector[sel_rowid_idx[i]];
        }
    }

    for (auto cid : read_column_ids) {
        auto& colunm = (*mutable_columns)[cid];
        if (_no_need_read_key_data(cid, colunm, select_size)) {
            continue;
        }
        if (_prune_column(cid, colunm, select_size)) {
            continue;
        }

        DBUG_EXECUTE_IF("segment_iterator._read_columns_by_index", {
            auto debug_col_name = DebugPoints::instance()->get_debug_param_or_default<std::string>(
                    "segment_iterator._read_columns_by_index", "column_name", "");
            if (debug_col_name.empty()) {
                return Status::Error<ErrorCode::INTERNAL_ERROR>("does not need to read data");
            }
            auto col_name = _opts.tablet_schema->column(cid).name();
            if (debug_col_name.find(col_name) != std::string::npos) {
                return Status::Error<ErrorCode::INTERNAL_ERROR>("does not need to read data, {}",
                                                                debug_col_name);
            }
        })

        if (_current_return_columns[cid].get() == nullptr) {
            return Status::InternalError(
                    "SegmentIterator meet invalid column, return columns size {}, cid {}",
                    _current_return_columns.size(), cid);
        }

        auto* column_iter = _column_iterators[cid].get();
        ScopedColumnIteratorReadPhase scoped_read_phase {
                column_iter, read_for_predicate && _support_lazy_read_pruned_columns.contains(cid)
                                     ? ColumnIterator::ReadPhase::PREDICATE
                                     : ColumnIterator::ReadPhase::NORMAL};

        RETURN_IF_ERROR(column_iter->read_by_rowids(rowids.data(), select_size,
                                                    _current_return_columns[cid]));
    }

    return Status::OK();
}

Status SegmentIterator::_read_lazy_pruned_columns(Block* block) {
    if (_support_lazy_read_pruned_columns.empty()) {
        return Status::OK();
    }

    SCOPED_RAW_TIMER(&_opts.stats->lazy_read_pruned_ns);
    DorisVector<rowid_t> rowids(_selected_size);
    for (size_t i = 0; i < _selected_size; ++i) {
        rowids[i] = _block_rowids[_sel_rowid_idx[i]];
    }

    for (auto cid : _support_lazy_read_pruned_columns) {
        auto loc = _schema->column_index(cid);
        auto column = IColumn::mutate(std::move(block->get_by_position(loc).column));
        auto* column_iter = _column_iterators[cid].get();
        ScopedColumnIteratorReadPhase scoped_read_phase {column_iter,
                                                         ColumnIterator::ReadPhase::LAZY};
        if (_selected_size > 0) {
            RETURN_IF_ERROR(column_iter->read_by_rowids(rowids.data(), _selected_size, column));
        }
        column_iter->finalize_lazy_phase(column);
        block->get_by_position(loc).column = std::move(column);
    }
    return Status::OK();
}

Status SegmentIterator::next_batch(Block* block) {
    // Replace virtual columns with ColumnNothing at the begining of each next_batch call.
    _init_virtual_columns(block);
    auto status = [&]() {
        RETURN_IF_CATCH_EXCEPTION({
            // Adaptive batch size: predict how many rows this batch should read.
            if (_block_size_predictor) {
                auto predicted = static_cast<uint32_t>(_block_size_predictor->predict_next_rows());
                _opts.block_row_max = std::min(predicted, _initial_block_row_max);
                _opts.stats->adaptive_batch_size_predict_min_rows =
                        std::min(_opts.stats->adaptive_batch_size_predict_min_rows,
                                 static_cast<int64_t>(predicted));
                _opts.stats->adaptive_batch_size_predict_max_rows =
                        std::max(_opts.stats->adaptive_batch_size_predict_max_rows,
                                 static_cast<int64_t>(predicted));
            } else {
                // No predictor — record the fixed batch size using min/max so we don't
                // clobber values already accumulated by other segment iterators that
                // share the same OlapReaderStatistics.
                _opts.stats->adaptive_batch_size_predict_min_rows =
                        std::min(_opts.stats->adaptive_batch_size_predict_min_rows,
                                 static_cast<int64_t>(_opts.block_row_max));
                _opts.stats->adaptive_batch_size_predict_max_rows =
                        std::max(_opts.stats->adaptive_batch_size_predict_max_rows,
                                 static_cast<int64_t>(_opts.block_row_max));
            }

            auto res = _next_batch_internal(block);

            if (res.is<END_OF_FILE>()) {
                // Since we have a type check at the caller.
                // So a replacement of nothing column with real column is needed.
                for (const auto& [cid, expr_ctx] : _virtual_column_exprs) {
                    auto idx = _schema->column_index(cid);
                    auto type = expr_ctx->root()->data_type();
                    block->replace_by_position(idx, type->create_column());
                }

                if (_opts.condition_cache_digest && !_find_condition_cache) {
                    auto* condition_cache = ConditionCache::instance();
                    ConditionCache::CacheKey cache_key(_opts.rowset_id, _segment->id(),
                                                       _opts.condition_cache_digest);
                    VLOG_DEBUG << "Condition cache insert, query id: "
                               << print_id(_opts.runtime_state->query_id())
                               << ", rowset id: " << _opts.rowset_id.to_string()
                               << ", segment id: " << _segment->id()
                               << ", cache digest: " << _opts.condition_cache_digest;
                    condition_cache->insert(cache_key, std::move(_condition_cache));
                }
                return res;
            }

            RETURN_IF_ERROR(res);
            // reverse block row order if read_orderby_key_reverse is true for key topn
            // it should be processed for all success _next_batch_internal
            if (_opts.read_orderby_key_reverse) {
                size_t num_rows = block->rows();
                if (num_rows == 0) {
                    return Status::OK();
                }
                size_t num_columns = block->columns();
                IColumn::Permutation permutation;
                for (size_t i = 0; i < num_rows; ++i) permutation.emplace_back(num_rows - 1 - i);

                for (size_t i = 0; i < num_columns; ++i)
                    block->get_by_position(i).column =
                            block->get_by_position(i).column->permute(permutation, num_rows);
            }

            RETURN_IF_ERROR(block->check_type_and_column());

            // Adaptive batch size: update EWMA estimate from the completed batch.
            // block->bytes() is accurate here: predicates have been applied and non-predicate
            // columns have been filled for surviving rows by _next_batch_internal.
            if (_block_size_predictor && block->rows() > 0) {
                _block_size_predictor->update(*block);
            }

            return Status::OK();
        });
    }();

    // if rows read by batch is 0, will return end of file, we should not remove segment cache in this situation.
    if (!status.ok() && !status.is<END_OF_FILE>()) {
        _segment->update_healthy_status(status);
    }
    return status;
}

Status SegmentIterator::_convert_to_expected_type(const std::vector<ColumnId>& col_ids) {
    for (ColumnId i : col_ids) {
        if (!_current_return_columns[i] || _converted_column_ids[i] || _is_pred_column[i]) {
            continue;
        }
        const TabletColumn* column_desc = _schema->column(i);
        DataTypePtr expected_type = Schema::get_data_type_ptr(*column_desc);
        DataTypePtr file_column_type = _storage_name_and_type[i].second;
        if (!file_column_type->equals(*expected_type)) {
            ColumnPtr expected;
            ColumnPtr original = _current_return_columns[i]->assert_mutable()->get_ptr();
            RETURN_IF_ERROR(variant_util::cast_column({original, file_column_type, ""},
                                                      expected_type, &expected));
            _current_return_columns[i] = expected->assert_mutable();
            _converted_column_ids[i] = true;
            VLOG_DEBUG << fmt::format("Convert {} fom file column type {} to {}, num_rows {}",
                                      column_desc->path_info_ptr() == nullptr
                                              ? ""
                                              : column_desc->path_info_ptr()->get_path(),
                                      file_column_type->get_name(), expected_type->get_name(),
                                      _current_return_columns[i]->size());
        }
    }
    return Status::OK();
}

Status SegmentIterator::copy_column_data_by_selector(IColumn* input_col_ptr,
                                                     MutableColumnPtr& output_col,
                                                     uint16_t* sel_rowid_idx, uint16_t select_size,
                                                     size_t batch_size) {
    if (is_column_nullable(*output_col) != is_column_nullable(*input_col_ptr)) {
        LOG(WARNING) << "nullable mismatch for output_column: " << output_col->dump_structure()
                     << " input_column: " << input_col_ptr->dump_structure()
                     << " select_size: " << select_size;
        return Status::RuntimeError("copy_column_data_by_selector nullable mismatch");
    }
    output_col->reserve(select_size);
    return input_col_ptr->filter_by_selector(sel_rowid_idx, select_size, output_col.get());
}
// Apache Doris BE 中 存储引擎数据读取（Scan）与谓词过滤（Filtering）最核心的物理执行流程（Execution Core）
// 完整实现了存储引擎层的 延迟物化（Lazy Materialization）、谓词列与非谓词列分级读取（Multi-stage Reading）、向量化/短路谓词评估（Vectorized & Short-Circuit Predicate Evaluation），
// 以及 虚拟列/索引分级推导（Virtual Column / Match Project Materialization）。
// 整体执行顺序遵循“先读少量谓词列剪枝行号，过滤后再按需提取非谓词列（数据列）”的延迟物化思想：
Status SegmentIterator::_next_batch_internal(Block* block) {
    SCOPED_CONCURRENCY_COUNT(ConcurrencyStatsManager::instance().segment_iterator_next_batch);
    // 内存重用（mem_reuse）：Doris 存储引擎为了极力减少 JVM/C++ 堆内存分配与 GC 压力，要求上层传入的 Block 必须复用底层的 Column Buffer。
    bool is_mem_reuse = block->mem_reuse();
    DCHECK(is_mem_reuse);
    // 真正触发索引裁剪（ZoneMap, BloomFilter, Inverted Index 等），生成最终待扫描的物理行号集合（_row_bitmap）
    RETURN_IF_ERROR(_lazy_init(block));

    SCOPED_RAW_TIMER(&_opts.stats->block_load_ns);
    // 自适应 Batch Limit 上限裁剪（TopN / Limit 优化）
    if (_opts.read_limit > 0 && _rows_returned >= _opts.read_limit) {
        return _process_eof(block);
    }

    // If the row bitmap size is smaller than nrows_read_limit, there's no need to reserve that many column rows.
    uint32_t nrows_read_limit =
            std::min(cast_set<uint32_t>(_row_bitmap.cardinality()), _opts.block_row_max);
    // 当下推到 Segment 的查询只有 LIMIT 而没有剩余需要在 SegmentIterator 侧评估的 Filter 时，直接将本批次的读取上限封顶为 cap。
    // 这极大地优化了单纯 TopN/Limit 查询的磁盘 IO 开销，避免了多余的数据 Page 读取。
    if (_can_opt_limit_reads()) {
        // No SegmentIterator-side conjunct remains to be evaluated, so LIMIT is equivalent before
        // and after filtering. Cap the first read directly; this is the no-conjunct fast path that
        // avoids reading rows past the pushed-down local LIMIT.
        size_t cap = (_opts.read_limit > _rows_returned) ? (_opts.read_limit - _rows_returned) : 0;
        if (cap < nrows_read_limit) {
            nrows_read_limit = static_cast<uint32_t>(cap);
        }
    }
    DBUG_EXECUTE_IF("segment_iterator.topn_opt_1", {
        if (nrows_read_limit != 1) {
            return Status::Error<ErrorCode::INTERNAL_ERROR>(
                    "topn opt 1 execute failed: nrows_read_limit={}, "
                    "_opts.read_limit={}",
                    nrows_read_limit, _opts.read_limit);
        }
    })
    // 重置与初始化输出 Block 结构
    RETURN_IF_ERROR(_init_current_block(block, _current_return_columns, nrows_read_limit));
    // 重置标志位向量，用于记录在当前 Block 读取过程中，有哪些列已经完成过低基数字典编码转换或 Variant 类型转换（防止同一 Block 内重复转换）。
    _converted_column_ids.assign(_schema->columns().size(), false);

    _selected_size = 0;
    // 按索引定位并提取谓词列（数据读取关键）
    // 这是延迟物化（Lazy Materialization）的第一个物理 IO 动作。
    // 读取范围：它只读取 _predicate_column_ids 中的列（即 WHERE 条件中涉及到的列，如 age > 18 中的 age 列）。
    RETURN_IF_ERROR(_read_columns_by_index(nrows_read_limit, _selected_size));
    // MVCC 多版本与时间戳替换（Unique / Aggregate 模型特有）
    // 在 Doris 的 Unique Key 模型（尤其是 MoW - Merge on Write 或 Sequence 列）或 Aggregate 模型中，如果谓词列中包含了隐式的 Version 列/ Sequence 比较列，此处会用最新的版本号替换当前 Block 中的旧版本数据。
    _replace_version_col_if_needed(_predicate_column_ids, _selected_size);
    // 在支持 事务/TSO（Timestamp Oracle）的存储场景下，将对应谓词列上的时间戳信息更新为可读可见状态。
    _update_tso_col_if_needed(_predicate_column_ids, _selected_size);
    // 增加加载的 Block 计数。
    _opts.stats->blocks_load += 1;
    // 累加本次真正从存储层原始读取的行数（用于在 Profile 的 RawRowsRead 中展示）。
    _opts.stats->raw_rows_read += _selected_size;
    // 快速裁剪（Fast-path Return）：如果当前 Segment 数据已经被彻底读完，或者当前 Block 范围内所有数据行都已经全被索引裁剪掉，导致 _selected_size == 0，则直接调用 _process_eof(block) 结束当前 Block 读取。
    if (_selected_size == 0) {
        return _process_eof(block);
    }
    // Apache Doris BE 存储引擎中 SegmentIterator 在向量化（Vectorized）模式下做“谓词过滤（Filtering）与延迟物化数据提取（Lazy Materialization Read）”的核心执行流水线。
    // 是否需要向量化/短路/复杂表达式评估?
    if (_is_need_vec_eval || _is_need_short_eval || _is_need_expr_eval) {
        _sel_rowid_idx.resize(_selected_size);

        // 向量化（Vectorized）与短路（Short-circuit）谓词评估核心控制逻辑。
        if (_is_need_vec_eval || _is_need_short_eval) {
            // 如果列数据类型为 String/VARCHAR 且在物理 Page 存储时使用了字典编码（Low Cardinality/Dictionary Encoding），Doris 会将上层传入的字符串谓词（如 city = 'Beijing'）隐式转换为对字典 ID（整数） 的比较。
            _convert_dict_code_for_predicate_if_necessary();

            // step 1: evaluate vectorization predicate
            // Step 1: SIMD 向量化谓词评估（Vectorized Evaluation）
            // 处理对象：数值类型（Int, Float, Decimal 等）的简单过滤条件（如 a > 10）。
            // 底层机制：利用 C++ 向量化与 SIMD 指令并行计算生成 Selection Vector（选择位图）。
            // 作用：将当前批次（Block）中存活的行相对索引写入 _sel_rowid_idx 数组，并更新 _selected_size（缩小计算基数）。
            _selected_size =
                    _evaluate_vectorization_predicate(_sel_rowid_idx.data(), _selected_size);

            // step 2: evaluate short circuit predicate
            // todo(wb) research whether need to read short predicate after vectorization evaluation
            //          to reduce cost of read short circuit columns.
            //          In SSB test, it make no difference; So need more scenarios to test
            // Step 2: 动态短路谓词评估（Short-Circuit Evaluation）
            // 处理对象：变长数据类型、正则表达式、IN 集合过滤或开销较大的标量函数过滤。
            // 级联效益：前置的 Step 1 已经淘汰了大部分不匹配的行，因此 Step 2 的短路评估仅需要作用于 _sel_rowid_idx 中残存的少数行上，有效节省 CPU 算力。
            _selected_size =
                    _evaluate_short_circuit_predicate(_sel_rowid_idx.data(), _selected_size);
            VLOG_DEBUG << fmt::format("After evaluate predicates, selected size: {} ",
                                      _selected_size);
            if (_selected_size > 0) {
                // step 3.1: output short circuit and predicate column
                // when lazy materialization enables, _predicate_column_ids = distinct(_short_cir_pred_column_ids + _vec_pred_column_ids)
                // see _vec_init_lazy_materialization
                // todo(wb) need to tell input columnids from output columnids
                // 输出对齐：当经过 Step 1 和 Step 2 筛选后仍有行存活（_selected_size > 0）时，通过存活索引数组 _sel_rowid_idx 将这些谓词列的真实数据拷贝写入输出 Block 的对应列中。
                RETURN_IF_ERROR(_output_column_by_sel_idx(block, _predicate_column_ids,
                                                          _sel_rowid_idx.data(), _selected_size));

                // step 3.2: read remaining expr column and evaluate it.
                // Step 3.2: 复杂通用表达式列的“二次延迟读取与评估”（Common Expr Evaluation）
                // 延迟提取：如果 SQL 中包含了不能直接在物理存储层下推计算的复合表达式（例如 concat(a, b) = 'xyz'）：
                // Doris 在此之前完全不读取 a 和 b 列。
                // 直到 Step 1/2 计算完成且有数据存活时，才调用 _read_columns_by_rowids 根据存活行的物理 RowID 精准提取 _common_expr_column_ids。
                if (_is_need_expr_eval) {
                    // The predicate column contains the remaining expr column, no need second read.
                    if (_common_expr_column_ids.size() > 0) {
                        SCOPED_RAW_TIMER(&_opts.stats->non_predicate_read_ns);
                        RETURN_IF_ERROR(_read_columns_by_rowids(
                                _common_expr_column_ids, _block_rowids, _sel_rowid_idx.data(),
                                _selected_size, &_current_return_columns, false, true));
                        _replace_version_col_if_needed(_common_expr_column_ids, _selected_size);
                        _update_tso_col_if_needed(_common_expr_column_ids, _selected_size);
                        RETURN_IF_ERROR(_process_columns(_common_expr_column_ids, block));
                    }

                    DCHECK(block->columns() > _schema->column_index(*_common_expr_columns.begin()));
                    RETURN_IF_ERROR(
                            _process_common_expr(_sel_rowid_idx.data(), _selected_size, block));
                }
            } else {
            // 全剪枝 Fast-path (Zero-selected Handle)
            // 零存活优化：如果 Step 1 或 Step 2 执行后 _selected_size == 0（当前 Block 中的行全被 Filter 过滤掉）：
                _fill_column_nothing();
                if (_is_need_expr_eval) {
                    RETURN_IF_ERROR(_process_columns(_common_expr_column_ids, block));
                }
            }
        } else if (_is_need_expr_eval) {
        // 当物理层不需要（或无法）执行 SIMD 向量化/短路谓词剪枝时，系统会回退到此逻辑，直接对当前 Block 内的所有行进行表达式求解。
        // 进入该分支的前提是需要进行表达式评估（_is_need_expr_eval == true），因此表达式所依赖的物理输入列（_predicate_column_ids）绝不能为空。
        // 如果为空，说明上层 Planner/Optimizer 下推的表达式解析有问题，在 Debug 模式下会直接触发断言崩溃以提醒开发者。
            DCHECK(!_predicate_column_ids.empty());
            // 将底层列数据输出到 Block 容器
            // _process_columns：负责将这些原始物理列格式（例如 Run-Length / Bit-Shuffle 编码解压后的列）转换为向量化 Block 所要求的标准 Column 接口形态（如 ColumnVector, ColumnString 等），为后面的 VExpr（向量化表达式）计算提供统一的数据结构。
            RETURN_IF_ERROR(_process_columns(_predicate_column_ids, block));
            // first read all rows are insert block, initialize sel_rowid_idx to all rows.
            for (uint16_t i = 0; i < _selected_size; ++i) {
                _sel_rowid_idx[i] = i;
            }
            // 执行通用表达式评估
            RETURN_IF_ERROR(_process_common_expr(_sel_rowid_idx.data(), _selected_size, block));
        }
        // 负责对最终存活的行进行 Limit 截断、稀疏物理读取“非谓词数据列（Non-predicate Columns）”，并更新条件缓存（Condition Cache）与补全延迟剪枝列。
        // 如果查询包含 LIMIT（例如 SELECT col_a FROM tbl WHERE col_b > 10 LIMIT 5），且经过谓词评估后存活的行数（_selected_size）依然大于剩余需要的 read_limit 行数，该函数会直接将 _selected_size 裁剪压缩到 limit 对应的大小。
        RETURN_IF_ERROR(_apply_read_limit_to_selected_rows(block, _selected_size));

        // step4: read non_predicate column
        // 非谓词数据列的按需稀疏读取（Late Materialization Core）
        // 延迟物化真正的性能爆发点：_non_predicate_columns 指的是仅出现在 SELECT 目标列表中、但未参与任何 WHERE 谓词过滤的数据列（例如超大文本 comment 列）
        if (_selected_size > 0) {
            if (!_non_predicate_columns.empty()) {
                // 传入了存活行下标 _sel_rowid_idx.data() 和最终存活行数 _selected_size。
                // 机制：它只去磁盘读取这 _selected_size 行数据所在的 Data Page（点查/跳跃式 Page 提取），完全跳过了那些被过滤掉的几万/几十万行数据，极大地节省了磁盘 IO 吞吐与内存解压开销。
                RETURN_IF_ERROR(_read_columns_by_rowids(
                        _non_predicate_columns, _block_rowids, _sel_rowid_idx.data(),
                        _selected_size, &_current_return_columns,
                        _opts.condition_cache_digest && !_find_condition_cache, false));
                // 针对刚读出的非谓词列，补充 Unique/Aggregate 模型的 MVCC 版本号与 TSO 时间戳修正。
                _replace_version_col_if_needed(_non_predicate_columns, _selected_size);
                _update_tso_col_if_needed(_non_predicate_columns, _selected_size);
            } else {
            // 触发场景：当前查询没有需要读取的非谓词列（例如 SELECT count(*) WHERE ... 或只查谓词列），但启用了条件缓存（Condition Cache）。
            // 作用机制：通过物理 rowid 计算出对应的 Cache Offset，将该 Segment 内匹配当前条件行的 Block 粒度缓存位置置为 true。后续重复/相似查询可以直接复用该 Cache，跳过对底层物理索引和谓词的二次计算。
                if (_opts.condition_cache_digest && !_find_condition_cache) {
                    auto& condition_cache = *_condition_cache;
                    for (size_t i = 0; i < _selected_size; ++i) {
                        auto rowid = _block_rowids[_sel_rowid_idx[i]];
                        condition_cache[rowid / SegmentIterator::CONDITION_CACHE_OFFSET] = true;
                    }
                }
            }
        }
        // 对于动态 Schema 类型（如 Variant / JSON 列）或复杂的 Complex/Nested 嵌套数据类型，为了进一步提升性能，部分子列的提取会被推迟到最后阶段。
        RETURN_IF_ERROR(_read_lazy_pruned_columns(block));
    }

    // step5: output columns
    // 在经历了前面的谓词列过滤、数据列延迟物化提取之后，这段代码负责将非谓词列写回 Block、将倒排索引匹配结果/打分转为虚拟列、完成虚拟列物化、更新返回行数计数，并对最终输出的 Block 进行合规性检查。
    // 将提取的非谓词列数据填充/写回 Block
    RETURN_IF_ERROR(_output_non_pred_columns(block));
    // Convert inverted index bitmaps to result columns for virtual column exprs
    // (e.g., MATCH projections). This must run before _materialization_of_virtual_column
    // so that fast_execute() can find the pre-computed result columns.

    // 倒排索引结果转虚拟列（Inverted Index Bitmap / Match Projection）
    // 核心背景：在全文检索/倒排索引场景下，用户可能会在 SELECT 目标列表中直接投影 MATCH 的匹配结果（例如相关性打分 Score、高亮位置或匹配标志位）。
    // 作用：根据前面的选择集位图（sel_rowid_idx）和存活行数 _selected_size，把倒排索引推导出的内存位图/匹配数据预先转化为真实的 Result Column 并填入 Block，供后续表达式的 fast_execute() 快速提取使用。
    if (!_virtual_column_exprs.empty()) {
        bool use_sel = _is_need_vec_eval || _is_need_short_eval || _is_need_expr_eval;
        uint16_t* sel_rowid_idx = use_sel ? _sel_rowid_idx.data() : nullptr;
        VExprContextSPtrs vir_ctxs;
        vir_ctxs.reserve(_virtual_column_exprs.size());
        for (auto& [cid, ctx] : _virtual_column_exprs) {
            vir_ctxs.push_back(ctx);
        }
        _output_index_result_column(vir_ctxs, sel_rowid_idx, _selected_size);
    }
    // 虚拟列物化（Virtual Column Materialization）
    // 针对虚拟列（Virtual Columns，例如由表达式派生出的非物理持久化列、全文检索得分列、或 Variant 提取的动态虚拟字段），在此处调用表达式上下文进行物化计算，将其转化为标准 Column 追加到 block 中。
    RETURN_IF_ERROR(_materialization_of_virtual_column(block));
    if (_opts.read_limit > 0) {
        _rows_returned += block->rows();
    }
    return _check_output_block(block);
}

Status SegmentIterator::_process_columns(const std::vector<ColumnId>& column_ids, Block* block) {
    RETURN_IF_ERROR(_convert_to_expected_type(column_ids));
    for (auto cid : column_ids) {
        auto loc = _schema->column_index(cid);
        block->replace_by_position(loc, std::move(_current_return_columns[cid]));
    }
    return Status::OK();
}

void SegmentIterator::_fill_column_nothing() {
    // If column_predicate filters out all rows, the corresponding column in _current_return_columns[cid] must be a ColumnNothing.
    // Because:
    // 1. Before each batch, _init_return_columns is called to initialize _current_return_columns, and virtual columns in _current_return_columns are initialized as ColumnNothing.
    // 2. When select_size == 0, the read method of VirtualColumnIterator will definitely not be called, so the corresponding Column remains a ColumnNothing
    for (const auto& [cid, expr_ctx] : _virtual_column_exprs) {
        [[maybe_unused]] const auto* nothing_col =
                assert_cast<const ColumnNothing*>(_current_return_columns[cid].get());
        _current_return_columns[cid] = expr_ctx->root()->data_type()->create_column();
    }
}

Status SegmentIterator::_check_output_block(Block* block) {
#ifndef NDEBUG
    size_t rows = block->rows();
    size_t idx = 0;
    for (const auto& entry : *block) {
        if (!entry.column) {
            return Status::InternalError(
                    "Column in idx {} is null, block columns {}, normal_columns {}, "
                    "virtual_columns {}",
                    idx, block->columns(), _schema->num_column_ids(), _virtual_column_exprs.size());
        } else if (check_and_get_column<ColumnNothing>(entry.column.get())) {
            if (rows > 0) {
                std::vector<ColumnId> virtual_column_ids;
                for (const auto& pair : _virtual_column_exprs) {
                    virtual_column_ids.push_back(pair.first);
                }
                return Status::InternalError(
                        "Column in idx {} is nothing, block columns {}, normal_columns {}, "
                        "virtual_column_ids [{}]",
                        idx, block->columns(), _schema->num_column_ids(),
                        fmt::join(virtual_column_ids, ","));
            }
        } else if (entry.column->size() != rows) {
            return Status::InternalError(
                    "Unmatched size {}, expected {}, column: {}, type: {}, idx_in_block: {}, "
                    "block: {}",
                    entry.column->size(), rows, entry.column->get_name(), entry.type->get_name(),
                    idx, block->dump_structure());
        }
        idx++;
    }
#endif
    return Status::OK();
}

Status SegmentIterator::_process_eof(Block* block) {
    // Convert all columns in _current_return_columns to schema column
    RETURN_IF_ERROR(_convert_to_expected_type(_schema->column_ids()));
    for (int i = 0; i < block->columns(); i++) {
        auto cid = _schema->column_id(i);
        if (!_is_pred_column[cid]) {
            block->replace_by_position(i, std::move(_current_return_columns[cid]));
        }
    }
    block->clear_column_data();
    // clear and release iterators memory footprint in advance
    _column_iterators.clear();
    _index_iterators.clear();
    return Status::EndOfFile("no more data in segment");
}

Status SegmentIterator::_process_common_expr(uint16_t* sel_rowid_idx, uint16_t& selected_size,
                                             Block* block) {
    VLOG_DEBUG << fmt::format("Execute common expr. block rows {}, selected size {}", block->rows(),
                              _selected_size);

    RETURN_IF_ERROR(_execute_common_expr(sel_rowid_idx, selected_size, block));

    VLOG_DEBUG << fmt::format("Execute common expr end. block rows {}, selected size {}",
                              block->rows(), _selected_size);
    return Status::OK();
}

Status SegmentIterator::_execute_common_expr(uint16_t* sel_rowid_idx, uint16_t& selected_size,
                                             Block* block) {
    SCOPED_RAW_TIMER(&_opts.stats->expr_filter_ns);
    DCHECK(!_common_expr_ctxs_push_down.empty());
    _output_index_result_column(_common_expr_ctxs_push_down, sel_rowid_idx, selected_size);

    uint16_t original_size = selected_size;
    _opts.stats->expr_cond_input_rows += original_size;

    // Some output columns may stay empty until after common expr filtering. Use the
    // selected row count instead of Block::rows(), which is derived from the first column.
    IColumn::Filter filter(selected_size, 1);
    bool can_filter_all = false;
    auto* __restrict filter_data = filter.data();
    for (const auto& expr_ctx : _common_expr_ctxs_push_down) {
        RETURN_IF_ERROR(expr_ctx->execute_filter(block, filter_data, selected_size, false,
                                                 &can_filter_all));
        if (can_filter_all) {
            break;
        }
    }
    RETURN_IF_CATCH_EXCEPTION(Block::filter_block_internal(block, _columns_to_filter, filter));

    selected_size = _evaluate_common_expr_filter(sel_rowid_idx, selected_size, filter);
    _opts.stats->rows_expr_cond_filtered += original_size - selected_size;
    return Status::OK();
}

uint16_t SegmentIterator::_evaluate_common_expr_filter(uint16_t* sel_rowid_idx,
                                                       uint16_t selected_size,
                                                       const IColumn::Filter& filter) {
    size_t count = filter.size() - simd::count_zero_num((int8_t*)filter.data(), filter.size());
    if (count == 0) {
        return 0;
    } else {
        const UInt8* filt_pos = filter.data();

        uint16_t new_size = 0;
        uint32_t sel_pos = 0;
        const uint32_t sel_end = selected_size;
        static constexpr size_t SIMD_BYTES = simd::bits_mask_length();
        const uint32_t sel_end_simd = sel_pos + selected_size / SIMD_BYTES * SIMD_BYTES;

        while (sel_pos < sel_end_simd) {
            auto mask = simd::bytes_mask_to_bits_mask(filt_pos + sel_pos);
            if (0 == mask) {
                //pass
            } else if (simd::bits_mask_all() == mask) {
                for (uint32_t i = 0; i < SIMD_BYTES; i++) {
                    sel_rowid_idx[new_size++] = sel_rowid_idx[sel_pos + i];
                }
            } else {
                simd::iterate_through_bits_mask(
                        [&](const size_t bit_pos) {
                            sel_rowid_idx[new_size++] = sel_rowid_idx[sel_pos + bit_pos];
                        },
                        mask);
            }
            sel_pos += SIMD_BYTES;
        }

        for (; sel_pos < sel_end; sel_pos++) {
            if (filt_pos[sel_pos]) {
                sel_rowid_idx[new_size++] = sel_rowid_idx[sel_pos];
            }
        }
        return new_size;
    }
}

void SegmentIterator::_output_index_result_column(const VExprContextSPtrs& expr_ctxs,
                                                  uint16_t* sel_rowid_idx, uint16_t select_size) {
    SCOPED_RAW_TIMER(&_opts.stats->output_index_result_column_timer);
    if (select_size == 0) {
        return;
    }
    for (const auto& expr_ctx : expr_ctxs) {
        auto index_ctx = expr_ctx->get_index_context();
        if (index_ctx == nullptr) {
            continue;
        }
        for (auto& inverted_index_result_bitmap_for_expr : index_ctx->get_index_result_bitmap()) {
            const auto* expr = inverted_index_result_bitmap_for_expr.first;
            const auto& result_bitmap = inverted_index_result_bitmap_for_expr.second;
            const auto& index_result_bitmap = result_bitmap.get_data_bitmap();
            auto index_result_column = ColumnUInt8::create();
            ColumnUInt8::Container& vec_match_pred = index_result_column->get_data();
            vec_match_pred.resize(select_size);
            std::fill(vec_match_pred.begin(), vec_match_pred.end(), 0);

            const auto& null_bitmap = result_bitmap.get_null_bitmap();
            bool has_null_bitmap = null_bitmap != nullptr && !null_bitmap->isEmpty();
            bool expr_returns_nullable = expr->data_type()->is_nullable();

            ColumnUInt8::MutablePtr null_map_column = nullptr;
            ColumnUInt8::Container* null_map_data = nullptr;
            if (has_null_bitmap && expr_returns_nullable) {
                null_map_column = ColumnUInt8::create();
                auto& null_map_vec = null_map_column->get_data();
                null_map_vec.resize(select_size);
                std::fill(null_map_vec.begin(), null_map_vec.end(), 0);
                null_map_data = &null_map_column->get_data();
            }

            roaring::BulkContext bulk_context;
            for (uint32_t i = 0; i < select_size; i++) {
                auto rowid = sel_rowid_idx ? _block_rowids[sel_rowid_idx[i]] : _block_rowids[i];
                if (index_result_bitmap) {
                    vec_match_pred[i] = index_result_bitmap->containsBulk(bulk_context, rowid);
                }
                if (null_map_data != nullptr && null_bitmap->contains(rowid)) {
                    (*null_map_data)[i] = 1;
                    vec_match_pred[i] = 0;
                }
            }

            DCHECK(select_size == vec_match_pred.size());

            if (null_map_column) {
                index_ctx->set_index_result_column_for_expr(
                        expr, ColumnNullable::create(std::move(index_result_column),
                                                     std::move(null_map_column)));
            } else {
                index_ctx->set_index_result_column_for_expr(expr, std::move(index_result_column));
            }
        }
    }
}

// Dictionary codes are initially assigned in dictionary insertion order, so their numeric order
// does not necessarily match the order of the encoded values. For example, an initial dictionary
// {0: "zebra", 1: "apple", 2: "mango"} is sorted into {0: "apple", 1: "mango", 2: "zebra"},
// and row codes are remapped from {0, 2, 1} to {2, 1, 0}. Range predicates compare codes with <,
// <=, >, or >= and therefore require this conversion. IN/NOT IN predicates do not: they build a
// membership bitmap indexed by the existing dictionary codes. Bloom-filter predicates instead need
// hash values initialized for dictionary entries.
void SegmentIterator::_convert_dict_code_for_predicate_if_necessary() {
    for (auto predicate : _short_cir_eval_predicate) {
        _convert_dict_code_for_predicate_if_necessary_impl(predicate);
    }

    for (auto predicate : _pre_eval_block_predicate) {
        _convert_dict_code_for_predicate_if_necessary_impl(predicate);
    }

    for (auto column_id : _delete_range_column_ids) {
        _current_return_columns[column_id].get()->convert_dict_codes_if_necessary();
    }

    for (auto column_id : _delete_bloom_filter_column_ids) {
        _current_return_columns[column_id].get()->initialize_hash_values_for_runtime_filter();
    }
}

void SegmentIterator::_convert_dict_code_for_predicate_if_necessary_impl(
        std::shared_ptr<ColumnPredicate> predicate) {
    auto& column = _current_return_columns[predicate->column_id()];
    auto* col_ptr = column.get();

    if (PredicateTypeTraits::is_range(predicate->type())) {
        col_ptr->convert_dict_codes_if_necessary();
    } else if (PredicateTypeTraits::is_bloom_filter(predicate->type())) {
        col_ptr->initialize_hash_values_for_runtime_filter();
    }
}

Status SegmentIterator::current_block_row_locations(std::vector<RowLocation>* block_row_locations) {
    DCHECK(_opts.record_rowids);
    DCHECK_GE(_block_rowids.size(), _selected_size);
    block_row_locations->resize(_selected_size);
    uint32_t sid = segment_id();
    if (!_is_need_vec_eval && !_is_need_short_eval && !_is_need_expr_eval) {
        for (auto i = 0; i < _selected_size; i++) {
            (*block_row_locations)[i] = RowLocation(sid, _block_rowids[i]);
        }
    } else {
        for (auto i = 0; i < _selected_size; i++) {
            (*block_row_locations)[i] = RowLocation(sid, _block_rowids[_sel_rowid_idx[i]]);
        }
    }
    return Status::OK();
}

Status SegmentIterator::_construct_compound_expr_context() {
    ColumnIteratorOptions iter_opts {
            .use_page_cache = _opts.use_page_cache,
            .file_reader = _file_reader.get(),
            .stats = _opts.stats,
            .io_ctx = _opts.io_ctx,
    };
    auto inverted_index_context = std::make_shared<IndexExecContext>(
            _schema->column_ids(), _index_iterators, _storage_name_and_type,
            _common_expr_index_exec_status, _score_runtime, _segment.get(), iter_opts);
    inverted_index_context->set_index_query_context(_index_query_context);
    for (const auto& expr_ctx : _opts.common_expr_ctxs_push_down) {
        VExprContextSPtr context;
        // _ann_range_search_runtime will do deep copy.
        RETURN_IF_ERROR(expr_ctx->clone(_opts.runtime_state, context));
        context->set_index_context(inverted_index_context);
        _common_expr_ctxs_push_down.emplace_back(context);
    }
    // Clone virtual column exprs before setting IndexExecContext, because
    // IndexExecContext holds segment-specific index iterator references.
    // Without cloning, shared VExprContext would be overwritten per-segment
    // and could point to the wrong segment's context.
    for (auto& [cid, expr_ctx] : _virtual_column_exprs) {
        VExprContextSPtr context;
        RETURN_IF_ERROR(expr_ctx->clone(_opts.runtime_state, context));
        context->set_index_context(inverted_index_context);
        expr_ctx = context;
    }
    return Status::OK();
}

Status SegmentIterator::_apply_expr_zonemap_to_row_ranges(const VExprContextSPtrs& conjuncts,
                                                          rowid_t min_rowid,
                                                          RowRanges* row_ranges) {
    DORIS_CHECK(row_ranges != nullptr);
    if (!expr_zonemap::is_expr_zonemap_filter_enabled(_opts.runtime_state) || conjuncts.empty() ||
        row_ranges->is_empty()) {
        return Status::OK();
    }

    std::unordered_map<int, VExprContextSPtrs> ctxs_by_slot;
    for (const auto& conjunct : conjuncts) {
        auto slot_index = expr_zonemap::single_slot_zonemap_index(conjunct);
        if (slot_index >= 0) {
            ctxs_by_slot[slot_index].emplace_back(conjunct);
        }
    }
    // Page zone maps are stored per column. Multi-slot expressions need page alignment across
    // multiple column readers and are therefore left to segment-level pruning for now.
    if (ctxs_by_slot.empty()) {
        return Status::OK();
    }

    ColumnIteratorOptions iter_opts {
            .use_page_cache = _opts.use_page_cache,
            .file_reader = _file_reader.get(),
            .stats = _opts.stats,
            .io_ctx = _opts.io_ctx,
    };
    for (const auto& [slot_index, slot_conjuncts] : ctxs_by_slot) {
        if (cast_set<size_t>(slot_index) >= _schema->num_column_ids()) {
            continue;
        }
        const auto cid = _schema->column_id(cast_set<size_t>(slot_index));
        if (!_segment->can_apply_predicate_safely(cid, *_schema,
                                                  _opts.target_cast_type_for_variants, _opts)) {
            continue;
        }
        const auto* tablet_column = _schema->column(cid);
        if (tablet_column == nullptr) {
            continue;
        }
        std::shared_ptr<ColumnReader> reader;
        Status st =
                _segment->get_column_reader(*tablet_column, &reader, _opts.stats, &_opts.io_ctx);
        if (st.is<ErrorCode::NOT_FOUND>()) {
            continue;
        }
        RETURN_IF_ERROR(st);
        if (reader == nullptr || !reader->has_zone_map()) {
            continue;
        }
        const std::vector<ZoneMapPB>* page_zone_maps = nullptr;
        RETURN_IF_ERROR(reader->get_page_zone_maps(iter_opts, &page_zone_maps));
        if (page_zone_maps == nullptr || page_zone_maps->empty()) {
            continue;
        }
        auto data_type = _segment->get_data_type_of(*tablet_column, _opts);
        if (data_type == nullptr) {
            continue;
        }

        RowRanges column_ranges;
        ZoneMapEvalStats page_stats;
        for (uint32_t page_index = 0; page_index < page_zone_maps->size(); ++page_index) {
            RowRange page_range;
            RETURN_IF_ERROR(reader->get_row_range_for_page(page_index, iter_opts, &page_range));
            if (!page_range.is_valid() || page_range.to() <= min_rowid) {
                continue;
            }
            ZoneMapEvalContext ctx;
            ZoneMapEvalContext::SlotZoneMap slot_zone_map;
            slot_zone_map.data_type = data_type;
            ZoneMap zone_map;
            RETURN_IF_ERROR(
                    ZoneMap::from_proto((*page_zone_maps)[page_index], data_type, zone_map));
            slot_zone_map.zone_map = std::make_shared<ZoneMap>(std::move(zone_map));
            ctx.slots.emplace(slot_index, std::move(slot_zone_map));
            const auto result = VExprContext::evaluate_zonemap_filter(slot_conjuncts, ctx);
            page_stats.merge_page_eval_stats(ctx.stats);
            if (result != ZoneMapFilterResult::kNoMatch) {
                column_ranges.add(
                        RowRange(std::max<int64_t>(page_range.from(), min_rowid), page_range.to()));
            } else {
                ++_opts.stats->expr_zonemap_filtered_pages;
            }
        }
        page_stats.accumulate_to(_opts.stats);
        RowRanges::ranges_intersection(*row_ranges, column_ranges, row_ranges);
        if (row_ranges->is_empty()) {
            return Status::OK();
        }
    }
    return Status::OK();
}

void SegmentIterator::_calculate_common_expr_index_exec_status() {
    for (const auto& root_expr_ctx : _common_expr_ctxs_push_down) {
        const auto& root_expr = root_expr_ctx->root();
        if (root_expr == nullptr) {
            continue;
        }
        _common_expr_to_slotref_map[root_expr_ctx.get()] = std::unordered_map<ColumnId, VExpr*>();

        std::stack<VExprSPtr> stack;
        stack.emplace(root_expr);

        while (!stack.empty()) {
            const auto& expr = stack.top();
            stack.pop();

            for (const auto& child : expr->children()) {
                if (child->is_virtual_slot_ref()) {
                    // Expand virtual slot ref to its underlying expression tree and
                    // collect real slot refs used inside. We still associate those
                    // slot refs with the current parent expr node for inverted index
                    // tracking, just like normal slot refs.
                    auto* vir_slot_ref = assert_cast<VirtualSlotRef*>(child.get());
                    auto vir_expr = vir_slot_ref->get_virtual_column_expr();
                    if (vir_expr) {
                        std::stack<VExprSPtr> vir_stack;
                        vir_stack.emplace(vir_expr);

                        while (!vir_stack.empty()) {
                            const auto& vir_node = vir_stack.top();
                            vir_stack.pop();

                            for (const auto& vir_child : vir_node->children()) {
                                if (vir_child->is_slot_ref()) {
                                    auto* inner_slot_ref = assert_cast<VSlotRef*>(vir_child.get());
                                    auto cid = _schema->column_id(inner_slot_ref->column_id());
                                    _common_expr_index_exec_status[cid][expr.get()] = false;
                                    _common_expr_to_slotref_map[root_expr_ctx.get()]
                                                               [inner_slot_ref->column_id()] =
                                                                       expr.get();
                                }

                                if (!vir_child->children().empty()) {
                                    vir_stack.emplace(vir_child);
                                }
                            }
                        }
                    }
                }
                // Example: CAST(v['a'] AS VARCHAR) MATCH 'hello', do not add CAST expr to index tracking.
                auto expr_without_cast = VExpr::expr_without_cast(child);
                if (expr_without_cast->is_slot_ref() && expr->op() != TExprOpcode::CAST) {
                    auto* column_slot_ref = assert_cast<VSlotRef*>(expr_without_cast.get());
                    auto cid = _schema->column_id(column_slot_ref->column_id());
                    _common_expr_index_exec_status[cid][expr.get()] = false;
                    _common_expr_to_slotref_map[root_expr_ctx.get()][column_slot_ref->column_id()] =
                            expr.get();
                }
            }

            const auto& children = expr->children();
            for (int i = cast_set<int>(children.size()) - 1; i >= 0; --i) {
                if (!children[i]->children().empty()) {
                    stack.emplace(children[i]);
                }
            }
        }
    }
}

bool SegmentIterator::_no_need_read_key_data(ColumnId cid, MutableColumnPtr& column,
                                             size_t nrows_read) {
    if (_opts.runtime_state && !_opts.runtime_state->query_options().enable_no_need_read_data_opt) {
        return false;
    }

    if (!((_opts.tablet_schema->keys_type() == KeysType::DUP_KEYS ||
           (_opts.tablet_schema->keys_type() == KeysType::UNIQUE_KEYS &&
            _opts.enable_unique_key_merge_on_write)))) {
        return false;
    }

    if (_opts.push_down_agg_type_opt != TPushAggOp::COUNT_ON_INDEX) {
        return false;
    }

    if (!_opts.tablet_schema->column(cid).is_key()) {
        return false;
    }

    if (_has_delete_predicate(cid)) {
        return false;
    }

    if (!_check_all_conditions_passed_inverted_index_for_column(cid)) {
        return false;
    }

    insert_many_not_null_defaults(column, nrows_read);
    return true;
}

bool SegmentIterator::_has_delete_predicate(ColumnId cid) {
    std::set<uint32_t> delete_columns_set;
    _opts.delete_condition_predicates->get_all_column_ids(delete_columns_set);
    return delete_columns_set.contains(cid);
}

bool SegmentIterator::_can_opt_limit_reads() {
    if (_opts.read_limit == 0) {
        return false;
    }

    // If SegmentIterator still needs to evaluate predicates/common exprs, LIMIT must be applied to
    // post-filter rows by _apply_read_limit_to_selected_rows(); capping the raw read here could
    // return fewer rows than the query LIMIT.
    if (_is_need_vec_eval || _is_need_short_eval || _is_need_expr_eval) {
        return false;
    }

    if (_opts.delete_condition_predicates->num_of_column_predicate() > 0) {
        return false;
    }

    bool all_true = std::ranges::all_of(_schema->column_ids(), [this](auto cid) {
        if (cid == _opts.tablet_schema->delete_sign_idx()) {
            return true;
        }
        if (_check_all_conditions_passed_inverted_index_for_column(cid, true)) {
            return true;
        }
        return false;
    });

    DBUG_EXECUTE_IF("segment_iterator.topn_opt_1", {
        LOG(INFO) << "col_predicates: " << _col_predicates.size() << ", all_true: " << all_true;
    })

    DBUG_EXECUTE_IF("segment_iterator.topn_opt_2", {
        if (all_true) {
            return Status::Error<ErrorCode::INTERNAL_ERROR>("topn opt 2 execute failed");
        }
    })

    return all_true;
}

// Before get next batch. make sure all virtual columns in block has type ColumnNothing.
void SegmentIterator::_init_virtual_columns(Block* block) {
    for (const auto& [cid, expr_ctx] : _virtual_column_exprs) {
        auto idx = _schema->column_index(cid);
        auto& col_with_type_and_name = block->get_by_position(idx);
        col_with_type_and_name.column = ColumnNothing::create(0);
        col_with_type_and_name.type = expr_ctx->root()->data_type();
    }
}

Status SegmentIterator::_materialization_of_virtual_column(Block* block) {
    // Some expr can not process empty block, such as function `element_at`.
    // So materialize virtual column in advance to avoid errors.
    if (_selected_size == 0) {
        for (const auto& [cid, expr_ctx] : _virtual_column_exprs) {
            auto idx = _schema->column_index(cid);
            auto& col_with_type_and_name = block->get_by_position(idx);
            col_with_type_and_name.column = expr_ctx->root()->data_type()->create_column();
            col_with_type_and_name.type = expr_ctx->root()->data_type();
        }
        return Status::OK();
    }
    if (_virtual_column_exprs.empty()) {
        return Status::OK();
    }

    for (const auto& cid_and_expr : _virtual_column_exprs) {
        auto cid = cid_and_expr.first;
        auto column_expr = cid_and_expr.second;
        auto materialized_pos = _schema->column_index(cid);
        auto& column = block->get_by_position(materialized_pos).column;
        if (check_and_get_column<const ColumnNothing>(column.get())) {
            VLOG_DEBUG << fmt::format("Virtual column is doing materialization, cid {}, col idx {}",
                                      cid, materialized_pos);
            ColumnPtr result_column;
            // The first block column may still be ColumnNothing(0) for a virtual column, while
            // predicates have already reduced _selected_size. Evaluate the expression over the
            // selected row count instead of Block::rows().
            RETURN_IF_ERROR(column_expr->root()->execute_column(column_expr.get(), block, nullptr,
                                                                _selected_size, result_column));

            block->replace_by_position(materialized_pos, std::move(result_column));
        }
    }
    return Status::OK();
}

void SegmentIterator::_prepare_score_column_materialization() {
    if (_score_runtime == nullptr) {
        return;
    }

    ScoreRangeFilterPtr filter;
    if (_score_runtime->has_score_range_filter()) {
        const auto& range_info = _score_runtime->get_score_range_info();
        filter = std::make_shared<ScoreRangeFilter>(range_info->op, range_info->threshold);
    }

    IColumn::MutablePtr result_column;
    auto result_row_ids = std::make_unique<std::vector<uint64_t>>();
    if (_score_runtime->get_limit() > 0 && _col_predicates.empty() &&
        _common_expr_ctxs_push_down.empty()) {
        OrderType order_type = _score_runtime->is_asc() ? OrderType::ASC : OrderType::DESC;
        _index_query_context->collection_similarity->get_topn_bm25_scores(
                &_row_bitmap, result_column, result_row_ids, order_type,
                _score_runtime->get_limit(), filter);
    } else {
        _index_query_context->collection_similarity->get_bm25_scores(&_row_bitmap, result_column,
                                                                     result_row_ids, filter);
    }
    const size_t dst_col_idx = _score_runtime->get_dest_column_idx();
    auto* column_iter = _column_iterators[_schema->column_id(dst_col_idx)].get();
    auto* virtual_column_iter = dynamic_cast<VirtualColumnIterator*>(column_iter);
    virtual_column_iter->prepare_materialization(
            std::move(result_column),
            std::shared_ptr<std::vector<uint64_t>>(std::move(result_row_ids)));
}

} // namespace segment_v2
} // namespace doris
