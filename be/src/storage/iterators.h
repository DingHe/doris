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

#include <cstddef>
#include <memory>
#include <set>

#include "common/status.h"
#include "core/block/block.h"
#include "exprs/score_runtime.h"
#include "exprs/vexpr.h"
#include "io/io_common.h"
#include "runtime/runtime_state.h"
#include "storage/index/ann/ann_topn_runtime.h"
#include "storage/olap_common.h"
#include "storage/predicate/block_column_predicate.h"
#include "storage/predicate/column_predicate.h"
#include "storage/row_cursor.h"
#include "storage/segment/row_ranges.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

class Schema;
class ColumnPredicate;

struct IteratorRowRef;

namespace segment_v2 {
struct SubstreamIterator;
}

// StorageReadOptions 是 Apache Doris 存储层（Segment V2）在执行数据读取时的核心配置与上下文载体。
// 从上层（OlapScanNode / Scanner）一路透传至底层 SegmentIterator，充当了读取配置、过滤下推、算子加速与物理 IO 上下文的“指挥中心”。
class StorageReadOptions {
public:
    // KeyRange 用于表示前缀短 Key 索引（Short Key Index）的检索区间。
    struct KeyRange {
        KeyRange()
                : lower_key(nullptr),
                  include_lower(false),
                  upper_key(nullptr),
                  include_upper(false) {}

        KeyRange(const RowCursor* lower_key_, bool include_lower_, const RowCursor* upper_key_,
                 bool include_upper_)
                : lower_key(lower_key_),
                  include_lower(include_lower_),
                  upper_key(upper_key_),
                  include_upper(include_upper_) {}

        // the lower bound of the range, nullptr if not existed
        // Key 范围的下界游标指针。若为 nullptr 则表示没有下界限制（即 $-\infty$）。
        const RowCursor* lower_key = nullptr;
        // whether `lower_key` is included in the range
        // 标识下界是否为闭区间（true 表示包含下界 >=，false 表示不包含 >）。
        bool include_lower;
        // the upper bound of the range, nullptr if not existed
        // Key 范围的上界游标指针。若为 nullptr 则表示没有上界限制（即 $+\infty$）。
        const RowCursor* upper_key = nullptr;
        // whether `upper_key` is included in the range
        // 标识上界是否为闭区间（true 表示包含上界 <=，false 表示不包含 <）。
        bool include_upper;
        // 计算当前 KeyRange 的哈希摘要（Digest）。
        // 将下界/上界对应的字符串形式以及开闭标志依次进行 64 位哈希计算。主要用于存储层判定查询 Key 范围是否发生变更以及条件缓存（Condition Cache）的 Key 生成。
        uint64_t get_digest(uint64_t seed) const {
            if (lower_key != nullptr) {
                auto key_str = lower_key->to_string();
                seed = HashUtil::hash64(key_str.c_str(), key_str.size(), seed);
                seed = HashUtil::hash64(&include_lower, sizeof(include_lower), seed);
            }

            if (upper_key != nullptr) {
                auto key_str = upper_key->to_string();
                seed = HashUtil::hash64(key_str.c_str(), key_str.size(), seed);
                seed = HashUtil::hash64(&include_upper, sizeof(include_upper), seed);
            }

            return seed;
        }
    };

    // reader's key ranges, empty if not existed.
    // used by short key index to filter row blocks
    // 查询的前缀 Key 范围列表，用于 Short Key 索引在 Segment 中过滤不相关的 Row Block。
    // Doris 存储层的数据是按照排序列（Key Columns）进行升序排列存放的。利用这一物理有序的特性，Doris 不需要为每一行数据都建立索引，而是采用了稀疏索引（Sparse Index）的机制。
    // 自动截取前缀列：系统根据表结构规则（或用户指定的 data_sort_sequence），抽取数据表的前若干个 Key 列，拼造成一个固定/最大长度的“前缀字节串”（Short Key）。
    // 固定步长采样（Index Page）：在生成 Segment 数据文件时，默认每隔 1024 行数据（即一个 RowBlock / Data Page 的起始位置）抽取第一行的前缀 Key，并将该 Key 与对应的行号偏移量（Row ID / Page Offset）写入独立的 Short Key Index Page 中。
    std::vector<KeyRange> key_ranges;

    // For unique-key merge-on-write, the effect is similar to delete_conditions
    // that filters out rows that are deleted in realtime.
    // For a particular row, if delete_bitmap.contains(rowid) means that row is
    // marked deleted and invisible to user anymore.
    // segment_id -> roaring::Roaring*
    // 针对 Unique Key Merge-on-Write (MoW) 模式的删除位图（映射为 segment_id -> RoaringBitmap）。
    // 记录该 Segment 中哪些行已被实时删除或覆盖，读取时直接过滤掉。
    // 记录主键模型中哪些记录已经被删除
    std::unordered_map<uint32_t, std::shared_ptr<roaring::Roaring>> delete_bitmap;
    // Merge-on-Read 模式或 DELETE 语句下推的删除条件谓词，用于判定行数据的可见性。
    std::shared_ptr<AndBlockColumnPredicate> delete_condition_predicates =
            AndBlockColumnPredicate::create_shared();
    // reader's column predicate, nullptr if not existed
    // used to fiter rows in row block
    // 直接应用在数据 Block 上的列级过滤谓词列表（如 a > 10），用于行级别的向量化过滤。
    std::vector<std::shared_ptr<ColumnPredicate>> column_predicates;
    // 按 Column ID 归类的谓词映射，方便 Page 级 ZoneMap 和倒排索引按列快速检索对应的谓词。
    std::unordered_map<int32_t, std::shared_ptr<AndBlockColumnPredicate>> col_id_to_predicates;
    // 专门用于 ZoneMap 索引剪枝判断的删除谓词集合。
    std::unordered_map<int32_t, std::vector<std::shared_ptr<const ColumnPredicate>>>
            del_predicates_for_zone_map;
    // 下推的聚合操作类型（如 COUNT(*), MIN, MAX）。若触发，可利用 SegmentStatsIterator 直接读取统计元数据返回。
    TPushAggOp::type push_down_agg_type_opt = TPushAggOp::NONE;
    // Non-key columns whose predicates were proven always true by the segment zone map and then
    // removed from column_predicates. SegmentIterator can skip reading these non-output columns,
    // or COUNT_ON_INDEX columns, because the column values no longer affect filtering or counting.
    // 已被 Segment 级 ZoneMap 证明在此 Segment 内100% 成立（Always True）的非 Key 谓词列。由于其不再影响过滤结果，SegmentIterator 可跳过该列物理 Page 的读取。
    std::set<uint32_t> zonemap_always_true_pred_cols;

    // REQUIRED (null is not allowed)
    // 物理读取指标收集器，记录 IO 次数、解压耗时、ZoneMap 剪枝数等统计数据，不允许传入 nullptr。
    OlapReaderStatistics* stats = nullptr;
    // 是否允许启用 Doris 的 Data Page Cache 内存缓存。
    bool use_page_cache = false;
    // 向量化迭代器单次返回的最大 Block 行数上限（默认值为 $4096 - 32 = 4064$）。
    uint32_t block_row_max = 4096 - 32; // see https://github.com/apache/doris/pull/11816
    // Effective adaptive batch size byte budget.
    // 向量化 Block 的自适应目标字节预算上限（默认 8MB），用于控制 Batch 内存大小。
    size_t preferred_block_size_bytes = 8388608UL;
    // 当前表/Tablet 的 Schema 元数据信息。
    TabletSchemaSPtr tablet_schema = nullptr;
    // 标记当前 Unique Key 表是否开启了 Merge-on-Write 模式。
    bool enable_unique_key_merge_on_write = false;
    // 是否需要记录并返回每行数据对应的全局 RowID（常用于全局索引、删改事务或二级索引回表）。
    bool record_rowids = false;
    // 运行时 TopN 动态过滤（Runtime Filter）的源节点与目标节点 ID 信息。
    std::vector<int> topn_filter_source_node_ids;
    int topn_filter_target_node_id = -1;
    // used for special optimization for query : ORDER BY key DESC LIMIT n
    // 针对 ORDER BY key DESC LIMIT N 查询的特殊优化标志，指示按短 Key 索引逆序物理读取。
    bool read_orderby_key_reverse = false;
    // For rows with the same key, use ascending order (small-to-large) for tie-breakers.
    // For example, use lower rowset version / segment id first.
    // 在 Key 相同时的排序规则；为 true 时优先按插入顺序（如较低的 Rowset Version / Segment ID）破局。
    bool use_insert_order_when_same = false;
    // 控制读取 Row Binlog 数据的开关及 TSO 对应的列索引。
    bool read_row_binlog = false;
    int binlog_tso_idx = -1;
    // columns for orderby keys
    // 需要进行排序下推的 Key 列 ID 列表。
    std::vector<uint32_t>* read_orderby_key_columns = nullptr;
    // 传递给底层文件系统的 IO 上下文（含 Reader 类型、优先权、异步预读配置等）。
    io::IOContext io_ctx;
    // 下推到存储层执行的通用向量化表达式上下文（支持 Expr-Zonemap 剪枝与过滤）。
    VExprContextSPtrs common_expr_ctxs_push_down;
    // SQL 上层最终需要的输出列集合（投影列）。
    const std::set<int32_t>* output_columns = nullptr;
    // Extra storage key columns that are included only to keep the scan schema
    // aligned with the storage key prefix. SegmentIterator can synthesize
    // placeholders only after proving predicates, delete conditions, and
    // expressions do not need their real values.
    // 为了对齐存储层前缀 Key 结构而额外保留的 Key 列。在证明其不参与过滤和表达式计算后，允许填充占位符。
    std::set<ColumnId> extra_columns;
    // runtime state
    // 上层查询执行期的运行时状态指针。
    RuntimeState* runtime_state = nullptr;
    // 当前正在读取的 Rowset 唯一标识。
    RowsetId rowset_id;
    // 当前读取的数据 Version 版本号。
    Version version;
    // 用于增量同步/Binlog 的事务提交时间戳范围。
    TsoRange commit_tso;
    // 当前 Tablet 的 ID。
    int64_t tablet_id = 0;
    // slots that cast may be eliminated in storage layer
    // 针对 Variant 变体类型，存储层可以消除隐式 Cast 转换的目标数据类型映射。
    std::map<std::string, DataTypePtr> target_cast_type_for_variants;
    // 经过各种索引（ZoneMap、Bitmap、Inverted Index）过滤后最终计算出的待读取物理行号区间集合（Row ID Ranges）。
    RowRanges row_ranges;

    // Per-segment row budget pushed down from the scanner (topn or general
    // limit). SegmentIterator applies it after predicate/common-expr filtering;
    // _can_opt_limit_reads() only decides whether the pre-filter read can also
    // be capped. 0 disables the optimization.
    // 针对当前 Segment 级别的读取行数 Budget（如 LIMIT N）；满足阈值后提前终止扫描。
    size_t read_limit = 0;
    // 虚拟列（如 Variant/JSON 抽取的字段）的生成表达式映射。
    std::map<ColumnId, VExprContextSPtr> virtual_column_exprs;
    // 向量近似最近邻搜索（ANN / Vector Index）的运行时控制上下文。
    std::shared_ptr<segment_v2::AnnTopNRuntime> ann_topn_runtime;
    // 复杂类型（如 JSON/Variant）全量属性访问路径信息。
    std::map<int32_t, TColumnAccessPaths> all_access_paths;
    // 仅在谓词中用到的复杂类型属性访问路径，用于加速子列（Sub-column）提取。
    std::map<int32_t, TColumnAccessPaths> predicate_access_paths;
    // 全文检索/倒排索引相关性打分（如 BM25）的运行时上下文。
    std::shared_ptr<ScoreRuntime> score_runtime;
    // 倒排索引打分所需的集合词频统计数据（Collection Level Statistics）。
    CollectionStatisticsPtr collection_statistics;

    // Cache for sparse column data to avoid redundant reads
    // col_unique_id -> cached column_ptr
    // 稀疏列（如 Variant 的动态列）数据的缓存，避免多轮过滤中的重复读取。
    std::unordered_map<int32_t, ColumnPtr> sparse_column_cache;
    // 整个读取条件集的哈希摘要，用于查询级别的执行条件缓存匹配。
    uint64_t condition_cache_digest = 0;
};

struct CompactionSampleInfo {
    int64_t bytes = 0;
    int64_t rows = 0;
    int64_t group_data_size = 0;
    int64_t null_count = 0; // Number of NULL cells in this column group
};

struct BlockWithSameBit {
    Block* block;
    std::vector<bool>& same_bit;

    bool empty() const { return block->rows() == 0; }
};

class RowwiseIterator;
using RowwiseIteratorUPtr = std::unique_ptr<RowwiseIterator>;
class RowwiseIterator {
public:
    RowwiseIterator() = default;
    virtual ~RowwiseIterator() = default;

    // Initialize this iterator and make it ready to read with
    // input options.
    // Input options may contain scan range in which this scan.
    // Return Status::OK() if init successfully,
    // Return other error otherwise
    virtual Status init(const StorageReadOptions& opts) {
        return Status::InternalError("to be implemented, current class: " +
                                     demangle(typeid(*this).name()));
    }

    virtual Status init(const StorageReadOptions& opts, CompactionSampleInfo* sample_info) {
        return Status::InternalError("should not reach here, current class: " +
                                     demangle(typeid(*this).name()));
    }

    // If there is any valid data, this function will load data
    // into input batch with Status::OK() returned
    // If there is no data to read, will return Status::EndOfFile.
    // If other error happens, other error code will be returned.
    virtual Status next_batch(Block* block) {
        return Status::InternalError("should not reach here, current class: " +
                                     demangle(typeid(*this).name()));
    }

    virtual Status next_batch(BlockWithSameBit* block_with_same_bit) {
        return Status::InternalError("should not reach here, current class: " +
                                     demangle(typeid(*this).name()));
    }

    virtual Status next_batch(BlockView* block_view) {
        return Status::InternalError("should not reach here, current class: " +
                                     demangle(typeid(*this).name()));
    }

    virtual Status next_row(IteratorRowRef* ref) {
        return Status::InternalError("should not reach here, current class: " +
                                     demangle(typeid(*this).name()));
    }
    virtual Status unique_key_next_row(IteratorRowRef* ref) {
        return Status::InternalError("should not reach here, current class: " +
                                     demangle(typeid(*this).name()));
    }

    virtual bool is_merge_iterator() const { return false; }

    virtual Status current_block_row_locations(std::vector<RowLocation>* block_row_locations) {
        return Status::InternalError("should not reach here, current class: " +
                                     demangle(typeid(*this).name()));
    }

    // return schema for this Iterator
    virtual const Schema& schema() const = 0;

    // Return the data id such as segment id, used for keep the insert order when do
    // merge sort in priority queue
    virtual uint64_t data_id() const { return 0; }

    virtual void update_profile(RuntimeProfile* profile) {}
    // return rows merged count by iterator
    virtual uint64_t merged_rows() const { return 0; }

    // return if it's an empty iterator
    virtual bool empty() const { return false; }
};

} // namespace doris
