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

#include <gen_cpp/PlanNodes_types.h>
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <list>
#include <map>
#include <memory>
#include <queue>
#include <utility>
#include <vector>

#include "common/cast_set.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "core/block/block.h"
#include "storage/iterators.h"
#include "storage/schema.h"
#include "storage/segment/column_reader.h"
#include "storage/utils.h"

namespace doris {
class RuntimeProfile;

namespace segment_v2 {
class Segment;
class ColumnIterator;
} // namespace segment_v2

class VStatisticsIterator : public RowwiseIterator {
public:
    // Will generate num_rows rows in total
    VStatisticsIterator(std::shared_ptr<Segment> segment, const Schema& schema)
            : _segment(std::move(segment)), _schema(schema) {}

    ~VStatisticsIterator() override = default;

    Status init(const StorageReadOptions& opts) override;

    Status next_batch(Block* block) override;

    const Schema& schema() const override { return _schema; }

private:
    std::shared_ptr<Segment> _segment;
    const Schema& _schema;
    size_t _target_rows = 0;
    size_t _output_rows = 0;
    bool _init = false;
    TPushAggOp::type _push_down_agg_type_opt;
    std::map<int32_t, std::unique_ptr<ColumnIterator>> _column_iterators_map;
    std::vector<ColumnIterator*> _column_iterators;

    static constexpr size_t MAX_ROW_SIZE_IN_COUNT = 65535;
};

// Used to store merge state for a VMergeIterator input.
// This class will iterate all data from internal iterator
// through client call advance().
// Usage:
//      VMergeIteratorContext ctx(iter);
//      RETURN_IF_ERROR(ctx.init());
//      while (ctx.valid()) {
//          visit(ctx.current_row());
//          RETURN_IF_ERROR(ctx.advance());
//      }
// 在多路归并排序（Multi-way Merge Sort）中，VMergeIterator 维护了一个优先队列（最小堆/最大堆），而堆中的元素就是 VMergeIteratorContext。
// 数据流封装与缓存：封装单个底层的物理读取迭代器 RowwiseIterator（如 SegmentIterator），按 Batch（Block）预读并缓存数据到内存中。
// 游标管理（Cursor Tracking）：记录并管理当前迭代到的 Block 内部的行索引 _index_in_block。
// 行级比较与排序逻辑（Comparison Contract）：实现 compare() 方法，定义不同数据流（Context）当前行之间的比较规则（按 Key 列、Sequence 列、Version 版本以及插入顺序等），为堆排序提供比较接口。
// 延迟批量拷贝（Batch Copy Optimization）：配合 VMergeIterator 记录连续命中的行数（_cur_batch_num），在需要时一次性将连续行批量拷贝至目标 Block，避免逐行拷贝带来的巨大开销。
// 去重与 Same Bit 状态标记：在 Unique 模型或 Aggregate 模型下，标记当前行是否需要跳过（_skip）、是否与上一行 Key 相同（_same），并维护 _pre_ctx_same_bit。
class VMergeIteratorContext {
public:
    VMergeIteratorContext(RowwiseIteratorUPtr&& iter, int sequence_id_idx, bool is_unique,
                          bool is_reverse, bool use_insert_order_when_same,
                          std::vector<uint32_t>* read_orderby_key_columns, SchemaSPtr output_schema,
                          bool small_seq_first = false)
            : _iter(std::move(iter)),
              _sequence_id_idx(sequence_id_idx),
              _is_unique(is_unique),
              _is_reverse(is_reverse),
              _use_insert_order_when_same(use_insert_order_when_same),
              _small_seq_first(small_seq_first),
              _output_schema(std::move(output_schema)),
              _num_key_columns(cast_set<int>(_output_schema->num_key_columns())),
              _compare_columns(read_orderby_key_columns) {}

    VMergeIteratorContext(const VMergeIteratorContext&) = delete;
    VMergeIteratorContext(VMergeIteratorContext&&) = delete;
    VMergeIteratorContext& operator=(const VMergeIteratorContext&) = delete;
    VMergeIteratorContext& operator=(VMergeIteratorContext&&) = delete;

    ~VMergeIteratorContext() = default;

    // Reset (or initialize) the internal _block using the output schema.
    //
    // The output schema contains only the columns the caller requested (return_columns),
    // excluding delete predicate columns. For example, if the query reads columns {c1, c2}
    // but there is a delete predicate on column c3 (e.g., "DELETE FROM t WHERE c3 = 'foo'"):
    //   - input schema  (iter->schema) = {c1, c2, c3}   (3 columns)
    //   - output schema                = {c1, c2}       (2 columns)
    //
    // It is safe to build the block with only the output schema because SegmentIterator
    // handles delete predicate columns independently of the block structure:
    //   - _init_current_block() skips predicate columns (including delete predicates)
    //     via the _is_pred_column[cid] check, never accessing the block for them.
    //   - _output_non_pred_columns() checks loc < block->columns() before filling any
    //     column, so delete predicate columns are simply skipped when the block is smaller.
    //   - Delete predicate evaluation uses _current_return_columns and
    //     _evaluate_short_circuit_predicate(), independent of the block.
    // 根据 _output_schema 重置或初始化传入的 _block
    // 按输出 Schema 为 block 分配列结构（只包含需要返回的列）。即使底层读取包含了用于 Delete 谓词过滤的辅助列，输出 Block 也仅按照 _output_schema 格式构造，保证内存高效利用。
    Status block_reset(const std::shared_ptr<Block>& block);

    // Initialize this context and will prepare data for current_row()
    // 初始化 Context 状态并加载第一批数据。
    Status init(const StorageReadOptions& opts);
    // 比较当前 Context 的当前行（_index_in_block）与 rhs Context 的当前行的大小。
    bool compare(const VMergeIteratorContext& rhs) const;

    // Copy rows from internal _block to the destination block.
    // Both blocks have _output_schema columns (return_columns only).
    // Only _output_schema->num_column_ids() columns are copied.
    //
    // `advanced = false` when current block finished
    // when input argument type is block, we do not process same_bit,
    // this case we only need merge and return ordered data (VCollectIterator::_topn_next), data mode is dup/mow can guarantee all rows are different
    // todo: we can reduce same_bit processing in this case to improve performance
    // 批量拷贝数据的三个重载接口。将内部 _block 中积攒的连续 _cur_batch_num 行数据批量追加写入到目标 block / block_with_same_bit / view 中。
    Status copy_rows(Block* block, bool advanced = true);
    Status copy_rows(BlockWithSameBit* block, bool advanced = true);
    Status copy_rows(BlockView* view, bool advanced = true);
    // 获取当前行（_index_in_block）对应的底层磁盘物理位置 RowLocation（含有 DCHECK(_record_rowids) 检查）。
    RowLocation current_row_location() {
        DCHECK(_record_rowids);
        return _block_row_locations[_index_in_block];
    }

    // Advance internal row index to next valid row
    // Return error if error happens
    // Don't call this when valid() is false, action is undefined
    // 推进当前 Context 的行游标到下一行。
    Status advance();

    // Return if it has remaining data in this context.
    // Only when this function return true, current_row()
    // will return a valid row
    // 查询 Context 当前是否有效（即是否还有可读的行）。返回 _valid。
    bool valid() const { return _valid; }
    // 返回底层 _iter 的 data_id()（代表 Segment / Rowset 的逻辑创建顺序，用于平局决胜）。
    uint64_t data_id() const { return _iter->data_id(); }
    // 查询当前行是否被标记为跳过（被高版本覆盖）。返回 _skip。
    bool need_skip() const { return _skip; }

    void set_skip(bool skip) const { _skip = skip; }
    // 查询当前行与上一行 Key 是否相同。返回 _same。
    bool is_same() const { return _same; }

    void set_same(bool same) const { _same = same; }
    // 获取记录当前 Batch 连续行 Same Bit 状态的布尔向量 _pre_ctx_same_bit
    const std::vector<bool>& get_pre_ctx_same() const { return _pre_ctx_same_bit; }
    // 将传入 ctx 的 is_same() 状态同步更新到当前 Context 维护的 _pre_ctx_same_bit 对应的索引位置上。
    void set_pre_ctx_same(VMergeIteratorContext* ctx) const {
        int64_t index = ctx->get_cur_batch() - 1;
        DCHECK(index >= 0);
        DCHECK_LT(index, _pre_ctx_same_bit.size());
        _pre_ctx_same_bit[index] = ctx->is_same();
    }
    // 获取当前 Context 已积累的待批量拷贝行数 _cur_batch_num。
    size_t get_cur_batch() const { return _cur_batch_num; }
    // 将待批量拷贝行数自增 1（_cur_batch_num++）。当该 Context 在堆顶连续被选中时调用。
    void add_cur_batch() { _cur_batch_num++; }
    // 重置待批量拷贝计数器 _cur_batch_num = 0。
    void reset_cur_batch() { _cur_batch_num = 0; }
    // 判断当前游标是否处于内部 _block 的最后一行（_index_in_block == _block->rows() - 1）。用于触发边界拷贝。
    bool is_cur_block_finished() { return _index_in_block == _block->rows() - 1; }

private:
    // Load next block into _block
    // 从底层 _iter 载入下一个 Block 到 _block 中。
    Status _load_next_block();

    // Validate that every block position compare() may touch actually exists in _block
    // and, for the default key-prefix comparison, that the projection really starts with
    // the full ordered key prefix. Returns an error instead of letting compare() perform
    // an out-of-bounds or semantically wrong positional access (issue #66390).
    // 校验排序列契约（防止越界和语义错误）。
    Status _validate_compare_contract(const StorageReadOptions& opts) const;
    // 底层的物理数据迭代器（例如 SegmentIterator），负责从磁盘或 Segment 读出数据块（Block）。
    RowwiseIteratorUPtr _iter;
    // Sequence 列（逻辑时间戳列/自增列）在 Schema 中的列索引（默认 -1 表示无 Sequence 列）。在 Unique 模型中用于比较相同 Key 的数据新旧。
    int _sequence_id_idx = -1;
    // 标识是否为 Unique Key 模型。为 true 时开启去重逻辑（保留最新版本，标记旧版本为 _skip = true）。
    bool _is_unique = false;
    // 标识是否逆序（降序）归并。
    bool _is_reverse = false;
    // 当相同 Key 且 Sequence/Version 均相同时，控制是否按 Segment 的插入顺序（Data ID / Segment ID）来判定先后。
    bool _use_insert_order_when_same = false;
    // Tie-break direction on the sequence column when keys are equal:
    // false = larger value sorts first (UNIQUE_KEYS sequence column);
    // true  = smaller value sorts first (row binlog TSO column).
    // equence 列的比较规则打平标记：
    // false（默认）：Sequence 值大的排在前面（Unique 模型最新的数据覆盖旧数据）。
    // true：Sequence 值小的排在前面（用于行 Binlog TSO 排序等特定场景）。
    bool _small_seq_first = false;
    // 标识当前 Context 是否还有有效数据。如果底层的 _iter 已读完且当前 _block 遍历结束，该值为 false。
    bool _valid = false;
    // 当前行是否需要被跳过（不输出）。例如在 Unique 模型中，被更高版本覆盖的旧数据行会在归并比较中被标记为 _skip = true。
    mutable bool _skip = false;
    // 标识当前 Context 指向的行是否与前一个选中的 Context 的行具有相同的 Key（用于聚合模型或 Same Bit 统计）。
    mutable bool _same = false;
    // 当前正在处理的数据行在内部缓存 _block 中的行下标索引（从 0 到 _block->rows() - 1）。初始为 -1。
    size_t _index_in_block = -1;
    // 4096 minus 16 + 16 bytes padding that in padding pod array
    // 单次从底层 _iter 预读 Block 的最大行数限制（默认 4064，减去了 POD 数组补齐空间）。
    int _block_row_max = 4064;
    // The output schema defines which columns are in _block and in the caller's dst block.
    // It contains only the requested return_columns, excluding delete predicate columns.
    // For example:
    //   - _iter->schema() (input)  = {c1, c2, c3}  — c3 for "DELETE WHERE c3='foo'"
    //   - _output_schema           = {c1, c2}      — only the requested columns
    // block_reset() uses _output_schema to build _block, and copy_rows() iterates over
    // _output_schema->num_column_ids() columns to copy from _block to the destination.
    // 最终输出和内部 _block 采用的 Schema。仅包含上层查询请求的列（return_columns），排除了存储层内部的删除谓词列（Delete Predicate Columns）。
    const SchemaSPtr _output_schema;
    // Schema 中 Key 列（排序列）的数量（从 _output_schema 提取）。
    int _num_key_columns;
    // 自定义排序列索引数组指针。如果设置了该值，compare() 会优先按照该数组指定的列下标顺序进行 Key 比较，而不是默认的前 _num_key_columns 列。
    std::vector<uint32_t>* _compare_columns;
    // 内部缓存数组，记录当前 _block 中每一行数据在磁盘 Segment 上的物理行地址 RowLocation（Segment ID + Row ID）。
    std::vector<RowLocation> _block_row_locations;
    // 标识是否需要记录和读取物理行地址（Row ID）。
    bool _record_rowids = false;
    // 记录当前 Context 连续被选中且未刷新的数据行数。用于向量化批量拷贝优化。
    size_t _cur_batch_num = 0;

    // used to store data load from iterator->next_batch(Block*)
    // 内部的数据缓冲区（按向量化列存格式组织的 Block），保存从 _iter 读取的当前 Batch 数据。
    std::shared_ptr<Block> _block;
    // used to store data still on block view
    // 当使用 BlockView 零拷贝视图时，保存依然被视图引用的 Block 对象的智能指针生命周期链表，防止内存提前释放。
    std::list<std::shared_ptr<Block>> _block_list;
    // 记录当前 Batch 连续拷贝区间中，每一行与前一行 Key 是否相同的比特标记（用于 BlockWithSameBit）。
    mutable std::vector<bool> _pre_ctx_same_bit;
};
// VMergeIterator 是 Apache Doris BE（Backend）存储引擎中核心的向量化多路归并迭代器（继承自 RowwiseIterator）。
// 核心作用是：将来自多个底层 Segment（或 Rowset）的有序数据流（Iterator）在内存中进行多路归并排序（Multi-way Merge Sort），并输出全局有序且经过版本去重的向量化数据块（Block）。
// 具体应用场景与关键职责包括：
// 多路归并排序：当查询需要有序结果，或者多个 Segment 之间存在数据重叠（Overlapping）时，利用最小堆（优先队列 _merge_heap）将多路 Iterator 按 Key 序进行 Merge。
// 读时合并与去重（Merge-on-Read）：在 Aggregate 模型或 Unique Key 模型（Merge-on-Read 模式）下，相同 Key 的数据可能分布在不同的 Segment 中。VMergeIterator 配合 VMergeIteratorContext 能够对比数据版本或 Sequence 列，跳过（Skip）被覆盖的旧版本数据，实现实时去重。
// 批量/连续拷贝优化（Batch Copying）：为了提升向量化执行效率，如果同一数据源（VMergeIteratorContext）连续多行命中排序产出，它不会逐行拷贝，而是通过 add_cur_batch() 记录连续区间，最后一次性批量 Block 拷贝，极大降低了内存复制和函数调用的开销。
// RowID 追踪与转换：在支持全局 RowID 追踪的场景下，同步记录输出 Block 中每一行对应底层的物理 RowLocation（Segment ID + Row ID）。
class VMergeIterator : public RowwiseIterator {
public:
    // VMergeIterator takes the ownership of input iterators
    VMergeIterator(std::vector<RowwiseIteratorUPtr>&& iters, int sequence_id_idx, bool is_unique,
                   bool is_reverse, uint64_t* merged_rows, SchemaSPtr output_schema,
                   bool small_seq_first = false)
            : _origin_iters(std::move(iters)),
              _output_schema(std::move(output_schema)),
              _sequence_id_idx(sequence_id_idx),
              _is_unique(is_unique),
              _is_reverse(is_reverse),
              _small_seq_first(small_seq_first),
              _merged_rows(merged_rows) {}

    ~VMergeIterator() override = default;
    // 初始化归并迭代器
    Status init(const StorageReadOptions& opts) override;
    // 直接转发调用模板函数 _next_batch(block)
    Status next_batch(Block* block) override { return _next_batch(block); }

    Status next_batch(BlockWithSameBit* block_with_same_bit) override {
        return _next_batch(block_with_same_bit);
    }
    Status next_batch(BlockView* block_view) override { return _next_batch(block_view); }

    const Schema& schema() const override { return *_output_schema; }

    // 获取当前批量 Block 中每一行的物理位置信息（RowLocation）
    Status current_block_row_locations(std::vector<RowLocation>* block_row_locations) override {
        DCHECK(_record_rowids);
        *block_row_locations = _block_row_locations;
        return Status::OK();
    }
    // 更新并同步 Query Profile 监控统计指标
    void update_profile(RuntimeProfile* profile) override {
        if (!_origin_iters.empty()) {
            _origin_iters[0]->update_profile(profile);
        }
    }

private:
    // 用于统一获取不同数据结构当前的已填入行数
    int _get_size(const BlockWithSameBit* block_with_same_bit) {
        return cast_set<int>(block_with_same_bit->block->rows());
    }
    int _get_size(BlockView* block_view) { return cast_set<int>(block_view->size()); }
    int _get_size(Block* block) { return cast_set<int>(block->rows()); }

    // 向量化归并与去重的主循环逻辑实现
    // 设计精髓在于通过“延迟拷贝（Batch Copying）”将逐行的多路归并排序转化为连续内存块的批量复制，大幅提升了向量化引擎的 CPU 缓存命中率与执行效率。
    template <typename T>
    Status _next_batch(T* block) {
        // RowID 空间预留：若开启了全局 RowID 追踪，直接按 _block_row_max 预分配 _block_row_locations 数组，避免在循环中频繁触发重新分配（realloc）。
        if (UNLIKELY(_record_rowids)) {
            _block_row_locations.resize(_block_row_max);
        }
        size_t row_idx = 0;
        std::shared_ptr<VMergeIteratorContext> pre_ctx;
        while (_get_size(block) < _block_row_max) {
            if (_merge_heap.empty()) {
                break;
            }
            // 弹出当前全局最小（或最大）行：堆顶的 ctx 包含了当前所有数据流中 Key 顺序最靠前的数据。
            auto ctx = _merge_heap.top();
            _merge_heap.pop();
            // 情况 A：当前行是有效数据（!ctx->need_skip()）
            if (!ctx->need_skip()) {
                // 连续行计数：调用 add_cur_batch()，使当前 ctx 内部的待拷贝行数自增 1。
                ctx->add_cur_batch();
                // 如果 pre_ctx != ctx，说明数据源发生了改变（例如从 Segment-1 切换到了 Segment-2）。
                if (pre_ctx != ctx) {
                    // 此时之前的 pre_ctx 收集的连续行中断，必须立即调用 pre_ctx->copy_rows(block) 将之前积累的连续行一次性批量写入输出 Block，然后再将 pre_ctx 更新为当前 ctx。
                    if (pre_ctx) {
                        RETURN_IF_ERROR(pre_ctx->copy_rows(block));
                    }
                    pre_ctx = ctx;
                }
                // 相同 Key 标记与 RowID 收集：更新相邻行 Key 的相同状态（用于聚合模型），并记录当前物理行的 RowLocation。
                pre_ctx->set_pre_ctx_same(ctx.get());
                if (UNLIKELY(_record_rowids)) {
                    _block_row_locations[row_idx] = ctx->current_row_location();
                }
                row_idx++;
                // 如果当前 ctx 内部的 Block 数据已经全部用完（is_cur_block_finished()），或者输出 block 已经填满（row_idx >= _block_row_max）。
                if (ctx->is_cur_block_finished() || row_idx >= _block_row_max) {
                    // current block finished, ctx not advance
                    // so copy start_idx = (_index_in_block - _cur_batch_num + 1)
                    // 强制将当前 ctx 积攒的数据批量写入 block，并将 pre_ctx 置空。
                    RETURN_IF_ERROR(ctx->copy_rows(block, false));
                    pre_ctx = nullptr;
                }
            // 情况 B：当前行为被覆盖/去重的旧数据（ctx->need_skip() == true）
            } else if (_merged_rows != nullptr) {
                // 在 Unique Key 模型中，发现旧版本数据被覆盖，累加 (*_merged_rows)++。
                (*_merged_rows)++;
                // need skip cur row, so flush rows in pre_ctx
                // 因为跳过了这一行，破坏了物理内存上的连续性，所以必须立即刷新 pre_ctx 中先前积累的有效行（copy_rows），并将 pre_ctx 重置。
                if (pre_ctx) {
                    RETURN_IF_ERROR(pre_ctx->copy_rows(block));
                    pre_ctx = nullptr;
                }
            }
            // 将该 Context 的游标移动到下一行。若该 Iterator 还有数据（valid() == true），重新压入堆中参与下一轮比较。
            RETURN_IF_ERROR(ctx->advance());
            if (ctx->valid()) {
                _merge_heap.push(ctx);
            }
        }
        // 4. 退出循环与 EOF 状态处理
        // 未结束返回：如果循环结束后 _merge_heap 仍有数据，说明 Block 已填满但全部数据未读完，返回 Status::OK()，等待上层下一次调用。
        if (!_merge_heap.empty()) {
            return Status::OK();
        }
        // Still last batch needs to be processed
        // 读完返回 EOF：如果 _merge_heap 为空，说明所有 Segment 的数据均已合并完毕。调整 _block_row_locations 的实际大小，并返回 Status::EndOfFile 告知调用方数据读取完毕。i
        if (UNLIKELY(_record_rowids)) {
            _block_row_locations.resize(row_idx);
        }

        return Status::EndOfFile("no more data in segment");
    }

    // It will be released after '_merge_heap' has been built.
    // 传入的原始底层物理迭代器集合（如 SegmentIterator）
    // 存储所有参与归并的 Iterator 所有权。在 init() 阶段用其构建 VMergeIteratorContext 并压入堆后，其内部指针所有权会被转移，随后清空释放。
    std::vector<RowwiseIteratorUPtr> _origin_iters;

    // The output schema (excludes delete predicate columns). Passed down to each
    // VMergeIteratorContext to control how many columns copy_rows() copies.
    // 输出数据的 Schema 结构定义指针。
    // 定义了最终返回给上层的 Block 包含哪些列（排除了存储层内部辅助列，如 Delete Predicate 列），并传递给各个 Context 用于控制拷贝哪些列。
    const SchemaSPtr _output_schema;
    // 优先队列（堆）的自定义比较器仿函数。
    // 直接调用 lhs->compare(*rhs)。根据底层 Context 当前行的 Key、Version、Sequence ID 等多维字段进行优先级判定，决定堆的排序规则。
    struct VMergeContextComparator {
        bool operator()(const std::shared_ptr<VMergeIteratorContext>& lhs,
                        const std::shared_ptr<VMergeIteratorContext>& rhs) const {
            return lhs->compare(*rhs);
        }
    };
    // 定义优先队列别名
    using VMergeHeap = std::priority_queue<std::shared_ptr<VMergeIteratorContext>,
                                           std::vector<std::shared_ptr<VMergeIteratorContext>>,
                                           VMergeContextComparator>;
    // 优先队列（堆排序结构），数据类型为 VMergeHeap
    // 存储所有当前处于有效状态（valid() == true）的 VMergeIteratorContext。利用 VMergeContextComparator 堆顶始终保持“当前 Key 最小（或最大，根据降序配置）”的 Context，实现 $O(\log N)$ 复杂度的多路归并。
    VMergeHeap _merge_heap;
    // 单次 next_batch 填充 Block 的最大行数限制（通常为 4096 或由 StorageReadOptions 传入）。
    // 控制向量化 Batch 的粒度，防止单次读取占用过多内存。
    int _block_row_max = 0;
    // Sequence 列（顺序列/逻辑时间戳列）在 Schema 中的列下标索引。
    // 在 Unique Key 模型中，若指定了 Sequence 列（如 updated_time），用于在 Key 相同且 Version 相同的情况下进一步比较 Sequence 值，决定保留哪一条数据。若未开启则为 -1。
    int _sequence_id_idx = -1;
    // 标识当前表是否为 Unique Key 数据模型
    // 为 true 时开启去重模式，遇到相同 Key 的多条数据只会保留最新一条，其余旧版本行被标记为 need_skip()。
    bool _is_unique = false;
    // 是否开启逆序/降序归并。
    // 为 true 时，优先队列按照从大到小（降序）弹出数据。
    bool _is_reverse = false;
    // See VMergeIteratorContext::_small_seq_first; forwarded to every context on init().
    // 在相同 Key 且 Sequence ID 相同时，控制是否“小 Sequence 优先”。
    // 下发给每个 Context，用于某些特殊的替换策略或特定排序规则。
    bool _small_seq_first = false;
    // 被归并/覆盖丢弃的物理行数统计计数器指针（Profile 累加器）。
    // 当某行数据因为版本覆盖或去重被跳过（need_skip() == true）时，该指针指向的值会自增 (*_merged_rows)++，用于 Query Profile 性能监控。
    uint64_t* _merged_rows = nullptr;
    // 是否需要记录输出 Block 中每一行的物理 RowID 映射。
    // 如果上层要求追踪 RowID（例如用于全局 RowID 转换或点查优化），设为 true
    bool _record_rowids = false;
    // 保存当前 Block 中每一行数据物理地址的数组。
    std::vector<RowLocation> _block_row_locations;
};

// Create a merge iterator for input iterators. Merge iterator will merge
// ordered input iterator to one ordered iterator. So client should ensure
// that every input iterator is ordered, otherwise result is undefined.
//
// Inputs iterators' ownership is taken by created merge iterator. And client
// should delete returned iterator after usage.
RowwiseIteratorUPtr new_merge_iterator(std::vector<RowwiseIteratorUPtr>&& inputs,
                                       int sequence_id_idx, bool is_unique, bool is_reverse,
                                       uint64_t* merged_rows, SchemaSPtr output_schema,
                                       bool small_seq_first = false);

// Create a union iterator for input iterators. Union iterator will read
// input iterators one by one.
//
// Inputs iterators' ownership is taken by created union iterator.
RowwiseIteratorUPtr new_union_iterator(std::vector<RowwiseIteratorUPtr>&& inputs,
                                       SchemaSPtr output_schema);

// Create an auto increment iterator which returns num_rows data in format of schema.
// This class aims to be used in unit test.
//
// Client should delete returned iterator.
RowwiseIteratorUPtr new_auto_increment_iterator(const Schema& schema, size_t num_rows);

RowwiseIterator* new_vstatistics_iterator(std::shared_ptr<Segment> segment, const Schema& schema);

} // namespace doris
