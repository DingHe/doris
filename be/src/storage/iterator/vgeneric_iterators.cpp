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

#include "storage/iterator/vgeneric_iterators.h"

#include <algorithm>
#include <memory>
#include <utility>

#include "common/status.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/data_type/data_type.h"
#include "storage/iterators.h"
#include "storage/olap_common.h"
#include "storage/schema.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/segment.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {
class RuntimeProfile;

using namespace ErrorCode;

Status VStatisticsIterator::init(const StorageReadOptions& opts) {
    if (!_init) {
        _push_down_agg_type_opt = opts.push_down_agg_type_opt;

        for (size_t i = 0; i < _schema.num_column_ids(); i++) {
            auto cid = _schema.column_id(i);
            auto unique_id = _schema.column(cid)->unique_id();
            if (_column_iterators_map.count(unique_id) < 1) {
                RETURN_IF_ERROR(_segment->new_column_iterator(
                        opts.tablet_schema->column(cid), &_column_iterators_map[unique_id], &opts));
            }
            _column_iterators.push_back(_column_iterators_map[unique_id].get());
        }

        _target_rows = _push_down_agg_type_opt == TPushAggOp::MINMAX ? 2 : _segment->num_rows();
        _init = true;
    }

    return Status::OK();
}

Status VStatisticsIterator::next_batch(Block* block) {
    DCHECK(block->columns() == _column_iterators.size());
    if (_output_rows < _target_rows) {
        block->clear_column_data();
        auto columns_guard = block->mutate_columns_scoped();
        auto& columns = columns_guard.mutable_columns();

        size_t size = _push_down_agg_type_opt == TPushAggOp::MINMAX
                              ? 2
                              : std::min(_target_rows - _output_rows, MAX_ROW_SIZE_IN_COUNT);
        if (_push_down_agg_type_opt == TPushAggOp::COUNT) {
            for (auto& column : columns) {
                column->insert_many_defaults(size);
            }
        } else {
            for (int i = 0; i < columns.size(); ++i) {
                RETURN_IF_ERROR(_column_iterators[i]->next_batch_of_zone_map(&size, columns[i]));
                if (auto cid = _schema.column_id(i);
                    _schema.column(cid)->type() == FieldType::OLAP_FIELD_TYPE_CHAR) {
                    auto col = columns[i]->clone_empty();
                    for (size_t j = 0; j < columns[i]->size(); ++j) {
                        const auto ref = columns[i]->get_data_at(j).trim_tail_padding_zero();
                        col->insert_data(ref.data, ref.size);
                    }
                    columns[i].swap(col);
                }
            }
        }
        _output_rows += size;
        return Status::OK();
    }
    return Status::EndOfFile("End of VStatisticsIterator");
}

// Build the block using the output schema, which contains only the columns
// the caller requested (return_columns). Delete predicate columns are excluded
// because SegmentIterator handles them independently:
//   - _init_current_block() skips predicate columns (including delete predicates)
//     via the _is_pred_column[cid] check, so it never accesses the block by those positions.
//   - _output_non_pred_columns() checks loc < block->columns() before filling any column,
//     so delete predicate columns (whose loc exceeds block->columns()) are simply skipped.
//   - Delete predicate evaluation happens entirely through _current_return_columns and
//     _evaluate_short_circuit_predicate(), which are independent of the block structure.
// 主要职责是：根据上层查询请求的 _output_schema（输出 Schema），为 block 分配并初始化各个列（Column），或者在跨 Block 预读时复用现有内存并清空数据。
// 传入一个指向 Block 对象的智能指针引用。这个 block 就是当前 Context 用来存放从物理迭代器（如 SegmentIterator）读出数据的内存缓冲区。
Status VMergeIteratorContext::block_reset(const std::shared_ptr<Block>& block) {
    // 当 block 刚被创建、内部没有任何列结构（列数为 0）时，进入此分支进行列结构的首次构建：
    if (!block->columns()) {
        // 获取 _output_schema 中定义的列 ID 列表。这里关键的设计在于只遍历 _output_schema 中需要的列（即上层查询要求的 return_columns），而自动忽略了存储层内部用于过滤的 Delete Predicate（删除谓词）列
        const auto& column_ids = _output_schema->column_ids();
        for (size_t i = 0; i < _output_schema->num_column_ids(); ++i) {
            // 类型推导与校验：根据列描述符获取该列的数据类型指针（DataTypePtr），若类型为空则返回错误状态。
            auto column_desc = _output_schema->column(column_ids[i]);
            auto data_type = Schema::get_data_type_ptr(*column_desc);
            if (data_type == nullptr) {
                return Status::RuntimeError("invalid data type");
            }
            // 调用 create_column() 创建对应类型的空数据列（如 ColumnInt32、ColumnString 等）。
            auto column = data_type->create_column();
            // 预留内存空间（reserve(_block_row_max)）：按 _block_row_max（通常为 4064 行）提前预分配底层内存容量，大幅减少后续插入数据时的动态扩容（realloc）开销。
            column->reserve(_block_row_max);
            // 将组合好的列（包含列数据、类型、列名）插入到 block 容器中。
            block->insert(ColumnWithTypeAndName(std::move(column), data_type, column_desc->name()));
        }
    } else {
    // 分支二：复用 Block 结构并清空数据 (else)
        block->clear_column_data();
    }
    return Status::OK();
}

// 最核心的多维排序与去重裁决函数
// 它在 VMergeIterator 的优先队列（最小/最大堆）比较器中被调用。
// 其核心作用是：比较当前 Context 与另一个 Context (rhs) 各自指针指向的数据行，确定哪一行具有更高的优先级（先输出），并在 Unique Key 模型下进行版本覆盖标记（设置 _skip 和 _same）。
// 返回 true 表示 this（当前 Context）的优先级低于 rhs（应该后弹出/后输出）；返回 false 表示 this 的优先级高于或等于 rhs（先弹出/先输出）。注意：在 C++ std::priority_queue（大顶堆）中，比较仿函数返回 true 代表排在后面。
bool VMergeIteratorContext::compare(const VMergeIteratorContext& rhs) const {
    // 自定义列比较 (_compare_columns)：如果指定了 _compare_columns（通过 UNLIKELY 优化），按数组指定的列索引顺序比较 _block 中 _index_in_block 行与 rhs._block 中 rhs._index_in_block 行。
    // 默认比较 Schema 中的前 _num_key_columns 列。
    int cmp_res = UNLIKELY(_compare_columns)
                          ? _block->compare_at(_index_in_block, rhs._index_in_block,
                                               _compare_columns, *rhs._block, -1)
                          : _block->compare_at(_index_in_block, rhs._index_in_block,
                                               _num_key_columns, *rhs._block, -1);

    if (cmp_res != 0) {
        return UNLIKELY(_is_reverse) ? cmp_res < 0 : cmp_res > 0;
    }
    // 阶段二：Sequence 列比较与 Tie-breaker 打平决策
    // 当 Key 完全相同（cmp_res == 0）时，进入此阶段：
    // 比较 Sequence 列：如果指定了 Sequence 列（如 updated_time），比较两行在该列的值。col_cmp_res > 0 表示 this 的 Sequence 值更大。
    auto col_cmp_res = 0;
    if (_sequence_id_idx != -1) {
        col_cmp_res = _block->compare_column_at(_index_in_block, rhs._index_in_block,
                                                _sequence_id_idx, *rhs._block, -1);
    }
    // When the sequence column is equal too, fall back to data_id ordering.
    // Otherwise pick the sort direction by `_small_seq_first`:
    //   false => larger value sorts first; true => smaller value sorts first.
    // 平局打平逻辑（三元表达式解析）：
    // 当 Sequence 值也相同（col_cmp_res == 0，或者没有 Sequence 列）：
    // 依赖 Data ID 打平：比较 data_id()（代表 Segment / Rowset 的物理写入/导入先后顺序）。
    auto result = col_cmp_res == 0 ? (_use_insert_order_when_same ? (data_id() > rhs.data_id())
                                                                  : (data_id() < rhs.data_id()))
    // 当 Sequence 值不相同（col_cmp_res != 0）：
    // _small_seq_first == false（默认模式，用于 Unique Key 表）：Sequence 值大的代表最新版本，优先级更高。如果 col_cmp_res < 0（即 this 的 Sequence 小于 rhs），result 为 true（this 优先级低）。
                                   : (_small_seq_first ? (col_cmp_res > 0) : (col_cmp_res < 0));
    // 阶段三：Unique 去重标记与状态设置（副作用机制）
    if (_is_unique) {
        result ? set_skip(true) : rhs.set_skip(true);
    }
    result ? set_same(true) : rhs.set_same(true);
    return result;
}

// Copy rows from the internal _block to the destination block.
// Both blocks are built with the output schema (return_columns only), so they
// have the same number of columns. We iterate over _output_schema->num_column_ids()
// columns to copy from src to dst.
Status VMergeIteratorContext::copy_rows(Block* block, bool advanced) {
    Block& src = *_block;
    Block& dst = *block;
    DCHECK_EQ(src.columns(), _output_schema->num_column_ids());
    DCHECK_EQ(dst.columns(), _output_schema->num_column_ids());
    if (_cur_batch_num == 0) {
        return Status::OK();
    }

    // copy a row to dst block column by column
    size_t start = _index_in_block - _cur_batch_num + 1 - advanced;

    RETURN_IF_CATCH_EXCEPTION({
        for (size_t i = 0; i < _output_schema->num_column_ids(); ++i) {
            auto& s_col = src.get_by_position(i);
            auto& d_col = dst.get_by_position(i);

            ColumnPtr& s_cp = s_col.column;
            ColumnPtr& d_cp = d_col.column;

            d_cp->assert_mutable()->insert_range_from(*s_cp, start, _cur_batch_num);
        }
    });
    _cur_batch_num = 0;
    return Status::OK();
}

// `advanced = false` when current block finished
Status VMergeIteratorContext::copy_rows(BlockWithSameBit* block_with_same_bit, bool advanced) {
    const auto& tmp_pre_ctx_same_bit = get_pre_ctx_same();
    block_with_same_bit->same_bit.insert(block_with_same_bit->same_bit.end(),
                                         tmp_pre_ctx_same_bit.begin(),
                                         tmp_pre_ctx_same_bit.begin() + _cur_batch_num);
    return copy_rows(block_with_same_bit->block, advanced);
}

Status VMergeIteratorContext::copy_rows(BlockView* view, bool advanced) {
    if (_cur_batch_num == 0) {
        return Status::OK();
    }
    size_t start = _index_in_block - _cur_batch_num + 1 - advanced;

    const auto& tmp_pre_ctx_same_bit = get_pre_ctx_same();
    RETURN_IF_CATCH_EXCEPTION({
        for (size_t i = 0; i < _cur_batch_num; ++i) {
            view->push_back({_block, static_cast<int>(start + i), tmp_pre_ctx_same_bit[i]});
        }
    });

    _cur_batch_num = 0;
    return Status::OK();
}

// This iterator will generate ordered data. For example for schema
// (int, int) this iterator will generator data like
// (0, 1), (1, 2), (2, 3), (3, 4)...
//
// Usage:
//      Schema schema;
//      VAutoIncrementIterator iter(schema, 1000);
//      StorageReadOptions opts;
//      RETURN_IF_ERROR(iter.init(opts));
//      Block block;
//      do {
//          st = iter.next_batch(&block);
//      } while (st.ok());
class VAutoIncrementIterator : public RowwiseIterator {
public:
    // Will generate num_rows rows in total
    VAutoIncrementIterator(const Schema& schema, size_t num_rows)
            : _schema(schema), _num_rows(num_rows), _rows_returned() {}
    ~VAutoIncrementIterator() override = default;

    // NOTE: Currently, this function will ignore StorageReadOptions
    Status init(const StorageReadOptions& opts) override;

    Status next_batch(Block* block) override {
        int row_idx = 0;
        while (_rows_returned < _num_rows) {
            for (int j = 0; j < _schema.num_columns(); ++j) {
                ColumnWithTypeAndName& vc = block->get_by_position(j);
                IColumn& vi = (IColumn&)(*vc.column);

                char data[16] = {};
                size_t data_len = 0;
                const auto* col_schema = _schema.column(j);
                switch (col_schema->type()) {
                case FieldType::OLAP_FIELD_TYPE_SMALLINT:
                    *(int16_t*)data = cast_set<int16_t>(_rows_returned + j);
                    data_len = sizeof(int16_t);
                    break;
                case FieldType::OLAP_FIELD_TYPE_INT:
                    *(int32_t*)data = cast_set<int32_t>(_rows_returned + j);
                    data_len = sizeof(int32_t);
                    break;
                case FieldType::OLAP_FIELD_TYPE_BIGINT:
                    *(int64_t*)data = cast_set<int64_t>(_rows_returned + j);
                    data_len = sizeof(int64_t);
                    break;
                case FieldType::OLAP_FIELD_TYPE_FLOAT:
                    *(float*)data = cast_set<float>(_rows_returned + j);
                    data_len = sizeof(float);
                    break;
                case FieldType::OLAP_FIELD_TYPE_DOUBLE:
                    *(double*)data = cast_set<double>(_rows_returned + j);
                    data_len = sizeof(double);
                    break;
                default:
                    break;
                }

                vi.insert_data(data, data_len);
            }

            ++row_idx;
            ++_rows_returned;
        }

        if (row_idx > 0) {
            return Status::OK();
        }
        return Status::EndOfFile("End of VAutoIncrementIterator");
    }

    const Schema& schema() const override { return _schema; }

private:
    const Schema& _schema;
    size_t _num_rows;
    size_t _rows_returned;
};

Status VAutoIncrementIterator::init(const StorageReadOptions& opts) {
    return Status::OK();
}

Status VMergeIteratorContext::init(const StorageReadOptions& opts) {
    _block_row_max = opts.block_row_max;
    _record_rowids = opts.record_rowids;
    RETURN_IF_ERROR(_load_next_block());
    if (valid()) {
        RETURN_IF_ERROR(_validate_compare_contract(opts));
        RETURN_IF_ERROR(advance());
    }
    _pre_ctx_same_bit.reserve(_block_row_max);
    _pre_ctx_same_bit.assign(_block_row_max, false);
    return Status::OK();
}

// compare() reads block positions that are only DCHECK-bounds-checked in Block::compare_at(),
// so in a release build a projection violating the merge contract turns into an out-of-bounds
// read inside std::push_heap and kills the BE (issue #66390). Verify the contract once the
// first block is loaded and surface a diagnosable error instead:
//   - explicit compare columns (_compare_columns) must all point inside the block;
//   - otherwise the default comparison touches positions [0, _num_key_columns), where
//     _num_key_columns counts the key columns of the WHOLE tablet schema. Key columns always
//     occupy column ids [0, num_key_columns) of the tablet schema, so the projection must
//     start with exactly those ids, in order, for the positional comparison to be key order;
//   - the sequence tie-break column, when present, must point inside the block as well.
Status VMergeIteratorContext::_validate_compare_contract(const StorageReadOptions& opts) const {
    const size_t block_columns = _block->columns();
    auto contract_error = [&](const std::string& detail) {
        std::string projected_ids;
        for (auto cid : _output_schema->column_ids()) {
            if (!projected_ids.empty()) {
                projected_ids += ',';
            }
            projected_ids += std::to_string(cid);
        }
        return Status::InternalError(
                "merge iterator compare contract violated: {}, tablet_id={}, rowset_id={}, "
                "version={}, block_columns={}, num_key_columns={}, sequence_id_idx={}, "
                "projected_column_ids=[{}]",
                detail, opts.tablet_id, opts.rowset_id.to_string(), opts.version.to_string(),
                block_columns, _num_key_columns, _sequence_id_idx, projected_ids);
    };
    if (_compare_columns != nullptr) {
        for (uint32_t pos : *_compare_columns) {
            if (pos >= block_columns) {
                return contract_error(fmt::format("compare column position {} out of range", pos));
            }
        }
    } else {
        const auto num_key_columns = static_cast<size_t>(_num_key_columns);
        if (num_key_columns > _output_schema->num_column_ids() || num_key_columns > block_columns) {
            return contract_error("projection has fewer columns than the key prefix");
        }
        for (size_t i = 0; i < num_key_columns; ++i) {
            if (_output_schema->column_ids()[i] != static_cast<ColumnId>(i)) {
                return contract_error(
                        fmt::format("position {} holds column id {} instead of key column {}", i,
                                    _output_schema->column_ids()[i], i));
            }
        }
    }
    if (_sequence_id_idx != -1 && static_cast<size_t>(_sequence_id_idx) >= block_columns) {
        return contract_error("sequence column position out of range");
    }
    return Status::OK();
}

Status VMergeIteratorContext::advance() {
    _skip = false;
    _same = false;
    // NOTE: we increase _index_in_block directly to valid one check
    do {
        _index_in_block++;
        if (LIKELY(_index_in_block < _block->rows())) {
            return Status::OK();
        }
        // current batch has no data, load next batch
        RETURN_IF_ERROR(_load_next_block());
    } while (_valid);
    return Status::OK();
}
// 当当前 Block 的数据被消费完毕后，该方法负责从底层的物理迭代器 _iter 读取下一个非空 Block。它的设计精髓在于实现了 Block 内存对象的池化复用（Memory Reuse）与生命周期安全管理（Block View 兼容）。
Status VMergeIteratorContext::_load_next_block() {
    do {
        // 废弃旧 Block：将当前刚消费完数据的 _block 转移并推入 _block_list 链表中保存。
        if (_block != nullptr) {
            _block_list.push_back(_block);
            _block = nullptr;
        }
        // 寻找可安全的复用 Block（引用计数检查）：
        for (auto it = _block_list.begin(); it != _block_list.end(); it++) {
            // 如果 it->use_count() == 1，说明上层（如 BlockView）已经不再持有这个 Block 的任何数据指针（只剩下 _block_list 自身这一个引用）。
            if (it->use_count() == 1) {
                RETURN_IF_ERROR(block_reset(*it));
                _block = *it;
                _block_list.erase(it);
                break;
            }
        }
        // 如果 _block_list 中的 Block 都在被上层引用（use_count > 1），或者链表为空，则创建新的 Block 结构并调用 block_reset(_block) 分配列结构。
        if (_block == nullptr) {
            _block = std::make_shared<Block>();
            RETURN_IF_ERROR(block_reset(_block));
        }
        // 调用物理迭代器：驱动底层的 _iter（如 SegmentIterator）向 _block 中填充数据 Batch。
        Status st = _iter->next_batch(_block.get());
        if (!st.ok()) {
            _valid = false;
            if (st.is<END_OF_FILE>()) {
                return Status::OK();
            } else {
                return st;
            }
        }
        if (UNLIKELY(_record_rowids)) {
            RETURN_IF_ERROR(_iter->current_block_row_locations(&_block_row_locations));
        }
    } while (_block->rows() == 0);
    _index_in_block = -1;
    _valid = true;
    return Status::OK();
}

Status VMergeIterator::init(const StorageReadOptions& opts) {
    if (_origin_iters.empty()) {
        return Status::OK();
    }
    _record_rowids = opts.record_rowids;

    for (auto& iter : _origin_iters) {
        auto ctx = std::make_shared<VMergeIteratorContext>(
                std::move(iter), _sequence_id_idx, _is_unique, _is_reverse,
                opts.use_insert_order_when_same, opts.read_orderby_key_columns, _output_schema,
                _small_seq_first);
        RETURN_IF_ERROR(ctx->init(opts));
        if (!ctx->valid()) {
            continue;
        }
        _merge_heap.push(ctx);
    }

    _origin_iters.clear();

    _block_row_max = opts.block_row_max;

    return Status::OK();
}

// VUnionIterator will read data from input iterator one by one.
// Unlike VMergeIterator, VUnionIterator does NOT have its own internal block or copy_rows().
// It passes the caller's block directly to the underlying SegmentIterator via next_batch(),
// so there is no input-schema vs output-schema mismatch issue here.
// The output_schema parameter is accepted only so that schema() can return the output schema
// consistently with VMergeIterator.
class VUnionIterator : public RowwiseIterator {
public:
    // Iterators' ownership it transferred to this class.
    // This class will delete all iterators when destructs
    // Client should not use iterators anymore.
    VUnionIterator(std::vector<RowwiseIteratorUPtr>&& v, SchemaSPtr output_schema)
            : _output_schema(std::move(output_schema)), _origin_iters(std::move(v)) {}

    ~VUnionIterator() override = default;

    Status init(const StorageReadOptions& opts) override;

    Status next_batch(Block* block) override;

    const Schema& schema() const override { return *_output_schema; }

    Status current_block_row_locations(std::vector<RowLocation>* locations) override;

    void update_profile(RuntimeProfile* profile) override {
        if (_cur_iter != nullptr) {
            _cur_iter->update_profile(profile);
        }
    }

private:
    const SchemaSPtr _output_schema;
    RowwiseIteratorUPtr _cur_iter = nullptr;
    StorageReadOptions _read_options;
    std::vector<RowwiseIteratorUPtr> _origin_iters;
};

Status VUnionIterator::init(const StorageReadOptions& opts) {
    if (_origin_iters.empty()) {
        return Status::OK();
    }
    // we use back() and pop_back() of std::vector to handle each iterator,
    // so reverse the vector here to keep result block of next_batch to be
    // in the same order as the original segments.
    std::reverse(_origin_iters.begin(), _origin_iters.end());

    _read_options = opts;
    _cur_iter = std::move(_origin_iters.back());
    RETURN_IF_ERROR(_cur_iter->init(_read_options));
    return Status::OK();
}

Status VUnionIterator::next_batch(Block* block) {
    while (_cur_iter != nullptr) {
        auto st = _cur_iter->next_batch(block);
        if (st.is<END_OF_FILE>()) {
            _origin_iters.pop_back();
            if (!_origin_iters.empty()) {
                _cur_iter = std::move(_origin_iters.back());
                RETURN_IF_ERROR(_cur_iter->init(_read_options));
            } else {
                _cur_iter = nullptr;
            }
        } else {
            return st;
        }
    }
    return Status::EndOfFile("End of VUnionIterator");
}

Status VUnionIterator::current_block_row_locations(std::vector<RowLocation>* locations) {
    if (!_cur_iter) {
        locations->clear();
        return Status::EndOfFile("End of VUnionIterator");
    }
    return _cur_iter->current_block_row_locations(locations);
}

// 工厂函数（Factory Function），用于创建并返回一个包装在 std::unique_ptr 中的向量化归并迭代器 VMergeIterator
// 核心作用是将传入的多个底层 Segment 迭代器（inputs）组合成一个统一的迭代器，在读取数据时按 Key 序进行多路归并（Multi-way Merge Sort），同时处理版本覆盖、Sequence 列对比和去重逻辑。
// inputs 底层待归并的所有 Segment 迭代器右值引用（所有权转移）。
// sequence_id_idx  Sequence 列在 Schema 中的列索引。在 Unique Key 模型中用于按序列号（如 updated_time）判定同一 Key 的最新记录。
// is_unique 是否为 Unique Key 数据模型。如果是 true，归并过程中相同的 Key 只会保留最新版本/最大 Sequence 的一行（即 Merge-on-Read 模式下的去重）。
// is_reverse 是否进行逆序（降序）归并（例如为了支持倒序 TopN 或特定方向的索引扫描）。
// merged_rows 输出统计指针，记录归并过程中因为版本覆盖/去重而被合并/丢弃的数据行数（用于 Profile 统计）。
// output_schema 归并后输出 Block 的列结构（Schema）。
// small_seq_first 当相同 Key 出现相同的 Sequence ID 时，控制是否“小序列优先”（通常用于特定场景的数据替换规则）。
RowwiseIteratorUPtr new_merge_iterator(std::vector<RowwiseIteratorUPtr>&& inputs,
                                       int sequence_id_idx, bool is_unique, bool is_reverse,
                                       uint64_t* merged_rows, SchemaSPtr output_schema,
                                       bool small_seq_first) {
    // when the size of inputs is 1, we also need to use VMergeIterator, because the
    // next_block_view function only be implemented in VMergeIterator. The reason why
    // the size of inputs is 1 is that the segment was filtered out by zone map or others.
    // 现象：按常理，如果 inputs.size() == 1（只有一个 Segment），单路数据天然有序，直接返回原 Iterator（或走 UnionIterator）性能最好。但这里即使只有 1 个 input，依然强制创建了 VMergeIterator。
    // 接口能力差异（next_block_view）：上层向量化读取逻辑（如垂直合并 Vertical Merge 或特定的 Batch 组装算子）依赖 VMergeIterator 独有的 next_block_view(...) 接口，而普通的 SegmentIterator 没有实现该接口。
    // 索引过滤导致的边界情况：一个 Rowset 原本可能包含多个 Segment，但由于 ZoneMap、BloomFilter 或 Bitmap 索引过滤，其他 Segment 被剪枝（裁剪）掉，最终只剩下 1 个 Segment（甚至经过过滤后为空）。为了向上层对外暴露统一且一致的迭代器行为与接口，引擎选择统一封装为 VMergeIterator。

    return std::make_unique<VMergeIterator>(std::move(inputs), sequence_id_idx, is_unique,
                                            is_reverse, merged_rows, std::move(output_schema),
                                            small_seq_first);
}

RowwiseIteratorUPtr new_union_iterator(std::vector<RowwiseIteratorUPtr>&& inputs,
                                       SchemaSPtr output_schema) {
    if (inputs.size() == 1) {
        return std::move(inputs[0]);
    }
    return std::make_unique<VUnionIterator>(std::move(inputs), std::move(output_schema));
}

RowwiseIterator* new_vstatistics_iterator(std::shared_ptr<Segment> segment, const Schema& schema) {
    return new VStatisticsIterator(segment, schema);
}

RowwiseIteratorUPtr new_auto_increment_iterator(const Schema& schema, size_t num_rows) {
    return std::make_unique<VAutoIncrementIterator>(schema, num_rows);
}

} // namespace doris
