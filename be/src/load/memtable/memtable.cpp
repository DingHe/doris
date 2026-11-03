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

#include "load/memtable/memtable.h"

#include <fmt/format.h>
#include <gen_cpp/olap_file.pb.h>
#include <pdqsort.h>

#include <algorithm>
#include <limits>
#include <string>
#include <vector>

#include "bvar/bvar.h"
#include "common/config.h"
#include "core/column/column.h"
#include "exprs/aggregate/aggregate_function_reader.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "load/delta_writer/delta_writer_context.h"
#include "load/memtable/memtable_memory_limiter.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_profile.h"
#include "runtime/thread_context.h"
#include "runtime/workload_management/resource_context.h"
#include "storage/olap_define.h"
#include "storage/tablet/tablet_schema.h"
#include "util/debug_points.h"
#include "util/stopwatch.hpp"

namespace doris {

bvar::Adder<int64_t> g_memtable_cnt("memtable_cnt");
bvar::Adder<uint64_t> g_flush_cuz_memtable_full("flush_cuz_memtable_full");

using namespace ErrorCode;

MemTable::MemTable(int64_t tablet_id, std::shared_ptr<TabletSchema> tablet_schema,
                   const std::vector<SlotDescriptor*>* slot_descs, TupleDescriptor* tuple_desc,
                   bool enable_unique_key_mow, PartialUpdateInfo* partial_update_info,
                   const std::shared_ptr<ResourceContext>& resource_ctx, bool need_row_binlog_lsn)
        : _mem_type(MemType::ACTIVE),
          _tablet_id(tablet_id),
          _enable_unique_key_mow(enable_unique_key_mow),
          _keys_type(tablet_schema->keys_type()),
          _tablet_schema(tablet_schema),
          _resource_ctx(resource_ctx),
          _need_row_binlog_lsn(need_row_binlog_lsn),
          _is_first_insertion(true),
          _agg_functions(tablet_schema->num_columns()),
          _offsets_of_aggregate_states(tablet_schema->num_columns()),
          _total_size_of_aggregate_states(0) {
    g_memtable_cnt << 1;
    _mem_tracker = std::make_shared<MemTracker>();
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
            _resource_ctx->memory_context()->mem_tracker()->write_tracker());
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);
    _vec_row_comparator = std::make_shared<RowInBlockComparator>(_tablet_schema);
    if (partial_update_info != nullptr) {
        _partial_update_mode = partial_update_info->update_mode();
        if (_partial_update_mode == UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS) {
            if (partial_update_info->is_schema_contains_auto_inc_column &&
                !partial_update_info->is_input_columns_contains_auto_inc_column) {
                _is_partial_update_and_auto_inc = true;
            }
        }
    }
    _init_columns_offset_by_slot_descs(slot_descs, tuple_desc);
    // TODO: Support ZOrderComparator in the future
    _row_in_blocks = std::make_unique<DorisVector<std::shared_ptr<RowInBlock>>>();
    _load_mem_limit = MemInfo::mem_limit() * config::load_process_max_memory_limit_percent / 100;
}

void MemTable::_init_columns_offset_by_slot_descs(const std::vector<SlotDescriptor*>* slot_descs,
                                                  const TupleDescriptor* tuple_desc) {
    for (auto slot_desc : *slot_descs) {
        const auto& slots = tuple_desc->slots();
        for (int j = 0; j < slots.size(); ++j) {
            if (slot_desc->id() == slots[j]->id()) {
                _column_offset.emplace_back(j);
                break;
            }
        }
    }
    if (_is_partial_update_and_auto_inc) {
        _column_offset.emplace_back(_column_offset.size());
    }
    _num_columns = _column_offset.size();
}
// 专门用于初始化非主键列（Value Columns）的聚合函数及内存状态布局的内部私有方法。
// 当表模型包含聚合逻辑（如 Aggregate Key 模型）或特定的唯一键写时合并（Unique Key MOW）模型时，数据在内存中合并（Aggregation）时需要依赖特定的聚合函数（如 SUM、MAX、REPLACE 等）。
// 该方法的主要作用是：根据表结构和数据块的类型，为每个非主键列创建并校验对应的聚合函数实例，同时精确计算并规划这些聚合状态（Aggregate State）在内存中的对齐偏移量与总占用空间。
void MemTable::_init_agg_functions(const Block* block) {
    // 1. 结构安全检查（极小概率分支优化）
    // 如果 MemTable 记录的列总数（_num_columns）大于实际 block 中的列偏移量数组大小（_column_offset.size()），
    // 说明元数据与实际数据块不匹配，属于内部错误，直接抛出异常。
    if (_num_columns > _column_offset.size()) [[unlikely]] {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                               "num_columns {} is greater than block columns {}", _num_columns,
                               _column_offset.size());
    }
    // 2. 第一阶段循环：为所有非主键列初始化并验证聚合函数
    // 主键列（Key Columns）不需要聚合函数，因此循环从 cid = _tablet_schema->num_key_columns() 开始，一直遍历到最后一列。
    for (auto cid = _tablet_schema->num_key_columns(); cid < _num_columns; ++cid) {
        AggregateFunctionPtr function;
        // 3. 分支判断：Unique Key MOW（写时合并）模型的特殊处理
        if (_keys_type == KeysType::UNIQUE_KEYS && _enable_unique_key_mow) {
            // In such table, non-key column's aggregation type is NONE, so we need to construct
            // the aggregate function manually.
            // 在 Unique Key MOW 表中，非主键列的聚合类型本质上是 "NONE"（直接覆盖/替换），
            // 因此需要手动构造合适的聚合函数。
            if (_skip_bitmap_col_idx != cid) {
                // 如果当前列不是跳过位图列（skip bitmap），则获取 "replace_load"（导入替换）聚合函数，
                // 用于处理相同 Key 时的值覆盖逻辑。
                function = AggregateFunctionSimpleFactory::instance().get(
                        "replace_load", {block->get_data_type(cid)}, block->get_data_type(cid),
                        block->get_data_type(cid)->is_nullable(),
                        BeExecVersionManager::get_newest_version());
            } else {
                // 如果当前列是 skip bitmap 列，则为其获取 "bitmap_intersect"（位图求交）聚合函数。
                function = AggregateFunctionSimpleFactory::instance().get(
                        "bitmap_intersect", {block->get_data_type(cid)}, block->get_data_type(cid),
                        false, BeExecVersionManager::get_newest_version());
            }
        } else {
            // 4. 标准分支：Aggregate Key 或其他模型的聚合函数获取
            // 直接从 TabletSchema 中获取该列定义绑定的聚合函数（通过 AGG_LOAD_SUFFIX 后缀匹配）。
            function = _tablet_schema->column(cid).get_aggregate_function(
                    AGG_LOAD_SUFFIX, _tablet_schema->column(cid).get_be_exec_version(),
                    block->get_data_type(cid));
            if (function == nullptr) {
                LOG(WARNING) << "column get aggregate function failed, column="
                             << _tablet_schema->column(cid).name();
            }
        }

        DCHECK(function != nullptr);
        // 获取输入可变块中当前列的裸指针
        const auto* input_column = _input_mutable_block.get_column_by_position(cid).get();
        const IColumn* columns[] = {input_column};
        // 校验输入列的数据类型是否符合聚合函数的要求
        function->check_input_columns_type(columns);
        // 校验输出结果列的数据类型是否与输出可变块匹配
        function->check_result_column_type(*_output_mutable_block.get_column_by_position(cid));
        // 将初始化好的聚合函数存入成员数组中
        _agg_functions[cid] = function;
    }
    // 6. 第二阶段循环：计算聚合状态（Aggregate State）在内存中的偏移量与对齐填充
    for (auto cid = _tablet_schema->num_key_columns(); cid < _num_columns; ++cid) {
        // 记录当前列的聚合状态在连续内存块中的起始偏移量等于当前累计的总大小
        _offsets_of_aggregate_states[cid] = _total_size_of_aggregate_states;
        // 累加上当前聚合函数数据状态所需的内存大小
        _total_size_of_aggregate_states += _agg_functions[cid]->size_of_data();

        // If not the last aggregate_state, we need pad it so that next aggregate_state will be aligned.
        // 7. 内存对齐填充（Padding）
        // 如果当前列不是最后一个聚合列，为了满足 CPU 对下一个状态的内存对齐要求（alignment），需要进行对齐填充。
        if (cid + 1 < _num_columns) {
            size_t alignment_of_next_state = _agg_functions[cid + 1]->align_of_data();

            /// Extend total_size to next alignment requirement
            /// Add padding by rounding up 'total_size_of_aggregate_states' to be a multiplier of alignment_of_next_state.
            _total_size_of_aggregate_states =
                    (_total_size_of_aggregate_states + alignment_of_next_state - 1) /
                    alignment_of_next_state * alignment_of_next_state;
        }
    }
}

MemTable::~MemTable() {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
            _resource_ctx->memory_context()->mem_tracker()->write_tracker());
    {
        SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);
        g_memtable_cnt << -1;
        if (_keys_type != KeysType::DUP_KEYS) {
            for (auto it = _row_in_blocks->begin(); it != _row_in_blocks->end(); it++) {
                if (!(*it)->has_init_agg()) {
                    continue;
                }
                // We should release agg_places here, because they are not released when a
                // load is canceled.
                for (size_t i = _tablet_schema->num_key_columns(); i < _num_columns; ++i) {
                    auto function = _agg_functions[i];
                    DCHECK(function != nullptr);
                    function->destroy((*it)->agg_places(i));
                }
            }
        }

        _arena.clear(true);
        _vec_row_comparator.reset();
        _row_in_blocks.reset();
        _agg_functions.clear();
        _input_mutable_block.clear();
        _output_mutable_block.clear();
        // Reset the LSN sidecar to release its capacity tracked by the memtable tracker.
        _output_row_binlog_lsns = DorisVector<int64_t>();
    }
    if (_is_flush_success) {
        // If the memtable is flush success, then its memtracker's consumption should be 0
        if (_mem_tracker->consumption() != 0 && config::crash_in_memory_tracker_inaccurate) {
            LOG(FATAL) << "memtable flush success but cosumption is not 0, it is "
                       << _mem_tracker->consumption();
        }
    }
}

int RowInBlockComparator::operator()(const RowInBlock* left, const RowInBlock* right) const {
    return _pblock->compare_at(left->_row_pos, right->_row_pos, _tablet_schema->num_key_columns(),
                               *_pblock, -1);
}

// 数据写入存储引擎内存缓冲区的核心入口
// 当上游计算层（如 Load 任务或 Insert 语句）产生数据块（Block）并需要写入某个 Tablet 时，就会调用该方法将数据灌入内存中。
// const Block* input_block  输入的数据块指针。包含了待写入的原始列数据、行数据及可能携带的中间辅助列。
// const TabletAddRowsPayload& rows 行负载载荷对象。其中封装了本次需要写入的行索引列表（row_idxs，即选择哪些行写入）以及对应的行级 Binlog LSN 列表（row_binlog_lsns）。
Status MemTable::insert(const Block* input_block, const TabletAddRowsPayload& rows) {
    // 1. 切换线程内存追踪器限制
    // 切换当前线程的 MemTracker 限制器到写操作专用的 Tracker，确保后续内存分配被正确计入写资源上下文中。
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
            _resource_ctx->memory_context()->mem_tracker()->write_tracker());
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);
    // 3. 提取行索引与 Binlog LSN
    // 从 rows 中提取出具体的行索引数组（row_idxs）和对应的 Binlog 逻辑序列号数组（row_binlog_lsns）。
    const auto& row_idxs = rows.row_idxs;
    const auto& row_binlog_lsns = rows.row_binlog_lsns;
    // 4. 行级 Binlog LSN 的严格校验逻辑
    if (_need_row_binlog_lsn) {
        if (row_binlog_lsns.empty()) {
            return Status::InternalError(
                    "row binlog lsn is missing for memtable insert, "
                    "tablet_id={}",
                    _tablet_id);
        }
        // 校验 LSN 数量必须与待写入的行数（row_idxs 数量）严格对齐。
        DCHECK_EQ(row_binlog_lsns.size(), row_idxs.size());
    } else if (!row_binlog_lsns.empty()) {
        return Status::InternalError(
                "row binlog lsn is unexpectedly provided for memtable insert, tablet_id={}",
                _tablet_id);
    }
    // 5. 首次插入时的初始化逻辑（懒加载初始化结构）
    if (_is_first_insertion) {
        _is_first_insertion = false;
        // 5.1 初始化输入可变块（_input_mutable_block）
        // 根据 input_block 克隆一份没有具体列数据的结构骨架
        auto clone_block = input_block->clone_without_columns(&_column_offset);
        // 构建用于存放输入原始数据的 MutableBlock。
        _input_mutable_block = MutableBlock::build_mutable_block(std::move(clone_block));
        // 将行比较器绑定到这个输入块上，供后续排序使用。
        _vec_row_comparator->set_block(&_input_mutable_block);
        clone_block = input_block->clone_without_columns(&_column_offset);
        // 5.2 初始化输出可变块（_output_mutable_block）
        // 同样克隆结构并构建用于存放排序/聚合后输出结果的 MutableBlock。
        _output_mutable_block = MutableBlock::build_mutable_block(std::move(clone_block));
        // 5.3 确定 Sequence 列在块中的索引位置
        if (_tablet_schema->has_sequence_col()) {
            if (_partial_update_mode == UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS) {
                // for unique key fixed partial update, sequence column index in block
                // may be different with the index in `_tablet_schema`
                // 对于唯一键固定列部分更新，块中的 Sequence 列索引可能与 Schema 中的不一致，需遍历查找。
                for (int32_t i = 0; i < clone_block.columns(); i++) {
                    if (clone_block.get_by_position(i).name == SEQUENCE_COL) {
                        _seq_col_idx_in_block = i;
                        break;
                    }
                }
            } else {
                // 标准模式下直接从 TabletSchema 获取 Sequence 列的索引。
                _seq_col_idx_in_block = _tablet_schema->sequence_col_idx();
            }
        }
        // 5.4 处理灵活部分更新（Flexible Partial Update）中的特殊列索引
        if (_partial_update_mode == UniqueKeyUpdateModePB::UPDATE_FLEXIBLE_COLUMNS &&
            _tablet_schema->has_skip_bitmap_col()) {
            // init of _skip_bitmap_col_idx and _delete_sign_col_idx must be before _init_agg_functions()
            _skip_bitmap_col_idx = _tablet_schema->skip_bitmap_col_idx();
            _delete_sign_col_idx = _tablet_schema->delete_sign_idx();
            _delete_sign_col_unique_id = _tablet_schema->column(_delete_sign_col_idx).unique_id();
            if (_seq_col_idx_in_block != -1) {
                _seq_col_unique_id = _tablet_schema->column(_seq_col_idx_in_block).unique_id();
            }
        }
        // 5.5 初始化聚合函数（针对非 Duplicate 键模型）
        if (_keys_type != KeysType::DUP_KEYS) {
            // there may be additional intermediate columns in input_block
            // we only need columns indicated by column offset in the output
            // 输入块中可能包含额外的中间辅助列，这里根据列偏移量初始化聚合函数和状态。
            // RETURN_IF_CATCH_EXCEPTION 宏用于捕获初始化期间可能抛出的异常。
            RETURN_IF_CATCH_EXCEPTION(_init_agg_functions(&clone_block));
        }
    }
    // 6. 获取待插入的行数与当前输入块的光标位置
    auto num_rows = row_idxs.size();
    size_t cursor_in_mutableblock = _input_mutable_block.rows();
    // 7. 将指定行追加到输入可变块中
    // 调用 _input_mutable_block.add_rows，根据 row_idxs 指定的行范围，从 input_block 中批量拷贝数据到内存块中。
    RETURN_IF_ERROR(_input_mutable_block.add_rows(input_block, row_idxs.data(),
                                                  row_idxs.data() + num_rows, &_column_offset));
    // 8. 为每行创建 RowInBlock 索引元数据并存入向量
    for (int i = 0; i < num_rows; i++) {
        _row_in_blocks->emplace_back(std::make_shared<RowInBlock>(
                cursor_in_mutableblock + i, _need_row_binlog_lsn ? row_binlog_lsns[i] : 0));
    }
    // 9. 更新统计指标并返回成功
    // 将原始插入的行数累加到 _stat.raw_rows 中，最后返回 Status::OK() 表示插入成功。
    _stat.raw_rows += num_rows;
    return Status::OK();
}

void MemTable::_merge_row_binlog_lsn(RowInBlock* src_row, RowInBlock* dst_row) {
    if (_need_row_binlog_lsn) {
        dst_row->_row_binlog_lsn = std::max(dst_row->_row_binlog_lsn, src_row->_row_binlog_lsn);
    }
}

void MemTable::_append_output_row_binlog_lsn(RowInBlock* row) {
    if (_need_row_binlog_lsn) {
        _output_row_binlog_lsns.emplace_back(row->_row_binlog_lsn);
    }
}

void MemTable::_aggregate_two_row_with_sequence_map(MutableBlock& mutable_block,
                                                    RowInBlock* src_row, RowInBlock* dst_row) {
    _merge_row_binlog_lsn(src_row, dst_row);
    // for each mapping replace value columns according to the sequence column compare result
    // for example: a b c d s1 s2  (key:a , s1=>[b,c], s2=>[d])
    // src row:     1 4 5 6  8 9
    // dst row:     1 2 3 4  7 10
    // after aggregate
    // dst row:     1 4 5 4  8 10  (b,c,s1 will be replaced, d,s2)
    const auto& seq_map = _tablet_schema->seq_col_idx_to_value_cols_idx();
    for (const auto& it : seq_map) {
        auto sequence = it.first;
        auto* sequence_col_ptr = mutable_block.mutable_columns()[sequence].get();
        auto res = sequence_col_ptr->compare_at(dst_row->_row_pos, src_row->_row_pos,
                                                *sequence_col_ptr, -1);
        if (res > 0) {
            continue;
        }
        for (auto cid : it.second) {
            if (cid < _num_columns) {
                auto* col_ptr = mutable_block.mutable_columns()[cid].get();
                _agg_functions[cid]->add(dst_row->agg_places(cid),
                                         const_cast<const doris::IColumn**>(&col_ptr),
                                         src_row->_row_pos, _arena);
            }
        }
        if (sequence < _num_columns) {
            _agg_functions[sequence]->add(dst_row->agg_places(sequence),
                                          const_cast<const doris::IColumn**>(&sequence_col_ptr),
                                          src_row->_row_pos, _arena);
            // must use replace column instead of update row_pos
            // because one row may have multi sequence column
            // and agg function add method won't change the real column value
            sequence_col_ptr->replace_column_data(*sequence_col_ptr, src_row->_row_pos,
                                                  dst_row->_row_pos);
        }
    }
}

template <bool has_skip_bitmap_col>
void MemTable::_aggregate_two_row_in_block(MutableBlock& mutable_block, RowInBlock* src_row,
                                           RowInBlock* dst_row) {
    _merge_row_binlog_lsn(src_row, dst_row);
    // for flexible partial update, the caller must guarantees that either src_row and dst_row
    // both specify the sequence column, or src_row and dst_row both don't specify the
    // sequence column
    if (_tablet_schema->has_sequence_col() && _seq_col_idx_in_block >= 0) {
        DCHECK_LT(_seq_col_idx_in_block, mutable_block.columns());
        auto col_ptr = mutable_block.mutable_columns()[_seq_col_idx_in_block].get();
        auto res = col_ptr->compare_at(dst_row->_row_pos, src_row->_row_pos, *col_ptr, -1);
        // dst sequence column larger than src, don't need to update
        if (res > 0) {
            return;
        }
        // need to update the row pos in dst row to the src row pos when has
        // sequence column
        dst_row->_row_pos = src_row->_row_pos;
    }
    // dst is non-sequence row, or dst sequence is smaller
    if constexpr (!has_skip_bitmap_col) {
        DCHECK(_skip_bitmap_col_idx == -1);
        for (size_t cid = _tablet_schema->num_key_columns(); cid < _num_columns; ++cid) {
            auto* col_ptr = mutable_block.mutable_columns()[cid].get();
            _agg_functions[cid]->add(dst_row->agg_places(cid),
                                     const_cast<const doris::IColumn**>(&col_ptr),
                                     src_row->_row_pos, _arena);
        }
    } else {
        DCHECK(_skip_bitmap_col_idx != -1);
        DCHECK_LT(_skip_bitmap_col_idx, mutable_block.columns());
        const BitmapValue& skip_bitmap =
                assert_cast<ColumnBitmap*, TypeCheckOnRelease::DISABLE>(
                        mutable_block.mutable_columns()[_skip_bitmap_col_idx].get())
                        ->get_data()[src_row->_row_pos];
        for (size_t cid = _tablet_schema->num_key_columns(); cid < _num_columns; ++cid) {
            const auto& col = _tablet_schema->column(cid);
            if (cid != _skip_bitmap_col_idx && skip_bitmap.contains(col.unique_id())) {
                continue;
            }
            auto* col_ptr = mutable_block.mutable_columns()[cid].get();
            _agg_functions[cid]->add(dst_row->agg_places(cid),
                                     const_cast<const doris::IColumn**>(&col_ptr),
                                     src_row->_row_pos, _arena);
        }
    }
}
Status MemTable::_put_into_output(Block& in_block) {
    SCOPED_RAW_TIMER(&_stat.put_into_output_ns);
    DorisVector<uint32_t> row_pos_vec;
    DCHECK(in_block.rows() <= std::numeric_limits<int>::max());
    row_pos_vec.reserve(in_block.rows());
    if (_need_row_binlog_lsn) {
        _output_row_binlog_lsns.reserve(_output_row_binlog_lsns.size() + in_block.rows());
    }
    for (int i = 0; i < _row_in_blocks->size(); i++) {
        row_pos_vec.emplace_back((*_row_in_blocks)[i]->_row_pos);
        _append_output_row_binlog_lsn((*_row_in_blocks)[i].get());
    }
    return _output_mutable_block.add_rows(&in_block, row_pos_vec.data(),
                                          row_pos_vec.data() + in_block.rows());
}
// 对内存中暂存的所有行索引（_row_in_blocks）进行高效的多列排序，确保主键/排序列有序，同时支持增量排序、稳定排序以及新老数据的原地归并（Inplace Merge），并返回相同 Key（重复键/冲突键）的数量供后续聚合使用。
size_t MemTable::_sort() {
	// 1. 耗时统计与计数器自增
    // 启动原始计时器累加排序总耗时（sort_ns），并将排序执行次数（sort_times）加 1，用于性能 Profile 监控。
    SCOPED_RAW_TIMER(&_stat.sort_ns);
    _stat.sort_times++;
	// 初始化相同键（重复键）的计数器
    size_t same_keys_num = 0;
    // sort new rows
	// 2. 初始化 Tie（相等区间管理）对象
    // Tie 用于记录在多列排序过程中键值相等的行区间（即具有相同 Key 的行范围）。
    // 它的范围是从上次已排序的位置（_last_sorted_pos）到当前所有行向量的末尾（_row_in_blocks->size()），
    // 实现了“增量排序”优化，避免对之前已经排好序的老数据进行重复排序。
    Tie tie = Tie(_last_sorted_pos, _row_in_blocks->size());
    // 3. 逐列进行基数排序 / 独立列排序（逐个排序列比较）
    // 遍历表定义中的每一个排序列（Key Columns），按列从左到右依次进行排序。
    for (size_t i = 0; i < _tablet_schema->num_key_columns(); i++) {
		// 定义一个局部的行比较 Lambda 函数：
        // 针对当前第 i 列，比较左行（lhs）和右行（rhs）在该列上的大小。
        auto cmp = [&](RowInBlock* lhs, RowInBlock* rhs) -> int {
            return _input_mutable_block.compare_one_column(lhs->_row_pos, rhs->_row_pos, i, -1);
        };
		// 调用 _sort_one_column，结合当前的 tie 区间和比较器，对指定列进行排序。
        _sort_one_column(*_row_in_blocks, tie, cmp);
    }
	// 判断当前表是否为 Duplicate 键模型（DUP_KEYS）
    bool is_dup = (_keys_type == KeysType::DUP_KEYS);
    // sort extra round by _row_pos to make the sort stable
	// 4. 利用 Tie 迭代器进行稳定排序（按原行号 _row_pos 排序）
    // 经过上一轮多列排序后，相同 Key 的行可能会乱序。这里通过 tie.iter() 找出所有 Key 相同的区间。
    auto iter = tie.iter();
    while (iter.next()) {
		// 使用高效的 pdqsort（Pattern-Defeating QuickSort）对每个相同 Key 的局部区间进行排序。
        // - 如果是 Duplicate 模型：按原始行号逆序排列（l->_row_pos > r->_row_pos），保持写入先后顺序。
        // - 如果是 Unique/Aggregate 模型：按原始行号正序排列，确保后写入的覆盖先写入的。
        pdqsort(std::next(_row_in_blocks->begin(), iter.left()),
                std::next(_row_in_blocks->begin(), iter.right()),
                [&is_dup](const std::shared_ptr<RowInBlock>& lhs,
                          const std::shared_ptr<RowInBlock>& rhs) -> bool {
                    return is_dup ? lhs->_row_pos > rhs->_row_pos : lhs->_row_pos < rhs->_row_pos;
                });
		// 累加相同 Key 的行数统计
        same_keys_num += iter.right() - iter.left();
    }
    // merge new rows and old rows
	// 5. 新老数据归并准备
    // 将行比较器（_vec_row_comparator）绑定到当前的输入可变块（_input_mutable_block）上。
    _vec_row_comparator->set_block(&_input_mutable_block);
	// 定义用于新老数据归并的复合比较器（cmp_func）：
    auto cmp_func = [this, is_dup, &same_keys_num](const std::shared_ptr<RowInBlock>& l,
                                                   const std::shared_ptr<RowInBlock>& r) -> bool {
        auto value = (*(this->_vec_row_comparator))(l.get(), r.get());
        if (value == 0) {
			// 如果新老数据中的 Key 相同，冲突数自增 1，并根据模型决定行号先后顺序
            same_keys_num++;
            return is_dup ? l->_row_pos > r->_row_pos : l->_row_pos < r->_row_pos;
        } else {
			// 否则返回常规的大小比较结果（左边小于右边返回 true）
            return value < 0;
        }
    };
	// 6. 新老数据就地归并（Inplace Merge）
    // 找到新写入数据的起始迭代器位置（_last_sorted_pos）。
    auto new_row_it = std::next(_row_in_blocks->begin(), _last_sorted_pos);
	// 使用 std::inplace_merge，将“之前已排好序的老数据区间”和“刚刚排好序的新数据区间”进行高效的就地归并，
    // 使整个 _row_in_blocks 整体保持完全有序。
    std::inplace_merge(_row_in_blocks->begin(), new_row_it, _row_in_blocks->end(), cmp_func);
	// 7. 更新已排序位置并返回
    // 将 _last_sorted_pos 更新为当前总行数，标记所有数据均已完成排序。
    _last_sorted_pos = _row_in_blocks->size();
    return same_keys_num;
}

Status MemTable::_sort_by_cluster_keys() {
    SCOPED_RAW_TIMER(&_stat.sort_ns);
    _stat.sort_times++;
    // sort all rows
    Block in_block = _output_mutable_block.to_block();
    auto clone_block = in_block.clone_without_columns();
    MutableBlock mutable_block = MutableBlock::build_mutable_block(std::move(in_block));
    _output_mutable_block = MutableBlock::build_mutable_block(std::move(clone_block));

    DorisVector<std::shared_ptr<RowInBlock>> row_in_blocks;
    row_in_blocks.reserve(mutable_block.rows());
    if (_need_row_binlog_lsn) {
        DCHECK_EQ(_output_row_binlog_lsns.size(), mutable_block.rows());
    }
    for (size_t i = 0; i < mutable_block.rows(); i++) {
        row_in_blocks.emplace_back(
                _need_row_binlog_lsn ? std::make_shared<RowInBlock>(i, _output_row_binlog_lsns[i])
                                     : std::make_shared<RowInBlock>(i));
    }
    if (_need_row_binlog_lsn) {
        _output_row_binlog_lsns.clear();
        _output_row_binlog_lsns.reserve(mutable_block.rows());
    }
    Tie tie = Tie(0, mutable_block.rows());

    for (auto cid : _tablet_schema->cluster_key_uids()) {
        auto index = _tablet_schema->field_index(cid);
        if (index == -1) {
            return Status::InternalError("could not find cluster key column with unique_id=" +
                                         std::to_string(cid) + " in tablet schema");
        }
        auto cmp = [&](const RowInBlock* lhs, const RowInBlock* rhs) -> int {
            return mutable_block.compare_one_column(lhs->_row_pos, rhs->_row_pos, index, -1);
        };
        _sort_one_column(row_in_blocks, tie, cmp);
    }

    // sort extra round by _row_pos to make the sort stable
    auto iter = tie.iter();
    while (iter.next()) {
        pdqsort(std::next(row_in_blocks.begin(), iter.left()),
                std::next(row_in_blocks.begin(), iter.right()),
                [](const std::shared_ptr<RowInBlock>& lhs, const std::shared_ptr<RowInBlock>& rhs)
                        -> bool { return lhs->_row_pos < rhs->_row_pos; });
    }

    in_block = mutable_block.to_block();
    SCOPED_RAW_TIMER(&_stat.put_into_output_ns);
    DorisVector<uint32_t> row_pos_vec;
    DCHECK(in_block.rows() <= std::numeric_limits<int>::max());
    row_pos_vec.reserve(in_block.rows());
    for (int i = 0; i < row_in_blocks.size(); i++) {
        row_pos_vec.emplace_back(row_in_blocks[i]->_row_pos);
        _append_output_row_binlog_lsn(row_in_blocks[i].get());
    }
    std::vector<int> column_offset;
    for (int i = 0; i < _column_offset.size(); ++i) {
        column_offset.emplace_back(i);
    }
    return _output_mutable_block.add_rows(&in_block, row_pos_vec.data(),
                                          row_pos_vec.data() + in_block.rows(), &column_offset);
}

void MemTable::_sort_one_column(DorisVector<std::shared_ptr<RowInBlock>>& row_in_blocks, Tie& tie,
                                std::function<int(RowInBlock*, RowInBlock*)> cmp) {
    auto iter = tie.iter();
    while (iter.next()) {
        pdqsort(std::next(row_in_blocks.begin(), static_cast<int>(iter.left())),
                std::next(row_in_blocks.begin(), static_cast<int>(iter.right())),
                [&cmp](auto lhs, auto rhs) -> bool { return cmp(lhs.get(), rhs.get()) < 0; });
        tie[iter.left()] = 0;
        for (auto i = iter.left() + 1; i < iter.right(); i++) {
            tie[i] = (cmp(row_in_blocks[i - 1].get(), row_in_blocks[i].get()) == 0);
        }
    }
}

template <bool is_final>
void MemTable::_finalize_one_row(RowInBlock* row, MutableBlock& mutable_block, int row_pos) {
    // move key columns
    for (size_t i = 0; i < _tablet_schema->num_key_columns(); ++i) {
        _output_mutable_block.get_column_by_position(i)->insert_from(
                *mutable_block.get_column_by_position(i), row->_row_pos);
    }
    if (row->has_init_agg()) {
        // get value columns from agg_places
        for (size_t i = _tablet_schema->num_key_columns(); i < _num_columns; ++i) {
            auto function = _agg_functions[i];
            auto* agg_place = row->agg_places(i);
            auto* col_ptr = _output_mutable_block.get_column_by_position(i).get();
            function->insert_result_into(agg_place, *col_ptr);

            if constexpr (is_final) {
                function->destroy(agg_place);
            } else {
                function->reset(agg_place);
            }
        }

        if constexpr (is_final) {
            row->remove_init_agg();
        } else {
            for (size_t i = _tablet_schema->num_key_columns(); i < _num_columns; ++i) {
                auto function = _agg_functions[i];
                auto* agg_place = row->agg_places(i);
                auto* col_ptr = _output_mutable_block.get_column_by_position(i).get();
                function->add(agg_place, const_cast<const doris::IColumn**>(&col_ptr), row_pos,
                              _arena);
            }
        }
    } else {
        // move columns for rows do not need agg
        for (size_t i = _tablet_schema->num_key_columns(); i < _num_columns; ++i) {
            _output_mutable_block.get_column_by_position(i)->insert_from(
                    *mutable_block.get_column_by_position(i), row->_row_pos);
        }
    }
    _append_output_row_binlog_lsn(row);
    if constexpr (!is_final) {
        row->_row_pos = row_pos;
    }
}

void MemTable::_init_row_for_agg(RowInBlock* row, MutableBlock& mutable_block) {
    row->init_agg_places(_arena.aligned_alloc(_total_size_of_aggregate_states, 16),
                         _offsets_of_aggregate_states.data());
    for (auto cid = _tablet_schema->num_key_columns(); cid < _num_columns; cid++) {
        auto* col_ptr = mutable_block.mutable_columns()[cid].get();
        auto* data = row->agg_places(cid);
        _agg_functions[cid]->create(data);
        _agg_functions[cid]->add(data, const_cast<const doris::IColumn**>(&col_ptr), row->_row_pos,
                                 _arena);
    }
}
void MemTable::_clear_row_agg(RowInBlock* row) {
    if (row->has_init_agg()) {
        for (size_t i = _tablet_schema->num_key_columns(); i < _num_columns; ++i) {
            auto function = _agg_functions[i];
            auto* agg_place = row->agg_places(i);
            function->destroy(agg_place);
        }
        row->remove_init_agg();
    }
}
// only in `to_block` the `is_final` flag will be true, in other cases, it will be false
template <bool is_final, bool has_skip_bitmap_col>
void MemTable::_aggregate() {
    SCOPED_RAW_TIMER(&_stat.agg_ns);
    _stat.agg_times++;
    Block in_block = _input_mutable_block.to_block();
    std::unique_ptr<Block> empty_input_block = in_block.create_same_struct_block(0);
    MutableBlock mutable_block = MutableBlock::build_mutable_block(std::move(in_block));
    _vec_row_comparator->set_block(&mutable_block);
    DorisVector<std::shared_ptr<RowInBlock>> temp_row_in_blocks;
    temp_row_in_blocks.reserve(_last_sorted_pos);
    //only init agg if needed

    if constexpr (!has_skip_bitmap_col) {
        RowInBlock* prev_row = nullptr;
        int row_pos = -1;
        for (const auto& cur_row_ptr : *_row_in_blocks) {
            RowInBlock* cur_row = cur_row_ptr.get();
            if (!temp_row_in_blocks.empty() && (*_vec_row_comparator)(prev_row, cur_row) == 0) {
                if (!prev_row->has_init_agg()) {
                    _init_row_for_agg(prev_row, mutable_block);
                }
                _stat.merged_rows++;
                if (_tablet_schema->has_seq_map()) {
                    _aggregate_two_row_with_sequence_map(mutable_block, cur_row, prev_row);
                } else {
                    _aggregate_two_row_in_block<has_skip_bitmap_col>(mutable_block, cur_row,
                                                                     prev_row);
                }

                // Clean up aggregation state of the merged row to avoid memory leak
                if (cur_row) {
                    _clear_row_agg(cur_row);
                }
            } else {
                prev_row = cur_row;
                if (!temp_row_in_blocks.empty()) {
                    // The rows from the previous batch of _row_in_blocks have been merged into temp_row_in_blocks,
                    // now call finalize to write the aggregation results into _output_mutable_block.
                    _finalize_one_row<is_final>(temp_row_in_blocks.back().get(), mutable_block,
                                                row_pos);
                }
                temp_row_in_blocks.push_back(cur_row_ptr);
                row_pos++;
            }
        }
        if (!temp_row_in_blocks.empty()) {
            // finalize the last low
            _finalize_one_row<is_final>(temp_row_in_blocks.back().get(), mutable_block, row_pos);
        }
    } else {
        DCHECK(_delete_sign_col_idx != -1);
        if (_seq_col_idx_in_block == -1) {
            _aggregate_for_flexible_partial_update_without_seq_col<is_final>(mutable_block,
                                                                             temp_row_in_blocks);
        } else {
            _aggregate_for_flexible_partial_update_with_seq_col<is_final>(mutable_block,
                                                                          temp_row_in_blocks);
        }
    }
    if constexpr (!is_final) {
        // if is not final, we collect the agg results to input_block and then continue to insert
        _input_mutable_block.swap(_output_mutable_block);
        //TODO(weixang):opt here.
        _output_mutable_block = MutableBlock::build_mutable_block(std::move(*empty_input_block));
        _output_mutable_block.clear_column_data();
        _output_row_binlog_lsns.clear();
        *_row_in_blocks = temp_row_in_blocks;
        _last_sorted_pos = _row_in_blocks->size();
    }
}

template <bool is_final>
void MemTable::_aggregate_for_flexible_partial_update_without_seq_col(
        MutableBlock& mutable_block, DorisVector<std::shared_ptr<RowInBlock>>& temp_row_in_blocks) {
    std::shared_ptr<RowInBlock> prev_row {nullptr};
    int row_pos = -1;
    auto& skip_bitmaps =
            assert_cast<ColumnBitmap*>(mutable_block.mutable_columns()[_skip_bitmap_col_idx].get())
                    ->get_data();
    auto& delete_signs =
            assert_cast<ColumnInt8*>(mutable_block.mutable_columns()[_delete_sign_col_idx].get())
                    ->get_data();
    std::shared_ptr<RowInBlock> row_with_delete_sign {nullptr};
    std::shared_ptr<RowInBlock> row_without_delete_sign {nullptr};

    auto finalize_rows = [&]() {
        if (row_with_delete_sign != nullptr) {
            temp_row_in_blocks.push_back(row_with_delete_sign);
            _finalize_one_row<is_final>(row_with_delete_sign.get(), mutable_block, ++row_pos);
            row_with_delete_sign = nullptr;
        }
        if (row_without_delete_sign != nullptr) {
            temp_row_in_blocks.push_back(row_without_delete_sign);
            _finalize_one_row<is_final>(row_without_delete_sign.get(), mutable_block, ++row_pos);
            row_without_delete_sign = nullptr;
        }
        // _arena.clear();
    };

    auto add_row = [&](std::shared_ptr<RowInBlock> row, bool with_delete_sign) {
        if (with_delete_sign) {
            row_with_delete_sign = std::move(row);
        } else {
            row_without_delete_sign = std::move(row);
        }
    };
    for (const auto& cur_row_ptr : *_row_in_blocks) {
        RowInBlock* cur_row = cur_row_ptr.get();
        const BitmapValue& skip_bitmap = skip_bitmaps[cur_row->_row_pos];
        bool cur_row_has_delete_sign = (!skip_bitmap.contains(_delete_sign_col_unique_id) &&
                                        delete_signs[cur_row->_row_pos] != 0);
        prev_row =
                (row_with_delete_sign == nullptr) ? row_without_delete_sign : row_with_delete_sign;
        // compare keys, the keys of row_with_delete_sign and row_without_delete_sign is the same,
        // choose any of them if it's valid
        if (prev_row != nullptr && (*_vec_row_comparator)(prev_row.get(), cur_row) == 0) {
            if (cur_row_has_delete_sign) {
                if (row_without_delete_sign != nullptr) {
                    // if there exits row without delete sign, remove it first
                    _merge_row_binlog_lsn(row_without_delete_sign.get(), cur_row);
                    _clear_row_agg(row_without_delete_sign.get());
                    _stat.merged_rows++;
                    row_without_delete_sign = nullptr;
                }
                // and then unconditionally replace the previous row
                prev_row = row_with_delete_sign;
            } else {
                prev_row = row_without_delete_sign;
            }

            if (prev_row == nullptr) {
                add_row(cur_row_ptr, cur_row_has_delete_sign);
            } else {
                if (!prev_row->has_init_agg()) {
                    _init_row_for_agg(prev_row.get(), mutable_block);
                }
                _stat.merged_rows++;
                _aggregate_two_row_in_block<true>(mutable_block, cur_row, prev_row.get());
            }
        } else {
            finalize_rows();
            add_row(cur_row_ptr, cur_row_has_delete_sign);
        }
    }
    // finalize the last lows
    finalize_rows();
}

template <bool is_final>
void MemTable::_aggregate_for_flexible_partial_update_with_seq_col(
        MutableBlock& mutable_block, DorisVector<std::shared_ptr<RowInBlock>>& temp_row_in_blocks) {
    // For flexible partial update, when table has sequence column, we don't do any aggregation
    // in memtable. These duplicate rows will be aggregated in VerticalSegmentWriter
    int row_pos = -1;
    for (const auto& row_ptr : *_row_in_blocks) {
        RowInBlock* row = row_ptr.get();
        temp_row_in_blocks.push_back(row_ptr);
        _finalize_one_row<is_final>(row, mutable_block, ++row_pos);
    }
}

void MemTable::shrink_memtable_by_agg() {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
            _resource_ctx->memory_context()->mem_tracker()->write_tracker());
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);
    if (_keys_type == KeysType::DUP_KEYS) {
        return;
    }
    size_t same_keys_num = _sort();
    if (same_keys_num != 0) {
        (_skip_bitmap_col_idx == -1) ? _aggregate<false, false>() : _aggregate<false, true>();
    }
    _last_agg_pos = memory_usage();
}

bool MemTable::need_flush() const {
    DBUG_EXECUTE_IF("MemTable.need_flush", { return true; });
    auto max_size = _adaptive_write_buffer_size();
    if (_partial_update_mode == UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS) {
        auto update_columns_size = _num_columns;
        auto min_buffer_size = config::min_write_buffer_size_for_partial_update;
        max_size = max_size * update_columns_size / _tablet_schema->num_columns();
        max_size = max_size > min_buffer_size ? max_size : min_buffer_size;
    }

    if (memory_usage() >= max_size) {
        g_flush_cuz_memtable_full << 1;
        return true;
    }
    return false;
}

int64_t MemTable::_adaptive_write_buffer_size() const {
    if (!config::enable_adaptive_write_buffer_size) [[unlikely]] {
        return config::write_buffer_size;
    }
    auto* memtable_limiter = ExecEnv::GetInstance()->memtable_memory_limiter();
    int64_t memtable_mem =
            (memtable_limiter != nullptr && memtable_limiter->mem_tracker() != nullptr)
                    ? memtable_limiter->mem_tracker()->consumption()
                    : 0;
    int64_t factor = 4;
    // Memory usage intervals:
    // (80 %, 100 %] → 1× buffer
    // (50 %, 80 %]  → 2× buffer
    // [0 %, 50 %]   → 4× buffer
    if (memtable_mem > (_load_mem_limit * 4) / 5) { // > 80 %
        factor = 1;
    } else if (memtable_mem > _load_mem_limit / 2) { // > 50 %
        factor = 2;
    }
    return config::write_buffer_size * factor;
}

bool MemTable::need_agg() const {
    if (_keys_type == KeysType::AGG_KEYS) {
        auto max_size = _last_agg_pos + config::write_buffer_size_for_agg;
        return memory_usage() >= max_size;
    }
    return false;
}

size_t MemTable::get_flush_reserve_memory_size() const {
    if (_keys_type == KeysType::DUP_KEYS && _tablet_schema->num_key_columns() == 0) {
        return 0; // no need to reserve
    }
    return static_cast<size_t>(static_cast<double>(_input_mutable_block.allocated_bytes()) * 1.2);
}

// 内存表准备刷盘（Flush）时的核心转换与处理引擎
// 当 MemTable 的内存达到阈值需要生成底层 Segment 文件时，该方法被触发
// 核心职责是：对内存中的原始数据进行排序（Sort）、消解重复键、执行聚合（Aggregate）或版本覆盖、处理集群键（Cluster Keys）排序、管理行级 Binlog LSN，最终将处理完的数据打包转换为标准的不可变 Block 输出。
Status MemTable::_to_block(std::unique_ptr<Block>* res) {
    // 1. 清空输出的行 Binlog LSN 列表
    // 每次转换开始前，先重置输出的 Binlog LSN 缓存，确保数据干净。
    _output_row_binlog_lsns.clear();
	// 2. 对内存中的行索引进行排序
    // 调用 _sort() 对内部行指针（_row_in_blocks）进行排序，将相同的 Key 聚拢在一起，
    // 并返回相同 Key 的数量（same_keys_num）。
    size_t same_keys_num = _sort();
	// 3. 分支判断：无需聚合或无重复键的情况
    // 如果是 Duplicate Key 模型（DUP_KEYS，本身不需要聚合），或者虽然是 Unique/Aggregate 模型但没有重复键（same_keys_num == 0），
    // 说明行与行之间不会发生冲突合并。
    if (_keys_type == KeysType::DUP_KEYS || same_keys_num == 0) {
		// 3.1 极速路径：无排序键的 Duplicate 模型
        // 如果是 Duplicate 键模型且没有定义任何排序列（num_key_columns() == 0），
        // 则直接通过 swap 将输入可变块的内容整体置换到输出可变块中，零数据拷贝开销。
        if (_keys_type == KeysType::DUP_KEYS && _tablet_schema->num_key_columns() == 0) {
            _output_mutable_block.swap(_input_mutable_block);
			// 如果开启了行级 Binlog，则为每一行追加对应的 Binlog LSN。
            if (_need_row_binlog_lsn) {
                _output_row_binlog_lsns.reserve(_row_in_blocks->size());
                for (const auto& row : *_row_in_blocks) {
                    _append_output_row_binlog_lsn(row.get());
                }
            }
        } else {
			// 3.2 常规非聚合路径
            // 将输入可变块转换为标准的不可变 Block，然后通过 _put_into_output 规整后放入输出块中。
            Block in_block = _input_mutable_block.to_block();
            RETURN_IF_ERROR(_put_into_output(in_block));
        }
    } else {
		// 4. 聚合/去重路径（有相同 Key 的 Aggregate 或 Unique 模型）
        // 如果存在重复键且表模型需要聚合或版本覆盖，则调用模板化的 _aggregate 方法。
        // 根据是否存在跳过位图列（skip_bitmap_col_idx），决定是否开启跳过位图模板分支。
        (_skip_bitmap_col_idx == -1) ? _aggregate<true, false>() : _aggregate<true, true>();
    }
	// 5. Cluster Keys（集群键）排序处理
    // 如果是 Unique Key 且开启了 MOW（写时合并）模式，并且表定义了集群键（Cluster Keys）：
    if (_keys_type == KeysType::UNIQUE_KEYS && _enable_unique_key_mow &&
        !_tablet_schema->cluster_key_uids().empty()) {
        if (_partial_update_mode != UniqueKeyUpdateModePB::UPSERT) {
            return Status::InternalError(
                    "Partial update for mow with cluster keys is not supported");
        }
		// 执行按集群键的二次排序（_sort_by_cluster_keys），确保物理存储顺序符合集群键定义。
        RETURN_IF_ERROR(_sort_by_cluster_keys());
    }
    if (_need_row_binlog_lsn) {
        DCHECK_EQ(_output_row_binlog_lsns.size(), _output_mutable_block.rows());
    }
	// 7. 清理输入块内存
    // 数据已经处理并转移到了输出端，清空输入可变块（_input_mutable_block）以释放内存。
    _input_mutable_block.clear();
	// 8. 封装并输出最终的 Block
    // 将最终整理好的 _output_mutable_block 转换为不可变 Block，
    // 并通过 Block::create_unique 包装成 unique_ptr 赋值给输出参数 *res。
    *res = Block::create_unique(_output_mutable_block.to_block());
    return Status::OK();
}

Status MemTable::to_block(std::unique_ptr<Block>* res) {
    RETURN_IF_ERROR_OR_CATCH_EXCEPTION(_to_block(res));
    return Status::OK();
}

} // namespace doris
