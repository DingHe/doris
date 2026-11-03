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

#include <stddef.h>
#include <stdint.h>

#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <vector>

#include "common/status.h"
#include "core/arena.h"
#include "core/block/block.h"
#include "core/custom_allocator.h"
#include "exprs/aggregate/aggregate_function.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/thread_context.h"
#include "storage/partial_update_info.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

class Schema;
class SlotDescriptor;
class TabletSchema;
struct TabletAddRowsPayload;
class TupleDescriptor;
enum KeysType : int;

// Active: the memtable is currently used by writer to insert into blocks
// Write_finished: the memtable finished write blocks and in the queue waiting for flush
// FLUSH: the memtable is under flushing, write segment to disk.
// ACTIVE = 0：活跃状态。Writer 线程当前正在向其中插入新的数据块（Block）。
// WRITE_FINISHED = 1：写入完成状态。数据已停止写入，MemTable 被放入待刷写队列中等待 Flush。
// FLUSH = 2：刷写状态。MemTable 正在被后台刷写线程将内存数据写入磁盘生成 Segment 文件。
enum MemType { ACTIVE = 0, WRITE_FINISHED = 1, FLUSH = 2 };

// row pos in _input_mutable_block
// 表示 _input_mutable_block 中某一行数据的元数据索引结构，
// 用于在内存中进行高效的排序和聚合操作，避免直接移动庞大的行数据。
struct RowInBlock {
    // 该行在 _input_mutable_block 中的行号（位置）
    size_t _row_pos;
    // 行级别的 Binlog LSN（默认 0）
    int64_t _row_binlog_lsn = 0;
    // 聚合状态所在的内存指针。
    char* _agg_mem = nullptr;
    // 聚合状态的偏移量数组
    size_t* _agg_state_offset = nullptr;
    // 标记该行的聚合状态是否已经初始化。
    bool _has_init_agg;
    // 构造函数，初始化行位置及可选的 Binlog LSN。
    RowInBlock(size_t row) : _row_pos(row), _has_init_agg(false) {}
    RowInBlock(size_t row, int64_t row_binlog_lsn)
            : _row_pos(row), _row_binlog_lsn(row_binlog_lsn), _has_init_agg(false) {}

    // 初始化聚合内存及偏移量。
    void init_agg_places(char* agg_mem, size_t* agg_state_offset) {
        _has_init_agg = true;
        _agg_mem = agg_mem;
        _agg_state_offset = agg_state_offset;
    }
    // 获取指定偏移量处的聚合状态内存地址。
    char* agg_places(size_t offset) const { return _agg_mem + _agg_state_offset[offset]; }

    inline bool has_init_agg() const { return _has_init_agg; }

    inline void remove_init_agg() { _has_init_agg = false; }
};
// 用于多列排序或分块排序（Tie 区间管理），在相同键（Key）的情况下标记相等的行范围。
class Tie {
public:
    class Iter {
    public:
        Iter(Tie& tie) : _tie(tie), _next(tie._begin + 1) {}
        // 获取当前范围的左右边界
        size_t left() const { return _left; }
        size_t right() const { return _right; }

        // return false means no more ranges
        // 寻找下一个相等的键范围，若无更多范围则返回 false
        bool next() {
            if (_next >= _tie._end) {
                return false;
            }
            _next = _find(1, _next);
            if (_next >= _tie._end) {
                return false;
            }
            _left = _next - 1;
            _next = _find(0, _next);
            _right = _next;
            return true;
        }

    private:
        // 在位图中查找指定值的辅助函数。
         size_t _find(uint8_t value, size_t start) {
            if (start >= _tie._end) {
                return start;
            }
            size_t offset = start - _tie._begin;
            size_t size = _tie._end - start;
            void* p = std::memchr(_tie._bits.data() + offset, value, size);
            if (p == nullptr) {
                return _tie._end;
            }
            return static_cast<uint8_t*>(p) - _tie._bits.data() + _tie._begin;
        }

    private:
        Tie& _tie;
        size_t _left;
        size_t _right;
        size_t _next;
    };

public:
    Tie(size_t begin, size_t end) : _begin(begin), _end(end) {
        _bits = std::vector<uint8_t>(_end - _begin, 1);
    }
    // 获取或设置指定索引的位值。
    uint8_t operator[](size_t i) const { return _bits[i - _begin]; }
    uint8_t& operator[](size_t i) { return _bits[i - _begin]; }
    // 返回一个迭代器对象
    Iter iter() { return Iter(*this); }

private:
    // 区间的起止位置
    const size_t _begin;
    const size_t _end;
    // 存储位信息的 vector
    std::vector<uint8_t> _bits;
};
// 行比较器，用于在排序时对比两个 RowInBlock 的大小。
class RowInBlockComparator {
public:
    RowInBlockComparator(std::shared_ptr<TabletSchema> tablet_schema)
            : _tablet_schema(tablet_schema) {}
    // call set_block before operator().
    // only first time insert block to create _input_mutable_block,
    // so can not Comparator of construct to set pblock
    void set_block(MutableBlock* pblock) { _pblock = pblock; }
    int operator()(const RowInBlock* left, const RowInBlock* right) const;

private:
    // 表结构定义。
    std::shared_ptr<TabletSchema> _tablet_schema;
    // 对应 MemTable::_input_mutable_block 的可变块指针。
    MutableBlock* _pblock = nullptr; //  corresponds to Memtable::_input_mutable_block
};
// 统计 MemTable 运行过程中的各项指标（行数、耗时等）
class MemTableStat {
public:
    MemTableStat& operator+=(const MemTableStat& stat) {
        raw_rows += stat.raw_rows;
        merged_rows += stat.merged_rows;
        sort_ns += stat.sort_ns;
        agg_ns += stat.agg_ns;
        put_into_output_ns += stat.put_into_output_ns;
        duration_ns += stat.duration_ns;
        sort_times += stat.sort_times;
        agg_times += stat.agg_times;

        return *this;
    }
    // 原始插入的行数。
    std::atomic<int64_t> raw_rows = 0;
    // 聚合/合并后的行数。
    std::atomic<int64_t> merged_rows = 0;
    // 排序耗时（纳秒）
    int64_t sort_ns = 0;
    // 聚合耗时（纳秒）。
    int64_t agg_ns = 0;
    // 写入输出块耗时。
    int64_t put_into_output_ns = 0;
    // 总持续时间。
    int64_t duration_ns = 0;
    // 排序和聚合执行的次数。
    std::atomic<int64_t> sort_times = 0;
    std::atomic<int64_t> agg_times = 0;
};

// MemTable 是数据写入（Load / Insert）流向存储引擎时的内存缓冲区与预处理核心类
// 当客户端写入数据时，数据首先被攒入 MemTable 中。在这里，Doris 会根据表类型（如 Aggregate、Unique、Duplicate 模型）在内存中进行排序（Sort）、聚合（Aggregate）、去重、版本合并或部分更新处理。
// 当内存达到阈值时，MemTable 会被冻结并刷写（Flush）到磁盘上生成底部的 Segment 文件。

class MemTable {
public:
    MemTable(int64_t tablet_id, std::shared_ptr<TabletSchema> tablet_schema,
             const std::vector<SlotDescriptor*>* slot_descs, TupleDescriptor* tuple_desc,
             bool enable_unique_key_mow, PartialUpdateInfo* partial_update_info,
             const std::shared_ptr<ResourceContext>& resource_ctx,
             bool need_row_binlog_lsn = false);
    ~MemTable();

    int64_t tablet_id() const { return _tablet_id; }
    size_t memory_usage() const { return _mem_tracker->consumption(); }
    size_t get_flush_reserve_memory_size() const;
    // insert tuple from (row_pos) to (row_pos+num_rows)
    Status insert(const Block* block, const TabletAddRowsPayload& rows);

    void shrink_memtable_by_agg();

    bool need_flush() const;

    bool need_agg() const;

    Status to_block(std::unique_ptr<Block>* res);

    const DorisVector<int64_t>& row_binlog_lsns() const { return _output_row_binlog_lsns; }

    bool empty() const { return _input_mutable_block.rows() == 0; }

    const MemTableStat& stat() { return _stat; }

    std::shared_ptr<ResourceContext> resource_ctx() { return _resource_ctx; }

    std::shared_ptr<MemTracker> mem_tracker() { return _mem_tracker; }

    void set_flush_success() { _is_flush_success = true; }

    MemType get_mem_type() { return _mem_type; }

    void update_mem_type(MemType memtype) { _mem_type = memtype; }

    int64_t raw_rows() { return _stat.raw_rows.load(); }

private:
    // for vectorized
    template <bool has_skip_bitmap_col>
    void _aggregate_two_row_in_block(MutableBlock& mutable_block, RowInBlock* new_row,
                                     RowInBlock* row_in_skiplist);

    // Merge row-binlog LSN sidecar only when MemTable merges two RowInBlock objects.
    // Table models that require complex merge semantics, such as AGG tables and unique key
    // merge-on-read tables, do not support row-binlog LSN now and are rejected in insert().
    void _merge_row_binlog_lsn(RowInBlock* src_row, RowInBlock* dst_row);

    void _append_output_row_binlog_lsn(RowInBlock* row);

    void _aggregate_two_row_with_sequence_map(MutableBlock& mutable_block, RowInBlock* new_row,
                                              RowInBlock* row_in_skiplist);

    // Used to wrapped by to_block to do exception handle logic
    Status _to_block(std::unique_ptr<Block>* res);

    int64_t _adaptive_write_buffer_size() const;

private:
    // 标记当前内存块状态（ACTIVE / WRITE_FINISHED / FLUSH）
    std::atomic<MemType> _mem_type;
    // 所属的 Tablet ID
    int64_t _tablet_id;
    // 是否开启 Unique Key Merge-on-Write（写时合并）模式
    bool _enable_unique_key_mow = false;
    // 标记刷盘是否成功。
    bool _is_flush_success = false;
    // 部分更新模式（如 UPSERT）
    UniqueKeyUpdateModePB _partial_update_mode {UniqueKeyUpdateModePB::UPSERT};
    // 表的键类型（如 DUP_KEYS, AGG_KEYS, UNIQUE_KEYS）
    const KeysType _keys_type;
    // 表结构元数据指针
    std::shared_ptr<TabletSchema> _tablet_schema;
    // 向量化行比较器
    std::shared_ptr<RowInBlockComparator> _vec_row_comparator;
    // 资源上下文
    std::shared_ptr<ResourceContext> _resource_ctx;
    // 内存追踪器，监控当前 MemTable 消耗的内存大小
    std::shared_ptr<MemTracker> _mem_tracker;
    // Only the rows will be inserted into block can allocate memory from _arena.
    // In this way, we can make MemTable::memory_usage() to be more accurate, and eventually
    // reduce the number of segment files that are generated by current load
    // 内存分配池（Arena），用于行数据和聚合状态的高效内存分配，确保 memory_usage() 更加精准。
    Arena _arena;
    // 导入内存限制。
    int64_t _load_mem_limit = -1;

    void _init_columns_offset_by_slot_descs(const std::vector<SlotDescriptor*>* slot_descs,
                                            const TupleDescriptor* tuple_desc);
    // 列的索引
    // 常用于只需要提取部分特定列或调整列顺序的场景
    std::vector<int> _column_offset;

    // Number of rows inserted to this memtable.
    // This is not the rows in this memtable, because rows may be merged
    // in unique or aggregate key model.
    // 统计信息对象。
    MemTableStat _stat;

    //for vectorized
    // 输入的可变数据块，暂存原始写入数据。
    MutableBlock _input_mutable_block;
    // 输出的可变数据块，存放排序/聚合后的结果。
    MutableBlock _output_mutable_block;
    // 输出的 Binlog LSN 列表
    DorisVector<int64_t> _output_row_binlog_lsns;
    // 是否需要记录行级 Binlog LSN
    bool _need_row_binlog_lsn = false;
	// 上次排序和聚合的位置记录。
    size_t _last_sorted_pos = 0;
    size_t _last_agg_pos = 0;

    //return number of same keys
    size_t _sort();
    Status _sort_by_cluster_keys();
    void _sort_one_column(DorisVector<std::shared_ptr<RowInBlock>>& row_in_blocks, Tie& tie,
                          std::function<int(RowInBlock*, RowInBlock*)> cmp);
    template <bool is_final>
    void _finalize_one_row(RowInBlock* row, MutableBlock& mutable_block, int row_pos);
    void _init_row_for_agg(RowInBlock* row, MutableBlock& mutable_block);
    void _clear_row_agg(RowInBlock* row);

    template <bool is_final, bool has_skip_bitmap_col = false>
    void _aggregate();

    template <bool is_final>
    void _aggregate_for_flexible_partial_update_without_seq_col(
            MutableBlock& mutable_block,
            DorisVector<std::shared_ptr<RowInBlock>>& temp_row_in_blocks);

    template <bool is_final>
    void _aggregate_for_flexible_partial_update_with_seq_col(
            MutableBlock& mutable_block,
            DorisVector<std::shared_ptr<RowInBlock>>& temp_row_in_blocks);

    Status _put_into_output(Block& in_block);
	// 标记是否为第一次插入
    bool _is_first_insertion;
    // 初始化聚合函数
    void _init_agg_functions(const Block* block);
	// 聚合函数指针列表
    std::vector<AggregateFunctionPtr> _agg_functions;
	// 聚合状态在内存中的偏移量。
    std::vector<size_t> _offsets_of_aggregate_states;
	// 聚合状态的总大小
    size_t _total_size_of_aggregate_states;
	// 指向行索引数组的智能指针
    std::unique_ptr<DorisVector<std::shared_ptr<RowInBlock>>> _row_in_blocks;
	// 列数。
    size_t _num_columns;
	// 特殊列在块中的索引位置（Sequence 列、Skip Bitmap 列、Delete Sign 列）
    int32_t _seq_col_idx_in_block {-1};
    int32_t _skip_bitmap_col_idx {-1};
    int32_t _delete_sign_col_idx {-1};
	// 特殊列的唯一 ID。
    int32_t _delete_sign_col_unique_id {-1};
    int32_t _seq_col_unique_id {-1};
	// 是否为部分更新且包含自增列
    bool _is_partial_update_and_auto_inc = false;
}; // class MemTable

} // namespace doris
