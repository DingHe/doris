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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/Block.h
// and modified by Doris

#pragma once

#include <glog/logging.h>
#include <parallel_hashmap/phmap.h>

#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <list>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/be_mock_util.h"
#include "common/exception.h"
#include "common/factory_creator.h"
#include "common/status.h"
#include "core/block/column_with_type_and_name.h"
#include "core/block/columns_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_nullable.h"
#include "core/types.h"

class SipHash;

namespace doris {

class TupleDescriptor;
class PBlock;
class SlotDescriptor;

namespace segment_v2 {
enum CompressionTypePB : int;
} // namespace segment_v2

/** Container for set of columns for bunch of rows in memory.
  * This is unit of data processing.
  * Also contains metadata - data types of columns and their names
  *  (either original names from a table, or generated names during temporary calculations).
  * Allows to insert, remove columns in arbitrary position, to change order of columns.
  */
class MutableBlock;
// Block 是向量化执行引擎（Vectorized Execution Engine）中最核心的数据结构。
// Block 是 Doris 内存中列式数据批次（Data Batch）的逻辑抽象与容器。
// 在 Doris 的向量化计算架构中，数据不再逐行处理，而是以 Block 为单位在各个算子（Scan, Filter, Join, Aggregation, Sort, Sink 等）之间进行流转和计算：
// 按列组织的内存块：Block 内部维护了一组列（Column）。每列由 数据（ColumnPtr）、数据类型（DataTypePtr） 和 列名（Name） 三要素（ColumnWithTypeAndName）组成。
// 三位一体的数据模式：不仅包含内存中的实际物理列数据，还绑定了逻辑 Schema，保证计算算子可以安全地根据索引或列名提取对应类型的数据列。
// 高效流转与零拷贝：提供智能的指针转移、资源剥离（Scoped Guards）、引用替换与序列化/反序列化能力，在算子间传递时能最大程度降低内存拷贝开销。

class Block {
    // Doris 宏，用于自动生成工厂创建方法（如 create_shared），方便使用智能指针创建 Block。
    ENABLE_FACTORY_CREATOR(Block);

private:
    // 私有类型别名，代表底层存储多个列元素的容器类型（本质是 std::vector<ColumnWithTypeAndName>）
    using Container = ColumnsWithTypeAndName;
    // 存储当前 Block 包含的所有列（包括列的数据指针、类型和列名）。
    Container data;

public:
    // 默认构造函数，创建一个不包含任何列的空 Block。
    Block() = default;
    // 使用初始化列表构造 Block，常用于测试或手动组装特定列。
    Block(std::initializer_list<ColumnWithTypeAndName> il);
    // 直接使用已有的列容器 ColumnsWithTypeAndName 构造 Block。
    Block(ColumnsWithTypeAndName data_);
	// 根据查询执行计划中的 SlotDescriptor 指针数组及其预估行数 block_size，预先初始化 Block 的列结构与内存空间。
    Block(const std::vector<SlotDescriptor*>& slots, size_t block_size);
    Block(const std::vector<SlotDescriptor>& slots, size_t block_size);

    MOCK_FUNCTION ~Block() = default;
    Block(const Block& block) = default;
    Block& operator=(const Block& p) = default;
    Block(Block&& block) = default;
    Block& operator=(Block&& other) = default;
	// 为底层的列容器 data 预分配存放 count 个列的容量（减少 vector 扩容开销）。
    void reserve(size_t count);
    // Make sure the nammes is useless when use block
	// 清空当前 Block 中所有列的名称（将 name 设为空字符串）。在算子内部计算时列名无用，清空可节省字符串内存与比较开销。
    void clear_names();

    /// insert the column at the specified position
	// 在指定索引 position 处插入一列（支持拷贝和移动）
    void insert(size_t position, const ColumnWithTypeAndName& elem);
	// 在 Block 的末尾追加一列（支持拷贝和移动）。
    void insert(size_t position, ColumnWithTypeAndName&& elem);
    /// insert the column to the end
    void insert(const ColumnWithTypeAndName& elem);
    void insert(ColumnWithTypeAndName&& elem);
    /// remove the column at the specified position
	// 删除指定索引 position 处的列。
    void erase(size_t position);
    /// remove the column at the [start, end)
	// 删除从索引 start 开始一直到末尾的所有列。
    void erase_tail(size_t start);
    /// remove the columns at the specified positions
	// 批量删除指定位置集合 positions 中的所有列。
    void erase(const std::set<size_t>& positions);
    // T was std::set<int>, std::vector<int>, std::list<int>
	// 保留在 container（如 vector/set<int>）中指定的列索引，删除其他所有未被指定的列。
    template <class T>
    void erase_not_in(const T& container) {
        Container new_data;
        for (auto pos : container) {
            new_data.emplace_back(std::move(data[pos]));
        }
        std::swap(data, new_data);
    }
	// 构建并返回一个“列名 -> 列在 Block 中的位置索引”的哈希映射表。
    std::unordered_map<std::string, uint32_t> get_name_to_pos_map() const {
        std::unordered_map<std::string, uint32_t> name_to_index_map;
        for (uint32_t i = 0; i < data.size(); ++i) {
            name_to_index_map[data[i].name] = i;
        }
        return name_to_index_map;
    }

    /// References are invalidated after calling functions above.
	// 根据索引位置直接获取 ColumnWithTypeAndName 对象的引用（内含 DCHECK 越界检查）。
    ColumnWithTypeAndName& get_by_position(size_t position) {
        DCHECK(data.size() > position)
                << ", data.size()=" << data.size() << ", position=" << position;
        return data[position];
    }
	// 替换指定位置上的物理列指针（例如将表达式计算后的新列覆盖旧列）。
    const ColumnWithTypeAndName& get_by_position(size_t position) const { return data[position]; }

	// 如果指定位置的列是常量列（ColumnConst），将其展开解压为完全展开的普通物理列（Full Column）。
    void replace_by_position(size_t position, ColumnPtr&& res) {
        this->get_by_position(position).column = std::move(res);
    }

    void replace_by_position(size_t position, const ColumnPtr& res) {
        this->get_by_position(position).column = res;
    }

    void replace_by_position_if_const(size_t position) {
        auto& element = this->get_by_position(position);
        element.column = element.column->convert_to_full_column_if_const();
    }
	// 安全地获取指定位置的列（内部带有严格的边界检查与异常处理）。
    ColumnWithTypeAndName& safe_get_by_position(size_t position);
    const ColumnWithTypeAndName& safe_get_by_position(size_t position) const;
	// 提供标准 C++ 容器迭代器，支持用 for (auto& col : block) 遍历所有列。
    Container::iterator begin() { return data.begin(); }
    Container::iterator end() { return data.end(); }
    Container::const_iterator begin() const { return data.begin(); }
    Container::const_iterator end() const { return data.end(); }
    Container::const_iterator cbegin() const { return data.cbegin(); }
    Container::const_iterator cend() const { return data.cend(); }

    // Get position of column by name. Returns -1 if there is no column with that name.
    // ATTN: this method is O(N). better maintain name -> position map in caller if you need to call it frequently.
	// 根据列名查找对应的索引位置，若找不到则返回 -1（时间复杂度 $O(N)$）。
    int get_position_by_name(const std::string& name) const;
	// 获取底层包含所有列完整信息的引用。
    const ColumnsWithTypeAndName& get_columns_with_type_and_name() const;
	// 提取并返回当前 Block 中所有列的列名列表。
    std::vector<std::string> get_names() const;
	// 提取并返回当前 Block 中所有列的数据类型列表。
    DataTypes get_data_types() const;
	// 获取指定索引位置列的数据类型。
    DataTypePtr get_data_type(size_t index) const {
        CHECK(index < data.size());
        return data[index].type;
    }

    /// Returns number of rows from first column in block, not equal to nullptr. If no columns, returns 0.
	// 获取当前 Block 的总行数（即取第一个非空列的行数；若无列则返回 0）。
    size_t rows() const;

    // Cut the rows in block, use in LIMIT operation
	// 截断或设置 Block 内所有列的行数为 length（常用于 LIMIT 算子）。
    void set_num_rows(size_t length);

    // Skip the rows in block, use in OFFSET, LIMIT operation
	// 在所有列中裁切跳过前 offset 行数据（常用于 OFFSET 算子）。
    void skip_num_rows(int64_t& offset);

    /// As the assumption we used around, the number of columns won't exceed int16 range. so no need to worry when we
    ///  assign it to int32.
	// 获取当前 Block 拥有的列数。
    uint32_t columns() const { return static_cast<uint32_t>(data.size()); }

    /// Checks that every column in block is not nullptr and has same number of elements.
	// 校验检查当前 Block 中所有列的行数是否严格一致。
    void check_number_of_rows(bool allow_null_columns = false) const;
	// 校验列的实际物理数据类型与 ColumnWithTypeAndName 中声明的 DataType 是否匹配。
    Status check_type_and_column() const;
	// 校验检查所有列的类型指针和列数据指针是否都不为空（nullptr）。
    Status check_column_and_type_not_null() const;
	// 检查 Block 中是否不包含 64 位大长字符串列（某些算子不支持超大字符串列）。
    Status check_no_column_string64() const;

    /// Approximate number of bytes used by column data in memory.
    /// This reflects the actual data footprint (e.g. string contents, numeric arrays)
    /// and is the metric used by adaptive batch size byte budgets.
	// 计算并返回当前 Block 中列数据占用的实际物理内存字节数（不包含预分配未使用的保留空间），常用于自适应 Batch 内存管控。
    size_t bytes() const;

    /// Approximate number of allocated (reserved) bytes in memory.
    /// This may be larger than bytes() due to pre-allocated capacity in vectors/arenas.
    /// Used for memory tracking and profiling.
	// 计算并返回当前 Block 已申请分配的总内存字节数（包含预分配的 Capacity 空间），用于内存 Profiling 和 Tracker 统计。
    MOCK_FUNCTION size_t allocated_bytes() const;

    /** Get a list of column names separated by commas. */
	// 返回以逗号分隔的所有列名拼接成的字符串。
    std::string dump_names() const;
	// 返回以逗号分隔的所有列类型名称字符串。
    std::string dump_types() const;

    /** List of names, types and lengths of columns. Designed for debugging. */
	// 打印 Block 的结构信息（列名、数据类型、行数、数据列指针等），常用于调试日志。
    std::string dump_structure() const;

    /** Get the same block, but empty. */
	// 深度克隆一个结构相同但内容为空（0 行）的新 Block（保留相同的列名和数据类型，但物理列是空的）。
    Block clone_empty() const;
	// 将 Block 中所有列的物理指针提取并打包为一个 Columns 数组返回。
    Columns get_columns() const;
	// 提取所有列指针，并将其中的常量列/稀疏列转换为标准完整列。
    Columns get_columns_and_convert();
	// 克隆一个不包含物理列数据的 Schema 壳子 Block。
    Block clone_without_columns(const std::vector<int>* column_offset = nullptr) const;

    /** Get empty columns with the same types as in block. */
	// 根据当前 Block 的类型契约，创建一组可变的、初始为空的列指针（MutableColumns）。
    MutableColumns clone_empty_columns() const;

    // RAII owner for mutating columns borrowed from a live Block. While the
    // guard is alive, the Block's column slots are moved out and column data
    // must be accessed through mutable_columns(). The guard restores columns on
    // destruction, so use it when the caller may exit early after detaching.
	// 全列可变作用域保护器。构造时会将 Block 内部的物理列全部 move 出来变为可修改的 MutableColumns，在此期间 Block 内部列指针暂留为空。当该 Guard 析构时，会自动将修改后的列重新写回并还原给 Block，保证异常安全。
    class ScopedMutableColumns {
    public:
        explicit ScopedMutableColumns(Block& block);
        ~ScopedMutableColumns();

        ScopedMutableColumns(const ScopedMutableColumns&) = delete;
        ScopedMutableColumns& operator=(const ScopedMutableColumns&) = delete;
        ScopedMutableColumns(ScopedMutableColumns&& other) noexcept;
        ScopedMutableColumns& operator=(ScopedMutableColumns&& other) noexcept;
		// 获取可变列数组引用。
        MutableColumns& mutable_columns() { return _columns; }
        const MutableColumns& mutable_columns() const { return _columns; }
		// 获取指定列的元数据。
        const DataTypePtr& get_datatype_by_position(size_t position) const;
        const std::string& get_name_by_position(size_t position) const;

        // Transfer the borrowed owners to another RAII object that will restore
        // them. After release(), the original Block remains without columns
        // until that owner restores them. Normal callers should let this guard
        // restore on destruction.
		// 放弃恢复权并转移可变列所有权。
        MutableColumns release();
		// 手动提前还原列数据给 Block。
        void restore();

    private:
        Block* _block = nullptr;
        MutableColumns _columns;
    };

    // Single-column variant for localized mutation of a live Block slot. The
    // selected slot is unavailable from the Block until this guard restores it.
	// 单列可变作用域保护器。仅将 Block 中指定位置 position 的单个列 move 出来修改，析构时自动写回。
    class ScopedMutableColumn {
    public:
        ScopedMutableColumn(Block& block, size_t position);
        ~ScopedMutableColumn();

        ScopedMutableColumn(const ScopedMutableColumn&) = delete;
        ScopedMutableColumn& operator=(const ScopedMutableColumn&) = delete;
        ScopedMutableColumn(ScopedMutableColumn&& other) noexcept;
        ScopedMutableColumn& operator=(ScopedMutableColumn&& other) noexcept;
		// 获取该单列的可变指针引用。
        MutableColumnPtr& mutable_column() { return _column; }
        const MutableColumnPtr& mutable_column() const { return _column; }
		// 手动提前还原该列数据。
        void restore();

    private:
        Block* _block = nullptr;
        size_t _position = 0;
        MutableColumnPtr _column;
    };

    /** Get columns from a consumed block for mutation. Columns in block will be nullptr. */
    MutableColumns mutate_columns() &&;
    MutableColumns mutate_columns() & = delete;

    /** Temporarily mutate a live Block's columns. The returned guard owns the columns and
      * restores them on destruction; prefer this over manual move/writeback.
      */
    ScopedMutableColumns mutate_columns_scoped() &;
    ScopedMutableColumns mutate_columns_scoped() && = delete;

    /** Temporarily mutate one live Block column; use when only one slot needs ownership. */
    ScopedMutableColumn mutate_column_scoped(size_t position) &;
    ScopedMutableColumn mutate_column_scoped(size_t position) && = delete;

    /** Replace columns in a block */
	// 用传入的移动列数组 columns 整体替换当前 Block 内部的所有物理列数据。
    void set_columns(MutableColumns&& columns);
	// 彻底清空 Block（清除所有列和元数据，使其变成空 Block）。
    void clear();
	// 无开销地交换两个 Block 的底层数据容器。
    void swap(Block& other) noexcept;
    void swap(Block&& other) noexcept;

    // Shuffle columns in place based on the result_column_ids
	// 根据传入的列 ID 映射数组 result_column_ids，对当前 Block 中的列进行原地重排。
    void shuffle_columns(const std::vector<int>& result_column_ids);

    // column_size == -1 clears all columns; otherwise clear [0, column_size)
    // and drop the rest. Shared columns are detached through clone_empty(), so
    // allocation or clone failures propagate.
	// 清空列中的数据（保留列结构与 Schema）。若传入 column_size，则清空前 column_size 列的数据并丢弃其余列。
    void clear_column_data(int64_t column_size = -1);
	// 仅清空指定索引集合 columns_to_clear 中列的数据内容。
    void clear_column_data(const std::vector<uint32_t>& columns_to_clear);
	// 检查当前 Block 是否包含列结构，用于判定是否可以复用内存。
    MOCK_FUNCTION bool mem_reuse() { return !data.empty(); }
	// 判断 Block 是否不包含任何列（data.empty()）。
    bool is_empty_column() { return data.empty(); }
	// 判断 Block 是否没有数据行（即 rows() == 0）。
    bool empty() const { return rows() == 0; }

    /** 
      * Updates SipHash of the Block, using update method of columns.
      * Returns hash for block, that could be used to differentiate blocks
      *  with same structure, but different data.
      */
	// 调用所有列的 update_hash 方法，将当前 Block 内所有行数据的 SipHash 累计更新到 hash 对象中（用于计算 Block 级 Hash 校验和）。
    void update_hash(SipHash& hash) const;

    /** 
     *  Get block data in string. 
     *  If code is in default_implementation_for_nulls or something likely, type and column's nullity could
     *   temporarily be not same. set allow_null_mismatch to true to dump it correctly.
    */
	// 将 Block 中的数据按可读格式（文本表格形态）转义为字符串（默认最多打印 100 行），用于 Debug 场景。
    std::string dump_data(size_t begin = 0, size_t row_limit = 100,
                          bool allow_null_mismatch = false) const;
	// 将 Block 中的数据转义为 JSON 格式字符串。
    std::string dump_data_json(size_t begin = 0, size_t row_limit = 100,
                               bool allow_null_mismatch = false) const;

    /** Get one line data from block, only use in load data */
	// 格式化输出 Block 中指定某一行（row）的数据，常用于数据导入（Load）错误日志打印。
    std::string dump_one_line(size_t row, int column_end) const;
	// 根据选择器 selector（记载了每一行目标路由），将当前 Block 中的行散列/追加分发到目标可变块 dst 中（常用于 Shuffle / Hash Partition 算子）。
    Status append_to_block_by_selector(MutableBlock* dst, const IColumn::Selector& selector) const;

    // need exception safety
	// 根据传入的条件过滤器位图 IColumn::Filter，直接对 Block 内部的列进行原地过滤行裁切（保留 Filter 中为 1 的行）。
    static void filter_block_internal(Block* block, const std::vector<uint32_t>& columns_to_filter,
                                      const IColumn::Filter& filter);
    // need exception safety
    static void filter_block_internal(Block* block, const IColumn::Filter& filter,
                                      uint32_t column_to_keep);
    // need exception safety
    static void filter_block_internal(Block* block, const IColumn::Filter& filter);

    static Status filter_block(Block* block, const std::vector<uint32_t>& columns_to_filter,
                               size_t filter_column_id, size_t column_to_keep);
	// 对外暴露的过滤静态接口，根据指定列 filter_column_id 的布尔值过滤整个 Block。
    static Status filter_block(Block* block, size_t filter_column_id, size_t column_to_keep);
	// 快捷清除 block 中第 column_to_keep 列之后的所有辅助/临时计算列。
    static void erase_useless_column(Block* block, size_t column_to_keep) {
        block->erase_tail(column_to_keep);
    }

    // serialize block to PBlock
	// 将当前 Block 序列化压缩为 RPC 传输使用的 Protocol Buffer 结构 PBlock（支持设置压缩算法类型如 LZ4/Snappy，并记录压缩前后字节数及耗时）。
    Status serialize(int be_exec_version, PBlock* pblock, size_t* uncompressed_bytes,
                     size_t* compressed_bytes, int64_t* compress_time,
                     segment_v2::CompressionTypePB compression_type,
                     bool allow_transfer_large_data = false) const;
	// 将 RPC 接收到的 Protobuf PBlock 反序列化并解压恢复为内存中的 Block 对象。
    Status deserialize(const PBlock& pblock, size_t* uncompressed_bytes, int64_t* decompress_time);
	// 创建一个与当前 Block 包含相同列类型和 Schema 的新 Block，并预分配 size 行空间。
    std::unique_ptr<Block> create_same_struct_block(size_t size, bool is_reserve = false) const;

    /** Compares (*this) n-th row and rhs m-th row.
      * Returns negative number, 0, or positive number  (*this) n-th row is less, equal, greater than rhs m-th row respectively.
      * Is used in sortings.
      *
      * If one of element's value is NaN or NULLs, then:
      * - if nan_direction_hint == -1, NaN and NULLs are considered as least than everything other;
      * - if nan_direction_hint ==  1, NaN and NULLs are considered as greatest than everything other.
      * For example, if nan_direction_hint == -1 is used by descending sorting, NaNs will be at the end.
      *
      * For non Nullable and non floating point types, nan_direction_hint is ignored.
      */
	// 比较当前 Block 的第 n 行与另一个 rhs Block 的第 m 行（全列逐一比较）。返回 <0, 0, >0；nan_direction_hint 指定 NULL / NaN 值的排序方向。
    int compare_at(size_t n, size_t m, const Block& rhs, int nan_direction_hint) const {
        DCHECK_EQ(columns(), rhs.columns());
        return compare_at(n, m, columns(), rhs, nan_direction_hint);
    }

    int compare_at(size_t n, size_t m, size_t num_columns, const Block& rhs,
                   int nan_direction_hint) const {
        DCHECK_GE(columns(), num_columns);
        DCHECK_GE(rhs.columns(), num_columns);

        DCHECK_LE(n, rows());
        DCHECK_LE(m, rhs.rows());
        for (size_t i = 0; i < num_columns; ++i) {
            DCHECK(get_by_position(i).type->equals(*rhs.get_by_position(i).type));
            auto res = get_by_position(i).column->compare_at(n, m, *(rhs.get_by_position(i).column),
                                                             nan_direction_hint);
            if (res) {
                return res;
            }
        }
        return 0;
    }

    int compare_at(size_t n, size_t m, const std::vector<uint32_t>* compare_columns,
                   const Block& rhs, int nan_direction_hint) const {
        DCHECK_GE(columns(), compare_columns->size());
        DCHECK_GE(rhs.columns(), compare_columns->size());

        DCHECK_LE(n, rows());
        DCHECK_LE(m, rhs.rows());
        for (auto i : *compare_columns) {
            DCHECK(get_by_position(i).type->equals(*rhs.get_by_position(i).type));
            auto res = get_by_position(i).column->compare_at(n, m, *(rhs.get_by_position(i).column),
                                                             nan_direction_hint);
            if (res) {
                return res;
            }
        }
        return 0;
    }

    //note(wb) no DCHECK here, because this method is only used after compare_at now, so no need to repeat check here.
    // If this method is used in more places, you can add DCHECK case by case.
    int compare_column_at(size_t n, size_t m, size_t col_idx, const Block& rhs,
                          int nan_direction_hint) const {
        auto res = get_by_position(col_idx).column->compare_at(
                n, m, *(rhs.get_by_position(col_idx).column), nan_direction_hint);
        return res;
    }
	// 根据标记数组 column_keep_flags 释放不需要保留的列的物理内存，优化长 Pipeline 执行过程中的内存峰值。
    void clear_column_mem_not_keep(const std::vector<bool>& column_keep_flags,
                                   bool need_keep_first);

    // Helper: sum byte_size() of all mutable columns.
    // Unlike Block::bytes() which operates on immutable ColumnPtr,
    // this works on MutableColumns during block construction (e.g. in BlockReader).
	// 计算传入的 MutableColumns 数组中所有列占用的总物理内存字节数（在构建 Block 的过程中计算累计大小）。
    static inline size_t columns_byte_size(const MutableColumns& cols) {
        size_t total = 0;
        for (const auto& col : cols) {
            total += col->byte_size();
        }
        return total;
    }

private:
	// 实现底层列删除的内部细节。
    void erase_impl(size_t position);
};

using Blocks = std::vector<Block>;
using BlocksList = std::list<Block>;
using BlocksPtr = std::shared_ptr<Blocks>;
using BlocksPtrs = std::shared_ptr<std::vector<BlocksPtr>>;


// MutableBlock 是与不可变的 Block 相对立的可变数据块（Mutable Block）容器。
// MutableBlock 的主要职责是作为高效构建、追加、合并与修改数据列的缓冲构建器（Block Builder）：
// 解决不可变性带来的构建开销：Doris 中的标准 Block 主要持有不可变的列指针（ColumnPtr，即 CowPtr<IColumn>::wrapped_ptr），直接向其追加数据需要频繁触发写时复制（Copy-On-Write, COW）。MutableBlock 内部直接持有独占且可变的列指针（MutableColumns），允许直接在原列内存上做追加（insert）、扩展（resize）等修改操作。
// 高效的行/块级追加与合并：常用于 Aggregation（聚合累加）、Join（连接结果拼装）、Sort（排序缓冲区构建）等需要流式组装数据的算子中。它提供了高效的 add_row、add_rows 和 merge 方法。
// 安全地完成构建到不可变 Block 的转换：提供 to_block() 接口，通过移动语义（Move Semantics）将可变列指针瞬间转换为只读的 Block 并输出给下一级算子，实现零拷贝转换。
class MutableBlock {
    ENABLE_FACTORY_CREATOR(MutableBlock);

private:
    MutableColumns _columns;
    DataTypes _data_types;
    std::vector<std::string> _names;

    void materialize_const_column(size_t position) {
        if (is_column_const(*_columns[position])) {
            // ScopedMutableBlock can retain a const destination while merge materializes
            // its source, so normalize the destination before appending full columns.
            _columns[position] =
                    IColumn::mutate(_columns[position]->convert_to_full_column_if_const());
        }
    }

public:
    // Build from a consumed Block. This has no restore contract: the source
    // Block is left without columns and must not be used as a live output block.
    // For caller-owned live Blocks, use ScopedMutableBlock or
    // mutate_columns_scoped() instead.
    static MutableBlock build_mutable_block(Block&& block) {
        return MutableBlock(std::move(block));
    }
    static MutableBlock build_mutable_block(std::nullptr_t) { return MutableBlock(); }
    static MutableBlock build_mutable_block(Block* block) = delete;
    MutableBlock() = default;
    ~MutableBlock() = default;
    MutableBlock(const MutableBlock&) = delete;
    MutableBlock& operator=(const MutableBlock&) = delete;
    MutableBlock(MutableBlock&& m_block) noexcept
            : _columns(std::move(m_block._columns)),
              _data_types(std::move(m_block._data_types)),
              _names(std::move(m_block._names)) {}

    // Consumes block columns and converts them to mutable columns recursively.
    // This constructor is for temporary/owned Blocks only.
    MutableBlock(Block&& block)
            : _columns(std::move(block).mutate_columns()),
              _data_types(block.get_data_types()),
              _names(block.get_names()) {}

    MutableBlock& operator=(MutableBlock&& m_block) noexcept {
        _columns = std::move(m_block._columns);
        _data_types = std::move(m_block._data_types);
        _names = std::move(m_block._names);
        return *this;
    }

    size_t rows() const;
    size_t columns() const { return _columns.size(); }

    bool empty() const { return rows() == 0; }

    MutableColumns& mutable_columns() { return _columns; }
    const MutableColumns& mutable_columns() const { return _columns; }

    void set_mutable_columns(MutableColumns&& columns) { _columns = std::move(columns); }

    DataTypes& data_types() { return _data_types; }

    MutableColumnPtr& get_column_by_position(size_t position) { return _columns[position]; }
    const MutableColumnPtr& get_column_by_position(size_t position) const {
        return _columns[position];
    }

    DataTypePtr& get_datatype_by_position(size_t position) { return _data_types[position]; }
    const DataTypePtr& get_datatype_by_position(size_t position) const {
        return _data_types[position];
    }

    int compare_one_column(size_t n, size_t m, size_t column_id, int nan_direction_hint) const {
        DCHECK_LE(column_id, columns());
        DCHECK_LE(n, rows());
        DCHECK_LE(m, rows());
        auto& column = get_column_by_position(column_id);
        return column->compare_at(n, m, *column, nan_direction_hint);
    }

    int compare_at(size_t n, size_t m, size_t num_columns, const MutableBlock& rhs,
                   int nan_direction_hint) const {
        DCHECK_GE(columns(), num_columns);
        DCHECK_GE(rhs.columns(), num_columns);

        DCHECK_LE(n, rows());
        DCHECK_LE(m, rhs.rows());
        for (size_t i = 0; i < num_columns; ++i) {
            DCHECK(get_datatype_by_position(i)->equals(*rhs.get_datatype_by_position(i)));
            auto res = get_column_by_position(i)->compare_at(n, m, *(rhs.get_column_by_position(i)),
                                                             nan_direction_hint);
            if (res) {
                return res;
            }
        }
        return 0;
    }

    int compare_at(size_t n, size_t m, const std::vector<uint32_t>* compare_columns,
                   const MutableBlock& rhs, int nan_direction_hint) const {
        DCHECK_GE(columns(), compare_columns->size());
        DCHECK_GE(rhs.columns(), compare_columns->size());

        DCHECK_LE(n, rows());
        DCHECK_LE(m, rhs.rows());
        for (auto i : *compare_columns) {
            DCHECK(get_datatype_by_position(i)->equals(*rhs.get_datatype_by_position(i)));
            auto res = get_column_by_position(i)->compare_at(n, m, *(rhs.get_column_by_position(i)),
                                                             nan_direction_hint);
            if (res) {
                return res;
            }
        }
        return 0;
    }

    std::string dump_types() const {
        std::string res;
        for (auto type : _data_types) {
            if (!res.empty()) {
                res += ", ";
            }
            res += type->get_name();
        }
        return res;
    }

    template <typename T>
    [[nodiscard]] Status merge(T&& block) {
        RETURN_IF_CATCH_EXCEPTION(return merge_impl(block););
    }

    template <typename T>
    [[nodiscard]] Status merge_ignore_overflow(T&& block) {
        RETURN_IF_CATCH_EXCEPTION(return merge_impl_ignore_overflow(block););
    }

    // only use for join. call ignore_overflow to prevent from throw exception in join
    template <typename T>
    [[nodiscard]] Status merge_impl_ignore_overflow(T&& block) {
        if (_columns.size() != block.columns()) {
            return Status::Error<ErrorCode::INTERNAL_ERROR>(
                    "Merge block not match, self column count: {}, [columns: {}, types: {}], "
                    "input column count: {}, [columns: {}, "
                    "types: {}], ",
                    _columns.size(), dump_names(), dump_types(), block.columns(),
                    block.dump_names(), block.dump_types());
        }
        for (int i = 0; i < _columns.size(); ++i) {
            if (!_data_types[i]->equals(*block.get_by_position(i).type)) {
                throw doris::Exception(doris::ErrorCode::FATAL_ERROR,
                                       "Merge block not match, self:[columns: {}, types: {}], "
                                       "input:[columns: {}, types: {}], ",
                                       dump_names(), dump_types(), block.dump_names(),
                                       block.dump_types());
            }
            materialize_const_column(i);
            _columns[i]->insert_range_from_ignore_overflow(
                    *block.get_by_position(i).column->convert_to_full_column_if_const().get(), 0,
                    block.rows());
        }
        return Status::OK();
    }

    template <typename T>
    [[nodiscard]] Status merge_impl(T&& block) {
        // merge is not supported in dynamic block
        if (_columns.empty() && _data_types.empty()) {
            _data_types = block.get_data_types();
            _names = block.get_names();
            _columns.resize(block.columns());
            for (size_t i = 0; i < block.columns(); ++i) {
                if (block.get_by_position(i).column) {
                    _columns[i] = (*std::move(block.get_by_position(i)
                                                      .column->convert_to_full_column_if_const()))
                                          .mutate();
                } else {
                    _columns[i] = _data_types[i]->create_column();
                }
            }
        } else {
            if (_columns.size() != block.columns()) {
                return Status::Error<ErrorCode::INTERNAL_ERROR>(
                        "Merge block not match, self column count: {}, [columns: {}, types: {}], "
                        "input column count: {}, [columns: {}, "
                        "types: {}], ",
                        _columns.size(), dump_names(), dump_types(), block.columns(),
                        block.dump_names(), block.dump_types());
            }
            for (int i = 0; i < _columns.size(); ++i) {
                materialize_const_column(i);
                if (!_data_types[i]->equals(*block.get_by_position(i).type)) {
                    DCHECK(_data_types[i]->is_nullable())
                            << " target type: " << _data_types[i]->get_name()
                            << " src type: " << block.get_by_position(i).type->get_name();
                    DCHECK(((DataTypeNullable*)_data_types[i].get())
                                   ->get_nested_type()
                                   ->equals(*block.get_by_position(i).type));
                    DCHECK(!block.get_by_position(i).type->is_nullable());
                    _columns[i]->insert_range_from(*make_nullable(block.get_by_position(i).column)
                                                            ->convert_to_full_column_if_const(),
                                                   0, block.rows());
                } else {
                    _columns[i]->insert_range_from(
                            *block.get_by_position(i)
                                     .column->convert_to_full_column_if_const()
                                     .get(),
                            0, block.rows());
                }
            }
        }
        return Status::OK();
    }

    // move to columns' data to a Block. this will invalidate
    Block to_block(int start_column = 0);
    Block to_block(int start_column, int end_column);

    void swap(MutableBlock& other) noexcept;

    void add_row(const Block* block, int row);
    // Batch add row should return error status if allocate memory failed.
    Status add_rows(const Block* block, const uint32_t* row_begin, const uint32_t* row_end,
                    const std::vector<int>* column_offset = nullptr);
    Status add_rows(const Block* block, size_t row_begin, size_t length);

    std::string dump_data(size_t row_limit = 100) const;
    std::string dump_data_json(size_t row_limit = 100) const;

    void clear() {
        _columns.clear();
        _data_types.clear();
        _names.clear();
    }

    // Clear owned mutable columns in place. MutableBlock already owns its
    // columns exclusively, so this does not perform COW detaching or cloning.
    void clear_column_data() noexcept;

    size_t allocated_bytes() const;

    size_t bytes() const {
        size_t res = 0;
        for (const auto& elem : _columns) {
            res += elem->byte_size();
        }

        return res;
    }

    std::vector<std::string>& get_names() { return _names; }

    /** Get a list of column names separated by commas. */
    std::string dump_names() const;
};

// RAII adapter for code that wants the MutableBlock API over a live Block. It
// owns only the temporary mutable columns and restores them to the Block on
// destruction. While the adapter is alive, read/write column data through
// mutable_block()/mutable_columns(); the Block's column slots are moved out.
// ScopedMutableBlock 是一个基于 RAII（Resource Acquisition Is Initialization，资源获取即初始化） 设计模式的适配器类。它专门用于管理对外部活体（Live）Block 的临时修改。
// ScopedMutableBlock 的核心职责是在作用域生命周期内安全地“借用”并修改一个已有的 Block：
// 解决所有权与安全转换问题：通常情况下，Block 持有不可变/只读的列（ColumnPtr），而修改列需要使用 MutableBlock。如果直接将 Block 转换为 MutableBlock，源 Block 内部的列会被剥离置空。如果调用方后续还要继续使用原 Block（即该 Block 是一个外部活体对象），就需要手动将修改后的列再塞回原 Block 中，这一过程极易出错或发生内存泄露/指针空悬。
// RAII 自动还原机制：ScopedMutableBlock 在构造时借用并移出 Block 内部的数据列，包装成 MutableBlock 供外部进行修改/追加；当 ScopedMutableBlock 离开作用域被析构（或主动调用 restore()）时，会自动将修改后的可变列重新转换并还回给原 Block。
// 提供简化的修改接口：它充当了代理角色，直接对外暴露 MutableBlock 和 MutableColumns 的引用，使得在上层逻辑中对原 Block 追加数据就像在局部作用域内操作 MutableBlock 一样简单和安全。
class ScopedMutableBlock {
public:
    ScopedMutableBlock() = delete;
    explicit ScopedMutableBlock(Block* block);
    ~ScopedMutableBlock() { restore(); }

    ScopedMutableBlock(const ScopedMutableBlock&) = delete;
    ScopedMutableBlock& operator=(const ScopedMutableBlock&) = delete;

    ScopedMutableBlock(ScopedMutableBlock&& other) noexcept
            : _block(std::exchange(other._block, nullptr)),
              _mutable_block(std::move(other._mutable_block)) {}

    ScopedMutableBlock& operator=(ScopedMutableBlock&& other) noexcept {
        if (this != &other) {
            restore();
            _block = std::exchange(other._block, nullptr);
            _mutable_block = std::move(other._mutable_block);
        }
        return *this;
    }

    MutableBlock& mutable_block() { return _mutable_block; }
    const MutableBlock& mutable_block() const { return _mutable_block; }
    MutableColumns& mutable_columns() { return _mutable_block.mutable_columns(); }
    const MutableColumns& mutable_columns() const { return _mutable_block.mutable_columns(); }

    void restore() {
        if (_block != nullptr) {
            _block->set_columns(std::move(_mutable_block.mutable_columns()));
            _block = nullptr;
        }
    }

private:
    Block* _block = nullptr;
    MutableBlock _mutable_block;
};
// IteratorRowRef 是一个轻量级的“行指针/行引用”结构体。它不直接存储数据行的具体内容，而是记录某一个行数据在特定 Block 中的位置与归属，主要用于对不同数据块中的数据行进行跨 Block 的比较、游标跟踪与状态重置。
// 多路归并/排序中的跨 Block 行定位：在 Merge-Join、聚合、多路排序归并（Sort-Merge）等算子中，常常需要在多个不同的 Block 之间进行行的比较与迭代。IteratorRowRef 记录了 block 对象的指针和行号 row_pos，相当于数据行的“逻辑指针”。
// 轻量级零拷贝比较：不需要把行数据拷贝出来，直接通过底层绑定的 block 及其 compare_at 接口与其他行进行原地对比，极大地提升了比较和排序效率。
// 等值状态标记（例如 Hash / Distinct / Join 比较）：内置 is_same 标记，用于在比较或归并过程中记录当前行与上一行（或对应行）是否相同（例如用于去重或分组判定）。
struct IteratorRowRef {
	// 指向目标数据块 Block 的共享指针（shared_ptr）。用来确保该行数据所在的内存块在 IteratorRowRef 使用期间生命周期有效，防止内存被提前释放。
    std::shared_ptr<Block> block;
	// 记录当前行在该 Block 中的具体行索引/行号（从 0 开始）。
    int row_pos;
	// 通常在比较排序或归并迭代时使用，用于记录当前行与另一个比较行（如前一行或右表行）的数据是否完全等值。
    bool is_same;
	// 跨 Block 的行比较接口。
	// compare_arguments：比较参数/列定义。可以是列数（size_t num_columns）或者指定的排序列索引向量（const std::vector<uint32_t>* compare_columns）。
    template <typename T>
    int compare(const IteratorRowRef& rhs, const T& compare_arguments) const {
        return block->compare_at(row_pos, rhs.row_pos, compare_arguments, *rhs.block, -1);
    }
	// 重置/清空接口。将该引用归位到无效的初始状态。
    void reset() {
        block = nullptr;
        row_pos = -1;
        is_same = false;
    }
};

using BlockView = std::vector<IteratorRowRef>;
using BlockUPtr = std::unique_ptr<Block>;

} // namespace doris
