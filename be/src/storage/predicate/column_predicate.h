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

#include <memory>
#include <roaring/roaring.hh>

#include "common/compiler_util.h"
#include "common/exception.h"
#include "core/column/column.h"
#include "core/data_type/define_primitive_type.h"
#include "exec/runtime_filter/runtime_filter_selectivity.h"
#include "exprs/runtime_filter_expr.h"
#include "format/parquet/parquet_predicate.h"
#include "runtime/runtime_profile.h"
#include "storage/index/bloom_filter/bloom_filter.h"
#include "storage/index/inverted/inverted_index_iterator.h"
#include "storage/index/zone_map/zone_map_index.h"
#include "util/defer_op.h"

using namespace doris::segment_v2;

namespace doris {

enum class PredicateType {
    UNKNOWN = 0,
    EQ = 1,
    NE = 2,
    LT = 3,
    LE = 4,
    GT = 5,
    GE = 6,
    IN_LIST = 7,
    NOT_IN_LIST = 8,
    IS_NULL = 9,
    IS_NOT_NULL = 10,
    BF = 11,    // BloomFilter
    MATCH = 13, // fulltext match
};

template <PrimitiveType primitive_type, typename ResultType>
ResultType get_zone_map_value(void* data_ptr) {
    ResultType res;
    // DecimalV2's storage value is different from predicate or compute value type
    // need convert it to DecimalV2Value
    if constexpr (primitive_type == PrimitiveType::TYPE_DECIMALV2) {
        decimal12_t decimal_12_t_value;
        memcpy((char*)(&decimal_12_t_value), data_ptr, sizeof(decimal12_t));
        res.from_olap_decimal(decimal_12_t_value.integer, decimal_12_t_value.fraction);
    } else if constexpr (primitive_type == PrimitiveType::TYPE_DATE) {
        static_assert(std::is_same_v<ResultType, VecDateTimeValue>);
        uint24_t date;
        memcpy(&date, data_ptr, sizeof(uint24_t));
        res.from_olap_date(date);
    } else if constexpr (primitive_type == PrimitiveType::TYPE_DATETIME) {
        static_assert(std::is_same_v<ResultType, VecDateTimeValue>);
        uint64_t datetime;
        memcpy(&datetime, data_ptr, sizeof(uint64_t));
        res.from_olap_datetime(datetime);
    } else {
        memcpy(reinterpret_cast<void*>(&res), data_ptr, sizeof(ResultType));
    }
    return res;
}

inline std::string type_to_string(PredicateType type) {
    switch (type) {
    case PredicateType::UNKNOWN:
        return "UNKNOWN";

    case PredicateType::EQ:
        return "EQ";

    case PredicateType::NE:
        return "NE";

    case PredicateType::LT:
        return "LT";

    case PredicateType::LE:
        return "LE";

    case PredicateType::GT:
        return "GT";

    case PredicateType::GE:
        return "GE";

    case PredicateType::IN_LIST:
        return "IN_LIST";

    case PredicateType::NOT_IN_LIST:
        return "NOT_IN_LIST";

    case PredicateType::IS_NULL:
        return "IS_NULL";

    case PredicateType::IS_NOT_NULL:
        return "IS_NOT_NULL";

    case PredicateType::BF:
        return "BF";
    default:
        return "";
    };

    return "";
}

inline std::string type_to_op_str(PredicateType type) {
    switch (type) {
    case PredicateType::EQ:
        return "=";

    case PredicateType::NE:
        return "!=";

    case PredicateType::LT:
        return "<<";

    case PredicateType::LE:
        return "<=";

    case PredicateType::GT:
        return ">>";

    case PredicateType::GE:
        return ">=";

    case PredicateType::IN_LIST:
        return "*=";

    case PredicateType::NOT_IN_LIST:
        return "!*=";

    case PredicateType::IS_NULL:
    case PredicateType::IS_NOT_NULL:
        return "is";

    default:
        break;
    };

    return "";
}

struct PredicateTypeTraits {
    static constexpr bool is_range(PredicateType type) {
        return (type == PredicateType::LT || type == PredicateType::LE ||
                type == PredicateType::GT || type == PredicateType::GE);
    }

    static constexpr bool is_bloom_filter(PredicateType type) { return type == PredicateType::BF; }

    static constexpr bool is_list(PredicateType type) {
        return (type == PredicateType::IN_LIST || type == PredicateType::NOT_IN_LIST);
    }

    static constexpr bool is_equal_or_list(PredicateType type) {
        return (type == PredicateType::EQ || type == PredicateType::IN_LIST);
    }

    static constexpr bool is_comparison(PredicateType type) {
        return (type == PredicateType::EQ || type == PredicateType::NE ||
                type == PredicateType::LT || type == PredicateType::LE ||
                type == PredicateType::GT || type == PredicateType::GE);
    }
};

template <bool is_nullable, typename PredColumn, typename WithNullFunc, typename WithoutNullFunc>
inline ALWAYS_INLINE void evaluate_by_selector(const PredColumn& pred_col, uint16_t size,
                                               uint16_t* sel, uint16_t& new_size,
                                               WithNullFunc&& with_null_func,
                                               WithoutNullFunc&& without_null_func) {
    const bool is_dense_column = pred_col.size() == size;
    for (uint16_t i = 0; i < size; i++) {
        uint16_t idx = is_dense_column ? i : sel[i];
        if constexpr (is_nullable) {
            if (with_null_func(idx)) {
                sel[new_size++] = idx;
            }
        } else {
            if (without_null_func(idx)) {
                sel[new_size++] = idx;
            }
        }
    }
}
// 在 Apache Doris 的 BE 存储层（Segment V2）中，ColumnPredicate 是原子级列谓词（Single-Column Predicate）的抽象基类
// 与 BlockColumnPredicate（负责处理 AND/OR 等逻辑树与块级操作）不同，ColumnPredicate 聚焦于单列具体的过滤逻辑（如等于 EQ、大于 GT、范围 IN、空值判断 IS_NULL、布隆过滤器 BF 等）。
// 原子谓词计算与求值：定义单列谓词在向量化列（IColumn）上的具体过滤行为，支持 Selection Vector 筛选和全量 Vector 判定。
// 多维索引感知与剪枝：针对数据块的元信息（ZoneMap、BloomFilter、字典编码 Dict、倒排索引 Inverted Index、Parquet Page Index）提供针对单列的剪枝评估接口。
// 运行时过滤器（Runtime Filter）自适应钝化（Adaptive Ignoring）：内置动态选择率（Selectivity）采样计算逻辑。当判定某个 Runtime Filter 过滤效果极差（选择率接近 0%，即变成 always_true）时，能自动跳过求值，降低 CPU 开销。
// 性能监控与 Profile 指标收集：内置过滤行数、输入行数以及短路（Always True）跳过行数的 Counter 统计指标。
class ColumnPredicate : public std::enable_shared_from_this<ColumnPredicate> {
public:
    explicit ColumnPredicate(uint32_t column_id, std::string col_name, PrimitiveType primitive_type,
                             bool opposite = false)
            : _column_id(column_id),
              _col_name(col_name),
              _primitive_type(primitive_type),
              _opposite(opposite) {
        reset_judge_selectivity();
    }
    // 拷贝构造函数变体。在保留原谓词所有属性的基础上，将 _column_id 替换为传入的新 col_id。
    ColumnPredicate(const ColumnPredicate& other, uint32_t col_id) : ColumnPredicate(other) {
        _column_id = col_id;
    }

    virtual ~ColumnPredicate() = default;
    // 返回当前谓词的具体类型枚举值（如 PredicateType::EQ, PredicateType::IN_LIST 等）。
    virtual PredicateType type() const = 0;
    // 获取当前谓词绑定的列原始数据类型 _primitive_type。
    virtual PrimitiveType primitive_type() const { return _primitive_type; }
    // 深拷贝当前谓词，并将其绑定到新的 col_id 上。
    virtual std::shared_ptr<ColumnPredicate> clone(uint32_t col_id) const = 0;

    //evaluate predicate on inverted
    // 利用倒排索引（Inverted Index）查询匹配的行号集合，将行号写入 bitmap 中。
    virtual Status evaluate(const IndexFieldNameAndTypePair& name_with_type,
                            IndexIterator* iterator, uint32_t num_rows,
                            roaring::Roaring* bitmap) const {
        return Status::NotSupported(
                "Not Implemented evaluate with inverted index, please check the predicate");
    }
    // 获取忽略当前谓词的选择率阈值（默认为 0）。
    // 当过滤掉的行数占比低于此阈值时，该 Runtime Filter 将被动态忽略。
    virtual double get_ignore_threshold() const { return 0; }

    // Return the size of value set for IN/NOT IN predicates and 0 for others.
    virtual std::string debug_string() const {
        fmt::memory_buffer debug_string_buffer;
        fmt::format_to(debug_string_buffer,
                       "Column ID: {}, Data Type: {}, PredicateType: {}, opposite: {}, Runtime "
                       "Filter ID: {}",
                       _column_id, type_to_string(primitive_type()), pred_type_string(type()),
                       _opposite, _runtime_filter_id);
        return fmt::to_string(debug_string_buffer);
    }

    // evaluate predicate on IColumn
    // a short circuit eval way
    // 选择向量过滤求值入口。
    // const IColumn& column  待评估的向量化列数据对象（Doris 内存存储列，如 ColumnInt32、ColumnString 等）。作用：提供该列在内存中的真实物理数据，供谓词进行逻辑比较。
    // uint16_t* sel 选择向量数组指针（Selection Vector）。作用：传入时，它包含了当前待处理的行号索引数组（例如 [0, 1, 2, 4, 5]）；函数执行完毕后，它会被原地覆盖写回，仅保留过滤后满足谓词条件的行号（例如 [1, 5]）。
    // uint16_t size 含义：输入的待处理行号数量（即 sel 数组的初始长度）。
    // 返回值 uint16_t 含义：通过谓词筛选后保留下来的合格行数（即写回后的 sel 数组的新长度 new_size）。
    uint16_t evaluate(const IColumn& column, uint16_t* sel, uint16_t size) const {
        // 1. 周期性采样状态维护（RAII 清理）
        Defer defer([&] { try_reset_judge_selectivity(); });
        // 作用：判断当前谓词是否已被系统标记为“恒真/可忽略”（即该 Runtime Filter 过滤效果极差，已被动态钝化）。
        if (always_true()) {
            update_filter_info(0, 0, size);
            return size;
        }
        // 调用派生类实现的具体计算逻辑。
        uint16_t new_size = _evaluate_inner(column, sel, size);
        // 判断当前谓词是否支持钝化（通常要求当前谓词是由 Runtime Filter 生成的，即 _runtime_filter_id != -1）。
        if (_can_ignore()) {
            // 传入本次评估过滤掉的行数（size - new_size）和输入的总行数（size）。内部会累加统计，并计算过滤率；如果过滤率低于设定阈值且数据量足够，会将 _always_true 置为 true，供未来的计算直接触发步骤 2 的短路。
            do_judge_selectivity(size - new_size, size);
        }
        update_filter_info(size - new_size, size, 0);
        return new_size;
    }
    // 在选定的行上执行 与（AND） 过滤，将结果更新至 flags 数组。
    // const IColumn& column 含义：待评估的向量化列数据对象（如 ColumnInt32、ColumnString 等）。作用：提供该列在内存中的真实物理数据。
    // const uint16_t* sel 含义：选择向量数组指针（Selection Vector）。作用：包含了当前待评估的数据行号索引（例如 [0, 2, 5]）。与 evaluate(...) 不同的是，这里的 sel 数组是 const 的，不会在函数内部被重写修改。
    // uint16_t size 含义：待评估的行数（即 sel 数组的长度）。
    // bool* flags 含义：布尔结果标记数组指针（输出/更新参数）。作用：存储每一行之前的过滤结果。函数内部会对每一行计算当前谓词的结果，并以 “逻辑与” 的方式更新该数组 $$\text{flags}[i] = \text{flags}[i] \text{ \&\& (row } sel[i] \text{ 满足当前谓词)}$$
    virtual void evaluate_and(const IColumn& column, const uint16_t* sel, uint16_t size,
                              bool* flags) const {}
    // 在选定的行上执行 或（OR） 过滤，将结果更新至 flags 数组。
    virtual void evaluate_or(const IColumn& column, const uint16_t* sel, uint16_t size,
                             bool* flags) const {}
    // 返回当前谓词是否支持 ZoneMap 索引剪枝（默认返回 true）。
    virtual bool support_zonemap() const { return true; }
    // 利用 ZoneMap 的 Max/Min/HasNull 信息进行区间交集判断。
    // 若数据块可能包含满足条件的行返回 true，否则返回 false（剪枝）。
    virtual bool evaluate_and(const segment_v2::ZoneMap& zone_map) const { return true; }
    // 判断在当前 ZoneMap 范围内，该谓词是否恒成立（即 100% 的行都满足）。
    // 若恒成立，存储层可将该列从过滤列表中移除，甚至跳过数据页读取（No-Need-Read）。
    virtual bool is_always_true(const segment_v2::ZoneMap& zone_map) const { return false; }
    // 判断删除条件谓词是否匹配整个 ZoneMap 区间（默认返回 false）
    virtual bool evaluate_del(const segment_v2::ZoneMap& zone_map) const { return false; }
    // 评估外表 Parquet 的 Block-Split 布隆过滤器（默认返回 true）。
    virtual bool evaluate_and(const ParquetBlockSplitBloomFilter* bf) const { return true; }
    // 评估 Doris 内置 Segment 的 BloomFilter 索引。不命中时返回 false（剪枝）。
    virtual bool evaluate_and(const BloomFilter* bf) const { return true; }
    // 针对字典编码列（Dict Encoding），用当前谓词检索字典中的词条。若字典中无一匹配，则直接返回 false 跳过数据页。
    virtual bool evaluate_and(const StringRef* dict_words, const size_t dict_count) const {
        return true;
    }
    // 查询当前谓词是否能够下推并使用 BloomFilter/N-Gram BloomFilter 加速（默认返回 false）。
    virtual bool can_do_bloom_filter(bool ngram) const { return false; }

    /**
     * Figure out whether this page is matched partially or completely.
     */
    // Parquet 文件列统计信息剪枝接口（未重写前抛出异常）。
    virtual bool evaluate_and(ParquetPredicate::ColumnStat* statistic) const {
        throw Exception(ErrorCode::INTERNAL_ERROR,
                        "ParquetPredicate is not supported by this predicate!");
        return true;
    }
    // Parquet Page Index 页级剪枝接口（未重写前抛出异常）。
    virtual bool evaluate_and(ParquetPredicate::CachedPageIndexStat* statistic,
                              RowRanges* row_ranges) const {
        throw Exception(ErrorCode::INTERNAL_ERROR,
                        "ParquetPredicate is not supported by this predicate!");
        return true;
    }

    // used to evaluate pre read column in lazy materialization
    // now only support integer/float
    // a vectorized eval way
    // 全量向量化求值（常用于延迟物化/预读列评估）。将每行的过滤结果直接写入 flags 数组。
    virtual void evaluate_vec(const IColumn& column, uint16_t size, bool* flags) const {
        DCHECK(false) << "should not reach here";
    }
    // 全量向量化与（AND）求值。将每行的过滤结果与 flags 原有的值做逻辑与。
    virtual void evaluate_and_vec(const IColumn& column, uint16_t size, bool* flags) const {
        DCHECK(false) << "should not reach here";
    }
    // 获取文本检索/模糊匹配谓词（如 MATCH 或 LIKE）的搜索关键词（基类默认触发 DCHECK(false)）。
    virtual std::string get_search_str() const {
        DCHECK(false) << "should not reach here";
        return "";
    }
    // 设置数据页级别的 N-Gram 布隆过滤器对象（基类默认触发 DCHECK(false)）。
    virtual void set_page_ng_bf(std::unique_ptr<segment_v2::BloomFilter>) {
        DCHECK(false) << "should not reach here";
    }
    // ：获取当前谓词对应的列 ID（_column_id）。
    uint32_t column_id() const { return _column_id; }
    // 获取当前谓词对应的列名称（_col_name）。
    std::string col_name() const { return _col_name; }
    // 获取取反标识 _opposite（是否取反）。
    bool opposite() const { return _opposite; }

    void attach_profile_counter(
            int filter_id, std::shared_ptr<RuntimeProfile::Counter> predicate_filtered_rows_counter,
            std::shared_ptr<RuntimeProfile::Counter> predicate_input_rows_counter,
            std::shared_ptr<RuntimeProfile::Counter> predicate_always_true_rows_counter,
            const RuntimeFilterSelectivity& rf_selectivity) {
        _runtime_filter_id = filter_id;
        _rf_selectivity = rf_selectivity;
        DCHECK(predicate_filtered_rows_counter != nullptr);
        DCHECK(predicate_input_rows_counter != nullptr);

        if (predicate_filtered_rows_counter != nullptr) {
            _predicate_filtered_rows_counter = predicate_filtered_rows_counter;
        }
        if (predicate_input_rows_counter != nullptr) {
            _predicate_input_rows_counter = predicate_input_rows_counter;
        }
        if (predicate_always_true_rows_counter != nullptr) {
            _predicate_always_true_rows_counter = predicate_always_true_rows_counter;
        }
    }

    /// TODO: Currently we only record statistics for runtime filters, in the future we should record for all predicates
    void update_filter_info(int64_t filter_rows, int64_t input_rows,
                            int64_t always_true_rows) const {
        if (_predicate_input_rows_counter == nullptr ||
            _predicate_filtered_rows_counter == nullptr ||
            _predicate_always_true_rows_counter == nullptr) {
            throw Exception(INTERNAL_ERROR, "Predicate profile counters are not initialized");
        }
        COUNTER_UPDATE(_predicate_input_rows_counter, input_rows);
        COUNTER_UPDATE(_predicate_filtered_rows_counter, filter_rows);
        COUNTER_UPDATE(_predicate_always_true_rows_counter, always_true_rows);
    }
    // 将 PredicateType 枚举转换为对应的字符串文本（如 "eq", "in", "is_null" 等）。
    static std::string pred_type_string(PredicateType type) {
        switch (type) {
        case PredicateType::EQ:
            return "eq";
        case PredicateType::NE:
            return "ne";
        case PredicateType::LT:
            return "lt";
        case PredicateType::LE:
            return "le";
        case PredicateType::GT:
            return "gt";
        case PredicateType::GE:
            return "ge";
        case PredicateType::IN_LIST:
            return "in";
        case PredicateType::NOT_IN_LIST:
            return "not_in";
        case PredicateType::IS_NULL:
            return "is_null";
        case PredicateType::IS_NOT_NULL:
            return "is_not_null";
        case PredicateType::BF:
            return "bf";
        case PredicateType::MATCH:
            return "match";
        default:
            return "unknown";
        }
    }

    bool always_true() const { return _rf_selectivity.maybe_always_true_can_ignore(); }
    // Return whether the ColumnPredicate was created by a runtime filter.
    // If true, it was definitely created by a runtime filter.
    // If false, it may still have been created by a runtime filter,
    // as certain filters like "in filter" generate key ranges instead of ColumnPredicate.
    virtual bool is_runtime_filter() const { return _can_ignore(); }

protected:
    virtual bool _can_ignore() const { return _runtime_filter_id != -1; }
    // 由派生类实现具体的列过滤算法。若派生类未重写直接调用则抛出 INTERNAL_ERROR 异常。
    virtual uint16_t _evaluate_inner(const IColumn& column, uint16_t* sel, uint16_t size) const {
        throw Exception(INTERNAL_ERROR, "Not Implemented _evaluate_inner");
    }

    void reset_judge_selectivity() const { _rf_selectivity.reset_judge_selectivity(); }

    void try_reset_judge_selectivity() const {
        if (_can_ignore()) {
            _rf_selectivity.update_judge_counter();
        }
    }

    void do_judge_selectivity(uint64_t filter_rows, uint64_t input_rows) const {
        _rf_selectivity.update_judge_selectivity(_runtime_filter_id, filter_rows, input_rows,
                                                 get_ignore_threshold());
    }
    // 当前谓词作用的列 ID（物理列索引）。
    uint32_t _column_id;
    // 当前谓词作用的列名称。
    const std::string _col_name;
    // 当前列的底层原始数据类型（如 TYPE_INT, TYPE_VARCHAR 等）。
    PrimitiveType _primitive_type;
    // TODO: the value is only in delete condition, better be template value
    // 取反标识。
    // 用于表示逻辑上的非关系（例如删除条件下推时的反向条件，如把 a = 1 取反为 a != 1）。
    bool _opposite;
    // 对应的 Runtime Filter ID。若不属于 Runtime Filter 生成的谓词，默认值为 -1。
    int _runtime_filter_id = -1;
    // RuntimeFilterExpr and ColumnPredicate share the same logic,
    // but it's challenging to unify them, so the code is duplicated.
    // _judge_counter, _judge_input_rows, _judge_filter_rows, and _always_true
    // are variables used to implement the _always_true logic, calculated periodically
    // based on runtime_filter_sampling_frequency. During each period, if _always_true
    // is evaluated as true, the logic for always_true is applied for the rest of that period
    // without recalculating. At the beginning of the next period,
    // reset_judge_selectivity is used to reset these variables.
    // Runtime Filter 选择率计算器。
    // 以采样周期为单位，记录输入行数与过滤行数；若判定过滤效率极低，会动态将其标记为 always_true 以禁用该谓词。
    mutable RuntimeFilterSelectivity _rf_selectivity;
    // Profile 计数器，记录被该谓词过滤掉的行数。
    std::shared_ptr<RuntimeProfile::Counter> _predicate_filtered_rows_counter =
            std::make_shared<RuntimeProfile::Counter>(TUnit::UNIT, 0);
    // Profile 计数器，记录输入给该谓词评估的总行数。
    std::shared_ptr<RuntimeProfile::Counter> _predicate_input_rows_counter =
            std::make_shared<RuntimeProfile::Counter>(TUnit::UNIT, 0);
    // Profile 计数器，记录因开启 always_true 短路优化而直接跳过过滤的行数。
    std::shared_ptr<RuntimeProfile::Counter> _predicate_always_true_rows_counter =
            std::make_shared<RuntimeProfile::Counter>(TUnit::UNIT, 0);

private:
    ColumnPredicate(const ColumnPredicate& other) = default;
};

} //namespace doris
