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

#include <cstdint>
#include <type_traits>

#include "common/compare.h"
#include "core/column/column_dictionary.h"
#include "core/column/column_execute_util.h"
#include "core/column/column_string.h"
#include "core/field.h"
#include "storage/index/bloom_filter/bloom_filter.h"
#include "storage/index/inverted/inverted_index_cache.h" // IWYU pragma: keep
#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/predicate/column_predicate.h"

namespace doris {
// ComparisonPredicateBase 是基于矢量化执行引擎（Vectorized Engine）实现的通用二元比较谓词模板基类（用于处理 col op value 形式的 SQL 过滤条件，如 a > 10、b = 'hello' 等）
// 通过 C++ 模板参数编译期特化支持了绝大部分数据类型和比较操作符，在存储层（Segment/Page 过滤）、索引层（ZoneMap、BloomFilter、倒排索引、Parquet PageIndex）以及执行层（Vectorized Execution）扮演着极其关键的角色
// 统一标量比较操作：通过模板参数 PrimitiveType Type（数据类型，如 INT, VARCHAR, DATE 等）和 PredicateType PT（谓词类型，如 EQ, NE, LT, LE, GT, GE），统一实现具体的比较逻辑。
// 多层级谓词下推与下剪枝（Pruning）：
// ZoneMap 过滤：利用数据块的 [min_value, max_value] 快速判断当前 Data Page / Segment 是否可以整体跳过。
// Bloom Filter 过滤：针对 EQ 谓词提取数值或 Byte 序列进行哈希碰撞测试，跳过肯定不存在的数据块。
// 倒排索引（Inverted Index）求值：将谓词转换为倒排索引查询（如 LESS_THAN_QUERY），计算出匹配的 Bitmap。
// Parquet 格式下推：支持 Parquet 文件的 Statistics、PageIndex 和 Block Split Bloom Filter 过滤。
// 低基数字典编码优化（Low Cardinality Optimization）：对 String 类型的字典编码列（ColumnDictI32），直接将谓词值转换为字典 Code（dict_code），后续的数据比较退化为简单的整数 Code 比较，并使用并发 Hash Map 缓存该 Code，大幅提升字符串比较性能。
// 向量化评估（Vectorized Evaluation）：提供基于内存连续 Column 数组的批量评估函数（如 evaluate_vec），充分利用 CPU SIMD 矢量化指令。
template <PrimitiveType Type, PredicateType PT>
class ComparisonPredicateBase final : public ColumnPredicate {
public:
    ENABLE_FACTORY_CREATOR(ComparisonPredicateBase);
    using T = typename PrimitiveTypeTraits<Type>::CppType;
    //  常规构造函数。初始化列 ID、列名、数据类型 Type、是否取反标志 opposite，并将传入的通用 Field 对象转换为内部强类型的 _value。
    ComparisonPredicateBase(uint32_t column_id, std::string col_name, const Field& value,
                            bool opposite = false)
            : ColumnPredicate(column_id, col_name, Type, opposite),
              _value(value.template get<Type>()) {}
    // 拷贝构造函数（带有重新指定 col_id 的能力）。用于为新的列 ID 复制一份谓词实例，同时复制 _value。
    ComparisonPredicateBase(const ComparisonPredicateBase<Type, PT>& other, uint32_t col_id)
            : ColumnPredicate(other, col_id), _value(other._value) {}

    ComparisonPredicateBase(const ComparisonPredicateBase<Type, PT>& other) = delete;
	// 深拷贝当前谓词对象
	// 在 Pipeline 并行执行或表达式重构时使用。
	// 这里带有 DCHECK(_segment_id_to_cached_code.empty()) 校验，确保克隆前缓存映射为空，避免跨上下文共享字典 Code 导致并发冲突或错乱
    std::shared_ptr<ColumnPredicate> clone(uint32_t col_id) const override {
        DCHECK(_segment_id_to_cached_code.empty());
        return ComparisonPredicateBase<Type, PT>::create_shared(*this, col_id);
    }

    std::string debug_string() const override {
        fmt::memory_buffer debug_string_buffer;
        fmt::format_to(debug_string_buffer, "ComparisonPredicateBase({})",
                       ColumnPredicate::debug_string());
        return fmt::to_string(debug_string_buffer);
    }

    // 返回当前谓词的具体类型枚举 PredicateType（即模板参数 PT，如 EQ, LT 等）。
    PredicateType type() const override { return PT; }
    // 负责在存储层利用倒排索引（Inverted Index）快速对列谓词进行评估，通过按位图（Roaring Bitmap）计算跳过不符合条件的数据行。
    Status evaluate(const IndexFieldNameAndTypePair& name_with_type, IndexIterator* iterator,
                    uint32_t num_rows, roaring::Roaring* bitmap) const override {
        // 第一阶段：校验倒排索引 Reader 可用性
        if (iterator == nullptr) {
            return Status::Error<ErrorCode::INVERTED_INDEX_EVALUATE_SKIPPED>(
                    "Inverted index evaluate skipped, no inverted index reader can not support "
                    "comparison predicate");
        }
        // 类型支持校验：判断底层 Index Reader 是否支持 STRING_TYPE 索引或 BKD 树索引（数值/日期等范围查询索引）。
        // 若均不支持，则返回 INVERTED_INDEX_EVALUATE_SKIPPED 错误码，执行引擎会退化为扫描解压数据页进行逐行评估。
        if (iterator->get_reader(segment_v2::InvertedIndexReaderType::STRING_TYPE) == nullptr &&
            iterator->get_reader(segment_v2::InvertedIndexReaderType::BKD) == nullptr) {
            return Status::Error<ErrorCode::INVERTED_INDEX_EVALUATE_SKIPPED>(
                    "Inverted index evaluate skipped, no inverted index reader can not support "
                    "comparison predicate");
        }
        // 第二阶段：谓词类型（PredicateType）到查询类型转换
        // 根据编译期模板参数 PT，将 SQL 比较谓词映射为倒排索引引擎可识别的 InvertedIndexQueryType：
        InvertedIndexQueryType query_type = InvertedIndexQueryType::UNKNOWN_QUERY;
        switch (PT) {
        case PredicateType::EQ:
            query_type = InvertedIndexQueryType::EQUAL_QUERY;
            break;
        case PredicateType::NE:
            query_type = InvertedIndexQueryType::EQUAL_QUERY;
            break;
        case PredicateType::LT:
            query_type = InvertedIndexQueryType::LESS_THAN_QUERY;
            break;
        case PredicateType::LE:
            query_type = InvertedIndexQueryType::LESS_EQUAL_QUERY;
            break;
        case PredicateType::GT:
            query_type = InvertedIndexQueryType::GREATER_THAN_QUERY;
            break;
        case PredicateType::GE:
            query_type = InvertedIndexQueryType::GREATER_EQUAL_QUERY;
            break;
        default:
            return Status::InvalidArgument("invalid comparison predicate type {}", PT);
        }
        // 第三阶段：构造参数并查询索引
        // 打包参数：构造 InvertedIndexParam 结构体，包括列名、列类型、
        // 强类型的查询右值 _value（包装为 Field）、查询类型以及存放结果的 std::make_shared<roaring::Roaring>()。
        InvertedIndexParam param;
        param.column_name = name_with_type.first;
        param.column_type = name_with_type.second;
        param.query_value = Field::create_field<Type>(_value);
        param.query_type = query_type;
        param.num_rows = num_rows;
        param.roaring = std::make_shared<roaring::Roaring>();
        // 读索引：调用 iterator->read_from_index(...) 检索倒排索引/BKD 树。执行完毕后，匹配谓词条件的行号位图会写入 param.roaring 中。
        RETURN_IF_ERROR(iterator->read_from_index(segment_v2::IndexParam {&param}));

        // mask out null_bitmap, since NULL cmp VALUE will produce NULL
        //  and be treated as false in WHERE
        // keep it after query, since query will try to read null_bitmap and put it to cache
        // 第四阶段：NULL 值屏蔽（Mask Out NULLs）
        // SQL 语义保障：SQL 规范中 NULL <cmp> VALUE 的结果为 UNKNOWN（在 WHERE 子句中视同 false）。
        if (iterator->has_null()) {
            InvertedIndexQueryCacheHandle null_bitmap_cache_handle;
            // 若列中存在 NULL 值（iterator->has_null() 为 true），读取该 Segment 的 null_bitmap（带 Cache 机制）。
            RETURN_IF_ERROR(iterator->read_null_bitmap(&null_bitmap_cache_handle));
            std::shared_ptr<roaring::Roaring> null_bitmap = null_bitmap_cache_handle.get_bitmap();
            // 将传入的候选位图 bitmap 减去 null_bitmap（*bitmap -= *null_bitmap），剔除所有包含 NULL 的行。
            if (null_bitmap) {
                *bitmap -= *null_bitmap;
            }
        }
        // 第五阶段：位图集合运算与输出
        // PredicateType::NE（不等于）：
        // 采用取反求交集的思想：*bitmap -= *param.roaring（从传入的候选行号集中扣除等于 _value 的行号）。
        if constexpr (PT == PredicateType::NE) {
            *bitmap -= *param.roaring;
        } else {
        // 其他谓词（EQ, LT, LE, GT, GE）：
        // 直接求交集：*bitmap &= *param.roaring（仅保留满足条件的行号）。
            *bitmap &= *param.roaring;
        }

        return Status::OK();
    }

    void evaluate_and(const IColumn& column, const uint16_t* sel, uint16_t size,
                      bool* flags) const override {
        _evaluate_bit<true>(column, sel, size, flags);
    }

    bool evaluate_and(const segment_v2::ZoneMap& zone_map) const override {
        if (!zone_map.has_not_null) {
            return false;
        }

        if constexpr (PT == PredicateType::EQ) {
            return _operator(
                    Compare::less_equal(zone_map.min_value.template get<Type>(), _value) &&
                            Compare::greater_equal(zone_map.max_value.template get<Type>(), _value),
                    true);
        } else if constexpr (PT == PredicateType::NE) {
            return _operator(
                    Compare::equal(zone_map.min_value.template get<Type>(), _value) &&
                            Compare::equal(zone_map.max_value.template get<Type>(), _value),
                    true);
        } else if constexpr (PT == PredicateType::LT || PT == PredicateType::LE) {
            return _operator(zone_map.min_value.template get<Type>(), _value);
        } else {
            static_assert(PT == PredicateType::GT || PT == PredicateType::GE);
            return _operator(zone_map.max_value.template get<Type>(), _value);
        }
    }

    /**
     * To figure out whether this page is matched partially or completely.
     *
     * 1. EQ: if `_value` belongs to the interval [min, max], return true to further compute each value in this page.
     * 2. NE: return true to further compute each value in this page if some values not equal to `_value`.
     * 3. LT|LE: if `_value` is greater than min, return true to further compute each value in this page.
     * 4. GT|GE: if `_value` is less than max, return true to further compute each value in this page.
     */

    bool camp_field(const Field& min_field, const Field& max_field) const {
        T min_value = min_field.template get<Type>();
        T max_value = max_field.template get<Type>();

        if constexpr (PT == PredicateType::EQ) {
            return Compare::less_equal(min_value, _value) &&
                   Compare::greater_equal(max_value, _value);
        } else if constexpr (PT == PredicateType::NE) {
            return !Compare::equal(min_value, _value) || !Compare::equal(max_value, _value);
        } else if constexpr (PT == PredicateType::LT || PT == PredicateType::LE) {
            return Compare::less_equal(min_value, _value);
        } else {
            static_assert(PT == PredicateType::GT || PT == PredicateType::GE);
            return Compare::greater_equal(max_value, _value);
        }
    }

    bool evaluate_and(ParquetPredicate::ColumnStat* statistic) const override {
        bool result = true;
        if ((*statistic->get_stat_func)(statistic, column_id())) {
            Field min_field;
            Field max_field;
            if (statistic->is_all_null) {
                result = false;
            } else if (!ParquetPredicate::parse_min_max_value(
                                statistic->col_schema, statistic->encoded_min_value,
                                statistic->encoded_max_value, *statistic->ctz, &min_field,
                                &max_field)
                                .ok()) [[unlikely]] {
                result = true;
            } else {
                result = camp_field(min_field, max_field);
            }
        }

        if constexpr (PT == PredicateType::EQ) {
            if (result && statistic->get_bloom_filter_func != nullptr &&
                (*statistic->get_bloom_filter_func)(statistic, column_id())) {
                if (!statistic->bloom_filter) {
                    return result;
                }
                return evaluate_and(statistic->bloom_filter.get());
            }
        }
        return result;
    }

    bool evaluate_and(ParquetPredicate::CachedPageIndexStat* statistic,
                      RowRanges* row_ranges) const override {
        ParquetPredicate::PageIndexStat* stat = nullptr;
        if (!(statistic->get_stat_func)(&stat, column_id())) {
            row_ranges->add(statistic->row_group_range);
            return true;
        }

        for (int page_id = 0; page_id < stat->num_of_pages; page_id++) {
            if (stat->is_all_null[page_id]) {
                // all null page, not need read.
                continue;
            }

            Field min_field;
            Field max_field;
            if (!ParquetPredicate::parse_min_max_value(
                         stat->col_schema, stat->encoded_min_value[page_id],
                         stat->encoded_max_value[page_id], *statistic->ctz, &min_field, &max_field)
                         .ok()) [[unlikely]] {
                row_ranges->add(stat->ranges[page_id]);
                continue;
            };

            if (camp_field(min_field, max_field)) {
                row_ranges->add(stat->ranges[page_id]);
            }
        };
        return row_ranges->count() > 0;
    }

    bool is_always_true(const segment_v2::ZoneMap& zone_map) const override {
        if (zone_map.has_null) {
            return false;
        }

        if constexpr (PT == PredicateType::LT) {
            return _value > zone_map.max_value.template get<Type>();
        } else if constexpr (PT == PredicateType::LE) {
            return _value >= zone_map.max_value.template get<Type>();
        } else if constexpr (PT == PredicateType::GT) {
            return _value < zone_map.min_value.template get<Type>();
        } else if constexpr (PT == PredicateType::GE) {
            return _value <= zone_map.min_value.template get<Type>();
        }

        return false;
    }

    bool evaluate_del(const segment_v2::ZoneMap& zone_map) const override {
        if (zone_map.has_null) {
            return false;
        }
        if constexpr (PT == PredicateType::EQ) {
            return zone_map.min_value.template get<Type>() == _value &&
                   zone_map.max_value.template get<Type>() == _value;
        } else if constexpr (PT == PredicateType::NE) {
            return zone_map.min_value.template get<Type>() > _value ||
                   zone_map.max_value.template get<Type>() < _value;
        } else if constexpr (PT == PredicateType::LT || PT == PredicateType::LE) {
            return _operator(zone_map.max_value.template get<Type>(), _value);
        } else {
            static_assert(PT == PredicateType::GT || PT == PredicateType::GE);
            return _operator(zone_map.min_value.template get<Type>(), _value);
        }
    }

    bool evaluate_and(const segment_v2::BloomFilter* bf) const override {
        if constexpr (PT == PredicateType::EQ) {
            // EQ predicate can not use ngram bf, just return true to accept
            if (bf->is_ngram_bf()) {
                return true;
            }
            if constexpr (Type == TYPE_CHAR) {
                // CHAR BFs hash zero-padded bytes while the predicate value is
                // unpadded, so probing the BF would always miss. Skip BF
                // pruning for CHAR entirely and let the scan filter the rows.
                return true;
            } else if constexpr (is_string_type(Type)) {
                return bf->test_bytes(_value.data(), _value.size());
            } else {
                // DecimalV2 using decimal12_t in bloom filter, should convert value to decimal12_t
                if constexpr (Type == PrimitiveType::TYPE_DECIMALV2) {
                    decimal12_t decimal12_t_val(_value.int_value(), _value.frac_value());
                    return bf->test_bytes(reinterpret_cast<const char*>(&decimal12_t_val),
                                          sizeof(decimal12_t));
                    // Datev1 using uint24_t in bloom filter
                } else if constexpr (Type == PrimitiveType::TYPE_DATE) {
                    uint24_t date_value(uint32_t(_value.to_olap_date()));
                    return bf->test_bytes(reinterpret_cast<const char*>(&date_value),
                                          sizeof(uint24_t));
                    // DatetimeV1 using int64_t in bloom filter
                } else if constexpr (Type == PrimitiveType::TYPE_DATETIME) {
                    int64_t datetime_value(_value.to_olap_datetime());
                    return bf->test_bytes(reinterpret_cast<const char*>(&datetime_value),
                                          sizeof(int64_t));
                } else {
                    return bf->test_bytes(reinterpret_cast<const char*>(&_value), sizeof(T));
                }
            }
        } else {
            LOG(FATAL) << "Bloom filter is not supported by predicate type.";
            return true;
        }
    }

    bool evaluate_and(const StringRef* dict_words, const size_t count) const override {
        if constexpr (is_string_type(Type)) {
            for (size_t i = 0; i != count; ++i) {
                if (_operator(dict_words[i], _value) ^ _opposite) {
                    return true;
                }
            }
            return false;
        }

        return true;
    }

    bool can_do_bloom_filter(bool ngram) const override {
        return PT == PredicateType::EQ && !ngram;
    }

    bool evaluate_and(const ParquetBlockSplitBloomFilter* bf) const override {
        if constexpr (PT == PredicateType::EQ) {
            auto test_bytes = [&]<typename V>(const V& value) {
                return bf->test_bytes(const_cast<char*>(reinterpret_cast<const char*>(&value)),
                                      sizeof(V));
            };

            // Only support Parquet native types where physical == logical representation
            // BOOLEAN -> hash as int32 (Parquet bool stored as int32)
            if constexpr (Type == PrimitiveType::TYPE_BOOLEAN) {
                int32_t int32_value = static_cast<int32_t>(_value);
                return test_bytes(int32_value);
            } else if constexpr (Type == PrimitiveType::TYPE_INT) {
                // INT -> hash as int32
                return test_bytes(_value);
            } else if constexpr (Type == PrimitiveType::TYPE_BIGINT) {
                // BIGINT -> hash as int64
                return test_bytes(_value);
            } else if constexpr (Type == PrimitiveType::TYPE_FLOAT) {
                // FLOAT -> hash as float
                return test_bytes(_value);
            } else if constexpr (Type == PrimitiveType::TYPE_DOUBLE) {
                // DOUBLE -> hash as double
                return test_bytes(_value);
            } else if constexpr (is_string_type(Type)) {
                // VARCHAR/STRING -> hash bytes
                return bf->test_bytes(_value.data(), _value.size());
            } else {
                // Unsupported types: return true (accept)
                return true;
            }
        } else {
            LOG(FATAL) << "Bloom filter is not supported by predicate type.";
            return true;
        }
    }

    void evaluate_or(const IColumn& column, const uint16_t* sel, uint16_t size,
                     bool* flags) const override {
        _evaluate_bit<false>(column, sel, size, flags);
    }

    template <bool is_and>
    void __attribute__((flatten))
    _evaluate_vec_internal(const IColumn& column, uint16_t size, bool* flags) const {
        uint16_t current_evaluated_rows = 0;
        uint16_t current_passed_rows = 0;
        if (_can_ignore()) {
            if (is_and) {
                for (uint16_t i = 0; i < size; i++) {
                    current_evaluated_rows += flags[i];
                }
            } else {
                current_evaluated_rows += size;
            }
        }

        // defer is created after its reference args are created.
        // so defer will be destroyed BEFORE the reference args.
        // so reference here is safe.
        // https://stackoverflow.com/questions/14688285/c-local-variable-destruction-order
        Defer defer([&]() {
            update_filter_info(current_evaluated_rows - current_passed_rows, current_evaluated_rows,
                               0);
            try_reset_judge_selectivity();
        });

        if (is_column_nullable(column)) {
            const auto* nullable_column_ptr = assert_cast<const ColumnNullable*>(&column);
            const auto& nested_column = nullable_column_ptr->get_nested_column();
            const auto& null_map = nullable_column_ptr->get_null_map_column().get_data();

            if (nested_column.is_column_dictionary()) {
                if constexpr (is_string_type(Type)) {
                    const auto* dict_column_ptr = assert_cast<const ColumnDictI32*>(&nested_column);

                    auto dict_code = _find_code_from_dictionary_column(*dict_column_ptr);
                    do {
                        if constexpr (PT == PredicateType::EQ) {
                            if (dict_code == -2) {
                                memset(flags, 0, size);
                                break;
                            }
                        }
                        const auto* data_array = dict_column_ptr->get_data().data();

                        _base_loop_vec<true, is_and>(size, flags, null_map.data(), data_array,
                                                     dict_code);
                    } while (false);
                } else {
                    LOG(FATAL) << "column_dictionary must use StringRef predicate.";
                    __builtin_unreachable();
                }
            } else {
                ColumnElementView<Type> data_array {nested_column};
                _base_loop_vec<true, is_and>(size, flags, null_map.data(), data_array, _value);
            }
        } else {
            if (column.is_column_dictionary()) {
                if constexpr (is_string_type(Type)) {
                    const auto* dict_column_ptr = assert_cast<const ColumnDictI32*>(&column);
                    auto dict_code = _find_code_from_dictionary_column(*dict_column_ptr);
                    do {
                        if constexpr (PT == PredicateType::EQ) {
                            if (dict_code == -2) {
                                memset(flags, 0, size);
                                break;
                            }
                        }
                        const auto* data_array = dict_column_ptr->get_data().data();

                        _base_loop_vec<false, is_and>(size, flags, nullptr, data_array, dict_code);
                    } while (false);
                } else {
                    LOG(FATAL) << "column_dictionary must use StringRef predicate.";
                    __builtin_unreachable();
                }
            } else {
                ColumnElementView<Type> data_array {column};
                _base_loop_vec<false, is_and>(size, flags, nullptr, data_array, _value);
            }
        }

        if (_opposite) {
            for (uint16_t i = 0; i < size; i++) {
                flags[i] = !flags[i];
            }
        }

        if (_can_ignore()) {
            for (uint16_t i = 0; i < size; i++) {
                current_passed_rows += flags[i];
            }
            do_judge_selectivity(current_evaluated_rows - current_passed_rows,
                                 current_evaluated_rows);
        }
    }

    void evaluate_vec(const IColumn& column, uint16_t size, bool* flags) const override {
        _evaluate_vec_internal<false>(column, size, flags);
    }

    void evaluate_and_vec(const IColumn& column, uint16_t size, bool* flags) const override {
        _evaluate_vec_internal<true>(column, size, flags);
    }

    double get_ignore_threshold() const override { return get_comparison_ignore_thredhold(); }

private:
    uint16_t _evaluate_inner(const IColumn& column, uint16_t* sel, uint16_t size) const override {
        if (is_column_nullable(column)) {
            const auto* nullable_column_ptr = assert_cast<const ColumnNullable*>(&column);
            const auto& nested_column = nullable_column_ptr->get_nested_column();
            const auto& null_map = nullable_column_ptr->get_null_map_column().get_data();

            return _base_evaluate<true>(&nested_column, null_map.data(), sel, size);
        } else {
            return _base_evaluate<false>(&column, nullptr, sel, size);
        }
    }

    template <typename LeftT, typename RightT>
    bool _operator(const LeftT& lhs, const RightT& rhs) const {
        if constexpr (std::is_same_v<std::string, RightT> && !std::is_same_v<LeftT, RightT>) {
            if constexpr (PT == PredicateType::EQ) {
                return Compare::equal(lhs, StringRef(rhs.data(), rhs.size()));
            } else if constexpr (PT == PredicateType::NE) {
                return Compare::not_equal(lhs, StringRef(rhs.data(), rhs.size()));
            } else if constexpr (PT == PredicateType::LT) {
                return Compare::less(lhs, StringRef(rhs.data(), rhs.size()));
            } else if constexpr (PT == PredicateType::LE) {
                return Compare::less_equal(lhs, StringRef(rhs.data(), rhs.size()));
            } else if constexpr (PT == PredicateType::GT) {
                return Compare::greater(lhs, StringRef(rhs.data(), rhs.size()));
            } else if constexpr (PT == PredicateType::GE) {
                return Compare::greater_equal(lhs, StringRef(rhs.data(), rhs.size()));
            }
        } else {
            if constexpr (PT == PredicateType::EQ) {
                return Compare::equal(lhs, rhs);
            } else if constexpr (PT == PredicateType::NE) {
                return Compare::not_equal(lhs, rhs);
            } else if constexpr (PT == PredicateType::LT) {
                return Compare::less(lhs, rhs);
            } else if constexpr (PT == PredicateType::LE) {
                return Compare::less_equal(lhs, rhs);
            } else if constexpr (PT == PredicateType::GT) {
                return Compare::greater(lhs, rhs);
            } else if constexpr (PT == PredicateType::GE) {
                return Compare::greater_equal(lhs, rhs);
            }
        }
    }

    constexpr bool _is_range() const { return PredicateTypeTraits::is_range(PT); }

    constexpr bool _is_greater() const { return _operator(1, 0); }

    constexpr bool _is_eq() const { return _operator(1, 1); }

    template <bool is_and>
    void _evaluate_bit(const IColumn& column, const uint16_t* sel, uint16_t size,
                       bool* flags) const {
        if (is_column_nullable(column)) {
            const auto* nullable_column_ptr = assert_cast<const ColumnNullable*>(&column);
            const auto& nested_column = nullable_column_ptr->get_nested_column();
            const auto& null_map = nullable_column_ptr->get_null_map_column().get_data();

            _base_evaluate_bit<true, is_and>(&nested_column, null_map.data(), sel, size, flags);
        } else {
            _base_evaluate_bit<false, is_and>(&column, nullptr, sel, size, flags);
        }
    }

    // `data_array` is raw pointer or ColumnElementView wrapper; passed by value
    // (no __restrict on struct), SIMD preserved via loop versioning.
    template <bool is_nullable, bool is_and, typename TArray, typename TValue>
    void __attribute__((flatten))
    _base_loop_vec(uint16_t size, bool* __restrict bflags, const uint8_t* __restrict null_map,
                   TArray data_array, const TValue& value) const {
        //uint8_t helps compiler to generate vectorized code
        auto* flags = reinterpret_cast<uint8_t*>(bflags);
        if constexpr (is_and) {
            for (uint16_t i = 0; i < size; i++) {
                if constexpr (is_nullable) {
                    flags[i] &= (uint8_t)(!null_map[i] && _operator(data_array[i], value));
                } else {
                    flags[i] &= (uint8_t)_operator(data_array[i], value);
                }
            }
        } else {
            for (uint16_t i = 0; i < size; i++) {
                if constexpr (is_nullable) {
                    flags[i] = !null_map[i] && _operator(data_array[i], value);
                } else {
                    flags[i] = _operator(data_array[i], value);
                }
            }
        }
    }

    template <bool is_nullable, bool is_and, typename TArray, typename TValue>
    void _base_loop_bit(const uint16_t* sel, uint16_t size, bool* flags,
                        const uint8_t* __restrict null_map, TArray data_array,
                        const TValue& value) const {
        for (uint16_t i = 0; i < size; i++) {
            if (is_and ^ flags[i]) {
                continue;
            }
            if constexpr (is_nullable) {
                if (_opposite ^ is_and ^
                    (!null_map[sel[i]] && _operator(data_array[sel[i]], value))) {
                    flags[i] = !is_and;
                }
            } else {
                if (_opposite ^ is_and ^ _operator(data_array[sel[i]], value)) {
                    flags[i] = !is_and;
                }
            }
        }
    }

    template <bool is_nullable, bool is_and>
    void _base_evaluate_bit(const IColumn* column, const uint8_t* null_map, const uint16_t* sel,
                            uint16_t size, bool* flags) const {
        if (column->is_column_dictionary()) {
            if constexpr (is_string_type(Type)) {
                const auto* dict_column_ptr = assert_cast<const ColumnDictI32*>(column);
                const auto* data_array = dict_column_ptr->get_data().data();
                auto dict_code = _find_code_from_dictionary_column(*dict_column_ptr);
                _base_loop_bit<is_nullable, is_and>(sel, size, flags, null_map, data_array,
                                                    dict_code);
            } else {
                LOG(FATAL) << "column_dictionary must use StringRef predicate.";
                __builtin_unreachable();
            }
        } else {
            ColumnElementView<Type> data_array {*column};
            _base_loop_bit<is_nullable, is_and>(sel, size, flags, null_map, data_array, _value);
        }
    }

    template <bool is_nullable>
    uint16_t _base_evaluate(const IColumn* column, const uint8_t* null_map, uint16_t* sel,
                            uint16_t size) const {
        if (column->is_column_dictionary()) {
            if constexpr (is_string_type(Type)) {
                const auto* dict_column_ptr = assert_cast<const ColumnDictI32*>(column);
                const auto& pred_col = dict_column_ptr->get_data();
                const auto* pred_col_data = pred_col.data();
                auto dict_code = _find_code_from_dictionary_column(*dict_column_ptr);

                if constexpr (PT == PredicateType::EQ) {
                    if (dict_code == -2) {
                        return _opposite ? size : 0;
                    }
                }
                uint16_t new_size = 0;
                auto with_null = [&](uint16_t idx) {
                    return _opposite ^ (!null_map[idx] && _operator(pred_col_data[idx], dict_code));
                };
                auto without_null = [&](uint16_t idx) {
                    return _opposite ^ _operator(pred_col_data[idx], dict_code);
                };
                evaluate_by_selector<is_nullable>(pred_col, size, sel, new_size, with_null,
                                                  without_null);

                return new_size;
            } else {
                LOG(FATAL) << "column_dictionary must use StringRef predicate.";
                return 0;
            }
        } else {
            uint16_t new_size = 0;
            ColumnElementView<Type> pred_col {*column};
            auto with_null = [&](uint16_t idx) {
                return _opposite ^ (!null_map[idx] && _operator(pred_col.get_element(idx), _value));
            };
            auto without_null = [&](uint16_t idx) {
                return _opposite ^ _operator(pred_col.get_element(idx), _value);
            };
            evaluate_by_selector<is_nullable>(pred_col, size, sel, new_size, with_null,
                                              without_null);
            return new_size;
        }
    }

    int32_t __attribute__((flatten))
    _find_code_from_dictionary_column(const ColumnDictI32& column) const {
        static_assert(is_string_type(Type),
                      "Only string type predicate can use dictionary column.");
        int32_t code = 0;
        if (_segment_id_to_cached_code.if_contains(
                    column.get_rowset_segment_id(),
                    [&code](const auto& pair) { code = pair.second; })) {
            return code;
        }
        code = _is_range() ? column.find_code_by_bound(StringRef(_value.data(), _value.size()),
                                                       _is_greater(), _is_eq())
                           : column.find_code(StringRef(_value.data(), _value.size()));
        // Sometimes the dict is not initialized when run comparison predicate here, for example,
        // the full page is null, then the reader will skip read, so that the dictionary is not
        // inited. The cached code is wrong during this case, because the following page maybe not
        // null, and the dict should have items in the future.
        //
        // Cached code may have problems, so that add a config here, if not opened, then
        // we will return the code and not cache it.
        if (!column.is_dict_empty() && config::enable_low_cardinality_cache_code) {
            _segment_id_to_cached_code.emplace(std::pair {column.get_rowset_segment_id(), code});
        }

        return code;
    }
    // 并发安全的哈希映射
    // 作用：针对低基数字典列（Low Cardinality）的字典编码缓存。
    // Key 为 Segment 的唯一标识（std::pair<RowsetId, uint32_t>），Value 为当前 _value 在该 Segment 字典中对应的字典编码 dict_code。
    // mutable 使得该属性可以在 const 成员函数中被修改更新。
    mutable phmap::parallel_flat_hash_map<
            std::pair<RowsetId, uint32_t>, int32_t,
            phmap::priv::hash_default_hash<std::pair<RowsetId, uint32_t>>,
            phmap::priv::hash_default_eq<std::pair<RowsetId, uint32_t>>,
            std::allocator<std::pair<const std::pair<RowsetId, uint32_t>, int32_t>>, 4,
            std::shared_mutex>
            _segment_id_to_cached_code;
    // 类型：T（通过 PrimitiveTypeTraits<Type>::CppType 推导得到的 C++ 原生类型，如 int32_t、StringRef 等）
    // 作用：存储该比较谓词右值的具体数值（即 SQL 中的常量值，如 a > 50 中的 50）。在构造函数中通过 value.template get<Type>() 解析提取。
    T _value;
};
} //namespace doris
