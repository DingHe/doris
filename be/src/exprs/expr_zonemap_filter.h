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

#include <compare>
#include <map>
#include <optional>
#include <vector>

#include "common/check.h"
#include "common/status.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/define_primitive_type.h"
#include "core/field.h"
#include "exprs/vexpr_fwd.h"
#include "storage/index/zone_map/zonemap_eval_context.h"
#include "storage/index/zone_map/zonemap_filter_result.h"

namespace doris {
class HybridSetBase;
class RuntimeState;
class TExprNode;

namespace segment_v2 {
class BloomFilter;
} // namespace segment_v2
} // namespace doris

namespace doris::expr_zonemap {

// 基于表达式的 ZoneMap 索引裁剪（Expression-based ZoneMap Pruning）、字典过滤（Dictionary Pruning） 和 布隆过滤器剪枝（Bloom Filter Pruning） 的核心评估框架。
// 在传统的列式存储引擎中，ZoneMap（区段索引，包含 min/max/has_null 等统计信息）通常只能用于简单的列与常量的比较谓词（如 a > 10）。
// expr_zonemap 模块的作用在于拓展索引剪枝的能力，使得 Doris 可以在存储层（Segment/RowGroup 级别）对更复杂的表达式谓词（如 a + 1 > 10、a IN (1, 2, 3)、is_null(a)、starts_with(a, 'foo') 等）
// 利用 ZoneMap 统计信息、字典集以及布隆过滤器（Bloom Filter）进行粗粒度的数据块过滤（Skip RowGroup / Segment）。
// 核心功能：
// 表达式解构与归一化：抽取表达式中的 Slot（列引用）和 Literal（常量字面量），处理类似于 5 < slot 和 slot > 5 的等价翻转。
// 多维索引评估：ZoneMap（区间/NULL过滤）：通过 Max/Min/Null 统计信息过滤不符合谓词的数据块。
// Dictionary（字典过滤）：对字典编码列，通过评估文件级字典项能否满足表达式来裁剪数据块。
// Bloom Filter（布隆过滤器过滤）：利用 Bloom Filter 快速排除绝对不存在的等值谓词（= 或 IN）。
// 类型安全校验：在索引评估前，严格校验 Reader Schema 的物理列类型与表达式绑定的 Slot/Literal 类型兼容性，防止类型错配导致的错误裁剪。

// 用于存储 IN 谓词（例如 col IN (1, 2, 3)）在 ZoneMap 评估前被物化（Materialized）后的 Field 集合及边界值，方便后续做 ZoneMap 范围过滤。
struct InZonemapMaterializedSet {
    // IN 列表中是否包含 NULL 值。
    bool contains_null = false;
    // 物化后的常量列表，按 Field（Doris 内存通用值对象）形式存储。
    std::vector<Field> values;
    // 物化集合中的最小值，用于快速与 ZoneMap 的 max_value 做比较。
    Field min_value;
    // 物化集合中的最大值，用于快速与 ZoneMap 的 min_value 做比较。
    Field max_value;
};

// Dictionary pruning evaluates file-level dictionary values, not row-level data. A kNoMatch result
// means no non-null dictionary entry can satisfy the expression, so the whole row group can be
// skipped only for dictionary-encoded columns whose dictionary contains all non-null values.
// 字典过滤求值上下文。提供文件级字典编码列的字典项列表，用于针对字典条目进行表达式评估。如果字典中的所有非 NULL 值都无法满足表达式（kNoMatch），且列全部为字典编码，则可以跳过整个 RowGroup。
struct DictionaryEvalContext {
    struct SlotDictionary {
        // 字典列的数据类型。
        DataTypePtr data_type;
        // 该列字典中包含的所有物理值（Field 格式）。
        std::vector<Field> values;
    };
    // 根据列的槽位索引 slot_index 获取对应的字典数据指针。若不存在则返回 nullptr。
    const SlotDictionary* slot(int slot_index) const;
    // 从 slot_index 到其对应的字典数据的映射字典。
    std::map<int, SlotDictionary> slots;
};

// Bloom-filter pruning can only disprove equality-style predicates. A kNoMatch result means every
// literal candidate required by the expression is definitely absent from the file bloom filter.
// Bloom Filter 剪枝求值上下文。传入各 Slot 对应的 Bloom Filter 指针，用于评估等值类谓词。
struct BloomFilterEvalContext {
    struct SlotBloomFilter {
        // 列数据类型。
        DataTypePtr data_type;
        // 底层 Segment 的 Bloom Filter 索引对象指针。
        const segment_v2::BloomFilter* bloom_filter = nullptr;
    };
    // 根据 slot_index 获取对应 Bloom Filter 指针。若不存在则返回 nullptr。
    const SlotBloomFilter* slot(int slot_index) const;
    // 从 slot_index 到对应 SlotBloomFilter 的映射。
    std::map<int, SlotBloomFilter> slots;
};
// 表示从一个二元表达式中提取出的“单 Slot + 单 Literal”组合的结构化表示。
struct SlotLiteral {
    // Slot ordinal in the current expression binding. It is also the key used to look up the
    // corresponding reader-schema type and zone map from ZoneMapEvalContext.
    // 当前表达式绑定的 Slot 序号（用于在 ZoneMapEvalContext 中查找该列的 ZoneMap）。
    int slot_index;
    // Type carried by the slot expression. It is kept separately because evaluation first has to
    // verify that the expression binding is compatible with the reader-schema type stored in the
    // ZoneMapEvalContext before using schema-indexed zone-map statistics.
    // Slot 表达式带有的数据类型（需要与 Reader Schema 类型做兼容性检查）。
    DataTypePtr slot_type;
    // Constant literal value paired with the slot, already materialized as a Field for zone-map
    // comparisons.
    // 已物化为 Field 对象的常量字面量值，用于 ZoneMap 比较。
    Field literal;
    // Type carried by the literal expression. It is needed with slot_type so capability checks can
    // reject incompatible slot/literal comparisons before runtime evaluation instead of evaluating
    // zone-map ranges with mismatched Field types.
    // 字面量表达式的数据类型（用于在运行时比较前，拒绝类型不兼容的比较）。
    DataTypePtr literal_type;
    // Whether the original expression shape is literal <op> slot instead of slot <op> literal. The
    // comparison operator is directional, so evaluators need this flag to normalize cases like
    // `5 < slot` into the equivalent slot-side comparison before checking the zone map. Non-symmetric
    // function evaluators also use it to reject unsupported shapes such as `starts_with(literal,
    // slot)`, because only `starts_with(slot, literal)` can be mapped to a safe zone-map range.
    // 标记原始表达式形式是否为 Literal <op> Slot（例如 5 < slot）。
    // 求值器需要根据此标志将算子归一化（如 5 < slot 归一化为 slot > 5）
    // 对于非对称函数（如 starts_with(literal, slot)），若该标志为 true，则直接拒绝 ZoneMap 剪枝，因为只有 starts_with(slot, literal) 才能映射为安全的 ZoneMap 范围。
    bool literal_on_left;
};
// 解析表达式的参数列表（args），从中提取出一个 Slot（列引用）和一个 Literal（常量）。
// 若参数列表恰好由一个 Slot 表达式和一个 Constant Literal 表达式组成，则返回填充好的 SlotLiteral 对象；若结构不符合（例如两个都是 Slot，或者两个都是 Literal），则返回 std::nullopt。
std::optional<SlotLiteral> extract_slot_and_literal(const VExprSPtrs& args);

// 将内存中 HybridSet（In 谓词底层使用的哈希集合）的具体 C++ 内存指针值（data），根据其物理类型 type、精度 precision 和标度 scale，反序列化并构造为一个 Thrift 的 TExprNode（常量表达式节点）。
// 用途：在将 In 谓词物化为表达式或者 Field 时作为桥梁工具。
TExprNode create_texpr_node_from_hybrid_set_value(const void* data, const PrimitiveType& type,
                                                  int precision, int scale);
// 将 IN 谓词对应的内存集合 HybridSetBase 中的所有常量元素，按照指定的 data_type 转换为 Field 形式，物化填入 InZonemapMaterializedSet 中。同时计算出该集合中的 min_value 和 max_value。
Status materialize_hybrid_set_for_zonemap_filter(HybridSetBase& set, const DataTypePtr& data_type,
                                                 InZonemapMaterializedSet* result);
// 断两个原生类型（PrimitiveType）在 Field 级别比较时是否兼容。
// 如果类型完全相同，或者两者都是字符串类型（如 VARCHAR 与 STRING、CHAR），则视为兼容（返回 true）；否则返回 false。
inline bool field_types_compatible(PrimitiveType lhs, PrimitiveType rhs) {
    return lhs == rhs || (is_string_type(lhs) && is_string_type(rhs));
}
// 判断两个高级数据类型 DataTypePtr 是否兼容（忽略 Nullable 属性）。
// 去除 Nullable 包装后，若两者均为字符串类型则直接兼容；否则调用 lhs_type->equals(*rhs_type) 进行精确类型等价判定。
inline bool data_types_compatible(const DataTypePtr& lhs, const DataTypePtr& rhs) {
    if (lhs == nullptr || rhs == nullptr) {
        return false;
    }
    const auto lhs_type = remove_nullable(lhs);
    const auto rhs_type = remove_nullable(rhs);
    const auto lhs_primitive_type = lhs_type->get_primitive_type();
    const auto rhs_primitive_type = rhs_type->get_primitive_type();
    if (is_string_type(lhs_primitive_type) && is_string_type(rhs_primitive_type)) {
        return true;
    }
    return lhs_type->equals(*rhs_type);
}
// 从 ZoneMap 上下文中获取存储层（Reader Schema）的列类型，并校验其与表达式中的 Slot 类型（expr_slot_type）是否兼容。
inline DataTypePtr fetch_compatible_slot_type(const ZoneMapEvalContext& ctx, int slot_index,
                                              const DataTypePtr& expr_slot_type) {
    auto slot_type = ctx.data_type(slot_index);
    if (slot_type == nullptr) {
        return nullptr;
    }
    DORIS_CHECK(data_types_compatible(slot_type, expr_slot_type));
    return slot_type;
}
// 检查指定的 zone_map 内部记录的 Min/Max 范围统计信息对于数据类型 data_type 是否合法且可用于 ZoneMap 过滤（例如检查 ZoneMap 是否有有效值、浮点数是否包含 NaN 等无法比较的特殊情况）。
bool range_stats_usable_for_zonemap(const segment_v2::ZoneMap& zone_map,
                                    const DataTypePtr& data_type);
// 评估 IS NULL 或 IS NOT NULL 谓词在 ZoneMap 上的过滤结果。
ZoneMapFilterResult eval_null_zonemap(const ZoneMapEvalContext& ctx, const VExprSPtrs& arguments,
                                      bool is_null);
// 评估 IN 或 NOT IN 谓词在 ZoneMap 上的过滤结果。
// 使用物化集合的 min_value / max_value 与 ZoneMap 统计信息的 min_value / max_value 做重叠（Overlap）判定。如果 [min_value, max_value] 与 ZoneMap 范围无交集，且非 NOT IN，则直接判定 kNoMatch 跳过该数据块。
ZoneMapFilterResult eval_in_zonemap(const ZoneMapEvalContext& ctx, const VExprSPtr& slot_expr,
                                    bool is_not_in, const std::vector<Field>& values,
                                    const Field& min_value, const Field& max_value);
// 评估等值谓词（如 col = 'abc'）在列字典（Dictionary）上的过滤结果。
ZoneMapFilterResult eval_eq_dictionary(const DictionaryEvalContext& ctx,
                                       const SlotLiteral& slot_literal);
// 估 IN / NOT IN 谓词在列字典上的过滤结果。
ZoneMapFilterResult eval_in_dictionary(const DictionaryEvalContext& ctx, const VExprSPtr& slot_expr,
                                       bool is_not_in, const std::vector<Field>& values);
// 评估等值谓词（如 col = 123）在 Bloom Filter 上的过滤结果。
ZoneMapFilterResult eval_eq_bloom_filter(const BloomFilterEvalContext& ctx,
                                         const SlotLiteral& slot_literal);
// 评估 IN / NOT IN 谓词在 Bloom Filter 上的过滤结果。
ZoneMapFilterResult eval_in_bloom_filter(const BloomFilterEvalContext& ctx,
                                         const VExprSPtr& slot_expr, bool is_not_in,
                                         const std::vector<Field>& values);

// Return the only slot ordinal referenced by a zonemap-evaluable expression in its current
// binding. Expressions that are unsupported by zonemap pruning, reference multiple slots, or use an
// invalid negative slot ordinal return a negative value.
// 分析表达式上下文 ctx，返回该表达式引用的唯一 Slot 序号（Slot Ordinal）。
// ZoneMap 过滤通常只适用于单列表达式（如 a + 1 > 10 仅引用了列 a）。如果表达式无法进行 ZoneMap 评估、引用了多列（如 a + b > 10），或者引用的 Slot 序号非法（负数），则返回负数（如 -1）。
int single_slot_zonemap_index(const VExprContextSPtr& ctx);
// 析表达式上下文 ctx，返回其进行字典过滤所引用的唯一 Slot 序号。非单列表达式或不支持字典过滤的表达式返回负数。
int single_slot_dictionary_index(const VExprContextSPtr& ctx);
// 分析表达式上下文 ctx，返回其进行 Bloom Filter 剪枝所引用的唯一 Slot 序号。非单列表达式或不支持 Bloom Filter 过滤的表达式返回负数。
int single_slot_bloom_filter_index(const VExprContextSPtr& ctx);
// ：从查询的 RuntimeState（运行状态/Session 变量）中获取开关配置，判断当前查询是否启用了基于表达式的 ZoneMap 过滤功能（用于控制特性的开启与关闭）。
bool is_expr_zonemap_filter_enabled(const RuntimeState* state);

} // namespace doris::expr_zonemap

namespace doris {
using DictionaryEvalContext = expr_zonemap::DictionaryEvalContext;
using BloomFilterEvalContext = expr_zonemap::BloomFilterEvalContext;
} // namespace doris
