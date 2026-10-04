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

namespace cpp doris
namespace java org.apache.doris.thrift

include "Types.thrift"
include "Opcodes.thrift"

enum TExprNodeType {
  AGG_EXPR = 0,
  ARITHMETIC_EXPR = 1,
  BINARY_PRED = 2,
  BOOL_LITERAL = 3,
  CASE_EXPR = 4,
  CAST_EXPR = 5,
  COMPOUND_PRED = 6,
  DATE_LITERAL = 7,
  FLOAT_LITERAL = 8,
  INT_LITERAL = 9,
  DECIMAL_LITERAL = 10,
  IN_PRED = 11,
  IS_NULL_PRED = 12,
  LIKE_PRED = 13,
  LITERAL_PRED = 14,
  NULL_LITERAL = 15,
  SLOT_REF = 16,
  STRING_LITERAL = 17,
  TUPLE_IS_NULL_PRED = 18,
  INFO_FUNC = 19,
  FUNCTION_CALL = 20,
  ARRAY_LITERAL = 21,
  
  // TODO: old style compute functions. this will be deprecated
  COMPUTE_FUNCTION_CALL = 22,
  LARGE_INT_LITERAL = 23,
  
  // only used in runtime filter
  BLOOM_PRED = 24,
  
  // for josn
  JSON_LITERAL = 25,
  
  // Deprecated: bitmap runtime filter predicate is no longer planned; only used in runtime filter
  BITMAP_PRED = 26,

  // for fulltext search
  MATCH_PRED = 27,
  
  // for map 
  MAP_LITERAL = 28,
  
  // for struct
  STRUCT_LITERAL = 29,
  
  // for schema change
  SCHEMA_CHANGE_EXPR = 30,
  // for lambda function expr
  LAMBDA_FUNCTION_EXPR = 31,
  LAMBDA_FUNCTION_CALL_EXPR = 32,
  // for column_ref expr
  COLUMN_REF = 33,
  
  IPV4_LITERAL = 34,
  IPV6_LITERAL = 35,
  
  // only used in runtime filter
  // to prevent push to storage layer
  NULL_AWARE_IN_PRED = 36,
  NULL_AWARE_BINARY_PRED = 37,
  TIMEV2_LITERAL = 38,
  VIRTUAL_SLOT_REF = 39,
  VARBINARY_LITERAL = 40,
  TRY_CAST_EXPR = 41
  // for search DSL function
  SEARCH_EXPR = 42,
  // Normal predicate expression
  PREDICATE = 43,
  // Normal literal
  LITERAL = 44,
}

//enum TAggregationOp {
//  INVALID = 0,
//  COUNT = 1,
//  MAX = 2,
//  DISTINCT_PC = 3,
//  MERGE_PC = 4,
//  DISTINCT_PCSA = 5,
//  MERGE_PCSA = 6,
//  MIN = 7,
//  SUM = 8,
//}
//
//struct TAggregateExpr {
//  1: required bool is_star
//  2: required bool is_distinct
//  3: required TAggregationOp op
//}
struct TAggregateExpr {
  // Indicates whether this expr is the merge() of an aggregation.
  1: required bool is_merge_agg
  2: optional list<Types.TTypeDesc> param_types
}
struct TBoolLiteral {
  1: required bool value
}

struct TCaseExpr {
  1: required bool has_case_expr
  2: required bool has_else_expr
}

struct TDateLiteral {
  1: required string value
}

struct TTimeV2Literal {
  1: required double value
}

struct TFloatLiteral {
  1: required double value
}

struct TDecimalLiteral {
  1: required string value
}

struct TIntLiteral {
  1: required i64 value
}

struct TLargeIntLiteral {
  1: required string value
}

struct TIPv4Literal {
  1: required i64 value
}

struct TIPv6Literal {
  1: required string value
}

struct TInPredicate {
  1: required bool is_not_in
}

struct TIsNullPredicate {
  1: required bool is_not_null
}

struct TLikePredicate {
  1: required string escape_char;
}

struct TMatchPredicate {
  1: required string parser_type;
  2: required string parser_mode;
  3: optional map<string, string> char_filter_map;
  4: optional bool parser_lowercase = true;
  5: optional string parser_stopwords = "";
  6: optional string analyzer_name = "";
}

struct TLiteralPredicate {
  1: required bool value
  2: required bool is_null
}

enum TNullSide {
   LEFT = 0,
   RIGHT = 1
}

struct TTupleIsNullPredicate {
  1: required list<Types.TTupleId> tuple_ids
  2: optional TNullSide null_side
}

struct TSlotRef {
  1: required Types.TSlotId slot_id
  2: required Types.TTupleId tuple_id
  3: optional i32 col_unique_id
  4: optional bool is_virtual_slot
}

struct TColumnRef {
  1: optional Types.TSlotId column_id
  2: optional string column_name
}

struct TStringLiteral {
  1: required string value;
}

struct TVarBinaryLiteral {
  1: required binary value;
}

struct TNullableStringLiteral {
  1: optional string value;
  2: optional bool is_null = false;
}

struct TJsonLiteral {
  1: required string value;
}

struct TInfoFunc {
  1: required i64 int_value;
  2: required string str_value;
}

struct TFunctionCallExpr {
  // The aggregate function to call.
  1: required Types.TFunction fn

  // If set, this aggregate function udf has varargs and this is the index for the
  // first variable argument.
  2: optional i32 vararg_start_idx
}

struct TSchemaChangeExpr {
  // target schema change table
  1: optional i64 table_id 
}

// Search DSL parameter structure

// Occur type for Lucene-style boolean queries
enum TSearchOccur {
  MUST = 0,      // Term must appear (equivalent to +term)
  SHOULD = 1,    // Term should appear (optional, but contributes to matching)
  MUST_NOT = 2   // Term must not appear (equivalent to -term)
}

struct TSearchClause {
  1: required string clause_type  // TERM, QUOTED, PREFIX, WILDCARD, REGEXP, RANGE, LIST, ANY_ALL, AND, OR, NOT, OCCUR_BOOLEAN, NESTED
  2: optional string field_name   // Field name for leaf clauses
  3: optional string value        // Search value for leaf clauses
  4: optional list<TSearchClause> children  // Child clauses for compound clauses (AND, OR, NOT, OCCUR_BOOLEAN)
  5: optional TSearchOccur occur  // Occur type for this clause (used with OCCUR_BOOLEAN parent)
  6: optional i32 minimum_should_match  // Minimum number of SHOULD clauses that must match (for OCCUR_BOOLEAN)
  7: optional string nested_path  // Path for NESTED clause (e.g., "items")
}

struct TSearchFieldBinding {
  1: required string field_name   // Field name from DSL (may include path like "field.subcolumn")
  2: required i32 slot_index      // Index in the slot reference arguments
  3: optional string parent_field_name    // Parent field name for variant subcolumns
  4: optional string subcolumn_path       // Subcolumn path for variant fields (e.g., "subcolumn" or "sub1.sub2")
  5: optional bool is_variant_subcolumn   // True if this is a variant subcolumn access
  6: optional map<string, string> index_properties  // Index properties (parser, lower_case, etc.) from FE Index lookup
}

struct TSearchParam {
  1: required string original_dsl         // Original DSL string for debugging
  2: required TSearchClause root     // Parsed AST root
  3: required list<TSearchFieldBinding> field_bindings  // Field to slot mappings
  4: optional string default_operator     // "and" or "or" for TERM tokenization (default: "or")
  5: optional i32 minimum_should_match    // Minimum number of SHOULD clauses that must match (for Lucene mode TERM tokenization)
}

// This is essentially a union over the subclasses of Expr.
// TExprNode 的作用
// 表达式树中的单节点载体（Tree Node Variant）：
// 注释明确指出：“This is essentially a union over the subclasses of Expr.”
// 在 Doris 中，上一问介绍的 TExpr（表达式树）是由展平的 TExprNode 列表组成的。每一个 TExprNode 代表表达式树中的一个具体节点（如：属性列引用 SlotRef、字面量常量 Literal、函数调用 FunctionCall、谓词条件 Predicat// e、分支判断 CaseExpr 等）。
// 离散联合（Tagged Union）设计：
// 由于 Thrift 不支持 C++ 风格的面向对象多态继承，TExprNode 采用了 Tagged Union 模式：所有节点共享通用元数据（节点类型 node_type、数据类型 type、子节点数量 num_children 等），同时通过 optional 字段挂载特定表达/// 式类型的私有数据（如 int_literal、fn、slot_ref 等）。
// FE 逻辑表达式与 BE 向量化表达式（VExpr）的转换桥梁：
// FE 优化器生成的逻辑表达式节点序列化为 TExprNode 后，通过 RPC 传输给 BE。BE 在初始化算子（如 OperatorXBase::init）时，读取 TExprNode 并调用 VExpr::create_expr_tree，将其构建为 BE 侧高效执行的向量化表达式对象（如 VSlotRef、VFunctionCall 等）。
struct TExprNode {
  // 表达式节点的类型枚举（如 SLOT_REF、INT_LITERAL、FUNCTION_CALL、CASE_EXPR 等）。BE 根据此属性判断当前节点属于哪种表达式，并解析对应的 optional 字段。
  1: required TExprNodeType node_type
  // 该表达式节点输出结果的完整数据类型描述符（支持复杂类型，如 ARRAY<INT>、MAP、STRUCT 或普通标量类型）。
  2: required Types.TTypeDesc type
  // 内置算子/表达式的操作码（Opcode），用于快速标识一元、二元运算符或特定内置函数（如 ADD、SUBTRACT、EQ、NE 等）。
  3: optional Opcodes.TExprOpcode opcode
  // 该表达式节点在表达式树中的直接子节点数量。由于 TExpr 是按前序遍历展平的，BE 依赖 num_children 来正确重建父子节点层级关系（例如加法节点 A + B 的 num_children = 2）。
  4: required i32 num_children

  5: optional TAggregateExpr agg_expr
  6: optional TBoolLiteral bool_literal
  7: optional TCaseExpr case_expr
  8: optional TDateLiteral date_literal
  9: optional TFloatLiteral float_literal
  10: optional TIntLiteral int_literal
  11: optional TInPredicate in_predicate
  12: optional TIsNullPredicate is_null_pred
  13: optional TLikePredicate like_pred
  14: optional TLiteralPredicate literal_pred
  15: optional TSlotRef slot_ref
  16: optional TStringLiteral string_literal
  17: optional TTupleIsNullPredicate tuple_is_null_pred
  18: optional TInfoFunc info_func
  19: optional TDecimalLiteral decimal_literal
  // 表达式计算输出结果的小数位数（Scale）。主要用于 DECIMAL 类型的数据精度控制。
  20: required i32 output_scale
  21: optional TFunctionCallExpr fn_call_expr
  22: optional TLargeIntLiteral large_int_literal
  //指示该表达式计算结果输出到 Vectorized Block 中的目标列索引（Column Index）。
  23: optional i32 output_column
  // 传统的简单类型描述（已被 2: type 进一步丰富扩展，用于向下兼容）。
  24: optional Types.TColumnType output_type
  // 专为向量化执行引擎优化的操作码（Vectorized Opcode）
  25: optional Opcodes.TExprOpcode vector_opcode
  // The function to execute. Not set for SlotRefs and Literals.
  // 当 node_type 为函数调用（FUNCTION_CALL）或聚合函数（AGG_EXPR）时设置。包含函数的签名、评估函数指针名、符号映射、入参/出参定义等信息。字面量（Literal）和列引用（SlotRef）不设置此字段。
  26: optional Types.TFunction fn
  // If set, child[vararg_start_idx] is the first vararg child.
  // 对于变长参数函数（Varargs，如 concat(a, b, c...)），指定从第几个子节点开始属于变长参数部分。
  27: optional i32 vararg_start_idx
  // 子节点的基础数据类型，早期用于类型推导，现已被统一的类型系统取代。
  28: optional Types.TPrimitiveType child_type // Deprecated

  // For vectorized engine
  // 向量化引擎核心字段。标识该表达式节点的计算结果是否可能为 NULL。BE 会根据此标识提前决定是否为其创建 NullMap 向量，对执行性能至关重要。
  29: optional bool is_nullable
  
  30: optional TJsonLiteral json_literal
  31: optional TSchemaChangeExpr schema_change_expr 

  32: optional TColumnRef column_ref 
  33: optional TMatchPredicate match_predicate
  34: optional TIPv4Literal ipv4_literal
  35: optional TIPv6Literal ipv6_literal
  36: optional string label // alias name, a/b in `select xxx as a, count(1) as b`
  37: optional TTimeV2Literal timev2_literal
  38: optional TVarBinaryLiteral varbinary_literal
  39: optional bool is_cast_nullable
  40: optional TSearchParam search_param
  41: optional bool short_circuit_evaluation
  // Lambda argument names in the current lambda scope. It is used by BE to
  // distinguish current-scope lambda arguments from captured outer lambda
  // arguments when nested lambda expressions contain duplicated column ids.
  42: optional list<string> lambda_argument_names
}

// A flattened representation of a tree of Expr nodes, obtained by depth-first
// traversal.
// 在 Doris 中，几乎所有的计算逻辑（如 WHERE 过滤条件、SELECT 中的计算列、GROUP BY/ORDER BY 表达式、聚合函数参数等）在语法解析和物理计划生成阶段，都会被抽象为一棵表达式树。TExpr 就是这棵表达式树在 FE（Frontendi// ）与 BE（Backend）之间传输时的序列化容器。
// 在内存中，表达式本来是一棵树（例如：a + b * 2）。为了通过网络进行高效传输和序列化，Doris 将这棵树按前序遍历（Pre-order Traversal）顺序“展平”成了一个一维数组（list<TExprNode>）。
// 列表中索引为 0 的节点是整个表达式树的根节点（Root Node）
// 每个 TExprNode 内部都记录了它的类型、返回数据类型以及子节点数量（num_children）。BE 在拿到这个一维数组后，可以通过简单的递归或栈操作，快速重建出完整的物理表达式树（即 BE 端的 VExpr / VExprContext）
struct TExpr {
  1: required list<TExprNode> nodes
}

struct TExprList {
  1: required list<TExpr> exprs
}
