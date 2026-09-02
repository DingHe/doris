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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/ColumnsWithTypeAndName.h
// and modified by Doris

#pragma once

#include <string>
#include <utility>
#include <vector>

#include "core/block/column_with_type_and_name.h"
#include "core/data_type/data_type.h"

// 定义了 Apache Doris 存储层与表达式引擎中针对倒排索引（Inverted Index）的核心数据结构别名。
// 其最主要的作用是解决倒排索引在不同存储格式（V1 vs V2）以及复杂数据类型（如 Variant 变体类型）下列标识符（Field Name）的映射与定位问题
namespace doris {
// Doris 向量化执行引擎中的经典 Block 列集合结构（包含列的内存数据 IColumn、数据类型 DataTypePtr 和列名 string）。
// 这里在头文件中通过类型别名引入，作为索引处理时与底层向量化 Column 交互的接口载体。
using ColumnsWithTypeAndName = std::vector<ColumnWithTypeAndName>;
// only used in inverted index
// <field_name, storage_type>
// field_name is the name of inverted index document's filed
//     1. for inverted_index_storage_format_v1, field_name is the `column_name` in Doris
//     2. for inverted_index_storage_format_v2
//         2.1 for normal column, field_name is the `column_unique_id` in Doris
//         2.2 for variant column, field_name is the `parent_column_unique_id.sub_column_name` in Doris
// storage_type is the data type in Doris
// 单个倒排索引字段的 (field_name, storage_type) 键值对
// 倒排索引底层（如 Lucene / CLucene 存储）以 Document/Field 的形式组织数据。field_name 的生成逻辑在不同格式下有所不同：
// V1 格式 (inverted_index_storage_format_v1)： field_name 直接使用 SQL 中的列名（column_name，如 "age"、"user_name"）。局限性：如果发生 Schema Change（例如重命名列），V1 格式需要重建索引或面临映射不一致问题。
// V2 格式 (inverted_index_storage_format_v2):  普通列（Normal Column）：field_name 使用列的全局唯一 ID（column_unique_id，如 "10002"）。即使修改列名，ID 依然保持不变，使得 Schema Change 更加高效。
// Variant 变体列（Variant / JSON Dynamic Sub-column）：field_name 采用 parent_column_unique_id.sub_column_name（如 "10005.city"）。针对半结构化数据，Variant 类型的子路径（Sub-column）动态提取后，可以通过这种拼接前缀的方式独立建立和检索倒排索引。
using IndexFieldNameAndTypePair = std::pair<std::string, DataTypePtr>;
// IndexFieldNameAndTypePair 的集合类型，通常用于需要同时对多个索引字段进行批量初始化、读取或索引扫描的场景。
using NameAndTypePairs = std::vector<std::pair<std::string, DataTypePtr>>;
} // namespace doris
