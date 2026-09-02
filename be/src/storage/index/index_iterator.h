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

#include <gen_cpp/olap_file.pb.h>

#include <variant>

#include "common/exception.h"
#include "common/factory_creator.h"
#include "runtime/runtime_state.h"
#include "storage/index/ann/ann_index_reader.h"
#include "storage/index/index_query_context.h"
#include "storage/index/index_reader.h"
#include "storage/index/inverted/inverted_index_query_type.h"

namespace doris {
struct AnnTopNParam;
}

namespace doris::segment_v2 {

class InvertedIndexQueryCacheHandle;

struct InvertedIndexParam;
// 索引查询参数指针的变体联合体。在调用 read_from_index 时，既可以传入传统的倒排索引查询参数 InvertedIndexParam*，也可以传入向量 TOP-N 搜索参数 AnnTopNParam*。
// std::variant 是 C++17 引入的一种类型安全（Type-safe）的联合体（Union）。
// 在传统的 C 语言风格 union 中，程序员必须手动记录当前存储的是哪种类型，且原生 union 无法直接存放非 POD 类型（如 std::string 或包含构造/析构函数的自定义对象）。
// std::variant 则完美解决了这些安全隐患与类型限制。
// 类型安全：明确知道当前保存的具体类型，访问错误类型时会在编译期报错或在运行时抛出 std::bad_variant_access 异常。
using IndexParam = std::variant<InvertedIndexParam*, segment_v2::AnnTopNParam*>;

// 向量近似最近邻（ANN, Approximate Nearest Neighbor）索引 Reader 的类型枚举。目前包含 ANN = 0。
enum class AnnIndexReaderType {
    ANN = 0,
};
// IndexIterator 是 Apache Doris BE（Backend）模块存储层中索引查询迭代器的抽象基类（Abstract Base Class）。
// 在 Doris 的 Segment 数据组织结构中，一个 Segment 文件可能包含多种类型的索引（如基于 CLucene 的倒排索引、基于 BKD 树的数值/范围索引、基于 HNSW 等算法的向量 ANN 近邻搜索索引等）。IndexIterator 的核心职责包括：
// 统一索引查询抽象：为上一层（如谓词计算 ColumnPredicate、ComparisonPredicateBase 等）提供统一的索引读取与求值接口，屏蔽底层不同索引结构和存储格式的差异。
// 支持多种索引类型：利用 C++17 的 std::variant 类型绑定，灵活适配倒排索引（InvertedIndexParam）和向量 ANN 索引（AnnTopNParam）等不同场景的查询参数与 Reader。
// 统一处理 NULL 值：针对数据库中的 NULL 语义提供索引层面的 null_bitmap 读取与存在性校验能力。

// 索引 Reader 类型的变体联合体。支持通过同一个类型标识传入倒排索引 Reader 类型（如 STRING_TYPE、BKD 等）或 ANN 向量索引 Reader 类型。
using IndexReaderType = std::variant<InvertedIndexReaderType, AnnIndexReaderType>;
class IndexIterator {
public:
    IndexIterator() = default;
    virtual ~IndexIterator() = default;
    // 根据传入的索引 Reader 类型获取对应的底层 IndexReader 指针。
    virtual IndexReaderPtr get_reader(IndexReaderType reader_type) const = 0;
    // 索引查询的核心执行入口。
    // param: 索引查询参数 IndexParam（包含列名、数据类型、查询值 Field、查询类型 InvertedIndexQueryType 以及用于接收匹配行号结果的 roaring::Roaring 位图 shared_ptr 等）。
    virtual Status read_from_index(const IndexParam& param) = 0;
    // 读取当前 Segment/列的 NULL 值位图（null_bitmap），并写入传入的 Cache 句柄中。
    // 应用场景：SQL 语义中 NULL 与任何值比较均返回 NULL（WHERE 中判为 false）。在通过索引求得筛选位图后，需要调用此方法获取 null_bitmap 并从结果集中剔除（如 *bitmap -= *null_bitmap）。
    virtual Status read_null_bitmap(InvertedIndexQueryCacheHandle* cache_handle) = 0;
    // 快速检查当前索引列在当前 Segment 数据块中是否存在 NULL 值。
    virtual Result<bool> has_null() = 0;
    // 设置当前迭代器的 _context 查询上下文。
    void set_context(const IndexQueryContextPtr& context) { _context = context; }
    IndexQueryContextPtr get_context() const { return _context; }

protected:
    // 存储当前索引迭代器执行过程中的上下文状态信息（如执行跟踪、统计指标、内存 Allocator 绑定或 RuntimeState 等），确保索引执行过程中的上下文统一。
    IndexQueryContextPtr _context = nullptr;
};

} // namespace doris::segment_v2