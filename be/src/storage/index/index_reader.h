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

#include <memory>

#include "storage/metadata_adder.h"

namespace doris {
class RuntimeState;
}

namespace doris::segment_v2 {

class IndexIterator;

class InvertedIndexReader;
// 指向通用倒排索引 Reader（InvertedIndexReader）的共享指针类型别名。
using InvertedIndexReaderPtr = std::shared_ptr<InvertedIndexReader>;

// IndexReader 是 Apache Doris 存储层（Segment V2）中所有存储索引读取器（Index Reader）的核心抽象基类（Abstract Base Class）。
// 在 Doris 的 Segment 文件组织结构中，单个 Segment 可能包含多种不同类型的索引文件（例如 CLucene 倒排索引、BKD 树数值索引、向量 ANN 索引等）。IndexReader 的主要作用包括：
// 统一索引读取接口：定义所有索引 Reader 必须遵循的标准协议，为上层数据检索提供统一的抽象，屏蔽底层具体的索引文件存储格式与实现机制。
// 连接 IndexIterator：作为工厂角色，负责创建与其生命周期绑定的索引迭代器 IndexIterator，供谓词计算（ColumnPredicate）和存储下推（Push-down）进行高效的位图（Roaring Bitmap）求值。
//
class IndexReader : public std::enable_shared_from_this<IndexReader>,
                    public MetadataAdder<IndexReader> {
public:
    IndexReader() = default;
    ~IndexReader() override = default;
    // IndexType（Protobuf 生成的索引类型枚举，如 INVERTED，BLOOM_FILTER 等）。
    // 返回当前 Reader 实例对应的具体索引类型，用于上层在运行时识别和分发不同类型的索引逻辑。
    virtual IndexType index_type() = 0;
    // 获取当前索引在表结构 Schema 定义中分配的唯一 Index ID，主要用于元数据检索、缓存（Cache Key）构建与日志排查。
    virtual uint64_t get_index_id() const = 0;
    // iterator: std::unique_ptr<IndexIterator>*（输出参数，用于接收创建出的 IndexIterator 实例指针）。
    // 索引查询迭代器的工厂方法。为当前的 IndexReader 创建一个新的 IndexIterator 实例，供谓词求值（如 ComparisonPredicateBase::evaluate）调用 read_from_index 进行实际的数据检索。
    virtual Status new_iterator(std::unique_ptr<IndexIterator>* iterator) = 0;
};
using IndexReaderPtr = std::shared_ptr<IndexReader>;

} // namespace doris::segment_v2