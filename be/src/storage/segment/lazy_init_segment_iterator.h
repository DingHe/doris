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

#include "core/block/block.h"
#include "storage/segment/common.h"
#include "storage/segment/segment.h"
#include "storage/segment/segment_iterator.h"

namespace doris {
class BetaRowset;
using BetaRowsetSharedPtr = std::shared_ptr<BetaRowset>;
}; // namespace doris
namespace doris::segment_v2 {
// LazyInitSegmentIterator 是 Apache Doris BE（Backend）存储引擎中一个非常关键的延迟初始化代理迭代器（继承自 RowwiseIterator）。
// 在 Doris 执行查询时，一个 Rowset 通常包含多个物理 Segment 文件，每个 Segment 需要创建一个 SegmentIterator 来读取数据。Segment 的初始化（init）包含非常重的操作：打开 Segment 文件句柄、加载列元数据（Column Meta）、初始化 Primary Key / ZoneMap / Short Key 索引、创建 Bitmap / BloomFilter 过滤器等。
// 如果一个查询涉及数百个 Segment，但在优化器下推了谓词（Predicate）、Runtime Filter 或 Short Key 索引切片后，很多 Segment 根本不包含符合条件的行；或者在多路归并/排序（如 LIMIT 查询）中，排在后面的 Segment 的数据根本不会被读取到。
// 轻量级占位与延迟加载（Lazy Load）：创建迭代器时仅保存元数据句柄和读取选项，不真正打开文件或初始化物理迭代器。
// 按需初始化（On-Demand Initialization）：将昂贵的物理 Segment 初始化延迟到真正调用 next_batch() 读取数据的那一刻。如果某个 Segment 的数据从未被请求，则其文件句柄和索引永远不会被初始化和加载。
// 减少 IO/CPU 与内存峰值：大幅降低并发查询时的文件句柄占用、元数据加载 CPU 损耗以及内存开销，极大地提升了针对海量 Segment 查询（尤其是带 LIMIT 或高性能点查）的响应速度。
class LazyInitSegmentIterator : public RowwiseIterator {
public:
    // 采用轻量级赋值的方式，仅将输入的 rowset、segment_id、schema 和 opts 保存到内部属性中，不进行任何磁盘 IO 或物理 Segment 的初始化操作，耗时极短（近乎 $O(1)$）。
    LazyInitSegmentIterator(BetaRowsetSharedPtr rowset, int64_t segment_id, bool should_use_cache,
                            SchemaSPtr schema, const StorageReadOptions& opts);

    ~LazyInitSegmentIterator() override = default;
    // 真正执行底层的物理 Segment 初始化。
    Status init(const StorageReadOptions& opts) override;
    // 向量化读取数据 Batch 的核心入口（兼具延迟触发器作用）。
    Status next_batch(Block* block) override {
        if (UNLIKELY(_need_lazy_init)) {
            RETURN_IF_ERROR(init(_read_options));
            DCHECK(_inner_iterator != nullptr);
        }

        return _inner_iterator->next_batch(block);
    }

    const Schema& schema() const override { return *_schema; }

    Status current_block_row_locations(std::vector<RowLocation>* locations) override {
        return _inner_iterator->current_block_row_locations(locations);
    }

    void update_profile(RuntimeProfile* profile) override {
        if (_inner_iterator != nullptr) {
            _inner_iterator->update_profile(profile);
        }
    }

private:
    // 标识当前迭代器是否仍处于“待延迟初始化”状态。
    // 初始值为 true。当第一次真正触发 init() 并成功创建底层的物理 _inner_iterator 后，该标志位会被置为 false，后续的 next_batch 即可直接跳过初始化检查。
    bool _need_lazy_init {true};
    // 目标 Segment 所在的 BetaRowset（Rowset 存储句柄）智能指针。
    BetaRowsetSharedPtr _rowset;
    // 当前迭代器对应的物理 Segment ID（从 0 开始递增）。
    int64_t _segment_id {-1};
    // 标识读取 Segment 元数据/数据页时，是否开启 Page Cache（页缓存）。
    bool _should_use_cache {false};
    // 当前查询请求的 Schema 结构定义（包含所需读取的列信息）。
    SchemaSPtr _schema = nullptr;
    // 存储层读取配置选项结构体。
    StorageReadOptions _read_options;
    // 实际负责物理数据读取的内部底层迭代器（通常是 SegmentIterator 的独占智能指针 std::unique_ptr<RowwiseIterator>）。
    RowwiseIteratorUPtr _inner_iterator;
};
} // namespace doris::segment_v2