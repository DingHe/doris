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

#include <vector>

#include "common/status.h"
#include "storage/index/bloom_filter/bloom_filter.h"

namespace doris {
// 魔数/伪随机数生成种子。用于在计算 N-Gram 子串哈希时，为多个哈希函数衍生生成不同的 Seed。
static constexpr uint64_t SEED_GEN = 217728422;

namespace segment_v2 {
enum HashStrategyPB : int;
// NGramBloomFilter 是继承自 BloomFilter 的特化实现，专门用于支持文本模糊匹配（如 LIKE '%pattern%'）的下推裁剪索引。
// 常规的布隆过滤器只能用于等值匹配（= 或 IN）。对于 LIKE '%abc%' 这类非前缀模糊查询，传统 Bloom Filter 会失效。
// NGramBloomFilter 专门解决这一痛点：
// N-Gram 分词切片：在向布隆过滤器写入文本字段时，它并不对整个字符串直接计算哈希，而是将字符串切割为固定长度（$N$）的滑动子串（N-grams）。
// 基于子串建索引：将这些切割出的 N-gram 子串分别计算哈希打入位图中。
// 支持子串包含裁剪：查询 LIKE '%abc%' 时，把模式串 abc 拆解为相同的 N-grams 并生成对应的查询位图。
// 如果当前 Segment 或 Data Page 的 N-Gram 布隆过滤器包含（contains）查询位图的所有 1-bit，说明数据可能匹配；
// 如果不包含，则说明该数据块绝不可能包含该子串，从而实现高效的页面/区段剪枝（Pruning）。
class NGramBloomFilter : public BloomFilter {
public:
    // Fixed hash function number
    // 固定使用的哈希函数数量。对于每个 N-Gram 子串，使用 2 个哈希函数将其映射到位图中。
    static const size_t HASH_FUNCTIONS = 2;
    // 类型别名。定义底层位图的基本存储单元类型为 64 位无符号整数（8 字节）。
    using UnderType = uint64_t;
    // 传入期望的字节大小 size，内部将其向上调整对齐后，初始化 words 数量以及 filter 容器的大小，并将 _size 赋为实际物理占用字节数。
    NGramBloomFilter(size_t size);
    // 向过滤器中追加一段文本数据（字符串）。
    // 它在内部会按设定的 $N$ 长度（例如 3-gram）对 data 进行滑动窗口切片，逐个提取 N-gram 子串，计算 2 次哈希，并在 filter 的对应 Bit 位置 1。
    void add_bytes(const char* data, size_t len) override;
    // 判断当前 N-Gram 布隆过滤器是否完全包含另一个 N-Gram 布隆过滤器 bf_ 的所有设置位。
    // 应用场景：在 LIKE '%abc%' 查询时，会将查询词 abc 构造成一个临时的 NGramBloomFilter（即 bf_），然后调用 page_bf.contains(query_bf)。如果 (page_bf & query_bf) == query_bf（即按位与后仍然等于 query_bf），说明当前 Data Page 可能包含该模式串；否则可以直接跳过该 Page。
    bool contains(const BloomFilter& bf_) const override;
    // 从磁盘或网络接收到的二进制数据流 buf（长度为 size）中恢复并构建 NGramBloomFilter 对象。
    Status init(const char* buf, size_t size, HashStrategyPB strategy) override;
    // 返回底层 filter 内存数组的只读字节指针 const char*，用于写盘序列化。
    const char* data() const override { return reinterpret_cast<const char*>(filter.data()); }
    // 返回当前 N-Gram 布隆过滤器占用的总内存字节数 _size。
    size_t size() const override { return _size; }
    // 因为 N-Gram 布隆过滤器的写入依赖字符串切片，不能直接传入单个预计算的 64 位哈希，因此将该基类虚方法覆盖为空操作。
    void add_hash(uint64_t) override {}
    // 重写基类方法，始终返回 true。理由同上，N-Gram 的测试必须基于 contains 进行集合级别的位掩码判定，而非单哈希测试。
    bool test_hash(uint64_t hash) const override { return true; }
    // 始终返回 true。在模糊匹配场景下，默认不剔除包含 NULL 的评估，或者交由上层 NULL Marker 处理。
    bool has_null() const override { return true; }
    // 覆盖基类方法并返回 true，用于上层（如 BloomFilterIndexReader）判断当前实例是否为 N-Gram 类型的布隆过滤器，以便走专门的模糊匹配裁剪路径。
    bool is_ngram_bf() const override { return true; }

private:
// FIXME: non-static data member '_size' of 'NGramBloomFilter' shadows member inherited from type 'BloomFilter'
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshadow-field"
#endif
    // 当前布隆过滤器占用的总内存字节数。
    size_t _size = 0;
#ifdef __clang__
#pragma clang diagnostic pop
#endif
    // 底层 filter 容器中 uint64_t 元素的数量。即 $words = \text{bit\_size} / 64$。
    size_t words = 0;
    // 实际存储布隆过滤器位图的连续内存容器。每个 uint64_t 包含 64 个 Bit。
    std::vector<uint64_t> filter;
};

} // namespace segment_v2
} // namespace doris
