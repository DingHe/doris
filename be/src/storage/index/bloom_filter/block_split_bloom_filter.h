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

#include <stdint.h>

#include "storage/index/bloom_filter/bloom_filter.h"

namespace doris {
namespace segment_v2 {

// This Bloom filter is implemented using block-based Bloom filter algorithm
// from Putze et al.'s "Cache-, Hash- and Space-Efficient Bloom filters". The basic
// idea is to hash the item to a tiny Bloom filter which size fit a single cache line
// or smaller. This implementation sets 8 bits in each tiny Bloom filter. Each tiny
// Bloom filter is 32 bytes to take advantage of 32-byte SIMD instruction.
// 标准/通用布隆过滤器（Bloom Filter）的核心默认实现类（继承自 BloomFilter）
// 常规布隆过滤器（Standard Bloom Filter）在插入或查询一个元素时，需要在整个巨大的位图数组中随机访问 $k$ 个独立的 Bit。
// 这种随机访问会导致多次 CPU L1/L2/L3 Cache Miss，成为 OLAP 引擎向量化执行时的计算瓶颈。
// BlockSplitBloomFilter 基于 Putze 等人提出的 "Cache-, Hash- and Space-Efficient Bloom filters" 论文算法（即 Block-Based Bloom Filter），核心作用包括：
// 缓存线亲和（Cache-line Efficiency）：
// 将整个布隆过滤器划分为多个大小为 32 字节（256 比特） 的微型块（Tiny Block）。无论数据总量有多大 ，一个元素的全部 8 个比特打点或校验操作，都被限制在同一个 32 字节的 Block 内部。因此，一次查询最多仅触发一次 L1 Cache 加载，彻底消除了跨 Cache Line 调度的开销。
// SIMD 向量化加速（SIMD Hardware Acceleration）：
//每个 Block 恰好是 32 字节（256 Bits），完美匹配现代 CPU 的 AVX2 / AVX-512 等 256 位 SIMD 寄存器宽度。无论是设置位还是比较位，都可以通过一条 SIMD 向量指令在单周期内完成 8 个位置的并行运算。
// 等值与 Runtime Filter 过滤：
//是 Doris 存储层列索引（Bloom Filter Index）以及执行引擎 Runtime Filter（运行时动态过滤）的默认高效实现。
class BlockSplitBloomFilter : public BloomFilter {
public:
    void add_hash(uint64_t hash) override;

    bool test_hash(uint64_t hash) const override;

private:
    // Bytes in a tiny Bloom filter block.
    // 定义每个 Tiny Bloom Filter Block 的物理字节大小为 32 字节（256 Bits），对应 8 个 uint32_t 整数。
    static constexpr int BYTES_PER_BLOCK = 32;
    // The number of bits to set in a tiny Bloom filter block
    // 定义每个数据元素映射到 Block 内部时需要设置/校验的比特数量（固定为 8 个 Bit）。
    static constexpr int BITS_SET_PER_BLOCK = 8;
    // 包含了 8 个伪随机无符号 32 位整数的盐值数组（Salt）。
    // 用于在 Block 内部通过乘法散列（Multiplicative Hashing）将 32 位低位哈希拆分衍生出 8 个互不相关的 32 位值，用于确定这 8 个 Bit 在 Block 内的具体位置。
    static constexpr uint32_t SALT[BITS_SET_PER_BLOCK] = {0x47b6137bU, 0x44974d91U, 0x8824ad5bU,
                                                          0xa2b7289dU, 0x705495c7U, 0x2df1424bU,
                                                          0x9efc4947U, 0x5c6bfb31U};
    // 内存掩码容器。包含一个长度为 8 的 uint32_t 数组（恰好占用 32 字节）。
    // 存储经过 _set_masks 计算后生成的 8 个 Bit Mask（即只有目标 Bit 位为 1，其余位全为 0 的 32 位整数）。
    struct BlockMask {
        uint32_t item[BITS_SET_PER_BLOCK];
    };

private:
    // 根据传入的 32 位哈希子键 key，在 block_mask 中生成对应 Block 内部 8 个位置的比特掩码。
    void _set_masks(uint32_t key, BlockMask& block_mask) const {
        for (int i = 0; i < BITS_SET_PER_BLOCK; ++i) {
            block_mask.item[i] = key * SALT[i];
        }

        for (int i = 0; i < BITS_SET_PER_BLOCK; ++i) {
            block_mask.item[i] = block_mask.item[i] >> 27;
        }

        for (int i = 0; i < BITS_SET_PER_BLOCK; ++i) {
            block_mask.item[i] = uint32_t(0x1) << block_mask.item[i];
        }
    }
};

} // namespace segment_v2
} // namespace doris
