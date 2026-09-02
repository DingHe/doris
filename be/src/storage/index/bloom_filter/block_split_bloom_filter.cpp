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

#include "storage/index/bloom_filter/block_split_bloom_filter.h"

#include <glog/logging.h>

namespace doris {
namespace segment_v2 {

// 向布隆过滤器写入一个 64 位哈希值的核心实现函数。
// 将 64 位哈希值一分为二，高 32 位用于定位 32 字节的 Block（Bucket），低 32 位用于在该 Block 内部生成 8 个 Bit 掩码并执行按位或（|=）打点。
void BlockSplitBloomFilter::add_hash(uint64_t hash) {
    // most significant 32 bit mod block size as block index(BTW:block size is
    // power of 2)
    // 1. 断言检查：确保当前位图总字节数不小于单个 Block 的字节数（32 字节）
    DCHECK(_num_bytes >= BYTES_PER_BLOCK);
    // 2. 高 32 位取模计算 Block 索引 (bucket_index)
    const uint32_t bucket_index =
            static_cast<uint32_t>(hash >> 32) & (_num_bytes / BYTES_PER_BLOCK - 1);
    //  3. 提取低 32 位作为 Block 内部打点的 key
    uint32_t key = static_cast<uint32_t>(hash);
    // 4. 将 char* 类型的 _data 内存指针强转为 uint32_t*，方便以 4 字节为单位进行访问
    uint32_t* bitset32 = reinterpret_cast<uint32_t*>(_data);

    // Calculate mask for bucket.
    //  5. 调用私有函数，根据低 32 位 key 生成 32 字节（包含 8 个 uint32_t）的比特掩码 block_mask
    BlockMask block_mask;
    _set_masks(key, block_mask);
    //  6. 遍历该 Block 内部的 8 个 uint32_t，将掩码按位或（|=）写入目标 Block 物理内存
    for (int i = 0; i < BITS_SET_PER_BLOCK; i++) {
        bitset32[bucket_index * BITS_SET_PER_BLOCK + i] |= block_mask.item[i];
    }
}

bool BlockSplitBloomFilter::test_hash(uint64_t hash) const {
    // most significant 32 bit mod block size as block index(BTW:block size is
    // power of 2)
    const uint32_t bucket_index =
            static_cast<uint32_t>((hash >> 32) & (_num_bytes / BYTES_PER_BLOCK - 1));
    uint32_t key = static_cast<uint32_t>(hash);
    uint32_t* bitset32 = reinterpret_cast<uint32_t*>(_data);

    // Calculate masks for bucket.
    BlockMask block_mask;
    _set_masks(key, block_mask);

    for (int i = 0; i < BITS_SET_PER_BLOCK; ++i) {
        if (0 == (bitset32[BITS_SET_PER_BLOCK * bucket_index + i] & block_mask.item[i])) {
            return false;
        }
    }
    return true;
}

} // namespace segment_v2
} // namespace doris
