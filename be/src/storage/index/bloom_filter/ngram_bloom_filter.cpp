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

#include "storage/index/bloom_filter/ngram_bloom_filter.h"

#include <gen_cpp/segment_v2.pb.h>
#include <glog/logging.h>

#include "absl/strings/substitute.h"
#include "util/hash/city.h"

namespace doris::segment_v2 {
NGramBloomFilter::NGramBloomFilter(size_t size)
        : _size(size),
          words((size + sizeof(UnderType) - 1) / sizeof(UnderType)),
          filter(words, 0) {}

// for read
Status NGramBloomFilter::init(const char* buf, size_t size, HashStrategyPB strategy) {
    if (size == 0) {
        return Status::InvalidArgument(absl::Substitute("invalid size:$0", size));
    }
    DCHECK(_size == size);

    if (strategy != CITY_HASH_64) {
        return Status::InvalidArgument(absl::Substitute("invalid strategy:$0", strategy));
    }
    words = (_size + sizeof(UnderType) - 1) / sizeof(UnderType);
    filter.reserve(words);
    const auto* from = reinterpret_cast<const UnderType*>(buf);
    for (size_t i = 0; i < words; ++i) {
        filter[i] = from[i];
    }

    return Status::OK();
}
// 并没有进行 N-Gram 切片，而是采用了经典的 Double Hashing（双重哈希）二次探测算法将整段 data 的哈希值打入位图中。
void NGramBloomFilter::add_bytes(const char* data, size_t len) {
    // 1. 使用种子 0 计算字符串的第一个 64 位 CityHash 值
    size_t hash1 = util_hash::CityHash64WithSeed(data, len, 0);
    // 2. 使用固定种子 SEED_GEN 计算字符串的第二个 64 位 CityHash 值
    size_t hash2 = util_hash::CityHash64WithSeed(data, len, SEED_GEN);
    // 3. 循环计算 HASH_FUNCTIONS (常量为 2) 个独立的 Bit 位点
    for (size_t i = 0; i < HASH_FUNCTIONS; ++i) {
        // 利用二次探针公式生成全局 Bit 索引 pos
        // 使用了 Kirsch-Mitzenmacher 优化算法（二次探针生成法），仅通过两次哈希函数计算（hash1 和 hash2），就可以模拟出无限个独立的哈希位置：
        // i * i 二次项探针，引入非线性变化，进一步破坏周期性。
        size_t pos = (hash1 + i * hash2 + i * i) % (8 * _size);
        filter[pos / (8 * sizeof(UnderType))] |= (1ULL << (pos % (8 * sizeof(UnderType))));
    }
}
// 实现文本模糊查询（如 LIKE '%pattern%'）过滤裁剪的最核心裁剪逻辑。
// 执行集合包含关系的快速位图校验（Subset Verification via Bitwise AND）。
bool NGramBloomFilter::contains(const BloomFilter& bf_) const {
    const auto& bf = static_cast<const NGramBloomFilter&>(bf_);
    for (size_t i = 0; i < words; ++i) {
        if ((filter[i] & bf.filter[i]) != bf.filter[i]) {
            return false;
        }
    }
    return true;
}
} // namespace doris::segment_v2
