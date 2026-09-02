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

#include <bvar/reducer.h>
#include <gen_cpp/segment_v2.pb.h>
#include <glog/logging.h>
#include <string.h>

#include <cstdint>
#include <functional>
#include <memory>

#include "common/status.h"
#include "util/hash/murmur_hash3.h"

namespace doris::segment_v2 {
// 这些变量用于实时追踪和监控 Doris 节点内部 Bloom Filter 的数量和内存消耗
// 当前系统中处于存活状态的 BloomFilter 总对象数。
inline bvar::Adder<int64_t> g_total_bloom_filter_num("doris_total_bloom_filter_num");
// 用于读取/查询路径上的 BloomFilter 实例数量。
inline bvar::Adder<int64_t> g_read_bloom_filter_num("doris_read_bloom_filter_num");
// 用于写入/构建路径上的 BloomFilter 实例数量。
inline bvar::Adder<int64_t> g_write_bloom_filter_num("doris_write_bloom_filter_num");
// 所有存活 BloomFilter 占用的总内存字节数。
inline bvar::Adder<int64_t> g_total_bloom_filter_total_bytes("doris_total_bloom_filter_bytes");
inline bvar::Adder<int64_t> g_read_bloom_filter_total_bytes("doris_read_bloom_filter_bytes");
inline bvar::Adder<int64_t> g_write_bloom_filter_total_bytes("doris_write_bloom_filter_bytes");

inline bvar::Adder<int64_t> g_pk_total_bloom_filter_num("doris_pk_total_bloom_filter_num");
inline bvar::Adder<int64_t> g_pk_read_bloom_filter_num("doris_pk_read_bloom_filter_num");
inline bvar::Adder<int64_t> g_pk_write_bloom_filter_increase_num(
        "doris_pk_write_bloom_filter_increase_num");
inline bvar::Adder<int64_t> g_pk_write_bloom_filter_decrease_num(
        "doris_pk_write_bloom_filter_decrease_num");

inline bvar::Adder<int64_t> g_pk_total_bloom_filter_total_bytes(
        "doris_pk_total_bloom_filter_bytes");
inline bvar::Adder<int64_t> g_pk_read_bloom_filter_total_bytes("doris_pk_read_bloom_filter_bytes");
inline bvar::Adder<int64_t> g_pk_write_bloom_filter_increase_bytes(
        "doris_pk_write_bloom_filter_increase_bytes");
inline bvar::Adder<int64_t> g_pk_write_bloom_filter_decrease_bytes(
        "doris_pk_write_bloom_filter_decrease_bytes");

struct BloomFilterOptions {
    // false positive probability
    // 默认误判率（False Positive Probability），默认为 5%。
    double fpp = 0.05;
    // 哈希计算策略，默认使用 MurmurHash3_x64_64。
    HashStrategyPB strategy = HASH_MURMUR3_X64_64;
};

// Base class for bloom filter
// To support null value, the size of bloom filter is optimize bytes + 1.
// The last byte is for null value flag.
// 存储层布隆过滤器（Bloom Filter）的基类及相关全局监控指标。它属于 Segment V2 存储引擎架构，主要用于在存储层（Segment/Page）进行数据裁剪与谓词下推，快速判定某个值“绝对不存在”或“可能存在”，从而大幅减少磁盘 I/O。
// doris::segment_v2::BloomFilter 是所有布隆过滤器实现（如 Block-Based Bloom Filter、NGram Bloom Filter 等）的抽象基类，主要作用包括：
// 统一接口封装：定义了布隆过滤器初始化的 init、写入的 add_bytes/add_hash、查询的 test_bytes/test_hash 以及合并的 merge 等虚接口。
// 支持 NULL 值处理：标准的布隆过滤器无法区分空值（NULL）。Doris 在这里采用了一个巧妙的优化：总字节数 = 优化计算的比特字节数 (_num_bytes) + 1 字节。这多出来的最后一个字节专门用作 _has_null 标志位。
// 运行时内存与读写指标监控：集成 bvar（百度开源的指标监控库），实时统计当前 BE 节点中布隆过滤器的创建数量、读写数量以及内存占用（包括 Primary Key 相关的布隆过滤器指标）。
// 哈希策略与位运算工具：提供统一的 MurmurHash3 计算逻辑以及基于预期数据量和误判率（FPP）自动计算最优 Bit 长度的数学工具方法。
class BloomFilter {
public:
    // Default seed for the hash function. It comes from date +%s.
    // 哈希函数的默认种子（Seed），来源于 Linux 时间戳 date +%s。
    static const uint32_t DEFAULT_SEED = 1575457558;

    // Minimum Bloom filter size, set to the size of a tiny Bloom filter block
    // Bloom Filter 最小字节数（32 字节，即 256 比特，对应一个 Tiny Block）。
    static const uint32_t MINIMUM_BYTES = 32;

    // Maximum Bloom filter size, set it to half of max segment file size
    // Bloom Filter 允许的最大字节数（128 MB）。
    static const uint32_t MAXIMUM_BYTES = 128 * 1024 * 1024;

    // Factory function for BloomFilter
    // 根据传入的算法类型 algorithm（如 BLOCK_BLOOM_FILTER 或 NGRAM_BLOOM_FILTER）实例化对应的子类对象，并将其写入 unique_ptr 中。
    static Status create(BloomFilterAlgorithmPB algorithm, std::unique_ptr<BloomFilter>* bf,
                         size_t bf_size = 0);

    BloomFilter() { g_total_bloom_filter_num << 1; }

    virtual ~BloomFilter() {
        if (_data) {
            if (_is_write) {
                g_write_bloom_filter_total_bytes << -static_cast<int64_t>(_size);
                g_write_bloom_filter_num << -1;
            } else {
                g_read_bloom_filter_total_bytes << -static_cast<int64_t>(_size);
                g_read_bloom_filter_num << -1;
            }
            g_total_bloom_filter_total_bytes << -static_cast<int64_t>(_size);
            delete[] _data;
        }
        g_total_bloom_filter_num << -1;
    }
    // 判断当前实例是否为用于文本/字符串匹配的 N-Gram Bloom Filter。基类默认返回 false。
    virtual bool is_ngram_bf() const { return false; }

    // for write
    // 写入端常用） 传入预估数据量 n、误判率 fpp 和哈希策略，内部先根据公式计算最优 Bit 数并换算为 Byte，再调用重载的 init 方法。
    Status init(uint64_t n, double fpp, HashStrategyPB strategy) {
        return this->init(optimal_bit_num(n, fpp) / 8, strategy);
    }
    // 重载函数。仅指定字节数 filter_size，默认采用 HASH_MURMUR3_X64_64 哈希算法进行初始化。
    virtual Status init(uint64_t filter_size) { return init(filter_size, HASH_MURMUR3_X64_64); }
    // 写路径底层初始化） 校验哈希策略，断言 filter_size 必须为 2 的幂；
    // 分配 filter_size + 1 字节的内存并清零；将 _has_null 指向末尾字节；标记 _is_write = true；更新全局写监控指标。
    virtual Status init(uint64_t filter_size, HashStrategyPB strategy) {
        if (strategy == HASH_MURMUR3_X64_64) {
            _hash_func = murmur_hash3_x64_64;
        } else {
            return Status::InvalidArgument("invalid strategy:{}", strategy);
        }
        _num_bytes = filter_size;
        DCHECK((_num_bytes & (_num_bytes - 1)) == 0);
        _size = _num_bytes + 1;
        // reserve last byte for null flag
        _data = new char[_size];
        memset(_data, 0, _size);
        _has_null = (bool*)(_data + _num_bytes);
        *_has_null = false;
        _is_write = true;
        g_write_bloom_filter_num << 1;
        g_write_bloom_filter_total_bytes << _size;
        g_total_bloom_filter_total_bytes << _size;
        return Status::OK();
    }

    // for read
    // use deep copy to acquire the data
    // （读路径底层初始化） 从磁盘/网络读取到的二进制 Buffer (buf) 中深拷贝构建 BloomFilter。对 Buffer 大小进行合法性检查（必须大于 1 且 size-1 必须为 2 的幂），初始化指针与监控指标。
    virtual Status init(const char* buf, size_t size, HashStrategyPB strategy) {
        if (size <= 1) {
            return Status::InvalidArgument("invalid size:{}", size);
        }
        DCHECK(size > 1);
        if (strategy == HASH_MURMUR3_X64_64) {
            _hash_func = murmur_hash3_x64_64;
        } else {
            return Status::InvalidArgument("invalid strategy:{}", strategy);
        }
        if (buf == nullptr) {
            return Status::InvalidArgument("buf is nullptr");
        }
        if (((size - 1) & (size - 2)) != 0) {
            return Status::InvalidArgument("size - 1 must be power of two");
        }
        _data = new char[size];
        memcpy(_data, buf, size);
        _size = size;
        _num_bytes = _size - 1;
        DCHECK((_num_bytes & (_num_bytes - 1)) == 0);
        _has_null = (bool*)(_data + _num_bytes);
        g_read_bloom_filter_num << 1;
        g_read_bloom_filter_total_bytes << _size;
        g_total_bloom_filter_total_bytes << _size;
        return Status::OK();
    }
    // 使用 memset 将 _data 内存全部清零（重置所有 Bit 位以及 _has_null 标志）。
    void reset() { memset(_data, 0, _size); }
    // 使用内部保存的 _hash_func 和 DEFAULT_SEED，对指定的字节数组计算 64 位哈希值。
    uint64_t hash(const char* buf, size_t size) const {
        uint64_t hash_code;
        _hash_func(buf, size, DEFAULT_SEED, &hash_code);
        return hash_code;
    }
    // 静态哈希工具函数。无需实例化对象即可对指定 Buffer 计算哈希值。
    static Result<uint64_t> hash(const char* buf, size_t size, HashStrategyPB strategy) {
        if (strategy == HASH_MURMUR3_X64_64) {
            uint64_t hash_code;
            murmur_hash3_x64_64(buf, size, DEFAULT_SEED, &hash_code);
            return hash_code;
        } else {
            return Status::InvalidArgument("invalid strategy:{}", strategy);
        }
    }
    // 向 Bloom Filter 中添加数据。如果 buf == nullptr，则直接将 *_has_null 置为 true；否则先计算其哈希值，再调用纯虚函数 add_hash(code) 将哈希值打入位图。
    virtual void add_bytes(const char* buf, size_t size) {
        if (buf == nullptr) {
            *_has_null = true;
            return;
        }
        uint64_t code = hash(buf, size);
        add_hash(code);
    }
    // 查询数据是否存在。如果 buf == nullptr，直接返回 *_has_null 的值；否则计算哈希值并调用纯虚函数 test_hash(code) 返回判定结果。
    virtual bool test_bytes(const char* buf, size_t size) const {
        if (buf == nullptr) {
            return *_has_null;
        }
        uint64_t code = hash(buf, size);
        return test_hash(code);
    }

    /// Checks if this contains everything from another bloom filter.
    /// Bloom filters must have equal size and seed.
    // 校验当前 Bloom Filter 是否包含另一个 Bloom Filter 的所有集合元素（基类默认返回 true，子类可按需实现）。
    virtual bool contains(const BloomFilter& bf_) const { return true; };
    // 返回底层物理内存指针 _data（常用于写盘序列化）。
    virtual const char* data() const { return _data; }
    // 返回纯位图字节数 _num_bytes。
    size_t num_bytes() const { return _num_bytes; }
    // 返回物理缓冲区总大小 _size (_num_bytes + 1)。
    virtual size_t size() const { return _size; }
    // 手动设置 *_has_null 标志位。
    virtual void set_has_null(bool has_null) { *_has_null = has_null; }
    // 读取当前 Bloom Filter 是否包含空值（NULL）。
    virtual bool has_null() const { return *_has_null; }
    // 根据计算好的 64 位哈希值，将其映射并设置到 Bloom Filter 具体的 Bit 数组/ Block 中。
    virtual void add_hash(uint64_t hash) = 0;
    // 根据 64 位哈希值，检查其对应的 Bit 位/ Block 是否全为 1。
    virtual bool test_hash(uint64_t hash) const = 0;
    // 合并（求并集）另一个相同大小的 Bloom Filter。原理是对 _data 数组中的每个字节逐字节进行按位或（按位 OR）操作，同时合并 has_null 标志。
    Status merge(const BloomFilter* other) {
        DCHECK(other->size() == _size);
        for (uint32_t i = 0; i < other->size(); i++) {
            _data[i] |= other->_data[i];
        }
        return Status::OK();
    }
    // 计算传入的 64 位整数二进制表示中，最高位 1 所在的位置（即占用多少个 Bit）。
    static uint32_t used_bits(uint64_t value);

    // Compute the optimal bit number according to the following rule:
    //     m = -n * ln(fpp) / (ln(2) ^ 2)
    // n: expected distinct record number
    // fpp: false positive probability
    // the result will be power of 2
    // 根据预估元素个数 $n$ 和期望误判率 $fpp$，依据公式计算最佳 Bit 数量：
    //  m = -n * ln(fpp) / (ln(2) ^ 2)
    static uint32_t optimal_bit_num(uint64_t n, double fpp);

protected:
    // bloom filter data
    // specially add one byte for null flag
    // 指向动态分配的物理内存连续缓冲区。前 _num_bytes 字节用于存储真正的 Bloom Filter 位图，最后一个字节（即第 _num_bytes + 1 个字节）用于存储 NULL 标记。
    char* _data = nullptr;
    // optimal bloom filter num bytes
    // it is calculated by optimal_bit_num() / 8
    // 纯 Bloom Filter 位图所占用的字节数（不包含 NULL 标志字节）。必须是 2 的幂次方（Power of Two）。
    size_t _num_bytes = 0;
    // equal to _num_bytes + 1
    // last byte is for has_null flag
    // _data 缓冲区的实际物理大小，恒等于 _num_bytes + 1。
    size_t _size = 0;
    // last byte's pointer in data for null flag
    // 指针，直接指向 _data + _num_bytes（即缓冲区的最后一个字节），解引用后作为布尔标志位，标记当前索引的数据集中是否包含 NULL 值。
    bool* _has_null = nullptr;
    // is this bf used for write
    // 标识该 BloomFilter 实例是用于写入构建（true）还是读取查询（false）。析构时会根据此标志位回减对应的 bvar 监控计数。
    bool _is_write = false;
    // 保存哈希函数的函数对象（例如 murmur_hash3_x64_64），避免在 Hot Path 中重复判断哈希策略。
    std::function<void(const void*, const int64_t, const uint64_t, void*)> _hash_func;
};

} // namespace doris::segment_v2
