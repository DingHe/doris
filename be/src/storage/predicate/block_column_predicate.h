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

#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <ostream>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/factory_creator.h"
#include "common/status.h"
#include "core/column/column.h"
#include "format/parquet/parquet_predicate.h"
#include "storage/olap_common.h"
#include "storage/predicate/column_predicate.h"

namespace roaring {
class Roaring;
} // namespace roaring

namespace doris {
namespace segment_v2 {
class BloomFilter;
class InvertedIndexIterator;
} // namespace segment_v2

// Block Column Predicate support do column predicate and support OR and AND predicate
// Block Column Predicate will replace column predicate as a unified external vectorized interface
// in the future
// TODO: support do predicate on Bitmap and ZoneMap, So we can use index of column to do predicate on
// page and segment
// 块级列谓词（Block Column Predicate）的抽象基类
// 核心作用是为复合逻辑谓词（如 AND、OR 条件）以及单列谓词（ColumnPredicate）提供统一的块级评估与索引剪枝抽象。
// 统一向量化过滤接口：定义在数据块（MutableColumns / Block）上进行逐行过滤或选择操作（Selected Vector）的虚接口，支持向量化引擎的过滤求值。
// 多级索引剪枝（Indexing Pruning）：抽象出 ZoneMap 粗粒度范围过滤、BloomFilter 布隆过滤器过滤、字典编码（Dict）匹配以及倒排索引（Inverted Index）评估的统一接口。
// 支持复合谓词树：派生出 AndBlockColumnPredicate 与 OrBlockColumnPredicate，使得简单谓词可以按逻辑树形结构嵌套（如 (A AND B) OR C），并将逻辑运算隐式吸收到索引剪枝与向量化求值过程中。
class BlockColumnPredicate {
public:
    BlockColumnPredicate() = default;
    virtual ~BlockColumnPredicate() = default;
    // 参数：column_id_set - 用于接收所有关联 Column ID 的集合引用。
    // 递归收集当前谓词及其所有子谓词中涉及到的所有列的唯一 ID（ColumnId）。存储层利用此接口知道哪些列参与了谓词计算，从而决定是否加载对应列的物理数据或索引。
    virtual void get_all_column_ids(std::set<ColumnId>& column_id_set) const = 0;
    // predicate_set - 用于接收单列谓词指针的集合引用。
    // 提取并展开当前谓词树中包含的所有底层底层单列谓词（ColumnPredicate）。主要用于下推优化时获取最原子层的过滤算子。
    virtual void get_all_column_predicate(
            std::set<std::shared_ptr<const ColumnPredicate>>& predicate_set) const = 0;
    // 数据块（Block）向量化求值方法
    // block - 待评估的向量化列数据数组（Block 中的列集合）。
    // sel - 选择向量数组（Selection Vector），存储当前通过过滤条件的物理行号索引。
    // selected_size - 输入的合法行号数量。
    // 返回值：通过当前谓词计算后剩余的合法行号数量。
    // 作用：基于 Selection Vector 的过滤求值。对输入的 sel 数组原地筛选，剔除不满足当前谓词的行，并将保留下来的行号原地收缩存放，返回新的数量。默认实现不做筛选，直接返回原大小。
    virtual uint16_t evaluate(MutableColumns& block, uint16_t* sel, uint16_t selected_size) const {
        return selected_size;
    }
    // 针对选定行进行 与（AND） 逻辑运算。计算结果将以flags[i] = flags[i] && result 的方式写回 flags 数组。
    virtual void evaluate_and(MutableColumns& block, uint16_t* sel, uint16_t selected_size,
                              bool* flags) const {}
    // 针对选定行进行 或（OR） 逻辑运算。计算结果将以 flags[i] = flags[i] || result 的方式写回 flags 数组。
    virtual void evaluate_or(MutableColumns& block, uint16_t* sel, uint16_t selected_size,
                             bool* flags) const {}
    // 全量向量化求值接口。直接对 Block 中的前 size 行做谓词判断，并将布尔结果（true/false）直接填充到 flags 数组中。
    virtual void evaluate_vec(MutableColumns& block, uint16_t size, bool* flags) const {}
    // 若当前谓词支持使用 ZoneMap/Statistics 进行剪枝过滤返回 true；否则返回 false。
    // 作用：用于判断该谓词能力是否具备 ZoneMap 裁切属性（默认返回 true）。部分复杂谓词（如正则表达式或函数调用）可能不支持 ZoneMap，会覆盖此接口返回 false。
    virtual bool support_zonemap() const { return true; }
    // 参数：zone_map - 当前 Segment/Page 的 ZoneMap 统计信息（包含 Max/Min 值、HasNull 等）。
    // 返回值：true 表示可能包含满足条件的数据（无法裁剪，需要继续读取）；false 表示绝对不包含满足条件的数据（安全跳过该数据块）。
    virtual bool evaluate_and(const segment_v2::ZoneMap& zone_map) const {
        throw Exception(Status::FatalError("should not reach here"));
    }
    // 参数：statistic - Parquet 文件的列统计信息（Min/Max/NullCount）。
    // 作用：针对外表 Parquet 格式的列统计信息（ColumnStat）做谓词剪枝评估。基类默认抛出 FatalError 异常。
    virtual bool evaluate_and(ParquetPredicate::ColumnStat* statistic) const {
        throw Exception(Status::FatalError("should not reach here"));
    }

    /**
     * For Parquet page indexes, since the number of rows filtered by each column's page index is not the same,
     * a `RowRanges` is needed to represent the range of rows to be read after filtering. If no rows need to
     * be read, it returns false; otherwise, it returns true. Because the page index needs to be
     * parsed, `CachedPageIndexStat` is used to avoid repeatedly parsing the page index information
     * of the same column.
     */
    // 作用：针对 Parquet 的 Page Index 做细粒度页级剪枝，并将需要读取的行号追加记录到 row_ranges 中。使用缓存统计对象避开重复解析相同的 Page Index。基类默认抛出 FatalError 异常。
    virtual bool evaluate_and(ParquetPredicate::CachedPageIndexStat* statistic,
                              RowRanges* row_ranges) const {
        throw Exception(Status::FatalError("should not reach here"));
    }
    // 参数：bf - 针对当前数据块构建的 BloomFilter（布隆过滤器）指针。
    virtual bool evaluate_and(const segment_v2::BloomFilter* bf) const {
        throw Exception(Status::FatalError("should not reach here"));
    }
    // dict_words - 当前 Page/Segment 字符串字典的词条数组。
    // dict_num - 字典中词条的总数量。
    virtual bool evaluate_and(const StringRef* dict_words, const size_t dict_num) const {
        throw Exception(Status::FatalError("should not reach here"));
    }
    // 参数：ngram - 是否为 N-Gram 类型的 BloomFilter。
    // 作用：用于判断某些谓词类型（如 LIKE '%abc%'）是否满足使用（或 N-Gram 扩展的）BloomFilter 索引剪枝的先决条件。
    virtual bool can_do_bloom_filter(bool ngram) const { return false; }

    //evaluate predicate on inverted
    // 利用倒排索引（Inverted Index）快速查找匹配的行号。基类默认返回 INVERTED_INDEX_NOT_IMPLEMENTED 错误，由支持倒排索引的派生类重写实现。
    virtual Status evaluate(const std::string& column_name, InvertedIndexIterator* iterator,
                            uint32_t num_rows, roaring::Roaring* bitmap) const {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_IMPLEMENTED>(
                "Not Implemented evaluate with inverted index, please check the predicate");
    }
};

class SingleColumnBlockPredicate : public BlockColumnPredicate {
    ENABLE_FACTORY_CREATOR(SingleColumnBlockPredicate);

public:
    explicit SingleColumnBlockPredicate(const std::shared_ptr<const ColumnPredicate>& pre)
            : _predicate(pre) {}

    void get_all_column_ids(std::set<ColumnId>& column_id_set) const override {
        column_id_set.insert(_predicate->column_id());
    }

    void get_all_column_predicate(
            std::set<std::shared_ptr<const ColumnPredicate>>& predicate_set) const override {
        predicate_set.insert(_predicate);
    }

    uint16_t evaluate(MutableColumns& block, uint16_t* sel, uint16_t selected_size) const override;
    void evaluate_and(MutableColumns& block, uint16_t* sel, uint16_t selected_size,
                      bool* flags) const override;
    bool support_zonemap() const override { return _predicate->support_zonemap(); }
    bool evaluate_and(const segment_v2::ZoneMap& zone_map) const override;
    bool evaluate_and(ParquetPredicate::ColumnStat* statistic) const override {
        return _predicate->evaluate_and(statistic);
    }

    bool evaluate_and(ParquetPredicate::CachedPageIndexStat* statistic,
                      RowRanges* row_ranges) const override {
        return _predicate->evaluate_and(statistic, row_ranges);
    }
    bool evaluate_and(const segment_v2::BloomFilter* bf) const override;
    bool evaluate_and(const StringRef* dict_words, const size_t dict_num) const override;
    void evaluate_or(MutableColumns& block, uint16_t* sel, uint16_t selected_size,
                     bool* flags) const override;

    void evaluate_vec(MutableColumns& block, uint16_t size, bool* flags) const override;

    bool can_do_bloom_filter(bool ngram) const override {
        return _predicate->can_do_bloom_filter(ngram);
    }

private:
    const std::shared_ptr<const ColumnPredicate> _predicate = nullptr;
};

class MutilColumnBlockPredicate : public BlockColumnPredicate {
public:
    MutilColumnBlockPredicate() = default;

    ~MutilColumnBlockPredicate() override = default;

    bool support_zonemap() const override {
        for (const auto& child_block_predicate : _block_column_predicate_vec) {
            if (!child_block_predicate->support_zonemap()) {
                return false;
            }
        }

        return true;
    }

    void add_column_predicate(std::unique_ptr<BlockColumnPredicate> column_predicate) {
        _block_column_predicate_vec.push_back(std::move(column_predicate));
    }

    size_t num_of_column_predicate() const { return _block_column_predicate_vec.size(); }

    void get_all_column_ids(std::set<ColumnId>& column_id_set) const override {
        for (auto& child_block_predicate : _block_column_predicate_vec) {
            child_block_predicate->get_all_column_ids(column_id_set);
        }
    }

    void get_all_column_predicate(
            std::set<std::shared_ptr<const ColumnPredicate>>& predicate_set) const override {
        for (auto& child_block_predicate : _block_column_predicate_vec) {
            child_block_predicate->get_all_column_predicate(predicate_set);
        }
    }

protected:
    std::vector<std::unique_ptr<BlockColumnPredicate>> _block_column_predicate_vec;
};

class OrBlockColumnPredicate : public MutilColumnBlockPredicate {
    ENABLE_FACTORY_CREATOR(OrBlockColumnPredicate);

public:
    uint16_t evaluate(MutableColumns& block, uint16_t* sel, uint16_t selected_size) const override;
    void evaluate_and(MutableColumns& block, uint16_t* sel, uint16_t selected_size,
                      bool* flags) const override;
    void evaluate_or(MutableColumns& block, uint16_t* sel, uint16_t selected_size,
                     bool* flags) const override;
    bool evaluate_and(ParquetPredicate::ColumnStat* statistic) const override {
        if (num_of_column_predicate() == 1) {
            return _block_column_predicate_vec[0]->evaluate_and(statistic);
        } else {
            for (int i = 0; i < num_of_column_predicate(); ++i) {
                if (_block_column_predicate_vec[i]->evaluate_and(statistic)) {
                    return true;
                }
            }
            return false;
        }
    }

    bool evaluate_and(ParquetPredicate::CachedPageIndexStat* statistic,
                      RowRanges* row_ranges) const override;

    // note(wb) we didnt't implement evaluate_vec method here, because storage layer only support AND predicate now;
};

class AndBlockColumnPredicate : public MutilColumnBlockPredicate {
    ENABLE_FACTORY_CREATOR(AndBlockColumnPredicate);

public:
    uint16_t evaluate(MutableColumns& block, uint16_t* sel, uint16_t selected_size) const override;
    void evaluate_and(MutableColumns& block, uint16_t* sel, uint16_t selected_size,
                      bool* flags) const override;
    void evaluate_or(MutableColumns& block, uint16_t* sel, uint16_t selected_size,
                     bool* flags) const override;

    void evaluate_vec(MutableColumns& block, uint16_t size, bool* flags) const override;

    bool evaluate_and(const segment_v2::ZoneMap& zone_map) const override;

    bool evaluate_and(const segment_v2::BloomFilter* bf) const override;

    bool evaluate_and(const StringRef* dict_words, const size_t dict_num) const override;

    bool evaluate_and(ParquetPredicate::ColumnStat* statistic) const override {
        for (auto& block_column_predicate : _block_column_predicate_vec) {
            if (!block_column_predicate->evaluate_and(statistic)) {
                return false;
            }
        }
        return true;
    }

    bool evaluate_and(ParquetPredicate::CachedPageIndexStat* statistic,
                      RowRanges* row_ranges) const override;

    bool can_do_bloom_filter(bool ngram) const override {
        for (auto& pred : _block_column_predicate_vec) {
            if (!pred->can_do_bloom_filter(ngram)) {
                return false;
            }
        }
        return true;
    }

    Status evaluate(const std::string& column_name, InvertedIndexIterator* iterator,
                    uint32_t num_rows, roaring::Roaring* bitmap) const override;
};

} //namespace doris
