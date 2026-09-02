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
#include <unordered_map>
#include <vector>

#include "core/field.h"
#include "storage/index/analyzer_key_matcher.h"
#include "storage/index/index_iterator.h"
#include "storage/index/inverted/inverted_index_parser.h"
#include "storage/index/inverted/inverted_index_reader.h"

namespace doris::segment_v2 {
// 向倒排索引提交查询请求时传入的统一参数包。
struct InvertedIndexParam {
    // 待查询的列名称。
    std::string column_name;
    // 列的数据类型（如 Int32, String 等）。
    DataTypePtr column_type;
    // 谓词查询的目标值（如 "apple" 或 100）。
    Field query_value;
    // 查询类型枚举（如 EQUAL, LESS_THAN, MATCH_PHRASE 等）。
    InvertedIndexQueryType query_type;
    // 当前 Segment 的总行数。
    uint32_t num_rows;
    // 输出/结果位图指针，满足查询条件的行号会被写入其中。
    std::shared_ptr<roaring::Roaring> roaring;
    // 标记是否跳过预查/试查逻辑（Direct Query）。
    bool skip_try = false;
    // Pointer to analyzer context (can be nullptr if not needed)
    // Used by FullTextIndexReader for tokenization
    // 分词器上下文指针（用于全文检索的分词处理，可为 nullptr）。
    const InvertedIndexAnalyzerCtx* analyzer_ctx = nullptr;
};

// Entry representing an inverted index reader with its type and analyzer key.
// Used by InvertedIndexIterator and AnalyzerKeyMatcher for reader selection.
// 记录包含类型和分词器 Key 的 Reader 实体，用于 Reader 的挑选与匹配。
struct ReaderEntry {
    // Reader 类型（如 FULLTEXT, STRING_TYPE, BKD）。
    InvertedIndexReaderType type;
    // 分词器标识 Key（规范化为小写，如 "chinese"、"standard"）。
    std::string analyzer_key;
    // 真正的倒排索引读取器共享指针。
    InvertedIndexReaderPtr reader;
};
// InvertedIndexIterator 是 Apache Doris 存储层（Segment V2）中负责倒排索引查询执行与 Reader 路由选择的核心迭代器类。
// InvertedIndexIterator 的主要职责包括：
// 多 Reader 统一管理与路由：作为容器，维护该列下所有的 InvertedIndexReader 实例。根据上层谓词查询（如 EQUAL, MATCH_ANY, RANGE 等）、数据类型（字符串、数值等）以及分词器 Key（analyzer_key），智能路由选择最优的 Reader。
// 驱动索引计算：实现基类 IndexIterator 的核心接口 read_from_index，将上层传入的查询条件下推至选中的 Reader 进行高效计算，并输出匹配的行号位图（Roaring Bitmap）。
// NULL 值索引加速：支持快速提取和查询 NULL 值的行号位图。
class InvertedIndexIterator : public IndexIterator {
public:
    InvertedIndexIterator();
    ~InvertedIndexIterator() override = default;
    // type：倒排索引 Reader 类型。
    // reader：指向具体 Reader 实例的共享指针。
    // 向迭代器中注册一个 InvertedIndexReader，同时自动提取其 analyzer_key 进行规范化处理，并建立 _key_to_entries 哈希索引。
    void add_reader(InvertedIndexReaderType type, const InvertedIndexReaderPtr& reader);

    // Note: analyzer_ctx is now passed via InvertedIndexParam.analyzer_ctx
    // const IndexParam& param（向上转型基类参数，实际会转换为 InvertedIndexParam）。
    // 索引查询核心入口函数。提取查询参数，自动调用 select_best_reader 挑选最佳 Reader，随后将查询下推给 Reader 并将结果写入位图。
    Status read_from_index(const IndexParam& param) override;
    // cache_handle（用于接收/缓存 null 位图的句柄指针）。
    // 从关联的 Reader 中读取保存了标示 NULL 值行号的位图。
    Status read_null_bitmap(InvertedIndexQueryCacheHandle* cache_handle) override;
    // 检查当前列的倒排索引中是否包含 NULL 值行。
    [[nodiscard]] Result<bool> has_null() override;
    // reader_type（索引 Reader 通用类型）。
    // 根据传入的通用索引类型，从存储的 Reader 列表中查找并返回对应的基类 IndexReaderPtr 指针。
    IndexReaderPtr get_reader(IndexReaderType reader_type) const override;
    // column_type：列数据类型。
    // query_type：查询谓词类型。
    // analyzer_key：指定的分词器 Key。
    // 根据列类型、查询类型和分词器 Key，分发选择最佳 Reader 的主路由函数。内部会根据 column_type 决定调用 select_for_text 还是 select_for_numeric。
    [[nodiscard]] Result<InvertedIndexReaderPtr> select_best_reader(
            const DataTypePtr& column_type, InvertedIndexQueryType query_type,
            const std::string& analyzer_key);

    [[nodiscard]] Result<InvertedIndexReaderPtr> select_best_reader(
            const std::string& analyzer_key);

private:
    ENABLE_FACTORY_CREATOR(InvertedIndexIterator);
    // reader：选中的倒排索引 Reader。
    // column_name：列名。
    // query_value：查询值。
    // query_type：查询类型。
    // count：输出参数，返回索引匹配到的行数。
    // 索引试查（Try Read）或评估函数。在正式读取位图前测试索引查询开销/估算匹配行数，决定后续是否走倒排索引加速。
    Status try_read_from_inverted_index(const InvertedIndexReaderPtr& reader,
                                        const std::string& column_name, const Field& query_value,
                                        InvertedIndexQueryType query_type, size_t* count);

    // Normalize analyzer_key to lowercase.
    // Empty input stays empty (means "user did not specify").
    // 规范化分词器 Key（将字符全转为小写）。若输入为空则保持为空。
    static std::string ensure_normalized_key(const std::string& analyzer_key);

    // Select best reader for text (string) columns.
    // Handles FULLTEXT vs STRING_TYPE priority based on query type.
    // Returns BYPASS error if explicit analyzer not found.

    // match：分词器匹配结果。
    // query_type：查询类型。
    // analyzer_key：分词器 Key。
    // 文本/字符串列（Text/String）的 Reader 专属选择逻辑：处理 FULLTEXT（全文检索）与 STRING_TYPE（字符串精确匹配/前缀匹配等）之间的优先级；
    // 若用户明确指定了不存在的 analyzer_key，则返回 BYPASS 错误跳过索引。
    [[nodiscard]] Result<InvertedIndexReaderPtr> select_for_text(const AnalyzerMatchResult& match,
                                                                 InvertedIndexQueryType query_type,
                                                                 const std::string& analyzer_key);

    // Select best reader for numeric columns.
    // Handles BKD priority for range queries.
    // match：分词器匹配结果。
    // query_type：查询类型。
    // 数值列（Numeric）的 Reader 专属选择逻辑：在进行范围查询（Range Query，如 LESS_THAN、GREATER_THAN）时优先选择 BKD 树索引 Reader。
    [[nodiscard]] Result<InvertedIndexReaderPtr> select_for_numeric(
            const AnalyzerMatchResult& match, InvertedIndexQueryType query_type);

    // THREAD SAFETY: _reader_entries and _key_to_entries are populated during initialization
    // phase (via add_reader) and only read during query phase (via read_from_index/select_best_reader).
    // These two phases are guaranteed not to overlap, so no synchronization is needed.
    // Do NOT call add_reader() after any read_from_index() call on the same iterator.
    // 顺序保存当前列绑定的所有 Reader 实体列表。
    std::vector<ReaderEntry> _reader_entries;

    // Index for O(1) lookup by analyzer_key. Maps normalized key to indices in _reader_entries.
    // Built incrementally in add_reader().
    // 以规范化后的 analyzer_key 为键建立的哈希索引表，值为 _reader_entries 中的下标数组。实现依据 analyzer_key 进行 $O(1)$ 时间复杂度的快速 Reader 定位。
    std::unordered_map<std::string, std::vector<size_t>> _key_to_entries;
};

} // namespace doris::segment_v2