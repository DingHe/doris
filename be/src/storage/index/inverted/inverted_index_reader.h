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

#include <CLucene/util/bkd/bkd_reader.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/data_type/primitive_type.h"
#include "core/field.h"
#include "io/fs/file_system.h"
#include "io/fs/path.h"
#include "storage/index/index_query_context.h"
#include "storage/index/index_reader.h"
#include "storage/index/inverted/inverted_index_cache.h"
#include "storage/index/inverted/inverted_index_compound_reader.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/inverted/inverted_index_parser.h"
#include "storage/index/inverted/inverted_index_query_type.h"
#include "storage/tablet/tablet_schema.h"
#include "util/once.h"

#define FINALIZE_INPUT(x) \
    if (x != nullptr) {   \
        x->close();       \
        _CLDELETE(x);     \
    }
#define FINALLY_FINALIZE_INPUT(x) \
    try {                         \
        FINALIZE_INPUT(x)         \
    } catch (...) {               \
    }

namespace lucene {
namespace store {
class Directory;
} // namespace store
namespace util {
namespace bkd {
class bkd_docid_set_iterator;
} // namespace bkd
} // namespace util
} // namespace lucene
namespace roaring {
class Roaring;
} // namespace roaring

namespace doris {
class KeyCoder;
struct OlapReaderStatistics;
class RuntimeState;

namespace segment_v2 {

class InvertedIndexIterator;
class InvertedIndexQueryCacheHandle;
class IndexFileReader;
class InvertedIndexQueryInfo;
class IndexIterator;

class InvertedIndexResultBitmap {
private:
    std::shared_ptr<roaring::Roaring> _data_bitmap = nullptr;
    std::shared_ptr<roaring::Roaring> _null_bitmap = nullptr;

public:
    // Default constructor
    InvertedIndexResultBitmap() = default;
    ~InvertedIndexResultBitmap() = default;

    // Constructor with arguments
    InvertedIndexResultBitmap(std::shared_ptr<roaring::Roaring> data_bitmap,
                              std::shared_ptr<roaring::Roaring> null_bitmap)
            : _data_bitmap(std::move(data_bitmap)), _null_bitmap(std::move(null_bitmap)) {}

    // Copy constructor
    InvertedIndexResultBitmap(const InvertedIndexResultBitmap& other)
            : _data_bitmap(other._data_bitmap
                                   ? std::make_shared<roaring::Roaring>(*other._data_bitmap)
                                   : nullptr),
              _null_bitmap(other._null_bitmap
                                   ? std::make_shared<roaring::Roaring>(*other._null_bitmap)
                                   : nullptr) {}

    // Move constructor
    InvertedIndexResultBitmap(InvertedIndexResultBitmap&& other) noexcept
            : _data_bitmap(std::move(other._data_bitmap)),
              _null_bitmap(std::move(other._null_bitmap)) {}

    // Copy assignment operator
    InvertedIndexResultBitmap& operator=(const InvertedIndexResultBitmap& other) {
        if (this != &other) { // Prevent self-assignment
            _data_bitmap = other._data_bitmap
                                   ? std::make_shared<roaring::Roaring>(*other._data_bitmap)
                                   : nullptr;
            _null_bitmap = other._null_bitmap
                                   ? std::make_shared<roaring::Roaring>(*other._null_bitmap)
                                   : nullptr;
        }
        return *this;
    }

    // Move assignment operator
    InvertedIndexResultBitmap& operator=(InvertedIndexResultBitmap&& other) noexcept {
        if (this != &other) { // Prevent self-assignment
            _data_bitmap = std::move(other._data_bitmap);
            _null_bitmap = std::move(other._null_bitmap);
        }
        return *this;
    }

    // Operator &=
    InvertedIndexResultBitmap& operator&=(const InvertedIndexResultBitmap& other) {
        if (_data_bitmap && other._data_bitmap) {
            const auto& my_null = _null_bitmap ? *_null_bitmap : _empty_bitmap();
            const auto& ot_null = other._null_bitmap ? *other._null_bitmap : _empty_bitmap();
            auto new_null_bitmap = (*_data_bitmap & ot_null) | (my_null & *other._data_bitmap) |
                                   (my_null & ot_null);
            *_data_bitmap &= *other._data_bitmap;
            if (!_null_bitmap) {
                _null_bitmap = std::make_shared<roaring::Roaring>();
            }
            *_null_bitmap = std::move(new_null_bitmap);
        }
        return *this;
    }

    // Operator |=
    InvertedIndexResultBitmap& operator|=(const InvertedIndexResultBitmap& other) {
        if (_data_bitmap && other._data_bitmap) {
            const auto& my_null = _null_bitmap ? *_null_bitmap : _empty_bitmap();
            const auto& ot_null = other._null_bitmap ? *other._null_bitmap : _empty_bitmap();
            // SQL three-valued logic for OR:
            // - TRUE OR anything = TRUE (not NULL)
            // - FALSE OR NULL = NULL
            // - NULL OR NULL = NULL
            // Result is NULL when the row is NULL on either side while the other side
            // is not TRUE. Rows that become TRUE must be removed from the NULL bitmap.
            *_data_bitmap |= *other._data_bitmap;
            auto new_null_bitmap = (my_null - *other._data_bitmap) | (ot_null - *_data_bitmap);
            new_null_bitmap -= *_data_bitmap;
            if (!_null_bitmap) {
                _null_bitmap = std::make_shared<roaring::Roaring>();
            }
            *_null_bitmap = std::move(new_null_bitmap);
        }
        return *this;
    }

    // NOT operation
    const InvertedIndexResultBitmap& op_not(const roaring::Roaring* universe) const {
        if (_data_bitmap) {
            if (_null_bitmap) {
                *_data_bitmap = *universe - *_data_bitmap - *_null_bitmap;
            } else {
                *_data_bitmap = *universe - *_data_bitmap;
            }
            // The _null_bitmap remains unchanged.
        }
        return *this;
    }

    // Operator -=
    InvertedIndexResultBitmap& operator-=(const InvertedIndexResultBitmap& other) {
        if (_data_bitmap && other._data_bitmap) {
            *_data_bitmap -= *other._data_bitmap;
            if (other._null_bitmap) {
                *_data_bitmap -= *other._null_bitmap;
            }
            if (_null_bitmap && other._null_bitmap) {
                *_null_bitmap -= *other._null_bitmap;
            }
        }
        return *this;
    }

    void mask_out_null() {
        if (_data_bitmap && _null_bitmap) {
            *_data_bitmap -= *_null_bitmap;
        }
    }

    const std::shared_ptr<roaring::Roaring>& get_data_bitmap() const { return _data_bitmap; }

    const std::shared_ptr<roaring::Roaring>& get_null_bitmap() const { return _null_bitmap; }

    // Check if both bitmaps are empty
    bool is_empty() const { return (_data_bitmap == nullptr && _null_bitmap == nullptr); }

private:
    static const roaring::Roaring& _empty_bitmap() {
        static const roaring::Roaring empty;
        return empty;
    }
};
// InvertedIndexReader 是 Apache Doris 存储层（Segment V2）中倒排索引底层读取器的抽象基类（Abstract Base Class）
// 在 Doris 的倒排索引查询体系中，不同数据类型（文本、数值、日期等）和不同索引类型（CLucene 全文检索、字符串精确索引、BKD 树等）在底层的物理结构和查询算法上存在很大差异。
// 定义统一的倒排索引读取规范：为所有具体的倒排索引读取实现（如 FullTextIndexReader、StringTypeInvertedIndexReader、BkdInvertedIndexReader 等）提供一致的查询与元数据访问接口。
// 连接存储层与内存缓存：管理与底层倒排索引物理文件的读取句柄 IndexFileReader，并集成了查询结果缓存（Query Cache）与 Searcher 句柄缓存（Searcher Cache）。
// 提供跨类型的基础能力：实现了读取 NULL 值位图、获取索引文件路径、构建 CLucene IndexSearcher 以及低层级的全文检索搜索匹配（match_index_search）等通用基础设施。
class InvertedIndexReader : public IndexReader {
public:
	// index_meta：指向索引元数据 TabletIndex 的指针。
	// index_file_reader：底层物理文件读取器共享指针。
    explicit InvertedIndexReader(const TabletIndex* index_meta,
                                 std::shared_ptr<IndexFileReader> index_file_reader)
            : _index_file_reader(std::move(index_file_reader)), _index_meta(*index_meta) {}
    virtual ~InvertedIndexReader() = default;
	// IndexType 枚举（固定返回 IndexType::INVERTED）。
    IndexType index_type() override { return IndexType::INVERTED; }

	// context：IndexQueryContextPtr（索引查询上下文，包含执行状态、Runtime state 等）。
	// column_name：待查询的列名称。
	// query_value：Field 谓词目标值（如查询字符串或数值）。
	// query_type：InvertedIndexQueryType 查询类型（如 EQUAL, MATCH_ANY, RANGE 等）。
	// bit_map：std::shared_ptr<roaring::Roaring>& 输出参数，用于接收满足查询条件的匹配行号位图。
	// analyzer_ctx：const InvertedIndexAnalyzerCtx* 分词器上下文指针（可选，默认 nullptr）。
	// 核心执行查询接口。子类必须实现此函数，在具体的索引结构上执行查询，并将命中行号填充至 bit_map 中。
    virtual Status query(const IndexQueryContextPtr& context, const std::string& column_name,
                         const Field& query_value, InvertedIndexQueryType query_type,
                         std::shared_ptr<roaring::Roaring>& bit_map,
                         const InvertedIndexAnalyzerCtx* analyzer_ctx = nullptr) = 0;
	// 试查/评估接口。计算符合条件的行数而不填充整个位图，主要用于 InvertedIndexIterator 中评估选择率（判断是否需要 Bypass 倒排索引）。
    virtual Status try_query(const IndexQueryContextPtr& context, const std::string& column_name,
                             const Field& query_value, InvertedIndexQueryType query_type,
                             size_t* count) = 0;
	// 从索引文件中读取记录了列上所有 NULL 值所在行号的位图（Null Bitmap），并尝试将其存入 Query Cache 以供下次复用。
    Status read_null_bitmap(const IndexQueryContextPtr& context,
                            InvertedIndexQueryCacheHandle* cache_handle,
                            lucene::store::Directory* dir = nullptr);
	// InvertedIndexReaderType（如 FULLTEXT, STRING_TYPE, BKD 等）。
    virtual InvertedIndexReaderType type() = 0;
	// 获取该倒排索引在元数据中对应的唯一索引 ID（index_id）。
    [[nodiscard]] uint64_t get_index_id() const override { return _index_meta.index_id(); }
	// 获取建表时为该索引指定的属性表（如 "parser"="chinese" 等）。包含 MOCK_FUNCTION 宏说明其可在单元测试中被 Mock。
    [[nodiscard]] MOCK_FUNCTION const std::map<std::string, std::string>& get_index_properties()
            const {
        return _index_meta.properties();
    }
	// 判断该索引列中是否可能包含 NULL 值。
    [[nodiscard]] bool has_null() const { return _has_null; }
    void set_has_null(bool has_null) { _has_null = has_null; }
	// cache：InvertedIndexQueryCache*（倒排索引查询结果全局缓存句柄）。
	// cache_handler：缓存输出 Handle。
	// 通用查询缓存处理逻辑。在正式执行物理查询前检查缓存，若命中则直接复用已有的 Roaring 位图，避免重复执行开销极高的索引检索。
    bool handle_query_cache(const IndexQueryContextPtr& context, InvertedIndexQueryCache* cache,
                            const InvertedIndexQueryCache::CacheKey& cache_key,
                            InvertedIndexQueryCacheHandle* cache_handler,
                            std::shared_ptr<roaring::Roaring>& bit_map);
	// inverted_index_cache_handle：输出句柄，保存从 Cache 中拿到的 Searcher。
	// 管理 CLucene IndexSearcher 对象实例的缓存（Searcher Cache）。避免每次查询都重新解析索引 Directory 和重新打开索引文件，显著提升热点查询性能。
    virtual Status handle_searcher_cache(const IndexQueryContextPtr& context,
                                         InvertedIndexCacheHandle* inverted_index_cache_handle);
	// 通过调用 _index_file_reader 拼装并获取该 Segment 倒排索引物理文件的完整绝对路径。
    std::string get_index_file_path();
	// 用于基于传入的 CLucene Directory 目录高效构建 CLucene 搜索引擎对象 IndexSearcher 并计算其内存占用。
    static Status create_index_searcher(IndexSearcherBuilder* index_searcher_builder,
                                        lucene::store::Directory* dir, IndexSearcherPtr* searcher,
                                        size_t& reader_size);
	// 获取底层文件读取器 _index_file_reader 的智能指针。
    std::shared_ptr<IndexFileReader> get_index_file_reader() const { return _index_file_reader; }
	// 获取元数据对象 _index_meta 的常引用。
    const TabletIndex& get_index_meta() const { return _index_meta; }

protected:
	// 受保护的底层搜索执行辅助方法。被子类（如 FullTextIndexReader）调用，将上层解析好的 query_info 翻译为 CLucene 的具体 Query 对象并在 index_searcher 上发起真正的检索，将命中结果汇总写入 term_match_bitmap 中。
    Status match_index_search(const IndexQueryContextPtr& context,
                              InvertedIndexQueryType query_type,
                              const InvertedIndexQueryInfo& query_info,
                              const FulltextIndexSearcherPtr& index_searcher,
                              const std::shared_ptr<roaring::Roaring>& term_match_bitmap);

    friend class InvertedIndexIterator;
	// 底层索引文件读取器句柄。
	// 抽象了 V1/V2 格式的物理文件 IO 操作，用于从复合文件（.idx）或分散文件中读取特定列的倒排索引字节流。
    std::shared_ptr<IndexFileReader> _index_file_reader;
	// 索引元数据信息结构体（通常由 Schema/Tablet Meta 定义）。
	// 包含索引 ID (index_id)、索引名称、索引类型（如 INVERTED）以及在 DDL 中指定的属性表（如分词器配置 properties）。
    TabletIndex _index_meta;
	// 标记当前 Segment 的该索引列中是否存在 NULL 值。默认为 true；如果在生成索引时确定没有 NULL 值，可以设为 false 以加速跳过 NULL 位图的读取。
    bool _has_null = true;
};
// InvertedIndexReader 的共享智能指针别名，用于多线程环境及 InvertedIndexIterator 中跨生命周期安全地共享和引用 Reader 对象。
using InvertedIndexReaderPtr = std::shared_ptr<InvertedIndexReader>;

class FullTextIndexReader : public InvertedIndexReader {
    ENABLE_FACTORY_CREATOR(FullTextIndexReader);

public:
    explicit FullTextIndexReader(const TabletIndex* index_meta,
                                 const std::shared_ptr<IndexFileReader>& index_file_reader)
            : InvertedIndexReader(index_meta, index_file_reader) {}
    ~FullTextIndexReader() override = default;

    Status new_iterator(std::unique_ptr<IndexIterator>* iterator) override;
    Status query(const IndexQueryContextPtr& context, const std::string& column_name,
                 const Field& query_value, InvertedIndexQueryType query_type,
                 std::shared_ptr<roaring::Roaring>& bit_map,
                 const InvertedIndexAnalyzerCtx* analyzer_ctx = nullptr) override;
    Status try_query(const IndexQueryContextPtr& context, const std::string& column_name,
                     const Field& query_value, InvertedIndexQueryType query_type,
                     size_t* count) override {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>(
                "FullTextIndexReader not support try_query");
    }

    InvertedIndexReaderType type() override;
};

class StringTypeInvertedIndexReader : public InvertedIndexReader {
    ENABLE_FACTORY_CREATOR(StringTypeInvertedIndexReader);

public:
    explicit StringTypeInvertedIndexReader(
            const TabletIndex* index_meta,
            const std::shared_ptr<IndexFileReader>& index_file_reader)
            : InvertedIndexReader(index_meta, index_file_reader) {}
    ~StringTypeInvertedIndexReader() override = default;

    Status new_iterator(std::unique_ptr<IndexIterator>* iterator) override;
    Status query(const IndexQueryContextPtr& context, const std::string& column_name,
                 const Field& query_value, InvertedIndexQueryType query_type,
                 std::shared_ptr<roaring::Roaring>& bit_map,
                 const InvertedIndexAnalyzerCtx* analyzer_ctx = nullptr) override;
    Status try_query(const IndexQueryContextPtr& context, const std::string& column_name,
                     const Field& query_value, InvertedIndexQueryType query_type,
                     size_t* count) override {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>(
                "StringTypeInvertedIndexReader not support try_query");
    }
    InvertedIndexReaderType type() override;
};

template <InvertedIndexQueryType QT>
class InvertedIndexVisitor : public lucene::util::bkd::bkd_reader::intersect_visitor {
private:
    const void* _io_ctx = nullptr;
    roaring::Roaring* _hits = nullptr;
    uint32_t _num_hits;
    bool _only_count;
    lucene::util::bkd::bkd_reader* _reader = nullptr;

public:
    std::string query_min;
    std::string query_max;

public:
    InvertedIndexVisitor(const void* io_ctx, lucene::util::bkd::bkd_reader* r,
                         roaring::Roaring* hits, bool only_count = false);
    ~InvertedIndexVisitor() override = default;

    void set_reader(lucene::util::bkd::bkd_reader* r) { _reader = r; }
    lucene::util::bkd::bkd_reader* get_reader() { return _reader; }

    void visit(int row_id) override;
    void visit(roaring::Roaring& r) override;
    void visit(roaring::Roaring&& r) override;
    void visit(roaring::Roaring* doc_id, std::vector<uint8_t>& packed_value) override;
    void visit(std::vector<char>& doc_id, std::vector<uint8_t>& packed_value) override;
    int visit(int row_id, std::vector<uint8_t>& packed_value) override;
    void visit(lucene::util::bkd::bkd_docid_set_iterator* iter,
               std::vector<uint8_t>& packed_value) override;
    int matches(uint8_t* packed_value);
    lucene::util::bkd::relation compare(std::vector<uint8_t>& min_packed,
                                        std::vector<uint8_t>& max_packed) override;
    lucene::util::bkd::relation compare_prefix(std::vector<uint8_t>& prefix) override;
    uint32_t get_num_hits() const { return _num_hits; }
    const void* get_io_context() override { return _io_ctx; }
};

class BkdIndexReader : public InvertedIndexReader {
    ENABLE_FACTORY_CREATOR(BkdIndexReader);

public:
    explicit BkdIndexReader(const TabletIndex* index_meta,
                            const std::shared_ptr<IndexFileReader>& index_file_reader)
            : InvertedIndexReader(index_meta, index_file_reader) {}
    ~BkdIndexReader() override = default;

    Status new_iterator(std::unique_ptr<IndexIterator>* iterator) override;
    Status query(const IndexQueryContextPtr& context, const std::string& column_name,
                 const Field& query_value, InvertedIndexQueryType query_type,
                 std::shared_ptr<roaring::Roaring>& bit_map,
                 const InvertedIndexAnalyzerCtx* analyzer_ctx = nullptr) override;
    Status try_query(const IndexQueryContextPtr& context, const std::string& column_name,
                     const Field& query_value, InvertedIndexQueryType query_type,
                     size_t* count) override;
    Status invoke_bkd_try_query(const IndexQueryContextPtr& context, const Field& query_value,
                                InvertedIndexQueryType query_type,
                                std::shared_ptr<lucene::util::bkd::bkd_reader> r, size_t* count);
    Status invoke_bkd_query(const IndexQueryContextPtr& context, const Field& query_value,
                            InvertedIndexQueryType query_type,
                            std::shared_ptr<lucene::util::bkd::bkd_reader> r,
                            std::shared_ptr<roaring::Roaring>& bit_map);
    template <InvertedIndexQueryType QT>
    Status construct_bkd_query_value(const Field& query_value,
                                     std::shared_ptr<lucene::util::bkd::bkd_reader> r,
                                     InvertedIndexVisitor<QT>* visitor);

    InvertedIndexReaderType type() override;
    Status get_bkd_reader(const IndexQueryContextPtr& context, BKDIndexSearcherPtr& reader);

private:
    FieldType _type = FieldType::OLAP_FIELD_TYPE_NONE;
    const KeyCoder* _value_key_coder {};
};

} // namespace segment_v2
} // namespace doris
