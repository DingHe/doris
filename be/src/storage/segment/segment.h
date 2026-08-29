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

#include <butil/macros.h>
#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/segment_v2.pb.h>
#include <glog/logging.h>

#include <cstdint>
#include <map>
#include <memory> // for unique_ptr
#include <optional>
#include <string>
#include <unordered_map>

#include "agent/be_exec_version_manager.h"
#include "common/be_mock_util.h"
#include "common/status.h" // Status
#include "core/column/column.h"
#include "core/data_type/data_type.h"
#include "io/cache/file_cache_common.h" // io::UInt128Wrapper returned by value
#include "io/fs/file_reader.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/file_system.h"
#include "io/io_common.h"
#include "runtime/descriptors.h"
#include "storage/cache/page_cache.h"
#include "storage/olap_common.h"
#include "storage/schema.h"
#include "storage/segment/page_handle.h"
#include "storage/tablet/tablet_schema.h"
#include "util/once.h"
#include "util/slice.h"
namespace doris {
class IDataType;

class ShortKeyIndexDecoder;
class Schema;
class StorageReadOptions;
class PrimaryKeyIndexReader;
class RowwiseIterator;
struct RowLocation;

namespace segment_v2 {

class Segment;
class InvertedIndexIterator;
class IndexFileReader;
class IndexIterator;
class ColumnReader;
class ColumnIterator;
class ColumnReaderCache;
class ColumnMetaAccessor;

using SegmentSharedPtr = std::shared_ptr<Segment>;

struct SparseColumnCache;
using SparseColumnCacheSPtr = std::shared_ptr<SparseColumnCache>;

// key is column path, value is the sparse column cache
// now column path is only SPARSE_COLUMN_PATH, in the future, we can add more sparse column paths
using PathToSparseColumnCache = std::unordered_map<std::string, SparseColumnCacheSPtr>;
using PathToSparseColumnCacheUPtr = std::unique_ptr<PathToSparseColumnCache>;

struct BinaryColumnCache;
using BinaryColumnCacheSPtr = std::shared_ptr<BinaryColumnCache>;
using PathToBinaryColumnCache = std::unordered_map<std::string, BinaryColumnCacheSPtr>;
using PathToBinaryColumnCacheUPtr = std::unique_ptr<PathToBinaryColumnCache>;

// A Segment is used to represent a segment in memory format. When segment is
// generated, it won't be modified, so this struct aimed to help read operation.
// It will prepare all ColumnReader to create ColumnIterator as needed.
// And user can create a RowwiseIterator through new_iterator function.
//
// NOTE: This segment is used to a specified TabletSchema, when TabletSchema
// is changed, this segment can not be used any more. For example, after a schema
// change finished, client should disable all cached Segment for old TabletSchema.
// 在 Apache Doris 的 Segment V2 存储格式中，Segment 表示磁盘上存储数据的只读段文件（Segment File）在内存中的抽象与句柄。
// 存储与读取核心接口：一个 Segment 文件内部按列存储了数据，并包含了多种索引（短拼键索引、主键索引、Bloom Filter、倒排索引等）以及 Footer 元数据。
// Segment 类负责管理该文件的打开、元数据解析以及各类索引的加载。
// 不可变性（Immutable）：Segment 一旦生成并写入磁盘后就不再被修改（写入阶段由 SegmentWriter 处理，读取阶段由 Segment 句柄处理）。这使得 Segment 的读取操作天然具备良好的线程安全性。
// 迭代器工厂：该类不直接进行数据的顺序逐行遍历，而是通过 new_iterator 生成 RowwiseIterator（行级迭代器），或通过 new_column_iterator 生成 ColumnIterator（列级迭代器），供上层查询引擎（如 SegmentIterator）拉取数据。
// 绑定 TabletSchema：Segment 严格绑定创建它时的 TabletSchema。如果表的 Schema 发生变更（Schema Change），旧 Segment 的句柄必须失效并从缓存中清理，重新以新的 Schema 解析。
// Segment 代表了一个只读的数据段文件在内存中的抽象句柄。由于 Doris 采用了 LSM-Tree 类似的存储架构，Segment 文件遵循“一次写入、多次读取、不可变更（Immutable）” 的设计原则。
class Segment : public std::enable_shared_from_this<Segment>, public MetadataAdder<Segment> {
public:
    // 打开并初始化 Segment 的外部静态工厂方法。
    // 通过文件系统（fs）、路径（path）、Segment ID、Rowset ID、Tablet Schema 以及读取选项，分配 Segment 实例并调用内部的 _open 完成最基本的 Segment Footer 解析与内存对象构建，最后将指针写入 output 返回。
    static Status open(io::FileSystemSPtr fs, const std::string& path, int64_t tablet_id,
                       uint32_t segment_id, RowsetId rowset_id, TabletSchemaSPtr tablet_schema,
                       const io::FileReaderOptions& reader_options,
                       std::shared_ptr<Segment>* output, InvertedIndexFileInfo idx_file_info = {},
                       OlapReaderStatistics* stats = nullptr,
                       const io::IOContext* io_ctx = nullptr);
    // 根据传入的 rowset_id 字符串和 seg_id，计算并生成一个 128 位的全局唯一文件缓存键（File Cache Key），用于 Doris 的底层文件缓存系统机制。
    static io::UInt128Wrapper file_cache_key(std::string_view rowset_id, uint32_t seg_id);
    io::UInt128Wrapper file_cache_key() const {
        return file_cache_key(_rowset_id.to_string(), _segment_id);
    }

    ~Segment() override;
    // 继承自 MetadataAdder，获取 Segment 对象本身及其直接持有的元数据占用的内存大小。
    int64_t get_metadata_size() const override;
    // 作用：计算并更新 Segment 元数据在系统级内存追踪器（MemTracker）中的占用数值。
    void update_metadata_size();
    // 根据给定的 Schema 和读取选项，创建一个按行读取数据的 RowwiseIterator 迭代器（实际返回 SegmentIterator），这是数据查询的主要入口。
    Status new_iterator(SchemaSPtr schema, const StorageReadOptions& read_options,
                        std::unique_ptr<RowwiseIterator>* iter);
    // 针对新增列或包含默认值的列，生成一个无需从磁盘读取、直接返回列默认值的 DefaultValueColumnIterator。
    static Status new_default_iterator(const TabletColumn& tablet_column,
                                       std::unique_ptr<ColumnIterator>* iter);
    // 返回该 Segment 的 ID（在所在的 Rowset 内部唯一）。
    uint32_t id() const { return _segment_id; }
    // 返回该 Segment 所属的 Rowset ID。
    RowsetId rowset_id() const { return _rowset_id; }
    // 返回 Segment 文件中包含的总行数（带有 MOCK_FUNCTION 方便单测打桩）。
    MOCK_FUNCTION uint32_t num_rows() const { return _num_rows; }

    // if variant_sparse_column_cache is nullptr, means the sparse column cache is not used
    // 为指定的物理列（TabletColumn）创建专用的列迭代器 ColumnIterator，用于单列数据的解包与读取。针对 Variant 复杂类型，可传入稀疏列缓存（variant_sparse_column_cache）。
    Status new_column_iterator(const TabletColumn& tablet_column,
                               std::unique_ptr<ColumnIterator>* iter, const StorageReadOptions* opt,
                               const std::unordered_map<int32_t, PathToBinaryColumnCacheUPtr>*
                                       variant_sparse_column_cache = nullptr);
    // 创建针对特定列的索引迭代器（例如倒排索引 Inverted Index 迭代器），用于基于索引的谓词过滤。
    Status new_index_iterator(const TabletColumn& tablet_column, const TabletIndex* index_meta,
                              const StorageReadOptions& read_options,
                              std::unique_ptr<IndexIterator>* iter);
    // 获取前缀/短拼键索引解码器。带有 DCHECK 断言，确保调用前 load_index 已经被成功执行。
    const ShortKeyIndexDecoder* get_short_key_index() const {
        DCHECK(_load_index_once.has_called() && _load_index_once.stored_result().ok());
        return _sk_index_decoder.get();
    }
    // 获取主键索引读取器（仅用于 Unique 主键模型）。同样断言索引必须已加载。
    const PrimaryKeyIndexReader* get_primary_key_index() const {
        DCHECK(_load_index_once.has_called() && _load_index_once.stored_result().ok());
        return _pk_index_reader.get();
    }
    // 在主键模型中进行主键点查（Point Lookup）。
    // 根据传入的 key，通过主键索引查找该 key 对应的 RowLocation（包含所在 Segment 内的行号 row_id），并可选择性返回序列列（Sequence column）的值。
    Status lookup_row_key(const Slice& key, const TabletSchema* latest_schema, bool with_seq_col,
                          bool with_rowid, RowLocation* row_location, OlapReaderStatistics* stats,
                          std::string* encoded_seq_value = nullptr,
                          const io::IOContext* io_ctx = nullptr);
    // 根据 Segment 内部的行号 row_id 反查出对应行的主键 Key。
    Status read_key_by_rowid(uint32_t row_id, std::string* key);

    // row_ids must be strictly increasing.
    // 批量点查接口。传入一组严格递增的 row_ids，直接定位并读取这些行在指定列（slot）的数据，并将结果填充进向量列 result 中。
    Status seek_and_read_by_rowid(const TabletSchema& schema, SlotDescriptor* slot,
                                  const std::vector<uint32_t>& row_ids, MutableColumnPtr& result,
                                  StorageReadOptions& storage_read_options,
                                  std::unique_ptr<ColumnIterator>& iterator_hint);
    // 显式加载 Segment 的短拼键索引（Short Key Index）以及主键索引元数据到内存中。
    Status load_index(OlapReaderStatistics* stats, const io::IOContext* io_ctx = nullptr);
    // 加载主键索引以及主键 Bloom Filter（布隆过滤器）。主要用于 Unique Key 模型的主键点查与写时合并（Merge-on-Write）的行查找过程。
    Status load_pk_index_and_bf(OlapReaderStatistics* stats, const io::IOContext* io_ctx = nullptr);
    // 更新 Segment 的健康状态。如果在异步加载索引或读取过程中发生文件损坏/IO 错误，将状态标记为异常。
    void update_healthy_status(Status new_status) { _healthy_status.update(new_status); }
    // The segment is loaded into SegmentCache and then will load indices, if there are something wrong
    // during loading indices, should remove it from SegmentCache. If not, it will always report error during
    // query. So we add a healthy status API, the caller should check the healhty status before using the segment.
    // 检查当前 Segment 的健康状态。在从 SegmentCache 中取出 Segment 使用前必须先调用此接口判断，避免持续读取已坏损的 Segment。
    Status healthy_status();
    // 分别获取 Unique 主键模型 Segment 中主键的最小值和最大值，用于快速判断 Range 是否覆盖。
    std::string min_key() {
        DCHECK(_tablet_schema->keys_type() == UNIQUE_KEYS && _pk_index_meta != nullptr);
        return _pk_index_meta->min_key();
    }
    std::string max_key() {
        DCHECK(_tablet_schema->keys_type() == UNIQUE_KEYS && _pk_index_meta != nullptr);
        return _pk_index_meta->max_key();
    }
    // 获取底层用于读取 Segment 文件的 FileReader 指针。
    io::FileReaderSPtr file_reader() { return _file_reader; }

    // Including the column reader memory.
    // another method `get_metadata_size` not include the column reader, only the segment object itself.
    // 返回当前 Segment 消耗的总元数据内存（不仅包含 Segment 对象本身，还包含底层列读取器 ColumnReader 所占用的内存）。
    int64_t meta_mem_usage() const { return _meta_mem_usage; }

    // Get the inner file column's data type.
    // When `read_options` is provided, the decision (e.g. flat-leaf vs hierarchical) can depend
    // on the reader type and tablet schema; when it is nullptr, we treat it as a query reader.
    // nullptr will be returned if storage type does not contain such column.
    // 获取该列在 Segment 文件内实际存储的内部数据类型（用于处理 Variant 动态列类型或 Schema 演进后的类型差异）。
    std::shared_ptr<const IDataType> get_data_type_of(const TabletColumn& column,
                                                      const StorageReadOptions& read_options);

    // If column in segment is the same type in schema, then it is safe to apply predicate.
    // 判断是否可以安全地将谓词下推（Predicate Pushdown）到该列。特别是针对 Variant 类型，需校验磁盘上的存储类型与目标 Cast 类型是否一致，防止类型转换不一致导致的下推计算错误。
    bool can_apply_predicate_safely(
            int cid, const Schema& schema,
            const std::map<std::string, DataTypePtr>& target_cast_type_for_variants,
            const StorageReadOptions& read_options) {
        const TabletColumn* col = schema.column(cid);
        DCHECK(col != nullptr) << "Column not found in schema for cid=" << cid;
        DataTypePtr storage_column_type = get_data_type_of(*col, read_options);
        if (storage_column_type == nullptr || col->type() != FieldType::OLAP_FIELD_TYPE_VARIANT ||
            !target_cast_type_for_variants.contains(col->name())) {
            // Default column iterator or not variant column
            return true;
        }
        if (storage_column_type->equals(*target_cast_type_for_variants.at(col->name()))) {
            return true;
        } else {
            return false;
        }
    }

    // The tso column (__DORIS_BINLOG_TSO__) is a NULL placeholder on disk on a
    // single-version binlog segment, replaced with the real commit_tso at read time
    // (SegmentIterator::_update_tso_col_if_needed). Its zonemap reflects the placeholder, so
    // it must NOT drive zonemap pruning. Mirrors the guards of _update_tso_col_if_needed.
    // Returns false for range (compaction) segments whose on-disk value is real.
    // 判断特定列（如二进制日志的版本戳 __DORIS_BINLOG_TSO__）在当前 Segment 中是否只是磁盘占位符（NULL 占位）。如果是占位符，则不能使用其 ZoneMap 进行谓词裁剪（Zonemap Pruning）。
    bool is_tso_placeholder_col(int cid, const Schema& schema,
                                const StorageReadOptions& read_options) const;
    // 获取该 Segment 绑定的 Tablet Schema。
    const TabletSchemaSPtr& tablet_schema() const { return _tablet_schema; }

    // get the column reader by tablet column, return NOT_FOUND if not found reader in this segment
    // 根据 TabletColumn 对象或列的唯一 ID（col_uid），获取或创建该列对应的 ColumnReader。
    Status get_column_reader(const TabletColumn& col, std::shared_ptr<ColumnReader>* column_reader,
                             OlapReaderStatistics* stats, const io::IOContext* io_ctx = nullptr,
                             std::optional<Field> const_value = std::nullopt);

    // get the column reader by column unique id, return NOT_FOUND if not found reader in this segment
    Status get_column_reader(int32_t col_uid, std::shared_ptr<ColumnReader>* column_reader,
                             OlapReaderStatistics* stats, const io::IOContext* io_ctx = nullptr,
                             std::optional<Field> const_value = std::nullopt);
    // 遍历 Segment Footer 中所有列的元数据 Protocol Buffer（ColumnMetaPB），并对每个列元数据执行传入的 visitor 回调函数。
    Status traverse_column_meta_pbs(const std::function<void(const ColumnMetaPB&)>& visitor);

    // Returns the cached raw_data_bytes for the given column unique id, or 0 if not found.
    // Data is populated during _create_column_meta (under call_once), so thread-safe after init.
    // 根据列的 unique id，获取该列在 Segment 中的原始未压缩数据字节数（Raw Data Bytes）。
    uint64_t column_raw_data_bytes(int32_t column_uid) const {
        auto it = _column_uid_to_raw_bytes.find(column_uid);
        return it != _column_uid_to_raw_bytes.end() ? it->second : 0;
    }
    // 根据传入的 file_reader 指针生成 Segment Footer 在 StoragePageCache 中对应的缓存 Key。
    static StoragePageCache::CacheKey get_segment_footer_cache_key(
            const io::FileReaderSPtr& file_reader);

private:
    DISALLOW_COPY_AND_ASSIGN(Segment);
    Segment(uint32_t segment_id, RowsetId rowset_id, TabletSchemaSPtr tablet_schema,
            InvertedIndexFileInfo idx_file_info = InvertedIndexFileInfo());
    // 执行 Segment 对象的分配、基础变量设置，并调用实例方法 _open 执行具体的解析。
    static Status _open(io::FileSystemSPtr fs, const std::string& path, uint32_t segment_id,
                        RowsetId rowset_id, TabletSchemaSPtr tablet_schema,
                        const io::FileReaderOptions& reader_options,
                        std::shared_ptr<Segment>* output, InvertedIndexFileInfo idx_file_info,
                        OlapReaderStatistics* stats = nullptr,
                        const io::IOContext* io_ctx = nullptr);
    // open segment file and read the minimum amount of necessary information (footer)

    // 打开 Segment 文件，读取并解析最基础的 Footer 信息。
    Status _open(OlapReaderStatistics* stats, const io::IOContext* io_ctx = nullptr);
    // 从文件尾部读取并反序列化出 Protocol Buffer 结构的 SegmentFooterPB。
    Status _parse_footer(std::shared_ptr<SegmentFooterPB>& footer,
                         OlapReaderStatistics* stats = nullptr,
                         const io::IOContext* io_ctx = nullptr);
    // 解析 SegmentFooterPB 中的列元信息，初始化内部的 _file_column_types 以及列数据大小映射表 _column_uid_to_raw_bytes。
    Status _create_column_meta(const SegmentFooterPB& footer, OlapReaderStatistics* stats = nullptr,
                               const io::IOContext* io_ctx = nullptr);
    // 实际执行加载主键 Bloom Filter Page 数据到内存的操作。
    Status _load_pk_bloom_filter(OlapReaderStatistics* stats,
                                 const io::IOContext* io_ctx = nullptr);
    // 当读取 Segment Footer 或数据遇到损坏时，将错误数据片段转储（Dump）到本地磁盘文件，方便后续排查文件损坏原因。
    Status _write_error_file(size_t file_size, size_t offset, size_t bytes_read, char* data,
                             io::IOContext& io_ctx);
    // 打开当前 Segment 对应的倒排索引文件（.idx 文件）的读取器 IndexFileReader。
    Status _open_index_file_reader();
    // 结合 _create_column_meta_once_call 线程安全保证（call_once），确保 Segment 的列元数据只会被初始化一次。
    Status _create_column_meta_once(OlapReaderStatistics* stats,
                                    const io::IOContext* io_ctx = nullptr);
    // 获取 SegmentFooterPB 指针，优先从 Page Cache 中读取；若未命中则从磁盘文件读取。
    virtual Status _get_segment_footer(std::shared_ptr<SegmentFooterPB>&,
                                       OlapReaderStatistics* stats,
                                       const io::IOContext* io_ctx = nullptr);
    // 基于当前 Segment 实例的信息生成 Footer Cache Key。
    StoragePageCache::CacheKey get_segment_footer_cache_key() const;

    friend class SegmentIterator;
    friend class ColumnReaderCache;
    friend class MockSegment;
    // 指向存储该 Segment 文件的文件系统对象（如本地 POSIX、S3、HDFS 等）。
    io::FileSystemSPtr _fs;
    // Segment 数据文件（.dat）的底层读取器。
    io::FileReaderSPtr _file_reader;
    // Relative path passed to `open`, used to derive the inverted index path (see
    // _open_index_file_reader).
    // Segment 文件的相对/绝对路径。
    std::string _seg_path;
    // Segment 的整数 ID。
    uint32_t _segment_id;
    // 该 Segment 文件中包含的总数据行数。
    uint32_t _num_rows;
    // 原子的 Segment 健康状态。记录 Segment 是否遇到了磁盘 IO 故障或数据解析损坏。
    AtomicStatus _healthy_status;

    // 1. Tracking memory use by segment meta data such as footer or index page.
    // 2. Tracking memory use by segment column reader
    // The memory consumed by querying is tracked in segment iterator.
    // 记录该 Segment 占用的内存总大小（动态计算）。
    int64_t _meta_mem_usage;
    // 已向系统 MemTracker 注册申请的元数据内存大小，用于内存管控。
    int64_t _tracked_meta_mem_usage = 0;
    // Segment 所属的 Rowset ID。
    RowsetId _rowset_id;
    // 指向关联的表结构 Schema。
    TabletSchemaSPtr _tablet_schema;
    // 主键索引的元数据 Protocol Buffer（包含 Key 范围等）。
    std::unique_ptr<PrimaryKeyIndexMetaPB> _pk_index_meta;
    // 短拼键索引（Short Key Index）在文件中的 Page 位置指针（偏移量与长度）。
    PagePointerPB _sk_index_page;

    // Limited cache for column readers
    // 针对列读取器（ColumnReader）的有限缓存，避免重复创建列读取器。
    std::unique_ptr<ColumnReaderCache> _column_reader_cache;

    // Centralized accessor for column metadata layout and uid->column_ordinal mapping.
    // 列元数据的中央访问器，加速从 column_uid 到列序号（ordinal）的映射查找。
    std::unique_ptr<ColumnMetaAccessor> _column_meta_accessor;

    // Init from ColumnMetaPB in SegmentFooterPB
    // map column unique id ---> it's inner data type
    // 映射表：Column Unique ID -> 磁盘存储的数据类型。
    std::map<int32_t, std::shared_ptr<const IDataType>> _file_column_types;

    // used to guarantee that short key index will be loaded at most once in a thread-safe way
    // 保证 load_index 逻辑在多线程并发时仅执行一次。
    DorisCallOnce<Status> _load_index_once;
    // used to guarantee that primary key bloom filter will be loaded at most once in a thread-safe way
    // 保证主键 Bloom Filter 仅被并发加载一次。
    DorisCallOnce<Status> _load_pk_bf_once;
    // 保证 _create_column_meta 逻辑仅执行一次。
    DorisCallOnce<Status> _create_column_meta_once_call;
    // 使用弱引用缓存解析后的 SegmentFooterPB，避免不必要的重复反序列化，同时允许在内存紧张时被回收。
    std::weak_ptr<SegmentFooterPB> _footer_pb;

    // Cached raw_data_bytes per column unique id, populated once in _create_column_meta().
    // 映射表：Column Unique ID -> 原始未压缩数据字节数。
    std::unordered_map<int32_t, uint64_t> _column_uid_to_raw_bytes;

    // used to hold short key index page in memory
    // 持有短拼键索引 Page 在内存中的 Handle，防止内存 Page 被 PageCache 释放。
    PageHandle _sk_index_handle;
    // short key index decoder
    // all content is in memory
    // 解码短拼键索引的解码器对象。
    std::unique_ptr<ShortKeyIndexDecoder> _sk_index_decoder;
    // primary key index reader
    // 主键索引的读取器（负责二分查找或 Index Page 检索）。
    std::unique_ptr<PrimaryKeyIndexReader> _pk_index_reader;
    // Segment 内部互斥锁，保护底层文件打开与关键索引加载时的临界区。
    std::mutex _open_lock;
    // inverted index file reader
    // 倒排索引文件（.idx）的读取器。
    std::shared_ptr<IndexFileReader> _index_file_reader;
    // 保证倒排索引文件读取器仅被打开一次。
    DorisCallOnce<Status> _index_file_reader_open;
    // 倒排索引文件的相关元数据信息（如文件大小、格式等）。
    InvertedIndexFileInfo _idx_file_info;
    // Segment 所属的 Tablet ID。
    int64_t _tablet_id = -1;
    // 执行引擎的版本号，用于向下兼容旧版本的 Segment 格式。
    int _be_exec_version = BeExecVersionManager::get_newest_version();
};

} // namespace segment_v2
} // namespace doris
