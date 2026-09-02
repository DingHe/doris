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

#include <gen_cpp/segment_v2.pb.h>
#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/data_type/data_type.h"
#include "core/data_type/define_primitive_type.h"
#include "core/string_ref.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "storage/metadata_adder.h"
#include "storage/tablet/tablet_schema.h"
#include "util/once.h"

namespace doris {
namespace io {
class FileWriter;
struct IOContext;
} // namespace io

namespace segment_v2 {
// ZoneMap 是一种轻量级数据过滤索引。
// 在不额外建立复杂树状结构或倒排列表的前提下，通过记录数据块内的统计信息（如最小值、最大值、是否有 NULL 等）来实现快速的谓词下推（Predicate Pushdown）与数据剪枝（Data Pruning）。
// 多层级裁剪能力：
// Page 级别：为 Segment 内的每一个 Data Page 生成对应的 ZoneMapPB 序列化信息。查询时若发现 SQL 过滤条件不满足 Page 的 Min-Max 范围，可直接跳过读取该 Data Page。
// Segment 级别：将当前 Segment 内所有 Page 的统计信息汇总归纳为一个 Segment 级别的 ZoneMap，直接写入元数据 ColumnIndexMetaPB 中。查询时无需打开任何 Data Page 即可裁剪掉整个 Segment。
// 高效的写入与统计：
// 提供针对高频写入的热点路径（Hot-path）优化。在往 Page 中添加数据（add_values）时，采用内存友好的 C++ 原生类型（ValType）实时计算 Min/Max，仅在 flush 时才将其物化为通用对象 Field，大幅降低内存分配和类型转换开销。
// 完美支持 NULL 与非 NULL 值标记、无穷大/无穷小（±Inf）、NaN 特殊浮点数以及变长字符串（StringRef）的边界存储。
// ZoneMap 用于在内存中表示一个 Page 或 Segment 的统计信息。
struct ZoneMap {
    // min value of zone
    // 记录当前区域（Zone）内所有非 NULL 值的最小值（通用 Field 对象）。
    doris::Field min_value;
    // max value of zone
    // 记录当前区域（Zone）内所有非 NULL 值的最大值（通用 Field 对象）。
    doris::Field max_value;

    // if both has_null and has_not_null is false, means no rows.
    // if has_null is true and has_not_null is false, means all rows is null.
    // if has_null is false and has_not_null is true, means all rows is not null.
    // if has_null is true and has_not_null is true, means some rows is null and others are not.
    // has_null means whether zone has null value
    // 标记该区域内是否存在 NULL 值。
    bool has_null = false;
    // has_not_null means whether zone has none-null value
    // 标记该区域内是否存在非 NULL 的有效值。
    bool has_not_null = false;
    // 标记是否放弃使用该 ZoneMap 过滤（例如当字符串超长无法精确定界，或强制不做剪枝时设为 true）。
    bool pass_all = false;
    // 标记是否存在正无穷大 (+Inf) 浮点数值。
    bool has_positive_inf = false;
    // 标记是否存在负无穷大 (-Inf) 浮点数值。
    bool has_negative_inf = false;
    // 标记是否存在 NaN (Not a Number) 浮点数值。
    bool has_nan = false;
    // 将内存中的 ZoneMap 结构序列化为 Protobuf 报文对象 ZoneMapPB（准备写入磁盘）。
    void to_proto(ZoneMapPB* dst, const DataTypePtr& data_type) const {
        if (pass_all || !has_not_null) {
            dst->set_min("");
            dst->set_max("");
        } else {
            dst->set_min(data_type->get_serde()->to_olap_string(min_value));
            dst->set_max(data_type->get_serde()->to_olap_string(max_value));
        }
        dst->set_has_null(has_null);
        dst->set_has_not_null(has_not_null);
        dst->set_pass_all(pass_all);
        dst->set_has_positive_inf(has_positive_inf);
        dst->set_has_negative_inf(has_negative_inf);
        dst->set_has_nan(has_nan);
    }

    static Status from_proto(const ZoneMapPB& zone_map, const DataTypePtr& data_type,
                             ZoneMap& zone_map_info);
};
// Min-Max 索引写入器的顶层抽象接口
class ZoneMapIndexWriter {
public:
    // 根据传入列的 PrimitiveType，实例化对应模板类型 TypedZoneMapIndexWriter<Type> 的指针对象并返回给 res。
    static Status create(DataTypePtr data_type, const TabletColumn* column,
                         std::unique_ptr<ZoneMapIndexWriter>& res);

    ZoneMapIndexWriter() = default;

    virtual ~ZoneMapIndexWriter() = default;
    // 向当前 Page 追加批量非 NULL 值，用于实时更新当前 Page 的 Min/Max 统计边界。
    virtual void add_values(const void* values, size_t count) = 0;
    // 向当前 Page 追加 count 个 NULL 值，主要用于触发 has_null = true 标记。
    virtual void add_nulls(uint32_t count) = 0;

    // mark the end of one data page so that we can finalize the corresponding zone map
    // 当一个 Data Page 写入完成时被调用。它会将当前 Page 的 C++ 原生 Min/Max 物化成 Field，
    // 把 _page_zone_map 序列化为 Protobuf 存入数组，并汇总更新 Segment 级别的 _segment_zone_map，最后重置 Page 状态。
    virtual Status flush() = 0;
    // 当整个 Segment 数据写完时调用。将所有积累的 Page ZoneMap（通过 IndexedColumnWriter）写入物理磁盘文件 file_writer，并把 Segment 级别的 ZoneMap 与 Page 索引元数据写入 index_meta。
    virtual Status finish(io::FileWriter* file_writer, ColumnIndexMetaPB* index_meta) = 0;
    // 在 Page 级 ZoneMap 刷盘序列化前，允许对 zone_map 结构做出特殊修改（如截断超长字符串 Min/Max）。
    virtual void modify_index_before_flush(ZoneMap& zone_map) = 0;
    // 返回当前 ZoneMapWriter 在内存中累积的估算字节大小，用于 Segment 写入时的内存监控。
    virtual uint64_t size() const = 0;
    // 将当前正在收集的 Page 级 ZoneMap 标记为失效（例如设置 pass_all = true），通常用于处理异常数据或跳过特定 Page 的索引。
    virtual void invalid_page_zone_map() = 0;
};

// Zone map index is represented by an IndexedColumn with ordinal index.
// The IndexedColumn stores serialized ZoneMapPB for each data page.
// It also create and store the segment-level zone map in the index meta so that
// reader can prune an entire segment without reading pages.
// 针对具体物理类型（如 INT, BIGINT, DATETIME, STRING 等）高效实现 ZoneMap 计算的具体类。
template <PrimitiveType Type>
class TypedZoneMapIndexWriter final : public ZoneMapIndexWriter {
public:
    using ValType = std::conditional_t<is_string_type(Type), StringRef,
                                       typename PrimitiveTypeTraits<Type>::StorageFieldType>;
    explicit TypedZoneMapIndexWriter(DataTypePtr&& data_type);

    void add_values(const void* values, size_t count) override;

    void add_nulls(uint32_t count) override { _page_zone_map.has_null = true; }

    // mark the end of one data page so that we can finalize the corresponding zone map
    Status flush() override;

    Status finish(io::FileWriter* file_writer, ColumnIndexMetaPB* index_meta) override;

    void modify_index_before_flush(ZoneMap& zone_map) override;

    uint64_t size() const override { return _estimated_size; }

    void invalid_page_zone_map() override;

private:
    void _reset_zone_map(ZoneMap* zone_map) {
        // Do not reset min_value/max_value here: on the next page's first
        // value write, min/max get updated and has_not_null is then set to
        // true.
        zone_map->has_null = false;
        zone_map->has_not_null = false;
        zone_map->pass_all = false;
        zone_map->has_positive_inf = false;
        zone_map->has_negative_inf = false;
        zone_map->has_nan = false;
    }

    void _update_page_zonemap(const ValType& min_value, const ValType& max_value);

    // Materialize the running CppType min/max into _page_zone_map.{min,max}_value.
    // Called at flush() time, so the per-row hot path never constructs a Field.
    void _materialize_page_minmax();
    // 当前列的数据类型指针（用于 SerDe 转换与类型校验）。
    DataTypePtr _data_type;
    // 当前正在构建的 Page 级 ZoneMap 内存对象。
    ZoneMap _page_zone_map;
    // 当前整个 Segment 级的 ZoneMap 内存对象（在 flush() 时按 Page 逐步累加归并）
    ZoneMap _segment_zone_map;
    // Running min/max for the current page kept as raw ValType.
    // For string types, _page_min/_page_max are StringRefs that borrow into _page_min_storage/_page_max_storage.
    // 当前 Page 内原生强类型的最小值（例如 int32_t 或 StringRef）。避免在添加每行数据时频繁构造开销昂贵的 Field 对象。
    ValType _page_min {};
    // 当前 Page 内原生强类型的最大值。
    ValType _page_max {};
    // 当 Type 为字符串类型时，_page_min（StringRef）引用的实际底层的字符串内存缓冲区。
    std::string _page_min_storage;
    // 当 Type 为字符串类型时，_page_max（StringRef）引用的实际底层的字符串内存缓冲区。
    std::string _page_max_storage;

    // serialized ZoneMapPB for each data page
    // 保存每个 Data Page 序列化后的 ZoneMapPB 二进制字符串列表（准备一次性写入 IndexedColumn）。
    std::vector<std::string> _values;
    // 预估当前 Writer 占用的内存大小总量（字节数）
    uint64_t _estimated_size = 0;
};
// 虽然主要类是 Writer，但文件中包含配套的 ZoneMapIndexReader，用于从 Segment 文件的元数据中读取 ZoneMap 索引信息。
class ZoneMapIndexReader : public MetadataAdder<ZoneMapIndexReader> {
public:
    explicit ZoneMapIndexReader(io::FileReaderSPtr file_reader,
                                const IndexedColumnMetaPB& page_zone_maps)
            : _file_reader(std::move(file_reader)) {
        _page_zone_maps_meta.reset(new IndexedColumnMetaPB(page_zone_maps));
    }

    virtual ~ZoneMapIndexReader();

    // load all page zone maps into memory
    Status load(bool use_page_cache, bool kept_in_memory,
                OlapReaderStatistics* index_load_stats = nullptr,
                const io::IOContext* io_ctx = nullptr);
    // 获取内存中加载好的所有 Page ZoneMap 数组列表。
    const std::vector<ZoneMapPB>& page_zone_maps() const { return _page_zone_maps; }
    // 返回当前列拥有的 Data Page 总数。
    size_t num_pages() const { return _page_zone_maps.size(); }

private:
    // 实际的加载逻辑。创建 IndexedColumnReader 并遍历读取磁盘中序列化的二进制块，反序列化填充至 _page_zone_maps 数组。
    Status _load(bool use_page_cache, bool kept_in_memory, std::unique_ptr<IndexedColumnMetaPB>,
                 OlapReaderStatistics* index_load_stats, const io::IOContext* io_ctx);

    int64_t get_metadata_size() const override;

private:
    // 程安全的“仅执行一次”初始化保证工具（保证索引数据只被加载一次）。
    DorisCallOnce<Status> _load_once;
    // TODO: yyq, we shoud remove file_reader from here.
    // 底层的物理文件读取句柄。
    io::FileReaderSPtr _file_reader;
    // 指向保存 Page ZoneMap 的 IndexedColumn 元数据 Protobuf 对象的指针。
    std::unique_ptr<IndexedColumnMetaPB> _page_zone_maps_meta;
    // 反序列化并解析后保存在内存中的所有 Page 级别的 ZoneMap 列表。
    std::vector<ZoneMapPB> _page_zone_maps;
    // Protobuf 元数据在内存中占用的内存字节大小。
    int64_t _pb_meta_size {0};
};

} // namespace segment_v2
} // namespace doris
