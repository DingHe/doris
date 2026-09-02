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

#include "storage/index/zone_map/zone_map_index.h"

#include <gen_cpp/segment_v2.pb.h>
#include <glog/logging.h>

#include <algorithm>
#include <limits>
#include <type_traits>

#include "core/column/column.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type.h"
#include "core/data_type/define_primitive_type.h"
#include "core/data_type/primitive_type.h"
#include "core/string_ref.h"
#include "core/value/decimalv2_value.h"
#include "core/value/vdatetime_value.h"
#include "storage/index/indexed_column_reader.h"
#include "storage/index/indexed_column_writer.h"
#include "storage/olap_common.h"
#include "storage/segment/encoding_info.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/types.h"
#include "util/slice.h"
#include "util/unaligned.h"

namespace doris {
struct uint24_t;

namespace segment_v2 {
Status ZoneMap::from_proto(const ZoneMapPB& zone_map, const DataTypePtr& data_type,
                           ZoneMap& zone_map_info) {
    zone_map_info.has_null = zone_map.has_null();
    zone_map_info.has_not_null = zone_map.has_not_null();
    zone_map_info.pass_all = zone_map.pass_all();
    zone_map_info.has_negative_inf = zone_map.has_negative_inf();
    zone_map_info.has_positive_inf = zone_map.has_positive_inf();
    zone_map_info.has_nan = zone_map.has_nan();

    auto field_type = data_type->get_storage_field_type();
    // min value and max value are valid if has_not_null is true
    if (zone_map.has_not_null()) {
        if (zone_map.has_negative_inf()) {
            if (FieldType::OLAP_FIELD_TYPE_FLOAT == field_type) {
                static auto constexpr float_neg_inf = -std::numeric_limits<float>::infinity();
                zone_map_info.min_value = Field::create_field<TYPE_FLOAT>(float_neg_inf);
            } else if (FieldType::OLAP_FIELD_TYPE_DOUBLE == field_type) {
                static auto constexpr double_neg_inf = -std::numeric_limits<double>::infinity();
                zone_map_info.min_value = Field::create_field<TYPE_DOUBLE>(double_neg_inf);
            } else {
                return Status::InternalError("invalid zone map with negative Infinity");
            }
        } else {
            if (!zone_map_info.pass_all) {
                RETURN_IF_ERROR(data_type->get_serde()->from_zonemap_string(
                        zone_map.min(), zone_map_info.min_value));
            }
        }

        if (zone_map.has_nan()) {
            if (FieldType::OLAP_FIELD_TYPE_FLOAT == field_type) {
                static auto constexpr float_nan = std::numeric_limits<float>::quiet_NaN();
                zone_map_info.max_value = Field::create_field<TYPE_FLOAT>(float_nan);
            } else if (FieldType::OLAP_FIELD_TYPE_DOUBLE == field_type) {
                static auto constexpr double_nan = std::numeric_limits<double>::quiet_NaN();
                zone_map_info.max_value = Field::create_field<TYPE_DOUBLE>(double_nan);
            } else {
                return Status::InternalError("invalid zone map with NaN");
            }
        } else if (zone_map.has_positive_inf()) {
            if (FieldType::OLAP_FIELD_TYPE_FLOAT == field_type) {
                static auto constexpr float_pos_inf = std::numeric_limits<float>::infinity();
                zone_map_info.max_value = Field::create_field<TYPE_FLOAT>(float_pos_inf);
            } else if (FieldType::OLAP_FIELD_TYPE_DOUBLE == field_type) {
                static auto constexpr double_pos_inf = std::numeric_limits<double>::infinity();
                zone_map_info.max_value = Field::create_field<TYPE_DOUBLE>(double_pos_inf);
            } else {
                return Status::InternalError("invalid zone map with positive Infinity");
            }
        } else {
            if (!zone_map_info.pass_all) {
                RETURN_IF_ERROR(data_type->get_serde()->from_zonemap_string(
                        zone_map.max(), zone_map_info.max_value));
            }
        }
    }
    return Status::OK();
}

template <PrimitiveType Type>
TypedZoneMapIndexWriter<Type>::TypedZoneMapIndexWriter(DataTypePtr&& data_type)
        : _data_type(std::move(data_type)) {
    _page_zone_map.min_value =
            doris::Field::create_field<Type>(typename PrimitiveTypeTraits<Type>::CppType());
    _page_zone_map.max_value =
            doris::Field::create_field<Type>(typename PrimitiveTypeTraits<Type>::CppType());
    _segment_zone_map.min_value =
            doris::Field::create_field<Type>(typename PrimitiveTypeTraits<Type>::CppType());
    _segment_zone_map.max_value =
            doris::Field::create_field<Type>(typename PrimitiveTypeTraits<Type>::CppType());
    _reset_zone_map(&_page_zone_map);
    _reset_zone_map(&_segment_zone_map);
}

// 负责在向 Data Page 添加数据时，实时更新当前 Page 的最小值（_page_min）和最大值（_page_max）。
// 通过 C++17 的 if constexpr 编译期分支，它将变长字符串类型与定长数值类型的逻辑完全解耦，实现了零频繁内存分配、零临时 Field 对象构造的高性能边界追踪。
// 避免 Hot Path 构造临时 Field 对象：
//在老版本实现中，每次更新 Min/Max 都需要把 C++ 原生类型（如 int32_t、StringRef）包装成通用的 doris::Field 对象。Field 内部涉及虚函数和动态内存分配，在几百万行数据写入时开销极大。这里直接使用原生的 ValType（如 int32_t 或 StringRef）进行直接比较。
//生命周期管理与内存拷贝（针对 String 类型）：
//传入的 min_value / max_value 通常是批次 Cache 中的临时 StringRef（只包含指针和长度）。如果当前值成为了新的 _page_min 或 _page_max，必须把字符串的内容深拷贝到 _page_min_storage / _page_max_storage（std::string）中，否则下一批数据写入后，指针就会变成悬空指针（Dangling Pointer）。
//超长字符串截断优化（MAX_ZONE_MAP_INDEX_SIZE）：
//ZoneMap 的作用是做过滤裁剪，不需要存储几 KB 长的完整字符串。如果字符串很长，直接按 MAX_ZONE_MAP_INDEX_SIZE（默认 512 字节）截断，极大地节省了存储空间和比较开销。
// 模板函数定义：Type 为当前列的物理原生类型（如 TYPE_INT, TYPE_VARCHAR 等）。ValType 在类定义中通过 std::conditional_t 被推导为：如果是字符串类型则为 StringRef，否则为对应的 C++ 基础类型（如 int32_t）。
template <PrimitiveType Type>
void TypedZoneMapIndexWriter<Type>::_update_page_zonemap(const ValType& min_value,
                                                         const ValType& max_value) {
    // Hot path: compare/store using raw CppType to avoid Field temporaries.
    // For string types, truncate to MAX_ZONE_MAP_INDEX_SIZE (matching the old
    // Field-based path) and copy bytes into _page_{min,max}_storage so the
    // StringRef stays valid across add_values() calls.
    // if constexpr 编译期求值：只有当 Type 是字符串类（TYPE_CHAR, TYPE_VARCHAR, TYPE_STRING 等）时，这段代码才会被编译器编译，避免非字符串类型引入不必要的 Lambda 和 std::string 逻辑。
    if constexpr (is_string_type(Type)) {
        // 截断：计算 src.size 与上限 MAX_ZONE_MAP_INDEX_SIZE 的较小者 sz。
    // 深拷贝保存：使用 dst.assign(...) 将截断后的字节流复制到类成员 _page_min_storage 或 _page_max_storage（重用已分配的内存，避免频繁重新分配）。
        auto truncate_into = [](const StringRef& src, std::string& dst) {
            auto sz = std::min<size_t>(src.size, MAX_ZONE_MAP_INDEX_SIZE);
            dst.assign(src.data, sz);
            return StringRef(dst.data(), dst.size());
        };
        // 轻量级临时截断 View：构造两个临时的 StringRef（min_t 与 max_t），只截断 size，不发生任何内存拷贝。它们仅用于和当前已有的 _page_min / _page_max 做字典序比较。
        StringRef min_t(min_value.data, std::min<size_t>(min_value.size, MAX_ZONE_MAP_INDEX_SIZE));
        StringRef max_t(max_value.data, std::min<size_t>(max_value.size, MAX_ZONE_MAP_INDEX_SIZE));
        if (!_page_zone_map.has_not_null || min_t < _page_min) {
            _page_min = truncate_into(min_value, _page_min_storage);
        }
        if (!_page_zone_map.has_not_null || _page_max < max_t) {
            _page_max = truncate_into(max_value, _page_max_storage);
        }
    } else {
        if (!_page_zone_map.has_not_null || min_value < _page_min) {
            _page_min = min_value;
        }
        if (!_page_zone_map.has_not_null || max_value > _page_max) {
            _page_max = max_value;
        }
    }
    _page_zone_map.has_not_null = true;
}

template <PrimitiveType Type>
void TypedZoneMapIndexWriter<Type>::_materialize_page_minmax() {
    if (!_page_zone_map.has_not_null) {
        return;
    }
    _page_zone_map.min_value = doris::Field::create_field_from_olap_value<Type>(_page_min);
    _page_zone_map.max_value = doris::Field::create_field_from_olap_value<Type>(_page_max);
}

template <PrimitiveType Type>
void TypedZoneMapIndexWriter<Type>::add_values(const void* values, size_t count) {
    if (count == 0) {
        return;
    }
    const auto* vals = reinterpret_cast<const ValType*>(values);
    if constexpr (Type == TYPE_FLOAT || Type == TYPE_DOUBLE) {
        ValType min = std::numeric_limits<ValType>::max();
        ValType max = std::numeric_limits<ValType>::lowest();
        for (size_t i = 0; i < count; ++i) {
            if (std::isnan(vals[i])) {
                _page_zone_map.has_nan = true;
            } else if (vals[i] == std::numeric_limits<ValType>::infinity()) {
                _page_zone_map.has_positive_inf = true;
            } else if (vals[i] == -std::numeric_limits<ValType>::infinity()) {
                _page_zone_map.has_negative_inf = true;
            } else {
                if (vals[i] < min) {
                    min = vals[i];
                }
                if (vals[i] > max) {
                    max = vals[i];
                }
            }
        }
        _update_page_zonemap(min, max);
    } else {
        auto [min, max] = std::minmax_element(vals, vals + count);
        _update_page_zonemap(unaligned_load<ValType>(min), unaligned_load<ValType>(max));
    }
}

template <PrimitiveType Type>
void TypedZoneMapIndexWriter<Type>::modify_index_before_flush(
        struct doris::segment_v2::ZoneMap& zone_map) {
    // Only varchar/string filed need modify zone map index when zone map max_value
    // For varchar/string type, the zone map buffer is truncated at MAX_ZONE_MAP_INDEX_SIZE (512 bytes).
    // When a string value is longer than 512 bytes, only the first 512 bytes are stored.
    //
    // Truncating the max value creates a correctness problem: the truncated max is now smaller than the actual max.
    // This means the zone map could incorrectly skip pages that actually contain matching rows.
    //
    // So here we add one for the last byte if the max value is truncated, which makes the truncated max value
    // slightly larger than any real string that shares the same 512-byte prefix, ensuring no false negatives —
    // the zone map will never incorrectly skip a page that contains matching data.
    //
    // In UTF8 encoding, here do not appear 0xff in last byte
    if constexpr (Type == TYPE_CHAR || Type == TYPE_VARCHAR || Type == TYPE_STRING) {
        auto& str = zone_map.max_value.get<Type>();
        if (str.size() == MAX_ZONE_MAP_INDEX_SIZE) {
            str[str.size() - 1] += 1;
        }
    }
}

template <PrimitiveType Type>
void TypedZoneMapIndexWriter<Type>::invalid_page_zone_map() {
    _page_zone_map.pass_all = true;
}
// 当一个 Data Page 写入完成时被调用的核心清理与汇总函数。
// 把当前 Page 的原生 Min/Max 状态物化为正式的 Field、更新 Segment 级别的全局 ZoneMap 汇总边界、序列化 Page 级别的 Protobuf 报文，并重置 Page 状态以迎接收集下一个 Data Page。
template <PrimitiveType Type>
Status TypedZoneMapIndexWriter<Type>::flush() {
    // Materialize the running CppType min/max into the Field-typed page zone map
    // before merging into the segment zone map / serializing to proto.
    // 在调用 _update_page_zonemap 的 Hot Path 中，为了极致性能，_page_min 和 _page_max 一直维护在原生的 C++ 类型（如 int32_t 或 StringRef）中。
    // 此处在 Page 结束时，调用 _materialize_page_minmax() 一次性将其包装转换为通用的 _page_zone_map.min_value 与 _page_zone_map.max_value（doris::Field 类型）。
    _materialize_page_minmax();

    // Update segment zone map.
    // 汇总拓宽 Segment 级别的 ZoneMap 边界
    // 若当前 Page 包含非 NULL 值（_page_zone_map.has_not_null 为 true）：
    if (_page_zone_map.has_not_null) {
        // 若全局 Segment 尚无非 NULL 值，或当前 Page 的 Min 值小于全局 Min 值（通过 .get<Type>() 转换为强类型比较），则更新 _segment_zone_map.min_value。
        if (!_segment_zone_map.has_not_null ||
            _segment_zone_map.min_value.get<Type>() > _page_zone_map.min_value.get<Type>()) {
            _segment_zone_map.min_value = _page_zone_map.min_value;
        }
        // Max 值归并：若当前 Page 的 Max 值大于全局 Max 值，更新 _segment_zone_map.max_value。
        if (!_segment_zone_map.has_not_null ||
            _segment_zone_map.max_value.get<Type>() < _page_zone_map.max_value.get<Type>()) {
            _segment_zone_map.max_value = _page_zone_map.max_value;
        }
    }
    // 合并特殊状态标志位：把当前 Page 搜集的布尔标志位（has_null、has_not_null、±Inf、NaN）采用按位逻辑或（OR）的思想汇总累加至整个 Segment 级别。只要有一个 Page 包含特殊值，Segment 级别即被标记。
    if (_page_zone_map.has_null) {
        _segment_zone_map.has_null = true;
    }
    if (_page_zone_map.has_not_null) {
        _segment_zone_map.has_not_null = true;
    }
    if (_page_zone_map.has_positive_inf) {
        _segment_zone_map.has_positive_inf = true;
    }
    if (_page_zone_map.has_negative_inf) {
        _segment_zone_map.has_negative_inf = true;
    }
    if (_page_zone_map.has_nan) {
        _segment_zone_map.has_nan = true;
    }
    // 序列化与重置状态
    ZoneMapPB zone_map_pb;
    // 例如对超长字符串做二次修剪
    modify_index_before_flush(_page_zone_map);
    // 调用 to_proto()，利用 _data_type 将 _page_zone_map 转化为 ZoneMapPB Protobuf 对象。
    _page_zone_map.to_proto(&zone_map_pb, _data_type);
    // 清空当前 _page_zone_map 的状态位（重置 has_null、has_not_null 等为 false）。
    _reset_zone_map(&_page_zone_map);

    std::string serialized_zone_map;
    // 将 zone_map_pb 序列化为二进制字符串 serialized_zone_map。
    bool ret = zone_map_pb.SerializeToString(&serialized_zone_map);
    if (!ret) {
        return Status::InternalError("serialize zone map failed");
    }
    _estimated_size += serialized_zone_map.size() + sizeof(uint32_t);
    // 通过 std::move 零拷贝存入 _values 数组中，等待最终 Segment 结束时统一写入 IndexedColumn。
    _values.push_back(std::move(serialized_zone_map));
    return Status::OK();
}
// 把汇总好的 Segment 级 ZoneMap 写入元数据，并将包含所有 Data Page ZoneMap 序列化数据的数组写盘存储为 IndexedColumn 结构的索引流。
template <PrimitiveType Type>
Status TypedZoneMapIndexWriter<Type>::finish(io::FileWriter* file_writer,
                                             ColumnIndexMetaPB* index_meta) {
    // 第一阶段：写入 Segment 级 ZoneMap 到元数据
    // 设置 index_meta 的索引类型为 ZONE_MAP_INDEX
    index_meta->set_type(ZONE_MAP_INDEX);
    ZoneMapIndexPB* meta = index_meta->mutable_zone_map_index();
    // store segment zone map
    // 调用 modify_index_before_flush 钩子函数，对全局 Segment 级别的 _segment_zone_map 进行最终修饰（如对超长字符串的 Min/Max 进行截断保护）。
    modify_index_before_flush(_segment_zone_map);
    // 调用 to_proto 将全局汇总的 _segment_zone_map 转化为 Protobuf 报文，直接嵌入到元数据的 meta->mutable_segment_zone_map() 中。
    _segment_zone_map.to_proto(meta->mutable_segment_zone_map(), _data_type);

    // write out zone map for each data pages
    // 第二阶段：配置 IndexedColumnWriter（写 Page 级 ZoneMap）
    // 存储格式抽象：Doris 使用统一的 IndexedColumn 机制来存储这种“按 Page 序号索引的元数据数组”。
    // type = FieldType::OLAP_FIELD_TYPE_BITMAP：虽然存储的是 Protobuf 序列化的二进制串（ZoneMapPB），但物理底层作为 Slice / Blob 写入。
    constexpr FieldType type = FieldType::OLAP_FIELD_TYPE_BITMAP;
    IndexedColumnWriterOptions options;
    // 开启行号/Page序号索引。因为数据是与 Data Page 的 Ordinal（序号）一一对应的，读取时通过第 $N$ 个 Page 序号即可直接定位到第 $N$ 个 ZoneMapPB。
    options.write_ordinal_index = true;
    // 关闭值索引。这里不需要对二进制的 ZoneMapPB 本身建立 B 树值查找索引。
    options.write_value_index = false;
    // Zone map page always uses PLAIN_ENCODING. Do not change.
    // 固定采用平铺直接编码（PLAIN），保证读写解析的极高速度。
    options.encoding = PLAIN_ENCODING;
    options.compression = NO_COMPRESSION; // currently not compressed
    // 第三阶段：逐条写入 Page 级 ZoneMap 并 Flush 索引列
    IndexedColumnWriter writer(options, type, file_writer);
    RETURN_IF_ERROR(writer.init());

    for (auto& value : _values) {
        Slice value_slice(value);
        RETURN_IF_ERROR(writer.add(&value_slice));
    }
    return writer.finish(meta->mutable_page_zone_maps());
}

Status ZoneMapIndexReader::load(bool use_page_cache, bool kept_in_memory,
                                OlapReaderStatistics* index_load_stats,
                                const io::IOContext* io_ctx) {
    // TODO yyq: implement a new once flag to avoid status construct.
    return _load_once.call([this, use_page_cache, kept_in_memory, index_load_stats, io_ctx] {
        return _load(use_page_cache, kept_in_memory, std::move(_page_zone_maps_meta),
                     index_load_stats, io_ctx);
    });
}

Status ZoneMapIndexReader::_load(bool use_page_cache, bool kept_in_memory,
                                 std::unique_ptr<IndexedColumnMetaPB> page_zone_maps_meta,
                                 OlapReaderStatistics* index_load_stats,
                                 const io::IOContext* io_ctx) {
    IndexedColumnReader reader(_file_reader, *page_zone_maps_meta);
    RETURN_IF_ERROR(reader.load(use_page_cache, kept_in_memory, index_load_stats, io_ctx));
    IndexedColumnIterator iter(&reader, index_load_stats, io_ctx);

    _page_zone_maps.resize(reader.num_values());

    // read and cache all page zone maps
    for (int i = 0; i < reader.num_values(); ++i) {
        size_t num_to_read = 1;
        // The type of reader is FieldType::OLAP_FIELD_TYPE_BITMAP.
        // ColumnBitmap will be created when using FieldType::OLAP_FIELD_TYPE_BITMAP.
        // But what we need actually is ColumnString.
        MutableColumnPtr column = ColumnString::create();

        RETURN_IF_ERROR(iter.seek_to_ordinal(i));
        size_t num_read = num_to_read;
        RETURN_IF_ERROR(iter.next_batch(&num_read, column));
        DCHECK(num_to_read == num_read);

        if (!_page_zone_maps[i].ParseFromArray(column->get_data_at(0).data,
                                               cast_set<int>(column->get_data_at(0).size))) {
            return Status::Corruption("Failed to parse zone map");
        }
        _pb_meta_size += _page_zone_maps[i].ByteSizeLong();
    }

    update_metadata_size();
    return Status::OK();
}

int64_t ZoneMapIndexReader::get_metadata_size() const {
    return sizeof(ZoneMapIndexReader) + _pb_meta_size;
}

ZoneMapIndexReader::~ZoneMapIndexReader() = default;
#define APPLY_FOR_PRIMITITYPE(M) \
    M(TYPE_TINYINT)              \
    M(TYPE_SMALLINT)             \
    M(TYPE_INT)                  \
    M(TYPE_BIGINT)               \
    M(TYPE_LARGEINT)             \
    M(TYPE_FLOAT)                \
    M(TYPE_DOUBLE)               \
    M(TYPE_CHAR)                 \
    M(TYPE_DATE)                 \
    M(TYPE_DATETIME)             \
    M(TYPE_DATEV2)               \
    M(TYPE_DATETIMEV2)           \
    M(TYPE_TIMESTAMPTZ)          \
    M(TYPE_IPV4)                 \
    M(TYPE_IPV6)                 \
    M(TYPE_VARCHAR)              \
    M(TYPE_STRING)               \
    M(TYPE_DECIMAL32)            \
    M(TYPE_DECIMAL64)            \
    M(TYPE_DECIMAL128I)          \
    M(TYPE_DECIMAL256)

Status ZoneMapIndexWriter::create(DataTypePtr data_type, const TabletColumn* column,
                                  std::unique_ptr<ZoneMapIndexWriter>& res) {
    switch (column->type()) {
#define M(NAME)                                                             \
    case FieldType::OLAP_FIELD_##NAME: {                                    \
        res.reset(new TypedZoneMapIndexWriter<NAME>(std::move(data_type))); \
        return Status::OK();                                                \
    }
        APPLY_FOR_PRIMITITYPE(M)
#undef M
    case FieldType::OLAP_FIELD_TYPE_DECIMAL: {
        res.reset(new TypedZoneMapIndexWriter<TYPE_DECIMALV2>(std::move(data_type)));
        return Status::OK();
    }
    case FieldType::OLAP_FIELD_TYPE_BOOL: {
        res.reset(new TypedZoneMapIndexWriter<TYPE_BOOLEAN>(std::move(data_type)));
        return Status::OK();
    }
    default:
        return Status::InvalidArgument("Invalid type!");
    }
}
} // namespace segment_v2
} // namespace doris
