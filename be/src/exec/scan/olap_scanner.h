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

#include <gen_cpp/PaloInternalService_types.h>

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/factory_creator.h"
#include "common/status.h"
#include "core/data_type/data_type.h"
#include "exec/scan/scanner.h"
#include "runtime/runtime_state.h"
#include "storage/data_dir.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_reader.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

struct OlapScanRange;
class FunctionFilter;
class RuntimeProfile;
class RuntimeState;
class TPaloScanRange;
class ScanLocalStateBase;
struct FilterPredicates;
#ifndef NDEBUG
struct OlapReaderStatistics;
#endif

namespace io {
struct FileCacheStatistics;
struct IOContext;
} // namespace io

class Block;

io::IOContext build_score_runtime_collection_io_context(RuntimeState* state, ReaderType reader_type,
                                                        int64_t expiration_time,
                                                        io::FileCacheStatistics* file_cache_stats);

// OlapScanner 是继承自 Scanner 基类的核心类之一，专门负责从 Doris 自研的内表存储引擎（即 OLAP 表 / Tablet，对应 Rowset、Segment 文件）中读取列式数据。
// OlapScanner 是 Doris 存储层（Storage Engine）与向量化执行层（Pipeline Execution Engine）之间的关键桥梁。
// 其主要作用包括：
// 对接 OLAP 存储引擎：封装对 Doris 本地/存算分离 OLAP Tablet 的读取逻辑，调用底层的 TabletReader（如 BlockReader）读取 Rowset/Segment 文件中的数据。
// 支持多样化读取特性：
// 扫描范围与裁剪：支持基于 主键/排序键 范围（OlapScanRange）的数据过滤和切片。
// 版本控制与 Binlog：支持历史版本读取、数据变更 Binlog 读取以及基于 TSO（Timestamp Oracle）的时间旅行/增量查询。
// 半结构化与复杂类型扩展：支持 Variant 异构列推导、虚列（Virtual Column）表达式计算等。
// 高级索引与向量检索：支持 ANN（近似最近邻）向量检索运行时上下文（AnnTopNRuntime）及分数计算（ScoreRuntime）。
class OlapScanner : public Scanner {
    ENABLE_FACTORY_CREATOR(OlapScanner);

public:
    struct Params {
        RuntimeState* state = nullptr;
        RuntimeProfile* profile = nullptr;
        std::vector<OlapScanRange*> key_ranges;
        // 当前正在读取的目标 Tablet（分片）的智能指针。
        BaseTabletSPtr tablet;
        // 读取的数据版本号（Data Version）。
        int64_t version;
        // 读取源，包含了关联的 Rowset 句柄、RSReader 集合等。
        TabletReadSource read_source;
        // 初始的 FileCache 文件缓存统计数据，用于性能分析。
        io::FileCacheStatistics initial_file_cache_stats;
        // 当前 Scanner 允许返回的最大行数限制（LIMIT）。
        int64_t limit;
        // 是否开启聚合（例如 Aggregate Key 模型是否需要在存储层做传输前预聚合）。
        bool aggregation;
        // 是否读取行级 Binlog 数据（默认 false）。
        bool read_row_binlog = false;
        // Binlog 扫描类型（如全部、仅变更记录等）。
        TBinlogScanType::type binlog_scan_type = TBinlogScanType::NONE;
        // TSO 事务起始时间戳（用于增量/事务流读取）。
        std::optional<int64_t> start_tso;
        std::optional<int64_t> end_tso;
    };

    OlapScanner(ScanLocalStateBase* parent, Params&& params);

    Status _prepare_impl() override;

    Status _open_impl(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    doris::TabletStorageType get_storage_type() override;

    bool check_partition_pruned() const override;

    void update_realtime_counters() override;

protected:
    Status _get_block_impl(RuntimeState* state, Block* block, bool* eos) override;
    void _collect_profile_before_close() override;

private:
    Status _init_tablet_reader_params(
            const phmap::flat_hash_map<int, SlotDescriptor*>& slot_id_to_slot_desc,
            const std::vector<OlapScanRange*>& key_ranges,
            const phmap::flat_hash_map<int, std::vector<std::shared_ptr<ColumnPredicate>>>&
                    predicates,
            const std::vector<FunctionFilter>& function_filters);

    [[nodiscard]] Status _init_tso_pushdown();
    [[nodiscard]] Status _init_return_columns();
    [[nodiscard]] Status _init_variant_columns();
#ifndef NDEBUG
    Status _check_ann_cache_hit_debug_points(const OlapReaderStatistics& stats);
#endif
    // 当前 Scanner 负责扫描的按 Key 裁剪后的范围列表（主键/排序键区间）。
    std::vector<OlapScanRange*> _key_ranges;

    TabletReader::ReaderParams _tablet_reader_params;
    std::unique_ptr<TabletReader> _tablet_reader;
    std::optional<int64_t> _start_tso;
    std::optional<int64_t> _end_tso;

public:
    std::vector<ColumnId> _return_columns;

    std::unordered_set<uint32_t> _tablet_columns_convert_to_null_set;
    io::FileCacheStatistics _initial_file_cache_stats;

    // This field is copied from OlapScanLocalState.
    std::map<SlotId, VExprContextSPtr> _slot_id_to_virtual_column_expr;

    // ColumnId of virtual column to its expr context
    std::map<ColumnId, VExprContextSPtr> _virtual_column_exprs;
    std::shared_ptr<ScoreRuntime> _score_runtime;

    std::shared_ptr<segment_v2::AnnTopNRuntime> _ann_topn_runtime;

    VectorSearchUserParams _vector_search_params;
};
} // namespace doris
