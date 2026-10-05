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

#include "exec/scan/parallel_scanner_builder.h"

#include <cstddef>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet_hotspot.h"
#include "cloud/config.h"
#include "common/status.h"
#include "exec/operator/olap_scan_operator.h"
#include "exec/scan/olap_scanner.h"
#include "io/io_common.h"
#include "runtime/query_context.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/segment/segment_loader.h"
#include "storage/tablet/base_tablet.h"

namespace doris {

namespace {

io::FileCacheStatistics take_initial_file_cache_stats(
        std::unordered_map<int64_t, io::FileCacheStatistics>* preload_stats, int64_t tablet_id) {
    auto it = preload_stats->find(tablet_id);
    if (it == preload_stats->end()) {
        return {};
    }
    auto stats = std::move(it->second);
    preload_stats->erase(it);
    return stats;
}

io::IOContext create_preload_io_context(RuntimeState* state, OlapReaderStatistics* preload_stats) {
    io::IOContext io_ctx;
    io_ctx.reader_type = ReaderType::READER_QUERY;
    io_ctx.file_cache_stats = preload_stats ? &preload_stats->file_cache_stats : nullptr;
    if (state == nullptr) {
        return io_ctx;
    }
    io_ctx.query_id = &state->query_id();
    io_ctx.read_file_cache = state->query_options().enable_file_cache;
    io_ctx.is_disposable = state->query_options().disable_file_cache;
    if (auto* query_ctx = state->get_query_ctx(); query_ctx != nullptr) {
        io_ctx.remote_scan_cache_write_limiter = query_ctx->remote_scan_cache_write_limiter();
    }
    return io_ctx;
}

} // namespace

// 生成并发扫描器（OlapScanner）的顶层入口函数。
// 核心职责是根据当前表的数据模型和系统配置，决定并执行最合适的并行切分策略（按 Segment 文件切分、按 Row ID 粒度切分等），最终将实例化好的 Scanner 注入到出参列表中。
// 参数 scanners (出参，引用传递)：
Status ParallelScannerBuilder::build_scanners(std::list<ScannerSPtr>& scanners) {
    // 预加载元数据与行数计算
    RETURN_IF_ERROR(_load());
    // 分支一 — 强制按 Segment 粒度切分
    // 跳过所有基于行数的切割算法，直接以物理 Segment 文件为最小不可分割单元，为每一个 Segment 文件都独立创建一个 OlapScanner。
    if (_scan_parallelism_by_per_segment) {
        return _build_scanners_by_per_segment(scanners);
    // 分支二 — 明细表/写时合并表的默认 Row ID 滑动切分
    // Duplicate Key（明细模型）：存储的数据完全无须在存储层做合并（No Merge）。
    // Unique Key with MOW（Merge-On-Write，写时合并）：在写入时已经完成了 Delete Bitmap 的标记，读取时同样无须做跨 Version 的 Merge。
    } else if (_is_dup_mow_key) {
        // Default strategy for DUP/MOW tables: split by rowids within segments
        return _build_scanners_by_rowid(scanners);
    } else {
    // 进入 else 分支意味着当前表属于 Aggregate 模型（聚合表） 或 Unique Key with MOR（Merge-On-Read，读时合并） 模型，且未开启 Pre-aggregation（预聚合）。
    // 为什么不能用 Row ID 切分：因为 MOR/聚合表在读取时，相同的 Primary/Sort Key 的不同版本行可能分散在不同的 Rowset 或 Segment 中，必须保证同一个 Key 的所有历史版本数据被发送给同一个 Scanner 统一进行 Merge/Aggregate 逻辑，否则会导致聚合结果不正确。
    // 当前处理方式：若要对此类表做并行切分，理论上必须按照 Sort Key 的区间（Key Range）切分。但目前代码注释中指出 split by key range not supported yet，因此直接返回 Status::NotSupported(...) 状态码，上层收到后会退化为传统的非 Parallel Scan 模式进行串行/单 Tablet 扫描。
        // TODO: support to split by key range
        return Status::NotSupported("split by key range not supported yet.");
    }
}
// 根据预先算好的目标行数（_rows_per_scanner），按照 RowID（行号区间） 粒度将 Tablet 中的 Segment 细分为多个互不重叠或按量配比的读取范围（RowRanges），进而构建出多个可以并行执行的 OlapScanner。
Status ParallelScannerBuilder::_build_scanners_by_rowid(std::list<ScannerSPtr>& scanners) {
    DCHECK_GE(_rows_per_scanner, _min_rows_per_scanner);

    for (auto&& [tablet, version] : _tablets) {
        // 断言该 Tablet 必须在 _all_read_sources 映射表中存在，并获取该 Tablet 完整的读取源 entire_read_source（包含所有的 Rowset 切片、删除谓词和 Delete Bitmap）
        DCHECK(_all_read_sources.contains(tablet->tablet_id()));
        auto& entire_read_source = _all_read_sources[tablet->tablet_id()];
        // 云原生模式统计：如果是存算分离（Cloud Mode），更新当前 Tablet 的访问热度（Hotspot Statistics），以便后续支持数据缓存（Cache）淘汰策略等。
        if (config::is_cloud_mode()) {
            // FIXME(plat1ko): Avoid pointer cast
            ExecEnv::GetInstance()->storage_engine().to_cloud().tablet_hotspot().count(*tablet);
        }

        // `rs_splits` in `entire read source` will be devided into several partitial read sources
        // to build several parallel scanners, based on segment rows number. All the partitial read sources
        // share the same delete predicates from their corresponding entire read source.
        // 用于收集构建当前这一个 Scanner 所需的切片信息（数据子集）。
        TabletReadSource partitial_read_source;
        // 记录当前正在组装的 Scanner 已经累积收集了多少行数据。
        int64_t rows_collected = 0;
        for (auto& rs_split : entire_read_source.rs_splits) {
            auto reader = rs_split.rs_reader;
            auto rowset = reader->rowset();
            const auto rowset_id = rowset->rowset_id();
            // 获取该 Rowset 中各个 Segment 的具体行数数组（例如 [10000, 10000, 5000]）。
            const auto& segments_rows = _all_segments_rows[rowset_id];
            // 过滤空 Rowset：若 rowset->num_rows() == 0，跳过处理。
            if (rowset->num_rows() == 0) {
                continue;
            }

            int64_t segment_start = 0;
            auto split = RowSetSplits(reader->clone());
            // Segment 内部按行切分（核心循环）
            for (size_t i = 0; i != segments_rows.size(); ++i) {
                const size_t rows_of_segment = segments_rows[i];
                RowRanges row_ranges;
                int64_t offset_in_segment = 0;

                // try to split large segments into RowRanges
                // 当当前 Segment 还有剩余行数时，持续进行切分。
                while (offset_in_segment < rows_of_segment) {
                    const int64_t remaining_rows = rows_of_segment - offset_in_segment;
                    auto rows_need = _rows_per_scanner - rows_collected;

                    // 0.9: try to avoid splitting the segments into excessively small parts.
                    // 尾数防碎片优化：如果当前 Scanner 还需要的数据量 rows_need 已经占了 Segment 剩余行数的 90% 以上，则不再强制切割，直接把剩余行数全部分给当前 Scanner。这避免了把 Segment 尾部切成非常小的碎片（Tail Chunk）。
                    if (rows_need >= remaining_rows * 9 / 10) {
                        rows_need = remaining_rows;
                    }
                    DCHECK_LE(rows_need, remaining_rows);

                    // RowRange stands for range: [From, To), From is inclusive, To is exclusive.
                    row_ranges.add({offset_in_segment,
                                    offset_in_segment + static_cast<int64_t>(rows_need)});
                    rows_collected += rows_need;
                    offset_in_segment += rows_need;

                    // If collected enough rows, build a new scanner
                    // 凑满一个 Scanner 并触发构建
                    if (rows_collected >= _rows_per_scanner) {
                        split.segment_offsets.first = segment_start,
                        split.segment_offsets.second = i + 1;
                        split.segment_row_ranges.emplace_back(std::move(row_ranges));

                        DCHECK_EQ(split.segment_offsets.second - split.segment_offsets.first,
                                  split.segment_row_ranges.size());

                        partitial_read_source.rs_splits.emplace_back(std::move(split));

                        scanners.emplace_back(_build_scanner(
                                tablet, version, _key_ranges,
                                {.rs_splits = std::move(partitial_read_source.rs_splits),
                                 .delete_predicates = entire_read_source.delete_predicates,
                                 .delete_bitmap = entire_read_source.delete_bitmap},
                                take_initial_file_cache_stats(&_tablet_preload_file_cache_stats,
                                                              tablet->tablet_id())));

                        partitial_read_source = {};
                        split = RowSetSplits(reader->clone());
                        row_ranges = RowRanges();

                        segment_start = offset_in_segment < rows_of_segment ? i : i + 1;
                        rows_collected = 0;
                    }
                }

                // The non-empty `row_ranges` means there are some rows left in this segment not added into `split`.
                if (!row_ranges.is_empty()) {
                    DCHECK_GT(rows_collected, 0);
                    DCHECK_EQ(row_ranges.to(), rows_of_segment);
                    split.segment_row_ranges.emplace_back(std::move(row_ranges));
                }
            }

            DCHECK_LE(rows_collected, _rows_per_scanner);
            if (rows_collected > 0) {
                split.segment_offsets.first = segment_start;
                split.segment_offsets.second = segments_rows.size();
                DCHECK_GT(split.segment_offsets.second, split.segment_offsets.first);
                DCHECK_EQ(split.segment_row_ranges.size(),
                          split.segment_offsets.second - split.segment_offsets.first);
                partitial_read_source.rs_splits.emplace_back(std::move(split));
            }
        } // end `for (auto& rowset : rowsets)`

        DCHECK_LE(rows_collected, _rows_per_scanner);
        if (rows_collected > 0) {
            DCHECK_GT(partitial_read_source.rs_splits.size(), 0);
#ifndef NDEBUG
            for (auto& split : partitial_read_source.rs_splits) {
                DCHECK(split.rs_reader != nullptr);
                DCHECK_LT(split.segment_offsets.first, split.segment_offsets.second);
                DCHECK_EQ(split.segment_row_ranges.size(),
                          split.segment_offsets.second - split.segment_offsets.first);
            }
#endif
            scanners.emplace_back(
                    _build_scanner(tablet, version, _key_ranges,
                                   {.rs_splits = std::move(partitial_read_source.rs_splits),
                                    .delete_predicates = entire_read_source.delete_predicates,
                                    .delete_bitmap = entire_read_source.delete_bitmap},
                                   take_initial_file_cache_stats(&_tablet_preload_file_cache_stats,
                                                                 tablet->tablet_id())));
        }
    }

    return Status::OK();
}

// Build scanners so that each segment is exclusively scanned by a single scanner.
// This guarantees the number of scanners equals the number of segments across all rowsets
// for the involved tablets. It preserves delete predicates and key ranges, and clones
// RowsetReader per scanner to avoid sharing between scanners.
// 按物理 Segment 文件粒度构建并发扫描器（OlapScanner）的实现函数。
// 与按 Row ID 粒度切割（切分成指定行数区间）的切分策略不同，该函数的算法非常直接且简单：以每一个独立的物理 Segment 文件为最小不可切割单元，为每个 Segment 独立构建一个 OlapScanner。
// 出参 scanners (引用传递)： 用于收集并接收所有实例化完成的 OlapScanner 智能指针。函数内部会将创建好的 Scanner 追加（emplace_back）到该链表中，供上层线程池调度。
Status ParallelScannerBuilder::_build_scanners_by_per_segment(std::list<ScannerSPtr>& scanners) {
    DCHECK_GE(_rows_per_scanner, _min_rows_per_scanner);
    // 遍历 Tablet 与存算分离（Cloud Mode）热点统计
    // 遍历所有待扫描的 Tablet 及其数据版本号（Version）。
    for (auto&& [tablet, version] : _tablets) {
        // 确保 _load() 阶段已经在 _all_read_sources 映射字典中填入了当前 tablet_id 对应的 TabletReadSource。
        DCHECK(_all_read_sources.contains(tablet->tablet_id()));
        // 获取当前 Tablet 完整的 TabletReadSource 资源引用（包含该 Tablet 下所有 Rowset 切片、删除谓词和 Delete Bitmap）
        auto& entire_read_source = _all_read_sources[tablet->tablet_id()];
        // 存算分离模式处理：如果是 Doris 存算分离（Cloud Mode/Compute Node）模式，调用 tablet_hotspot().count(*tablet)，对当前 Tablet 进行访问热点计数，用于存算分离架构下的数据 Cache 淘汰与热点调度策略。
        if (config::is_cloud_mode()) {
            // FIXME(plat1ko): Avoid pointer cast
            ExecEnv::GetInstance()->storage_engine().to_cloud().tablet_hotspot().count(*tablet);
        }

        // For each RowSet split in the read source, split by segment id and build
        // one scanner per segment. Keep delete predicates shared.
        // 遍历 Rowset 分片与空数据检查
        // 遍历当前 Tablet 包含的每一个 Rowset 分片（RowSet Split）。
        for (auto& rs_split : entire_read_source.rs_splits) {
            // 获取该 Rowset 的读取器句柄（RowsetReader）。
            auto reader = rs_split.rs_reader;
            auto rowset = reader->rowset();
            const auto rowset_id = rowset->rowset_id();
            // 从 _load() 准备好的字典中取出该 Rowset 内各个 Segment 的行数列表。
            const auto& segments_rows = _all_segments_rows[rowset_id];
            // 过滤空数据。如果该 Rowset 没有 Segment 或者总行数为 0，则跳过不处理，避免创建无意义的空 Scanner。
            if (segments_rows.empty() || rowset->num_rows() == 0) {
                continue;
            }

            // Build scanners for [i, i+1) segment range, without row-range slicing.
            // Segment 文件级别的范围切割 (按 Segment 分片)
            // 遍历当前 Rowset 中的每一个 Segment（索引为 i）。
            for (int64_t i = 0; i < rowset->num_segments(); ++i) {
                // 克隆 Reader。为当前 Segment 切片独立克隆（clone()）一份新的 RowsetReader 句柄，保证多线程并发读取时内部状态（指针、Seek 位置）互不干扰。
                RowSetSplits split(reader->clone());
                // 设置当前切片的起始 Segment 物理偏移量为 i。
                split.segment_offsets.first = i;
                split.segment_offsets.second = i + 1;
                // No row-ranges slicing; scan whole segment i.
                DCHECK_GE(split.segment_offsets.second, split.segment_offsets.first + 1);
                // 构造局部读取源、创建 Scanner 并入队
                TabletReadSource partitial_read_source;
                partitial_read_source.rs_splits.emplace_back(std::move(split));
                // 调用 _build_scanner 实例方法，创建一个新的 OlapScanner 智能指针并直接追加到出参 scanners 链表中。
                scanners.emplace_back(_build_scanner(
                        tablet, version, _key_ranges,
                        {.rs_splits = std::move(partitial_read_source.rs_splits),
                         .delete_predicates = entire_read_source.delete_predicates,
                         .delete_bitmap = entire_read_source.delete_bitmap},
                        take_initial_file_cache_stats(&_tablet_preload_file_cache_stats,
                                                      tablet->tablet_id())));
            }
        }
    }

    return Status::OK();
}

/**
 * Load rowsets of each tablet with specified version, segments of each rowset.
 */
// 并行切分算法执行前的元数据初始化与评估函数
Status ParallelScannerBuilder::_load() {
    // 初始化累加器与查询选项读取
    // 将全局待扫描总行数清零，准备开始跨所有 Tablet 累加。
    _total_rows = 0;
    // 定义索引计数器，用于后续遍历时与外部传入的 _read_sources 数组进行下标匹配。
    size_t idx = 0;
    // 判断当前查询是否显式设置了 enable_segment_cache 选项；若已设置则使用用户设置的值，否则默认开启（true） Segment 缓存。开启该缓存后，Segment 的 Footer 元数据读取后会被缓存至内存，避免重复磁盘 I/O。
    bool enable_segment_cache = _state->query_options().__isset.enable_segment_cache
                                        ? _state->query_options().enable_segment_cache
                                        : true;
    // 遍历 Tablet 并建立关联映射
    // 循环遍历 _tablets 容器中的每一个元素（包含 tablet 智能指针和对应的版本号 version）
    for (auto&& [tablet, version] : _tablets) {
        // 获取当前 Tablet 的唯一 ID
        const auto tablet_id = tablet->tablet_id();
        // 将外部传入的第 idx 个 TabletReadSource 资源赋值并存入类内部的 _all_read_sources 哈希表中，建立 tablet_id -> TabletReadSource 的映射字典。
        _all_read_sources[tablet_id] = _read_sources[idx];
        const auto& read_source = _all_read_sources[tablet_id];
        // 遍历 Rowset 分片并加载元数据
        // 遍历当前 Tablet 内部的所有 Rowset 分片（Rowset Split）。一个 Tablet 通常包含多个历史版本的 Rowset。
        for (auto& rs_split : read_source.rs_splits) {
            auto rowset = rs_split.rs_reader->rowset();
            // 调用 rowset->load() 加载当前 Rowset 的元数据（例如解析 Segment 文件的 Footer）。如果磁盘读取异常则通过 RETURN_IF_ERROR 立即向外层返回错误。
            RETURN_IF_ERROR(rowset->load());
            const auto rowset_id = rowset->rowset_id();
            // 转换 Rowset 类型并准备预加载 I/O 上下文
            auto beta_rowset = std::dynamic_pointer_cast<BetaRowset>(rowset);
            std::vector<uint32_t> segment_rows;
            OlapReaderStatistics preload_stats;
            auto preload_io_ctx = create_preload_io_context(_state, &preload_stats);
            // 获取各 Segment 行数与缓存指标合并
            RETURN_IF_ERROR(beta_rowset->get_segment_num_rows(&segment_rows, enable_segment_cache,
                                                              &preload_stats, &preload_io_ctx));
            _tablet_preload_file_cache_stats[tablet_id].merge_from(preload_stats.file_cache_stats);
            // 记录 Segment 行数映射与累加总行数
            auto segment_count = rowset->num_segments();
            for (int64_t i = 0; i != segment_count; i++) {
                _all_segments_rows[rowset_id].emplace_back(segment_rows[i]);
            }
            _total_rows += rowset->num_rows();
        }
        idx++;
    }
    // 计算单个 Scanner 目标行数并返回
    _rows_per_scanner = _total_rows / _max_scanners_count;
    _rows_per_scanner = std::max<size_t>(_rows_per_scanner, _min_rows_per_scanner);

    return Status::OK();
}
// 封装传入的执行状态、Tablet 信息、数据切片源（TabletReadSource）和监控指标，组装出 OlapScanner::Params 参数结构体，并最终实例化创建一个具体的 OlapScanner 智能指针对象
// tablet (BaseTabletSPtr)：  当前 Scanner 需要读取的目标 Tablet 对象的智能指针（包含该 Tablet 的元数据、Schema、存储路径等）。
// version (int64_t)： 读取的数据版本号（Data Version）。例如为某个 Tablet 的读取指定的最大 Version（比如 Read Version = 10）。
// key_ranges (const std::vector<OlapScanRange*>&)： 推到存储层的排序键/主键范围谓词区间（Key Ranges），用于指导底层 Short Key Index 或 Primary Key Index 进行过滤与 Locate 操作。
// read_source (TabletReadSource&&)： 专属于当前这一个 Scanner 的数据源切片包。包含了具体分给该 Scanner 的 Segment 级别/Row ID 级别的 rs_splits、全局删除谓词（Delete Predicates）以及 Delete Bitmap。通过右值引用传入允许使用 std::move 进行零拷贝（Zero-Copy）所有权转移。
// initial_file_cache_stats (io::FileCacheStatistics&&)： 该 Scanner 在构建/元数据预加载阶段产生的初始 FileCache 缓存命中指标。
std::shared_ptr<OlapScanner> ParallelScannerBuilder::_build_scanner(
        BaseTabletSPtr tablet, int64_t version, const std::vector<OlapScanRange*>& key_ranges,
        TabletReadSource&& read_source, io::FileCacheStatistics&& initial_file_cache_stats) {
    OlapScanner::Params params {
            .state = _state,
            .profile = _scanner_profile.get(),
            .key_ranges = key_ranges,
            .tablet = std::move(tablet),
            .version = version,
            .read_source = std::move(read_source),
            .initial_file_cache_stats = std::move(initial_file_cache_stats),
            .limit = _limit,
            .aggregation = _is_preaggregation,
            .read_row_binlog = false,
            .binlog_scan_type = TBinlogScanType::NONE,
            .start_tso = std::nullopt,
            .end_tso = std::nullopt,
    };
    return OlapScanner::create_shared(_parent, std::move(params));
}

} // namespace doris
