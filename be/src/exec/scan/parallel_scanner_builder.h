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

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "exec/scan/olap_scanner.h"
#include "io/io_common.h"
#include "storage/rowset/rowset_fwd.h"
#include "storage/segment/row_ranges.h"
#include "storage/segment/segment_loader.h"
#include "storage/tablet/base_tablet.h"
#include "storage/tablet/tablet.h"

namespace doris {

class OlapScanLocalState;

class Scanner;

using ScannerSPtr = std::shared_ptr<Scanner>;

// 用于实现“并行扫描（Parallel Scan）”切分与 Scanner 构建的核心构建器
// 在 Apache Doris 的 OLAP 扫描引擎中，为了最大化多核 CPU 和 SSD 磁盘 I/O 的吞吐量，一个 Tablet（或多个 Tablet）的数据往往不会交由单个 OlapScanner 串行读取，而是会在数据行数（Row ID）粒度或 Segment 文件粒度上进行动态切割，分配给多个并发的 OlapScanner 共同读取。ParallelScannerBuilder 就是承担该“负载估算、切片划分与 Scanner 实例化”的构建工厂。
// ParallelScannerBuilder 的作用
// 自动计算最优并发度：根据待扫描 Tablet 中的物理总行数（_total_rows）、系统的最大 Scanner 线程限制（_max_scanners_count）以及单个 Scanner 负责的最小行数阈值（_min_rows_per_scanner），动态推算出最佳的 _rows_per_scanner。
// 多模式并行切分：
// 按 Row ID 切分（_build_scanners_by_rowid）：默认且最常用的高效切分方式。按数据行数范围（[StartRow, EndRow]）精准平分算法，将跨 Rowset/Segment 的连续行切块分配给不同 Scanner，保证各并发线程负载极度均衡。
// 按 Segment 粒度切分（_build_scanners_by_per_segment）：在特定的索引扫描（如向量 ANN 检索等）场景下，强制为每个 Segment 文件分配独立的 Scanner。
// 数据源分发与生命周期管理：解析 TabletReadSource 中的 rs_splits，为每个切片分配独自克隆/切割的 rs_reader 副本，避免多线程读取冲突，并收集底层文件缓存（FileCache）的统计指标。
class ParallelScannerBuilder {
public:
    ParallelScannerBuilder(OlapScanLocalState* parent,
                           const std::vector<TabletWithVersion>& tablets,
                           std::vector<TabletReadSource>& read_sources,
                           const std::shared_ptr<RuntimeProfile>& profile,
                           const std::vector<OlapScanRange*>& key_ranges, RuntimeState* state,
                           int64_t limit, bool is_dup_mow_key, bool is_preaggregation)
            : _parent(parent),
              _scanner_profile(profile),
              _state(state),
              _limit(limit),
              _is_dup_mow_key(is_dup_mow_key),
              _is_preaggregation(is_preaggregation),
              _tablets(tablets.cbegin(), tablets.cend()),
              _key_ranges(key_ranges.cbegin(), key_ranges.cend()),
              _read_sources(read_sources) {}
    // scanners (出参)：用于接收构建好的并发 OlapScanner 列表。
    Status build_scanners(std::list<ScannerSPtr>& scanners);
    // 设置允许构建的并发 Scanner 数量的最大上限。
    void set_max_scanners_count(size_t count) { _max_scanners_count = count; }
    // 设置单个 Scanner 分配的最少数据行数
    void set_min_rows_per_scanner(int64_t size) { _min_rows_per_scanner = size; }

    void set_scan_parallelism_by_per_segment(bool v) { _scan_parallelism_by_per_segment = v; }

    const OlapReaderStatistics* builder_stats() const { return &_builder_stats; }

private:
    Status _load();

    Status _build_scanners_by_rowid(std::list<ScannerSPtr>& scanners);

    // Build scanners so that each segment is handled by its own scanner.
    Status _build_scanners_by_per_segment(std::list<ScannerSPtr>& scanners);

    std::shared_ptr<OlapScanner> _build_scanner(BaseTabletSPtr tablet, int64_t version,
                                                const std::vector<OlapScanRange*>& key_ranges,
                                                TabletReadSource&& read_source,
                                                io::FileCacheStatistics&& initial_file_cache_stats);
    // 指向当前 Pipeline 执行引擎中 OlapScan 算子的 LocalState 上下文对象。用于给创建出的 OlapScanner 提供算子级别的上下文依赖。
    OlapScanLocalState* _parent;

    /// Max scanners count limit to build
    // 能够构建的最大 Scanner 数量上限，默认值为 16。可以通过 set_max_scanners_count() 配置（通常根据系统的 CPU 核心数自动调整）。
    size_t _max_scanners_count {16};

    /// Min rows per scanner
    // 单个 Scanner 负责的最少数据行数，默认值为 2,097,152 行（2M 行）。防止在小数据量查询时创建过多无意义的小 Scanner，避免线程上下文切换开销。
    size_t _min_rows_per_scanner {2 * 1024 * 1024};
    // 累加并记录当前所有待扫描 Tablet 中各个 Segment 的实际数据总行数。在 _load() 阶段计算得出。
    size_t _total_rows {};
    // 经过动态计算后，最终决定的每个 Scanner 实际分配的目标数据行数。公式为：std::max(_min_rows_per_scanner, _total_rows / _max_scanners_count)
    size_t _rows_per_scanner {_min_rows_per_scanner};
    // 缓存每个 Rowset 内部各个 Segment 的行数列表。
    // Key 为 RowsetId；
    // Value 为一个数组，按顺序记录该 Rowset 内每个 Segment 的行数（例如 [100000, 100000, 50000]），用于按 Row ID 切分时准确定位行偏移。
    std::map<RowsetId, std::vector<size_t>> _all_segments_rows;
    // 以 tablet_id 为 Key，记录在元数据加载或预热阶段产生的 FileCache 缓存统计信息，最后会汇总到 _builder_stats 中。
    std::unordered_map<int64_t, io::FileCacheStatistics> _tablet_preload_file_cache_stats;

    // Force building one scanner per segment when true.
    // 强制开关。
    // 若为 true，则跳过按 Row ID 切分的算法，直接使用 _build_scanners_by_per_segment 为每一个 Segment 文件创建一个独立 Scanner。
    bool _scan_parallelism_by_per_segment {false};
    // 传递给各个 OlapScanner 的 Performance Profile 句柄，用于收集扫描阶段的耗时、计数器等指标。
    std::shared_ptr<RuntimeProfile> _scanner_profile;
    // 构建器内部的统计信息收集对象，专门用于汇总在构建/预加载 Scanner 阶段产生的底层文件 IO 与缓存命中指标。
    OlapReaderStatistics _builder_stats;
    // 当前的查询执行状态对象，包含了全局查询选项（Query Options）、内存 Tracker 以及会话变量。
    RuntimeState* _state;
    // SQL 下推的 Limit 条件限制值（如 LIMIT 100）。如果存在 Limit，Scanner 在读取达到限制后可提前终止。
    int64_t _limit;
    // 标记当前扫描的表是否属于 Duplicate（明细表）或者 Unique Key MOW（写时合并）模型。这两个模型的特点是行与行之间无须在读取时做合并（No Merge），是进行按 Row ID 并行切分的前提。
    bool _is_dup_mow_key;
    // The flag of preagg's meaning is whether return pre agg data(or partial agg data)
    // PreAgg ON: The storage layer returns partially aggregated data without additional processing. (Fast data reading)
    // for example, if a table is select userid,count(*) from base table.
    // And the user send a query like select userid,count(*) from base table group by userid.
    // then the storage layer do not need do aggregation, it could just return the partial agg data, because the compute layer will do aggregation.
    // PreAgg OFF: The storage layer must complete pre-aggregation and return fully aggregated data. (Slow data reading)
    // 标记存储层是否开启预聚合（Pre-Aggregation）。若为 true，存储层直接返回局部聚合数据，无需在读取时做 Multi-Version Merge，允许并行切分。
    bool _is_preaggregation;
    // 存储待扫描的 Tablet 及其对应版本号（Version）的结构体列表。
    std::vector<TabletWithVersion> _tablets;
    // 下推的主键/排序键过滤范围（OlapScanRange）列表。
    std::vector<OlapScanRange*> _key_ranges;
    // 以 tablet_id 为 Key 建立映射，保存每一个 Tablet 原始的 TabletReadSource 资源包。
    std::unordered_map<int64_t, TabletReadSource> _all_read_sources;
    // 对外部传入的 TabletReadSource 数组的引用，在构造时传入并在内部组装时使用。
    std::vector<TabletReadSource>& _read_sources;
};

} // namespace doris
