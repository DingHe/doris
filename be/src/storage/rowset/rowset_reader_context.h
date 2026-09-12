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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_ROWSET_READER_CONTEXT_H
#define DORIS_BE_SRC_OLAP_ROWSET_ROWSET_READER_CONTEXT_H

#include <optional>
#include <set>
#include <vector>

#include "exprs/score_runtime.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "io/io_common.h"
#include "runtime/runtime_state.h"
#include "storage/index/ann/ann_topn_runtime.h"
#include "storage/olap_common.h"
#include "storage/predicate/column_predicate.h"
#include "storage/rowid_conversion.h"

namespace doris {

class RowCursor;
class DeleteBitmap;
class DeleteHandler;
class TabletSchema;
// RowsetReaderContext 是存储引擎中连接上层查询执行引擎/后台任务与底层行集读取器（RowsetReader）的配置上下文结构体。
// 通过封装全套的读取配置参数，指导 BetaRowsetReader 及其底层的 SegmentIterator 如何打开文件、应用过滤条件、进行下推优化以及管理内存与批次。
// 定义读取场景与模式：区分当前读取是来自上层 SQL 查询（Query）、Compaction 任务、还是 Schema Change / Binlog 导出。
// 指导谓词与索引下推：承载标量谓词、区间 Bound 谓词、Delete 谓词、向量检索（ANN）、倒排索引以及表达式下推信息，用于跳过无效的数据 Page/Segment。
// 管理列投影（Projection）与 Schema：明确指示存储引擎需要真正从磁盘读取并返回哪些列（return_columns），以及哪些列仅用于过滤计算。
// 控制排序与聚合下推：传递 ORDER BY 限制、逆序读取（ORDER BY DESC LIMIT）、TopN 下推以及 Group By / Count 聚合下推标记。
// 支持 Unique Key 模型特性：提供 Merge-on-Read (MoR) 和 Merge-on-Write (MoW) 所需的 DeleteBitmap、Sequence 列索引以及 RowID 转换映射。
// 资源与性能调优：透传 RuntimeState、PageCache 开销开关、Adaptive Batch Size 内存预算及 OlapReaderStatistics 统计信息采集指针。
struct RowsetReaderContext {
    // 标识触发本次读取的场景类型（如 READER_QUERY 普通查询、READER_ALTER_TABLE 变表、READER_CUMULATIVE_COMPACTION / READER_BASE_COMPACTION 等）。存储引擎会据此调整内部的 IO 优先级和缓存策略。
    ReaderType reader_type = ReaderType::READER_QUERY;
    // 是否读取数据行的增量变更日志（Binlog）。当设为 true 时，Reader 会包含辅助元数据列以导出完整的 Binlog。
    bool read_row_binlog = false;
    // 指定要读取的数据版本区间（包含起始和终止版本号，如 [1, 10]）。
    Version version {-1, -1};
    // 当前 Tablet 的元数据 Schema 智能指针，定义了所有列的类型、Key 类型、索引定义以及编码方式。
    TabletSchemaSPtr tablet_schema = nullptr;
    // 在分布式 TopN 动态过滤下推场景中，记录产生当前 TopN 过滤条件的上层 PlanNode ID 列表。
    std::vector<int> topn_filter_source_node_ids;
    // TopN 动态过滤的目标 PlanNode ID。
    int topn_filter_target_node_id = -1;
    // whether rowset should return ordered rows.
    // 指示当前 RowsetReader 是否必须输出按 Key 有序的数据。如果为 false，多 Segment 间可以采用无需排序的 VUnionIterator 并行读取，极大提升吞吐量。
    bool need_ordered_result = true;
    // used for special optimization for query : ORDER BY key DESC LIMIT n
    // 针对 ORDER BY key DESC LIMIT n 查询的专门优化。设为 true 时，SegmentIterator 会逆序扫描 Key 索引Page。
    bool read_orderby_key_reverse = false;
    // For rows with the same key, use ascending order (small-to-large) for tie-breakers.
    // For example, use lower rowset version / segment id first.
    // 在 Key 相同时的决胜（Tie-breaker）规则。如果为 true，当 Key 相同时按写入顺序（如较低的 Rowset Version 或 Segment ID）排列。
    bool use_insert_order_when_same = false;
    // 是否强制按 Key 有序读取。即使某些条件允许无序，强制开启后也会使用 VMergeIterator 进行归并排序。
    bool force_key_ordered_read = false;
    // columns for orderby keys
    // 下推的 ORDER BY 列的 Column ID 列表，供存储引擎内部实现物理排序列的快速比较。
    std::vector<uint32_t>* read_orderby_key_columns = nullptr;
    // limit of rows for read_orderby_key
    // ORDER BY 列下推时的 LIMIT 截断行数阈值。
    size_t read_orderby_key_limit = 0;
    // projection columns: the set of columns rowset reader should return
    // 列投影集合。指针指向存储引擎真正需要返回给上层的列 Column ID 列表（非列表中的列尽量不进行磁盘 IO）。
    const std::vector<uint32_t>* return_columns = nullptr;
    // TSO predicate column that is absent from return_columns but must be read by storage.
    // 用于分布式一致性/混合逻辑时钟（TSO）的过滤列 Column ID。即使该列不在 return_columns 中，存储引擎也必须强制读取它。
    std::optional<ColumnId> tso_predicate_column_id;
    // Binlog/snapshot incremental read TSO range (start_tso, end_tso]. BetaRowsetReader builds
    // the tso comparison predicates from this range and forces them onto read options.
    // Binlog 或快照增量读取的起始时间戳 TSO（开区间 (start_tso, ...）。
    std::optional<int64_t> start_tso;
    // Binlog 或快照增量读取的终止时间戳 TSO（闭区间 ..., end_tso]）。BetaRowsetReader 会基于 [start_tso, end_tso] 自动构建 TSO 范围谓词。
    std::optional<int64_t> end_tso;
    // 下推的聚合操作类型（如 COUNT、MIN、MAX 等）。当整个 Segment 满足全表 Count 或索引聚合时，可直接读取元数据返回而无需逐行扫描。
    TPushAggOp::type push_down_agg_type_opt = TPushAggOp::NONE;
    // column name -> column predicate
    // adding column_name for predicate to make use of column selectivity
    // 指针指向下推到存储层的一般列谓词条件列表（如 a > 10），用于ZoneMap 过滤、BloomFilter 过滤及行级过滤。
    const std::vector<std::shared_ptr<ColumnPredicate>>* predicates = nullptr;
    // value column predicate in UNIQUE table
    // 指针指向 UNIQUE KEY 模型表专用的 Value 列（非 Key 列）谓词条件列表。
    const std::vector<std::shared_ptr<ColumnPredicate>>* value_predicates = nullptr;
    // 主键/前缀索引查询的下界 Key 列表（起点）。
    const std::vector<RowCursor>* lower_bound_keys = nullptr;
    // 标识对应下界 Key 是否包含边界（>= 为 true，> 为 false）。
    const std::vector<bool>* is_lower_keys_included = nullptr;
    // 主键/前缀索引查询的上界 Key 列表（终点）。
    const std::vector<RowCursor>* upper_bound_keys = nullptr;
    // 标识对应上界 Key 是否包含边界（<= 为 true，< 为 false）。
    const std::vector<bool>* is_upper_keys_included = nullptr;
    // 存储累积的 DELETE FROM 语句生成的删除谓词句柄，用于在 Merge-on-Read 模式下过滤已删除的行。
    const DeleteHandler* delete_handler = nullptr;
    // 收集读取性能指标的统计指针（如磁盘读取字节数、PageCache 命中率、谓词过滤行数等）。
    OlapReaderStatistics* stats = nullptr;
    // 指向 Doris 查询执行层的全局运行时状态对象，Reader 借助它判断查询是否已被取消（is_cancelled()）或获取内存 Tracker。
    RuntimeState* runtime_state = nullptr;
    // 下推到存储层的复杂通用向量化表达式（VExpr）上下文列表，用于短路过滤或高级计算。
    VExprContextSPtrs common_expr_ctxs_push_down;
    // 是否启用 Storage PageCache（数据页缓存）。普通 Query 设为 true，Compaction 任务通常设为 false 以防 PageCache 污染。
    bool use_page_cache = false;
    // 如果表指定了 Sequence 列（用于解决相同 Key 写入乱序覆盖问题），记录 Sequence 列在 Schema 中的列索引。
    int sequence_id_idx = -1;
    // 单次 next_batch 预期望拉取的行数（默认 1024 行）。
    int batch_size = 1024;
    // Effective adaptive batch size byte budget. 0 means disabled internally.
    // 自适应 Block 内存预算大小（默认 8MB）
    // 配合 batch_size 动态调整单批次返回的真实行数，防止大宽表导致内存爆炸。
    size_t preferred_block_size_bytes = 8388608UL;
    // 标识当前表是否为 UNIQUE KEY 模型表。
    bool is_unique = false;
    //record row num merged in generic iterator
    // 输出参数指针，用于累计记录在 Generic Iterator 中因为 Key 合并（如 Aggregate / Unique 覆盖）而剔除的旧版本行数。
    uint64_t* merged_rows = nullptr;
    // for unique key merge on write
    // 标识当前 Unique Key 表是否开启了 写时合并（Merge-on-Write, MoW） 模式。
    bool enable_unique_key_merge_on_write = false;
    // Unique Key MoW 模式下的删除位图（DeleteBitmap）。标识哪些 Segment 的哪些 Row ID 已经被后写入的数据标记删除。
    DeleteBitmapPtr delete_bitmap = nullptr;
    // 指示在读取过程中是否需要记录并输出每一行数据对应的物理 RowID（Segment ID + 行号）。
    bool record_rowids = false;
    // RowID 映射转换器指针，用于 Compaction 或 Partial Update（部分列更新）过程中旧 RowID 到新 RowID 的定位追踪。
    RowIdConversion* rowid_conversion = nullptr;
    // 标识当前读取是否作用于 Key 列的列组（Column Group）。
    bool is_key_column_group = false;
    // 输出列的索引集合，辅助计算 Schema 映射。
    const std::set<int32_t>* output_columns = nullptr;
    // 除了 return_columns 外，因为某些下推表达式计算或过滤需要额外读取的补全列集合。
    std::set<ColumnId> extra_columns;
    // 当前读取的 Rowset 唯一标识符 ID。
    RowsetId rowset_id;
    // slots that cast may be eliminated in storage layer
    // 针对 Variant 半结构化动态类型（JSON 列），记录存储层尝试消除 CAST 转换的目标数据类型映射。
    std::map<std::string, DataTypePtr> target_cast_type_for_variants;
    // 表级别的 TTL（数据生存时间）秒数，用于判断数据Page是否因过期可直接跳过。
    int64_t ttl_seconds = 0;
    // 虚拟列（Virtual Column，如从 Variant 中提取的路径字段或特殊元数据列）的计算表达式映射表。
    std::map<ColumnId, VExprContextSPtr> virtual_column_exprs;
    // 列的全量访问路径映射（如 JSON/Struct 类型的子字段访问路径，如 a.b.c），用于列存的子字段裁减。
    std::map<int32_t, TColumnAccessPaths> all_access_paths;
    // 仅在谓词过滤中用到的子列访问路径映射。
    std::map<int32_t, TColumnAccessPaths> predicate_access_paths;
    // 全文检索或相关性打分（BM25/TF-IDF）时的运行时打分上下文。
    std::shared_ptr<ScoreRuntime> score_runtime;
    // 全文检索时词频与文档集合统计信息的共享指针。
    CollectionStatisticsPtr collection_statistics;
    // 向量检索（近似最近邻 ANN TopN）的运行时上下文，包含向量索引的搜索参数及 Filter 缓存。
    std::shared_ptr<segment_v2::AnnTopNRuntime> ann_topn_runtime;
    // 当前下推条件集合的哈希摘要（Digest），用于查询或索引条件缓存的快速 Hash Match。
    uint64_t condition_cache_digest = 0;

    // When true, push down value predicates for MOR tables
    // 针对 Merge-on-Read (MoR) 表，是否开启将 Value 列谓词直接下推到 SegmentIterator 的优化开关（传统 MoR 由于多版本覆盖，通常不能直接下推 Value 谓词）。
    bool enable_mor_value_predicate_pushdown = false;

    // General LIMIT budget forwarded to SegmentIterator.
    // 下推给 SegmentIterator 的通用读取数据上限限制（Limit Budget），-1 表示无限制。当扫描行数达到此 Limit 时直接提前终止 IO。
    int64_t general_read_limit = -1;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_ROWSET_READER_CONTEXT_H
