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

#include <gen_cpp/olap_file.pb.h>
#include <stdint.h>

#include <memory>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "storage/iterators.h"
#include "storage/olap_common.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/schema.h"
#include "storage/segment/segment_loader.h"
#include "util/once.h"

namespace doris {
class RuntimeProfile;
class Schema;
struct RowLocation;
struct RowsetReaderContext;

// BetaRowsetReader 是面向 Segment V2 (Beta 存储格式) 的底层行集（Rowset）数据读取器。
// BetaRowsetReader 继承自 RowsetReader 抽象基类，是连接底层磁盘存储（Segment V2）与上层执行引擎（如向量化执行引擎 Vectorized Engine）以及后端后台任务（如 Compaction、Schema Change）的关键桥梁。
// 数据读取迭代封装：屏蔽物理文件存储的细节，将单个 Rowset 内包含的一个或多个 Segment 文件抽象为一个统一的按批次（Block / BlockView / BlockWithSameBit）吐数据的迭代器。
// 延迟初始化与迭代器按需构建：在调用 next_batch 提取数据时，通过懒加载（Lazy Initialization）策略将多个 Segment 迭代器组合成最终的数据读取流（有序读取时构建 VMergeIterator 归并排序，无序读取时构建 VUnionIterator 并行或串行拼接）。
// 下推过滤与谓词裁剪：根据上层传入的 RowsetReaderContext，将查询谓词、Delete Predicate（删除谓词）、 ZoneMap 索引、前缀索引、BloomFilter 及倒排索引下推到 Segment 层面，甚至支持将整行谓词或短路谓词在 SegmentIterator 内部过滤掉。
// 统计信息与 Profile 收集：实时追踪 IO 次数、过滤行数、解压耗时、索引命中情况等统计数据，并汇总更新到系统的 RuntimeProfile 中。
// 支持特定分段与行号范围读取：支持只读取 Rowset 中某个范围的 Segment（rs_splits），以及基于 Primary Key / Unique Key 或索引计算出的特定行号区间（RowRanges）。
class BetaRowsetReader : public RowsetReader {
public:
    // 接收并保存传入的 BetaRowset 智能指针（即当前读取器对应的 Rowset 物理对象），初始化基础引用。
    BetaRowsetReader(BetaRowsetSharedPtr rowset);

    ~BetaRowsetReader() override { _rowset->release(); }
    // 初始化读取器的上下文环境。保存传入的 read_context（包含谓词、读取列、Schema 信息、运行时状态等），
    // 解析 rs_splits（确定当前 Reader 负责读取该 Rowset 的哪些 Segment 以及哪些行号范围 RowRanges），设置对应的统计信息指针，并配置 StorageReadOptions。
    Status init(RowsetReaderContext* read_context, const RowSetSplits& rs_splits) override;

    Status get_segment_iterators(RowsetReaderContext* read_context,
                                 std::vector<RowwiseIteratorUPtr>* out_iters,
                                 bool use_cache = false) override;
    // 根据最新的 _read_context 重新重置或更新内部的 _read_options 属性（例如在 Segment 内部扫描谓词下推变动时进行重置）。
    void reset_read_options() override;
    // 提取下一批次数据到 Doris 向量化列存块 Block 中。内部直接转调用模版私有方法 _next_batch(block)。
    Status next_batch(Block* block) override { return _next_batch(block); }
    // 提取下一批次数据到 BlockView 视图对象中（通常用于特定过滤或轻量级视图场景），内部调用 _next_batch(block_view)。
    Status next_batch(BlockView* block_view) override { return _next_batch(block_view); }
    // 提取下一批次数据到带有 Bit 标记的数据块 BlockWithSameBit 中，内部调用 _next_batch(block_with_same_bit)。
    Status next_batch(BlockWithSameBit* block_with_same_bit) override {
        return _next_batch(block_with_same_bit);
    }
    // 判定在当前 Rowset Reader 内部，读取多个 Segment 时是否需要采用“多路归并迭代器”（Merge Iterator）来保证按 Key 序输出数据，还是可以直接采用“顺序追加迭代器”（Union / Concat Iterator）
    // Merge Iterator（多路归并读取）
    // 利用最小堆对多个 Segment 的首行 Key 进行比较，每次弹出最小/最大的一行，保证全局有序。开销相对较大。
    // 前提：Segments 之间存在重叠（Overlapping），或者强制要求按 Key 严格有序。
    bool is_merge_iterator() const override {
        // 上层算子（如 TopN / GroupBy / Merge-on-Read）显式要求存储层输出的数据必须严格按照 Key 排序。
        return _read_context->need_ordered_result &&
               // Segment 数量是否大于 1
                _get_segment_num() > 1 &&
                // 否重叠或强制排序
               (_rowset->rowset_meta()->is_segments_overlapping() ||
                _read_context->force_key_ordered_read);
    }
    // 返回当前 Rowset 是否带有删除标记（即当前 Rowset 是否是由 DELETE 语句生成的专门用于标记删除的数据集）。
    bool delete_flag() override { return _rowset->delete_flag(); }
    // 返回当前 Rowset 的版本号（Version 结构体，包含起始版本号和终止版本号，如 [2-2]）。
    Version version() override { return _rowset->version(); }
    // 返回当前 Rowset 中数据的最新写入时间戳（以毫秒/秒为单位）。
    int64_t newest_write_timestamp() override { return _rowset->newest_write_timestamp(); }
    // 获取当前 Reader 所绑定的底层 Rowset 对象的通用智能指针（强转为基类 Rowset 类型）。
    RowsetSharedPtr rowset() override { return std::dynamic_pointer_cast<Rowset>(_rowset); }

    // Return the total number of filtered rows, will be used for validation of schema change
    // 计算并返回在该 Rowset 读取过程中被各类谓词条件过滤掉的行数总和。统计项包含：删除谓词过滤行数、Bitmap 过滤行数、普通谓词过滤行数、向量化谓词过滤行数、短路谓词过滤行数等。
    int64_t filtered_rows() override {
        return _stats->rows_del_filtered + _stats->rows_del_by_bitmap +
               _stats->rows_conditions_filtered + _stats->rows_vec_del_cond_filtered +
               _stats->rows_vec_cond_filtered + _stats->rows_short_circuit_cond_filtered;
    }
    // 返回在进行 Key 聚合/合并（如 Aggregate Key / Unique Key 模型）时被合并掉的行数（从 _read_context->merged_rows 中取出）。
    uint64_t merged_rows() override { return *(_read_context->merged_rows); }
    // 返回当前 Rowset 的存储类型枚举，始终返回 RowsetTypePB::BETA_ROWSET。
    RowsetTypePB type() const override { return RowsetTypePB::BETA_ROWSET; }
    // 获取当前提取出的这一批次 Block 中，每一行数据在物理存储中的精确位置（Segment ID + Row ID），主要用于点查更新或 Unique Key 模型的主键索引定位。
    Status current_block_row_locations(std::vector<RowLocation>* locations) override {
        return _iterator->current_block_row_locations(locations);
    }
    // 将当前 Reader 读取过程中汇总在 _stats 中的各项执行耗时、磁盘 IO、解压开销等指标，更新/附加到上层查询引擎传入的 RuntimeProfile 对象中，方便在 FE 页面查看 Profile。
    void update_profile(RuntimeProfile* profile) override;
    // 克隆当前 BetaRowsetReader 对象，生成一个新的共享实例（用于并行读取或并发 Query 场景）。
    RowsetReaderSharedPtr clone() override;
    // 设置 TopN 优化限制行数 topn_limit。下推给底层的 Segment 读取器，使其在满足 TopN 条件时提前终止扫描或跳过部分数据。
    void set_topn_limit(size_t topn_limit) override { _topn_limit = topn_limit; }
    // 暴露内部指针，返回当前的 OLAP 扫描统计信息对象 _stats。
    OlapReaderStatistics* get_stats() { return _stats; }

private:
    // 是所有 next_batch 重载的真正底层实现逻辑：
    template <typename T>
    Status _next_batch(T* block) {
        RETURN_IF_ERROR(_init_iterator_once());
        SCOPED_RAW_TIMER(&_stats->block_fetch_ns);
        if (_empty) {
            return Status::Error<ErrorCode::END_OF_FILE>("BetaRowsetReader is empty");
        }

        RuntimeState* runtime_state = nullptr;
        if (_read_context != nullptr) {
            runtime_state = _read_context->runtime_state;
        }

        do {
            Status s = _iterator->next_batch(block);
            if (!s.ok()) {
                if (!s.is<ErrorCode::END_OF_FILE>()) {
                    LOG(WARNING) << "failed to read next block: " << s.to_string();
                }
                return s;
            }

            if (runtime_state != nullptr && runtime_state->is_cancelled()) [[unlikely]] {
                return runtime_state->cancel_reason();
            }
        } while (block->empty());

        return Status::OK();
    }
    // 基于 DorisCallOnce（_init_iter_once）包装的线程安全延迟初始化函数。确保 _init_iterator() 方法在多线程/多次调用 next_batch 时只会被成功触发一次。
    [[nodiscard]] Status _init_iterator_once();
    // 构建底层读取迭代器的最核心实现：
    [[nodiscard]] Status _init_iterator();
    // 判断当前是否可以将 Value（普通非 Key 列）谓词直接下推到 SegmentIterator 中进行行级过滤（例如判断模型类型是 Dup Key 还是 Agg Key/Unique Key 等）。
    bool _should_push_down_value_predicates() const;
    // 计算并返回当前 Reader 实际上需要扫描的 Segment 文件数量。若 _segment_offsets 没有指定范围，则默认计算 Rowset 所有的 Segment 数量。
    int64_t _get_segment_num() const {
        auto [seg_start, seg_end] = _segment_offsets;
        if (seg_start == seg_end) {
            seg_start = 0;
            seg_end = _rowset->num_segments();
        }
        return seg_end - seg_start;
    }
    // 保证 _init_iterator() 线程安全且仅执行一次的控制工具。
    DorisCallOnce<Status> _init_iter_once;
    // 存储当前 Reader 需要负责的 Segment 索引的起始与结束区间 [start, end)。
    std::pair<int64_t, int64_t> _segment_offsets;
    // 存储每个 Segment 内部需要读取的行号区间列表（Row Ranges）。通过 ZoneMap 或主键索引过滤后，不需要全表扫描，只扫描特定的这些行范围。
    std::vector<RowRanges> _segment_row_ranges;

    // _input_schema: includes return_columns + delete_predicate_columns.
    // Used by SegmentIterator internally (iter->schema() returns this). SegmentIterator
    // handles the extra delete predicate columns through _current_return_columns and
    // _evaluate_short_circuit_predicate(), independent of the block structure.
    // e.g. return_columns={c1, c2}, delete_pred on c3 => input_schema={c1, c2, c3}
    // 包含“查询返回列 + 删除谓词依赖列”的总 Schema。SegmentIterator 内部使用该 Schema，以便在读取时评估删除谓词，即使某些删除谓词用到的列并不在查询结果集里。
    SchemaSPtr _input_schema;
    // _output_schema: includes only return_columns (a subset of input_schema).
    // Passed to VMergeIterator/VUnionIterator. block_reset() builds the internal block
    // with this schema, and copy_rows() copies exactly these columns to the destination.
    // e.g. return_columns={c1, c2} => output_schema={c1, c2}
    // 仅包含“上层查询真正需要返回的列”的 Schema。交给 VMergeIterator / VUnionIterator 用于最终构建填充返回给上层的 Block。
    SchemaSPtr _output_schema;
    // 指向外层（通常是 TabletReader）传入的读取上下文指针，包含运行时 RuntimeState、谓词下推列表、版本号、读取模型等全套配置。
    RowsetReaderContext* _read_context = nullptr;
    // 指向当前 Reader 所对应的底层物理 Rowset 实体对象（BetaRowset）。
    BetaRowsetSharedPtr _rowset;
    // 如果外部 _read_context 没有提供 stats 指针，则使用 Reader 内部自己创建并持有的 _owned_stats 来记录读取过程中的统计指标。
    OlapReaderStatistics _owned_stats;
    // 指向实际生效的统计信息对象的指针（优先指向 _read_context->stats，若无则指向 &_owned_stats）。
    OlapReaderStatistics* _stats = nullptr;
    // 最核心的数据提取迭代器。在 _init_iterator() 中被赋值，具体可能是 VMergeIterator、VUnionIterator 或者单个 SegmentIterator，通过调用其 next_batch 提取数据。
    std::unique_ptr<RowwiseIterator> _iterator;
    // 传递给 Segment 内部读取器的各种选项参数集合（如是否使用 PageCache、倒排索引过滤条件、Key 范围限制、IO 上下文等）。
    StorageReadOptions _read_options;
    // 标记当前 Reader 是否为空（例如经索引裁减后发现无任何行符合条件，或者 Rowset 本身为 0 行数据）。如果为 true，next_batch 直接返回 EOF。
    bool _empty = false;
    // 记录 TopN 优化传进来的截断阈值，0 表示无限制。
    size_t _topn_limit = 0;
    // 记录在当前 Reader 内部发生行合并（Compaction 或 Agg 过滤）时被扣除的行数。
    uint64_t _merged_rows = 0;
};

} // namespace doris
