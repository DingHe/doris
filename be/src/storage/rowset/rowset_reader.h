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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_ROWSET_READER_H
#define DORIS_BE_SRC_OLAP_ROWSET_ROWSET_READER_H

#include <gen_cpp/olap_file.pb.h>

#include <memory>

#include "core/block/block.h"
#include "storage/iterators.h"
#include "storage/rowset/rowset_fwd.h"
#include "storage/rowset/rowset_reader_context.h"
#include "storage/segment/row_ranges.h"

namespace doris {

class Block;
// RowSetSplits 是用于细粒度切分与并行扫描 Rowset 的上下文数据结构。
// 物理分段（Segment-level Split）：允许将一个包含多个 Segment 的 Rowset 切分成多个任务，只扫描特定范围的 Segment [start, end)。
// 行号筛选（RowRange-level Split）：包含每个 Segment 内部经过索引裁减后需要扫描的实际行号区间（RowRanges）。
// 并行度优化：在 Pipeline 执行引擎或 Duplicate Key 模型下，借助 RowSetSplits 可以将单个 Rowset 的读取任务拆分给多个线程并行扫描，提升吞吐量。
struct RowSetSplits {
    // 指向当前被切分的 RowsetReader 共享智能指针
    RowsetReaderSharedPtr rs_reader;

    // if segment_offsets is not empty, means we only scan
    // [pair.first, pair.second) segment in rs_reader, only effective in dup key
    // and pipeline
    // 指定当前分片仅需扫描该 Reader 负责的 Segment 索引区间 [pair.first, pair.second)。
    // 仅在 Duplicate Key 模型或 Pipeline 并行扫描场景下生效（默认 {0, 0} 表示扫描全部 Segment）。
    std::pair<int64_t, int64_t> segment_offsets;

    // RowRanges of each segment.
    // 存储该 Rowset 下每个 Segment 对应需要读取的物理行号集合（Row Ranges）。
    // 通过 ZoneMap、前缀索引或倒排索引裁剪后，只保留命中条件的行区间，避免全表扫描。
    std::vector<RowRanges> segment_row_ranges;

    RowSetSplits(RowsetReaderSharedPtr rs_reader_)
            : rs_reader(rs_reader_), segment_offsets({0, 0}) {}
    RowSetSplits() = default;
};


// RowsetReader 是 Doris 存储引擎中所有行集（Rowset）数据读取器统一实现的抽象基类接口（如 BetaRowsetReader 继承并实现了该类）。
// 统一数据读取抽象：面向上层（如 TabletReader 或查询引擎）屏蔽底层存储格式（如 Segment V2 / BetaRowset）的差异，暴露统一的按批次提取数据的接口。
// 生命周期与上下文管理：负责接收读取上下文 RowsetReaderContext，管理 Segment 层的迭代器创建、裁剪范围配置以及资源销毁。
// 向量化 Block 数据拉取：提供不同形态数据块（Block / BlockView / BlockWithSameBit）的批次拉取（next_batch）纯虚接口。
// 元数据与统计汇总：暴露 Rowset 的版本号、物理属性、删改标记，并负责收集和向上层 Profile 汇报过滤行数、合并行数、IO 统计等信息。
// 多线程并发支持：支持通过 clone() 方法创建等价的 Reader 副本，以支持多并发读取。
class RowsetReader {
public:
    virtual ~RowsetReader() = default;
    // 初始化读取器环境。传入读取上下文 read_context（包含谓词下推、扫描列 Schema、运行时状态等）和分片信息 rs_splits，完成迭代器的前期准备工作。
    virtual Status init(RowsetReaderContext* read_context, const RowSetSplits& rs_splits = {}) = 0;
    // 底层 Segment 迭代器构建接口。根据 read_context 生成该 Rowset 下各个 Segment 对应的 RowwiseIterator 并填入 out_iters 数组中，use_cache 用于控制是否使用 Segment 缓存。
    virtual Status get_segment_iterators(RowsetReaderContext* read_context,
                                         std::vector<RowwiseIteratorUPtr>* out_iters,
                                         bool use_cache = false) = 0;
    // 用于重置或重新配置内部的 StorageReadOptions（例如当上层推导出的谓词或扫描策略发生变动时刷新底层配置）。
    virtual void reset_read_options() = 0;
    // 向量化执行引擎的核心拉取接口。将下一批数据填充到标准列存块 Block 中，若数据读取完毕则返回 END_OF_FILE 状态。
    virtual Status next_batch(Block* block) = 0;
    // 将下一批数据填充到 BlockView 视图结构中，适用于特定视图级或轻量级过滤场景。
    virtual Status next_batch(BlockView* block_view) = 0;
    // 将下一批数据填充到带有位标记的数据块 BlockWithSameBit 中
    virtual Status next_batch(BlockWithSameBit* block_view) = 0;
    // 判断当前 Reader 内部的数据迭代器是否为归并排序迭代器（Merge Iterator）
    virtual bool is_merge_iterator() const { return false; }
    // 查询当前 Rowset 是否包含删除标记（即当前 Rowset 是否由 DELETE 导入操作生成）。
    virtual bool delete_flag() = 0;
    // 获取当前 Rowset 的版本信息（包含起始和终点 Version，如 [3-3]）。
    virtual Version version() = 0;
    // 获取当前 Reader 绑定的底层 Rowset 物理对象智能指针。
    virtual RowsetSharedPtr rowset() = 0;
    // 获取当前 Rowset 在读取过程中，被各类下推谓词（删除谓词、 ZoneMap、Bitmap、倒排索引等）过滤掉的行数总和。
    virtual int64_t filtered_rows() = 0;
    // 获取当前 Rowset 在数据读取合并（如聚合模型、Unique Key 模型合并历史版本）时被压缩合并掉的行数。
    virtual uint64_t merged_rows() = 0;
    // 返回当前 Rowset 的底层存储物理格式枚举类型（如 BETA_ROWSET）。
    virtual RowsetTypePB type() const = 0;
    // 返回当前 Rowset 中数据的最新物理写入时间戳。
    virtual int64_t newest_write_timestamp() = 0;
    // 获取最近一次 next_batch 读取出的这批数据在物理磁盘中的真实位置（Segment ID + Row ID），主要用于点查、主键索引更新等场景。派生类可按需重写。
    virtual Status current_block_row_locations(std::vector<RowLocation>* locations) {
        return Status::NotSupported("to be implemented");
    }
    // 将 Reader 内部收集到的各项读取开销（如 IO 耗时、解压时间、过滤行数等）刷新/汇总到查询引擎的 RuntimeProfile 对象中。
    virtual void update_profile(RuntimeProfile* profile) = 0;
    // 深拷贝/克隆当前 Reader 对象，返回一个新的共享实例，使多线程能够安全地并行读取同一个 Rowset。
    virtual RowsetReaderSharedPtr clone() = 0;
    // 下推 TopN 优化限制行数（topn_limit）到底层 Reader，让 Segment 扫描在满足 TopN 阈值时及时终止或裁减数据。
    virtual void set_topn_limit(size_t topn_limit) = 0;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_ROWSET_READER_H
