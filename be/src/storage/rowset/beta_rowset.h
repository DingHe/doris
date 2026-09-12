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

#ifndef DORIS_SRC_OLAP_ROWSET_BETA_ROWSET_H_
#define DORIS_SRC_OLAP_ROWSET_BETA_ROWSET_H_

#include <stddef.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/segment/segment.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

class BetaRowset;

namespace io {
struct IOContext;
class RemoteFileSystem;
} // namespace io
struct RowsetId;

using BetaRowsetSharedPtr = std::shared_ptr<BetaRowset>;

// BetaRowset 是目前标准的物理存储行集（Rowset）实现类，基于 Segment V2（Beta 列存格式） 构建。
// Segment V2 数据物理管理：负责管理属于该 Rowset 的所有列存数据文件（.dat）、倒排索引文件（.idx）以及 Segcompaction 临时生成的段文件。
// Segment 对象的按需加载与缓存：作为 Segment 的工厂和加载器，提供 load_segment / load_segments 接口，结合 SegmentCache 实现内存复用与句柄管理。
// 读取与协同：实现纯虚函数 create_reader()，用于创建专门的 BetaRowsetReader 供查询引擎或 Compaction 提取数据。
// 数据生命周期与运维操作：支持物理文件的删除（remove）、硬链接（link_files_to）、跨路径/远端文件复制（copy_files_to / upload_to）、文件完整性校验（CRC）以及数据写入 Binlog 等底层 IO 操作。

class BetaRowset final : public Rowset {
public:
    ~BetaRowset() override;
	// 实例化一个专门针对 Beta 格式的 BetaRowsetReader（将自身 shared_from_this() 传入），并将指针赋给 result，用于后续的数据迭代读取。
    Status create_reader(RowsetReaderSharedPtr* result) override;

    // Return the absolute path of local segcompacted segment file
	// 拼接并返回在 Segcompaction（段合并）过程中，合并特定范围 [begin, end) 产生的小 Segment 文件的本地绝对路径。
    static std::string local_segment_path_segcompacted(const std::string& tablet_path,
                                                       const RowsetId& rowset_id, int64_t begin,
                                                       int64_t end);
	// 物理删除该 Rowset 对应的所有存储文件（包括数据文件 .dat 和倒排索引文件 .idx），并同步清理对应的索引缓存。
    Status remove() override;
	// 通过操作系统的硬链接（Hard Link），将该 Rowset 的物理文件链接到指定目录 dir 下并重命名为 new_rowset_id 对应的格式。常用于 Snapshot（快照）、Clone（副本修复）或 Compaction 后的快速文件转移，避免物理拷贝。
    Status link_files_to(const std::string& dir, RowsetId new_rowset_id,
                         size_t new_rowset_start_seg_id = 0,
                         std::set<int64_t>* without_index_uids = nullptr) override;
	// 通过物理深拷贝方式，将当前 Rowset 的所有文件复制到目标路径 dir 下。
    Status copy_files_to(const std::string& dir, const RowsetId& new_rowset_id) override;
	// 将当前 Rowset 的所有数据及索引文件上传至指定的远端存储资源 dest_fs（如对象存储 S3 / HDFS，用于存算分离或冷热数据归档）。
    Status upload_to(const StorageResource& dest_fs, const RowsetId& new_rowset_id) override;

    // only applicable to alpha rowset, no op here
	// 由于该方法仅用于早期的 AlphaRowset 格式，在 BetaRowset 中为 No-op（无操作），直接返回 Status::OK()。
    Status remove_old_files(std::vector<std::string>* files_to_remove) override {
        return Status::OK();
    }
	// 检查该 Rowset 包含的所有物理 Segment 文件和索引文件在文件系统中是否存在。如果文件丢失则返回错误。
    Status check_file_exist() override;
	// 加载该 Rowset 下的所有 Segment 对象，并将结果填充至 segments 数组中。
    Status load_segments(std::vector<segment_v2::SegmentSharedPtr>* segments);
	// 按 Segment ID 范围批量加载区间为 [seg_id_begin, seg_id_end) 的指定 Segment 对象。
    Status load_segments(int64_t seg_id_begin, int64_t seg_id_end,
                         std::vector<segment_v2::SegmentSharedPtr>* segments);
	// 核心底层的单 Segment 加载函数。根据 seg_id（从 0 开始）加载单个 Segment 对象。
	// 入参 read_stats 用于收集 IO 统计信息，io_ctx 用于透传 IO 上下文（如 Profiling、优先级控制）。优先尝试从 SegmentCache 缓存获取。
    Status load_segment(int64_t seg_id, OlapReaderStatistics* read_stats,
                        segment_v2::SegmentSharedPtr* segment,
                        const io::IOContext* io_ctx = nullptr);
	// 获取该 Rowset 下各个 Segment 文件各自的物理大小（字节数），依次填入 segments_size 数组。
    Status get_segments_size(std::vector<size_t>* segments_size);
	// 计算并获取该 Rowset 中所有 Segment 对应的倒排索引文件（.idx）占用的磁盘空间总大小。
    Status get_inverted_index_size(int64_t* index_size) override;
	// 将当前 Rowset 的元数据与变更数据写至 Binlog 模块，用于 CDC（变更数据捕获）或主从副本增量同步。[[nodiscard]] 强制要求调用方必须检查其返回的 Status。
    [[nodiscard]] virtual Status add_to_binlog() override;
	// 计算当前 Rowset 所有物理文件的 CRC32 校验码总和及文件数量，用于数据一致性比对与文件损坏校验。
    Status calc_file_crc(uint32_t* crc_value, int64_t* file_count);
	// 将该 Rowset 下嵌套倒排索引文件（Nested Index File）的信息解析并格式化为 RapidJSON 对象，主要用于 BE HTTP Debug 页面或调试接口呈现。
    Status show_nested_index_file(rapidjson::Value* rowset_value,
                                  rapidjson::Document::AllocatorType& allocator);
	// 获取每个 Segment 包含的数据行数列表（利用 _load_segment_rows_once 实现延迟懒加载与单次解析），可选择是否开启缓存。
    Status get_segment_num_rows(std::vector<uint32_t>* segment_rows, bool enable_segment_cache,
                                OlapReaderStatistics* read_stats,
                                const io::IOContext* io_ctx = nullptr);

protected:
    BetaRowset(const TabletSchemaSPtr& schema, const RowsetMetaSharedPtr& rowset_meta,
               std::string tablet_path);

    // init segment groups
	// 初始化接口（由 RowsetFactory 在构造后立即调用）。检查并解析 Segment 布局，校验 Rowset 的物理合法性。
    Status init() override;
	// 当 Rowset 引用计数归 0 且触发关闭时，释放内部引用的 Segment 句柄并清除关联的缓存。
    void do_close() override;
	// 对当前 Rowset 的各个 Segment 文件做物理完整性校验（如检查 Header/Footer 结构是否正常）。
    Status check_current_rowset_segment() override;
	// 主动清空与当前 Rowset 关联的倒排索引（Inverted Index）内存缓存，释放内存。
    void clear_inverted_index_cache() override;

private:
    friend class RowsetFactory;
    friend class BetaRowsetReader;
	// 基于 Doris 封装的 CallOnce 工具类（多线程安全的单次执行保证，类似于 std::call_once）。
	// 确保每个 Segment 的行数列表（_segments_rows）在多线程并发访问时只被加载/计算一次，避免重复读取 Segment Footer 的开销。
    DorisCallOnce<Status> _load_segment_rows_once;
	// 存储该 Rowset 下各个 Segment 文件分别包含的数据行数（例如：[1000, 1000, 500] 表示第 0、1、2 个 Segment 的行数）。
    std::vector<uint32_t> _segments_rows;
};

} // namespace doris

#endif //DORIS_SRC_OLAP_ROWSET_BETA_ROWSET_H_
