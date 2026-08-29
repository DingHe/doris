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

#include <gen_cpp/olap_common.pb.h>

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>

#include "common/metrics/metrics.h"
#include "common/status.h"
#include "io/io_common.h"
#include "storage/iterators.h"
#include "storage/olap_common.h"
#include "storage/partial_update_info.h"
#include "storage/segment/segment.h"
#include "storage/tablet/tablet_fwd.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/version_graph.h"
#include "util/bthread_shared_mutex.h"

namespace doris {
struct RowSetSplits;
struct RowsetWriterContext;
class RowsetWriter;
class CalcDeleteBitmapToken;
class SegmentCacheHandle;
class RowIdConversion;
struct PartialUpdateInfo;
class PartialUpdateReadPlan;
struct CaptureRowsetOps;
struct CaptureRowsetResult;
struct TabletReadSource;
class FixedReadPlan;

struct TabletWithVersion {
    BaseTabletSPtr tablet;
    int64_t version;
};

enum class CompactionStage { NOT_SCHEDULED, PENDING, EXECUTING };

// Base class for all tablet classes
// BaseTablet 封装了 Doris 存储分片的核心元数据管理、Rowset 存储映射、版本管理以及写时合并（Merge-on-Write, MoW）机制。
// 元数据与 Schema 管理：封装 TabletMeta 与 TabletSchema，统一提供 Tablet 的元数据访问接口，支持动态 Schema 变更（Schema Change）与最大版本 Schema 的维护。
// Rowset 生命周期管理：使用 _rs_version_map 管理生效中的数据版本，使用 _stale_rs_version_map 与版本追踪器管理因 Compaction 产生的旧版本，支持高并发下的多版本并发控制（MVCC）。
// MoW（Merge-on-Write）主键模型核心引擎：提供主键索引查找（lookup_row_key）、实时行数据读取（lookup_row_data）、写时/Compaction 时 Delete Bitmap（删除位图）的计算与更新。
// 并发控制与锁机制：提供 _meta_lock（读写锁）控制版本映射和元数据的并发修改，提供 _schema_change_lock 保证 Schema 变更与倒排索引构建等互斥。
// 监控与可观测性：维护分片的读写、Compaction、IO 吞吐等 Prometheus 指标统计。
class BaseTablet : public std::enable_shared_from_this<BaseTablet> {
public:
    // 传入 TabletMetaSharedPtr 初始化 Tablet 元数据。
    explicit BaseTablet(TabletMetaSharedPtr tablet_meta);
    virtual ~BaseTablet();
    BaseTablet(const BaseTablet&) = delete;
    BaseTablet& operator=(const BaseTablet&) = delete;
    // 获取 Tablet 的状态（如 RUNNING, NOT_READY, TOMBSTONE）。
    TabletState tablet_state() const { return _tablet_meta->tablet_state(); }
    // 更新 Tablet 状态。
    Status set_tablet_state(TabletState state);
    // 获取该 Tablet 所属的 Table ID。
    int64_t table_id() const { return _tablet_meta->table_id(); }
    // 获取当前 Schema 单行数据的预计字节大小。
    size_t row_size() const { return _tablet_meta->tablet_schema()->row_size(); }
    // 获取索引 ID（Index ID / Rollup ID）。
    int64_t index_id() const { return _tablet_meta->index_id(); }
    // 获取分区 ID（Partition ID）。
    int64_t partition_id() const { return _tablet_meta->partition_id(); }
    // 获取唯一 Tablet ID。
    int64_t tablet_id() const { return _tablet_meta->tablet_id(); }
    // 获取 Schema 的 Hash 值（用于区分历史不同 Schema）。
    int32_t schema_hash() const { return _tablet_meta->schema_hash(); }
    // 获取 Tablet 列数据的默认压缩类型（如 ZSTD, LZ4）。
    CompressKind compress_kind() const { return _tablet_meta->tablet_schema()->compress_kind(); }
    // 获取数据模型类型（DUP_KEYS, UNIQUE_KEYS, AGG_KEYS）。
    KeysType keys_type() const { return _tablet_meta->tablet_schema()->keys_type(); }
    // 获取主键列/排序键列的数量。
    size_t num_key_columns() const { return _tablet_meta->tablet_schema()->num_key_columns(); }
    // 获取数据的 TTL（生存时间，单位秒）。
    int64_t ttl_seconds() const { return _tablet_meta->ttl_seconds(); }
    // currently used by schema change, inverted index building, and cooldown
    // 获取 Schema Change 互斥锁引用。
    std::timed_mutex& get_schema_change_lock() { return _schema_change_lock; }
    // 返回当前 Unique Key 表是否开启了写时合并（Merge-on-Write, MoW）。
    bool enable_unique_key_merge_on_write() const {
#ifdef BE_TEST
        if (_tablet_meta == nullptr) {
            return false;
        }
#endif
        return _tablet_meta->enable_unique_key_merge_on_write();
    }
    // 判断读取当前 Tablet 时是否需要加载并读取 Delete Bitmap（MoW 或 Row Binlog 模式需要）。
    bool need_read_delete_bitmap() const {
        return _tablet_meta->enable_unique_key_merge_on_write() ||
               _tablet_meta->is_row_binlog_tablet();
    }
    // 判断是否开启了行级 Binlog 记录。
    bool is_row_binlog_tablet() const { return _tablet_meta->is_row_binlog_tablet(); }

    // Property encapsulated in TabletMeta
    // 获取底层的 TabletMeta 共享指针。
    const TabletMetaSharedPtr& tablet_meta() const { return _tablet_meta; }
    // 获取允许的最大版本配置限制。
    int32_t max_version_config();

    // FIXME(plat1ko): It is not appropriate to expose this lock
    // 获取控制 Tablet 元数据的读写锁（即 _meta_lock）。
    BthreadSharedMutex& get_header_lock() { return _meta_lock; }
    // 更新当前 Tablet 的最大版本 Schema。
    void update_max_version_schema(const TabletSchemaSPtr& tablet_schema);
    // 在加共享锁（读锁）保护下，获取当前最新的 TabletSchema 指针。
    TabletSchemaSPtr tablet_schema() const {
        std::shared_lock rlock(_meta_lock);
        return _max_version_schema;
    }
    // 设置 Alter（Schema Change）操作是否失败标记。
    void set_alter_failed(bool alter_failed) { _alter_failed = alter_failed; }
    // 查询 Alter 操作是否失败。
    bool is_alter_failed() { return _alter_failed; }
    // 获取当前 Tablet 在本地磁盘或共享存储上的数据目录绝对路径。
    virtual std::string tablet_path() const = 0;
    // 检查当前 Tablet 未合并的 Rowset 版本数量是否超过给定的上限阈值（用于反压和触发 Compaction）。
    virtual bool exceed_version_limit(int32_t limit) = 0;
    // 根据上下文创建用于写数据的 RowsetWriter 实例，vertical 标识是否启用垂直写入模式（按列攒批）。
    virtual Result<std::unique_ptr<RowsetWriter>> create_rowset_writer(RowsetWriterContext& context,
                                                                       bool vertical) = 0;
    // 根据给定的目标版本范围（spec_version），捕获并构建读取该版本范围数据所需的 Rowset Reader 分片。
    virtual Status capture_rs_readers(const Version& spec_version,
                                      std::vector<RowSetSplits>* rs_splits,
                                      const CaptureRowsetOps& opts) = 0;
    // 获取当前 Tablet 占用的物理存储总大小（字节）。
    virtual size_t tablet_footprint() = 0;

    // this method just return the compaction sum on each rowset
    // note(tsy): we should unify the compaction score calculation finally
    // 加锁计算并返回当前 Tablet 的真实 Compaction 得分（通常基于未合并 Rowset 的数量和 Segment 重叠度）。
    uint32_t get_real_compaction_score() const;
    // MUST hold shared `_meta_lock`. Use this variant when the caller already
    // holds the header lock to avoid recursively re-acquiring the (now
    // writer-preferring) `_meta_lock`, which would self-deadlock.
    // 不加锁计算 Compaction 得分（调用方必须已持有 _meta_lock 共享锁，防止死锁）。
    uint32_t get_real_compaction_score_unlocked() const;

    // MUST hold shared meta lock
    // 在无锁/外部已加锁保护下，根据版本路径（version_path）捕获 Rowset Reader。
    Status capture_rs_readers_unlocked(const Versions& version_path,
                                       std::vector<RowSetSplits>* rs_splits) const;

    // _rs_version_map and _stale_rs_version_map should be protected by _meta_lock
    // The caller must call hold _meta_lock when call this three function.
    // 根据指定 Version 在 _rs_version_map（或当 find_is_stale=true 时在 _stale_rs_version_map）中查找 Rowset。需持有锁。
    RowsetSharedPtr get_rowset_by_version(const Version& version, bool find_is_stale = false) const;
    // 专门从 _stale_rs_version_map 中查找过期的 Rowset。
    RowsetSharedPtr get_stale_rowset_by_version(const Version& version) const;
    // 获取当前包含最大版本号（max_version）的 Rowset。
    RowsetSharedPtr get_rowset_with_max_version() const;
    // 线程安全地获取版本不大于 max_version 的所有 Rowset ID 集合。
    Status get_all_rs_id(int64_t max_version, RowsetIdUnorderedSet* rowset_ids) const;
    // 无锁版本，获取版本不大于 max_version 的所有 Rowset ID 集合。
    Status get_all_rs_id_unlocked(int64_t max_version, RowsetIdUnorderedSet* rowset_ids) const;

    // Get the missed versions until the spec_version.
    // 查询从 0 到 spec_version 之间缺少的版本区间列表。
    Versions get_missed_versions(int64_t spec_version) const;
    // 无锁版本的 get_missed_versions。
    Versions get_missed_versions_unlocked(int64_t spec_version) const;
    // 深拷贝当前 Tablet 的元数据到 new_tablet_meta 对象中（带锁）。
    void generate_tablet_meta_copy(TabletMeta& new_tablet_meta, bool cloud_get_rowset_meta) const;
    // 无锁版本的元数据拷贝。
    void generate_tablet_meta_copy_unlocked(TabletMeta& new_tablet_meta,
                                            bool cloud_get_rowset_meta) const;
    // 获取当前最大的可见版本号（不加锁）。
    virtual int64_t max_version_unlocked() const { return _tablet_meta->max_version().second; }
    // 合并多个 RowsetMeta 中的 Schema，推导出包含最大 schema_version 的最新 TabletSchema。
    static TabletSchemaSPtr tablet_schema_with_merged_max_schema_version(
            const std::vector<RowsetMetaSharedPtr>& rowset_metas);

    ////////////////////////////////////////////////////////////////////////////
    // begin MoW functions
    ////////////////////////////////////////////////////////////////////////////
    // 根据输入的 RowsetId 集合批量获取 Rowset 实例指针。
    std::vector<RowsetSharedPtr> get_rowset_by_ids(
            const RowsetIdUnorderedSet* specified_rowset_ids);

    // Lookup a row with TupleDescriptor and fill Block
    // 在点查（Point Query）或部分更新中，根据 row_location（Segment ID + Row ID）在特定的 Rowset 中读取整行数据并填充到 values 中。
    Status lookup_row_data(const Slice& encoded_key, const RowLocation& row_location,
                           RowsetSharedPtr rowset, OlapReaderStatistics& stats, std::string& values,
                           bool write_to_cache = false, const io::IOContext* io_ctx = nullptr);
    // Lookup the row location of `encoded_key`, the function sets `row_location` on success.
    // NOTE: the method only works in unique key model with primary key index, you will got a
    //       not supported error in other data model.
    // 主键模型关键方法：在 specified_rowsets 中通过主键索引查找编码后的 encoded_key 对应的物理行位置 row_location。
    Status lookup_row_key(const Slice& encoded_key, TabletSchema* latest_schema, bool with_seq_col,
                          const std::vector<RowsetSharedPtr>& specified_rowsets,
                          RowLocation* row_location, int64_t version,
                          std::vector<std::unique_ptr<SegmentCacheHandle>>& segment_caches,
                          RowsetSharedPtr* rowset = nullptr, bool with_rowid = true,
                          std::string* encoded_seq_value = nullptr,
                          OlapReaderStatistics* stats = nullptr,
                          DeleteBitmapPtr tablet_delete_bitmap = nullptr,
                          const io::IOContext* io_ctx = nullptr);

    // calc delete bitmap when flush memtable, use a fake version to calc
    // For example, cur max version is 5, and we use version 6 to calc but
    // finally this rowset publish version with 8, we should make up data
    // for rowset 6-7. Also, if a compaction happens between commit_txn and
    // publish_txn, we should remove compaction input rowsets' delete_bitmap
    // and build newly generated rowset's delete_bitmap
    // 在导入/Memtable Flush 阶段，为新建的 Rowset/Segments 计算 Delete Bitmap（将历史被覆盖的行在 Delete Bitmap 中标记为已删除）。
    static Status calc_delete_bitmap(const BaseTabletSPtr& tablet, RowsetSharedPtr rowset,
                                     const std::vector<segment_v2::SegmentSharedPtr>& segments,
                                     const std::vector<RowsetSharedPtr>& specified_rowsets,
                                     DeleteBitmapPtr delete_bitmap, int64_t version,
                                     CalcDeleteBitmapToken* token,
                                     RowsetWriter* rowset_writer = nullptr,
                                     DeleteBitmapPtr tablet_delete_bitmap = nullptr);
    // 针对单个 Segment 计算对应的 Delete Bitmap。
    Status calc_segment_delete_bitmap(RowsetSharedPtr rowset,
                                      const segment_v2::SegmentSharedPtr& seg,
                                      const std::vector<RowsetSharedPtr>& specified_rowsets,
                                      DeleteBitmapPtr delete_bitmap, int64_t end_version,
                                      RowsetWriter* rowset_writer,
                                      DeleteBitmapPtr tablet_delete_bitmap = nullptr,
                                      int64_t queue_time_us = 0);
    // 计算同一个 Rowset 内部不同 Segment 之间重复主键产生的 Delete Bitmap（处理同一个 Batch/Memtable 内后写入覆盖先写入的情况）。
    Status calc_delete_bitmap_between_segments(
            TabletSchemaSPtr schema, RowsetId rowset_id,
            const std::vector<segment_v2::SegmentSharedPtr>& segments,
            DeleteBitmapPtr delete_bitmap, int64_t queue_time_us = 0);
    // 在事务 Commit 阶段，补偿计算从 Flush 到 Commit 期间新增版本的 Delete Bitmap 并更新。
    static Status commit_phase_update_delete_bitmap(
            const BaseTabletSPtr& tablet, const RowsetSharedPtr& rowset,
            RowsetIdUnorderedSet& pre_rowset_ids, DeleteBitmapPtr delete_bitmap,
            const std::vector<segment_v2::SegmentSharedPtr>& segments, int64_t txn_id,
            CalcDeleteBitmapToken* token, RowsetWriter* rowset_writer = nullptr);
    // 向 Delete Bitmap 中插入哨兵标记（Sentinel Mark），用于标记某些 Rowset 是否被锁定或正在进行版本演算。
    static void add_sentinel_mark_to_delete_bitmap(DeleteBitmap* delete_bitmap,
                                                   const RowsetIdUnorderedSet& rowsetids);
    // 校验 Delete Bitmap 的正确性与一致性（防重存、防漏标记）。
    Status check_delete_bitmap_correctness(DeleteBitmapPtr delete_bitmap, int64_t max_version,
                                           int64_t txn_id, const RowsetIdUnorderedSet& rowset_ids,
                                           std::vector<RowsetSharedPtr>* rowsets = nullptr);
    // 从 Block 数据块中提取 __DORIS_DELETE_SIGN__（删除标记列）的底层数组数据。
    static const signed char* get_delete_sign_column_data(const Block& block,
                                                          size_t rows_at_least = 0);
    // 当部分更新缺少某些列时，根据 Schema 的默认值生成填充对应的 Block 数据。
    static Status generate_default_value_block(const TabletSchema& schema,
                                               const std::vector<uint32_t>& cids,
                                               const std::vector<std::string>& default_values,
                                               const Block& ref_block, Block& default_value_block);
    // 针对固定模式的部分更新（Partial Update），根据 Read Plan 从旧 Rowset 中读取未修改列与新列组合生成新的 Block。
    static Status generate_new_block_for_partial_update(
            TabletSchemaSPtr rowset_schema, const PartialUpdateInfo* partial_update_info,
            const FixedReadPlan& read_plan_ori, const FixedReadPlan& read_plan_update,
            const std::map<RowsetId, RowsetSharedPtr>& rsid_to_rowset, Block* output_block);
    // 针对灵活模式（Flexible Mode）的部分更新生成融合后的 Block。
    static Status generate_new_block_for_flexible_partial_update(
            TabletSchemaSPtr rowset_schema, const PartialUpdateInfo* partial_update_info,
            std::set<uint32_t>& rids_be_overwritten, const FixedReadPlan& read_plan_ori,
            const FixedReadPlan& read_plan_update,
            const std::map<RowsetId, RowsetSharedPtr>& rsid_to_rowset, Block* output_block);

    // We use the TabletSchema from the caller because the TabletSchema in the rowset'meta
    // may be outdated due to schema change. Also note that the the cids should indicate the indexes
    // of the columns in the TabletSchema passed in.
    // 如果开启了变长行存（Row Store / DORIS_ROW_STORE），直接从行存列中反序列化提取指定 cids 列的数据并填充到 Block。
    static Status fetch_value_through_row_column(RowsetSharedPtr input_rowset,
                                                 const TabletSchema& tablet_schema, uint32_t segid,
                                                 const std::vector<uint32_t>& rowids,
                                                 const std::vector<uint32_t>& cids, Block& block);
    // 根据指定的 rowids 列表，从 Rowset 的特定列中抓取数据并追加到目标列 dst 中。
    static Status fetch_value_by_rowids(RowsetSharedPtr input_rowset, uint32_t segid,
                                        const std::vector<uint32_t>& rowids,
                                        const TabletColumn& tablet_column, MutableColumnPtr& dst);
    // 为 MoW 部分更新（Partial Update）创建临时的 Rowset Writer。
    virtual Result<std::unique_ptr<RowsetWriter>> create_transient_rowset_writer(
            const Rowset& rowset, std::shared_ptr<PartialUpdateInfo> partial_update_info,
            int64_t txn_expiration = 0) = 0;
    // 在 Publish Version 阶段更新最终生效的 Delete Bitmap。
    static Status update_delete_bitmap(const BaseTabletSPtr& self, TabletTxnInfo* txn_info,
                                       int64_t txn_id, int64_t txn_expiration = 0,
                                       DeleteBitmapPtr tablet_delete_bitmap = nullptr);
    // 将计算好的 Delete Bitmap 持久化或同步至存储。
    virtual Status save_delete_bitmap(const TabletTxnInfo* txn_info, int64_t txn_id,
                                      DeleteBitmapPtr delete_bitmap, RowsetWriter* rowset_writer,
                                      const RowsetIdUnorderedSet& cur_rowset_ids,
                                      int64_t lock_id = -1, int64_t next_visible_version = -1) = 0;
    // 获取用于并行/异步计算 Delete Bitmap 的执行器线程池指针。
    virtual CalcDeleteBitmapExecutor* calc_delete_bitmap_executor() = 0;
    // 在 MoW Compaction 过程中，重新计算和重定向输出 Rowset 的 Delete Bitmap（将旧 Rowset 的删除位图映射迁移至新 Rowset 上）。
    void calc_compaction_output_rowset_delete_bitmap(
            const std::vector<RowsetSharedPtr>& input_rowsets,
            const RowIdConversion& rowid_conversion, uint64_t start_version, uint64_t end_version,
            std::set<RowLocation>* missed_rows,
            std::map<RowsetSharedPtr, std::list<std::pair<RowLocation, RowLocation>>>* location_map,
            const DeleteBitmap& input_delete_bitmap, DeleteBitmap* output_rowset_delete_bitmap);
    // 校验 Compaction 前后 RowID 转换映射（RowIdConversion）的准确性。
    Status check_rowid_conversion(
            RowsetSharedPtr dst_rowset,
            const std::map<RowsetSharedPtr, std::list<std::pair<RowLocation, RowLocation>>>&
                    location_map);
    // 无锁版本的 Delete Bitmap 更新方法。
    static Status update_delete_bitmap_without_lock(
            const BaseTabletSPtr& self, const RowsetSharedPtr& rowset,
            const std::vector<RowsetSharedPtr>* specified_base_rowsets = nullptr);

    using DeleteBitmapKeyRanges =
            std::vector<std::tuple<DeleteBitmap::BitmapKey, DeleteBitmap::BitmapKey>>;
    // 聚合并清理已过期的 Stale Rowsets 上的 Delete Bitmap 范围键。
    void agg_delete_bitmap_for_stale_rowsets(
            Version version, DeleteBitmapKeyRanges& remove_delete_bitmap_key_ranges);
    // 检查可被聚合清理的无用 Stale Rowsets 及其版本的数量。
    void check_agg_delete_bitmap_for_stale_rowsets(int64_t& useless_rowset_count,
                                                   int64_t& useless_rowset_version_count);
    ////////////////////////////////////////////////////////////////////////////
    // end MoW functions
    ////////////////////////////////////////////////////////////////////////////
    // 根据 RowsetId 查找对应的 Rowset。
    RowsetSharedPtr get_rowset(const RowsetId& rowset_id);
    // 获取当前所有有效 Rowset 的快照列表（可选包含 stale rowsets），常用于一致性检查或 Checkpoint。
    std::vector<RowsetSharedPtr> get_snapshot_rowset(bool include_stale_rowset = false) const;
    // 清理该 Tablet 绑定的各种缓存（如 Segment Cache, Index Cache）。
    virtual void clear_cache() = 0;

    // Find the first consecutive empty rowsets. output->size() >= limit
    // 在给定的候选 Rowset 中，查找连续的空 Rowset（行数为 0），用于后续优化清理。
    void calc_consecutive_empty_rowsets(std::vector<RowsetSharedPtr>* empty_rowsets,
                                        const std::vector<RowsetSharedPtr>& candidate_rowsets,
                                        int64_t limit);
    // 带锁遍历当前 Tablet 的所有 Rowset，对每个 Rowset 执行 visitor 回调函数。
    void traverse_rowsets(std::function<void(const RowsetSharedPtr&)> visitor,
                          bool include_stale = false) {
        std::shared_lock rlock(_meta_lock);
        traverse_rowsets_unlocked(visitor, include_stale);
    }
    // 无锁版本的 Rowset 遍历。
    void traverse_rowsets_unlocked(std::function<void(const RowsetSharedPtr&)> visitor,
                                   bool include_stale = false) const {
        for (auto& [v, rs] : _rs_version_map) {
            visitor(rs);
        }
        if (!include_stale) return;
        for (auto& [v, rs] : _stale_rs_version_map) {
            visitor(rs);
        }
    }
    // 计算从 start_version 到 end_version 范围内所有 Rowset 数据文件的 CRC32 校验码，用于副本一致性检查（Check Consistency）。
    Status calc_file_crc(uint32_t* crc_value, int64_t start_version, int64_t end_version,
                         uint32_t* rowset_count, int64_t* file_count);
    // 获取并以 JSON 格式输出嵌套索引（如倒排索引等）的元信息。
    Status show_nested_index_file(std::string* json_meta);
    // 获取 Tablet 的 128 位唯一 UID。
    TabletUid tablet_uid() const { return _tablet_meta->tablet_uid(); }
    // 构造并返回包含 tablet_id 和 tablet_uid 的 TabletInfo 结构体。
    TabletInfo get_tablet_info() const { return TabletInfo(tablet_id(), tablet_uid()); }

    void get_base_rowset_delete_bitmap_count(
            uint64_t* max_base_rowset_delete_bitmap_score,
            int64_t* max_base_rowset_delete_bitmap_score_tablet_id);
    // 检查特定事务的 Delete Bitmap 缓存状态（默认返回 OK，子类重写）。
    virtual Status check_delete_bitmap_cache(int64_t txn_id, DeleteBitmap* expected_delete_bitmap) {
        return Status::OK();
    }
    // 预热并填充 Delete Bitmap 聚合缓存（DbmAggCache）。
    void prefill_dbm_agg_cache(const RowsetSharedPtr& rowset, int64_t version);
    // 在 Compaction 完成后，预热输出 Rowset 的 Delete Bitmap 聚合缓存。
    void prefill_dbm_agg_cache_after_compaction(const RowsetSharedPtr& output_rowset);
    // 在无锁保护下，捕获满足指定版本范围（version_range）且版本连续一致的 Rowset 集合。
    [[nodiscard]] Result<CaptureRowsetResult> capture_consistent_rowsets_unlocked(
            const Version& version_range, const CaptureRowsetOps& options) const;
    // 无锁捕获指定版本范围内的连续版本列表。
    [[nodiscard]] virtual Result<std::vector<Version>> capture_consistent_versions_unlocked(
            const Version& version_range, const CaptureRowsetOps& options) const;

    [[nodiscard]] Result<std::vector<RowSetSplits>> capture_rs_readers_unlocked(
            const Version& version_range, const CaptureRowsetOps& options) const;
    // 构建并返回完整的读取源对象（TabletReadSource），打包了查询所需的 Rowsets 和对应 Delete Bitmap。
    [[nodiscard]] Result<TabletReadSource> capture_read_source(const Version& version_range,
                                                               const CaptureRowsetOps& options);

protected:
    // Find the missed versions until the spec_version.
    //
    // for example:
    //     [0-4][5-5][8-8][9-9][14-14]
    // for cloud, if spec_version = 12, it will return [6-7],[10-12]
    // for local, if spec_version = 12, it will return [6, 6], [7, 7], [10, 10], [11, 11], [12, 12]
    // 保护方法：比较已有版本与目标版本 spec_version，计算并返回缺失的版本区间（用于数据恢复或 Version 获取）。
    virtual Versions calc_missed_versions(int64_t spec_version,
                                          Versions existing_versions) const = 0;

    void _print_missed_versions(const Versions& missed_versions) const;
    bool _reconstruct_version_tracker_if_necessary();

    static void _rowset_ids_difference(const RowsetIdUnorderedSet& cur,
                                       const RowsetIdUnorderedSet& pre,
                                       RowsetIdUnorderedSet* to_add, RowsetIdUnorderedSet* to_del);

    // We can only know if a key is excluded from the segment
    // based on strictly order compare result with segments key bounds
    static bool _key_is_not_in_segment(Slice key, const KeyBoundsPB& segment_key_bounds,
                                       bool is_segments_key_bounds_truncated);

    Status sort_block(Block& in_block, Block& output_block,
                      std::vector<uint32_t>* permutation = nullptr);

    Result<CaptureRowsetResult> _remote_capture_rowsets(const Version& version_range) const;
    // 控制 Tablet 元数据（如 _rs_version_map、_stale_rs_version_map、_max_version_schema）并发访问的读写锁。使用 BthreadSharedMutex 适配协程/bthread 环境。
    mutable BthreadSharedMutex _meta_lock;
    // 过期版本追踪器。维护过期的 Rowset（因 Compaction 产生），根据路径版本和超时策略判定旧 Rowset 何时可安全物理回收。
    TimestampedVersionTracker _timestamped_version_tracker;

    // After version 0.13, all newly created rowsets are saved in _rs_version_map.
    // And if rowset being compacted, the old rowsets will be saved in _stale_rs_version_map;
    // 当前 Tablet 内部生效中的 Version 到 Rowset 的映射表（MVCC 版本链）。
    std::unordered_map<Version, RowsetSharedPtr, HashOfVersion> _rs_version_map;
    // This variable _stale_rs_version_map is used to record these rowsets which are be compacted.
    // These _stale rowsets are been removed when rowsets' pathVersion is expired,
    // this policy is judged and computed by TimestampedVersionTracker.
    // 当前 Tablet 中已完成 Compaction 但尚未物理清理的过期 Version 到 Rowset 映射表。
    std::unordered_map<Version, RowsetSharedPtr, HashOfVersion> _stale_rs_version_map;
    // Tablet 元数据指针（不可变共享指针），保存 tablet_id、table_id、schema_hash 等元数据。
    const TabletMetaSharedPtr _tablet_meta;
    // 当前 Tablet 拥有的最新版本的 Schema 指针（例如执行过 Schema Change 后的最新列定义）。
    TabletSchemaSPtr _max_version_schema;

    // `_alter_failed` is used to indicate whether the tablet failed to perform a schema change
    // 标识当前 Tablet 在执行 Schema Change/Alter 操作时是否发生失败。
    std::atomic<bool> _alter_failed = false;

    // metrics of this tablet
    // Tablet 级别的度量指标实体指针，用于注册与监控。
    std::shared_ptr<MetricEntity> _metric_entity;

protected:
    // 时间锁，用于 Schema Change、倒排索引构建（Inverted Index Building）、冷热数据冷却（Cooldown）等重度后台任务的互斥。
    std::timed_mutex _schema_change_lock;

public:
    // 扫描数据字节数计数器。
    IntCounter* query_scan_bytes = nullptr;
    // 扫描行数计数器。
    IntCounter* query_scan_rows = nullptr;
    // 查询扫描次数计数器。
    IntCounter* query_scan_count = nullptr;
    // Memtable 落盘（Flush）数据量计数器。
    IntCounter* flush_bytes = nullptr;
    // Flush 完成次数计数器。
    IntCounter* flush_finish_count = nullptr;
    // 成功 Publish（版本生效）的事务计数。
    std::atomic<int64_t> published_count = 0;
    // 读取 Block 的数量计数。
    std::atomic<int64_t> read_block_count = 0;
    // 写操作次数计数。
    std::atomic<int64_t> write_count = 0;
    // 执行 Compaction 的次数计数。
    std::atomic<int64_t> compaction_count = 0;
    // 当前 Tablet 的 Compaction 调度状态（如 NOT_SCHEDULED）。
    CompactionStage compaction_stage = CompactionStage::NOT_SCHEDULED;
    // Separate sample_infos for each compaction type to avoid race condition
    // when different types of compaction run concurrently on the same tablet
    // 保护 Cumulative Compaction 采样信息的互斥锁。
    std::mutex cumu_sample_info_lock;
    // 保护 Base Compaction 采样信息的互斥锁。
    std::mutex base_sample_info_lock;
    // 保护 Full Compaction 采样信息的互斥锁。
    std::mutex full_sample_info_lock;
    // 累积 Compaction 的采样统计信息列表。
    std::vector<CompactionSampleInfo> cumu_sample_infos;
    // Base Compaction 的采样统计信息列表。
    std::vector<CompactionSampleInfo> base_sample_infos;
    // 全量 Compaction 的采样统计信息列表。
    std::vector<CompactionSampleInfo> full_sample_infos;
    // 记录上一次 Compaction 的执行状态（OK 或错误信息）。
    Status last_compaction_status = Status::OK();
    // 根据 Reader 类型（如 CUMULATIVE/BASE/FULL Compaction）返回对应的采样锁。
    std::mutex& get_sample_info_lock(ReaderType reader_type) {
        switch (reader_type) {
        case ReaderType::READER_CUMULATIVE_COMPACTION:
            return cumu_sample_info_lock;
        case ReaderType::READER_BASE_COMPACTION:
            return base_sample_info_lock;
        case ReaderType::READER_FULL_COMPACTION:
            return full_sample_info_lock;
        default:
            // For other compaction types, use base_sample_info_lock as default
            return base_sample_info_lock;
        }
    }
    // 根据 Reader 类型获取对应的 Compaction 采样信息容器。
    std::vector<CompactionSampleInfo>& get_sample_infos(ReaderType reader_type) {
        switch (reader_type) {
        case ReaderType::READER_CUMULATIVE_COMPACTION:
            return cumu_sample_infos;
        case ReaderType::READER_BASE_COMPACTION:
            return base_sample_infos;
        case ReaderType::READER_FULL_COMPACTION:
            return full_sample_infos;
        default:
            // For other compaction types, use base_sample_infos as default
            return base_sample_infos;
        }
    }

    // Density ratio for sparse optimization (non_null_cells / total_cells)
    // Value range: [0.0, 1.0], smaller value means more sparse
    // Default 1.0 means no history data, will not enable sparse optimization initially
    // 数据的密度比率（非空单元格/总单元格），用于稀疏数据优化（范围 [0.0, 1.0]）。
    std::atomic<double> compaction_density {1.0};
};

struct CaptureRowsetOps {
    bool skip_missing_versions = false;
    bool quiet = false;
    bool include_stale_rowsets = true;
    bool enable_fetch_rowsets_from_peers = false;

    // ======== only take effect in cloud mode ========

    // Enable preference for cached/warmed-up rowsets when building version paths.
    // When enabled, the capture process will prioritize already cached rowsets
    // to avoid cold data reads and improve query performance.
    bool enable_prefer_cached_rowset {false};

    // Query freshness tolerance in milliseconds.
    // Defines the time window for considering data as "fresh enough".
    // Rowsets that became visible within this time range can be skipped if not warmed up,
    // but older rowsets (before current_time - query_freshness_tolerance_ms) that are
    // not warmed up will trigger fallback to normal capture.
    // Set to -1 to disable freshness tolerance checking.
    int64_t query_freshness_tolerance_ms {-1};
};

struct CaptureRowsetResult {
    std::vector<RowsetSharedPtr> rowsets;
    std::shared_ptr<DeleteBitmap> delete_bitmap;
};

struct TabletReadSource {
    std::vector<RowSetSplits> rs_splits;
    std::vector<RowsetMetaSharedPtr> delete_predicates;
    std::shared_ptr<DeleteBitmap> delete_bitmap;
    // Fill delete predicates with `rs_splits`
    void fill_delete_predicates();
};

} /* namespace doris */
