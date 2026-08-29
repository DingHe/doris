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

#include <butil/macros.h>
#include <bvar/bvar.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/olap_file.pb.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <ctime>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "agent/task_worker_pool.h"
#include "common/config.h"
#include "common/status.h"
#include "runtime/heartbeat_flags.h"
#include "storage/adaptive_thread_pool_controller.h"
#include "storage/compaction/compaction_permit_limiter.h"
#include "storage/delete/calc_delete_bitmap_executor.h"
#include "storage/olap_common.h"
#include "storage/options.h"
#include "storage/rowset/pending_rowset_helper.h"
#include "storage/rowset/rowset_fwd.h"
#include "storage/segment/segment.h"
#include "storage/tablet/tablet_fwd.h"
#include "storage/task/index_builder.h"
#include "util/countdown_latch.h"

namespace doris {

class DataDir;
class EngineTask;
class MemTableFlushExecutor;
class SegcompactionWorker;
class BaseCompaction;
class CumulativeCompaction;
class CumulativeCompactionPolicy;
class StreamLoadRecorder;
class TCloneReq;
class TCreateTabletReq;
class TabletManager;
class Thread;
class ThreadPool;
class TxnManager;
class ReportWorker;
class CreateTabletRRIdxCache;
struct DirInfo;
class SnapshotManager;
class WorkloadGroup;

using SegCompactionCandidates = std::vector<segment_v2::SegmentSharedPtr>;
using SegCompactionCandidatesSharedPtr = std::shared_ptr<SegCompactionCandidates>;
using CumuCompactionPolicyTable =
        std::unordered_map<std::string_view, std::shared_ptr<CumulativeCompactionPolicy>>;

class StorageEngine;
class CloudStorageEngine;

extern bvar::Status<int64_t> g_max_rowsets_with_useless_delete_bitmap;
extern bvar::Status<int64_t> g_max_rowsets_with_useless_delete_bitmap_version;

// StorageEngine singleton to manage all Table pointers.
// Providing add/drop/get operations.
// StorageEngine instance doesn't own the Table resources, just hold the pointer,
// allocation/deallocation must be done outside.

// storage_engine.h 是 Apache Doris 存储层最核心的头文件之一，定义了存储引擎的基类 BaseStorageEngine、主存储引擎 StorageEngine、Compaction 任务注册表 CompactionSubmitRegistry
// 以及相关的辅助类。
// 在 Apache Doris BE 节点中，存储引擎类（主要是 StorageEngine）充当了存储子系统的单例核心管理者。它的主要作用包括：
// 存储资源与生命周期管理：管理节点上所有的物理数据目录（DataDir）、数据分片（Tablet）及事务（TxnManager），控制存储引擎从启动、运行到停止的全生命周期。
// 后台任务调度与线程池管理：维护和运行一系列后台线程与线程池，用于执行数据 Compaction（合并）、垃圾回收（Garbage Collection）、磁盘状态监控、冷热数据冷却（Cooldown）、异步 Publish 事务等。
// 架构解耦与抽象（存算分离/存算一体）：继承自 BaseStorageEngine，将传统存算一体架构（LOCAL）与存算分离架构（CLOUD）的公共接口抽象统一，便于上层逻辑调用。
// 读写辅助与内存控制：管理写路径上的 MemTable Flush 线程池、Delete Bitmap 计算线程池，以及读路径上的 Querying Rowset 引用，避免正在查询的数据被 GC 清理。

// BaseStorageEngine 是本地存储引擎（StorageEngine）和云原生存储引擎（CloudStorageEngine）的抽象基类。
class BaseStorageEngine {
protected:
    enum Type : uint8_t {
        LOCAL, // Shared-nothing integrated compute and storage architecture
        CLOUD, // Separating compute and storage architecture
    };
    // 标识当前存储引擎类型。LOCAL 或 CLOUD
    Type _type;

public:
    BaseStorageEngine(Type type, const UniqueId& backend_uid);
    virtual ~BaseStorageEngine();
    // 强转为 StorageEngine 或 CloudStorageEngine 的引用。
    StorageEngine& to_local();
    CloudStorageEngine& to_cloud();
    // 初始化并打开存储引擎。
    virtual Status open() = 0;
    virtual void stop() = 0;
    virtual bool stopped() = 0;

    // start all background threads. This should be call after env is ready.
    virtual Status start_bg_threads(std::shared_ptr<WorkloadGroup> wg_sptr = nullptr) = 0;

    /* Parameters:
     * - tablet_id: the id of tablet to get
     * - sync_stats: the stats of sync rowset
     * - force_use_only_cached: whether only use cached tablet meta
     * - cache_on_miss: whether cache the tablet meta when missing in cache
     */
    // 根据 tablet_id 获取 Tablet 实例。
    virtual Result<BaseTabletSPtr> get_tablet(int64_t tablet_id,
                                              SyncRowsetStats* sync_stats = nullptr,
                                              bool force_use_only_cached = false,
                                              bool cache_on_miss = true) = 0;
    // 获取指定 Tablet 的元数据（TabletMeta）
    virtual Status get_tablet_meta(int64_t tablet_id, TabletMetaSharedPtr* tablet_meta,
                                   bool force_use_only_cached = false) = 0;
    // 注册/注销汇报监听器。
    void register_report_listener(ReportWorker* listener);
    void deregister_report_listener(ReportWorker* listener);
    // 通知汇报监听器触发数据/任务汇报。
    void notify_listeners();
    bool notify_listener(std::string_view name);
    // 设置 FE 发送的心跳标记。
    void set_heartbeat_flags(HeartbeatFlags* heartbeat_flags) {
        _heartbeat_flags = heartbeat_flags;
    }
    // 设置 Cluster ID。
    virtual Status set_cluster_id(int32_t cluster_id) = 0;
    // 获取当前生效的 Cluster ID。
    int32_t effective_cluster_id() const { return _effective_cluster_id; }
    // 生成并返回下一个唯一的 RowsetId。
    RowsetId next_rowset_id();
    // 获取 MemTable Flush 执行器指针。
    MemTableFlushExecutor* memtable_flush_executor() { return _memtable_flush_executor.get(); }
    // 获取自适应线程池控制器指针。
    AdaptiveThreadPoolController* adaptive_thread_controller() {
        return &_adaptive_thread_controller;
    }
    CalcDeleteBitmapExecutor* calc_delete_bitmap_executor() {
        return _calc_delete_bitmap_executor.get();
    }

    CalcDeleteBitmapExecutor* calc_delete_bitmap_executor_for_load() {
        return _calc_delete_bitmap_executor_for_load.get();
    }
    // 注册正在被查询的 Rowset，赋予其临时引用防 GC。
    void add_quering_rowset(RowsetSharedPtr rs);
    // 根据 RowsetId 获取正在被查询的 Rowset。
    RowsetSharedPtr get_quering_rowset(RowsetId rs_id);

    int64_t memory_limitation_bytes_per_thread_for_schema_change() const;
    // 获取物理磁盘总数。
    int get_disk_num() { return _disk_num; }
    // 初始化 Stream Load 历史记录组件。
    Status init_stream_load_recorder(const std::string& stream_load_record_path);

    const std::shared_ptr<StreamLoadRecorder>& get_stream_load_recorder() {
        return _stream_load_recorder;
    }

protected:
    // 启动自适应线程池控制器。
    void _start_adaptive_thread_controller();
    // 踢出过期的 Querying Rowset 引用。
    void _evict_querying_rowset();
    void _evict_quring_rowset_thread_callback();
    bool _should_delay_large_task();
    // 有效的 Cluster ID，用于校验数据文件是否属于当前 Doris 集群。
    int32_t _effective_cluster_id = -1;
    // 心跳标记指针，记录从 FE（Frontend）心跳发来的全局配置状态。
    HeartbeatFlags* _heartbeat_flags = nullptr;

    // For task, tablet and disk report
    // 用于管理心跳/汇报监听器（ReportWorker）的互斥锁与列表。
    std::mutex _report_mtx;
    std::vector<ReportWorker*> _report_listeners;
    // 全局 Rowset ID 生成器，保证 Rowset ID 唯一。
    std::unique_ptr<RowsetIdGenerator> _rowset_id_generator;
    // MemTable 刷盘（Flush）执行器。
    std::unique_ptr<MemTableFlushExecutor> _memtable_flush_executor;
    // 自适应线程池控制器，根据系统负载动态调整线程数。
    AdaptiveThreadPoolController _adaptive_thread_controller;
    // 主 Delete Bitmap 计算执行器（MOW 主键模型使用）。
    std::unique_ptr<CalcDeleteBitmapExecutor> _calc_delete_bitmap_executor;
    // 专门用于导入（Load）过程中的 Delete Bitmap 计算执行器。
    std::unique_ptr<CalcDeleteBitmapExecutor> _calc_delete_bitmap_executor_for_load;
    // 用于优雅停止后台线程的倒计时锁（CountDownLatch）。
    CountDownLatch _stop_background_threads_latch;

    // Hold reference of quering rowsets
    // 正在被查询的 Rowset 的互斥锁与 Map，防止其被 GC 删除。
    std::mutex _quering_rowsets_mutex;
    std::unordered_map<RowsetId, RowsetSharedPtr> _querying_rowsets;
    // 后台定期清理不再被查询的 Rowset 引用缓存的线程。
    std::shared_ptr<Thread> _evict_quering_rowset_thread;
    // Schema Change 操作的单线程内存上限。
    int64_t _memory_limitation_bytes_for_schema_change;
    // 可用的物理磁盘数量。
    int _disk_num {-1};
    // Stream Load 导入历史记录器。
    std::shared_ptr<StreamLoadRecorder> _stream_load_recorder;
    // bvar 监控指标，记录 Delete Bitmap 分数最大的 Tablet。
    std::shared_ptr<bvar::Status<size_t>> _tablet_max_delete_bitmap_score_metrics;
    // bvar 监控指标，记录 Base Rowset 的 Delete Bitmap 分数。
    std::shared_ptr<bvar::Status<size_t>> _tablet_max_base_rowset_delete_bitmap_score_metrics;
    // Base Compaction 线程池。
    std::unique_ptr<ThreadPool> _base_compaction_thread_pool;
    // Cumulative Compaction 线程池。
    std::unique_ptr<ThreadPool> _cumu_compaction_thread_pool;
    // Binlog Compaction 线程池。
    std::unique_ptr<ThreadPool> _binlog_compaction_thread_pool;
    // Cumulative Compaction 线程池已用线程数。
    int _cumu_compaction_thread_pool_used_threads {0};
    // 正在运行的小型 Compaction 任务数。
    int _cumu_compaction_thread_pool_small_tasks_running {0};
};


// 用于记录和追踪每个数据目录（DataDir）上已提交、正在执行的各类 Compaction 任务，防止对同一个 Tablet 重复提交 Compaction，并提供任务限流和状态查询。
class CompactionSubmitRegistry {
    using TabletSet = std::unordered_set<TabletSharedPtr>;
    using Registry = std::map<DataDir*, TabletSet>;

public:
    CompactionSubmitRegistry() = default;
    CompactionSubmitRegistry(CompactionSubmitRegistry&& r);

    // create a snapshot for current registry, operations to the snapshot can be lock-free.
    // 创建当前注册表状态的只读快照，快照上的操作无锁（Lock-free）。
    CompactionSubmitRegistry create_snapshot();
    // 根据传入的数据目录列表重置注册表。
    void reset(const std::vector<DataDir*>& stores);
    // 统计指定磁盘上某种 Compaction 类型正在执行的任务数。
    uint32_t count_executing_compaction(DataDir* dir, CompactionType compaction_type);
    // 统计指定磁盘上累计执行的 Cumu 和 Base Compaction 任务总数。
    uint32_t count_executing_cumu_and_base(DataDir* dir);
    // 检查某磁盘上是否有指定类型的 Compaction 任务。
    bool has_compaction_task(DataDir* dir, CompactionType compaction_type);
    // 向注册表中登记一个待执行/执行中的 Compaction 任务，若已存在则返回 false。
    bool insert(TabletSharedPtr tablet, CompactionType compaction_type);
    // 任务完成后从注册表中移除，并执行唤醒回调函数。
    void remove(TabletSharedPtr tablet, CompactionType compaction_type,
                std::function<void()> wakeup_cb);
    // 将 Compaction 提交注册状态序列化为 JSON 字符串（用于 Web 监控页面展示）。
    void jsonfy_compaction_status(std::string* result);
    // 依据分数从 TabletManager 中筛选出得分最高、最需要执行 Compaction 的 Top-N 个 Tablet。
    std::vector<TabletCompactionContext> pick_topn_tablets_for_compaction(
            TabletManager* tablet_mgr, DataDir* data_dir, CompactionType compaction_type,
            const CumuCompactionPolicyTable& cumu_compaction_policies,
            CompactionScoreStats* disk_score_stats);

private:
    // 根据磁盘和 Compaction 类型获取对应的 TabletSet。
    TabletSet& _get_tablet_set(DataDir* dir, CompactionType compaction_type);
    // 保护提交状态注册表的互斥锁。
    std::mutex _tablet_submitted_compaction_mutex;
    // 各磁盘上已提交 Cumulative Compaction 的 Tablet 集合。
    Registry _tablet_submitted_cumu_compaction;
    // 各磁盘上已提交 Base Compaction 的 Tablet 集合。
    Registry _tablet_submitted_base_compaction;
    // 各磁盘上已提交 Full Compaction 的 Tablet 集合。
    Registry _tablet_submitted_full_compaction;
    // 各磁盘上已提交 Binlog Compaction 的 Tablet 集合。
    Registry _tablet_submitted_binlog_compaction;
};
// StorageEngine 是 Doris 传统本地存储（Shared-Nothing 架构）的核心实现。
class StorageEngine final : public BaseStorageEngine {
public:
    StorageEngine(const EngineOptions& options);
    ~StorageEngine() override;

    Status open() override;

    Status create_tablet(const TCreateTabletReq& request, RuntimeProfile* profile);

    /* Parameters:
     * - tablet_id: the id of tablet to get
     * - sync_stats: the stats of sync rowset
     * - force_use_only_cached: whether only use cached tablet meta
     * - cache_on_miss: whether cache the tablet meta when missing in cache
     */
    Result<BaseTabletSPtr> get_tablet(int64_t tablet_id, SyncRowsetStats* sync_stats = nullptr,
                                      bool force_use_only_cached = false,
                                      bool cache_on_miss = true) override;

    Status get_tablet_meta(int64_t tablet_id, TabletMetaSharedPtr* tablet_meta,
                           bool force_use_only_cached = false) override;

    void clear_transaction_task(const TTransactionId transaction_id);
    void clear_transaction_task(const TTransactionId transaction_id,
                                const std::vector<TPartitionId>& partition_ids);

    std::vector<DataDir*> get_stores(bool include_unused = false);

    // get all info of root_path
    Status get_all_data_dir_info(std::vector<DataDirInfo>* data_dir_infos, bool need_update);

    static int64_t get_file_or_directory_size(const std::string& file_path);

    // get root path for creating tablet. The returned vector of root path should be round robin,
    // for avoiding that all the tablet would be deployed one disk.
    std::vector<DataDir*> get_stores_for_create_tablet(int64_t partition_id,
                                                       TStorageMedium::type storage_medium);

    DataDir* get_store(const std::string& path);

    uint32_t available_storage_medium_type_count() const {
        return _available_storage_medium_type_count;
    }

    Status set_cluster_id(int32_t cluster_id) override;

    void start_delete_unused_rowset();
    void add_unused_rowset(RowsetSharedPtr rowset);
    using DeleteBitmapKeyRanges =
            std::vector<std::tuple<DeleteBitmap::BitmapKey, DeleteBitmap::BitmapKey>>;
    void add_unused_delete_bitmap_key_ranges(int64_t tablet_id,
                                             const std::vector<RowsetId>& rowsets,
                                             const DeleteBitmapKeyRanges& key_ranges);

    // Obtain shard path for new tablet.
    //
    // @param [out] shard_path choose an available root_path to clone new tablet
    // @return error code
    Status obtain_shard_path(TStorageMedium::type storage_medium, int64_t path_hash,
                             std::string* shared_path, DataDir** store, int64_t partition_id);

    // Load new tablet to make it effective.
    //
    // @param [in] root_path specify root path of new tablet
    // @param [in] request specify new tablet info
    // @param [in] restore whether we're restoring a tablet from trash
    // @return OK if load tablet success
    Status load_header(const std::string& shard_path, const TCloneReq& request,
                       bool restore = false);

    TabletManager* tablet_manager() { return _tablet_manager.get(); }
    TxnManager* txn_manager() { return _txn_manager.get(); }
    SnapshotManager* snapshot_mgr() { return _snapshot_mgr.get(); }
    // Rowset garbage collection helpers
    bool check_rowset_id_in_unused_rowsets(const RowsetId& rowset_id);
    PendingRowsetSet& pending_local_rowsets() { return _pending_local_rowsets; }
    PendingRowsetSet& pending_remote_rowsets() { return _pending_remote_rowsets; }
    PendingRowsetGuard add_pending_rowset(const RowsetWriterContext& ctx);

    RowsetTypePB default_rowset_type() const {
        if (_heartbeat_flags != nullptr && _heartbeat_flags->is_set_default_rowset_type_to_beta()) {
            return BETA_ROWSET;
        }
        return _default_rowset_type;
    }

    Status start_bg_threads(std::shared_ptr<WorkloadGroup> wg_sptr = nullptr) override;

    // clear trash and snapshot file
    // option: update disk usage after sweep
    Status start_trash_sweep(double* usage, bool ignore_guard = false);

    // Must call stop() before storage_engine is deconstructed
    void stop() override;

    void get_tablet_rowset_versions(const PGetTabletVersionsRequest* request,
                                    PGetTabletVersionsResponse* response);

    bool get_peers_replica_backends(int64_t tablet_id, std::vector<TBackend>* backends);

    const std::shared_ptr<StreamLoadRecorder>& get_stream_load_recorder() {
        return _stream_load_recorder;
    }

    void get_compaction_status_json(std::string* result);

    Status submit_compaction_task(TabletSharedPtr tablet, CompactionType compaction_type,
                                  bool force, bool eager = true, int trigger_method = 0);
    Status submit_seg_compaction_task(std::shared_ptr<SegcompactionWorker> worker,
                                      SegCompactionCandidatesSharedPtr segments);

    ThreadPool* tablet_publish_txn_thread_pool() { return _tablet_publish_txn_thread_pool.get(); }
    bool stopped() override { return _stopped; }

    Status process_index_change_task(const TAlterInvertedIndexReq& reqest);

    void gc_binlogs(const std::unordered_map<int64_t, int64_t>& gc_tablet_infos);

    void add_async_publish_task(int64_t partition_id, int64_t tablet_id, int64_t publish_version,
                                int64_t transaction_id, bool is_recover, int64_t commit_tso);
    int64_t get_pending_publish_min_version(int64_t tablet_id);

    bool add_broken_path(std::string path);
    bool remove_broken_path(std::string path);

    std::set<std::string> get_broken_paths() { return _broken_paths; }

    Status submit_clone_task(Tablet* tablet, int64_t version);

    std::unordered_map<int64_t, std::unique_ptr<TaskWorkerPoolIf>>* workers;

    int64_t get_compaction_num_per_round() const { return _compaction_num_per_round; }

#ifdef BE_TEST
    std::vector<TabletSharedPtr> generate_compaction_tasks_for_test(
            CompactionType compaction_type, std::vector<DataDir*>& data_dirs, bool check_score) {
        auto tablet_contexts = _generate_compaction_tasks(compaction_type, data_dirs, check_score);
        std::vector<TabletSharedPtr> tablets;
        tablets.reserve(tablet_contexts.size());
        for (auto& context : tablet_contexts) {
            tablets.emplace_back(std::move(context.tablet));
        }
        return tablets;
    }

    CompactionSubmitRegistry& compaction_submit_registry_for_test() {
        return _compaction_submit_registry;
    }
#endif

private:
    // Instance should be inited from `static open()`
    // MUST NOT be called in other circumstances.
    Status _open();

    Status _init_store_map();

    void _update_storage_medium_type_count();

    // Some check methods
    Status _check_file_descriptor_number();
    Status _check_all_root_path_cluster_id();
    Status _judge_and_update_effective_cluster_id(int32_t cluster_id);

    void _exit_if_too_many_disks_are_failed();

    void _clean_unused_txns();

    void _clean_unused_rowset_metas();

    void _clean_unused_binlog_metas();

    void _clean_unused_delete_bitmap();

    void _clean_unused_pending_publish_info();

    void _clean_unused_partial_update_info();

    Status _do_sweep(const std::string& scan_root, const time_t& local_tm_now,
                     const int32_t expire);

    // All these xxx_callback() functions are for Background threads
    // unused rowset monitor thread
    void _unused_rowset_monitor_thread_callback();

    // garbage sweep thread process function. clear snapshot and trash folder
    void _garbage_sweeper_thread_callback();

    // delete tablet with io error process function
    void _disk_stat_monitor_thread_callback();

    // path gc process function
    void _path_gc_thread_callback(DataDir* data_dir);

    void _tablet_path_check_callback();

    void _tablet_checkpoint_callback(const std::vector<DataDir*>& data_dirs);

    // parse the default rowset type config to RowsetTypePB
    void _parse_default_rowset_type();

    // Disk status monitoring. Monitoring unused_flag Road King's new corresponding root_path unused flag,
    // When the unused mark is detected, the corresponding table information is deleted from the memory, and the disk data does not move.
    // When the disk status is unusable, but the unused logo is not _push_tablet_into_submitted_compactiondetected, you need to download it from root_path
    // Reload the data.
    void _start_disk_stat_monitor();

    void _compaction_tasks_producer_callback();
    void _binlog_compaction_tasks_producer_callback();

    std::vector<TabletCompactionContext> _generate_compaction_tasks(
            CompactionType compaction_type, std::vector<DataDir*>& data_dirs, bool check_score);
    void _update_cumulative_compaction_policy();
    CumuCompactionPolicyTable _snapshot_cumulative_compaction_policy();
    std::shared_ptr<CumulativeCompactionPolicy> _get_cumulative_compaction_policy(
            std::string_view compaction_policy);

    void _pop_tablet_from_submitted_compaction(TabletSharedPtr tablet,
                                               CompactionType compaction_type);

    Status _submit_compaction_task(TabletSharedPtr tablet, CompactionType compaction_type,
                                   bool force, int trigger_method = 0);

    void _handle_compaction(TabletSharedPtr tablet, std::shared_ptr<CompactionMixin> compaction,
                            CompactionType compaction_type, int64_t permits, bool force,
                            int64_t compaction_id = 0);

    void _adjust_compaction_thread_num();

    void _cooldown_tasks_producer_callback();
    void _remove_unused_remote_files_callback();
    void do_remove_unused_remote_files();
    void _cold_data_compaction_producer_callback();
    void _handle_cold_data_compaction(TabletSharedPtr tablet);
    void _follow_cooldown_meta(TabletSharedPtr tablet);

    Status _handle_seg_compaction(std::shared_ptr<SegcompactionWorker> worker,
                                  SegCompactionCandidatesSharedPtr segments,
                                  uint64_t submission_time);

    Status _handle_index_change(IndexBuilderSharedPtr index_builder);

    void _gc_binlogs(int64_t tablet_id, int64_t version);

    void _async_publish_callback();

    void _process_async_publish();

    Status _persist_broken_paths();

    bool _increase_low_priority_task_nums(DataDir* dir);

    void _decrease_low_priority_task_nums(DataDir* dir);

    void _get_candidate_stores(TStorageMedium::type storage_medium,
                               std::vector<DirInfo>& dir_infos);

    int _get_and_set_next_disk_index(int64_t partition_id, TStorageMedium::type storage_medium);

    int32_t _auto_get_interval_by_disk_capacity(DataDir* data_dir);

    void _check_tablet_delete_bitmap_score_callback();

private:
    // 存储引擎初始化配置参数（如路径列表等）。
    EngineOptions _options;
    // 保护 _store_map 的互斥锁。
    std::mutex _store_lock;
    // 保护垃圾回收清理过程的互斥锁。
    std::mutex _trash_sweep_lock;
    // 存储路径名称到 DataDir 对象的映射表。
    std::map<std::string, std::unique_ptr<DataDir>> _store_map;
    // 故障/坏盘路径集合及互斥锁。
    std::set<std::string> _broken_paths;
    std::mutex _broken_paths_mutex;
    // 当前可用的存储介质类型数量（如 HDD, SSD）。
    uint32_t _available_storage_medium_type_count;
    // 标记所有根目录的 Cluster ID 是否均正确存在。
    bool _is_all_cluster_id_exist;
    // 标记存储引擎是否处于停止状态。
    std::atomic_bool _stopped {false};
    // 保护无用 Rowset/DeleteBitmap 垃圾回收列表的互斥锁。
    std::mutex _gc_mutex;
    // 等待被后台物理删除的无用 Rowset 集合。
    std::unordered_map<RowsetId, RowsetSharedPtr> _unused_rowsets;
    // tablet_id, unused_rowsets, [start_version, end_version]
    // 等待被清理的无用 Delete Bitmap Key 范围列表。
    std::vector<std::tuple<int64_t, std::vector<RowsetId>, DeleteBitmapKeyRanges>>
            _unused_delete_bitmap;
    // 本地临时/未生效的 Rowset 挂起集合。
    PendingRowsetSet _pending_local_rowsets;
    // 远端/存算分离模式下临时 Rowset 挂起集合。
    PendingRowsetSet _pending_remote_rowsets;
    // 无用 Rowset 垃圾监控线程。
    std::shared_ptr<Thread> _unused_rowset_monitor_thread;
    // thread to monitor snapshot expiry
    // 垃圾清理线程（清理 Trash 和 Snapshot 目录）。
    std::shared_ptr<Thread> _garbage_sweeper_thread;
    // thread to monitor disk stat
    // 磁盘状态监控线程。
    std::shared_ptr<Thread> _disk_stat_monitor_thread;
    // thread to produce both base and cumulative compaction tasks
    // Compaction 任务生产者线程（定期触发合并）。
    std::shared_ptr<Thread> _compaction_tasks_producer_thread;
    //  Binlog Compaction 任务生产者线程。
    std::shared_ptr<Thread> _binlog_compaction_tasks_producer_thread;
    // 缓存清理线程。
    std::shared_ptr<Thread> _cache_clean_thread;
    // threads to clean all file descriptor not actively in use
    // 数据目录文件描述符与路径回收线程组。
    std::vector<std::shared_ptr<Thread>> _path_gc_threads;
    // thread to produce tablet checkpoint tasks
    // Tablet Checkpoint 任务生产者线程。
    std::shared_ptr<Thread> _tablet_checkpoint_tasks_producer_thread;
    // thread to check tablet path
    // Tablet 路径合规性检查线程。
    std::shared_ptr<Thread> _tablet_path_check_thread;
    // thread to clean tablet lookup cache
    // Lookup 缓存清理线程。
    std::shared_ptr<Thread> _lookup_cache_clean_thread;
    // 引擎级别任务互斥锁。
    std::mutex _engine_task_mutex;
    // TabletManager 独占指针，管理节点上所有 Tablet。
    std::unique_ptr<TabletManager> _tablet_manager;
    // TxnManager 独占指针，管理导入事务。
    std::unique_ptr<TxnManager> _txn_manager;

    // Used to control the migration from segment_v1 to segment_v2, can be deleted in futrue.
    // Type of new loaded data
    // 默认 Rowset 格式（Beta Rowset）。
    RowsetTypePB _default_rowset_type;
    // Segment Compaction 线程池（单 Rowset 内部 Segment 合并）。
    std::unique_ptr<ThreadPool> _seg_compaction_thread_pool;
    // 冷数据 Compaction 线程池。
    std::unique_ptr<ThreadPool> _cold_data_compaction_thread_pool;
    // Tablet 事务 Publish 线程池。
    std::unique_ptr<ThreadPool> _tablet_publish_txn_thread_pool;
    // Tablet Meta 保存 Checkpoint 的线程池。
    std::unique_ptr<ThreadPool> _tablet_meta_checkpoint_thread_pool;
    // Compaction 许可限制器（基于内存/配额控制 Compaction 并发）。
    CompactionPermitLimiter _permit_limiter;
    // Compaction 任务提交注册表。
    CompactionSubmitRegistry _compaction_submit_registry;
    // 各磁盘上低优先级任务数量计数器。
    std::mutex _low_priority_task_nums_mutex;
    std::unordered_map<DataDir*, int32_t> _low_priority_task_nums;
    // 唤醒生产者线程的原子标记。
    std::atomic<int32_t> _wakeup_producer_flag {0};
    // Compaction 生产者休眠与唤醒的条件变量。
    std::mutex _compaction_producer_sleep_mutex;
    std::condition_variable _compaction_producer_sleep_cv;

    // we use unordered_map to store all cumulative compaction policy sharded ptr
    // Cumu Compaction 策略映射表（如 Size-based 策略）。
    std::mutex _cumulative_compaction_policy_mtx;
    CumuCompactionPolicyTable _cumulative_compaction_policies;
    // 数据下冷/冷却任务生产者线程。
    std::shared_ptr<Thread> _cooldown_tasks_producer_thread;
    // 无用远端对象存储文件清理线程。
    std::shared_ptr<Thread> _remove_unused_remote_files_thread;
    // 冷数据 Compaction 生产者线程。
    std::shared_ptr<Thread> _cold_data_compaction_producer_thread;
    // 缓存文件清理生产者线程。
    std::shared_ptr<Thread> _cache_file_cleaner_tasks_producer_thread;
    // 冷热数据转移（Cooldown）优先级线程池。
    std::unique_ptr<PriorityThreadPool> _cooldown_thread_pool;
    // 正在进行冷却操作的 Tablet ID 集合。
    std::mutex _running_cooldown_mutex;
    std::unordered_set<int64_t> _running_cooldown_tablets;
    // 已提交冷数据 Compaction 的 Tablet ID 集合。
    std::mutex _cold_compaction_tablet_submitted_mtx;
    std::unordered_set<int64_t> _cold_compaction_tablet_submitted;
    // Cumulative Compaction 延迟互斥锁。
    std::mutex _cumu_compaction_delay_mtx;

    // tablet_id, publish_version, transaction_id, partition_id, commit_tso
    // 异步 Publish 任务队列（Tablet -> Version -> Task info）。
    std::map<int64_t, std::map<int64_t, std::tuple<int64_t, int64_t, int64_t>>>
            _async_publish_tasks;
    // aync publish for discontinuous versions of merge_on_write table
    // 异步 Publish Version 任务处理线程。
    std::shared_ptr<Thread> _async_publish_thread;
    // 异步 Publish 读写锁。
    std::shared_mutex _async_publish_lock;
    // 标记是否需要进行垃圾回收。
    std::atomic<bool> _need_clean_trash {false};

    // next index for create tablet
    // 不同存储介质上一次选盘轮询的索引。
    std::map<TStorageMedium::type, int> _last_use_index;
    // 建表轮询选盘 LRU 缓存。
    std::unique_ptr<CreateTabletRRIdxCache> _create_tablet_idx_lru_cache;
    // 快照管理器（处理备份、恢复、Clone 快照）。
    std::unique_ptr<SnapshotManager> _snapshot_mgr;

    // thread to check tablet delete bitmap count tasks
    // 检查 Tablet 的 Delete Bitmap 得分状况线程。

    std::shared_ptr<Thread> _check_delete_bitmap_score_thread;
    // 上一次获取 Peer 副本 BE 节点列表的时间戳。
    int64_t _last_get_peers_replica_backends_time_ms {0};
    // 每轮选出的 Compaction 任务数量。
    int64_t _compaction_num_per_round {1};
};

// lru cache for create tabelt round robin in disks
// key: partitionId_medium
// value: index
class CreateTabletRRIdxCache : public LRUCachePolicy {
public:
    // get key, delimiter with DELIMITER '-'
    static std::string get_key(int64_t partition_id, TStorageMedium::type medium) {
        return fmt::format("{}-{}", partition_id, medium);
    }

    // -1 not found key in lru
    int get_index(const std::string& key);

    void set_index(const std::string& key, int next_idx);

    class CacheValue : public LRUCacheValueBase {
    public:
        int idx = 0;
    };

    CreateTabletRRIdxCache(size_t capacity)
            : LRUCachePolicy(CachePolicy::CacheType::CREATE_TABLET_RR_IDX_CACHE, capacity,
                             LRUCacheType::NUMBER,
                             /*stale_sweep_time_s*/ 30 * 60, /*num shards*/ 1,
                             /*element count capacity */ 0,
                             /*enable prune*/ true, /*is lru-k*/ false) {}
};

struct DirInfo {
    DataDir* data_dir;

    double usage = 0;
    int available_level = 0;

    bool operator<(const DirInfo& other) const {
        if (available_level != other.available_level) {
            return available_level < other.available_level;
        }
        return data_dir->path_hash() < other.data_dir->path_hash();
    }
};

} // namespace doris
