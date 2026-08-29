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

#include <gen_cpp/Types_types.h>
#include <stddef.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <vector>

#include "common/metrics/metrics.h"
#include "common/status.h"
#include "storage/olap_common.h"

namespace doris {

class Tablet;
class TabletManager;
class TxnManager;
class OlapMeta;
class RowsetIdGenerator;
class StorageEngine;
// 存储健康检查测试文件的文件名（.testfile）。健康检查时通过向磁盘尝试写入/删除该文件来判断磁盘是否可写且正常。
const char* const kTestFilePath = ".testfile";

// A DataDir used to manage data in same path.
// Now, After DataDir was created, it will never be deleted for easy implementation.
// DataDir 类是 Apache Doris 后端（BE）存储引擎的核心基础设施组件之一。
// 负责管理节点上一个具体物理存储路径（数据目录/磁盘）的整个生命周期、物理磁盘 IO 与空间状态监控、元数据管理（RocksDB）、以及分布在该路径上的 Tablet 状态与后台 GC 垃圾回收。
// 在 Doris 的存储架构中，一个 BE 节点可以配置多个 storage_root_path（如 /data1/doris, /data2/doris）。针对每一个根路径，存储引擎都会实例化一个独立的 DataDir 对象，其核心作用包括：
// 磁盘物理管理与健康检查：负责检查磁盘的挂载状态、读写权限（写入 .testfile）、获取与更新磁盘的总容量与可用容量（statfs），防止磁盘满导致进程崩溃。
// 集群与节点标识隔离：在数据根目录下持久化保存 cluster_id 文件，防止错误启动导致的跨集群数据污染；同时校验目录物理属性与 Shard 目录结构（0/, 1/ ... 1023/）。
// 本地元数据管理（OlapMeta）：每个 DataDir 独占一个底层的 RocksDB 实例（即 OlapMeta），用来持久化存储属于该磁盘上所有 Tablet 的 Meta 数据（TabletMeta）以及 Rowset Header。
// Tablet 注册与路由：维护挂载在该磁盘上的 Tablet 集合（_tablet_set），提供分片目录（Shard）路径计算以及动态注册/注销接口。
// 后台垃圾回收（GC）与清理：负责清理过期/废弃的数据目录、垃圾回收站（trash/ 目录）中的旧 Tablet、以及清理废弃的 Remote Rowset / Remote Tablet 状态。
// 指标采集（Metrics）：向 Prometheus / Doris Metrics 系统暴露该磁盘的可用空间、使用量、垃圾占用、Compaction 负载得分等关键运维指标。
class DataDir {
public:
    DataDir(StorageEngine& engine, const std::string& path, int64_t capacity_bytes = -1,
            TStorageMedium::type storage_medium = TStorageMedium::HDD);
    ~DataDir();
    // 初始化物理数据目录。包括依次调用 _init_cluster_id()、_init_capacity_and_create_shards()、_init_meta()（若 init_meta 为 true）以及注册 Metrics，完成磁盘使用前的就绪工作。
    Status init(bool init_meta = true);
    // 停止该数据目录关联的所有后台工作（将 _stop_bg_worker 标记置为 true），打断后续的路径 GC 和健康检查等任务。
    void stop_bg_worker();
    // 返回该数据目录的物理绝对路径（如 /data1/doris）。
    const std::string& path() const { return _path; }
    // 返回路径字符串的 Hash 值（_path_hash），在 Doris 全局用于快速索引和比对磁盘路径。
    size_t path_hash() const { return _path_hash; }
    // 返回该磁盘是否处于正常可用状态（_is_used）。若健康检查失败或坏盘，该值为 false。
    bool is_used() const { return _is_used; }
    // 返回从当前数据目录读取到的 cluster_id（集群唯一标识）。
    int32_t cluster_id() const { return _cluster_id; }
    // 返回 cluster_id 是否处于未完全初始化/不完整状态。
    bool cluster_id_incomplete() const { return _cluster_id_incomplete; }
    // 打包并获取当前磁盘的状态快照信息（包含路径、Hash、总容量、可用容量、Trash 占用、存储介质、可用状态等），主要用于 FE 心跳汇报和系统 Monitoring。
    DataDirInfo get_dir_info() {
        DataDirInfo info;
        info.path = _path;
        info.path_hash = _path_hash;
        info.disk_capacity = _disk_capacity_bytes;
        info.available = _available_bytes;
        info.trash_used_capacity = _trash_used_bytes;
        info.is_used = _is_used;
        info.storage_medium = _storage_medium;
        return info;
    }

    // save a cluster_id file under data path to prevent
    // invalid be config for example two be use the same
    // data path
    // 将指定的 cluster_id 写入到该数据目录根下的 cluster_id 持久化文件中。
    Status set_cluster_id(int32_t cluster_id);
    // 对该磁盘执行物理健康检查。
    // 通过调用 _check_disk() 检测磁盘是否发生 IO 错误或写入失败；若坏盘则将 _is_used 设置为 false 并记录日志。
    void health_check();
    // 使用原子自增 (_current_shard) 并对 MAX_SHARD_NUM (1024) 取模，轮询（Round-Robin）返回下一个可用于存放新 Tablet 的 Shard ID。
    int32_t get_shard() {
        return _current_shard.fetch_add(1, std::memory_order_relaxed) % MAX_SHARD_NUM;
    }
    // 获取该磁盘对应的 RocksDB 元数据存储指针（_meta）。
    OlapMeta* get_meta() { return _meta; }
    // 判断当前磁盘的存储介质是否为 SSD。
    bool is_ssd_disk() const { return _storage_medium == TStorageMedium::SSD; }
    // 返回当前磁盘的存储介质枚举类型（TStorageMedium::HDD 或 TStorageMedium::SSD）。
    TStorageMedium::type storage_medium() const { return _storage_medium; }
    // 线程安全地将一个 Tablet 的索引信息（TabletInfo）注册到当前磁盘的 _tablet_set 集合中。
    void register_tablet(Tablet* tablet);
    // 线程安全地从当前磁盘的 _tablet_set 集合中注销指定的 Tablet。
    void deregister_tablet(Tablet* tablet);
    // 从 _tablet_set 中清空所有 Tablet 信息，并将原持有的 TabletInfo 导出追加到传入的 tablet_infos 容器中。
    void clear_tablets(std::vector<TabletInfo>* tablet_infos);
    // 根据 Shard ID 拼接并返回物理 Shard 目录的绝对路径（例如 /data1/doris/data/0/）。
    std::string get_absolute_shard_path(int64_t shard_id);
    // 拼接并返回指定 Tablet 的物理绝对路径（例如 /data1/doris/data/0/10001/34521/）。
    std::string get_absolute_tablet_path(int64_t shard_id, int64_t tablet_id, int32_t schema_hash);
    // 扫描当前磁盘的垃圾回收站（trash/）目录，寻找指定 tablet_id 物理数据所在的绝对路径，并追加存入 paths 中（常用于数据恢复/Debug）。
    void find_tablet_in_trash(int64_t tablet_id, std::vector<std::string>* paths);
    // 传入一个在 trash/ 里的 schema_hash 级别路径，反向解析并提取出其对应的根数据目录路径（DataDir path）。
    static std::string get_root_path_from_schema_hash_path_in_trash(
            const std::string& schema_hash_dir_in_trash);

    // load data from meta and data files
    // 存储引擎启动阶段调用。负责扫描该磁盘 OlapMeta (RocksDB) 中存储的所有 TabletMeta 和 Rowset Header，将其反序列化加载到内存中，恢复该磁盘上所有的 Tablet 结构。
    Status load();
    // 执行物理路径垃圾回收。分步（由 _path_gc_step 控制）扫描 data/ 目录和垃圾回收站 trash/ 目录，调用 _perform_tablet_gc 和 _perform_rowset_gc 清理已经注销或过期的文件与目录。
    void perform_path_gc();
    // 针对冷热分层/存算分离架构，执行已失效或已过期的远程存储 Rowset 的垃圾回收。
    void perform_remote_rowset_gc();
    // 执行已被删除的远程存储 Tablet 相关的元数据和资源回收。
    void perform_remote_tablet_gc();

    // check if the capacity reach the limit after adding the incoming data
    // return true if limit reached, otherwise, return false.
    // TODO(cmy): for now we can not precisely calculate the capacity Doris used,
    // so in order to avoid running out of disk capacity, we currently use the actual
    // disk available capacity and total capacity to do the calculation.
    // So that the capacity Doris actually used may exceeds the user specified capacity.
    // 评估在拟写入 incoming_data_size 大小的数据后，该磁盘的可用空间是否会触及设定的容量警戒线/上限 limit。若触及上限返回 true。
    bool reach_capacity_limit(int64_t incoming_data_size);
    // 通过系统底层调用（如 statfs）更新磁盘的实际物理总容量（_disk_capacity_bytes）和实际剩余可用容量（_available_bytes）。
    Status update_capacity();
    // 计算并更新当前磁盘 trash/ 垃圾回收站目录所占用的物理空间大小（_trash_used_bytes）。
    void update_trash_capacity();
    // 更新本地存储数据量大小统计，并同步更新 Metrics 监控数值。
    void update_local_data_size(int64_t size);
    // 更新远程云存储/冷数据在该磁盘上映射的数据总量统计，并同步更新 Metrics。
    void update_remote_data_size(int64_t size);
    // 获取当前挂载在该磁盘上的 Tablet 总数量（_tablet_set.size()）。
    size_t tablet_size() const;
    // 原子增加或减少该磁盘当前的 Compaction 待处理分数（Compaction Score），用于选盘负载均衡评估及 Metrics 展现。
    void disks_compaction_score_increment(int64_t delta);
    // 原子增加或减少当前正在该磁盘上执行 Compaction 的任务数量计数。
    void disks_compaction_num_increment(int64_t delta);
    // 计算并返回加入 incoming_data_size 增量数据后的磁盘物理空间预计使用比例（范围 0.0 ~ 1.0）。
    double get_usage(int64_t incoming_data_size) const {
        return _disk_capacity_bytes == 0
                       ? 0
                       : (double)(_disk_capacity_bytes - _available_bytes + incoming_data_size) /
                                 (double)_disk_capacity_bytes;
    }

// 在 Mock/单元测试环境下，手动设置磁盘的总容量与可用容量，用于测试容量计算与选盘逻辑。
#ifdef BE_TEST
    void set_capacity_for_test(size_t disk_capacity_bytes, size_t available_bytes) {
        _disk_capacity_bytes = disk_capacity_bytes;
        _available_bytes = available_bytes;
    }
#endif

    // Move tablet to trash.
    // 将指定的 Tablet 物理数据目录原子移动（rename）到磁盘的垃圾回收站 trash/ 目录下，等待后续异步 GC 真正删除。
    Status move_to_trash(const std::string& tablet_path);
    // 当删除某个 Tablet 目录后，检查并尝试删除其上层的空目录（如空 Shard 目录或空的 tablet_id 目录），保持文件系统整洁。
    static Status delete_tablet_parent_path_if_empty(const std::string& tablet_path);

private:
    // 读取根目录下的 cluster_id 文件。
    // 若文件不存在则创建并写入当前集群的 cluster_id；若存在则校验是否与当前节点一致。
    Status _init_cluster_id();
    // 检查并创建数据根目录下的关键物理子目录结构（data/, trash/, snapshot/ 等），
    // 同时创建 0/ 到 1023/ 的物理 Shard 文件夹，并初始化磁盘容量。
    Status _init_capacity_and_create_shards();
    // 实例化并打开当前磁盘独占的 OlapMeta（RocksDB 数据库），用于后续读写 Tablet/Rowset 元数据。
    Status _init_meta();
    // 调用 _read_and_write_test_file() 检测磁盘读写是否正常；检查物理目录是否存在。
    Status _check_disk();
    // 在数据根目录下尝试创建、写入并立刻删除 .testfile 文件。若触发 IO 错误或权限拒绝则返回失败。
    Status _read_and_write_test_file();
    // Check whether has old format (hdr_ start) in olap. When doris updating to current version,
    // it may lead to data missing. When conf::storage_strict_check_incompatible_old_format is true,
    // process will log fatal.
    // 检查磁盘中是否存在老旧格式（hdr_ 开头的升级遗留文件）的 Tablet。若存在且开启了严格兼容性检查配置，将打出 Fatal 日志终止进程，防止升级导致数据丢失。
    Status _check_incompatible_old_format_tablet();

    int _path_gc_step {0};
    // 针对特定 Shard 路径下的 schema_hash 目录执行真正的数据删除，清理由于 Drop Table/Tablet 移动到 Trash 中超时的物理文件。
    void _perform_tablet_gc(const std::string& tablet_schema_hash_path, int16_t shard_name);
    // 扫描并清理特定 Tablet 目录下孤立的、过期的或未成功 Commit/Stale 的物理 Rowset 数据文件（.dat / .idx）。
    void _perform_rowset_gc(const std::string& tablet_schema_hash_path);

private:
    // 标识当前 DataDir 的后台线程（如 GC、健康检查）是否应当停止运行。
    std::atomic<bool> _stop_bg_worker = false;
    // 持有全局存储引擎 StorageEngine 对象的引用，便于跨组件协调通信。
    StorageEngine& _engine;
    // 记录当前数据目录在操作系统中的根路径字符串（如 /data1/doris）。
    std::string _path;
    // 当前路径字符串 _path 的 Hash 值，提高 Hash Map 检索时的效率。
    size_t _path_hash;

    // the actual available capacity of the disk of this data dir
	// 当前磁盘实际剩余的物理可用字节数（实时通过 update_capacity() 刷更新）。
    size_t _available_bytes;
    // the actual capacity of the disk of this data dir
	// 当前磁盘的物理总字节数或用户配置的限制上限。
    size_t _disk_capacity_bytes;
	// 当前磁盘 trash/ 垃圾回收站目录占用的磁盘容量大小（字节）。
    size_t _trash_used_bytes;
	// 当前磁盘物理介质类型（HDD 或 SSD）。
    TStorageMedium::type _storage_medium;
	// 记录当前磁盘是否可用（true 表示正常，false 表示坏盘或无法读写）。
    bool _is_used;
	// 记录当前磁盘绑定的 Doris 集群 ID。
    int32_t _cluster_id;
	// 标记当前磁盘的 cluster_id 文件是否处于未完全写入/损坏状态。
    bool _cluster_id_incomplete = false;
    // This flag will be set true if this store was not in root path when reloading
	// 标记该存储路径是否在热加载/配置重新加载后被剔除，需要后续被异步清理。
    bool _to_be_deleted;
	// 定义物理数据存储的最大 Shard 目录数量（限制为 1024 个目录：0 ~ 1023），用于离散化大目录下的文件数量。
    static constexpr int32_t MAX_SHARD_NUM = 1024;
	// 轮询分配 Shard 目录的原子计数器，通过 fetch_add 循环分配新的 Tablet 到不同的 Shard 子目录。
    std::atomic<int32_t> _current_shard {0};
    // used to protect and _tablet_set
	// 互斥锁，保护针对 _tablet_set 的并发读写访问。
    mutable std::mutex _mutex;

    // 存储归属于当前磁盘的所有 Tablet 的基本信息集合（TabletInfo），实现磁盘维度的 Tablet 元数据索引。
    std::set<TabletInfo> _tablet_set;
    // 指向基于 RocksDB 实现的 OlapMeta 实例指针。
    // 负责持久化存储写在该磁盘上的所有 TabletMeta 和 Rowset Meta 信息。
    OlapMeta* _meta = nullptr;
    // 当前 DataDir 绑定的 Prometheus Metric Entity 实体，管理以下具体监控指标：
    std::shared_ptr<MetricEntity> _data_dir_metric_entity;
    // 磁盘总容量 Gauge 监控指标。
    IntGauge* disks_total_capacity = nullptr;
    // 磁盘可用容量 Gauge 监控指标。
    IntGauge* disks_avail_capacity = nullptr;
    // 磁盘本地已用数据量 Gauge 监控指标。
    IntGauge* disks_local_used_capacity = nullptr;
    // 磁盘关联远程数据量 Gauge 监控指标。
    IntGauge* disks_remote_used_capacity = nullptr;
    // 磁盘垃圾回收站占用量 Gauge 监控指标。
    IntGauge* disks_trash_used_capacity = nullptr;
    // 磁盘健康状态 Gauge 监控指标（1 为健康，0 为异常）。
    IntGauge* disks_state = nullptr;
    // 磁盘当前积压的 Compaction 综合得分 Gauge 指标。
    IntGauge* disks_compaction_score = nullptr;
    // 磁盘当前正在运行的 Compaction 任务数 Gauge 指标。
    IntGauge* disks_compaction_num = nullptr;
};

} // namespace doris
