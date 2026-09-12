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
#include <fmt/format.h>
#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/types.pb.h>
#include <stddef.h>
#include <stdint.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <vector>

#include "common/logging.h"
#include "common/status.h"
#include "storage/metadata_adder.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {


// 在 Apache Doris 中，Rowset（行集） 代表物理存储层面上一次导入或一次 Compaction（数据合并）所产生的不可变（Immutable）数据集合。
// 一个 Rowset 通常包含一个或多个 Segment 文件（存储实际数据）以及对应的索引文件（如倒排索引、Bloom Filter 等）。
// Rowset 抽象类的主要职责包括：
// 生命周期管理与状态追踪：维护 Rowset 从未加载（UNLOADED）、已加载（LOADED）到卸载中（UNLOADED/UNLOADING）的状态，并借助引用计数实现多线程安全关闭与资源释放。
// 元数据（Metadata）代理与暴露：通过持有的 RowsetMetaSharedPtr，对外暴露版本号（Version）、数据/索引大小、行数、Segment 数量、主键范围等元信息。
// 数据读取入口：作为 RowsetReader 的工厂，负责创建读取底层 Segment 数据的 Reader 接口。
// 物理文件与存储管理：提供文件移除（remove）、硬链接复制（link_files_to）、上传到远程存储（upload_to）、文件完整性校验等物理 IO 相关的操作。
class Rowset;

namespace io {
class RemoteFileSystem;
} // namespace io

using RowsetSharedPtr = std::shared_ptr<Rowset>;
class RowsetReader;

// the rowset state transfer graph:
//    ROWSET_UNLOADED    <--|
//          ↓               |
//    ROWSET_LOADED         |
//          ↓               |
//    ROWSET_UNLOADING   -->|
enum RowsetState {
    // state for new created rowset
    // 初始未加载状态，或者资源已安全关闭/卸载的状态。
    ROWSET_UNLOADED,
    // state after load() called
    // 调用 load() 成功后的状态，表示底层文件/索引已被打开或加载到内存。
    ROWSET_LOADED,
    // state for closed() called but owned by some readers
    // 调用了 close() 但当前仍有 RowsetReader 在读取（引用计数 _refs_by_reader > 0），此时延迟卸载资源。
    ROWSET_UNLOADING
};

class RowsetStateMachine {
public:
    // 构造函数，初始化状态为 ROWSET_UNLOADED。
    RowsetStateMachine() : _rowset_state(ROWSET_UNLOADED) {}
    // 处理加载事件。
    // 仅允许从 ROWSET_UNLOADED 转移至 ROWSET_LOADED，非法转移返回错误码 ROWSET_INVALID_STATE_TRANSITION。
    Status on_load() {
        switch (_rowset_state) {
        case ROWSET_UNLOADED:
            _rowset_state = ROWSET_LOADED;
            break;

        default:
            return Status::Error<ErrorCode::ROWSET_INVALID_STATE_TRANSITION>(
                    "RowsetStateMachine meet invalid state");
        }
        return Status::OK();
    }
    // 处理关闭事件（从 ROWSET_LOADED 触发）。
    // 若无 Reader 引用（refs_by_reader == 0）则直接转为 ROWSET_UNLOADED；若仍有 Reader 引用，则转为 ROWSET_UNLOADING。
    Status on_close(uint64_t refs_by_reader) {
        switch (_rowset_state) {
        case ROWSET_LOADED:
            if (refs_by_reader == 0) {
                _rowset_state = ROWSET_UNLOADED;
            } else {
                _rowset_state = ROWSET_UNLOADING;
            }
            break;

        default:
            return Status::Error<ErrorCode::ROWSET_INVALID_STATE_TRANSITION>(
                    "RowsetStateMachine meet invalid state");
        }
        return Status::OK();
    }
    // 当 Reader 释放引用且引用计数归零时触发。将状态从 ROWSET_UNLOADING 转移至 ROWSET_UNLOADED。
    Status on_release() {
        switch (_rowset_state) {
        case ROWSET_UNLOADING:
            _rowset_state = ROWSET_UNLOADED;
            break;

        default:
            return Status::Error<ErrorCode::ROWSET_INVALID_STATE_TRANSITION>(
                    "RowsetStateMachine meet invalid state");
        }
        return Status::OK();
    }
    // 获取当前 Rowset 的状态。
    RowsetState rowset_state() { return _rowset_state; }

private:
    RowsetState _rowset_state;
};

class Rowset : public std::enable_shared_from_this<Rowset>, public MetadataAdder<Rowset> {
public:
    // Open all segment files in this rowset and load necessary metadata.
    // - `use_cache` : whether to use fd cache, only applicable to alpha rowset now
    //
    // May be called multiple times, subsequent calls will no-op.
    // Derived class implements the load logic by overriding the `do_load_once()` method.
    Status load(bool use_cache = true);

    // returns Status::Error<ErrorCode::ROWSET_CREATE_READER>() when failed to create reader
    virtual Status create_reader(std::shared_ptr<RowsetReader>* result) = 0;

    const RowsetMetaSharedPtr& rowset_meta() const { return _rowset_meta; }

    void merge_rowset_meta(const RowsetMeta& other);

    bool is_pending() const { return _is_pending; }

    bool is_local() const { return _rowset_meta->is_local(); }

    const std::string& tablet_path() const { return _tablet_path; }

    // publish rowset to make it visible to read
    void make_visible(Version version, int64_t commit_tso);
    void set_version(Version version);
    const TabletSchemaSPtr& tablet_schema() const;

    // helper class to access RowsetMeta
    int64_t start_version() const { return rowset_meta()->version().first; }
    int64_t end_version() const { return rowset_meta()->version().second; }
    int64_t index_disk_size() const { return rowset_meta()->index_disk_size(); }
    int64_t data_disk_size() const { return rowset_meta()->data_disk_size(); }
    int64_t total_disk_size() const { return rowset_meta()->total_disk_size(); }
    bool empty() const { return rowset_meta()->empty(); }
    bool zero_num_rows() const { return rowset_meta()->num_rows() == 0; }
    size_t num_rows() const { return rowset_meta()->num_rows(); }
    Version version() const { return rowset_meta()->version(); }
    RowsetId rowset_id() const { return rowset_meta()->rowset_id(); }
    int64_t creation_time() const { return rowset_meta()->creation_time(); }
    PUniqueId load_id() const { return rowset_meta()->load_id(); }
    int64_t txn_id() const { return rowset_meta()->txn_id(); }
    int64_t partition_id() const { return rowset_meta()->partition_id(); }
    // flag for push delete rowset
    bool delete_flag() const { return rowset_meta()->delete_flag(); }
    MOCK_FUNCTION int64_t num_segments() const { return rowset_meta()->num_segments(); }
    void to_rowset_pb(RowsetMetaPB* rs_meta) const { return rowset_meta()->to_rowset_pb(rs_meta); }
    RowsetMetaPB get_rowset_pb() const { return rowset_meta()->get_rowset_pb(); }
    // The writing time of the newest data in rowset, to measure the freshness of a rowset.
    int64_t newest_write_timestamp() const { return rowset_meta()->newest_write_timestamp(); }
    // The commit tso range of the data in rowset.
    TsoRange commit_tso() const { return rowset_meta()->commit_tso(); }

    bool is_segments_overlapping() const { return rowset_meta()->is_segments_overlapping(); }
    KeysType keys_type() { return _schema->keys_type(); }
    RowsetStatePB rowset_meta_state() const { return rowset_meta()->rowset_state(); }
    bool produced_by_compaction() const { return rowset_meta()->produced_by_compaction(); }

    // remove all files in this rowset
    // TODO should we rename the method to remove_files() to be more specific?
    virtual Status remove() = 0;

    // close to clear the resource owned by rowset
    // including: open files, indexes and so on
    // NOTICE: can not call this function in multithreads
    void close() {
        RowsetState old_state = _rowset_state_machine.rowset_state();
        if (old_state != ROWSET_LOADED) {
            return;
        }
        Status st = Status::OK();
        {
            std::lock_guard close_lock(_lock);
            uint64_t current_refs = _refs_by_reader;
            old_state = _rowset_state_machine.rowset_state();
            if (old_state != ROWSET_LOADED) {
                return;
            }
            if (current_refs == 0) {
                do_close();
            }
            st = _rowset_state_machine.on_close(current_refs);
        }
        if (!st.ok()) {
            LOG(WARNING) << "state transition failed from:" << _rowset_state_machine.rowset_state();
            return;
        }
        VLOG_NOTICE << "rowset is close. rowset state from:" << old_state << " to "
                    << _rowset_state_machine.rowset_state() << ", version:" << start_version()
                    << "-" << end_version() << ", tabletid:" << _rowset_meta->tablet_id();
    }

    // hard link all files in this rowset to `dir` to form a new rowset with id `new_rowset_id`.
    virtual Status link_files_to(const std::string& dir, RowsetId new_rowset_id,
                                 size_t new_rowset_start_seg_id = 0,
                                 std::set<int64_t>* without_index_uids = nullptr) = 0;

    virtual Status get_inverted_index_size(int64_t* index_size) = 0;

    // copy all files to `dir`
    virtual Status copy_files_to(const std::string& dir, const RowsetId& new_rowset_id) = 0;

    virtual Status upload_to(const StorageResource& dest_fs, const RowsetId& new_rowset_id) = 0;

    virtual Status remove_old_files(std::vector<std::string>* files_to_remove) = 0;

    virtual Status check_file_exist() = 0;

    bool need_delete_file() const { return _need_delete_file; }

    void set_need_delete_file() { _need_delete_file = true; }

    bool contains_version(Version version) const {
        return rowset_meta()->version().contains(version);
    }

    static bool comparator(const RowsetSharedPtr& left, const RowsetSharedPtr& right) {
        return left->end_version() < right->end_version();
    }

    // this function is called by reader to increase reference of rowset
    void acquire() { ++_refs_by_reader; }

    void release() {
        // if the refs by reader is 0 and the rowset is closed, should release the resouce
        uint64_t current_refs = --_refs_by_reader;
        if (current_refs == 0 && _rowset_state_machine.rowset_state() == ROWSET_UNLOADING) {
            {
                std::lock_guard release_lock(_lock);
                // rejudge _refs_by_reader because we do not add lock in create reader
                if (_refs_by_reader == 0 &&
                    _rowset_state_machine.rowset_state() == ROWSET_UNLOADING) {
                    // first do close, then change state
                    do_close();
                    static_cast<void>(_rowset_state_machine.on_release());
                }
            }
            if (_rowset_state_machine.rowset_state() == ROWSET_UNLOADED) {
                VLOG_NOTICE
                        << "close the rowset. rowset state from ROWSET_UNLOADING to ROWSET_UNLOADED"
                        << ", version:" << start_version() << "-" << end_version()
                        << ", tabletid:" << _rowset_meta->tablet_id();
            }
        }
    }

    void update_delayed_expired_timestamp(uint64_t delayed_expired_timestamp) {
        if (delayed_expired_timestamp > _delayed_expired_timestamp) {
            _delayed_expired_timestamp = delayed_expired_timestamp;
        }
    }

    uint64_t delayed_expired_timestamp() { return _delayed_expired_timestamp; }

    virtual Status get_segments_key_bounds(std::vector<KeyBoundsPB>* segments_key_bounds) {
        _rowset_meta->get_segments_key_bounds(segments_key_bounds);
        return Status::OK();
    }

    void get_num_segment_rows(std::vector<uint32_t>* num_segment_rows) {
        _rowset_meta->get_num_segment_rows(num_segment_rows);
    }

    // min key of the first segment
    bool first_key(std::string* min_key) {
        KeyBoundsPB key_bounds;
        bool ret = _rowset_meta->get_first_segment_key_bound(&key_bounds);
        if (!ret) {
            return false;
        }
        *min_key = key_bounds.min_key();
        return true;
    }

    // max key of the last segment
    bool last_key(std::string* max_key) {
        KeyBoundsPB key_bounds;
        bool ret = _rowset_meta->get_last_segment_key_bound(&key_bounds);
        if (!ret) {
            return false;
        }
        *max_key = key_bounds.max_key();
        return true;
    }

    bool is_segments_key_bounds_truncated() const {
        return _rowset_meta->is_segments_key_bounds_truncated();
    }

    bool is_segments_key_bounds_aggregated() const {
        return _rowset_meta->is_segments_key_bounds_aggregated();
    }

    bool check_rowset_segment();

    [[nodiscard]] virtual Status add_to_binlog() { return Status::OK(); }

    // is skip index compaction this time
    bool is_skip_index_compaction(int32_t column_id) const {
        return skip_index_compaction.find(column_id) != skip_index_compaction.end();
    }

    // set skip index compaction next time
    void set_skip_index_compaction(int32_t column_id) { skip_index_compaction.insert(column_id); }

    std::string get_rowset_info_str();

    void clear_cache();

    MOCK_FUNCTION Result<std::string> segment_path(int64_t seg_id);

    std::vector<std::string> get_index_file_names();

    // check if the rowset is a hole rowset
    bool is_hole_rowset() const { return _is_hole_rowset; }
    // set the rowset as a hole rowset
    void set_hole_rowset(bool is_hole_rowset) { _is_hole_rowset = is_hole_rowset; }

    int64_t approximate_cached_data_size();

    int64_t approximate_cache_index_size();

    std::chrono::time_point<std::chrono::system_clock> visible_timestamp() const;

protected:
    friend class RowsetFactory;

    DISALLOW_COPY_AND_ASSIGN(Rowset);
    // this is non-public because all clients should use RowsetFactory to obtain pointer to initialized Rowset
    Rowset(const TabletSchemaSPtr& schema, RowsetMetaSharedPtr rowset_meta,
           std::string tablet_path);

    // this is non-public because all clients should use RowsetFactory to obtain pointer to initialized Rowset
    virtual Status init() = 0;

    // release resources in this api
    virtual void do_close() = 0;

    virtual Status check_current_rowset_segment() = 0;

    virtual void clear_inverted_index_cache() = 0;
    // 向该 Rowset 所对应的 Tablet Schema（表结构 schema信息）。
    TabletSchemaSPtr _schema;
    // 指向 Rowset 的元数据对象，包含版本、Segment 数量、统计数据等。
    RowsetMetaSharedPtr _rowset_meta;

    // Local rowset requires a tablet path to obtain the absolute path on the local fs
    // Tablet 在本地文件系统中的绝对路径（本地存储模式必需）。
    std::string _tablet_path;

    // init in constructor
    // 标识该 Rowset 是否处于 Pending 状态（未发布/对查询不可见）。
    bool _is_pending;    // rowset is pending iff it's not in visible state
    // 标识该 Rowset 是否为 Cumulative Compaction 产生的 Rowset。
    bool _is_cumulative; // rowset is cumulative iff it's visible and start version < end version

    // mutex lock for load/close api because it is costly
    // 保护 load / close / release 等高昂资源操作的互斥锁，确保线程安全。
    std::mutex _lock;
    // 标记该 Rowset 的物理文件是否需要在析构或过期时被物理删除。
    bool _need_delete_file = false;
    // variable to indicate how many rowset readers owned this rowset
    // 原子引用计数，记录当前有多少个 RowsetReader 正持有并读取该 Rowset。
    std::atomic<uint64_t> _refs_by_reader;
    // rowset state machine
    // 管理该 Rowset 生命周期的状态机。
    RowsetStateMachine _rowset_state_machine;
    // 延迟过期/清理的时间戳，用于回收机制。
    std::atomic<uint64_t> _delayed_expired_timestamp = 0;

    // <column_uniq_id>, skip index compaction
    // 记录跳过倒排索引合并的列 ID（column_uniq_id）集合。
    std::set<int32_t> skip_index_compaction;

    // only used for cloud mode, it indicates whether this rowset is a hole rowset.
    // a hole rowset is a rowset that has no data, but is used to fill the version gap
    // it is used to ensure that the version sequence is continuous.
    // 仅用于存算分离/存算一体云模式，标记该 Rowset 是否为“空洞 Rowset”（无实际数据，仅用于填补版本连续性空缺）。
    bool _is_hole_rowset = false;
};

// `rs_metas` MUST already be sorted by `RowsetMeta::comparator`
Status check_version_continuity(const std::vector<RowsetSharedPtr>& rowsets);

} // namespace doris
