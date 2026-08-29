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
#include <stdint.h>

#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "io/cache/file_cache_common.h"
#include "util/uid_util.h"

namespace doris {
// Apache Doris 存储引擎中用来描述单个物理存储路径（数据目录）及其硬件属性的基础结构体。
struct StorePath {
    StorePath() : capacity_bytes(-1), storage_medium(TStorageMedium::HDD) {}
    StorePath(const std::string& path_, int64_t capacity_bytes_)
            : path(path_), capacity_bytes(capacity_bytes_), storage_medium(TStorageMedium::HDD) {}
    StorePath(const std::string& path_, int64_t capacity_bytes_,
              TStorageMedium::type storage_medium_)
            : path(path_), capacity_bytes(capacity_bytes_), storage_medium(storage_medium_) {}
    // 数据目录在操作系统中的绝对路径（例如 /home/disk1/doris.HDD 或 /mnt/ssd/doris）。
    // Doris BE 会在该路径下建立具体的存储目录结构（如 data/ 用于存放 Tablet 数据文件，trash/ 用于回收站，snapshot/ 用于数据快照）。
    std::string path;
    // 该存储路径被允许使用的最大字节数上限。
    // 默认值为 -1，表示不手动限制最大容量，存储引擎将直接使用整个物理磁盘的实际可用空间。
    int64_t capacity_bytes;
    // 标识该存储路径所对应的物理介质类型（存储介质）。
    // 枚举值通常包括 TStorageMedium::HDD（机械硬盘）和 TStorageMedium::SSD（固态硬盘）。
    // Doris 依靠此属性支持冷热数据分层与按介质选盘。例如，用户可以在建表时指定将热数据放在 SSD 介质的目录中以获得极高读写性能。
    TStorageMedium::type storage_medium;
};

// parse a single root path of storage_root_path
Status parse_root_path(const std::string& root_path, StorePath* path);

Status parse_conf_store_paths(const std::string& config_path, std::vector<StorePath>* path);

void parse_conf_broken_store_paths(const std::string& config_path, std::set<std::string>* paths);

struct CachePath {
    io::FileCacheSettings init_settings() const;

    CachePath(std::string path, int64_t total_bytes, int64_t query_limit_bytes,
              size_t normal_percent, size_t disposable_percent, size_t index_percent,
              size_t ttl_percent, std::string storage)
            : path(std::move(path)),
              total_bytes(total_bytes),
              query_limit_bytes(query_limit_bytes),
              normal_percent(normal_percent),
              disposable_percent(disposable_percent),
              index_percent(index_percent),
              ttl_percent(ttl_percent),
              storage(storage) {}

    std::string path;
    int64_t total_bytes = 0;
    int64_t query_limit_bytes = 0;
    size_t normal_percent = io::DEFAULT_NORMAL_PERCENT;
    size_t disposable_percent = io::DEFAULT_DISPOSABLE_PERCENT;
    size_t index_percent = io::DEFAULT_INDEX_PERCENT;
    size_t ttl_percent = io::DEFAULT_TTL_PERCENT;
    std::string storage = "disk";
};

Status parse_conf_cache_paths(const std::string& config_path, std::vector<CachePath>& path);

// EngineOptions 是 Apache Doris 存储引擎在初始化（通过 StorageEngine::open()）时传入的核心配置结构体。
// 它的主要作用是为存储引擎提供必要的节点身份标识以及底层的物理存储路径配置。
struct EngineOptions {
    // list paths that tablet will be put into.
    // 记录 BE 节点配置的所有有效数据存储路径列表（对应配置项 storage_root_path）。
    // 每个 StorePath 对象包含路径字符串（如 /data1/doris）、存储介质类型（HDD 或 SSD）以及指定的磁盘容量上限。
    // 存储引擎在启动时会遍历该列表，为每个路径创建一个 DataDir 对象，用于后续的数据分片（Tablet）分布、数据写入以及磁盘容量监控。
    std::vector<StorePath> store_paths;
    // 记录已经被标识为损坏或不可用的物理磁盘路径集合。
    // 在 BE 运行过程中，如果某些路径发生 IO 错误或被检测为坏盘，或者启动前已经持久化标记为损坏，这些路径会被放入该集合中。
    // 存储引擎在初始化和选盘建表时会主动跳过这些损坏路径，避免写入失败。
    std::set<std::string> broken_paths;
    // BE's UUID. It will be reset every time BE restarts.
    // 当前 BE 进程本次启动运行时的唯一标识符（UUID）。
    // 这是一个由高 64 位和低 64 位整数组成的 128 位唯一 ID（由 UniqueId::gen_uid() 生成）。
    // 每次 BE 进程重新启动时都会重新生成一个新的 UUID。它主要用于在内存中唯一标识当前 BE 实例的生命周期，辅助集群管理、分布式事务以及隔离不同次启动产生的临时状态。
    UniqueId backend_uid {0, 0};
};
} // namespace doris
