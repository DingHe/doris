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

#include <rocksdb/iterator.h>

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"

namespace rocksdb {
class ColumnFamilyHandle;
class DB;
class WriteBatch;
} // namespace rocksdb

namespace doris {

// OlapMeta 是 Apache Doris BE（Backend）存储引擎中负责物理节点本地元数据持久化存储的核心组件。
// 在 Doris BE 节点的架构中，每个物理存储路径（即 DataDir）下都会建立并维护一个独立的 OlapMeta 实例。OlapMeta 充当了 Doris 存储引擎与底层 RocksDB 嵌入式 Key-Value 数据库之间的封装适配层（Adapter/Wrapper）。
// 元数据管理与隔离：通过 RocksDB 的列族（Column Family，简称 CF）对不同类型的元数据进行逻辑隔离（例如 Tablet Meta、Rowset Meta、Header Meta 等）。
// 高效 KV 操作：向存储引擎（如 TabletManager、DataDir、Rowset）提供统一的键值对读写（get/put）、存在性快速预判（key_may_exist）、数据批量操作（Batch）与批量删除（remove）接口。
// 元数据遍历与恢复：提供基于 Key 前缀（Prefix）或定点 Seek 的迭代器遍历能力（iterate），用于 BE 启动时从 RocksDB 中扫描反序列化所有 Tablet 及其 Rowset 的 Header 元数据。
// 生命周期与资源管控：负责安全地管理 RocksDB 数据库实例（rocksdb::DB）与列族句柄（ColumnFamilyHandle）的初始化、打开、写批处理及析构顺序（确保列族句柄先于数据库实例释放，防止 C++ 悬空指针与崩溃）。
class OlapMeta final {
public:
    // 批量写入（Batch Put）操作的输入数据结构容器。
    struct BatchEntry {
        // 要写入的键（Key）的常引用，避免不必要的字符串拷贝。
        const std::string& key;
        // 要写入的值（Value）的常引用。
        const std::string& value;

        BatchEntry(const std::string& key_arg, const std::string& value_arg)
                : key(key_arg), value(value_arg) {}
    };
    // root_path：对应 DataDir 的物理根路径（例如 /data1/doris）。OlapMeta 会在该路径下创建并维护名为 meta 的 RocksDB 数据库目录。
    OlapMeta(const std::string& root_path);
    ~OlapMeta();
    // 初始化并打开 RocksDB 数据库。
    // 检查并在 {root_path}/meta 路径下建立或打开 RocksDB 物理文件
    Status init();
    // 从指定的列族中查找并读取 Key 对应的 Value。
    Status get(const int column_family_index, const std::string& key, std::string* value);
    // 利用 RocksDB 内置的 Bloom Filter（布隆过滤器）快速预判 Key 是否可能存在。
    bool key_may_exist(const int column_family_index, const std::string& key, std::string* value);
    // 向指定的列族中写入/更新单个 Key-Value 键值对（如持久化更新单个 Tablet 的 Meta 信息）。
    Status put(const int column_family_index, const std::string& key, const std::string& value);
    // 批量向指定列族写入多条 Key-Value 键值对。
    Status put(const int column_family_index, const std::vector<BatchEntry>& entries);
    // 直接将外部构造好的 rocksdb::WriteBatch 原子批处理对象写入 RocksDB。
    Status put(rocksdb::WriteBatch* batch);

    Status remove(const int column_family_index, const std::string& key);
    Status remove(const int column_family_index, const std::vector<std::string>& keys);
    // 带有精准 Seek 起始点及 Key 前缀 限制的迭代器遍历函数。
    Status iterate(const int column_family_index, std::string_view prefix,
                   std::function<bool(std::string_view, std::string_view)> const& func);

    Status iterate(const int column_family_index, std::string_view seek_key,
                   std::string_view prefix,
                   std::function<bool(std::string_view, std::string_view)> const& func);
    // 获取当前 OlapMeta 所绑定的物理根路径 _root_path。
    [[nodiscard]] std::string get_root_path() const { return _root_path; }

    rocksdb::ColumnFamilyHandle* get_handle(const int column_family_index) {
        return _handles[column_family_index].get();
    }

private:
    // 记录当前 OlapMeta 实例所管理的物理数据根目录路径。
    std::string _root_path;
    // keep order of _db && _handles, we need destroy _handles before _db
    // 持有的底层 RocksDB 数据库实例智能指针。
    std::unique_ptr<rocksdb::DB, std::function<void(rocksdb::DB*)>> _db;
    // 保存所有初始化的 RocksDB 列族句柄（ColumnFamilyHandle）智能指针数组。
    std::vector<std::unique_ptr<rocksdb::ColumnFamilyHandle>> _handles;
};

} // namespace doris
