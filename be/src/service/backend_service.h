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

#include <gen_cpp/BackendService.h>

#include <memory>
#include <string>
#include <vector>

#include "agent/agent_server.h"
#include "agent/topic_subscriber.h"
#include "common/status.h"
#include "load/stream_load/stream_load_recorder.h"

namespace doris {

class StorageEngine;
class ExecEnv;
class TAgentResult;
class TAgentTaskRequest;
class TAgentPublishRequest;
class TStreamLoadRecordResult;
class TDiskTrashInfo;
class TCheckStorageFormatResult;
class TRoutineLoadTask;
class TScanBatchResult;
class TScanCloseParams;
class TScanCloseResult;
class TScanNextBatchParams;
class TScanOpenParams;
class TScanOpenResult;
class TSnapshotRequest;
class TStatus;
class TTabletStatResult;
class TUniqueId;
class TIngestBinlogRequest;
class TIngestBinlogResult;
class ThreadPool;

// This class just forward rpc for actual handler
// make this class because we can bind multiple service on single point
// BaseBackendService 是 Apache Doris BE 节点中 Thrift RPC 服务的核心抽象基类（继承自 Thrift 生成的 BackendServiceIf 接口）。
// 其核心作用可以概括为以下几点：
// RPC 请求转发与适配器（RPC Forwarder / Adapter）：
// 它充当 FE（Frontend）或外部客户端与 BE 内部各核心子系统之间的桥梁。它本身通常不直接实现复杂的业务逻辑，而是将接收到的 RPC 请求转发/分发给具体的内部组件（如 AgentServer、存储引擎 StorageEngine、线程池等）。
// 多服务绑定与解耦：
// 如类头部的注释所言（“forward rpc for actual handler make this class because we can bind multiple service on single point”），通过设计抽象基类，可以在单一 Thrift 端口上绑定并组合多种服务逻辑（如本地存算一体架构下的逻辑与云原生/存算分离架构下的逻辑）。
// 架构抽象（存算一体与存算分离/不同引擎的兼容）：
// BaseBackendService 实现了大部分通用的控制流和状态查询接口；而对于高度依赖具体存储引擎实现的方法，声明为虚函数，由派生类（如 BackendService 结合 StorageEngine）进行具体的重写实现。

class BaseBackendService : public BackendServiceIf {
public:
    BaseBackendService(ExecEnv* exec_env);

    ~BaseBackendService() override;

    // Start runtime workers that the thrift server depends on (agent workers,
    // ingest-binlog thread pool, etc.). Must be called before constructing the
    // thrift server. The name makes the side effects explicit: this is not a
    // lightweight preparation hook.
    // 用于启动 Thrift Server 依赖的后台运行 Worker（如 Agent 上报 Worker、Binlog 导入线程池等）。必须在 Thrift Server 启动之前调用。
    virtual Status start_thrift_dependencies() = 0;
    // 接收 FE 下发的批量 Agent 任务（如创建副本、删除副本、Compaction 任务、Schema Change 任务等），直接转发给 _agent_server 处理。
    // Agent service
    void submit_tasks(TAgentResult& return_value,
                      const std::vector<TAgentTaskRequest>& tasks) override {
        _agent_server->submit_tasks(return_value, tasks);
    }
    // 接收并发布集群元数据/状态更新信息（如 FE Master 变更、Cluster ID 变更等）。
    void publish_cluster_state(TAgentResult& result, const TAgentPublishRequest& request) override {
        _agent_server->publish_cluster_state(result, request);
    }
    // 接收来自 FE 的 Topic 订阅广播信息（例如 Routine Load 状态 updates、资源状态 updates 等），交由 _agent_server 的 TopicSubscriber 处理。
    void publish_topic_info(TPublishTopicResult& result,
                            const TPublishTopicRequest& topic_request) override {
        _agent_server->get_topic_subscriber()->handle_topic_info(topic_request);
    }
    // 接收并提交 Kafka 等例行导入（Routine Load）任务，将例行消费任务分配给具体的执行线程。
    void submit_routine_load_task(TStatus& t_status,
                                  const std::vector<TRoutineLoadTask>& tasks) override;

    // used for external service, open means start the scan procedure
    // 外部数据扫描接口 (External Scan Service)
    // 开启数据扫描流程。解析请求中的 Query/Tablet 参数，初始化 Scanner 上下文并返回 scanner_id。
    void open_scanner(TScanOpenResult& result_, const TScanOpenParams& params) override;

    // used for external service, external use getNext to fetch data batch after batch until eos = true
    // 迭代获取数据批次（Batch）。客户端通过 scanner_id 循环调用此接口拉取数据，直到 eos（End of Stream）为 true。
    void get_next(TScanBatchResult& result_, const TScanNextBatchParams& params) override;

    // used for external service, close some context and release resource related with this context
    // 关闭 Scanner，释放对应的内存、句柄和扫描上下文资源。
    void close_scanner(TScanCloseResult& result_, const TScanCloseParams& params) override;

    ////////////////////////////////////////////////////////////////////////////
    // begin local backend functions
    ////////////////////////////////////////////////////////////////////////////
    // 本地后台/存储层方法 (Local Backend Functions)
    // 获取当前 BE 节点上所有 Tablet 的统计信息（如数据行数、磁盘占用字节数等），用于 FE 衡量负载和容量。
    void get_tablet_stat(TTabletStatResult& result) override;
    // 获取当前 BE 节点回收站（Trash 目录，保存被删除但尚未物理清理的数据文件）占用的磁盘总空间（字节）。
    int64_t get_trash_used_capacity() override;
    // 获取指定时间戳（last_stream_record_time）之后的 Stream Load 导入历史记录（包含导入状态、导入数据量等），用于监控和审计。
    void get_stream_load_record(TStreamLoadRecordResult& result,
                                int64_t last_stream_record_time) override;
    // 分路径/分磁盘（Disk）获取各个存储路径下垃圾回收站占用的空间详情。
    void get_disk_trash_used_capacity(std::vector<TDiskTrashInfo>& diskTrashInfos) override;
    // 对指定的 Tablet/Rowset 创建物理快照（Snapshot），常用于备份恢复、跨集群数据迁移或副本修复。
    void make_snapshot(TAgentResult& return_value,
                       const TSnapshotRequest& snapshot_request) override;
    // 释放/清理之前创建的物理快照文件，防止快照长期占用磁盘空间。
    void release_snapshot(TAgentResult& return_value, const std::string& snapshot_path) override;
    // 检查并返回当前 BE 节点的存储格式状态（如检查是否存在旧版的 V1/Row-based 存储格式，确定是否已全部升级为 Segment V2 格式）。
    void check_storage_format(TCheckStorageFormatResult& result) override;
    // 接收并导入 Binlog 数据（主要用于主备集群同步/CCR 跨集群复制），将远端 Binlog 写入本地存储。
    void ingest_binlog(TIngestBinlogResult& result, const TIngestBinlogRequest& request) override;
    // 查询 Binlog 导入任务的执行状态与进度。
    void query_ingest_binlog(TQueryIngestBinlogResult& result,
                             const TQueryIngestBinlogRequest& request) override;
    // 获取当前 BE 上正在执行的 Query/Fragment 的实时执行状态（如 Profile、算子内存占用等），用于实时监控与排查慢查询。
    void get_realtime_exec_status(TGetRealtimeExecStatusResponse& response,
                                  const TGetRealtimeExecStatusRequest& request) override;
    // 根据传入的字典 ID 列表，获取数据字典（Dictionary）在当前 BE 上的加载状态和缓存占用。
    void get_dictionary_status(TDictionaryStatusList& result,
                               const std::vector<int64_t>& dictionary_id) override;
    // 测试 BE 与远端存储系统（如 S3、HDFS、对象存储）的连通性，常用于创建 Resource 或外表前的网络/鉴权校验。
    void test_storage_connectivity(TTestStorageConnectivityResponse& response,
                                   const TTestStorageConnectivityRequest& request) override;
    // 获取当前 BE 节点上安装/配置的 Python 运行环境信息（用于 Python UDF 部署与校验）。
    void get_python_envs(std::vector<TPythonEnvInfo>& result) override;
    // 获取指定 Python 版本下已安装的三方包/依赖库列表。
    void get_python_packages(std::vector<TPythonPackageInfo>& result,
                             const std::string& python_version) override;

    ////////////////////////////////////////////////////////////////////////////
    // begin cloud backend functions
    ////////////////////////////////////////////////////////////////////////////
    // 云原生/存算分离架构扩展方法 (Cloud Backend Functions)
    // 主要用于 Doris 云原生/存算分离模式（Cloud Mode），处理冷热数据分离、缓存预热等操作：
    // 异步触发数据缓存预热（Warm Up），将远端对象存储（S3/HDFS）上的数据/索引异步加载到 BE 本地 NVMe/SSD 缓存盘中。
    void warm_up_cache_async(TWarmUpCacheAsyncResponse& response,
                             const TWarmUpCacheAsyncRequest& request) override;
    // 查询异步缓存预热任务的进度与完成状态。
    void check_warm_up_cache_async(TCheckWarmUpCacheAsyncResponse& response,
                                   const TCheckWarmUpCacheAsyncRequest& request) override;

    // If another cluster load, FE need to notify the cluster to sync the load data
    // 当其他计算 Cluster 完成导入后，通知当前 Cluster 同步并更新指定 Tablet 的元数据与数据缓存。
    void sync_load_for_tablets(TSyncLoadForTabletsResponse& response,
                               const TSyncLoadForTabletsRequest& request) override;
    // 统计并获取当前节点访问频次最高的前 N 个热点分区（Top-N Hot Partitions），用于指导数据缓存淘汰策略或弹性扩缩容。
    void get_top_n_hot_partitions(TGetTopNHotPartitionsResponse& response,
                                  const TGetTopNHotPartitionsRequest& request) override;
    // 针对特定的 Tablet 集合发起同步/批量预热指令，提升后续查询命中本地缓存的概率。
    void warm_up_tablets(TWarmUpTabletsResponse& response,
                         const TWarmUpTabletsRequest& request) override;

    // 停止 Agent 服务中的心跳与状态汇报 Worker 线程（stop_report_workers），在 BE 节点关机或停止服务时调用。
    void stop_works() { _agent_server->stop_report_workers(); }

protected:
    void get_stream_load_record(TStreamLoadRecordResult& result, int64_t last_stream_record_time,
                                std::shared_ptr<StreamLoadRecorder> stream_load_recorder);
    // 指向 BE 执行环境上下文（Execution Environment）的指针。包含 BE 节点运行所需的全局单例对象（如内存池、Fragment 管理器、DataStream 接收器管理器等）。
    ExecEnv* _exec_env = nullptr;
    // 管理 Agent 服务组件。用于接收并处理来自 FE 的集群管理任务（如建表、Drop 表、Publish Version、Report 汇报状态等）以及订阅主题。
    std::unique_ptr<AgentServer> _agent_server;
    // 用于异步/并行处理 Binlog 导入（Ingest Binlog）任务的专用线程池，避免阻塞 RPC 主线程。
    std::unique_ptr<ThreadPool> _ingest_binlog_workers;
};

// `StorageEngine` mixin for `BaseBackendService`
// 存算一体（Shared-Nothing）模式下 BE 节点存储层 RPC 服务的最终落地实现
// 组合与具体实现（Mixin Design pattern）：
// BaseBackendService 声明并实现了与具体的本地存储细节解耦的通用控制逻辑（如 Scanner 管理、Agent 逻辑等）。而 BackendService 作为其具体子类，通过将本地存储引擎 StorageEngine 注入进来，补全了所有依赖具体存储引擎的具体 RPC 方法。
// 连接 RPC 接口与底层 StorageEngine：
// 作为 Thrift RPC 服务的真正执行主体，负责接收来自 FE 的存储级指令（如副本快照、垃圾回收查询、副本统计信息汇总、Binlog 导入等），然后调用底层 _engine（StorageEngine）对应的方法来完成物理磁盘与 Tablet 的具体操作。
class BackendService final : public BaseBackendService {
public:
    BackendService(StorageEngine& engine, ExecEnv* exec_env);

    ~BackendService() override;

    Status start_thrift_dependencies() override;

    void get_tablet_stat(TTabletStatResult& result) override;

    int64_t get_trash_used_capacity() override;

    void get_stream_load_record(TStreamLoadRecordResult& result,
                                int64_t last_stream_record_time) override;

    void get_disk_trash_used_capacity(std::vector<TDiskTrashInfo>& diskTrashInfos) override;

    void make_snapshot(TAgentResult& return_value,
                       const TSnapshotRequest& snapshot_request) override;

    void release_snapshot(TAgentResult& return_value, const std::string& snapshot_path) override;

    void check_storage_format(TCheckStorageFormatResult& result) override;

    void ingest_binlog(TIngestBinlogResult& result, const TIngestBinlogRequest& request) override;

    void query_ingest_binlog(TQueryIngestBinlogResult& result,
                             const TQueryIngestBinlogRequest& request) override;

private:
    // 引用 BE 的本地存储引擎实例（如 Segment/Tablet 管理器）。BackendService 派生类利用它来实现具体的存储层 RPC 操作（如快照、垃圾回收查询、Tablet 统计等）。
    StorageEngine& _engine;
};

} // namespace doris
