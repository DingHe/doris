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

#include <stdint.h>

#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "load/routine_load/data_consumer_pool.h"
#include "util/threadpool.h"
#include "util/uid_util.h"

namespace doris {

class ExecEnv;
class Status;
class StreamLoadContext;
class TRoutineLoadTask;
class PIntegerPair;
class PKafkaMetaProxyRequest;
class PKinesisMetaProxyRequest;

// A routine load task executor will receive routine load
// tasks from FE, put it to a fixed thread pool.
// The thread pool will process each task and report the result
// to FE finally.
//  负责接收、管理和并发执行来自 FE 的例行导入任务（Routine Load Task）的核心组件
// 核心职责包括：
//任务接收与线程池分发：接收 FE 端下发的例行导入任务（TRoutineLoadTask），并将其放入内部的固定线程池（ThreadPool）中异步并发执行。
// 元数据代理查询（Metadata Proxy）：为 FE 提供与流式数据源（如 Kafka Partition 元数据、最新 Offset、时间戳 Offset、Kinesis Shard 元数据等）交互的代理查询接口。
// 内存与资源管控：管理导入过程中的内存限制，并在任务执行完成后向 FE 异步汇报执行结果。
class RoutineLoadTaskExecutor {
public:
    using ExecFinishCallback = std::function<void(std::shared_ptr<StreamLoadContext>)>;

    RoutineLoadTaskExecutor(ExecEnv* exec_env);

    ~RoutineLoadTaskExecutor();
    // 初始化方法。根据传入的进程内存限制初始化 _load_mem_limit，并创建内部用于执行例行导入任务的固定线程池。
    Status init(int64_t process_mem_limit);
    // 停止方法。安全关闭线程池，清理正在运行的任务和相关连接。
    void stop();

    // submit a routine load task
    // 提交任务的核心接口。接收来自 FE 的 Thrift 任务结构体 TRoutineLoadTask，将其包装并投递到内部线程池中异步执行。
    Status submit_task(const TRoutineLoadTask& task);
    // Kafka 元数据代理接口。根据请求获取指定 Kafka Topic 的 Partition 列表（partition_ids）。
    Status get_kafka_partition_meta(const PKafkaMetaProxyRequest& request,
                                    std::vector<int32_t>* partition_ids);
    // Kafka 时间戳查询接口。根据给定的时间戳列表，查询并返回 Kafka 分区在对应时间点的 Offset 偏移量。
    Status get_kafka_partition_offsets_for_times(const PKafkaMetaProxyRequest& request,
                                                 std::vector<PIntegerPair>* partition_offsets,
                                                 int timeout);
    // Kafka 最新 Offset 查询接口。获取指定分区当前的最新消费位置（High-watermark）。
    Status get_kafka_latest_offsets_for_partitions(const PKafkaMetaProxyRequest& request,
                                                   std::vector<PIntegerPair>* partition_offsets,
                                                   int timeout);
    // Kafka 实际 Offset 查询接口。用于获取分区的实际消费偏移量。
    Status get_kafka_real_offsets_for_partitions(const PKafkaMetaProxyRequest& request,
                                                 std::vector<PIntegerPair>* partition_offsets,
                                                 int timeout);
    // Kinesis 元数据代理接口。用于获取 AWS Kinesis 数据源的 Shard（分片）ID 列表。
    Status get_kinesis_shard_meta(const PKinesisMetaProxyRequest& request,
                                  std::vector<std::string>* shard_ids);
    // 返回内部线程池的引用，供外部或测试使用。
    ThreadPool& get_thread_pool() { return *_thread_pool; }

private:
    // execute the task
    // 内部实际执行任务的方法。通过数据消费者池从流式源拉取数据，执行 Stream Load 导入计划，并在完成后通过回调函数 cb 将结果上报。
    void exec_task(std::shared_ptr<StreamLoadContext> ctx, DataConsumerPool* pool,
                   ExecFinishCallback cb);
    // 错误处理函数。当任务执行失败时被调用，记录错误状态与日志，并触发失败回调。
    void err_handler(std::shared_ptr<StreamLoadContext> ctx, const Status& st,
                     const std::string& err_msg);

    // for test only
    Status _execute_plan_for_test(std::shared_ptr<StreamLoadContext> ctx);
    // create a dummy StreamLoadContext for PKafkaMetaProxyRequest
    Status _prepare_ctx(const PKafkaMetaProxyRequest& request,
                        std::shared_ptr<StreamLoadContext> ctx);

    // create a dummy StreamLoadContext for PKinesisMetaProxyRequest
    // 为 Kafka 元数据代理请求准备一个虚拟的（Dummy）StreamLoadContext 上下文。
    Status _prepare_ctx(const PKinesisMetaProxyRequest& request,
                        std::shared_ptr<StreamLoadContext> ctx);
    // 内存检查方法。判断当前导入任务是否超出了 _load_mem_limit 限制，若超出则将原因记录在 reason 中并返回 true。
    bool _reach_memory_limit(std::string& reason);

private:
    // 指向 Doris 后端执行环境（Execution Environment）的全局指针，提供对内存、RPC、存取路径等系统级资源的访问。
    ExecEnv* _exec_env = nullptr;
    // 智能指针管理的内部固定大小线程池。专门用于并发异步执行例行导入任务，防止阻塞 RPC 接收线程。
    std::unique_ptr<ThreadPool> _thread_pool;
    // 数据消费者连接池。用于缓存和管理与外部流式数据源（如 Kafka 消费客户端）的网络连接与消费者实例，提升消费性能。
    DataConsumerPool _data_consumer_pool;
    // 用于保护 _task_map 等内部共享数据结构的线程安全。
    std::mutex _lock;
    // task id -> load context
    // 任务运行状态映射表（Key: 任务唯一 ID UniqueId，Value: 导入上下文 StreamLoadContext），用于追踪当前 BE 上所有正在运行的例行导入任务上下文。
    std::unordered_map<UniqueId, std::shared_ptr<StreamLoadContext>> _task_map;
    // 例行导入任务在当前 BE 节点上允许使用的最大内存限制（字节数）。
    int64_t _load_mem_limit = -1;
};

} // namespace doris
