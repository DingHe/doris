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

#include <memory>
#include <mutex>
#include <string>

#include "common/metrics/metrics.h"
#include "service/http/http_handler.h"

namespace doris {

class ExecEnv;
class Status;
class StreamLoadContext;
class HttpRequest;

// StreamLoadAction 继承自 HttpHandler，主要作用是接收、解析、控制并响应客户端发起的 HTTP Stream Load 数据导入请求。
// 在 Apache Doris 的数据导入体系中，Stream Load 允许用户通过 HTTP 协议直接将本地文件或流式数据导入到 Doris 集中存储表中。StreamLoadAction 在 BE 端扮演着 HTTP 接入入口的角色，其核心职责包括：
// HTTP 请求生命周期管理：利用事件驱动/流式读取（Chunked / Progressive Read）机制，接收 HTTP 请求头和请求体数据。
// 鉴权与参数解析：提取 HTTP Header 中的认证信息（如 Basic Auth）、目标数据库/表名、导入格式（CSV, JSON 等）、过滤条件（Where）等配置，并封装进 StreamLoadContext（流导入上下文）。
// 元数据与路由校验：向 FE（Frontend）提交请求或校验事务，确认导入的目标 Pipe / Channel，或者判断是否可以走 Group Commit（组合提交）模式。
// 数据传输与下发：将通过 HTTP Chunk 分批到达的数据写入 BE 内存缓冲、WAL（Write-Ahead Log，如 Group Commit 场景）或下发给具体的数据接收 Pipeline/Fragment 引擎进行写盘。
class StreamLoadAction : public HttpHandler {
public:
    StreamLoadAction(ExecEnv* exec_env);
    ~StreamLoadAction() override;
    // HTTP 请求的总入口处理函数
    // 当 HTTP 请求完全接收完成或达到可处理状态时（对于流式请求，通常在 on_chunk_data 处理完毕后），Web 服务器框架会调用此方法。它会获取绑定在 HttpRequest 上的 StreamLoadContext 上下文，调用内部逻辑 _handle() 执行收尾、事务提交、生成 HTTP Response 并通过 _send_reply() 发送给客户端。
    void handle(HttpRequest* req) override;
    // 告知 HTTP Server 本 Handler 是否采用“渐进式/流式”读取请求体。
    // 返回 true。这意味着底层的 Web 服务器（如 Libevent / Netty 等封装层）在接收 HTTP 请求时，不会等待全部 Body 传输完毕才开始回调，而是会在收到 Header 以及每收到一部分数据 Chunk 时就立即回调 on_header 和 on_chunk_data。
    // 这对于处理数 GB 级甚至巨大的数据流导入至关重要，避免了 BE 内存爆满。
    bool request_will_be_read_progressively() override { return true; }
    // HTTP 请求头解析完成时的回调函数。
    // 当底层 HTTP 解析器读完客户端发送的所有 HTTP Header 后自动触发。
    int on_header(HttpRequest* req) override;
    // HTTP Body Chunk 数据到达时的回调函数。
    // 在流式传输过程中，客户端每发送一段数据块（Data Chunk），底层网络库就会回调一次该方法。它从 req 中读取当前 Chunk 的数据内容（如 ByteBuffer / Slice），并将数据追加写入到 StreamLoadContext 绑定的数据管道（如 ByteBuffer 队列或 StreamLoadPipe）中。如果使用了 Group Commit WAL，还会把数据写入 WAL 存储。
    void on_chunk_data(HttpRequest* req) override;
    // 清理与 HTTP 请求绑定的自定义上下文（Context）。
    void free_handler_ctx(std::shared_ptr<void> ctx) override;

private:
    // on_header 的具体逻辑实现。
    Status _on_header(HttpRequest* http_req, std::shared_ptr<StreamLoadContext> ctx);
    // 处理数据传输完成后的核心控制逻辑。
    // 在数据全部接收完毕后被 handle() 调用。负责向 FE 汇报导入状态（如 Commit Transaction 或 Abort Transaction）、获取导入结果统计信息（读取行数、过滤行数、加载字节数等），并调用 _on_finish() 准备最终回复。
    Status _handle(std::shared_ptr<StreamLoadContext> ctx, HttpRequest* req);
    // 确定导入数据的本地临时存储路径（如果需要落盘）。
    // 某些特殊导入场景（如需要先暂存本地文件或落盘处理）下，根据请求信息生成在 BE 本地磁盘上的文件保存路径，并检查磁盘剩余空间是否满足 file_bytes 的需求。
    Status _data_saved_path(HttpRequest* req, std::string* file_path, int64_t file_bytes);
    // 处理 HTTP PUT 请求模式的准备逻辑。
    // Stream Load 标准接口通常使用 HTTP PUT 方法。该函数负责与 FE 通信（或在 BE 本地）开启事务（Begin Transaction）、分配 Label，并向 FE 申请 Plan（即数据导入的执行计划及目标 Fragment 调度路径）。
    Status _process_put(HttpRequest* http_req, std::shared_ptr<StreamLoadContext> ctx);
    // 判断当前 Stream Load 是否符合 Group Commit（组合提交）模式的条件。
    Status _can_group_commit(HttpRequest* http_req, std::shared_ptr<StreamLoadContext> ctx,
                             std::string& group_commit_header, bool& can_group_commit);
    // 持久化保存 Stream Load 的历史操作记录。
    // 将导入任务的执行结果摘要（如 Label、状态、错误信息链接 url、耗时等）写入 BE 的 RocksDB 或内存日志库，以便用户之后查询 show load warnings 或追溯 Stream Load 历史。
    void _save_stream_load_record(std::shared_ptr<StreamLoadContext> ctx, const std::string& str);
    // 检查 WAL（预写日志）存储空间。
    // 在 Group Commit 模式下，接收到的数据会先写入 BE 本地的 WAL 文件。该方法依据请求头中的 Content-Length（数据总字节数），检查本地 WAL 盘是否有足够的剩余磁盘空间。若空间不足，则拒绝导入以防止磁盘被撑爆。
    Status _check_wal_space(const std::string& group_commit_mode, int64_t content_length);
    // 针对 Group Commit 模式的专项处理流程。
    Status _handle_group_commit(HttpRequest* http_req, std::shared_ptr<StreamLoadContext> ctx);
    // 导入任务完成时的收尾回调。
    void _on_finish(std::shared_ptr<StreamLoadContext> ctx, HttpRequest* req);
    // 向 HTTP 客户端发送响应回复。
    // 设置 HTTP 响应头（如 Content-Type: application/json 和状态码 200 OK / 500 Error），将 _on_finish() 生成的 JSON 结果体写入 HTTP 响应管道并发送给客户端，随后关闭或复用 HTTP 连接。
    void _send_reply(std::shared_ptr<StreamLoadContext> ctx, HttpRequest* req);

private:
    // BE 执行环境全局指针。通过该指针可以获取 BE 的各类全局服务及组件，如 Fragment 调度器（FragmentMgr）、元数据/配置、集群连接池、StorageEngine 等。
    ExecEnv* _exec_env;
    // Stream Load 相关的 Metric 实体。用于注册和管理与流式导入相关的 Prometheus 监控指标集合。
    std::shared_ptr<MetricEntity> _stream_load_entity;
    // 流式导入请求总数计数器。统计累计收到的 Stream Load 请求次数（Prometheus 递增 Counter 指标）。
    IntCounter* streaming_load_requests_total;
    // 流式导入累计耗时（毫秒）计数器。累加所有 Stream Load 请求处理的总时长，用于计算平均响应时间或吞吐量。
    IntCounter* streaming_load_duration_ms;
    // 当前正在处理的流式导入请求数 Gauge 仪表。请求开始时 +1，处理完成（无论成功与否）时 -1，反映 BE 当前的并发 Stream Load 负载情况。
    IntGauge* streaming_load_current_processing;
};

} // namespace doris
