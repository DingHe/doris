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

#include "service/http/action/stream_load.h"

// use string iequal
#include <event2/buffer.h>
#include <event2/http.h>
#include <gen_cpp/FrontendService.h>
#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/HeartbeatService_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <sys/time.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <functional>
#include <future>
#include <sstream>
#include <stdexcept>
#include <utility>

#include "cloud/config.h"
#include "common/config.h"
#include "common/consts.h"
#include "common/logging.h"
#include "common/metrics/doris_metrics.h"
#include "common/metrics/metrics.h"
#include "common/status.h"
#include "common/utils.h"
#include "io/fs/stream_load_pipe.h"
#include "load/group_commit/group_commit_mgr.h"
#include "load/load_path_mgr.h"
#include "load/message_body_sink.h"
#include "load/stream_load/new_load_stream_mgr.h"
#include "load/stream_load/stream_load_context.h"
#include "load/stream_load/stream_load_executor.h"
#include "load/stream_load/stream_load_recorder.h"
#include "runtime/cluster_info.h"
#include "runtime/exec_env.h"
#include "service/http/action/action_constants.h"
#include "service/http/http_channel.h"
#include "service/http/http_common.h"
#include "service/http/http_headers.h"
#include "service/http/http_request.h"
#include "service/http/utils.h"
#include "storage/storage_engine.h"
#include "util/byte_buffer.h"
#include "util/client_cache.h"
#include "util/load_util.h"
#include "util/string_util.h"
#include "util/thrift_rpc_helper.h"
#include "util/time.h"
#include "util/uid_util.h"
#include "util/url_coding.h"

namespace doris {
using namespace ErrorCode;

DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(streaming_load_requests_total, MetricUnit::REQUESTS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(streaming_load_duration_ms, MetricUnit::MILLISECONDS);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(streaming_load_current_processing, MetricUnit::REQUESTS);

bvar::LatencyRecorder g_stream_load_receive_data_latency_ms("stream_load_receive_data_latency_ms");
bvar::LatencyRecorder g_stream_load_commit_and_publish_latency_ms("stream_load",
                                                                  "commit_and_publish_ms");

static constexpr size_t MIN_CHUNK_SIZE = 64 * 1024;
static const std::string CHUNK = "chunked";
static const std::string OFF_MODE = "off_mode";
static const std::string SYNC_MODE = "sync_mode";
static const std::string ASYNC_MODE = "async_mode";

#ifdef BE_TEST
TStreamLoadPutResult k_stream_load_put_result;
#endif

StreamLoadAction::StreamLoadAction(ExecEnv* exec_env) : _exec_env(exec_env) {
    // Stream load forwards the parsed HTTP credentials to FE load RPCs, where LOAD
    // privilege is checked against the actual db/table/txn. A generic BE HTTP
    // pre-check cannot model every stream-load variant and would duplicate that
    // resource-scoped authorization.
    _stream_load_entity =
            DorisMetrics::instance()->metric_registry()->register_entity("stream_load");
    INT_COUNTER_METRIC_REGISTER(_stream_load_entity, streaming_load_requests_total);
    INT_COUNTER_METRIC_REGISTER(_stream_load_entity, streaming_load_duration_ms);
    INT_GAUGE_METRIC_REGISTER(_stream_load_entity, streaming_load_current_processing);
}

StreamLoadAction::~StreamLoadAction() {
    DorisMetrics::instance()->metric_registry()->deregister_entity(_stream_load_entity);
}

void StreamLoadAction::handle(HttpRequest* req) {
    std::shared_ptr<StreamLoadContext> ctx =
            std::static_pointer_cast<StreamLoadContext>(req->handler_ctx());
    if (ctx == nullptr) {
        return;
    }

    {
        std::unique_lock<std::mutex> lock1(ctx->_send_reply_lock);
        ctx->_can_send_reply = true;
        ctx->_can_send_reply_cv.notify_all();
    }

    // status already set to fail
    if (ctx->status.ok()) {
        ctx->status = _handle(ctx, req);
        if (!ctx->status.ok() && !ctx->status.is<PUBLISH_TIMEOUT>()) {
            _send_reply(ctx, req);
        }
    }
}

Status StreamLoadAction::_handle(std::shared_ptr<StreamLoadContext> ctx, HttpRequest* req) {
    if (ctx->body_bytes > 0 && ctx->receive_bytes != ctx->body_bytes) {
        LOG(WARNING) << "recevie body don't equal with body bytes, body_bytes=" << ctx->body_bytes
                     << ", receive_bytes=" << ctx->receive_bytes << ", id=" << ctx->id;
        return Status::Error<ErrorCode::NETWORK_ERROR>("receive body don't equal with body bytes");
    }

    // if we use non-streaming, MessageBodyFileSink.finish will close the file
    RETURN_IF_ERROR(ctx->body_sink->finish());
    if (!ctx->use_streaming) {
        // we need to close file first, then execute_plan_fragment here
        ctx->body_sink.reset();
        TPipelineFragmentParamsList mocked;
        RETURN_IF_ERROR(_exec_env->stream_load_executor()->execute_plan_fragment(
                ctx, mocked,
                [req, this](std::shared_ptr<StreamLoadContext> ctx) { _on_finish(ctx, req); }));
    }

    return Status::OK();
}

void StreamLoadAction::_on_finish(std::shared_ptr<StreamLoadContext> ctx, HttpRequest* req) {
    ctx->status = ctx->load_status_future.get();
    if (ctx->status.ok()) {
        if (ctx->group_commit) {
            LOG(INFO) << "skip commit because this is group commit, pipe_id="
                      << ctx->id.to_string();
        } else if (ctx->two_phase_commit) {
            int64_t pre_commit_start_time = MonotonicNanos();
            ctx->status = _exec_env->stream_load_executor()->pre_commit_txn(ctx.get());
            ctx->pre_commit_txn_cost_nanos = MonotonicNanos() - pre_commit_start_time;
        } else {
            // If put file success we need commit this load
            int64_t commit_and_publish_start_time = MonotonicNanos();
            ctx->status = _exec_env->stream_load_executor()->commit_txn(ctx.get());
            ctx->commit_and_publish_txn_cost_nanos =
                    MonotonicNanos() - commit_and_publish_start_time;
            g_stream_load_commit_and_publish_latency_ms
                    << ctx->commit_and_publish_txn_cost_nanos / 1000000;
        }
    }
    _send_reply(ctx, req);
}

void StreamLoadAction::_send_reply(std::shared_ptr<StreamLoadContext> ctx, HttpRequest* req) {
    std::unique_lock<std::mutex> lock1(ctx->_send_reply_lock);
    // 1. _can_send_reply: ensure `send_reply` is invoked only after on_header/handle complete,
    //    avoid client errors (e.g., broken pipe).
    // 2. _finish_send_reply: Prevent duplicate reply sending; skip reply if HTTP request is canceled
    //    due to long import execution time.
    while (!ctx->_finish_send_reply && !ctx->_can_send_reply) {
        ctx->_can_send_reply_cv.wait(lock1);
    }
    if (ctx->_finish_send_reply) {
        return;
    }
    DCHECK(ctx->_can_send_reply);
    ctx->_finish_send_reply = true;
    ctx->_can_send_reply_cv.notify_all();
    ctx->load_cost_millis = UnixMillis() - ctx->start_millis;

    if (!ctx->status.ok() && !ctx->status.is<PUBLISH_TIMEOUT>()) {
        LOG(WARNING) << "handle streaming load failed, id=" << ctx->id
                     << ", errmsg=" << ctx->status;
        if (ctx->need_rollback) {
            _exec_env->stream_load_executor()->rollback_txn(ctx.get());
            ctx->need_rollback = false;
        }
        if (ctx->body_sink != nullptr) {
            ctx->body_sink->cancel(ctx->status.to_string());
        }
    }

    auto str = ctx->to_json();
    // add new line at end
    str = str + '\n';

#ifndef BE_TEST
    if (config::enable_stream_load_record || config::enable_stream_load_record_to_audit_log_table) {
        if (req->header(HTTP_SKIP_RECORD_TO_AUDIT_LOG_TABLE).empty()) {
            str = ctx->prepare_stream_load_record(str);
            _save_stream_load_record(ctx, str);
        }
    }
#endif

    HttpChannel::send_reply(req, str);

    LOG(INFO) << "finished to execute stream load. label=" << ctx->label
              << ", txn_id=" << ctx->txn_id << ", query_id=" << ctx->id
              << ", load_cost_ms=" << ctx->load_cost_millis << ", receive_data_cost_ms="
              << (ctx->receive_and_read_data_cost_nanos - ctx->read_data_cost_nanos) / 1000000
              << ", read_data_cost_ms=" << ctx->read_data_cost_nanos / 1000000
              << ", write_data_cost_ms=" << ctx->write_data_cost_nanos / 1000000
              << ", commit_and_publish_txn_cost_ms="
              << ctx->commit_and_publish_txn_cost_nanos / 1000000
              << ", number_total_rows=" << ctx->number_total_rows
              << ", number_loaded_rows=" << ctx->number_loaded_rows
              << ", receive_bytes=" << ctx->receive_bytes << ", loaded_bytes=" << ctx->loaded_bytes
              << ", error_url=" << ctx->error_url;

    // update statistics
    streaming_load_requests_total->increment(1);
    streaming_load_duration_ms->increment(ctx->load_cost_millis);
    if (!ctx->data_saved_path.empty()) {
        _exec_env->load_path_mgr()->clean_tmp_files(ctx->data_saved_path);
    }
}

int StreamLoadAction::on_header(HttpRequest* req) {
    // 通知底层 HTTP 框架（如 Libevent / evhttp 封装层），
    // 当前请求将在合适的时机由 StreamLoadAction 显式接管并发送 HTTP 响应（Reply），防止网络框架在方法返回时自动关闭连接或提前发送默认响应。
    req->mark_send_reply();
    // 将 Prometheus 监控指标中的当前活跃流式导入计数器（streaming_load_current_processing）加 1，用于实时追踪 BE 端正在并发处理的 Stream Load 请求数量。
    streaming_load_current_processing->increment(1);
    // 创建并挂载导入上下文对象
    // 实例化一个 StreamLoadContext（流导入上下文）智能指针，传入全局执行环境 _exec_env。该上下文对象会贯穿整个 Stream Load 生命周期的每一个环节（Header 解析、Chunk 接收、Plan 执行、事务提交等）
    std::shared_ptr<StreamLoadContext> ctx = std::make_shared<StreamLoadContext>(_exec_env);
    req->set_handler_ctx(ctx);
    // 标记导入类型为手动/常规导入
    ctx->load_type = TLoadType::MANUL_LOAD;
    // 标记数据源类型为原始文本/二进制数据流（非 Kafka 等 Routine Load 模式）。
    ctx->load_src_type = TLoadSourceType::RAW;
    // 解析数据库名、表名与基础 Header 属性
    url_decode(req->param(HTTP_DB_KEY), &ctx->db);
    url_decode(req->param(HTTP_TABLE_KEY), &ctx->table);
    ctx->label = req->header(HTTP_LABEL_KEY);
    // 判断 Header 中是否开启了两阶段提交（two_phase_commit: true），并将结果保存为布尔值。
    ctx->two_phase_commit = req->header(HTTP_TWO_PHASE_COMMIT) == "true";
    // 校验并决定当前请求是否走 Group Commit（组合提交，用于高并发小批量数据写入优化）。如果符合条件，会在此处进行 WAL 磁盘空间检查并准备 Group Commit 相关管道。其状态存入 st
    Status st = _handle_group_commit(req, ctx);
    // 如果不是 Group Commit 模式，且用户在 HTTP Header 中没有显式指定 label，系统会自动生成一个 UUID 字符串作为该导入任务的默认 Label，确保每个 Stream Load 事务的 Label 都是唯一且非空的。
    if (!ctx->group_commit && ctx->label.empty()) {
        ctx->label = generate_uuid_string();
    }
    // 日志记录与计时起始
    LOG(INFO) << "new income streaming load request." << ctx->brief() << ", db=" << ctx->db
              << ", tbl=" << ctx->table << ", group_commit=" << ctx->group_commit
              << ", group_commit_mode=" << ctx->group_commit_mode
              << ", HTTP headers=" << req->get_all_headers();
    ctx->begin_receive_and_read_data_cost_nanos = MonotonicNanos();
    // 执行深度 Header 解析与业务初始化
    if (st.ok()) {
        st = _on_header(req, ctx);
        LOG(INFO) << "finished to handle HTTP header, " << ctx->brief();
    }
    if (!st.ok()) {
        // 将失败的状态码/错误信息（st）转移赋值给上下文的 ctx->status
        ctx->status = std::move(st);
        {
            std::unique_lock<std::mutex> lock1(ctx->_send_reply_lock);
            ctx->_can_send_reply = true;
            ctx->_can_send_reply_cv.notify_all();
        }
        _send_reply(ctx, req);
        return -1;
    }
    return 0;
}

Status StreamLoadAction::_on_header(HttpRequest* http_req, std::shared_ptr<StreamLoadContext> ctx) {
    // auth information
    // 1. HTTP Basic Auth 认证解析
    if (!parse_basic_auth(*http_req, &ctx->auth)) {
        LOG(WARNING) << "parse basic authorization failed." << ctx->brief();
        return Status::NotAuthorized("no valid Basic authorization");
    }

    // get format of this put
    // 解析数据格式与压缩类型
    // 获取请求头中的 format 字段（例如 csv、json、parquet、csv_with_names 等）。
    std::string format_str = http_req->header(HTTP_FORMAT_KEY);
    if (iequal(format_str, BeConsts::CSV_WITH_NAMES) ||
        iequal(format_str, BeConsts::CSV_WITH_NAMES_AND_TYPES)) {
        ctx->header_type = format_str;
        //treat as CSV
        format_str = BeConsts::CSV;
    }
    // 调用工具函数 LoadUtil::parse_format，根据 format_str 和请求头中的 compress_type（如 gzip、bz2、lz4 等），
    // 转译并设置 ctx->format（文件格式枚举）和 ctx->compress_type（压缩类型枚举）
    LoadUtil::parse_format(format_str, http_req->header(HTTP_COMPRESS_TYPE), &ctx->format,
                           &ctx->compress_type);
    if (ctx->format == TFileFormatType::FORMAT_UNKNOWN) {
        return Status::Error<ErrorCode::DATA_FILE_TYPE_ERROR>("unknown data format, format={}",
                                                              http_req->header(HTTP_FORMAT_KEY));
    }

    // check content length
    // 校验请求体大小限制（Content-Length / Body Size）
    ctx->body_bytes = 0;
    // 从 BE 配置项 config::streaming_load_max_mb 读取常规（CSV 等）单次流式导入的最大允许兆字节数（MB），并转换为字节数（csv_max_body_bytes）
    const auto csv_max_body_mb = config::streaming_load_max_mb;
    size_t csv_max_body_bytes = csv_max_body_mb * MEBIBYTE;
    // 从 BE 配置项 config::streaming_load_json_max_mb 读取 JSON 格式单次导入的最大允许兆字节数（MB），并转换为字节数（json_max_body_bytes）
    const auto json_max_body_mb = config::streaming_load_json_max_mb;
    size_t json_max_body_bytes = json_max_body_mb * MEBIBYTE;
    // 检查请求头中是否包含 read_json_by_line。若为 true，说明 JSON 数据按行分割（像 CSV 一样流式处理），可以免受单大对象 JSON 内存限制。
    bool read_json_by_line = false;
    if (!http_req->header(HTTP_READ_JSON_BY_LINE).empty()) {
        if (iequal(http_req->header(HTTP_READ_JSON_BY_LINE), "true")) {
            read_json_by_line = true;
        }
    }
    // 检查 HTTP 请求头中是否传了 Content-Length
    if (!http_req->header(HttpHeaders::CONTENT_LENGTH).empty()) {
        try {
        // 尝试将 Content-Length 字符串转换为长整型 int64_t 并存入 ctx->body_bytes。如果转换抛出异常，返回 InvalidArgument 错误。
            ctx->body_bytes = std::stol(http_req->header(HttpHeaders::CONTENT_LENGTH));
        } catch (const std::exception& e) {
            return Status::InvalidArgument("invalid HTTP header CONTENT_LENGTH={}: {}",
                                           http_req->header(HttpHeaders::CONTENT_LENGTH), e.what());
        }
        // json max body size
        // 若格式为 JSON 且没有开启 read_json_by_line，则判断 Content-Length 是否超过 json_max_body_bytes。如果超过限制，返回 EXCEEDED_LIMIT 错误（提示提示可通过调整 streaming_load_json_max_mb 配置放大额度）。
        if ((ctx->format == TFileFormatType::FORMAT_JSON) &&
            (ctx->body_bytes > json_max_body_bytes) && !read_json_by_line) {
            return Status::Error<ErrorCode::EXCEEDED_LIMIT>(
                    "json body size {} bytes ({:.2f} MiB) exceeds the limit of {} bytes ({} MiB) "
                    "set by BE's conf streaming_load_json_max_mb. Increase it if you are sure "
                    "this load is reasonable",
                    ctx->body_bytes, static_cast<double>(ctx->body_bytes) / MEBIBYTE,
                    json_max_body_bytes, json_max_body_mb);
        }
        // csv max body size
        // 对于非 JSON 或开启了行读取的场景，校验 Content-Length 是否超过了 csv_max_body_bytes（常规最大限制）。超过则打印 Warning 并返回 EXCEEDED_LIMIT 错误。
        else if (ctx->body_bytes > csv_max_body_bytes) {
            LOG(WARNING) << "body exceed max size." << ctx->brief();
            return Status::Error<ErrorCode::EXCEEDED_LIMIT>(
                    "body size {} bytes ({:.2f} MiB) exceeds the limit of {} bytes ({} MiB) set "
                    "by BE's conf streaming_load_max_mb. Increase it if you are sure this load is "
                    "reasonable",
                    ctx->body_bytes, static_cast<double>(ctx->body_bytes) / MEBIBYTE,
                    csv_max_body_bytes, csv_max_body_mb);
        }
    } else {
#ifndef BE_TEST
        evhttp_connection_set_max_body_size(
                evhttp_request_get_connection(http_req->get_evhttp_request()), csv_max_body_bytes);
#endif
    }
    // 4. 传输编码校验（Chunked 协议合规检查）
    // 检查请求头 Transfer-Encoding 中是否包含 chunked 关键字。若包含，将 ctx->is_chunked_transfer 置为 true。
    if (!http_req->header(HttpHeaders::TRANSFER_ENCODING).empty()) {
        if (http_req->header(HttpHeaders::TRANSFER_ENCODING).find(CHUNK) != std::string::npos) {
            ctx->is_chunked_transfer = true;
        }
    }
    // 检查异常组合 1：既没有提供 Content-Length，又没有设置 Transfer-Encoding: chunked。HTTP 无法确定边界，打印 Warning 并返回 InvalidArgument 错误。
    if (UNLIKELY((http_req->header(HttpHeaders::CONTENT_LENGTH).empty() &&
                  !ctx->is_chunked_transfer))) {
        LOG(WARNING) << "content_length is empty and transfer-encoding!=chunked, please set "
                        "content_length or transfer-encoding=chunked";
        return Status::InvalidArgument(
                "content_length is empty and transfer-encoding!=chunked, please set content_length "
                "or transfer-encoding=chunked");
    // 检查异常组合 2：同时设置了 Content-Length 和 Transfer-Encoding: chunked。这违反 HTTP 标准规范，打印 Warning 并返回 InvalidArgument 错误。
    } else if (UNLIKELY(!http_req->header(HttpHeaders::CONTENT_LENGTH).empty() &&
                        ctx->is_chunked_transfer)) {
        LOG(WARNING) << "please do not set both content_length and transfer-encoding";
        return Status::InvalidArgument(
                "please do not set both content_length and transfer-encoding");
    }
    // 5. 解析 Timeout 与 Comment 参数
    if (!http_req->header(HTTP_TIMEOUT).empty()) {
        ctx->timeout_second = DORIS_TRY(safe_stoi(http_req->header(HTTP_TIMEOUT), HTTP_TIMEOUT));
    }
    if (!http_req->header(HTTP_COMMENT).empty()) {
        ctx->load_comment = http_req->header(HTTP_COMMENT);
    }
    // begin transaction
    // 6. 开启导入事务（Begin Transaction）
    if (!ctx->group_commit) {
        int64_t begin_txn_start_time = MonotonicNanos();
        // 通过全局执行环境中的 stream_load_executor 向 FE 发起 RPC，开启一个 Doris 导入事务（生成 Transaction ID 并关联 Label）。若开启失败，利用 RETURN_IF_ERROR 宏直接返回错误。
        RETURN_IF_ERROR(_exec_env->stream_load_executor()->begin_txn(ctx.get()));
        ctx->begin_txn_cost_nanos = MonotonicNanos() - begin_txn_start_time;
        // 降级防线检查。如果向 FE 开启事务的过程中，FE 返回或动态协商判定该请求需要转为 group_commit 模式，则在此处补做 WAL 磁盘空间检查（_check_wal_space）。
        if (ctx->group_commit) {
            RETURN_IF_ERROR(_check_wal_space(ctx->group_commit_mode, ctx->body_bytes));
        }
    }

    // process put file
    // 提交处理 Pipeline（Process Put）
    return _process_put(http_req, ctx);
}

void StreamLoadAction::on_chunk_data(HttpRequest* req) {
    std::shared_ptr<StreamLoadContext> ctx =
            std::static_pointer_cast<StreamLoadContext>(req->handler_ctx());
    if (ctx == nullptr || !ctx->status.ok()) {
        return;
    }

    struct evhttp_request* ev_req = req->get_evhttp_request();
    auto evbuf = evhttp_request_get_input_buffer(ev_req);

    SCOPED_ATTACH_TASK(ExecEnv::GetInstance()->stream_load_pipe_tracker());

    int64_t start_read_data_time = MonotonicNanos();
    while (evbuffer_get_length(evbuf) > 0) {
        ByteBufferPtr bb;
        Status st = ByteBuffer::allocate(128 * 1024, &bb);
        if (!st.ok()) {
            ctx->status = st;
            return;
        }
        auto remove_bytes = evbuffer_remove(evbuf, bb->ptr, bb->capacity);
        bb->pos = remove_bytes;
        bb->flip();
        st = ctx->body_sink->append(bb);
        if (!st.ok()) {
            LOG(WARNING) << "append body content failed. errmsg=" << st << ", " << ctx->brief();
            ctx->status = st;
            return;
        }
        ctx->receive_bytes += remove_bytes;
    }
    int64_t read_data_time = MonotonicNanos() - start_read_data_time;
    int64_t last_receive_and_read_data_cost_nanos = ctx->receive_and_read_data_cost_nanos;
    ctx->read_data_cost_nanos += read_data_time;
    ctx->receive_and_read_data_cost_nanos =
            MonotonicNanos() - ctx->begin_receive_and_read_data_cost_nanos;
    g_stream_load_receive_data_latency_ms
            << (ctx->receive_and_read_data_cost_nanos - last_receive_and_read_data_cost_nanos -
                read_data_time) /
                       1000000;
}

void StreamLoadAction::free_handler_ctx(std::shared_ptr<void> param) {
    std::shared_ptr<StreamLoadContext> ctx = std::static_pointer_cast<StreamLoadContext>(param);
    if (ctx == nullptr) {
        return;
    }
    // sender is gone, make receiver know it
    if (ctx->body_sink != nullptr) {
        ctx->body_sink->cancel("sender is gone");
    }
    // remove stream load context from stream load manager and the resource will be released
    ctx->exec_env()->new_load_stream_mgr()->remove(ctx->id);
    streaming_load_current_processing->increment(-1);
}
// 主要负责接收和解析 HTTP Stream Load 请求参数，将其组装成 Thrift 结构体发送给 Frontend (FE) 进行导入执行计划（Plan）的生成，并根据导入模式（内存管道流式处理还是落盘处理）初始化管道并启动执行。
Status StreamLoadAction::_process_put(HttpRequest* http_req,
                                      std::shared_ptr<StreamLoadContext> ctx) {
    // Now we use stream
    // 1. 判断是否使用流式传输 (Streaming)
    // 根据导入的数据格式（如 CSV, JSON, Parquet, ORC 等），检查该格式是否支持边接收 HTTP Body 边解析的“流式传输”。结果写入 ctx->use_streaming。
    ctx->use_streaming = LoadUtil::is_format_support_streaming(ctx->format);

    // put request
    // 2. 初始化 Thrift 请求并填充基础元数据
    // 创建一个 RPC 请求对象 TStreamLoadPutRequest（发送给 FE），将 ctx 中解析好的认证信息、数据库名、表名、事务 ID (txnId)、数据格式、压缩类型、Header 类型以及全局唯一导入 ID (loadId) 填充进去。
    TStreamLoadPutRequest request;
    set_request_auth(&request, ctx->auth);
    request.db = ctx->db;
    request.tbl = ctx->table;
    request.txnId = ctx->txn_id;
    request.formatType = ctx->format;
    request.__set_compress_type(ctx->compress_type);
    request.__set_header_type(ctx->header_type);
    request.__set_loadId(ctx->id.to_thrift());
    // 3. 分流处理：流式 Pipe 处理 vs 本地文件落盘处理
    if (ctx->use_streaming) {
        std::shared_ptr<io::StreamLoadPipe> pipe;
        if (ctx->is_chunked_transfer) {
            pipe = std::make_shared<io::StreamLoadPipe>(
                    io::kMaxPipeBufferedBytes /* max_buffered_bytes */);
            pipe->set_is_chunked_transfer(true);
        } else {
            pipe = std::make_shared<io::StreamLoadPipe>(
                    io::kMaxPipeBufferedBytes /* max_buffered_bytes */,
                    MIN_CHUNK_SIZE /* min_chunk_size */, ctx->body_bytes /* total_length */);
        }
        request.fileType = TFileType::FILE_STREAM;
        // 将 pipe 绑定到 ctx 上，并将 ctx 注册到 BE 的 NewLoadStreamMgr 管理器中，以便后续 HTTP 读线程写入数据时能找到对应的 Pipe。
        ctx->body_sink = pipe;
        ctx->pipe = pipe;
        RETURN_IF_ERROR(_exec_env->new_load_stream_mgr()->put(ctx->id, ctx));
    } else {
        // 若不支持流式（如部分复杂格式），数据需要先下载并落盘到本地。
        RETURN_IF_ERROR(_data_saved_path(http_req, &request.path, ctx->body_bytes));
        auto file_sink = std::make_shared<MessageBodyFileSink>(request.path);
        RETURN_IF_ERROR(file_sink->open());
        request.__isset.path = true;
        request.fileType = TFileType::FILE_LOCAL;
        request.__set_file_size(ctx->body_bytes);
        ctx->body_sink = file_sink;
        ctx->data_saved_path = request.path;
    }
    // 4. 解析 HTTP Headers 并设置到 RPC Request
    // 提取列映射与转换规则 (columns)、过滤条件 (where)、列分隔符 (column_separator)、行分隔符 (line_delimiter)。
    if (!http_req->header(HTTP_COLUMNS).empty()) {
        request.__set_columns(http_req->header(HTTP_COLUMNS));
    }
    if (!http_req->header(HTTP_WHERE).empty()) {
        request.__set_where(http_req->header(HTTP_WHERE));
    }
    if (!http_req->header(HTTP_COLUMN_SEPARATOR).empty()) {
        request.__set_columnSeparator(http_req->header(HTTP_COLUMN_SEPARATOR));
    }
    if (!http_req->header(HTTP_LINE_DELIMITER).empty()) {
        request.__set_line_delimiter(http_req->header(HTTP_LINE_DELIMITER));
    }
    // 提取包围符 (enclose) 和转义符 (escape)。两者都要求必须是单字符，否则返回 InvalidArgument 报错。
    if (!http_req->header(HTTP_ENCLOSE).empty() && !http_req->header(HTTP_ENCLOSE).empty()) {
        const auto& enclose_str = http_req->header(HTTP_ENCLOSE);
        if (enclose_str.length() != 1) {
            return Status::InvalidArgument("enclose must be single-char, actually is {}",
                                           enclose_str);
        }
        request.__set_enclose(http_req->header(HTTP_ENCLOSE)[0]);
    }
    if (!http_req->header(HTTP_ESCAPE).empty() && !http_req->header(HTTP_ESCAPE).empty()) {
        const auto& escape_str = http_req->header(HTTP_ESCAPE);
        if (escape_str.length() != 1) {
            return Status::InvalidArgument("escape must be single-char, actually is {}",
                                           escape_str);
        }
        request.__set_escape(http_req->header(HTTP_ESCAPE)[0]);
    }
    // 解析目标分区。不允许同时指定普通分区 (partitions) 和临时分区 (temporary_partitions)，校验冲突后设置对应的标志。
    if (!http_req->header(HTTP_PARTITIONS).empty()) {
        request.__set_partitions(http_req->header(HTTP_PARTITIONS));
        request.__set_isTempPartition(false);
        if (!http_req->header(HTTP_TEMP_PARTITIONS).empty()) {
            return Status::InvalidArgument(
                    "Can not specify both partitions and temporary partitions");
        }
    }
    if (!http_req->header(HTTP_TEMP_PARTITIONS).empty()) {
        request.__set_partitions(http_req->header(HTTP_TEMP_PARTITIONS));
        request.__set_isTempPartition(true);
        if (!http_req->header(HTTP_PARTITIONS).empty()) {
            return Status::InvalidArgument(
                    "Can not specify both partitions and temporary partitions");
        }
    }
    // 设置是否开启“负导入”（聚合模型用于做数据抵扣/撤销）
    if (!http_req->header(HTTP_NEGATIVE).empty() && http_req->header(HTTP_NEGATIVE) == "true") {
        request.__set_negative(true);
    } else {
        request.__set_negative(false);
    }
    // 解析严格模式 (strict_mode)。开启后会对列类型转换失败等错误进行更严格的清洗检查。
    bool strictMode = false;
    if (!http_req->header(HTTP_STRICT_MODE).empty()) {
        if (iequal(http_req->header(HTTP_STRICT_MODE), "false")) {
            strictMode = false;
        } else if (iequal(http_req->header(HTTP_STRICT_MODE), "true")) {
            strictMode = true;
        } else {
            return Status::InvalidArgument("Invalid strict mode format. Must be bool type");
        }
        request.__set_strictMode(strictMode);
    }
    // timezone first. if not, try system time_zone
    // 解析时区参数 (timezone 或 time_zone)。
    if (!http_req->header(HTTP_TIMEZONE).empty()) {
        request.__set_timezone(http_req->header(HTTP_TIMEZONE));
    } else if (!http_req->header(HTTP_TIME_ZONE).empty()) {
        request.__set_timezone(http_req->header(HTTP_TIME_ZONE));
    }
    // 解析单个导入任务的执行内存限制 (exec_mem_limit)。
    if (!http_req->header(HTTP_EXEC_MEM_LIMIT).empty()) {
        try {
            request.__set_execMemLimit(std::stoll(http_req->header(HTTP_EXEC_MEM_LIMIT)));
        } catch (const std::invalid_argument& e) {
            return Status::InvalidArgument("Invalid mem limit format, {}", e.what());
        }
    }
    // 解析 JSON 格式导入专属的 jsonpaths（解析路径）和 json_root（根节点）。
    if (!http_req->header(HTTP_JSONPATHS).empty()) {
        request.__set_jsonpaths(http_req->header(HTTP_JSONPATHS));
    }
    if (!http_req->header(HTTP_JSONROOT).empty()) {
        request.__set_json_root(http_req->header(HTTP_JSONROOT));
    }
    // 解析 JSON 解析方式：strip_outer_array（是否裁剪最外层数组 []）和 read_json_by_line（按行读取 JSON，即 NDJSON）。若均未配置，默认开启按行读取 JSON。
    if (!http_req->header(HTTP_STRIP_OUTER_ARRAY).empty()) {
        if (iequal(http_req->header(HTTP_STRIP_OUTER_ARRAY), "true")) {
            request.__set_strip_outer_array(true);
        } else {
            request.__set_strip_outer_array(false);
        }
    } else {
        request.__set_strip_outer_array(false);
    }
    // 解析 JSON 参数：数字是否当作字符串解析 (num_as_string) 以及是否启用模糊解析模式 (fuzzy_parse，提高 JSON 解析吞吐量)。
    if (!http_req->header(HTTP_READ_JSON_BY_LINE).empty()) {
        if (iequal(http_req->header(HTTP_READ_JSON_BY_LINE), "true")) {
            request.__set_read_json_by_line(true);
        } else {
            request.__set_read_json_by_line(false);
        }
    } else {
        request.__set_read_json_by_line(false);
    }

    if (http_req->header(HTTP_READ_JSON_BY_LINE).empty() &&
        http_req->header(HTTP_STRIP_OUTER_ARRAY).empty()) {
        request.__set_read_json_by_line(true);
        request.__set_strip_outer_array(false);
    }

    if (!http_req->header(HTTP_NUM_AS_STRING).empty()) {
        if (iequal(http_req->header(HTTP_NUM_AS_STRING), "true")) {
            request.__set_num_as_string(true);
        } else {
            request.__set_num_as_string(false);
        }
    } else {
        request.__set_num_as_string(false);
    }
    if (!http_req->header(HTTP_FUZZY_PARSE).empty()) {
        if (iequal(http_req->header(HTTP_FUZZY_PARSE), "true")) {
            request.__set_fuzzy_parse(true);
        } else {
            request.__set_fuzzy_parse(false);
        }
    } else {
        request.__set_fuzzy_parse(false);
    }
    // 设置 Sequence 列列名（针对 Unique Key 模型的 Sequence 列功能，控制版本更新顺序）。
    if (!http_req->header(HTTP_FUNCTION_COLUMN + "." + HTTP_SEQUENCE_COL).empty()) {
        request.__set_sequence_col(
                http_req->header(HTTP_FUNCTION_COLUMN + "." + HTTP_SEQUENCE_COL));
    }
    // 解析数据发送批次并行度 (send_batch_parallelism) 以及是否将数据全部导入到单个 Tablet (load_to_single_tablet)。
    if (!http_req->header(HTTP_SEND_BATCH_PARALLELISM).empty()) {
        int parallelism = DORIS_TRY(safe_stoi(http_req->header(HTTP_SEND_BATCH_PARALLELISM),
                                              HTTP_SEND_BATCH_PARALLELISM));
        request.__set_send_batch_parallelism(parallelism);
    }

    if (!http_req->header(HTTP_LOAD_TO_SINGLE_TABLET).empty()) {
        if (iequal(http_req->header(HTTP_LOAD_TO_SINGLE_TABLET), "true")) {
            request.__set_load_to_single_tablet(true);
        } else {
            request.__set_load_to_single_tablet(false);
        }
    }
    // 设置任务超时时间 (timeout) 及 Thrift RPC 调用的超时阈值。
    if (ctx->timeout_second != -1) {
        request.__set_timeout(ctx->timeout_second);
    }
    request.__set_thrift_rpc_timeout_ms(config::thrift_rpc_timeout_ms);
    // 解析 merge_type（数据合并类型：APPEND、DELETE、MERGE）
    // 强制校验逻辑：如果 merge_type 是 MERGE，必须提供 delete_condition；反之如果不为 MERGE 则不能提供 delete_condition。
    TMergeType::type merge_type = TMergeType::APPEND;
    StringCaseMap<TMergeType::type> merge_type_map = {{"APPEND", TMergeType::APPEND},
                                                      {"DELETE", TMergeType::DELETE},
                                                      {"MERGE", TMergeType::MERGE}};
    if (!http_req->header(HTTP_MERGE_TYPE).empty()) {
        std::string merge_type_str = http_req->header(HTTP_MERGE_TYPE);
        auto iter = merge_type_map.find(merge_type_str);
        if (iter != merge_type_map.end()) {
            merge_type = iter->second;
        } else {
            return Status::InvalidArgument("Invalid merge type {}", merge_type_str);
        }
        if (merge_type == TMergeType::MERGE && http_req->header(HTTP_DELETE_CONDITION).empty()) {
            return Status::InvalidArgument("Excepted DELETE ON clause when merge type is MERGE.");
        } else if (merge_type != TMergeType::MERGE &&
                   !http_req->header(HTTP_DELETE_CONDITION).empty()) {
            return Status::InvalidArgument(
                    "Not support DELETE ON clause when merge type is not MERGE.");
        }
    }
    request.__set_merge_type(merge_type);
    if (!http_req->header(HTTP_DELETE_CONDITION).empty()) {
        request.__set_delete_condition(http_req->header(HTTP_DELETE_CONDITION));
    }

    if (!http_req->header(HTTP_MAX_FILTER_RATIO).empty()) {
        ctx->max_filter_ratio = strtod(http_req->header(HTTP_MAX_FILTER_RATIO).c_str(), nullptr);
        request.__set_max_filter_ratio(ctx->max_filter_ratio);
    }

    if (!http_req->header(HTTP_HIDDEN_COLUMNS).empty()) {
        request.__set_hidden_columns(http_req->header(HTTP_HIDDEN_COLUMNS));
    }
    if (!http_req->header(HTTP_TRIM_DOUBLE_QUOTES).empty()) {
        if (iequal(http_req->header(HTTP_TRIM_DOUBLE_QUOTES), "true")) {
            request.__set_trim_double_quotes(true);
        } else {
            request.__set_trim_double_quotes(false);
        }
    }
    if (!http_req->header(HTTP_SKIP_LINES).empty()) {
        int skip_lines = DORIS_TRY(safe_stoi(http_req->header(HTTP_SKIP_LINES), HTTP_SKIP_LINES));
        if (skip_lines < 0) {
            return Status::InvalidArgument("Invalid 'skip_lines': {}", skip_lines);
        }
        request.__set_skip_lines(skip_lines);
    }
    if (!http_req->header(HTTP_ENABLE_PROFILE).empty()) {
        if (iequal(http_req->header(HTTP_ENABLE_PROFILE), "true")) {
            request.__set_enable_profile(true);
        } else {
            request.__set_enable_profile(false);
        }
    }
    // 5. 主键模型局部更新 (Partial Update) 逻辑校验
    // 解析 Unique Key 表的更新模式 (unique_key_update_mode)。特别地，对于 灵活列局部更新 (UPDATE_FLEXIBLE_COLUMNS)，进行了极其严格的互斥性与合法性检查：
    // 仅支持 JSON 格式输入；
    if (!http_req->header(HTTP_UNIQUE_KEY_UPDATE_MODE).empty()) {
        static const StringCaseMap<TUniqueKeyUpdateMode::type> unique_key_update_mode_map = {
                {"UPSERT", TUniqueKeyUpdateMode::UPSERT},
                {"UPDATE_FIXED_COLUMNS", TUniqueKeyUpdateMode::UPDATE_FIXED_COLUMNS},
                {"UPDATE_FLEXIBLE_COLUMNS", TUniqueKeyUpdateMode::UPDATE_FLEXIBLE_COLUMNS}};
        std::string unique_key_update_mode_str = http_req->header(HTTP_UNIQUE_KEY_UPDATE_MODE);
        auto iter = unique_key_update_mode_map.find(unique_key_update_mode_str);
        if (iter != unique_key_update_mode_map.end()) {
            TUniqueKeyUpdateMode::type unique_key_update_mode = iter->second;
            if (unique_key_update_mode == TUniqueKeyUpdateMode::UPDATE_FLEXIBLE_COLUMNS) {
                // check constraints when flexible partial update is enabled
                if (ctx->format != TFileFormatType::FORMAT_JSON) {
                    return Status::InvalidArgument(
                            "flexible partial update only support json format as input file "
                            "currently");
                }
                if (!http_req->header(HTTP_FUZZY_PARSE).empty() &&
                    iequal(http_req->header(HTTP_FUZZY_PARSE), "true")) {
                    return Status::InvalidArgument(
                            "Don't support flexible partial update when 'fuzzy_parse' is enabled");
                }
                if (!http_req->header(HTTP_COLUMNS).empty()) {
                    return Status::InvalidArgument(
                            "Don't support flexible partial update when 'columns' is specified");
                }
                if (!http_req->header(HTTP_JSONPATHS).empty()) {
                    return Status::InvalidArgument(
                            "Don't support flexible partial update when 'jsonpaths' is specified");
                }
                if (!http_req->header(HTTP_HIDDEN_COLUMNS).empty()) {
                    return Status::InvalidArgument(
                            "Don't support flexible partial update when 'hidden_columns' is "
                            "specified");
                }
                if (!http_req->header(HTTP_FUNCTION_COLUMN + "." + HTTP_SEQUENCE_COL).empty()) {
                    return Status::InvalidArgument(
                            "Don't support flexible partial update when "
                            "'function_column.sequence_col' is specified");
                }
                if (!http_req->header(HTTP_MERGE_TYPE).empty()) {
                    return Status::InvalidArgument(
                            "Don't support flexible partial update when "
                            "'merge_type' is specified");
                }
                if (!http_req->header(HTTP_WHERE).empty()) {
                    return Status::InvalidArgument(
                            "Don't support flexible partial update when "
                            "'where' is specified");
                }
            }
            request.__set_unique_key_update_mode(unique_key_update_mode);
        } else {
            return Status::InvalidArgument(
                    "Invalid unique_key_partial_mode {}, must be one of 'UPSERT', "
                    "'UPDATE_FIXED_COLUMNS' or 'UPDATE_FLEXIBLE_COLUMNS'",
                    unique_key_update_mode_str);
        }
    }

    if (http_req->header(HTTP_UNIQUE_KEY_UPDATE_MODE).empty() &&
        !http_req->header(HTTP_PARTIAL_COLUMNS).empty()) {
        // only consider `partial_columns` parameter when `unique_key_update_mode` is not set
        if (iequal(http_req->header(HTTP_PARTIAL_COLUMNS), "true")) {
            request.__set_unique_key_update_mode(TUniqueKeyUpdateMode::UPDATE_FIXED_COLUMNS);
            // for backward compatibility
            request.__set_partial_update(true);
        }
    }
    // 设置局部更新遇到新 Key 时的处理策略（APPEND：直接追加新行；ERROR：报错提示）。
    if (!http_req->header(HTTP_PARTIAL_UPDATE_NEW_ROW_POLICY).empty()) {
        static const std::map<std::string, TPartialUpdateNewRowPolicy::type> policy_map {
                {"APPEND", TPartialUpdateNewRowPolicy::APPEND},
                {"ERROR", TPartialUpdateNewRowPolicy::ERROR}};

        auto policy_name = http_req->header(HTTP_PARTIAL_UPDATE_NEW_ROW_POLICY);
        std::transform(policy_name.begin(), policy_name.end(), policy_name.begin(),
                       [](unsigned char c) { return std::toupper(c); });
        auto it = policy_map.find(policy_name);
        if (it == policy_map.end()) {
            return Status::InvalidArgument(
                    "Invalid partial_update_new_key_behavior {}, must be one of {'APPEND', "
                    "'ERROR'}",
                    policy_name);
        }
        request.__set_partial_update_new_key_policy(it->second);
    }
    // 6. 内存表与分布式计算参数设置
    if (!http_req->header(HTTP_MEMTABLE_ON_SINKNODE).empty()) {
        // memtable_on_sink_node：控制是否在 Data Sink 节点构建 MemTable（减少节点间 RPC 传输）。
        bool value = iequal(http_req->header(HTTP_MEMTABLE_ON_SINKNODE), "true");
        request.__set_memtable_on_sink_node(value);
    }
    if (!http_req->header(HTTP_LOAD_STREAM_PER_NODE).empty()) {
        // stream_per_node：每个目标 BE 建立的数据流连接数量。
        int stream_per_node = DORIS_TRY(
                safe_stoi(http_req->header(HTTP_LOAD_STREAM_PER_NODE), HTTP_LOAD_STREAM_PER_NODE));
        request.__set_stream_per_node(stream_per_node);
    }
    if (ctx->group_commit) {
        request.__set_group_commit_mode(ctx->group_commit_mode);
    }

    // Keep cloud_cluster for compatibility with old FEs during rolling upgrade. New FEs use
    // backend_id below to bind planning to the compute group of the receiving BE.
    if (!http_req->header(HTTP_COMPUTE_GROUP).empty()) {
        // cloud_cluster/compute_group：存算分离架构下绑定的计算集群/计算组。
        request.__set_cloud_cluster(http_req->header(HTTP_COMPUTE_GROUP));
    } else if (!http_req->header(HTTP_CLOUD_CLUSTER).empty()) {
        request.__set_cloud_cluster(http_req->header(HTTP_CLOUD_CLUSTER));
    }

    if (_exec_env->cluster_info()->backend_id != 0) {
        request.__set_backend_id(_exec_env->cluster_info()->backend_id);
    } else {
        LOG(WARNING) << "_exec_env->cluster_info not set backend_id";
    }

    if (!http_req->header(HTTP_EMPTY_FIELD_AS_NULL).empty()) {
        if (iequal(http_req->header(HTTP_EMPTY_FIELD_AS_NULL), "true")) {
            request.__set_empty_field_as_null(true);
        }
    }

#ifndef BE_TEST
    // plan this load
    // 7. 发送 RPC 请求至 Master FE 生成导入执行计划 (Plan)
    // 获取 Master FE 地址，通过 Thrift 发送 streamLoadPut RPC 请求。FE 接收到请求后会进行权限校验、表结构元数据获取、分片分配并生成 Query Plan（ Fragment 执行计划），
    // 然后将结果返回写回 ctx->put_result 中。计时并记录 RPC 耗时。
    TNetworkAddress master_addr = _exec_env->cluster_info()->master_fe_addr;
    int64_t stream_load_put_start_time = MonotonicNanos();
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, ctx](FrontendServiceConnection& client) {
                client->streamLoadPut(ctx->put_result, request);
            }));
    ctx->stream_load_put_cost_nanos = MonotonicNanos() - stream_load_put_start_time;
#else
    ctx->put_result = k_stream_load_put_result;
#endif
    // 8. 校验 Plan 状态与参数调整
    // 检查 FE 生成 Plan 的状态，失败则记录日志并提前退出；
    Status plan_status(Status::create(ctx->put_result.status));
    if (!plan_status.ok()) {
        LOG(WARNING) << "plan streaming load failed. errmsg=" << plan_status << ctx->brief();
        return plan_status;
    }
    DCHECK(ctx->put_result.__isset.pipeline_params);
    ctx->put_result.pipeline_params.query_options.__set_enable_strict_cast(false);
    ctx->put_result.pipeline_params.query_options.__set_enable_insert_strict(strictMode);
    if (config::is_cloud_mode() && ctx->two_phase_commit && ctx->is_mow_table()) {
        return Status::NotSupported("stream load 2pc is unsupported for mow table");
    }
    // 9. 针对 Async Group Commit 计算 Content-Length
    // 如果是异步 Group Commit 模式，解析 Content-Length 并估算解压后的数据体大小（若被压缩则简单乘以 3），供分配 WAL (Write-Ahead Log) 预留空间使用。
    if (iequal(ctx->group_commit_mode, ASYNC_MODE)) {
        // FIXME find a way to avoid chunked stream load write large WALs
        size_t content_length = 0;
        if (!http_req->header(HttpHeaders::CONTENT_LENGTH).empty()) {
            try {
                content_length = std::stol(http_req->header(HttpHeaders::CONTENT_LENGTH));
            } catch (const std::exception& e) {
                return Status::InvalidArgument("invalid HTTP header CONTENT_LENGTH={}: {}",
                                               http_req->header(HttpHeaders::CONTENT_LENGTH),
                                               e.what());
            }
            if (LoadUtil::is_compressed_load(ctx->compress_type, ctx->format)) {
                content_length *= 3;
            }
        }
        ctx->put_result.pipeline_params.__set_content_length(content_length);
    }

    VLOG_NOTICE << "params is "
                << apache::thrift::ThriftDebugString(ctx->put_result.pipeline_params);
    // if we not use streaming, we must download total content before we begin
    // to process this load
    // 如果不支持流式传输，函数在此结束返回 Status::OK()，后方的 HTTP 接收逻辑会继续将全部 Body 写入本地文件，写完后再触发实际导入。
    if (!ctx->use_streaming) {
        return Status::OK();
    }
    // 10. 启动执行计划或返回
    // 如果是流式传输，直接调用 StreamLoadExecutor 开始在当前 BE 上执行由 FE 返回的 Pipeline Plan Fragment。同时注册回调匿名函数 _on_finish，当导入任务完成（无论成功还是失败）时回调以响应 HTTP 客户端。
    TPipelineFragmentParamsList mocked;
    return _exec_env->stream_load_executor()->execute_plan_fragment(
            ctx, mocked, [http_req, this](std::shared_ptr<StreamLoadContext> ctx) {
                _on_finish(ctx, http_req);
            });
}

Status StreamLoadAction::_data_saved_path(HttpRequest* req, std::string* file_path,
                                          int64_t file_bytes) {
    std::string prefix;
    RETURN_IF_ERROR(_exec_env->load_path_mgr()->allocate_dir(req->param(HTTP_DB_KEY), "", &prefix,
                                                             file_bytes));
    timeval tv;
    gettimeofday(&tv, nullptr);
    struct tm tm;
    time_t cur_sec = tv.tv_sec;
    localtime_r(&cur_sec, &tm);
    char buf[64];
    strftime(buf, 64, "%Y%m%d%H%M%S", &tm);
    std::stringstream ss;
    ss << prefix << "/" << req->param(HTTP_TABLE_KEY) << "." << buf << "." << tv.tv_usec;
    *file_path = ss.str();
    return Status::OK();
}

void StreamLoadAction::_save_stream_load_record(std::shared_ptr<StreamLoadContext> ctx,
                                                const std::string& str) {
    std::shared_ptr<StreamLoadRecorder> stream_load_recorder =
            ExecEnv::GetInstance()->storage_engine().get_stream_load_recorder();

    if (stream_load_recorder != nullptr) {
        std::string key =
                std::to_string(ctx->start_millis + ctx->load_cost_millis) + "_" + ctx->label;
        auto st = stream_load_recorder->put(key, str);
        if (st.ok()) {
            LOG(INFO) << "put stream_load_record rocksdb successfully. label: " << ctx->label
                      << ", key: " << key;
        }
    } else {
        LOG(WARNING) << "put stream_load_record rocksdb failed. stream_load_recorder is null.";
    }
}

Status StreamLoadAction::_check_wal_space(const std::string& group_commit_mode,
                                          int64_t content_length) {
    if (iequal(group_commit_mode, ASYNC_MODE) &&
        !load_size_smaller_than_wal_limit(content_length)) {
        std::stringstream ss;
        ss << "There is no space for group commit stream load async WAL. This stream load "
              "size is "
           << content_length
           << ". WAL dir info: " << ExecEnv::GetInstance()->wal_mgr()->get_wal_dirs_info_string();
        LOG(WARNING) << ss.str();
        return Status::Error<EXCEEDED_LIMIT>(ss.str());
    }
    return Status::OK();
}

Status StreamLoadAction::_can_group_commit(HttpRequest* req, std::shared_ptr<StreamLoadContext> ctx,
                                           std::string& group_commit_header,
                                           bool& can_group_commit) {
    int64_t content_length = 0;
    const auto& content_length_str = req->header(HttpHeaders::CONTENT_LENGTH);
    if (!content_length_str.empty()) {
        try {
            content_length = std::stoll(content_length_str);
        } catch (const std::exception& e) {
            return Status::InvalidArgument("invalid HTTP header CONTENT_LENGTH={}: {}",
                                           content_length_str, e.what());
        }
    }
    if (content_length < 0) {
        std::stringstream ss;
        ss << "This stream load content length <0 (" << content_length
           << "), please check your content length.";
        LOG(WARNING) << ss.str();
        return Status::InvalidArgument(ss.str());
    }
    auto is_chunk = !req->header(HttpHeaders::TRANSFER_ENCODING).empty() &&
                    req->header(HttpHeaders::TRANSFER_ENCODING).find(CHUNK) != std::string::npos;
    if (content_length == 0 && !is_chunk) {
        // off_mode and empty
        can_group_commit = false;
        return Status::OK();
    }
    if (is_chunk) {
        ctx->label = "";
    }

    auto partial_columns = !req->header(HTTP_PARTIAL_COLUMNS).empty() &&
                           iequal(req->header(HTTP_PARTIAL_COLUMNS), "true");
    auto temp_partitions = !req->header(HTTP_TEMP_PARTITIONS).empty();
    auto partitions = !req->header(HTTP_PARTITIONS).empty();
    auto update_mode =
            !req->header(HTTP_UNIQUE_KEY_UPDATE_MODE).empty() &&
            (iequal(req->header(HTTP_UNIQUE_KEY_UPDATE_MODE), "UPDATE_FIXED_COLUMNS") ||
             iequal(req->header(HTTP_UNIQUE_KEY_UPDATE_MODE), "UPDATE_FLEXIBLE_COLUMNS"));
    if (!partial_columns && !partitions && !temp_partitions && !ctx->two_phase_commit &&
        !update_mode) {
        if (!config::wait_internal_group_commit_finish && !group_commit_header.empty() &&
            !ctx->label.empty()) {
            return Status::InvalidArgument("label and group_commit can't be set at the same time");
        }
        RETURN_IF_ERROR(_check_wal_space(group_commit_header, content_length));
        can_group_commit = true;
    }
    return Status::OK();
}

Status StreamLoadAction::_handle_group_commit(HttpRequest* req,
                                              std::shared_ptr<StreamLoadContext> ctx) {
    std::string group_commit_header = req->header(HTTP_GROUP_COMMIT);
    if (!group_commit_header.empty() && !iequal(group_commit_header, SYNC_MODE) &&
        !iequal(group_commit_header, ASYNC_MODE) && !iequal(group_commit_header, OFF_MODE)) {
        return Status::InvalidArgument(
                "group_commit can only be [async_mode, sync_mode, off_mode]");
    }
    if (config::wait_internal_group_commit_finish) {
        group_commit_header = SYNC_MODE;
    }

    // if group_commit_header is off_mode, we will not use group commit
    if (iequal(group_commit_header, OFF_MODE)) {
        ctx->group_commit_mode = OFF_MODE;
        ctx->group_commit = false;
        return Status::OK();
    }
    bool can_group_commit = false;
    RETURN_IF_ERROR(_can_group_commit(req, ctx, group_commit_header, can_group_commit));
    if (!can_group_commit) {
        ctx->group_commit_mode = OFF_MODE;
        ctx->group_commit = false;
    } else {
        if (!group_commit_header.empty()) {
            ctx->group_commit_mode = group_commit_header;
            ctx->group_commit = true;
        } else {
            // use table property to decide group commit or not
            ctx->group_commit_mode = "";
            ctx->group_commit = false;
        }
    }
    return Status::OK();
}

} // namespace doris
