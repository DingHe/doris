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

#include <brpc/closure_guard.h>
#include <gen_cpp/internal_service.pb.h>

#include <string>

#include "common/status.h"
#include "util/work_thread_pool.hpp"

namespace google::protobuf {
class Closure;
class RpcController;
} // namespace google::protobuf

namespace doris {

class StorageEngine;
class ExecEnv;
class PHandShakeRequest;
class PHandShakeResponse;
class RuntimeState;
// 在 Apache Doris 的 BE（Backend）模块中，PInternalService 和继承它的最终派生类 PInternalServiceImpl 是基于 bRPC（Baidu RPC）框架实现的 BE 节点内部 RPC 服务核心实现类。
// 继承自 Proto 文件自动生成的基类 PBackendService，专门用于处理 FE 与 BE 之间 以及 BE 与 BE 之间 的绝大多数内部网络通信请求（包括查询计划分发、中间数据传输、数据写入/导入、Runtime Filter 广播、缓存管理等）。
// 通信中枢：作为 Doris BE 的核心网络 RPC 接收端，将所有接收到的 bRPC 异步请求转发给对应的后端线程池处理（防止 bRPC EventDispatcher IO 线程被阻塞）。
// 资源隔离：通过内部维护的多个不同类型的线程池（_heavy_work_pool、_light_work_pool 等），实现高吞吐数据传输任务与高吞吐轻量控制信令之间的解耦与资源隔离。
class PInternalService : public PBackendService {
public:
    PInternalService(ExecEnv* exec_env);
    ~PInternalService() override;
    // FE 向 BE 分发并直接执行一个 PlanFragment（查询执行片段）的总入口（兼容/传统单步模式）。
    void exec_plan_fragment(google::protobuf::RpcController* controller,
                            const PExecPlanFragmentRequest* request,
                            PExecPlanFragmentResult* result,
                            google::protobuf::Closure* done) override;
    // 两阶段启动模式的第一阶段：准备（Prepare）执行片段。
    // 在 BE 端解析 PlanFragment 结构并完成内存分配、算子构建等初始化准备工作，但暂不启动数据流动。
    void exec_plan_fragment_prepare(google::protobuf::RpcController* controller,
                                    const PExecPlanFragmentRequest* request,
                                    PExecPlanFragmentResult* result,
                                    google::protobuf::Closure* done) override;
    // 两阶段启动模式的第二阶段：启动（Start）执行片段。
    // 正式触发已被 Prepare 的 Fragment 开始调度运行。两阶段启动可以避免多节点分布式 Join 时的死锁与数据流竞争问题。
    void exec_plan_fragment_start(google::protobuf::RpcController* controller,
                                  const PExecPlanFragmentStartRequest* request,
                                  PExecPlanFragmentResult* result,
                                  google::protobuf::Closure* done) override;
    // 取消指定的 PlanFragment 执行。
    // 放入 _light_work_pool 快速处理，通知 FragmentMgr 终止对应的 RuntimeState 并在算子层下发 Cancel 信号。
    void cancel_plan_fragment(google::protobuf::RpcController* controller,
                              const PCancelPlanFragmentRequest* request,
                              PCancelPlanFragmentResult* result,
                              google::protobuf::Closure* done) override;
    // 客户端（或 FE）向 BE 请求拉取查询结果集数据（传统格式/RowBatch 格式）。
    void fetch_data(google::protobuf::RpcController* controller, const PFetchDataRequest* request,
                    PFetchDataResult* result, google::protobuf::Closure* done) override;
    // 向 BE 请求拉取基于 Apache Arrow 内存格式序列化的查询结果集（大幅提升结果集传输效率）。
    void fetch_arrow_data(google::protobuf::RpcController* controller,
                          const PFetchArrowDataRequest* request, PFetchArrowDataResult* result,
                          google::protobuf::Closure* done) override;
    // 当使用 SELECT INTO OUTFILE 导出数据到外部存储（如 S3/HDFS）成功时，向相关节点通知导出完成状态。
    void outfile_write_success(google::protobuf::RpcController* controller,
                               const POutfileWriteSuccessRequest* request,
                               POutfileWriteSuccessResult* result,
                               google::protobuf::Closure* done) override;
    // 从 BE 侧获取指定表的 Schema 描述元数据。
    void fetch_table_schema(google::protobuf::RpcController* controller,
                            const PFetchTableSchemaRequest* request,
                            PFetchTableSchemaResult* result,
                            google::protobuf::Closure* done) override;
    // 获取 Arrow Flight 协议传输时所需的 Arrow Schema 结构信息。
    void fetch_arrow_flight_schema(google::protobuf::RpcController* controller,
                                   const PFetchArrowFlightSchemaRequest* request,
                                   PFetchArrowFlightSchemaResult* result,
                                   google::protobuf::Closure* done) override;
    // 初始化并打开本地 Tablet 的写入器（DeltaWriter），准备接收数据 Block 进行数据导入（如 Stream Load、Routine Load）。
    void tablet_writer_open(google::protobuf::RpcController* controller,
                            const PTabletWriterOpenRequest* request,
                            PTabletWriterOpenResult* response,
                            google::protobuf::Closure* done) override;
    // 打开基于 Stream/Pipeline 的高吞吐数据导入流通道（用于新的 Stream Load 架构）。
    void open_load_stream(google::protobuf::RpcController* controller,
                          const POpenLoadStreamRequest* request, POpenLoadStreamResponse* response,
                          google::protobuf::Closure* done) override;
    // 通过 bRPC 标准数据通道，向被打开的 Tablet Writer 追加写一个数据块（vectorized::Block）。
    void tablet_writer_add_block(google::protobuf::RpcController* controller,
                                 const PTabletWriterAddBlockRequest* request,
                                 PTabletWriterAddBlockResult* response,
                                 google::protobuf::Closure* done) override;
    // 当数据块体积巨大时，绕过 protobuf 序列化，直接通过 Attachment/HTTP 管道追加数据块，降低 CPU 序列化开销。
    void tablet_writer_add_block_by_http(google::protobuf::RpcController* controller,
                                         const ::doris::PEmptyRequest* request,
                                         PTabletWriterAddBlockResult* response,
                                         google::protobuf::Closure* done) override;
    // 取消/中断正在进行的 Tablet 写入任务，清理未 Commit 的临时数据。
    void tablet_writer_cancel(google::protobuf::RpcController* controller,
                              const PTabletWriterCancelRequest* request,
                              PTabletWriterCancelResult* response,
                              google::protobuf::Closure* done) override;

    void get_info(google::protobuf::RpcController* controller, const PProxyRequest* request,
                  PProxyResult* response, google::protobuf::Closure* done) override;
    // 向 BE 的 Cache 模块中写入/更新查询缓存结果（Sql Cache / Partition Cache）。
    void update_cache(google::protobuf::RpcController* controller,
                      const PUpdateCacheRequest* request, PCacheResponse* response,
                      google::protobuf::Closure* done) override;
    // 向 BE 请求拉取已缓存的查询结果块。
    void fetch_cache(google::protobuf::RpcController* controller, const PFetchCacheRequest* request,
                     PFetchCacheResult* result, google::protobuf::Closure* done) override;
    // 根据条件清理 BE 上失效或被淘汰的查询缓存。
    void clear_cache(google::protobuf::RpcController* controller, const PClearCacheRequest* request,
                     PCacheResponse* response, google::protobuf::Closure* done) override;
    // 当某个 Hash Join Build 侧在多个 BE 上局部生成 Runtime Filter 后，将各 BE 的 Filter 汇总合并至 Coordinator 或指定合并节点。
    void merge_filter(::google::protobuf::RpcController* controller,
                      const ::doris::PMergeFilterRequest* request,
                      ::doris::PMergeFilterResponse* response,
                      ::google::protobuf::Closure* done) override;
    // 发送动态过滤器的实际大小/基数估计值，用于自适应调整过滤策略。
    void send_filter_size(::google::protobuf::RpcController* controller,
                          const ::doris::PSendFilterSizeRequest* request,
                          ::doris::PSendFilterSizeResponse* response,
                          ::google::protobuf::Closure* done) override;

    void sync_filter_size(::google::protobuf::RpcController* controller,
                          const ::doris::PSyncFilterSizeRequest* request,
                          ::doris::PSyncFilterSizeResponse* response,
                          ::google::protobuf::Closure* done) override;
    void apply_filterv2(::google::protobuf::RpcController* controller,
                        const ::doris::PPublishFilterRequestV2* request,
                        ::doris::PPublishFilterResponse* response,
                        ::google::protobuf::Closure* done) override;
    void transmit_rec_cte_block(google::protobuf::RpcController* controller,
                                const PTransmitRecCTEBlockParams* request,
                                PTransmitRecCTEBlockResult* response,
                                google::protobuf::Closure* done) override;
    // 重跑/重新触发某个特定的 PlanFragment（主要用于容错恢复或错误重试机制）。
    void rerun_fragment(google::protobuf::RpcController* controller,
                        const PRerunFragmentParams* request, PRerunFragmentResult* response,
                        google::protobuf::Closure* done) override;
    void reset_global_rf(google::protobuf::RpcController* controller,
                         const PResetGlobalRfParams* request, PResetGlobalRfResult* response,
                         google::protobuf::Closure* done) override;
    // 分布式 Shuffle/Broadcast 数据传输核心方法。上游 Fragment 的 DataStreamSink 调用此方法将计算产生的 Block 数据发往下游 Fragment 的 ExchangeNode。
    void transmit_block(::google::protobuf::RpcController* controller,
                        const ::doris::PTransmitDataParams* request,
                        ::doris::PTransmitDataResult* response,
                        ::google::protobuf::Closure* done) override;
    //  通过带 Payload/Attachment 的方式传输 Block 数据（针对大 Block 数据传输进行零拷贝/低 CPU 占用优化）。
    void transmit_block_by_http(::google::protobuf::RpcController* controller,
                                const ::doris::PEmptyRequest* request,
                                ::doris::PTransmitDataResult* response,
                                ::google::protobuf::Closure* done) override;

    void send_data(google::protobuf::RpcController* controller, const PSendDataRequest* request,
                   PSendDataResult* response, google::protobuf::Closure* done) override;
    void commit(google::protobuf::RpcController* controller, const PCommitRequest* request,
                PCommitResult* response, google::protobuf::Closure* done) override;
    void rollback(google::protobuf::RpcController* controller, const PRollbackRequest* request,
                  PRollbackResult* response, google::protobuf::Closure* done) override;
    void fold_constant_expr(google::protobuf::RpcController* controller,
                            const PConstantExprRequest* request, PConstantExprResult* response,
                            google::protobuf::Closure* done) override;
    void check_rpc_channel(google::protobuf::RpcController* controller,
                           const PCheckRPCChannelRequest* request,
                           PCheckRPCChannelResponse* response,
                           google::protobuf::Closure* done) override;
    void reset_rpc_channel(google::protobuf::RpcController* controller,
                           const PResetRPCChannelRequest* request,
                           PResetRPCChannelResponse* response,
                           google::protobuf::Closure* done) override;
    void hand_shake(google::protobuf::RpcController* controller, const PHandShakeRequest* request,
                    PHandShakeResponse* response, google::protobuf::Closure* done) override;

    void report_stream_load_status(google::protobuf::RpcController* controller,
                                   const PReportStreamLoadStatusRequest* request,
                                   PReportStreamLoadStatusResponse* response,
                                   google::protobuf::Closure* done) override;

    void glob(google::protobuf::RpcController* controller, const PGlobRequest* request,
              PGlobResponse* response, google::protobuf::Closure* done) override;

    void group_commit_insert(google::protobuf::RpcController* controller,
                             const PGroupCommitInsertRequest* request,
                             PGroupCommitInsertResponse* response,
                             google::protobuf::Closure* done) override;

    void get_wal_queue_size(google::protobuf::RpcController* controller,
                            const PGetWalQueueSizeRequest* request,
                            PGetWalQueueSizeResponse* response,
                            google::protobuf::Closure* done) override;

    void multiget_data(google::protobuf::RpcController* controller, const PMultiGetRequest* request,
                       PMultiGetResponse* response, google::protobuf::Closure* done) override;

    void multiget_data_v2(google::protobuf::RpcController* controller,
                          const PMultiGetRequestV2* request, PMultiGetResponseV2* response,
                          google::protobuf::Closure* done) override;

    void tablet_fetch_data(google::protobuf::RpcController* controller,
                           const PTabletKeyLookupRequest* request,
                           PTabletKeyLookupResponse* response,
                           google::protobuf::Closure* done) override;

    void test_jdbc_connection(google::protobuf::RpcController* controller,
                              const PJdbcTestConnectionRequest* request,
                              PJdbcTestConnectionResult* result,
                              google::protobuf::Closure* done) override;

    void fetch_remote_tablet_schema(google::protobuf::RpcController* controller,
                                    const PFetchRemoteSchemaRequest* request,
                                    PFetchRemoteSchemaResponse* response,
                                    google::protobuf::Closure* done) override;

    void get_be_resource(google::protobuf::RpcController* controller,
                         const PGetBeResourceRequest* request, PGetBeResourceResponse* response,
                         google::protobuf::Closure* done) override;

    void sync_tablet_meta(google::protobuf::RpcController* controller,
                          const PSyncTabletMetaRequest* request, PSyncTabletMetaResponse* response,
                          google::protobuf::Closure* done) override;

    void delete_dictionary(google::protobuf::RpcController* controller,
                           const PDeleteDictionaryRequest* request,
                           PDeleteDictionaryResponse* response,
                           google::protobuf::Closure* done) override;

    void commit_refresh_dictionary(google::protobuf::RpcController* controller,
                                   const PCommitRefreshDictionaryRequest* request,
                                   PCommitRefreshDictionaryResponse* response,
                                   google::protobuf::Closure* done) override;
    void abort_refresh_dictionary(google::protobuf::RpcController* controller,
                                  const PAbortRefreshDictionaryRequest* request,
                                  PAbortRefreshDictionaryResponse* response,
                                  google::protobuf::Closure* done) override;

    void get_tablet_rowsets(google::protobuf::RpcController* controller,
                            const PGetTabletRowsetsRequest* request,
                            PGetTabletRowsetsResponse* response,
                            google::protobuf::Closure* done) override;

    void request_cdc_client(google::protobuf::RpcController* controller,
                            const PRequestCdcClientRequest* request,
                            PRequestCdcClientResult* result,
                            google::protobuf::Closure* done) override;

private:
    void _exec_plan_fragment_in_pthread(google::protobuf::RpcController* controller,
                                        const PExecPlanFragmentRequest* request,
                                        PExecPlanFragmentResult* result,
                                        google::protobuf::Closure* done);

    Status _exec_plan_fragment_impl(const std::string& s_request, PFragmentRequestVersion version,
                                    bool compact,
                                    const std::function<void(RuntimeState*, Status*)>& cb =
                                            std::function<void(RuntimeState*, Status*)>());

    void _transmit_block(::google::protobuf::RpcController* controller,
                         const ::doris::PTransmitDataParams* request,
                         ::doris::PTransmitDataResult* response, ::google::protobuf::Closure* done,
                         const Status& extract_st, const int64_t wait_for_worker);

    Status _tablet_fetch_data(const PTabletKeyLookupRequest* request,
                              PTabletKeyLookupResponse* response);

protected:
    // BE 执行环境全局指针。
    // 包含 BE 节点运行所需的各类全局组件（如 FragmentMgr、DataStreamMgr、LoadStreamMgr、线程池、内存追踪器等）。
    ExecEnv* _exec_env = nullptr;

    // every brpc service request should put into thread pool
    // the reason see issue #16634
    // define the interface for reading and writing data as heavy interface
    // otherwise as light interface
    // 重型任务线程池。
    // 专门用于处理消耗 CPU/内存/磁盘或数据量较大的 RPC 操作（如 exec_plan_fragment 计划执行、transmit_block 数据传输、tablet_writer_add_block 数据写入等）。
    FifoThreadPool _heavy_work_pool;
    // 节点间（Peer-to-Peer）数据拉取线程池。
    // 专门处理 BE 与 BE 节点间数据互拉取相关的 RPC 请求，防止点对点数据传输阻塞普通写/算任务。
    FifoThreadPool _peer_fetch_pool;
    // 轻量控制信令线程池。
    // 用于处理执行时间极短、响应要求极高的元数据或控制指令（如 cancel_plan_fragment 取消执行、merge_filter / apply_filterv2 动态过滤器发布、心跳/Channel 检查等）。
    FifoThreadPool _light_work_pool;
    // Arrow Flight 专属任务线程池。
    // 专门用于处理通过 Arrow Flight 高效导出/传输数据相关的轻重任务。
    FifoThreadPool _arrow_flight_work_pool;
};

// `StorageEngine` mixin for `PInternalService`
class PInternalServiceImpl final : public PInternalService {
public:
    PInternalServiceImpl(StorageEngine& engine, ExecEnv* exec_env);

    ~PInternalServiceImpl() override;
    void request_slave_tablet_pull_rowset(google::protobuf::RpcController* controller,
                                          const PTabletWriteSlaveRequest* request,
                                          PTabletWriteSlaveResult* response,
                                          google::protobuf::Closure* done) override;
    void response_slave_tablet_pull_rowset(google::protobuf::RpcController* controller,
                                           const PTabletWriteSlaveDoneRequest* request,
                                           PTabletWriteSlaveDoneResult* response,
                                           google::protobuf::Closure* done) override;

    void get_column_ids_by_tablet_ids(google::protobuf::RpcController* controller,
                                      const PFetchColIdsRequest* request,
                                      PFetchColIdsResponse* response,
                                      google::protobuf::Closure* done) override;

    void get_tablet_rowset_versions(google::protobuf::RpcController* controller,
                                    const PGetTabletVersionsRequest* request,
                                    PGetTabletVersionsResponse* response,
                                    google::protobuf::Closure* done) override;

private:
    Status _multi_get(const PMultiGetRequest& request, PMultiGetResponse* response);

    void _get_column_ids_by_tablet_ids(google::protobuf::RpcController* controller,
                                       const PFetchColIdsRequest* request,
                                       PFetchColIdsResponse* response,
                                       google::protobuf::Closure* done);
    // 存储引擎引用。
    // 直接持有 BE 本地存储引擎对象的引用，以便在此类的方法中访问底层的 Tablet、Rowset 以及 Meta 目录。
    StorageEngine& _engine;
};
} // namespace doris
