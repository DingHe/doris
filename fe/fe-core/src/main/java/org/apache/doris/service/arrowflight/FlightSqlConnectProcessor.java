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

package org.apache.doris.service.arrowflight;

import org.apache.doris.analysis.Expr;
import org.apache.doris.common.ConnectionException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.Status;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.Types;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectProcessor;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.service.arrowflight.results.FlightSqlEndpointsLocation;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Process one flgiht sql connection.
 * <p>
 * Must use try-with-resources.
 */
// 专门用来处理 Arrow Flight SQL 客户端连接的处理器
// 核心作用包括：
// Arrow Flight 协议适配：将连接类型固定为 ConnectType.ARROW_FLIGHT_SQL，并配置线程局部变量和本地结果返回模式。
// Flight 命令准备与生命周期管理：在接收到客户端命令（如 COM_QUERY）时进行状态重置、命令设置和起始时间记录。
// 分布式 Arrow Schema 获取 (fetchArrowFlightSchema)：在分布式查询执行后，向对应的后端节点（BE）发送 RPC 请求（PFetchArrowFlightSchemaRequest），利用 Apache Arrow 的 ArrowStreamReader 解析并校验返回的 Arrow Schema 结构，确保多个分片之间的 Schema 一致性，并设置公共访问地址。
// 资源安全的生命周期清理 (close)：实现 AutoCloseable 接口，配合 try-with-resources 语法，在连接或请求结束时妥善清理远程执行器、释放非延迟等待的查询资源、清理上下文（ConnectContext）并从线程中移除。
public class FlightSqlConnectProcessor extends ConnectProcessor implements AutoCloseable {
    private static final Logger LOG = LogManager.getLogger(FlightSqlConnectProcessor.class);
    // 保存当前 Arrow Flight SQL 查询所对应的 Arrow 列存 Schema 结构（由 Apache Arrow 提供）。用于在多端返回或分布式传输时校验数据结构的一致性。
    private Schema arrowSchema;

    public FlightSqlConnectProcessor(ConnectContext context) {
        super(context);
        connectType = ConnectType.ARROW_FLIGHT_SQL;
        // 设置当前线程的本地信息以及开启本地返回结果模式（setReturnResultFromLocal(true)）
        context.setThreadLocalInfo();
        context.setReturnResultFromLocal(true);
    }

    public Schema getArrowSchema() {
        return arrowSchema;
    }
    // 命令准备阶段。
    public void prepare(MysqlCommand command) {
        // set status of query to OK.
        ctx.getState().reset();
        executor = null;
        // 校验传入的命令是否为空，若为空则通过错误报告系统记录 ERR_UNKNOWN_COM_ERROR 错误并记录日志。
        if (command == null) {
            ErrorReport.report(ErrorCode.ERR_UNKNOWN_COM_ERROR);
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_COM_ERROR, "Unknown command(" + command.toString() + ")");
            LOG.warn("Unknown command(" + command + ")");
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("arrow flight sql handle command {}", command);
        }
        ctx.setCommand(command);
        ctx.setStartTime();
    }
    // 处理 Arrow Flight SQL 的查询请求。
    public void handleQuery(String query) throws ConnectionException {
        // 将命令指定为 MysqlCommand.COM_QUERY，调用 prepare 初始化
        MysqlCommand command = MysqlCommand.COM_QUERY;
        prepare(command);
        resolveWorkloadGroupName();
        // 将运行中的查询语句记录到上下文中
        ctx.setRunningQuery(query);
        // 执行标准的查询流程。
        super.handleQuery(query);
    }

    // TODO
    // private void handleInitDb() {
    //     handleInitDb(fullDbName);
    // }

    // TODO
    // private void handleFieldList() {
    //     handleFieldList(tableName);
    // }
    // int timeoutMs  获取 Arrow Flight Schema 的超时时间（单位：毫秒）
    public void fetchArrowFlightSchema(int timeoutMs) {
        // 在执行抓取前，先检查会话上下文中保存的 Arrow Flight 端点位置列表（FlightSqlEndpointsLocations）是否为空。
        // 如果为空，说明没有关联的查询结果端点，直接抛出运行时异常。
        if (ctx.getFlightSqlEndpointsLocations().isEmpty()) {
            throw new RuntimeException("fetch arrow flight schema failed, no FlightSqlEndpointsLocations.");
        }
        // 第二部分：遍历端点与构建 RPC 请求
        for (FlightSqlEndpointsLocation endpointLoc : ctx.getFlightSqlEndpointsLocations()) {
            // 从端点中提取目标后端 BE 的内部服务网络地址（address）、实例唯一标识（tid）以及查询结果的输出表达式列表（resultOutputExprs）。
            TNetworkAddress address = endpointLoc.getResultInternalServiceAddr();
            TUniqueId tid = endpointLoc.getFinstId();
            ArrayList<Expr> resultOutputExprs = endpointLoc.getResultOutputExprs();
            // 将 Thrift 定义的 TUniqueId（由高位 hi 和低位 lo 组成）转换为 Protobuf 对应的 Types.PUniqueId 对象。
            Types.PUniqueId queryId = Types.PUniqueId.newBuilder().setHi(tid.hi).setLo(tid.lo).build();
            try {
                // 使用 Protobuf 构建器构建 PFetchArrowFlightSchemaRequest 请求对象，将 queryId 传入其中，用于向 BE 请求该实例的 Schema 信息。
                InternalService.PFetchArrowFlightSchemaRequest request
                        = InternalService.PFetchArrowFlightSchemaRequest.newBuilder().setFinstId(queryId).build();
                // 第三部分：发起 RPC 异步调用与超时等待
                // 通过 BackendServiceProxy 单例向指定的后端 BE 地址发送异步 RPC 请求，获取一个 Future<PFetchArrowFlightSchemaResult> 对象。
                Future<InternalService.PFetchArrowFlightSchemaResult> future = BackendServiceProxy.getInstance()
                        .fetchArrowFlightSchema(address, request);
                InternalService.PFetchArrowFlightSchemaResult pResult;
                // 进行阻塞等待，在指定的 timeoutMs 毫秒内获取返回结果 pResult。如果返回结果为 null，则抛出超时异常。
                pResult = future.get(timeoutMs, TimeUnit.MILLISECONDS);
                if (pResult == null) {
                    throw new RuntimeException(
                            String.format("fetch arrow flight schema timeout, queryId: %s", DebugUtil.printId(tid)));
                }
                // 第四部分：检查 RPC 返回状态
                Status resultStatus = new Status(pResult.getStatus());
                if (resultStatus.getErrorCode() != TStatusCode.OK) {
                    throw new RuntimeException(
                            String.format("fetch arrow flight schema failed, queryId: %s, errmsg: %s",
                                    DebugUtil.printId(tid), resultStatus));
                }
                // 第五部分：解析并设置后端公共访问地址
                // 如果 BE 返回了专用于 Arrow Flight 服务的对外 IP（BeArrowFlightIp）和端口（BeArrowFlightPort），则将其设置到地址对象中
                // 并最终写入当前的端点位置 endpointLoc 中，供客户端后续直连 BE 拉取数据使用。
                TNetworkAddress resultPublicAccessAddr = new TNetworkAddress();
                if (pResult.hasBeArrowFlightIp()) {
                    resultPublicAccessAddr.setHostname(pResult.getBeArrowFlightIp().toStringUtf8());
                }
                if (pResult.hasBeArrowFlightPort()) {
                    resultPublicAccessAddr.setPort(pResult.getBeArrowFlightPort());
                }
                endpointLoc.setResultPublicAccessAddr(resultPublicAccessAddr);
                // 第六部分：反序列化并校验 Arrow Schema
                if (pResult.hasSchema() && pResult.getSchema().size() > 0) {
                    // 检查 RPC 结果中是否包含有效的 Schema 数据且大小大于 0。若为空则抛出异常。
                    // 建内存分配器 RootAllocator（最大分配额为 Integer.MAX_VALUE）和 ArrowStreamReader。
                    // 将返回的 Schema 字节数组通过 ByteArrayInputStream 传入，用于反序列化 Arrow 数据流。
                    try (RootAllocator rootAllocator = new RootAllocator(Integer.MAX_VALUE);
                            ArrowStreamReader arrowStreamReader = new ArrowStreamReader(
                                    new ByteArrayInputStream(pResult.getSchema().toByteArray()), rootAllocator)) {
                        Schema schema;
                        // SchemaRoot belongs to ArrowStreamReader, it will be released when ArrowStreamReader is closed
                        // 从 arrowStreamReader 获取向量结构根 VectorSchemaRoot 并提取字段向量列表（fieldVectors）。
                        // 校验字段向量的个数是否与前端期望的输出表达式数量（resultOutputExprs.size()）一致，不一致则抛出异常。
                        VectorSchemaRoot root = arrowStreamReader.getVectorSchemaRoot();
                        List<FieldVector> fieldVectors = root.getFieldVectors();
                        if (fieldVectors.size() != resultOutputExprs.size()) {
                            throw new RuntimeException(
                                    String.format("Schema size %s' is not equal to arrow field size %s, queryId: %s.",
                                            fieldVectors.size(), resultOutputExprs.size(), DebugUtil.printId(tid)));
                        }
                        schema = root.getSchema();
                        if (arrowSchema == null) {
                            arrowSchema = schema;
                        } else if (!arrowSchema.equals(schema)) {
                            throw new RuntimeException(String.format(
                                    "The schema returned by results BE is different, first schema: %s, "
                                            + "new schema: %s, queryId: %s，backend: %s", arrowSchema, schema,
                                    DebugUtil.printId(tid), address));
                        }
                    } catch (Exception e) {
                        throw new RuntimeException("Read Arrow Flight Schema failed.", e);
                    }
                } else {
                    throw new RuntimeException(
                            String.format("get empty arrow flight schema, queryId: %s", DebugUtil.printId(tid)));
                }
            } catch (RpcException e) {
                throw new RuntimeException(
                        String.format("arrow flight schema fetch catch rpc exception, queryId: %s，backend: %s",
                                DebugUtil.printId(tid), address), e);
            } catch (InterruptedException e) {
                throw new RuntimeException(
                        String.format("arrow flight schema future get interrupted exception, queryId: %s，backend: %s",
                                DebugUtil.printId(tid), address), e);
            } catch (ExecutionException e) {
                throw new RuntimeException(
                        String.format("arrow flight schema future get execution exception, queryId: %s，backend: %s",
                                DebugUtil.printId(tid), address), e);
            } catch (TimeoutException e) {
                throw new RuntimeException(String.format("arrow flight schema fetch timeout, queryId: %s，backend: %s",
                        DebugUtil.printId(tid), address), e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        ctx.setCommand(MysqlCommand.COM_SLEEP);
        // Executors whose results are pulled from the BE keep their coordinator alive past
        // GetFlightInfo (registered as deferred executors on the ConnectContext) so the BE can
        // still fetch external-table splits during DoGet. Do NOT finalize those here; they are
        // finalized when the next query starts or the connection is torn down. Executors that are
        // not deferred (local results, or a query that already failed) are finalized now. See #62259.
        for (StmtExecutor asynExecutor : returnResultFromRemoteExecutor) {
            if (!asynExecutor.isDeferredForArrowFlight()) {
                asynExecutor.finalizeQuery();
            }
        }
        returnResultFromRemoteExecutor.clear();
        if (executor != null && !executor.isDeferredForArrowFlight()) {
            executor.finalizeQuery();
        }
        ctx.clear();
        ConnectContext.remove();
    }
}
