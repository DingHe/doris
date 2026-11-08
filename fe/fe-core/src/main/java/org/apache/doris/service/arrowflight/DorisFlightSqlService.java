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

import org.apache.doris.common.Config;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.service.arrowflight.auth2.FlightBearerTokenAuthenticator;
import org.apache.doris.service.arrowflight.auth2.FlightRemoteIpServerStreamTracer;
import org.apache.doris.service.arrowflight.sessions.FlightSessionsManager;
import org.apache.doris.service.arrowflight.sessions.FlightSessionsWithTokenManager;
import org.apache.doris.service.arrowflight.tokens.FlightTokenManager;
import org.apache.doris.service.arrowflight.tokens.FlightTokenManagerImpl;

import io.grpc.ServerBuilder;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * flight sql protocol implementation based on nio.
 */
// Apache Doris 中基于 NIO 和 gRPC 实现的 Arrow Flight SQL 服务端核心服务类。
// 初始化并构建 Arrow Flight 服务端：基于 Apache Arrow Flight 框架与 gRPC，在指定的端口上启动高性能的列式数据传输服务，允许客户端（如使用 ADBC 驱动或 Arrow Flight 客户端）直接与 Doris FE 建立高性能连接。
// 管理令牌与会话生命周期：内部初始化并管理 FlightTokenManager（Bearer 令牌管理器）与 FlightSessionsManager（会话管理器），通过 Token 机制在无状态的 Flight SQL 协议中维护和追踪用户的会话上下文（ConnectContext）。
// 服务生命周期控制：提供标准的 start() 和 stop() 方法，用于安全地启动和关闭底层封装的 FlightServer。
public class DorisFlightSqlService {
    private static final Logger LOG = LogManager.getLogger(DorisFlightSqlService.class);
    // gRPC 构建器的消费者配置常量（值为 "grpc.builderConsumer"），用于在配置底层 gRPC transport hints 时标识拦截器或流追踪器的扩展。
    private static final String GRPC_BUILDER_CONSUMER = "grpc.builderConsumer";
    // 底层的 Arrow Flight 服务端实例
    // 基于 gRPC 运行，负责监听网络端口并处理实际的 Flight RPC 请求。
    private final FlightServer flightServer;
    // Flight 令牌管理器（FlightTokenManager）。用于管理基于 Bearer Token 的认证凭证，结合最大连接数、缓存大小及存活时间配置，当 Token 从缓存中被驱逐时会自动注销对应的会话上下文。
    private final FlightTokenManager flightTokenManager;
    // Flight 会话管理器（FlightSessionsManager）。用于管理与 Token 绑定的客户端会话状态。
    private final FlightSessionsManager flightSessionsManager;
    // 服务运行状态标志。使用 volatile 修饰，用于多线程安全地指示当前 Arrow Flight SQL 服务是否正在运行。
    private volatile boolean running;


    // 负责初始化整个 Arrow Flight SQL 服务的核心组件
    public DorisFlightSqlService(int port) {
        BufferAllocator allocator = new RootAllocator();
        // arrow flight sql is a stateless protocol, connection is usually not actively disconnected.
        // bearer token is evict from the cache will unregister ConnectContext.
        this.flightTokenManager = new FlightTokenManagerImpl(
                Math.min(Config.arrow_flight_max_connections, Config.arrow_flight_token_cache_size),
                Config.arrow_flight_token_alive_time_second);
        this.flightSessionsManager = new FlightSessionsWithTokenManager(flightTokenManager);

        DorisFlightSqlProducer producer = new DorisFlightSqlProducer(
                Location.forGrpcInsecure(FrontendOptions.getLocalHostAddress(), port), flightSessionsManager);
        flightServer = FlightServer.builder(allocator, Location.forGrpcInsecure("0.0.0.0", port), producer)
                .transportHint(GRPC_BUILDER_CONSUMER, (Consumer<ServerBuilder<?>>) builder ->
                        builder.addStreamTracerFactory(new FlightRemoteIpServerStreamTracer.Factory()))
                .headerAuthenticator(new FlightBearerTokenAuthenticator(flightTokenManager)).build();
        LOG.info("Arrow Flight SQL service is created, port: {}, arrow_flight_max_connections: {}，"
                        + "arrow_flight_token_alive_time_second: {}", port, Config.arrow_flight_max_connections,
                Config.arrow_flight_token_alive_time_second);
    }

    // start Arrow Flight SQL service, return true if success, otherwise false
    // 启动 Arrow Flight SQL 服务的方法。
    public boolean start() {
        try {
            flightServer.start();
            running = true;
            LOG.info("Arrow Flight SQL service is started.");
        } catch (IOException e) {
            LOG.error("Start Arrow Flight SQL service failed.", e);
            return false;
        }
        return true;
    }

    public void stop() {
        if (running) {
            running = false;
            try {
                flightServer.close();
            } catch (InterruptedException e) {
                LOG.warn("close Arrow ‘’light SQL server failed.", e);
            }
        }
    }
}
