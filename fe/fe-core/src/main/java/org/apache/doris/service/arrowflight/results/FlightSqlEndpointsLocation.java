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

package org.apache.doris.service.arrowflight.results;

import org.apache.doris.analysis.Expr;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TUniqueId;

import java.util.ArrayList;

// 封装和管理 Arrow Flight SQL 查询结果在后端（BE）分布式的存储与访问位置信息：
// 定位数据分片：在 Doris 执行分布式查询时，查询结果可能分布在不同的后端（BE）节点上。该类记录了每个结果分片（Fragment Instance）所在的具体后端节点信息。
// 桥接内部与外部网络：同时维护了后端节点的内部 RPC 服务地址（用于 FE 与 BE 之间的通信，如抓取 Schema）以及 Arrow Flight 服务地址（供客户端直连拉取列存数据）。
// 支撑多端并行拉取：为 Arrow Flight SQL 客户端的 GetFlightInfo 和 DoGet 阶段提供精确的路由和端点（Endpoint）定位，确保客户端能够高效、并行地从对应的 BE 节点拉取列式数据。
public class FlightSqlEndpointsLocation {
    // 片段实例唯一标识（Fragment Instance ID）。用于精确标识该结果集对应的是哪一个具体在 BE 上运行的查询执行实例。
    private TUniqueId finstId;
    // 后端 BE 节点上提供 Arrow Flight 服务监听的网络地址（包含主机名和端口），客户端后续可以通过该地址直接建立 Arrow Flight 连接并拉取数据。
    private TNetworkAddress resultFlightServerAddr;
    // 后端 BE 节点的内部服务网络地址（Thrift/RPC 地址）。主要用于 FE 节点与该 BE 节点进行内部通信（例如在前面分析的 fetchArrowFlightSchema 中，FE 通过此地址向 BE 发送抓取 Schema 的内部 RPC 请求）。
    private TNetworkAddress resultInternalServiceAddr;
    // 对外的公共访问网络地址。通常在通过 RPC 从后端获取到更准确的外部 IP/端口后，通过 setResultPublicAccessAddr 动态填充，供客户端进行公网或特定网络环境下的直连访问。
    private TNetworkAddress resultPublicAccessAddr;
    // 查询结果的输出表达式列表（Output Expressions）。在 FE 抓取并校验 Schema 时，用于与 Arrow 返回的字段向量数量进行比对，确保数据结构的一致性与正确性。
    private ArrayList<Expr> resultOutputExprs;

    public FlightSqlEndpointsLocation(TUniqueId finstId, TNetworkAddress resultFlightServerAddr,
            TNetworkAddress resultInternalServiceAddr, ArrayList<Expr> resultOutputExprs) {
        this.finstId = finstId;
        this.resultFlightServerAddr = resultFlightServerAddr;
        this.resultInternalServiceAddr = resultInternalServiceAddr;
        this.resultPublicAccessAddr = new TNetworkAddress();
        this.resultOutputExprs = resultOutputExprs;
    }

    public TUniqueId getFinstId() {
        return finstId;
    }

    public TNetworkAddress getResultFlightServerAddr() {
        return resultFlightServerAddr;
    }

    public TNetworkAddress getResultInternalServiceAddr() {
        return resultInternalServiceAddr;
    }

    public void setResultPublicAccessAddr(TNetworkAddress resultPublicAccessAddr) {
        this.resultPublicAccessAddr = resultPublicAccessAddr;
    }

    public TNetworkAddress getResultPublicAccessAddr() {
        return resultPublicAccessAddr;
    }

    public ArrayList<Expr> getResultOutputExprs() {
        return resultOutputExprs;
    }
}
