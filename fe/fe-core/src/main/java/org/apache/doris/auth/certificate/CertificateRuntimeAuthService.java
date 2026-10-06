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

package org.apache.doris.auth.certificate;

import java.security.cert.X509Certificate;

// CertificateRuntimeAuthService 接口的作用是定义统一的 TLS 客户端证书运行时认证契约：
// 解耦认证逻辑：它将“如何校验客户端证书”、“如何将证书信息与 Doris 内部的 UserIdentity 进行映射”以及“是否允许免密”等具体实现逻辑封装在实现类中，与上层的 Controller 或 Protocol 处理器（如 BaseController）解耦。
// 支持多种认证场景：接口同时提供了直连模式（Live）与转发模式（Forwarded）两种认证规范，既能处理客户端直接建立 TLS 连接的场景，也能处理请求经由 Master/Follower 节点转发时的证书身份传递场景。
// 输出标准化决策：方法均返回上一问题中介绍的 CertificateAuthDecision 对象，将认证结果（VERIFIED / REJECT / NOT_APPLICABLE）标准化后传递给上层。
public interface CertificateRuntimeAuthService {
    // 处理直连/实时（Live）请求的客户端 TLS 证书认证。当客户端直接与当前 FE 节点建立 TLS 加密连接并提交请求时（如标准的 HTTP API 请求或 MySQL 客户端直连），调用此方法进行实时校验。
    CertificateAuthDecision authenticateLive(String userName, String remoteIp, X509Certificate clientCert);
    // 处理转发/代理（Forwarded）请求的客户端 TLS 证书认证。在 Doris 集群分布式架构中，某些写操作或特定 HTTP 请求可能会从 Observer/Follower 节点转发给 Master 节点处理。
    // 此时，原始的 X509Certificate 对象无法跨网络序列化传递，而是被提取并封装成轻量级的 ForwardedCertificateInfo 结构。Master 节点调用此方法对转发过来的证书元信息进行二次认证判定。
    CertificateAuthDecision authenticateForwarded(String userName, String remoteIp,
            ForwardedCertificateInfo certInfo);
}
