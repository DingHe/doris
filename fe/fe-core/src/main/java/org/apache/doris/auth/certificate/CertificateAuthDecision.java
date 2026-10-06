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

import org.apache.doris.analysis.UserIdentity;

// 专门用于封装 TLS 客户端证书（mTLS）认证决策结果的不可变（Immutable）数据模型类
// 在 Doris 支持客户端 TLS 证书认证（双向认证）的架构中，系统从客户端 HTTP 请求（或 MySQL 协议连接）中提取证书后，会将其交由证书认证服务进行实时验证。
// CertificateAuthDecision 的作用是统一并标准化证书认证后的输出结果：
// 结果状态表达：明确告知上层调用方（如 BaseController 或其他认证控制器）本次证书校验的结果是通过（VERIFIED）、拒绝（REJECT）还是不适用/未提供证书（NOT_APPLICABLE）。
// 凭证与策略传递：如果证书验证通过，它会将证书绑定的 Doris 用户身份（UserIdentity）以及是否可以免密登录（skipPassword）等策略信息安全地传递给后续流程。
public final class CertificateAuthDecision {
    // 定义了证书认证过程的三种可能结论：
    public enum Outcome {
        // NOT_APPLICABLE（不适用）：表示本次请求未提供客户端证书，或者系统未开启证书认证功能。此时认证流程应该回退（Fallback）到普通的账号密码认证。
        NOT_APPLICABLE,
        // REJECT（拒绝）：表示客户端提供了证书，但证书未能通过验证（例如证书已过期、CA 不受信任、证书与请求的用户名/IP 不匹配等）。此时应直接终止认证并拒绝访问。
        REJECT,
        // VERIFIED（通过）：表示客户端证书成功通过了校验，证书是合法且匹配的。
        VERIFIED
    }
    // 认证结果枚举。标明本次决策的具体结论（NOT_APPLICABLE / REJECT / VERIFIED）。声明为 final，初始化后不可变更。
    private final Outcome outcome;
    // 绑定的用户身份。当 outcome == VERIFIED 时，保存根据证书解析出来的 Doris 用户身份对象（如 'admin'@'%'）；在其他结果状态下通常为 null。
    private final UserIdentity userIdentity;
    // 免密标志。当证书验证通过时，标明是否允许用户直接免除密码校验登录。true 表示仅靠证书即可完成登录，false 表示虽然证书合法但后续仍需校验密码。
    private final boolean skipPassword;
    private final String errorMessage;

    private CertificateAuthDecision(Outcome outcome, UserIdentity userIdentity,
            boolean skipPassword, String errorMessage) {
        this.outcome = outcome;
        this.userIdentity = userIdentity;
        this.skipPassword = skipPassword;
        this.errorMessage = errorMessage;
    }
    // 创建一个表示“不适用 / 未提供证书”的决策对象。
    public static CertificateAuthDecision notApplicable() {
        return new CertificateAuthDecision(Outcome.NOT_APPLICABLE, null, false, null);
    }
    // 创建一个表示“证书校验被拒绝”的决策对象。
    public static CertificateAuthDecision reject(String errorMessage) {
        return new CertificateAuthDecision(Outcome.REJECT, null, false, errorMessage);
    }

    public static CertificateAuthDecision verified(UserIdentity userIdentity, boolean skipPassword) {
        return new CertificateAuthDecision(Outcome.VERIFIED, userIdentity, skipPassword, null);
    }

    public Outcome getOutcome() {
        return outcome;
    }

    public boolean isVerified() {
        return outcome == Outcome.VERIFIED;
    }

    public boolean isReject() {
        return outcome == Outcome.REJECT;
    }

    public boolean isNotApplicable() {
        return outcome == Outcome.NOT_APPLICABLE;
    }

    public UserIdentity getUserIdentity() {
        return userIdentity;
    }

    public boolean isSkipPassword() {
        return skipPassword;
    }

    public boolean shouldSkipPasswordVerification() {
        return isVerified() && skipPassword;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}
