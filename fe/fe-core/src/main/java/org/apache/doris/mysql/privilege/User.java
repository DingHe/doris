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

package org.apache.doris.mysql.privilege;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherException;
import org.apache.doris.persist.gson.GsonPostProcessable;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
// 专门用于抽象与表示数据库用户身份认证与账号凭证信息的核心类
// 在 Doris 的权限与认证体系中，客户端连接登录时的身份验证（Authentication）主要依靠 User 类进行处理。它与表示“操作许可与资源授权”的权限控制逻辑不同，专门专注于账号匹配、密码校验、Host 域名/IP 模糊匹配以及域名解析（Domain Resolver）。
// 标识用户账号与凭证：封装了 UserIdentity（用户标识，如 'jack'@'192.168.1.%'）以及加盐/哈希处理后的 Password（密码）。
// 连接 Host 模式匹配：维护针对用户主机地址（Host）的通配符/正则表达式正则匹配器（hostPattern），支持如 '%'（任意 Host）或 IP 网段的模糊匹配登录。
//支持动态域名解析（Domain Resolver）：Doris 支持按域名授权（如 'jack'@'%.example.com'）。针对这类账号，FE 后台线程（Domain Resolver）会将域名异步解析为具体 IP 对应的 UserIdentity。User 类保留了原始的域名用户标识与解析后的具体 IP 用户标识（domainUserIdentity）。
public class User implements Comparable<User>, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(User.class);
    // 当前用户对象的身份标识（包含用户名 username 和主机 host），带有 Gson 的 @SerializedName 注解参与序列化。
    @SerializedName(value = "userIdentity")
    private UserIdentity userIdentity;
    // 基于域名解析生成的具体用户标识（非序列化持久化字段）。
    // 当该账号是由后台 DomainResolver（域名解析器）将域名解析为 IP 后动态生成的，此属性记录原始带有域名的 UserIdentity 标识。
    private UserIdentity domainUserIdentity;
    // 标识当前 User 实例是否是由后台域名解析器动态创建/设置的。
    // 区分普通手动创建的用户与后台通过域名 IP 映射动态生成的临时用户。
    private boolean isSetByDomainResolver = false;
    // 主机名/IP 通配符匹配器对象（非直接序列化字段，由 gsonPostProcess() 动态构建）。
    // 用于在客户端发起 MySQL 协议连接时，判断客户端来源 IP 是否与当前用户定义的 Host 规则匹配（如 '192.168.1.%'）。
    // host is not case sensitive
    protected PatternMatcher hostPattern;
    // 标识当前用户的 Host 是否为任意主机通配符 "%"（非序列化字段）。
    protected boolean isAnyHost = false;
    // 加密/哈希后的密码对象
    // 存储用户密码的哈希结果（如 MySQL 4.1+ 密文散列），供客户端握手认证时进行密文比对。
    @SerializedName(value = "password")
    private Password password;
    // 云原生/云上版本（Doris Cloud）下的原始用户全局唯一 ID，带有序列化注解。
    @SerializedName(value = "userid")
    private String origUserId = "";

    @SerializedName(value = "comment")
    private String comment;

    public User() {
    }

    public User(UserIdentity userIdent, byte[] pwd, boolean setByResolver, UserIdentity domainUserIdent,
            PatternMatcher hostPattern, String comment) {
        this.isAnyHost = userIdent.getHost().equals(UserManager.ANY_HOST);
        this.userIdentity = userIdent;
        this.password = new Password(pwd);
        this.hostPattern = hostPattern;
        this.isSetByDomainResolver = setByResolver;
        if (setByResolver) {
            Preconditions.checkNotNull(domainUserIdent);
            this.domainUserIdentity = domainUserIdent;
        }
        this.comment = comment;
    }

    // ====== CLOUD ======
    public String getUserId() {
        return origUserId;
    }

    public void setUserId(String userId) {
        this.origUserId = userId;
    }
    // ====== CLOUD ======


    public Password getPassword() {
        return password;
    }

    public void setPassword(Password password) {
        this.password = password;
    }

    public void setPassword(byte[] password) {
        this.password = new Password(password);
    }

    public UserIdentity getUserIdentity() {
        return userIdentity;
    }

    public void setUserIdentity(UserIdentity userIdentity) {
        this.userIdentity = userIdentity;
    }

    public UserIdentity getDomainUserIdentity() {
        if (isSetByDomainResolver()) {
            return domainUserIdentity;
        } else {
            return userIdentity;
        }

    }

    public void setDomainUserIdentity(UserIdentity domainUserIdentity) {
        this.domainUserIdentity = domainUserIdentity;
    }

    public boolean isSetByDomainResolver() {
        return isSetByDomainResolver;
    }

    public void setSetByDomainResolver(boolean setByDomainResolver) {
        isSetByDomainResolver = setByDomainResolver;
    }

    public PatternMatcher getHostPattern() {
        return hostPattern;
    }

    public void setHostPattern(PatternMatcher hostPattern) {
        this.hostPattern = hostPattern;
    }

    public boolean isAnyHost() {
        return isAnyHost;
    }

    public void setAnyHost(boolean anyHost) {
        isAnyHost = anyHost;
    }

    public boolean hasPassword() {
        return password != null && password.getPassword() != null && password.getPassword().length != 0;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    @Override
    public int compareTo(@NotNull User o) {
        return -userIdentity.getHost().compareTo(o.userIdentity.getHost());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("userIdentity: ").append(userIdentity).append(", isSetByDomainResolver: ")
                .append(isSetByDomainResolver).append(", domainUserIdentity: ").append(domainUserIdentity)
            .append(", userId: ").append(origUserId);
        return sb.toString();
    }

    @Override
    public void gsonPostProcess() throws IOException {
        try {
            hostPattern = PatternMatcher
                    .createMysqlPattern(userIdentity.getHost(), CaseSensibility.HOST.getCaseSensibility());
        } catch (PatternMatcherException e) {
            // will not happen
            LOG.warn("readFields error,", e);
        }
        isAnyHost = userIdentity.getHost().equals(UserManager.ANY_HOST);
    }
}
