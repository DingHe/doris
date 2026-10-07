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

package org.apache.doris.httpv2.controller;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.auth.certificate.CertificateAuthDecision;
import org.apache.doris.auth.certificate.CertificateRuntimeAuthFactory;
import org.apache.doris.auth.certificate.CertificateRuntimeAuthService;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.httpv2.HttpAuthManager;
import org.apache.doris.httpv2.HttpAuthManager.SessionValue;
import org.apache.doris.httpv2.exception.UnauthorizedException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.service.FrontendOptions;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.base64.Base64;
import io.netty.util.CharsetUtil;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.UUID;

// BaseController 是 Apache Doris Frontend (FE) 模块中 HTTP v2 接口的核心基类，主要负责处理 HTTP 请求的 身份认证 (Authentication)、权限校验 (Authorization)、Session/Cookie 管理 以及提供 Controller 层通用的辅助工具方法
// BaseController 类的作用
// 统一认证与鉴权：处理 Basic Auth（HTTP 协议头中的用户名密码/证书认证）和 Cookie/Session 认证，校验用户是否有权限访问对应接口或资源（全局权限、数据库权限、表级权限）。
// Session 与 Cookie 管理：为认证成功的请求创建、维护以及更新 Session Cookie (PALO_SESSION_ID)。
// 上下文初始化：在处理 HTTP 请求时，建立当前线程的 ConnectContext（连接上下文），保证权限检查与日志记录能够正确获取当前用户和环境信息。
// 云原生/存算分离模式支持：在 Doris Cloud 模式下，检查存算分离实例是否处于过期/欠费（Overdue）状态。
public class BaseController {

    private static final Logger LOG = LogManager.getLogger(BaseController.class);
    // 证书运行时认证服务实例（单例），用于处理 TLS 客户端证书（mTLS）的认证判定
    private static final CertificateRuntimeAuthService CERT_RUNTIME_AUTH_SERVICE =
            CertificateRuntimeAuthFactory.getInstance();
    // Cookie 键名常量，默认值为 "PALO_SESSION_ID"，用于标识 HTTP 会话
    public static final String PALO_SESSION_ID = "PALO_SESSION_ID";
    // Session/Cookie 的过期时间常量，单位为秒，值为 3600 * 24（即 1 天）。
    private static final int PALO_SESSION_EXPIRED_TIME = 3600 * 24; // one day

    public void checkAuthWithCookie(HttpServletRequest request, HttpServletResponse response) {
        checkWithCookie(request, response, true);
    }
    // Apache Doris FE（Frontend）HTTP Controller 层中最为关键的认证与鉴权核心入口函数。
    // 实现了 双重认证机制：优先检查请求头中的 Authorization（即 Basic Auth 或证书认证），如果不存在，则降级校验 Cookie（Session 机制）
    // boolean checkAuth 控制是否开启默认的高级别权限与状态校验
    public ActionAuthorizationInfo checkWithCookie(HttpServletRequest request,
            HttpServletResponse response, boolean checkAuth) {
        // First we check if the request has Authorization header.
        // 第一阶段：优先处理 Authorization 请求头（Basic Auth / 证书路径）
        String encodedAuthString = request.getHeader("Authorization");
        if (encodedAuthString != null) {
            // If has Authorization header, check auth info
            ActionAuthorizationInfo authInfo = getAuthorizationInfo(request);
            // 调用 checkPassword 方法校验身份。内部会先尝试 TLS 客户端证书认证，若无证书或需密码，
            // 则调用 Doris 系统的密码校验引擎（checkPlainPassword）比对密码。校验通过后，返回 Doris 内部确定的用户身份标识 currentUser（UserIdentity 对象）。
            UserIdentity currentUser = checkPassword(authInfo, request);
            // Callers do privilege checks on the returned authInfo, so the resolved identity must be
            // carried back out. Leaving it null makes every such check throw NPE.
            authInfo.userIdentity = currentUser;
            // 当处于云原生/存算分离模式（isCloudMode() 为 true）且开启了默认校验（checkAuth 为 true）时
            if (Config.isCloudMode() && checkAuth) {
                // 检查当前云实例/仓库状态是否已经欠费过期（OVERDUE）。若已过期且非 root 用户，直接抛出 UnauthorizedException
                checkInstanceOverdue(currentUser);
                // 检查当前用户是否拥有 ADMIN（管理员）或 NODE（节点管理）全局权限。若没有则抛出权限不足异常
                checkGlobalAuth(currentUser, PrivPredicate.ADMIN_OR_NODE);
            }
            // 为本次通过 Header 认证成功的用户创建并保存会话信息
            // 调用 addSession 方法：在内存/服务端缓存（HttpAuthManager）中生成一个随机的 Session ID 与 value 建立映射，
            // 同时构造一个名为 PALO_SESSION_ID 的 Cookie（设置 HttpOnly、过期时间 1 天等），通过 response 返回给客户端。这样客户端下次请求只需携带此 Cookie 即可。
            SessionValue value = new SessionValue();
            value.currentUser = currentUser;
            value.password = authInfo.password;
            addSession(request, response, value);
            // 构造并初始化当前处理线程的 ConnectContext（连接上下文）
            ConnectContext ctx = new ConnectContext();
            ctx.setRemoteIP(authInfo.remoteIp);
            ctx.setCurrentUserIdentity(currentUser);
            ctx.setEnv(Env.getCurrentEnv());
            ctx.setThreadLocalInfo();
            if (LOG.isDebugEnabled()) {
                LOG.debug("check auth without cookie success for user: {}, thread: {}",
                        currentUser, Thread.currentThread().getId());
            }
            return authInfo;
        }

        // No Authorization header, check cookie
        // 第二阶段：降级处理 Cookie 认证路径
        // 从请求的 Cookie 中寻找 PALO_SESSION_ID
        // 如果 Cookie 有效，会自动刷新 Cookie 的过期时间（续期），初始化线程 ConnectContext，并返回构造好的 ActionAuthorizationInfo。
        ActionAuthorizationInfo authInfo = checkCookie(request, response, checkAuth);
        if (authInfo == null) {
            throw new UnauthorizedException("Cookie is invalid");
        }
        return authInfo;
    }

    protected void addSession(HttpServletRequest request, HttpServletResponse response, SessionValue value) {
        String key = UUID.randomUUID().toString();
        Cookie cookie = new Cookie(PALO_SESSION_ID, key);
        if (Config.enable_https) {
            cookie.setSecure(true);
        } else {
            cookie.setSecure(false);
        }
        cookie.setMaxAge(PALO_SESSION_EXPIRED_TIME);
        cookie.setPath("/");
        cookie.setHttpOnly(true);
        response.addCookie(cookie);
        if (LOG.isDebugEnabled()) {
            LOG.debug("add session cookie: {} {}", PALO_SESSION_ID, key);
        }
        HttpAuthManager.getInstance().addSessionValue(key, value);
    }

    private ActionAuthorizationInfo checkCookie(HttpServletRequest request, HttpServletResponse response,
            boolean checkAuth) {
        List<String> sessionIds = getCookieValues(request, PALO_SESSION_ID, response);
        if (sessionIds.isEmpty()) {
            return null;
        }

        HttpAuthManager authMgr = HttpAuthManager.getInstance();
        SessionValue sessionValue = authMgr.getSessionValue(sessionIds);
        if (sessionValue == null) {
            return null;
        }

        if (checkAuth && !Env.getCurrentEnv().getAccessManager().checkGlobalPriv(sessionValue.currentUser,
                PrivPredicate.ADMIN_OR_NODE)) {
            // need to check auth and check auth failed
            return null;
        }

        if (Config.isCloudMode() && checkAuth && !sessionValue.currentUser.isRootUser()
                && ((CloudSystemInfoService) Env.getCurrentSystemInfo()).getInstanceStatus()
                == Cloud.InstanceInfoPB.Status.OVERDUE) {
            return null;
        }


        updateCookieAge(request, PALO_SESSION_ID, PALO_SESSION_EXPIRED_TIME, response);

        ConnectContext ctx = new ConnectContext();
        ctx.setRemoteIP(request.getRemoteHost());
        ctx.setCurrentUserIdentity(sessionValue.currentUser);
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setThreadLocalInfo();
        if (LOG.isDebugEnabled()) {
            LOG.debug("check cookie success for user: {}, thread: {}",
                    sessionValue.currentUser, Thread.currentThread().getId());
        }
        ActionAuthorizationInfo authInfo = new ActionAuthorizationInfo();
        authInfo.fullUserName = sessionValue.currentUser.getQualifiedUser();
        authInfo.remoteIp = request.getRemoteHost();
        authInfo.password = sessionValue.password;
        authInfo.userIdentity = sessionValue.currentUser;
        return authInfo;
    }

    public List<String> getCookieValues(HttpServletRequest request, String cookieName, HttpServletResponse response) {
        Cookie[] cookies = request.getCookies();
        List<String> sessionIds = Lists.newArrayList();
        if (cookies != null) {
            for (Cookie cookie : cookies) {
                if (cookie.getName() != null && cookie.getName().equals(cookieName)) {
                    String sessionId = cookie.getValue();
                    sessionIds.add(sessionId);
                }
            }
        }
        return sessionIds;
    }

    public void updateCookieAge(HttpServletRequest request, String cookieName, int age, HttpServletResponse response) {
        Cookie[] cookies = request.getCookies();
        for (Cookie cookie : cookies) {
            if (cookie.getName() != null && cookie.getName().equals(cookieName)) {
                cookie.setMaxAge(age);
                cookie.setPath("/");
                cookie.setHttpOnly(true);
                if (Config.enable_https) {
                    cookie.setSecure(true);
                } else {
                    cookie.setSecure(false);
                }
                response.addCookie(cookie);
            }
        }
    }

    public static class ActionAuthorizationInfo {
        // 解析出的完整用户名（若包含 @ 则截取用户名部分）
        public String fullUserName;
        // 客户端或请求来源的 IP 地址
        public String remoteIp;
        // 用户登录密码
        public String password;
        // 集群名称/标识（用于集群隔离上下文）
        public String cluster;
        // 经过 Doris 认证引擎校验后确定的用户身份标识对象（UserIdentity），方便后续传递给权限引擎
        public UserIdentity userIdentity;  // Add this field for convenient parameter passing

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("user: ").append(fullUserName).append(", remote ip: ").append(remoteIp);
            sb.append(", password: ").append("********").append(", cluster: ").append(cluster);
            return sb.toString();
        }
    }

    /**
     * The overdue-warehouse fence for handlers that call checkWithCookie(.., false).
     *
     * checkWithCookie's `checkAuth` flag gates two unrelated things at once: the global
     * ADMIN_OR_NODE requirement and, in cloud mode, the overdue check. A handler that passes false
     * is saying "I do my own, narrower authorization" -- it is not saying "serve this from an
     * overdue warehouse". Such a handler calls this to get the fence back without the ADMIN
     * requirement.
     *
     * This is deliberately opt-in per handler rather than unconditional inside checkWithCookie:
     * /api/query also passes false, but it hands the statement to a real JDBC session that
     * enforces the overdue state itself and reports it as a common error. Moving that rejection
     * up to this layer would silently change that endpoint's response from COMMON_ERROR to
     * UNAUTHORIZED.
     */
    protected void checkInstanceOverdueIfCloud(UserIdentity currentUser) {
        if (Config.isCloudMode()) {
            checkInstanceOverdue(currentUser);
        }
    }

    protected void checkInstanceOverdue(UserIdentity currentUsr) {
        Cloud.InstanceInfoPB.Status s = ((CloudSystemInfoService) Env.getCurrentSystemInfo()).getInstanceStatus();
        if (!currentUsr.isRootUser()
                && s == Cloud.InstanceInfoPB.Status.OVERDUE) {
            LOG.warn("this warehouse is overdue root:{}, status:{}", currentUsr.isRootUser(), s);
            throw new UnauthorizedException("The warehouse is overdue!");
        }
    }

    protected void checkGlobalAuth(UserIdentity currentUser, PrivPredicate predicate) throws UnauthorizedException {
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(currentUser, predicate)) {
            throw new UnauthorizedException("Access denied; you need (at least one of) the "
                    + predicate.getPrivs().toString() + " privilege(s) for this operation");
        }
    }
    // 针对当前请求发起的用户，检查其是否拥有对指定数据库执行特定操作的权限。 如果用户缺乏对应权限，则直接抛出 UnauthorizedException 异常，中断后续业务逻辑并向客户端返回“无权限访问（403 Access Denied）”的响应。
    // currentUser 当前发起请求的用户身份。包含用户名（username）和客户端 Host/IP 信息（host），通常从 Session 或 HTTP Header 中解析提取。
    // db 目标数据库名称。即当前操作尝试访问或修改的 Database 名称。
    // predicate 权限判定谓词（所需权限断言）。定义了本次操作要求具备的具体权限类型（如 PrivPredicate.SELECT、PrivPredicate.LOAD、PrivPredicate.ALTER 等）。
    protected void checkDbAuth(UserIdentity currentUser, String db, PrivPredicate predicate)
            throws UnauthorizedException {
        if (!Env.getCurrentEnv().getAccessManager()
                .checkDbPriv(currentUser, InternalCatalog.INTERNAL_CATALOG_NAME, db, predicate)) {
            throw new UnauthorizedException("Access denied; you need (at least one of) the "
                    + predicate.getPrivs().toString() + " privilege(s) for this operation");
        }
    }

    protected void checkTblAuth(UserIdentity currentUser, String db, String tbl, PrivPredicate predicate)
            throws UnauthorizedException {
        checkTblAuth(currentUser, InternalCatalog.INTERNAL_CATALOG_NAME, db, tbl, predicate);
    }

    protected void checkTblAuth(UserIdentity currentUser, String catalog, String db, String tbl,
            PrivPredicate predicate)
            throws UnauthorizedException {
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(currentUser, catalog, db, tbl, predicate)) {
            throw new UnauthorizedException("Access denied; you need (at least one of) the "
                    + predicate.getPrivs().toString() + " privilege(s) for this operation");
        }
    }

    // return currentUserIdentity from Doris auth
    // 用于对用户身份进行实质性认证校验的核心受保护（protected）方法。它实现了双重认证逻辑：优先校验 TLS 客户端证书（mTLS），再根据证书决策结果校验明文密码，最终解析出 Doris 内部唯一的 UserIdentity
    // ActionAuthorizationInfo authInfo  授权信息数据载体对象。包含此前从 HTTP Header 解析出来的 fullUserName（用户名）、password（密码）以及 remoteIp（来源 IP）
    protected UserIdentity checkPassword(ActionAuthorizationInfo authInfo, HttpServletRequest request)
            throws UnauthorizedException {
        // 第一阶段：尝试 TLS 客户端证书认证（mTLS 决策）
        // 尝试从 request 中获取客户端 X509 证书，并提交给 Doris 的证书运行时认证服务进行校验
        CertificateAuthDecision certDecision = tryCertificateAuth(authInfo, request);
        // 如果配置了高信任度的证书认证且决策表明可以跳过密码校验（shouldSkipPasswordVerification() 为 true），
        // 则无需再比对用户密码，直接返回证书中绑定的 certDecision.getUserIdentity()，提前完成认证。
        if (certDecision.shouldSkipPasswordVerification()) {
            return certDecision.getUserIdentity();
        }
        // 第二阶段：根据证书校验状态执行密码比对
        List<UserIdentity> currentUser = Lists.newArrayList();
        try {
            // 分支判断证书是否已通过验证（但仍需密码双重验证的情况）
            if (certDecision.isVerified()) {
                // 直接使用证书中提取并判定的 certDecision.getUserIdentity() 和 authInfo.password 进行比对，避免重新通过用户名和 IP 去搜索匹配的用户规则，校验成功后将确定的身份添加到 currentUser 列表中。
                Env.getCurrentEnv().getAuth().checkPlainPasswordForUserIdentity(
                        certDecision.getUserIdentity(), authInfo.password, currentUser);
            } else {
                // 分支条件，处理证书未提供或证书未启用的传统认证路径。
                // 传入用户名（fullUserName）、客户端 IP（remoteIp）和密码（password）。认证引擎会根据用户名和客户端 IP 匹配最合适的 Doris 用户账号策略（处理 Host 匹配规则，如 % 或特定网段），校验密码并填充 currentUser。
                Env.getCurrentEnv().getAuth().checkPlainPassword(authInfo.fullUserName,
                        authInfo.remoteIp, authInfo.password, currentUser);
            }
        } catch (AuthenticationException e) {
            throw new UnauthorizedException(e.formatErrMsg());
        }
        Preconditions.checkState(currentUser.size() == 1);
        return currentUser.get(0);
    }
    // 核心职责是：从 HttpServletRequest 中解析出客户端传入的 Authorization 头（Basic Auth），并将其转换为内部的 ActionAuthorizationInfo 数据对象；
    // 若解析失败，则抛出未授权异常（UnauthorizedException）。
    public ActionAuthorizationInfo getAuthorizationInfo(HttpServletRequest request)
            throws UnauthorizedException {
        // 实例化一个空的 ActionAuthorizationInfo 对象 authInfo
        ActionAuthorizationInfo authInfo = new ActionAuthorizationInfo();
        // 执行真正的请求头提取与 Base64 解密逻辑，并对返回的布尔值进行逻辑非（!）判断。
        if (!parseAuthInfo(request, authInfo)) {
            LOG.info("parse auth info failed, Authorization header {}, url {}",
                    request.getHeader("Authorization"), request.getRequestURI());
            throw new UnauthorizedException("Need auth information.");
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("get auth info: {}", authInfo);
        }
        return authInfo;
    }
    // 负责底层 HTTP Basic Authentication（基本认证）头的提取、Base64 解密以及用户名和密码的拆分处理。
    private boolean parseAuthInfo(HttpServletRequest request, ActionAuthorizationInfo authInfo) {
        // 提取并校验 HTTP Header 格式
        // 从 HTTP 请求头中获取名为 "Authorization" 的字符串（例：Basic YWRtaW46MTIzNDU2）
        String encodedAuthString = request.getHeader("Authorization");
        if (Strings.isNullOrEmpty(encodedAuthString)) {
            return false;
        }
        // 按一个或多个连续空格（正则 \s+）对请求头字符串进行拆分
        // 标准的 HTTP Basic Auth 格式为 Basic <base64-encoded-string>，按空格拆分后应包含两部分：认证类型（Basic）和 Base64 密文。
        String[] parts = encodedAuthString.split("\\s+");
        // 校验拆分后的数组长度是否恰好为 2。若不是（说明格式不对，如缺失 Base64 部分或空格数量不对），直接返回 false
        if (parts.length != 2) {
            return false;
        }
        encodedAuthString = parts[1];
        // buf：用于存放 Base64 密文数据的 Buffer
        ByteBuf buf = null;
        // decodeBuf：用于存放 Base64 解密后的明文二进制 Buffer
        ByteBuf decodeBuf = null;
        try {
            // 获取 Base64 字符串的字节数组
            buf = Unpooled.copiedBuffer(ByteBuffer.wrap(encodedAuthString.getBytes()));

            // The authString is a string connecting user-name and password with
            // a colon(':')
            // 对 buf 进行 Base64 解码，将解码后的明文数据存入新的 ByteBuf 对象 decodeBuf 中
            decodeBuf = Base64.decode(buf);
            String authString = decodeBuf.toString(CharsetUtil.UTF_8);
            // Note that password may contain colon, so can not simply use a
            // colon to split.
            // 解析用户名与密码
            // 查找字符串中第一个冒号 : 出现的位置索引。
            int index = authString.indexOf(":");
            // 截取从位置 0 到 index - 1 的子字符串，得到完整的用户名，并赋值给 authInfo.fullUserName。
            authInfo.fullUserName = authString.substring(0, index);
            // 将解析出的用户名按 @ 拆分。如果正好拆分为 2 部分（说明带了 @cluster 后缀），则提取 @ 前面的部分作为纯用户名并覆盖赋值给 authInfo.fullUserName
            final String[] elements = authInfo.fullUserName.split("@");
            if (elements != null && elements.length == 2) {
                authInfo.fullUserName = elements[0];
            }
            // 截取从 index + 1 开始到末尾的所有字符作为密码，赋值给 authInfo.password
            authInfo.password = authString.substring(index + 1);
            authInfo.remoteIp = request.getRemoteAddr();
        } finally {
            // release the buf and decode buf after using Unpooled.copiedBuffer
            // or it will get memory leak
            if (buf != null) {
                buf.release();
            }

            if (decodeBuf != null) {
                decodeBuf.release();
            }
        }
        return true;
    }
    // 从请求中提取客户端证书，连同用户名和来源 IP 提交给证书运行时认证服务进行实时判定；如果证书验证结果为“拒绝（Reject）”，则直接抛出未授权异常中断流程；若结果为“允许”或“跳过/未校验”，则返回具体的决策对象（CertificateAuthDecision）供上层决策后续是否需要校验密码。
    protected CertificateAuthDecision tryCertificateAuth(ActionAuthorizationInfo authInfo, HttpServletRequest request)
            throws UnauthorizedException {
        // 调用证书运行时认证服务单例（CERT_RUNTIME_AUTH_SERVICE，即 CertificateRuntimeAuthService）的 authenticateLive 方法进行实时证书认证与决策计算。
        // authInfo.fullUserName：从 HTTP 请求头解析出的用户名。
        // authInfo.remoteIp：客户端的 IP 地址。
        // 从 request 的 Servlet 属性（如 jakarta.servlet.request.X509Certificate）中提取客户端 X509 证书对象（若无证书则返回 null）。
        CertificateAuthDecision decision = CERT_RUNTIME_AUTH_SERVICE.authenticateLive(
                authInfo.fullUserName, authInfo.remoteIp, getClientCertificate(request));
        // 检查认证决策对象中的状态标志。如果 decision.isReject() 为 true，说明客户端提供了证书但证书校验未通过（如证书过期、CA 不信任、域名/IP 不匹配等），属于明确拒绝访问的情况。
        if (decision.isReject()) {
            throw new UnauthorizedException(
                    decision.getErrorMessage() == null ? "TLS certificate verification failed"
                            : decision.getErrorMessage());
        }
        return decision;
    }
    // 负责从 HTTP 请求上下文中提取客户端的双向 TLS 证书（X.509 证书）
    protected X509Certificate getClientCertificate(HttpServletRequest request) {
        // 优先尝试从 request 中获取标准属性 "jakarta.servlet.request.X509Certificate"。
        // 背景/原理：在 Jakarta EE 9 及以上版本（如 Spring Boot 3.x / Spring 6.x 所使用的 Servlet 5.0+ 规范）中，包名全面由 javax.* 迁移到了 jakarta.*。Web 容器在 SSL 握手成功后会将客户端证书数组放入此属性名下。
        Object value = request.getAttribute("jakarta.servlet.request.X509Certificate");
        if (!(value instanceof X509Certificate[])) {
            // 如果获取结果为 null 或不是证书数组类型（说明当前运行环境可能还是旧版的 Java EE / Servlet 4.0 及以下版本），则回退尝试读取旧版的规范属性名 "javax.servlet.request.X509Certificate"
            value = request.getAttribute("javax.servlet.request.X509Certificate");
        }
        // 二次校验。如果新旧两个属性名均未提取到合法的 X509Certificate[] 数组类型（例如请求走的是普通 HTTP、单向 HTTPS、或者客户端未提供证书），说明本次请求不包含有效的 TLS 客户端证书，直接返回 null。
        if (!(value instanceof X509Certificate[])) {
            return null;
        }
        X509Certificate[] certs = (X509Certificate[]) value;
        return certs.length == 0 ? null : certs[0];
    }

    protected int checkIntParam(String strParam) {
        return Integer.parseInt(strParam);
    }

    protected long checkLongParam(String strParam) {
        return Long.parseLong(strParam);
    }

    protected String getCurrentFrontendURL() {
        if (Config.enable_https) {
            // this could be the result of redirection.
            return "https://" + NetUtils
                    .getHostPortInAccessibleFormat(FrontendOptions.getLocalHostAddress(), Config.https_port);
        } else {
            return "http://" + NetUtils
                    .getHostPortInAccessibleFormat(FrontendOptions.getLocalHostAddress(), Config.http_port);
        }
    }
}
