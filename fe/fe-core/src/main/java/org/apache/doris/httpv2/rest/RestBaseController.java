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

package org.apache.doris.httpv2.rest;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.HttpURLUtil;
import org.apache.doris.common.util.InternalHttpsUtils;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.httpv2.controller.BaseController;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.exception.UnauthorizedException;
import org.apache.doris.master.MetaHelper;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.servlet.view.RedirectView;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.net.ssl.HttpsURLConnection;
// RestBaseController 是 Apache Doris FE（Frontend）模块中所有 RESTful API Controller 的抽象基类（继承自 BaseController）。
// 所有的 REST API 接口（如 Stream Load 导入、元数据查询、集群运维管理、镜像文件拉取等）提供了通用的框架级支持。
// 统一身份认证与线程上下文初始化：解析 HTTP 请求头的 Basic Auth 凭证，校验用户密码，并将登录用户的 UserIdentity 绑定至当前线程的 ConnectContext 中，保障鉴权上下文正确传递。
// 请求转发与重定向管理（Forward & Redirect）：
// Master 转发/重定向：Doris 属于 Master-Follower 架构，部分写操作或元数据变更请求只能由 FE Master 节点处理。该类提供了将非 Master 接收到的请求重定向（HTTP 307）或代理转发（RestTemplate）给 FE Master 的能力。
// BE 重定向：在数据导入（如 Stream Load）场景下，FE 接收到请求后需将数据流重定向到指定的 BE（Backend）节点上处理。
// HTTP 与 HTTPS 协议适配：支持配置驱动的 HTTP $\rightarrow$ HTTPS 强制重定向，以及内部节点间 HTTPS 通信时的 SSL 证书校验忽略（自签名证书适配）。
// 文件下载与元数据传输流控制：提供通用的二进制文件下载工具方法（如下载元数据 Image 镜像文件），并自动构建包含 MD5 和 Size 标头的标准 HTTP Response。
// 权限与 Token 校验：提供基于角色/全局权限的检查封装（如 Admin 权限校验）以及集群内部 Token 认证。
public class RestBaseController extends BaseController {
    // 静态常量 "ns"，用于在 REST 请求中提取 Namespace（命名空间）参数。
    protected static final String NS_KEY = "ns";
    // 静态常量 "catalog"，用于提取编目名称参数。
    protected static final String CATALOG_KEY = "catalog";
    protected static final String DB_KEY = "db";
    protected static final String TABLE_KEY = "table";
    // 静态常量 "label"，用于提取导入任务 Label 参数。
    protected static final String LABEL_KEY = "label";
    // 静态常量 "txn_id"，用于提取事务 ID 参数。
    protected static final String TXN_ID_KEY = "txn_id";
    // 静态常量 "txn_operation"，用于提取事务操作类型（如 commit/abort）参数。
    protected static final String TXN_OPERATION_KEY = "txn_operation";
    // 静态常量 "single_replica"，用于标识单副本导入/操作参数
    protected static final String SINGLE_REPLICA_KEY = "single_replica";
    protected static final String FORWARD_MASTER_UT_TEST = "forward_master_ut_test";
    private static final Logger LOG = LogManager.getLogger(RestBaseController.class);

    // 解析 HTTP 请求并执行密码身份校验，同时初始化当前线程的连接上下文（ConnectContext）
    public ActionAuthorizationInfo executeCheckPassword(HttpServletRequest request,
                                                        HttpServletResponse response) throws UnauthorizedException {
        ActionAuthorizationInfo authInfo = getAuthorizationInfo(request);
        // check password
        UserIdentity currentUser = checkPassword(authInfo, request);

        // Store UserIdentity in authInfo for convenient parameter passing
        authInfo.userIdentity = currentUser;

        // Set ConnectContext for backward compatibility
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setRemoteIP(authInfo.remoteIp);
        ctx.setCurrentUserIdentity(currentUser);
        ctx.setThreadLocalInfo();
        return authInfo;
    }
    // 重定向 URL 构建方法
    protected String buildRedirectUrl(HttpServletRequest request, TNetworkAddress addr) {
        return buildRedirectUrl(request, addr, request.getRequestURI(), request.getQueryString());
    }

    protected String buildRedirectUrl(HttpServletRequest request, TNetworkAddress addr, String requestPath,
            String queryString) {
        return buildRedirectUrl(request.getScheme(), request, addr, requestPath, queryString);
    }

    // BE's stream-load listener never terminates TLS, so BE-bound redirects must stay "http".
    // 专门构建指向 Backend（BE）节点的重定向 URL。
    // 强制将 Scheme 设置为 "http"（因为 BE 的 Stream Load 端口通常不处理 TLS 握手）
    protected String buildRedirectUrlToBackend(HttpServletRequest request, TNetworkAddress addr,
            String requestPath, String queryString) {
        return buildRedirectUrl("http", request, addr, requestPath, queryString);
    }
    // buildRedirectUrl 是 RestBaseController 中用于拼接完整 HTTP 重定向 URL 的私有核心工具方法。
    // 在 Doris 中（例如客户端发起 Stream Load 导入，需要从 FE 重定向到具体 BE，或从 Follower FE 重定向到 Master FE 时），该方法负责将认证信息、目标主机端口以及请求路径参数合成为合法的目标 URL。
    // scheme 网络传输协议。例如 "http" 或 "https"。
    // addr 重定向的目标节点网络地址（Thrift 生成的数据结构）。包含目标节点的 hostname（主机名/IP）和 port（端口号）。
    //  requestPath 请求的 URI 路径部分。例如 /api/db1/tbl1/_stream_load。
    // queryString 请求的 URL 查询参数部分。例如 label=my_label_1&columns=k1,v1（不含 ?）。
    private String buildRedirectUrl(String scheme, HttpServletRequest request, TNetworkAddress addr,
            String requestPath, String queryString) {
        // 解析与提取 Authorization 认证凭证
        String userInfo = null;
        if (!Strings.isNullOrEmpty(request.getHeader("Authorization"))) {
            ActionAuthorizationInfo authInfo = getAuthorizationInfo(request);
            // 拼接成符合 URI 规范的 userInfo 格式（如 admin:123456），以便将登录凭证透传给重定向后的目标节点。
            userInfo = authInfo.fullUserName + ":" + authInfo.password;
        }
        try {
            // Preserve the original request path to avoid re-encoding an already encoded URI path.
            // 利用 java.net.URI 的构造函数生成只包含“网络位置/权限（Authority）”部分的 URI 对象。
            // 后续 3 个 null 分别代表 path、query 和 fragment。
            // 为什么要这么做？：URI 构造器会自动处理 userInfo、hostname 中的特殊字符转义编码，确保生成合法的 URL 前缀（例如 [http://admin:123456@192.168.1.10:8030](http://admin:123456@192.168.1.10:8030)）。
            URI authorityUri = new URI(scheme, userInfo, addr.getHostname(),
                    addr.getPort(), null, null, null);
            // 拼接请求路径与查询参数
            String redirectUrl = authorityUri.toASCIIString() + requestPath;
            // 检查原请求是否存在 Query 参数。若存在且不为空，则追加 ? 以及完整的 queryString，还原原请求的所有 URL 参数。
            if (!Strings.isNullOrEmpty(queryString)) {
                redirectUrl += "?" + queryString;
            }
            LOG.info("Redirect url: {}", scheme + "://" + addr.getHostname() + ":"
                    + addr.getPort() + requestPath);
            return redirectUrl;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    // 通过原生 HttpServletResponse 写入 HTTP 307 临时重定向响应
    // 设置 Response Header 的 Location 为 redirectUrl，状态码为 307 TEMPORARY_REDIRECT，设置 Content-Type 为 text/html;charset=utf-8 并刷新缓冲区。
    protected void writeTemporaryRedirect(HttpServletResponse response, String redirectUrl) throws IOException {
        response.setContentType("text/html;charset=utf-8");
        response.setStatus(HttpStatus.TEMPORARY_REDIRECT.value());
        response.setHeader("Location", redirectUrl);
        response.flushBuffer();
    }
    // 返回 Spring MVC 的 RedirectView 对象，重定向到指定地址。
    public RedirectView redirectTo(HttpServletRequest request, TNetworkAddress addr) {
        RedirectView redirectView = new RedirectView(buildRedirectUrl(request, addr));
        redirectView.setContentType("text/html;charset=utf-8");
        redirectView.setStatusCode(org.springframework.http.HttpStatus.TEMPORARY_REDIRECT);
        return redirectView;
    }

    // Use for redirects whose destination is a BE (e.g. stream load), which never speaks HTTPS.
    // 返回指向 BE 节点的 RedirectView 对象（HTTP 协议）
    public RedirectView redirectToBackend(HttpServletRequest request, TNetworkAddress addr) {
        RedirectView redirectView = new RedirectView(
                buildRedirectUrlToBackend(request, addr, request.getRequestURI(), request.getQueryString()));
        redirectView.setContentType("text/html;charset=utf-8");
        redirectView.setStatusCode(org.springframework.http.HttpStatus.TEMPORARY_REDIRECT);
        return redirectView;
    }
    // 对外暴露获取重定向字符串的简单 Getter。
    public String getRedirectUrL(HttpServletRequest request, TNetworkAddress addr) {
        return buildRedirectUrl(request, addr);
    }
    // 重定向到任意指定的 URL 字符串（例如对象存储预签名地址）
    public RedirectView redirectToObj(String sign) throws URISyntaxException {
        RedirectView redirectView = new RedirectView(sign);
        redirectView.setContentType("text/html;charset=utf-8");
        redirectView.setStatusCode(org.springframework.http.HttpStatus.TEMPORARY_REDIRECT);
        return redirectView;
    }
    // 检查当前 FE 是否为 Master 节点，若不是则返回重定向到 Master 节点的 RedirectView；若是则返回 null。
    public RedirectView redirectToMasterOrException(HttpServletRequest request, HttpServletResponse response)
                    throws Exception {
        Env env = Env.getCurrentEnv();
        if (env.isMaster()) {
            return null;
        }
        env.checkReadyOrThrow();
        return redirectTo(request, new TNetworkAddress(env.getMasterHost(), env.getMasterHttpPort()));
    }

    public Object redirectToMaster(HttpServletRequest request, HttpServletResponse response) {
        try {
            return redirectToMasterOrException(request, response);
        } catch (Exception e) {
            return ResponseEntityBuilder.okWithCommonError(e.getMessage());
        }
    }
    // 向 HTTP 响应写出二进制文件下载流（支持 File 对象或 byte[] 字节数组）。
    public void getFile(HttpServletRequest request, HttpServletResponse response, Object obj, String fileName)
            throws IOException {
        response.setHeader("Content-type", "application/octet-stream");
        response.addHeader("Content-Disposition", "attachment;fileName=" + fileName); // set file name
        if (obj instanceof File) {
            File file = (File) obj;
            byte[] buffer = new byte[1024];
            FileInputStream fis = null;
            BufferedInputStream bis = null;
            try {
                fis = new FileInputStream(file);
                bis = new BufferedInputStream(fis);
                OutputStream os = response.getOutputStream();
                int i = bis.read(buffer);
                while (i != -1) {
                    os.write(buffer, 0, i);
                    i = bis.read(buffer);
                }
                return;
            } finally {
                if (bis != null) {
                    try {
                        bis.close();
                    } catch (IOException e) {
                        LOG.warn("", e);
                    }
                }
                if (fis != null) {
                    try {
                        fis.close();
                    } catch (IOException e) {
                        LOG.warn("", e);
                    }
                }
            }
        } else if (obj instanceof byte[]) {
            OutputStream os = response.getOutputStream();
            os.write((byte[]) obj);
        }
    }
    // 专门用于向响应写出元数据镜像（Image）文件
    // 断言文件非空且存在，计算文件的 MD5（DigestUtils.md5Hex）与字节大小，在 Header 中加入 Doris 专用的 X-Image-Size 和 X-Image-MD5 元数据标头，最后调用 getFile 将文件流传输给客户端/从节点。
    public void writeFileResponse(HttpServletRequest request,
            HttpServletResponse response, File imageFile) throws IOException {
        Preconditions.checkArgument(imageFile != null && imageFile.exists());
        response.setHeader("Content-type", "application/octet-stream");
        response.addHeader("Content-Disposition", "attachment;fileName=" + imageFile.getName());
        response.setHeader(MetaHelper.X_IMAGE_SIZE, imageFile.length() + "");
        response.setHeader(MetaHelper.X_IMAGE_MD5, DigestUtils.md5Hex(new FileInputStream(imageFile)));
        getFile(request, response, imageFile, imageFile.getName());
    }
    // 判断当前请求是否需要从 HTTP 重定向到 HTTPS。
    // 若系统配置了 Config.enable_https == true 且当前请求 Scheme 为 "http" 则返回 true。
    public boolean needRedirect(String scheme) {
        return Config.enable_https && "http".equalsIgnoreCase(scheme);
    }
    // 生成将当前请求重定向至 HTTPS 端口的 RedirectView
    // 提取请求的 ServerName、URI、QueryString，结合配置中的 Config.https_port 拼装 https:// 开头的新 URL，返回 307 状态码的重定向视图。
    public Object redirectToHttps(HttpServletRequest request) {
        String serverName = request.getServerName();
        String uri = request.getRequestURI();
        String query = request.getQueryString();
        query = query == null ? "" : query;
        String newUrl = "https://" + NetUtils.getHostPortInAccessibleFormat(serverName, Config.https_port) + uri + "?"
                + query;
        LOG.info("redirect to new url: {}", newUrl);
        RedirectView redirectView = new RedirectView(newUrl);
        redirectView.setStatusCode(HttpStatus.TEMPORARY_REDIRECT);
        return redirectView;
    }
    // 判断当前请求是否需要由当前 FE 代理转发（Forward）给 Master FE 处理
    public Object forwardToMaster(HttpServletRequest request) {
        try {
            return forwardToMaster(request, (Object) getRequestBody(request));
        } catch (Exception e) {
            LOG.warn(e);
            return ResponseEntityBuilder.okWithCommonError(e.getMessage());
        }
    }

    public boolean checkForwardToMaster(HttpServletRequest request) {
        if (FeConstants.runningUnitTest) {
            String forbidForward = request.getHeader(FORWARD_MASTER_UT_TEST);
            if (forbidForward != null) {
                return "true".equals(forbidForward);
            }
        }
        return !Env.getCurrentEnv().isMaster();
    }

    // NOTE: This function can only be used for AuditlogPlugin stream load for now.
    // AuditlogPlugin should be re-disigned carefully, and blow method focuses on
    // temporarily addressing the users' needs for audit logs.
    // So this function is not widely tested under general scenario
    protected boolean checkClusterToken(String token) {
        try {
            return Env.getCurrentEnv().getTokenManager().checkAuthToken(token);
        } catch (UserException e) {
            throw new UnauthorizedException(e.getMessage());
        }
    }


    private String getRequestBody(HttpServletRequest request) throws IOException {
        BufferedReader reader = request.getReader();
        return reader.lines().collect(Collectors.joining(System.lineSeparator()));
    }

    public Object forwardToMaster(HttpServletRequest request, @Nullable Object body) {
        try {
            Env env = Env.getCurrentEnv();
            String redirectUrl = null;
            if (FeConstants.runningUnitTest) {
                redirectUrl =
                        getRedirectUrL(request, new TNetworkAddress(request.getServerName(), request.getServerPort()));
            } else {
                redirectUrl = HttpURLUtil.buildInternalFeUrl(
                        env.getMasterHost(), request.getRequestURI(), request.getQueryString());
            }
            String method = request.getMethod();

            HttpHeaders headers = new HttpHeaders();
            for (String headerName : Collections.list(request.getHeaderNames())) {
                // remove Content-Length because RestTemplate will recalculate Content-Length for request body
                if ("Content-Length".equalsIgnoreCase(headerName)) {
                    continue;
                }
                headers.add(headerName, request.getHeader(headerName));
            }

            if (FeConstants.runningUnitTest) {
                //remove header to avoid forward.
                headers.remove(FORWARD_MASTER_UT_TEST);
            }

            HttpEntity<Object> entity = new HttpEntity<>(body, headers);

            RestTemplate restTemplate;
            if (Config.enable_https) {
                SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory() {
                    @Override
                    protected void prepareConnection(HttpURLConnection conn, String httpMethod)
                            throws IOException {
                        if (conn instanceof HttpsURLConnection) {
                            HttpsURLConnection https = (HttpsURLConnection) conn;
                            https.setSSLSocketFactory(
                                    InternalHttpsUtils.getSslContext().getSocketFactory());
                            https.setHostnameVerifier(NoopHostnameVerifier.INSTANCE);
                        }
                        super.prepareConnection(conn, httpMethod);
                    }
                };
                restTemplate = new RestTemplate(factory);
            } else {
                restTemplate = new RestTemplate();
            }

            ResponseEntity<Object> responseEntity;
            switch (method) {
                case "GET":
                    responseEntity = restTemplate.exchange(redirectUrl, HttpMethod.GET, entity, Object.class);
                    break;
                case "POST":
                    responseEntity = restTemplate.exchange(redirectUrl, HttpMethod.POST, entity, Object.class);
                    break;
                case "PUT":
                    responseEntity = restTemplate.exchange(redirectUrl, HttpMethod.PUT, entity, Object.class);
                    break;
                case "DELETE":
                    responseEntity = restTemplate.exchange(redirectUrl, HttpMethod.DELETE, entity, Object.class);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported HTTP method: " + method);
            }

            return responseEntity.getBody();
        } catch (Exception e) {
            LOG.warn(e);
            return ResponseEntityBuilder.okWithCommonError(e.getMessage());
        }
    }

    /**
     * Check if admin privilege is required.
     * When enable_all_http_auth is enabled, check if the user has admin privilege.
     * If not authorized, throws UnauthorizedException.
     *
     * @param userIdentity The user identity to check
     */
    // 检查指定用户是否具备 Admin（超级管理员）权限
    protected void checkAdminAuth(UserIdentity userIdentity) throws UnauthorizedException {
        if (Config.enable_all_http_auth) {
            checkGlobalAuth(userIdentity, org.apache.doris.mysql.privilege.PrivPredicate.ADMIN);
        }
    }
}
