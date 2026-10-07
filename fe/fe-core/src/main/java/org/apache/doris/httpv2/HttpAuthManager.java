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

package org.apache.doris.httpv2;

import org.apache.doris.analysis.UserIdentity;

import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.TimeUnit;

// We simulate a simplified session here: only store user-name of clients who already logged in,
// and we only have a default admin user for now.
// 简易 HTTP 会话（Session）与身份认证状态管理器
// 管理 HTTP Web/API 的会话状态（Session Management）
// HTTP 协议本身是无状态的。Web 界面或 HTTP API 登录成功后，系统会为客户端生成一个 sessionId。HttpAuthManager 负责在 FE 进程内存中维护这些已登录会话与用户身份的映射关系，避免用户在后续的每次 HTTP 请求中都重新触发完整的密码校验。
// 基于 Guava Cache 的自动过期与容量限制：
// 使用 Guava 的 Cache 容器来缓存 Session，配置了基于时间的自动过期策略（LRU 缓存机制，超时未访问自动失效）以及最大容量限制（防止 Session 数量过多导致 JVM 内存溢出）。
// 采用单例模式（Singleton Pattern）：
// 确保整个 FE 进程中只存在一个 HttpAuthManager 实例，所有的 HTTP 控制器（Controller）、拦截器（Interceptor）或过滤器（Filter）共享同一套 HTTP 会话上下文。
public final class HttpAuthManager {
    private static final Logger LOG = LogManager.getLogger(HttpAuthManager.class);
    // Session 过期时间限制。静态常量，默认值为 2，单位为小时（Hours）。表示当一个 Session 在 2 小时内没有任何访问请求时，将被自动过期销毁。
    private static long SESSION_EXPIRE_TIME = 2; // hour
    // Session 最大缓存容量限制。静态常量，默认值为 100。防止恶意请求或海量并发连接建立过多 Session 挤爆 FE 内存。达到上限后将按照 LRU（最近最少使用）算法剔除旧 Session。
    private static long SESSION_MAX_SIZE = 100; // avoid to store too many

    private static HttpAuthManager instance = new HttpAuthManager();

    public static class SessionValue {
        // 当前登录用户的身份标识。包含用户名（username）和允许登录的主机/IP（host），用于后续 HTTP 请求的权限鉴权（如检查该用户是否有权调用某个 Web API）。
        public UserIdentity currentUser;
        // 用户的密码（凭证）。在某些内部转发或跨节点/后端转发 HTTP 请求时，可能需要携带凭证进行二次鉴权。
        public String password;
    }

    // session_id => session value
    // Session 核心缓存容器。基于 Guava CacheBuilder 构建，Key 为 sessionId（字符串），Value 为 SessionValue 对象。
    // 具备访问后超时（expireAfterAccess）和最大容量限额（maximumSize）特性。注意：该缓存为内存缓存，不进行持久化，FE 重启后 Session 会失效需重新登录。
    private Cache<String, SessionValue> authSessions = CacheBuilder.newBuilder()
            .maximumSize(SESSION_MAX_SIZE)
            .expireAfterAccess(SESSION_EXPIRE_TIME, TimeUnit.HOURS)
            .build();

    private HttpAuthManager() {
        // do nothing
    }

    public static HttpAuthManager getInstance() {
        return instance;
    }

    public SessionValue getSessionValue(List<String> sessionIds) {
        for (String sessionId : sessionIds) {
            SessionValue sv = authSessions.getIfPresent(sessionId);
            if (sv != null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("get session value {} by session id: {}, left size: {}",
                            sv == null ? null : sv.currentUser, sessionId, authSessions.size());
                }
                return sv;
            }
        }
        return null;
    }

    public void removeSession(String sessionId) {
        if (!Strings.isNullOrEmpty(sessionId)) {
            authSessions.invalidate(sessionId);
            if (LOG.isDebugEnabled()) {
                LOG.debug("remove session id: {}, left size: {}", sessionId, authSessions.size());
            }
        }
    }

    public void addSessionValue(String key, SessionValue value) {
        authSessions.put(key, value);
    }

    public Cache<String, SessionValue> getAuthSessions() {
        return authSessions;
    }
}
