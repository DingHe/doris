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
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.lock.MonitoredReentrantReadWriteLock;
import org.apache.doris.mysql.MysqlPassword;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.Lock;
// 负责用户账号及其密码认证管理的核心类。它维护了 Doris 系统中所有用户身份与密码信息的内存映射结构，并提供了用户账号创建、密码校验、域名解析（Domain Resolver）动态关联以及持久化等功能。
// 用户身份与密码元数据管理：在 Doris 中，一个用户名（如 cmy）可能对应多个具体的访问主机（Host，例如 '192.168.1.%' 或 '%'）。UserManager 维护了用户名到具体的 User 对象列表的映射，负责用户账号的创建、更新、修改密码和删除。
// 客户端登录密码认证：当客户端（如 MySQL Client 或 JDBC）连接 Doris 时，UserManager 负责匹配对应主机限制的账号，并根据 MySQL 的 Scramble 认证协议（或 Plain 明文协议）完成密码校验，同时配合 PasswordPolicyManager 处理账号锁定与密码过期策略。
// 域名解析（Domain Resolver）动态扩展：支持基于域名配置的用户（如 cmy@'domain.com'）。DomainResolver 线程定时解析域名为 IP 后，UserManager 动态生成对应的 IP 用户节点并绑定到原域名用户，实现基于FQDN/动态IP的粒度控制。
//
public class UserManager implements Writable {
    // 静态常量，值为 "%"。代表匹配任意主机的通配符。
    public static final String ANY_HOST = "%";
    private static final Logger LOG = LogManager.getLogger(UserManager.class);

    private static final MonitoredReentrantReadWriteLock rwLock = new MonitoredReentrantReadWriteLock(false);
    private static final Lock rlock = rwLock.readLock();
    private static final Lock wlock = rwLock.writeLock();

    // One name may have multiple User,because host can be different
    // 核心内存数据结构。
    // Key 为用户名（Qualified Name），Value 为对应不同 Host/Domain 的 User 对象列表。
    @SerializedName(value = "nameToUsers")
    private Map<String, List<User>> nameToUsers = Maps.newHashMap();
    // 判断指定的 userIdentity（用户名+Host）是否存在。
    public boolean userIdentityExist(UserIdentity userIdentity, boolean includeByDomain) {
        rlock.lock();
        try {
            return userIdentityExistWithoutLock(userIdentity, includeByDomain);
        } finally {
            rlock.unlock();
        }
    }
    // includeByDomain 表示是否包含由 Domain Resolver 动态解析出来的临时 IP 用户。
    public boolean userIdentityExistWithoutLock(UserIdentity userIdentity, boolean includeByDomain) {
        List<User> users = nameToUsers.get(userIdentity.getQualifiedUser());
        if (CollectionUtils.isEmpty(users)) {
            return false;
        }
        for (User user : users) {
            if (user.getUserIdentity().getHost().equalsIgnoreCase(userIdentity.getHost())) {
                if (includeByDomain || !user.isSetByDomainResolver()) {
                    return true;
                }
            }
        }
        return false;
    }

    public List<User> getUserByName(String name) {
        rlock.lock();
        try {
            List<User> users = nameToUsers.get(name);
            return users == null ? Collections.EMPTY_LIST : users;
        } finally {
            rlock.unlock();
        }
    }

    public void checkPassword(String remoteUser, String remoteHost, byte[] remotePasswd, byte[] randomString,
            List<UserIdentity> currentUser) throws AuthenticationException {
        checkPasswordInternal(remoteUser, remoteHost, remotePasswd, randomString, null, currentUser, false);
    }

    public void checkPlainPassword(String remoteUser, String remoteHost, String remotePasswd,
            List<UserIdentity> currentUser) throws AuthenticationException {
        checkPasswordInternal(remoteUser, remoteHost, null, null, remotePasswd, currentUser, true);
    }

    public void checkPasswordForUserIdentity(UserIdentity userIdentity, byte[] remotePasswd, byte[] randomString,
            List<UserIdentity> currentUser) throws AuthenticationException {
        checkPasswordInternalForUser(userIdentity, remotePasswd, randomString, null, currentUser, false);
    }

    public void checkPlainPasswordForUserIdentity(UserIdentity userIdentity, String remotePasswd,
            List<UserIdentity> currentUser) throws AuthenticationException {
        checkPasswordInternalForUser(userIdentity, null, null, remotePasswd, currentUser, true);
    }
    // 负责根据客户端传入的用户名和 Host 匹配对应的权限条目、校验密码有效性，并配合密码策略（如锁定、过期等）完成登录。
    // remotePasswd MySQL Scramble 认证协议下客户端发送的密文加密 responses（哈希盐值计算后的结果）。若为明文认证（plain=true），该参数传 null。
    // randomString MySQL 握手阶段服务端生成的 20 字节随机挑战盐值（Challenge Salt）。用于与密文计算比对。
    // remotePasswdStr 明文认证协议下客户端传入的明文密码字符串。若为加密认证，该参数传 null。
    // plain  认证方式标识。true 表示使用明文密码认证；false 表示使用 MySQL 标准 Scramble 加密认证。
    private void checkPasswordInternal(String remoteUser, String remoteHost, byte[] remotePasswd, byte[] randomString,
            String remotePasswdStr, List<UserIdentity> currentUser, boolean plain) throws AuthenticationException {
        // 获取全局环境中的密码策略管理器 PasswordPolicyManager，用于后续检查账号是否被锁定、密码是否过期，以及记录失败登录次数。
        PasswordPolicyManager passwdPolicyMgr = Env.getCurrentEnv().getAuth().getPasswdPolicyManager();
        List<User> users = new ArrayList<>();
        // 加读锁获取用户名对应的规则列表
        rlock.lock();
        try {
            users = nameToUsers.get(remoteUser);
            if (CollectionUtils.isEmpty(users)) {
                throw new AuthenticationException(ErrorCode.ERR_ACCESS_DENIED_ERROR, remoteUser + "@" + remoteHost,
                    "YES");
            }
        } finally {
            rlock.unlock();
        }
        // 遍历账号规则列表，进行 Host 匹配与密码校验
        for (User user : users) {
            // 如果当前规则是纯域名规则（如 cmy@'domain.com'），则直接跳过。因为域名规则是由后台 DomainResolver 动态解析成 IP 节点后另行匹配的，不能直接拿 Host 文本去匹配 IP。
            if (user.getUserIdentity().isDomain()) {
                continue;
            }
            // check host
            // 当前规则不是允许任意主机（即不是 %）
            // 来源 remoteHost 不符合当前规则定义的通配符正则（如 '192.168.1.%'）。
            if (!user.isAnyHost() && !user.getHostPattern().match(remoteHost)) {
                continue;
            }
            // 获取该用户规则对应的绑定身份 curUser
            UserIdentity curUser = user.getDomainUserIdentity();
            // 根据 plain 参数决定走明文比对还是 MySQL 加密盐值比对。
            if (comparePassword(user.getPassword(), remotePasswd, randomString, remotePasswdStr, plain)) {
                // 密码正确后，调用密码策略管理器检查当前账号是否处于锁定状态（如多次输错被冻结），或者密码是否已过期。若异常则内部会抛出异常中断登录。
                passwdPolicyMgr.checkAccountLockedAndPasswordExpiration(curUser);
                if (currentUser != null) {
                    currentUser.add(curUser);
                }
                return;
            } else {
                // case A. this means we already matched a entry by user@host, but password is incorrect.
                // return false, NOT continue matching other entries.
                // For example, there are 2 entries in order:
                // 1. cmy@"192.168.%" identified by '123';
                // 2. cmy@"%" identified by 'abc';
                // if user cmy@'192.168.1.1' try to login with password 'abc', it will be denied.
                passwdPolicyMgr.onFailedLogin(curUser);
                throw new AuthenticationException(ErrorCode.ERR_ACCESS_DENIED_ERROR, remoteUser + "@" + remoteHost,
                        hasRemotePasswd(plain, remotePasswd));
            }
        }
        throw new AuthenticationException(ErrorCode.ERR_ACCESS_DENIED_ERROR, remoteUser + "@" + remoteHost,
                hasRemotePasswd(plain, remotePasswd));
    }

    private void checkPasswordInternalForUser(UserIdentity userIdentity, byte[] remotePasswd, byte[] randomString,
            String remotePasswdStr, List<UserIdentity> currentUser, boolean plain) throws AuthenticationException {
        PasswordPolicyManager passwdPolicyMgr = Env.getCurrentEnv().getAuth().getPasswdPolicyManager();
        User user = getUserByUserIdentity(userIdentity);
        if (user == null) {
            throw new AuthenticationException(ErrorCode.ERR_ACCESS_DENIED_ERROR, userIdentity.toString(),
                    hasRemotePasswd(plain, remotePasswd));
        }
        UserIdentity currentIdentity = user.getDomainUserIdentity();
        if (comparePassword(user.getPassword(), remotePasswd, randomString, remotePasswdStr, plain)) {
            passwdPolicyMgr.checkAccountLockedAndPasswordExpiration(currentIdentity);
            if (currentUser != null) {
                currentUser.add(currentIdentity);
            }
            return;
        }
        passwdPolicyMgr.onFailedLogin(currentIdentity);
        throw new AuthenticationException(ErrorCode.ERR_ACCESS_DENIED_ERROR, userIdentity.toString(),
                hasRemotePasswd(plain, remotePasswd));
    }

    public List<UserIdentity> getUserIdentityUncheckPasswd(String remoteUser, String remoteHost) {
        List<UserIdentity> userIdentities = Lists.newArrayList();
        rlock.lock();
        try {
            List<User> users = nameToUsers.getOrDefault(remoteUser, Lists.newArrayList());
            for (User user : users) {
                if (!user.getUserIdentity().isDomain()
                        && (user.isAnyHost() || user.getHostPattern().match(remoteHost))) {
                    userIdentities.add(user.getUserIdentity());
                }
            }
            return userIdentities;
        } finally {
            rlock.unlock();
        }
    }

    private String hasRemotePasswd(boolean plain, byte[] remotePasswd) {
        if (plain) {
            return "YES";
        }
        return remotePasswd.length == 0 ? "NO" : "YES";
    }

    private boolean comparePassword(Password curUserPassword, byte[] remotePasswd,
            byte[] randomString, String remotePasswdStr, boolean plain) {
        // check password
        if (plain) {
            return MysqlPassword.checkPlainPass(curUserPassword.getPassword(), remotePasswdStr);
        } else {
            byte[] saltPassword = MysqlPassword.getSaltFromPassword(curUserPassword.getPassword());
            // when the length of password is zero, the user has no password
            return ((remotePasswd.length == saltPassword.length)
                    && (remotePasswd.length == 0
                    || MysqlPassword.checkScramble(remotePasswd, randomString, saltPassword)));
        }
    }


    public void clearEntriesSetByResolver() {
        wlock.lock();
        try {
            Iterator<Entry<String, List<User>>> iterator = nameToUsers.entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<String, List<User>> next = iterator.next();
                Iterator<User> iter = next.getValue().iterator();
                while (iter.hasNext()) {
                    User user = iter.next();
                    if (user.isSetByDomainResolver()) {
                        iter.remove();
                    }
                }
                if (CollectionUtils.isEmpty(next.getValue())) {
                    iterator.remove();
                } else {
                    Collections.sort(next.getValue());
                }
            }
        } finally {
            wlock.unlock();
        }
    }

    public User createUser(UserIdentity userIdent, byte[] pwd, UserIdentity domainUserIdent, boolean setByResolver,
                           String comment) throws PatternMatcherException {
        wlock.lock();
        try {
            return createUserWithoutLock(userIdent, pwd, domainUserIdent, setByResolver, comment);
        } finally {
            wlock.unlock();
        }
    }

    public User createUserWithoutLock(UserIdentity userIdent, byte[] pwd, UserIdentity domainUserIdent,
                                      boolean setByResolver, String comment)
            throws PatternMatcherException {
        if (userIdentityExistWithoutLock(userIdent, true)) {
            User userByUserIdentity = getUserByUserIdentityWithoutLock(userIdent);
            if (!userByUserIdentity.isSetByDomainResolver() && setByResolver) {
                // If the user is NOT created by domain resolver,
                // and the current operation is done by DomainResolver,
                // we should not override it, just return
                return userByUserIdentity;
            }
            userByUserIdentity.setPassword(pwd);
            userByUserIdentity.setComment(comment);
            userByUserIdentity.setSetByDomainResolver(setByResolver);
            userByUserIdentity.setUserIdentity(userIdent);
            return userByUserIdentity;
        }

        PatternMatcher hostPattern = PatternMatcher
                .createMysqlPattern(userIdent.getHost(), CaseSensibility.HOST.getCaseSensibility());
        User user = new User(userIdent, pwd, setByResolver, domainUserIdent, hostPattern, comment);
        List<User> nameToLists = nameToUsers.get(userIdent.getQualifiedUser());
        if (CollectionUtils.isEmpty(nameToLists)) {
            nameToLists = Lists.newArrayList(user);
            nameToUsers.put(userIdent.getQualifiedUser(), nameToLists);
        } else {
            nameToLists.add(user);
            Collections.sort(nameToLists);
        }
        return user;

    }

    public User getUserByUserIdentity(UserIdentity userIdent) {
        rlock.lock();
        try {
            return getUserByUserIdentityWithoutLock(userIdent);
        } finally {
            rlock.unlock();
        }
    }

    public User getUserByUserIdentityWithoutLock(UserIdentity userIdent) {
        List<User> nameToLists = nameToUsers.get(userIdent.getQualifiedUser());
        if (CollectionUtils.isEmpty(nameToLists)) {
            return null;
        }
        Iterator<User> iter = nameToLists.iterator();
        while (iter.hasNext()) {
            User user = iter.next();
            if (user.getUserIdentity().equals(userIdent)) {
                return user;
            }
        }
        return null;
    }

    public void removeUser(UserIdentity userIdent) {
        wlock.lock();
        try {
            List<User> nameToLists = nameToUsers.get(userIdent.getQualifiedUser());
            if (CollectionUtils.isEmpty(nameToLists)) {
                return;
            }
            Iterator<User> iter = nameToLists.iterator();
            while (iter.hasNext()) {
                User user = iter.next();
                if (user.getUserIdentity().equals(userIdent)) {
                    iter.remove();
                }
            }
            if (CollectionUtils.isEmpty(nameToLists)) {
                nameToUsers.remove(userIdent.getQualifiedUser());
            } else {
                Collections.sort(nameToLists);
            }
        } finally {
            wlock.unlock();
        }
    }

    public Map<String, List<User>> getNameToUsers() {
        rlock.lock();
        try {
            return ImmutableMap.copyOf(nameToUsers);
        } finally {
            rlock.unlock();
        }
    }

    public void setPassword(UserIdentity userIdentity, byte[] password, boolean errOnNonExist) throws DdlException {
        User user = getUserByUserIdentity(userIdentity);
        if (user == null) {
            if (errOnNonExist) {
                throw new DdlException("user " + userIdentity + " does not exist");
            }
            return;
        }
        user.setPassword(password);
    }

    public void getAllDomains(Set<String> allDomains) {
        rlock.lock();
        try {
            for (Entry<String, List<User>> entry : nameToUsers.entrySet()) {
                for (User user : entry.getValue()) {
                    if (user.getUserIdentity().isDomain()) {
                        allDomains.add(user.getUserIdentity().getHost());
                    }
                }
            }
        } finally {
            rlock.unlock();
        }
    }

    // handle new resolved IPs.
    // it will only modify password entry of these resolved IPs. All other privileges are binded
    // to the domain, so no need to modify.
    public void addUserPrivEntriesByResolvedIPs(Map<String, Set<String>> resolvedIPsMap) {
        wlock.lock();
        try {
            for (Entry<String, List<User>> userEntry : nameToUsers.entrySet()) {
                for (Map.Entry<String, Set<String>> entry : resolvedIPsMap.entrySet()) {
                    User domainUser = getDomainUser(userEntry.getValue(), entry.getKey());
                    if (domainUser == null) {
                        continue;
                    }
                    // this user ident will be saved along with each resolved "IP" user ident, so that when checking
                    // password, this "domain" user ident will be returned as "current user".
                    for (String newIP : entry.getValue()) {
                        UserIdentity userIdent = UserIdentity.createAnalyzedUserIdentWithIp(userEntry.getKey(), newIP);
                        UserIdentity domainUserIdent = domainUser.getUserIdentity();
                        userIdent.setSan(domainUserIdent.getSan());
                        userIdent.setIssuer(domainUserIdent.getIssuer());
                        userIdent.setCipher(domainUserIdent.getCipher());
                        userIdent.setSubject(domainUserIdent.getSubject());
                        byte[] password = domainUser.getPassword().getPassword();
                        Preconditions.checkNotNull(password, entry.getKey());
                        try {
                            createUserWithoutLock(userIdent, password, domainUser.getUserIdentity(), true, "");
                        } catch (PatternMatcherException e) {
                            LOG.info("failed to create user for user ident: {}, {}", userIdent, e.getMessage());
                        }
                    }
                }
            }
        } finally {
            wlock.unlock();
        }
    }

    private User getDomainUser(List<User> users, String domain) {
        for (User user : users) {
            if (user.getUserIdentity().isDomain() && user.getUserIdentity().getHost().equals(domain)) {
                return user;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        rlock.lock();
        try {
            return nameToUsers.toString();
        } finally {
            rlock.unlock();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static UserManager read(DataInput in) throws IOException {
        String json = Text.readString(in);
        UserManager um = GsonUtils.GSON.fromJson(json, UserManager.class);
        return um;
    }

    // ====== CLOUD ======
    public Set<String> getAllUsers() {
        rlock.lock();
        try {
            return new HashSet<>(nameToUsers.keySet());
        } finally {
            rlock.unlock();
        }
    }

    public String getUserId(String userName) {
        rlock.lock();
        try {
            if (!nameToUsers.containsKey(userName)) {
                LOG.warn("can't find userName {} 's userId, nameToUsers {}", userName, nameToUsers);
                return "";
            }
            List<User> users = nameToUsers.get(userName);
            if (users.isEmpty()) {
                LOG.warn("userName {}  empty users in map {}", userName, nameToUsers);
            }
            // here, all the users has same userid, just return one
            String userId = users.stream().map(User::getUserId).filter(Strings::isNotEmpty).findFirst().orElse("");
            LOG.debug("userName {}, userId {}, map {}", userName, userId, nameToUsers);
            return userId;
        } finally {
            rlock.unlock();
        }
    }

    // ====== CLOUD =====
}
