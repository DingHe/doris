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
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections4.CollectionUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
// 负责维护用户身份（UserIdentity）与角色（Role）之间的双向绑定关系。
// 维护用户与角色的多对多绑定关系：在 Doris 中，一个用户可以被授予多个角色，一个角色也可以被赋予多个用户。UserRoleManager 内部通过正向映射和反向索引双重映射结构，高效管理这种多对多关系。
// 提供双向快速检索：支持从“用户 $\rightarrow$ 角色列表”的正向查询（用于用户登录鉴权和权限继承），以及从“角色 $\rightarrow$ 用户列表”的反向查询（用于删除角色或审计用户分布）。
// 配合 RBAC 角色过滤：提供针对系统默认隐藏角色（如用户默认角色 DEFAULT_ROLE_PREFIX）的过滤查询功能，避免在展示用户角色时露出现象级的内部自动创建角色。
public class UserRoleManager implements Writable, GsonPostProcessable {
    // Concurrency control is delegated by Auth, so not concurrentMap
    @SerializedName(value = "userToRoles")
    private Map<UserIdentity, Set<String>> userToRoles = Maps.newHashMap();
    // Will not be persisted,generage by userToRoles
    private Map<String, Set<UserIdentity>> roleToUsers = Maps.newHashMap();

    public void addUserRole(UserIdentity userIdentity, String roleName) {
        Set<String> roles = userToRoles.get(userIdentity);
        if (CollectionUtils.isEmpty(roles)) {
            roles = Sets.newHashSet();
        }
        roles.add(roleName);
        userToRoles.put(userIdentity, roles);
        Set<UserIdentity> userIdentities = roleToUsers.get(roleName);
        if (CollectionUtils.isEmpty(userIdentities)) {
            userIdentities = Sets.newHashSet();
        }
        userIdentities.add(userIdentity);
        roleToUsers.put(roleName, userIdentities);
    }

    public void addUserRoles(UserIdentity userIdentity, List<String> roles) {
        for (String roleName : roles) {
            addUserRole(userIdentity, roleName);
        }
    }

    public void removeUserRoles(UserIdentity userIdentity, List<String> roles) {
        for (String roleName : roles) {
            removeUserRole(userIdentity, roleName);
        }
    }

    public void removeUserRole(UserIdentity userIdentity, String roleName) {
        Set<String> roles = userToRoles.get(userIdentity);
        if (!CollectionUtils.isEmpty(roles)) {
            roles.remove(roleName);
        }
        if (CollectionUtils.isEmpty(roles)) {
            userToRoles.remove(userIdentity);
        }
        Set<UserIdentity> userIdentities = roleToUsers.get(roleName);
        if (!CollectionUtils.isEmpty(userIdentities)) {
            userIdentities.remove(userIdentity);
        }
        if (CollectionUtils.isEmpty(userIdentities)) {
            roleToUsers.remove(roleName);
        }
    }

    public void dropUser(UserIdentity userIdentity) {
        if (!userToRoles.containsKey(userIdentity)) {
            return;
        }
        Set<String> roles = userToRoles.remove(userIdentity);
        for (String roleName : roles) {
            Set<UserIdentity> userIdentities = roleToUsers.get(roleName);
            if (CollectionUtils.isEmpty(userIdentities)) {
                continue;
            }
            userIdentities.remove(userIdentity);
            if (CollectionUtils.isEmpty(userIdentities)) {
                roleToUsers.remove(roleName);
            }
        }
    }

    public void dropRole(String roleName) {
        if (!roleToUsers.containsKey(roleName)) {
            return;
        }
        Set<UserIdentity> remove = roleToUsers.remove(roleName);
        for (UserIdentity userIdentity : remove) {
            Set<String> roles = userToRoles.get(userIdentity);
            if (CollectionUtils.isEmpty(roles)) {
                continue;
            }
            roles.remove(roleName);
            if (CollectionUtils.isEmpty(roles)) {
                userToRoles.remove(userIdentity);
            }
        }
    }

    public Set<String> getRolesByUser(UserIdentity user) {
        Set<String> roles = userToRoles.get(user);
        return roles == null ? Collections.EMPTY_SET : roles;
    }

    public Set<String> getRolesByUser(UserIdentity user, boolean showUserDefaultRole) {
        Set<String> rolesByUser = getRolesByUser(user);
        if (showUserDefaultRole) {
            return rolesByUser;
        } else {
            return rolesByUser.stream().filter(role ->
                    !role.startsWith(RoleManager.DEFAULT_ROLE_PREFIX)).collect(
                    Collectors.toSet());
        }
    }

    public Set<UserIdentity> getUsersByRole(String roleName) {
        Set<UserIdentity> userIdentities = roleToUsers.get(roleName);
        return userIdentities == null ? Collections.EMPTY_SET : userIdentities;
    }

    @Override
    public String toString() {
        return userToRoles.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static UserRoleManager read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, UserRoleManager.class);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        roleToUsers = Maps.newHashMap();
        for (Entry<UserIdentity, Set<String>> entry : userToRoles.entrySet()) {
            for (String roleName : entry.getValue()) {
                Set<UserIdentity> userIdentities = roleToUsers.get(roleName);
                if (CollectionUtils.isEmpty(userIdentities)) {
                    userIdentities = Sets.newHashSet();
                }
                userIdentities.add(entry.getKey());
                roleToUsers.put(roleName, userIdentities);
            }
        }
    }
}
