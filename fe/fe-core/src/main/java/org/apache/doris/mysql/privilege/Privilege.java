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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;

import com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
// 权限管理模块中核心的权限枚举类（Enum）
// 在 Doris 的认证与权限架构中，用户或角色所拥有的具体操作许可（如查询、导入、建表、删除、使用资源等）都是通过该枚举定义的。
// 定义权限项元数据：枚举了 Doris 系统中所有的具体权限类型（如 SELECT_PRIV、ADMIN_PRIV 等），并为每个权限绑定名称、底层位索引（idx）、功能描述以及适用的作用域上下文（context，如 GLOBAL、TABLE、RESOURCE 等）。
// 位图（BitSet）映射基础：通过为每个权限赋予唯一的整型索引 idx（0, 1, 2...），使权限系统可以使用高效率的位图数据结构（PrivBitSet）进行权限的集合运算与快速匹配。
// 适配云原生与历史版本兼容：维护了标准 Doris 和 Doris Cloud 在演进过程中的废弃索引（如 9 ~ 11），保证元数据反序列化时的前向与后向兼容性。
// 与 MySQL 协议映射：提供 Doris 内部权限名称到 MySQL 兼容权限字符串（如 LOAD_PRIV $\to$ INSERT）的映射关系，用于向后兼容 MySQL 客户端生态（如 SHOW GRANTS 视图展示）。
public enum Privilege {
    // 节点操作权限（如添加/删除 FE、BE 节点）。
    NODE_PRIV("Node_priv", 0, "Privilege for cluster node operations", "GLOBAL"),
    // 全局超级管理员权限，拥有一切集群管理与数据访问权限。
    ADMIN_PRIV("Admin_priv", 1, "Privilege for admin user", "GLOBAL"),
    // 赋权/转授权权限，允许将自身拥有的权限授予其他用户或角色。
    GRANT_PRIV("Grant_priv", 2, "Privilege for granting privilege",
            "GLOBAL,CATALOG,DATABASE,TABLE,RESOURCE,WORKLOAD GROUP"),
    // 数据查询权限（SELECT）。
    SELECT_PRIV("Select_priv", 3, "Privilege for select data in tables", "GLOBAL,CATALOG,DATABASE,TABLE"),

    // 数据导入/写入权限（对应 MySQL 的 INSERT）。
    LOAD_PRIV("Load_priv", 4, "Privilege for loading data into tables", "GLOBAL,CATALOG,DATABASE,TABLE"),
    // 结构变更权限（如修改表结构、修改 Catalog/Database 属性等）。
    ALTER_PRIV("Alter_priv", 5, "Privilege for alter catalog, database or table", "GLOBAL,CATALOG,DATABASE,TABLE"),

    CREATE_PRIV("Create_priv", 6, "Privilege for creating catalog, database or table", "GLOBAL,CATALOG,DATABASE,TABLE"),
    DROP_PRIV("Drop_priv", 7, "Privilege for dropping catalog, database or table", "GLOBAL,CATALOG,DATABASE,TABLE"),
    USAGE_PRIV("Usage_priv", 8, "Privilege for using resource or workloadGroup", "RESOURCE,WORKLOAD GROUP"),
    // in doris code < VERSION_130
    SHOW_VIEW_PRIV_DEPRECATED("Show_view_priv", 9, "Privilege for show create view", ""),

    // in cloud code < VERSION_130
    CLUSTER_USAGE_PRIV_DEPRECATED("Cluster_usage_priv", 9, "Privilege for using cluster", ""),
    STAGE_USAGE_PRIV_DEPRECATED("Stage_usage_priv", 10, "Privilege for using stage", ""),
    SHOW_VIEW_PRIV_CLOUD_DEPRECATED("Show_view_priv", 11, "Privilege for show create view", ""),
    // compatible doris and cloud, and 9 ~ 11 has been contaminated
    CLUSTER_USAGE_PRIV("Cluster_usage_priv", 12, "Privilege for using cluster", "RESOURCE"),
    // 13 placeholder for stage
    STAGE_USAGE_PRIV("Stage_usage_priv", 13, "Privilege for using stage", "RESOURCE"),
    SHOW_VIEW_PRIV("Show_view_priv", 14, "Privilege for show create view", "GLOBAL,CATALOG,DATABASE,TABLE");

    public static final Map<Integer, Privilege> privileges;

    static {
        privileges = new HashMap<>();
        privileges.put(0, NODE_PRIV);
        privileges.put(1, ADMIN_PRIV);
        privileges.put(2, GRANT_PRIV);
        privileges.put(3, SELECT_PRIV);
        privileges.put(4, LOAD_PRIV);
        privileges.put(5, ALTER_PRIV);
        privileges.put(6, CREATE_PRIV);
        privileges.put(7, DROP_PRIV);
        privileges.put(8, USAGE_PRIV);
        if (Config.isCloudMode()) {
            privileges.put(9, CLUSTER_USAGE_PRIV_DEPRECATED);
            privileges.put(10, STAGE_USAGE_PRIV_DEPRECATED);
            privileges.put(11, SHOW_VIEW_PRIV_CLOUD_DEPRECATED);
        } else {
            privileges.put(9, SHOW_VIEW_PRIV_DEPRECATED);
        }
        privileges.put(12, CLUSTER_USAGE_PRIV);
        privileges.put(13, STAGE_USAGE_PRIV);
        privileges.put(14, SHOW_VIEW_PRIV);
    }


    // only GRANT_PRIV and USAGE_PRIV can grant on resource
    public static final Privilege[] notBelongToResourcePrivileges = {
            NODE_PRIV,
            ADMIN_PRIV,
            SELECT_PRIV,
            LOAD_PRIV,
            ALTER_PRIV,
            CREATE_PRIV,
            DROP_PRIV,
            SHOW_VIEW_PRIV,
    };

    // only GRANT_PRIV and USAGE_PRIV can grant on workloadGroup
    public static final Privilege[] notBelongToWorkloadGroupPrivileges = {
            NODE_PRIV,
            ADMIN_PRIV,
            SELECT_PRIV,
            LOAD_PRIV,
            ALTER_PRIV,
            CREATE_PRIV,
            DROP_PRIV,
            SHOW_VIEW_PRIV,
    };

    public static final Privilege[] notBelongToTablePrivileges = {
            USAGE_PRIV,
            CLUSTER_USAGE_PRIV,
            STAGE_USAGE_PRIV,
    };

    public static final Map<Privilege, String> privInDorisToMysql =
            ImmutableMap.<Privilege, String>builder() // No NODE_PRIV and ADMIN_PRIV in the mysql
                    .put(SELECT_PRIV, "SELECT")
                    .put(LOAD_PRIV, "INSERT")
                    .put(ALTER_PRIV, "ALTER")
                    .put(CREATE_PRIV, "CREATE")
                    .put(DROP_PRIV, "DROP")
                    .put(USAGE_PRIV, "USAGE")
                    .put(CLUSTER_USAGE_PRIV, "USAGE")
                    .put(SHOW_VIEW_PRIV, "SHOW VIEW")
                    .build();

    private String name;
    private int idx;
    private String desc;
    private String context;

    private Privilege(String name, int index, String desc, String context) {
        this.name = name;
        this.idx = index;
        this.desc = desc;
        this.context = context;
    }

    public String getName() {
        return name;
    }

    public int getIdx() {
        return idx;
    }

    public String getDesc() {
        return desc;
    }

    public String getContext() {
        return context;
    }

    public boolean isDeprecated() {
        return idx >= 9 && idx <= 11;
    }

    public static Privilege getPriv(int index) {
        if (!privileges.containsKey(index)) {
            return null;
        }
        return privileges.get(index);
    }

    public static boolean satisfy(PrivBitSet grantPriv, PrivPredicate wanted) {
        return grantPriv.satisfy(wanted);
    }

    @Override
    public String toString() {
        return name;
    }

    public static void checkIncorrectPrivilege(Privilege[] incorrectPrivileges,
                                               Collection<Privilege> privileges) throws AnalysisException {
        for (int i = 0; i < incorrectPrivileges.length; i++) {
            if (privileges.contains(incorrectPrivileges[i])) {
                throw new AnalysisException(
                    String.format("Can not grant/revoke %s to/from any other users or roles",
                        incorrectPrivileges[i]));
            }
        }
    }
}
