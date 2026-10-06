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

import com.google.common.base.Objects;
import com.google.gson.annotations.SerializedName;
// 专门用于抽象与唯一标识“列级别权限（Column-Level Privilege）维度”的核心键（Key）类。
// 在 Doris 的权限控制体系中，表级及以上的权限可以直接使用 TablePattern -> PrivBitSet 进行映射管理。但是，列级权限（如只允许用户查询某张表中的特定几列，即 GRANT SELECT(col1, col2) ON ctl.db.tbl TO ...）不仅需要定位到具体的数据表上下文（Catalog + Database + Table），还需要区分权限类型（如 SELECT_PRIV 等）。
// ColPrivilegeKey 的核心作用包括：
// 组合标识列权限作用域：将特定的权限类型索引（privilegeIdx）与表的三段式元数据定位符（ctl Catalog、db Database、tbl Table）强绑定，唯一确定一个列权限映射维度。
// 作为权限 Map 的哈希键（Key）：在 Role（角色）或 UserProperty（用户属性）中，列权限以 Map<ColPrivilegeKey, Set<String>> colPrivMap 的形式组织（其中 Key 为 ColPrivilegeKey，Value 为该权限下允许访问的列名集合 Set<String>）。
public class ColPrivilegeKey {
    // 列权限类型的索引值（对应 Privilege 枚举中的 idx 属性，例如 SELECT_PRIV 的索引），参与 JSON 序列化持久化。
    // 指定当前列权限的类型（如只读 SELECT 权限）。
    @SerializedName(value = "privilegeIdx")
    private int privilegeIdx;
    // 所属 Catalog（数据目录）的名称，参与 JSON 序列化持久化。
    @SerializedName(value = "ctl")
    private String ctl;
    @SerializedName(value = "db")
    private String db;
    @SerializedName(value = "tbl")
    private String tbl;

    public ColPrivilegeKey(Privilege privilege, String ctl, String db, String tbl) {
        this.privilegeIdx = privilege.getIdx();
        this.ctl = ctl;
        this.db = db;
        this.tbl = tbl;
    }

    public Privilege getPrivilege() {
        return Privilege.getPriv(privilegeIdx);
    }

    public int getPrivilegeIdx() {
        return privilegeIdx;
    }

    public void setPrivilegeIdx(int privilegeIdx) {
        this.privilegeIdx = privilegeIdx;
    }

    public String getCtl() {
        return ctl;
    }

    public void setCtl(String ctl) {
        this.ctl = ctl;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getTbl() {
        return tbl;
    }

    public void setTbl(String tbl) {
        this.tbl = tbl;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ColPrivilegeKey that = (ColPrivilegeKey) o;
        return privilegeIdx == that.privilegeIdx
                && Objects.equal(ctl, that.ctl)
                && Objects.equal(db, that.db)
                && Objects.equal(tbl, that.tbl);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(privilegeIdx, ctl, db, tbl);
    }
}
