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

package org.apache.doris.analysis;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.Auth.PrivLevel;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Three-segment-format: catalog.database.table. If the lower segment is specific,
 * the higher segment can't be a wildcard. The following examples are not allowed:
 * "ctl1.*.table1", "*.*.table2", "*.db1.*", ...
 */
// TablePattern 是前端（Frontend, FE）模块中用于匹配和限定数据资源（Catalog、Database、Table）及权限控制范围的核心抽象类。
// TablePattern 主要用于实现 三段式资源匹配模式 (catalog.database.table)，主要应用在 权限控制（Privilege/Grant/Revoke） 和 数据对象识别 场景中。
// 具备以下核心特性与作用：
// 统一资源抽象：支持通过通配符 * 代表全局、Catalog 级、Database 级或 Table 级的资源表达（例如 *.*.* 表示全局，ctl1.db1.* 表示指定库下的所有表）。
// 层次化约束与合法性校验（Analyze）：强制要求资源匹配模式符合从上到下的确定性原则（若底层节点是具体名称，上层节点不能使用通配符 *）。例如，不允许出现 *.db1.tbl1 或 ctl1.*.tbl1 这种越级通配的非法模式。
// 权限级别映射：根据资源模式中通配符 * 的位置，精准映射并计算出当前的权限控制级别（GLOBAL、CATALOG、DATABASE 或 TABLE）。
// 元数据持久化支持：实现了 Writable 接口和 Gson 的 GsonPostProcessable 接口，支持将对象序列化为 JSON 写入磁盘或网络流，并能在反序列化后自动恢复分析状态。
public class TablePattern implements Writable, GsonPostProcessable {
    // 表示 Catalog（数据目录）名称。使用 @SerializedName("ctl") 标注，用于 Gson 序列化。如果未指定或为通配，值可能为 * 或具体 Catalog 名字。
    @SerializedName(value = "ctl")
    private String ctl;
    // 表示 Database（数据库）名称。使用 @SerializedName("db") 标注。若为空则默认为通配符 *。
    @SerializedName(value = "db")
    private String db;
    // 表示 Table（表）名称。使用 @SerializedName("tbl") 标注。若为空则默认为通配符 *。
    @SerializedName(value = "tbl")
    private String tbl;
    // 标识当前对象是否已经通过语义分析（Analyze/Validation）。许多 Getter 方法和序列化操作都依赖此标记（必须为 true 才能调用）。
    boolean isAnalyzed = false;

    public static TablePattern ALL;

    static {
        ALL = new TablePattern("*", "*", "*");
        try {
            ALL.analyze();
        } catch (AnalysisException e) {
            // will not happen
        }
    }

    private TablePattern() {
    }

    public TablePattern(String ctl, String db, String tbl) {
        this.ctl = Strings.isNullOrEmpty(ctl) ? "*" : ctl;
        this.db = Strings.isNullOrEmpty(db) ? "*" : db;
        this.tbl = Strings.isNullOrEmpty(tbl) ? "*" : tbl;
    }

    public TablePattern(String db, String tbl) {
        this.ctl = null;
        this.db = Strings.isNullOrEmpty(db) ? "*" : db;
        this.tbl = Strings.isNullOrEmpty(tbl) ? "*" : tbl;
    }
    // 获取分析后合格的 Catalog 名称。
    // 通过 Preconditions.checkState(isAnalyzed) 强制要求必须在 analyze() 之后调用，直接返回 ctl。
    public String getQualifiedCtl() {
        Preconditions.checkState(isAnalyzed);
        return ctl;
    }

    public String getQualifiedDb() {
        Preconditions.checkState(isAnalyzed);
        return db;
    }

    public String getTbl() {
        return tbl;
    }
    // 判定当前模式所对应的权限级别（PrivLevel）的核心方法。
    public PrivLevel getPrivLevel() {
        Preconditions.checkState(isAnalyzed);
        // Catalog 为通配符 *。
        // 由于 analyze() 校验规则不允许出现类似 *.db.tbl 的跨级通配，因此当 ctl 为 * 时，db 和 tbl 必然也为 *（即 *.*.*），代表全局最高权限级别。
        if (ctl.equals("*")) {
            return PrivLevel.GLOBAL;
        // Catalog 指定了具体名称，但 Database 为通配符 *（模式如 ctl1.*.*）。
        // 代表指定 Catalog 级别的权限，作用于该 Catalog 下的所有数据库和表。
        } else if (db.equals("*")) {
            return PrivLevel.CATALOG;
        // Catalog 和 Database 都指定了具体名称，但 Table 为通配符 *（模式如 ctl1.db1.*）
        // 代表指定数据库级别的权限，作用于该数据库下的所有表。
        } else if (tbl.equals("*")) {
            return PrivLevel.DATABASE;
        } else {
            // Catalog、Database 和 Table 三段均指定了具体名称（模式如 ctl1.db1.tbl1）。
            // 代表指定表级别的权限，只作用于某张具体的表。
            return PrivLevel.TABLE;
        }
    }

    private void analyze(String catalogName) throws AnalysisException {
        if (isAnalyzed) {
            return;
        }
        this.ctl = Strings.isNullOrEmpty(catalogName) ? InternalCatalog.INTERNAL_CATALOG_NAME : catalogName;
        if ((!tbl.equals("*") && (db.equals("*") || ctl.equals("*")))
                || (!db.equals("*") && ctl.equals("*"))) {
            throw new AnalysisException("Do not support format: " + toString());
        }
        isAnalyzed = true;
    }

    public void analyze() throws AnalysisException {
        analyze(ctl);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TablePattern)) {
            return false;
        }
        TablePattern other = (TablePattern) obj;
        return ctl.equals(other.getQualifiedCtl()) && db.equals(other.getQualifiedDb()) && tbl.equals(other.getTbl());
    }

    @Override
    public int hashCode() {
        return Stream.of(ctl, db, tbl).filter(Objects::nonNull)
                .map(String::hashCode)
                .reduce(17, (acc, h) -> 31 * acc + h);
    }

    @Override
    public String toString() {
        return Stream.of(ctl, db, tbl).filter(Objects::nonNull).collect(Collectors.joining("."));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Preconditions.checkState(isAnalyzed);
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        isAnalyzed = true;
    }
}
