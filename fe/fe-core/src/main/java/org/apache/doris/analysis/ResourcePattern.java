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
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.mysql.privilege.Auth.PrivLevel;
import org.apache.doris.persist.gson.GsonPostProcessable;

import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;

import java.io.IOException;

// only the following 2 formats are allowed
// *
// resource
// 用于抽象与表示“非表类资源”权限匹配模式的核心类
// 在 Doris 的权限控制体系中，数据对象（如 Catalog、Database、Table）使用 TablePattern 来表达授权作用域；而对于非数据表类的各种系统级资源（如 Spark Resource、ODBC Resource、存算分离模式下的 Compute Cluster、Stage 资源以及 Storage Vault 存储库等），
// 则统一由 ResourcePattern 来描述其授权作用域与目标对象。
// 其核心作用包括：
// 定义授权对象：标识具体的资源名称（如 spark_resource_1）或匹配所有资源的通配模式（如 % / *）。
// 标识资源类型：区分不同的资源类别（通过 ResourceTypeEnum 枚举区分通用资源 GENERAL、计算集群 CLUSTER、数据暂存区 STAGE、存储库 STORAGE_VAULT）。
public class ResourcePattern implements GsonPostProcessable {
    // 资源的匹配名称，参与 JSON 序列化持久化。
    @SerializedName(value = "resourceName")
    private String resourceName;

    // just for cloud
    // GRANT USAGE_PRIV ON CLUSTER '${clusterName}' TO '${userName}';
    // 资源的类型枚举，参与 JSON 序列化持久化（主要用于云原生/存算分离扩展以及不同资源分类隔离）。
    // 常见取值包括 GENERAL（常规资源）、CLUSTER（存算分离计算集群）、STAGE（数据暂存区）、STORAGE_VAULT（存储库）
    @SerializedName(value = "resourceType")
    private ResourceTypeEnum resourceType;
    // 代表匹配所有“通用资源（GENERAL）”的全局通配模式对象。对应匹配名称为 %，类型为 ResourceTypeEnum.GENERAL。
    public static ResourcePattern ALL_GENERAL;
    // 代表匹配所有“计算集群（CLUSTER，主要用于存算分离架构）”的全局通配模式对象。对应匹配名称为 %，类型为 ResourceTypeEnum.CLUSTER。
    public static ResourcePattern ALL_CLUSTER;
    // 代表匹配所有“Stage 资源（STAGE，数据导入暂存区）”的全局通配模式对象。对应匹配名称为 %，类型为 ResourceTypeEnum.STAGE。
    public static ResourcePattern ALL_STAGE;
    // 代表匹配所有“存储库（STORAGE_VAULT，存算分离存储元数据）”的全局通配模式对象。对应匹配名称为 %，类型为 ResourceTypeEnum.STORAGE_VAULT。
    public static ResourcePattern ALL_STORAGE_VAULT;

    static {
        ALL_GENERAL = new ResourcePattern("%", ResourceTypeEnum.GENERAL);
        ALL_CLUSTER = new ResourcePattern("%", ResourceTypeEnum.CLUSTER);
        ALL_STAGE = new ResourcePattern("%", ResourceTypeEnum.STAGE);
        ALL_STORAGE_VAULT = new ResourcePattern("%", ResourceTypeEnum.STORAGE_VAULT);

        try {
            ALL_GENERAL.analyze();
            ALL_CLUSTER.analyze();
            ALL_STAGE.analyze();
            ALL_STORAGE_VAULT.analyze();
        } catch (AnalysisException e) {
            // will not happen
        }
    }

    public ResourcePattern(String resourceName, ResourceTypeEnum type) {
        // To be compatible with previous syntax
        if ("*".equals(resourceName)) {
            resourceName = "%";
        }
        this.resourceName = Strings.isNullOrEmpty(resourceName) ? "%" : resourceName;
        resourceType = type;
    }

    public void setResourceType(ResourceTypeEnum type) {
        resourceType = type;
    }

    public ResourceTypeEnum getResourceType() {
        return resourceType;
    }

    public String getResourceName() {
        return resourceName;
    }

    public PrivLevel getPrivLevel() {
        return PrivLevel.RESOURCE;
    }

    public void analyze() throws AnalysisException {
        if (!resourceName.equals("%")) {
            FeNameFormat.checkResourceName(resourceName, resourceType);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ResourcePattern)) {
            return false;
        }
        ResourcePattern other = (ResourcePattern) obj;
        return resourceName.equals(other.getResourceName()) && resourceType.equals(other.resourceType);
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + resourceName.hashCode() + resourceType.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return resourceName;
    }

    @Override
    public void gsonPostProcess() throws IOException {
        // // To be compatible with previous syntax
        if ("*".equals(resourceName)) {
            resourceName = "%";
        }
        // 2.x -> 3.0 compatibility logic
        if (resourceType == null) {
            resourceType = ResourceTypeEnum.GENERAL;
        }
    }
}
