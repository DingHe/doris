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
import org.apache.doris.mysql.privilege.Auth.PrivLevel;

import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;
// WorkloadGroupPattern 是 Apache Doris FE（Frontend）模块中专门用于抽象和表示 Workload Group（工作负载组）权限匹配模式的类
// Workload Group 是 Doris 用于实现资源隔离与负载管理（如限制查询 CPU、内存资源使用等）的核心机制。为了对 Workload Group 进行细粒度的权限控制（例如：限制哪些用户或角色可以使用特定的 Workload Group），Doris 在 RBAC 权限体系中引入了对应的资源匹配模式。
// WorkloadGroupPattern 的核心作用包括：
// 表达授权作用域：标识具体的目标 Workload Group 名称，作为授权/撤权语句（GRANT / REVOKE）作用的对象。
// 标识权限层级：明确指示该权限模式属于 PrivLevel.WORKLOAD_GROUP 层级。
public class WorkloadGroupPattern {
    // 保存目标 Workload Group 的名称，带有 Gson 的 @SerializedName 注解，参与元数据的 JSON 序列化与反序列化。
    @SerializedName(value = "workloadGroupName")
    private String workloadGroupName;

    private WorkloadGroupPattern() {
    }

    public WorkloadGroupPattern(String workloadGroupName) {
        this.workloadGroupName = workloadGroupName;
    }

    public String getworkloadGroupName() {
        return workloadGroupName;
    }

    public PrivLevel getPrivLevel() {
        return PrivLevel.WORKLOAD_GROUP;
    }

    public void analyze() throws AnalysisException {
        if (Strings.isNullOrEmpty(workloadGroupName)) {
            throw new AnalysisException("Workload group name is empty.");
        }
        if (workloadGroupName.equals("*")) {
            throw new AnalysisException("Global workload group priv is not supported.");
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof WorkloadGroupPattern)) {
            return false;
        }
        WorkloadGroupPattern other = (WorkloadGroupPattern) obj;
        return workloadGroupName.equals(other.getworkloadGroupName());
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + workloadGroupName.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return workloadGroupName;
    }
}
