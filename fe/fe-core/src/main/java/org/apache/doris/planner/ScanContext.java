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

package org.apache.doris.planner;

/**
 * Shared context for scan planning/runtime decisions.
 * <p>
 * Keep this object immutable so scan nodes can safely cache it and
 * we can evolve fields incrementally in future.
 */
// 统一管理数据扫描（Scan）阶段的共享上下文与运行时决策：
// 在执行计划（Planning）和运行时（Runtime）阶段，为各种扫描节点（ScanNode，如 OlapScanNode、FileScanNode 等）提供统一、通用的配置和上下文信息。
// 保证线程安全与高效缓存（不可变设计）：
// 该类被设计为 final 类 且其内部属性均为 final，确保其实例是完全不可变（Immutable）的。
// 不可变性使得各种 ScanNode 可以安全地共享和缓存 ScanContext 对象，无需担心多线程下的数据竞争或并发修改问题。
// 架构解耦与未来扩展性：
// 用于解耦扫描节点与全局上下文（如 ConnectContext），将扫描所需的核心变量提取出来。未来若需在扫描阶段新增通用控制参数（如特定调优参数、存储选择策略等），可以平滑且渐进地在 ScanContext 中进行扩展，而不会破坏现有架构。
// 支撑云原生/存算分离架构：
// 当前核心作用是传递云原生架构下的 计算集群名称（clusterName），以便物理 ScanNode 能够将数据读取任务精确分配到指定的 Compute Group / Compute Cluster（计算集群）上执行。
public final class ScanContext {
    public static final ScanContext EMPTY = new ScanContext("");
    // 存储当前扫描操作所对应的云计算集群（Compute Cluster）名称。非 null（若传入 null 会自动归一化为 ""）。
    private final String clusterName;

    private ScanContext(String clusterName) {
        this.clusterName = clusterName == null ? "" : clusterName;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getClusterName() {
        return clusterName;
    }

    public static final class Builder {
        private String clusterName = "";

        public Builder clusterName(String clusterName) {
            this.clusterName = clusterName;
            return this;
        }

        public ScanContext build() {
            if (clusterName == null || clusterName.isEmpty()) {
                return ScanContext.EMPTY;
            }
            return new ScanContext(clusterName);
        }
    }
}
