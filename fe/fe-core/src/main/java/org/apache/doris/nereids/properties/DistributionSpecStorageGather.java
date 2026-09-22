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

package org.apache.doris.nereids.properties;

/**
 * Gather distribution which put all data into one instance and
 * the execution on it only could be done on the node storages its data.
 */
// DistributionSpecStorageGather 是物理属性（Physical Properties）推导与分布式执行计划生成体系中的一种特化的存储亲和型单节点分布规格（Storage-Affinitized Gather Distribution Specification）。
// 单节点集中分布（Gather）语义：
// 与 DistributionSpecGather 类似，它表示某份数据的所有行（Rows）都位于同一个 Backend（BE）物理节点/实例上。
// 存储局部性/亲和性约束（Storage Locality / Affinity）：
// 它的核心差异在于注释中所提到的：“the execution on it only could be done on the node storages its data”（其上的计算必须严格在存储该数据的具体 BE 节点上执行）。
// 常见应用场景：在读取某些特定表类型（如单 Tablet 的内表、特定 Unpartitioned 表）或执行需要本地数据亲和的扫描算子（Data Scan）时，数据只存在于某个具体的 BE 节点磁盘上。为了避免不必要的数据跨节点传输，下游依赖该数据的计算任务必须调度到存储该数据的特定 BE 节点上运行。
public class DistributionSpecStorageGather extends DistributionSpec {

    public static final DistributionSpecStorageGather INSTANCE = new DistributionSpecStorageGather();

    public DistributionSpecStorageGather() {
        super();
    }

    @Override
    public boolean satisfy(DistributionSpec other) {
        return other instanceof DistributionSpecGather
                || other instanceof DistributionSpecStorageGather
                || other instanceof DistributionSpecAny;
    }
}
