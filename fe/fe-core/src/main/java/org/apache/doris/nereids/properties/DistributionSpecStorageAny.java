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
 * Data can be in any instance, but it restricted by physical storage nodes.
 * When Plan's distribution is DistributionSpecStorageAny,
 * the execution on it only could be done on the node storages its data.
 */
// DistributionSpecStorageAny 是物理属性（Physical Properties）推导与分布式执行计划生成体系中一种带有存储亲和性约束的任意物理数据分布规格（Storage-Affinitized Any Distribution Specification）。
// DistributionSpecStorageAny 结合了“任意分布（Any Distribution）”与“存储局部性（Storage Locality）”的双重语义：
// “数据分布任意，但计算受限于存储节点”物理语义：
// 注释明确说明：Data can be in any instance, but it restricted by physical storage nodes.（数据可以分布在任意实例/节点上，但受限于物理存储节点）。
// 约束计算调度的存储局部性（Storage Affinity）：
// the execution on it only could be done on the node storages its data.（其上的计算只能在存储该数据的具体节点上执行）。
// 典型应用场景：在扫描基础表（Data Scan / Table Scan）时，数据存储在集群中若干指定的 BE（Backend）节点磁盘/Tablet 上。从总体架构看，数据在集群中的分布形态是分散且不特定（类似于 Any）；但由于数据已经落盘在特定的 BE 节点上，针对该数据 Scan 的计算任务必须严格调度到保存该 Tablet/数据的物理 BE 节点上运行，以避免不必要的跨网络数据传输。
// 优化器属性匹配与物理 Plan 推导：
// 物理优化阶段，DistributionSpecStorageAny 明确告知优化器和调度器（Scheduler）：当前算子的数据流虽然没有特定的 Hash 或 Partition 键分布约束，
// 但其物理位置已被锁定在具体的存储节点，后续算子需要尊重这一存储亲和性。
public class DistributionSpecStorageAny extends DistributionSpec {

    public static final DistributionSpecStorageAny INSTANCE = new DistributionSpecStorageAny();

    private DistributionSpecStorageAny() {
        super();
    }

    @Override
    public boolean satisfy(DistributionSpec other) {
        return other instanceof DistributionSpecAny || other instanceof DistributionSpecStorageAny;
    }
}
