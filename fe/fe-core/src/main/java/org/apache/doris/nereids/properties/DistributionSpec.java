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

import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;

import com.google.common.collect.Lists;

/**
 * Spec of data distribution.
 * GPORCA has more type in CDistributionSpec.
 */
// DistributionSpec 是 Apache Doris Nereids 新优化器框架中用于描述数据物理分布规格（Data Distribution Specification）的抽象基类。
// 在 Cascades / GPORCA 风格的现代查询优化器中，执行计划由“逻辑算子”转换为“物理算子”时，
// 不仅要考虑数据本身的 Schema，还要考虑数据在集群多节点/多线程间的物理分布属性（例如：是否是 Hash 分片、是否已经按某 Key 倾斜/汇总、是否广播等）。DistributionSpec 的核心作用包括：
// 物理属性（Physical Property）建模：作为物理属性描述符，定义了计划节点输出数据的物理分布状态（如 DistributionSpecHash、DistributionSpecGather、DistributionSpecAny 等）。
// 属性满足与推导（Satisfy 机制）：父算子可以对其子算子提出特定的数据分布要求（Require Spec），DistributionSpec 提供了判定“当前实际数据分布是否满足上层要求”的接口。
// 强制执行器（Enforcer）自动插入：当子节点当前的数据分布不满足父算子的要求时，优化器会调用 addEnforcer() 方法，
// 在子节点上方自动强制插入一个负责数据重分发的物理算子（PhysicalDistribute，即 Shuffle/Exchange 算子），以确保生成的物理执行计划语义正确。
public abstract class DistributionSpec {
    /**
     * Self satisfies other DistributionSpec.
     * Example:
     * `DistributionSpecGather` satisfies `DistributionSpecAny`
     */
    // 判断当前数据分布规范（Self）是否能满足另一种数据分布规范（other，通常是父节点要求的 DistributionSpec）
    // 满足关系（Satisfaction） 是物理属性推导的核心。举例来说：
    // 如果当前分布是 DistributionSpecGather（全量数据汇聚到单点），它能够满足 DistributionSpecAny（任意分布要求）。
    // 如果当前分布是按 [a, b] 列进行 Hash Shuffle，它能够满足要求按 [a, b] 进行 Hash 分布的上层算子（如 HashJoin）。
    // 如果返回 true，优化器认为无需重新 Shuffle 数据；如果返回 false，优化器则会强制插入数据重分发算子。
    public abstract boolean satisfy(DistributionSpec other);

    /**
     * Add physical operator of enforcer.
     */
    // 当当前子节点 child 的数据分布不满足父节点要求时，构建并插入一个物理重分发执行器（Enforcer / Distribute Physical Operator）
    // child：当前的子计划组（Cascades Memo 优化框架中的 Group）
    public GroupExpression addEnforcer(Group child) {
        // TODO:maybe we need to new a LogicalProperties or just do not set logical properties for this node.
        // If we don't set LogicalProperties explicitly, node will compute a applicable LogicalProperties for itself.
        PhysicalDistribute<GroupPlan> distribution = new PhysicalDistribute<>(
                this,
                child.getGroupPlan());
        return new GroupExpression(distribution, Lists.newArrayList(child));
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }

    public String shapeInfo() {
        return this.getClass().getSimpleName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return 0;
    }
}
