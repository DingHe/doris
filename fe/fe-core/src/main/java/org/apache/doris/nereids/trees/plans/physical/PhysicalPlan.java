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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.statistics.Statistics;

/**
 * interface for all physical plan.
 */
// PhysicalPlan 是 Apache Doris Nereids 新优化器框架中所有物理算子/物理执行计划（Physical Plan）的顶层核心接口，继承自 generic 的 Plan 接口。
// 在现代 Cascade/GPORCA 架构的查询优化器中，执行计划分为逻辑计划（Logical Plan）和物理计划（Physical Plan）两个阶段：
// 语义解耦与执行抽象：Logical Plan 只关注“要做什么”（例如 LogicalJoin），而 PhysicalPlan 则具体定义了“怎么在 BE 上真正高效执行”（例如 PhysicalHashJoin 或 PhysicalNestedLoopJoin）。
// 物理属性绑定：物理计划阶段引入了物理属性（PhysicalProperties），如数据的分布方式（Shuffle/Hash/Gather）以及数据的排序状态（Order），用于确保物理分布式执行的语义正确性。
// 代价模型与统计信息关联：物理算子会与统计信息（Statistics）及 Cost（执行代价） 强绑定，优化器借此选出全局 Cost 最优的最终物理执行计划。
public interface PhysicalPlan extends Plan {
    // 获取当前物理算子输出数据的物理属性（PhysicalProperties）
    // PhysicalProperties 包含两个核心维度：数据分布规格（DistributionSpec）（如 Hash 分布、Gather 单点汇聚）和排序规格（OrderSpec）（如按某列升序/降序）。
    // 优化器在做属性推导（Property Enforcement）时，会调用此方法获取子节点的属性，判断是否需要插入重分发（Exchange）或排序（Sort）算子。
    PhysicalProperties getPhysicalProperties();
    // 基于当前物理算子，不可变地（Immutable）生成并返回一个新的物理算子对象，同时为其赋予指定的物理属性和统计信息。
    // Nereids 优化器中的 Plan 树节点是不可变的。在 Memo 优化过程或属性推导阶段，优化器需要为物理算子绑定/更新推导出的 PhysicalProperties 以及 CBO 计算出的 Statistics。
    // 返回绑定了新属性/统计信息的当前物理算子拷贝，确保了优化器节点变换时的不可变安全性。
    PhysicalPlan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
            Statistics statistics);
    // 重置或重新计算当前物理算子的逻辑属性（LogicalProperties，如输出列 Output Slots、数据可空性 Nullable 等）。
    default PhysicalPlan resetLogicalProperties() {
        return this;
    }
}
