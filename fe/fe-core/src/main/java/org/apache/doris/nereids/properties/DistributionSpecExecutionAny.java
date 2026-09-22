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
 * Data can be in any instance, used in PhysicalDistribute.
 * Because all candidates in group could save as DistributionSpecAny's value in LowestCostPlan map
 * to distinguish DistributionSpecAny, we need a new Spec to represent must shuffle require.
 */
// 在 Apache Doris 的 Nereids 新一代 CBO（基于成本的优化器）中，DistributionSpecExecutionAny 是物理属性（Physical Properties）推导与 Cascades 优化框架搜索体系中的一个特殊的物理数据分布规格（Distribution Spec）。
// 在 Nereids 优化器的 Cascades 搜索框架中，优化器采用自顶向下的搜索策略，通过匹配算子的 要求物理属性（Required Physical Properties） 与子算子提供的 实际物理属性（Provided Physical Properties），并在 Memo 结构的 LowestCostPlan 映射表（Map<PhysicalProperties, Cost>）中记录不同物理属性下的最小代价计划。
// DistributionSpecExecutionAny 的引入主要是为了解决以下核心问题：
// 区分“要求（Requirement）”与“物理执行状态（Execution State）”：
// DistributionSpecAny 是一种抽象的要求，代表上层算子对下层算子的数据分布“无任何特定要求（Any）”。
// DistributionSpecExecutionAny 则代表一种具体的物理分布状态，专门用于物理 Shuffle 算子（PhysicalDistribute）。它表示数据已经被强制调度/执行到了各个具体的 BE 实例节点上（Must Shuffle Requirement）。虽然它不限制具体的 Hash Key 或 Partition 规则，但它表明数据已经经过了物理 Shuffle 阶段。
// 解决 Memo 缓存（LowestCostPlan Map）中的键冲突与覆盖问题：
// 在 Cascades 框架中，一个 Group 内的所有候选物理计划都会将其计算出的物理属性和 Cost 保存到 LowestCostPlan 映射表中。
// 如果 PhysicalDistribute 算子产出的实际分布也被简单归类为 DistributionSpecAny，就会在 LowestCostPlan Map 中与父节点下发的“无要求（DistributionSpecAny）”发生语义混淆，导致不同执行状态下的 Cost 记账相互覆盖，甚至错选不需要 Shuffle 的计划。
public class DistributionSpecExecutionAny extends DistributionSpec {

    public static final DistributionSpecExecutionAny INSTANCE = new DistributionSpecExecutionAny();

    private DistributionSpecExecutionAny() {
        super();
    }

    @Override
    public boolean satisfy(DistributionSpec other) {
        return other instanceof DistributionSpecAny || other instanceof DistributionSpecExecutionAny;
    }
}
