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
 * Gather distribution which put all data into one instance.
 */
// 在分布式数据库中，查询计划的执行通常需要将分散在各个 BE（Backend）节点上的数据汇总到一个单一的节点/实例（Instance）上进行集中处理。DistributionSpecGather 代表的就是这种单节点/集中式数据分布规格（Gather / Single Instance Distribution Specification）：
// 描述单节点集中分布状态：
// 它表示当前数据流已经被收集（Gather）到了某一个特定的 BE 实例或 FE 节点上（例如通过 Gather Exchange 算子）。
// 满足特定算子的分布约束：
// 许多物理算子要求输入数据必须在同一个节点上才能正确执行，例如：
// 全局 Limit / TopN：全局 ORDER BY ... LIMIT n 需要将各个节点的局部结果汇总到一个节点进行最终的全局排序与截取。
// 全局无 Group By 的聚合（Global Non-Group-By Aggregate）：如 SELECT COUNT(*) FROM t，需要将所有节点的局部 count 结果汇总到一个节点相加。
// Physical Empty Relation / Data Scan：某些特殊的单点逻辑算子。
// 当这些上层算子向下传递物理属性要求时，就会下发 DistributionSpecGather。
// 驱动 Enforcer 插入 Gather Exchange 节点：
// 如果子节点当前的实际分布不是 DistributionSpecGather（例如是分散在多台机器上的 DistributionSpecHash），优化器在进行物理属性满足性检查（satisfy）时会返回 false，从而触发 Enforcer 机制在中间插入一个 PhysicalDistribute(DistributionSpecGather) 算子（即 Gather 类型的 Exchange/Shuffle）。
public class DistributionSpecGather extends DistributionSpec {

    public static final DistributionSpecGather INSTANCE = new DistributionSpecGather();

    public DistributionSpecGather() {
        super();
    }

    @Override
    public boolean satisfy(DistributionSpec other) {
        return other instanceof DistributionSpecGather || other instanceof DistributionSpecAny;
    }
}
