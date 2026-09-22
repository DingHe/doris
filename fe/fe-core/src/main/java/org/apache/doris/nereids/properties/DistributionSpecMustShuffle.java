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
 * present data must use after shuffle
 */
// DistributionSpecMustShuffle 是物理属性（Physical Properties）推导与分布式执行计划生成体系中一种特殊的强制 Shuffle 物理数据分布规格（Must-Shuffle Specification）。
// 在分布式查询优化中，物理算子（如 Hash Join、Aggregate、Exchange 等）会对输入的物理数据分布提出要求。DistributionSpecMustShuffle 的核心作用是标记一种“必须经过重新分发（Shuffle）才能满足要求”的特殊中间状态：
// 强制触发数据重分发（Enforce Shuffle）：
// 注释明确指出：present data must use after shuffle（表示当前数据必须在 Shuffle 之后才能使用）。在优化器的属性推导和匹配过程中，如果上层算子对数据分布有某种特定的物理分布要求（例如要求按特定 Key 分发的 DistributionSpecHash 或单节点汇总的 DistributionSpecGather），DistributionSpecMustShuffle 会故意拒绝（返回 false）这些物理要求。
// 驱动物理 Plan 插入 Exchange 算子：
// 由于其 satisfy 方法对于几乎所有的具体物理分布要求均返回 false，物理优化器的 Enforcer 机制会被强制触发，从而在当前算子上方插入一个物理 PhysicalDistribute（即 Exchange / Shuffle）节点，强制将数据重新打散分发。
public class DistributionSpecMustShuffle extends DistributionSpec {

    public static final DistributionSpecMustShuffle INSTANCE = new DistributionSpecMustShuffle();

    public DistributionSpecMustShuffle() {
        super();
    }

    @Override
    public boolean satisfy(DistributionSpec other) {
        return other instanceof DistributionSpecAny;
    }
}
