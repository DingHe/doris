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
 * Data can be in any instance
 */
// 在基于 Cascades 框架的查询优化器中，物理计划算子在执行时需要满足特定的物理属性（Physical Properties），其中最重要的属性之一就是数据分布属性（DistributionSpec）（例如 Hash 分布、Gather/Gather-To-Single-Node 集中分布、Replicate 广播分布等）。
// DistributionSpecAny 代表一种无任何特定要求/任意数据分布（Any Distribution Requirement）的物理分布规格：
// “通配符 / 极弱约束”物理属性：
//当某个上层父节点算子对下层子节点的输出数据分布没有任何特定格式限制（例如 Filter、Project、Logical/Physical Limit 等对数据在节点间的分布不敏感的算子）时，就会传递 DistributionSpecAny 作为对子节点的分布要求（Distribution Requirement）。
// 避免不必要的 Shuffle / Data Movement：
//在物理属性满足性检查（Property Satisfaction Matching）和 Enforcer（数据交换/Shuffle 插入机制）推导过程中，如果要求是 DistributionSpecAny，意味着子节点当前的任何物理分布状态（无论是 Hash 还是 Round-Robin）均符合要求，优化器不会在此处插入额外的 Exchange（Shuffle/Gather）算子，从而避免不必要的网络开销。
public class DistributionSpecAny extends DistributionSpec {

    public static final DistributionSpecAny INSTANCE = new DistributionSpecAny();

    private DistributionSpecAny() {
        super();
    }

    @Override
    public boolean satisfy(DistributionSpec other) {
        // 只有当父节点要求的分布（other）同样是 DistributionSpecAny（即父节点没有任何具体的分布约束）时，该方法才返回 true。
        return other instanceof DistributionSpecAny;
    }
}
