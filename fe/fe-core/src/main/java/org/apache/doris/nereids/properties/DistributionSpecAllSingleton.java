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
 * Gather. Then broadcast exact one instance to every BE. now only for DictionarySink
 */
// DistributionSpecAllSingleton 是物理属性（Physical Properties）推导与分布式执行计划生成体系中一种特殊的全节点单实例广播物理数据分布规格（All-Singleton / Broadcast Single-Instance Specification）。
// DistributionSpecAllSingleton 描述了一种两阶段组合的数据分布形态：先集中（Gather），再广播（Broadcast）。
// “Gather + Broadcast” 物理语义：
// Gather 阶段：首先将分散在各个 BE（Backend）节点上的数据流集中汇总到某一个特定的单节点/实例（Single Instance / Singleton）上进行处理。
// Broadcast 阶段：处理完成后，将这个唯一的单实例结果广播（Broadcast）分发给集群中的每一个 BE 节点。
// 注释明确指出：now only for DictionarySink（目前主要用于字典写入/字典表构建场景）。在 Doris 构建全局字典（Global Dictionary）或向字典索引/字典表（DictionarySink）写入数据时：
// 字典数据的生成或更新通常要求全局一致且必须在单一节点上完成排序、去重或字典编码分配（Gather 到单实例）。
// 编码完成后，为了让后续所有的 BE 节点在执行查询或写入时都能直接在本地读取并使用最新的完整字典，必须将这同一个单实例的字典结果广播给集群里的每一个 BE 节点（Broadcast 到所有 BE）。
// 驱动物理 Plan 插入对应的 Exchange 算子：
// 当物理算子（如 DictionarySink）下发 DistributionSpecAllSingleton 要求时，优化器会检查子节点的分布。
// 如果不满足该规格，Enforcer 机制会在中间插入对应的 Gather 与 Broadcast 两阶段 Exchange/Distribute 算子。

public class DistributionSpecAllSingleton extends DistributionSpec {

    public static final DistributionSpecAllSingleton INSTANCE = new DistributionSpecAllSingleton();

    private DistributionSpecAllSingleton() {
        super();
    }

    // only accept itself
    @Override
    public boolean satisfy(DistributionSpec other) {
        return other instanceof DistributionSpecAllSingleton;
    }
}
