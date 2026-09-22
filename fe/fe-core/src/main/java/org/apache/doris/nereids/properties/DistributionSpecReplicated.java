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
 * Data is replicated across all instances.
 * Like: broadcast join.
 */
// DistributionSpecReplicated 用于表示数据流在分布式集群中处于全量复制（Replicated / Broadcast）的分布状态。
// 物理语义：数据流的完整副本被分发（广播）到了每一个参与计算的 BE 节点（执行实例）上。
// 应用场景：最典型的应用就是 Broadcast Join（广播连接）。在 Hash Join 算子中，优化器会将小表构建侧（Build Side）的数据通过 Exchange 节点广播到大表探查侧（Probe Side）所在的所有节点，此时小表数据流的物理分布规格就会被赋予 DistributionSpecReplicated。
public class DistributionSpecReplicated extends DistributionSpec {

    public static final DistributionSpecReplicated INSTANCE = new DistributionSpecReplicated();

    @Override
    public boolean satisfy(DistributionSpec other) {
        return other instanceof DistributionSpecReplicated || other instanceof DistributionSpecAny;
    }
}
