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

package org.apache.doris.planner;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.stream.Collectors;

/*
 * MultiCast plan fragment.
 */
// MultiCastPlanFragment 是分布式查询规划器（Planner）层用于支持多播/广播数据发送（MultiCast Data Stream）的一个特殊物理执行片段类
// 在分布式数据库中，通常一个 PlanFragment（执行片段）的输出数据流只会被发送给下游的单个消费节点或片段（例如经过 DataStreamSink / Exchange Node 发送给下一个算子）。
// 但在某些复杂的查询场景下（例如 CTE 共享公共子查询复用、Multi-Distinct 优化、或者一写多读的计算逻辑），同一个 Fragment 的计算结果需要同时分发给多个不同的下游 Fragment 消费。
// 支持一对多数据分发：封装并表示一个“一对多”数据广播/多播片段。它代表一段产生数据的管道，能够将其处理后的数据流并发地发送到多个下游目的地。
// 维护下游消费目标映射：内部维护了一个 downstream 接收节点列表（ExchangeNode 列表），用于在生成 Thrift 物理执行计划下发给 Backend（BE）时，指示 BE 节点的 DataStreamSink 或 MultiCastDataStreamSink 将数据同时发送到哪些下游目标节点。
public class MultiCastPlanFragment extends PlanFragment {
    // 目的地 Exchange 节点列表。
    // 存放所有消费当前 MultiCastPlanFragment 输出数据的下游 ExchangeNode 引用。由于数据要分发给多个下游，因此这里是一个 List，记录了所有的下游接收端算子。
    private final List<ExchangeNode> destNodeList = Lists.newArrayList();
    // 根据一个普通的 PlanFragment 实例构建/升级为一个多播执行片段 MultiCastPlanFragment。
    public MultiCastPlanFragment(PlanFragment planFragment) {
        super(planFragment.getFragmentId(), planFragment.getPlanRoot(), planFragment.getDataPartition(),
                planFragment.getBuilderRuntimeFilterIds(), planFragment.getTargetRuntimeFilterIds());
        this.hasColocatePlanNode = planFragment.hasColocatePlanNode;
        this.outputPartition = DataPartition.RANDOM;
        this.children.addAll(planFragment.getChildren());
    }
    // 向当前多播片段中注册/添加一个下游的目标接收节点。
    public void addToDest(ExchangeNode exchangeNode) {
        destNodeList.add(exchangeNode);
    }

    public List<ExchangeNode> getDestNodeList() {
        return destNodeList;
    }

    public List<PlanFragment> getDestFragmentList() {
        return destNodeList.stream().map(PlanNode::getFragment).collect(Collectors.toList());
    }
}
