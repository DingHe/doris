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

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.util.MutableState;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Abstract class for all concrete physical plan.
 */
// AbstractPhysicalPlan 是 Apache Doris Nereids 新优化器框架中所有具体物理算子（Concrete Physical Plan Node）的抽象基类。
// 作为整个物理计划节点体系骨架的核心类，它的主要作用包括：
// 统一物理上下文抽象：为所有物理算子（如 PhysicalHashJoin、PhysicalOlapScan、PhysicalFilter 等）提供通用的状态存储与行为规范，
// 将逻辑属性（LogicalProperties）、物理属性（PhysicalProperties）以及统计信息（Statistics）无缝串联。
// 运行时过滤（Runtime Filter, RF）生命周期管理：封装物理执行计划在 CBO（基于代价的优化）推导与物理节点优化阶段生成的 Runtime Filter，
// 维护“已生成/收集的 RF（runtimeFilters）”与“已成功应用/下推的 RF（appliedRuntimeFilters）”。
// Memo 组与统计信息传承：提供算子转换与优化过程中（如克隆、重构算子树时）快捷复制源节点统计信息（Stats）和 Memo Group ID 的通用能力（copyStatsAndGroupIdFrom）。
// EXPLAIN 可视化支持：实现 Explainable 接口，为执行计划的树状文本化格式输出（Explain/Profile）提供统一入口。
public abstract class AbstractPhysicalPlan extends AbstractPlan implements PhysicalPlan, Explainable {
    // 记录当前物理算子输出数据满足的物理属性（Physical Properties）。
    // 包含数据的物理分布规格（DistributionSpec，如 Hash 分布、Gather 单点汇聚等）以及排序规格（OrderSpec）。如果构造时传入 null，默认初始化为 PhysicalProperties.ANY（任意分布与顺序）。
    protected final PhysicalProperties physicalProperties;
    protected final List<RuntimeFilter> runtimeFilters = Lists.newArrayList();
    private final List<RuntimeFilter> appliedRuntimeFilters = Lists.newArrayList();

    public AbstractPhysicalPlan(PlanType type, LogicalProperties logicalProperties, Plan... children) {
        this(type, Optional.empty(), logicalProperties, children);
    }

    public AbstractPhysicalPlan(PlanType type, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, Plan... children) {
        this(type, groupExpression, logicalProperties, PhysicalProperties.ANY, null, children);
    }

    public AbstractPhysicalPlan(PlanType type, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, @Nullable PhysicalProperties physicalProperties,
            Statistics statistics, Plan... children) {
        super(type, groupExpression,
                logicalProperties == null ? Optional.empty() : Optional.of(logicalProperties),
                statistics, ImmutableList.copyOf(children));
        this.physicalProperties =
                physicalProperties == null ? PhysicalProperties.ANY : physicalProperties;
    }

    public PhysicalProperties getPhysicalProperties() {
        return physicalProperties;
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) {
        return this;
    }

    public <T extends AbstractPhysicalPlan> AbstractPhysicalPlan copyStatsAndGroupIdFrom(T from) {
        T newPlan = (T) withPhysicalPropertiesAndStats(
                from.getPhysicalProperties(), from.getStats());
        newPlan.setMutableState(MutableState.KEY_GROUP, from.getGroupIdAsString());
        return newPlan;
    }

    public List<org.apache.doris.nereids.trees.plans.physical.RuntimeFilter> getAppliedRuntimeFilters() {
        return appliedRuntimeFilters;
    }

    public void addAppliedRuntimeFilter(org.apache.doris.nereids.trees.plans.physical.RuntimeFilter filter) {
        appliedRuntimeFilters.add(filter);
    }

    public void addRuntimeFilter(RuntimeFilter filter) {
        runtimeFilters.add(filter);
    }

    public List<RuntimeFilter> getRuntimeFilters() {
        return runtimeFilters;
    }

    public void removeAppliedRuntimeFilter(org.apache.doris.nereids.trees.plans.physical.RuntimeFilter filter) {
        appliedRuntimeFilters.remove(filter);
    }
}
