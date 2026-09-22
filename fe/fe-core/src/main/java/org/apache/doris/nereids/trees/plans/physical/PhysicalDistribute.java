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
import org.apache.doris.nereids.properties.DistributionSpec;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.PropagateFuncDeps;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.json.JSONObject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Enforcer plan. Generated when upstream's physical property mismatches downstream's required physical property.
 */
// PhysicalDistribute 是一个非常关键的物理算子节点，属于强制/强化算子（Enforcer Plan）。
// 在 CBO（基于代价的优化器）进行物理属性（Physical Properties）推导时，上层算子（如 PhysicalHashJoin 或 PhysicalHashAggregate）通常对其子节点的数据分布（Distribution）有明确的要求（例如要求数据按 Join Key 进行 Hash Shuffle 分配，或者要求将数据广播至全节点）。
// PhysicalDistribute 类的核心作用：
// 网络传输与数据重分发（Exchange / Reshuffle）：它在物理执行计划中直接对应于后端 BE 执行时的 Data Stream Sender / Exchange Node，负责将数据从上游节点重分配到下游节点（如 Hash Shuffle、Gather 汇聚、Broadcast 广播等）。
// 物理属性强化（Physical Property Enforcer）：当上游子节点产出的数据物理分布规格（DistributionSpec）无法满足下游父节点的要求时，优化器会自动在两者之间“插入/强化”一个 PhysicalDistribute 节点，将其数据转换为下游所期望的 distributionSpec 目标分布。
// 透传数据特征：它实现了 PropagateFuncDeps 接口，表明数据在经过重分发后，其函数依赖（FD）、唯一性（Unique）等数据特征（Data Trait）依然得以保留和向上透传。

public class PhysicalDistribute<CHILD_TYPE extends Plan> extends PhysicalUnary<CHILD_TYPE>
        implements PropagateFuncDeps {
    // downstream's required physical property(i.e. target distribution spec)
    // 记录目标物理分布规格（Target Distribution Spec）
    // 这是该算子的核心状态，表示下游（父节点）所要求的数据分布形态。常见的子类实现有：
    protected DistributionSpec distributionSpec;

    // the upstream's physical property saves in base class
    // 传入目标分布 spec 和子节点 child。内部默认设置 groupExpression 为 Optional.empty()，并自动继承子节点的逻辑属性 child.getLogicalProperties()。
    public PhysicalDistribute(DistributionSpec spec, CHILD_TYPE child) {
        this(spec, Optional.empty(), child.getLogicalProperties(), child);
    }
    // 带有 Memo 节点表达式（GroupExpression）的构造函数
    // 在优化器将物理节点挂载/绑定到 Memo 图中的 Group 时使用。调用 super(PlanType.PHYSICAL_DISTRIBUTE, ...) 初始化父类 PhysicalUnary。
    public PhysicalDistribute(DistributionSpec spec, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_DISTRIBUTE, groupExpression, logicalProperties, child);
        this.distributionSpec = spec;
    }
    // 完全体构造函数，包含物理属性和基数统计信息。
    // 在优化器完成物理属性推导和 CBO 代价计算（Cost Model）后使用，将推导出的 PhysicalProperties 以及计算出的 Statistics 一并记录下来。
    public PhysicalDistribute(DistributionSpec spec, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, PhysicalProperties physicalProperties,
            Statistics statistics, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_DISTRIBUTE, groupExpression, logicalProperties, physicalProperties, statistics,
                child);
        this.distributionSpec = spec;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalDistribute[" + id.asInt() + "]" + getGroupIdWithPrefix(),
                "stats", statistics,
                "distributionSpec", distributionSpec
        );
    }
    // 将算子的核心状态导出为 JSON 对象。
    // 调用 super.toJson() 基础 JSON 对象后，在其中追加 Properties 键，将 distributionSpec 转为字符串记录进去，便于在 Web UI 或分析工具中可视化展示。
    @Override
    public JSONObject toJson() {
        JSONObject physicalDistributeJson = super.toJson();
        JSONObject properties = new JSONObject();
        properties.put("DistributionSpec", distributionSpec.toString());
        physicalDistributeJson.put("Properties", properties);
        return physicalDistributeJson;
    }

    public DistributionSpec getDistributionSpec() {
        return distributionSpec;
    }
    // 实现访问者模式（Visitor Pattern）。
    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalDistribute(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return ImmutableList.of();
    }

    // 替换当前节点的子节点，生成一个新的 PhysicalDistribute 副本。
    @Override
    public PhysicalDistribute<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return AbstractPlan.copyWithSameId(this, () -> new PhysicalDistribute<>(distributionSpec, Optional.empty(),
                getLogicalProperties(), physicalProperties, statistics, children.get(0)));
    }

    @Override
    public PhysicalDistribute<CHILD_TYPE> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return AbstractPlan.copyWithSameId(this, () -> new PhysicalDistribute<>(distributionSpec, groupExpression,
                getLogicalProperties(), child()));
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return AbstractPlan.copyWithSameId(this, () -> new PhysicalDistribute<>(distributionSpec, groupExpression,
                logicalProperties.get(), children.get(0)));
    }

    @Override
    public PhysicalDistribute<CHILD_TYPE> withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
            Statistics statistics) {
        return AbstractPlan.copyWithSameId(this, () -> new PhysicalDistribute<>(distributionSpec, groupExpression,
                getLogicalProperties(), physicalProperties, statistics, child()));
    }

    // 计算并返回当前算子的输出列列表（List<Slot>）
    // 直接返回 child().getOutput()。即数据重分发不改变数据表的 Schema 和列内容，输出列与子节点完全保持一致。
    @Override
    public List<Slot> computeOutput() {
        return child().getOutput();
    }

    // 将逻辑属性重置为 null，生成新副本。
    // 在需要重新强制推导或计算逻辑属性的优化阶段使用。
    @Override
    public PhysicalDistribute<CHILD_TYPE> resetLogicalProperties() {
        return new PhysicalDistribute<>(distributionSpec, groupExpression,
                null, physicalProperties, statistics, child());
    }

    @Override
    public String shapeInfo() {
        StringBuilder builder = new StringBuilder("PhysicalDistribute");
        builder.append("[").append(getDistributionSpec().shapeInfo()).append("]");
        ConnectContext context = ConnectContext.get();
        if (context != null
                && context.getSessionVariable().getDetailShapePlanNodesSet().contains(getClass().getSimpleName())) {
            if (distributionSpec instanceof DistributionSpecHash) {
                builder.append(" Hash Columns:");
                DistributionSpecHash hash = (DistributionSpecHash) distributionSpec;
                Map<ExprId, Slot> outputById = child().getOutput().stream()
                        .collect(Collectors.toMap(NamedExpression::getExprId, slot -> slot, (a, b) -> a));
                builder.append(
                        hash.getOrderedShuffledColumns().stream()
                                .map(id -> {
                                    Slot slot = outputById.get(id);
                                    return slot == null ? String.valueOf(id) : slot.getName();
                                })
                                .collect(Collectors.toList())
                );
            }
        }
        return builder.toString();
    }

    // 获取重分发时按顺序排列的 Hash 散列列对象（Slot）列表。
    /**getOrderedShuffledSlots*/
    public List<Slot> getOrderedShuffledSlots() {
        if (distributionSpec instanceof DistributionSpecHash) {
            DistributionSpecHash hash = (DistributionSpecHash) distributionSpec;
            Map<ExprId, Slot> outputById = child().getOutput().stream()
                    .collect(Collectors.toMap(NamedExpression::getExprId, slot -> slot, (a, b) -> a));
            return hash.getOrderedShuffledColumns().stream()
                    .map(outputById::get)
                    .collect(ImmutableList.toImmutableList());
        } else {
            return ImmutableList.of();
        }
    }
}
