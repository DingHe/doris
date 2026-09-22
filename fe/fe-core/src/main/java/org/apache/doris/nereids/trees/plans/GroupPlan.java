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

package org.apache.doris.nereids.trees.plans;

import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.logical.AbstractLogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalLeaf;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.LazyCompute;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * A virtual node that represents a sequence plan in a Group.
 * Used in {@link org.apache.doris.nereids.pattern.GroupExpressionMatching.GroupExpressionIterator},
 * as a place-holder when do match root.
 */
// 在 Apache Doris 的新一代查询优化器 Nereids 中，GroupPlan 是 Memo 数据结构以及 Pattern 模式匹配系统里的一个非常关键的占位与代理节点。
// 在基于 Cascades 框架的 Nereids 优化器中，执行计划（Plan Tree）会被探索并存储在 Memo（备忘录） 数据结构中：
// Memo 由多个 Group（组） 组成，每个 Group 表达一组逻辑上等价的表达式（GroupExpression）。
// 在对 Memo 中的计划树进行模式匹配（Pattern Matching）或规则优化（Rule Application）时，为了避免将子树完整展开成庞大的实体 Plan 结构，优化器引入了 GroupPlan。
// 主要作用总结：
//  Memo 节点的占位符（Placeholder / Leaf Wrapper）：
// GroupPlan 继承自 LogicalLeaf，代表一个没有子节点的逻辑叶子节点。它封装了一个 Group 引用，表明“这里代表某个 Group”，而不需要关心该 Group 内部具体包含哪些物理或逻辑计划。
// 隔离与延迟展开：
// 在做 Root 节点的 Pattern 匹配（如 GroupExpressionMatching）时，GroupPlan 充当了子计划的占位节点。只有在模式匹配成功、需要深入匹配子节点时，优化器才会迭代展开其对应的真正 Group。
// 复用 Group 的属性与元数据：
//尽管 GroupPlan 本身是一个虚拟的占位节点，但它代理了它所持有的 Group 的 LogicalProperties（例如输出列 Slot 等）和 Statistics（统计信息），使得上一层节点可以正常进行类型检查、列引用推导及代价估算。
public class GroupPlan extends LogicalLeaf implements BlockFuncDepsPropagation {
    // 当前 GroupPlan 所代表/封装的 Group（Memo 中的组对象）引用。
    private final Group group;

    public GroupPlan(Group group) {
        super(PlanType.GROUP_PLAN, Optional.empty(), LazyCompute.ofInstance(group.getLogicalProperties()), true);
        this.group = group;
    }

    @Override
    public Optional<GroupExpression> getGroupExpression() {
        return Optional.empty();
    }

    public Group getGroup() {
        return group;
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return ImmutableList.of();
    }

    @Override
    public Statistics getStats() {
        return group.getStatistics();
    }

    @Override
    public GroupPlan withChildren(List<Plan> children) {
        throw new IllegalStateException("GroupPlan can not invoke withChildren()");
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        throw new IllegalStateException("GroupPlan can not invoke withGroupExpression()");
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        throw new IllegalStateException("GroupPlan can not invoke withGroupExprLogicalPropChildren()");
    }

    @Override
    public List<Slot> computeOutput() {
        throw new IllegalStateException("GroupPlan can not compute output."
                + " You should invoke GroupPlan.getOutput()");
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitGroupPlan(this, context);
    }

    @Override
    public String toString() {
        return "GroupPlan( " + group.getGroupId() + " )";
    }

    @Override
    public String getFingerprint() {
        if (!getGroup().getLogicalExpressions().isEmpty()
                && getGroup().getLogicalExpressions().get(0).getPlan() instanceof AbstractLogicalPlan) {
            AbstractLogicalPlan logicalPlan = (AbstractLogicalPlan) getGroup()
                    .getLogicalExpressions().get(0).getPlan();
            return logicalPlan.getPlanTreeFingerprint();
        } else if (getGroup().getLogicalExpressions().isEmpty()
                && !getGroup().getPhysicalExpressions().isEmpty()
                && getGroup().getPhysicalExpressions().get(0).getPlan() instanceof AbstractPhysicalPlan) {
            AbstractPhysicalPlan physicalPlan = (AbstractPhysicalPlan) getGroup()
                    .getPhysicalExpressions().get(0).getPlan();
            if (!isLocalAggPhysicalNode(physicalPlan)) {
                return physicalPlan.getPlanTreeFingerprint();
            } else {
                return ((AbstractPlan) physicalPlan.child(0)).getPlanTreeFingerprint();
            }
        } else {
            throw new IllegalStateException("illegal group plan type during getFingerprint");
        }
    }

}
