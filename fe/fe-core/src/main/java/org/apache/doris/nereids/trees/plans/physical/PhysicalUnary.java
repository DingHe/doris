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
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.UnaryPlan;
import org.apache.doris.statistics.Statistics;

import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Abstract class for all physical plan that have one child.
 */
// 在 Nereids 优化器中，执行计划树由抽象语法树（AST）逐步转换为逻辑计划（Logical Plan），最终通过 Cost-Based Optimizer (CBO) 变换为物理计划（Physical Plan）。
// 统一单孩子物理算子的基底：它是所有恰好拥有一个子节点（Child Node）的物理计划算子（如 PhysicalFilter、PhysicalProject、PhysicalSort、PhysicalLimit、PhysicalDistribute 等）的通用抽象基类。
// 连接物理计划层次与单孩子接口：它继承自物理计划抽象基类 AbstractPhysicalPlan，同时实现了单孩子计划接口 UnaryPlan<CHILD_TYPE>。这使得所有继承它的物理算子能够自动获得单孩子算子的通用操作能力（例如 child() 方法定位、子树遍历、子节点替换等），无需重复编写这些样板代码。
// 支持范型约束：通过泛型 <CHILD_TYPE Plan extends>，可以在编译期严格限定子节点的类型，保证算子树构造时的类型安全。
public abstract class PhysicalUnary<CHILD_TYPE extends Plan>
        extends AbstractPhysicalPlan
        implements UnaryPlan<CHILD_TYPE> {

    public PhysicalUnary(PlanType type, LogicalProperties logicalProperties, CHILD_TYPE child) {
        super(type, logicalProperties, child);
    }

    public PhysicalUnary(PlanType type, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, CHILD_TYPE child) {
        super(type, groupExpression, logicalProperties, child);
    }

    public PhysicalUnary(PlanType type, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, @Nullable PhysicalProperties physicalProperties,
            Statistics statistics, CHILD_TYPE child) {
        super(type, groupExpression, logicalProperties, physicalProperties, statistics, child);
    }
}
