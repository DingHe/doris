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

package org.apache.doris.nereids.memo;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.cost.Cost;
import org.apache.doris.nereids.metrics.EventChannel;
import org.apache.doris.nereids.metrics.EventProducer;
import org.apache.doris.nereids.metrics.consumer.LogConsumer;
import org.apache.doris.nereids.metrics.event.CostStateUpdateEvent;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.ObjectId;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.text.DecimalFormat;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Representation for group expression in cascades optimizer.
 */
// org.apache.doris.nereids.memo.GroupExpression 是 Apache Doris 新一代现代优化器 Nereids 中基于 Cascades 优化模型的核心类之一。
// 在 Cascades 优化模型中，搜索空间被组织为一个名为 Memo（备忘录） 的图结构。GroupExpression（组表达式）是 Memo 中的基本节点/表达式单元，其核心作用可以概括为以下几点：
// 抽象关系算子与子组连接：GroupExpression 封装了一个具体的算子节点（Plan，如 LogicalJoin 或 PhysicalHashJoin），但它的子节点不再直接引用具体的算子，而是指向子等价组 Group（即 List<Group> children）。这种“算子 + 子组”的结构实现了查询计划树的表达式解耦和共享。
// 规则匹配与优化状态跟踪：维护 ruleMasks（位图记录），精准跟踪哪些优化规则（Rule）已经在该组表达式上应用过，防止优化规则死循环或重复匹配。
// 代价与物理属性管理（Cost & Physical Properties Table）：在搜索和 Implementation（物理实现）阶段，维护当前表达式在不同上层要求的物理属性（如分布方式 Distribution、排序属性 Order Spec）下的最小代价 Cost，以及为满足该属性需要向下层子 Group 传递的物理属性要求。
// 统计信息推导标识：记录基数估计（Card/Row Count）和代价计算的中间状态，支撑动态规划及代价剪枝。
// 等价类合并（Group Expression Merge）：当优化器推导发现不同的表达式逻辑等价时，提供将当前表达式合并（mergeTo）到另一个表达式的技术支持，同步更新 Cost Table 与规则位图。
public class GroupExpression {
    // 静态事件追踪器。
    // 用于订阅和记录代价状态更新事件（CostStateUpdateEvent），方便在调试或日志中追踪代价变化过程。
    private static final EventProducer COST_STATE_TRACER = new EventProducer(CostStateUpdateEvent.class,
            EventChannel.getDefaultChannel().addConsumers(new LogConsumer(CostStateUpdateEvent.class,
                    EventChannel.LOG)));
    // 当前表达式的代价对象。包含 CPU 代价、内存代价（Memory）、网络代价（Network）等，表示当前表达式自身的计算/传输消耗。
    private Cost cost = null;
    // 所属的 Group。标识当前 GroupExpression 归属于 Memo 中的哪一个等价组（Group）。
    private Group ownerGroup;
    // 子组列表。
    // 当前表达式的下层子节点，指向子 Group 集合（而非具体的算子）。
    private final List<Group> children;
    // 引用的算子计划节点。
    // 当前表达式代表的单层关系算子（如 LogicalProject、PhysicalHashJoin 等），其内部会绑定指向当前的 GroupExpression。
    private final Plan plan;
    // 规则遮罩/应用记录位图。使用 BitSet 记录每一个 RuleType 是否已在该表达式上执行过，防止规则重复应用。
    private final BitSet ruleMasks;
    // 统计信息推导状态。标记当前表达式是否已经推导过统计信息（Statistics），避免重复估算。
    private boolean statDerived;
    // 估计输出行数。存储统计信息推导出的基数估算值（RowCount），默认初始值为 -1。
    private double estOutputRowCount = -1;

    // Record the rule that generate this plan. It's used for debugging
    // 生成来源规则。记录是通过哪一条 Rule（转换或探索规则）生成了当前的 GroupExpression，主要用于 Debug 和执行计划追踪。
    private Rule fromRule;

    // Mapping from output properties to the corresponding best cost, statistics, and child properties.
    // key is the physical properties the group expression support for its parent
    // and value is cost and request physical properties to its children.
    // 最低代价映射表。Key 为当前表达式向父节点输出/满足的物理属性；Value 包含了在满足该属性下的最低总 Cost，以及传递给各个子组（children）的物理属性要求列表。
    private final Map<PhysicalProperties, Pair<Cost, List<PhysicalProperties>>> lowestCostTable;
    // Each physical group expression maintains mapping incoming requests to the corresponding child requests.
    // key is the output physical properties satisfying the incoming request properties
    // value is the request physical properties
    // 请求属性到输出属性的映射表。Key 为父节点对当前节点要求的物理属性；Value 为当前表达式实际提供的输出物理属性（必须满足/匹配要求的属性）。
    private final Map<PhysicalProperties, PhysicalProperties> requestPropertiesMap;

    // After mergeGroup(), source Group was cleaned up, but it may be in the Job Stack. So use this to mark and skip it.
    // 废弃/无用状态标识。
    // 在 Group 合并（Group Merge）过程中，源 GroupExpression 被清理后，若其指针仍残留在并发或 Job 堆栈中，通过该标记将其跳过，防止无效操作。
    private boolean isUnused = false;
    // 语句作用域内唯一的 ObjectId。在单个查询编译生命周期内为当前 GroupExpression 生成唯一 ID，便于日志打印和标识识别。
    private final ObjectId id = StatementScopeIdGenerator.newObjectId();

    /**
     * Just for UT.
     */
    public GroupExpression(Plan plan) {
        this(plan, Lists.newArrayList());
    }

    /**
     * Notice!!!: children will use param `children` directly, So don't modify it after this constructor outside.
     * Constructor for GroupExpression.
     *
     * @param plan {@link Plan} to reference
     * @param children children groups in memo
     */
    public GroupExpression(Plan plan, List<Group> children) {
        this.plan = Objects.requireNonNull(plan, "plan can not be null")
                .withGroupExpression(Optional.of(this));
        this.children = Objects.requireNonNull(children, "children can not be null");
        this.ruleMasks = new BitSet(RuleType.SENTINEL.ordinal());
        this.statDerived = false;
        this.lowestCostTable = Maps.newHashMap();
        this.requestPropertiesMap = Maps.newHashMap();
        for (Group child : children) {
            child.addParentExpression(this);
        }
    }

    public PhysicalProperties getOutputProperties(PhysicalProperties requestProperties) {
        PhysicalProperties outputProperties = requestPropertiesMap.get(requestProperties);
        Preconditions.checkNotNull(outputProperties);
        return outputProperties;
    }

    public int arity() {
        return children.size();
    }

    public void setFromRule(Rule rule) {
        this.fromRule = rule;
    }

    public Group getOwnerGroup() {
        return ownerGroup;
    }

    public void setOwnerGroup(Group ownerGroup) {
        this.ownerGroup = ownerGroup;
    }

    public Plan getPlan() {
        return plan;
    }

    public Group child(int i) {
        return children.get(i);
    }

    public void setChild(int i, Group group) {
        child(i).removeParentExpression(this);
        children.set(i, group);
        group.addParentExpression(this);
    }

    public List<Group> children() {
        return children;
    }

    /**
     * replaceChild.
     *
     * @param oldChild origin child group
     * @param newChild new child group
     */
    public void replaceChild(Group oldChild, Group newChild) {
        oldChild.removeParentExpression(this);
        newChild.addParentExpression(this);
        Utils.replaceList(children, oldChild, newChild);
    }

    public boolean hasApplied(Rule rule) {
        return ruleMasks.get(rule.getRuleType().ordinal());
    }

    public boolean notApplied(Rule rule) {
        return !hasApplied(rule);
    }

    public void setApplied(Rule rule) {
        ruleMasks.set(rule.getRuleType().ordinal());
    }

    public void propagateApplied(GroupExpression toGroupExpression) {
        toGroupExpression.ruleMasks.or(ruleMasks);
    }

    public void clearApplied() {
        ruleMasks.clear();
    }

    public boolean isStatDerived() {
        return statDerived;
    }

    public void setStatDerived(boolean statDerived) {
        this.statDerived = statDerived;
    }

    /**
     * Check this GroupExpression isUnused. See detail of `isUnused` in its comment.
     */
    public boolean isUnused() {
        if (isUnused) {
            Preconditions.checkState(children.isEmpty() && ownerGroup == null);
            return true;
        }
        Preconditions.checkState(ownerGroup != null);
        return false;
    }

    public void setUnused(boolean isUnused) {
        this.isUnused = isUnused;
    }

    public Map<PhysicalProperties, Pair<Cost, List<PhysicalProperties>>> getLowestCostTable() {
        return lowestCostTable;
    }

    public List<PhysicalProperties> getInputPropertiesList(PhysicalProperties require) {
        Preconditions.checkState(lowestCostTable.containsKey(require));
        return lowestCostTable.get(require).second;
    }

    public List<PhysicalProperties> getInputPropertiesListOrEmpty(PhysicalProperties require) {
        Pair<Cost, List<PhysicalProperties>> costAndChildRequire = lowestCostTable.get(require);
        return costAndChildRequire == null ? ImmutableList.of() : costAndChildRequire.second;
    }

    /**
     * Add a (outputProperties) -> (cost, childrenInputProperties) in lowestCostTable.
     * if the outputProperties exists, will be covered.
     *
     * @return true if lowest cost table change.
     */
    public boolean updateLowestCostTable(PhysicalProperties outputProperties,
            List<PhysicalProperties> childrenInputProperties, Cost cost) {
        COST_STATE_TRACER.log(CostStateUpdateEvent.of(this, cost.getValue(), outputProperties));
        if (lowestCostTable.containsKey(outputProperties)) {
            if (lowestCostTable.get(outputProperties).first.getValue() > cost.getValue()) {
                lowestCostTable.put(outputProperties, Pair.of(cost, childrenInputProperties));
                return true;
            } else {
                return false;
            }
        } else {
            lowestCostTable.put(outputProperties, Pair.of(cost, childrenInputProperties));
            return true;
        }
    }

    /**
     * get the lowest cost when satisfy property
     *
     * @param property property that needs to be satisfied
     * @return Lowest cost to satisfy that property
     */
    public double getCostByProperties(PhysicalProperties property) {
        Preconditions.checkState(lowestCostTable.containsKey(property));
        return lowestCostTable.get(property).first.getValue();
    }

    public Cost getCostValueByProperties(PhysicalProperties property) {
        Preconditions.checkState(lowestCostTable.containsKey(property));
        return lowestCostTable.get(property).first;
    }

    public void putOutputPropertiesMap(PhysicalProperties outputProperties,
            PhysicalProperties requiredProperties) {
        this.requestPropertiesMap.put(requiredProperties, outputProperties);
    }

    /**
     * Merge GroupExpression.
     */
    public void mergeTo(GroupExpression target) {
        this.ownerGroup.removeGroupExpression(this);
        this.mergeToNotOwnerRemove(target);
    }

    /**
     * Merge GroupExpression, but owner don't remove this GroupExpression.
     */
    public void mergeToNotOwnerRemove(GroupExpression target) {
        // LowestCostTable
        this.getLowestCostTable()
                .forEach((properties, pair) -> target.updateLowestCostTable(properties, pair.second, pair.first));
        // requestPropertiesMap
        // ATTN: when do merge, we should update target requestPropertiesMap
        //   ONLY IF the cost of source's request property lower than target one.
        //   Otherwise, the requestPropertiesMap will not sync with lowestCostTable.
        //   Then, we will get wrong output property when get the final plan.
        for (Map.Entry<PhysicalProperties, PhysicalProperties> entry : requestPropertiesMap.entrySet()) {
            PhysicalProperties request = entry.getKey();
            if (!target.requestPropertiesMap.containsKey(request)) {
                target.requestPropertiesMap.put(entry.getKey(), entry.getValue());
            } else {
                PhysicalProperties sourceOutput = entry.getValue();
                PhysicalProperties targetOutput = target.getRequestPropertiesMap().get(request);
                if (this.getLowestCostTable().containsKey(sourceOutput)
                        && target.getLowestCostTable().containsKey(targetOutput)) {
                    Cost sourceCost = this.getLowestCostTable().get(sourceOutput).first;
                    Cost targetCost = target.getLowestCostTable().get(targetOutput).first;
                    if (sourceCost.getValue() < targetCost.getValue()) {
                        target.requestPropertiesMap.put(entry.getKey(), entry.getValue());
                    }
                }
            }
        }
        // ruleMasks
        target.ruleMasks.or(this.ruleMasks);

        // clear
        this.children.forEach(child -> child.removeParentExpression(this));
        this.children.clear();
        this.ownerGroup = null;
    }

    public Cost getCost() {
        return cost;
    }

    public void setCost(Cost cost) {
        this.cost = cost;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GroupExpression that = (GroupExpression) o;
        return children.equals(that.children) && plan.equals(that.plan);
    }

    @Override
    public int hashCode() {
        long hashCode = 1;
        for (int i = 0; i < children.size(); i++) {
            hashCode = 31 * hashCode + children.get(i).hashCode();
        }
        hashCode = hashCode * 31 + plan.hashCode();
        return (int) hashCode;
    }

    public Statistics childStatistics(int idx) {
        return child(idx).getStatistics();
    }

    public void setEstOutputRowCount(double estOutputRowCount) {
        this.estOutputRowCount = estOutputRowCount;
    }

    public double getEstOutputRowCount() {
        return estOutputRowCount;
    }

    /**
     * Clear cost-related state (cost, lowestCostTable, requestPropertiesMap).
     * Does NOT reset estOutputRowCount — that is stats state, set during the
     * stats computation phase which runs before cost recomputation.
     */
    public void clearCostState() {
        cost = null;
        lowestCostTable.clear();
        requestPropertiesMap.clear();
    }

    public Map<PhysicalProperties, PhysicalProperties> getRequestPropertiesMap() {
        return ImmutableMap.copyOf(requestPropertiesMap);
    }

    @Override
    public String toString() {
        DecimalFormat format = new DecimalFormat("#,###.##");
        StringBuilder builder = new StringBuilder();
        String separator = " | ";
        // Format ID
        builder.append("id:").append(id.asInt());
        if (ownerGroup == null) {
            builder.append("OWNER GROUP IS NULL[]");
        } else {
            builder.append("#").append(ownerGroup.getGroupId().asInt());
        }
        // Format cost information
        builder.append(separator);
        if (cost != null) {
            builder.append("cost=").append(format.format(cost.getValue()));
            builder.append(" [cpu=").append(format.format(cost.getCpuCost()))
                    .append(", mem=").append(format.format(cost.getMemoryCost()))
                    .append(", net=").append(format.format(cost.getNetworkCost())).append("]");
        } else {
            builder.append("cost=null");
        }
        // Format estimated rows
        builder.append(separator);
        builder.append("estRows=").append(format.format(estOutputRowCount));

        // Format children
        builder.append(separator);
        if (!children.isEmpty()) {
            builder.append("children=[").append(Joiner.on(", ").join(
                    children.stream().map(Group::getGroupId).collect(Collectors.toList())))
                    .append("]");
        } else {
            builder.append("children=[]");
        }
        // Format plan (simplified)
        builder.append(separator).append(plan.toString());
        return builder.toString();
    }

    public ObjectId getId() {
        return id;
    }

    /**
     * the first child plan of clazz
     * @param clazz the operator type, like join/aggregate
     * @return child operator of type clazz, if not found, return null
     */
    public Plan getFirstChildPlan(Class clazz) {
        for (Group childGroup : children) {
            for (GroupExpression logical : childGroup.getLogicalExpressions()) {
                if (clazz.isInstance(logical.getPlan())) {
                    return logical.getPlan();
                }
            }
        }
        // for dphyp
        for (Group childGroup : children) {
            for (GroupExpression physical : childGroup.getPhysicalExpressions()) {
                if (clazz.isInstance(physical.getPlan())) {
                    return physical.getPlan();
                }
            }
        }
        return null;
    }
}
