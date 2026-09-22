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
import org.apache.doris.nereids.properties.DistributionSpec;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalQuickSort;
import org.apache.doris.nereids.util.TreeStringUtils;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Representation for group in cascades optimizer.
 */
// Group 是 Apache Doris 新一代查询优化器 Nereids 中基于 Cascades 优化模型 的核心类之一。
// 在 Cascades 优化框架中，Memo（备忘录） 数据结构用于存储搜索过程中的所有等价查询计划空间。Group（等价组） 是 Memo 中的核心容器：
// 逻辑等价类的集合：Group 表示一组逻辑上等价的表达式（即输出相同数据集合与 Schema 的不同算子树/执行路径）。
// 连接逻辑与物理算子：内部同时管理逻辑组表达式（logicalExpressions）和物理组表达式（physicalExpressions）。
// 物理属性与 Cost 剪枝管理：维护当前 Group 在不同物理属性要求（PhysicalProperties）下的最低代价计划（lowestCostPlans），实现动态规划和代价剪枝。
// 统计信息与属性共享：同一个 Group 内的所有等价表达式共享同一份逻辑属性（LogicalProperties）和基数/基数估计统计信息（Statistics）。
// 等价组合并（Group Merge）：当优化器发现两个原本独立的 Group 在逻辑上等价时，提供将一个 Group 完整合并（mergeTo）到另一个 Group 的能力。
public class Group {
    // Group 的唯一标识符。用于在 Memo 中区分不同的等价组。
    private final GroupId groupId;
    // Save all parent GroupExpression to avoid traversing whole Memo.
    // 父组表达式集合。
    // 记录所有将当前 Group 作为子节点的 GroupExpression。使用 IdentityHashMap（基于引用比较）避免递归解包计算 equals/hashCode，方便在 Group 变更时快速反向通知父节点。
    private final IdentityHashMap<GroupExpression, Void> parentExpressions = new IdentityHashMap<>();
    // 逻辑组表达式列表。
    // 存储当前等价组内包含的所有逻辑算子表达式（如 LogicalJoin、LogicalProject 等）。
    private final List<GroupExpression> logicalExpressions = Lists.newArrayList();
    // 物理组表达式列表。存储由逻辑表达式转换/探索（Exploration/Implementation）得到的物理算子表达式（如 PhysicalHashJoin、PhysicalNestedLoopJoin 等）。
    private final List<GroupExpression> physicalExpressions = Lists.newArrayList();
    // 物理属性强制算子映射表。存储为了满足特定物理属性（如排序 Sort、数据分布 Distribute）而动态插入的 Enforcer 表达式，避免重复添加相同的强制算子。
    private final Map<GroupExpression, GroupExpression> enforcers = Maps.newHashMap();
    // 数据分布强制算子映射表。按分布规格（DistributionSpec）索引 Enforcer 算子（如 PhysicalDistribute），实现更快速的查找与去重。
    private final Map<DistributionSpec, GroupExpression> enforcerSpecs = Maps.newHashMap();
    // 占位物理/逻辑计划算子。
    // 包装当前 Group 的轻量级 Plan 实例，在算子树匹配模式（Pattern Matching）或构建树结构时作为当前 Group 的代理节点。
    private final GroupPlan groupPlan;
    // 统计信息可靠性标识。
    // 默认 true。标识当前 Group 计算出的 Statistics 是否可靠（例如推导过程中缺乏完整直方图或准确基数时可能设为 false）。
    private boolean isStatsReliable = true;
    // 逻辑属性。
    // 存储当前 Group 的输出 Schema、输出列（Output Slots）、空值属性（Nullable）等。组内所有表达式共享此属性。
    private LogicalProperties logicalProperties;

    // Map of cost lower bounds
    // Map required plan props to cost lower bound of corresponding plan
    // 最低代价计划映射表。Cascades 优化器的核心 Cost 表。Key 为要求的物理属性 PhysicalProperties（如特定分布或排序），Value 为满足该属性的最低代价 Cost 以及对应的 GroupExpression。
    private final Map<PhysicalProperties, Pair<Cost, GroupExpression>> lowestCostPlans = Maps.newLinkedHashMap();
    // 探索状态标识。
    // 标记当前 Group 是否已经完成了规则探索（Exploration Phase），防止重复搜索。
    private boolean isExplored = false;
    // 统计信息。存储当前 Group 的基数（RowCount）、列统计（ColumnStat）等数据，用于 Cost 计算。
    private Statistics statistics;
    // 最终选中的物理属性。优化器完成搜索并挑选最佳计划时，记录根节点或当前节点最终采用的物理属性。
    private PhysicalProperties chosenProperties;
    // 最终选中的表达式 ID。在最佳计划提取阶段，记录选中的物理组表达式 ID，默认 -1（未选择）。
    private int chosenGroupExpressionId = -1;
    // 最终选中的 Enforcer 物理属性列表。如果在生成最佳计划时插入了 Enforcer，记录所使用 Enforcer 要求的属性。
    private List<PhysicalProperties> chosenEnforcerPropertiesList = new ArrayList<>();
    // 最终选中的 Enforcer 表达式 ID 列表。记录生成最终最佳计划时选中的 Enforcer 表达式 ID。
    private List<Integer> chosenEnforcerIdList = new ArrayList<>();
    // 结构化信息映射。用于物化视图匹配（Materialized View Rewrite）和下推优化，存储当前 Group 的结构化句法/语义信息。
    private StructInfoMap structInfoMap = new StructInfoMap();

    /**
     * Constructor for Group.
     *
     * @param groupExpression first {@link GroupExpression} in this Group
     */
    // 基于初始 GroupExpression 构造 Group。
    // 初始化 groupId 和 logicalProperties，创建 GroupPlan 代理，并将传入的第一个表达式通过 addGroupExpression 加入组内。
    public Group(GroupId groupId, GroupExpression groupExpression, LogicalProperties logicalProperties) {
        this.groupId = groupId;
        addGroupExpression(groupExpression);
        this.logicalProperties = logicalProperties;
        this.groupPlan = new GroupPlan(this);
    }

    /**
     * Construct a Group without any group expression
     *
     * @param groupId the groupId in memo
     */
    // 构造一个不带初始表达式的空 Group。
    public Group(GroupId groupId, LogicalProperties logicalProperties) {
        this.groupId = groupId;
        this.logicalProperties = logicalProperties;
        this.groupPlan = new GroupPlan(this);
    }

    public GroupId getGroupId() {
        return groupId;
    }
    // 获取当前 Group 中已计算出最低 Cost 计划的所有物理属性集合。
    public List<PhysicalProperties> getAllProperties() {
        return new ArrayList<>(lowestCostPlans.keySet());
    }

    /**
     * Add new {@link GroupExpression} into this group.
     *
     * @param groupExpression {@link GroupExpression} to be added
     * @return added {@link GroupExpression}
     */
    // 自动识别并添加组表达式。
    public GroupExpression addGroupExpression(GroupExpression groupExpression) {
        if (groupExpression.getPlan() instanceof LogicalPlan) {
            logicalExpressions.add(groupExpression);
        } else {
            physicalExpressions.add(groupExpression);
        }
        groupExpression.setOwnerGroup(this);
        return groupExpression;
    }
    // 设置统计信息是否可靠。
    public void setStatsReliable(boolean statsReliable) {
        this.isStatsReliable = statsReliable;
    }

    public boolean isStatsReliable() {
        return isStatsReliable;
    }

    public void addLogicalExpression(GroupExpression groupExpression) {
        groupExpression.setOwnerGroup(this);
        logicalExpressions.add(groupExpression);
    }

    public void addPhysicalExpression(GroupExpression groupExpression) {
        groupExpression.setOwnerGroup(this);
        physicalExpressions.add(groupExpression);
    }

    public List<GroupExpression> getLogicalExpressions() {
        return logicalExpressions;
    }

    public GroupExpression logicalExpressionsAt(int index) {
        return logicalExpressions.get(index);
    }

    /**
     * Get the first logical group expression in this group.
     * If there is no logical group expression or more than one, throw an exception.
     *
     * @return the first logical group expression in this group
     */
    public GroupExpression getLogicalExpression() {
        Preconditions.checkArgument(logicalExpressions.size() == 1,
                "There should be only one Logical Expression in Group");
        return logicalExpressions.get(0);
    }

    public GroupExpression getFirstLogicalExpression() {
        Preconditions.checkArgument(!logicalExpressions.isEmpty(),
                "There should be more than one Logical Expression in Group");
        return logicalExpressions.get(0);
    }

    public List<GroupExpression> getPhysicalExpressions() {
        return physicalExpressions;
    }
    // 获取指向当前 Group 的 GroupPlan 代理算子。
    public GroupPlan getGroupPlan() {
        return groupPlan;
    }

    /**
     * Remove groupExpression from this group.
     *
     * @param groupExpression to be removed
     * @return removed {@link GroupExpression}
     */
    public GroupExpression removeGroupExpression(GroupExpression groupExpression) {
        // use identityRemove to avoid equals() method
        if (groupExpression.getPlan() instanceof LogicalPlan) {
            Utils.identityRemove(logicalExpressions, groupExpression);
        } else {
            Utils.identityRemove(physicalExpressions, groupExpression);
        }
        groupExpression.setOwnerGroup(null);
        return groupExpression;
    }

    public List<GroupExpression> clearLogicalExpressions() {
        List<GroupExpression> move = logicalExpressions.stream()
                .peek(groupExpr -> groupExpr.setOwnerGroup(null))
                .collect(Collectors.toList());
        logicalExpressions.clear();
        return move;
    }

    public List<GroupExpression> clearPhysicalExpressions() {
        List<GroupExpression> move = physicalExpressions.stream()
                .peek(groupExpr -> groupExpr.setOwnerGroup(null))
                .collect(Collectors.toList());
        physicalExpressions.clear();
        return move;
    }

    public void clearLowestCostPlans() {
        lowestCostPlans.clear();
    }

    public double getCostLowerBound() {
        return -1D;
    }

    /**
     * Get the lowest cost {@link org.apache.doris.nereids.trees.plans.physical.PhysicalPlan}
     * which meeting the physical property constraints in this Group.
     *
     * @param physicalProperties the physical property constraints
     * @return {@link Optional} of cost and {@link GroupExpression} of physical plan pair.
     */
    public Optional<Pair<Cost, GroupExpression>> getLowestCostPlan(PhysicalProperties physicalProperties) {
        if (physicalProperties == null || lowestCostPlans.isEmpty()) {
            return Optional.empty();
        }
        Optional<Pair<Cost, GroupExpression>> costAndGroupExpression =
                Optional.ofNullable(lowestCostPlans.get(physicalProperties));
        return costAndGroupExpression;
    }

    public Map<PhysicalProperties, Cost> getLowestCosts() {
        return lowestCostPlans.entrySet()
                .stream()
                .collect(ImmutableMap.toImmutableMap(Entry::getKey, kv -> kv.getValue().first));
    }
    // 寻找物理表达式对应的逻辑计划。
    public GroupExpression getBestPlan(PhysicalProperties properties) {
        if (lowestCostPlans.containsKey(properties)) {
            return lowestCostPlans.get(properties).second;
        }
        return null;
    }

    /**
     * extract the best physical plan's corresponding logical plan
     */
    public Plan getBestLogicalPlan(GroupExpression groupExpression) {
        List<Group> childrenGroups = groupExpression.children();
        for (GroupExpression logicalExpression : logicalExpressions) {
            if (childrenGroups.equals(logicalExpression.children())) {
                return logicalExpression.getPlan();
            }
        }
        if (groupExpression.getPlan() instanceof PhysicalDistribute
                || groupExpression.getPlan() instanceof PhysicalQuickSort || logicalExpressions.isEmpty()) {
            return null;
        } else {
            return getLogicalExpression().getPlan();
        }
    }

    /**
     * add a new enforcer to this group.
     */
    public void addEnforcer(GroupExpression enforcer) {
        enforcer.setOwnerGroup(this);
        if (enforcer.getPlan() instanceof PhysicalDistribute) {
            DistributionSpec distributionSpec = ((PhysicalDistribute) enforcer.getPlan()).getDistributionSpec();
            if (null != enforcerSpecs.put(distributionSpec, enforcer)) {
                return;
            }
        }
        enforcers.put(enforcer, enforcer);
    }

    public Map<GroupExpression, GroupExpression> getEnforcers() {
        return enforcers;
    }

    public Map<DistributionSpec, GroupExpression> getEnforcerSpecs() {
        return enforcerSpecs;
    }

    /**
     * Set or update lowestCostPlans: properties --> Pair.of(cost, expression)
     */
    // 尝试更新指定物理属性下的最佳计划。
    // 若当前属性尚未记录最佳计划，直接存入；若已记录，仅当新计划 Cost 低于旧计划 Cost 时才更新。
    public void setBestPlan(GroupExpression expression, Cost cost, PhysicalProperties properties) {
        if (lowestCostPlans.containsKey(properties)) {
            if (lowestCostPlans.get(properties).first.getValue() > cost.getValue()) {
                lowestCostPlans.put(properties, Pair.of(cost, expression));
            }
        } else {
            lowestCostPlans.put(properties, Pair.of(cost, expression));
        }
    }

    public void putBestPlan(GroupExpression expression, Cost cost, PhysicalProperties properties) {
        setBestPlan(expression, cost, properties);
    }

    /**
     * replace best plan with new properties
     */
    public void replaceBestPlanProperty(PhysicalProperties oldProperty,
            PhysicalProperties newProperty, Cost cost) {
        Pair<Cost, GroupExpression> pair = lowestCostPlans.get(oldProperty);
        GroupExpression lowestGroupExpr = pair.second;
        lowestGroupExpr.updateLowestCostTable(newProperty,
                lowestGroupExpr.getInputPropertiesList(oldProperty), cost);
        lowestCostPlans.remove(oldProperty);
        lowestCostPlans.put(newProperty, pair);
    }

    /**
     * replace oldGroupExpression with newGroupExpression in lowestCostPlans.
     */
    public void replaceBestPlanGroupExpr(GroupExpression oldGroupExpression, GroupExpression newGroupExpression) {
        Map<PhysicalProperties, Pair<Cost, GroupExpression>> needReplaceBestExpressions = Maps.newHashMap();
        for (Iterator<Entry<PhysicalProperties, Pair<Cost, GroupExpression>>> iterator =
                lowestCostPlans.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry<PhysicalProperties, Pair<Cost, GroupExpression>> entry = iterator.next();
            Pair<Cost, GroupExpression> pair = entry.getValue();
            if (pair.second.equals(oldGroupExpression)) {
                needReplaceBestExpressions.put(entry.getKey(), Pair.of(pair.first, newGroupExpression));
                iterator.remove();
            }
        }
        lowestCostPlans.putAll(needReplaceBestExpressions);
    }

    public Statistics getStatistics() {
        return statistics;
    }

    public void setStatistics(Statistics statistics) {
        this.statistics = statistics;
    }

    public LogicalProperties getLogicalProperties() {
        return logicalProperties;
    }

    public void setLogicalProperties(LogicalProperties logicalProperties) {
        this.logicalProperties = logicalProperties;
    }

    public boolean isExplored() {
        return isExplored;
    }

    public void setExplored(boolean explored) {
        isExplored = explored;
    }

    public List<GroupExpression> getParentGroupExpressions() {
        return ImmutableList.copyOf(parentExpressions.keySet());
    }

    public void addParentExpression(GroupExpression parent) {
        parentExpressions.put(parent, null);
    }

    /**
     * remove the reference to parent groupExpression
     *
     * @param parent group expression
     * @return parentExpressions's num
     */
    public int removeParentExpression(GroupExpression parent) {
        parentExpressions.remove(parent);
        return parentExpressions.size();
    }

    public void removeParentPhysicalExpressions() {
        parentExpressions.entrySet().removeIf(entry -> entry.getKey().getPlan() instanceof PhysicalPlan);
    }

    /**
     * move the ownerGroup to target group.
     *
     * @param target the new owner group of expressions
     */
    public void mergeTo(Group target) {
        // move parentExpressions Ownership
        parentExpressions.keySet().forEach(parent -> target.addParentExpression(parent));

        // move enforcers Ownership
        enforcers.forEach((k, v) -> k.children().set(0, target));
        // TODO: dedup?
        enforcers.forEach((k, v) -> target.addEnforcer(k));
        enforcers.clear();
        enforcerSpecs.clear();

        // move LogicalExpression PhysicalExpression Ownership
        Map<GroupExpression, GroupExpression> logicalSet = target.getLogicalExpressions().stream()
                .collect(Collectors.toMap(Function.identity(), Function.identity()));
        for (GroupExpression logicalExpression : logicalExpressions) {
            GroupExpression existGroupExpr = logicalSet.get(logicalExpression);
            if (existGroupExpr != null) {
                Preconditions.checkState(logicalExpression != existGroupExpr, "must not equals");
                // lowCostPlans must be physical GroupExpression, don't need to replaceBestPlanGroupExpr
                logicalExpression.mergeToNotOwnerRemove(existGroupExpr);
            } else {
                target.addLogicalExpression(logicalExpression);
            }
        }
        logicalExpressions.clear();
        // movePhysicalExpressionOwnership
        Map<GroupExpression, GroupExpression> physicalSet = target.getPhysicalExpressions().stream()
                .collect(Collectors.toMap(Function.identity(), Function.identity()));
        for (GroupExpression physicalExpression : physicalExpressions) {
            GroupExpression existGroupExpr = physicalSet.get(physicalExpression);
            if (existGroupExpr != null) {
                Preconditions.checkState(physicalExpression != existGroupExpr, "must not equals");
                physicalExpression.getOwnerGroup().replaceBestPlanGroupExpr(physicalExpression, existGroupExpr);
                physicalExpression.mergeToNotOwnerRemove(existGroupExpr);
            } else {
                target.addPhysicalExpression(physicalExpression);
            }
        }
        physicalExpressions.clear();

        // Above we already replaceBestPlanGroupExpr, but we still need to moveLowestCostPlansOwnership.
        lowestCostPlans.forEach((physicalProperties, costAndGroupExpr) -> {
            // move lowestCostPlans Ownership
            if (!target.lowestCostPlans.containsKey(physicalProperties)) {
                // we must set owner group here, because the instance in logical expression, physical expression
                // and enforcer maybe not same with the instance in the lowestCostPlans map
                costAndGroupExpr.second.setOwnerGroup(target);
                target.lowestCostPlans.put(physicalProperties, costAndGroupExpr);
            } else {
                if (costAndGroupExpr.first.getValue()
                        < target.lowestCostPlans.get(physicalProperties).first.getValue()) {
                    // we must set owner group here, because the instance in logical expression, physical expression
                    // and enforcer maybe not same with the instance in the lowestCostPlans map
                    costAndGroupExpr.second.setOwnerGroup(target);
                    target.lowestCostPlans.put(physicalProperties, costAndGroupExpr);
                }
            }
        });
        lowestCostPlans.clear();

        // If statistics is null, use other statistics
        if (target.statistics == null) {
            target.statistics = this.statistics;
        }
    }

    /**
     * This function used to check whether the group is an end node in DPHyp
     */
    public boolean isValidJoinGroup() {
        Plan plan = getLogicalExpression().getPlan();
        if (plan instanceof LogicalJoin
                && ((LogicalJoin) plan).getJoinType() == JoinType.INNER_JOIN
                && !((LogicalJoin) plan).isMarkJoin()) {
            Preconditions.checkArgument(!((LogicalJoin) plan).getExpressions().isEmpty(),
                    "inner join must have join conjuncts");
            if (((LogicalJoin) plan).getHashJoinConjuncts().isEmpty()
                    && ((LogicalJoin) plan).getOtherJoinConjuncts().get(0) instanceof Literal) {
                return false;
            } else {
                // Right now, we only support inner join with some conjuncts referencing any side of the child's output
                return true;
            }
        }
        return false;
    }

    public StructInfoMap getStructInfoMap() {
        return structInfoMap;
    }

    public boolean isProjectGroup() {
        return getFirstLogicalExpression().getPlan() instanceof LogicalProject;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Group group = (Group) o;
        return groupId.equals(group.groupId);
    }

    @Override
    public int hashCode() {
        return 31 * groupId.asInt();
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder("Group[" + groupId + "]\n");
        // Logical expressions with numbering
        str.append("  Logical Expressions:\n");
        if (logicalExpressions.isEmpty()) {
            str.append("    (none)\n");
        } else {
            int index = 1;
            for (GroupExpression logicalExpression : logicalExpressions) {
                str.append("    [").append(index++).append("] ").append(logicalExpression).append("\n");
            }
        }
        // Physical expressions with numbering
        str.append("  Physical Expressions:\n");
        if (physicalExpressions.isEmpty()) {
            str.append("    (none)\n");
        } else {
            int index = 1;
            for (GroupExpression physicalExpression : physicalExpressions) {
                str.append("    [").append(index++).append("] ").append(physicalExpression).append("\n");
            }
        }
        // Enforcers with numbering
        str.append("  Enforcers:\n");
        List<GroupExpression> enforcerList = enforcers.keySet().stream()
                .sorted(java.util.Comparator.comparing(e1 -> e1.getId().asInt()))
                .collect(Collectors.toList());

        if (enforcerList.isEmpty()) {
            str.append("    (none)\n");
        } else {
            int index = 1;
            for (GroupExpression enforcer : enforcerList) {
                str.append("    [").append(index++).append("] ").append(enforcer).append("\n");
            }
        }
        if (!chosenEnforcerIdList.isEmpty()) {
            str.append("  Chosen Enforcer(ID, RequiredProperties):\n");
            for (int i = 0; i < chosenEnforcerIdList.size(); i++) {
                str.append("      (").append(i).append(")").append(chosenEnforcerIdList.get(i)).append(",  ")
                        .append(chosenEnforcerPropertiesList.get(i)).append("\n");
            }
        }
        if (chosenGroupExpressionId != -1) {
            str.append("  Chosen Expression ID: ").append(chosenGroupExpressionId).append("\n");
            str.append("  Chosen Properties: ").append(chosenProperties).append("\n");
        }
        str.append("  Statistics").append("\n");
        str.append(getStatistics() == null ? "" : getStatistics().detail("    "));

        str.append("  Lowest Plan");
        DecimalFormat format = new DecimalFormat("#,###.##");
        // Sort by cost for better readability
        List<Map.Entry<PhysicalProperties, Pair<Cost, GroupExpression>>> sortedEntries =
                lowestCostPlans.entrySet().stream()
                        .sorted(Map.Entry.comparingByValue((a, b) ->
                                Double.compare(a.first.getValue(), b.first.getValue())))
                        .collect(Collectors.toList());
        int planIndex = 0;
        for (Map.Entry<PhysicalProperties, Pair<Cost, GroupExpression>> entry : sortedEntries) {
            PhysicalProperties prop = entry.getKey();
            Pair<Cost, GroupExpression> costGroupExpressionPair = entry.getValue();
            Cost cost = costGroupExpressionPair.first;
            GroupExpression child = costGroupExpressionPair.second;
            List<PhysicalProperties> inputProps = child.getInputPropertiesListOrEmpty(prop);
            boolean isChosen = false;
            // Check if it's a chosen physical expression
            if (chosenGroupExpressionId != -1
                    && child.getId().asInt() == chosenGroupExpressionId
                    && prop.equals(chosenProperties)) {
                isChosen = true;
            }
            // Check if it's a chosen enforcer
            if (!isChosen && !chosenEnforcerIdList.isEmpty()) {
                for (int i = 0; i < chosenEnforcerIdList.size(); i++) {
                    if (child.getId().asInt() == chosenEnforcerIdList.get(i)
                            && prop.equals(chosenEnforcerPropertiesList.get(i))) {
                        isChosen = true;
                        break;
                    }
                }
            }
            String marker = isChosen ? " BEST" : "";
            str.append("\n    ── Entry #").append(++planIndex)
                    .append(" ──────────────────────────────").append(marker);
            str.append("\n    Cost: ").append(format.format(cost.getValue()));
            str.append("\n    Properties: ").append(prop);
            str.append("\n    Expression ID: ").append(child.getId().asInt()).append("#").append(groupId.asInt());
            if (!inputProps.isEmpty()) {
                str.append("\n    ChildrenRequires:");
                for (int i = 0; i < inputProps.size(); i++) {
                    str.append("\n      [").append(i).append("] ").append(inputProps.get(i));
                }
            }
        }
        str.append("\n").append("  struct info map").append("\n");
        str.append(structInfoMap);

        return str.toString();
    }

    /**
     * Simplify plan string by removing redundant information.
     */
    private String simplifyPlanString(String planStr) {
        // Remove redundant information that doesn't add value
        String simplified = planStr;
        // Remove stats=null (common and not informative)
        simplified = simplified.replaceAll("\\s*stats=null,?", "");
        // Remove markJoinSlotReference=Optional.empty (only show if present)
        simplified = simplified.replaceAll("\\s*markJoinSlotReference=Optional\\.empty,?", "");
        // Remove empty otherCondition=[]
        simplified = simplified.replaceAll("\\s*otherCondition=\\[\\],?", "");
        // Remove empty markCondition=[]
        simplified = simplified.replaceAll("\\s*markCondition=\\[\\],?", "");
        // Clean up multiple spaces and commas
        simplified = simplified.replaceAll(",\\s*,+", ","); // Remove multiple commas
        simplified = simplified.replaceAll("\\s+", " "); // Normalize spaces
        simplified = simplified.replaceAll("\\(\\s*,", "("); // Remove leading comma after (
        simplified = simplified.replaceAll(",\\s*\\)", ")"); // Remove trailing comma before )
        return simplified.trim();
    }

    /**
     * Get tree like string describing group.
     *
     * @return tree like string describing group
     */
    public String treeString() {
        Function<Object, String> toString = obj -> {
            if (obj instanceof Group) {
                Group group = (Group) obj;
                Map<PhysicalProperties, Cost> lowestCosts = group.getLowestCosts();
                return "Group[" + group.groupId + ", lowestCosts: " + lowestCosts + "]";
            } else if (obj instanceof GroupExpression) {
                GroupExpression groupExpression = (GroupExpression) obj;
                Map<PhysicalProperties, Pair<Cost, List<PhysicalProperties>>> lowestCostTable
                        = groupExpression.getLowestCostTable();
                Map<PhysicalProperties, PhysicalProperties> requestPropertiesMap
                        = groupExpression.getRequestPropertiesMap();
                Cost cost = groupExpression.getCost();
                return groupExpression.getPlan().toString() + " [cost: " + cost + ", lowestCostTable: "
                        + lowestCostTable + ", requestPropertiesMap: " + requestPropertiesMap + "]";
            } else if (obj instanceof Pair) {
                // print logicalExpressions or physicalExpressions
                // first is name, second is group expressions
                return ((Pair<?, ?>) obj).first.toString();
            } else {
                return obj.toString();
            }
        };

        Function<Object, List<Object>> getChildren = obj -> {
            if (obj instanceof Group) {
                Group group = (Group) obj;
                List children = new ArrayList<>();

                // to <name, children> pair
                if (!group.getLogicalExpressions().isEmpty()) {
                    children.add(Pair.of("logicalExpressions", group.getLogicalExpressions()));
                }
                if (!group.getPhysicalExpressions().isEmpty()) {
                    children.add(Pair.of("physicalExpressions", group.getPhysicalExpressions()));
                }
                return children;
            } else if (obj instanceof GroupExpression) {
                return (List) ((GroupExpression) obj).children();
            } else if (obj instanceof Pair) {
                return (List) ((Pair<String, List<GroupExpression>>) obj).second;
            } else {
                return ImmutableList.of();
            }
        };

        Function<Object, List<Object>> getExtraPlans = obj -> {
            if (obj instanceof Plan) {
                return (List) ((Plan) obj).extraPlans();
            } else {
                return ImmutableList.of();
            }
        };

        Function<Object, Boolean> displayExtraPlan = obj -> {
            if (obj instanceof Plan) {
                return ((Plan) obj).displayExtraPlanFirst();
            } else {
                return false;
            }
        };

        return TreeStringUtils.treeString(this, toString, getChildren, getExtraPlans, displayExtraPlan);
    }

    public PhysicalProperties getChosenProperties() {
        return chosenProperties;
    }

    public void setChosenProperties(PhysicalProperties chosenProperties) {
        this.chosenProperties = chosenProperties;
    }

    public void setChosenGroupExpressionId(int chosenGroupExpressionId) {
        Preconditions.checkArgument(this.chosenGroupExpressionId == -1,
                "chosenGroupExpressionId is already set");
        this.chosenGroupExpressionId = chosenGroupExpressionId;
    }

    public void addChosenEnforcerProperties(PhysicalProperties chosenEnforcerProperties) {
        this.chosenEnforcerPropertiesList.add(chosenEnforcerProperties);
    }

    public void addChosenEnforcerId(int chosenEnforcerId) {
        this.chosenEnforcerIdList.add(chosenEnforcerId);
    }
}
