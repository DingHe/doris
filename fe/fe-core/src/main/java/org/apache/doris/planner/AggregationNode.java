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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/AggregationNode.java
// and modified by Doris

package org.apache.doris.planner;

import org.apache.doris.analysis.AggregateInfo;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprToThriftVisitor;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.SortInfo;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeType;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeTypeRequire;
import org.apache.doris.planner.normalize.Normalizer;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TAggregationNode;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TNormalizedAggregateNode;
import org.apache.doris.thrift.TNormalizedPlanNode;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TSortInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Aggregation computation.
 */
// AggregationNode 代表分布式执行计划中的聚合计算节点（在向量化引擎中对应 BE 端的 VAGGREGATE 算子）。
// 聚合算子建模：基于优化器传入的 AggregateInfo，定义 GROUP BY 列、聚合函数（如 SUM, COUNT, AVG）、HAVING 过滤条件以及排序规则（SortInfo）。
// 多阶段分布式聚合调度：支持两阶段/多阶段分布式聚合策略。通过控制 needsFinalize（是否是最终输出阶段）和 aggInfo.isMerge()（是否处理中间聚合状态/序列化数据），
//  配合预聚合（Pre-aggregation）和流式聚合（Streaming Aggregation）来优化大数据量 Shuffle 性能。
// 序列化与 Thrift 转换：将 FE 生成的物理聚合计划翻译转换为 Thrift 结构体（TAggregationNode），下发给后端 BE 节点构建具体的 C++ 算子（如 AggSinkOperatorX / StreamingAggOperatorX / DistinctStreamingAggOperatorX）。
// 并发与本地数据分布推导（Local Exchange）：针对 Pipeline 执行引擎，实现 enforceAndDeriveLocalExchange，推导节点间是否需要插入 Hash 或 Passthrough 类型的 Local Exchange（并行度交换/本地 Shuffle）以保障正确性与发挥最大并行能
public class AggregationNode extends PlanNode {
    // 聚合元数据对象。
    // 包含当前聚合节点所需的所有表达式，包括分组表达式列表（groupingExprs）、聚合函数表达式列表（aggregateExprs）以及输出 Tuple 描述符等。
    private final AggregateInfo aggInfo;

    // Set to true if this aggregation node needs to run the Finalize step. This
    // node is the root node of a distributed aggregation.
    // 是否为 Finalize 阶段（最终计算阶段）。
    // • true：表示该节点是分布式聚合的根节点，需要执行 finalize() 操作（将中间状态转化为最终的聚合列值并输出）。
    // • false：表示该节点只进行局部预聚合或中间 Merge，输出的是序列化后的中间聚合状态（Serialize）。
    private boolean needsFinalize;
    // 是否为 Colocate 聚合。
    // 标识该聚合是否属于 Colocate Join / Colocate Agg 场景（即数据在 Backend 上已经按照 Group Key 完成了物理分布，无需重新 Shuffle）。
    private boolean isColocate = false;

    // If true, use streaming preaggregation algorithm. Not valid if this is a merge agg.
    // 是否使用流式预聚合算法（Streaming Pre-aggregation）。
    // 用于一阶段局部预聚合。开启后，数据不会构建大的 Hash 表，而是以流水线（Streaming）方式快速规约，如果 Hash 冲突率高则直接 Passthrough 下发，能极大节省内存和降低延迟。
    private boolean useStreamingPreagg;
    // 按 Group Key 排序的信息。
    // 当聚合操作后续需要按照 Group By 的列进行排序输出，或者使用了某些支持流式/排序输入的聚合优化时，存放相关的排序元素（Order Elements）。
    private SortInfo sortByGroupKey;
    // 是否为 Query Cache（查询缓存）候选节点。
    // 标识该聚合节点及其生成的中间/最终结果是否符合放入 Doris 查询缓存的条件。
    private boolean queryCacheCandidate;

    /**
     * Create an agg node that is not an intermediate node.
     * isIntermediate is true if it is a slave node in a 2-part agg plan.
     */
    public AggregationNode(PlanNodeId id, PlanNode input, AggregateInfo aggInfo) {
        super(id, aggInfo.getOutputTupleId().asList(), "AGGREGATE");
        this.aggInfo = aggInfo;
        this.children.add(input);
        this.needsFinalize = true;
        updateplanNodeName();
    }

    // Unsets this node as requiring finalize. Only valid to call this if it is
    // currently marked as needing finalize.
    public void unsetNeedsFinalize() {
        Preconditions.checkState(needsFinalize);
        needsFinalize = false;
        updateplanNodeName();
    }

    public boolean isNeedsFinalize() {
        return needsFinalize;
    }

    // Used by new optimizer
    // 设置是否启用流式预聚合（useStreamingPreagg）。通常由优化器（如 Nereids）在推导物理计划时调用。
    public void setUseStreamingPreagg(boolean useStreamingPreagg) {
        this.useStreamingPreagg = useStreamingPreagg;
    }
    // 依据当前节点的聚合属性（aggInfo.isMerge() 与 needsFinalize）动态更新节点在 Explain 和日志中显示的名称。
    // 固定前缀：VAGGREGATE (（V 代表 Vectorized 向量化）。
    private void updateplanNodeName() {
        StringBuilder sb = new StringBuilder();
        sb.append("VAGGREGATE");
        sb.append(" (");
        if (aggInfo.isMerge()) {
            sb.append("merge");
        } else {
            sb.append("update");
        }
        if (needsFinalize) {
            sb.append(" finalize");
        } else {
            sb.append(" serialize");
        }
        sb.append(")");
        setPlanNodeName(sb.toString());
    }
    // 将当前 Java 物理计划节点转换为 Thrift 结构体 TPlanNode，这是下发给 BE Backend 节点的通信对象。
    @Override
    protected void toThrift(TPlanNode msg) {
        aggInfo.updateMaterializedSlots();
        msg.node_type = TPlanNodeType.AGGREGATION_NODE;
        List<TExpr> aggregateFunctions = Lists.newArrayList();
        List<TSortInfo> aggSortInfos = Lists.newArrayList();
        // only serialize agg exprs that are being materialized
        for (FunctionCallExpr e : aggInfo.getMaterializedAggregateExprs()) {
            aggregateFunctions.add(ExprToThriftVisitor.treeToThrift(e));
            List<TExpr> orderingExpr = Lists.newArrayList();
            List<Boolean> isAscs = Lists.newArrayList();
            List<Boolean> nullFirsts = Lists.newArrayList();

            e.getOrderByElements().forEach(o -> {
                orderingExpr.add(ExprToThriftVisitor.treeToThrift(o.getExpr()));
                isAscs.add(o.getIsAsc());
                nullFirsts.add(o.getNullsFirstParam());
            });
            aggSortInfos.add(new TSortInfo(orderingExpr, isAscs, nullFirsts));
        }

        msg.agg_node = new TAggregationNode(
                aggregateFunctions,
                aggInfo.getOutputTupleId().asInt(),
                aggInfo.getOutputTupleId().asInt(), needsFinalize);
        msg.agg_node.setAggSortInfos(aggSortInfos);
        msg.agg_node.setUseStreamingPreaggregation(useStreamingPreagg);
        msg.agg_node.setIsFirstPhase(aggInfo.isFirstPhase());
        msg.agg_node.setIsColocate(isColocate);
        if (sortByGroupKey != null) {
            msg.agg_node.setAggSortInfoByGroupKey(sortByGroupKey.toThrift());
        }
        List<Expr> groupingExprs = aggInfo.getGroupingExprs();
        if (groupingExprs != null) {
            msg.agg_node.setGroupingExprs(ExprToThriftVisitor.treesToThrift(groupingExprs));
        }
    }
    // 规整化（Normalize）聚合节点，主要用于复用查询缓存（Query Cache）的抽象计划匹配。
    @Override
    public void normalize(TNormalizedPlanNode normalizedPlan, Normalizer normalizer) {
        TNormalizedAggregateNode normalizedAggregateNode = new TNormalizedAggregateNode();

        // if (aggInfo.getGroupingExprs().size() > 3) {
        //     throw new IllegalStateException("Too many grouping expressions, not use query cache");
        // }

        normalizedAggregateNode.setOutputTupleId(
                normalizer.normalizeTupleId(aggInfo.getOutputTupleId().asInt()));
        normalizedAggregateNode.setGroupingExprs(normalizeExprs(aggInfo.getGroupingExprs(), normalizer));
        normalizedAggregateNode.setAggregateFunctions(normalizeExprs(aggInfo.getAggregateExprs(), normalizer));
        normalizedAggregateNode.setIsFinalize(needsFinalize);
        normalizedAggregateNode.setUseStreamingPreaggregation(useStreamingPreagg);

        normalizeAggOutputProjects(normalizedAggregateNode, normalizer);

        normalizedPlan.setNodeType(TPlanNodeType.AGGREGATION_NODE);
        normalizedPlan.setAggregationNode(normalizedAggregateNode);
        if (sortByGroupKey != null) {
            normalizedAggregateNode.setSortInfo(sortByGroupKey.toThrift());
        }
    }
    // 规范化当前聚合节点的 Output Project（投影列）。
    @Override
    protected void normalizeProjects(TNormalizedPlanNode normalizedPlanNode, Normalizer normalizer) {
        List<SlotDescriptor> outputSlots =
                getOutputTupleIds()
                        .stream()
                        .flatMap(tupleId -> normalizer.getDescriptorTable().getTupleDesc(tupleId).getSlots().stream())
                        .collect(Collectors.toList());

        List<Expr> projectList = this.projectList;
        if (projectList == null) {
            projectList = this.aggInfo.getOutputTupleDesc()
                    .getSlots()
                    .stream()
                    .map(SlotRef::new)
                    .collect(Collectors.toList());
        }

        List<TExpr> projectThrift = normalizeProjects(outputSlots, projectList, normalizer);
        normalizedPlanNode.setProjects(projectThrift);
    }

    private void normalizeAggOutputProjects(TNormalizedAggregateNode aggregateNode, Normalizer normalizer) {
        List<Expr> projectToIntermediateTuple = ImmutableList.<Expr>builder()
                .addAll(aggInfo.getGroupingExprs())
                .addAll(aggInfo.getAggregateExprs())
                .build();

        List<SlotDescriptor> intermediateSlots = aggInfo.getOutputTupleDesc().getSlots();
        List<TExpr> projects = normalizeProjects(intermediateSlots, projectToIntermediateTuple, normalizer);
        aggregateNode.setProjectToAggOutputTuple(projects);
    }

    @Override
    public String getNodeExplainString(String detailPrefix, TExplainLevel detailLevel) {
        aggInfo.updateMaterializedSlots();
        StringBuilder output = new StringBuilder();
        if (useStreamingPreagg) {
            output.append(detailPrefix).append("STREAMING").append("\n");
        }

        if (detailLevel == TExplainLevel.BRIEF) {
            output.append(detailPrefix).append(String.format(
                    "cardinality=%,d",  cardinality)).append("\n");
            return output.toString();
        }

        if (aggInfo.getAggregateExprs() != null && aggInfo.getMaterializedAggregateExprs().size() > 0) {
            List<String> labels = aggInfo.getMaterializedAggregateExprLabels();
            if (labels.isEmpty()) {
                output.append(detailPrefix).append("output: ")
                        .append(getExplainString(aggInfo.getMaterializedAggregateExprs())).append("\n");
            } else {
                output.append(detailPrefix).append("output: ")
                        .append(StringUtils.join(labels, ", ")).append("\n");
            }
        }
        // TODO: group by can be very long. Break it into multiple lines
        output.append(detailPrefix).append("group by: ")
                .append(getExplainString(aggInfo.getGroupingExprs()))
                .append("\n");
        if (!conjuncts.isEmpty()) {
            output.append(detailPrefix).append("having: ").append(getExplainString(conjuncts)).append("\n");
        }
        output.append(detailPrefix).append("sortByGroupKey:").append(sortByGroupKey != null).append("\n");
        output.append(detailPrefix).append(String.format(
                "cardinality=%,d", cardinality)).append("\n");
        return output.toString();
    }

    // If `GroupingExprs` is empty and agg need to finalize, the result must be output by single instance
    // 判断当前节点是否是一个必须单线程/单实例（Serial）运行的节点。
    // 如果 groupingExprs 为空（即标量聚合 SELECT SUM(a) FROM t，没有 Group By）且 needsFinalize 为 true，则返回 true。因为全局无 Group By 的最终聚合结果必须汇总到一个节点上输出
    @Override
    public boolean isSerialNode() {
        return aggInfo.getGroupingExprs().isEmpty() && needsFinalize;
    }

    public void setColocate(boolean colocate) {
        isColocate = colocate;
    }

    public boolean isColocate() {
        return isColocate;
    }

    public void setSortByGroupKey(SortInfo sortByGroupKey) {
        this.sortByGroupKey = sortByGroupKey;
    }

    public boolean isQueryCacheCandidate() {
        return queryCacheCandidate;
    }

    public void setQueryCacheCandidate(boolean queryCacheCandidate) {
        this.queryCacheCandidate = queryCacheCandidate;
    }

    // 在分布式执行计划建立后，为当前聚合节点与其子节点之间推导并强制插入（Enforce）合适的 LocalExchangeNode（用于 Backend 节点内多线程间的并行数据 Shuffle/Passthrough）。
    @Override
    public Pair<PlanNode, LocalExchangeType> enforceAndDeriveLocalExchange(
            PlanTranslatorContext translatorContext, PlanNode parent, LocalExchangeTypeRequire parentRequire) {

        ConnectContext connectContext = translatorContext.getConnectContext();
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        // PR #62438: when false, non-finalize agg falls back to BE base class.
        boolean enableLeBeforeAgg = sessionVariable.enableLocalExchangeBeforeAgg;
        boolean hasKeys = !aggInfo.getGroupingExprs().isEmpty();

        // Each branch mirrors the corresponding BE operator's required_data_distribution()
        // check order 1:1. The helper baseClassRequire() expands BE's base class behavior.
        LocalExchangeTypeRequire requireChild;
        if (canUseDistinctStreamingAgg(sessionVariable)) {
            // DistinctStreamingAggOperatorX.  Two flavors share this operator class:
            //   - streaming preagg (useStreamingPreagg=true): performance-only,
            //     flag controls
            //   - non-streaming dedup (useStreamingPreagg=false): correctness-required,
            //     always HASH regardless of flag
            // Diverges from BE: BE's `!_needs_finalize && !enable_local_exchange_before_agg`
            // early return catches non-streaming dedup too, causing the same family of
            // wrong-result bug as AggSink (DORIS-25413).
            if (needsFinalize && !hasKeys) {
                requireChild = LocalExchangeTypeRequire.noRequire();
            } else if (!needsFinalize && useStreamingPreagg && !enableLeBeforeAgg) {
                requireChild = baseClassRequire(connectContext);
            } else if (needsFinalize || (hasKeys && !useStreamingPreagg)) {
                requireChild = AddLocalExchange.isColocated(this)
                        ? LocalExchangeTypeRequire.requireHash()
                        : parentRequire.autoRequireHash();
            } else if (sessionVariable.enableDistinctStreamingAggForcePassthrough) {
                requireChild = LocalExchangeTypeRequire.requirePassthrough();
            } else {
                requireChild = baseClassRequire(connectContext);
            }
        } else if (useStreamingPreagg) {
            // StreamingAggOperatorX
            if (children.get(0) instanceof HashJoinNode
                    && sessionVariable.enableStreamingAggHashJoinForcePassthrough) {
                requireChild = LocalExchangeTypeRequire.requirePassthrough();
            } else if (!needsFinalize && !enableLeBeforeAgg) {
                requireChild = baseClassRequire(connectContext);
            } else if (!hasKeys) {
                requireChild = needsFinalize
                        ? LocalExchangeTypeRequire.noRequire()
                        : baseClassRequire(connectContext);
            } else {
                requireChild = LocalExchangeTypeRequire.requireHash();
            }
        } else {
            // AggSinkOperatorX — covers finalize phase AND non-finalize phases (LOCAL
            // preagg / FIRST_MERGE dedup). Streaming preagg goes through the StreamingAgg
            // branch above, not here.
            //
            // Phase semantics for !needsFinalize:
            //   - FIRST / SECOND (LOCAL phase, !isMerge): performance-only, flag controls
            //   - FIRST_MERGE (correctness-required): always HASH regardless of flag
            //
            // Diverges from BE here: BE's `!_needs_finalize && !enable_local_exchange_before_agg`
            // early return also catches FIRST_MERGE, dropping the HASH requirement and
            // causing wrong-result (e.g. PASSTHROUGH over serial child breaks the
            // group-by-key invariant — DORIS-25413).
            if (!hasKeys) {
                requireChild = needsFinalize
                        ? LocalExchangeTypeRequire.noRequire()
                        : baseClassRequire(connectContext);
            } else if (!needsFinalize && !aggInfo.isMerge() && !enableLeBeforeAgg) {
                // LOCAL phase (FIRST preagg / SECOND distinct local) + user opted out
                // of pre-agg LE → base class decides: serial child → PASSTHROUGH
                // (parallelism), non-serial child → NOOP (no LE).
                requireChild = baseClassRequire(connectContext);
            } else if (!needsFinalize || AddLocalExchange.isColocated(this)) {
                // FIRST_MERGE (correctness) or finalize+colocate → HASH.
                requireChild = parentRequire.autoRequireHash();
            } else if (hasPartitionExprs(parentRequire)) {
                // FE-only heuristic: finalize non-colocate with parent hash requirement
                // → inherit parent's specific hash type.
                requireChild = parentRequire.autoRequireHash();
            } else {
                // FE-only heuristic: finalize non-colocate without parent hash → skip
                // LE (child Exchange already provides hash distribution).
                requireChild = LocalExchangeTypeRequire.noRequire();
            }
        }

        Pair<PlanNode, LocalExchangeType> enforceResult
                = enforceRequire(translatorContext, children.get(0), 0, requireChild);
        children = Lists.newArrayList(enforceResult.first);
        return Pair.of(this, enforceResult.second);
    }

    /** BE base class required_data_distribution: serial child → PASSTHROUGH, else → NOOP. */
    private LocalExchangeTypeRequire baseClassRequire(ConnectContext connectContext) {
        return children.get(0).isSerialOperatorOnBe(connectContext)
                ? LocalExchangeTypeRequire.requirePassthrough()
                : LocalExchangeTypeRequire.noRequire();
    }

    @Override
    protected List<Expr> getSemanticPartitionExprs() {
        return aggInfo.getGroupingExprs();
    }

    @Override
    protected List<Expr> getLocalExchangeDistributeExprs(int childIndex, boolean followedByShuffled) {
        // Mirror BE AggSinkOperatorX::update_operator / StreamingAggOperatorX::update_operator:
        //   _partition_exprs = (distribute_expr_lists set && (followed_by_shuffled || has_distinct))
        //                      ? distribute_expr_lists[0] : grouping_exprs
        // The HASH LocalExchange must partition by _partition_exprs so a streaming partial preagg
        // locally collapses same-key rows.  Using child distribution (default) for a non-shuffled
        // chain scatters same-group rows across N instances, leaving partial_preagg essentially a
        // no-op and breaking row-arrival order at downstream merge-finalize (e.g. group_concat).
        List<Expr> childDist = getChildDistributeExprList(childIndex);
        // Multi-distinct aggregates are detected by function name. Nereids rewrites
        // count/sum(distinct ...) into dedicated MultiDistinct* functions constructed with
        // distinct=false and a "multi_distinct_" name, so by this legacy FunctionCallExpr layer
        // isDistinct() is already false and the function name is the only remaining signal —
        // there is no structural flag to test here.
        boolean hasDistinct = aggInfo.getAggregateExprs().stream()
                .map(FunctionCallExpr::getFnName)
                .filter(name -> name != null)
                .map(name -> name.getFunction())
                .filter(name -> name != null)
                .anyMatch(name -> name.startsWith("multi_distinct_"));
        if (childDist != null && !childDist.isEmpty() && (followedByShuffled || hasDistinct)) {
            return childDist;
        }
        return Lists.newArrayList(aggInfo.getGroupingExprs());
    }
    // 判断当前聚合算子为了计算正确性，是否要求上游输入数据必须按 Hash 分布。
    @Override
    public boolean requiresShuffleForCorrectness() {
        // Mirrors BE's AggSinkOperatorX::is_shuffled_operator() exactly:
        //   finalize agg with group keys needs hash-distributed input for correctness.
        // GLOBAL dedup (!needsFinalize) is intentionally NOT included here — if a
        // GLOBAL dedup exists, a finalize agg always sits above it (e.g. DISTINCT_GLOBAL
        // above DISTINCT_LOCAL/GLOBAL_DEDUP), and the finalize agg propagates the flag
        // down via inheritedShuffled. A solo finalize agg satisfies hash distribution
        // through its own child requirement.
        return needsFinalize && !aggInfo.getGroupingExprs().isEmpty();
    }

    private boolean canUseDistinctStreamingAgg(SessionVariable sessionVariable) {
        return aggInfo.getAggregateExprs().isEmpty() && sortByGroupKey == null
                && sessionVariable.enableDistinctStreamingAggregation;
    }

    @Override
    protected boolean shouldResetSerialFlagForChild(int childIndex) {
        // Non-streaming AGG is a pipeline breaker: child is in AGG_Sink pipeline,
        // parent is in AGG_Source pipeline. Reset inherited serial flag from parent
        // (different pipeline), but enforceRequire still adds this node's own
        // isSerialNode() so the child sees AGG_Sink's serial status correctly.
        return !useStreamingPreagg;
    }
}
