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

package org.apache.doris.nereids.glue.translator;

import org.apache.doris.analysis.ColumnRefExpr;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.processor.post.TopnFilterContext;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEProducer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.planner.CTEScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TPushAggOp;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Context of physical plan.
 */
// 在 Doris 中，优化器 Nereids 生成的是一套全新的物理计划树（Physical Plan Tree），而后端（BE）和传统执行引擎依赖的是旧版的执行计划结构（Planner/Exec Plan），例如 PlanFragment、PlanNode、TupleDescriptor、SlotRef 等。
// PlanTranslatorContext 扮演着 “桥梁与翻译上下文” 的角色，主要有以下四大核心职责：
// 类型与表达形式转换（Nereids ↔ Legacy Planner）：
// 保存 Nereids 抽象概念（如 ExprId、SlotReference）到旧执行计划概念（如 SlotId、SlotRef、TupleDescriptor）的双向映射关系。
// 描述符表（DescriptorTable）管理：维护并生成查询执行所需的 DescriptorTable、TupleDescriptor（元组描述符）和 SlotDescriptor（槽位描述符）。
// 全局唯一 ID 生成与资源跟踪：生成全局唯一的 PlanNodeId 和 PlanFragmentId；
// 收集并管理所有的 PlanFragment（计划片段）、ScanNode（扫描节点）、PhysicalRelation 以及 CTE（公共表表达式）相关的执行结构。
public class PlanTranslatorContext {
    // 当前客户端连接的上下文信息，包含会话变量（SessionVariable）、用户认证、当前数据库等。
    private final ConnectContext connectContext;
    // 当前 SQL 语句执行的上下文，记录语句级别的解析和执行状态。
    private final StatementContext statementContext;
    // 存储扫描节点所需的上下文（例如云原生存算分离架构下的 Cluster Name 存储集群名称等）。
    private final ScanContext scanContext;
    // 保存当前物理计划翻译后生成的所有 PlanFragment（执行片段）列表，最终提交给执行器执行。
    private final List<PlanFragment> planFragments = Lists.newArrayList();
    // 包含此查询用到的所有 TupleDescriptor 和 SlotDescriptor 的表结构信息，传给 BE 用于分配数据内存和 Schema 解析。
    private DescriptorTable descTable;
    // 负责将 Nereids 中的 Runtime Filter 转换为传统执行器可识别的 Runtime Filter。
    private final RuntimeFilterTranslator translator;
    // 管理 TopN 谓词下推与 TopN Filter 相关的上下文状态。
    private final TopnFilterContext topnFilterContext;
    /**
     * index from Nereids' slot to legacy slot.
     */
    // 将 Nereids 的表达式 ID（ExprId）映射到旧执行计划的槽位引用（SlotRef）。
    private final Map<ExprId, SlotRef> exprIdToSlotRef = Maps.newHashMap();
    // exprIdToSlotRef 的反向索引，将旧执行计划的 SlotId 映射回 Nereids 的 ExprId。
    private final Map<Integer, PlanNodeId> nereidsIdToPlanNodeIdMap = Maps.newHashMap();

    /**
     * Inverted index from legacy slot to Nereids' slot.
     */
    // 建立 Nereids 节点 ID 与旧执行计划 PlanNodeId 的对应关系，方便后续进行节点状态追踪和替换。
    private final Map<SlotId, ExprId> slotIdToExprId = Maps.newHashMap();

    /**
     * For each lambda argument (ArrayItemReference),
     * we create a ColumnRef representing it and
     * then translate it based on the ExprId of the ArrayItemReference.
     */
    // 用于 Lambda 表达式参数（例如 ArrayItemReference），将 Nereids 的 ExprId 映射到 ColumnRefExpr。
    private final Map<ExprId, ColumnRefExpr> exprIdToColumnRef = Maps.newHashMap();
    // 收集翻译过程中生成的所有数据扫描节点（ScanNode，如 OlapScanNode）。
    private final List<ScanNode> scanNodes = Lists.newArrayList();
    // 记录与 scanNodes 一一对应的 Nereids 物理关系节点（PhysicalRelation）。
    private final List<PhysicalRelation> physicalRelations = Lists.newArrayList();
    // PlanFragmentId 的唯一递增 ID 生成器。
    private final IdGenerator<PlanFragmentId> fragmentIdGenerator = PlanFragmentId.createGenerator();
    // PlanNodeId 的唯一递增 ID 生成器。
    private final IdGenerator<PlanNodeId> nodeIdGenerator = PlanNodeId.createGenerator();
    // 记录 CTEId 对应的生成者（Producer）所在的 PlanFragment。
    private final Map<CTEId, PlanFragment> cteProduceFragments = Maps.newHashMap();
    // 记录 CTEId 到 Nereids PhysicalCTEProducer 节点的映射。
    private final Map<CTEId, PhysicalCTEProducer> cteProducerMap = Maps.newHashMap();
    // 记录 CTEId 到 Nereids PhysicalCTEConsumer 节点的映射。
    private final Map<CTEId, PhysicalCTEConsumer> cteConsumerMap = Maps.newHashMap();
    // 记录 PlanFragmentId 与生成的 CTEScanNode 之间的映射。
    private final Map<PlanFragmentId, CTEScanNode> cteScanNodeMap = Maps.newHashMap();
    // 存储表级别（RelationId）下推的聚合算子操作类型（如 COUNT 或 MIN/MAX 下推至存储层）。
    private final Map<RelationId, TPushAggOp> tablePushAggOp = Maps.newHashMap();
    // 存储下推到存储层的 COUNT 聚合参数对应的 ExprId 列表。
    private final Map<RelationId, List<ExprId>> tablePushCountArgumentExprIds = Maps.newHashMap();
    // 记录每个 ScanNode 中缺乏统计信息（Unknown Stats）的列（SlotId），用于后续的 forbid_unknown_col_stats 安全性校验。
    private final Map<ScanNode, Set<SlotId>> statsUnknownColumnsMap = Maps.newHashMap();

    // Per-node "is there a serial operator between me and the pipeline's sink" flag.
    // Mirrors BE's any_of(operators[idx..end], is_serial_operator) check used by
    // _add_local_exchange / need_to_local_exchange to skip LE insertion when an ancestor
    // in the same pipeline is already serial (the whole pipeline runs with 1 task, so an
    // extra LE would be a no-op).  Written by AddLocalExchange entry + PlanNode.enforceRequire
    // step 1 (root → leaf during traversal).  Read by PlanNode.enforceRequire step 4 (Layer 1
    // skip) and by child overrides that compute their require.  Reset to false at fragment
    // root and across pipeline boundaries (see shouldResetSerialFlagForChild).
    // 标记当前节点在同一 Pipeline 中是否存在串行（Serial）祖先节点。若存在，整个管道在 BE 上只会以单任务运行，此时无需插入额外的 Local Exchange。
    private final Map<PlanNodeId, Boolean> serialAncestorInPipelineMap = Maps.newHashMap();

    // Per-node "is there a downstream operator that depends on hash distribution for
    // correctness, with HASH/NOOP path connecting it to me" flag.  Mirrors BE's
    // _followed_by_shuffled_operator propagation in pipeline_fragment_context.cpp.
    // Written by PlanNode.enforceRequire step 1b (root → leaf).  Read by SetOperationNode
    // to decide whether to propagate hash requirement to its inputs (only when downstream
    // needs shuffle for correctness, not just for performance like StreamingAgg pre-agg).
    // 标记下游是否存在依赖 Hash 分布保证正确性的算子，协助 SetOperation 等算子判断是否必须向输入传递 Hash 分布要求。
    private final Map<PlanNodeId, Boolean> shuffledAncestorMap = Maps.newHashMap();

    // Whether the fragment currently being processed by AddLocalExchange is eligible for the
    // bucket → local-hash parallelism upgrade: a pooled bucket-join fragment whose per-BE
    // instance count exceeds (buckets-with-data per BE) × local_shuffle_bucket_upgrade_ratio.
    // Computed once per fragment in AddLocalExchange.addLocalExchange from the distributed
    // plan's LocalShuffleBucketJoinAssignedJob assignments; read by
    // HashJoinNode.enforceAndDeriveLocalExchange.
    // 标记当前处理的 Fragment 是否满足升级“Bucket Join → Local Hash”并行度的资格。
    private boolean currentFragmentBucketUpgradeEligible = false;

    // Per-node "a bucket join above me in this fragment already upgraded to local hash" flag.
    // An upgraded join marks its direct children so a stacked bucket join below keeps its
    // BUCKET_HASH_SHUFFLE requires: if it also upgraded, its LOCAL hash output (keyed by ITS
    // join keys) would type-satisfy the upper join's requireSpecific(LOCAL_EXECUTION_HASH)
    // and suppress the LE that re-aligns data to the upper join's keys → wrong results.
    // 标记祖先节点中是否有已升级为 Local Hash 的 Bucket Join，防止上下的叠加 Bucket Join 重复升级导致结果错误。
    private final Map<PlanNodeId, Boolean> bucketUpgradedAncestorMap = Maps.newHashMap();

    // Whether the current fragment uses LocalShuffleAssignedJob (pooling scan with
    // ignoreDataDistribution → _parallel_instances=1 in BE). When true, serial operators
    // indicate real pipeline bottlenecks needing PASSTHROUGH fan-out (heavy_ops).
    // 标记当前 Fragment 是否使用了 Pooling 扫描（LocalShuffleAssignedJob）。为 true 时，串行算子表示瓶颈并可能需要 Passthrough 拓展。
    private boolean isTopMaterializeNode = true;
    // 存储虚拟列（Virtual Columns）的 SlotId 集合。
    private final Set<SlotId> virtualColumnIds = Sets.newHashSet();

    /** PlanTranslatorContext */
    public PlanTranslatorContext(CascadesContext ctx) {
        this.connectContext = ctx.getConnectContext();
        this.statementContext = ctx.getStatementContext();
        this.scanContext = connectContext == null || connectContext.getSessionVariable() == null
                ? ScanContext.EMPTY
                : ScanContext.builder()
                        .clusterName(connectContext.getSessionVariable().resolveCloudClusterName(connectContext))
                        .build();
        this.translator = new RuntimeFilterTranslator(ctx.getRuntimeFilterContext());
        this.topnFilterContext = ctx.getTopnFilterContext();
        this.descTable = new DescriptorTable();
    }

    /** PlanTranslatorContext */
    public PlanTranslatorContext(CascadesContext ctx, DescriptorTable descTable) {
        this.connectContext = ctx.getConnectContext();
        this.statementContext = ctx.getStatementContext();
        this.scanContext = connectContext == null || connectContext.getSessionVariable() == null
                ? ScanContext.EMPTY
                : ScanContext.builder()
                        .clusterName(connectContext.getSessionVariable().resolveCloudClusterName(connectContext))
                        .build();
        this.translator = new RuntimeFilterTranslator(ctx.getRuntimeFilterContext());
        this.topnFilterContext = ctx.getTopnFilterContext();
        this.descTable = descTable;
    }

    /**
     * Constructor for testing purposes with default values.
     */
    @VisibleForTesting
    public PlanTranslatorContext() {
        this.connectContext = null;
        this.statementContext = new StatementContext();
        this.scanContext = ScanContext.EMPTY;
        this.translator = null;
        this.topnFilterContext = new TopnFilterContext();
        this.descTable = new DescriptorTable();
    }

    /**
     * remember the unknown-stats column and its scan, used for forbid_unknown_col_stats check
     */
    public void addUnknownStatsColumn(ScanNode scan, SlotId slotId) {
        Set<SlotId> slots = statsUnknownColumnsMap.get(scan);
        if (slots == null) {
            statsUnknownColumnsMap.put(scan, Sets.newHashSet(slotId));
        } else {
            statsUnknownColumnsMap.get(scan).add(slotId);
        }
    }

    public boolean isColumnStatsUnknown(ScanNode scan, SlotId slotId) {
        Set<SlotId> unknownSlots = statsUnknownColumnsMap.get(scan);
        if (unknownSlots == null) {
            return false;
        }
        return unknownSlots.contains(slotId);
    }

    public void removeScanFromStatsUnknownColumnsMap(ScanNode scan) {
        statsUnknownColumnsMap.remove(scan);
    }

    public SessionVariable getSessionVariable() {
        return connectContext == null ? null : connectContext.getSessionVariable();
    }

    public ConnectContext getConnectContext() {
        return connectContext;
    }

    public StatementContext getStatementContext() {
        return statementContext;
    }

    public Set<ScanNode> getScanNodeWithUnknownColumnStats() {
        return statsUnknownColumnsMap.keySet();
    }

    public List<PlanFragment> getPlanFragments() {
        return planFragments;
    }

    public Map<CTEId, PlanFragment> getCteProduceFragments() {
        return cteProduceFragments;
    }

    public Map<CTEId, PhysicalCTEProducer> getCteProduceMap() {
        return cteProducerMap;
    }

    public Map<CTEId, PhysicalCTEConsumer> getCteConsumerMap() {
        return cteConsumerMap;
    }

    public Map<PlanFragmentId, CTEScanNode> getCteScanNodeMap() {
        return cteScanNodeMap;
    }

    public TupleDescriptor generateTupleDesc() {
        return descTable.createTupleDescriptor();
    }

    public Optional<RuntimeFilterTranslator> getRuntimeTranslator() {
        return Optional.ofNullable(translator);
    }

    public TopnFilterContext getTopnFilterContext() {
        return topnFilterContext;
    }

    public PlanFragmentId nextFragmentId() {
        return fragmentIdGenerator.getNextId();
    }

    public PlanNodeId nextPlanNodeId() {
        return nodeIdGenerator.getNextId();
    }

    public void setHasSerialAncestorInPipeline(PlanNode node, boolean hasSerialAncestorInPipeline) {
        serialAncestorInPipelineMap.put(node.getId(), hasSerialAncestorInPipeline);
    }

    public boolean hasSerialAncestorInPipeline(PlanNode node) {
        return serialAncestorInPipelineMap.getOrDefault(node.getId(), false);
    }

    public void setHasShuffleForCorrectnessAncestor(PlanNode node, boolean value) {
        shuffledAncestorMap.put(node.getId(), value);
    }

    public boolean hasShuffleForCorrectnessAncestor(PlanNode node) {
        return shuffledAncestorMap.getOrDefault(node.getId(), false);
    }

    public void setCurrentFragmentBucketUpgradeEligible(boolean eligible) {
        this.currentFragmentBucketUpgradeEligible = eligible;
    }

    public boolean isCurrentFragmentBucketUpgradeEligible() {
        return currentFragmentBucketUpgradeEligible;
    }

    public void setHasBucketUpgradedAncestor(PlanNode node, boolean value) {
        bucketUpgradedAncestorMap.put(node.getId(), value);
    }

    public boolean hasBucketUpgradedAncestor(PlanNode node) {
        return bucketUpgradedAncestorMap.getOrDefault(node.getId(), false);
    }

    public SlotDescriptor addSlotDesc(TupleDescriptor t) {
        return descTable.addSlotDescriptor(t);
    }

    public void addPlanFragment(PlanFragment planFragment) {
        this.planFragments.add(planFragment);
    }

    public void addExprIdSlotRefPair(ExprId exprId, SlotRef slotRef) {
        exprIdToSlotRef.put(exprId, slotRef);
        slotIdToExprId.put(slotRef.getDesc().getId(), exprId);
    }

    public Map<Integer, PlanNodeId> getNereidsIdToPlanNodeIdMap() {
        return nereidsIdToPlanNodeIdMap;
    }

    public void addExprIdColumnRefPair(ExprId exprId, ColumnRefExpr columnRefExpr) {
        exprIdToColumnRef.put(exprId, columnRefExpr);
    }

    /**
     * merge source fragment info into target fragment.
     * include runtime filter info and fragment attribute.
     */
    public void mergePlanFragment(PlanFragment srcFragment, PlanFragment targetFragment) {
        srcFragment.getTargetRuntimeFilterIds().forEach(targetFragment::setTargetRuntimeFilterIds);
        srcFragment.getBuilderRuntimeFilterIds().forEach(targetFragment::setBuilderRuntimeFilterIds);
        targetFragment.setHasColocatePlanNode(targetFragment.hasColocatePlanNode()
                || srcFragment.hasColocatePlanNode());
        this.planFragments.remove(srcFragment);
    }

    public SlotRef findSlotRef(ExprId exprId) {
        return exprIdToSlotRef.get(exprId);
    }

    public ColumnRefExpr findColumnRef(ExprId exprId) {
        return exprIdToColumnRef.get(exprId);
    }

    public void addScanNode(ScanNode scanNode, PhysicalRelation physicalRelation) {
        scanNodes.add(scanNode);
        physicalRelations.add(physicalRelation);
    }

    public String getClusterName() {
        return scanContext.getClusterName();
    }

    public ScanContext getScanContext() {
        return scanContext;
    }

    public List<PhysicalRelation> getPhysicalRelations() {
        return physicalRelations;
    }

    public ExprId findExprId(SlotId slotId) {
        return slotIdToExprId.get(slotId);
    }

    public List<ScanNode> getScanNodes() {
        return scanNodes;
    }

    /**
     * Create SlotDesc and add it to the mappings from expression to the stales expr.
     */
    public SlotDescriptor createSlotDesc(TupleDescriptor tupleDesc, SlotReference slotReference) {
        SlotDescriptor slotDescriptor = this.addSlotDesc(tupleDesc);
        // Only the SlotDesc that in the tuple generated for scan node would have corresponding column.
        Optional<Column> column = slotReference.getOriginalColumn();
        if (column.isPresent()) {
            slotDescriptor.setColumn(column.get());
        } else {
            slotDescriptor.setCaptionAndNormalize(slotReference.toString());
        }
        slotDescriptor.setLabel(slotReference.getName());
        slotDescriptor.setType(slotReference.getDataType().toCatalogDataType());
        slotDescriptor.setIsNullable(slotReference.nullable());

        if (column.isPresent()) {
            slotDescriptor.setAutoInc(column.get().isAutoInc());
        }
        if (slotReference.getAllAccessPaths().isPresent()) {
            slotDescriptor.setAllAccessPaths(slotReference.getAllAccessPaths().get());
            slotDescriptor.setPredicateAccessPaths(slotReference.getPredicateAccessPaths().get());
            slotDescriptor.setDisplayAllAccessPaths(slotReference.getDisplayAllAccessPaths().get());
            slotDescriptor.setDisplayPredicateAccessPaths(slotReference.getDisplayPredicateAccessPaths().get());
        }
        SlotRef slotRef;
        slotRef = new SlotRef(slotDescriptor);
        slotRef.setLabel(slotReference.getName());
        if (slotReference.hasSubColPath() && slotReference.getOriginalColumn().isPresent()) {
            slotDescriptor.setSubColLables(slotReference.getSubPath());
            // use lower case name for variant's root, since backend treat parent column as lower case
            // see issue: https://github.com/apache/doris/pull/32999/commits
            slotDescriptor.setMaterializedColumnName(slotRef.getColumnName().toLowerCase()
                    + "." + String.join(".", slotReference.getSubPath()));
        }
        this.addExprIdSlotRefPair(slotReference.getExprId(), slotRef);
        return slotDescriptor;
    }

    public List<TupleDescriptor> getTupleDesc(PlanNode planNode) {
        if (planNode.getOutputTupleDesc() != null) {
            return Lists.newArrayList(planNode.getOutputTupleDesc());
        }
        return planNode.getOutputTupleIds().stream().map(this::getTupleDesc).collect(Collectors.toList());
    }

    public TupleDescriptor getTupleDesc(TupleId tupleId) {
        return descTable.getTupleDesc(tupleId);
    }

    public DescriptorTable getDescTable() {
        return descTable;
    }

    public void setRelationPushAggOp(RelationId relationId, TPushAggOp aggOp) {
        tablePushAggOp.put(relationId, aggOp);
    }

    public TPushAggOp getRelationPushAggOp(RelationId relationId) {
        return tablePushAggOp.getOrDefault(relationId, TPushAggOp.NONE);
    }

    public void setRelationPushCountArgumentExprIds(RelationId relationId, List<ExprId> exprIds) {
        tablePushCountArgumentExprIds.put(relationId, Lists.newArrayList(exprIds));
    }

    public List<ExprId> getRelationPushCountArgumentExprIds(RelationId relationId) {
        return tablePushCountArgumentExprIds.getOrDefault(relationId, Collections.emptyList());
    }

    public boolean isTopMaterializeNode() {
        return isTopMaterializeNode;
    }

    public void setTopMaterializeNode(boolean topMaterializeNode) {
        isTopMaterializeNode = topMaterializeNode;
    }

    public Set<SlotId> getVirtualColumnIds() {
        return virtualColumnIds;
    }
}
