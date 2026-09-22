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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/PlanFragment.java
// and modified by Doris

package org.apache.doris.planner;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprToSqlVisitor;
import org.apache.doris.analysis.ExprToThriftVisitor;
import org.apache.doris.analysis.JoinOperator;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.ToSqlParams;
import org.apache.doris.common.TreeNode;
import org.apache.doris.nereids.trees.plans.distribute.NereidsSpecifyInstances;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.ScanSource;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPartitionType;
import org.apache.doris.thrift.TPlanFragment;
import org.apache.doris.thrift.TQueryCacheParam;
import org.apache.doris.thrift.TResultSinkType;

import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * PlanFragments form a tree structure via their ExchangeNodes. A tree of fragments
 * connected in that way forms a plan. The output of a plan is produced by the root
 * fragment and is either the result of the query or an intermediate result
 * needed by a different plan (such as a hash table).
 *
 * Plans are grouped into cohorts based on the consumer of their output: all
 * plans that materialize intermediate results for a particular consumer plan
 * are grouped into a single cohort.
 *
 * A PlanFragment encapsulates the specific tree of execution nodes that
 * are used to produce the output of the plan fragment, as well as output exprs,
 * destination node, etc. If there are no output exprs, the full row that is
 * produced by the plan root is marked as materialized.
 *
 * A plan fragment can have one or many instances, each of which in turn is executed by
 * an individual node and the output sent to a specific instance of the destination
 * fragment (or, in the case of the root fragment, is materialized in some form).
 *
 * A hash-partitioned plan fragment is the result of one or more hash-partitioning data
 * streams being received by plan nodes in this fragment. In the future, a fragment's
 * data partition could also be hash partitioned based on a scan node that is reading
 * from a physically hash-partitioned table.
 *
 * The sequence of calls is:
 * - c'tor
 * - assemble with getters, etc.
 * - finalize()
 * - toThrift()
 *
 * TODO: the tree of PlanNodes is connected across fragment boundaries, which makes
 *   it impossible search for things within a fragment (using TreeNode functions);
 *   fix that
 */
// PlanFragment（执行计划片段）是分布式查询执行计划的核心封装类。它继承自 TreeNode<PlanFragment>，表示一个可在集群节点上独立、并行执行的树状计划片段。
// 在 Doris 分布式查询引擎中，一个完整的 SQL 逻辑执行计划（Physical Plan）会被切分成多个物理上分布执行的片段，这些片段被称为 PlanFragment。
// 在 FE 构造执行计划时，当一个 PlanFragment 需要接收来自上游 Fragment 的数据时，ExchangeNode 就是该 Fragment 算子树的叶子节点（数据源头）。
// 如何体现接收：
// 调用的 setDestination(ExchangeNode destNode) 方法中，传入的 destNode 正是下游 Fragment 内部用来“接收数据”的算子。


// 其主要作用包括：
// 树形结构节点：封装当前 Fragment 内部的物理执行算子树（以 PlanNode 为根）。多个 Fragment 通过 ExchangeNode 和 DataStreamSink 相互连接，形成一个 Fragment 树。
// 数据流分发定义：定义输入数据如何被切分/接收（dataPartition），以及计算出的输出数据如何传输给下游 Fragment 或客户端（outputPartition 和 sink）。
// 并行度与调度依据：记录 Fragment 执行时的并行度（parallelExecNum）、Bucket/Colocate 属性以及 Runtime Filter 生产者/消费者关系，供 FE 的 Coordinator 模块生成具体的 ExecPlanInstance 并下发调度。
// 序列化与下发：提供 toThrift() 方法，将前端构建的 Java 描述转换为 Thrift 结构体 TPlanFragment，进而下发给后端 Backend（BE）执行。
public class PlanFragment extends TreeNode<PlanFragment> {
    private static final Logger LOG = LogManager.getLogger(PlanFragment.class);

    // id for this plan fragment
    // 当前 Fragment 的唯一标识符 ID（如 F00, F01）。
    private PlanFragmentId fragmentId;
    // nereids planner and original planner generate fragments in different order.
    // This makes nereids fragment id different from that of original planner, and
    // hence different from that in profile.
    // in original planner, fragmentSequenceNum is fragmentId, and in nereids planner,
    // fragmentSequenceNum is the id displayed in profile
    // Fragment 的序号。旧 Planner 中 fragmentSequenceNum 与 fragmentId 一致；在 Nereids 优化器中，生成 Fragment 顺序不同，此序号专门用于 Profile 显示时的顺序标识。
    private int fragmentSequenceNum;
    // private PlanId planId_;
    // private CohortId cohortId_;

    // root of plan tree executed by this fragment
    // 当前 Fragment 包含的算子树的根节点（如 HashJoinNode, AggregationNode, ScanNode 等）。
    private PlanNode planRoot;

    // exchange node which this fragment sends its output to
    // 当前 Fragment 的输出数据目标接收节点（即下游 Fragment 中的 ExchangeNode）。如果为 null，说明该 Fragment 为根 Fragment，输出直接返回给客户端。
    private ExchangeNode destNode;

    // if null, set with the planRoot's output exprs when translate PhysicalPlan. see `translatePlan`
    // 当前 Fragment 输出的表达式列表。若为空，则默认将根节点的完整 Row 标记为 Materialized（物化）。
    private ArrayList<Expr> outputExprs;

    // created in finalize() or set in setSink()
    // 数据输出接收器。可能为传输给下游的 DataStreamSink，或返回给客户端的 ResultSink。
    protected DataSink sink;

    // data source(or sender) of specific partition in the fragment;
    // an UNPARTITIONED fragment is executed on only a single node
    // 描述当前 Fragment 接收/输入数据的分区方式（如 UNPARTITIONED, RANDOM, HASH_PARTITIONED 等）
    private DataPartition dataPartition;

    // specification of the actually input partition of this fragment when transmitting to be.
    // By default, the value of the data partition in planner and the data partition transmitted to be are the same.
    // So this attribute is empty.
    // But sometimes the planned value and the serialized value are inconsistent. You need to set this value.
    // At present, this situation only occurs in the fragment where the scan node is located.
    // Since the data partition expression of the scan node is actually constructed from the schema of the table,
    //   the expression is not analyzed.
    // This will cause this expression to not be serialized correctly and transmitted to be.
    // In this case, you need to set this attribute to DataPartition RANDOM to avoid the problem.
    // 专门用于 Thrift 序列化的输入数据分区。当 Scan 节点的计算分区表达式未经过完整 Analyze 导致无法直接序列化时，在此处覆盖为 RANDOM 避免 BE 异常。
    private DataPartition dataPartitionForThrift;

    // specification of how the output of this fragment is partitioned (i.e., how
    // it's sent to its destination);
    // if the output is UNPARTITIONED, it is being broadcast
    // 描述当前 Fragment 输出数据的分区/路由策略（发送给目标 ExchangeNode 时的路由规则）。
    protected DataPartition outputPartition;

    // Whether query statistics is sent with every batch. In order to get the query
    // statistics correctly when query contains limit, it is necessary to send query
    // statistics with every batch, or only in close.
    // 是否随每个 Data Batch 实时发送查询统计信息（在含 LIMIT 查询时保证统计准确性）。
    private boolean transferQueryStatisticsWithEveryBatch;

    // TODO: SubstitutionMap outputSmap;
    // substitution map to remap exprs onto the output of this fragment, to be applied
    // at destination fragment

    // specification of the number of parallel when fragment is executed
    // default value is 1
    // 该 Fragment 在单个 BE 节点上的并行执行实例数量（默认值为 1）。
    private int parallelExecNum = 1;

    // The runtime filter id that produced
    // 当前 Fragment 负责生成/构建的 Runtime Filter ID 集合。
    private Set<RuntimeFilterId> builderRuntimeFilterIds;
    // The runtime filter id that is expected to be used
    // 当前 Fragment 期望应用/使用的 Runtime Filter ID 集合。
    private Set<RuntimeFilterId> targetRuntimeFilterIds;
    // 分桶数量，用于 Bucket Shuffle 等优化策略中的分桶计算。
    private int bucketNum;

    // has colocate plan node
    // 标识当前 Fragment 内是否包含 Colocate Join 算子。
    protected boolean hasColocatePlanNode = false;
    // 使用 Supplier 懒加载懒求值，标识当前 Fragment 是否包含 Bucket Shuffle Join 或 Bucket Shuffle SetOperation 算子。
    protected final Supplier<Boolean> hasBucketShuffleNode;
    // 结果返回的协议类型（如 MYSQL_PROTOCOL, ARROW_FLIGHT 等），默认为 MySQL 协议。
    private TResultSinkType resultSinkType = TResultSinkType.MYSQL_PROTOCOL;
    // Nereids 优化器中指定的 Scan 数据源实例分配策略（可选）。
    public Optional<NereidsSpecifyInstances<ScanSource>> specifyInstances = Optional.empty();
    // 查询缓存参数（Cache 节点 ID、Digest 摘要等）
    public TQueryCacheParam queryCacheParam;
    // 是否强制当前 Fragment 只以单实例（单线程）方式运行。
    private boolean forceSingleInstance = false;

    /**
     * C'tor for fragment with specific partition; the output is by default broadcast.
     */
    public PlanFragment(PlanFragmentId id, PlanNode root, DataPartition partition) {
        this.fragmentId = id;
        this.planRoot = root;
        this.dataPartition = partition;
        this.outputPartition = DataPartition.UNPARTITIONED;
        this.transferQueryStatisticsWithEveryBatch = false;
        this.builderRuntimeFilterIds = new HashSet<>();
        this.targetRuntimeFilterIds = new HashSet<>();
        this.hasBucketShuffleNode = buildHasBucketShuffleNode();
        setParallelExecNumIfExists();
        setFragmentInPlanTree(planRoot);
    }

    public PlanFragment(PlanFragmentId id, PlanNode root, DataPartition partition, DataPartition partitionForThrift) {
        this(id, root, partition);
        this.dataPartitionForThrift = partitionForThrift;
    }

    public PlanFragment(PlanFragmentId id, PlanNode root, DataPartition partition,
            Set<RuntimeFilterId> builderRuntimeFilterIds, Set<RuntimeFilterId> targetRuntimeFilterIds) {
        this(id, root, partition);
        this.builderRuntimeFilterIds = new HashSet<>(builderRuntimeFilterIds);
        this.targetRuntimeFilterIds = new HashSet<>(targetRuntimeFilterIds);
    }
    // collectInCurrentFragment 是定义在 PlanNode（或其基类 TreeNode）中的方法。它的内部实现逻辑本质上是一个受控的树形遍历（DFS/BFS）
    // 在 Doris 的物理执行计划中，PlanFragment 是集群调度的基本物理单元。而整体的 PlanNode 算子树在结构上是跨越 Fragment 连成一棵大树的（通过 ExchangeNode 互相连接）。
    private Supplier<Boolean> buildHasBucketShuffleNode() {
        return Suppliers.memoize(() -> {
            // 以当前 Fragment 的根节点 getPlanRoot() 为起点，递归向下检索并收集所有类型为 HashJoinNode 的算子。
            // 关键点：collectInCurrentFragment 会在遇到 ExchangeNode 时自动截断递归，确保只收集属于当前 Fragment 的节点。
            List<HashJoinNode> hashJoinNodes = getPlanRoot().collectInCurrentFragment(HashJoinNode.class::isInstance);
            // 遍历并检查是否存在 Bucket Shuffle Join
            // 判断该 Hash Join 是否采用了 Bucket Shuffle Join 策略（即数据按照分桶 Key 进行了 Hash 重分区与对齐）。
            for (HashJoinNode hashJoinNode : hashJoinNodes) {
                if (hashJoinNode.isBucketShuffle()) {
                    return true;
                }
            }
            // 若前面没有匹配到 Bucket Shuffle Join，继续从根节点遍历当前 Fragment，收集所有集合操作算子 SetOperationNode（如 UNION、INTERSECT、EXCEPT 等）。
            List<SetOperationNode> setOperationNodes
                    = getPlanRoot().collectInCurrentFragment(SetOperationNode.class::isInstance);
            // 遍历并检查是否存在 Bucket Shuffle 集合操作
            for (SetOperationNode setOperationNode : setOperationNodes) {
                if (setOperationNode.isBucketShuffle()) {
                    return true;
                }
            }
            return false;
        });
    }

    /**
     * Assigns 'this' as fragment of all PlanNodes in the plan tree rooted at node.
     * Does not traverse the children of ExchangeNodes because those must belong to a
     * different fragment.
     */
    // 入参 PlanNode node：当前递归遍历的起点算子节点（通常为物理执行计划树的根节点 planRoot 或某个子树根节点）。
    // 将当前 PlanFragment 对象（即 this）绑定到以 node 为根的物理算子树中的所有 PlanNode 上。
    // 关键边界：不会继续向下遍历 ExchangeNode 的子节点，因为 ExchangeNode 是不同 Fragment 之间的物理分隔符，其子节点必然属于上游的其他 Fragment。
    public void setFragmentInPlanTree(PlanNode node) {
        // 如果传入的 node 为 null（例如 Fragment 的 planRoot 尚未构建或子节点为空），直接返回，避免引发 NullPointerException，这也是递归调用的基准终止条件之一。
        if (node == null) {
            return;
        }
        node.setFragment(this);
        if (node instanceof ExchangeNode) {
            return;
        }
        for (PlanNode child : node.getChildren()) {
            setFragmentInPlanTree(child);
        }
    }

    /**
     * Assign ParallelExecNum by PARALLEL_FRAGMENT_EXEC_INSTANCE_NUM in SessionVariable for synchronous request
     * Assign ParallelExecNum by default value for Asynchronous request
     */
    // 同步请求（用户交互式查询）：从当前会话变量 SessionVariable 的 PARALLEL_FRAGMENT_EXEC_INSTANCE_NUM 中获取并行度配置并赋值。
    // 异步请求（后台任务/非交互式线程）：由于没有绑定的上下文，会保持类成员变量 parallelExecNum 的默认初始值（默认值为 1）。
    public void setParallelExecNumIfExists() {
        // 通过 ConnectContext 的静态方法 get() 获取当前线程绑定的客户端连接上下文。
        ConnectContext context = ConnectContext.get();
        if (context != null) {
            // 解析出当前查询所使用的存算分离集群名称或计算资源组（Cloud Cluster Name）。
            // 用途：Doris 支持按计算集群动态配置资源，因此不同计算集群的物理节点规格不同，设置的并行度参数也可能不同。
            String clusterName = context.getSessionVariable().resolveCloudClusterName(context);
            // 从会话变量中提取对应的物理执行并行度（即 parallel_fragment_exec_instance_num），并赋值给当前 PlanFragment 实例的成员变量 parallelExecNum。
            parallelExecNum = context.getSessionVariable().getParallelExecInstanceNum(clusterName);
        }
    }

    // Manually set parallel exec number
    // Currently for broker load
    public void setParallelExecNum(int parallelExecNum) {
        this.parallelExecNum = parallelExecNum;
    }

    public void setOutputExprs(List<Expr> outputExprs) {
        this.outputExprs = Expr.cloneList(outputExprs, null);
    }

    public ArrayList<Expr> getOutputExprs() {
        return outputExprs;
    }

    public void setBuilderRuntimeFilterIds(RuntimeFilterId rid) {
        this.builderRuntimeFilterIds.add(rid);
    }

    public void setTargetRuntimeFilterIds(RuntimeFilterId rid) {
        this.targetRuntimeFilterIds.add(rid);
    }

    public void setHasColocatePlanNode(boolean hasColocatePlanNode) {
        this.hasColocatePlanNode = hasColocatePlanNode;
    }

    public boolean hasBucketShuffleNode() {
        return hasBucketShuffleNode.get();
    }

    public void setResultSinkType(TResultSinkType resultSinkType) {
        this.resultSinkType = resultSinkType;
    }

    public boolean hasColocatePlanNode() {
        return hasColocatePlanNode;
    }

    /**
     * Finalize plan tree and create stream sink, if needed.
     */
    // 入参 StatementBase stmtBase：抽象语法树（AST）的声明基类，代表触发当前执行计划的原始 SQL 语句对象（例如 SelectStmt、InsertStmt 等）。
    // 在当前方法实现中，该参数保留用于拓展或兼容接口，实际函数体中未直接使用。
    // 在完成物理算子树的连接构建后进行收尾工作（Finalize），根据当前 Fragment 在分布式拓扑中的位置，自动创建对应的输出数据接收器（DataSink）。
    public void finalize(StatementBase stmtBase) {
        // 检查当前 Fragment 的 sink 是否已经被创建。
        if (sink != null) {
            return;
        }
        // 检查当前 Fragment 的 destNode（目标 ExchangeNode）是否存在
        // 若 destNode != null，说明当前 Fragment 不是根 Fragment，其计算生成的数据需要通过网络传输发送给下游 Fragment 的 ExchangeNode。
        if (destNode != null) {
            Preconditions.checkState(sink == null);
            // we're streaming to an exchange node
            // 实例化一个 DataStreamSink，传入下游目标 destNode 的唯一 ID（PlanNodeId）。BE 节点将根据该 ID 确定数据发送的目的地 Exchange 节点。
            DataStreamSink streamSink = new DataStreamSink(destNode.getId());
            // 将当前 Fragment 中配置的输出数据路由/分区规则（outputPartition，如 Hash 分区、Broadcast 广播、Random 随机等）绑定到 streamSink 上。
            streamSink.setOutputPartition(outputPartition);
            streamSink.setFragment(this);
            sink = streamSink;
        } else {
            // 进入 destNode == null 的分支。说明当前 Fragment 处于计划树的最顶层（即根 Fragment），其计算结果不需要发往下一个算子，而是直接向客户端返回。
            if (planRoot == null) {
                // only output expr, no FROM clause
                // "select 1 + 2"
                return;
            }
            Preconditions.checkState(sink == null);
            sink = new ResultSink(planRoot.getId(), resultSinkType);
        }
    }

    /**
     * Return the number of nodes on which the plan fragment will execute.
     * invalid: -1
     */
    public int getNumNodes() {
        return dataPartition == DataPartition.UNPARTITIONED ? 1 : planRoot.getNumNodes();
    }

    public int getParallelExecNum() {
        if (forceSingleInstance) {
            return 1;
        }
        return parallelExecNum;
    }
    // FE（Frontend）的 Planner 最终目的就是生成能够发送给 BE（Backend）执行的指令集。
    // toThrift() 汇集了当前 Fragment 内部的算子树（planRoot）、数据输出接收器（sink）、输入分区（partition）以及输出表达式（outputExprs），是 FE 到 BE 数据传输的关键桥梁。
    public TPlanFragment toThrift() {
        // 实例化 Thrift 结果对象
        // 在内存中创建一个空的 TPlanFragment Thrift 传输结构体对象，用于逐一填充序列化后的数据。
        TPlanFragment result = new TPlanFragment();
        // 序列化物理算子树（PlanNode Tree）
        // 以当前 planRoot 为起点递归地将整棵物理算子树（如 ScanNode、JoinNode、AggNode 等）转换为 Thrift 结构的 TPlan，并设置到 result 的 plan 字段中。
        // 这是整个 Fragment 执行算子逻辑的核心载体。
        if (planRoot != null) {
            result.setPlan(planRoot.treeToThrift());
        }
        // 序列化输出表达式列表（Output Expressions）
        // 将 Java 表达式列表（List<Expr>）批量递归转换为 Thrift 格式的表达式树列表（List<TExpr>），并设置给 result。BE 节点将根据这些表达式对最终计算出的结果行进行投影或转换。
        if (outputExprs != null) {
            result.setOutputExprs(ExprToThriftVisitor.treesToThrift(outputExprs));
        }
        // 序列化数据输出接收器（DataSink）
        // 将 DataSink（可能是用于跨节点传输的 DataStreamSink 或用于直接向 MySQL/Arrow 客户端返回结果的 ResultSink）转换为 TDataSink Thrift 对象并存入 result。它告诉 BE 节点当前 Fragment 计算完的数据应该发送给谁。
        if (sink != null) {
            result.setOutputSink(sink.toThrift());
        }
        // 确定并序列化输入数据分区（Data Partition）
        // 若没有特殊指定的 dataPartitionForThrift，则直接将 Planner 计算出的标准输入数据分区策略 dataPartition（如 UNPARTITIONED、HASH_PARTITIONED 等）转换为 TPartition 并填充入 result。
        if (dataPartitionForThrift == null) {
            result.setPartition(dataPartition.toThrift());
        } else {
            result.setPartition(dataPartitionForThrift.toThrift());
        }

        // TODO chenhao , calculated by cost
        // 填充内存保留相关的硬编码占位参数
        // 显式设置当前 Fragment 最小要求的内存预留字节数为 0。
        result.setMinReservationBytes(0);
        // 显式设置初始请求的总预留内存字节数为 0。
        result.setInitialReservationTotalClaims(0);
        return result;
    }

    public String getExplainString(TExplainLevel explainLevel) {
        StringBuilder str = new StringBuilder();
        Preconditions.checkState(dataPartition != null);
        if (CollectionUtils.isNotEmpty(outputExprs)) {
            str.append("  OUTPUT EXPRS:\n    ");
            str.append(outputExprs.stream()
                    .map(e -> e.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE))
                    .collect(Collectors.joining("\n    ")));
        }
        str.append("\n");
        str.append("  PARTITION: " + dataPartition.getExplainString(explainLevel) + "\n");
        str.append("  HAS_COLO_PLAN_NODE: " + hasColocatePlanNode + "\n");
        if (queryCacheParam != null) {
            str.append("\n");
            str.append("  QUERY_CACHE:\n");
            str.append("    CACHE_NODE_ID: " + queryCacheParam.getNodeId() + "\n");
            str.append("    DIGEST: " + Hex.encodeHexString(queryCacheParam.getDigest()) + "\n");
        }

        str.append("\n");
        if (sink != null) {
            str.append(sink.getExplainString("  ", explainLevel) + "\n");
        }
        if (planRoot != null) {
            str.append(planRoot.getExplainString("  ", "  ", explainLevel));
        }
        return str.toString();
    }

    public void getExplainStringMap(Map<Integer, String> planNodeMap) {
        org.apache.doris.thrift.TExplainLevel explainLevel = org.apache.doris.thrift.TExplainLevel.NORMAL;
        if (planRoot != null) {
            planRoot.getExplainStringMap(explainLevel, planNodeMap);
        }
    }

    /**
     * Returns true if this fragment is partitioned.
     */
    public boolean isPartitioned() {
        return (dataPartition.getType() != TPartitionType.UNPARTITIONED);
    }

    public void updateDataPartition(DataPartition dataPartition) {
        if (this.dataPartition == DataPartition.UNPARTITIONED) {
            return;
        }
        this.dataPartition = dataPartition;
    }

    public PlanFragmentId getId() {
        return fragmentId;
    }

    public ExchangeNode getDestNode() {
        return destNode;
    }

    public PlanNode getDeepestLinearSource() {
        if (getChildren().size() > 1) {
            throw new IllegalStateException("getDeepestLinearSource() called on a fragment with multiple children");
        } else if (getChildren().isEmpty()) {
            // this is the root fragment
            return getPlanRoot();
        } else {
            // this is a non-root fragment
            return getChild(0).getDeepestLinearSource();
        }
    }

    public PlanFragment getDestFragment() {
        if (destNode == null) {
            return null;
        }
        return destNode.getFragment();
    }
    // 入参 ExchangeNode destNode：目标数据接收节点（即下游 Fragment 中负责接收网络数据流的 ExchangeNode 实例）。
    public void setDestination(ExchangeNode destNode) {
        // 将传入的下游接收节点 destNode 赋值给当前 PlanFragment 的成员变量 this.destNode。
        this.destNode = destNode;
        // 获取下游 Fragment 实例
        PlanFragment dest = getDestFragment();
        Preconditions.checkNotNull(dest);
        // 建立 Fragment 树的双向拓扑关系
        // 调用下游 Fragment（dest）的 addChild 方法（继承自 TreeNode<PlanFragment>），将当前 PlanFragment（this）添加为下游 Fragment 的子节点。
        dest.addChild(this);
    }

    public DataPartition getDataPartition() {
        return dataPartition;
    }

    public DataPartition getOutputPartition() {
        return outputPartition;
    }

    public void setOutputPartition(DataPartition outputPartition) {
        this.outputPartition = outputPartition;
    }

    public PlanNode getPlanRoot() {
        return planRoot;
    }

    public void setPlanRoot(PlanNode root) {
        planRoot = root;
        setFragmentInPlanTree(planRoot);
    }

    /**
     * Adds a node as the new root to the plan tree. Connects the existing
     * root as the child of newRoot.
     */
    public void addPlanRoot(PlanNode newRoot) {
        Preconditions.checkState(newRoot.getChildren().size() == 1);
        newRoot.setChild(0, planRoot);
        planRoot = newRoot;
        planRoot.setFragment(this);
    }

    public DataSink getSink() {
        return sink;
    }

    public void setSink(DataSink sink) {
        Preconditions.checkState(this.sink == null);
        Preconditions.checkNotNull(sink);
        sink.setFragment(this);
        this.sink = sink;
    }

    public PlanFragmentId getFragmentId() {
        return fragmentId;
    }

    public Set<RuntimeFilterId> getBuilderRuntimeFilterIds() {
        return builderRuntimeFilterIds;
    }

    public Set<RuntimeFilterId> getTargetRuntimeFilterIds() {
        return targetRuntimeFilterIds;
    }

    public boolean isTransferQueryStatisticsWithEveryBatch() {
        return transferQueryStatisticsWithEveryBatch;
    }

    public int getFragmentSequenceNum() {
        return fragmentSequenceNum;
    }

    public void setFragmentSequenceNum(int seq) {
        fragmentSequenceNum = seq;
    }

    public int getBucketNum() {
        return bucketNum;
    }

    public void setBucketNum(int bucketNum) {
        this.bucketNum = bucketNum;
    }

    public boolean hasNullAwareLeftAntiJoin() {
        return planRoot.anyMatch(plan -> plan instanceof JoinNodeBase
                && ((JoinNodeBase) plan).getJoinOp() == JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN);
    }

    public boolean useSerialSource(ConnectContext context) {
        return context != null
                && context.getSessionVariable().isIgnoreStorageDataDistribution()
                && queryCacheParam == null
                && !hasNullAwareLeftAntiJoin()
                // If planRoot is not a serial operator and has serial children, we can use serial source and improve
                // parallelism of non-serial operators.
                // For bucket shuffle / colocate join fragment, always use serial source if the bucket scan nodes are
                // serial.
                && (hasSerialScanNode() || (sink instanceof DataStreamSink && !planRoot.isSerialNode()
                && planRoot.hasSerialChildren()));
    }

    public boolean hasSerialScanNode() {
        return planRoot.hasSerialScanChildren();
    }

    public void setForceSingleInstance() {
        this.forceSingleInstance = true;
    }
}
