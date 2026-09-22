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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/ExchangeNode.java
// and modified by Doris

package org.apache.doris.planner;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprToThriftVisitor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TLocalExchangeNode;
import org.apache.doris.thrift.TLocalPartitionType;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** LocalExchangeNode */
// LocalExchangeNode 是 Apache Doris 前端 (FE) 物理执行计划层中用于表示 单节点内部（Intra-Node / Local）数据重新分发与线程间 Exchange 的算子节点，继承自 PlanNode。
// 与负责 RPC 跨节点数据传输的 ExchangeNode 不同，LocalExchangeNode 作用于 单个 BE 进程内部 的 Pipeline 引擎中：
// 单节点线程间数据重分发（Local Shuffle）：在 Pipeline 执行引擎中，一个 Fragment 会被拆分为多个 Pipeline Task（并发线程）。LocalExchangeNode 负责在同一个 BE 内部的不同 Task 之间打散、重分发或汇聚数据流，以满足算子的数据分布要求（如 HashJoin 查找表构建、聚合操作按 Key 汇总等）。
// 多核并发扩展（Fan-out / Expansion）：当上游算子（如串行 Scan 线程或受限输入）并发度较低时，通过插入 Passthrough 或 Adaptive Passthrough 类型的 LocalExchangeNode，将单线程数据流分发到多个下游并发线程中，避免线程瓶颈并榨干多核性能。
// 数据倾斜与线程平衡：提供多种本地 Exchange 策略（如轮询直通 Passthrough、自适应直通 Adaptive Passthrough、本地 Hash Shuffle、广播 Broadcast、汇聚Pass To One 等），优化本地线程间的数据分布状态。
// 与规划器协同（LocalExchangeTypeRequire 接口体系）：定义了父算子对子算子本地数据分布的要求，在逻辑计划向 Pipeline 物理计划翻译（enforceAndDeriveLocalExchange 机制）过程中，精确决定是否需要自动插入 LocalExchangeNode。
public class LocalExchangeNode extends PlanNode {
    // 表示该算子的名称标识，主要用于 EXPLAIN 物理执行计划文本展示和日志打印。
    public static final String EXCHANGE_NODE = "LOCAL-EXCHANGE";
    // 记录当前 LocalExchangeNode 采用的本地 Exchange 类型（如 Hash Shuffle、Passthrough、Broadcast 等），用于指导 BE 侧构建对应的本地数据管道（Local Data Stream Sink/Source）。
    private LocalExchangeType exchangeType;

    /**
     * use for Nereids only.
     */
    // 面向 Nereids 优化器使用的快捷构造函数。
    public LocalExchangeNode(PlanNodeId id, PlanNode inputNode, LocalExchangeType exchangeType) {
        this(id, inputNode, exchangeType, null);
    }
    // 主构造函数，负责完全初始化一个 LocalExchangeNode 实例。
    public LocalExchangeNode(PlanNodeId id, PlanNode inputNode, LocalExchangeType exchangeType,
            List<Expr> distributeExprs) {
        super(id, inputNode, EXCHANGE_NODE);
        // 将 offset 初始化为 0，limit 初始化为 -1（不作数据拦截与偏移）
        this.offset = 0;
        this.limit = -1;
        // 将条件谓词 conjuncts 赋值为空列表（不进行任何数据过滤）。
        this.conjuncts = Collections.emptyList();
        // 将 inputNode 记为当前节点的子节点（children.add(inputNode)）。
        this.children.add(inputNode);
        this.exchangeType = exchangeType;
        // 将当前节点归属的 fragment 设为输入节点的 fragment（属于同一 Fragment）。
        this.fragment = inputNode.getFragment();
        // 判断 exchangeType 是否为 Hash 类的 Shuffle 类型（isHashShuffle）。如果是且传入的 distributeExprs 非空，则调用继承自 PlanNode 的 setDistributeExprLists() 保存 Hash 分布表达式。
        List<Expr> hashExprs = distributeExprs;
        boolean isHashShuffle = (exchangeType == LocalExchangeType.BUCKET_HASH_SHUFFLE
                || exchangeType == LocalExchangeType.LOCAL_EXECUTION_HASH_SHUFFLE
                || exchangeType == LocalExchangeType.GLOBAL_EXECUTION_HASH_SHUFFLE);
        if (isHashShuffle && hashExprs != null && !hashExprs.isEmpty()) {
            setDistributeExprLists(hashExprs);
        }
        // 获取子节点的输出 Tuple 描述符并调用 updateTupleIds() 继承/同步元数据。
        TupleDescriptor outputTupleDesc = inputNode.getOutputTupleDesc();
        updateTupleIds(outputTupleDesc);
    }

    public void updateTupleIds(TupleDescriptor outputTupleDesc) {
        if (outputTupleDesc != null) {
            clearTupleIds();
            tupleIds.add(outputTupleDesc.getId());
        } else {
            clearTupleIds();
            tupleIds.addAll(getChild(0).getOutputTupleIds());
        }
    }
    // 将 FE 层的物理节点序列化为 Thrift RPC 结构体 TPlanNode，下发给 BE。
    @Override
    protected void toThrift(TPlanNode msg) {
        // FE-planned LocalExchangeNode itself must stay non-serial. In the BE-planned path,
        // the serial semantics belong to the upstream scan/exchange pipeline rather than the
        // downstream LocalExchangeSource pipeline. Marking LocalExchangeNode as serial would
        // incorrectly reduce the downstream pipeline's task count to 1.
        msg.setIsSerialOperator(false);

        msg.node_type = TPlanNodeType.LOCAL_EXCHANGE_NODE;
        msg.local_exchange_node = new TLocalExchangeNode();
        msg.local_exchange_node.setPartitionType(exchangeType.toThrift());

        if (exchangeType.isHashShuffle()) {
            List<TExpr> thriftDistributeExprLists = new ArrayList<>();
            for (Expr expr : distributeExprLists()) {
                thriftDistributeExprLists.add(ExprToThriftVisitor.treeToThrift(expr));
            }
            msg.local_exchange_node.setDistributeExprLists(thriftDistributeExprLists);
        }
    }
    // 用于获取 Hash 分发表达式列表。
    private List<Expr> distributeExprLists() {
        if (distributeExprLists == null) {
            return Collections.emptyList();
        }
        return distributeExprLists;
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        return prefix + "type: " + exchangeType.name() + "\n";
    }

    public LocalExchangeType getExchangeType() {
        return exchangeType;
    }
    // 控制串行标志（Serial Flag）的向上传递与重置行为
    // 始终返回 true。表明 LocalExchangeNode 会截断并重置子节点传上来的串行标志，因为 LocalExchange 本身就是为了将上游可能单线程/串行的数据流打散并重新并行化给下游算子。
    @Override
    protected boolean shouldResetSerialFlagForChild(int childIndex) {
        return true;
    }

    /**
     * Describes what a parent operator demands of its child's output distribution.
     * Returned by the parent in {@code enforceAndDeriveLocalExchange} and consumed by
     * {@link PlanNode#enforceRequire}, which decides whether to insert a LocalExchangeNode.
     *
     * <p>How to pick the right require when overriding {@code enforceAndDeriveLocalExchange}:
     * <ul>
     *   <li>{@link NoRequire} — "I don't care about the child's distribution".  Use for
     *     operators whose correctness doesn't depend on partitioning (e.g. base default,
     *     limit, select).  The framework still upgrades this to {@code requirePassthrough}
     *     automatically when the child turns out to be serial — see
     *     {@link PlanNode#enforceRequire} step 3.</li>
     *
     *   <li>{@link RequireHash} (via {@code requireHash()}) — "I need hash-partitioned
     *     input, any flavour of hash will do".  Accepts {@code GLOBAL_EXECUTION_HASH_SHUFFLE},
     *     {@code LOCAL_EXECUTION_HASH_SHUFFLE}, and {@code BUCKET_HASH_SHUFFLE}.  This is
     *     the right choice for shuffled correctness consumers (finalize AggSink with keys,
     *     partitioned HashJoin, Intersect, Except) — the upstream may already provide a
     *     compatible flavour and we shouldn't insert a redundant exchange.</li>
     *
     *   <li>{@link RequireSpecific} (via {@code requirePassthrough()},
     *     {@code requireBroadcast()}, {@code requireBucketHash()},
     *     {@code requireGlobalExecutionHash()}, etc.) — "I need exactly this exchange type".
     *     Use only when the operator's correctness or efficiency hinges on that exact
     *     type (e.g. NLJ probe wants ADAPTIVE_PASSTHROUGH; BucketShuffle join build wants
     *     BUCKET_HASH_SHUFFLE).  Note: PASSTHROUGH is satisfied by ADAPTIVE_PASSTHROUGH
     *     (superset), but other specific types require exact match.</li>
     * </ul>
     *
     * <p>Rule of thumb: prefer {@code requireHash()} over
     * {@code requireSpecific(GLOBAL_EXECUTION_HASH_SHUFFLE)} unless you genuinely need to
     * reject other hash flavours.  RequireSpecific is fragile because the upstream may
     * legitimately output a different (still correct) hash type.
     */
    // 描述父算子在进行 Pipeline 物理计划翻译时，对其子算子本地数据分布的要求。
    public interface LocalExchangeTypeRequire {
        // 判断子节点或当前上游提供的 Exchange 类型 provide 是否能满足当前要求。
        boolean satisfy(LocalExchangeType provide);
        // 当不满足要求需要强制插入 LocalExchangeNode 时，返回首选的 LocalExchangeType。
        LocalExchangeType preferType();
        // 默认实现，直接返回 RequireHash.INSTANCE（自动将要求提升为 Hash 要求）。
        default LocalExchangeTypeRequire autoRequireHash() {
            return RequireHash.INSTANCE;
        }
        // 获取不带任何要求（NoRequire）的单例。
        static NoRequire noRequire() {
            return NoRequire.INSTANCE;
        }
        // 获取任意 Hash 要求的单例（RequireHash）。
        static RequireHash requireHash() {
            return RequireHash.INSTANCE;
        }
        // 创建对 PASSTHROUGH 的特定要求。
        static RequireSpecific requirePassthrough() {
            return requireSpecific(LocalExchangeType.PASSTHROUGH);
        }
        // 创建对 PASS_TO_ONE 的特定要求。
        static RequireSpecific requirePassToOne() {
            return requireSpecific(LocalExchangeType.PASS_TO_ONE);
        }
        // 创建对 BROADCAST 的特定要求。
        static RequireSpecific requireBroadcast() {
            return requireSpecific(LocalExchangeType.BROADCAST);
        }
        // 创建对 ADAPTIVE_PASSTHROUGH 的特定要求。
        static RequireSpecific requireAdaptivePassthrough() {
            return requireSpecific(LocalExchangeType.ADAPTIVE_PASSTHROUGH);
        }
        // 创建对 BUCKET_HASH_SHUFFLE 的特定要求。
        static RequireSpecific requireBucketHash() {
            return requireSpecific(LocalExchangeType.BUCKET_HASH_SHUFFLE);
        }
        // 创建对 GLOBAL_EXECUTION_HASH_SHUFFLE 的特定要求。
        static RequireSpecific requireGlobalExecutionHash() {
            return requireSpecific(LocalExchangeType.GLOBAL_EXECUTION_HASH_SHUFFLE);
        }
        // 包裹一个指定 LocalExchangeType 的特定要求对象。
        static RequireSpecific requireSpecific(LocalExchangeType require) {
            return new RequireSpecific(require);
        }
        // 如果首选类型是 NOOP（无需 Exchange），则返回 defaultType；否则返回首选类型。
        default LocalExchangeType noopTo(LocalExchangeType defaultType) {
            LocalExchangeType preferType = preferType();
            return (preferType == LocalExchangeType.NOOP) ? defaultType : preferType;
        }
    }

    /** NoRequire */
    // 代表父节点对子节点的数据分布无任何要求（如 Limit、Select 等对数据分布不敏感的算子）
    public static class NoRequire implements LocalExchangeTypeRequire {
        public static final NoRequire INSTANCE = new NoRequire();
        // 始终返回 true（任何数据分布都可以满足）
        @Override
        public boolean satisfy(LocalExchangeType provide) {
            return true;
        }
        // 返回 LocalExchangeType.NOOP（不需要插入 Exchange）。
        @Override
        public LocalExchangeType preferType() {
            return LocalExchangeType.NOOP;
        }
    }

    /** RequireHash */
    // 代表父节点需要 Hash 分区的输入数据，但接受任何形式的 Hash Shuffle（如聚合算子、Hash Join 等）
    public static class RequireHash implements LocalExchangeTypeRequire {
        public static final RequireHash INSTANCE = new RequireHash();

        @Override
        public boolean satisfy(LocalExchangeType provide) {
            // 如果上游提供的是 GLOBAL_EXECUTION_HASH_SHUFFLE、LOCAL_EXECUTION_HASH_SHUFFLE 或 BUCKET_HASH_SHUFFLE 中的任意一种，即返回 true；否则返回 false。
            switch (provide) {
                case GLOBAL_EXECUTION_HASH_SHUFFLE:
                case LOCAL_EXECUTION_HASH_SHUFFLE:
                case BUCKET_HASH_SHUFFLE:
                    return true;
                default:
                    return false;
            }
        }
        // 默认返回 GLOBAL_EXECUTION_HASH_SHUFFLE（最通用、安全的默认 Hash 类型）。
        @Override
        public LocalExchangeType preferType() {
            // GLOBAL is the safe abstract default for a generic "any hash" requirement: it is the
            // unconditionally-valid hash partition (full cross-backend redistribution). LOCAL only
            // rebalances within a backend, so it is correct only when each key's rows are already
            // backend-local — a precondition. AddLocalExchange.resolveExchangeType() deliberately
            // specializes RequireHash to LOCAL_EXECUTION_HASH_SHUFFLE for FE-planned intra-fragment
            // exchanges (where that precondition holds and GLOBAL's shuffle_idx_to_instance_idx may be
            // empty); that override is scoped to that path, so the default here stays GLOBAL.
            return LocalExchangeType.GLOBAL_EXECUTION_HASH_SHUFFLE;
        }

        @Override
        public LocalExchangeTypeRequire autoRequireHash() {
            return this;
        }
    }
    // 代表父节点严格要求子节点提供特定的某种 Exchange 类型。
    public static class RequireSpecific implements LocalExchangeTypeRequire {
        // 保存要求的具体 LocalExchangeType
        LocalExchangeType requireType;

        public RequireSpecific(LocalExchangeType requireType) {
            this.requireType = requireType;
        }

        @Override
        public boolean satisfy(LocalExchangeType provide) {
            if (requireType == provide) {
                return true;
            }
            // ADAPTIVE_PASSTHROUGH is a superset of PASSTHROUGH — both fan out data
            // from fewer to more tasks. BE's need_to_local_exchange treats them as
            // compatible, so ADAPTIVE_PASSTHROUGH satisfies a PASSTHROUGH requirement.
            // 如果要求的是 PASSTHROUGH 且上游提供的是 ADAPTIVE_PASSTHROUGH，因为后者是前者的超集（均支持扇出分发），判定为满足并返回 true。
            if (requireType == LocalExchangeType.PASSTHROUGH
                    && provide == LocalExchangeType.ADAPTIVE_PASSTHROUGH) {
                return true;
            }
            return false;
        }

        @Override
        public LocalExchangeType preferType() {
            return requireType;
        }

        @Override
        public LocalExchangeTypeRequire autoRequireHash() {
            // Callers are pass-through operators (union / streaming agg / sort) that report
            // resolveExchangeType(requireChild) upward while leaving row placement to their
            // children. A specific hash require must therefore be forwarded as-is: degrading
            // LOCAL_EXECUTION_HASH_SHUFFLE to the generic RequireHash lets a bucket-distributed
            // child satisfy the requirement and keep its bucket placement, while the operator
            // still claims LOCAL_EXECUTION_HASH_SHUFFLE to its parent — the parent (e.g. a
            // bucket join upgraded to local hash) then skips its re-align local exchange and
            // the mixed placements compute wrong results.
            if (requireType.isHashShuffle()) {
                return this;
            }
            return RequireHash.INSTANCE;
        }
    }
    // 定义了所有支持的本地数据 Exchange 模式，并提供枚举属性与转换逻辑：
    public enum LocalExchangeType {
        // 无操作（不进行任何本地 Exchange）
        NOOP,
        //全局级别的 Hash Shuffle（在当前 BE 的所有线程间基于 Hash 键做全面打散重分布）。
        GLOBAL_EXECUTION_HASH_SHUFFLE,
        // 局部级别的 Hash Shuffle（在当前 Pipeline 内部的局部线程间打散）。
        LOCAL_EXECUTION_HASH_SHUFFLE,
        // 针对 Bucket 数据分布优化的本地 Hash Shuffle。
        BUCKET_HASH_SHUFFLE,
        // 轮询/直通分发（轮流将数据行发送给各个下游 Task，用于负载均衡）。
        PASSTHROUGH,
        // 自适应直通（基于队列拥塞状态动态决定分发给哪个下游 Task）。
        ADAPTIVE_PASSTHROUGH,
        // 本地广播（将全量数据复制并广播给当前 BE 上的所有并发 Task，常用于 Broadcast Join）。
        BROADCAST,
        // 汇聚到单线程（将多线程的数据汇总交由单个并发 Task 串行处理）。
        PASS_TO_ONE,
        // 本地归并排序（多路有序数据流在单节点内部进行多路归并）。
        LOCAL_MERGE_SORT;

        public boolean isHashShuffle() {
            switch (this) {
                case GLOBAL_EXECUTION_HASH_SHUFFLE:
                case LOCAL_EXECUTION_HASH_SHUFFLE:
                case BUCKET_HASH_SHUFFLE:
                    return true;
                default:
                    return false;
            }
        }

        // Mirrors BE Pipeline::heavy_operations_on_the_sink():
        // HASH_SHUFFLE, BUCKET_HASH_SHUFFLE, and ADAPTIVE_PASSTHROUGH perform
        // heavy computation on the sink side. When the upstream pipeline has only
        // 1 task (serial/pooling scan), a PASSTHROUGH fan-out must be inserted
        // before these exchanges to avoid a single-task bottleneck.
        public boolean isHeavyOperation() {
            switch (this) {
                case GLOBAL_EXECUTION_HASH_SHUFFLE:
                case LOCAL_EXECUTION_HASH_SHUFFLE:
                case BUCKET_HASH_SHUFFLE:
                case ADAPTIVE_PASSTHROUGH:
                    return true;
                default:
                    return false;
            }
        }

        public TLocalPartitionType toThrift() {
            switch (this) {
                case GLOBAL_EXECUTION_HASH_SHUFFLE:
                    return TLocalPartitionType.GLOBAL_EXECUTION_HASH_SHUFFLE;
                case LOCAL_EXECUTION_HASH_SHUFFLE:
                    return TLocalPartitionType.LOCAL_EXECUTION_HASH_SHUFFLE;
                case BUCKET_HASH_SHUFFLE:
                    return TLocalPartitionType.BUCKET_HASH_SHUFFLE;
                case PASSTHROUGH:
                    return TLocalPartitionType.PASSTHROUGH;
                case ADAPTIVE_PASSTHROUGH:
                    return TLocalPartitionType.ADAPTIVE_PASSTHROUGH;
                case BROADCAST:
                    return TLocalPartitionType.BROADCAST;
                case PASS_TO_ONE:
                    return TLocalPartitionType.PASS_TO_ONE;
                case LOCAL_MERGE_SORT:
                    return TLocalPartitionType.LOCAL_MERGE_SORT;
                default: {
                    throw new IllegalStateException("Unsupported LocalExchangeType: " + this);
                }
            }
        }
    }
}
