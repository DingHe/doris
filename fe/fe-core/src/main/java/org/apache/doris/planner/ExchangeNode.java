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

import org.apache.doris.analysis.SortInfo;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeType;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeTypeRequire;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TExchangeNode;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPartitionType;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;

import java.util.Collections;

/**
 * Receiver side of a 1:n data stream. Logically, an ExchangeNode consumes the data
 * produced by its children. For each of the sending child nodes the actual data
 * transmission is performed by the DataStreamSink of the PlanFragment housing
 * that child node. Typically, an ExchangeNode only has a single sender child but,
 * e.g., for distributed union queries an ExchangeNode may have one sender child per
 * union operand.
 *
 * If a (optional) SortInfo field is set, the ExchangeNode will merge its
 * inputs on the parameters specified in the SortInfo object. It is assumed that the
 * inputs are also sorted individually on the same SortInfo parameter.
 */
// ExchangeNode 是 Apache Doris 在前端 (FE) 物理查询计划层中的一个核心数据接收算子，继承自 PlanNode。
// 跨节点数据交换的接收端（Receiver Side）：在分布式执行计划中，查询会被划分为多个 PlanFragment。当一个 Fragment 需要消费上游 Fragment 算出的数据时，该 Fragment 的算子树底部就会插入一个 ExchangeNode 作为数据接收入口（上游数据由上游 Fragment 的 DataStreamSink 发送）。
// 多流合并与归并排序（Merging Exchange）：如果上游数据在发送前已经按特定列排好序，ExchangeNode 能够配置 SortInfo 将多路有序数据流进行多路归并排序（Merge Sort），保证输出给上层算的数据是全局有序的。
// 维护数据 Shuffle/分区属性：记录数据流到达当前 Fragment 时的分区类型（如 HASH_PARTITIONED、UNPARTITIONED、BUCKET_SHFFULE_HASH_PARTITIONED），指导管道（Pipeline）引擎调度与 Local Exchange 优化。
public class ExchangeNode extends PlanNode {
    // 普通数据交换节点的名称标识，用于执行计划打印和日志显示。
    public static final String EXCHANGE_NODE = "EXCHANGE";
    // 带归并排序功能的数据交换节点名称标识。
    public static final String MERGING_EXCHANGE_NODE = "MERGING-EXCHANGE";

    // The parameters based on which sorted input streams are merged by this
    // exchange node. Null if this exchange does not merge sorted streams
    // 归并排序的元数据信息。如果为空，说明只是普通的数据接收；若非空，包含排序列、排序方向等参数，节点会将多路有序流归并合并。
    private SortInfo mergeInfo;
    // 标记当前 ExchangeNode 是否作为 Broadcast Hash Join 的右子节点（Build 端/广播端）。用于指导 BE 侧资源的分配与特定优化策略。
    private boolean isRightChildOfBroadcastHashJoin = false;
    // 标识上游发送到当前 ExchangeNode 的数据分区类型（例如 UNPARTITIONED 单流/广播、HASH_PARTITIONED 哈希分片、BUCKET_SHFFULE_HASH_PARTITIONED 桶 Shuffle 等）。
    private TPartitionType partitionType;

    /**
     * use for Nereids only.
     */
    // 专供 Nereids 新优化器使用的构造方法
    // 调用父类 PlanNode 构造方法，指定节点 ID 并设置初始名称为 EXCHANGE
    // 将 offset 初始化为 0，limit 初始化为 -1（无限制）
    public ExchangeNode(PlanNodeId id, PlanNode inputNode) {
        super(id, inputNode, EXCHANGE_NODE);
        offset = 0;
        limit = -1;
        // 将条件谓词列表 conjuncts 初始化为空列表（ExchangeNode 不作过滤）
        this.conjuncts = Collections.emptyList();
        // 将 inputNode 添加为子节点，并调用 updateTupleIds() 根据输入节点的输出 Tuple 描述符更新当前节点的 tupleIds。
        children.add(inputNode);
        TupleDescriptor outputTupleDesc = inputNode.getOutputTupleDesc();
        updateTupleIds(outputTupleDesc);
    }
    // 获取上游数据发送到此 ExchangeNode 的分区类型 partitionType。
    public TPartitionType getPartitionType() {
        return partitionType;
    }
    // 设置数据分区类型 partitionType。
    public void setPartitionType(TPartitionType partitionType) {
        this.partitionType = partitionType;
    }
    // 更新当前 ExchangeNode 输出的 Tuple ID 列表
    // 如果 outputTupleDesc 非空，清空原有 tupleIds 并添加其 ID；否则将子节点（输入算子）的 outputTupleIds 拷贝过来。
    public void updateTupleIds(TupleDescriptor outputTupleDesc) {
        if (outputTupleDesc != null) {
            clearTupleIds();
            tupleIds.add(outputTupleDesc.getId());
        } else {
            clearTupleIds();
            tupleIds.addAll(getChild(0).getOutputTupleIds());
        }
    }

    /**
     * Set the parameters used to merge sorted input streams. This can be called
     * after init().
     */
    // 设置归并排序参数，将节点升级为 Merging Exchange。
    // 将传入的 info 赋值给 this.mergeInfo，同时将节点的打印名称修改为 "V" + MERGING_EXCHANGE_NODE（如 VEXCHANGE 或 VMERGING-EXCHANGE）。
    public void setMergeInfo(SortInfo info) {
        this.mergeInfo = info;
        this.planNodeName =  "V" + MERGING_EXCHANGE_NODE;
    }
    // 将 Java 端的 ExchangeNode 物理节点属性序列化为 ThriftRPC 结构体 TPlanNode，下发给 BE 节点。
    @Override
    protected void toThrift(TPlanNode msg) {
        msg.setIsSerialOperator(isSerialOperatorOnBe(ConnectContext.get()));
        msg.node_type = TPlanNodeType.EXCHANGE_NODE;
        msg.exchange_node = new TExchangeNode();
        for (TupleId tid : tupleIds) {
            msg.exchange_node.addToInputRowTuples(tid.asInt());
        }
        if (mergeInfo != null) {
            msg.exchange_node.setSortInfo(mergeInfo.toThrift());
        }
        msg.exchange_node.setOffset(offset);
        msg.exchange_node.setPartitionType(partitionType);
    }
    // 获取当前节点的并行实例数。直接返回成员变量 numInstances。
    @Override
    public int getNumInstances() {
        return numInstances;
    }
    // 生成 EXPLAIN 执行计划树时，属于该节点特有的展示文本。
    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        return prefix + "offset: " + offset + "\n";
    }
    // 查询当前算子是否是 Broadcast Hash Join 的右子节点。
    public boolean isRightChildOfBroadcastHashJoin() {
        return isRightChildOfBroadcastHashJoin;
    }
    // 设置当前算子是否是 Broadcast Hash Join 的右子节点。
    public void setRightChildOfBroadcastHashJoin(boolean value) {
        isRightChildOfBroadcastHashJoin = value;
    }

    /**
     * If table `t1` has unique key `k1` and value column `v1`.
     * Now use plan below to load data into `t1`:
     * ```
     * FRAGMENT 0:
     *  Merging Exchange (id = 1)
     *   NL Join (id = 2)
     *  DataStreamSender (id = 3, dst_id = 3) (OLAP_TABLE_SINK_HASH_PARTITIONED)
     *
     * FRAGMENT 1:
     *  Exchange (id = 3)
     *  OlapTableSink (id = 4) ```
     *
     * In this plan, `Exchange (id = 1)` needs to do merge sort using column `k1` and `v1` so parallelism
     * of FRAGMENT 0 must be 1 and data will be shuffled to FRAGMENT 1 which also has only 1 instance
     * because this loading job relies on the global ordering of column `k1` and `v1`.
     *
     * So FRAGMENT 0 should not use serial source.
     *
     * Important: this method does NOT call fragment.useSerialSource() — that path would
     * recurse into hasNullAwareLeftAntiJoin and walk the entire plan tree, and was
     * previously found to blow the stack on deep plans.  The fragment-level gating is
     * applied in {@link #isSerialOperatorOnBe} instead.
     */
    // 判断当前 ExchangeNode 自身逻辑上是否需要串行（单并行度）执行
    // 若启用了 isUseSerialExchange 配置或分区类型为 UNPARTITIONED（未分区单流），且没有设置归并排序（mergeInfo == null），则判定为串行节点。
    @Override
    public boolean isSerialNode() {
        return (ConnectContext.get() != null && ConnectContext.get().getSessionVariable().isUseSerialExchange()
                || partitionType == TPartitionType.UNPARTITIONED) && mergeInfo == null;
    }
    // 判断在 BE 端该 Exchange 算子是否最终以单线程（串行）模式运行。
    // 若开启了 FE Local Shuffle 优化器（isEnableLocalShufflePlanner）：打断扫描算子的串行标志传递，仅在 Fragment 使用串行源且自身是串行节点时返回 true
    // 默认情况：当前 Fragment 非空，且（自身是串行节点 OR Fragment 包含串行 ScanNode）且 Fragment 确认使用串行源时，返回 true。
    @Override
    public boolean isSerialOperatorOnBe(ConnectContext context) {
        if (context != null && context.getSessionVariable().isEnableLocalShufflePlanner()) {
            // When FE local shuffle planner is on, decouple exchange from scan's serial flag.
            // Scan pooling is handled by LE(PT) after scan; exchange keeps its own parallelism.
            return fragment != null
                    && isSerialNode()
                    && fragment.useSerialSource(context);
        }
        return fragment != null
                && (isSerialNode() || fragment.hasSerialScanNode())
                && fragment.useSerialSource(context);
    }
    // 判断当前节点是否有串行的子算子。直接返回 isSerialNode() 的评估结果。
    @Override
    public boolean hasSerialChildren() {
        return isSerialNode();
    }
    // 判断当前算子下方是否直接包含串行扫描算子（ScanNode）。ExchangeNode 作为跨 Fragment 屏障，始终返回 false。
    @Override
    public boolean hasSerialScanChildren() {
        return false;
    }
    // 在将逻辑计划翻译为 Pipeline 执行计划时，推导并强加本地数据 Shuffle（Local Exchange）类型。
    // 在从 Nereids 逻辑计划翻译为后端 Pipeline 物理执行计划时，根据 partitionType 决定是否需要在当前 BE 节点内部追加 LocalExchange（本地线程间的数据打散/重新分片）。
    // 决定了数据从网络层到达 BE 节点后，如何在当前 BE 的多个 CPU 线程（Pipeline Task）之间分配数据（例如推导为 GLOBAL_EXECUTION_HASH_SHUFFLE 或 BUCKET_HASH_SHUFFLE），是充分发挥多核并发性能的关键。
    @Override
    public Pair<PlanNode, LocalExchangeType> enforceAndDeriveLocalExchange(PlanTranslatorContext translatorContext,
            PlanNode parent, LocalExchangeTypeRequire parentRequire) {
        // Report actual distribution. Serial handling is done by the framework
        // (enforceRequire step 2.5 overrides serial child output to NOOP).
        // 若 partitionType 为 HASH_PARTITIONED，返回 GLOBAL_EXECUTION_HASH_SHUFFLE。
        if (partitionType == TPartitionType.HASH_PARTITIONED) {
            return Pair.of(this, LocalExchangeType.GLOBAL_EXECUTION_HASH_SHUFFLE);
        // 若 partitionType 为 BUCKET_SHFFULE_HASH_PARTITIONED，返回 BUCKET_HASH_SHUFFLE。
        } else if (partitionType == TPartitionType.BUCKET_SHFFULE_HASH_PARTITIONED) {
            return Pair.of(this, LocalExchangeType.BUCKET_HASH_SHUFFLE);
        }
        return Pair.of(this, LocalExchangeType.NOOP);
    }
}
