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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/DataStreamSink.java
// and modified by Doris

package org.apache.doris.planner;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprToSqlVisitor;
import org.apache.doris.analysis.ExprToThriftVisitor;
import org.apache.doris.analysis.ToSqlParams;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TDataStreamSink;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TOlapTableLocationParam;
import org.apache.doris.thrift.TOlapTablePartitionParam;
import org.apache.doris.thrift.TOlapTableSchemaParam;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Data sink that forwards data to an exchange node.
 */
// DataStreamSink 是 Apache Doris 前端 (FE) 物理执行计划层中继承自 DataSink 的最核心具体实现类之一。
// 在分布式查询执行中，一个完整 SQL 的物理计划会被分割为多个 PlanFragment。DataStreamSink 的核心作用就是作为跨 Fragment 数据传输的发送端组件：
// 跨 Fragment/跨节点网络传输：负责将当前 PlanFragment 计算产生的数据，通过网络（RPC 数据流）发送给下游 Fragment 的 ExchangeNode（接收节点）。
// 数据 Shuffle 与重分发：封装输出数据分区规则 DataPartition（如 Hash 分发、广播 Broadcast、随机分发等），指导 BE（Backend）按照特定列的 Hash 值或模式将 RowBatch 发送到目标 BE 节点。
// 数据投影、过滤与下推：在发送网络数据前，支持进行行级过滤（conjuncts）、列级投影裁剪（projections）以及应用 Runtime Filter 过滤，减少跨节点网络传输的数据量。
// 支持 Tablet ID Shuffle 优化：针对特定数据写入/导入场景，包含了直接按照 Doris 存储层 Tablet ID 进行网路分发（Tablet ID Shuffle）所需的 Schema、Partition、Location 等元数据配置。
public class DataStreamSink extends DataSink {
    // 目标 ExchangeNode 的算子节点 ID。
    // 标识当前 Sink 吐出的数据流最终要发送到下游哪个 ExchangeNode 接收。
    private PlanNodeId exchNodeId;
    // 数据发送的分区/分片规则描述对象。
    // 定义数据流的网络 Shuffle 策略（例如 UNPARTITIONED 广播/单流、HASH_PARTITIONED 哈希分区、RANDOM 随机分配等），BE 的 StreamSender 会根据它计算数据路由目标。
    private DataPartition outputPartition;
    // 输出元组描述符（Tuple Descriptor）。
    // 当存在投影（Projections）操作时，定义经过投影裁剪后输出给下游的数据行结构定义及其对应的 Tuple ID。
    protected TupleDescriptor outputTupleDesc;
    // 投影表达式列表（Output Projection Expressions）。
    // 指定数据在通过网络发送前需要计算或裁剪的列/表达式。如果非空，BE 节点会先对数据做投影转换再发送。
    protected List<Expr> projections;
    // 连接谓词过滤条件列表（Filter Conjuncts）。
    // 允许在 Sink 发送端追加行过滤谓词，在数据打包发往网络之前先进行一步过滤，降低网络 IO。
    protected List<Expr> conjuncts = Lists.newArrayList();
    // 在当前 DataStreamSink 绑定的 Runtime Filter 列表。
    // 用于将动态生成的过滤条件（如 Hash Join Build 端生成的 RF）应用或传递给 Sink 操作。
    protected List<RuntimeFilter> runtimeFilters = Lists.newArrayList();

    // use for tablet id shuffle sink only
    // 用于 Tablet ID Shuffle Sink 的表 Schema 元数据参数（TOlapTableSchemaParam）。
    // 在 Tablet Shuffle 模式下，记录目标 Olap 表的列结构、Slot 映射等信息。
    protected TOlapTableSchemaParam tabletSinkSchemaParam = null;
    // 用于 Tablet ID Shuffle Sink 的分区元数据参数（TOlapTablePartitionParam）。
    // 包含目标表的分区 Range/List 范围，用于在 Sink 端计算数据应属于哪个 Partition。
    protected TOlapTablePartitionParam tabletSinkPartitionParam = null;
    // 记录目标 Tablet 所在的 BE 节点物理地址列表（IP/Port），让数据能够直达目标 Tablet 所在的 BE。
    protected TOlapTableLocationParam tabletSinkLocationParam = null;
    // 定义 Tablet Shuffle 模式下待写入数据的 Tuple 结构。
    protected TupleDescriptor tabletSinkTupleDesc = null;
    // 用于 Tablet ID Shuffle Sink 的事务 ID（Transaction ID）。
    // 默认值为 -1。在导入/写入数据场景下关联具体的写事务 ID，确保数据写入的数据一致性。
    protected long tabletSinkTxnId = -1;
    // 在将数据按照 Tablet ID Shuffle 分发前对列进行计算映射的表达式列表。
    protected List<Expr> tabletSinkExprs = null;

    public DataStreamSink() {

    }

    public DataStreamSink(PlanNodeId exchNodeId) {
        this.exchNodeId = exchNodeId;
    }

    @Override
    public PlanNodeId getExchNodeId() {
        return exchNodeId;
    }

    public void setExchNodeId(PlanNodeId exchNodeId) {
        this.exchNodeId = exchNodeId;
    }

    @Override
    public DataPartition getOutputPartition() {
        return outputPartition;
    }

    public void setOutputPartition(DataPartition outputPartition) {
        this.outputPartition = outputPartition;
    }

    public TupleDescriptor getOutputTupleDesc() {
        return outputTupleDesc;
    }

    public void setOutputTupleDesc(TupleDescriptor outputTupleDesc) {
        this.outputTupleDesc = outputTupleDesc;
    }

    public List<Expr> getProjections() {
        return projections;
    }

    public void setProjections(List<Expr> projections) {
        this.projections = projections;
    }

    public List<Expr> getConjuncts() {
        return conjuncts;
    }

    public void setConjuncts(List<Expr> conjuncts) {
        this.conjuncts = conjuncts;
    }

    public void addConjunct(Expr conjunct) {
        this.conjuncts.add(conjunct);
    }

    public List<RuntimeFilter> getRuntimeFilters() {
        return runtimeFilters;
    }

    public void addRuntimeFilter(RuntimeFilter runtimeFilter) {
        this.runtimeFilters.add(runtimeFilter);
    }

    public void setTabletSinkSchemaParam(TOlapTableSchemaParam schemaParam) {
        this.tabletSinkSchemaParam = schemaParam;
    }

    public void setTabletSinkPartitionParam(TOlapTablePartitionParam partitionParam) {
        this.tabletSinkPartitionParam = partitionParam;
    }

    public void setTabletSinkTupleDesc(TupleDescriptor tupleDesc) {
        this.tabletSinkTupleDesc = tupleDesc;
    }

    public void setTabletSinkLocationParam(TOlapTableLocationParam locationParam) {
        this.tabletSinkLocationParam = locationParam;
    }

    public void setTabletSinkExprs(List<Expr> tabletSinkExprs) {
        this.tabletSinkExprs = tabletSinkExprs;
    }

    public void setTabletSinkTxnId(long txnId) {
        this.tabletSinkTxnId = txnId;
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix).append("STREAM DATA SINK\n");
        strBuilder.append(prefix).append("  EXCHANGE ID: ").append(exchNodeId);
        if (outputPartition != null) {
            strBuilder.append("\n").append(prefix).append("  ").append(outputPartition.getExplainString(explainLevel));
        }
        if (!conjuncts.isEmpty()) {
            Expr expr = PlanNode.convertConjunctsToAndCompoundPredicate(conjuncts);
            strBuilder.append(prefix).append("  CONJUNCTS: ")
                    .append(expr.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE)).append("\n");
        }
        if (!runtimeFilters.isEmpty()) {
            strBuilder.append(prefix).append("  runtime filters: ");
            strBuilder.append(getRuntimeFilterExplainString(false, false));
        }
        if (!CollectionUtils.isEmpty(projections)) {
            strBuilder.append(prefix).append("  PROJECTIONS: ")
                    .append(PlanNode.getExplainString(projections)).append("\n");
            strBuilder.append(prefix).append("  PROJECTION TUPLE: ").append(outputTupleDesc.getId());
            strBuilder.append("\n");
        }
        if (isMerge) {
            strBuilder.append("IS_MERGE: true\n");
        }

        return strBuilder.toString();
    }

    protected String getRuntimeFilterExplainString(boolean isBuildNode, boolean isBrief) {
        if (runtimeFilters.isEmpty()) {
            return "";
        }
        List<String> filtersStr = new ArrayList<>();
        for (RuntimeFilter filter : runtimeFilters) {
            filtersStr.add(filter.getExplainString(getExchNodeId()));
        }
        return Joiner.on(", ").join(filtersStr) + "\n";
    }

    @Override
    protected TDataSink toThrift() {
        TDataSink result = new TDataSink(TDataSinkType.DATA_STREAM_SINK);
        TDataStreamSink tStreamSink =
                new TDataStreamSink(exchNodeId.asInt(), outputPartition.toThrift());
        for (Expr e : conjuncts) {
            tStreamSink.addToConjuncts(ExprToThriftVisitor.treeToThrift(e));
        }
        if (projections != null) {
            for (Expr expr : projections) {
                tStreamSink.addToOutputExprs(ExprToThriftVisitor.treeToThrift(expr));
            }
        }
        if (outputTupleDesc != null) {
            tStreamSink.setOutputTupleId(outputTupleDesc.getId().asInt());
        }

        if (runtimeFilters != null) {
            for (RuntimeFilter rf : runtimeFilters) {
                tStreamSink.addToRuntimeFilters(rf.toThrift());
            }
        }
        Preconditions.checkState((tabletSinkSchemaParam != null) == (tabletSinkPartitionParam != null),
                "schemaParam and partitionParam should be set together.");
        if (tabletSinkSchemaParam != null) {
            tStreamSink.setTabletSinkSchema(tabletSinkSchemaParam);
        }
        if (tabletSinkPartitionParam != null) {
            tStreamSink.setTabletSinkPartition(tabletSinkPartitionParam);
        }
        if (tabletSinkTupleDesc != null) {
            tStreamSink.setTabletSinkTupleId(tabletSinkTupleDesc.getId().asInt());
        }
        if (tabletSinkLocationParam != null) {
            tStreamSink.setTabletSinkLocation(tabletSinkLocationParam);
        }
        if (tabletSinkExprs != null) {
            for (Expr expr : tabletSinkExprs) {
                tStreamSink.addToTabletSinkExprs(ExprToThriftVisitor.treeToThrift(expr));
            }
        }
        tStreamSink.setIsMerge(isMerge);
        tStreamSink.setTabletSinkTxnId(tabletSinkTxnId);
        result.setStreamSink(tStreamSink);
        return result;
    }
}
