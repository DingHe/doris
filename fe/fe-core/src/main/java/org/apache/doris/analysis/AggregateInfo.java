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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/AggregateInfo.java
// and modified by Doris

package org.apache.doris.analysis;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

// AggregateInfo 封装了聚合计算（Aggregation）或窗口/分析函数（Analytics）在逻辑与物理执行层面所需的核心元数据。
// 主要作用包括：
// 聚合要素建模：记录聚合操作所需的 GROUP BY 表达式（Grouping Expressions）以及聚合函数表达式（Aggregate Expressions，如 SUM, COUNT, AVG 等）。
// 描述输出 Tuple 结构：维护用于存放聚合结果的 TupleDescriptor（元组描述符），指明 BE（Backend）节点计算完成后数据写入内存的 Layout（Slot 映射关系）。
// 管理聚合阶段（AggPhase）：标识当前聚合是处于一阶段局部聚合（FIRST）、二阶段全局 Merge（SECOND_MERGE）还是其他中间阶段。
// 物化列（Materialized Slots）管理：跟踪哪些聚合列真正需要被物化（Materialize），优化不必要的内存与网络传输开销。
public final class AggregateInfo {

    // For aggregations: All unique grouping expressions from a select block.
    // For analytics: Empty.
    // 分组表达式列表。
    // • 针对普通的聚合查询，存放 GROUP BY 子句中的所有唯一表达式。
    // • 针对分析/窗口函数（Analytics），此列表为空。
    private final ArrayList<Expr> groupingExprs;

    // For aggregations: All unique aggregate expressions from a select block.
    // For analytics: The results of AnalyticExpr.getFnCall() for the unique
    // AnalyticExprs of a select block.
    // 聚合函数表达式列表。
    // • 针对普通聚合，存放 SELECT/HAVING 中的唯一聚合函数（如 count(x), sum(y)）。
    // • 针对窗口函数，存放 AnalyticExpr.getFnCall() 的底层计算函数。
    private final ArrayList<FunctionCallExpr> aggregateExprs;

    // The tuple into which the final output of the aggregation is materialized.
    // Contains groupingExprs.size() + aggregateExprs.size() slots, the first of which
    // contain the values of the grouping exprs, followed by slots into which the
    // aggregateExprs' finalize() symbol write its result, i.e., slots of the aggregate
    // functions' output types.
    // 聚合输出 Tuple 描述符。
    // 描述了聚合计算结果写回的内存结构。它包含的 Slot 数量一般等于 groupingExprs.size() + aggregateExprs.size()。
    // • 前半部分 Slot 存放分组列的值；
    // • 后半部分 Slot 存放聚合函数 finalize() 计算后的最终输出值。
    private TupleDescriptor outputTupleDesc;

    // For aggregation: indices into aggregate exprs for that need to be materialized
    // For analytics: indices into the analytic exprs and their corresponding aggregate
    // exprs that need to be materialized.
    // Populated in materializeRequiredSlots() which must be implemented by subclasses.
    // 需物化的聚合函数索引列表。
    // 存放在 aggregateExprs 中的索引下标（0, 1, 2...），指示哪些聚合表达式是需要实际分配物理 Slot 并输出结果的。
    private ArrayList<Integer> materializedSlots = Lists.newArrayList();
    // 物化 Slot 的标签/名称列表。
    // 保存物化列的可读名称或 SQL 表示（可带 partial_ 前缀和表达式 ID），主要用于 Explain 打印物理执行计划和调试。
    private List<String> materializedSlotLabels = Lists.newArrayList();

    public enum AggPhase {
        // 第一阶段（局部/ Local 聚合），在数据源头 Backend 节点对原始数据直接进行的局部 Hash 聚合。
        FIRST,
        // 第一阶段 Merge 聚合（例如多级 Shuffle 或 Stream 场景下合并 Intermediate 状态）。
        FIRST_MERGE,
        // 第二阶段（全局/ Global 聚合），数据经 Network Shuffle 后在接收端进行的完整聚合。
        SECOND,
        // 第二阶段 Merge 聚合，合并第一阶段传输过来的中间聚合状态（Serialize / Intermediate State）。
        SECOND_MERGE;

        public boolean isMerge() {
            return this == FIRST_MERGE || this == SECOND_MERGE;
        }
    }
    // 当前聚合所处的执行阶段。
    private final AggPhase aggPhase;

    // C'tor creates copies of groupingExprs and aggExprs.
    private AggregateInfo(ArrayList<Expr> groupingExprs,
                          ArrayList<FunctionCallExpr> aggExprs, AggPhase aggPhase)  {
        Preconditions.checkState(groupingExprs != null || aggExprs != null);
        this.groupingExprs =
                groupingExprs != null ? Expr.cloneList(groupingExprs) : new ArrayList<>();
        aggregateExprs =
                aggExprs != null ? Expr.cloneList(aggExprs) : new ArrayList<>();
        this.aggPhase = aggPhase;
    }

    /**
     * C'tor for cloning.
     */
    private AggregateInfo(AggregateInfo other) {
        groupingExprs =
                (other.groupingExprs != null) ? Expr.cloneList(other.groupingExprs) : null;
        aggregateExprs =
                (other.aggregateExprs != null) ? Expr.cloneList(other.aggregateExprs) : null;
        outputTupleDesc = other.outputTupleDesc;
        materializedSlots = Lists.newArrayList(other.materializedSlots);
        materializedSlotLabels = Lists.newArrayList(other.materializedSlotLabels);
        aggPhase = other.aggPhase;
    }

    /**
     * Used by new optimizer.
     */
    public static AggregateInfo create(
            ArrayList<Expr> groupingExprs, ArrayList<FunctionCallExpr> aggExprs, List<Integer> aggExprIds,
            boolean isPartialAgg, TupleDescriptor tupleDesc, AggPhase phase) {
        AggregateInfo result = new AggregateInfo(groupingExprs, aggExprs, phase);
        result.outputTupleDesc = tupleDesc;
        int aggExprSize = result.getAggregateExprs().size();
        for (int i = 0; i < aggExprSize; i++) {
            result.materializedSlots.add(i);
            String label = (isPartialAgg ? "partial_" : "")
                    + aggExprs.get(i).accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE)
                    + "[#" + aggExprIds.get(i) + "]";
            result.materializedSlotLabels.add(label);
        }
        return result;
    }

    public ArrayList<Expr> getGroupingExprs() {
        return groupingExprs;
    }

    public ArrayList<FunctionCallExpr> getAggregateExprs() {
        return aggregateExprs;
    }

    public TupleDescriptor getOutputTupleDesc() {
        return outputTupleDesc;
    }

    public TupleId getOutputTupleId() {
        return outputTupleDesc.getId();
    }

    public List<String> getMaterializedAggregateExprLabels() {
        return Lists.newArrayList(materializedSlotLabels);
    }

    public ArrayList<FunctionCallExpr> getMaterializedAggregateExprs() {
        ArrayList<FunctionCallExpr> result = Lists.newArrayList();
        for (Integer i : materializedSlots) {
            result.add(aggregateExprs.get(i));
        }
        return result;
    }

    public boolean isMerge() {
        return aggPhase.isMerge();
    }

    public boolean isFirstPhase() {
        return aggPhase == AggPhase.FIRST;
    }

    public void updateMaterializedSlots() {
        // why output and intermediate may have different materialized slots?
        // because some slot is materialized by materializeSrcExpr method directly
        // in that case, only output slots is materialized
        // assume output tuple has correct materialized information
        // we update intermediate tuple and materializedSlots based on output tuple
        materializedSlots.clear();
        ArrayList<SlotDescriptor> outputSlots = outputTupleDesc.getSlots();
        int groupingExprNum = groupingExprs != null ? groupingExprs.size() : 0;
        Preconditions.checkState(groupingExprNum <= outputSlots.size());
        for (int i = groupingExprNum; i < outputSlots.size(); ++i) {
            materializedSlots.add(i - groupingExprNum);
        }
    }

    @Override
    public AggregateInfo clone() {
        return new AggregateInfo(this);
    }

}
