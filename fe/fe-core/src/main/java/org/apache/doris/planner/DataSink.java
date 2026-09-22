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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/DataSink.java
// and modified by Doris

package org.apache.doris.planner;

import org.apache.doris.planner.LocalExchangeNode.LocalExchangeTypeRequire;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TExplainLevel;

/**
 * A DataSink describes the destination of a plan fragment's output rows.
 * The destination could be another plan fragment on a remote machine,
 * or a table into which the rows are to be inserted
 * (i.e., the destination of the last fragment of an INSERT statement).
 */
// DataSink 是 Apache Doris 前端 (FE) 物理查询计划层中的一个抽象基类，用于定义一个 PlanFragment（计划片段）处理完数据后的最终输出目的地（数据接收端/去向）。
// 在 Doris 的分布式执行架构中，一个完整的物理查询计划会被拆分为多个 PlanFragment 执行。每个 Fragment 的顶部都需要配备一个 DataSink，它的核心作用包括：
// 跨 Fragment 数据传输（Stream Sink）：将当前 Fragment 计算出的结果通过网络 RPC 发送到另一个远程/本地机器上的 Fragment（例如发送给下游 Fragment 的 ExchangeNode 算子）。
// 数据落盘/写入（Storage/Result Sink）：将最终 Fragment 的计算结果导出或写入目的地。例如：执行 INSERT INTO 语句写回 Doris 内表（OlapTableSink）、写回 Hive/Iceberg 等外表、或者将 SELECT 查询结果通过网络返回给客户端 Client（ResultSink）。
// Pipeline 引擎的本地 Shuffle 协商：作为 Fragment 的终点，提供与 Pipeline 引擎沟通的接口（如 getLocalExchangeTypeRequire()），参与 Fragment 边界及内部 Local Exchange 的推导。
public abstract class DataSink {
    // Fragment that this DataSink belongs to. Set by the PlanFragment enclosing this sink.
    // 记录当前 DataSink 所归属的物理计划片段 (PlanFragment) 实例。
    // 该属性由包含当前 Sink 的 PlanFragment 在构建过程中通过 setFragment() 方法显式进行关联和设置。
    protected PlanFragment fragment;
    // 标记当前 DataSink 是否需要上游输出有序数据并开启多路归并/合并模式（Merge 模式）。
    // 默认值为 false。当上游存在 ORDER BY ... LIMIT 等排序逻辑时，可能需要设置为 true，配合下游的 MergingExchangeNode 来保证跨线程或跨节点传输时的全局有序性。
    protected boolean isMerge = false;

    /**
     * Return an explain string for the DataSink. Each line of the explain will be
     * prefixed
     * by "prefix"
     *
     * @param prefix each explain line will be started with the given prefix
     * @return
     */
    // 生成当前 DataSink 在 EXPLAIN 执行计划文本中的详细描述信息。
    public abstract String getExplainString(String prefix, TExplainLevel explainLevel);
    // 将 FE 端的 DataSink 对象及其子类特有属性序列化为 Thrift RPC 结构体 TDataSink。
    protected abstract TDataSink toThrift();
    // 设置当前 DataSink 归属的 PlanFragment。
    public void setFragment(PlanFragment fragment) {
        this.fragment = fragment;
    }

    public PlanFragment getFragment() {
        return fragment;
    }

    public abstract PlanNodeId getExchNodeId();
    // 获取当前 DataSink 吐出数据时的物理数据分区/分片类型描述（DataPartition）。
    // 返回表示当前 Sink 输出数据分布状态的对象（例如 UNPARTITIONED 单流/广播、HASH_PARTITIONED 哈希分区、RANDOM 随机分发等）。优化器利用此信息决定下游算子是否需要重新 Shuffle。
    public abstract DataPartition getOutputPartition();

    public boolean isMerge() {
        return isMerge;
    }

    public void setMerge(boolean merge) {
        isMerge = merge;
    }
    // 获取该 DataSink 对其所在 Fragment 内部上游最后一个算子输出数据的 Local Exchange（本地线程间重分发）要求。
    // 在基类中提供默认实现，直接返回 LocalExchangeTypeRequire.noRequire()（即对上游本地数据分布无特殊要求）。
    // 具体的子类（如某些写表 Sink 或特定分发的 DataStreamSink）可以重写此方法，要求上游在其前侧强制插入特定类型的 LocalExchangeNode（如 RequireHash），以确保数据进入 Sink 前在 BE 单节点内部线程间完成再分配。
    public LocalExchangeTypeRequire getLocalExchangeTypeRequire() {
        return LocalExchangeTypeRequire.noRequire();
    }
}
