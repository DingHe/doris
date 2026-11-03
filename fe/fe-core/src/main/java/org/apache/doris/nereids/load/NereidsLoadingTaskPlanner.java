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

package org.apache.doris.nereids.load;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.info.PartitionNamesInfo;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.FileLoadScanNode;
import org.apache.doris.planner.OlapTableSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TUniqueKeyUpdateMode;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 * NereidsLoadingTaskPlanner
 */
// 基于 Nereids 优化器架构实现的导入任务执行计划规划器
// 接收 Broker Load 导入作业的各项配置参数、文件状态以及目标表结构，利用 Nereids 体系构建逻辑导入计划，并将其转化为 Doris 分布式执行引擎所需的扫描节点（ScanNode）、描述符表（DescriptorTable）以及带有数据写入端（OlapTableSink）的执行片（PlanFragment），供 Coordinator 下发给 BE 执行。
public class NereidsLoadingTaskPlanner {
    private static final Logger LOG = LogManager.getLogger(NereidsLoadingTaskPlanner.class);

    // Input params
    // 当前导入作业的全局唯一作业 ID。
    private final long loadJobId;
    // 关联的分布式事务 ID（Transaction ID）。
    private final long txnId;
    // 目标数据库的 ID。
    private final long dbId;
    // 目标 OLAP 表对象指针。
    private final OlapTable table;
    // 外部存储系统（如 HDFS、S3 等）的凭证与访问描述符。
    private final BrokerDesc brokerDesc;
    // Nereids 格式的文件组列表，定义了源数据文件路径与目标分区/分桶的映射关系。
    private final List<NereidsBrokerFileGroup> fileGroups;
    // 是否开启严格模式（Strict Mode，用于校验数据类型转换是否合法）。
    private final boolean strictMode;
    // 标记当前是否为唯一键（Unique Key）的部分更新（Partial Update）操作。
    private final boolean isPartialUpdate;
    // 部分更新中遇到新行时的处理策略策略。
    private final TPartialUpdateNewRowPolicy partialUpdateNewKeyPolicy;
    // 执行导入时采用的时区。
    private final String timezone;
    // 导入作业的超时时间（单位：秒）。
    private final long timeoutS; // timeout of load job, in second
    // 导入任务的执行并发度。
    private final int loadParallelism;
    // 发送数据批次（Batch）的并行度。
    private final int sendBatchParallelism;
    // 每个 Sink 是否仅加载单个 Tablet 的优化控制项。
    private final boolean singleTabletLoadPerSink;
    // 是否在 Sink 节点直接启用 MemTable。
    private final boolean enableMemtableOnSinkNode;
    // 发起导入操作的用户身份信息。
    private UserIdentity userInfo;
    // 描述符表（Descriptor Table）。用于管理导入计划中涉及的所有 Tuple 结构和 Slot 描述符。
    private final DescriptorTable descTable = new DescriptorTable();

    // Output params
    // 输出参数。
    // 存放构建好的分布式执行计划片（Plan Fragment 列表）。
    private List<PlanFragment> fragments = Lists.newArrayList();
    // 输出参数。
    // 存放生成的扫描节点列表（主要包含负责从远端拉取文件的 FileLoadScanNode）。
    private List<ScanNode> scanNodes = Lists.newArrayList();

    /**
     * NereidsLoadingTaskPlanner
     */
    public NereidsLoadingTaskPlanner(Long loadJobId, long txnId, long dbId, OlapTable table,
            BrokerDesc brokerDesc, List<NereidsBrokerFileGroup> brokerFileGroups,
            boolean strictMode, boolean isPartialUpdate, TPartialUpdateNewRowPolicy partialUpdateNewKeyPolicy,
            String timezone, long timeoutS, int loadParallelism,
            int sendBatchParallelism, UserIdentity userInfo,
            boolean singleTabletLoadPerSink, boolean enableMemtableOnSinkNode) {
        this.loadJobId = loadJobId;
        this.txnId = txnId;
        this.dbId = dbId;
        this.table = table;
        this.brokerDesc = brokerDesc;
        this.fileGroups = brokerFileGroups;
        this.strictMode = strictMode;
        this.isPartialUpdate = isPartialUpdate;
        this.partialUpdateNewKeyPolicy = partialUpdateNewKeyPolicy;
        this.timezone = timezone;
        this.timeoutS = timeoutS;
        this.loadParallelism = loadParallelism;
        this.sendBatchParallelism = sendBatchParallelism;
        this.userInfo = userInfo;
        this.singleTabletLoadPerSink = singleTabletLoadPerSink;
        this.enableMemtableOnSinkNode = enableMemtableOnSinkNode;
    }

    /**
     * create a plan for broker loading task
     */
    // 核心执行计划生成方法。
    // 负责完成全套校验、上下文初始化、逻辑计划构建以及物理 Fragment 组装。
    public void plan(TUniqueId loadId, List<List<TBrokerFileStatus>> fileStatusesList, int filesAdded)
            throws UserException {
        // 若开启了部分更新（isPartialUpdate），强制校验目标表必须是开启了写时合并（Merge-on-Write）的 Unique Key 表，且不能包含集群键（Cluster Keys），否则抛出异常。
        if (isPartialUpdate && !table.getEnableUniqueKeyMergeOnWrite()) {
            throw new UserException("Only unique key merge on write support partial update");
        }
        if (isPartialUpdate && table.isUniqKeyMergeOnWriteWithClusterKeys()) {
            throw new UserException("Only unique key merge on write without cluster keys support partial update");
        }
        // 部分更新列收集与校验：
        // 遍历表的全量 Schema，检查用户导入的表达式中是否包含了所有必需的 Key 列；同时确保除 __delete_sign 外，不能包含其他不可见列。
        HashSet<String> partialUpdateInputColumns = new HashSet<>();
        if (isPartialUpdate) {
            List<NereidsImportColumnDesc> importColumnDescs = fileGroups.get(0).getColumnExprList();
            for (Column col : table.getFullSchema()) {
                boolean existInExpr = NereidsLoadUtils.hasImportColumn(importColumnDescs, col);
                if (existInExpr) {
                    if (!col.isVisible() && !Column.DELETE_SIGN.equals(col.getName())) {
                        throw new UserException("Partial update should not include invisible column except"
                                + " delete sign column: " + col.getName());
                    }
                    partialUpdateInputColumns.add(col.getName());
                }
                if (col.isKey() && !existInExpr) {
                    throw new UserException("Partial update should include all key columns, missing: " + col.getName());
                }
            }
        }

        Preconditions.checkState(!fileGroups.isEmpty() && fileGroups.size() == fileStatusesList.size());

        // make sure StatementContext is set in ConnectContext
        // 确保当前的 ConnectContext 中绑定了 StatementContext，以便 Nereids 优化器正常运行。
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext != null && connectContext.getStatementContext() == null) {
            StatementContext statementContext = new StatementContext();
            connectContext.setStatementContext(statementContext);
            statementContext.setConnectContext(connectContext);
        }
        // 检查并获取目标分区名称和是否为临时分区。
        PartitionNamesInfo partitionNamesInfo = getPartitionNamesInfo();
        long txnTimeout = timeoutS == 0 ? ConnectContext.get().getExecTimeoutS() : timeoutS;
        if (txnTimeout > Integer.MAX_VALUE) {
            txnTimeout = Integer.MAX_VALUE;
        }
        // 创建 NereidsBrokerLoadTask 实例；
        // 通过 descTable.createTupleDescriptor() 创建扫描端的目标 Tuple 描述符并关联目标表。
        NereidsBrokerLoadTask nereidsBrokerLoadTask = new NereidsBrokerLoadTask(txnId, (int) txnTimeout,
                sendBatchParallelism,
                strictMode, enableMemtableOnSinkNode, singleTabletLoadPerSink, partitionNamesInfo);

        TupleDescriptor scanTupleDesc = descTable.createTupleDescriptor();
        scanTupleDesc.setTable(table);
        // Collect all file group infos, contexts, and load plan infos
        List<NereidsFileGroupInfo> fileGroupInfos = new ArrayList<>(fileGroups.size());
        List<NereidsParamCreateContext> contexts = new ArrayList<>(fileGroups.size());
        List<NereidsLoadPlanInfoCollector.LoadPlanInfo> loadPlanInfos =
                new ArrayList<>(fileGroups.size());

        // Create a separate plan for each file group
        // 循环为每个文件组构建计划：
        // 针对每个文件组分别创建 NereidsFileGroupInfo、NereidsLoadScanProvider、解析上下文（context）、
        // 利用 NereidsLoadUtils.createLoadPlan 生成逻辑计划（LogicalPlan），并借助 NereidsLoadPlanInfoCollector 收集计划元数据。
        for (int i = 0; i < fileGroups.size(); ++i) {
            // 封装文件组（File Group）的元数据、文件状态列表（TBrokerFileStatus）、存储配置（BrokerDesc）以及目标表信息。
            NereidsFileGroupInfo fileGroupInfo = new NereidsFileGroupInfo(loadJobId, txnId, table, brokerDesc,
                    fileGroups.get(i), fileStatusesList.get(i), filesAdded, strictMode, loadParallelism);
            // 专门负责处理数据导入任务中列映射表达式、删除条件（Delete Conditions）、序列列（Sequence Columns）以及 Hadoop 兼容函数重写的核心提供者类。
            NereidsLoadScanProvider loadScanProvider = new NereidsLoadScanProvider(fileGroupInfo,
                    partialUpdateInputColumns);
            // 创建上下文
            NereidsParamCreateContext context = loadScanProvider.createLoadContext();
            // 创建逻辑计划
            LogicalPlan loadPlan = NereidsLoadUtils.createLoadPlan(fileGroupInfo, partitionNamesInfo, context,
                    isPartialUpdate, partialUpdateNewKeyPolicy);

            NereidsLoadPlanInfoCollector planInfoCollector = new NereidsLoadPlanInfoCollector(table,
                    nereidsBrokerLoadTask, loadId, dbId,
                    isPartialUpdate ? TUniqueKeyUpdateMode.UPDATE_FIXED_COLUMNS : TUniqueKeyUpdateMode.UPSERT,
                    partialUpdateNewKeyPolicy, partialUpdateInputColumns, context.exprMap);
            // 收集导入计划信息
            NereidsLoadPlanInfoCollector.LoadPlanInfo loadPlanInfo = planInfoCollector.collectLoadPlanInfo(loadPlan,
                    descTable, scanTupleDesc);

            fileGroupInfos.add(fileGroupInfo);
            contexts.add(context);
            loadPlanInfos.add(loadPlanInfo);
        }

        // Create a single FileLoadScanNode for all file groups
        // 组装扫描节点与 Fragment：
        // 创建全局唯一的 FileLoadScanNode，并调用 finalizeForNereids 绑定所有文件组信息与上下文。
        String clusterName = ConnectContext.get() == null ? ""
                : ConnectContext.get().getSessionVariable().resolveCloudClusterName();
        FileLoadScanNode fileScanNode = new FileLoadScanNode(new PlanNodeId(0), loadPlanInfos.get(0).getDestTuple(),
                ScanContext.builder().clusterName(clusterName).build());
        fileScanNode.finalizeForNereids(loadId, fileGroupInfos, contexts, loadPlanInfos);
        scanNodes.add(fileScanNode);

        // Create plan fragment
        // 创建根 PlanFragment（即 sinkFragment），设置为随机分发（DataPartition.RANDOM），指定并发度，并挂载 OlapTableSink。
        PlanFragment sinkFragment = new PlanFragment(new PlanFragmentId(0), fileScanNode, DataPartition.RANDOM);
        sinkFragment.setParallelExecNum(loadParallelism);
        sinkFragment.setSink(loadPlanInfos.get(0).getOlapTableSink());

        fragments.add(sinkFragment);

        // finalize
        // 遍历所有 Fragment 调用 fragment.finalize(null) 完成最后的分布式调度绑定，并倒序排列 Fragment。
        for (PlanFragment fragment : fragments) {
            fragment.finalize(null);
        }
        Collections.reverse(fragments);
    }

    public DescriptorTable getDescTable() {
        return descTable;
    }

    public List<PlanFragment> getFragments() {
        return fragments;
    }

    public List<ScanNode> getScanNodes() {
        return scanNodes;
    }

    public String getTimezone() {
        return timezone;
    }

    private PartitionNamesInfo getPartitionNamesInfo() throws LoadException {
        PartitionNamesInfo partitionNames = null;
        List<String> partitions = Lists.newArrayList();
        boolean isTemp = false;
        for (NereidsBrokerFileGroup brokerFileGroup : fileGroups) {
            if (brokerFileGroup.getPartitionIds() != null) {
                for (long partitionId : brokerFileGroup.getPartitionIds()) {
                    if (!table.getPartitionInfo().getIsMutable(partitionId)) {
                        throw new LoadException("Can't load data to immutable partition, table: "
                                + table.getName() + ", partition: " + table.getPartition(partitionId));
                    }
                    if (table.isTemporaryPartition(partitionId)) {
                        isTemp = true;
                    }
                    Partition partition = table.getPartition(partitionId);
                    if (partition == null) {
                        throw new LoadException(String.format("partition id %d not found", partitionId));
                    }
                    partitions.add(partition.getName());
                }
            }
            // all file group in fileGroups should have same partitions, so only need to get partition ids
            // from one of these file groups
            break;
        }
        if (!partitions.isEmpty()) {
            partitionNames = new PartitionNamesInfo(isTemp, partitions);
        }
        return partitionNames;

    }

    /**
     * when retry load by reusing this plan in load process, the load_id should be changed
     */
    public void updateLoadId(TUniqueId loadId) {
        for (PlanFragment planFragment : fragments) {
            if (!(planFragment.getSink() instanceof OlapTableSink)) {
                continue;
            }
            OlapTableSink olapTableSink = (OlapTableSink) planFragment.getSink();
            olapTableSink.updateLoadId(loadId);
        }

        LOG.info("update olap table sink's load id to {}, job: {}", DebugUtil.printId(loadId), loadJobId);
    }
}
