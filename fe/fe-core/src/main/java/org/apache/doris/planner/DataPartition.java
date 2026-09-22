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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/DataPartition.java
// and modified by Doris

package org.apache.doris.planner;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprToSqlVisitor;
import org.apache.doris.analysis.ExprToThriftVisitor;
import org.apache.doris.analysis.ToSqlParams;
import org.apache.doris.thrift.TDataPartition;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TIcebergPartitionField;
import org.apache.doris.thrift.TMergePartitionInfo;
import org.apache.doris.thrift.TPartitionType;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Specification of the partition of a single stream of data.
 * Examples of those streams of data are: the scan of a table; the output
 * of a plan fragment; etc. (ie, this is not restricted to direct exchanges
 * between two fragments, which in the backend is facilitated by the classes
 * DataStreamSender/DataStreamMgr/DataStreamRecvr).
 * TODO: better name? just Partitioning?
 */
// 在 Apache Doris 的分布式执行计划中，DataPartition 用于描述数据流在节点间或网络传输中的分区规格（Data Partitioning Specification）。
// 分布式数据路由抽象：它定义了数据流是如何被切分和路由的（例如：是哈希分区、随机发往任意节点、广播还是未分区）。
// 连接 PlanFragment 的纽带：在生成分布式执行计划时，PlanFragment 之间的网络 Shuffle、DataStreamSink 发送策略以及扫描节点（ScanNode）的数据分布，均依赖 DataPartition 提供指导。
// 支持多种数据写入/合并场景：除了常规的分布式查询，它还支持 Hive 数据写入、OlapTable 写入，以及 Iceberg/HUDI 等数据湖 Merge-Into 语句的合并分区计算（MERGE_PARTITIONED）。
public class DataPartition {
    // 表示单节点未分区模式（单流）。数据不需要进行任何物理 Shuffle，通常用于在单个节点上收集最终结果或小表计算。
    public static final DataPartition UNPARTITIONED = new DataPartition(TPartitionType.UNPARTITIONED);
    // 表示随机/轮询（Round-Robin）分区模式。数据会被随机或均匀地发送给下游各个执行节点，常用于无特定 Hash 要求的并行打散场景。
    public static final DataPartition RANDOM = new DataPartition(TPartitionType.RANDOM);
    // 表示针对内部 OLAP 表写入（Sink）的桶 ID（Tablet ID）哈希分区模式。
    public static final DataPartition TABLET_ID = new DataPartition(TPartitionType.OLAP_TABLE_SINK_HASH_PARTITIONED);
    // 分区的枚举类型（对应 Thrift 结构中的 TPartitionType）。枚举值包含 HASH_PARTITIONED、RANDOM、UNPARTITIONED、BUCKET_SHFFULE_HASH_PARTITIONED、MERGE_PARTITIONED 等，
    // 是决定数据路由核心逻辑的标志。
    private final TPartitionType type;
    // for hash partition: exprs used to compute hash value
    // 哈希分区或范围分区的计算表达式列表。当类型为 Hash 分区时，执行引擎会计算这些表达式的 Hash 值，并将数据行发送到对应的 BE 节点。
    private ImmutableList<Expr> partitionExprs;
    // 保存数据湖（如 Iceberg）在执行 Merge-Into 语义时的复杂分区规则（包含 Insert/Delete 表达式及转换字段）。仅在 type == MERGE_PARTITIONED 时非空。
    private MergePartitionInfo mergePartitionInfo;

    // 带表达式列表的分区构造函数。
    // 校验 exprs 不能为空，且 type 必须是需要依赖表达式进行分区的类型（如 HASH_PARTITIONED、RANGE_PARTITIONED、HIVE_TABLE_SINK_HASH_PARTITIONED、BUCKET_SHFFULE_HASH_PARTITIONED）。
    // 将表达式列表拷贝为不可变列表 partitionExprs。
    public DataPartition(TPartitionType type, List<Expr> exprs) {
        Preconditions.checkNotNull(exprs);
        Preconditions.checkState(!exprs.isEmpty());
        Preconditions.checkState(type == TPartitionType.HASH_PARTITIONED
                || type == TPartitionType.RANGE_PARTITIONED
                || type == TPartitionType.HIVE_TABLE_SINK_HASH_PARTITIONED
                || type == TPartitionType.BUCKET_SHFFULE_HASH_PARTITIONED);
        this.type = type;
        this.partitionExprs = ImmutableList.copyOf(exprs);
    }
    // 不带表达式的分区构造函数。
    // 校验 type 必须是无表达式要求的分区类型（如 UNPARTITIONED、RANDOM、HIVE_TABLE_SINK_UNPARTITIONED、OLAP_TABLE_SINK_HASH_PARTITIONED）。将 partitionExprs 设置为空列表。
    public DataPartition(TPartitionType type) {
        Preconditions.checkState(type == TPartitionType.UNPARTITIONED
                || type == TPartitionType.RANDOM
                || type == TPartitionType.HIVE_TABLE_SINK_UNPARTITIONED
                || type == TPartitionType.OLAP_TABLE_SINK_HASH_PARTITIONED);
        this.type = type;
        this.partitionExprs = ImmutableList.of();
    }
    // 专门为 Merge-Into 场景打造的构造函数。
    // 强校验 type 必须为 MERGE_PARTITIONED，将传入的各种复杂的 Insert/Delete 表达策略封装进内部对象 MergePartitionInfo 中。
    public DataPartition(TPartitionType type, Expr operationExpr, List<Expr> insertPartitionExprs,
            List<Expr> deletePartitionExprs, boolean insertRandom,
            List<MergePartitionField> insertPartitionFields, Integer partitionSpecId) {
        Preconditions.checkState(type == TPartitionType.MERGE_PARTITIONED);
        this.type = type;
        this.partitionExprs = ImmutableList.of();
        this.mergePartitionInfo = new MergePartitionInfo(operationExpr, insertPartitionExprs,
                deletePartitionExprs, insertRandom, insertPartitionFields, partitionSpecId);
    }
    // 判断当前数据流是否处于已分区状态。
    public boolean isPartitioned() {
        return type != TPartitionType.UNPARTITIONED;
    }
    // 判断当前分区策略是否为 Bucket Shuffle。
    // 若 type == TPartitionType.BUCKET_SHFFULE_HASH_PARTITIONED 则返回 true（该模式下能大幅提升 Join 的性能，减少网络传输）
    public boolean isBucketShuffleHashPartition() {
        return type == TPartitionType.BUCKET_SHFFULE_HASH_PARTITIONED;
    }
    // 获取分区的类型枚举值 type。
    public TPartitionType getType() {
        return type;
    }
    // 获取用于计算分区的表达式列表 partitionExprs。
    public List<Expr> getPartitionExprs() {
        return partitionExprs;
    }
    // 将 Java 端的 DataPartition 序列化为 ThriftRPC 传输结构体 TDataPartition。
    public TDataPartition toThrift() {
        TDataPartition result = new TDataPartition(type);
        if (partitionExprs != null) {
            result.setPartitionExprs(ExprToThriftVisitor.treesToThrift(partitionExprs));
        }
        if (mergePartitionInfo != null) {
            result.setMergePartitionInfo(mergePartitionInfo.toThrift());
        }
        return result;
    }
    // 生成在执行 EXPLAIN 命令时向用户展示的分区信息字符串。
    public String getExplainString(TExplainLevel explainLevel) {
        StringBuilder str = new StringBuilder();
        str.append(type.toString());
        if (explainLevel == TExplainLevel.BRIEF) {
            return str.toString();
        }
        if (mergePartitionInfo != null) {
            str.append(": op=").append(mergePartitionInfo.operationExpr
                    .accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE));
            if (mergePartitionInfo.insertRandom) {
                str.append(", insert=RR");
            } else if (!mergePartitionInfo.insertPartitionExprs.isEmpty()) {
                List<String> insertExprs = Lists.newArrayList();
                for (Expr expr : mergePartitionInfo.insertPartitionExprs) {
                    insertExprs.add(expr.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE));
                }
                str.append(", insert=").append(Joiner.on(", ").join(insertExprs));
            } else if (!mergePartitionInfo.insertPartitionFields.isEmpty()) {
                List<String> insertFields = Lists.newArrayList();
                for (MergePartitionField field : mergePartitionInfo.insertPartitionFields) {
                    insertFields.add(field.toSql());
                }
                str.append(", insert=").append(Joiner.on(", ").join(insertFields));
            }
            if (!mergePartitionInfo.deletePartitionExprs.isEmpty()) {
                List<String> deleteExprs = Lists.newArrayList();
                for (Expr expr : mergePartitionInfo.deletePartitionExprs) {
                    deleteExprs.add(expr.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE));
                }
                str.append(", delete=").append(Joiner.on(", ").join(deleteExprs));
            }
        } else if (!partitionExprs.isEmpty()) {
            List<String> strings = Lists.newArrayList();
            for (Expr expr : partitionExprs) {
                strings.add(expr.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE));
            }
            str.append(": ").append(Joiner.on(", ").join(strings));
        }
        str.append("\n");
        return str.toString();
    }

    public static class MergePartitionField {
        // 源表达式，代表计算分区值的原始列或表达式。
        private final Expr sourceExpr;
        // 分区转换函数（例如 Iceberg 的 day()、bucket()、truncate() 等）。
        private final String transform;
        // 转换函数的附加参数（如 bucket(10, col) 中的 10）。
        private final Integer param;
        // 分区字段的名称。
        private final String name;
        // 数据湖 Schema 中源字段的唯一 ID。
        private final Integer sourceId;
        // 嵌套数据类型（如 Struct）中源字段的路径层级。
        private final ImmutableList<Integer> sourceFieldPath;

        public MergePartitionField(Expr sourceExpr, String transform, Integer param,
                String name, Integer sourceId) {
            this(sourceExpr, transform, param, name, sourceId, ImmutableList.of());
        }

        public MergePartitionField(Expr sourceExpr, String transform, Integer param,
                String name, Integer sourceId, List<Integer> sourceFieldPath) {
            this.sourceExpr = Preconditions.checkNotNull(sourceExpr, "sourceExpr should not be null");
            this.transform = Preconditions.checkNotNull(transform, "transform should not be null");
            this.param = param;
            this.name = name;
            this.sourceId = sourceId;
            this.sourceFieldPath = ImmutableList.copyOf(sourceFieldPath);
        }

        public TIcebergPartitionField toThrift() {
            TIcebergPartitionField field = new TIcebergPartitionField();
            field.setTransform(transform);
            field.setSourceExpr(ExprToThriftVisitor.treeToThrift(sourceExpr));
            if (param != null) {
                field.setParam(param);
            }
            if (name != null) {
                field.setName(name);
            }
            if (sourceId != null) {
                field.setSourceId(sourceId);
            }
            if (!sourceFieldPath.isEmpty()) {
                field.setSourceFieldPath(sourceFieldPath);
            }
            return field;
        }

        public String toSql() {
            return transform + "(" + sourceExpr.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE) + ")";
        }
    }

    private static class MergePartitionInfo {
        // 用于判断当前行为是 INSERT、UPDATE 还是 DELETE 的操作标记表达式。
        private final Expr operationExpr;
        // 插入操作对应的分区表达式列表。
        private final ImmutableList<Expr> insertPartitionExprs;
        // 删除操作对应的分区表达式列表。
        private final ImmutableList<Expr> deletePartitionExprs;
        // 插入操作是否采用随机/轮询分布（当找不到精确的分区键时使用）。
        private final boolean insertRandom;
        // 针对 Iceberg 等表插入操作特有的高级转换分区字段列表。
        private final ImmutableList<MergePartitionField> insertPartitionFields;
        // 数据湖表的 Partition Spec 版本 ID。
        private final Integer partitionSpecId;

        private MergePartitionInfo(Expr operationExpr, List<Expr> insertPartitionExprs,
                List<Expr> deletePartitionExprs, boolean insertRandom,
                List<MergePartitionField> insertPartitionFields, Integer partitionSpecId) {
            this.operationExpr = Preconditions.checkNotNull(operationExpr, "operationExpr should not be null");
            this.insertPartitionExprs = ImmutableList.copyOf(
                    Preconditions.checkNotNull(insertPartitionExprs, "insertPartitionExprs should not be null"));
            this.deletePartitionExprs = ImmutableList.copyOf(
                    Preconditions.checkNotNull(deletePartitionExprs, "deletePartitionExprs should not be null"));
            this.insertRandom = insertRandom;
            this.insertPartitionFields = ImmutableList.copyOf(
                    Preconditions.checkNotNull(insertPartitionFields, "insertPartitionFields should not be null"));
            this.partitionSpecId = partitionSpecId;
        }

        private TMergePartitionInfo toThrift() {
            TMergePartitionInfo info = new TMergePartitionInfo();
            info.setOperationExpr(ExprToThriftVisitor.treeToThrift(operationExpr));
            if (!insertPartitionExprs.isEmpty()) {
                info.setInsertPartitionExprs(ExprToThriftVisitor.treesToThrift(insertPartitionExprs));
            }
            if (!deletePartitionExprs.isEmpty()) {
                info.setDeletePartitionExprs(ExprToThriftVisitor.treesToThrift(deletePartitionExprs));
            }
            info.setInsertRandom(insertRandom);
            if (!insertPartitionFields.isEmpty()) {
                List<TIcebergPartitionField> fields = Lists.newArrayList();
                for (MergePartitionField field : insertPartitionFields) {
                    fields.add(field.toThrift());
                }
                info.setInsertPartitionFields(fields);
            }
            if (partitionSpecId != null) {
                info.setPartitionSpecId(partitionSpecId);
            }
            return info;
        }
    }
}
