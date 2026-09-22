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

package org.apache.doris.nereids.properties;

import org.apache.doris.nereids.trees.expressions.ExprId;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/**
 * Merge-style distribution: route insert and delete/update rows using different partition keys.
 */
// 在 Apache Doris 的 Nereids 新一代优化器中，DistributionSpecMerge 是专门为数据湖（尤其是 Apache Iceberg）的 MERGE INTO / DML（UPDATE、DELETE、INSERT 混合操作） 操作设计的一种复合/混合路由分布规格（Distribution Specification）。
// 在处理数据湖（如 Iceberg）的 MERGE INTO 或 UPDATE / DELETE 语义时，数据流通常由两种类型的行（Rows）混合组成：
// INSERT（新增行）：需要写到新数据文件中，路由逻辑取决于目标表的最新分区 Spec（例如按 year(ts) 分区）。
// DELETE / UPDATE（删除/更新行）：通常需要写为 Delete File（Positional Delete / Equality Delete），路由逻辑取决于待删除旧数据所在的物理 File/Partition 位置（可能对应旧的分区结构）。
// 由于 INSERT 和 DELETE 行的分区路由 Key 及计算逻辑可能完全不同，传统的单一 DistributionSpecHash 无法满足这种复合场景。DistributionSpecMerge 应运而生，其核心作用包括：
// 支持多路由分支的复合数据分布规格：它封装了针对 INSERT 和 DELETE 的两套分区列表达式（insertPartitionExprIds / deletePartitionExprIds）以及动作列标识（operationExprId，区分某一行是 INSERT 还是 DELETE）。
// 记录 Iceberg 特有的分区转换元数据：通过内部静态类 MergePartitionField 记录了 Iceberg 的 Partition Transform（如 identity、bucket、truncate、year、day 等）及字段嵌套路径。
// 指导 Exchange 算子生成：物理优化阶段，DistributionSpecMerge 会指示物理执行引擎生成特化的 Shuffle 节点（如按照 operation_type 和不同的分区表达式进行的分支哈希路由/Shuffle），确保行数据被准确分发到正确的 Writer 节点。
public class DistributionSpecMerge extends DistributionSpec {
    /**
     * Iceberg partition field metadata for merge insert routing.
     */
    // 用于描述 Iceberg 分区字段（Partition Field）与数据源列之间的转换关系及元数据。
    public static class MergePartitionField {
        // Iceberg 分区转换函数名称。例如 "identity"、"bucket"、"truncate"、"year"、"month"、"day" 等。
        private final String transform;
        // 数据源列表达式的唯一 ID。标识当前分区字段是从哪个输入列（Slot）计算得来的。
        private final ExprId sourceExprId;
        // 转换函数的参数（可为空）。例如 Bucket 分区的桶数（如 Bucket[16] 的 16）或 Truncate 分区的截断长度。
        private final Integer param;
        // 分区字段的名称（可为空）。在 Iceberg Schema 中定义的 Partition Field Name。
        private final String name;
        // Iceberg Field ID（可为空）。对应 Iceberg 表 Schema 中原列的字段 ID（Field ID）。
        private final Integer sourceId;
        // 嵌套字段路径。如果源列是一个 Struct/Nested 类型，该列表按顺序记录了访问嵌套字段的 Field ID 路径。
        private final ImmutableList<Integer> sourceFieldPath;

        /**
         * Create a partition field mapping for merge insert routing.
         */
        public MergePartitionField(String transform, ExprId sourceExprId, Integer param,
                String name, Integer sourceId) {
            this(transform, sourceExprId, param, name, sourceId, ImmutableList.of());
        }

        /** Create a partition field mapping whose source is nested below a top-level slot. */
        public MergePartitionField(String transform, ExprId sourceExprId, Integer param,
                String name, Integer sourceId, List<Integer> sourceFieldPath) {
            this.transform = Objects.requireNonNull(transform, "transform should not be null");
            this.sourceExprId = Objects.requireNonNull(sourceExprId, "sourceExprId should not be null");
            this.param = param;
            this.name = name;
            this.sourceId = sourceId;
            this.sourceFieldPath = ImmutableList.copyOf(sourceFieldPath);
        }

        public String getTransform() {
            return transform;
        }

        public ExprId getSourceExprId() {
            return sourceExprId;
        }

        public Integer getParam() {
            return param;
        }

        public String getName() {
            return name;
        }

        public Integer getSourceId() {
            return sourceId;
        }

        public List<Integer> getSourceFieldPath() {
            return sourceFieldPath;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MergePartitionField that = (MergePartitionField) o;
            return transform.equals(that.transform)
                    && sourceExprId.equals(that.sourceExprId)
                    && Objects.equals(param, that.param)
                    && Objects.equals(name, that.name)
                    && Objects.equals(sourceId, that.sourceId)
                    && sourceFieldPath.equals(that.sourceFieldPath);
        }

        @Override
        public int hashCode() {
            return Objects.hash(transform, sourceExprId, param, name, sourceId, sourceFieldPath);
        }
    }
    // 操作类型列的表达式 ID。标识当前数据行属于什么操作（如 0 代表 INSERT，1 代表 DELETE，2 代表 UPDATE）。Shuffle 节点根据该列的值决定走哪条路由分支。
    private final ExprId operationExprId;
    // INSERT 操作的分区键表达式 ID 列表。当行类型为 INSERT 时，用于计算数据应 Hash/Shuffle 到哪个节点的列。
    private final ImmutableList<ExprId> insertPartitionExprIds;
    // DELETE 操作的分区键表达式 ID 列表。当行类型为 DELETE/UPDATE 时，用于路由到对应物理数据文件/分区节点的列。
    private final ImmutableList<ExprId> deletePartitionExprIds;
    // INSERT 行是否随机分布。如果为 true，表示 INSERT 数据不需要强行按分区键哈希 Shuffle（例如目标表无分区或开启了随机写入优化），直接 Random / Round-robin 即可。
    private final boolean insertRandom;
    // INSERT 分区转换元数据列表。
    // 与 insertPartitionExprIds 对应，记录具体的 Iceberg Transform 映射。
    private final ImmutableList<MergePartitionField> insertPartitionFields;
    // Iceberg Partition Spec ID（可为空）。记录当前写操作所使用的 Iceberg Partition Spec 版本号（针对 Evolution 演进后的 Schema）。
    private final Integer partitionSpecId;

    /**
     * Create merge distribution spec for Iceberg DML routing.
     */
    public DistributionSpecMerge(ExprId operationExprId, List<ExprId> insertPartitionExprIds,
            List<ExprId> deletePartitionExprIds, boolean insertRandom,
            List<MergePartitionField> insertPartitionFields, Integer partitionSpecId) {
        this.operationExprId = Objects.requireNonNull(operationExprId, "operationExprId should not be null");
        this.insertPartitionExprIds = ImmutableList.copyOf(
                Objects.requireNonNull(insertPartitionExprIds, "insertPartitionExprIds should not be null"));
        this.deletePartitionExprIds = ImmutableList.copyOf(
                Objects.requireNonNull(deletePartitionExprIds, "deletePartitionExprIds should not be null"));
        this.insertRandom = insertRandom;
        this.insertPartitionFields = ImmutableList.copyOf(
                Objects.requireNonNull(insertPartitionFields, "insertPartitionFields should not be null"));
        this.partitionSpecId = partitionSpecId;
        Preconditions.checkState(!deletePartitionExprIds.isEmpty(), "deletePartitionExprIds should not be empty");
    }

    public ExprId getOperationExprId() {
        return operationExprId;
    }

    public List<ExprId> getInsertPartitionExprIds() {
        return insertPartitionExprIds;
    }

    public List<ExprId> getDeletePartitionExprIds() {
        return deletePartitionExprIds;
    }

    public boolean isInsertRandom() {
        return insertRandom;
    }

    public List<MergePartitionField> getInsertPartitionFields() {
        return insertPartitionFields;
    }

    public Integer getPartitionSpecId() {
        return partitionSpecId;
    }

    @Override
    public boolean satisfy(DistributionSpec required) {
        if (required instanceof DistributionSpecAny) {
            return true;
        }
        if (!(required instanceof DistributionSpecMerge)) {
            return false;
        }
        DistributionSpecMerge other = (DistributionSpecMerge) required;
        return insertRandom == other.insertRandom
                && operationExprId.equals(other.operationExprId)
                && insertPartitionExprIds.equals(other.insertPartitionExprIds)
                && deletePartitionExprIds.equals(other.deletePartitionExprIds)
                && insertPartitionFields.equals(other.insertPartitionFields)
                && Objects.equals(partitionSpecId, other.partitionSpecId);
    }

    @Override
    public String shapeInfo() {
        return "DistributionSpecMerge";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DistributionSpecMerge that = (DistributionSpecMerge) o;
        return insertRandom == that.insertRandom
                && operationExprId.equals(that.operationExprId)
                && insertPartitionExprIds.equals(that.insertPartitionExprIds)
                && deletePartitionExprIds.equals(that.deletePartitionExprIds)
                && insertPartitionFields.equals(that.insertPartitionFields)
                && Objects.equals(partitionSpecId, that.partitionSpecId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operationExprId, insertPartitionExprIds, deletePartitionExprIds,
                insertRandom, insertPartitionFields, partitionSpecId);
    }
}
