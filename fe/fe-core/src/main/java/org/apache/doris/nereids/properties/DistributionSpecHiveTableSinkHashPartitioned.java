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

import java.util.List;
import java.util.Objects;

/**
 * use for shuffle data by partition keys before sink.
 */
// DistributionSpecHiveTableSinkHashPartitioned 是专为外表（尤其是 Hive Table Sink / 写入 Hive 动态分区表）设计的一种物理数据分布规格（Distribution Spec）。
// 在向 Hive 外表执行 INSERT INTO / OVERWRITE 数据写入（Table Sink）时，如果目标表定义了分区键（Partition Keys），写数据通常需要处理“动态分区写入（Dynamic Partition Insert）”的问题：
// 避免产生海量小文件和内存溢出（OOM）：
// 如果上游数据没有按分区键进行重新分布（Shuffle），每个 BE 节点/Writer 线程可能会同时收到属于所有不同分区的数据。这会导致每个 BE 节点同时打开成百上千个分区的 Writer 句柄，极易引发 BE 内存溢出（OOM） 或向 HDFS/S3 写入大量碎小文件。
// 在 Sink 算子前强制按分区键进行 Hash Shuffle：
// 为了保证同一个 Hive 分区的数据集中分发到特定的 BE 节点上处理，Nereids 优化器会在 Hive Table Sink 物理算子之前下发 DistributionSpecHiveTableSinkHashPartitioned 分布要求。
// 指示物理计划生成 Exchange 节点：
// 优化器捕获到该物理规格后，会根据指定的输出列/分区列（outputColExprIds）在 Hive Table Sink 之前插入一个按 Hive 分区键 Hash 分发的 PhysicalDistribute (Exchange) 算子，确保写入高效且稳定。

public class DistributionSpecHiveTableSinkHashPartitioned extends DistributionSpec {
    // Hive Partition 列的表达式 ID 列表。
    // 记录了用于计算 Hash 分发路由的目标分区列（或需要 Sink 输出列中用于分区的列）对应的 ExprId。物理 Exchange 算子将依据这些列的哈希值对数据行进行节点间分发。
    private List<ExprId> outputColExprIds;

    public DistributionSpecHiveTableSinkHashPartitioned() {
        super();
    }

    public List<ExprId> getOutputColExprIds() {
        return outputColExprIds;
    }

    public void setOutputColExprIds(List<ExprId> outputColExprIds) {
        this.outputColExprIds = outputColExprIds;
    }

    @Override
    public boolean satisfy(DistributionSpec other) {
        return other instanceof DistributionSpecHiveTableSinkHashPartitioned;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        DistributionSpecHiveTableSinkHashPartitioned that = (DistributionSpecHiveTableSinkHashPartitioned) o;
        return Objects.equals(outputColExprIds, that.outputColExprIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), outputColExprIds);
    }
}
