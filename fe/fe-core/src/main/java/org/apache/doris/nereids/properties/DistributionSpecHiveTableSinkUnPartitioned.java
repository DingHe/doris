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

/**
 * use for Round Robin by data sink.
 */
// DistributionSpecHiveTableSinkUnPartitioned 是专门为外表数据写入（Hive Unpartitioned Table Sink / 向无分区的 Hive 表写入数据）设计的一种物理数据分布规格（Distribution Spec）。
// 在向 Hive 外表执行 INSERT INTO 或 INSERT OVERWRITE 写入数据时，目标 Hive 表有两种常见类型：带分区的表（Partitioned Table）和无分区的表（Unpartitioned Table）。
// 针对无分区 Hive 表的数据分发：
// 对于无分区的 Hive 表，由于写入时不依赖任何特定分区键（Partition Keys）进行过滤或分流，数据不需要按 Hash 散列到指定节点。
// 采用轮询/随机分发（Round-Robin）平衡写入负载：
// 注释明确提到：use for Round Robin by data sink。当写入目标是没有分区的 Hive 表时，优化器会下发 DistributionSpecHiveTableSinkUnPartitioned 规格，指示执行引擎在 Data Sink 之前采用 Round-Robin（轮询/随机） 或均衡分发的策略将数据打散发送到各个 BE / Writer 节点。
// 提升并发写入吞吐并避免单点瓶颈：
// 通过 Round-Robin 分发，能够确保数据均匀地流向各个节点的写入线程，避免上游计算数据倾斜到某个单节点，从而充分利用集群的多节点 I/O 并发能力，提升向外表写文件的整体吞吐量。
public class DistributionSpecHiveTableSinkUnPartitioned extends DistributionSpec {

    public static final DistributionSpecHiveTableSinkUnPartitioned INSTANCE =
            new DistributionSpecHiveTableSinkUnPartitioned();

    private DistributionSpecHiveTableSinkUnPartitioned() {
        super();
    }

    @Override
    public boolean satisfy(DistributionSpec other) {
        return other instanceof DistributionSpecHiveTableSinkUnPartitioned;
    }
}
