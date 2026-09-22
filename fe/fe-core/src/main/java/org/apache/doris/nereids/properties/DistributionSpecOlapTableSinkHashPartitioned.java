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
 * use for shuffle data by tablet-id before sink.
 */
// DistributionSpecOlapTableSinkHashPartitioned 是专门为 Doris 内表数据写入（Olap Table Sink / 向 Doris Olap 表写入数据）设计的一种物理数据分布规格（Distribution Spec）。
// 在向 Doris 内表（OlapTable）执行数据写入（如 INSERT INTO、STREAM LOAD 或 Routine Load 内部执行计划）时，Doris 的数据存储架构是按照 Partition（分区） 和 Tablet（分桶） 进行组织和管理的：
// 按照 Tablet ID 进行数据预 Hash Shuffle：
// 注释明确提到：use for shuffle data by tablet-id before sink。在进入真正的 OlapTableSink 执行算子写盘之前，如果数据在各个 BE 节点间散乱分布，每个 BE 节点都需要向所有的 Tablet 节点发送数据，这会产生极高的网络连接开销和随机 I/O 吞吐瓶颈。
// 提高写入吞吐与内存利用率：
// 通过下发 DistributionSpecOlapTableSinkHashPartitioned 物理要求，优化器会在 OlapTableSink 算子之前插入一个按 Tablet ID（或分桶键）进行 Hash 分发/Shuffle 的 PhysicalDistribute 节点。
// 数据在 Shuffle 后，相同 Tablet 的数据会被归集到指定的 BE 节点/ Writer 线程统一处理。
// 这样可以极大减少 BE 节点同时打开的 Tablet Writer 句柄数量，降低内存消耗，并大幅提升内表批量写入的性能与稳定性。
public class DistributionSpecOlapTableSinkHashPartitioned extends DistributionSpec {

    public static final DistributionSpecOlapTableSinkHashPartitioned
            INSTANCE = new DistributionSpecOlapTableSinkHashPartitioned();

    private DistributionSpecOlapTableSinkHashPartitioned() {
        super();
    }

    @Override
    public boolean satisfy(DistributionSpec other) {
        return other instanceof DistributionSpecOlapTableSinkHashPartitioned;
    }
}
