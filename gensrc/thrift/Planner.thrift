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

namespace cpp doris
namespace java org.apache.doris.thrift

include "Types.thrift"
include "Exprs.thrift"
include "DataSinks.thrift"
include "PlanNodes.thrift"
include "Partitions.thrift"
include "QueryCache.thrift"

// TPlanFragment encapsulates info needed to execute a particular
// plan fragment, including how to produce and how to partition its output.
// It leaves out node-specific parameters neede for the actual execution.
// TPlanFragment 侧重于描述物理执行计划树本身的算子拓扑、表达式以及数据 Sink 输出策略。
// 定义单 Fragment 内的物理算子树（Plan Tree）：包含当前物理执行片段内部从叶子节点（如 OlapScanNode）到根节点（如 HashJoinNode、AggregateNode）的算子连接结构（TPlan）。
// 定义数据输出策略（Data Sink & Output Partitioning）：指定当前 Fragment 计算出的结果行应该如何输出（是写入存储表，还是通过网络 Shuffle 发送到下一个 Fragment，亦或是直接返回给 FE/客户端）。
// 定义数据分片/分区属性（Data Partition）：标识该 Fragment 在分布式环境中是如何对输入或处理的数据进行切片/分区的（如 UNPARTITIONED、RANDOM、HASH_PARTITIONED）。
// 内存预留与查询缓存：提供单 Instance 运行所需的最低/初始内存 Buffer 预留信息（用于内存分配器的优化），以及 Query Cache（查询缓存）的参数配置。
struct TPlanFragment {
  // no plan or descriptor table: query without From clause
  // 物理计划算子树（Plan Tree）。
  // 包含当前 Fragment 内部的所有执行节点（TPlanNode 列表，如 ScanNode、JoinNode、AggNode 等）。
  2: optional PlanNodes.TPlan plan

  // exprs that produce values for slots of output tuple (one expr per slot);
  // if not set, plan fragment materializes full rows of plan_tree
  // 输出元组 Slot 表达式列表。
  // 用于产生当前 Fragment 输出 Row 包含的各个 Slot 值的表达式（每一个 TExpr 对应输出元组中的某一列）。
  4: optional list<Exprs.TExpr> output_exprs
  
  // Specifies the destination of this plan fragment's output rows.
  // For example, the destination could be a stream sink which forwards 
  // the data to a remote plan fragment, or a sink which writes to a table (for
  // insert stmts).
  // 数据输出 Data Sink。
  // 定义当前 Fragment 算子树顶层的数据去向。例如：
  // - DATA_STREAM_SINK：将数据通过网络/本地队列 Shuffle 传输给上游 Fragment；
  // - OLAP_TABLE_SINK / HIVE_TABLE_SINK：将数据写入 Doris 存储引擎表或外表（用于 INSERT INTO 语句）；
  // - RESULT_SINK：将计算结果收集并回传给 FE/客户端。
  5: optional DataSinks.TDataSink output_sink

  // Partitioning of the data created by all instances of this plan fragment;
  // partitioning.type has the following meaning:
  // - UNPARTITIONED: there is only one instance of the plan fragment
  // - RANDOM: a particular output row is randomly assigned to any of the instances
  // - HASH_PARTITIONED: output row r is produced by
  //   hash_value(partitioning.partitioning_exprs(r)) % #partitions
  // - RANGE_PARTITIONING: currently not supported
  // This is distinct from the partitioning of each plan fragment's
  // output, which is specified by output_sink.output_partitioning.
  // 数据分区/切片方式。
  // 定义生成当前 Fragment 数据的各个 Instance 之间的数据划分规则（注意：这与 output_sink 中的输出路由不同，这里指的是当前 Fragment 自身的输入数据切片属性）。类型包括：
  6: required Partitions.TDataPartition partition

  // The minimum reservation size (in bytes) required for an instance of this plan
  // fragment to execute on a single host.
  // 单 Instance 最小预留内存（字节）。
  // 运行当前 Fragment 的一个物理 Instance 在单台主机上成功执行所需的最低保障 Buffer 字节数，用于内存管理和资源调度器的接纳控制（Admission Control）。
  7: optional i64 min_reservation_bytes

  // Total of the initial buffer reservations that we expect to be claimed by this
  // fragment. I.e. the sum of the min reservations over all operators (including the
  // sink) in a single instance of this fragment. This is used for an optimization in
  // InitialReservation. Measured in bytes. required in V1
  // 初始 Buffer 预留总声明量（字节）。
  // 一个 Instance 内部所有算子（含 Data Sink）初始阶段向内存管理器声明并申请的初始 Buffer 内存总和。用于优化内存预分配与内存管理器的 InitialReservation 阶段。
  8: optional i64 initial_reservation_total_claims
  // 查询缓存参数。
  // 包含当前 Fragment 在使能 Query Cache 时的相关参数（如 Cache Key、缓存切片列映射、过期时间等），用于加速重复查询的命中与读取。
  9: optional QueryCache.TQueryCacheParam query_cache_param
}

// location information for a single scan range
struct TScanRangeLocation {
  1: required Types.TNetworkAddress server

  // disk volume identifier of a particular scan range at 'server';
  // -1 indicates an unknown volume id;
  // only set for TScanRange.hdfs_file_split
  2: optional i32 volume_id = -1
  3: optional i64 backend_id
}

// A single scan range plus the hosts that serve it
struct TScanRangeLocations {
  1: required PlanNodes.TScanRange scan_range
  // non-empty list
  2: list<TScanRangeLocation> locations
}
