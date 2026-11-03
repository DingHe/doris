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

#pragma once

#include <brpc/stream.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/types.pb.h>

#include <vector>

#include "core/custom_allocator.h"

namespace doris {

class TupleDescriptor;
class SlotDescriptor;
class OlapTableSchemaParam;

enum class WriteRequestType {
    DATA = 0,       // data write
    ROW_BINLOG = 1, // row binlog write
    GROUP = 2,      // group write for data and binlog<row>
};

struct WriteRequest {
    int64_t tablet_id = 0;
    int32_t schema_hash = 0;
    int64_t txn_id = 0;
    int64_t txn_expiration = 0; // For cloud mode
    int64_t index_id = 0;
    int64_t partition_id = 0;
    // the row binlog tablet id written together with this base tablet, 0 means no binlog.
    int64_t binlog_tablet_id = 0;
    PUniqueId load_id;
    TupleDescriptor* tuple_desc = nullptr;
    // slots are in order of tablet's schema
    const std::vector<SlotDescriptor*>* slots = nullptr;
    std::shared_ptr<OlapTableSchemaParam> table_schema_param = nullptr;
    bool is_high_priority = false;
    bool write_file_cache = false;
    WriteRequestType write_req_type = WriteRequestType::DATA;
    std::string storage_vault_id;
    bool enable_table_memtable_backpressure = false;
};
// 在 Doris 的数据写入链路中（例如导入任务或 Insert 语句向 Tablet 灌入数据时），上游通常会产生一个包含大量行数的全量 Block。
// 但并不是 Block 中的每一行都需要全部写入，或者需要按特定的行索引顺序写入。
// TabletAddRowsPayload 就是用来精准控制和承载这些待写入行元数据的载荷容器。
struct TabletAddRowsPayload {
    // 行过滤与选择（Row Selection）：通过 row_idxs 指定输入 Block 中哪些行是有效且需要被写入当前 MemTable 的。
    // 待写入行的索引数组。
    // 存储了一组由 uint32_t 表示的行号（Row Indices）。这些行号对应于输入 Block（input_block）中的行位置。在执行 MemTable::insert() 时，Doris 不会盲目复制整个 Block 的所有行，
    // 而是根据 row_idxs 中指定的行索引，精准地把需要写入的行通过 add_rows 批量拷贝到 MemTable 的内部可变块中。这种设计完美支持了上游算子（如过滤条件、质量校验）筛选后的稀疏或按需写入
    DorisVector<uint32_t> row_idxs;
    // 行级 Binlog 关联（Row-level Binlog LSN）：通过 row_binlog_lsns 携带每一行数据对应的逻辑日志序列号（LSN），用于支持行级 Binlog 同步与数据流复制。
    // 当开启了行级 Binlog 功能（即 _need_row_binlog_lsn 为 true）时，每一行被写入的数据都会被赋予一个唯一的 Binlog 序列号。
    // 这个数组与 row_idxs 严格一一对应，用于在 MemTable 内部记录和追踪每行数据的变更日志，供后续生态（如 CDC、主备同步、流式计算等）消费。
    DorisVector<int64_t> row_binlog_lsns = {};
};

} // namespace doris
