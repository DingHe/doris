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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/DescriptorTable.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.common.IdGenerator;

import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.HashMap;

/**
 * Repository for tuple (and slot) descriptors.
 * Descriptors should only be created through this class, which assigns
 * them unique ids.
 */
// 在 Apache Doris 的查询编译与执行流程中，SQL 语句涉及的数据列和行结构会被抽象为：
// SlotDescriptor（槽位描述符）：代表单个数据列或表达式计算出的列字段（包含类型、是否为空、列名等信息）。
// TupleDescriptor（元组描述符）：代表一组 Slot 的集合（即一行数据的 Schema 或某个算子输出的数据行元组）。
// DescriptorTable 的核心作用包括：
// 统一管理 Tuple 和 Slot 的生成与分配：
// 类上的注释明确指出：Descriptors should only be created through this class, which assigns them unique ids.（所有的描述符都应当且仅能通过此类创建，由它为其分配全局唯一的 ID）。
// 保证全局唯一 ID 分配：
// 内部维护 TupleId 和 SlotId 的生成器，防止同一查询树中出现 ID 冲突或重复分配的情况。
// 维护结构层级与物理映射关系：
// 保持 TupleId -> TupleDescriptor 以及 SlotId -> SlotDescriptor 的双向及父子映射管理，为后续将 FE 的查询计划序列化（通过 Thrift）发送给 BE（后端执行引擎）提供标准的 Schema 基础。
//
public class DescriptorTable {
    // 存储当前查询中创建的所有 TupleDescriptor（元组描述符）。以全局唯一的 TupleId 为 Key，TupleDescriptor 对象实例为 Value，用于根据 ID 快速查找和管理 Tuple。
    private final HashMap<TupleId, TupleDescriptor> tupleDescs = new HashMap<TupleId, TupleDescriptor>();
    // TupleId 的唯一 ID 生成器。在当前 DescriptorTable 实例生命周期内，每次创建新的 TupleDescriptor 时生成一个自增且全局唯一的 TupleId。
    private final IdGenerator<TupleId> tupleIdGenerator = TupleId.createGenerator();
    // SlotId 的唯一 ID 生成器。在当前 DescriptorTable 实例生命周期内，每次创建新的 SlotDescriptor 时生成一个自增且全局唯一的 SlotId。
    private final IdGenerator<SlotId> slotIdGenerator = SlotId.createGenerator();
    // 存储当前查询中创建的所有 SlotDescriptor（槽位描述符）。以全局唯一的 SlotId 为 Key，SlotDescriptor 对象实例为 Value，用于根据 ID 快速检索 Slot 信息。
    private final HashMap<SlotId, SlotDescriptor> slotDescs = Maps.newHashMap();

    public DescriptorTable() {
    }

    public TupleDescriptor createTupleDescriptor() {
        TupleDescriptor d = new TupleDescriptor(tupleIdGenerator.getNextId());
        tupleDescs.put(d.getId(), d);
        return d;
    }

    public SlotDescriptor addSlotDescriptor(TupleDescriptor d) {
        SlotDescriptor result = new SlotDescriptor(slotIdGenerator.getNextId(), d.getId());
        d.addSlot(result);
        slotDescs.put(result.getId(), result);
        return result;
    }

    public TupleDescriptor getTupleDesc(TupleId id) {
        return tupleDescs.get(id);
    }

    public Collection<TupleDescriptor> getTupleDescs() {
        return tupleDescs.values();
    }

    public String getExplainString() {
        StringBuilder out = new StringBuilder();
        out.append("\nTuples:\n");
        for (TupleDescriptor desc : tupleDescs.values()) {
            out.append(desc.getExplainString()).append("\n");
        }
        return out.toString();
    }
}
