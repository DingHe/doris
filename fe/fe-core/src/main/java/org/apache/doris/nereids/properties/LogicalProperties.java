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

import org.apache.doris.common.Id;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.util.LazyCompute;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Logical properties used for analysis and optimize in Nereids.
 */
// LogicalProperties（逻辑属性）是逻辑计划节点（LogicalPlan）和 Memo 数据结构中 Group 节点的核心元数据载体。
// 在 Cascades / Volcano 优化器框架中，查询树节点分为逻辑属性（Logical Properties）和物理属性（Physical Properties）：
// 逻辑属性（Logical Properties）：是逻辑等价（Logical Equivalence）的基石。对于 Memo 结构中同一个 Group 下的所有表达式（GroupExpression），它们无论经过怎样的等价变换（如 Join 重排、谓词下推），其推导出的逻辑属性必须完全相同。
// 记录与缓存输出列信息：管理当前逻辑节点向外暴露的列集合（Slot），包括列表、集合、Map 映射、表达式 ID（ExprId）集合等多种方便高效查询的数据结构。
// 支持惰性计算（Lazy Evaluation）与性能优化：由于解析阶段（Analysis）部分子节点尚未 Bind，或计划树在变换过程中高频调用属性，LogicalProperties 通过 LazyCompute / Supplier 实现按需计算与结果缓存（Memorization）。
// 处理 SELECT * 逻辑：维护通配符（Asterisk）展开时的特定输出列（例如应对语法糖或未解绑表列的情况）。
// 维护数据特征（DataTrait）：保存列与列之间的函数依赖关系（Functional Dependencies）、唯一键（Unique Keys）以及 Equal/NotNull 关系，为谓词消除、 Join 重写等优化提供依据。
// 支持等价性判断与 DpHyper 重写：提供判断两个逻辑属性是否一致的 equals 和 hashCode 实现，并针对 DpHyper Join Reorder 算法提供忽略 Nullability 的特定等价判断。
public class LogicalProperties {
    // 原始输出列列表的惰性提供者。返回当前节点有序输出的 Slot 列表。通过 LazyCompute 包装，确保只计算一次并进行缓存。
    protected final Supplier<List<Slot>> outputSupplier;
    // 输出列表达式 ID 列表的提供者。将 outputSupplier 产生的每一个 Slot 提取出 ExprId（向下转型为 Id<?>），按原顺序构成的不可变列表（ImmutableList）。
    protected final Supplier<List<Id<?>>> outputExprIdsSupplier;
    // 输出列 Set 集合的提供者。将 List<Slot> 转换为不可变集合（ImmutableSet），用于高频的 O(1) 列包含判断（contains）。
    protected final Supplier<Set<Slot>> outputSetSupplier;
    // 输出列自映射 Map 的提供者。构建一个 Key 和 Value 都是 Slot 本身的不可变 Map（ImmutableMap）。主要用于 equals 方法中快速根据 Key 查找到另一个对象的对应 Slot。
    protected final Supplier<Map<Slot, Slot>> outputMapSupplier;
    // 输出列表达式 ID 集合（Set）的提供者。提取所有输出 Slot 的 ExprId 构成的不可变 Set，主要用于 hashCode() 计算和集合重叠度校验。
    protected final Supplier<Set<ExprId>> outputExprIdSetSupplier;
    // 通配符（SELECT *）输出列表的提供者。大部分情况下与 outputSupplier 相同，但在特定未解绑（Unbound）节点或需要特殊处理 SELECT * 的语法节点时提供定制列表。
    protected final Supplier<List<Slot>> asteriskOutputSupplier;
    // 数据特征（DataTrait）的提供者。延迟推导当前节点数据流的完整性约束、唯一键约束（Unique Key）、空值属性及列之间的相等推导关系（FD/FD Propagation）。
    protected final Supplier<DataTrait> dataTraitSupplier;
    private Integer hashCode = null;

    /**
     * constructor when output same as asterisk's output.
     */
    public LogicalProperties(Supplier<List<Slot>> outputSupplier, Supplier<DataTrait> dataTraitSupplier) {
        // the second parameters should be null to reuse memorized output supplier
        this(outputSupplier, null, dataTraitSupplier);
    }

    /**
     * constructor of LogicalProperties.
     *
     * @param outputSupplier provide the output. Supplier can lazy compute output without
     *                       throw exception for which children have UnboundRelation
     * @param asteriskOutputSupplier provide the output when do select *.
     * @param dataTraitSupplier provide the data trait.
     */
    public LogicalProperties(
            Supplier<List<Slot>> outputSupplier,
            Supplier<List<Slot>> asteriskOutputSupplier,
            Supplier<DataTrait> dataTraitSupplier) {
        this.outputSupplier = LazyCompute.of(
                Objects.requireNonNull(outputSupplier, "outputSupplier can not be null")
        );
        this.outputExprIdsSupplier = LazyCompute.of(() -> {
            List<Slot> output = this.outputSupplier.get();
            ImmutableList.Builder<Id<?>> exprIdSet
                    = ImmutableList.builderWithExpectedSize(output.size());
            for (Slot slot : output) {
                exprIdSet.add(slot.getExprId());
            }
            return exprIdSet.build();
        });
        this.outputSetSupplier = LazyCompute.of(() -> {
            List<Slot> output = this.outputSupplier.get();
            ImmutableSet.Builder<Slot> slots = ImmutableSet.builderWithExpectedSize(output.size());
            for (Slot slot : output) {
                slots.add(slot);
            }
            return slots.build();
        });
        this.outputMapSupplier = LazyCompute.of(() -> {
            Set<Slot> slots = this.outputSetSupplier.get();
            ImmutableMap.Builder<Slot, Slot> map = ImmutableMap.builderWithExpectedSize(slots.size());
            for (Slot slot : slots) {
                map.put(slot, slot);
            }
            return map.build();
        });
        this.outputExprIdSetSupplier = LazyCompute.of(() -> {
            List<Slot> output = this.outputSupplier.get();
            ImmutableSet.Builder<ExprId> exprIdSet
                    = ImmutableSet.builderWithExpectedSize(output.size());
            for (Slot slot : output) {
                exprIdSet.add(slot.getExprId());
            }
            return exprIdSet.build();
        });
        this.asteriskOutputSupplier = asteriskOutputSupplier == null ? this.outputSupplier : LazyCompute.of(
                Objects.requireNonNull(asteriskOutputSupplier, "asteriskOutputSupplier can not be null")
        );
        this.dataTraitSupplier = LazyCompute.of(
                Objects.requireNonNull(dataTraitSupplier, "Data Trait can not be null")
        );
    }

    public List<Slot> getOutput() {
        return outputSupplier.get();
    }

    public Set<Slot> getOutputSet() {
        return outputSetSupplier.get();
    }

    public Map<Slot, Slot> getOutputMap() {
        return outputMapSupplier.get();
    }

    public Set<ExprId> getOutputExprIdSet() {
        return outputExprIdSetSupplier.get();
    }

    public List<Id<?>> getOutputExprIds() {
        return outputExprIdsSupplier.get();
    }

    public List<Slot> getAsteriskOutput() {
        return asteriskOutputSupplier.get();
    }

    public DataTrait getTrait() {
        return dataTraitSupplier.get();
    }

    @Override
    public String toString() {
        return "LogicalProperties{"
                + "\noutputSupplier=" + outputSupplier.get()
                + "\noutputExprIdsSupplier=" + outputExprIdsSupplier.get()
                + "\noutputSetSupplier=" + outputSetSupplier.get()
                + "\noutputMapSupplier=" + outputMapSupplier.get()
                + "\noutputExprIdSetSupplier=" + outputExprIdSetSupplier.get()
                + "\nasteriskOutputSupplier=" + asteriskOutputSupplier.get()
                + "\nhashCode=" + hashCode
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalProperties that = (LogicalProperties) o;
        Set<Slot> thisOutSet = this.outputSetSupplier.get();
        Set<Slot> thatOutSet = that.outputSetSupplier.get();
        if (!Objects.equals(thisOutSet, thatOutSet)) {
            return false;
        }
        for (Slot thisOutSlot : thisOutSet) {
            Slot thatOutSlot = that.getOutputMap().get(thisOutSlot);
            if (thisOutSlot.nullable() != thatOutSlot.nullable()) {
                return false;
            }
        }
        return true;
    }

    /**
     * in dphyper join reorder, we ignore nullability comparison
     */
    public boolean equalsForDpHyper(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalProperties that = (LogicalProperties) o;
        Set<Slot> thisOutSet = this.outputSetSupplier.get();
        Set<Slot> thatOutSet = that.outputSetSupplier.get();
        if (!Objects.equals(thisOutSet, thatOutSet)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        if (hashCode == null) {
            hashCode = Objects.hash(outputExprIdSetSupplier.get());
        }
        return hashCode;
    }
}
