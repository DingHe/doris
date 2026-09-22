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

import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;


/**
 * Describe hash distribution.
 */
// DistributionSpecHash 是物理属性（Physical Properties）推导与分布式执行计划生成体系中最核心、也是最复杂的哈希数据分布规格（Hash Distribution Specification）。
// 在分布式 SQL 引擎中，Join（如 Hash Join、Colocate Join、Bucket Shuffle Join）、Aggregate（聚合算子）等分布式算子性能高低极大程度上依赖于数据在节点间的 Hash 分布形态。DistributionSpecHash 的主要作用包括：
// 描述 Hash 分布状态：记录数据流是按哪些列（orderedShuffledColumns）以何种 Hash 方式（shuffleType）重新分布（Shuffle）或原生地（Natural）落盘在节点上的。
// 支持等价列推导与属性传递（Equivalence Set）：在 Join 算子中，如 a.id = b.id，数据按 a.id 做了 Hash Shuffle 后，物理上等同于按 b.id 进行了 Hash Shuffle。DistributionSpecHash 维护了等价列集合（equivalenceExprIds），使物理优化器能够精准识别这种逻辑等价性，从而避免不必要的二次 Shuffle。
// 支持 Colocate Join 与 Bucket Shuffle Join 优化：保存底层 Doris 表的 tableId、selectedIndexId 以及 partitionIds，使得优化器在评估物理算子时，可以精确判断两张表的数据是否满足 Colocate Join（本地关联）或 Bucket Shuffle Join 的分布要求。
// 属性满足性判定（Satisfy Check）：根据上层算子对分布的要求（如严格相等 REQUIRE_EQUAL 或包含即可 REQUIRE），动态判断当前数据流是否需要插入 PhysicalDistribute（Shuffle Exchange）算子。
@Developing
public class DistributionSpecHash extends DistributionSpec {
    // 有序 Hash 分布列列表。按顺序记录了计算 Hash 值所依据的列的 ExprId。
    private final List<ExprId> orderedShuffledColumns;
    // Shuffle 类型。标识当前 Hash 分布是存储原生的、执行引擎 Shuffle 产出的，还是上层算子要求的规则。
    private final ShuffleType shuffleType;
    // use for satisfied judge
    // 等价列集合列表。索引 $i$ 对应 orderedShuffledColumns 中第 $i$ 个位置的列及其所有等价列（例如 [a.id, b.id]）。
    private final List<Set<ExprId>> equivalenceExprIds;
    // 列 ID 到等价集索引的映射表。快速根据某个列的 ExprId 查出它位于 equivalenceExprIds 中的第几个位置（即第几维 Hash Key）。
    private final Map<ExprId, Integer> exprIdToEquivalenceSet;

    // below two attributes use for colocate join, only store one table info is enough
    // 表 ID。
    // 关联的 Doris 物理表 ID（用户 Colocate Join 匹配）。若无相关表则设为 -1L。
    private final long tableId;
    // 分区 ID 集合。当前数据涉及的物理分区集合。如果分区不一致，可能无法触发 Bucket Shuffle / Colocate。
    private final Set<Long> partitionIds;
    // 物化视图/索引 ID。关联的 Table Index ID（如 Rollup / Materialized View），默认为 -1L。
    private final long selectedIndexId;

    /**
     * Use for no need set table related attributes.
     */
    public DistributionSpecHash(List<ExprId> orderedShuffledColumns, ShuffleType shuffleType) {
        this(orderedShuffledColumns, shuffleType, -1L, Collections.emptySet());
    }

    /**
     * Used in ut
     */
    public DistributionSpecHash(List<ExprId> orderedShuffledColumns, ShuffleType shuffleType,
            long tableId, Set<Long> partitionIds) {
        this(orderedShuffledColumns, shuffleType, tableId, -1L, partitionIds);
    }

    /**
     * Normal constructor.
     */
    public DistributionSpecHash(List<ExprId> orderedShuffledColumns, ShuffleType shuffleType,
            long tableId, long selectedIndexId, Set<Long> partitionIds) {
        this.orderedShuffledColumns = ImmutableList.copyOf(
                Objects.requireNonNull(orderedShuffledColumns, "orderedShuffledColumns should not null"));
        this.shuffleType = Objects.requireNonNull(shuffleType, "shuffleType should not null");
        this.partitionIds = ImmutableSet.copyOf(
                Objects.requireNonNull(partitionIds, "partitionIds should not null"));
        this.tableId = tableId;
        this.selectedIndexId = selectedIndexId;
        ImmutableList.Builder<Set<ExprId>> equivalenceExprIdsBuilder
                = ImmutableList.builderWithExpectedSize(orderedShuffledColumns.size());
        ImmutableMap.Builder<ExprId, Integer> exprIdToEquivalenceSetBuilder
                = ImmutableMap.builderWithExpectedSize(orderedShuffledColumns.size());
        int i = 0;
        for (ExprId id : orderedShuffledColumns) {
            equivalenceExprIdsBuilder.add(Sets.newHashSet(id));
            exprIdToEquivalenceSetBuilder.put(id, i++);
        }
        this.equivalenceExprIds = equivalenceExprIdsBuilder.build();
        this.exprIdToEquivalenceSet = exprIdToEquivalenceSetBuilder.buildKeepingLast();
    }

    /**
     * Used in ut
     */
    public DistributionSpecHash(List<ExprId> orderedShuffledColumns, ShuffleType shuffleType,
            long tableId, Set<Long> partitionIds, List<Set<ExprId>> equivalenceExprIds,
            Map<ExprId, Integer> exprIdToEquivalenceSet) {
        this(orderedShuffledColumns, shuffleType, tableId, -1L, partitionIds,
                equivalenceExprIds, exprIdToEquivalenceSet);
    }

    /**
     * Used in merge outside and put result into it.
     */
    public DistributionSpecHash(List<ExprId> orderedShuffledColumns, ShuffleType shuffleType, long tableId,
            long selectedIndexId, Set<Long> partitionIds, List<Set<ExprId>> equivalenceExprIds,
            Map<ExprId, Integer> exprIdToEquivalenceSet) {
        this.orderedShuffledColumns = ImmutableList.copyOf(Objects.requireNonNull(orderedShuffledColumns,
                "orderedShuffledColumns should not null"));
        this.shuffleType = Objects.requireNonNull(shuffleType, "shuffleType should not null");
        this.tableId = tableId;
        this.selectedIndexId = selectedIndexId;
        this.partitionIds = ImmutableSet.copyOf(
                Objects.requireNonNull(partitionIds, "partitionIds should not null"));
        this.equivalenceExprIds = ImmutableList.copyOf(
                Objects.requireNonNull(equivalenceExprIds, "equivalenceExprIds should not null"));
        this.exprIdToEquivalenceSet = ImmutableMap.copyOf(
                Objects.requireNonNull(exprIdToEquivalenceSet, "exprIdToEquivalenceSet should not null"));
    }

    static DistributionSpecHash merge(DistributionSpecHash left, DistributionSpecHash right, ShuffleType shuffleType) {
        List<ExprId> orderedShuffledColumns = left.getOrderedShuffledColumns();
        ImmutableList.Builder<Set<ExprId>> equivalenceExprIds
                = ImmutableList.builderWithExpectedSize(orderedShuffledColumns.size());
        for (int i = 0; i < orderedShuffledColumns.size(); i++) {
            ImmutableSet.Builder<ExprId> equivalenceExprId = ImmutableSet.builderWithExpectedSize(
                    left.getEquivalenceExprIds().get(i).size() + right.getEquivalenceExprIds().get(i).size());
            equivalenceExprId.addAll(left.getEquivalenceExprIds().get(i));
            equivalenceExprId.addAll(right.getEquivalenceExprIds().get(i));
            equivalenceExprIds.add(equivalenceExprId.build());
        }
        ImmutableMap.Builder<ExprId, Integer> exprIdToEquivalenceSet = ImmutableMap.builderWithExpectedSize(
                left.getExprIdToEquivalenceSet().size() + right.getExprIdToEquivalenceSet().size());
        exprIdToEquivalenceSet.putAll(left.getExprIdToEquivalenceSet());
        exprIdToEquivalenceSet.putAll(right.getExprIdToEquivalenceSet());
        return new DistributionSpecHash(orderedShuffledColumns, shuffleType,
                left.getTableId(), left.getSelectedIndexId(), left.getPartitionIds(), equivalenceExprIds.build(),
                exprIdToEquivalenceSet.buildKeepingLast());
    }

    static DistributionSpecHash merge(DistributionSpecHash left, DistributionSpecHash right) {
        return merge(left, right, left.getShuffleType());
    }

    public List<ExprId> getOrderedShuffledColumns() {
        return orderedShuffledColumns;
    }

    public ShuffleType getShuffleType() {
        return shuffleType;
    }

    public long getTableId() {
        return tableId;
    }

    public long getSelectedIndexId() {
        return selectedIndexId;
    }

    public Set<Long> getPartitionIds() {
        return partitionIds;
    }

    public List<Set<ExprId>> getEquivalenceExprIds() {
        return equivalenceExprIds;
    }

    public Map<ExprId, Integer> getExprIdToEquivalenceSet() {
        return exprIdToEquivalenceSet;
    }

    public Set<ExprId> getEquivalenceExprIdsOf(ExprId exprId) {
        if (exprIdToEquivalenceSet.containsKey(exprId)) {
            return equivalenceExprIds.get(exprIdToEquivalenceSet.get(exprId));
        }
        return new HashSet<>();
    }

    @Override
    public boolean satisfy(DistributionSpec required) {
        if (required instanceof DistributionSpecAny) {
            return true;
        }

        if (!(required instanceof DistributionSpecHash)) {
            return false;
        }

        DistributionSpecHash requiredHash = (DistributionSpecHash) required;

        if (this.orderedShuffledColumns.size() > requiredHash.orderedShuffledColumns.size()) {
            return false;
        }

        if (requiredHash.getShuffleType() == ShuffleType.REQUIRE) {
            return containsSatisfy(requiredHash.getOrderedShuffledColumns());
        }
        return requiredHash.getShuffleType() == this.getShuffleType()
                && equalsSatisfy(requiredHash.getOrderedShuffledColumns());
    }

    private boolean containsSatisfy(List<ExprId> required) {
        BitSet containsBit = new BitSet(orderedShuffledColumns.size());
        required.forEach(e -> {
            if (exprIdToEquivalenceSet.containsKey(e)) {
                containsBit.set(exprIdToEquivalenceSet.get(e));
            }
        });
        return containsBit.nextClearBit(0) >= orderedShuffledColumns.size();
    }

    private boolean equalsSatisfy(List<ExprId> required) {
        if (equivalenceExprIds.size() != required.size()) {
            return false;
        }
        for (int i = 0; i < required.size(); i++) {
            if (!equivalenceExprIds.get(i).contains(required.get(i))) {
                return false;
            }
        }
        return true;
    }

    public DistributionSpecHash withShuffleType(ShuffleType shuffleType) {
        return new DistributionSpecHash(orderedShuffledColumns, shuffleType, tableId, selectedIndexId, partitionIds,
                equivalenceExprIds, exprIdToEquivalenceSet);
    }

    public DistributionSpecHash withShuffleTypeAndForbidColocateJoin(ShuffleType shuffleType) {
        return new DistributionSpecHash(orderedShuffledColumns, shuffleType, -1, -1, partitionIds,
                equivalenceExprIds, exprIdToEquivalenceSet);
    }

    /**
     * Drops unused hash-shuffle slots and keeps some of the original slots. The list you pass is the new
     * shuffle column order. Each ExprId must already be in exprIdToEquivalenceSet, and you must not pick
     * two ExprIds that belong to the same original slot.
     * Example: orderedShuffledColumns(e1, e2, e3), with equivalenceExprIds (e1,e4), (e2,e5), (e3,e6).
     * exprIdToEquivalenceSet is(e1:0,e4:0,e2:1,e5:1,e3:2,e6:2)
     * prunedOrderedColumns is (e2) yields
     * orderedShuffledColumns(e2),with equivalenceExprIds(e2,e5).
     * exprIdToEquivalenceSet is(e2:0,e5:0)
     */
    public DistributionSpecHash withShuffleExprs(List<ExprId> prunedOrderedColumns) {
        Objects.requireNonNull(prunedOrderedColumns, "prunedOrderedColumns");
        int k = prunedOrderedColumns.size();
        List<Integer> origIndices = Lists.newArrayListWithCapacity(k);
        for (ExprId exprId : prunedOrderedColumns) {
            origIndices.add(exprIdToEquivalenceSet.get(exprId));
        }
        ImmutableList.Builder<Set<ExprId>> equivBuilder = ImmutableList.builderWithExpectedSize(k);
        ImmutableMap.Builder<ExprId, Integer> mapBuilder = ImmutableMap.builder();
        for (int newIdx = 0; newIdx < k; newIdx++) {
            int origIdx = origIndices.get(newIdx);
            Set<ExprId> equiv = equivalenceExprIds.get(origIdx);
            equivBuilder.add(ImmutableSet.copyOf(equiv));
            for (ExprId id : equiv) {
                mapBuilder.put(id, newIdx);
            }
        }
        return new DistributionSpecHash(ImmutableList.copyOf(prunedOrderedColumns),
                shuffleType, tableId, selectedIndexId, partitionIds, equivBuilder.build(),
                mapBuilder.buildKeepingLast());
    }

    /**
     * generate a new DistributionSpec after projection.
     */
    public DistributionSpec project(Map<ExprId, ExprId> projections,
            Set<ExprId> obstructions, DistributionSpec defaultAnySpec) {
        List<ExprId> orderedShuffledColumns = Lists.newArrayList();
        List<Set<ExprId>> equivalenceExprIds = Lists.newArrayList();
        Map<ExprId, Integer> exprIdToEquivalenceSet = Maps.newHashMap();
        for (ExprId shuffledColumn : this.orderedShuffledColumns) {
            if (obstructions.contains(shuffledColumn)) {
                return defaultAnySpec;
            }
            orderedShuffledColumns.add(projections.getOrDefault(shuffledColumn, shuffledColumn));
        }
        for (Set<ExprId> equivalenceSet : this.equivalenceExprIds) {
            Set<ExprId> projectionEquivalenceSet = Sets.newHashSet();
            for (ExprId equivalence : equivalenceSet) {
                if (obstructions.contains(equivalence)) {
                    return defaultAnySpec;
                }
                projectionEquivalenceSet.add(projections.getOrDefault(equivalence, equivalence));
            }
            equivalenceExprIds.add(projectionEquivalenceSet);
        }
        for (Map.Entry<ExprId, Integer> exprIdSetKV : this.exprIdToEquivalenceSet.entrySet()) {
            if (obstructions.contains(exprIdSetKV.getKey())) {
                return defaultAnySpec;
            }
            if (projections.containsKey(exprIdSetKV.getKey())) {
                exprIdToEquivalenceSet.put(projections.get(exprIdSetKV.getKey()), exprIdSetKV.getValue());
            } else {
                exprIdToEquivalenceSet.put(exprIdSetKV.getKey(), exprIdSetKV.getValue());
            }
        }
        return new DistributionSpecHash(orderedShuffledColumns, shuffleType, tableId, selectedIndexId, partitionIds,
                equivalenceExprIds, exprIdToEquivalenceSet);
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        DistributionSpecHash that = (DistributionSpecHash) o;
        return shuffleType == that.shuffleType && orderedShuffledColumns.equals(that.orderedShuffledColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shuffleType, orderedShuffledColumns);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("DistributionSpecHash",
                "orderedShuffledColumns", orderedShuffledColumns,
                "shuffleType", shuffleType,
                "tableId", tableId,
                "selectedIndexId", selectedIndexId,
                "partitionIds", partitionIds,
                "equivalenceExprIds", equivalenceExprIds,
                "exprIdToEquivalenceSet", exprIdToEquivalenceSet);
    }

    /**
     * Enums for concrete shuffle type.
     */
    public enum ShuffleType {
        // require, need to satisfy the distribution spec by contains.
        // （要求类型） 上层物理算子向下传递的要求，表示只要数据按包含指定列的子集进行 Hash 分布即可满足。
        REQUIRE,
        // output, execution only could be done on the node with data
        // （产出类型） 数据来自于存储层原生分布（如从 Scan 算子直接读取的数据）。
        NATURAL,
        // output, for shuffle by execution hash method
        // （产出类型） 数据是通过执行引擎的 Hash Shuffle 逻辑重新打散分发后的状态。
        EXECUTION_BUCKETED,
        // output, for shuffle by storage hash method
        // （产出类型） 数据分布严格契合 Doris 存储层分桶规则的状态（用于 Colocate Join 等场景）。
        STORAGE_BUCKETED,
        // require, need to satisfy the distribution spec by equals.
        // （要求类型） 上层物理算子要求的严格匹配类型，表示数据分布必须与要求的列数及顺序完全相等。
        REQUIRE_EQUAL
    }
}
