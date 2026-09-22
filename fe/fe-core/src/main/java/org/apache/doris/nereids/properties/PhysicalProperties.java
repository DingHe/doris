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

import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Physical properties used in cascades. If upstream mismatches downstream, a PhysicalDistribute will be generated.
 */
// PhysicalProperties 是 Apache Doris Nereids 新优化器框架中用于集合封装与管理物理属性（Physical Property）的核心类。
// 在 Cascades 优化器架构中，物理计划算子不仅传输语义数据，还会传递数据的物理形态描述。PhysicalProperties 包含两个核心维度：
// 数据分布规则 (DistributionSpec)：指定数据在分布式节点/线程间是如何分发的（如 Hash 校验分发、单点 Gather 汇聚、广播 Broadcast 等）。
// 数据排序规则 (OrderSpec)：指定数据在当前节点内部是否按照某些列具备有序性（如 ORDER BY 或 Index 顺序）。
public class PhysicalProperties {
    // 表示无任何要求的物理属性（任意分布 + 无序）。
    // 通常作为父算子对子算子没有特定分布和排序要求时的默认 Require 属性。
    public static PhysicalProperties ANY = new PhysicalProperties();
    // 表示存储层（Storage Engine）任意分布要求的物理属性（基于 DistributionSpecStorageAny）。
    // 场景：用于存储引擎/Scan 节点向逻辑层屏蔽具体底层存储分布时的属性抽象。
    public static PhysicalProperties STORAGE_ANY = new PhysicalProperties(DistributionSpecStorageAny.INSTANCE);
    // 作用：表示执行层任意分布要求的物理属性（基于 DistributionSpecExecutionAny）。
    // 场景：表示只关心在 BE 节点执行级别的任意分布。
    public static PhysicalProperties EXECUTION_ANY = new PhysicalProperties(DistributionSpecExecutionAny.INSTANCE);
    // 表示全复制/广播分布属性（基于 DistributionSpecReplicated）。
    // 常见于 Broadcast Hash Join 的 Build（右）子树，要求数据广播复制到所有执行节点。
    public static PhysicalProperties REPLICATED = new PhysicalProperties(DistributionSpecReplicated.INSTANCE);
    // 表示数据全量汇聚到单节点/单线程的物理属性（基于 DistributionSpecGather）。
    // 场景：用于顶级 ResultSink、未分区的 LIMIT 或全局 ORDER BY 等需要单点汇总数据的场景。
    public static PhysicalProperties GATHER = new PhysicalProperties(DistributionSpecGather.INSTANCE);
    // 表示存储层汇总属性（基于 DistributionSpecStorageGather）。
    // 针对存储层下推汇总的特定场景。
    public static PhysicalProperties STORAGE_GATHER = new PhysicalProperties(DistributionSpecStorageGather.INSTANCE);
    // 表示强制要求进行数据重分发（Shuffle）的物理属性（基于 DistributionSpecMustShuffle）。
    // 场景：用于必须重新 Shuffle 数据的物理节点约束。
    public static PhysicalProperties MUST_SHUFFLE = new PhysicalProperties(DistributionSpecMustShuffle.INSTANCE);
    // 作用：表示按 Olap 表的 Tablet ID 进行 Hash 分区 Shuffle 的物理属性（基于 DistributionSpecOlapTableSinkHashPartitioned）。
    // 场景：数据写入/导入 Doris Olap 表节点（OlapTableSink）时使用。
    public static PhysicalProperties TABLET_ID_SHUFFLE
            = new PhysicalProperties(DistributionSpecOlapTableSinkHashPartitioned.INSTANCE);
    // 作用：表示针对 Sink 输出端随机非分区的物理属性（基于 DistributionSpecHiveTableSinkUnPartitioned）。
    // 场景：写入 Hive 外表等不分区目标表时的 Sink 属性。
    public static PhysicalProperties SINK_RANDOM_PARTITIONED
            = new PhysicalProperties(DistributionSpecHiveTableSinkUnPartitioned.INSTANCE);

    // gather then broadcast to all BE with exact one instance
    // 作用：表示先 Gather 汇聚然后再广播到各个只有一个实例的 BE 节点的物理属性（基于 DistributionSpecAllSingleton）。
    public static PhysicalProperties ALL_SINGLETON = new PhysicalProperties(DistributionSpecAllSingleton.INSTANCE);
    // 内部包含的数据排序规格定义对象。
    // 记录当前数据流按哪些列排了序（升序/降序、NULL 值位置等）。
    private final OrderSpec orderSpec;
    // 内部包含的数据物理分布规格定义对象。
    // 记录当前数据流的具体分布模式（如 Hash 键列表、Gather、Broadcast 等）。
    private final DistributionSpec distributionSpec;

    private Integer hashCode = null;

    private PhysicalProperties() {
        this.orderSpec = new OrderSpec();
        this.distributionSpec = DistributionSpecAny.INSTANCE;
    }

    public PhysicalProperties(DistributionSpec distributionSpec) {
        this.distributionSpec = distributionSpec;
        this.orderSpec = new OrderSpec();
    }

    public PhysicalProperties(OrderSpec orderSpec) {
        this.orderSpec = orderSpec;
        this.distributionSpec = DistributionSpecAny.INSTANCE;
    }

    public PhysicalProperties(DistributionSpec distributionSpec, OrderSpec orderSpec) {
        this.distributionSpec = distributionSpec;
        this.orderSpec = orderSpec;
    }

    /**
     * create hash info from orderedShuffledColumns, ignore non slot reference expression.
     */
    // 从表达式集合构建基于 Hash 分布的物理属性，过滤非列引用表达式。
    // 遍历 orderedShuffledColumns 表达式列表，筛选出 SlotReference 类型算子。
    // 提取对应的 ExprId 构建列表。
    // 如果生成的 Slot 列表为空，降级返回 PhysicalProperties.GATHER；否则调用重载的 createHash() 构造 DistributionSpecHash。
    public static PhysicalProperties createHash(
            Collection<? extends Expression> orderedShuffledColumns, ShuffleType shuffleType) {
        List<ExprId> partitionedSlots = orderedShuffledColumns.stream()
                .filter(SlotReference.class::isInstance)
                .map(SlotReference.class::cast)
                .map(SlotReference::getExprId)
                .collect(Collectors.toList());
        return partitionedSlots.isEmpty() ? PhysicalProperties.GATHER : createHash(partitionedSlots, shuffleType);
    }
    // 基于指定的 ExprId 列表与 Shuffle 类型构建基于 Hash 分布的物理属性。
    // 如果 orderedShuffledColumns 为空返回 GATHER；否则返回带有 DistributionSpecHash 的新 PhysicalProperties 实例。
    public static PhysicalProperties createHash(List<ExprId> orderedShuffledColumns, ShuffleType shuffleType) {
        return orderedShuffledColumns.isEmpty()
                ? PhysicalProperties.GATHER
                : new PhysicalProperties(new DistributionSpecHash(orderedShuffledColumns, shuffleType));
    }

    public static PhysicalProperties createHash(DistributionSpecHash distributionSpecHash) {
        return new PhysicalProperties(distributionSpecHash);
    }

    /** createAnyFromHash */
    public static PhysicalProperties createAnyFromHash(DistributionSpecHash... childSpecs) {
        for (DistributionSpecHash childSpec : childSpecs) {
            if (childSpec.getShuffleType() == ShuffleType.NATURAL) {
                return PhysicalProperties.STORAGE_ANY;
            }
        }
        return PhysicalProperties.ANY;
    }

    public PhysicalProperties withOrderSpec(OrderSpec orderSpec) {
        return new PhysicalProperties(distributionSpec, orderSpec);
    }

    // Current properties satisfies other properties.
    public boolean satisfy(PhysicalProperties other) {
        return orderSpec.satisfy(other.orderSpec) && distributionSpec.satisfy(other.distributionSpec);
    }

    public OrderSpec getOrderSpec() {
        return orderSpec;
    }

    public DistributionSpec getDistributionSpec() {
        return distributionSpec;
    }

    public boolean isDistributionOnlyProperties() {
        return orderSpec.getOrderKeys().isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PhysicalProperties that = (PhysicalProperties) o;
        if (this.hashCode() != that.hashCode()) {
            return false;
        }
        return orderSpec.equals(that.orderSpec)
                && distributionSpec.equals(that.distributionSpec);
    }

    @Override
    public int hashCode() {
        if (hashCode == null) {
            hashCode = Objects.hash(orderSpec, distributionSpec);
        }
        return hashCode;
    }

    @Override
    public String toString() {
        if (this.equals(ANY)) {
            return "ANY";
        }
        if (this.equals(REPLICATED)) {
            return "REPLICATED";
        }
        if (this.equals(GATHER)) {
            return "GATHER";
        }
        return distributionSpec.toString() + " " + orderSpec.toString();
    }

}
