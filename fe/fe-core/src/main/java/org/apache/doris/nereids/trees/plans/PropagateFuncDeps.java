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

package org.apache.doris.nereids.trees.plans;

import org.apache.doris.nereids.properties.DataTrait;

/**
 * Propagate fd, keep children's fd
 */
// PropagateFuncDeps（Propagate Functional Dependencies，即“传递函数依赖”）是一个非常基础且重要的接口（Interface），属于逻辑与物理计划树节点（Plan）的属性推导框架的一部分。
// 在 SQL 查询优化中，数据特征/函数依赖（Data Trait / Functional Dependencies, FD） 包含了许多推导查询特性的重要信息，例如：
// 唯一性（Unique）：某些列组合能否唯一标识一行（主键/唯一键特性）。
// 常量性/等值性（Uniform）：某些列的值是否在整个结果集中是恒定单一的值。
// 等值集合（Equal Set）：哪些列之间存在恒等关系（如 $a = b$）。
// 函数依赖图（FD Directed Graph）：列与列之间的决定关系（如 $A \to B$）。
// 优化器利用这些信息可以进行大量的等价变换与剪枝（例如：去除不必要的 DISTINCT、解关联子查询、去除冗余 Group By 列等）。
// PropagateFuncDeps 接口的核心作用在于：
// 为那些不会改变或破坏子节点数据特征与函数依赖关系的算子节点（例如 Filter、Project、Sort、Limit 等单孩子节点，或者某些能够透传特性的算子），
// 提供默认的特征传递与继承机制
public interface PropagateFuncDeps extends Plan {
    // 计算并汇总当前算子节点的完整数据特征对象（DataTrait）。
    @Override
    default DataTrait computeDataTrait() {
        // 如果当前算子只有一个子节点（如 Filter、Sort 等），它不需要做任何复杂的合并计算，直接返回第一个子节点（child(0)）逻辑属性中的 DataTrait 即可。
        if (children().size() == 1) {
            // Note when changing function dependencies, we always clone it.
            // So it's safe to return a reference
            return child(0).getLogicalProperties().getTrait();
        }
        // 多孩子节点处理（如 Join 或 SetOperation）：
        // 如果存在多个子节点，方法内部会创建一个 DataTrait.Builder 构建器。
        // 遍历所有子节点（children().stream()），提取每个子节点的 Trait 并调用 builder.addDataTrait(...) 进行合并。
        DataTrait.Builder builder = new DataTrait.Builder();
        children().stream()
                .map(p -> p.getLogicalProperties().getTrait())
                .forEach(builder::addDataTrait);
        return builder.build();
    }
    // 计算当前节点继承获得的列唯一性（Unique Slots）特征，并写入传入的 builder 中。
    // 物理含义：如果子节点的数据在某列（如 id）上是唯一的，经过当前算子（例如 Filter 或 Sort）处理后，id 列的唯一性依然成立，因此直接向上继承。
    @Override
    default void computeUnique(DataTrait.Builder builder) {
        builder.addUniqueSlot(child(0).getLogicalProperties().getTrait());
    }
    // 计算当前节点继承获得的列常量/单一值（Uniform Slots）特征，并写入传入的 builder 中。
    // 物理含义：如果子节点的某列（如 status = 'ACTIVE'）在整张表中全是同一个常量值，当前算子不改变该列内容时，该列的 Uniform 属性继续有效，直接向上透传。
    @Override
    default void computeUniform(DataTrait.Builder builder) {
        builder.addUniformSlot(child(0).getLogicalProperties().getTrait());
    }
    // 计算当前节点继承获得的列等值集合（Equal Set）特征，并写入传入的 builder 中。
    // 物理含义：如果子节点中已经推导出 $a$ 列与 $b$ 列值相等，在经过透传算子后，这个等值关系在当前节点依然保持成立。
    @Override
    default void computeEqualSet(DataTrait.Builder builder) {
        builder.addEqualSet(child(0).getLogicalProperties().getTrait());
    }
    // 计算当前节点继承获得的通用函数依赖有向图（Functional Dependency Directed Graph, FD DG），并写入传入的 builder 中。
    // 物理含义：如子节点存在函数依赖关系（例如由 zip_code 可以决定 city，即 $zip\_code \to city$），在当前透传节点上，该决定关系依然有效，继续向下游优化规则提供推导依据。
    @Override
    default void computeFd(DataTrait.Builder builder) {
        builder.addFuncDepsDG(child(0).getLogicalProperties().getTrait());
    }
}
