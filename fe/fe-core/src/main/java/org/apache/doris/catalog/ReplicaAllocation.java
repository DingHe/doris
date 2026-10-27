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

package org.apache.doris.catalog;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.resource.Tag;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * ReplicaAllocation is used to describe the distribution of replicas of a tablet.
 * By default, 3 replicas of a tablet are distributed on 3 BE nodes with Tag "default".
 */
// 核心的元数据实体类
// 用于描述和管理 Tablet（数据分片副本）在集群中各个节点（Tag）上的分布与数量策略
// 精细化副本放置与 Tag 隔离：在分布式数据库中，数据的可靠性依赖于多副本。ReplicaAllocation 允许用户通过 Tag（如不同的机房、可用区、硬件类型）精细化控制每个 Tag 下面应该放置多少个副本。默认情况下，系统会在带有 "default" Tag 的 3 个 BE 节点上各放置 1 个副本（共 3 个副本）。
// 支持新旧版本兼容与转换：它提供了从旧版单一数字（如简单的 replication_num = 3）平滑转换为基于 Tag 映射的复杂多可用区分配策略的能力。
//
public class ReplicaAllocation implements Writable {
    // 集群默认的副本分配策略。它通过 EnvFactory 动态创建，通常代表默认的 3 副本且打在默认 Tag 节点上。
    public static final ReplicaAllocation DEFAULT_ALLOCATION;
    // represent that replica allocation is not set.
    // 表示“未设置”状态的特殊副本分配对象（其内部的 allocMap 为空），常用于逻辑判断或默认占位。
    public static final ReplicaAllocation NOT_SET;

    static {
        DEFAULT_ALLOCATION = EnvFactory.getInstance().createDefReplicaAllocation();
        NOT_SET = new ReplicaAllocation();
    }
    // 核心存储结构
    // 键（Tag）表示节点的标签（如地域、机房、机架），值（Short）表示在该标签对应的节点组上需要放置多少个副本。
    @SerializedName(value = "allocMap")
    private Map<Tag, Short> allocMap = Maps.newHashMap();
    // 参构造函数。初始化一个空的 allocMap（基于 Maps.newHashMap()），主要用于元数据反序列化或初始占位。
    public ReplicaAllocation() {

    }

    // For convert the old replica number to replica allocation
    // 单参数兼容构造函数。用于将旧版本的单一数字副本数（如 3）转换为基于 Tag 的分配策略——它会将指定的副本数直接绑定到默认后端标签（Tag.DEFAULT_BACKEND_TAG）上。
    public ReplicaAllocation(short replicaNum) {
        allocMap.put(Tag.DEFAULT_BACKEND_TAG, replicaNum);
    }

    public ReplicaAllocation(Map<Tag, Short> allocMap) {
        this.allocMap = allocMap;
    }
    // 向当前的分配映射中添加或更新一个 Tag 及其对应的副本数量。
    public void put(Tag tag, Short num) {
        this.allocMap.put(tag, num);
    }

    public Map<Tag, Short> getAllocMap() {
        return allocMap;
    }

    public short getTotalReplicaNum() {
        short num = 0;
        for (Short s : allocMap.values()) {
            num += s;
        }
        return num;
    }

    public boolean isEmpty() {
        return allocMap.isEmpty();
    }

    public boolean isNotSet() {
        return this.equals(NOT_SET);
    }

    public Short getReplicaNumByTag(Tag tag) {
        return allocMap.getOrDefault(tag, (short) 0);
    }

    public static ReplicaAllocation read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ReplicaAllocation.class);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ReplicaAllocation that = (ReplicaAllocation) o;
        return that.allocMap.equals(this.allocMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(allocMap);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Override
    public String toString() {
        return toCreateStmt();
    }

    // For show create table stmt. like:
    // "tag.location.zone1: 2, tag.location.zone2: 1"
    public String toCreateStmt() {
        List<String> tags = Lists.newArrayList();
        for (Map.Entry<Tag, Short> entry : allocMap.entrySet()) {
            tags.add(PropertyAnalyzer.TAG_LOCATION + "." + entry.getKey().value + ": " + entry.getValue());
        }
        return Joiner.on(", ").join(tags);
    }
}
