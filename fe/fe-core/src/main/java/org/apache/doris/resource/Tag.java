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

package org.apache.doris.resource;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/*
 * A Tag consists of type and value.
 * Tag type and value are both case insensitive, and represented in lower case.
 * Tag is printed as { "type": "value" }
 *
 * Type is mainly used to categorize a tag. For example, users can customize a certain type of tag.
 * And these tags all use the same type. So user can quickly find this type of tags by the type.
 * Doris reserves several built-in types:
 *     ROLE: the role of resource, such as FRONTEND, BACKEND, BROKER
 *     FUNCTION: the function of a tag, such as STORAGE, COMPUTATION
 *     LOCATION: A type of tags representing location information.
 *
 * Value is customized. And Doris also reserves several built-in values for built-in types:
 *     FRONTEND, BACKEND, BROKER of type ROLE.
 *     REMOTE_STORAGE, STORAGE, COMPUTATION for type FUNCTION.
 *
 * A Tag is immutable once it being created.
 */
// 核心的元数据实体类
// 用于对集群节点（如 FE、BE、CN）或资源进行分类、标记和精细化亲和性调度（例如指定某些分区的数据只能存放在特定机房、特定可用区或计算节点组上）。
// 多维度资源分类与隔离：通过 type（类型）和 value（值）的组合，Tag 能够对 Doris 集群中的节点角色（如 role：frontend/backend）、功能（如 function：storage/computation）以及物理位置（如 location：zone1/zone2）进行标准化建模。
// 支持精细化副本调度：配合前面提到的 ReplicaAllocation，Doris 能够实现基于 Tag 的多机房容灾、冷热隔离以及计算存储解耦。
//
public class Tag implements Writable {
    // 节点角色类型（如 FRONTEND, BACKEND, BROKER）。
    public static final String TYPE_ROLE = "role";
    // 节点功能类型（如 STORAGE, COMPUTATION）。
    public static final String TYPE_FUNCTION = "function";
    // 物理位置类型（如机房、可用区）。
    public static final String TYPE_LOCATION = "location";
    // 对应 ROLE 的内置角色。
    public static final String VALUE_FRONTEND = "frontend";
    public static final String VALUE_BACKEND = "backend";
    public static final String VALUE_BROKER = "broker";
    // 对应 FUNCTION 或节点职能的内置值。
    public static final String VALUE_REMOTE_STORAGE = "remote_storage";
    public static final String VALUE_STORE = "store";
    public static final String VALUE_COMPUTATION = "computation";
    public static final String VALUE_MIX = "mix";
    // 默认集群、默认标签及无效标签占位符。
    public static final String VALUE_DEFAULT_CLUSTER = "default_cluster";
    public static final String VALUE_DEFAULT_TAG = "default";
    public static final String VALUE_INVALID_TAG = "invalid";
    // 云环境集群相关的标识与状态字段。
    public static final String CLOUD_CLUSTER_NAME = "cloud_cluster_name";
    public static final String CLOUD_CLUSTER_ID = "cloud_cluster_id";
    public static final String CLOUD_UNIQUE_ID = "cloud_unique_id";
    public static final String CLOUD_CLUSTER_STATUS = "cloud_cluster_status";
    public static final String CLOUD_CLUSTER_PUBLIC_ENDPOINT = "cloud_cluster_public_endpoint";
    public static final String CLOUD_CLUSTER_PRIVATE_ENDPOINT = "cloud_cluster_private_endpoint";
    // 公网/内网访问终端。
    public static final String PUBLIC_ENDPOINT = "public_endpoint";
    public static final String PRIVATE_ENDPOINT = "private_endpoint";
    // 计算组名称及默认计算组。
    public static final String COMPUTE_GROUP_NAME = "compute_group_name";

    public static final String VALUE_DEFAULT_COMPUTE_GROUP_NAME = "default_compute_group";
    // 工作负载组标识。
    public static final String WORKLOAD_GROUP = "workload_group";

    public static final ImmutableSet<String> RESERVED_TAG_TYPE = ImmutableSet.of(
            TYPE_ROLE, TYPE_FUNCTION, TYPE_LOCATION);
    public static final ImmutableSet<String> RESERVED_TAG_VALUES = ImmutableSet.of(
            VALUE_FRONTEND, VALUE_BACKEND, VALUE_BROKER, VALUE_REMOTE_STORAGE, VALUE_STORE, VALUE_COMPUTATION,
            VALUE_MIX, VALUE_DEFAULT_CLUSTER);
    // Tag 类型的正则校验规则（以小写字母开头，包含小写字母、数字和下划线，长度 1~33）
    private static final String TAG_TYPE_REGEX = "^[a-z][a-z0-9_]{0,32}$";
    // Tag 值的正则校验规则（以大小写字母开头）。
    private static final String TAG_VALUE_REGEX = "^[a-zA-Z][a-zA-Z0-9_]{0,32}$";
    // Endpoint 的正则校验规则。
    private static final String ENDPOINT_REGEX = "^[a-zA-Z0-9.-]+(:[0-9]+)?$";


    public static final Tag DEFAULT_BACKEND_TAG;
    public static final Tag DEFAULT_NODE_ROLE_TAG;
    public static final Tag INVALID_TAG;

    static {
        DEFAULT_BACKEND_TAG = new Tag(TYPE_LOCATION, VALUE_DEFAULT_TAG);
        DEFAULT_NODE_ROLE_TAG = new Tag(TYPE_ROLE, VALUE_MIX);
        INVALID_TAG = new Tag(TYPE_LOCATION, VALUE_INVALID_TAG);
    }
    // Tag 的类型（如 location、role）
    @SerializedName(value = "type")
    public String type;
    // Tag 的具体值（如 default、zone1）
    @SerializedName(value = "value")
    public String value;

    private Tag(String type, String val) {
        this.type = type;
        this.value = val;
    }

    public static Tag create(String type, String value) throws AnalysisException {
        if (!type.matches(TAG_TYPE_REGEX)) {
            throw new AnalysisException("Invalid tag type format: " + type);
        }

        // if type is an endpoint type, value must be a valid endpoint
        if (type.equalsIgnoreCase(PUBLIC_ENDPOINT) || type.equalsIgnoreCase(PRIVATE_ENDPOINT)) {
            if (!value.matches(ENDPOINT_REGEX)) {
                throw new AnalysisException("Invalid " + type + " value format: " + value);
            }
        } else if (!value.matches(TAG_VALUE_REGEX)) {
            throw new AnalysisException("Invalid tag value format: " + value);
        }
        return new Tag(type, value);
    }

    public static Tag createNotCheck(String type, String value) {
        return new Tag(type, value);
    }

    // only support be and cn node role tag for be.
    public static boolean validNodeRoleTag(String value) {
        return value != null && (value.equals(VALUE_MIX) || value.equals(VALUE_COMPUTATION));
    }

    public String toKey() {
        return type + "_" + value;
    }

    public Map<String, String> toMap() {
        Map<String, String> map = Maps.newHashMap();
        map.put(type, value);
        return map;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, value);
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (!(other instanceof Tag)) {
            return false;
        }
        Tag otherTag = (Tag) other;
        return type.equals(otherTag.type) && value.equals(otherTag.value);
    }

    @Override
    public String toString() {
        return "{\"" + type + "\" : \"" + value + "\"}";
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static Tag read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, Tag.class);
    }
}
