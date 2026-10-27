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

import com.google.gson.annotations.SerializedName;

/*
 * Resource is the collective name of the nodes that provide various service capabilities in Doris cluster.
 * Each resource has a unique ID.
 * A resource may have one or more tags that represent the functional properties or custom groupings of a resource, etc.
 * eg:
 *      Backend, Frontend, Broker, RemoteStorage
 */
// 用于统一抽象和管理 Doris 集群中提供各类服务能力的节点或资源（例如 Backend、Frontend、Broker、RemoteStorage 等）
// 统一资源抽象：在 Doris 集群中，不同类型的节点（如 FE、BE、Broker 等）底层都有共同的属性需求（如全局唯一的资源 ID、节点标签集合等）。Resource 将这些共性提取到抽象基类中，为各类具体资源提供统一的规范。
// 标签与分类管理：每个资源都可以绑定一个或多个 TagSet（标签集），用于标识其功能属性、自定义分组或物理拓扑位置。这为后续的资源调度、亲和性分配以及计算存储解耦提供了基础支持。
public abstract class Resource {
    // 资源的唯一标识符（ID）。
    // 集群中的每一个服务节点或资源对象都对应一个唯一的数字 ID，用于在元数据中进行精准寻址和引用。
    @SerializedName(value = "id")
    protected long id;
    // 标签集合对象。用于存储该资源所拥有的所有 Tag（例如角色 Tag、位置 Tag、功能 Tag 等），决定了该资源所属的逻辑分组或物理拓扑。
    @SerializedName(value = "tagSet")
    protected TagSet tagSet;

    public Resource(long id, TagSet tagSet) {
        this.tagSet = tagSet;
    }

    public long getId() {
        return id;
    }

    public TagSet getTagSet() {
        return tagSet;
    }

    public void addTag(Tag tag) {
        tagSet.addTag(tag);
    }

    public void setTag(Tag tag) {
        TagSet newTagSet = TagSet.create(tag);
        tagSet.substituteMerge(newTagSet);
    }

    public void setTagSet(TagSet tagSet) {
        this.tagSet = tagSet;
    }
}
