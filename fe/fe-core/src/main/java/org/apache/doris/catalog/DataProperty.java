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

import org.apache.doris.common.Config;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;

import java.io.IOException;
import java.util.Objects;

// DataProperty 是一个核心的元数据实体类，用于描述和管理表或分区（Partition）的数据存储属性（如存储介质、降冷时间、远程存储策略、数据可变性等）
// 统一封装存储元数据：它将表或分区在底层存储时的关键属性（如使用 SSD 还是 HDD、什么时候将热数据降冷到冷存储、绑定的远端存储策略名称、数据是否允许修改等）聚合在同一个对象中。
// 支持数据生命周期与冷热分层管理：通过 storageMedium 和 cooldownTimeMs，Doris 能够自动把超过指定时间的数据从本地高性能介质（SSD）迁移到低成本介质（HDD）或远端对象存储中。
// 元数据持久化与兼容：实现了 GsonPostProcessable 接口，支持通过 Gson 进行序列化与反序列化，并能优雅处理旧版本元数据升级时的字段兼容问题。
public class DataProperty implements GsonPostProcessable {
    // 集群的默认存储介质。
    // 读取系统配置 Config.default_storage_medium，如果配置为 "SSD"（不区分大小写），则取 TStorageMedium.SSD，否则默认使用 TStorageMedium.HDD。
    public static final TStorageMedium DEFAULT_STORAGE_MEDIUM = "SSD".equalsIgnoreCase(Config.default_storage_medium)
            ? TStorageMedium.SSD : TStorageMedium.HDD;
    // 最大降冷时间戳（对应时间 9999-12-31 23:59:59，数值为 253402271999000L）。
    // 当不设置降冷或数据不需要降冷（如纯 HDD 介质）时，降冷时间会被设为此默认最大值，表示“永不降冷”。
    public static final long MAX_COOLDOWN_TIME_MS = 253402271999000L; // 9999-12-31 23:59:59
    // 表示默认的纯 HDD 存储属性配置
    public static final DataProperty DEFAULT_HDD_DATA_PROPERTY = new DataProperty(TStorageMedium.HDD);

    @SerializedName(value = "storageMedium")
    private TStorageMedium storageMedium;
    @SerializedName(value = "cooldownTimeMs")
    private long cooldownTimeMs;
    @SerializedName(value = "storagePolicy")
    private String storagePolicy;
    @SerializedName(value = "isMutable")
    private boolean isMutable = true;
    private boolean storageMediumSpecified;

    private DataProperty() {
        // for persist
    }

    public DataProperty(TStorageMedium medium) {
        this.storageMedium = medium;
        this.cooldownTimeMs = MAX_COOLDOWN_TIME_MS;
        this.storagePolicy = "";
    }

    public DataProperty(DataProperty other) {
        this.storageMedium = other.storageMedium;
        this.cooldownTimeMs = other.cooldownTimeMs;
        this.storagePolicy = other.storagePolicy;
        this.isMutable = other.isMutable;
    }

    /**
     * DataProperty construction.
     *
     * @param medium storage medium for the init storage of the table
     * @param cooldown cool down time for SSD->HDD
     * @param storagePolicy remote storage policy for remote storage
     */
    public DataProperty(TStorageMedium medium, long cooldown, String storagePolicy) {
        this(medium, cooldown, storagePolicy, true);
    }

    public DataProperty(TStorageMedium medium, long cooldown, String storagePolicy, boolean isMutable) {
        this.storageMedium = medium;
        this.cooldownTimeMs = cooldown;
        this.storagePolicy = storagePolicy;
        this.isMutable = isMutable;
    }

    public TStorageMedium getStorageMedium() {
        return storageMedium;
    }

    public long getCooldownTimeMs() {
        return cooldownTimeMs;
    }

    public String getStoragePolicy() {
        return storagePolicy;
    }

    public void setStoragePolicy(String storagePolicy) {
        this.storagePolicy = storagePolicy;
    }

    public boolean isStorageMediumSpecified() {
        return storageMediumSpecified;
    }

    public boolean isMutable() {
        return isMutable;
    }

    public void setMutable(boolean mutable) {
        isMutable = mutable;
    }

    public void setStorageMediumSpecified(boolean isSpecified) {
        storageMediumSpecified = isSpecified;
    }

    public void setStorageMedium(TStorageMedium medium) {
        this.storageMedium = medium;
    }

    @Override
    public int hashCode() {
        return Objects.hash(storageMedium, cooldownTimeMs, storagePolicy);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof DataProperty)) {
            return false;
        }

        DataProperty other = (DataProperty) obj;

        return this.storageMedium == other.storageMedium
                && this.cooldownTimeMs == other.cooldownTimeMs
                && Strings.nullToEmpty(this.storagePolicy).equals(Strings.nullToEmpty(other.storagePolicy))
                && this.isMutable == other.isMutable;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Storage medium[").append(this.storageMedium).append("]. ");
        sb.append("cool down[").append(TimeUtils.longToTimeString(cooldownTimeMs)).append("]. ");
        sb.append("remote storage policy[").append(this.storagePolicy).append("]. ");
        return sb.toString();
    }

    @Override
    public void gsonPostProcess() throws IOException {
        // storagePolicy is a newly added field, it may be null when replaying from old version.
        this.storagePolicy = Strings.nullToEmpty(this.storagePolicy);
    }

}
