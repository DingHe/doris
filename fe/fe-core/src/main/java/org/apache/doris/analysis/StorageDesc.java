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

package org.apache.doris.analysis;

import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.persist.gson.GsonPostProcessable;

import com.google.gson.annotations.SerializedName;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


/**
 * Describe storage properties
 * The structure diagram is divided into three levels:
 *            StorageDesc
 *          /            \
 *    BrokerDesc        The other StorageBackend.StorageType desc
 *        |
 *  The broker's StorageBackend.StorageType desc
 */
// 专门用于描述和管理外部存储系统（如 S3、OSS、COS、Broker 等）的连接属性与配置。
// 统一存储配置封装：在用户提交涉及外部存储交互的 SQL 命令（如 Broker Load、外部表访问等）时，封装存储的名称、存储类型（StorageType）以及详细的连接凭证和参数属性（Properties）。
// SPI 适配器懒加载绑定：引入了 StorageAdapter（存储适配器门面），根据传入的配置属性或存储类型，懒加载并绑定对应的底层存储实现，简化云存储（S3/BOS/OSS 等）和 Broker 交互的逻辑。
// 元数据持久化恢复：支持通过 Gson 序列化和反序列化持久化存储属性，并通过 gsonPostProcess() 在反序列化后自动恢复非序列化的适配器状态。
public class StorageDesc extends ResourceDesc implements GsonPostProcessable {
    // 存储后端的类型枚举
    @Deprecated
    @SerializedName("st")
    protected StorageBackend.StorageType storageType;

    /** SPI facade binding; lazily bound from the raw properties. Not Gson-serialized. */
    // SPI 适配器门面对象。
    // 通过原始属性配置懒加载绑定，用于屏蔽不同底层存储（如各种云存储客户端）的具体实现细节。
    protected StorageAdapter storageAdapter;

    public StorageDesc() {
    }

    public StorageDesc(String name, StorageBackend.StorageType storageType, Map<String, String> properties) {
        this.name = name;
        this.storageType = storageType;
        this.properties = properties;
        initStorageAdapter();
    }

    protected void initStorageAdapter() {
        if (storageAdapter != null) {
            return;
        }
        if (properties == null) {
            properties = new HashMap<>();
        }
        if (null != storageType && storageType.equals(StorageBackend.StorageType.BROKER)) {
            this.storageAdapter = StorageAdapter.ofBroker(name, properties);
            return;
        }
        if (!properties.isEmpty()) {
            this.storageAdapter = StorageAdapter.of(properties);
        }
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setStorageType(StorageBackend.StorageType storageType) {
        this.storageType = storageType;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public String getName() {
        return name;
    }

    public StorageBackend.StorageType getStorageType() {
        return storageType;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public Map<String, String> getBackendConfigProperties() {
        initStorageAdapter();
        if (null == storageAdapter) {
            return properties;
        }
        return storageAdapter.getBackendConfigProperties();
    }

    public StorageAdapter getStorageAdapter() {
        initStorageAdapter();
        return storageAdapter;
    }

    @Override
    public void gsonPostProcess() throws IOException {
        initStorageAdapter();
    }
}
