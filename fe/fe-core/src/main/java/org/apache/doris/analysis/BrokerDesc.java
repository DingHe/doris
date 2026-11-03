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

import org.apache.doris.analysis.StorageBackend.StorageType;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.foundation.property.StoragePropertiesException;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TFileType;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

// Broker descriptor
//
// Broker example:
// WITH BROKER "broker0"
// (
//   "username" = "user0",
//   "password" = "password0"
// )
// 门用于解析、封装和管理 SQL 语句中通过 WITH BROKER "broker0" (...)
// 或其他存储配置（如 S3、HDFS 等）指定的连接凭证、属性参数和存储后端类型，并负责为底层的导入任务提供文件路径规范化、文件类型映射以及元数据的持久化读写。
// 存储与 Broker 属性封装：解析用户在导入 SQL 中指定的存储配置（如用户名、密码、AccessKey、SecretKey 等）。
// 动态存储适配器绑定：根据传入的属性或存储类型，自动识别并绑定对应的 StorageAdapter（如 S3 适配器、Broker 适配器等）。
// 文件路径校验与规范化：提供 getFileLocation 方法，确保文件路径 URI 符合底层存储客户端的要求。
// RPC 协议类型转换：将 Doris 内部的存储类型映射为 Thrift 层识别的 TFileType（如 FILE_S3、FILE_HDFS、FILE_BROKER 等），供 BE（Backend）节点执行加载时使用。
public class BrokerDesc extends StorageDesc implements Writable {

    // just for multi load
    // 专门用于标识多并发导入（Multi Load）的特殊 Broker 标识符。
    public static final String MULTI_LOAD_BROKER = "__DORIS_MULTI_LOAD_BROKER__";
    // 用于在多并发导入中标识关联的后端节点 Key。
    public static final String MULTI_LOAD_BROKER_BACKEND_KEY = "__DORIS_MULTI_LOAD_BROKER_BACKEND__";
    // 标记该描述符是否已转换为 S3 存储类型。
    @Deprecated
    @SerializedName("cts3")
    private boolean convertedToS3 = false;

    // Only used for recovery
    private BrokerDesc() {
        this.properties = Maps.newHashMap();
        this.storageType = StorageBackend.StorageType.BROKER;
    }

    // for empty broker desc
    public BrokerDesc(String name) {
        this.name = name;
        this.properties = Maps.newHashMap();
        this.storageType = StorageType.LOCAL;
    }

    public BrokerDesc(String name, Map<String, String> properties) {
        this.name = name;
        this.properties = Maps.newHashMap();
        if (properties != null) {
            this.properties.putAll(properties);
        }
        // Assume the storage type is BROKER by default
        // If it's a multi-load broker, override the storage type to LOCAL
        if (isMultiLoadBroker()) {
            this.storageType = StorageBackend.StorageType.LOCAL;
        } else {
            this.storageType = StorageBackend.StorageType.BROKER;
        }

        // Try to determine the actual storage type from properties if available
        if (MapUtils.isNotEmpty(this.properties)) {
            try {
                // Bind the primary storage adapter from the given configuration
                this.storageAdapter = StorageAdapter.of(this.properties);
                // Override the storage type based on property configuration
                this.storageType = StorageBackend.StorageType.valueOfIgnoreCase(storageAdapter.getStorageName());
            } catch (StoragePropertiesException e) {
                // Currently ignored: these properties might be broker-specific.
                // Just keep the storage type as BROKER, and bind a broker adapter
                this.storageAdapter = StorageAdapter.ofBroker(name, this.properties);
                this.storageType = StorageBackend.StorageType.BROKER;
            }
        }
        //only storage type is broker
        if (StringUtils.isBlank(this.name) && (this.getStorageType() != StorageType.BROKER)) {
            this.name = this.storageType().name();
        }
    }

    public BrokerDesc(String name, StorageBackend.StorageType storageType, Map<String, String> properties) {
        this.name = name;
        this.properties = Maps.newHashMap();
        this.storageType = storageType;
        if (properties != null) {
            this.properties.putAll(properties);
        }
        if (StorageType.BROKER.equals(storageType)) {
            this.storageAdapter = StorageAdapter.ofBroker(name, properties);
            return;
        }
        if (MapUtils.isNotEmpty(this.properties) && StorageType.REFACTOR_STORAGE_TYPES.contains(storageType)) {
            this.storageAdapter = StorageAdapter.of(properties);
        }

    }

    public String getFileLocation(String location) throws UserException {
        return (null != storageAdapter) ? storageAdapter.validateAndNormalizeUri(location) : location;
    }

    public static BrokerDesc createForStreamLoad() {
        return new BrokerDesc("", StorageType.STREAM, null);
    }

    public boolean isMultiLoadBroker() {
        return StringUtils.isNotBlank(this.name) && this.name.equalsIgnoreCase(MULTI_LOAD_BROKER);
    }

    public TFileType getFileType() {
        switch (storageType) {
            case LOCAL:
                return TFileType.FILE_LOCAL;
            case S3:
                return TFileType.FILE_S3;
            case HDFS:
            case JFS:
                return TFileType.FILE_HDFS;
            case STREAM:
                return TFileType.FILE_STREAM;
            case BROKER:
            case OFS:
            default:
                return TFileType.FILE_BROKER;
        }
    }

    public StorageBackend.StorageType storageType() {
        return storageType;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static BrokerDesc read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), BrokerDesc.class);
    }
}
