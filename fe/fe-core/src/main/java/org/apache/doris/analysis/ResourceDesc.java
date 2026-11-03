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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Resource;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.load.EtlJobType;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.util.Map;

// Resource descriptor
//
// Spark example:
// WITH RESOURCE "spark0"
// (
//   "spark.jars" = "xxx.jar,yyy.jar",
//   "spark.files" = "/tmp/aaa,/tmp/bbb",
//   "spark.executor.memory" = "1g",
//   "spark.yarn.queue" = "queue0"
// )
// 用于表示资源描述符（Resource Descriptor）的分析阶段（Analysis）核心类
// 主要用于解析和封装诸如 WITH RESOURCE "resource_name" (...) 语法中指定的外部计算资源或存储资源配置（例如历史版本中的 Spark 资源、云资源等）
// 语法解析载体：在用户提交包含 WITH RESOURCE 子句的 SQL（如 Spark Load）时，封装资源的名称以及自定义的属性配置参数（Properties）。
// 元数据合法性校验：通过 analyze() 方法检查该资源在 Doris 的资源管理器（Resource Manager）中是否存在，并对废弃或不支持的资源类型（如 Spark 资源）进行拦截报错
public class ResourceDesc {
    // 资源的名称（对应 SQL 语句中双引号内的标识符，例如 "spark0"）。
    @SerializedName("nm")
    protected String name;
    // 资源的属性配置映射表。用于存储资源相关的键值对参数（例如 Spark 的 spark.executor.memory、spark.yarn.queue 等配置）。
    @SerializedName("prop")
    protected Map<String, String> properties;
    /**
     * TODO(tsy): transfer to LoadType
     */
    // 关联的 ETL 作业类型
    protected EtlJobType etlJobType;

    // Only used for recovery
    public ResourceDesc() {
    }

    public ResourceDesc(String name, Map<String, String> properties) {
        this.name = name;
        this.properties = properties;
        if (this.properties == null) {
            this.properties = Maps.newHashMap();
        }
        this.etlJobType = EtlJobType.UNKNOWN;
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public EtlJobType getEtlJobType() {
        return etlJobType;
    }

    public void analyze() throws AnalysisException {
        // check resource exist or not
        Resource resource = Env.getCurrentEnv().getResourceMgr().getResource(getName());
        if (resource == null) {
            throw new AnalysisException("Resource does not exist. name: " + getName());
        }
        if (resource.getType() == Resource.ResourceType.SPARK) {
            throw new AnalysisException("Spark Resource is no longer supported");
        }
    }
}
