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

package org.apache.doris.system;

import org.apache.doris.catalog.DiskInfo;
import org.apache.doris.catalog.DiskInfo.DiskState;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.DatasourcePrintableMap;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.SimpleScheduler;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.HeartbeatResponse.HbStatus;
import org.apache.doris.thrift.TBackend;
import org.apache.doris.thrift.TDisk;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class extends the primary identifier of a Backend with ephemeral state,
 * eg usage information, current administrative state etc.
 */
// Backend 是一个核心的系统管理类，用于描述和管理集群中的 Backend（简称 BE，即计算与存储节点）。
// 它不仅记录了节点的静态网络配置、端口和物理资源信息，还追踪了节点的动态运行状态（如存活状态、心跳信息、磁盘空间、负载以及各种可用性开关等）。
// 统一管理 BE 节点生命周期与元数据：维护 BE 的唯一标识（ID）、主机地址、各种网络端口（心跳端口、RPC 端口、Web 服务的 Http 端口、Brpc 端口等）。
// 心跳检测与健康监控：通过心跳（Heartbeat）机制定期与 BE 交互，收集 BE 的运行状态、CPU 核心数、物理内存、磁盘健康状况及剩余空间，判定节点是否存活（Alive）。
// 调度与负载评估：为 Doris 的分片调度器提供决策依据。通过判断 BE 是否可用（如 isQueryAvailable、isScheduleAvailable、isLoadAvailable），决定是否能向其分发查询、写入任务或创建副本。
// 元数据持久化：实现了 Writable 接口，支持通过 Gson 序列化并记录到 EditLog 中，确保集群重启后节点状态能够正确恢复。
public class Backend implements Writable {
    private static final Logger LOG = LogManager.getLogger(Backend.class);

    // Represent a meaningless IP
    public static final String DUMMY_IP = "0.0.0.0";
    // 节点的全局唯一标识 ID。
    @SerializedName("id")
    private long id;
    // 节点的 IP 地址或主机名。
    @SerializedName("host")
    private volatile String host;
    // 当前 BE 运行的软件版本号。
    private String version;
    // 心跳服务端口。
    @SerializedName("heartbeatPort")
    private int heartbeatPort; // heartbeat
    // BE 内部服务端口。
    @SerializedName("bePort")
    private volatile int bePort; // be
    // Web 服务 / HTTP 访问端口。
    @SerializedName("httpPort")
    private volatile int httpPort; // web service
    // BE 的 RPC 端口。
    @SerializedName("beRpcPort")
    private volatile int beRpcPort; // be rpc port
    // Baidu RPC 端口，用于高效的数据传输和通信。
    @SerializedName("brpcPort")
    private volatile int brpcPort = -1;
    // Arrow Flight SQL 服务端口。
    @SerializedName("arrowFlightSqlPort")
    private volatile int arrowFlightSqlPort = -1;
    // 上次收到心跳更新的时间戳。
    @SerializedName("lastUpdateMs")
    private volatile long lastUpdateMs;
    // BE 进程上次启动的时间戳（也用作进程纪元 process epoch）。
    @SerializedName("lastStartTime")
    private volatile long lastStartTime;
    // 节点存活起始时间戳。
    @SerializedName("liveSince")
    private volatile long liveSince;
    // 节点当前的存活状态（布尔原子类）。
    @SerializedName("isAlive")
    private AtomicBoolean isAlive;
    // 标记节点是否已被下线（Decommissioned）
    @SerializedName("isDecommissioned")
    private AtomicBoolean isDecommissioned;
    // 标记节点是否正处于下线迁移过程中（默认 false）。
    private AtomicBoolean isDecommissioning = new AtomicBoolean(false);

    // rootPath -> DiskInfo
    // 根路径（RootPath）到 DiskInfo 的不可变映射，记录了该 BE 上所有挂载磁盘的容量、状态与介质类型。
    @SerializedName("disksRef")
    private volatile ImmutableMap<String, DiskInfo> disksRef;
    // 上次累积发布任务数。
    private Long lastPublishTaskAccumulatedNum = 0L;
    // 当前运行中的任务数。
    private Long runningTasks = 0L;
    // 最近一次心跳失败时的错误信息
    private String heartbeatErrMsg = "";

    // This is used for the first time we init pathHashToDishInfo in SystemInfoService.
    // after init it, this variable is set to true.
    // 标记是否已经完成了路径信息的初次初始化。
    private boolean initPathInfo = false;
    // 最近一次丢失心跳的时间。
    private long lastMissingHeartbeatTime = -1;
    // the max tablet compaction score of this backend.
    // this field is set by tablet report, and just for metric monitor, no need to persist.
    // 该 BE 上 Tablet 的最大 Compaction 得分（主要用于指标监控）。
    private volatile long tabletMaxCompactionScore = 0;

    // additional backendStatus information for BE, display in JSON format
    // 封装了 BE 的额外状态信息（如是否禁用查询、禁用导入、是否关闭、当前 Fragment 数量等）。
    @SerializedName("backendStatus")
    private BackendStatus backendStatus = new BackendStatus();
    // the locationTag is also saved in tagMap, use a single field here to avoid
    // creating this everytime we get it.
    // 位置标签（默认值 Tag.DEFAULT_BACKEND_TAG）。
    @SerializedName(value = "locationTag", alternate = {"tag"})
    private Tag locationTag = Tag.DEFAULT_BACKEND_TAG;
    // 节点角色标签（默认值 Tag.DEFAULT_NODE_ROLE_TAG）
    @SerializedName("nodeRole")
    private Tag nodeRoleTag = Tag.DEFAULT_NODE_ROLE_TAG;

    // tag type -> tag value.
    // A backend can only be assigned to one tag type, and each type can only have one value.
    // 标签类型的键值对映射，用于支持多 Tag 隔离及云原生属性。
    @SerializedName("tagMap")
    private Map<String, String> tagMap = Maps.newHashMap();
    // 平滑升级过程中的源/目标节点标记。
    private boolean isSmoothUpgradeSrc = false; // This be process is old process when doing smooth upgrade
    private boolean isSmoothUpgradeDst = false; // This be process is new process when doing smooth upgrade

    // cpu cores
    // CPU 核心数。
    @SerializedName("cpuCores")
    private int cpuCores = 1;
    // The physical memory available for use by BE.
    // BE 可用的物理内存大小。
    @SerializedName("beMemory")
    private long beMemory = 0;
    // from config::pipeline_executor_size , default equal cpuCores
    // Pipeline 执行器大小（通常等于 CPU 核心数）。
    @SerializedName("pipelineExecutorSize")
    private int pipelineExecutorSize = 1;

    // Counter of heartbeat failure.
    // Once a heartbeat failed, increase this counter by one.
    // And if it reaches Config.max_backend_heartbeat_failure_tolerance_count, this backend
    // will be marked as dead.
    // And once it back to alive, reset this counter.
    // No need to persist, because only master FE handle heartbeat.
    // 心跳连续失败计数器。超过配置阈值后会被判定为死亡。
    private int heartbeatFailureCounter = 0;
    // 下一次强制写入 EditLog 的心跳时间（带随机偏移，避免集中写入）。
    private long nextForceEditlogHeartbeatTime = System.currentTimeMillis() + (new SecureRandom()).nextInt(60 * 1000);

    public Backend() {
        this.host = "";
        this.version = "";
        this.lastUpdateMs = 0;
        this.lastStartTime = 0;
        this.isAlive = new AtomicBoolean();
        this.isDecommissioned = new AtomicBoolean(false);

        this.bePort = 0;
        this.httpPort = 0;
        this.beRpcPort = 0;
        this.disksRef = ImmutableMap.of();

        this.tagMap.put(locationTag.type, locationTag.value);
    }

    public Backend(long id, String host, int heartbeatPort) {
        this.id = id;
        this.host = host;
        this.version = "";
        this.heartbeatPort = heartbeatPort;
        this.bePort = -1;
        this.httpPort = -1;
        this.beRpcPort = -1;
        this.lastUpdateMs = -1L;
        this.lastStartTime = -1L;
        this.disksRef = ImmutableMap.of();

        this.isAlive = new AtomicBoolean(false);
        this.isDecommissioned = new AtomicBoolean(false);

        this.tagMap.put(locationTag.type, locationTag.value);
    }

    public String getCloudClusterStatus() {
        return tagMap.getOrDefault(Tag.CLOUD_CLUSTER_STATUS, String.valueOf(Cloud.ClusterStatus.NORMAL));
    }

    public void setCloudClusterStatus(final String clusterStatus) {
        tagMap.put(Tag.CLOUD_CLUSTER_STATUS, clusterStatus);
    }

    public String getCloudClusterName() {
        return tagMap.getOrDefault(Tag.CLOUD_CLUSTER_NAME, "");
    }

    public boolean isInStandbyCluster() {
        return (((CloudSystemInfoService) Env.getCurrentSystemInfo()).isStandByComputeGroup(getCloudClusterName()));
    }

    public void setCloudClusterName(final String clusterName) {
        tagMap.put(Tag.CLOUD_CLUSTER_NAME, clusterName);
    }

    public String getCloudClusterId() {
        return tagMap.getOrDefault(Tag.CLOUD_CLUSTER_ID, "");
    }

    public String getCloudUniqueId() {
        return tagMap.getOrDefault(Tag.CLOUD_UNIQUE_ID, "");
    }

    // This modification changes
    // CLOUD_CLUSTER_PUBLIC_ENDPOINT/CLOUD_CLUSTER_PRIVATE_ENDPOINT to
    // PUBLIC_ENDPOINT/PRIVATE_ENDPOINT, but backend information
    // has been persisted in the edit log. For upgrade compatibility, the tag may
    // not have public_endpoint/private_endpoint
    // during initial upgrade, so we first try to get
    // CLOUD_CLUSTER_PUBLIC_ENDPOINT/CLOUD_CLUSTER_PRIVATE_ENDPOINT, and later it
    // will be
    // synchronized from meta service to the public_endpoint/private_endpoint tag.
    // CLOUD_CLUSTER_PUBLIC_ENDPOINT/CLOUD_CLUSTER_PRIVATE_ENDPOINT can be
    // removed in future versions.
    public String getPublicEndpoint() {
        return tagMap.getOrDefault(Tag.PUBLIC_ENDPOINT, tagMap.getOrDefault(Tag.CLOUD_CLUSTER_PUBLIC_ENDPOINT, ""));
    }

    public String getPrivateEndpoint() {
        return tagMap.getOrDefault(Tag.PRIVATE_ENDPOINT, tagMap.getOrDefault(Tag.CLOUD_CLUSTER_PRIVATE_ENDPOINT, ""));
    }

    public long getId() {
        return id;
    }

    // Return ip:heartbeat port
    public String getAddress() {
        return host + ":" + heartbeatPort;
    }

    public String getHost() {
        return host;
    }

    public String getVersion() {
        return version;
    }

    public int getBePort() {
        return bePort;
    }

    public int getHeartbeatPort() {
        return heartbeatPort;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public int getBeRpcPort() {
        return beRpcPort;
    }

    public int getBrpcPort() {
        return brpcPort;
    }

    public int getArrowFlightSqlPort() {
        return arrowFlightSqlPort;
    }

    public String getHeartbeatErrMsg() {
        return heartbeatErrMsg;
    }

    public long getLastStreamLoadTime() {
        return this.backendStatus.lastStreamLoadTime;
    }

    public void setLastStreamLoadTime(long lastStreamLoadTime) {
        this.backendStatus.lastStreamLoadTime = lastStreamLoadTime;
    }

    // ATTN: This method only return the value of "isQueryDisabled",
    // it does not determine the backend IS queryable or not, use isQueryAvailable instead.
    public boolean isQueryDisabled() {
        return backendStatus.isQueryDisabled;
    }

    // return true if be status is changed
    public boolean setQueryDisabled(boolean isQueryDisabled) {
        if (this.backendStatus.isQueryDisabled != isQueryDisabled) {
            this.backendStatus.isQueryDisabled = isQueryDisabled;
            return true;
        }
        return false;
    }

    // ATTN: This method only return the value of "isLoadDisabled",
    // it does not determine the backend IS loadable or not, use isLoadAvailable instead.
    public boolean isLoadDisabled() {
        return backendStatus.isLoadDisabled;
    }

    // return true if be status is changed
    public boolean setLoadDisabled(boolean isLoadDisabled) {
        if (this.backendStatus.isLoadDisabled != isLoadDisabled) {
            this.backendStatus.isLoadDisabled = isLoadDisabled;
            return true;
        }
        return false;
    }

    private boolean isShutDown() {
        return backendStatus.isShutdown;
    }

    public void setActive(boolean isActive) {
        this.backendStatus.isActive = isActive;
    }

    public boolean isActive() {
        return this.backendStatus.isActive;
    }

    public long getCurrentFragmentNum() {
        return this.backendStatus.currentFragmentNum;
    }

    public String getDetailsForCreateReplica() {
        int hddBad = 0;
        int hddExceedLimit = 0;
        int hddOk = 0;
        int ssdBad = 0;
        int ssdExceedLimit = 0;
        int ssdOk = 0;
        for (DiskInfo disk : disksRef.values()) {
            TStorageMedium storageMedium = disk.getStorageMedium();
            if (storageMedium == TStorageMedium.HDD) {
                if (!disk.isAlive()) {
                    hddBad++;
                } else if (disk.exceedLimit(true)) {
                    hddExceedLimit++;
                } else {
                    hddOk++;
                }
            } else if (storageMedium == TStorageMedium.SSD) {
                if (!disk.isAlive()) {
                    ssdBad++;
                } else if (disk.exceedLimit(true)) {
                    ssdExceedLimit++;
                } else {
                    ssdOk++;
                }
            }
        }

        StringBuilder sb = new StringBuilder("[");
        sb.append("backendId=").append(id);
        sb.append(", host=").append(host);
        if (!isAlive()) {
            sb.append(", isAlive=false, exclude it");
        } else if (isDecommissioned()) {
            sb.append(", isDecommissioned=true, exclude it");
        } else if (isComputeNode()) {
            sb.append(", isComputeNode=true, exclude it");
        } else if (!Config.disable_backend_black_list && !SimpleScheduler.isAvailable(this)) {
            sb.append(", is in black list, exclude it");
        } else {
            sb.append(", hdd disks count={");
            if (hddOk > 0) {
                sb.append("ok=").append(hddOk).append(",");
            }
            if (hddBad > 0) {
                sb.append("bad=").append(hddBad).append(",");
            }
            if (hddExceedLimit > 0) {
                sb.append("capExceedLimit=").append(hddExceedLimit).append(",");
            }
            sb.append("}, ssd disk count={");
            if (ssdOk > 0) {
                sb.append("ok=").append(ssdOk).append(",");
            }
            if (ssdBad > 0) {
                sb.append("bad=").append(ssdBad).append(",");
            }
            if (ssdExceedLimit > 0) {
                sb.append("capExceedLimit=").append(ssdExceedLimit).append(",");
            }
            sb.append("}");
        }
        sb.append("]");

        return sb.toString();
    }

    // for test only
    public void updateOnce(int bePort, int httpPort, int beRpcPort) {
        if (this.bePort != bePort) {
            this.bePort = bePort;
        }

        if (this.httpPort != httpPort) {
            this.httpPort = httpPort;
        }

        if (this.beRpcPort != beRpcPort) {
            this.beRpcPort = beRpcPort;
        }

        long currentTime = System.currentTimeMillis();
        this.lastUpdateMs = currentTime;
        if (!isAlive.get()) {
            this.lastStartTime = currentTime;
            LOG.info("{} is alive,", this.toString());
            this.isAlive.set(true);
        }

        heartbeatErrMsg = "";
    }

    public boolean setDecommissioned(boolean isDecommissioned) {
        if (this.isDecommissioned.compareAndSet(!isDecommissioned, isDecommissioned)) {
            LOG.warn("{} set decommission: {}", this.toString(), isDecommissioned);
            return true;
        }
        return false;
    }

    public boolean setDecommissioning(boolean isDecommissioning) {
        if (this.isDecommissioning.compareAndSet(!isDecommissioning, isDecommissioning)) {
            LOG.warn("{} set decommissioning: {}", this.toString(), isDecommissioning);
            return true;
        }
        return false;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setAlive(boolean isAlive) {
        this.isAlive.set(isAlive);
    }

    public void setBePort(int agentPort) {
        this.bePort = agentPort;
    }

    public void setHttpPort(int httpPort) {
        this.httpPort = httpPort;
    }

    public void setBeRpcPort(int beRpcPort) {
        this.beRpcPort = beRpcPort;
    }

    public void setBrpcPort(int brpcPort) {
        this.brpcPort = brpcPort;
    }

    public void setArrowFlightSqlPort(int arrowFlightSqlPort) {
        this.arrowFlightSqlPort = arrowFlightSqlPort;
    }

    public void setCpuCores(int cpuCores) {
        this.cpuCores = cpuCores;
    }

    public void setPipelineExecutorSize(int pipelineExecutorSize) {
        this.pipelineExecutorSize = pipelineExecutorSize;
    }

    public long getLastUpdateMs() {
        return this.lastUpdateMs;
    }

    public long getLiveSince() {
        return this.lastUpdateMs;
    }

    public void setLastUpdateMs(long currentTime) {
        this.lastUpdateMs = currentTime;
    }

    public long getLastStartTime() {
        return this.lastStartTime;
    }

    public void setLastStartTime(long currentTime) {
        this.lastStartTime = currentTime;
    }

    public int getCputCores() {
        return cpuCores;
    }

    public long getBeMemory() {
        return beMemory;
    }

    public int getPipelineExecutorSize() {
        return pipelineExecutorSize;
    }

    public long getLastMissingHeartbeatTime() {
        return lastMissingHeartbeatTime;
    }

    public void setLastMissingHeartbeatTime(long lastMissingHeartbeatTime) {
        this.lastMissingHeartbeatTime = lastMissingHeartbeatTime;
    }

    // Backend process epoch, is uesd to tag a beckend process
    // Currently it is always equal to be start time, even during oplog replay.
    public long getProcessEpoch() {
        return lastStartTime;
    }

    public boolean isAlive() {
        return this.isAlive.get();
    }

    public boolean isDecommissioned() {
        return this.isDecommissioned.get();
    }

    public boolean isDecommissioning() {
        return this.isDecommissioning.get();
    }

    public boolean isQueryAvailable() {
        String debugDeadBeIds = DebugPointUtil.getDebugParamOrDefault(
                "Backend.isQueryAvailable", "unavailableBeIds", "");
        if (!Strings.isNullOrEmpty(debugDeadBeIds)
                && Arrays.stream(debugDeadBeIds.split(",")).anyMatch(id -> Long.parseLong(id) == this.id)) {
            return false;
        }
        return isAlive() && !isQueryDisabled() && !isShutDown();
    }

    public boolean isScheduleAvailable() {
        return isAlive() && !isDecommissioned() && !isShutDown();
    }

    public boolean isLoadAvailable() {
        return isAlive() && !isLoadDisabled() && !isShutDown();
    }

    public void setDisks(ImmutableMap<String, DiskInfo> disks) {
        this.disksRef = disks;
    }

    public BackendStatus getBackendStatus() {
        return backendStatus;
    }

    public void setSmoothUpgradeSrc(boolean is) {
        this.isSmoothUpgradeSrc = is;
    }

    public boolean isSmoothUpgradeSrc() {
        return this.isSmoothUpgradeSrc;
    }

    public void setSmoothUpgradeDst(boolean is) {
        this.isSmoothUpgradeDst = is;
    }

    public boolean isSmoothUpgradeDst() {
        return this.isSmoothUpgradeDst;
    }

    public int getHeartbeatFailureCounter() {
        return heartbeatFailureCounter;
    }

    public ImmutableMap<String, DiskInfo> getDisks() {
        return this.disksRef;
    }

    public boolean hasPathHash() {
        return disksRef.values().stream().allMatch(DiskInfo::hasPathHash);
    }

    public boolean hasSpecifiedStorageMedium(TStorageMedium storageMedium) {
        return disksRef.values().stream().anyMatch(d -> d.isStorageMediumMatch(storageMedium));
    }

    public long getTotalCapacityB() {
        ImmutableMap<String, DiskInfo> disks = disksRef;
        long totalCapacityB = 0L;
        for (DiskInfo diskInfo : disks.values()) {
            if (diskInfo.getState() == DiskState.ONLINE) {
                totalCapacityB += diskInfo.getTotalCapacityB();
            }
        }
        return totalCapacityB;
    }

    public long getAvailableCapacityB() {
        // when system init, disks is empty, return 1L.
        ImmutableMap<String, DiskInfo> disks = disksRef;
        long availableCapacityB = 1L;
        for (DiskInfo diskInfo : disks.values()) {
            if (diskInfo.getState() == DiskState.ONLINE) {
                availableCapacityB += diskInfo.getAvailableCapacityB();
            }
        }
        return availableCapacityB;
    }

    public long getDataUsedCapacityB() {
        ImmutableMap<String, DiskInfo> disks = disksRef;
        long dataUsedCapacityB = 0L;
        for (DiskInfo diskInfo : disks.values()) {
            if (diskInfo.getState() == DiskState.ONLINE) {
                dataUsedCapacityB += diskInfo.getDataUsedCapacityB();
            }
        }
        return dataUsedCapacityB;
    }

    public long getTrashUsedCapacityB() {
        ImmutableMap<String, DiskInfo> disks = disksRef;
        long trashUsedCapacityB = 0L;
        for (DiskInfo diskInfo : disks.values()) {
            if (diskInfo.getState() == DiskState.ONLINE) {
                trashUsedCapacityB += diskInfo.getTrashUsedCapacityB();
            }
        }
        return trashUsedCapacityB;
    }

    public long getRemoteUsedCapacityB() {
        ImmutableMap<String, DiskInfo> disks = disksRef;
        long totalRemoteUsedCapacityB = 0L;
        for (DiskInfo diskInfo : disks.values()) {
            if (diskInfo.getState() == DiskState.ONLINE) {
                totalRemoteUsedCapacityB += diskInfo.getRemoteUsedCapacity();
            }
        }
        return totalRemoteUsedCapacityB;
    }

    public double getMaxDiskUsedPct() {
        ImmutableMap<String, DiskInfo> disks = disksRef;
        double maxPct = 0.0;
        for (DiskInfo diskInfo : disks.values()) {
            if (diskInfo.getState() == DiskState.ONLINE) {
                double percent = diskInfo.getUsedPct();
                if (percent > maxPct) {
                    maxPct = percent;
                }
            }
        }
        return maxPct;
    }

    public boolean diskExceedLimitByStorageMedium(TStorageMedium storageMedium) {
        if (getDiskNumByStorageMedium(storageMedium) <= 0) {
            return true;
        }
        ImmutableMap<String, DiskInfo> diskInfos = disksRef;
        boolean exceedLimit = true;
        for (DiskInfo diskInfo : diskInfos.values()) {
            if (diskInfo.getState() == DiskState.ONLINE && diskInfo.getStorageMedium()
                    == storageMedium && !diskInfo.exceedLimit(true)) {
                exceedLimit = false;
                break;
            }
        }
        return exceedLimit;
    }

    public boolean diskExceedLimit() {
        if (getDiskNum() <= 0) {
            return true;
        }
        ImmutableMap<String, DiskInfo> diskInfos = disksRef;
        boolean exceedLimit = true;
        for (DiskInfo diskInfo : diskInfos.values()) {
            if (diskInfo.getState() == DiskState.ONLINE && !diskInfo.exceedLimit(true)) {
                exceedLimit = false;
                break;
            }
        }
        return exceedLimit;
    }
    // 核心磁盘更新方法。解析从 BE 汇报上来的最新磁盘状态，识别新增、删除或状态变更的磁盘，触发路径信息更新并写 EditLog。
    public void updateDisks(Map<String, TDisk> backendDisks) {
        ImmutableMap<String, DiskInfo> disks = disksRef;
        // The very first time to init the path info
        if (!initPathInfo) {
            boolean allPathHashUpdated = true;
            for (DiskInfo diskInfo : disks.values()) {
                if (diskInfo.getPathHash() == 0) {
                    allPathHashUpdated = false;
                    break;
                }
            }
            if (allPathHashUpdated) {
                initPathInfo = true;
                Env.getCurrentSystemInfo().updatePathInfo(new ArrayList<>(disks.values()), Lists.newArrayList());
            }
        }

        // update status or add new diskInfo
        Map<String, DiskInfo> newDiskInfos = Maps.newHashMap();
        List<DiskInfo> addedDisks = Lists.newArrayList();
        List<DiskInfo> removedDisks = Lists.newArrayList();
        /*
         * set isChanged to true only if new disk is added or old disk is dropped.
         * we ignore the change of capacity, because capacity info is only used in master FE.
         */
        boolean isChanged = false;
        for (TDisk tDisk : backendDisks.values()) {
            String rootPath = tDisk.getRootPath();
            long totalCapacityB = tDisk.getDiskTotalCapacity();
            long dataUsedCapacityB = tDisk.getDataUsedCapacity();
            long trashUsedCapacityB = tDisk.getTrashUsedCapacity();
            long diskAvailableCapacityB = tDisk.getDiskAvailableCapacity();
            boolean isUsed = tDisk.isUsed();
            DiskInfo diskInfo = disks.get(rootPath);
            if (diskInfo == null) {
                diskInfo = new DiskInfo(rootPath);
                addedDisks.add(diskInfo);
                isChanged = true;
                LOG.info("add new disk info. backendId: {}, rootPath: {}", id, rootPath);
            }
            newDiskInfos.put(rootPath, diskInfo);

            diskInfo.setTotalCapacityB(totalCapacityB);
            diskInfo.setDataUsedCapacityB(dataUsedCapacityB);
            diskInfo.setTrashUsedCapacityB(trashUsedCapacityB);
            diskInfo.setAvailableCapacityB(diskAvailableCapacityB);
            if (tDisk.isSetRemoteUsedCapacity()) {
                diskInfo.setRemoteUsedCapacity(tDisk.getRemoteUsedCapacity());
            }

            if (tDisk.isSetPathHash()) {
                diskInfo.setPathHash(tDisk.getPathHash());
            }

            if (tDisk.isSetStorageMedium()) {
                diskInfo.setStorageMedium(tDisk.getStorageMedium());
            }

            if (isUsed) {
                if (diskInfo.setState(DiskState.ONLINE)) {
                    isChanged = true;
                }
            } else {
                if (diskInfo.setState(DiskState.OFFLINE)) {
                    isChanged = true;
                }
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("update disk info. backendId: {}, diskInfo: {}", id, diskInfo.toString());
            }
        }

        // remove not exist rootPath in backend
        for (DiskInfo diskInfo : disks.values()) {
            String rootPath = diskInfo.getRootPath();
            if (!backendDisks.containsKey(rootPath)) {
                removedDisks.add(diskInfo);
                isChanged = true;
                LOG.warn("remove not exist rootPath. backendId: {}, rootPath: {}", id, rootPath);
            }
        }

        if (isChanged) {
            // update disksRef
            disksRef = ImmutableMap.copyOf(newDiskInfos);
            Env.getCurrentSystemInfo().updatePathInfo(addedDisks, removedDisks);
            // log disk changing
            Env.getCurrentEnv().getEditLog().logBackendStateChange(this);
        }
    }

    public boolean updateCpuInfo(int cpuCores, int pipelineExecutorSize) {
        boolean isChanged = false;

        if (this.cpuCores != cpuCores) {
            this.cpuCores = cpuCores;
            isChanged = true;
        }
        if (this.pipelineExecutorSize != pipelineExecutorSize) {
            this.pipelineExecutorSize = pipelineExecutorSize;
            isChanged = true;
        }
        return isChanged;
    }

    /**
     * In old version, there is only one tag for a Backend, and it is a "location" type tag.
     * But in new version, a Backend can have multi tag, so we need to put locationTag to
     * the new tagMap
     */
    private void convertToTagMapAndSetLocationTag() {
        if (tagMap == null) {
            // When first upgrade from old version, tags may be null
            tagMap = Maps.newHashMap();
        }
        if (!locationTag.value.equals(tagMap.get(Tag.TYPE_LOCATION))) {
            // ATTN: here we use Tag.TYPE_LOCATION directly, not locationTag.type,
            // because we need to make sure the previous tag must be a location type tag,
            // and if not, convert it to location type.
            tagMap.put(Tag.TYPE_LOCATION, locationTag.value);
        }
    }

    public static Backend read(DataInput in) throws IOException {
        String json = Text.readString(in);
        Backend be = GsonUtils.GSON.fromJson(json, Backend.class);
        be.convertToTagMapAndSetLocationTag();
        return be;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, host, heartbeatPort, bePort, isAlive.get());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Backend)) {
            return false;
        }

        Backend backend = (Backend) obj;

        return (id == backend.id) && (host.equals(backend.host)) && (heartbeatPort == backend.heartbeatPort) && (bePort
                == backend.bePort) && (isAlive.get() == backend.isAlive.get());
    }

    @Override
    public String toString() {
        return "Backend [id=" + id + ", host=" + host + ", heartbeatPort=" + heartbeatPort + ", alive=" + isAlive.get()
                + ", lastStartTime=" + TimeUtils.longToTimeString(lastStartTime) + ", process epoch=" + lastStartTime
                + ", isDecommissioned=" + isDecommissioned + ", tags: " + tagMap + "]"
                + ", backendStatus: " + backendStatus;
    }

    /**
     * handle Backend's heartbeat response.
     * return true if any port changed, or alive state is changed.
     */
    // 核心心跳响应处理方法。解析 BE 返回的心跳响应：若心跳成功（HbStatus.OK），则更新版本号、各项端口、服务状态、内存、心跳时间戳；
    // 若节点由死转生，则更新启动时间戳（lastStartTime）与存活状态，并返回状态或端口是否发生过改变。
    public boolean handleHbResponse(BackendHbResponse hbResponse, boolean isReplay) {
        boolean isChanged = false;
        if (hbResponse.getStatus() == HbStatus.OK) {
            if (!this.version.equals(hbResponse.getVersion())) {
                isChanged = true;
                this.version = hbResponse.getVersion();
            }

            if (this.bePort != hbResponse.getBePort() && !FeConstants.runningUnitTest) {
                isChanged = true;
                this.bePort = hbResponse.getBePort();
            }

            if (this.httpPort != hbResponse.getHttpPort() && !FeConstants.runningUnitTest) {
                isChanged = true;
                this.httpPort = hbResponse.getHttpPort();
            }

            if (this.brpcPort != hbResponse.getBrpcPort() && !FeConstants.runningUnitTest) {
                isChanged = true;
                this.brpcPort = hbResponse.getBrpcPort();
            }

            if (this.arrowFlightSqlPort != hbResponse.getArrowFlightSqlPort() && !FeConstants.runningUnitTest) {
                isChanged = true;
                this.arrowFlightSqlPort = hbResponse.getArrowFlightSqlPort();
            }

            if (this.backendStatus.isShutdown != hbResponse.isShutDown()) {
                isChanged = true;
                LOG.info("{} shutdown state is changed", this.toString());
                this.backendStatus.isShutdown = hbResponse.isShutDown();
            }

            if (!this.getNodeRoleTag().value.equals(hbResponse.getNodeRole()) && Tag.validNodeRoleTag(
                    hbResponse.getNodeRole())) {
                isChanged = true;
                this.nodeRoleTag = Tag.createNotCheck(Tag.TYPE_ROLE, hbResponse.getNodeRole());
            }
            if (this.beMemory != hbResponse.getBeMemory()) {
                isChanged = true;
                this.beMemory = hbResponse.getBeMemory();
            }

            this.lastUpdateMs = hbResponse.getHbTime();
            if (!isAlive.get()) {
                isChanged = true;
                LOG.info("{} is back to alive, update start time from {} to {}, "
                        + "update be epoch from {} to {}.", this.toString(),
                        TimeUtils.longToTimeString(lastStartTime),
                        TimeUtils.longToTimeString(hbResponse.getBeStartTime()),
                        lastStartTime, hbResponse.getBeStartTime());
                this.lastStartTime = hbResponse.getBeStartTime();
                this.liveSince = System.currentTimeMillis();
                this.isAlive.set(true);
            }

            if (this.lastStartTime != hbResponse.getBeStartTime() && hbResponse.getBeStartTime() > 0) {
                LOG.info("{} update last start time from {} to {}, "
                        + "update be epoch from {} to {}.", this.toString(),
                        TimeUtils.longToTimeString(lastStartTime),
                        TimeUtils.longToTimeString(hbResponse.getBeStartTime()),
                        lastStartTime, hbResponse.getBeStartTime());
                this.lastStartTime = hbResponse.getBeStartTime();
                isChanged = true;
            }

            this.backendStatus.currentFragmentNum = hbResponse.getFragmentNum();
            this.backendStatus.lastFragmentUpdateTime = hbResponse.getLastFragmentUpdateTime();

            heartbeatErrMsg = "";
            this.heartbeatFailureCounter = 0;

            // even if no change, write an editlog to make lastUpdateMs in image update
            if (System.currentTimeMillis() >= this.nextForceEditlogHeartbeatTime) {
                isChanged = true;
                int delaySecond = Config.editlog_healthy_heartbeat_seconds + (new SecureRandom()).nextInt(60);
                this.nextForceEditlogHeartbeatTime = System.currentTimeMillis() + delaySecond * 1000L;
            }
        } else {
            // for a bad BackendHbResponse, its hbTime is last succ hbTime, not this hbTime
            if (hbResponse.getHbTime() > 0) {
                this.lastUpdateMs = hbResponse.getHbTime();
            }
            // Only set backend to dead if the heartbeat failure counter exceed threshold.
            // And if it is a replay process, must set backend to dead.
            if (isReplay || ++this.heartbeatFailureCounter >= Config.max_backend_heartbeat_failure_tolerance_count) {
                if (isAlive.compareAndSet(true, false)) {
                    isChanged = true;
                    LOG.warn("{} is dead,", this.toString());
                }
            }

            // still set error msg and missing time even if we may not mark this backend as dead,
            // for debug easily.
            // But notice that if isChanged = false, these msg will not sync to other FE.
            heartbeatErrMsg = hbResponse.getMsg() == null ? "Unknown error" : hbResponse.getMsg();
            lastMissingHeartbeatTime = System.currentTimeMillis();
        }

        return isChanged;
    }

    public void setTabletMaxCompactionScore(long compactionScore) {
        tabletMaxCompactionScore = compactionScore;
    }

    public long getTabletMaxCompactionScore() {
        return tabletMaxCompactionScore;
    }

    private long getDiskNumByStorageMedium(TStorageMedium storageMedium) {
        return disksRef.values().stream().filter(v -> v.getStorageMedium() == storageMedium).count();
    }

    private int getDiskNum() {
        return disksRef.size();
    }

    public Long getRunningTasks() {
        return runningTasks;
    }

    public void setRunningTasks(Long runningTasks) {
        this.runningTasks = runningTasks;
    }

    /**
     * Note: This class must be a POJO in order to display in JSON format
     * Add additional information in the class to show in `show backends`
     * if just change new added backendStatus, you can do like following
     *     BackendStatus status = Backend.getBackendStatus();
     *     status.newItem = xxx;
     */
    public class BackendStatus {
        // this will be output as json, so not using FeConstants.null_string;
        public volatile String lastSuccessReportTabletsTime = "N/A";
        @SerializedName("lastStreamLoadTime")
        // the last time when the stream load status was reported by backend
        public volatile long lastStreamLoadTime = -1;
        @SerializedName("isQueryDisabled")
        public volatile boolean isQueryDisabled = false;
        @SerializedName("isLoadDisabled")
        public volatile boolean isLoadDisabled = false;
        @SerializedName("isActive")
        public volatile boolean isActive = true;
        @SerializedName("isShutdown")
        public volatile boolean isShutdown = false;

        // cloud mode, cloud control just query master, so not need SerializedName
        public volatile long currentFragmentNum = 0;
        public volatile long lastFragmentUpdateTime = 0;

        @Override
        public String toString() {
            return "[" + "lastSuccessReportTabletsTime='" + lastSuccessReportTabletsTime + '\''
                    + ", lastStreamLoadTime=" + lastStreamLoadTime + ", isQueryDisabled=" + isQueryDisabled
                    + ", isLoadDisabled=" + isLoadDisabled
                    + ", currentFragmentNum=" + currentFragmentNum
                    + ", lastFragmentUpdateTime=" + lastFragmentUpdateTime + "]";
        }
    }

    public Tag getLocationTag() {
        return locationTag;
    }

    public Tag getNodeRoleTag() {
        return nodeRoleTag;
    }

    public boolean isMixNode() {
        return nodeRoleTag.value.equals(Tag.VALUE_MIX);
    }

    public boolean isComputeNode() {
        return nodeRoleTag.value.equals(Tag.VALUE_COMPUTATION);
    }

    public void setTagMap(Map<String, String> tagMap) {
        Preconditions.checkState(tagMap.containsKey(Tag.TYPE_LOCATION));
        this.tagMap = tagMap;
        this.locationTag = Tag.createNotCheck(Tag.TYPE_LOCATION, tagMap.get(Tag.TYPE_LOCATION));
        if (tagMap.containsKey(Tag.TYPE_ROLE) && Tag.validNodeRoleTag(tagMap.get(Tag.TYPE_ROLE))) {
            this.nodeRoleTag = Tag.createNotCheck(Tag.TYPE_ROLE, tagMap.get(Tag.TYPE_ROLE));
        }
    }

    public Map<String, String> getTagMap() {
        return tagMap;
    }

    public TNetworkAddress getBrpcAddress() {
        return new TNetworkAddress(getHost(), getBrpcPort());
    }

    public TNetworkAddress getHeartbeatAddress() {
        return new TNetworkAddress(getHost(), getHeartbeatPort());
    }

    public TNetworkAddress getArrowFlightAddress() {
        return new TNetworkAddress(getHost(), getArrowFlightSqlPort());
    }

    // Only used for users, we hide and rename some internal tags.
    public String getTagMapString() {
        Map<String, String> displayTagMap = Maps.newHashMap();
        displayTagMap.putAll(tagMap);

        // Migrate old cloud endpoint tags to new endpoint tags for backward compatibility
        migrateEndpointTag(displayTagMap, "cloud_cluster_public_endpoint", "public_endpoint");
        migrateEndpointTag(displayTagMap, "cloud_cluster_private_endpoint", "private_endpoint");
        if (displayTagMap.containsKey("cloud_cluster_status")) {
            displayTagMap.put("compute_group_status", displayTagMap.remove("cloud_cluster_status"));
        }
        if (displayTagMap.containsKey("cloud_cluster_id")) {
            displayTagMap.put("compute_group_id", displayTagMap.remove("cloud_cluster_id"));
        }
        if (displayTagMap.containsKey("cloud_cluster_name")) {
            displayTagMap.put("compute_group_name", displayTagMap.remove("cloud_cluster_name"));
        }

        return "{" + new DatasourcePrintableMap<>(displayTagMap, ":", true, false).toString() + "}";
    }

    public Long getPublishTaskLastTimeAccumulated() {
        return this.lastPublishTaskAccumulatedNum;
    }

    public void setPublishTaskLastTimeAccumulated(Long accumulatedNum) {
        this.lastPublishTaskAccumulatedNum = accumulatedNum;
    }

    public String getComputeGroup() {
        if (Config.isCloudMode()) {
            return getCloudClusterId();
        } else {
            return getLocationTag().value;
        }
    }

    /**
     * Migrate endpoint tag from old name to new name for backward compatibility.
     * If new tag already exists, just remove the old tag.
     * If new tag doesn't exist, rename old tag to new tag.
     */
    private void migrateEndpointTag(Map<String, String> tagMap, String oldTagName, String newTagName) {
        if (tagMap.containsKey(oldTagName)) {
            if (tagMap.containsKey(newTagName)) {
                // New tag exists, just remove the old one
                tagMap.remove(oldTagName);
            } else {
                // New tag doesn't exist, migrate old tag to new tag
                tagMap.put(newTagName, tagMap.remove(oldTagName));
            }
        }
    }

    public static Backend fromThrift(TBackend backend) {
        Backend result = new Backend();
        result.id = backend.getId();
        result.host = backend.getHost();
        result.httpPort = backend.getHttpPort();
        result.brpcPort = backend.getBrpcPort();
        result.bePort = backend.getBePort();
        result.setAlive(backend.isIsAlive());
        return result;
    }

}

