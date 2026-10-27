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

import org.apache.doris.catalog.ColocateTableIndex.GroupId;
import org.apache.doris.catalog.DiskInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.cloud.qe.ComputeGroupException;
import org.apache.doris.cluster.ClusterGuard;
import org.apache.doris.cluster.ClusterGuardException;
import org.apache.doris.cluster.ClusterGuardFactory;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.CountingDataOutputStream;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyBackendHostNameOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyBackendOp;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.Tag;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.io.DataInputStream;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

// 负责集群节点（主要是 Backend / BE 节点）管理、系统拓扑维护、健康状态追踪以及副本调度策略选择的核心服务类。
// 集群节点生命周期管理：统一维护集群中所有 BE 节点的增删改查（如 addBackend、dropBackend、decommissionBackend），并负责将节点的变更操作记录到元数据日志（EditLog）中。
// 多维节点查询与匹配：提供按 ID、IP、心跳端口、Be 端口等多种维度的节点查找方式，支持区分存活节点、下线节点、混部节点（Mix）及计算节点（CN）。
// 分片与副本放置策略（Replica Allocation / Selection）：基于 ReplicaAllocation 和 BeSelectionPolicy（后端选择策略），为建表或扩缩容时的 Tablet 创建、Round-Robin 轮询选择提供最优的后端节点组合。
// 版本与心跳跟踪：维护每个 BE 的上报版本（Report Version）以及路径信息引用（PathHash to DiskInfo），确保元数据的一致性和并发安全性。
public class SystemInfoService {
    private static final Logger LOG = LogManager.getLogger(SystemInfoService.class);
    // 默认集群名称。
    public static final String DEFAULT_CLUSTER = "default_cluster";
    // 无可用导入后端时的标准提示错误信息。
    public static final String NO_BACKEND_LOAD_AVAILABLE_MSG =
            "No backend available for load, please check the status of your backends.";
    // 无可用扫描节点后端时的标准提示错误信息。
    public static final String NO_SCAN_NODE_BACKEND_AVAILABLE_MSG =
            "No backend available as scan node, please check the status of your backends.";
    // 未使用有效云集群时的提示信息。
    public static final String NOT_USING_VALID_CLUSTER_MSG =
            "Not using valid cloud clusters, please use a cluster before issuing any queries";
    // 特定的错误码常量。
    public static final String ERROR_E230 = "E-230";
    // 需要触发重新规划（Replan）的错误消息集合。
    public static final ImmutableSet<String> NEED_REPLAN_ERRORS = ImmutableSet.of(
            NO_SCAN_NODE_BACKEND_AVAILABLE_MSG,
            ERROR_E230,
            ComputeGroupException.FailedTypeEnum.COMPUTE_GROUPS_NO_ALIVE_BE.toString()
    );
    // 节点 ID 到 Backend 对象的不可变映射。使用 volatile 和不可变集合保证高并发读写时的线程安全。
    protected volatile ImmutableMap<Long, Backend> idToBackendRef = ImmutableMap.of();
    // 节点 ID 到其最新上报版本（Report Version）的原子长整型映射。
    protected volatile ImmutableMap<Long, AtomicLong> idToReportVersionRef = ImmutableMap.of();
    // 磁盘路径哈希（PathHash）到 DiskInfo 的映射。
    private volatile ImmutableMap<Long, DiskInfo> pathHashToDiskInfoRef = ImmutableMap.of();

    public void invalidateCloudColocatePlacement(GroupId groupId) {
    }
    // 封装了主机的网络地址信息（host 和 port）
    public static class HostInfo implements Comparable<HostInfo> {
        public String host;
        public int port;

        public HostInfo(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getIdent() {
            return host + "_" + port;
        }

        @Override
        public int compareTo(@NotNull HostInfo o) {
            int res = host.compareTo(o.getHost());
            if (res == 0) {
                return Integer.compare(port, o.getPort());
            }
            return res;
        }

        public boolean isSame(HostInfo other) {
            if (other.getPort() != port) {
                return false;
            }
            return host.equals(other.getHost());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            HostInfo that = (HostInfo) o;
            return Objects.equals(host, that.getHost())
                    && Objects.equals(port, that.getPort());
        }

        @Override
        public String toString() {
            return "HostInfo{"
                    + "host='" + host + '\''
                    + ", port=" + port
                    + '}';
        }
    }

    // sort host backends list by num of backends, descending
    // 静态比较器，用于按后端列表的大小进行降序排序。
    private static final Comparator<List<Backend>> hostBackendsListComparator = new Comparator<List<Backend>>() {
        @Override
        public int compare(List<Backend> list1, List<Backend> list2) {
            if (list1.size() > list2.size()) {
                return -1;
            } else {
                return 1;
            }
        }
    };

    // for deploy manager
    // 使用默认的后端标签（Tag.DEFAULT_BACKEND_TAG）批量添加后端节点。
    public void addBackends(List<HostInfo> hostInfos, boolean isFree)
            throws UserException {
        addBackends(hostInfos, Tag.DEFAULT_BACKEND_TAG.toMap());
    }

    /**
     * @param hostInfos : backend's ip, hostName and port
     * @throws DdlException
     */
    // 批量添加后端节点的入口。
    // 遍历检查要添加的节点是否已存在（避免重复添加）；通过 ClusterGuard 校验集群许可时间有效性和最大节点数限制；校验通过后逐个调用底层 addBackend。
    public void addBackends(List<HostInfo> hostInfos, Map<String, String> tagMap) throws UserException {
        for (HostInfo hostInfo : hostInfos) {
            // check is already exist
            if (getBackendWithHeartbeatPort(hostInfo.getHost(), hostInfo.getPort()) != null) {
                String backendIdentifier = hostInfo.getHost() + ":"
                        + hostInfo.getPort();
                throw new DdlException("Same backend already exists[" + backendIdentifier + "]");
            }
        }

        // check cluster guard policy: time validity and node limit
        ClusterGuard clusterGuard = ClusterGuardFactory.getGuard();
        try {
            clusterGuard.checkTimeValidity();
            int currentCount = getAllClusterBackendsNoException().size();
            clusterGuard.checkNodeLimit(currentCount + hostInfos.size());
        } catch (ClusterGuardException e) {
            throw new DdlException("Cluster guard restriction: " + e.getMessage());
        }

        for (HostInfo hostInfo : hostInfos) {
            addBackend(hostInfo.getHost(), hostInfo.getPort(), tagMap);
        }
    }

    // for test
    public void addBackend(Backend backend) {
        Map<Long, Backend> copiedBackends = Maps.newHashMap(getAllClusterBackendsNoException());
        copiedBackends.put(backend.getId(), backend);
        ImmutableMap<Long, Backend> newIdToBackend = ImmutableMap.copyOf(copiedBackends);
        idToBackendRef = newIdToBackend;
    }

    // Final entry of adding backend
    // 添加 BE 节点的最终底层执行逻辑。
    private void addBackend(String host, int heartbeatPort, Map<String, String> tagMap) {
        Backend newBackend = new Backend(Env.getCurrentEnv().getNextId(), host, heartbeatPort);
        // update idToBackend
        Map<Long, Backend> copiedBackends = Maps.newHashMap(getAllClusterBackendsNoException());
        copiedBackends.put(newBackend.getId(), newBackend);
        ImmutableMap<Long, Backend> newIdToBackend = ImmutableMap.copyOf(copiedBackends);
        idToBackendRef = newIdToBackend;

        // set new backend's report version as 0L
        Map<Long, AtomicLong> copiedReportVersions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVersions.put(newBackend.getId(), new AtomicLong(0L));
        ImmutableMap<Long, AtomicLong> newIdToReportVersion = ImmutableMap.copyOf(copiedReportVersions);
        idToReportVersionRef = newIdToReportVersion;

        // set tags
        newBackend.setTagMap(tagMap);

        // log
        Env.getCurrentEnv().getEditLog().logAddBackend(newBackend);
        LOG.info("finished to add {} ", newBackend);

        // backends is changed, regenerated tablet number metrics
        MetricRepo.generateBackendsTabletMetrics();
    }

    public void dropBackends(List<HostInfo> hostInfos) throws DdlException {
        // check cluster guard time validity
        try {
            ClusterGuardFactory.getGuard().checkTimeValidity();
        } catch (ClusterGuardException e) {
            throw new DdlException("Cluster guard restriction: " + e.getMessage());
        }

        for (HostInfo hostInfo : hostInfos) {
            // check is already exist
            if (getBackendWithHeartbeatPort(hostInfo.getHost(), hostInfo.getPort()) == null) {
                String backendIdentifier = NetUtils
                        .getHostPortInAccessibleFormat(hostInfo.getHost(), hostInfo.getPort());
                throw new DdlException("backend does not exists[" + backendIdentifier + "]");
            }
            dropBackend(hostInfo.getHost(), hostInfo.getPort());
        }
    }

    public void dropBackendsByIds(List<String> ids) throws DdlException {

        for (String id : ids) {
            if (getBackend(Long.parseLong(id)) == null) {
                throw new DdlException("backend does not exists[" + id + "]");
            }
            dropBackend(Long.parseLong(id));
        }

    }

    // for decommission
    public void dropBackend(long backendId) throws DdlException {
        Backend backend = getBackend(backendId);
        if (backend == null) {
            throw new DdlException("Backend[" + backendId + "] does not exist");
        }
        dropBackend(backend.getHost(), backend.getHeartbeatPort());
    }

    // final entry of dropping backend
    public void dropBackend(String host, int heartbeatPort) throws DdlException {
        Backend droppedBackend = getBackendWithHeartbeatPort(host, heartbeatPort);
        if (droppedBackend == null) {
            throw new DdlException("backend does not exists[" + NetUtils
                    .getHostPortInAccessibleFormat(host, heartbeatPort) + "]");
        }
        // update idToBackend
        Map<Long, Backend> copiedBackends = Maps.newHashMap(getAllClusterBackendsNoException());
        copiedBackends.remove(droppedBackend.getId());
        ImmutableMap<Long, Backend> newIdToBackend = ImmutableMap.copyOf(copiedBackends);
        idToBackendRef = newIdToBackend;

        // update idToReportVersion
        Map<Long, AtomicLong> copiedReportVersions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVersions.remove(droppedBackend.getId());
        ImmutableMap<Long, AtomicLong> newIdToReportVersion = ImmutableMap.copyOf(copiedReportVersions);
        idToReportVersionRef = newIdToReportVersion;

        // log
        Env.getCurrentEnv().getEditLog().logDropBackend(droppedBackend);
        LOG.info("finished to drop {}", droppedBackend);

        // backends is changed, regenerated tablet number metrics
        MetricRepo.generateBackendsTabletMetrics();
    }
    // 将指定的后端节点标记为 decommissioned（下线中/数据迁移中状态），并记录状态变更日志到 EditLog。
    public void decommissionBackend(Backend backend) throws UserException {
        // set backend's state as 'decommissioned'
        // for decommission operation, here is no decommission job. the system handler will check
        // all backend in decommission state
        backend.setDecommissioned(true);
        Env.getCurrentEnv().getEditLog().logBackendStateChange(backend);
        LOG.info("set backend {} to decommission", backend.getId());
    }

    // only for test
    public void dropAllBackend() {
        // update idToBackend
        idToBackendRef = ImmutableMap.<Long, Backend>of();
        // update idToReportVersion
        idToReportVersionRef = ImmutableMap.<Long, AtomicLong>of();
    }

    public Backend getBackend(long backendId) {
        return getAllClusterBackendsNoException().get(backendId);
    }

    public Backend getBackendByIdWithBoxedId(Long backendId) {
        return getAllClusterBackendsNoException().get(backendId);
    }

    public List<Backend> getBackends(List<Long> backendIds) {
        List<Backend> backends = Lists.newArrayList();
        for (long backendId : backendIds) {
            Backend backend = getBackend(backendId);
            if (backend != null) {
                backends.add(backend);
            }
        }
        return backends;
    }

    public boolean checkBackendScheduleAvailable(long backendId) {
        Backend backend = getAllClusterBackendsNoException().get(backendId);
        if (backend == null || !backend.isScheduleAvailable()) {
            return false;
        }
        return true;
    }

    public boolean checkBackendAlive(long backendId) {
        Backend backend = getAllClusterBackendsNoException().get(backendId);
        if (backend == null || !backend.isAlive()) {
            return false;
        }
        return true;
    }

    public Backend getBackendWithHeartbeatPort(String host, int heartPort) {
        ImmutableMap<Long, Backend> idToBackend = getAllClusterBackendsNoException();
        for (Backend backend : idToBackend.values()) {
            if (backend.getHost().equals(host) && backend.getHeartbeatPort() == heartPort) {
                return backend;
            }
        }
        return null;
    }

    public Backend getBackendWithBePort(String ip, int bePort) {
        ImmutableMap<Long, Backend> idToBackend = getAllClusterBackendsNoException();
        for (Backend backend : idToBackend.values()) {
            if (backend.getHost().equals(ip) && backend.getBePort() == bePort) {
                return backend;
            }
        }
        return null;
    }

    public Backend getBackendWithHttpPort(String ip, int httpPort) {
        ImmutableMap<Long, Backend> idToBackend = getAllClusterBackendsNoException();
        for (Backend backend : idToBackend.values()) {
            if (backend.getHost().equals(ip) && backend.getHttpPort() == httpPort) {
                return backend;
            }
        }
        return null;
    }

    public List<Long> getAllBackendIds() {
        return getAllBackendIds(false);
    }

    public int getBackendsNumber(boolean needAlive) {
        int beNumber = ConnectContext.get().getSessionVariable().getBeNumberForTest();
        if (beNumber < 0) {
            beNumber = getAllBackendByCurrentCluster(needAlive).size();
        }
        return beNumber;
    }

    public List<Long> getAllBackendByCurrentCluster(boolean needAlive) {
        try {
            ImmutableMap<Long, Backend> bes = getBackendsByCurrentCluster();
            List<Long> beIds = new ArrayList<>(bes.size());
            for (Backend be : bes.values()) {
                if (!needAlive || be.isAlive()) {
                    beIds.add(be.getId());
                }
            }
            return beIds;
        } catch (AnalysisException e) {
            LOG.warn("failed to get backends by Current Cluster", e);
            return Lists.newArrayList();
        }
    }

    public List<Long> getAllBackendIds(boolean needAlive) {
        ImmutableMap<Long, Backend> idToBackend = getAllClusterBackendsNoException();
        List<Long> backendIds = Lists.newArrayList(idToBackend.keySet());
        if (!needAlive) {
            return backendIds;
        } else {
            Iterator<Long> iter = backendIds.iterator();
            while (iter.hasNext()) {
                Backend backend = this.getBackend(iter.next());
                if (backend == null || !backend.isAlive()) {
                    iter.remove();
                }
            }
            return backendIds;
        }
    }

    public List<Backend> getAllClusterBackends(boolean needAlive) {
        return getAllClusterBackendsNoException().values().stream()
                .filter(be -> !needAlive || be.isAlive())
                .collect(Collectors.toList());
    }

    public List<Long> getDecommissionedBackendIds() {
        ImmutableMap<Long, Backend> idToBackend = getAllClusterBackendsNoException();
        List<Long> backendIds = Lists.newArrayList(idToBackend.keySet());

        Iterator<Long> iter = backendIds.iterator();
        while (iter.hasNext()) {
            Backend backend = this.getBackend(iter.next());
            if (backend == null || !backend.isDecommissioned()) {
                iter.remove();
            }
        }
        return backendIds;
    }

    public List<Backend> getMixBackends() {
        return getAllClusterBackendsNoException().values()
                .stream().filter(backend -> backend.isMixNode()).collect(Collectors.toList());
    }

    public List<Backend> getCnBackends() {
        return getAllClusterBackendsNoException()
                .values().stream().filter(Backend::isComputeNode).collect(Collectors.toList());
    }

    // return num of backends that from different hosts
    public int getStorageBackendNumFromDiffHosts(boolean aliveOnly) {
        Set<String> hosts = Sets.newHashSet();
        ImmutableMap<Long, Backend> idToBackend = getAllClusterBackendsNoException();
        for (Backend backend : idToBackend.values()) {
            if ((aliveOnly && !backend.isAlive()) || backend.isComputeNode()) {
                continue;
            }
            hosts.add(backend.getHost());
        }
        return hosts.size();
    }
    // 用于对 Backend 列表进行排序。
    class BeIdComparator implements Comparator<Backend> {
        public int compare(Backend a, Backend b) {
            return Long.compare(a.getId(), b.getId());
        }
    }
    // 用于对 Backend 列表进行排序。
    class BeHostComparator implements Comparator<Backend> {
        public int compare(Backend a, Backend b) {
            return a.getHost().compareTo(b.getHost());
        }
    }

    // Select the smallest number of tablets as the starting position of
    // round robin in the BE that match the policy
    public int getStartPosOfRoundRobin(Tag tag, TStorageMedium storageMedium, boolean isStorageMediumSpecified) {
        BeSelectionPolicy.Builder builder = new BeSelectionPolicy.Builder()
                .needScheduleAvailable()
                .needCheckDiskUsage()
                .addTags(Sets.newHashSet(tag))
                .setStorageMedium(storageMedium);
        if (FeConstants.runningUnitTest || Config.allow_replica_on_same_host) {
            builder.allowOnSameHost();
        }

        BeSelectionPolicy policy = builder.build();
        List<Long> beIds = selectBackendIdsByPolicy(policy, -1);
        if (beIds.isEmpty() && storageMedium != null && !isStorageMediumSpecified) {
            storageMedium = (storageMedium == TStorageMedium.HDD) ? TStorageMedium.SSD : TStorageMedium.HDD;
            policy = builder.setStorageMedium(storageMedium).build();
            beIds = selectBackendIdsByPolicy(policy, -1);
        }

        long minBeTabletsNum = Long.MAX_VALUE;
        int minIndex = -1;
        for (int i = 0; i < beIds.size(); ++i) {
            long tabletsNum = Env.getCurrentInvertedIndex().getTabletSizeByBackendId(beIds.get(i));
            if (tabletsNum < minBeTabletsNum) {
                minBeTabletsNum = tabletsNum;
                minIndex = i;
            }
        }
        return minIndex;
    }

    /**
     * Select a set of backends for replica creation.
     * The following parameters need to be considered when selecting backends.
     *
     * @param replicaAlloc
     * @param nextIndexs create tablet round robin next be index, when enable_round_robin_create_tablet
     * @param storageMedium
     * @param isStorageMediumSpecified
     * @param isOnlyForCheck set true if only used for check available backend
     * @return return the selected backend ids group by tag.
     * @throws DdlException
     */
    // 极其核心的副本放置与后端节点选择方法。当系统需要为创建 Tablet（分片）选择一组最合适的 BE 节点时，就会调用该方法。
    // ReplicaAllocation replicaAlloc：副本分配策略对象。它描述了每个 Tag（如机房、可用区）下需要放置多少个副本（例如 tag.location.zone1: 2, tag.location.zone2: 1）。
    // Map<Tag, Integer> nextIndexs： 轮询创建 Tablet 时的下一个后端索引缓存（用于支持 enable_round_robin_create_tablet 特性），确保不同分片能够均匀分布在各个 BE 上。
    // TStorageMedium storageMedium： 期望的存储介质类型（如 SSD 或 HDD）。
    // boolean isStorageMediumSpecified： 标记用户是否在 SQL 中显式指定了存储介质。如果是显式指定的，当该介质没有可用节点时，系统不会自动尝试切换到另一种介质重试。
    // boolean isOnlyForCheck： 指示本次调用是否仅用于可用性检查（例如在 DDL 阶段预先校验是否有足够的可用节点）。如果是 true，则在找不到节点时不会触发跨存储介质的自动重试逻辑。

    public Pair<Map<Tag, List<Long>>, TStorageMedium> selectBackendIdsForReplicaCreation(
            ReplicaAllocation replicaAlloc, Map<Tag, Integer> nextIndexs,
            TStorageMedium storageMedium, boolean isStorageMediumSpecified,
            boolean isOnlyForCheck)
            throws DdlException {
        // 获取当前集群中所有的后端节点快照
        Map<Long, Backend> copiedBackends = Maps.newHashMap(getAllClusterBackendsNoException());
        // 创建用来存放最终选定结果的 chosenBackendIds 映射。
        Map<Tag, List<Long>> chosenBackendIds = Maps.newHashMap();
        // 提取副本分配策略中的 Tag 与副本数映射 allocMap。
        Map<Tag, Short> allocMap = replicaAlloc.getAllocMap();
        // 初始化已选中的总副本计数器 totalReplicaNum 为 0。
        short totalReplicaNum = 0;
        // 存活节点数量初步校验。
        int aliveBackendNum = (int) copiedBackends.values().stream().filter(Backend::isAlive).count();
        if (aliveBackendNum < replicaAlloc.getTotalReplicaNum()) {
            throw new DdlException("replication num should be less than the number of available backends. "
                    + "replication num is " + replicaAlloc.getTotalReplicaNum()
                    + ", available backend num is " + aliveBackendNum);
        } else {
            // 初始化一个存放失败条目的列表
            List<String> failedEntries = Lists.newArrayList();
            // 遍历每一个 Tag 及其对应的副本数要求
            for (Map.Entry<Tag, Short> entry : allocMap.entrySet()) {
                Tag tag = entry.getKey();
                // 遍历每一个 Tag 及其对应的副本数要求
                // 要求节点必须调度可用
                // 需要检查磁盘空间占用
                // 绑定当前的 Tag 并设置期望的 storageMedium
                BeSelectionPolicy.Builder builder = new BeSelectionPolicy.Builder()
                        .needScheduleAvailable().needCheckDiskUsage().addTags(Sets.newHashSet(entry.getKey()))
                        .setStorageMedium(storageMedium);
                // 如果正在运行单元测试或者开启了配置项 allow_replica_on_same_host（允许在同一台宿主机上放置多个副本），则调用 allowOnSameHost()。
                if (FeConstants.runningUnitTest || Config.allow_replica_on_same_host) {
                    builder.allowOnSameHost();
                }
                // 如果开启了轮询创建 Tablet（enable_round_robin_create_tablet），则为构建器启用轮询，并传入对应 Tag 上一次的轮询索引位置。
                if (Config.enable_round_robin_create_tablet) {
                    builder.setEnableRoundRobin(true);
                    builder.setNextRoundRobinIndex(nextIndexs.getOrDefault(tag, -1));
                }
                // 并调用 selectBackendIdsByPolicy 方法为当前 Tag 挑选指定数量 (entry.getValue()) 的后端节点 ID。
                BeSelectionPolicy policy = builder.build();
                List<Long> beIds = selectBackendIdsByPolicy(policy, entry.getValue());
                // first time empty, retry with different storage medium
                // if only for check, no need to retry different storage medium to get backend
                // 存储介质自动降级/升级重试机制：
                // 如果初次挑选节点结果为空（beIds.isEmpty()），
                // 且满足“指定了存储介质、用户没有强制显式指定该介质（!isStorageMediumSpecified）、且不是仅检查模式（!isOnlyForCheck）”的条件：
                TStorageMedium originalStorageMedium = storageMedium;
                if (beIds.isEmpty() && storageMedium != null && !isStorageMediumSpecified && !isOnlyForCheck) {
                    storageMedium = (storageMedium == TStorageMedium.HDD) ? TStorageMedium.SSD : TStorageMedium.HDD;
                    builder.setStorageMedium(storageMedium);
                    if (Config.enable_round_robin_create_tablet) {
                        builder.setNextRoundRobinIndex(nextIndexs.getOrDefault(tag, -1));
                    }
                    policy = builder.build();
                    beIds = selectBackendIdsByPolicy(policy, entry.getValue());
                }
                // 如果开启了轮询创建，将策略更新后的最新轮询索引保存回 nextIndexs 中，供下一次建表/建分片使用。
                if (Config.enable_round_robin_create_tablet) {
                    nextIndexs.put(tag, policy.nextRoundRobinIndex);
                }
                // after retry different storage medium, it's still empty
                // 如果 beIds 仍然为空，说明即使重试也无法满足该 Tag 的副本需求，记录错误日志并将失败信息加入 failedEntries。
                if (beIds.isEmpty()) {
                    LOG.error("failed backend(s) for policy: {} real medium {}", policy, originalStorageMedium);
                    String errorReplication = "replication tag: " + entry.getKey()
                            + ", replication num: " + entry.getValue()
                            + ", storage medium: " + originalStorageMedium;
                    failedEntries.add(errorReplication);
                } else {
                    // 如果成功选出了节点，则将该 Tag 与对应的节点 ID 列表放入 chosenBackendIds 中，并累加已选出的总副本数 totalReplicaNum。
                    chosenBackendIds.put(entry.getKey(), beIds);
                    totalReplicaNum += beIds.size();
                }
            }

            if (!failedEntries.isEmpty()) {
                String failedMsg = Joiner.on("\n").join(failedEntries);
                throw new DdlException("Failed to find enough backend, please check the replication num,"
                        + "replication tag and storage medium and avail capacity of backends "
                        + "or maybe all be on same host." + getDetailsForCreateReplica(replicaAlloc) + "\n"
                        + "Create failed replications:\n" + failedMsg);
            }
        }

        Preconditions.checkState(totalReplicaNum == replicaAlloc.getTotalReplicaNum());
        return Pair.of(chosenBackendIds, storageMedium);
    }

    public String getDetailsForCreateReplica(ReplicaAllocation replicaAlloc) {
        StringBuilder sb = new StringBuilder(" Backends details: ");
        for (Tag tag : replicaAlloc.getAllocMap().keySet()) {
            sb.append("backends with tag ").append(tag).append(" is ");
            sb.append(idToBackendRef.values().stream().filter(be -> be.getLocationTag().equals(tag))
                    .map(Backend::getDetailsForCreateReplica)
                    .collect(Collectors.toList()));
            sb.append(", ");
        }
        return sb.toString();
    }

    public List<Long> selectBackendIdsByPolicy(BeSelectionPolicy policy, int number) {
        return selectBackendIdsByPolicy(policy, number, getAllClusterBackendsNoException().values().asList());
    }

    /**
     * Select a set of backends by the given policy.
     *
     * @param policy if policy is enableRoundRobin, will update its nextRoundRobinIndex
     * @param number number of backends which need to be selected. -1 means return as many as possible.
     * @return return #number of backend ids,
     *         or empty set if no backends match the policy, or the number of matched backends is less than "number";
     */
    public List<Long> selectBackendIdsByPolicy(BeSelectionPolicy policy, int number,
            List<Backend> backendList) {
        Preconditions.checkArgument(number >= -1);

        List<Backend> candidates = policy.getCandidateBackends(backendList);

        if (candidates.size() < number || candidates.isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Not match policy: {}. candidates num: {}, expected: {}", policy, candidates.size(), number);
            }
            return Lists.newArrayList();
        }

        // If only need one Backend, just return a random one.
        if (number == 1 && !policy.enableRoundRobin) {
            Collections.shuffle(candidates);
            return Lists.newArrayList(candidates.get(0).getId());
        }

        boolean hasSameHost = false;
        if (!policy.allowOnSameHost) {
            // for each host, random select one backend.
            Map<String, List<Backend>> backendMaps = Maps.newHashMap();
            for (Backend backend : candidates) {
                if (backendMaps.containsKey(backend.getHost())) {
                    backendMaps.get(backend.getHost()).add(backend);
                } else {
                    List<Backend> list = Lists.newArrayList();
                    list.add(backend);
                    backendMaps.put(backend.getHost(), list);
                }
            }

            candidates.clear();
            for (List<Backend> list : backendMaps.values()) {
                if (list.size() > 1) {
                    Collections.shuffle(list);
                    hasSameHost = true;
                }
                candidates.add(list.get(0));
            }
        }

        if (candidates.size() < number) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Not match policy: {}. candidates num: {}, expected: {}", policy, candidates.size(), number);
            }
            return Lists.newArrayList();
        }

        if (policy.enableRoundRobin) {
            if (!policy.allowOnSameHost && hasSameHost) {
                // not allow same host and has same host,
                // then we compare them with their host
                Collections.sort(candidates, new BeHostComparator());
            } else {
                Collections.sort(candidates, new BeIdComparator());
            }

            if (policy.nextRoundRobinIndex < 0) {
                policy.nextRoundRobinIndex = new SecureRandom().nextInt(candidates.size());
            }

            int realIndex = policy.nextRoundRobinIndex % candidates.size();
            List<Long> partialOrderList = new ArrayList<Long>();
            partialOrderList.addAll(candidates.subList(realIndex, candidates.size())
                    .stream().map(Backend::getId).collect(Collectors.toList()));
            partialOrderList.addAll(candidates.subList(0, realIndex)
                    .stream().map(Backend::getId).collect(Collectors.toList()));

            List<Long> result = number == -1 ? partialOrderList : partialOrderList.subList(0, number);
            policy.nextRoundRobinIndex = realIndex + result.size();

            return result;
        } else {
            Collections.shuffle(candidates);
            if (number != -1) {
                return candidates.subList(0, number).stream().map(Backend::getId).collect(Collectors.toList());
            } else {
                return candidates.stream().map(Backend::getId).collect(Collectors.toList());
            }
        }
    }

    public long getBackendReportVersion(long backendId) {
        AtomicLong atomicLong = null;
        if ((atomicLong = idToReportVersionRef.get(backendId)) == null) {
            return -1L;
        } else {
            return atomicLong.get();
        }
    }

    public void updateBackendReportVersion(long backendId, long newReportVersion, long dbId, long tableId,
            boolean checkDbExist) {
        AtomicLong atomicLong = idToReportVersionRef.get(backendId);
        if (atomicLong == null) {
            return;
        }
        if (checkDbExist && Env.getCurrentInternalCatalog().getDbNullable(dbId) == null) {
            LOG.warn("failed to update backend report version, db {} does not exist", dbId);
            return;
        }
        while (true) {
            long curReportVersion = atomicLong.get();
            if (curReportVersion >= newReportVersion) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("skip update backend {} report version: {}, current version: {}, db: {}, table: {}",
                            backendId, newReportVersion, curReportVersion, dbId, tableId);
                }
                break;
            }
            if (atomicLong.compareAndSet(curReportVersion, newReportVersion)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("update backend {} report version: {}, db: {}, table: {}",
                            backendId, newReportVersion, dbId, tableId);
                }
                break;
            }
        }
    }

    public long saveBackends(CountingDataOutputStream dos, long checksum) throws IOException {
        ImmutableMap<Long, Backend> idToBackend = getAllClusterBackendsNoException();
        int backendCount = idToBackend.size();
        checksum ^= backendCount;
        dos.writeInt(backendCount);
        for (Map.Entry<Long, Backend> entry : idToBackend.entrySet()) {
            long key = entry.getKey();
            checksum ^= key;
            dos.writeLong(key);
            entry.getValue().write(dos);
        }
        return checksum;
    }

    public long loadBackends(DataInputStream dis, long checksum) throws IOException {
        int count = dis.readInt();
        checksum ^= count;
        for (int i = 0; i < count; i++) {
            long key = dis.readLong();
            checksum ^= key;
            Backend backend = Backend.read(dis);
            replayAddBackend(backend);
        }
        return checksum;
    }

    public void clear() {
        this.idToBackendRef = null;
        this.idToReportVersionRef = null;
    }

    public static HostInfo getHostAndPort(String hostPort)
            throws AnalysisException {
        hostPort = hostPort.replaceAll("\\s+", "");
        if (hostPort.isEmpty()) {
            throw new AnalysisException("Invalid host port: " + hostPort);
        }

        HostInfo hostInfo = NetUtils.resolveHostInfoFromHostPort(hostPort);

        String host = hostInfo.getHost();
        if (Strings.isNullOrEmpty(host)) {
            throw new AnalysisException("Host is null");
        }

        int heartbeatPort = -1;
        try {
            // validate port
            heartbeatPort = hostInfo.getPort();
            if (heartbeatPort <= 0 || heartbeatPort >= 65536) {
                throw new AnalysisException("Port is out of range: " + heartbeatPort);
            }

            return new HostInfo(host, heartbeatPort);
        } catch (Exception e) {
            throw new AnalysisException("Encounter unknown exception: " + e.getMessage());
        }
    }


    public static Pair<String, Integer> validateHostAndPort(String hostPort) throws AnalysisException {
        HostInfo hostInfo = getHostAndPort(hostPort);
        return Pair.of(hostInfo.getHost(), hostInfo.getPort());
    }

    public void replayAddBackend(Backend newBackend) {
        // update idToBackend
        Map<Long, Backend> copiedBackends = Maps.newHashMap(getAllClusterBackendsNoException());
        copiedBackends.put(newBackend.getId(), newBackend);
        ImmutableMap<Long, Backend> newIdToBackend = ImmutableMap.copyOf(copiedBackends);
        idToBackendRef = newIdToBackend;

        // set new backend's report version as 0L
        Map<Long, AtomicLong> copiedReportVersions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVersions.put(newBackend.getId(), new AtomicLong(0L));
        ImmutableMap<Long, AtomicLong> newIdToReportVersion = ImmutableMap.copyOf(copiedReportVersions);
        idToReportVersionRef = newIdToReportVersion;
    }

    public void replayDropBackend(Backend backend) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("replayDropBackend: {}", backend);
        }
        // update idToBackend
        Map<Long, Backend> copiedBackends = Maps.newHashMap(getAllClusterBackendsNoException());
        copiedBackends.remove(backend.getId());
        ImmutableMap<Long, Backend> newIdToBackend = ImmutableMap.copyOf(copiedBackends);
        idToBackendRef = newIdToBackend;

        // update idToReportVersion
        Map<Long, AtomicLong> copiedReportVersions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVersions.remove(backend.getId());
        ImmutableMap<Long, AtomicLong> newIdToReportVersion = ImmutableMap.copyOf(copiedReportVersions);
        idToReportVersionRef = newIdToReportVersion;

    }

    public void updateBackendState(Backend be) {
        long id = be.getId();
        Backend memoryBe = getBackend(id);
        // backend may already be dropped. this may happen when
        // drop and modify operations do not guarantee the order.
        if (memoryBe != null) {
            memoryBe.setHost(be.getHost());
            memoryBe.setBePort(be.getBePort());
            memoryBe.setAlive(be.isAlive());
            memoryBe.setDecommissioned(be.isDecommissioned());
            memoryBe.setHttpPort(be.getHttpPort());
            memoryBe.setBeRpcPort(be.getBeRpcPort());
            memoryBe.setBrpcPort(be.getBrpcPort());
            memoryBe.setArrowFlightSqlPort(be.getArrowFlightSqlPort());
            memoryBe.setLastUpdateMs(be.getLastUpdateMs());
            memoryBe.setLastStartTime(be.getLastStartTime());
            memoryBe.setDisks(be.getDisks());
            memoryBe.setCpuCores(be.getCputCores());
            memoryBe.setPipelineExecutorSize(be.getPipelineExecutorSize());
        }
    }

    private long getAvailableCapacityB() {
        long capacity = 0L;
        for (Backend backend : getAllClusterBackendsNoException().values()) {
            // Here we do not check if backend is alive,
            // We suppose the dead backends will back to alive later.
            if (backend.isDecommissioned()) {
                // Data on decommissioned backend will move to other backends,
                // So we need to minus size of those data.
                capacity -= backend.getDataUsedCapacityB();
            } else {
                capacity += backend.getAvailableCapacityB();
            }
        }
        return capacity;
    }

    public void checkAvailableCapacity() throws DdlException {
        if (getAvailableCapacityB() <= 0L) {
            throw new DdlException("System has no available disk capacity or no available BE nodes");
        }
    }

    public ImmutableMap<Long, Backend> getAllClusterBackendsNoException() {
        try {
            return getAllBackendsByAllCluster();
        } catch (AnalysisException e) {
            LOG.warn("getAllClusterBackendsNoException: ", e);
            return ImmutableMap.of();
        }
    }

    /*
     * Try to randomly get a backend id by given host.
     * If not found, return -1
     */
    public long getBackendIdByHost(String host) {
        ImmutableMap<Long, Backend> idToBackend = getAllClusterBackendsNoException();
        List<Backend> selectedBackends = Lists.newArrayList();
        for (Backend backend : idToBackend.values()) {
            if (backend.getHost().equals(host)) {
                selectedBackends.add(backend);
            }
        }

        if (selectedBackends.isEmpty()) {
            return -1L;
        }

        Collections.shuffle(selectedBackends);
        return selectedBackends.get(0).getId();
    }

    /*
     * Check if the specified disks' capacity has reached the limit.
     * bePathsMap is (BE id -> list of path hash)
     * If floodStage is true, it will check with the floodStage threshold.
     *
     * return Status.OK if not reach the limit
     */
    public Status checkExceedDiskCapacityLimit(Multimap<Long, Long> bePathsMap, boolean floodStage) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("pathBeMap: {}", bePathsMap);
        }
        ImmutableMap<Long, DiskInfo> pathHashToDiskInfo = pathHashToDiskInfoRef;
        for (Long beId : bePathsMap.keySet()) {
            for (Long pathHash : bePathsMap.get(beId)) {
                DiskInfo diskInfo = pathHashToDiskInfo.get(pathHash);
                if (diskInfo != null && diskInfo.exceedLimit(floodStage)) {
                    return new Status(TStatusCode.CANCELLED,
                            "disk " + diskInfo.getRootPath() + " on backend "
                                    + beId + " exceed limit usage, path hash: " + pathHash);
                }
            }
        }
        return Status.OK;
    }

    // update the path info when disk report
    // there is only one thread can update path info, so no need to worry about concurrency control
    public void updatePathInfo(List<DiskInfo> addedDisks, List<DiskInfo> removedDisks) {
        Map<Long, DiskInfo> copiedPathInfos = Maps.newHashMap(pathHashToDiskInfoRef);
        for (DiskInfo diskInfo : addedDisks) {
            copiedPathInfos.put(diskInfo.getPathHash(), diskInfo);
        }
        for (DiskInfo diskInfo : removedDisks) {
            copiedPathInfos.remove(diskInfo.getPathHash());
        }
        ImmutableMap<Long, DiskInfo> newPathInfos = ImmutableMap.copyOf(copiedPathInfos);
        pathHashToDiskInfoRef = newPathInfos;
        if (LOG.isDebugEnabled()) {
            LOG.debug("update path infos: {}", newPathInfos);
        }
    }

    public void modifyBackendHostName(String srcHost, int srcPort, String destHost) throws UserException {
        Backend be = getBackendWithHeartbeatPort(srcHost, srcPort);
        if (be == null) {
            throw new DdlException("backend does not exists[" + NetUtils
                    .getHostPortInAccessibleFormat(srcHost, srcPort) + "]");
        }
        if (be.getHost().equals(destHost)) {
            // no need to modify
            return;
        }
        be.setHost(destHost);
        Env.getCurrentEnv().getEditLog().logModifyBackend(be);
    }

    public void modifyBackendHost(ModifyBackendHostNameOp op) throws UserException {
        Backend be = getBackendWithHeartbeatPort(op.getHost(), op.getPort());
        if (be == null) {
            throw new DdlException("backend does not exists[" + NetUtils
                .getHostPortInAccessibleFormat(op.getHost(), op.getPort()) + "]");
        }
        if (be.getHost().equals(op.getNewHost())) {
            // no need to modify
            return;
        }
        be.setHost(op.getNewHost());
        Env.getCurrentEnv().getEditLog().logModifyBackend(be);
    }

    public void modifyBackends(ModifyBackendOp op) throws UserException {
        List<HostInfo> hostInfos = op.getHostInfos();
        List<Backend> backends = Lists.newArrayList();
        if (hostInfos.isEmpty()) {
            List<String> ids = op.getIds();
            for (String id : ids) {
                long backendId = Long.parseLong(id);
                Backend be = getBackend(backendId);
                if (be == null) {
                    throw new DdlException("backend does not exists[" + backendId + "]");
                }
                backends.add(be);
            }
        } else {
            for (HostInfo hostInfo : hostInfos) {
                Backend be = getBackendWithHeartbeatPort(hostInfo.getHost(), hostInfo.getPort());
                if (be == null) {
                    throw new DdlException(
                          "backend does not exists[" + NetUtils
                                  .getHostPortInAccessibleFormat(hostInfo.getHost(), hostInfo.getPort()) + "]");
                }
                backends.add(be);
            }
        }

        for (Backend be : backends) {
            boolean shouldModify = false;
            Map<String, String> tagMap = op.getTagMap();
            if (!tagMap.isEmpty()) {
                be.setTagMap(tagMap);
                shouldModify = true;
            }

            if (op.isQueryDisabled() != null) {
                shouldModify = be.setQueryDisabled(op.isQueryDisabled());
            }

            if (op.isLoadDisabled() != null) {
                shouldModify = be.setLoadDisabled(op.isLoadDisabled());
            }

            if (shouldModify) {
                Env.getCurrentEnv().getEditLog().logModifyBackend(be);
                LOG.info("finished to modify backend {} ", be);
            }
        }
    }

    public void replayModifyBackend(Backend backend) {
        Backend memBe = getBackend(backend.getId());
        memBe.setTagMap(backend.getTagMap());
        memBe.setQueryDisabled(backend.isQueryDisabled());
        memBe.setLoadDisabled(backend.isLoadDisabled());
        memBe.setHost(backend.getHost());
        if (LOG.isDebugEnabled()) {
            LOG.debug("replay modify backend: {}", backend);
        }
    }

    // Check if there is enough suitable BE for replica allocation
    public void checkReplicaAllocation(ReplicaAllocation replicaAlloc) throws DdlException {
        List<Backend> backends = getMixBackends();
        for (Map.Entry<Tag, Short> entry : replicaAlloc.getAllocMap().entrySet()) {
            if (backends.stream().filter(b -> b.getLocationTag().equals(entry.getKey()))
                    .count() < entry.getValue()) {
                throw new DdlException(
                        "Failed to find enough host with tag(" + entry.getKey() + ") in all backends. need: "
                                + entry.getValue());
            }
        }
    }

    public Set<Tag> getTags() {
        List<Backend> bes = getMixBackends();
        Set<Tag> tags = Sets.newHashSet();
        for (Backend be : bes) {
            tags.add(be.getLocationTag());
        }
        return tags;
    }

    public List<Backend> getBackendsByTag(Tag tag) {
        List<Backend> bes = getMixBackends();
        return bes.stream().filter(b -> b.getLocationTag().equals(tag)).collect(Collectors.toList());
    }

    // CloudSystemInfoService override
    public ImmutableMap<Long, Backend> getBackendsByCurrentCluster() throws AnalysisException {
        return idToBackendRef;
    }

    public ImmutableList<Backend> getBackendListByComputeGroup(Set<String> cgSet) {
        List<Backend> result = new ArrayList<>();
        for (Backend be : idToBackendRef.values()) {
            if (cgSet.contains(be.getLocationTag().value)) {
                result.add(be);
            }
        }
        return ImmutableList.copyOf(result);
    }

    // Cloud and NonCloud get all bes
    public ImmutableMap<Long, Backend> getAllBackendsByAllCluster() throws AnalysisException {
        return idToBackendRef;
    }

    // CloudSystemInfoService override.
    // Non-cloud ignores clusterName and calculates from all backends.
    public int getMinPipelineExecutorSize(String clusterName) {
        List<Backend> currentBackends = null;
        try {
            currentBackends = getAllBackendsByAllCluster().values().asList();
        } catch (UserException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("get current cluster backends failed: ", e);
            }
            return 1;
        }
        if (currentBackends.size() == 0) {
            return 1;
        }

        return currentBackends.stream()
                .mapToInt(Backend::getPipelineExecutorSize)
                .filter(size -> size > 0)
                .min()
                .orElse(1);
    }

    // CloudSystemInfoService override
    public int getTabletNumByBackendId(long beId) {
        return Env.getCurrentInvertedIndex().getTabletNumByBackendId(beId);
    }

    // If the error msg contains certain keywords, we need to retry the query with re-plan.
    public static boolean needRetryWithReplan(String errorMsg) {
        if (Strings.isNullOrEmpty(errorMsg)) {
            return false;
        }
        for (String keyword : NEED_REPLAN_ERRORS) {
            LOG.debug("key {}, errorMsg {}", keyword, errorMsg);
            if (errorMsg.contains(keyword)) {
                return true;
            }
        }
        return false;
    }
}
