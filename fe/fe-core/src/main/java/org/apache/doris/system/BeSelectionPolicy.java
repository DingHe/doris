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

import org.apache.doris.common.Config;
import org.apache.doris.qe.SimpleScheduler;
import org.apache.doris.resource.Tag;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Selection policy for building BE nodes
 */
// 核心的后端节点选择策略类
// 采用经典的 Builder 模式（建造者模式），用于封装为 Tablet 创建、查询、导入等操作挑选 BE 节点时的各种过滤和偏好规则。
// 统一封装节点选择条件：在分布式系统中，挑选节点需要考虑多维度的限制（如节点是否存活、是否被下线、磁盘空间是否超限、节点所属的 Tag 标签、存储介质类型、是否允许在同一台宿主机上分布多个副本等）。BeSelectionPolicy 将这些复杂的过滤条件聚合成一个策略对象。
// 过滤与候选集生成：提供 isMatch() 和 getCandidateBackends() 方法，对集群中所有的 Backend 节点进行精准筛选与洗牌，选出满足特定业务场景的最优候选节点列表。
public class BeSelectionPolicy {
    private static final Logger LOG = LogManager.getLogger(BeSelectionPolicy.class);
    // 要求节点必须是调度可用的
    public boolean needScheduleAvailable = false;
    // 要求节点必须是查询可用的
    public boolean needQueryAvailable = false;
    // 要求节点必须是导入可用的
    public boolean needLoadAvailable = false;
    // 要求节点不能处于下线或下线迁移状态
    public boolean needNonDecommissioned = false;
    // Resource tag. Empty means no need to consider resource tag.
    // 资源标签集合。如果非空，则只有标签匹配的节点才能被选中（为空时表示不限制 Tag）。
    public Set<Tag> resourceTags = Sets.newHashSet();
    // storage medium. null means no need to consider storage medium.
    // 期望的存储介质类型（如 SSD 或 HDD）。为 null 时表示不限制介质。
    public TStorageMedium storageMedium = null;
    // Check if disk usage reaches limit. false means no need to check.
    // 期望的存储介质类型（如 SSD 或 HDD）。为 null 时表示不限制介质。
    public boolean checkDiskUsage = false;
    // If set to false, do not select backends on same host.
    // 是否允许在同一台宿主机（Host）上选择多个后端节点。
    public boolean allowOnSameHost = false;
    // 是否优先选择纯计算节点（Compute Node / CN）。
    public boolean preferComputeNode = false;
    // 期望选出的后端节点数量。
    public int expectBeNum = 0;
    // 是否启用轮询（Round-Robin）方式选择节点。
    public boolean enableRoundRobin = false;
    // if enable round robin, choose next be from nextRoundRobinIndex
    // call SystemInfoService::selectBackendIdsByPolicy will update nextRoundRobinIndex
    // 启用轮询时的下一个起始索引位置。
    public int nextRoundRobinIndex = -1;
    // 优先考虑的主机位置列表。若匹配到会优先采用。
    public List<String> preferredLocations = new ArrayList<>();
    // 要求节点必须处于存活状态
    public boolean requireAliveBe = false;

    private BeSelectionPolicy() {

    }

    public static class Builder {
        private BeSelectionPolicy policy;

        public Builder() {
            policy = new BeSelectionPolicy();
        }

        public Builder needScheduleAvailable() {
            policy.needScheduleAvailable = true;
            return this;
        }

        public Builder needQueryAvailable() {
            policy.needQueryAvailable = true;
            return this;
        }

        public Builder needLoadAvailable() {
            policy.needLoadAvailable = true;
            policy.needNonDecommissioned = true;
            return this;
        }

        public Builder needNonDecommissioned() {
            policy.needNonDecommissioned = true;
            return this;
        }

        public Builder addTags(Set<Tag> tags) {
            policy.resourceTags.addAll(tags);
            return this;
        }

        public Builder setStorageMedium(TStorageMedium medium) {
            policy.storageMedium = medium;
            return this;
        }

        public Builder needCheckDiskUsage() {
            policy.checkDiskUsage = true;
            return this;
        }

        public Builder allowOnSameHost() {
            policy.allowOnSameHost = true;
            return this;
        }

        public Builder preferComputeNode(boolean prefer) {
            policy.preferComputeNode = prefer;
            return this;
        }

        public Builder assignExpectBeNum(int expectBeNum) {
            policy.expectBeNum = expectBeNum;
            return this;
        }

        public Builder addPreLocations(List<String> preferredLocations) {
            policy.preferredLocations.addAll(preferredLocations);
            return this;
        }

        public Builder setEnableRoundRobin(boolean enableRoundRobin) {
            policy.enableRoundRobin = enableRoundRobin;
            return this;
        }

        public Builder setNextRoundRobinIndex(int nextRoundRobinIndex) {
            policy.nextRoundRobinIndex = nextRoundRobinIndex;
            return this;
        }

        public Builder setRequireAliveBe() {
            policy.requireAliveBe = true;
            return this;
        }

        public BeSelectionPolicy build() {
            return policy;
        }
    }
    // 判断单个 Backend 节点是否满足当前策略的所有过滤规则。
    private boolean isMatch(Backend backend) {
        // Compute node is only used when preferComputeNode is set.
        if (!preferComputeNode && backend.isComputeNode()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Backend [{}] is not match by ComputeNode rule, policy: [{}]", backend.getHost(), this);
            }
            return false;
        }
        // 校验各项可用性开关（如 needScheduleAvailable、needQueryAvailable、needLoadAvailable、needNonDecommissioned、resourceTags、storageMedium、requireAliveBe），若不满足则返回 false。
        if (needScheduleAvailable && !backend.isScheduleAvailable()
                || needQueryAvailable && !backend.isQueryAvailable()
                || needLoadAvailable && !backend.isLoadAvailable()
                || needNonDecommissioned && (backend.isDecommissioned() || backend.isDecommissioning())
                || (!resourceTags.isEmpty() && !resourceTags.contains(backend.getLocationTag()))
                || storageMedium != null && !backend.hasSpecifiedStorageMedium(storageMedium)
                || (requireAliveBe && !backend.isAlive())) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Backend [{}] is not match by Other rules, policy: [{}]", backend.getHost(), this);
            }
            return false;
        }

        if (checkDiskUsage) {
            if (storageMedium == null && backend.diskExceedLimit()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Backend [{}] is not match by diskExceedLimit rule, policy: [{}]", backend.getHost(),
                            this);
                }
                return false;
            }
            if (storageMedium != null && backend.diskExceedLimitByStorageMedium(storageMedium)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Backend [{}] is not match by diskExceedLimitByStorageMedium rule, policy: [{}]",
                            backend.getHost(), this);
                }
                return false;
            }
        }
        return true;
    }
    // 核心候选后端过滤与挑选方法。
    public List<Backend> getCandidateBackends(Collection<Backend> backends) {
        List<Backend> filterBackends = backends.stream().filter(this::isMatch)
                .collect(Collectors.toList());
        List<Backend> preLocationFilterBackends = filterBackends.stream()
                .filter(iterm -> preferredLocations.contains(iterm.getHost())).collect(Collectors.toList());
        // If preLocations were chosen, use the preLocation backends. Otherwise we just ignore this filter.
        if (!preLocationFilterBackends.isEmpty()) {
            filterBackends = preLocationFilterBackends;
        }
        Collections.shuffle(filterBackends);
        int numComputeNode = filterBackends.stream().filter(Backend::isComputeNode).collect(Collectors.toList()).size();
        List<Backend> candidates = new ArrayList<>();
        if (preferComputeNode && numComputeNode > 0) {
            int realExpectBeNum = expectBeNum == -1 ? numComputeNode : expectBeNum;
            int num = 0;
            // pick compute node first
            for (Backend backend : filterBackends) {
                if (backend.isComputeNode()) {
                    candidates.add(backend);
                    num++;
                }
            }
            // fill with some mix node.
            if (num < realExpectBeNum) {
                for (Backend backend : filterBackends) {
                    if (backend.isMixNode()) {
                        if (num >= realExpectBeNum) {
                            break;
                        }
                        candidates.add(backend);
                        num++;
                    }
                }
            }
        } else {
            candidates.addAll(filterBackends);
        }
        // filter out backends in black list
        if (!Config.disable_backend_black_list) {
            candidates = candidates.stream().filter(b -> SimpleScheduler.isAvailable(b)).collect(Collectors.toList());
        }
        Collections.shuffle(candidates);
        return candidates;
    }

    @Override
    public String toString() {
        return String.format("computeNode=%s | query=%s | load=%s | schedule=%s | nonDecommissioned=%s"
                        + " | tags=%s | medium=%s",
                preferComputeNode, needQueryAvailable, needLoadAvailable, needScheduleAvailable,
                needNonDecommissioned,
                resourceTags.stream().map(tag -> tag.toString()).collect(Collectors.joining(",")), storageMedium);
    }
}
