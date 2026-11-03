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

package org.apache.doris.transaction;

import org.apache.doris.thrift.TTabletCommitInfo;

import com.google.common.collect.Lists;
import com.google.gson.Gson;

import java.util.List;
// 副本写入提交信息封装类
// 当后端（BE）节点完成数据的本地写入、准备提交事务时，BE 会向前端（FE）汇报哪些数据分片（Tablet）在哪些节点上写入成功。
// TabletCommitInfo 就是专门用于在 FE 端接收和解析这种“Tablet 与 Backend 对应成功关系”的数据载体。
// BE 与 FE 之间的提交桥梁：封装了底层 RPC（Thrift 协议）传输上来的副本写入成功汇报信息。
// Quorum 校验的数据输入源：在事务的预提交（Pre-Commit）和提交（Commit）阶段，DatabaseTransactionMgr 会收集这些 TabletCommitInfo 列表，以此来核对每个 Tablet 成功写入的副本数量是否满足多数派（Quorum）要求。

public class TabletCommitInfo {
    // 数据分片（Tablet）的唯一标识 ID。用于标识哪一个具体的分片成功完成了数据写入。
    private long tabletId;
    // 后端节点（Backend, BE）的唯一标识 ID。用于标识该分片的副本是在哪一台 BE 机器上成功完成写入的。
    private long backendId;

    public TabletCommitInfo(long tabletId, long backendId) {
        super();
        this.tabletId = tabletId;
        this.backendId = backendId;
    }

    public long getTabletId() {
        return tabletId;
    }

    public long getBackendId() {
        return backendId;
    }

    public static List<TabletCommitInfo> fromThrift(List<TTabletCommitInfo> tTabletCommitInfos) {
        List<TabletCommitInfo> commitInfos = Lists.newArrayList();
        for (TTabletCommitInfo tTabletCommitInfo : tTabletCommitInfos) {
            commitInfos.add(new TabletCommitInfo(tTabletCommitInfo.getTabletId(), tTabletCommitInfo.getBackendId()));
        }
        return commitInfos;
    }

    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}
