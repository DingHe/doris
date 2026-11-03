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

package org.apache.doris.qe;

import org.apache.doris.common.Status;
import org.apache.doris.thrift.TNetworkAddress;

import java.util.List;

// 查询协调器（Coordinator）的核心顶层抽象接口
// CoordInterface 定义了 Doris 中所有类型查询、任务或导入作业生命周期管理的统一契约（Interface）。
// 无论是标准的 SQL 查询、内部 SQL、还是像我们前面分析过的 Broker Load 导入作业，其底层驱动执行的协调器（Coordinator）都可以实现该接口。
// 这使得上层调度逻辑（如 QeProcessorImpl）能够以完全多态、统一的方式去控制不同任务的启动、拉取数据、取消、关闭和监控。
//
public interface CoordInterface {
    // 启动并执行查询或任务。
    // 当调用该方法时，协调器会正式将之前规划好的分布式执行计划（Plan Fragments）下发给参与计算的各个后端（BE）节点，触发底层算子或导入任务开始运行。
    public void exec() throws Exception;
    // 获取下一批查询结果数据。
    // 返回一个 RowBatch 对象，代表一小批次计算好的行数据。通常在上层客户端（如 MySQL Protocol Handler）通过迭代方式循环调用此方法，以流式、分批的方式将查询结果拉取并返回给用户。
    public RowBatch getNext() throws Exception;
    // 取消当前正在执行的查询或任务。
    public void cancel(Status cancelReason);

    // When call exec or get next data finished, should call this method to release
    // some resource.
    // 释放关联资源的默认接口方法（Default Method）
    public default void close() {}
    // 获取参与当前查询或任务的所有后端（BE）节点的网络地址列表。
    List<TNetworkAddress> getInvolvedBackends();
    // 设置当前语句的 Profile 是否安全。
    void setIsProfileSafeStmt(boolean isSafe);
}
