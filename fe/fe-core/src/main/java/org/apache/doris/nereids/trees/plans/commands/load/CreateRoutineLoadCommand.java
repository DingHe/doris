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

package org.apache.doris.nereids.trees.plans.commands.load;

import org.apache.doris.catalog.Env;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.ForwardWithSync;
import org.apache.doris.nereids.trees.plans.commands.info.CreateRoutineLoadInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.Objects;

/**
 Create routine Load statement,  continually load data from a streaming app

 syntax:
      CREATE ROUTINE LOAD [database.]name on table
      [load properties]
      [PROPERTIES
      (
          desired_concurrent_number = xxx,
          max_error_number = xxx,
          k1 = v1,
          ...
          kn = vn
      )]
      FROM type of routine load
      [(
          k1 = v1,
          ...
          kn = vn
      )]

      load properties:
          load property [[,] load property] ...

      load property:
          column separator | columns_mapping | partitions | where

      column separator:
          COLUMNS TERMINATED BY xxx
      columns_mapping:
          COLUMNS (c1, c2, c3 = c1 + c2)
      partitions:
          PARTITIONS (p1, p2, p3)
      where:
          WHERE c1 > 1

      type of routine load:
          KAFKA
*/
// CreateRoutineLoadCommand 主要用于执行创建例行导入作业（Routine Load）的 SQL 命令（即 CREATE ROUTINE LOAD ... 语句）。
// Routine Load 是 Apache Doris 提供的从流式数据源（如 Kafka）持续、实时导入数据到 Doris 表中的机制。
public class CreateRoutineLoadCommand extends Command implements ForwardWithSync {
    // 存储创建 Routine Load 作业所需的所有解析元数据信息（如目标数据库、目标表名、列映射关系、WHERE 过滤条件、Kafka Broker 地址、Topic 以及消费 Offset 等配置）。
    CreateRoutineLoadInfo createRoutineLoadInfo;
    // 调用父类 Command 的构造方法，传入 PlanType.CREATE_ROUTINE_LOAD_COMMAND 标识当前计划节点的类型。
    public CreateRoutineLoadCommand(CreateRoutineLoadInfo createRoutineLoadInfo) {
        super(PlanType.CREATE_ROUTINE_LOAD_COMMAND);
        this.createRoutineLoadInfo = Objects.requireNonNull(createRoutineLoadInfo, "require CreateTableInfo object");
    }
    // 执行命令的核心入口方法
    // 负责将 Routine Load 作业提交到 Doris 系统中。
    // ctx (ConnectContext)：当前客户端连接的上下文信息（如当前连接的用户、数据库、会话变量等）。
    // executor (StmtExecutor)：语句执行器，负责执行 SQL 语句并处理结果。
    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // 对传入的导入参数进行合法性与权限校验（如判断数据库/表是否存在、列映射是否合法、Kafka 配置是否正确等）。
        createRoutineLoadInfo.validate(ctx);
        Env.getCurrentEnv().getRoutineLoadManager().createRoutineLoadJob(this.createRoutineLoadInfo, ctx);
    }

    /**
     * getCreateRoutineLoadInfo
     *
     * @return createRoutineLoadInfo
     */
    public CreateRoutineLoadInfo getCreateRoutineLoadInfo() {
        return createRoutineLoadInfo;
    }
    // 实现 Visitor 设计模式，用于在对查询/命令计划树（Plan Tree）进行遍历或转换时处理该 Command 节点。
    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCreateRoutineLoadCommand(this, context);
    }
}
