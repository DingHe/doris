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

#pragma once

#include <stdint.h>

#include <string>

#include "common/status.h"
#include "exec/operator/operator.h"
#include "exec/operator/scan_operator.h"

namespace doris {

class JdbcScanner;
} // namespace doris

namespace doris {

class JDBCScanOperatorX;
// JDBCScanLocalState 是 JDBC 向量化外表扫描算子（JDBCScanOperatorX）在运行时线程级别的本地状态管理类（Local State）。
// 管理 JDBC 扫描的运行时线程状态：继承自 ScanLocalState 模板基类，专门为 JDBC 外表扫描任务提供生命周期管理与线程级别的资源隔离。
// 建与初始化 JdbcScanner：负责将上层 FE 下发的 JDBC 查询任务（如 JDBC 过滤条件、SQL 语句等）实例化为具体的 JdbcScanner 对象，并交付给调度器执行。
// 建立与 JdbcScanner 的友好协同：作为 JdbcScanner 的友元类，为底层 JDBC 数据读取器提供必要的上下文信息与状态访问能力。
class JDBCScanLocalState final : public ScanLocalState<JDBCScanLocalState> {
public:
    using Parent = JDBCScanOperatorX;
    ENABLE_FACTORY_CREATOR(JDBCScanLocalState);
    JDBCScanLocalState(RuntimeState* state, OperatorXBase* parent)
            : ScanLocalState<JDBCScanLocalState>(state, parent) {}
    // 最核心的方法
    // 用于实例化具体的数据读取器 JdbcScanner。
    Status _init_scanners(std::list<ScannerSPtr>* scanners) override;

    std::string name_suffix() const override;

private:
    friend class JdbcScanner;
};

class JDBCScanOperatorX final : public ScanOperatorX<JDBCScanLocalState> {
public:
    JDBCScanOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                      const DescriptorTbl& descs, int parallel_tasks);

private:
    friend class JDBCScanLocalState;
    std::string _table_name;
    TupleId _tuple_id;
    std::string _query_string;
    TOdbcTableType::type _table_type;
    bool _is_tvf;
};

} // namespace doris
