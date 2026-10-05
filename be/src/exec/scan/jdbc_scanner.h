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

#include <gen_cpp/Types_types.h>
#include <stdint.h>

#include <map>
#include <memory>
#include <string>

#include "common/factory_creator.h"
#include "common/global_types.h"
#include "common/status.h"
#include "exec/operator/jdbc_scan_operator.h"
#include "exec/scan/scanner.h"
#include "format/table/jdbc_jni_reader.h"
#include "runtime/runtime_profile.h"

namespace doris {
class RuntimeState;
class TupleDescriptor;

class Block;
class VExprContext;

/**
 * DEPRECATED: This class is transitional and should be removed once JDBC scanning
 * is fully integrated into the FileScanner path.
 *
 * JdbcScanner is the pipeline-level scanner for JDBC data sources.
 * It delegates to JdbcJniReader internally, which uses the unified
 * JniReader → JdbcJniScanner (Java) path for data reading.
 *
 * Prerequisites before deletion:
 * 1. FE: Change JdbcScanNode to generate FileScanNode plan with TFileFormatType::FORMAT_JDBC,
 *    so JDBC scans flow through FileScanner instead of JDBCScanLocalState → JdbcScanner.
 * 2. BE: Add FORMAT_JDBC case in FileScanner::_create_reader() to create JdbcJniReader
 *    (similar to Paimon/Hudi/MaxCompute/TrinoConnector).
 * 3. BE: Remove JDBCScanLocalState / jdbc_scan_operator.h/cpp which depend on this class.
 * 4. After the above, this file (jdbc_scanner.h/cpp) can be safely deleted.
 */
// JdbcScanner 是 JDBC 向量化外表数据扫描器。它继承自统一的 Scanner 抽象基类，是 Doris 执行引擎在 PipelineX 调度框架下进行外部关系型数据库（如 MySQL、PostgreSQL、Oracle、SQLServer 等）或者 TVF（Table-Valued Function，表值函数）查询的数据提取执行者。
// 由于跨语言调用的原因，BE（C++ 实现）无法直接通过 C++ 的原生驱动与所有外部数据库通信，因此 Doris 在 Java 侧封装了 JDBC 驱动逻辑，并利用 JNI（Java Native Interface）进行交互。
// JdbcScanner 的核心作用如下：
// Bridge 桥接作用（C++ 与 Java/JNI 交互）：它充当了 Doris C++ 执行引擎与 Java 侧 JDBC 驱动之间的桥梁。内部通过持有 JdbcJniReader 实例，将下发的 SQL 语句以及 JDBC 连接配置通过 JNI 传递给 Java 侧的 JDBC Scanner。
// 下推过滤与 SQL 查询配置：接收上层生成的带有 Push-down 谓词的外部 SQL（_query_string），配置 Tuple/Slot 描述符，通知 Java 侧建立 JDBC 连接并执行查询。
// 向量化数据拉取（Vectorized Fetching）：在 _get_block_impl 中，驱动 JNI Reader 将 Java 侧 JDBC ResultSet 读取到的数据转换为 Doris 内存格式的向量化 Block，批量推送给 Pipeline 执行管道。
// 指标收集与生命周期管理：搜集 JNI 交互中的读取耗时、行数指标并合并入 RuntimeProfile，并负责释放 JNI 句柄及相关 C++/Java 资源。
class JdbcScanner : public Scanner {
    ENABLE_FACTORY_CREATOR(JdbcScanner);

public:
    friend class JdbcJniReader;

    JdbcScanner(RuntimeState* state, doris::JDBCScanLocalState* parent, int64_t limit,
                const TupleId& tuple_id, const std::string& query_string,
                TOdbcTableType::type table_type, bool is_tvf, RuntimeProfile* profile);
    Status _open_impl(RuntimeState* state) override;
    Status close(RuntimeState* state) override;

    Status init(RuntimeState* state, const VExprContextSPtrs& conjuncts) override;

protected:
    Status _get_block_impl(RuntimeState* state, Block* block, bool* eos) override;
    void _collect_profile_before_close() override;

private:
    // Build JDBC params from TupleDescriptor for JdbcJniReader
    std::map<std::string, std::string> _build_jdbc_params(const TupleDescriptor* tuple_desc);

    // Convert TOdbcTableType enum to string for JdbcTypeHandlerFactory
    static std::string _odbc_table_type_to_string(TOdbcTableType::type type) {
        switch (type) {
        case TOdbcTableType::MYSQL:
            return "MYSQL";
        case TOdbcTableType::ORACLE:
            return "ORACLE";
        case TOdbcTableType::POSTGRESQL:
            return "POSTGRESQL";
        case TOdbcTableType::SQLSERVER:
            return "SQLSERVER";
        case TOdbcTableType::CLICKHOUSE:
            return "CLICKHOUSE";
        case TOdbcTableType::SAP_HANA:
            return "SAP_HANA";
        case TOdbcTableType::TRINO:
            return "TRINO";
        case TOdbcTableType::PRESTO:
            return "PRESTO";
        case TOdbcTableType::OCEANBASE:
            return "OCEANBASE";
        case TOdbcTableType::OCEANBASE_ORACLE:
            return "OCEANBASE_ORACLE";
        case TOdbcTableType::DB2:
            return "DB2";
        case TOdbcTableType::GBASE:
            return "GBASE";
        default:
            return "MYSQL";
        }
    }
    // 标识当前 JDBC 外表扫描是否已经到达末尾（End of Stream / End of Scan）
    bool _jdbc_eos;

    // Tuple id resolved in prepare() to set _tuple_desc;
    // 当前 JDBC 扫描所针对的 Tuple 标识符（ID）。
    TupleId _tuple_id;
    // SQL
    // 下发给外部 JDBC 数据库执行的最终 SQL 查询语句（字符串）。
    // 包含了上层 FE 已经拼接并推导好的列选择、表名以及下推的 WHERE 条件、LIMIT 等（如 SELECT col1, col2FROMdb.tblWHEREcol1 > 10）。
    std::string _query_string;
    // Descriptor of tuples read from JDBC table.
    const TupleDescriptor* _tuple_desc = nullptr;
    // the sql query database type: like mysql, PG..
    // 记录外部 JDBC 数据库的具体方言/类型枚举（由 Thrift 定义的 TOdbcTableType）。
    TOdbcTableType::type _table_type;
    // 标识本次 JDBC 扫描是否来自于 TVF（Table-Valued Function，表值函数，如 jdbc() 表函数）。
    bool _is_tvf;
    // Unified JNI reader
    // 指向 JdbcJniReader 实例的独占智能指针。
    // JdbcJniReader 是真正封装 JNI 方法调用的 C++ 类，负责通过 JNI 接口创建 Java 层的 JdbcScanner 对象、触发 get_next 提取数据并充填到 Doris 向量化 Block 中。
    std::unique_ptr<JdbcJniReader> _jni_reader;
};
} // namespace doris
