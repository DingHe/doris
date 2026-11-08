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

import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.DateLiteralUtils;
import org.apache.doris.analysis.DecimalLiteralUtils;
import org.apache.doris.analysis.ExplainOptions;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.IPv4Literal;
import org.apache.doris.analysis.IPv6Literal;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.JsonLiteral;
import org.apache.doris.analysis.LargeIntLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.qe.ComputeGroupException;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ConnectionException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.NotImplementedException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.SqlUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.DelegatedCredential;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.mysql.MysqlPacket;
import org.apache.doris.mysql.MysqlResultSetEndPacket;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.mysql.MysqlServerStatusFlag;
import org.apache.doris.nereids.SqlCacheContext;
import org.apache.doris.nereids.SqlCacheContext.CacheKeyType;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.NotSupportedException;
import org.apache.doris.nereids.exceptions.SyntaxParseException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.minidump.MinidumpUtils;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.parser.SqlDialectHelper;
import org.apache.doris.nereids.stats.StatsErrorEstimator;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.commands.PrepareCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSqlCache;
import org.apache.doris.proto.Data;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.cache.CacheAnalyzer;
import org.apache.doris.resource.workloadgroup.WorkloadGroupMgr;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TMasterOpResult;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionEntry;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;

/**
 * Process one connection, the life cycle is the same as connection
 */
// Apache Doris 查询引擎（Query Engine, qe）中的一个核心抽象基类
// 其生命周期与客户端的连接保持一致（即一个连接对应一个或一组处理逻辑）
// 主要职责包括：
// 统一管理客户端连接会话：通过关联的 ConnectContext（会话上下文）管理用户的认证信息、当前数据库、会话变量、查询状态以及对应的连接通道（如 MySQL 协议通道或 Arrow Flight SQL 通道）。
// SQL 解析与重写调度：负责接收客户端发送的原始 SQL 语句（originStmt），结合方言转换（SqlDialectHelper）、SQL 缓存（Sql Cache）、Nereids 解析器进行语法解析，并支持对转换失败的 SQL 进行原样回退重试（tryRetryOriginalSql）。
// 多语句与批处理执行：支持处理分号分隔的多条 SQL 语句（Multi-Statements），并在执行过程中逐条进行状态重置、Workload Group 动态解析、执行器（StmtExecutor）初始化及运行。
// 协议适配与响应封装：针对 MySQL 协议提供字段列表获取（handleFieldList）、结果包生成（getResultPacket）和命令收尾（finalizeCommand）等底层网络交互封装；同时也支持主备转发代理执行（proxyExecute）。

public abstract class ConnectProcessor {
    // 定义连接的协议类型，支持 MYSQL（标准 MySQL 客户端协议）和 ARROW_FLIGHT_SQL（Arrow Flight SQL 高性能列存传输协议）。
    public enum ConnectType {
        MYSQL,
        ARROW_FLIGHT_SQL
    }

    private static final Logger LOG = LogManager.getLogger(ConnectProcessor.class);
    // 当前连接的会话上下文对象（被 protected 修饰），保存了用户身份、数据库名、会话变量、SQL 执行状态、通道信息等核心会话数据。
    protected final ConnectContext ctx;
    // 当前正在执行的 SQL 语句执行器（StmtExecutor），负责具体的优化、分布式计划生成与任务调度。
    protected StmtExecutor executor;
    // 当前连接实例所使用的协议类型。
    protected ConnectType connectType;
    // 保存来自远程执行器的结果集列表（常用于 Arrow Flight SQL 或分布式代理转发场景下，管理远程返回的结果）。
    protected ArrayList<StmtExecutor> returnResultFromRemoteExecutor = new ArrayList<>();

    public ConnectProcessor(ConnectContext context) {
        this.ctx = context;
    }

    public ConnectContext getConnectContext() {
        return ctx;
    }

    // change current database of this session.
    // 切换当前会话的数据库（Catalog 和 Database）
    protected void handleInitDb(String fullDbName) {
        Optional<Pair<ErrorCode, String>> res = ConnectContextUtil.initCatalogAndDb(ctx, fullDbName);
        if (res.isPresent()) {
            ctx.getState().setError(res.get().first, res.get().second);
        }
    }

    // set killed flag
    // 处理客户端断开连接或退出命令（QUIT）
    // 将 ConnectContext 标记为已杀死（setKilled），并将当前状态设置为成功（setOk）。
    protected void handleQuit() {
        ctx.setKilled();
        ctx.getState().setOk();
    }

    // do nothing
    // 处理客户端的 PING 心跳请求。什么都不做，直接将连接状态设置为成功（setOk），用于维持长连接。
    protected void handlePing() {
        ctx.getState().setOk();
    }

    // Do nothing for now.
    protected void handleStatistics() {
        ctx.getState().setOk();
    }

    // Do nothing for now.
    protected void handleDebug() {
        ctx.getState().setOk();
    }

    protected void handleResetConnection() {
        try {
            ctx.resetConnection();
            ctx.getState().setOk();
        } catch (UserException e) {
            ctx.getState().setError(e.getMysqlErrorCode(), e.getMessage());
        }
    }
    // 根据 Prepared Statement ID 重置预编译语句。若找不到对应的预编译上下文则报错 ERR_UNKNOWN_STMT_HANDLER，否则设置成功状态
    protected void handleStmtResetById(int stmtId) {
        if (ctx.getPreparedStementContext(String.valueOf(stmtId)) == null) {
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_STMT_HANDLER,
                    String.format("Unknown prepared statement handler (%s) given to mysqld_stmt_reset", stmtId));
            return;
        }
        ctx.getState().setOk();
    }
    // 关闭指定的预编译语句。从 ConnectContext 中移除该 stmtId 对应的预编译缓存，并设置状态为 NOOP（根据 MySQL 协议，COM_STMT_CLOSE 不需要返回响应包）。
    protected void handleStmtClose(int stmtId) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("close stmt id: {}", stmtId);
        }
        ConnectContext.get().removePrepareStmt(String.valueOf(stmtId));
        // No response packet is sent back to the client, see
        // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_close.html
        ctx.getState().setNoop();
    }
    // 通过位图（bitmap）检查指定位置的列值是否为 NULL。
    protected static boolean isNull(byte[] bitmap, int position) {
        return (bitmap[position / 8] & (1 << (position & 7))) != 0;
    }
    // 在语句执行完成后触发审计日志。
    // 如果开启了 BDBJE 调试模式则直接返回，否则调用 AuditLogHelper.logAuditLog 写入审计日志。
    protected void auditAfterExec(String origStmt, StatementBase parsedStmt,
            Data.PQueryStatistics statistics, boolean printFuzzyVariables) {
        if (Config.enable_bdbje_debug_mode) {
            return;
        }
        AuditLogHelper.logAuditLog(ctx, origStmt, parsedStmt, statistics, printFuzzyVariables);
    }

    /**
     * Pre-resolve the workload group name and set it on ConnectContext.
     * This ensures the workload group is available for audit logging even if the query
     * fails when analysing and before reaching Coordinator.exec (where it is normally
     * set). The resolution follows the same priority as WorkloadGroupMgr:
     * session variable -> user property -> default group.
     * Callers should invoke this at the start of any command dispatch path that may
     * emit an audit log (COM_QUERY / COM_STMT_EXECUTE / COM_FIELD_LIST / FlightSql
     * handleQuery). It is also re-invoked at the top of every iteration of
     * {@link #executeQuery}'s per-statement loop so multi-statement requests that
     * change {@code @@workload_group} mid-batch still audit later statements with
     * the effective value.
     */
    // 预先解析 Workload Group 名称并设置到 ConnectContext 中。
    // 优先级遵循：会话变量（Session Variable） -> 用户属性（User Property） -> 默认组（Default Group）。
    // 确保即使查询在解析阶段失败或在 Coordinator 执行前报错，审计日志也能正确记录 Workload Group。
    // 同时在多语句循环中每条语句执行前都会重新调用，以适配中间修改 Workload Group 的场景。
    protected void resolveWorkloadGroupName() {
        ctx.setWorkloadGroupName("");
        if (!Config.enable_workload_group) {
            return;
        }
        String groupName = ctx.getSessionVariable().getWorkloadGroup();
        if (Strings.isNullOrEmpty(groupName)) {
            groupName = Env.getCurrentEnv().getAuth().getWorkloadGroup(ctx.getQualifiedUser());
        }
        if (Strings.isNullOrEmpty(groupName)) {
            groupName = WorkloadGroupMgr.DEFAULT_GROUP_NAME;
        }
        ctx.setWorkloadGroupName(groupName);
    }

    // only throw an exception when there is a problem interacting with the requesting client
    // 处理外部传入的原始 SQL 查询。在执行前重置 queryId（防止 SQL 解析失败时带入上一次的 queryId）。
    protected void handleQuery(String originStmt) throws ConnectionException {
        // Before executing the query, the queryId should be set to empty.
        // Otherwise, if SQL parsing fails, the audit log will record the queryId from the previous query.
        ctx.resetQueryId();
        if (Config.isCloudMode()) {
        // 在 Cloud 模式下，如果仓库（Warehouse）状态处于逾期（OVERDUE）且非 root 用户，则直接抛出仓库逾期的异常。
            if (!ctx.getCurrentUserIdentity().isRootUser()
                    && ((CloudSystemInfoService) Env.getCurrentSystemInfo()).getInstanceStatus()
                        == Cloud.InstanceInfoPB.Status.OVERDUE) {
                Exception exception = new Exception("warehouse is overdue!");
                try {
                    handleQueryException(exception, originStmt, null, null);
                } catch (ConnectionException ignore) {
                    // ignore, should not happen
                }
                return;
            }
        }
        try {
            // 执行查询
            executeQuery(originStmt);
        } catch (ConnectionException exception) {
            throw exception;
        } catch (UserException exception) {
            LOG.warn("execute query exception", exception);
        } catch (Exception ignored) {
            // saved use handleQueryException
        }
    }
    // 查询执行的核心入口与调度流程
    // String originStmt 客户端发送给 Doris 的原始 SQL 语句字符串（可能包含单条 SQL 或用分号分隔的多条 SQL）
    public void executeQuery(String originStmt) throws Exception {
        // 第一部分：监控指标收集（Metrics & Cloud Mode 检查）
        // 检查监控指标库是否已初始化（MetricRepo.isInit），并且当前会话不是内部查询（!ctx.getState().isInternal()）。
        // 如果满足，则将全局请求计数器 COUNTER_REQUEST_ALL 自增 1。
        if (MetricRepo.isInit && !ctx.getState().isInternal()) {
            MetricRepo.COUNTER_REQUEST_ALL.increase(1L);
            // 如果当前 Doris 运行在云模式（
            if (Config.isCloudMode()) {
                try {
                    // 获取当前会话关联的云集群名称
                    String clusterName = ctx.getCloudCluster(false);
                    // 通过系统信息服务查询对应的物理集群名称
                    String physicalClusterName = ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                            .getPhysicalCluster(clusterName);
                    if (clusterName.equals(physicalClusterName)) {
                    // 集群归属判断：如果 clusterName 等于 physicalClusterName（非虚拟计算组 VCG），则直接增加该集群的请求计数；
                        // not vcg
                        MetricRepo.increaseClusterRequestAll(clusterName);
                    } else {
                        // 如果是虚拟计算组（VCG），则同时增加虚拟集群和底层物理集群的请求计数
                        // vcg
                        MetricRepo.increaseClusterRequestAll(clusterName);
                        MetricRepo.increaseClusterRequestAll(physicalClusterName);
                    }
                } catch (ComputeGroupException e) {
                    LOG.warn("metrics get cluster exception", e);
                }
            }
        }
        // 第二部分：SQL 方言转换与哈希计算
        // 根据当前会话变量中配置的 SQL 方言规则，对原始 SQL（originStmt）进行方言转换，生成 convertedStmt。
        String convertedStmt = SqlDialectHelper.convertSqlByDialect(originStmt, ctx.getSessionVariable());
        // 计算转换后 SQL 的 MD5 哈希值（sqlHash），用于在审计日志、慢查询跟踪或执行计划统计中唯一标识该 SQL。
        String sqlHash = DigestUtils.md5Hex(convertedStmt);
        ctx.setSqlHash(sqlHash);
        // 第三部分：尝试从 SQL Cache 中解析
        SessionVariable sessionVariable = ctx.getSessionVariable();
        // 判断当前查询是否允许并适合使用 SQL 缓存。
        boolean wantToParseSqlFromSqlCache = CacheAnalyzer.canUseSqlCache(sessionVariable);
        List<StatementBase> stmts = null;
        long parseSqlStartTime = System.currentTimeMillis();
        List<StatementBase> cachedStmts = null;
        CacheKeyType cacheKeyType = null;
        // 如果满足缓存条件
        if (wantToParseSqlFromSqlCache) {
            // 尝试从 SQL 缓存管理器中获取解析结果
            cachedStmts = parseFromSqlCache(originStmt);
            Optional<SqlCacheContext> sqlCacheContext = ConnectContext.get()
                    .getStatementContext().getSqlCacheContext();
            // 若成功命中，提取其 CacheKeyType，并将缓存的语句赋值给 stmts。
            if (sqlCacheContext.isPresent()) {
                cacheKeyType = sqlCacheContext.get().getCacheKeyType();
            }

            if (cachedStmts != null) {
                stmts = cachedStmts;
            }
        }
        // 第四部分：常规 SQL 解析（FallBack 机制）
        if (stmts == null) {
            // 利用 NereidsParser 对转换后的 SQL 进行解析（内部带有原始 SQL 重试回退机制）
            stmts = parseWithFallback(originStmt, convertedStmt, sessionVariable);
            if (stmts == null || stmts.isEmpty()) {
                return;
            }
        }
        // 第五部分：多语句（Multi-Statements）拆分与耗时记录
        List<String> origSingleStmtList = null;
        // if stmts.size() > 1, split originStmt to multi singleStmts
        if (stmts.size() > 1) {
            try {
                origSingleStmtList = SqlUtils.splitMultiStmts(convertedStmt);
            } catch (Exception ignore) {
                LOG.warn("Try to parse multi origSingleStmt failed, originStmt: \"{}\"", convertedStmt);
            }
        }
        long parseSqlFinishTime = System.currentTimeMillis();
        // 第六部分：多语句循环遍历与逐条执行
        boolean usingOrigSingleStmt = origSingleStmtList != null && origSingleStmtList.size() == stmts.size();
        // 进入 for 循环遍历每一条 SQL 语句
        for (int i = 0; i < stmts.size(); ++i) {
            String auditStmt = usingOrigSingleStmt ? origSingleStmtList.get(i) : convertedStmt;
            if (stmts.size() > 1 && usingOrigSingleStmt) {
                ctx.setSqlHash(DigestUtils.md5Hex(auditStmt));
            }
            try {
                ctx.getState().reset();
                if (i > 0) {
                    ctx.resetReturnRows();
                }
                // Re-resolve per statement: an earlier statement in the same multi-stmt
                // request (e.g. SET workload_group=...) may have changed the effective
                // workload group, and later statements that fail before Coordinator.exec
                // would otherwise be audited with the stale value picked up once per
                // packet in dispatch().
                // 重新解析 Workload Group
                resolveWorkloadGroupName();

                StatementBase parsedStmt = stmts.get(i);
                parsedStmt.setOrigStmt(new OriginStatement(auditStmt, usingOrigSingleStmt ? 0 : i));
                parsedStmt.setUserInfo(ctx.getCurrentUserIdentity());
                executor = new StmtExecutor(ctx, parsedStmt);
                executor.getProfile().getSummaryProfile().setParseSqlStartTime(parseSqlStartTime);
                executor.getProfile().getSummaryProfile().setParseSqlFinishTime(parseSqlFinishTime);
                executor.getProfile().getSummaryProfile().parsedByConnectionProcess = true;
                // Here we set the MoreStmtExists flag without considering CLIENT_MULTI_STATEMENTS.
                // So the master will always set SERVER_MORE_RESULTS_EXISTS when the statement is not the last one.
                // When the Follower/Observer received the return result of Master, the Follower/Observer
                // will check CLIENT_MULTI_STATEMENTS is set or not. It sends SERVER_MORE_RESULTS_EXISTS back to client
                // only when CLIENT_MULTI_STATEMENTS is set.
                // See the code below : if (getConnectContext().getMysqlChannel().clientMultiStatements())
                // 第七部分：多语句标志设置与指标更新
                // 如果当前不是最后一条语句且连接类型为 MySQL，则通过 executor.setMoreStmtExists(true) 标记后面还有更多语句。
                if (i != stmts.size() - 1 && connectType.equals(ConnectType.MYSQL)) {
                    executor.setMoreStmtExists(true);
                }
                // 若 Metric 已初始化，将解析耗时更新到解析耗时直方图（HISTO_PLAN_PARSE_DURATION）中。
                if (MetricRepo.isInit) {
                    MetricRepo.HISTO_PLAN_PARSE_DURATION.update(
                            executor.getProfile().getSummaryProfile().getParseSqlTimeMs());
                }
                ctx.setExecutor(executor);

                if (cacheKeyType != null) {
                    SqlCacheContext sqlCacheContext =
                            executor.getContext().getStatementContext().getSqlCacheContext().get();
                    sqlCacheContext.setCacheKeyType(cacheKeyType);
                }
                // 第八部分：执行控制、协议适配与审计触发
                try {
                    // 正式开始执行当前 SQL 语句。
                    executor.execute();
                    // MySQL 协议分支
                    if (connectType.equals(ConnectType.MYSQL)) {
                        if (i != stmts.size() - 1) {
                            ctx.getState().serverStatus |= MysqlServerStatusFlag.SERVER_MORE_RESULTS_EXISTS;
                            if (ctx.getState().getStateType() != MysqlStateType.ERR) {
                                // here, doris do different with mysql.
                                // when client not request CLIENT_MULTI_STATEMENTS, mysql treat all query as
                                // single statement. Doris treat it with multi statement, but only return
                                // the last statement result.
                                if (getConnectContext().getMysqlChannel().clientMultiStatements()) {
                                    finalizeCommand();
                                }
                            }
                        }
                    // Arrow Flight SQL 协议分支
                    } else if (connectType.equals(ConnectType.ARROW_FLIGHT_SQL)) {
                        // 若结果非本地返回，将其加入远程执行器结果列表。
                        if (!ctx.isReturnResultFromLocal()) {
                            returnResultFromRemoteExecutor.add(executor);
                        }
                        // 校验返回结果集数量必须 $\le 1$，
                        // 且如果有返回结果的语句必须位于多语句的最后一条，否则记录警告日志并设置错误状态码 ERR_ARROW_FLIGHT_SQL_MUST_ONLY_RESULT_STMT 后跳出循环。
                        Preconditions.checkState(ctx.getFlightSqlChannel().resultNum() <= 1);
                        //
                        if (ctx.getFlightSqlChannel().resultNum() == 1 && i != stmts.size() - 1) {
                            String errMsg = "Only be one stmt that returns the result and it is at the end. "
                                    + "stmts.size(): " + stmts.size();
                            LOG.warn(errMsg);
                            ctx.getState().setError(ErrorCode.ERR_ARROW_FLIGHT_SQL_MUST_ONLY_RESULT_STMT, errMsg);
                            ctx.getState().setErrType(QueryState.ErrType.OTHER_ERR);
                            break;
                        }
                    }
                    // 单条语句执行成功后，调用 auditAfterExec 记录审计日志。
                    auditAfterExec(auditStmt, executor.getParsedStmt(), executor.getQueryStatisticsForAuditLog(),
                            true);
                    // execute failed, skip remaining stmts
                    // 如果当前语句报错（MysqlStateType.ERR），
                    // 或者在非 Master 节点上通过 Proxy 转发执行失败（StatusCode 不为 0），则直接 break 跳出多语句循环，不执行后续语句。
                    if (ctx.getState().getStateType() == MysqlStateType.ERR || (!Env.getCurrentEnv().isMaster()
                            && ctx.executor != null && ctx.executor.hasForwardedToMaster()
                            && ctx.executor.getProxyStatusCode() != 0)) {
                        break;
                    }
                } catch (Throwable throwable) {
                    handleQueryException(throwable, auditStmt, executor.getParsedStmt(),
                            executor.getQueryStatisticsForAuditLog());
                    // execute failed, skip remaining stmts
                    throw throwable;
                }
            } finally {
                StatementContext statementContext = ctx.getStatementContext();
                if (statementContext != null) {
                    statementContext.close();
                }
            }
        }
    }

    private List<StatementBase> parseFromSqlCache(String originStmt) {
        StatementContext statementContext = new StatementContext(ctx, new OriginStatement(originStmt, 0));
        ctx.setStatementContext(statementContext);

        // the mysql protocol has different between COM_QUERY and COM_STMT_EXECUTE,
        // the sql cache use the result of COM_QUERY, so we can not provide the
        // result of sql cache for COM_STMT_EXECUTE/COM_STMT_PREPARE
        switch (ctx.getCommand()) {
            case COM_STMT_EXECUTE:
            case COM_STMT_PREPARE:
                return null;
            default: { }
        }

        try {
            Optional<Pair<ExplainOptions, String>> explainPlan = NereidsParser.tryParseExplainPlan(originStmt);
            String cacheSqlKey = originStmt;
            if (explainPlan.isPresent()) {
                cacheSqlKey = explainPlan.get().second;
            }
            Env env = ctx.getEnv();
            Optional<LogicalSqlCache> sqlCachePlanOpt = env.getSqlCacheManager().tryParseSql(ctx, cacheSqlKey);
            if (MetricRepo.isInit) {
                if (sqlCachePlanOpt.isPresent()) {
                    MetricRepo.COUNTER_SQL_CACHE_HIT.increase(1L);
                }
                MetricRepo.COUNTER_SQL_SQL_CACHE_TOTAL_SEARCH_TIMES.increase(1L);
            }
            if (sqlCachePlanOpt.isPresent()) {
                LogicalSqlCache logicalSqlCache = sqlCachePlanOpt.get();
                LogicalPlan parsedPlan = logicalSqlCache;
                if (explainPlan.isPresent()) {
                    ExplainOptions explainOptions = explainPlan.get().first;
                    parsedPlan = new ExplainCommand(
                            explainOptions.getExplainLevel(),
                            parsedPlan,
                            explainOptions.showPlanProcess()
                    );
                }

                LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(parsedPlan, statementContext);
                logicalPlanAdapter.setColLabels(
                        Lists.newArrayList(logicalSqlCache.getColumnLabels())
                );
                logicalPlanAdapter.setFieldInfos(Lists.newArrayList(logicalSqlCache.getFieldInfos()));
                logicalPlanAdapter.setResultExprs(logicalSqlCache.getResultExprs());
                logicalPlanAdapter.setOrigStmt(statementContext.getOriginStatement());
                logicalPlanAdapter.setUserInfo(ctx.getCurrentUserIdentity());
                return ImmutableList.of(logicalPlanAdapter);
            }
        } catch (Throwable t) {
            LOG.warn("Parse from sql cache failed with unexpected exception: {}", Util.getRootCauseMessage(t), t);
        } finally {
            statementContext.releasePlannerResources();
        }
        return null;
    }

    /**
     * Parse converted SQL statement with fallback to original SQL
     */
    protected List<StatementBase> parseWithFallback(String originStmt, String convertedStmt,
            SessionVariable sessionVariable) throws ConnectionException {
        try {
            return new NereidsParser().parseSQL(convertedStmt, sessionVariable);
        } catch (NotSupportedException | SyntaxParseException e) {
            List<StatementBase> stmts = tryRetryOriginalSql(originStmt, convertedStmt, sessionVariable);
            if (stmts == null) {
                handleQueryException(e, convertedStmt, null, null);
                return null;
            }
            return stmts;
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Nereids parse sql failed. Reason: {}. Statement: \"{}\".",
                        e.getMessage(), convertedStmt, e);
            }
            Throwable exception = new AnalysisException(e.getMessage(), e);
            List<StatementBase> stmts = tryRetryOriginalSql(originStmt, convertedStmt, sessionVariable);
            if (stmts == null) {
                handleQueryException(exception, originStmt, null, null);
                return null;
            }
            return stmts;
        }
    }

    /**
     * Try to retry SQL parsing with original SQL when conversion fails
     */
    protected List<StatementBase> tryRetryOriginalSql(String originStmt, String convertedStmt,
            SessionVariable sessionVariable) {
        // Try to retry with original SQL if enabled and convertedStmt is different from originStmt
        if (sessionVariable.isRetryOriginSqlOnConvertFail()
                && !convertedStmt.equals(originStmt)) {
            try {
                List<StatementBase> stmts = new NereidsParser().parseSQL(originStmt, sessionVariable);
                // Update sqlHash to use original statement hash when retry succeeds
                String originalSqlHash = DigestUtils.md5Hex(originStmt);
                ctx.setSqlHash(originalSqlHash);
                return stmts;
            } catch (Exception e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Retry with original SQL failed. "
                                    + "Reason: {}. Original Statement: \"{}\". Converted Statement: \"{}\".",
                            e.getMessage(), originStmt, convertedStmt, e);
                }
                // Retry failed, return null
                return null;
            }
        }
        return null;
    }

    // Use a handler for exception to avoid big try catch block which is a little hard to understand
    protected void handleQueryException(Throwable throwable, String origStmt,
            StatementBase parsedStmt, Data.PQueryStatistics statistics) throws ConnectionException {
        if (ctx.getMinidump() != null) {
            MinidumpUtils.saveMinidumpString(ctx.getMinidump(), DebugUtil.printId(ctx.queryId()));
        }
        if (throwable instanceof SyntaxParseException) {
            // Syntax parse exception.
            Throwable e = new AnalysisException(throwable.getMessage(), throwable);
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, e.getMessage());
            ctx.getState().setErrType(QueryState.ErrType.SYNTAX_PARSE_ERR);
        } else if (throwable instanceof ConnectionException) {
            // Throw this exception to close the connection outside.
            throw (ConnectionException) throwable;
        } else if (throwable instanceof IOException) {
            // Client failed.
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, "Doris process failed: " + throwable.getMessage());
        } else if (throwable instanceof UserException) {
            ctx.getState().setError(((UserException) throwable).getMysqlErrorCode(), throwable.getMessage());
            // set it as ANALYSIS_ERR so that it won't be treated as a query failure.
            ctx.getState().setErrType(QueryState.ErrType.ANALYSIS_ERR);
        } else if (throwable instanceof NotSupportedException) {
            ctx.getState().setError(ErrorCode.ERR_NOT_SUPPORTED_YET, throwable.getMessage());
            // set it as ANALYSIS_ERR so that it won't be treated as a query failure.
            ctx.getState().setErrType(QueryState.ErrType.ANALYSIS_ERR);
        } else {
            // Catch all throwable.
            // If reach here, maybe palo bug.
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR,
                    throwable.getClass().getSimpleName() + ", msg: " + throwable.getMessage());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Process one query failed because: {}", throwable.getMessage());
        }
        auditAfterExec(origStmt, parsedStmt, statistics, true);
    }

    // Get the column definitions of a table
    @SuppressWarnings("rawtypes")
    protected void handleFieldList(String tableName) throws ConnectionException {
        // Already get command code.
        if (Strings.isNullOrEmpty(tableName)) {
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_TABLE, "Empty tableName");
            return;
        }
        DatabaseIf db = ctx.getCurrentCatalog().getDbNullable(ctx.getDatabase());
        if (db == null) {
            ctx.getState().setError(ErrorCode.ERR_BAD_DB_ERROR, "Unknown database(" + ctx.getDatabase() + ")");
            return;
        }
        TableIf table = db.getTableNullable(tableName);
        if (table == null) {
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_TABLE, "Unknown table(" + tableName + ")");
            return;
        }

        table.readLock();
        try {
            if (connectType.equals(ConnectType.MYSQL)) {
                MysqlChannel channel = ctx.getMysqlChannel();
                MysqlSerializer serializer = channel.getSerializer();

                // Send fields
                // NOTE: Field list doesn't send number of fields
                List<Column> baseSchema = table.getBaseSchema();
                for (Column column : baseSchema) {
                    serializer.reset();
                    serializer.writeField(db.getFullName(), table.getName(), column, true);
                    channel.sendOnePacket(serializer.toByteBuffer());
                }
            } else if (connectType.equals(ConnectType.ARROW_FLIGHT_SQL)) {
                // TODO
            }
        } catch (Throwable throwable) {
            handleQueryException(throwable, "", null, null);
        } finally {
            table.readUnlock();
        }
        ctx.getState().setEof();
    }

    // only Mysql protocol
    protected ByteBuffer getResultPacket() {
        Preconditions.checkState(connectType.equals(ConnectType.MYSQL));
        MysqlPacket packet;
        // When CLIENT_DEPRECATE_EOF is set and the state is EOF (end of result set),
        // we need to send a "ResultSet OK" packet (0xFE header with payload > 5 bytes)
        // instead of the traditional EOF packet. This is required by the MySQL protocol
        // and expected by MySQL Connector/J 9.5.0+.
        if (ctx.getState().getStateType() == QueryState.MysqlStateType.EOF
                && ctx.getMysqlChannel().clientDeprecatedEOF()) {
            packet = new MysqlResultSetEndPacket(ctx.getState());
        } else {
            packet = ctx.getState().toResponsePacket();
        }
        if (packet == null) {
            // possible two cases:
            // 1. handler has send request
            // 2. this command need not to send response
            return null;
        }

        MysqlSerializer serializer = ctx.getMysqlChannel().getSerializer();
        serializer.reset();
        packet.writeTo(serializer);
        return serializer.toByteBuffer();
    }

    // When any request is completed, it will generally need to send a response packet to the client
    // This method is used to send a response packet to the client
    // only Mysql protocol
    public void finalizeCommand() throws IOException {
        LOG.debug("Finalize command for query {}", DebugUtil.printId(ctx.queryId));
        Preconditions.checkState(connectType.equals(ConnectType.MYSQL));
        ByteBuffer packet;
        if (executor != null && executor.hasForwardedToMaster()
                && ctx.getState().getStateType() != QueryState.MysqlStateType.ERR) {
            ShowResultSet resultSet = executor.getShowResultSet();
            if (resultSet == null) {
                executor.sendProxyQueryResult();
                packet = executor.getOutputPacket();
            } else {
                executor.sendResultSet(resultSet);
                packet = getResultPacket();
            }
        } else {
            packet = getResultPacket();
        }

        if (packet == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("packet == null");
            }
            return;
        }

        LOG.debug("Send to mysql channel for query {}", DebugUtil.printId(ctx.queryId));
        MysqlChannel channel = ctx.getMysqlChannel();
        channel.sendAndFlush(packet);
        // note(wb) we should write profile after return result to mysql client
        // because write profile maybe take too much time
        // explain query stmt do not have profile
        if (executor != null && executor.getParsedStmt() != null && !executor.getParsedStmt().isExplain()
                && (executor.getParsedStmt() instanceof LogicalPlanAdapter)) {
            executor.updateProfile(true);
            StatsErrorEstimator statsErrorEstimator = ConnectContext.get().getStatsErrorEstimator();
            if (statsErrorEstimator != null) {
                statsErrorEstimator.updateProfile(ConnectContext.get().queryId());
            }
        }
        LOG.debug("End finalizing command for query {}", DebugUtil.printId(ctx.queryId));
    }

    public TMasterOpResult proxyExecute(TMasterOpRequest request) throws TException {
        ctx.setDatabase(request.db);
        ctx.setEnv(Env.getCurrentEnv());
        ctx.getState().reset();
        if (request.isSetUserIp()) {
            ctx.setRemoteIP(request.getUserIp());
        }
        if (request.isSetStmtId()) {
            ctx.setForwardedStmtId(request.getStmtId());
        }
        if (request.isSetCurrentUserIdent()) {
            UserIdentity currentUserIdentity = UserIdentity.fromThrift(request.getCurrentUserIdent());
            ctx.setCurrentUserIdentity(currentUserIdentity);
        } else {
            ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp(request.user, "%"));
        }
        if (request.isFoldConstantByBe()) {
            ctx.getSessionVariable().setEnableFoldConstantByBe(request.foldConstantByBe);
        }

        if (request.isSetSessionVariables()) {
            ctx.getSessionVariable().setForwardedSessionVariables(request.getSessionVariables());
        }

        if (request.isSetUserVariables()) {
            ctx.setUserVars(userVariableFromThrift(request.getUserVariables()));
        }

        if (request.isSetConnectAttributes()) {
            ctx.setConnectAttributes(request.getConnectAttributes());
        }
        restoreForwardedSessionContext(ctx, request);

        // set compute group
        ctx.setComputeGroup(Env.getCurrentEnv().getAuth().getComputeGroup(ctx.getQualifiedUser()));

        // Propagate the client's CLIENT_DEPRECATE_EOF capability to the proxy channel.
        // This ensures the master generates packets matching the original client's protocol.
        if (request.isSetClientDeprecatedEOF() && request.isClientDeprecatedEOF()) {
            ctx.getMysqlChannel().setClientDeprecatedEOF();
        }

        ctx.setThreadLocalInfo();
        StmtExecutor executor = null;
        try {
            // 0 for compatibility.
            int idx = request.isSetStmtIdx() ? request.getStmtIdx() : 0;
            executor = new StmtExecutor(ctx, new OriginStatement(request.getSql(), idx), true);
            // Set default catalog only if the catalog exists.
            if (request.isSetDefaultCatalog()) {
                CatalogIf catalog = ctx.getEnv().getCatalogMgr().getCatalog(request.getDefaultCatalog());
                if (catalog != null) {
                    ctx.getEnv().changeCatalog(ctx, request.getDefaultCatalog());
                    // Set default db only when the default catalog is set and the dbname exists in default catalog.
                    if (request.isSetDefaultDatabase()) {
                        DatabaseIf db = ctx.getCurrentCatalog().getDbNullable(request.getDefaultDatabase());
                        if (db != null) {
                            ctx.getEnv().changeDb(ctx, request.getDefaultDatabase());
                        }
                    }
                }
            }

            // set transaction entry for transaction load
            if (request.isSetTxnLoadInfo()) {
                TransactionEntry transactionEntry = new TransactionEntry();
                transactionEntry.setTxnInfoInMaster(request.getTxnLoadInfo());
                ctx.setTxnEntry(transactionEntry);
            }

            TUniqueId queryId; // This query id will be set in ctx
            if (request.isSetQueryId()) {
                queryId = request.getQueryId();
            } else {
                UUID uuid = UUID.randomUUID();
                queryId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
            }
            if (request.isSetPrepareExecuteBuffer()) {
                ctx.setCommand(MysqlCommand.COM_STMT_PREPARE);
                executor.execute();
                ctx.setCommand(MysqlCommand.COM_STMT_EXECUTE);
                String preparedStmtId = executor.getPrepareStmtName();
                PreparedStatementContext preparedStatementContext = ctx.getPreparedStementContext(preparedStmtId);
                if (preparedStatementContext == null) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Something error, just support nereids preparedStmtId:{}", preparedStmtId);
                    }
                    throw new RuntimeException("Prepare failed when proxy execute");
                }
                handleExecute(preparedStatementContext.command, Long.parseLong(preparedStmtId),
                        preparedStatementContext,
                        ByteBuffer.wrap(request.getPrepareExecuteBuffer()).order(ByteOrder.LITTLE_ENDIAN), queryId);
            } else {
                executor.queryRetry(queryId);
            }
        } catch (IOException e) {
            // Client failed.
            LOG.warn("Process one query failed because IOException: ", e);
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, "Doris process failed: " + e.getMessage());
        } catch (Throwable e) {
            // Catch all throwable.
            // If reach here, maybe Doris bug.
            LOG.warn("Process one query failed because unknown reason: ", e);
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, "Unexpected exception: " + e.getMessage());
        } finally {
            // Master-side forwarded statements (redirect DDL / SHOW) execute via Command.run and never reach
            // unregisterQuery, so their per-statement connector scope has no query-finish trigger. Close it here
            // via StatementContext.close() (the same per-statement close the direct-connection path runs in its
            // finally). Idempotent: a coordinated forwarded query's scope is already closed by its query-finish
            // callback. These statements run synchronously with no off-thread scan pump, so close-after-use holds.
            StatementContext forwardedStatementContext = ctx.getStatementContext();
            if (forwardedStatementContext != null) {
                forwardedStatementContext.close();
            }
        }
        // no matter the master execute success or fail, the master must transfer the result to follower
        // and tell the follower the current journalID.
        TMasterOpResult result = new TMasterOpResult();
        if (ctx.queryId() != null
                // If none master FE not set query id or query id was reset in StmtExecutor
                // when a query exec more than once, return it to none master FE.
                && (!request.isSetQueryId() || !request.getQueryId().equals(ctx.queryId()))
        ) {
            result.setQueryId(ctx.queryId());
        }
        result.setMaxJournalId(Env.getCurrentEnv().getMaxJournalId());
        if (request.moreResultExists) {
            ctx.getState().serverStatus |= MysqlServerStatusFlag.SERVER_MORE_RESULTS_EXISTS;
        }
        result.setPacket(getResultPacket());
        result.setStatus(ctx.getState().toString());
        if (ctx.getState().getStateType() == MysqlStateType.OK) {
            result.setStatusCode(0);
            result.setAffectedRows(ctx.getState().getAffectedRows());
        } else {
            ErrorCode errorCode = ctx.getState().getErrorCode();
            if (errorCode != null) {
                result.setStatusCode(errorCode.getCode());
            } else {
                result.setStatusCode(ErrorCode.ERR_UNKNOWN_ERROR.getCode());
            }
            result.setErrMessage(ctx.getState().getErrorMessage());
        }
        if (request.isSetTxnLoadInfo()) {
            TransactionEntry transactionEntry = ConnectContext.get().getTxnEntry();
            // null if this is a commit or rollback command
            if (transactionEntry != null) {
                result.setTxnLoadInfo(transactionEntry.getTxnInfoInMaster());
            }
        }
        if (executor != null) {
            if (executor.getProxyShowResultSet() != null) {
                result.setResultSet(executor.getProxyShowResultSet().tothrift());
            } else if (!executor.getProxyQueryResultBufList().isEmpty()) {
                result.setQueryResultBufList(executor.getProxyQueryResultBufList());
            }
        }
        return result;
    }

    static void restoreForwardedSessionContext(ConnectContext context, TMasterOpRequest request) {
        if (!request.isSetDelegatedCredentialToken()) {
            return;
        }
        Preconditions.checkState(request.isSetDelegatedCredentialType(),
                "delegated credential type is required");
        Preconditions.checkState(request.isSetDelegatedCredentialSessionId(),
                "delegated credential session id is required");
        OptionalLong expiresAtMillis = request.isSetDelegatedCredentialExpiresAtMillis()
                ? OptionalLong.of(request.getDelegatedCredentialExpiresAtMillis())
                : OptionalLong.empty();
        String sessionId = request.getDelegatedCredentialSessionId();
        context.setSessionContext(SessionContext.of(sessionId, new DelegatedCredential(
                DelegatedCredential.Type.valueOf(request.getDelegatedCredentialType()),
                request.getDelegatedCredentialToken(),
                expiresAtMillis)));
    }

    // only Mysql protocol
    public void processOnce() throws IOException, NotImplementedException {
        throw new NotImplementedException("Not Impl processOnce");
    }

    private Map<String, LiteralExpr> userVariableFromThrift(Map<String, TExprNode> thriftMap) throws TException {
        try {
            Map<String, LiteralExpr> userVariables = Maps.newHashMap();
            for (Map.Entry<String, TExprNode> entry : thriftMap.entrySet()) {
                TExprNode tExprNode = entry.getValue();
                LiteralExpr literalExpr = getLiteralExprFromThrift(tExprNode);
                userVariables.put(entry.getKey(), literalExpr);
            }
            return userVariables;
        } catch (AnalysisException e) {
            throw new TException(e.getMessage());
        }
    }

    private static LiteralExpr getLiteralExprFromThrift(TExprNode node) throws AnalysisException {
        TExprNodeType type = node.node_type;
        switch (type) {
            case NULL_LITERAL: return new NullLiteral();
            case BOOL_LITERAL: return new BoolLiteral(node.bool_literal.value);
            case INT_LITERAL: return new IntLiteral(node.int_literal.value);
            case LARGE_INT_LITERAL: return new LargeIntLiteral(node.large_int_literal.value);
            case FLOAT_LITERAL: return new FloatLiteral(node.float_literal.value);
            case DECIMAL_LITERAL: return DecimalLiteralUtils.create(node.decimal_literal.value);
            case STRING_LITERAL: return new StringLiteral(node.string_literal.value);
            case JSON_LITERAL: return new JsonLiteral(node.json_literal.value);
            case DATE_LITERAL: return DateLiteralUtils.createDateLiteral(node.date_literal.value, null);
            case IPV4_LITERAL: return new IPv4Literal(node.ipv4_literal.value);
            case IPV6_LITERAL: return new IPv6Literal(node.ipv6_literal.value);
            default: throw new AnalysisException("Wrong type from thrift;");
        }
    }


    protected void handleExecute(PrepareCommand prepareCommand, long stmtId, PreparedStatementContext prepCtx,
            ByteBuffer packetBuf, TUniqueId queryId) {
        throw new NotSupportedException("Just MysqlConnectProcessor support execute");
    }
}
