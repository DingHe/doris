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

package org.apache.doris.httpv2.rest;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.cloud.qe.ComputeGroupException;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.entity.RestBaseResult;
import org.apache.doris.httpv2.exception.UnauthorizedException;
import org.apache.doris.httpv2.util.StreamLoadRedirectDrainUtil;
import org.apache.doris.load.StreamLoadHandler;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.planner.GroupCommitPlanner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.resource.computegroup.ComputeGroup;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BeSelectionPolicy;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.base.Strings;
import com.google.common.net.HostAndPort;
import io.netty.handler.codec.http.HttpHeaderNames;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.view.RedirectView;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Enumeration;
import java.util.List;
import java.util.Optional;

// LoadAction 是 Apache Doris Frontend (FE) 模块中负责处理 HTTP 数据导入（如 Stream Load、HTTP Stream、2PC 两阶段提交等）请求的关键 RESTful API 控制器。
// 在 Apache Doris 的分布式架构中，FE（Frontend）主要负责元数据管理、权限校验、查询解析与调度，而实际的数据存储和计算（包括数据导入解析与写入）则由 BE（Backend）完成。
// LoadAction 的核心作用是 “导入请求的入口控制器与重定向路由器”：
// 统一入口与协议对接：暴露 RESTful API 接口，接收客户端发起的 HTTP Stream Load 请求（包含普通 Stream Load、根据 SQL 的 HTTP Stream、2PC 两阶段提交控制等）。
// 鉴权与状态校验：对请求进行身份认证（Password 或 Cluster Token）、数据库/数据表粒度的权限检查（LOAD 权限），以及 Group Commit 状态校验（如检查是否因 Schema Change 等而被 Block）。
// 目标 BE 节点选择（路由决策）：根据集群模式（存算一体 Local 模式或存算分离 Cloud 模式）、Group Commit 配置、负载均衡策略（Round-Robin 轮询等）、多网卡与公私网策略（Redirect Policy），选择最合适的 BE 节点。
// 请求重定向（HTTP Redirect / 307）：将客户端的 HTTP 数据流重定向（Redirect）至选定的 BE 节点，由 BE 直接接收并处理具体的数据写入，从而降低 FE 的网络与内存吞吐压力。
@RestController
public class LoadAction extends RestBaseController {

    private static final Logger LOG = LogManager.getLogger(LoadAction.class);
    // 对应常量 "sub_label"。用于历史/废弃的 Multi Load 子标签参数标识。
    public static final String SUB_LABEL_NAME_PARAM = "sub_label";
    // 对应 HTTP Header 常量 "redirect-policy"。允许客户端在 Request Header 中指定 FE 到 BE 的重定向网络策略。
    public static final String HEADER_REDIRECT_POLICY = "redirect-policy";
    // 对应策略值 "public-private"。根据客户端 IP（内网/公网 IP）自动选择 BE 的公网或私网 Endpoint。
    public static final String REDIRECT_POLICY_PUBLIC_PRIVATE = "public-private";
    // 对应策略值 "random-be"。随机/直接使用 BE 的默认主机地址。
    public static final String REDIRECT_POLICY_RANDOM_BE = "random-be";
    // 对应策略值 "direct"。直接重定向到 BE 的原始 Host 和 HTTP Port。
    public static final String REDIRECT_POLICY_DIRECT = "direct";
    // 对应策略值 "public"。强制使用 BE 标注的 Public Endpoint。
    public static final String REDIRECT_POLICY_PUBLIC = "public";
    // 对应策略值 "private"。强制使用 BE 标注的 Private Endpoint。
    public static final String REDIRECT_POLICY_PRIVATE = "private";
    // 用于获取当前 FE 节点的执行环境及相关服务组件。
    private ExecuteEnv execEnv = ExecuteEnv.getInstance();
    // 记录上一次轮询选择 BE 节点的索引位置，用于实现简易的 Round-Robin 负载均衡。
    private int lastSelectedBackendIndex = 0;
    // 处理旧版的 Mini Load 导入请求。
    @RequestMapping(path = "/api/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_load", method = RequestMethod.PUT)
    public Object load(HttpServletRequest request, HttpServletResponse response,
            @PathVariable(value = DB_KEY) String db, @PathVariable(value = TABLE_KEY) String table) {
        if (Config.disable_mini_load) {
            ResponseEntity entity = ResponseEntityBuilder.notFound("The mini load operation has been"
                    + " disabled by default, if you need to add disable_mini_load=false in fe.conf.");
            return entity;
        } else {
            executeCheckPassword(request, response);
            return executeWithoutPassword(request, response, db, table, false, false);
        }
    }
    // 标准的 Stream Load HTTP 入口，处理向指定 db.table 导入数据的请求。
    // @PathVariable(value = DB_KEY) String db：从 URL 路径中提取的目标数据库名称（Database）。
    // @PathVariable(value = TABLE_KEY) String table：从 URL 路径中提取的目标数据表名称（Table）。
    @RequestMapping(path = "/api/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_stream_load", method = RequestMethod.PUT)
    public Object streamLoad(HttpServletRequest request,
            HttpServletResponse response,
            @PathVariable(value = DB_KEY) String db, @PathVariable(value = TABLE_KEY) String table) {
        // 记录 Stream Load 调用的入口日志。
        LOG.info("streamload action, db: {}, tbl: {}, headers: {}", db, table, getAllHeaders(request));
        // 初始化组提交（Group Commit）标识变量 groupCommit 为 false，并尝试从 HTTP 请求头中读取名为 group_commit 的参数。
        boolean groupCommit = false;
        String groupCommitStr = request.getHeader("group_commit");
        // 组提交（Group Commit）逻辑校验
        if (groupCommitStr != null) {
            // 合法性校验。group_commit 参数只接受三种合法值（不区分大小写）：
            // sync_mode（同步组提交） async_mode（异步组提交）off_mode（关闭组提交）
            if (!groupCommitStr.equalsIgnoreCase("async_mode") && !groupCommitStr.equalsIgnoreCase("sync_mode")
                    && !groupCommitStr.equalsIgnoreCase("off_mode")) {
                return new RestBaseResult("Header `group_commit` can only be `sync_mode`, `async_mode` or `off_mode`.");
            }
            if (!groupCommitStr.equalsIgnoreCase("off_mode")) {
                groupCommit = true;
                // 调用 isGroupCommitBlock(db, table)，检查当前表是否因为存在正在进行的 Schema Change（加减列、修改数据类型等）或其他元数据变更而被暂停/阻塞写入。
                // 为什么针对 async_mode 检查：因为异步组提交会在 BE 先写 WAL 日志，如果 Schema 正在变更，异步 WAL 可能会因 Schema 不匹配导致落盘失败或无法回放，因此在此阶段直接拦截并返回提示信息（如 GroupCommitPlanner.SCHEMA_CHANGE 对应的拒接原因）。
                if (groupCommitStr.equalsIgnoreCase("async_mode")) {
                    try {
                        if (isGroupCommitBlock(db, table)) {
                            String msg = "insert table " + table + GroupCommitPlanner.SCHEMA_CHANGE;
                            return new RestBaseResult(msg);
                        }
                    } catch (Exception e) {
                        LOG.info("exception:" + e);
                        return new RestBaseResult(e.getMessage());
                    }
                }
            }
        }
        // 读取请求头中的 token 参数（通常用于 Doris 内部节点通信或内部插件的 Cluster Token 机制，如审计日志插件的内部写入）。
        String authToken = request.getHeader("token");
        // if auth token is not null, check it first
        // 判断是否提供了 token。如果 token 非空，优先走 Cluster Token 鉴权逻辑。
        // 校验成功后调用 executeWithClusterToken。该方法会将当前线程伪装为 Admin 身份，完成后端 BE 节点的挑选并返回重定向响应（HTTP 307）。
        if (!Strings.isNullOrEmpty(authToken)) {
            if (!checkClusterToken(authToken)) {
                throw new UnauthorizedException("Invalid token: " + authToken);
            }
            return executeWithClusterToken(request, response, db, table, true);
        } else {
            // 未提供 token 时，进入常规用户密码鉴权流程（使用 HTTP Basic Auth 或包含 username/password 的 Header）。
            try {
                executeCheckPassword(request, response);
                // 密码校验通过后，调用 executeWithoutPassword 方法执行实际的核心逻辑：
                return executeWithoutPassword(request, response, db, table, true, groupCommit);
            } finally {
                ConnectContext.remove();
            }
        }
    }

    @RequestMapping(path = "/api/_http_stream", method = RequestMethod.PUT)
    public Object streamLoadWithSql(HttpServletRequest request, HttpServletResponse response) {
        String sql = request.getHeader("sql");
        LOG.info("streaming load sql={}", sql);
        boolean groupCommit = false;
        long tableId = -1;
        String groupCommitStr = request.getHeader("group_commit");
        if (groupCommitStr != null) {
            if (!groupCommitStr.equalsIgnoreCase("async_mode") && !groupCommitStr.equalsIgnoreCase("sync_mode")
                    && !groupCommitStr.equalsIgnoreCase("off_mode")) {
                return new RestBaseResult("Header `group_commit` can only be `sync_mode`, `async_mode` or `off_mode`.");
            }
            if (!groupCommitStr.equalsIgnoreCase("off_mode")) {
                try {
                    groupCommit = true;
                    String[] pair = parseDbAndTb(sql);
                    Database db = Env.getCurrentInternalCatalog()
                            .getDbOrException(pair[0], s -> new TException("database is invalid for dbName: " + s));
                    Table tbl = db.getTableOrException(pair[1], s -> new TException("table is invalid: " + s));
                    tableId = tbl.getId();

                    // async mode needs to write WAL, we need to block load during waiting WAL.
                    if (groupCommitStr.equalsIgnoreCase("async_mode")) {
                        if (isGroupCommitBlock(pair[0], pair[1])) {
                            String msg = "insert table " + pair[1] + GroupCommitPlanner.SCHEMA_CHANGE;
                            return new RestBaseResult(msg);
                        }

                    }
                } catch (Exception e) {
                    LOG.info("exception:" + e);
                    return new RestBaseResult(e.getMessage());
                }
            }
        }
        executeCheckPassword(request, response);
        try {
            // A 'Load' request must have 100-continue header
            if (request.getHeader(HttpHeaderNames.EXPECT.toString()) == null) {
                return new RestBaseResult("There is no 100-continue header");
            }

            String label = request.getHeader(LABEL_KEY);
            TNetworkAddress redirectAddr = selectRedirectBackend(request, groupCommit, tableId);

            LOG.info("redirect load action to destination={}, label: {}",
                    redirectAddr.toString(), label);

            return createRedirectResponse(request, response, redirectAddr, true, null, null, label);
        } catch (Exception e) {
            return new RestBaseResult(e.getMessage());
        }
    }
    // boolean：返回布尔值。true 表示当前表在 Group Commit 模式下处于阻塞状态（禁止写入）；false 表示未阻塞（可以正常写入）
    private boolean isGroupCommitBlock(String db, String table) throws TException {
        // 获取当前 Doris FE（Frontend）内存中的内部 Catalog（InternalCatalog），它是所有内部数据库和表元数据的根入口
        Database dbObj = Env.getCurrentInternalCatalog()
                .getDbOrException(db, s -> new TException("database is invalid for dbName: " + s));
        // 从刚获取的数据库对象 dbObj 中，根据传入的表名 table 查找对应的 Table 元数据对象。
        Table tblObj = dbObj.getTableOrException(table, s -> new TException("table is invalid: " + s));
        // 获取 Group Commit 的全局管理器 GroupCommitManager，该管理器维护着当前集群中所有表的组提交状态、WAL 队列与写阻塞标记。
        // 调用 isBlock 方法传入 tableId，检查该表当前是否正处于组提交的 Block 锁定状态。
        return Env.getCurrentEnv().getGroupCommitManager().isBlock(tblObj.getId());
    }

    private String[] parseDbAndTb(String sql) throws Exception {
        String[] array = sql.split(" ");
        String tmp = null;
        int count = 0;
        for (String s : array) {
            if (!s.equals("")) {
                count++;
                if (count == 3) {
                    tmp = s;
                    break;
                }
            }
        }
        if (tmp == null) {
            throw new Exception("parse db and tb with wrong sql:" + sql);
        }
        String pairStr = null;
        if (tmp.contains("(")) {
            pairStr = tmp.split("\\(")[0];
        } else {
            pairStr = tmp;
        }
        String[] pair = pairStr.split("\\.");
        if (pair.length != 2) {
            throw new Exception("parse db and tb with wrong sql:" + sql);
        }
        return pair;
    }

    @RequestMapping(path = "/api/{" + DB_KEY + "}/_stream_load_2pc", method = RequestMethod.PUT)
    public Object streamLoad2PC(HttpServletRequest request,
            HttpServletResponse response,
            @PathVariable(value = DB_KEY) String db) {
        LOG.info("streamload action 2PC, db: {}, headers: {}", db, getAllHeaders(request));
        executeCheckPassword(request, response);
        return executeStreamLoad2PC(request, db);
    }

    @RequestMapping(path = "/api/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_stream_load_2pc", method = RequestMethod.PUT)
    public Object streamLoad2PC_table(HttpServletRequest request,
            HttpServletResponse response,
            @PathVariable(value = DB_KEY) String db,
            @PathVariable(value = TABLE_KEY) String table) {
        LOG.info("streamload action 2PC, db: {}, tbl: {}, headers: {}", db, table, getAllHeaders(request));
        executeCheckPassword(request, response);
        return executeStreamLoad2PC(request, db);
    }

    // Same as Multi load, to be compatible with http v1's response body,
    // we return error by using RestBaseResult.
    // boolean isStreamLoad：标识当前请求是否为 Stream Load。true 表示为 Stream Load；false 表示为 Mini Load（旧版导入方式）。
    // boolean groupCommit：标识本次导入请求是否开启了组提交（Group Commit）模式。
    private Object executeWithoutPassword(HttpServletRequest request,
            HttpServletResponse response, String db, String table, boolean isStreamLoad, boolean groupCommit) {
        // 阶段 1：变量声明与 HTTP 请求基础合法性校验
        String label = null;
        try {
            String dbName = db;
            String tableName = table;
            // A 'Load' request must have 100-continue header
            // 校验 HTTP 请求头中是否包含 Expect: 100-continue。
            // 设计意图：Stream Load 通常伴随着大体积的数据流传输。
            // 要求客户端先发送 Expect: 100-continue，可以确保 FE 在校验完权限和决定好目标 BE 节点后再通知客户端开始发送 Body 数据，避免因鉴权失败导致的大文件传输浪费。
            if (request.getHeader(HttpHeaderNames.EXPECT.toString()) == null) {
                return new RestBaseResult("There is no 100-continue header");
            }

            if (Strings.isNullOrEmpty(dbName)) {
                return new RestBaseResult("No database selected.");
            }

            if (Strings.isNullOrEmpty(tableName)) {
                return new RestBaseResult("No table selected.");
            }

            String fullDbName = dbName;
            // 阶段 2：导入 Label 校验与权限检查
            // 如果是 Stream Load（isStreamLoad == true），从 HTTP Header 中读取 label
            // 如果是 Mini Load（isStreamLoad == false），从 URL 参数 中读取 label。
            label = isStreamLoad ? request.getHeader(LABEL_KEY) : request.getParameter(LABEL_KEY);
            if (!isStreamLoad && Strings.isNullOrEmpty(label)) {
                // for stream load, the label can be generated by system automatically
                return new RestBaseResult("No label selected.");
            }

            // check auth
            // 数据库表级别的权限检查
            // 校验该用户对 fullDbName.tableName 是否拥有 LOAD（导入）权限，若权限不足会抛出未授权异常。
            checkTblAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, tableName, PrivPredicate.LOAD);
            // 阶段 3：废弃功能拦截与 Group Commit 元数据获取
            TNetworkAddress redirectAddr;
            // 拦截早已废弃的 Multi Load 功能（当存在 sub_label 参数时）。若触发则直接返回 "Multi load is longer supported" 错误提示。
            if (!isStreamLoad && !Strings.isNullOrEmpty(request.getParameter(SUB_LABEL_NAME_PARAM))) {
                return new RestBaseResult("Multi load is longer supported");
            } else {
                long tableId = -1;
                if (groupCommit) {
                    Optional<?> database = Env.getCurrentEnv().getCurrentCatalog().getDb(dbName);
                    if (!database.isPresent()) {
                        return new RestBaseResult("Database not found.");
                    }

                    Optional<?> olapTable = ((Database) database.get()).getTable(tableName);
                    if (!olapTable.isPresent()) {
                        return new RestBaseResult("OlapTable not found.");
                    }

                    tableId = ((OlapTable) olapTable.get()).getId();
                }
                // Handle stream load with potential group commit forwarding
                // 阶段 4：计算重定向 BE 节点并生成响应
                // 调用 handleStreamLoadRedirect 方法，根据轮询策略（Round-Robin）、网络策略（公网/私网 IP 映射）、云原生模式或组提交需求，计算挑选出最合适接收该数据流的目标 BE 节点的网络地址（TNetworkAddress）。
                redirectAddr = handleStreamLoadRedirect(request, groupCommit, tableId, dbName, tableName, label);
            }

            if (LOG.isDebugEnabled()) {
                LOG.info("redirect load action to destination={}, stream: {}, db: {}, tbl: {}, label: {}",
                        redirectAddr.toString(), isStreamLoad, dbName, tableName, label);
            }
            // 调用 createRedirectResponse，构造并返回一个 HTTP 重定向响应（通常是 HTTP 307 Temporary Redirect），其中包含指向目标 BE 节点的物理 URL 地址，引导客户端将实际的数据流直接推送到该 BE 节点。
            return createRedirectResponse(request, response, redirectAddr, isStreamLoad, dbName, tableName, label);
        } catch (StreamLoadForwardException e) {
            // Handle IOException from redirect response generation in the forwarding path.
            try {
                return createRedirectResponse(request, response, e.getRedirectView(),
                        isStreamLoad, db, table, label);
            } catch (IOException ioException) {
                LOG.warn("stream load forward redirect failed, stream: {}, db: {}, tbl: {}, label: {}, err: {}",
                        isStreamLoad, db, table, label, ioException.getMessage());
                return new RestBaseResult(ioException.getMessage());
            }
        } catch (Exception e) {
            LOG.warn("load failed, stream: {}, db: {}, tbl: {}, label: {}, err: {}",
                    isStreamLoad, db, table, label, e.getMessage());
            return new RestBaseResult(e.getMessage());
        }
    }

    private Object executeStreamLoad2PC(HttpServletRequest request, String db) {
        try {
            String dbName = db;

            if (Strings.isNullOrEmpty(dbName)) {
                return new RestBaseResult("No database selected.");
            }

            if (Strings.isNullOrEmpty(request.getHeader(TXN_ID_KEY))
                    && Strings.isNullOrEmpty(request.getHeader(LABEL_KEY))) {
                return new RestBaseResult("No transaction id or label selected.");
            }

            String txnOperation = request.getHeader(TXN_OPERATION_KEY);
            if (Strings.isNullOrEmpty(txnOperation)) {
                return new RestBaseResult("No transaction operation(\'commit\' or \'abort\') selected.");
            }

            TNetworkAddress redirectAddr = selectRedirectBackend(request, false, -1);
            LOG.info("redirect stream load 2PC action to destination={}, db: {}, txn: {}, operation: {}",
                    redirectAddr.toString(), dbName, request.getHeader(TXN_ID_KEY), txnOperation);

            RedirectView redirectView = redirectToBackend(request, redirectAddr);
            return redirectView;

        } catch (Exception e) {
            return new RestBaseResult(e.getMessage());
        }
    }

    private final synchronized int getLastSelectedBackendIndexAndUpdate() {
        int index = lastSelectedBackendIndex;
        lastSelectedBackendIndex = (index >= Integer.MAX_VALUE - 1) ? 0 : index + 1;
        return index;
    }

    private String getCloudClusterName(HttpServletRequest request) {
        String cloudClusterName = request.getHeader(SessionVariable.COMPUTE_GROUP);
        if (!Strings.isNullOrEmpty(cloudClusterName)) {
            return cloudClusterName;
        }

        cloudClusterName = request.getHeader(SessionVariable.CLOUD_CLUSTER);
        if (!Strings.isNullOrEmpty(cloudClusterName)) {
            return cloudClusterName;
        }

        try {
            cloudClusterName = ConnectContext.get().getCloudCluster();
        } catch (ComputeGroupException e) {
            LOG.warn("get cloud cluster name failed", e);
            return "";
        }
        if (!Strings.isNullOrEmpty(cloudClusterName)) {
            return cloudClusterName;
        }

        return "";
    }

    private TNetworkAddress selectRedirectBackend(HttpServletRequest request, boolean groupCommit, long tableId)
            throws LoadException {
        return selectRedirectBackend(request, groupCommit, tableId, null);
    }
    // HttpServletRequest request：原生的 HTTP 请求对象，用于获取 HTTP Header（例如提取 redirect-policy 网络重定向策略、云原生环境下的计算集群名称 cloudClusterName 等）。
    // boolean groupCommit：标识当前导入请求是否开启了组提交（Group Commit）模式。
    // long tableId：目标数据表的全局唯一 ID（Table ID）。在组提交模式下，用于通过 Hash 计算将同一张表的数据绑定到固定的 BE 节点上。
    private TNetworkAddress selectRedirectBackend(HttpServletRequest request, boolean groupCommit, long tableId,
            Backend preSelectedBackend) throws LoadException {
        // 1. Debug 测试打点逻辑（Fault Injection / Debug Point）
        long debugBackendId = DebugPointUtil.getDebugParamOrDefault("LoadAction.selectRedirectBackend.backendId", -1L);
        if (debugBackendId != -1L) {
            Backend backend = Env.getCurrentSystemInfo().getBackend(debugBackendId);
            return new TNetworkAddress(backend.getHost(), backend.getHttpPort());
        }
        // 2. 存算分离（Cloud Mode）分支逻辑
        if (Config.isCloudMode()) {
            // 从 HTTP 请求头或 Session 上下文中解析当前请求归属的计算集群（Compute Group / Cloud Cluster）名称。
            String cloudClusterName = getCloudClusterName(request);
            if (Strings.isNullOrEmpty(cloudClusterName)) {
                throw new LoadException("No cloud cluster name selected.");
            }
            // 转发调用 selectCloudRedirectBackend 方法，传入集群名称及预选节点等参数，进一步在指定计算集群内筛选出可用的 BE 网络地址并返回。
            return selectCloudRedirectBackend(cloudClusterName, request, groupCommit, tableId, preSelectedBackend);
        } else {
        // 3. 存算一体（Local / Traditional Mode）分支逻辑
            if (groupCommit && tableId == -1) {
                throw new LoadException("Group commit table id wrong.");
            }
            return selectLocalRedirectBackend(groupCommit, request, tableId, preSelectedBackend);
        }
    }

    private TNetworkAddress selectLocalRedirectBackend(boolean groupCommit, HttpServletRequest request, long tableId,
            Backend preSelectedBackend) throws LoadException {
        // 阶段 1：变量声明与连接上下文（ConnectContext）校验
        Backend backend = null;
        BeSelectionPolicy policy = null;
        ConnectContext ctx = ConnectContext.get();
        if (ctx == null) {
            throw new LoadException("ConnectContext should not be null");
        }
        // 阶段 2：计算组（Compute Group）获取与 BE 挑选策略构建
        ComputeGroup computeGroup = ctx.getComputeGroupSafely();
        // 构建 BE 筛选策略（BeSelectionPolicy）
        // 开启轮询（Round-Robin）负载均衡机制。
        // 要求筛选出的 BE 必须是健康且支持数据导入（Load Available）的节点。
        policy = new BeSelectionPolicy.Builder().setEnableRoundRobin(true).needLoadAvailable().build();
        // 获取并原子更新上一次选择的索引，将其设置为本次轮询选择的起始偏移量，实现多请求之间的平滑轮询。
        policy.nextRoundRobinIndex = getLastSelectedBackendIndexAndUpdate();
        // 阶段 3：按策略筛选可用 BE 列表及空节点检查
        List<Long> backendIds;
        int number = groupCommit ? -1 : 1;
        // 从当前计算组（computeGroup.getBackendList()）的所有 BE 中，按照策略筛选满足条件的 BE ID 列表：
        backendIds = Env.getCurrentSystemInfo().selectBackendIdsByPolicy(policy, number, computeGroup.getBackendList());
        if (backendIds.isEmpty()) {
            throw new LoadException(
                    SystemInfoService.NO_BACKEND_LOAD_AVAILABLE_MSG + ", policy: " + policy + ", compute group is "
                            + computeGroup.toString());
        }
        // 阶段 4：目标 BE 节点的具体选取（常规轮询 vs 组提交）
        if (groupCommit) {
            // Use pre-selected backend if provided to avoid duplicate calls
            backend = preSelectedBackend != null ? preSelectedBackend
                    : selectBackendForGroupCommit("", request, tableId);
        } else {
            backend = Env.getCurrentSystemInfo().getBackend(backendIds.get(0));
        }
        if (backend == null) {
            throw new LoadException(SystemInfoService.NO_BACKEND_LOAD_AVAILABLE_MSG + ", policy: " + policy);
        }
        // 阶段 5：按网络策略输出重定向 Endpoint
        // 将计算出的目标 BE 节点根据客户端的网络访问需求（如请求头中的 redirect-policy）进行 IP 和端口的转换映射。
        return selectEndpointByRedirectPolicy(request, backend);
    }

    private TNetworkAddress selectCloudRedirectBackend(String clusterName, HttpServletRequest req, boolean groupCommit,
            long tableId, Backend preSelectedBackend) throws LoadException {
        Backend backend = null;
        if (groupCommit) {
            // Use pre-selected backend if provided to avoid duplicate calls
            backend = preSelectedBackend != null ? preSelectedBackend
                    : selectBackendForGroupCommit(clusterName, req, tableId);
        } else {
            backend = StreamLoadHandler.selectBackend(clusterName);
        }
        return selectEndpointByRedirectPolicy(req, backend);
    }

    /**
     * Selects the endpoint address based on the redirect policy specified in the request header.
     * The available redirect policies are:
     * - DIRECT: Redirects to the backend's host.
     * - PUBLIC: Redirects to the public endpoint of the backend.
     * - PRIVATE: Redirects to the private endpoint of the backend.
     * - PUBLIC_PRIVATE: Redirects based on the host IP or domain. If the  host is a site-local
     *     address, redirects to the private endpoint. Otherwise, redirects to the public endpoint.
     * - DEFAULT: If request host equals to backend's public endpoint, redirects to the public endpoint.
     *     If private endpoint of backend is set, redirects to the private endpoint. Otherwise, redirects
     *     to the backend's host.
     *
     * @param req The HTTP request object.
     * @param backend The backend to redirect to.
     * @return The selected endpoint address.
     * @throws LoadException If there is an error in the redirect policy or endpoint selection.
     */
    private TNetworkAddress selectEndpointByRedirectPolicy(HttpServletRequest req, Backend backend)
            throws LoadException {
        Pair<String, Integer> publicHostPort = null;
        Pair<String, Integer> privateHostPort = null;
        try {
            if (!Strings.isNullOrEmpty(backend.getPublicEndpoint())) {
                publicHostPort = splitHostAndPort(backend.getPublicEndpoint());
            }
        } catch (AnalysisException e) {
            throw new LoadException(e.getMessage());
        }

        try {
            if (!Strings.isNullOrEmpty(backend.getPrivateEndpoint())) {
                privateHostPort = splitHostAndPort(backend.getPrivateEndpoint());
            }
        } catch (AnalysisException e) {
            throw new LoadException(e.getMessage());
        }

        String redirectPolicy = req.getHeader(LoadAction.HEADER_REDIRECT_POLICY);
        redirectPolicy = redirectPolicy == null || redirectPolicy.isEmpty()
                ? Config.streamload_redirect_policy : redirectPolicy;

        String reqHostStr = req.getHeader(HttpHeaderNames.HOST.toString());
        reqHostStr = reqHostStr.replaceAll("\\s+", "");
        if (reqHostStr.isEmpty()) {
            LOG.info("Invalid header host: {}", reqHostStr);
            throw new LoadException("Invalid header host: " + reqHostStr);
        }

        String reqHost = "";
        try {
            reqHost = HostAndPort.fromString(reqHostStr).getHost();
        } catch (IllegalArgumentException e) {
            LOG.info("Invalid header host: {}", reqHostStr);
            throw new LoadException("Invalid header host: " + reqHostStr);
        }

        // User specified redirect policy
        if (redirectPolicy != null && (redirectPolicy.equalsIgnoreCase(REDIRECT_POLICY_DIRECT)
                || redirectPolicy.equalsIgnoreCase(REDIRECT_POLICY_RANDOM_BE))) {
            return new TNetworkAddress(backend.getHost(), backend.getHttpPort());
        } else if (redirectPolicy != null && redirectPolicy.equalsIgnoreCase(REDIRECT_POLICY_PUBLIC)) {
            if (publicHostPort != null) {
                return new TNetworkAddress(publicHostPort.first, publicHostPort.second);
            }
            throw new LoadException("public endpoint is null, please check be public endpoint config");
        } else if (redirectPolicy != null && redirectPolicy.equalsIgnoreCase(REDIRECT_POLICY_PRIVATE)) {
            if (privateHostPort != null) {
                return new TNetworkAddress(privateHostPort.first, privateHostPort.second);
            }
            throw new LoadException("private endpoint is null, please check be private endpoint config");
        } else if (redirectPolicy != null && redirectPolicy.equalsIgnoreCase(REDIRECT_POLICY_PUBLIC_PRIVATE)) {
            // redirect with ip
            if (InetAddressValidator.getInstance().isValid(reqHost)) {
                InetAddress addr;
                try {
                    addr = InetAddress.getByName(reqHost);
                } catch (Exception e) {
                    LOG.warn("unknown host expection: {}", e.getMessage());
                    throw new LoadException(e.getMessage());
                }
                if (addr.isSiteLocalAddress() && privateHostPort != null) {
                    return new TNetworkAddress(privateHostPort.first, privateHostPort.second);
                } else if (publicHostPort != null) {
                    return new TNetworkAddress(publicHostPort.first, publicHostPort.second);
                } else {
                    LOG.warn("Invalid ip or wrong cluster, host: {}, public endpoint: {}, private endpoint: {}",
                            reqHostStr, publicHostPort, privateHostPort);
                    throw new LoadException("Invalid header host: " + reqHost);
                }
            }

            // redirect with domain
            if (publicHostPort != null && reqHost.toLowerCase().contains("public")) {
                return new TNetworkAddress(publicHostPort.first, publicHostPort.second);
            } else if (privateHostPort != null) {
                return new TNetworkAddress(privateHostPort.first, privateHostPort.second);
            } else {
                LOG.warn("Invalid host or wrong cluster, host: {}, public endpoint: {}, private endpoint: {}",
                        reqHostStr, publicHostPort, privateHostPort);
                throw new LoadException("Invalid header host: " + reqHost);
            }
        } else {
            if (InetAddressValidator.getInstance().isValid(reqHost)
                    && publicHostPort != null && reqHost.equalsIgnoreCase(publicHostPort.first)) {
                return new TNetworkAddress(publicHostPort.first, publicHostPort.second);
            } else if (privateHostPort != null) {
                // use request host here, because private endpoint may be unknown for cloud mode
                return new TNetworkAddress(reqHost, privateHostPort.second);
            } else {
                return new TNetworkAddress(backend.getHost(), backend.getHttpPort());
            }
        }
    }

    private Pair<String, Integer> splitHostAndPort(String hostPort) throws AnalysisException {
        hostPort = hostPort.replaceAll("\\s+", "");
        if (hostPort.isEmpty()) {
            LOG.info("empty endpoint");
            throw new AnalysisException("empty endpoint: " + hostPort);
        }

        String host;
        int port;
        try {
            HostAndPort hp = HostAndPort.fromString(hostPort);
            if (!hp.hasPort()) {
                throw new IllegalArgumentException("No port found");
            }
            host = hp.getHost();
            port = hp.getPort();
        } catch (IllegalArgumentException e) {
            LOG.info("Invalid endpoint: {}", hostPort);
            throw new AnalysisException("Invalid endpoint: " + hostPort);
        }

        if (port <= 0 || port >= 65536) {
            LOG.info("Invalid endpoint port: {}", port);
            throw new AnalysisException("Invalid endpoint port: " + port);
        }

        return Pair.of(host, port);
    }

    // NOTE: This function can only be used for AuditlogPlugin stream load for now.
    // AuditlogPlugin should be re-disigned carefully, and blow method focuses on
    // temporarily addressing the users' needs for audit logs.
    // So this function is not widely tested under general scenario
    private Object executeWithClusterToken(HttpServletRequest request, HttpServletResponse response, String db,
            String table, boolean isStreamLoad) {
        try {
            ConnectContext ctx = new ConnectContext();
            ctx.setEnv(Env.getCurrentEnv());
            ctx.setThreadLocalInfo();
            ctx.setRemoteIP(request.getRemoteAddr());
            // set user to ADMIN_USER, so that we can get the proper resource tag
            // cloud need
            ctx.setCurrentUserIdentity(UserIdentity.ADMIN);
            ctx.setThreadLocalInfo();

            String dbName = db;
            String tableName = table;
            // A 'Load' request must have 100-continue header
            if (request.getHeader(HttpHeaderNames.EXPECT.toString()) == null) {
                return new RestBaseResult("There is no 100-continue header");
            }

            if (Strings.isNullOrEmpty(dbName)) {
                return new RestBaseResult("No database selected.");
            }

            if (Strings.isNullOrEmpty(tableName)) {
                return new RestBaseResult("No table selected.");
            }

            String label = request.getParameter(LABEL_KEY);
            if (isStreamLoad) {
                label = request.getHeader(LABEL_KEY);
            }

            if (!isStreamLoad && Strings.isNullOrEmpty(label)) {
                // for stream load, the label can be generated by system automatically
                return new RestBaseResult("No label selected.");
            }

            TNetworkAddress redirectAddr = selectRedirectBackend(request, false, -1);

            LOG.info("Redirect load action with auth token to destination={},"
                            + "stream: {}, db: {}, tbl: {}, label: {}",
                    redirectAddr.toString(), isStreamLoad, dbName, tableName, label);

            return createRedirectResponse(request, response, redirectAddr, isStreamLoad, dbName, tableName, label);
        } catch (Exception e) {
            LOG.warn("Failed to execute stream load with cluster token, {}", e.getMessage(), e);
            return new RestBaseResult(e.getMessage());
        } finally {
            ConnectContext.remove();
        }
    }

    private String getAllHeaders(HttpServletRequest request) {
        StringBuilder headers = new StringBuilder();
        Enumeration<String> headerNames = request.getHeaderNames();
        while (headerNames.hasMoreElements()) {
            String headerName = headerNames.nextElement();
            String headerValue = isSensitiveHeader(headerName) ? "***MASKED***" : request.getHeader(headerName);
            headers.append(headerName).append(":").append(headerValue).append(", ");
        }
        return headers.toString();
    }

    private Object createRedirectResponse(HttpServletRequest request, HttpServletResponse response,
            TNetworkAddress redirectAddr, boolean isStreamLoad, String dbName, String tableName, String label)
            throws IOException {
        String redirectUrl = buildRedirectUrlToBackend(request, redirectAddr, request.getRequestURI(),
                request.getQueryString());
        if (!shouldUseBoundedDrainForStreamLoad(isStreamLoad)) {
            return redirectToBackend(request, redirectAddr);
        }
        writeTemporaryRedirect(response, redirectUrl);
        DrainDecision drainDecision = decideDrainDecisionForStreamLoadRedirect(request);
        if (drainDecision != DrainDecision.DRAIN) {
            LOG.info("skip bounded drain after stream load redirect, target: {}, db: {}, tbl: {}, label: {},"
                            + " reason: {}",
                    redirectAddr, dbName, tableName, label, drainDecision);
            return null;
        }
        drainStreamLoadRequestBodyAfterRedirect(request, redirectAddr.toString(), dbName, tableName, label);
        return null;
    }

    private Object createRedirectResponse(HttpServletRequest request, HttpServletResponse response,
            RedirectView redirectView, boolean isStreamLoad, String dbName, String tableName, String label)
            throws IOException {
        if (!shouldUseBoundedDrainForStreamLoad(isStreamLoad)) {
            return redirectView;
        }
        writeTemporaryRedirect(response, redirectView.getUrl());
        DrainDecision drainDecision = decideDrainDecisionForStreamLoadRedirect(request);
        if (drainDecision != DrainDecision.DRAIN) {
            LOG.info("skip bounded drain after stream load redirect, target: {}, db: {}, tbl: {}, label: {},"
                            + " reason: {}",
                    redirectView.getUrl(), dbName, tableName, label, drainDecision);
            return null;
        }
        drainStreamLoadRequestBodyAfterRedirect(request, redirectView.getUrl(), dbName, tableName, label);
        return null;
    }

    private boolean shouldUseBoundedDrainForStreamLoad(boolean isStreamLoad) {
        return isStreamLoad && Config.stream_load_redirect_bounded_drain_max_bytes > 0;
    }

    // Skip the bounded drain for header-only probes and oversized fixed-length bodies.
    private DrainDecision decideDrainDecisionForStreamLoadRedirect(HttpServletRequest request) {
        long contentLength = request.getContentLengthLong();
        String transferEncoding = request.getHeader(HttpHeaderNames.TRANSFER_ENCODING.toString());
        if (contentLength <= 0 && Strings.isNullOrEmpty(transferEncoding)) {
            return DrainDecision.SKIP_NO_REQUEST_BODY;
        }
        if (contentLength > Config.stream_load_redirect_bounded_drain_max_bytes) {
            return DrainDecision.SKIP_CONTENT_LENGTH_EXCEEDS_MAX_BYTES;
        }
        return DrainDecision.DRAIN;
    }

    private void drainStreamLoadRequestBodyAfterRedirect(HttpServletRequest request, String redirectTarget,
            String dbName, String tableName, String label) {
        long drainLimit = Config.stream_load_redirect_bounded_drain_max_bytes;
        LOG.info("write stream load redirect and start bounded drain, target: {}, db: {}, tbl: {}, label: {},"
                        + " max_drain_bytes: {}",
                redirectTarget, dbName, tableName, label, drainLimit);
        StreamLoadRedirectDrainUtil.DrainResult drainResult =
                StreamLoadRedirectDrainUtil.drainRequestBodyAfterRedirect(request, drainLimit);
        LOG.info("finish bounded drain after stream load redirect, target: {}, db: {}, tbl: {}, label: {},"
                        + " drained_bytes: {}, elapsed_ms: {}, exit_reason: {}",
                redirectTarget, dbName, tableName, label, drainResult.getDrainedBytes(),
                drainResult.getElapsedMillis(), drainResult.getExitReason());
    }

    private enum DrainDecision {
        SKIP_NO_REQUEST_BODY,
        SKIP_CONTENT_LENGTH_EXCEEDS_MAX_BYTES,
        DRAIN
    }

    private boolean isSensitiveHeader(String headerName) {
        return "Authorization".equalsIgnoreCase(headerName)
                || "Proxy-Authorization".equalsIgnoreCase(headerName)
                || "Cookie".equalsIgnoreCase(headerName)
                || "Set-Cookie".equalsIgnoreCase(headerName)
                || "token".equalsIgnoreCase(headerName)
                || "Auth-Token".equalsIgnoreCase(headerName);
    }

    private Backend selectBackendForGroupCommit(String clusterName, HttpServletRequest req, long tableId)
            throws LoadException {
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setThreadLocalInfo();
        ctx.setRemoteIP(req.getRemoteAddr());
        // We set this variable to fulfill required field 'user' in
        // TMasterOpRequest(FrontendService.thrift)
        ctx.setCurrentUserIdentity(UserIdentity.ADMIN);
        ctx.setThreadLocalInfo();
        if (Config.isCloudMode()) {
            ctx.setCloudCluster(clusterName);
        }

        Backend backend = null;
        try {
            backend = Env.getCurrentEnv().getGroupCommitManager()
                    .selectBackendForGroupCommit(tableId, ctx);
        } catch (DdlException e) {
            throw new LoadException(e.getMessage(), e);
        }
        return backend;
    }

    /*
     * Create redirect URL for stream load forward mode.
     *
     * This method constructs the special redirect URL used in the group commit forwarding mechanism:
     *
     * Key modifications to the standard redirect:
     * 1. Path modification: Changes "/_stream_load" to "/_stream_load_forward"
     *    - This tells the receiving BE that it needs to perform additional forwarding
     *    - The "_stream_load_forward" endpoint is specifically designed to handle forwarding logic
     *
     * 2. Forward target parameter: Adds "forward_to=host:port" to the query string
     *    - Specifies the actual target BE node that should process this request
     *    - Ensures all requests for the same table reach the same BE for optimal batching
     *
     * 3. Authentication preservation: Maintains user authentication in the URL if present
     *    - Ensures the forwarded request has proper authentication context
     *
     * Example transformation:
     * Original: http://endpoint:port/api/db/table/_stream_load?param=value
     * Forward:  http://endpoint:port/api/db/table/_stream_load_forward?param=value&forward_to=target_be:port
     *
     * @param request the original HTTP request
     * @param addr the endpoint address to redirect to (public/private endpoint)
     * @param forwardTarget the target BE node in "host:port" format for final processing
     * @return RedirectView configured for stream load forwarding
     */
    private RedirectView redirectToStreamLoadForward(HttpServletRequest request, TNetworkAddress addr,
            String forwardTarget) {
        // Replace _stream_load with _stream_load_forward in the path.
        String modifiedPath = request.getRequestURI().replace("/_stream_load", "/_stream_load_forward");
        String queryString = request.getQueryString();
        String redirectQuery = "forward_to=" + forwardTarget;
        if (!Strings.isNullOrEmpty(queryString)) {
            redirectQuery = queryString + "&" + redirectQuery;
        }
        String redirectUrl = buildRedirectUrlToBackend(request, addr, modifiedPath, redirectQuery);

        LOG.info("Redirect stream load forward url: {}, forward_to: {}",
                "http://" + addr.getHostname() + ":" + addr.getPort() + modifiedPath, forwardTarget);
        RedirectView redirectView = new RedirectView(redirectUrl);
        redirectView.setContentType("text/html;charset=utf-8");
        redirectView.setStatusCode(org.springframework.http.HttpStatus.TEMPORARY_REDIRECT);
        return redirectView;
    }

    /**
     * Handle stream load redirect with optional group commit forwarding.
     *
     * Group Commit Stream Load Forward Mode in Cloud Environment:
     *
     * Problem:
     * Group commit requires that requests for the same table be sent to the same BE node
     * to achieve better batching efficiency. However, in cloud mode with Load Balancer (LB),
     * the LB randomly selects a BE node for forwarding, which breaks the group commit strategy
     * and reduces batching effectiveness.
     *
     * Solution:
     * Implement a two-stage forwarding mechanism:
     * 1. FE redirects to public/private endpoint (LB) as usual
     * 2. BE performs a second forwarding to the actual target BE node that handles the specific table
     *
     * This ensures that all requests for the same table ultimately reach the same BE node,
     * preserving the group commit batching strategy while still utilizing the LB infrastructure.
     *
     * @param request the HTTP request
     * @param groupCommit whether group commit is enabled
     * @param tableId the table ID for group commit
     * @param dbName database name for logging
     * @param tableName table name for logging
     * @param label label for logging
     * @return redirect address for normal redirect
     * @throws StreamLoadForwardException if forward redirect is applied
     * @throws LoadException if redirect selection fails
     */
    // 问题背景：组提交（Group Commit）的核心思想是将同表的多笔小批量写请求攒到同一个 BE 节点上进行 Group 聚合作业，以极大提升攒批与吞吐效率。
    // 存算分离/云原生环境下的冲突：云环境下前端通常挂载了 负载均衡器（Load Balancer, LB）。即使 FE 重定向给了 LB 的公共 Endpoint，LB 也会随机分发给不同的 BE，这打破了“同一张表去往同一 BE”的策略，导致 Group Commit 攒批失效。
    // 解决方案（二级转发机制）：
    // 阶段 1 (FE 维度)：FE 正常将请求重定向到暴露给外部的 Endpoint / LB 地址（redirectAddr）。
    // 阶段 2 (BE 维度)：接收到请求的 BE（即入口 BE）检查发现自己不是该表设定的目标 BE 后，将数据流进一步二次转发（Forwarding） 到真正负责该表组提交的核心目标 BE 节点（targetBackend）。
    private TNetworkAddress handleStreamLoadRedirect(HttpServletRequest request, boolean groupCommit,
            long tableId, String dbName, String tableName, String label) throws LoadException {

        // Check if group commit forwarding is needed
        // 阶段 1：判定是否满足开启“BE 转发”的条件
        // 检查 3 个前置条件。若满足以下任意一项，说明不需要开启 BE 二级转发模式，直接退回到常规的重定向节点选择逻辑：
        // 不是存算分离（Cloud Mode）环境。
        // 请求没有开启组提交（Group Commit）。
        // 系统配置中关闭了组提交的 BE 转发开关。
        if (!Config.isCloudMode() || !groupCommit || !Config.enable_group_commit_streamload_be_forward) {
            return selectRedirectBackend(request, groupCommit, tableId);
        }
        // 阶段 2：存算分离环境下的集群与目标 BE 提取
        String cloudClusterName = getCloudClusterName(request);
        if (Strings.isNullOrEmpty(cloudClusterName)) {
            throw new LoadException("No cloud cluster name selected for group commit forwarding.");
        }

        // Get target backend for group commit
        // 根据 cloudClusterName 和 tableId，精准计算出该表在组提交逻辑下真正应该写入的终点 BE 节点（targetBackend）。若计算失败或无可用 BE，抛出 LoadException。
        Backend targetBackend = selectBackendForGroupCommit(cloudClusterName, request, tableId);
        if (targetBackend == null) {
            throw new LoadException("Failed to select target backend for group commit forwarding.");
        }

        // Get redirect address with optimized backend selection
        // 阶段 3：计算入口重定向地址与终点 BE 地址
        // 调用 selectCloudRedirectBackend，根据网络策略（redirect-policy，例如对外暴露的 LB 公网地址/域名）获取客户端首期应该连接的入口地址。
        TNetworkAddress redirectAddr = selectCloudRedirectBackend(cloudClusterName, request, groupCommit, tableId,
                targetBackend);
        // 根据 targetBackend 直接构造出内部网络中物理目标 BE 的真实 Host 与 HTTP Port。
        TNetworkAddress targetAddr = new TNetworkAddress(targetBackend.getHost(), targetBackend.getHttpPort());

        // Apply forwarding if addresses differ (compare hostname and port directly)
        // 阶段 4：二次转发判定与异常触发
        // 比较客户端即将连接的入口地址（redirectAddr）与真正处理数据的目标 BE 地址（targetAddr）的 Hostname 和 Port。
        if (!redirectAddr.getHostname().equals(targetAddr.getHostname())
                || redirectAddr.getPort() != targetAddr.getPort()) {
            // Apply stream load forwarding by throwing StreamLoadForwardException with RedirectView
            // 分支含义：如果地址不一致，说明请求通过 LB 或代理节点接入后，可能落到错误的 BE 上，必须应用 BE 级别的二次转发机制。
            String forwardTarget = targetAddr.getHostname() + ":" + targetAddr.getPort();
            RedirectView forwardRedirectView = redirectToStreamLoadForward(request, redirectAddr, forwardTarget);

            LOG.info("Using stream load forward mode for cloud group commit - "
                    + "db: {}, tbl: {}, label: {}, endpoint: {}, forward_to: {}, reason: redirect_differs_from_target",
                    dbName, tableName, label, redirectAddr.toString(), forwardTarget);

            throw new StreamLoadForwardException(forwardRedirectView);
        } else {
            LOG.debug("Skip stream load forward - redirect address matches target backend: {}",
                    redirectAddr.toString());
            return redirectAddr;
        }
    }

    /**
     * Special exception to carry RedirectView for stream load forwarding.
     */
    private static class StreamLoadForwardException extends RuntimeException {
        private final RedirectView redirectView;

        public StreamLoadForwardException(RedirectView redirectView) {
            this.redirectView = redirectView;
        }

        public RedirectView getRedirectView() {
            return redirectView;
        }
    }
}
