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

package org.apache.doris.load.loadv2;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.GlobListing;
import org.apache.doris.filesystem.Location;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.BrokerFileGroupAggInfo.FileGroupAggKey;
import org.apache.doris.load.FailMsg;
import org.apache.doris.thrift.TBrokerFileStatus;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

// BrokerLoadPendingTask 是 Broker 导入（Broker Load）作业流程中的预处理/文件扫描任务（继承自 LoadTask）
// 在作业刚开始执行（PENDING 阶段）时，异步连接外部存储系统（如 HDFS、S3、Bos 等），
// 解析并获取用户指定路径下的所有源文件状态、文件大小、目录结构，并进行过滤校验（如排除空的 Parquet/ORC 文件），最终将文件清单组装好并触发整个导入事务的开启。
// BrokerLoadPendingTask 负责解决 Broker Load 导入的第一步。当用户提交一条 Broker 导入命令后，FE 不会立刻把数据塞给 BE，而是先由该任务去远端存储（通过 FileSystem 或 Broker）把待加载文件的元数据（Path、Size、Directory 等）全量拉取并盘点清楚。
// 盘点完成后，它会通过回调将文件状态封装进 BrokerPendingTaskAttachment，并正式为该作业调用 beginTxn() 开启分布式事务。
public class BrokerLoadPendingTask extends LoadTask {

    private static final Logger LOG = LogManager.getLogger(BrokerLoadPendingTask.class);
    // 文件组聚合信息映射。
    // 保存了按表/目标分区聚合后的文件组列表，明确了当前任务需要去外部存储的哪些路径（File Paths）下加载什么格式的数据。
    protected Map<FileGroupAggKey, List<BrokerFileGroup>> aggKeyToBrokerFileGroups;
    // 外部存储与 Broker 描述符。
    // 包含了连接外部存储系统（如 HDFS 的 NameNode 地址、S3 的 Endpoint、AccessKey、SecretKey 等凭证信息）的完整配置。
    protected BrokerDesc brokerDesc;

    public BrokerLoadPendingTask(BrokerLoadJob loadTaskCallback,
                                 Map<FileGroupAggKey, List<BrokerFileGroup>> aggKeyToBrokerFileGroups,
                                 BrokerDesc brokerDesc, Priority priority) {
        super(loadTaskCallback, TaskType.PENDING, priority);
        this.retryTime = 3;
        this.attachment = new BrokerPendingTaskAttachment(signature);
        this.aggKeyToBrokerFileGroups = aggKeyToBrokerFileGroups;
        this.brokerDesc = brokerDesc;
        this.failMsg = new FailMsg(FailMsg.CancelType.ETL_RUN_FAIL);
    }

    @Override
    public void executeTask() throws UserException {
        LOG.info("begin to execute broker pending task. job: {}", callback.getCallbackId());
        // 去远端存储扫描、遍历并收集所有的文件状态与大小。
        getAllFileStatus();
        // 正式向全局事务管理器申请开启一个分布式事务（PREPARE 状态）。
        ((BrokerLoadJob) callback).beginTxn();
    }
    // 核心文件扫描与处理逻辑
    // 负责连接远端文件系统，支持 Glob 通配符解析、S3 路径枚举、空文件过滤及文件总大小/总行数统计。
    protected void getAllFileStatus() throws UserException {
        long start = System.currentTimeMillis();
        long totalFileSize = 0;
        int totalFileNum = 0;
        // 遍历文件组聚合映射：按表（aggKey）和文件组（fileGroups）逐组处理。
        for (Map.Entry<FileGroupAggKey, List<BrokerFileGroup>> entry : aggKeyToBrokerFileGroups.entrySet()) {
            FileGroupAggKey aggKey = entry.getKey();
            List<BrokerFileGroup> fileGroups = entry.getValue();

            List<List<TBrokerFileStatus>> fileStatusList = Lists.newArrayList();
            long tableTotalFileSize = 0;
            int tableTotalFileNum = 0;
            int groupNum = 0;
            // 分支 A（多文件/多 Load 代理模式）：校验文件路径数与文件大小数是否一致，若不一致抛出 AnalysisException；
            // 直接从配置中读取文件大小并组装成 TBrokerFileStatus 列表。
            if (brokerDesc.isMultiLoadBroker()) {
                for (BrokerFileGroup fileGroup : fileGroups) {
                    if (fileGroup.getFilePaths().size() != fileGroup.getFileSize().size()) {
                        LOG.warn("Cannot get file size, file path count {}, file size count {}",
                                fileGroup.getFilePaths().size(), fileGroup.getFileSize().size());
                        throw new AnalysisException("Cannot get file size.");
                    }
                    List<TBrokerFileStatus> fileStatuses = Lists.newArrayList();
                    tableTotalFileNum += fileGroup.getFilePaths().size();
                    for (int i = 0; i < fileGroup.getFilePaths().size(); i++) {
                        tableTotalFileSize += fileGroup.getFileSize().get(i);
                        TBrokerFileStatus fileStatus = new TBrokerFileStatus(fileGroup.getFilePaths().get(i),
                                false, fileGroup.getFileSize().get(i), false);
                        fileStatuses.add(fileStatus);
                    }
                    fileStatusList.add(fileStatuses);
                }
            } else {
                // 分支 B（标准文件系统/S3/HDFS 模式，更常见）：
                for (BrokerFileGroup fileGroup : fileGroups) {
                    long groupFileSize = 0;
                    List<TBrokerFileStatus> fileStatuses = Lists.newArrayList();
                    for (String path : fileGroup.getFilePaths()) {
                        // 获取对应文件系统客户端。
                        try (FileSystem fs = FileSystemFactory.getFileSystem(brokerDesc)) {
                            // Use glob semantics (matching the old BrokerUtil.parseFile/globList behavior):
                            // exact paths match only that file, glob patterns expand.
                            // Plain listFiles uses S3 prefix matching which can return unintended
                            // prefix-siblings (e.g. "file.csv.bz2" when listing "file.csv").
                            List<FileEntry> entries;
                            try {
                                // 利用 fs.globListWithLimit(...)（支持带通配符的路径匹配，兼顾老版本 BrokerUtil 的行为）或者降级使用 fs.listFiles(...) 扫描远端路径，获取所有的 FileEntry 列表。
                                GlobListing listing = fs.globListWithLimit(
                                        Location.of(path), null, 0, 0);
                                entries = listing.getFiles();
                            } catch (UnsupportedOperationException ex) {
                                entries = fs.listFiles(Location.of(path));
                            }
                            for (FileEntry e : entries) {
                                fileStatuses.add(new TBrokerFileStatus(
                                        e.location().uri(), e.isDirectory(), e.length(), !e.isDirectory()));
                            }
                        } catch (java.io.IOException e) {
                            throw new org.apache.doris.common.UserException(
                                    brokerDesc.getName() + " list path exception. path=" + path
                                    + ", err: " + e.getMessage(), e);
                        }
                    }
                    if (!fileStatuses.isEmpty()) {
                        fileGroup.initDeferredFileFormatPropertiesIfNecessary(fileStatuses);
                        boolean isBinaryFileFormat = fileGroup.isBinaryFileFormat();
                        List<TBrokerFileStatus> filteredFileStatuses = Lists.newArrayList();
                        for (TBrokerFileStatus fstatus : fileStatuses) {
                            // 空文件校验：如果文件大小为 0 且属于二进制文件格式（如 Parquet 或 ORC），由于无法读取空二进制文件，会打印 Debug 日志并自动忽略（过滤掉）该空文件。
                            if (fstatus.getSize() == 0 && isBinaryFileFormat) {
                                // For parquet or orc file, if it is an empty file, ignore it.
                                // Because we can not read an empty parquet or orc file.
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug(new LogBuilder(LogKey.LOAD_JOB, callback.getCallbackId())
                                            .add("empty file", fstatus).build());
                                }
                            } else {
                            // 累加有效文件的大小（groupFileSize）和数量，将过滤后的文件状态加入列表中。
                                groupFileSize += fstatus.size;
                                filteredFileStatuses.add(fstatus);
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug(new LogBuilder(LogKey.LOAD_JOB, callback.getCallbackId())
                                            .add("file_status", fstatus).build());
                                }
                            }
                        }
                        fileStatusList.add(filteredFileStatuses);
                        tableTotalFileSize += groupFileSize;
                        tableTotalFileNum += filteredFileStatuses.size();
                        LOG.info("get {} files in file group {} for table {}. size: {}. job: {}, broker: {} ",
                                filteredFileStatuses.size(), groupNum, entry.getKey(), groupFileSize,
                                callback.getCallbackId(),
                                brokerDesc.getStorageType() == StorageBackend.StorageType.BROKER
                                        ? BrokerUtil.getAddress(brokerDesc) : brokerDesc.getStorageType());
                    } else {
                        LOG.info("no file found in file group {} for table {}, job: {}, broker: {}",
                                groupNum, entry.getKey(), callback.getCallbackId(),
                                brokerDesc.getStorageType() == StorageBackend.StorageType.BROKER
                                        ? BrokerUtil.getAddress(brokerDesc) : brokerDesc.getStorageType());
                        throw new UserException("No source files found in the specified paths: "
                                + fileGroup.getFilePaths());
                    }
                    groupNum++;
                }
            }
            // 挂载附件与更新作业信息
            totalFileSize += tableTotalFileSize;
            totalFileNum += tableTotalFileNum;
            ((BrokerPendingTaskAttachment) attachment).addFileStatus(aggKey, fileStatusList);
            LOG.info("get {} files to be loaded. total size: {}. cost: {} ms, job: {}",
                    tableTotalFileNum, tableTotalFileSize, (System.currentTimeMillis() - start),
                    callback.getCallbackId());
        }

        ((BrokerLoadJob) callback).setLoadFileInfo(totalFileNum, totalFileSize);
    }
}
