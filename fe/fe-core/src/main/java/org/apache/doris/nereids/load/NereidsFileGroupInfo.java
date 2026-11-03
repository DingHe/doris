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

package org.apache.doris.nereids.load;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.scan.FederationBackendPolicy;
import org.apache.doris.datasource.scan.FileGroupInfo;
import org.apache.doris.datasource.scan.FilePartitionUtils;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TExternalScanRange;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanRange;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TUniqueKeyUpdateMode;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * NereidsFileGroupInfo
 */
// 用于管理和调度文件导入任务（File Group Metadata & Scan Range Planning）的核心辅助类
// 封装文件组（File Group）的元数据、文件状态列表（TBrokerFileStatus）、存储配置（BrokerDesc）以及目标表信息。
// 同时，它负责计算文件扫描的并发度（实例数）、判断文件是否可切分（Splittable）、以及为 Doris 的分布式扫描节点（Scan Node）切分并生成底层的扫描范围位置（TScanRangeLocations），
// 从而指导后端（BE）高效并行读取外部数据文件（如 HDFS、S3 或本地文件）。
public class NereidsFileGroupInfo {
    private static final Logger LOG = LogManager.getLogger(NereidsFileGroupInfo.class);

    /**
     * JobType
     */
    public enum JobType {
        // 批量导入作业（如 Broker Load）。
        BULK_LOAD,
        // 流式导入作业（Stream Load）。
        STREAM_LOAD
    }
    // 标识当前是批量导入还是流式导入。
    private FileGroupInfo.JobType jobType;
    // 当前导入任务的全局唯一标识。
    private TUniqueId loadId;
    // 所属的导入作业 ID。
    private long loadJobId;
    // 关联的分布式事务 ID。
    private long txnId;
    // 导入的目标表对象。
    private Table targetTable;
    // 外部存储系统的访问凭证与配置描述符。
    private BrokerDesc brokerDesc;
    // Nereids 格式的文件组描述，包含路径、列映射、过滤条件等。
    private NereidsBrokerFileGroup fileGroup;
    // 该文件组下所有源文件的状态列表（包含文件路径、大小、是否可分片等）。
    private List<TBrokerFileStatus> fileStatuses;
    // 添加的文件总数。
    private int filesAdded;
    // 是否开启严格模式。
    private boolean strictMode;
    // 导入任务的并发度。
    private int loadParallelism;
    // set by getFileStatusAndCalcInstance
    // 计算得出的文件扫描实例（Scanner 任务）数量（默认初始化为 1）。
    private int numInstances = 1;
    // 每个扫描实例平均处理的字节数（默认初始化为 0）。
    private long bytesPerInstance = 0;
    // used for stream load, FILE_LOCAL or FILE_STREAM
    // 文件的存储类型（如 HDFS、LOCAL、S3 等）。
    private TFileType fileType;
    // 隐藏列列表。
    private List<String> hiddenColumns = null;
    // 唯一键表的更新模式（默认为 UPSERT）。
    private TUniqueKeyUpdateMode uniqueKeyUpdateMode = TUniqueKeyUpdateMode.UPSERT;
    // Sequence 列的映射名称。
    private String sequenceMapCol = null;

    /**
     * for broker load
     */
    public NereidsFileGroupInfo(long loadJobId, long txnId, Table targetTable, BrokerDesc brokerDesc,
            NereidsBrokerFileGroup fileGroup, List<TBrokerFileStatus> fileStatuses, int filesAdded, boolean strictMode,
            int loadParallelism) {
        this.jobType = FileGroupInfo.JobType.BULK_LOAD;
        this.loadJobId = loadJobId;
        this.txnId = txnId;
        this.targetTable = targetTable;
        this.brokerDesc = brokerDesc;
        this.fileGroup = fileGroup;
        this.fileStatuses = fileStatuses;
        this.filesAdded = filesAdded;
        this.strictMode = strictMode;
        this.loadParallelism = loadParallelism;
        this.fileType = brokerDesc.getFileType();
    }

    /**
     * for stream load
     */
    public NereidsFileGroupInfo(TUniqueId loadId, long txnId, Table targetTable, BrokerDesc brokerDesc,
            NereidsBrokerFileGroup fileGroup, TBrokerFileStatus fileStatus, boolean strictMode,
            TFileType fileType, List<String> hiddenColumns, TUniqueKeyUpdateMode uniqueKeyUpdateMode,
            String sequenceMapCol) {
        this.jobType = FileGroupInfo.JobType.STREAM_LOAD;
        this.loadId = loadId;
        this.txnId = txnId;
        this.targetTable = targetTable;
        this.brokerDesc = brokerDesc;
        this.fileGroup = fileGroup;
        this.fileStatuses = Lists.newArrayList();
        this.fileStatuses.add(fileStatus);
        this.filesAdded = 1;
        this.strictMode = strictMode;
        this.fileType = fileType;
        this.hiddenColumns = hiddenColumns;
        this.uniqueKeyUpdateMode = uniqueKeyUpdateMode;
        this.sequenceMapCol = sequenceMapCol;
    }

    public Table getTargetTable() {
        return targetTable;
    }

    public BrokerDesc getBrokerDesc() {
        return brokerDesc;
    }

    public NereidsBrokerFileGroup getFileGroup() {
        return fileGroup;
    }

    public List<TBrokerFileStatus> getFileStatuses() {
        return fileStatuses;
    }

    public boolean isStrictMode() {
        return strictMode;
    }

    public int getLoadParallelism() {
        return loadParallelism;
    }

    public TFileType getFileType() {
        return fileType;
    }

    public String getExplainString(String prefix) {
        StringBuilder sb = new StringBuilder();
        sb.append("file scan\n");
        return sb.toString();
    }

    public List<String> getHiddenColumns() {
        return hiddenColumns;
    }

    public TUniqueKeyUpdateMode getUniqueKeyUpdateMode() {
        return uniqueKeyUpdateMode;
    }

    public boolean isFixedPartialUpdate() {
        return uniqueKeyUpdateMode == TUniqueKeyUpdateMode.UPDATE_FIXED_COLUMNS;
    }

    public boolean isFlexiblePartialUpdate() {
        return uniqueKeyUpdateMode == TUniqueKeyUpdateMode.UPDATE_FLEXIBLE_COLUMNS;
    }

    public String getSequenceMapCol() {
        return sequenceMapCol;
    }

    /**
     * getFileStatusAndCalcInstance
     */
    // 计算当前文件组需要切分多少个扫描实例（numInstances）以及每个实例负责的字节数（bytesPerInstance）
    public void getFileStatusAndCalcInstance(FederationBackendPolicy backendPolicy) throws UserException {
        // 若文件数为 0，则抛出异常。
        if (filesAdded == 0) {
            throw new UserException("No source file in this table(" + targetTable.getName() + ").");
        }
        // Bulk Load 分支：累加所有文件的总字节数（totalBytes）；
        // 根据配置项 min_bytes_per_broker_scanner 计算基础实例数；
        // 结合集群可用 BE 节点数与 loadParallelism 计算总并发度上限；并受控于最大并发配置 max_broker_concurrency；确保实例数至少为 1。
        if (jobType == FileGroupInfo.JobType.BULK_LOAD) {
            long totalBytes = 0;
            for (TBrokerFileStatus fileStatus : fileStatuses) {
                totalBytes += fileStatus.size;
            }
            numInstances = (int) (totalBytes / Config.min_bytes_per_broker_scanner);
            int totalLoadParallelism = loadParallelism * backendPolicy.numBackends();
            numInstances = Math.min(totalLoadParallelism, numInstances);
            numInstances = Math.min(numInstances, Config.max_broker_concurrency);
            numInstances = Math.max(1, numInstances);

            bytesPerInstance = totalBytes / numInstances + 1;
            if (bytesPerInstance > Config.max_bytes_per_broker_scanner) {
                throw new UserException(
                        "Scan bytes per file scanner exceed limit: " + Config.max_bytes_per_broker_scanner);
            }
        } else {
        // 流式导入无需切分，固定 numInstances = 1，bytesPerInstance = Long.MAX_VALUE。
            // stream load, not need to split
            numInstances = 1;
            bytesPerInstance = Long.MAX_VALUE;
        }
        LOG.info("number instance of file scan node is: {}, bytes per instance: {}", numInstances, bytesPerInstance);
    }

    /**
     * createScanRangeLocations
     */
    // 判断文件是否支持分片（Splittable），并分流调用对应的切分实现。
    // 遍历所有文件，检查其格式类型（如 CSV、JSON）以及压缩类型（如 PLAIN）。
    // 只有纯文本、未压缩且支持分片的 CSV 或 JSON 文件才会被认定为 isSplittable = true。
    // 随后根据该标志位分别调用 createScanRangeLocationsSplittable 或 createScanRangeLocationsUnsplittable。
    public void createScanRangeLocations(NereidsParamCreateContext context,
            FederationBackendPolicy backendPolicy,
            List<TScanRangeLocations> scanRangeLocations) throws UserException {
        // Currently, we do not support mixed file types (or compress types).
        // If any of the file is unsplittable, all files will be treated as unsplittable.
        boolean isSplittable = true;
        for (TBrokerFileStatus fileStatus : fileStatuses) {
            TFileFormatType formatType = formatType(context.fileGroup.getFileFormatProperties().getFormatName(),
                    fileStatus.path);
            TFileCompressType compressType = Util.getOrInferCompressType(
                    context.fileGroup.getFileFormatProperties().getCompressionType(),
                    fileStatus.path);
            // Now only support split plain text
            if (compressType == TFileCompressType.PLAIN
                    && ((formatType == TFileFormatType.FORMAT_CSV_PLAIN && fileStatus.isSplitable)
                            || formatType == TFileFormatType.FORMAT_JSON)) {
                // is splittable
            } else {
                isSplittable = false;
                break;
            }
        }

        if (isSplittable) {
            createScanRangeLocationsSplittable(context, backendPolicy, scanRangeLocations);
        } else {
            createScanRangeLocationsUnsplittable(context, backendPolicy, scanRangeLocations);
        }
    }

    /**
     * createScanRangeLocationsUnsplittable
     */
    // 针对不可分片的文件，将文件整体分配给不同的实例。
    // 获取所有文件的大小，调用静态方法 assignFilesToInstances 将文件均衡分配到多个组中；为每个组创建 TScanRangeLocations 并在其中填充每个文件的范围描述（TFileRangeDesc）。
    public void createScanRangeLocationsUnsplittable(NereidsParamCreateContext context,
            FederationBackendPolicy backendPolicy,
            List<TScanRangeLocations> scanRangeLocations)
            throws UserException {
        List<Long> fileSizes = fileStatuses.stream().map(x -> x.size).collect(Collectors.toList());
        List<List<Integer>> groups = assignFilesToInstances(fileSizes, numInstances);
        for (List<Integer> group : groups) {
            TScanRangeLocations locations = newLocations(context.params, brokerDesc, backendPolicy);
            for (int i : group) {
                TBrokerFileStatus fileStatus = fileStatuses.get(i);
                TFileFormatType formatType = formatType(context.fileGroup.getFileFormatProperties().getFormatName(),
                        fileStatus.path);
                context.params.setFormatType(formatType);
                TFileCompressType compressType = Util.getOrInferCompressType(
                        context.fileGroup.getFileFormatProperties().getCompressionType(),
                        fileStatus.path);
                context.params.setCompressType(compressType);
                FilePartitionUtils.ParsedColumnsFromPath columnsFromPath =
                        FilePartitionUtils.parseColumnsFromPathWithNullInfo(fileStatus.path,
                                context.fileGroup.getColumnNamesFromPath(), true, false);
                List<String> columnsFromPathKeys = context.fileGroup.getColumnNamesFromPath();
                TFileRangeDesc rangeDesc = createFileRangeDesc(0, fileStatus, fileStatus.size,
                        columnsFromPath.getValues(), columnsFromPathKeys, columnsFromPath.getIsNull());
                locations.getScanRange().getExtScanRange().getFileScanRange().addToRanges(rangeDesc);
            }
            scanRangeLocations.add(locations);
        }
    }

    /**
     * assignFilesToInstances
     */
    // 利用优先队列（PriorityQueue）实现文件大小的贪心均衡分配。
    // 按文件大小从大到小排序，每次将文件分配给当前累计字节数最小的实例组，确保各个实例负载均衡。
    public static List<List<Integer>> assignFilesToInstances(List<Long> fileSizes, int instances) {
        int n = Math.min(fileSizes.size(), instances);
        PriorityQueue<Pair<Long, List<Integer>>> pq = new PriorityQueue<>(n, Comparator.comparingLong(Pair::key));
        for (int i = 0; i < n; i++) {
            pq.add(Pair.of(0L, new ArrayList<>()));
        }
        List<Integer> index = IntStream.range(0, fileSizes.size()).boxed().collect(Collectors.toList());
        index.sort((i, j) -> Long.compare(fileSizes.get(j), fileSizes.get(i)));
        for (int i : index) {
            Pair<Long, List<Integer>> p = pq.poll();
            p.value().add(i);
            pq.add(Pair.of(p.key() + fileSizes.get(i), p.value()));
        }
        return pq.stream().map(Pair::value).collect(Collectors.toList());
    }

    /**
     * createScanRangeLocationsSplittable
     */
    // 针对可分片的文件（如大 CSV），按照 bytesPerInstance 将单个文件切分为多个连续的字节范围（Range）。
    // 遍历文件状态，计算剩余未分配的字节数；若超出 bytesPerInstance 则进行截断切分并生成一个 TFileRangeDesc 存入当前 Scan 范围，随后新建下一个 Location；否则将整个文件或剩余部分并入当前范围。
    public void createScanRangeLocationsSplittable(NereidsParamCreateContext context,
            FederationBackendPolicy backendPolicy,
            List<TScanRangeLocations> scanRangeLocations) throws UserException {

        TScanRangeLocations curLocations = newLocations(context.params, brokerDesc, backendPolicy);
        long curInstanceBytes = 0;
        long curFileOffset = 0;
        for (int i = 0; i < fileStatuses.size();) {
            TBrokerFileStatus fileStatus = fileStatuses.get(i);
            long leftBytes = fileStatus.size - curFileOffset;
            long tmpBytes = curInstanceBytes + leftBytes;
            // header_type
            TFileFormatType formatType = formatType(context.fileGroup.getFileFormatProperties().getFormatName(),
                    fileStatus.path);
            context.params.setFormatType(formatType);
            TFileCompressType compressType = Util.getOrInferCompressType(
                    context.fileGroup.getFileFormatProperties().getCompressionType(),
                    fileStatus.path);
            context.params.setCompressType(compressType);
            FilePartitionUtils.ParsedColumnsFromPath columnsFromPath =
                    FilePartitionUtils.parseColumnsFromPathWithNullInfo(fileStatus.path,
                            context.fileGroup.getColumnNamesFromPath(), true, false);
            List<String> columnsFromPathKeys = context.fileGroup.getColumnNamesFromPath();
            // Assign scan range locations only for broker load.
            // stream load has only one file, and no need to set multi scan ranges.
            if (tmpBytes > bytesPerInstance && jobType != FileGroupInfo.JobType.STREAM_LOAD) {
                long rangeBytes = bytesPerInstance - curInstanceBytes;
                TFileRangeDesc rangeDesc = createFileRangeDesc(curFileOffset, fileStatus, rangeBytes,
                        columnsFromPath.getValues(), columnsFromPathKeys, columnsFromPath.getIsNull());
                curLocations.getScanRange().getExtScanRange().getFileScanRange().addToRanges(rangeDesc);
                curFileOffset += rangeBytes;

                // New one scan
                scanRangeLocations.add(curLocations);
                curLocations = newLocations(context.params, brokerDesc, backendPolicy);
                curInstanceBytes = 0;
            } else {
                TFileRangeDesc rangeDesc = createFileRangeDesc(curFileOffset, fileStatus, leftBytes,
                        columnsFromPath.getValues(), columnsFromPathKeys, columnsFromPath.getIsNull());
                curLocations.getScanRange().getExtScanRange().getFileScanRange().addToRanges(rangeDesc);
                curFileOffset = 0;
                curInstanceBytes += leftBytes;
                i++;
            }
        }

        // Put the last file
        if (curLocations.getScanRange().getExtScanRange().getFileScanRange().isSetRanges()) {
            scanRangeLocations.add(curLocations);
        }
    }
    // 为扫描范围指定具体的执行后端（BE）和 Broker 地址。
    // 从 backendPolicy 获取下一个可用的后端 BE；初始化 TFileScanRange，若为 Broker 存储则从 Broker 管理器获取对应的 Host/Port 并加入参数中；
    // 构建外部扫描范围（TExternalScanRange）与位置信息（TScanRangeLocation），如果是 Bulk Load 则绑定选中的 BE 节点 ID 与网络地址。
    protected TScanRangeLocations newLocations(TFileScanRangeParams params, BrokerDesc brokerDesc,
            FederationBackendPolicy backendPolicy) throws UserException {

        Backend selectedBackend = backendPolicy.getNextBe();

        // Generate one file scan range
        TFileScanRange fileScanRange = new TFileScanRange();

        if (brokerDesc.getStorageType() == StorageBackend.StorageType.BROKER) {
            FsBroker broker = null;
            try {
                broker = Env.getCurrentEnv().getBrokerMgr().getBroker(brokerDesc.getName(), selectedBackend.getHost());
            } catch (AnalysisException e) {
                throw new UserException(e.getMessage());
            }
            params.addToBrokerAddresses(new TNetworkAddress(broker.host, broker.port));
        } else {
            params.setBrokerAddresses(new ArrayList<>());
        }
        fileScanRange.setParams(params);

        // Scan range
        TExternalScanRange externalScanRange = new TExternalScanRange();
        externalScanRange.setFileScanRange(fileScanRange);
        TScanRange scanRange = new TScanRange();
        scanRange.setExtScanRange(externalScanRange);

        // Locations
        TScanRangeLocations locations = new TScanRangeLocations();
        locations.setScanRange(scanRange);

        if (jobType == FileGroupInfo.JobType.BULK_LOAD) {
            TScanRangeLocation location = new TScanRangeLocation();
            location.setBackendId(selectedBackend.getId());
            location.setServer(new TNetworkAddress(selectedBackend.getHost(), selectedBackend.getBePort()));
            locations.addToLocations(location);
        } else {
            // stream load do not need locations
            locations.setLocations(Lists.newArrayList());
        }

        return locations;
    }

    private TFileFormatType formatType(String fileFormat, String path) throws UserException {
        if (fileFormat == null) {
            // get file format by the file path
            return Util.getFileFormatTypeFromPath(path);
        }
        TFileFormatType formatType = Util.getFileFormatTypeFromName(fileFormat);
        if (formatType == TFileFormatType.FORMAT_UNKNOWN) {
            throw new UserException("Not supported file format: " + fileFormat);
        }
        return formatType;
    }
    // 构建底层的 Thrift 文件范围描述符（TFileRangeDesc），供 BE 读取时使用。
    // Bulk Load：设置文件路径、起始偏移量（startOffset）、切片大小（size）、总文件大小、路径隐式列（ColumnsFromPath）及其 Null 信息；若为 HDFS 文件，利用 LocationPath 解析并设置 FsName（scheme://authority 形式）。
    private TFileRangeDesc createFileRangeDesc(long curFileOffset, TBrokerFileStatus fileStatus, long rangeBytes,
            List<String> columnsFromPath, List<String> columnsFromPathKeys, List<Boolean> columnsFromPathIsNull) {
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        if (jobType == FileGroupInfo.JobType.BULK_LOAD) {
            rangeDesc.setPath(fileStatus.path);
            rangeDesc.setStartOffset(curFileOffset);
            rangeDesc.setSize(rangeBytes);
            rangeDesc.setFileSize(fileStatus.size);
            rangeDesc.setColumnsFromPath(columnsFromPath);
            rangeDesc.setColumnsFromPathKeys(columnsFromPathKeys);
            rangeDesc.setColumnsFromPathIsNull(columnsFromPathIsNull);
            if (getFileType() == TFileType.FILE_HDFS) {
                // LocationPath parses scheme and authority into fsIdentifier in exactly this
                // "scheme://authority" shape, without pulling in hadoop.
                rangeDesc.setFsName(LocationPath.of(fileStatus.path).getFsIdentifier());
            }
        } else {
            // for stream load
            if (getFileType() == TFileType.FILE_LOCAL) {
                // when loading parquet via stream, there will be a local file saved on BE
                // so to read it as a local file.
                Preconditions.checkState(fileGroup.getFilePaths().size() == 1);
                rangeDesc.setPath(fileGroup.getFilePaths().get(0));
                rangeDesc.setStartOffset(0);
            }
            rangeDesc.setLoadId(loadId);
            rangeDesc.setSize(fileStatus.size);
            rangeDesc.setFileSize(fileStatus.size);
        }
        rangeDesc.setModificationTime(fileStatus.getModificationTime());
        return rangeDesc;
    }
}
