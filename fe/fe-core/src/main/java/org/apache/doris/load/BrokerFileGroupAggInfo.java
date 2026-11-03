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

package org.apache.doris.load;

import org.apache.doris.common.DdlException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * This class is mainly used to aggregate information of multiple DataDescriptors.
 * When the table name and specified partitions in the two DataDescriptors are same,
 * the BrokerFileGroup information corresponding to the two DataDescriptors will be aggregated together.
 * eg1：
 *
 *  DATA INFILE("hdfs://hdfs_host:hdfs_port/input/file1")
 *  INTO TABLE `tbl1`
 *  PARTITION (p1, p2)
 *
 *  and
 *
 *  DATA INFILE("hdfs://hdfs_host:hdfs_port/input/file2")
 *  INTO TABLE `tbl1`
 *  PARTITION (p1, p2)
 *
 *  will be aggregated together, because they have same table name and specified partitions
 *  =>
 *  FileGroupAggKey(tbl1, [p1, p2]) => List(file1, file2);
 *
 * eg2:
 *
 *  DATA INFILE("hdfs://hdfs_host:hdfs_port/input/file1")
 *  INTO TABLE `tbl1`
 *  PARTITION (p1)
 *
 *  and
 *
 *  DATA INFILE("hdfs://hdfs_host:hdfs_port/input/file2")
 *  INTO TABLE `tbl1`
 *  PARTITION (p2)
 *
 *  will NOT be aggregated together, because they have same table name but different specified partitions
 *  FileGroupAggKey(tbl1, [p1]) => List(file1);
 *  FileGroupAggKey(tbl1, [p2]) => List(file2);
 *
 * eg3:
 *
 *  DATA INFILE("hdfs://hdfs_host:hdfs_port/input/file1")
 *  INTO TABLE `tbl1`
 *  PARTITION (p1, p2)
 *
 *  and
 *
 *  DATA INFILE("hdfs://hdfs_host:hdfs_port/input/file2")
 *  INTO TABLE `tbl1`
 *  PARTITION (p2, p3)
 *
 *  will throw an Exception, because there is an overlap partition(p2) between 2 data descriptions. And we
 *  currently not allow this. You can equal the data descriptions like this:
 *
 *  DATA INFILE("hdfs://hdfs_host:hdfs_port/input/file1")
 *  INTO TABLE `tbl1`
 *  PARTITION (p1)
 *
 *  and
 *
 *  DATA INFILE("hdfs://hdfs_host:hdfs_port/input/file2")
 *  INTO TABLE `tbl1`
 *  PARTITION (p3)
 *
 *  and
 *
 *  DATA INFILE("hdfs://hdfs_host:hdfs_port/input/file1")
 *  INTO TABLE `tbl1`
 *  PARTITION (p2)
 *
 *  and
 *
 *  DATA INFILE("hdfs://hdfs_host:hdfs_port/input/file2")
 *  INTO TABLE `tbl1`
 *  PARTITION (p2)
 *
 *  they will be aggregate like:
 *  FileGroupAggKey(tbl1, [p1]) => List(file1);
 *  FileGroupAggKey(tbl1, [p3]) => List(file2);
 *  FileGroupAggKey(tbl1, [p2]) => List(file1, file2);
 *
 *  Although this transformation can be done automatically by system, but it change the "max_filter_ratio".
 *  So we have to let user decide what to do.
 */
// 用于聚合多个数据描述（Data Descriptions）及文件组（BrokerFileGroup）的核心辅助类
// 当用户在一条导入 SQL 中通过多组 DATA INFILE ... INTO TABLE ... PARTITION (...) 指定多个数据源时，该类负责将针对相同目标表和相同分区的数据源聚合在一起，并进行严格的分区重叠校验（Overlap Check），以防止数据冲突。
// 多数据源聚合：根据目标表 ID 和指定的分区 ID 组合成 FileGroupAggKey，将相同表、相同分区的数据源文件归类到同一个列表中（例如：向同一张表的同一分区导入多个不同文件路径的数据时，系统会将其合并处理）。
// 分区重叠拦截与校验：杜绝同一导入任务中对同一张表的同一个分区进行重复或重叠的数据追加，若发现分区有交叉重叠，则直接抛出 DdlException 异常，避免因自动拆分改变用户预期的最大过滤比例（max_filter_ratio）。
public class BrokerFileGroupAggInfo {
    private static final Logger LOG = LogManager.getLogger(BrokerFileGroupAggInfo.class);
    // 核心聚合映射表。
    // 以 FileGroupAggKey（表 ID + 分区 ID 集合）为 Key，
    // 以对应的 BrokerFileGroup 列表为 Value，存储聚类后的文件组信息。
    private Map<FileGroupAggKey, List<BrokerFileGroup>> aggKeyToFileGroups = Maps.newHashMap();
    // auxiliary structure, tbl id -> set of partition ids.
    // used to exam the overlapping partitions of same table.
    // 辅助索引结构（表 ID -》 该表已指定的所有分区 ID 集合）。
    // 专门用于高效检测同一张表在不同数据描述中是否存在分区重叠（Overlapping Partitions）。
    private Map<Long, Set<Long>> tableIdToPartitionIds = Maps.newHashMap();

    // this inner class This class is used to distinguish different combinations of table and partitions
    public static class FileGroupAggKey {
        private long tableId;
        private Set<Long> partitionIds; // empty means partition is not specified

        public FileGroupAggKey(long tableId, List<Long> partitionIds) {
            this.tableId = tableId;
            if (partitionIds != null) {
                this.partitionIds = Sets.newHashSet(partitionIds);
            } else {
                this.partitionIds = Sets.newHashSet();
            }
        }

        public long getTableId() {
            return tableId;
        }

        public Set<Long> getPartitionIds() {
            return partitionIds;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof FileGroupAggKey)) {
                return false;
            }

            FileGroupAggKey other = (FileGroupAggKey) obj;
            return other.tableId == this.tableId && other.partitionIds.equals(this.partitionIds);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tableId, partitionIds);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("[").append(tableId).append(": ").append(partitionIds).append("]");
            return sb.toString();
        }
    }
    // 核心添加与重叠校验方法。
    // 将一个新的 BrokerFileGroup 加入聚合器中，并在添加前执行严格的分区冲突检查。
    public void addFileGroup(BrokerFileGroup fileGroup) throws DdlException {
        // 根据传入的 fileGroup 构建对应的 FileGroupAggKey。
        FileGroupAggKey fileGroupAggKey = new FileGroupAggKey(fileGroup.getTableId(), fileGroup.getPartitionIds());
        List<BrokerFileGroup> fileGroupList = aggKeyToFileGroups.get(fileGroupAggKey);
        // check if there are overlapping of table with partitions and table without partitions of same table
        // 若当前 fileGroup 未指定分区（即 partitionIds == null），会检查已有聚合 Key 中是否包含同一张表但指定了特定分区的项
        if (fileGroup.getPartitionIds() == null) {
            for (FileGroupAggKey groupAggKey : aggKeyToFileGroups.keySet()) {
                if (groupAggKey.tableId == fileGroup.getTableId() && !groupAggKey.partitionIds.isEmpty()) {
                    throw new DdlException("There are overlapping partitions of same table"
                            + " in data description of load job stmt");
                }
            }
        } else {
            if (aggKeyToFileGroups.containsKey(new FileGroupAggKey(fileGroup.getTableId(), null))) {
                throw new DdlException("There are overlapping partitions of same table"
                        + " in data description of load job stmt");
            }
        }
        if (fileGroupList == null) {
            // check if there are overlapping partitions of same table
            if (tableIdToPartitionIds.containsKey(fileGroup.getTableId())
                    && tableIdToPartitionIds.get(fileGroup.getTableId()).stream()
                    .anyMatch(id -> fileGroup.getPartitionIds().contains(id))) {
                throw new DdlException("There are overlapping partitions of same table"
                        + " in data description of load job stmt");
            }

            fileGroupList = Lists.newArrayList();
            aggKeyToFileGroups.put(fileGroupAggKey, fileGroupList);
        }
        // exist, aggregate them
        fileGroupList.add(fileGroup);

        // update tableIdToPartitionIds
        Set<Long> partitionIds = tableIdToPartitionIds.get(fileGroup.getTableId());
        if (partitionIds == null) {
            partitionIds = Sets.newHashSet();
            tableIdToPartitionIds.put(fileGroup.getTableId(), partitionIds);
        }
        if (fileGroup.getPartitionIds() != null) {
            partitionIds.addAll(fileGroup.getPartitionIds());
        }
    }

    public Set<Long> getAllTableIds() {
        return aggKeyToFileGroups.keySet().stream().map(k -> k.tableId).collect(Collectors.toSet());
    }

    public Map<FileGroupAggKey, List<BrokerFileGroup>> getAggKeyToFileGroups() {
        return aggKeyToFileGroups;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(aggKeyToFileGroups);
        return sb.toString();
    }
}
