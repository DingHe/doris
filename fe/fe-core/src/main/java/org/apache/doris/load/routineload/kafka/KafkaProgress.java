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

package org.apache.doris.load.routineload.kafka;

import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.load.routineload.LoadDataSourceType;
import org.apache.doris.load.routineload.RLTaskTxnCommitAttachment;
import org.apache.doris.load.routineload.RoutineLoadProgress;
import org.apache.doris.thrift.TKafkaRLTaskProgress;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * this is description of kafka routine load progress
 * the data before offset was already loaded in Doris
 */
// {"partitionIdToOffset": {}}
// Kafka 例行导入消费进度管理类
// 专门用于跟踪和管理 Kafka 例行导入（Routine Load）作业中各个 Kafka 分区（Partition）的消费位点（Offset）进度。
// 位点追踪与管理：在内存中维护 Kafka 分区 ID 到消费位点（Offset）的映射关系（partitionIdToOffset）。需要注意的是，这里保存的 Offset 通常是下一个需要被消费的消息位点。
// 进度更新与事务闭环：当一个导入任务（Task）成功提交事务时，根据返回的附加信息（RLTaskTxnCommitAttachment）自动推进各个分区的消费位点。
// 运维展示与 Lag 监控：提供可读的进度转换、JSON 序列化以及消费者延迟（Lag）计算方法，供 SHOW ROUTINE LOAD 等运维命令展示消费状态。
public class KafkaProgress extends RoutineLoadProgress {
    private static final Logger LOG = LogManager.getLogger(KafkaProgress.class);
    // 字符串常量，对应底层特殊位点起始值（-2），表示从 Kafka 分区最早的数据开始消费。
    public static final String OFFSET_BEGINNING = "OFFSET_BEGINNING"; // -2
    // 字符串常量，对应底层特殊位点结束值（-1），表示从 Kafka 分区最新的数据开始消费。
    public static final String OFFSET_END = "OFFSET_END"; // -1
    // OFFSET_ZERO is just for show info, if user specified offset is 0
    // 展示专用的字符串常量，当用户指定的初始位点为 0 时用于运维展示。
    public static final String OFFSET_ZERO = "OFFSET_ZERO";
    // 分别对应 -2 和 -1 的数值常量，用于底层逻辑判断。
    public static final long OFFSET_BEGINNING_VAL = -2;
    public static final long OFFSET_END_VAL = -1;

    // (partition id, begin offset)
    // the offset saved here is the next offset need to be consumed
    // 核心并发映射表。Key 为 Kafka 分区 ID（Integer），Value 为对应的消费位点 Offset（Long）。
    @SerializedName(value = "pito")
    private ConcurrentMap<Integer, Long> partitionIdToOffset = Maps.newConcurrentMap();
    // 公平可重入锁（ReentrantLock(true)），用于在云模式（Cloud Mode）下保障多线程并发更新进度时的线程安全性。
    private ReentrantLock lock = new ReentrantLock(true);

    public KafkaProgress() {
        super(LoadDataSourceType.KAFKA);
    }

    public KafkaProgress(TKafkaRLTaskProgress tKafkaRLTaskProgress) {
        super(LoadDataSourceType.KAFKA);
        this.partitionIdToOffset = new ConcurrentHashMap<>();
        partitionIdToOffset.putAll(tKafkaRLTaskProgress.getPartitionCmtOffset());
    }

    public KafkaProgress(Map<Integer, Long> partitionIdToOffset) {
        super(LoadDataSourceType.KAFKA);
        this.partitionIdToOffset = new ConcurrentHashMap<>();
        this.partitionIdToOffset.putAll(partitionIdToOffset);
    }

    public KafkaProgress(ConcurrentMap<Integer, Long> partitionIdToOffset) {
        super(LoadDataSourceType.KAFKA);
        this.partitionIdToOffset = partitionIdToOffset;
    }

    public ConcurrentMap<Integer, Long> getPartitionIdToOffset(List<Integer> partitionIds) {
        ConcurrentMap<Integer, Long> result = Maps.newConcurrentMap();
        for (ConcurrentMap.Entry<Integer, Long> entry : partitionIdToOffset.entrySet()) {
            for (Integer partitionId : partitionIds) {
                if (entry.getKey().equals(partitionId)) {
                    result.put(partitionId, entry.getValue());
                }
            }
        }
        return result;
    }

    public void addPartitionOffset(Pair<Integer, Long> partitionOffset) {
        partitionIdToOffset.put(partitionOffset.first, partitionOffset.second);
    }

    public Long getOffsetByPartition(int kafkaPartition) {
        return partitionIdToOffset.get(kafkaPartition);
    }

    public ConcurrentMap<Integer, Long> getOffsetByPartition() {
        return partitionIdToOffset;
    }

    public boolean containsPartition(Integer kafkaPartition) {
        return partitionIdToOffset.containsKey(kafkaPartition);
    }

    public boolean hasPartition() {
        return !partitionIdToOffset.isEmpty();
    }

    // (partition id, end offset)
    // OFFSET_ZERO: user set offset == 0, no committed msg
    // OFFSET_END: user set offset = OFFSET_END, no committed msg
    // OFFSET_BEGINNING: user set offset = OFFSET_BEGINNING, no committed msg
    // other: current committed msg's offset
    private void getReadableProgress(ConcurrentMap<Integer, String> showPartitionIdToOffset) {
        for (ConcurrentMap.Entry<Integer, Long> entry : partitionIdToOffset.entrySet()) {
            if (entry.getValue() == 0) {
                showPartitionIdToOffset.put(entry.getKey(), OFFSET_ZERO);
            } else if (entry.getValue() == -1) {
                showPartitionIdToOffset.put(entry.getKey(), OFFSET_END);
            } else if (entry.getValue() == -2) {
                showPartitionIdToOffset.put(entry.getKey(), OFFSET_BEGINNING);
            } else {
                // The offset saved in partitionIdToOffset is the next offset to be consumed.
                // So here we minus 1 to return the "already consumed" offset.
                showPartitionIdToOffset.put(entry.getKey(), "" + (entry.getValue() - 1));
            }
        }
    }

    public void checkPartitions(List<Pair<Integer, Long>> kafkaPartitionOffsets) throws DdlException {
        for (Pair<Integer, Long> pair : kafkaPartitionOffsets) {
            if (!partitionIdToOffset.containsKey(pair.first)) {
                throw new DdlException("The specified partition " + pair.first + " is not in the consumed partitions");
            }
        }
    }

    // modify the partition offset of this progress.
    // throw exception is the specified partition does not exist in progress.
    public void modifyOffset(List<Pair<Integer, Long>> kafkaPartitionOffsets) {
        for (Pair<Integer, Long> pair : kafkaPartitionOffsets) {
            partitionIdToOffset.put(pair.first, pair.second);
        }
    }

    public List<Pair<Integer, String>> getPartitionOffsetPairs(boolean alreadyConsumed) {
        List<Pair<Integer, String>> pairs = Lists.newArrayList();
        for (ConcurrentMap.Entry<Integer, Long> entry : partitionIdToOffset.entrySet()) {
            if (entry.getValue() == 0) {
                pairs.add(Pair.of(entry.getKey(), OFFSET_ZERO));
            } else if (entry.getValue() == -1) {
                pairs.add(Pair.of(entry.getKey(), OFFSET_END));
            } else if (entry.getValue() == -2) {
                pairs.add(Pair.of(entry.getKey(), OFFSET_BEGINNING));
            } else {
                long offset = entry.getValue();
                if (alreadyConsumed) {
                    offset -= 1;
                }
                pairs.add(Pair.of(entry.getKey(), "" + offset));
            }
        }
        return pairs;
    }

    // Get the lag of each kafka partition.
    // the `partitionIdWithLatestOffsets` is the cached latest offsets of each partition,
    // which is periodically updated as job is running.
    // The latest offset saved in `partitionIdWithLatestOffsets` is the next offset of the partition,
    // And offset saved in `partitionIdToOffset` is the next offset to be consumed.
    // For example, if a partition has 4 msg with offsets: 0,1,2,3
    // The latest offset is 4, and offset to be consumed is 2,
    // so the lag should be (4-2=)2.
    public Map<Integer, Long> getLag(Map<Integer, Long> partitionIdWithLatestOffsets) {
        Map<Integer, Long> lagMap = Maps.newHashMap();
        for (ConcurrentMap.Entry<Integer, Long> entry : partitionIdToOffset.entrySet()) {
            if (partitionIdWithLatestOffsets.containsKey(entry.getKey())) {
                long lag = partitionIdWithLatestOffsets.get(entry.getKey()) - entry.getValue();
                lagMap.put(entry.getKey(), lag);
            } else {
                lagMap.put(entry.getKey(), -1L);
            }
        }
        return lagMap;
    }

    @Override
    public String toString() {
        ConcurrentMap<Integer, String> showPartitionIdToOffset = Maps.newConcurrentMap();
        getReadableProgress(showPartitionIdToOffset);
        return "KafkaProgress [partitionIdToOffset="
                + Joiner.on("|").withKeyValueSeparator("_").join(showPartitionIdToOffset) + "]";
    }

    @Override
    public String toJsonString() {
        ConcurrentMap<Integer, String> showPartitionIdToOffset = Maps.newConcurrentMap();
        getReadableProgress(showPartitionIdToOffset);
        Gson gson = new Gson();
        return gson.toJson(showPartitionIdToOffset);
    }

    @Override
    public void update(RLTaskTxnCommitAttachment attachment) {
        KafkaProgress newProgress = (KafkaProgress) attachment.getProgress();

        // + 1 to point to the next msg offset to be consumed
        if (Config.isCloudMode()) {
            lock.lock();
            try {
                newProgress.partitionIdToOffset.forEach((partitionId, newOffset) -> {
                    this.partitionIdToOffset.compute(partitionId, (key, oldOffset) -> {
                        return (oldOffset == null || newOffset + 1 > oldOffset) ? newOffset + 1 : oldOffset;
                    });
                });
            } finally {
                lock.unlock();
            }
        } else {
            newProgress.partitionIdToOffset.entrySet().stream()
                    .forEach(entity -> this.partitionIdToOffset.put(entity.getKey(), entity.getValue() + 1));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("update kafka progress: {}, task: {}, job: {}",
                    newProgress.toJsonString(), DebugUtil.printId(attachment.getTaskId()), attachment.getJobId());
        }
    }

    public Long totalProgress() {
        return partitionIdToOffset.values().stream().reduce(0L, Long::sum);
    }
}
