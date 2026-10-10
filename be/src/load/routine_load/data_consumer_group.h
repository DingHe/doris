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

#include <functional>
#include <memory>
#include <mutex>
#include <vector>

#include "common/cast_set.h"
#include "common/status.h"
#include "load/routine_load/data_consumer.h"
#include "util/blocking_queue.hpp"
#include "util/uid_util.h"
#include "util/work_thread_pool.hpp"

namespace RdKafka {
class Message;
} // namespace RdKafka

namespace doris {
class StreamLoadContext;

// data consumer group saves a group of data consumers.
// These data consumers share the same stream load pipe.
// This class is not thread safe.
// 核心职责是组织并协同多个数据消费者并发地从外部流式数据源（如 Kafka 的多个 Partition 或 Kinesis 的多个 Shard）拉取数据。
// 多线程并行消费：它内部持有一个专属的优先级线程池（PriorityThreadPool），能够让组内的每一个 DataConsumer 跑在独立的线程上，提升数据拉取的并发吞吐量。
// 共享管道与聚合：组内的所有数据消费者共享同一个流式导入管道（StreamLoadPipe），将拉取到的数据汇聚并写入管道中，供后续 Doris 执行引擎消费。
// 生命周期与计数协同：通过计数器和回调机制，协调组内所有消费者线程的退出与队列关闭。
class DataConsumerGroup {
public:
    typedef std::function<void(const Status&)> ConsumeFinishCallback;
    // 生成唯一的 _grp_id；初始化大小为 consumer_num 的优先级线程池 _thread_pool（最小线程数与最大线程数均设为 consumer_num，线程名为 "data_consumer"）；将计数器 _counter 初始化为 0。
    DataConsumerGroup(size_t consumer_num)
            : _grp_id(UniqueId::gen_uid()),
              _thread_pool(doris::cast_set<uint32_t>(consumer_num),
                           doris::cast_set<uint32_t>(consumer_num), "data_consumer"),
              _counter(0) {}

    virtual ~DataConsumerGroup() { _consumers.clear(); }

    const UniqueId& grp_id() { return _grp_id; }
    // 返回组内所有消费者的智能指针列表。
    const std::vector<std::shared_ptr<DataConsumer>>& consumers() { return _consumers; }
    // 向消费者组中添加一个新的数据消费者。
    // 将消费者的所属组设置为当前的 _grp_id，将其压入 _consumers 列表，并将计数器 _counter 自增 1。
    void add_consumer(std::shared_ptr<DataConsumer> consumer) {
        consumer->set_grp(_grp_id);
        _consumers.push_back(consumer);
        ++_counter;
    }

    // start all consumers
    // 用于启动组内的所有消费者开始拉取数据。基类中默认返回 Status::OK()，具体行为由子类（如 Kafka 或 Kinesis 消费者组）去重写实现。
    virtual Status start_all(std::shared_ptr<StreamLoadContext> ctx,
                             std::shared_ptr<io::StreamLoadPipe> pipe) {
        return Status::OK();
    }

protected:
    // Submit all consumers to thread pool.
    // consume_fn: wraps actual_consume per consumer.
    // shutdown_fn: called when last consumer finishes (shuts down queue).
    // Returns false if any submission fails.
    // 将所有消费者提交到内部线程池中运行。
    bool _submit_all_consumers(
            std::function<void(std::shared_ptr<DataConsumer>, ConsumeFinishCallback)> consume_fn,
            std::function<void()> shutdown_fn, Status& result_st);

    // Shared consumption loop skeleton. Calls _dequeue_and_process per iteration.
    // 共享的消费循环骨架。在多线程消费循环中不断调用子类实现的 _dequeue_and_process。
    Status _run_consume_loop(std::shared_ptr<StreamLoadContext> ctx,
                             std::shared_ptr<io::StreamLoadPipe> pipe, Status& result_st);

    // Dequeue one item and append to pipe. Update left_rows/left_bytes.
    // Returns false → queue empty/shutdown (eos). Returns true → continue.
    // 负责从子类的内部队列中取出一项数据并追加到 pipe 管道中，同时更新剩余行数/字节数限制。若返回 false 表示队列已空或关闭（到达流末尾 EOS）。
    virtual bool _dequeue_and_process(io::StreamLoadPipe* pipe, int64_t& left_rows,
                                      int64_t& left_bytes, Status& result_st) = 0;

    // Shutdown the subclass queue. Called at loop exit.
    // 在消费循环退出时关闭子类的内部数据队列。
    virtual void _shutdown_queue() = 0;

    // Called after successful finish. Override to collect post-consume state.
    // 在任务成功完成之后被调用，子类可以重写它来收集消费完成后的状态信息。
    virtual void _on_finish(std::shared_ptr<StreamLoadContext> ctx) {}
    // 消费者组的全局唯一标识符（通过 UniqueId::gen_uid() 生成），用于追踪和管理当前消费者组。
    UniqueId _grp_id;
    // 存储当前组内所有数据消费者对象的智能指针数组。
    std::vector<std::shared_ptr<DataConsumer>> _consumers;
    // thread pool to run each consumer in multi thread
    // 优先级线程池。
    // 用于在多线程环境下并发运行组内的每一个消费者任务（线程池命名为 "data_consumer"）。
    PriorityThreadPool _thread_pool;
    // mutex to protect counter.
    // the counter is init as the number of consumers.
    // once a consumer is done, decrease the counter.
    // when the counter becomes zero, shutdown the queue to finish
    // 互斥锁，专门用于保护计数器 _current / _counter 的线程安全。
    std::mutex _mutex;
    // 完成计数的计数器。初始值等于消费者的数量；每当一个消费者线程工作完成时，计数器递减。当计数器减到零时，触发队列关闭等收尾动作。
    int _counter;
};

// for kafka
class KafkaDataConsumerGroup : public DataConsumerGroup {
public:
    KafkaDataConsumerGroup(size_t consumer_num) : DataConsumerGroup(consumer_num), _queue(500) {}

    ~KafkaDataConsumerGroup() override;

    Status start_all(std::shared_ptr<StreamLoadContext> ctx,
                     std::shared_ptr<io::StreamLoadPipe> pipe) override;
    // assign topic partitions to all consumers equally
    Status assign_topic_partitions(std::shared_ptr<StreamLoadContext> ctx);

    // start a single consumer
    void actual_consume(std::shared_ptr<DataConsumer> consumer,
                        BlockingQueue<RdKafka::Message*>* queue, int64_t max_running_time_ms,
                        ConsumeFinishCallback cb);

private:
    bool _dequeue_and_process(io::StreamLoadPipe* pipe, int64_t& left_rows, int64_t& left_bytes,
                              Status& result_st) override;
    void _shutdown_queue() override { _queue.shutdown(); }
    void _on_finish(std::shared_ptr<StreamLoadContext> ctx) override;

    BlockingQueue<RdKafka::Message*> _queue;
    std::map<int32_t, int64_t> _cmt_offset;
    TFileFormatType::type _format;
};

// for kinesis
class KinesisDataConsumerGroup : public DataConsumerGroup {
public:
    KinesisDataConsumerGroup(size_t consumer_num) : DataConsumerGroup(consumer_num), _queue(500) {}

    ~KinesisDataConsumerGroup() override;

    Status start_all(std::shared_ptr<StreamLoadContext> ctx,
                     std::shared_ptr<io::StreamLoadPipe> pipe) override;

    Status assign_stream_shards(std::shared_ptr<StreamLoadContext> ctx);

private:
    void actual_consume(std::shared_ptr<DataConsumer> consumer,
                        BlockingQueue<std::shared_ptr<Aws::Kinesis::Model::Record>>* queue,
                        int64_t max_running_time_ms, ConsumeFinishCallback cb);

    bool _dequeue_and_process(io::StreamLoadPipe* pipe, int64_t& left_rows, int64_t& left_bytes,
                              Status& result_st) override;
    void _shutdown_queue() override { _queue.shutdown(); }
    void _on_finish(std::shared_ptr<StreamLoadContext> ctx) override;

    BlockingQueue<std::shared_ptr<Aws::Kinesis::Model::Record>> _queue;
    TFileFormatType::type _format;
};

} // end namespace doris
