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
#include "load/routine_load/data_consumer_group.h"

#include <gen_cpp/PlanNodes_types.h>
#include <stddef.h>

#include <map>
#include <ostream>
#include <utility>

#include "common/logging.h"
#include "io/fs/kafka_consumer_pipe.h"
#include "io/fs/kinesis_consumer_pipe.h"
#include "librdkafka/rdkafkacpp.h"
#include "load/routine_load/consumer_group_helpers.h"
#include "load/routine_load/data_consumer.h"
#include "load/stream_load/stream_load_context.h"
#include "util/stopwatch.hpp"

namespace doris {

bool DataConsumerGroup::_submit_all_consumers(
        std::function<void(std::shared_ptr<DataConsumer>, ConsumeFinishCallback)> consume_fn,
        std::function<void()> shutdown_fn, Status& result_st) {
    // 遍历当前消费者组（_consumers 向量）中管理的所有数据消费者实例。
    for (auto& consumer : _consumers) {
        auto cb = [this, shutdown_fn, &result_st](const Status& st) {
            std::unique_lock<std::mutex> lock(_mutex);
            // 将活跃计数器 _counter 自减 1。如果减到 0，说明组内的最后一个消费者也已经完成了工作。
            if (--_counter == 0) {
                // 触发传入的关闭函数，关闭底层的消费队列。
                shutdown_fn();
                LOG(INFO) << "all consumers finished, shutdown queue. grp: " << _grp_id;
            }
            if (result_st.ok() && !st.ok()) {
                result_st = st;
            }
        };
        // 将消费任务投递到线程池中异步执行。
        if (!_thread_pool.offer([consume_fn, consumer, cb] { consume_fn(consumer, cb); })) {
            LOG(WARNING) << "failed to submit consumer: " << consumer->id() << ", grp: " << _grp_id;
            return false;
        }
        VLOG_CRITICAL << "submit consumer: " << consumer->id() << ", grp: " << _grp_id;
    }
    return true;
}

Status DataConsumerGroup::_run_consume_loop(std::shared_ptr<StreamLoadContext> ctx,
                                            std::shared_ptr<io::StreamLoadPipe> pipe,
                                            Status& result_st) {
    // 将最大时间间隔（秒）转换为毫秒，作为剩余允许运行的时间。
    int64_t left_time = ctx->max_interval_s * 1000;
    // 初始化剩余允许消费的行数上限（max_batch_rows）。
    int64_t left_rows = ctx->max_batch_rows;
    // 初始化剩余允许消费的字节数上限（max_batch_size）。
    int64_t left_bytes = ctx->max_batch_size;

    LOG(INFO) << "start consumer group: " << _grp_id << ". max time(ms): " << left_time
              << ", batch rows: " << left_rows << ", batch size: " << left_bytes << ". "
              << ctx->brief();
    // 单调计时器 watch，用来精准监控当前消费批次的实际总耗时。
    MonotonicStopWatch watch;
    watch.start();
    bool eos = false;
    while (true) {
        // 每次循环开始时，检查是否满足批次退出的 4 大终结条件之一
        // eos：数据流已经读到末尾（如分区 EOF）。
        // left_time <= 0：批次运行时间超时。
        // left_rows <= 0：累计消费行数达到上限。
        // left_bytes <= 0：累计消费字节数达到上限。
        if (eos || left_time <= 0 || left_rows <= 0 || left_bytes <= 0) {
            LOG(INFO) << "consumer group done: " << _grp_id
                      << ". consume time(ms)=" << ctx->max_interval_s * 1000 - left_time
                      << ", received rows=" << ctx->max_batch_rows - left_rows
                      << ", received bytes=" << ctx->max_batch_size - left_bytes << ", eos: " << eos
                      << ", left_time: " << left_time << ", left_rows: " << left_rows
                      << ", left_bytes: " << left_bytes
                      << ", blocking get time(us): " << pipe->get_queue_size() << ", "
                      << ctx->brief();

            _shutdown_queue();
            for (auto& consumer : _consumers) {
                static_cast<void>(consumer->cancel(ctx));
            }
            _thread_pool.shutdown();
            _thread_pool.join();
            if (!result_st.ok()) {
                pipe->cancel(result_st.to_string());
                return result_st;
            }
            RETURN_IF_ERROR(pipe->finish());
            _on_finish(ctx);
            ctx->receive_bytes = ctx->max_batch_size - left_bytes;
            return Status::OK();
        }
        // 从内部队列中取出一个数据项，追加到管道 pipe 中，并内部扣减 left_rows（剩余行数）和 left_bytes（剩余字节数）。
        if (!_dequeue_and_process(pipe.get(), left_rows, left_bytes, result_st)) {
            eos = true;
        }
        // 更新剩余时间
        left_time = ctx->max_interval_s * 1000 - watch.elapsed_time() / 1000 / 1000;
    }
}

Status KafkaDataConsumerGroup::assign_topic_partitions(std::shared_ptr<StreamLoadContext> ctx) {
    DCHECK(ctx->kafka_info);
    DCHECK(_consumers.size() >= 1);

    // divide partitions using round-robin partitioner
    int consumer_size = doris::cast_set<int>(_consumers.size());
    auto divide_parts = WorkPartitioner<int32_t, int64_t>::partition_round_robin(
            ctx->kafka_info->begin_offset, consumer_size);

    // assign partitions to consumers equally
    for (int j = 0; j < consumer_size; ++j) {
        RETURN_IF_ERROR(
                std::static_pointer_cast<KafkaDataConsumer>(_consumers[j])
                        ->assign_topic_partitions(divide_parts[j], ctx->kafka_info->topic, ctx));
    }

    return Status::OK();
}

KafkaDataConsumerGroup::~KafkaDataConsumerGroup() {
    // clean the msgs left in queue
    _queue.shutdown();
    while (true) {
        RdKafka::Message* msg;
        if (_queue.blocking_get(&msg)) {
            delete msg;
            msg = nullptr;
        } else {
            break;
        }
    }
    DCHECK(_queue.get_size() == 0);
}

// std::shared_ptr<io::StreamLoadPipe> pipe  流式导入数据管道（Pipe）的智能指针。它是生产者-消费者模型中的下游通道，消费者组从 Kafka 拉取到的数据最终会写入该管道，供 Doris 的执行引擎读取解析。
Status KafkaDataConsumerGroup::start_all(std::shared_ptr<StreamLoadContext> ctx,
                                         std::shared_ptr<io::StreamLoadPipe> pipe) {
    DORIS_CHECK(std::dynamic_pointer_cast<io::KafkaConsumerPipe>(pipe) != nullptr);
    Status result_st = Status::OK();
    // （提交偏移量）保存到当前消费者组的成员变量 _cmt_offset 中。
    _cmt_offset = ctx->kafka_info->cmt_offset;
    // 将数据格式（如 CSV、JSON）保存到 _format 中。
    _format = ctx->format;
    // 把组内的所有消费者投递到优先级线程池中并发运行：
    if (!_submit_all_consumers(
    // 第一个 Lambda 参数（consume_fn）：定义了每个消费者要执行的具体逻辑。计算最大运行时间 max_time（将 ctx->max_interval_s 秒转换为毫秒），
    // 并调用 actual_consume(c, &_queue, max_time, cb) 让消费者 c 开始向内部阻塞队列 _queue 拉取消息。
    // 第二个 Lambda 参数（shutdown_fn）：当组内所有消费者都干完活退出时被调用，执行 _queue.shutdown() 关闭底层阻塞队列。
                [this, max_time = ctx->max_interval_s * 1000](std::shared_ptr<DataConsumer> c,
                                                              ConsumeFinishCallback cb) {
                    actual_consume(c, &_queue, max_time, cb);
                },
                [this] { _queue.shutdown(); }, result_st)) {
        return Status::InternalError("failed to submit data consumer");
    }
    // 运行主消费循环
    // 阻塞当前主线程，不断从内部队列 _queue 中取出 Kafka 消息并追加到 pipe 管道中，直到消费完成或出错。
    RETURN_IF_ERROR(_run_consume_loop(ctx, pipe, result_st));
    // 将消费过程中更新后的最新提交偏移量（_cmt_offset）通过 std::move 移交给上下文对象 ctx->kafka_info->cmt_offset，以便后续的事务提交和 Offset 提交逻辑使用。
    ctx->kafka_info->cmt_offset = std::move(_cmt_offset);
    return Status::OK();
}

bool KafkaDataConsumerGroup::_dequeue_and_process(io::StreamLoadPipe* pipe, int64_t& left_rows,
                                                  int64_t& left_bytes, Status& result_st) {
    RdKafka::Message* msg = nullptr;
    // 尝试限时（config::blocking_queue_cv_wait_timeout_ms）从队列中获取一条 Kafka 消息。
    if (!_queue.controlled_blocking_get(&msg, config::blocking_queue_cv_wait_timeout_ms)) {
        return false;
    }
    Defer delete_msg {[msg] { delete msg; }};
    VLOG_NOTICE << "get kafka message, partition: " << msg->partition()
                << ", offset: " << msg->offset() << ", len: " << msg->len();

    if (msg->err() == RdKafka::ERR__PARTITION_EOF) {
        if (msg->offset() > 0) {
            _cmt_offset[msg->partition()] = msg->offset() - 1;
        }
        return true;
    }
    // 根据数据格式将消息追加到管道
    auto append_fn = FormatAppender::get_append_function<io::StreamLoadPipe>(_format);
    Status st = (pipe->*append_fn)(static_cast<const char*>(msg->payload()),
                                   static_cast<size_t>(msg->len()));
    if (st.ok()) {
        left_rows--;
        left_bytes -= msg->len();
        _cmt_offset[msg->partition()] = msg->offset();
        VLOG_NOTICE << "consume partition[" << msg->partition() << " - " << msg->offset() << "]";
    } else {
        LOG(WARNING) << "failed to append msg to pipe. grp: " << _grp_id;
        std::unique_lock<std::mutex> lock(_mutex);
        if (result_st.ok()) {
            result_st = st;
        }
    }
    return true;
}

void KafkaDataConsumerGroup::_on_finish(std::shared_ptr<StreamLoadContext> ctx) {
    // cmt_offset is moved back in start_all after _run_consume_loop returns
}
// 传入共享的阻塞队列 queue 和最大允许运行时间 max_running_time_ms，开始循环从 Kafka 拉取消息，并接收返回的执行状态 st。
void KafkaDataConsumerGroup::actual_consume(std::shared_ptr<DataConsumer> consumer,
                                            BlockingQueue<RdKafka::Message*>* queue,
                                            int64_t max_running_time_ms, ConsumeFinishCallback cb) {
    Status st = std::static_pointer_cast<KafkaDataConsumer>(consumer)->group_consume(
            queue, max_running_time_ms);
    cb(st);
}

Status KinesisDataConsumerGroup::assign_stream_shards(std::shared_ptr<StreamLoadContext> ctx) {
    DCHECK(ctx->kinesis_info);
    DCHECK(_consumers.size() >= 1);

    // divide shards using round-robin partitioner
    int consumer_size = doris::cast_set<int>(_consumers.size());
    auto divide_shards = WorkPartitioner<std::string, std::string>::partition_round_robin(
            ctx->kinesis_info->begin_sequence_number, consumer_size);

    // assign shards to consumers equally
    for (int j = 0; j < consumer_size; ++j) {
        RETURN_IF_ERROR(std::static_pointer_cast<KinesisDataConsumer>(_consumers[j])
                                ->assign_shards(divide_shards[j], ctx->kinesis_info->stream, ctx));
    }

    return Status::OK();
}

KinesisDataConsumerGroup::~KinesisDataConsumerGroup() {
    _queue.shutdown();
    while (true) {
        std::shared_ptr<Aws::Kinesis::Model::Record> record;
        if (_queue.blocking_get(&record)) {
            record.reset();
        } else {
            break;
        }
    }
    DCHECK(_queue.get_size() == 0);
}

Status KinesisDataConsumerGroup::start_all(std::shared_ptr<StreamLoadContext> ctx,
                                           std::shared_ptr<io::StreamLoadPipe> pipe) {
    DORIS_CHECK(std::dynamic_pointer_cast<io::KinesisConsumerPipe>(pipe) != nullptr);
    Status result_st = Status::OK();
    _format = ctx->format;

    if (!_submit_all_consumers(
                [this, max_time = ctx->max_interval_s * 1000](std::shared_ptr<DataConsumer> c,
                                                              ConsumeFinishCallback cb) {
                    actual_consume(c, &_queue, max_time, cb);
                },
                [this] { _queue.shutdown(); }, result_st)) {
        return Status::InternalError("failed to submit kinesis data consumer");
    }
    return _run_consume_loop(ctx, pipe, result_st);
}

bool KinesisDataConsumerGroup::_dequeue_and_process(io::StreamLoadPipe* pipe, int64_t& left_rows,
                                                    int64_t& left_bytes, Status& result_st) {
    std::shared_ptr<Aws::Kinesis::Model::Record> record;
    if (!_queue.controlled_blocking_get(&record, config::blocking_queue_cv_wait_timeout_ms)) {
        return false;
    }
    auto& data = record->GetData();
    const char* payload = reinterpret_cast<const char*>(data.GetUnderlyingData());
    size_t len = data.GetLength();
    VLOG_NOTICE << "get kinesis record, seq: " << record->GetSequenceNumber() << ", len: " << len;

    auto append_fn = FormatAppender::get_append_function<io::StreamLoadPipe>(_format);
    Status st = (pipe->*append_fn)(payload, len);
    if (st.ok()) {
        left_rows--;
        left_bytes -= len;
        VLOG_NOTICE << "consume kinesis record [seq=" << record->GetSequenceNumber() << "]";
    } else {
        LOG(WARNING) << "failed to append kinesis record to pipe. grp: " << _grp_id;
        std::unique_lock<std::mutex> lock(_mutex);
        if (result_st.ok()) {
            result_st = st;
        }
    }
    return true;
}

void KinesisDataConsumerGroup::_on_finish(std::shared_ptr<StreamLoadContext> ctx) {
    for (auto& consumer : _consumers) {
        auto kinesis_consumer = std::static_pointer_cast<KinesisDataConsumer>(consumer);
        for (auto& [shard_id, seq_num] : kinesis_consumer->get_committed_sequence_numbers()) {
            ctx->kinesis_info->cmt_sequence_number[shard_id] = seq_num;
        }
        for (auto& [shard_id, millis] : kinesis_consumer->get_millis_behind_latest()) {
            auto [it, inserted] = ctx->kinesis_info->millis_behind_latest.emplace(shard_id, millis);
            if (!inserted && it->second < millis) {
                it->second = millis;
            }
        }
        for (auto& shard_id : kinesis_consumer->get_closed_shard_ids()) {
            ctx->kinesis_info->closed_shard_ids.insert(shard_id);
        }
    }
}

void KinesisDataConsumerGroup::actual_consume(
        std::shared_ptr<DataConsumer> consumer,
        BlockingQueue<std::shared_ptr<Aws::Kinesis::Model::Record>>* queue,
        int64_t max_running_time_ms, ConsumeFinishCallback cb) {
    Status st = std::static_pointer_cast<KinesisDataConsumer>(consumer)->group_consume(
            queue, max_running_time_ms);
    cb(st);
}

} // namespace doris
