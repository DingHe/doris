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

#include <glog/logging.h>

#include <cstdint>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include "common/cast_set.h"
#include "common/status.h"
#include "exec/operator/operator.h"
#include "runtime/runtime_profile.h"

namespace doris {
class PipelineFragmentContext;
class Pipeline;

using PipelinePtr = std::shared_ptr<Pipeline>;
using Pipelines = std::vector<PipelinePtr>;
using PipelineId = uint32_t;

// Pipeline 类是物理执行计划的核心抽象容器之一
// 在 Doris Pipeline 执行引擎中：
// 执行管道抽象：Pipeline 代表一条无阻塞的数据流管道（算子链），其物理结构由一个数据源算子（SourceOperator）、若干个中间处理算子（Operator）和一个数据接收算子（DataSinkOperator）按顺序串联而成。
// 并发与任务模板（Template）：Pipeline 扮演着“模板”的角色。一个 Pipeline 实例内部维护了一套共享的算子定义（_operators 与 _sink），并根据计算出的并发数（_num_tasks）实例化出多个具体物理执行的 PipelineTask（并发任务）。
// 拓扑与数据分布管理：维护 Pipeline 之间的父子层级关系（_children / _parents），记录当前管道的数据分布属性（_data_distribution），为物理执行引擎决定是否插入 LocalExchange（本地数据打散/重组）提供依据。
class Pipeline : public std::enable_shared_from_this<Pipeline> {
    friend class PipelineTask;
    friend class PipelineFragmentContext;

public:
    explicit Pipeline(PipelineId pipeline_id, int num_tasks, int num_tasks_of_parent)
            : _pipeline_id(pipeline_id),
              _num_tasks(num_tasks),
              _num_tasks_of_parent(num_tasks_of_parent) {
        _init_profile();
        _tasks.resize(_num_tasks, nullptr);
    }

    // Add operators for pipelineX
    // 向当前 Pipeline 的算子链末尾追加一个算子 op，并设置该算子的并行度参数 parallelism。
    Status add_operator(OperatorPtr& op, const int parallelism);
    // prepare operators for pipelineX
    // 预处理/初始化 Pipeline。
    // 遍历调用内部所有算子（_operators）以及 _sink 的 prepare 方法，初始化计算资源、编译表达式并配置共享状态。
    Status prepare(RuntimeState* state);
    // 将指定的 Sink 算子 sink_operator 绑定为当前 Pipeline 的数据输出终端 _sink。
    Status set_sink(DataSinkOperatorPtr& sink_operator);

    DataSinkOperatorXBase* sink() { return _sink.get(); }
    // 获取当前 Pipeline 内所有算子链的引用。
    Operators& operators() { return _operators; }
    DataSinkOperatorPtr sink_shared_pointer() { return _sink; }
    // 获取当前 Pipeline 最终输出数据的行描述符（RowDescriptor），即最后一个算子（_operators.back()）的行描述符。
    [[nodiscard]] const RowDescriptor& output_row_desc() const {
        return _operators.back()->row_desc();
    }

    [[nodiscard]] PipelineId id() const { return _pipeline_id; }
    // 判断指定的本地分区类型 idx 是否属于 Hash 数据交换（内部调用 is_shuffled_exchange(idx)）。
    static bool is_hash_exchange(TLocalPartitionType::type idx) {
        return is_shuffled_exchange(idx);
    }

    // For the hash-shuffle types (GLOBAL_EXECUTION_HASH_SHUFFLE, LOCAL_EXECUTION_HASH_SHUFFLE,
    // BUCKET_HASH_SHUFFLE) and ADAPTIVE_PASSTHROUGH, data is processed and shuffled on the sink.
    // Compared to PASSTHROUGH, this is a relatively heavy operation.
    // 判断指定的本地分区类型在 Sink 端是否属于高计算开销操作（例如 Hash Shuffle 类或自适应透传类型 ADAPTIVE_PASSTHROUGH），调度器据此决定是否需要额外的线程平衡。
    static bool heavy_operations_on_the_sink(TLocalPartitionType::type idx) {
        return is_shuffled_exchange(idx) || idx == TLocalPartitionType::ADAPTIVE_PASSTHROUGH;
    }
    // 根据当前 Pipeline 的数据分布与目标需求 target_data_distribution 及索引 idx，计算判断是否需要在物理执行时插入 Local Exchange 算子以重组数据。
    bool need_to_local_exchange(const DataDistribution target_data_distribution,
                                const int idx) const;
    // 初始化数据分布。
    // 以当前算子链首个算子（_operators.front()，即 Source 算子）所要求的输入数据分布作为当前 Pipeline 的初始数据分布。
    void init_data_distribution(RuntimeState* state) {
        set_data_distribution(_operators.front()->required_data_distribution(state));
    }
    void set_data_distribution(const DataDistribution& data_distribution) {
        _data_distribution = data_distribution;
    }
    const DataDistribution& data_distribution() const { return _data_distribution; }

    std::vector<std::shared_ptr<Pipeline>>& children() { return _children; }
    void set_children(std::shared_ptr<Pipeline> child) { _children.push_back(child); }
    void set_children(std::vector<std::shared_ptr<Pipeline>> children) {
        _children = std::move(children);
    }
    // 在某个 Task 被成功创建时调用。增加已创建计数 _num_tasks_created 和运行中计数 _num_tasks_running，并将指针记录在 _tasks[i] 中。
    void incr_created_tasks(int i, PipelineTask* task) {
        _num_tasks_created++;
        _num_tasks_running++;
        DCHECK_LT(i, _tasks.size());
        _tasks[i] = task;
    }
    // 唤醒当前 Pipeline 绑定的所有阻塞任务。当依赖的上游条件满足时，调用此方法将当前 Pipeline 中的所有 Task 标记为可运行（Runnable）状态并重新提交给调度器。
    void make_all_runnable(PipelineId wake_by);
    // 动态更新当前 Pipeline 的任务并发数 num_tasks。
    void set_num_tasks(int num_tasks) {
        _num_tasks = num_tasks;
        _tasks.resize(_num_tasks, nullptr);
        for (auto& op : _operators) {
            op->set_parallel_tasks(_num_tasks);
        }

#ifndef NDEBUG
        if (num_tasks > 1 &&
            std::any_of(_operators.begin(), _operators.end(),
                        [&](OperatorPtr op) -> bool { return op->is_serial_operator(); })) {
            DCHECK(false) << debug_string();
        }
#endif
    }
    int num_tasks() const { return _num_tasks; }
    // 当一个 PipelineTask 执行完毕关闭时调用。原子减小运行中的任务计数 _num_tasks_running。
    // 若返回 true（表示最后一个活跃任务已关闭），说明整个 Pipeline 彻底执行完成。
    bool close_task() { return _num_tasks_running.fetch_sub(1) == 1; }

    std::string debug_string() const {
        fmt::memory_buffer debug_string_buffer;
        fmt::format_to(debug_string_buffer,
                       "Pipeline [id: {}, _num_tasks: {}, _num_tasks_created: {}]", _pipeline_id,
                       _num_tasks, _num_tasks_created);
        for (int i = 0; i < _operators.size(); i++) {
            fmt::format_to(debug_string_buffer, "\n{}", _operators[i]->debug_string(i));
        }
        fmt::format_to(debug_string_buffer, "\n{}",
                       _sink ? _sink->debug_string(cast_set<int>(_operators.size())) : "null");
        return fmt::to_string(debug_string_buffer);
    }

    int num_tasks_of_parent() const { return _num_tasks_of_parent; }
    std::string& name() { return _name; }

private:
    void _init_profile();
    // 保存依赖当前 Pipeline 的父级 Pipeline 列表（使用 std::weak_ptr 防止循环引用）。配对的 int 通常表示父管道中对应依赖节点的 ID。
    std::vector<std::pair<int, std::weak_ptr<Pipeline>>> _parents;
    // 保存当前 Pipeline 所强依赖的其他子/上游 Pipeline 列表（例如 Hash Join 的 Build 端 Pipeline 是 Probe 端 Pipeline 的依赖）。
    std::vector<std::pair<int, std::shared_ptr<Pipeline>>> _dependencies;
    // 存储当前 Pipeline 的子 Pipeline 集合（拓扑树中的下游/子节点）。
    std::vector<std::shared_ptr<Pipeline>> _children;
    // 当前 Pipeline 在所属 Fragment 内的唯一标识符（ID）。
    PipelineId _pipeline_id;

    // pipline id + operator names. init when:
    //  build_operators(), if pipeline;
    //  _build_pipelines() and _create_tree_helper(), if pipelineX.
    // Pipeline 的可读名称，通常由 Pipeline ID + 包含的算子名称列表 拼接而成，用于日志、调试以及 Profile 显示。
    std::string _name;
    // 当前 Pipeline 级别的性能指标收集器（Profile），记录管道层面的执行时间、并发度等统计信息。
    std::unique_ptr<RuntimeProfile> _pipeline_profile;

    // Operators for pipelineX. All pipeline tasks share operators from this.
    // [SourceOperator -> ... -> SinkOperator]
    // 按数据流向顺序排列的算子链集合，从 SourceOperator 开始到最后一个中间处理算子。同一 Pipeline 下的所有 PipelineTask 共享这套算子模板。
    Operators _operators;
    // 当前 Pipeline 的数据输出终端算子，负责将管道处理后的数据发送至跨节点网络、写入本地 Buffer 或传递给下游管道。
    DataSinkOperatorPtr _sink = nullptr;
    // 对象池，用于管理在 Pipeline 及其算子生命周期内动态创建的对象的内存释放。
    std::shared_ptr<ObjectPool> _obj_pool;

    // Input data distribution of this pipeline. We do local exchange when input data distribution
    // does not match the target data distribution.
    // 记录输入到当前 Pipeline 的数据分布特征（如 HASH_SHUFFLED、BUCKET_SHUFFLED、PASSTHROUGH 或 NOOP）。
    DataDistribution _data_distribution {TLocalPartitionType::NOOP};

    // How many tasks should be created ?
    // 当前 Pipeline 需要创建并并行执行的物理任务（PipelineTask）总数（即物理并发度）。
    int _num_tasks = 1;
    // How many tasks are already created?
    // 原子计数器，记录当前 Pipeline 已经成功实例化创建的 PipelineTask 数量。
    std::atomic<int> _num_tasks_created = 0;
    // How many tasks are already created and not finished?
    // 记录当前处于运行/活跃状态（尚未完全 Close）的 PipelineTask 数量。当该值降为 0 时表示当前 Pipeline 的所有物理任务执行完毕。
    std::atomic<int> _num_tasks_running = 0;
    // Tasks in this pipeline.
    // 保存由当前 Pipeline 实例化出的所有物理 PipelineTask 对象的原始指针数组，数组大小为 _num_tasks。
    std::vector<PipelineTask*> _tasks;
    // Parallelism of parent pipeline.
    // 父级 Pipeline 的并发任务数。用于在决定数据分发策略与 Local Exchange 比例时进行参考。
    const int _num_tasks_of_parent;
};
} // namespace doris
