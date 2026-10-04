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

#include "exec/pipeline/pipeline.h"

#include <memory>
#include <string>
#include <utility>

#include "exec/exchange/local_exchange_source_operator.h"
#include "exec/operator/operator.h"
#include "exec/pipeline/pipeline_fragment_context.h"
#include "exec/pipeline/pipeline_task.h"

namespace doris {

void Pipeline::_init_profile() {
    auto s = fmt::format("Pipeline (pipeline id={})", _pipeline_id);
    _pipeline_profile = std::make_unique<RuntimeProfile>(std::move(s));
}
// 判断物理执行计划中是否需要在当前位置（第 idx 个算子之后）插入本地数据交换（Local Exchange，如 Local Hash/Passthrough/Bucket Shuffle）的核心决策函数。
// 通过 Local Exchange，Doris 可以调整线程间的并行度或按 key 重新打散数据，从而充分利用多核 CPU 资源或满足下步计算的物理数据分布要求。
// const DataDistribution target_data_distribution  指定下游算子/节点要求或期望的目标数据分布属性（包含分布类型 distribution_type 如 HASH_SHUFFLED、PASSTHROUGH、NOOP 等，以及是否需要做 Local Exchange 的标识位 need_local_exchange()）。
// const int idx  当前正在评估的算子在 _operators 算子链中的下标索引。该函数用于决策是否在第 idx 个算子之后插入 Local Exchange。
bool Pipeline::need_to_local_exchange(const DataDistribution target_data_distribution,
                                      const int idx) const {
    // 如果目标分布本身明确声明了不需要插入本地数据交换（need_local_exchange() 为 false），则直接返回 false，跳过后续所有复杂的判断。
    if (!target_data_distribution.need_local_exchange()) {
        return false;
    }
    // If serial operator exists after `idx`-th operator, we should not improve parallelism.
    // 串行算子（Serial Operator）拦截与并发提升规则
    // 检查在位置 idx 之后（包含第 idx 个算子及后续算子）是否存在串行算子（单线程算子）。
    // 如果在第 idx 个算子之后存在必须单线程执行的串行算子，此时通过插入 Local Exchange 来提升并行度是毫无意义的（因为后续依然会被串行算子打回单线程），因此直接返回 false。
    if (std::any_of(_operators.begin() + idx, _operators.end(),
                    [&](OperatorPtr op) -> bool { return op->is_serial_operator(); })) {
        return false;
    }
    // If all operators are serial and sink is not serial, we should improve parallelism for sink.
    // 处理管道内包含串行算子时的特例并发提升策略：
    // 分支 1（全串行算子但 Sink 可并行）：如果当前管道内所有算子（_operators）都是串行算子，但末端的 _sink 是支持多线程并行的（!_sink->is_serial_operator()），返回 true。通过在 Sink 前插入 Local Exchange，可以将前面单线程处理完的数据打散并发发送/写入给 Sink。
    if (std::all_of(_operators.begin(), _operators.end(),
                    [&](OperatorPtr op) -> bool { return op->is_serial_operator(); })) {
        if (!_sink->is_serial_operator()) {
            return true;
        }
    // 分支 2（部分串行算子，部分并行算子）：如果管道内既有串行算子也有非串行算子，说明管道中存在需要提升并发度的物理环节，返回 true 以触发 Local Exchange。
    } else if (std::any_of(_operators.begin(), _operators.end(),
                           [&](OperatorPtr op) -> bool { return op->is_serial_operator(); })) {
        // If non-serial operators exist, we should improve parallelism for those.
        return true;
    }
    // 上游 Local Exchange 类型的冲突与覆盖判断
    // 检查管道的 Source 算子（_operators.front()）是否已经是一个 LocalExchangeSourceOperatorX（即数据已经经过了一轮上游的 Local Exchange）
    // 如果下游目标分布要求 Hash Shuffle 类型的交换（is_hash_exchange(...)）；
    // 但上游已有的 Exchange 类型既不是空操作（NOOP），也不是 Hash 交换（例如上游做的是 PASSTHROUGH 轮询打散）；
    if (auto local_exchange_source =
                std::dynamic_pointer_cast<LocalExchangeSourceOperatorX>(_operators.front());
        local_exchange_source && is_hash_exchange(target_data_distribution.distribution_type)) {
        const auto source_exchange_type = local_exchange_source->exchange_type();
        if (source_exchange_type != TLocalPartitionType::NOOP &&
            !is_hash_exchange(source_exchange_type)) {
            return true;
        }
    }
    // 非 Hash 类型的强行交换 vs 全串行兜底校验
    // 如果下游要求的是非 Hash 类的交换（如 PASSTHROUGH 轮询透传/数据均匀打散），这类操作通常具有强制性（例如为了解除数据倾斜或重新分发），直接返回 true。
    if (!is_hash_exchange(target_data_distribution.distribution_type)) {
        // Always do local exchange if non-hash-partition exchanger is required.
        // For example, `PASSTHROUGH` exchanger is always required to distribute data evenly.
        return true;
    // 如果下游要求的是 Hash 交换，但 Source 算子是串行算子：
    // 因为从头到尾全是串行节点且要求 Hash 交换，此时再做 Local Exchange 没有性能收益，直接返回 false。
    } else if (_operators.front()->is_serial_operator()) {
        DCHECK(std::all_of(_operators.begin(), _operators.end(),
                           [&](OperatorPtr op) -> bool { return op->is_serial_operator(); }) &&
               _sink->is_serial_operator())
                << debug_string();
        // All operators and sink are serial in this path.
        return false;
    } else {
        // 判定条件 1：当前分布类型不等于目标分布类型
        // 判定条件 2（Hash 兼容性豁免）：如果当前分布与目标分布同时属于某种 Hash Exchange（例如一个是 BUCKET_HASH_SHUFFLE，一个是 LOCAL_EXECUTION_HASH_SHUFFLE），两者的 Key-Partition 语义兼容，不需要重打散，非运算 !(...) 也会将其判定为不需要 Exchange。
        return _data_distribution.distribution_type != target_data_distribution.distribution_type &&
               !(is_hash_exchange(_data_distribution.distribution_type) &&
                 is_hash_exchange(target_data_distribution.distribution_type));
    }
}
// 向当前 Pipeline 的算子链中添加一个新的算子（Operator），同时根据该算子的特性（是否为串行算子、是否为 Source 算子）动态调整 Pipeline 的物理并发度，并维护算子链正确的执行顺序（确保 Source 算子始终位于链头）。
// 在数据库执行引擎中，某些算子在物理语义上不能或者不需要多线程并发执行（例如单线程全局排序 MergeSort、无需分区的 Exchange/Gather 端、或者是特定的单线程流控算子）。这些算子会被标记为 is_serial_operator() == true。
Status Pipeline::add_operator(OperatorPtr& op, const int parallelism) {
    // 串行算子（Serial Operator）与并发度调整
    // 检查传入的算子是否是一个串行算子（Serial Operator）。
    // 如果是，且传入了有效的 parallelism 参数，则调用 set_num_tasks(parallelism) 强制重置当前 Pipeline 的物理并发任务数（通常将 parallelism 设为 1）。
    if (parallelism > 0 && op->is_serial_operator()) {
        set_num_tasks(parallelism);
    }
    op->set_parallel_tasks(num_tasks());
    // 将算子指针 op 追加到当前 Pipeline 的 _operators 算子链向量末尾。
    _operators.emplace_back(op);
    if (op->is_source()) {
        // 设计意图/为什么需要反转？：
        // 在 Doris 构建物理算子链（Pipeline Builder）的过程中，通常采用从下往上/从 downstream 到 upstream 的自底向上（或者从 Sink 到 Source）方式递归构建；
        // 因此，中间算子（如 Project、Filter）甚至 Sink 算子往往比 Source 算子先被加入到 _operators 中；
        // 当最后遍历到 Source 算子并将其加入向量末尾时，调用 std::reverse(...) 可以直接将整个向量反转，确保 _operators 最终严格满足 [SourceOperator -> Operator1 -> Operator2 -> ...] 的前向数据流顺序（即 _operators.front() 永远是数据起点 Source 算子）。
        std::reverse(_operators.begin(), _operators.end());
    }
    return Status::OK();
}

// 在 Doris Pipeline 架构中，算子的 prepare 通常会向后或向上传递状态/描述符。
// 若该算子在初始化过程中发生错误（如表达式编译失败、内存分配异常等），RETURN_IF_ERROR 会立即中断执行并向上层返回错误状态 Status。
Status Pipeline::prepare(RuntimeState* state) {
    // 获取算子链中的最后一个算子 _operators.back()（即紧邻 Data Sink 的 Operator），并调用其 prepare(state) 执行初始化。
    RETURN_IF_ERROR(_operators.back()->prepare(state));
    // 初始化 Sink 算子的相关资源（例如解析输出表达式、初始化网络传输 Buffer 或初始化本地数据打散/Exchange 接收端）。同样，若发生错误会直接返回。
    RETURN_IF_ERROR(_sink->prepare(state));
    _name.append(std::to_string(id()));
    _name.push_back('-');
    for (auto& op : _operators) {
        _name.append(std::to_string(op->node_id()));
        _name.append(op->get_name());
    }
    _name.push_back('-');
    _name.append(std::to_string(_sink->node_id()));
    _name.append(_sink->get_name());
    return Status::OK();
}

Status Pipeline::set_sink(DataSinkOperatorPtr& sink) {
    if (_sink) {
        return Status::InternalError("set sink twice");
    }
    if (!sink->is_sink()) {
        return Status::InternalError("should set a sink operator but {}", typeid(sink).name());
    }
    _sink = sink;
    return Status::OK();
}
// 用于异步唤醒当前管道旗下所有物理任务（PipelineTask）并取消其阻塞状态的关键函数。
// 当某个依赖条件解除（例如 Hash Join 的 Build 端哈希表构建完成、或者下游数据接收通道就绪）时，系统会调用此函数将对应的 PipelineTask 重置为可调度（Runnable）状态。
void Pipeline::make_all_runnable(PipelineId wake_by) {
    // 在单测或压力测试中，如果开启了 "Pipeline::make_all_runnable.sleep" 调试点，并且传入的 pipeline_id 参数匹配当前管道的 id()；
    // 用于模拟多线程并发唤醒、竞争条件（Race Condition）或长尾延迟场景，以测试 Pipeline 调度器在任务唤醒延迟时的稳定性与正确性。
    DBUG_EXECUTE_IF("Pipeline::make_all_runnable.sleep", {
        auto pipeline_id = DebugPoints::instance()->get_debug_param_or_default<int32_t>(
                "Pipeline::make_all_runnable.sleep", "pipeline_id", -1);
        if (pipeline_id == id()) {
            LOG(WARNING) << "Pipeline::make_all_runnable.sleep sleep 10s";
            sleep(10);
        }
    });
    // Sink 端目标计数递减校验
    if (_sink->count_down_destination()) {
        // 遍历并唤醒当前 Pipeline 的所有物理 Task
        for (auto* task : _tasks) {
            if (task) {
                task->set_wake_up_early(wake_by);
                task->unblock_all_dependencies();
            }
        }
    }
}

} // namespace doris
