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

#include <cstdint>

#include "common/logging.h"

namespace doris {

// Used to track the selectivity of runtime filters
// If the selectivity of a runtime filter is very low, it is considered ineffective and can be ignored
// Considering that the selectivity of runtime filters may change with data variations
// A dynamic selectivity tracking mechanism is needed
// Note: this is not a thread-safe class
// 在分布式查询执行过程中，Runtime Filter（如 IN / BloomFilter 条件）能显著减少存储层需要读取和计算的数据量。
// 然而，如果 BloomFilter 等过滤器的选择率极低（即过滤掉的行数很少，或者过滤率低于设定阈值），持续对每行数据执行谓词计算反而会浪费大量的 CPU 资源。
// 周期性采样评估：按照设定的采样频率（_sampling_frequency）收集一个周期内的输入行数和过滤行数。
// 计算选择率（过滤率）：评估当前 Runtime Filter 的实际过滤效果。
// 动态跳过（钝化/Disable）：如果在一个评估周期内，输入的行数达到了最小统计门槛（min_judge_input_rows），且过滤率（filter_rows / input_rows）低于设置的阈值（ignore_threshold），则将过滤器置为 always_true 状态。
class RuntimeFilterSelectivity {
public:
    RuntimeFilterSelectivity() = default;

    // If sampling_frequency is less than or equal to 0, the selectivity tracking will be disabled
    static constexpr int DISABLE_SAMPLING = -1;

    void set_sampling_frequency(int frequency) { _sampling_frequency = frequency; }

    void update_judge_counter() {
        if ((_judge_counter++) >= _sampling_frequency) {
            reset_judge_selectivity();
        }
    }

    void update_judge_selectivity(int filter_id, uint64_t filter_rows, uint64_t input_rows,
                                  double ignore_thredhold) {
        if (!_always_true) {
            _judge_filter_rows += filter_rows;
            _judge_input_rows += input_rows;
            _judge_selectivity(ignore_thredhold, _judge_filter_rows, _judge_input_rows,
                               _always_true);
        }

        VLOG_ROW << fmt::format(
                "Runtime filter[{}] selectivity update: filter_rows: {}, input_rows: {},  filter "
                "rate: {}, "
                "ignore_thredhold: {}, counter: {} , always_true: {}",
                filter_id, _judge_filter_rows, _judge_input_rows,
                static_cast<double>(_judge_filter_rows) / static_cast<double>(_judge_input_rows),
                ignore_thredhold, _judge_counter, _always_true);
    }

    bool maybe_always_true_can_ignore() const {
        /// TODO: maybe we can use session variable to control this behavior ?
        if (_sampling_frequency <= 0) {
            return false;
        } else {
            return _always_true;
        }
    }

    void reset_judge_selectivity() {
        _always_true = false;
        _judge_counter = 0;
        _judge_input_rows = 0;
        _judge_filter_rows = 0;
    }

private:
    void _judge_selectivity(double ignore_threshold, int64_t filter_rows, int64_t input_rows,
                            bool& always_true) {
        // if the judged input rows is too small, we think the selectivity is not reliable
        if (input_rows > min_judge_input_rows) {
            always_true = (static_cast<double>(filter_rows) / static_cast<double>(input_rows)) <
                          ignore_threshold;
        }
    }

    int64_t _judge_input_rows = 0;
    int64_t _judge_filter_rows = 0;
    int _judge_counter = 0;
    bool _always_true = false;
    int _sampling_frequency = -1;

    constexpr static int64_t min_judge_input_rows = 4096 * 10;
};

} // namespace doris
