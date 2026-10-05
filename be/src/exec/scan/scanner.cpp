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

#include "exec/scan/scanner.h"

#include <glog/logging.h>

#include "common/config.h"
#include "common/status.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_nothing.h"
#include "exec/operator/scan_operator.h"
#include "exec/scan/scan_node.h"
#include "exprs/vexpr_context.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_profile.h"
#include "util/concurrency_stats.h"
#include "util/defer_op.h"

namespace doris {

Scanner::Scanner(RuntimeState* state, ScanLocalStateBase* local_state, int64_t limit,
                 RuntimeProfile* profile)
        : _state(state),
          _local_state(local_state),
          _limit(limit),
          _profile(profile),
          _output_tuple_desc(_local_state->output_tuple_desc()),
          _output_row_descriptor(_local_state->_parent->output_row_descriptor()),
          _has_prepared(false) {
    _total_rf_num = cast_set<int>(_local_state->_helper.runtime_filter_nums());
    DorisMetrics::instance()->scanner_cnt->increment(1);
}
// 为当前 Scanner 实例绑定全局共享的 Limit 计数器，并深度克隆（Clone）上层算子下发的过滤谓词与投影表达式上下文。
// 由于多个 Scanner 会被调度到不同的工作线程中并发执行，直接共享表达式上下文对象会导致线程安全问题，因此必须为每个 Scanner 独立克隆一份专用的表达式实例（VExprContext）
// conjuncts (const VExprContextSPtrs&)： 上层 Scan 算子下发给当前 Scanner 的过滤谓词表达式集合（如 SQL 中的 WHERE a > 10 以及动态 Runtime Filter）。
Status Scanner::init(RuntimeState* state, const VExprContextSPtrs& conjuncts) {
    // All scanners share a remaining-limit counter so a LIMIT query can
    // stop once enough rows have been collected across scanners.
    // Key TopN scans have no ordinary scan LIMIT, so each scanner can
    // independently produce its full local top-N.
    // 从算子本地状态（ScanLocalState）中获取指向全局原子计数器 std::atomic<int64_t> 的指针。
    // 设计目的：在并发读取场景下（例如 SELECT * FROM table LIMIT 10），同一个 ScanNode 会创建多个并发执行的 Scanner。
    // 通过共享这个原子指针，任何一个 Scanner 读到了新数据，都可以原子递减剩余 Limit。一旦减到 0，所有并发 Scanner 都会感知并提前终止读取，避免无效的磁盘 I/O。
    // 对于 Key TopN 等无需普通 LIMIT 截断的场景，该指针可能为 nullptr。
    _shared_scan_limit = _local_state->shared_scan_limit_ptr();
    // 深克隆过滤谓词（Conjuncts）
    if (!conjuncts.empty()) {
        _conjuncts.resize(conjuncts.size());
        for (size_t i = 0; i != conjuncts.size(); ++i) {
            RETURN_IF_ERROR(conjuncts[i]->clone(state, _conjuncts[i]));
        }
    }
    // 深克隆最终投影表达式（Projections）
    const auto& projections = _local_state->_projections;
    if (!projections.empty()) {
        _projections.resize(projections.size());
        for (size_t i = 0; i != projections.size(); ++i) {
            RETURN_IF_ERROR(projections[i]->clone(state, _projections[i]));
        }
    }
    // 深克隆中间过程投影表达式（Intermediate Projections）
    const auto& intermediate_projections = _local_state->_intermediate_projections;
    if (!intermediate_projections.empty()) {
        _intermediate_projections.resize(intermediate_projections.size());
        for (int i = 0; i < intermediate_projections.size(); i++) {
            _intermediate_projections[i].resize(intermediate_projections[i].size());
            for (int j = 0; j < intermediate_projections[i].size(); j++) {
                RETURN_IF_ERROR(intermediate_projections[i][j]->clone(
                        state, _intermediate_projections[i][j]));
            }
        }
    }

    return Status::OK();
}

Status Scanner::get_block_after_projects(RuntimeState* state, Block* block, bool* eos) {
    SCOPED_CONCURRENCY_COUNT(ConcurrencyStatsManager::instance().vscanner_get_block);
    auto& row_descriptor = _local_state->_parent->row_descriptor();
    if (_output_row_descriptor) {
        _origin_block.clear_column_data(row_descriptor.num_materialized_slots());
        const auto min_batch_size = std::max(state->batch_size() / 2, 1);
        const auto block_max_bytes = state->preferred_block_size_bytes();
        while (_padding_block.rows() < min_batch_size && _padding_block.bytes() < block_max_bytes &&
               !*eos) {
            RETURN_IF_ERROR(get_block(state, &_origin_block, eos));
            if (*eos) {
                // For the final block, merge any padding directly and return eos in this call.
                // The merged tail can be larger than the target batch, but each source block is
                // already bounded by the lower scanner.
                RETURN_IF_ERROR(_merge_padding_block());
                _origin_block.clear_column_data(row_descriptor.num_materialized_slots());
                break;
            }
            if (_origin_block.rows() >= min_batch_size) {
                break;
            }

            if (_origin_block.rows() + _padding_block.rows() <= state->batch_size() &&
                _origin_block.bytes() + _padding_block.bytes() <= block_max_bytes) {
                RETURN_IF_ERROR(_merge_padding_block());
                _origin_block.clear_column_data(row_descriptor.num_materialized_slots());
            } else {
                if (_origin_block.rows() < _padding_block.rows()) {
                    _padding_block.swap(_origin_block);
                }
                break;
            }
        }

        if (_origin_block.empty() && !_padding_block.empty()) {
            _padding_block.swap(_origin_block);
        }
        return _do_projections(&_origin_block, block);
    } else {
        return get_block(state, block, eos);
    }
}

// 数据扫描节点中最核心的数据产生入口。它的主要职责是：
// 控制全局与局部 Limit 裁剪：在读取数据前与读取数据后校验剩余配额，实现跨线程/并发 Scanner 的早期短路（Early Exit）。
// 构建或重用内存数据块（Block）：根据输出 Tuple 的 Schema 构造向量化数据块。
// 调用存储引擎读取原始数据：调用子类实现的 _get_block_impl 获取底层存储的原始列数据。
// 执行谓词过滤（Filter）：对读取到的列数据应用 WHERE 表达式以及 Runtime Filter。
// 循环重试与阈值控制：若当前批次被过滤后为空（0 行），在达到单次扫描阈值前自动循环重试，避免向上层算子频繁返回空 Block。
// block (Block*)： 输出参数。 Scanner 将从存储层读取并过滤后的列数据填充到这个 Block 中，传给上层 pipeline 算子。
// eof (bool*)： 输出参数。告知调用方当前 Scanner 是否已经读取完毕（true 表示数据已全部读完或达到 Limit，后续无需再调用 get_block）。
Status Scanner::get_block(RuntimeState* state, Block* block, bool* eof) {
    // only empty block should be here
    // 要求传入的 block 必须是空的（0 行数据）。如果不为空，说明上层未清理旧数据，可能引发内存覆盖或拼接错误。
    DCHECK(block->rows() == 0);

    // Stop early if other scanners have already collected enough rows
    // for the SQL LIMIT. Skipped when _shared_scan_limit is null (topn
    // path or no LIMIT).
    // 当设置了 _shared_scan_limit（非 nullptr）时，通过原子操作 load(std::memory_order_acquire) 获取当前剩余的全局 Limit 额度。
    if (_shared_scan_limit && _shared_scan_limit->load(std::memory_order_acquire) <= 0) {
        *eof = true;
        return Status::OK();
    }

    // scanner running time
    SCOPED_RAW_TIMER(&_per_scanner_timer);
    // 计算本次 get_block 调用的最大允许读取数据行数阈值（当前已读行数 + 配置项 doris_scanner_row_num）。用于防范在极端全过滤场景下，死循环消耗过多 CPU。
    int64_t rows_read_threshold = _num_rows_read + config::doris_scanner_row_num;
    // 判断当前 block 是否开启了内存重用（mem_reuse()）。
    if (!block->mem_reuse()) {
        // 如果没有重用（初次分配），遍历输出元组描述符 _output_tuple_desc 中的所有槽位（Slot），创建对应的空可变列（get_empty_mutable_column()）、数据类型和列名，插入到 block 中，从而建立起当前 Block 的向量化结构（Schema）。
        for (auto* const slot_desc : _output_tuple_desc->slots()) {
            block->insert(ColumnWithTypeAndName(slot_desc->get_empty_mutable_column(),
                                                slot_desc->get_data_type_ptr(),
                                                slot_desc->col_name()));
        }
    }

    {
        // 核心读取与过滤循环 (Do-While 循环)
        do {
            // 1. Get input block from scanner
            {
                // get block time
                SCOPED_TIMER(_local_state->_scan_timer);
                // 多态调用派生类（如 OlapScanner / FileScanner）实现的物理读取函数，填充 block。
                RETURN_IF_ERROR(_get_block_impl(state, block, eof));
                if (*eof) {
                    DCHECK(block->rows() == 0);
                    break;
                }
                _num_rows_read += block->rows();
                _num_byte_read += block->allocated_bytes();
            }

            // 2. Filter the output block finally.
            // 谓词过滤（阶段 2）：
            // 对 block 应用谓词表达式（WHERE 条件和 Runtime Filter），对不符合条件的行进行筛选切片。
            {
                SCOPED_TIMER(_local_state->_filter_timer);
                RETURN_IF_ERROR(_filter_output_block(block));
            }
            // record rows return (after filter) for _limit check
            _num_rows_return += block->rows();
            // Publish progress to the shared counter so peer scanners can
            // observe it. The counter may go negative when several scanners
            // subtract concurrently; that is harmless because the operator's
            // reached_limit() makes the final cut.
            // 如果当前 Batch 过滤后依然得到了有效数据（block->rows() > 0），使用原子扣减操作 fetch_sub 更新共享的 _shared_scan_limit。
            if (_shared_scan_limit && block->rows() > 0) {
                _shared_scan_limit->fetch_sub(block->rows(), std::memory_order_acq_rel);
            }
        // 退出循环的条件（满足任意一个即停止）：
        // _should_stop == true（Scanner 被要求停止）；
        // state->is_cancelled() == true（查询被取消）；
        // block->rows() > 0（成功获取到了包含有效数据的 Block）；
        // *eof == true（底盘数据已全部读完）；
        // _num_rows_read >= rows_read_threshold（达到单次读取上限阈值，防止在过滤掉大量数据时卡死在当前调用中）。
        } while (!_should_stop && !state->is_cancelled() && block->rows() == 0 && !(*eof) &&
                 _num_rows_read < rows_read_threshold);
    }

    if (state->is_cancelled()) {
        // TODO: Should return the specific ErrorStatus instead of just Cancelled.
        return Status::Cancelled("cancelled");
    }
    // 状态校验与终止条件判定 (Post-Process & Exit)
    *eof = *eof || _should_stop;
    // set eof to true if per scanner limit is reached
    // currently for query: ORDER BY key LIMIT n
    *eof = *eof || (_limit > 0 && _num_rows_return >= _limit);
    *eof = *eof || (_shared_scan_limit && _shared_scan_limit->load(std::memory_order_acquire) <= 0);

    return Status::OK();
}
// 核心职责是：接收从存储引擎（如 Segment / Parquet / ORC 等）读取出来的原始数据块（Block），对其应用当前 Scanner 持有的所有过滤谓词（_conjuncts，包括 SQL 中的 WHERE 条件以及动态生成的 Runtime Filter），过滤掉不符合条件的行，并记录被过滤掉的行数度量指标（Metrics）。
// block (Block*)： 向量化数据块指针（Block 包含了多列数据，是 Doris 向量化执行引擎的核心数据载体）。
Status Scanner::_filter_output_block(Block* block) {
    // 在执行任何过滤计算之前，先调用 block->rows() 获取当前 Block 在过滤前的原始行数（Row Count），保存到变量 old_rows 中，用于后续计算被过滤掉的数据量。
    auto old_rows = block->rows();
    // 执行向量化谓词过滤（核心计算）
    // _conjuncts：当前 Scanner 内部持有的表达式上下文数组（VExprContextSPtrs），里面包含了下推给当前 Scanner 的所有 SQL 过滤条件和动态 Runtime Filter。
    Status st = VExprContext::filter_block(_conjuncts, block, block->columns());
    _counter.num_rows_unselected += old_rows - block->rows();
    return st;
}

Status Scanner::_do_projections(Block* origin_block, Block* output_block) {
    SCOPED_RAW_TIMER(&_per_scanner_timer);
    SCOPED_RAW_TIMER(&_projection_timer);

    const size_t rows = origin_block->rows();
    if (rows == 0) {
        return Status::OK();
    }

    {
        Block input_block = *origin_block;

        std::vector<int> result_column_ids;
        for (auto& projections : _intermediate_projections) {
            result_column_ids.resize(projections.size());
            for (int i = 0; i < projections.size(); i++) {
                RETURN_IF_ERROR(projections[i]->execute(&input_block, &result_column_ids[i]));
            }
            input_block.shuffle_columns(result_column_ids);
        }

        DCHECK_EQ(rows, input_block.rows());
        auto scoped_mutable_block = VectorizedUtils::build_scoped_mutable_mem_reuse_block(
                output_block, *_output_row_descriptor);
        auto& mutable_columns = scoped_mutable_block.mutable_columns();
        DCHECK_EQ(mutable_columns.size(), _projections.size());
        Columns shared_columns(mutable_columns.size());

        for (int i = 0; i < mutable_columns.size(); ++i) {
            ColumnPtr column_ptr;
            RETURN_IF_ERROR(_projections[i]->execute(&input_block, column_ptr));
            column_ptr = column_ptr->convert_to_full_column_if_const();
            if (mutable_columns[i]->is_nullable() != column_ptr->is_nullable()) {
                throw Exception(ErrorCode::INTERNAL_ERROR, "Nullable mismatch");
            }
            if (column_ptr->is_exclusive()) {
                mutable_columns[i] = IColumn::mutate(std::move(column_ptr));
            } else {
                shared_columns[i] = std::move(column_ptr);
            }
        }

        scoped_mutable_block.restore();
        for (int i = 0; i < shared_columns.size(); ++i) {
            if (shared_columns[i]) {
                output_block->replace_by_position(i, std::move(shared_columns[i]));
            }
        }
    }

    origin_block->clear_column_data(
            _local_state->_parent->row_descriptor().num_materialized_slots());
    DCHECK_EQ(output_block->rows(), rows);

    return Status::OK();
}

Status Scanner::try_append_late_arrival_runtime_filter() {
    if (_applied_rf_num == _total_rf_num) {
        return Status::OK();
    }
    DCHECK(_applied_rf_num < _total_rf_num);
    int arrived_rf_num = 0;
    RETURN_IF_ERROR(_local_state->update_late_arrival_runtime_filter(_state, arrived_rf_num));

    if (arrived_rf_num == _applied_rf_num) {
        // No newly arrived runtime filters, just return;
        return Status::OK();
    }

    // avoid conjunct destroy in used by storage layer
    _conjuncts.clear();
    RETURN_IF_ERROR(_local_state->clone_conjunct_ctxs(_conjuncts));
    _applied_rf_num = arrived_rf_num;
    return Status::OK();
}

uint64_t Scanner::_current_condition_cache_digest() const {
    DORIS_CHECK(_state != nullptr);
    DORIS_CHECK(_local_state != nullptr);
    if (_local_state->get_condition_cache_digest() == 0) {
        return 0;
    }

    // ScanLocalState computed its digest after collecting the RFs that were ready during open(). A
    // scanner may later clone more RF conjuncts between file splits, so rebuild from its current
    // snapshot instead of reusing that stale value. For example, split 0 may use P, while split 1
    // starts after an IN RF with payload {7, 9} arrives and must use digest(P AND RF{7, 9}). A
    // different payload {8, 10} consequently receives a different key. get_digest() returning zero
    // is the correctness fallback for an RF whose complete semantics cannot be represented.
    return _build_condition_cache_digest(_state->query_options().condition_cache_digest,
                                         _conjuncts);
}

uint64_t Scanner::_build_condition_cache_digest(uint64_t seed, const VExprContextSPtrs& conjuncts) {
    for (const auto& conjunct : conjuncts) {
        seed = conjunct->get_digest(seed);
        if (seed == 0) {
            return 0;
        }
    }
    return seed;
}

#ifdef BE_TEST
uint64_t Scanner::TEST_build_condition_cache_digest(uint64_t seed,
                                                    const VExprContextSPtrs& conjuncts) {
    return _build_condition_cache_digest(seed, conjuncts);
}
#endif

Status Scanner::close(RuntimeState* state) {
#ifndef BE_TEST
    COUNTER_UPDATE(_local_state->_scanner_wait_worker_timer, _scanner_wait_worker_timer);
#endif
    return Status::OK();
}

bool Scanner::_try_close() {
    bool expected = false;
    return _is_closed.compare_exchange_strong(expected, true);
}

void Scanner::_collect_profile_before_close() {
    COUNTER_UPDATE(_local_state->_scan_cpu_timer, _scan_cpu_timer);
    COUNTER_UPDATE(_local_state->_rows_read_counter, _num_rows_read);

    // Update stats for load. See _should_update_load_counters() for why this is gated.
    if (_should_update_load_counters()) {
        _state->update_num_rows_load_filtered(_counter.num_rows_filtered);
        _state->update_num_rows_load_unselected(_counter.num_rows_unselected);
    }
}

void Scanner::_update_scan_cpu_timer() {
    int64_t cpu_time = _cpu_watch.elapsed_time();
    _scan_cpu_timer += cpu_time;
    if (_state && _state->get_query_ctx()) {
        _state->get_query_ctx()->resource_ctx()->cpu_context()->update_cpu_cost_ms(cpu_time);
    }
}

} // namespace doris
