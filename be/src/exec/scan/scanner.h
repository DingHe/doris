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

#include <algorithm>
#include <atomic>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/tablet/tablet.h"
#include "util/stopwatch.hpp"

namespace doris {
class RuntimeProfile;
class TupleDescriptor;

class VExprContext;

class ScanLocalStateBase;
} // namespace doris

namespace doris {

// Counter for load
struct ScannerCounter {
    ScannerCounter() : num_rows_filtered(0), num_rows_unselected(0) {}

    int64_t num_rows_filtered;   // unqualified rows (unmatched the dest schema, or no partition)
    int64_t num_rows_unselected; // rows filtered by predicates
};
// Scanner 扮演着底层存储与上层执行引擎之间的“数据适配器”角色：
// 解耦存储与执行：上层查询执行引擎（Pipeline 执行框架）只需调用统一的 get_block 接口，而无需关心具体数据是在内表（OlapEngine）、外表（FileScanner / Iceberg / Paimon）、还是内存缓存中。
// 数据过滤与投影（Filter & Projection）：在 Scanner 内部完成列裁剪、谓词下推（Conjuncts 过滤）、投影表达式计算（Projections）以及 Late Arrival Runtime Filter（延迟到达的运行时过滤）的动态应用。
// 线程与执行调度控制：支持异步/多线程任务调度，提供 pause、resume 以及线程等待与 CPU 时间统计，协调 Scanner 线程池的工作。
// 指标收集（Profile & Metrics）：全生命周期收集 I/O、扫描行数/字节数、CPU 耗时、等待耗时等性能 Profile。
class Scanner {
public:
    Scanner(RuntimeState* state, ScanLocalStateBase* local_state, int64_t limit,
            RuntimeProfile* profile);

    //only used for FileScanner read one line.
    // 专为单行读取（如 FileScanner 读单行测试/探测）设计的轻量级构造函数。内部将 _limit 设为 1，并将全局度量指标 scanner_cnt 原子加 1。
    Scanner(RuntimeState* state, RuntimeProfile* profile)
            : _state(state), _limit(1), _profile(profile), _total_rf_num(0), _has_prepared(false) {
        DorisMetrics::instance()->scanner_cnt->increment(1);
    };

    virtual ~Scanner() {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_state->query_mem_tracker());
        _input_block.clear();
        _conjuncts.clear();
        _projections.clear();
        _origin_block.clear();
        _common_expr_ctxs_push_down.clear();
        DorisMetrics::instance()->scanner_cnt->increment(-1);
    }

    virtual Status init(RuntimeState* state, const VExprContextSPtrs& conjuncts);
    Status prepare() {
        SCOPED_RAW_TIMER(&_per_scanner_timer);
        SCOPED_RAW_TIMER(&_per_scanner_prepare_timer);
        return _prepare_impl();
    }
    // 打开 Scanner 模板方法。开启 Open 计时器并调用底层 _open_impl(state)，准备底层数据句柄（如打开文件、迭代器）。
    Status open(RuntimeState* state) {
        SCOPED_RAW_TIMER(&_per_scanner_timer);
        SCOPED_RAW_TIMER(&_per_scanner_open_timer);
        return _open_impl(state);
    }
    // 最核心的数据读取入口。内部会调用底层 _get_block_impl 获取数据，自动应用 _filter_output_block（谓词过滤），并更新行数计数器。当读到末尾时设置 *eos = true。
    Status get_block(RuntimeState* state, Block* block, bool* eos);
    // 在完成数据读取与过滤后，额外执行表达式投影计算（调用 _do_projections），将计算后的最终列填入 block 中返回。
    Status get_block_after_projects(RuntimeState* state, Block* block, bool* eos);
    // 关闭 Scanner，释放底层文件句柄、迭代器等资源，汇总 Profile 统计数据。
    virtual Status close(RuntimeState* state);

    // Try to stop scanner, and all running readers.
    virtual void try_stop() { _should_stop = true; };

    virtual std::string get_name() { return ""; }

    // return the readable name of current scan range.
    // eg, for file scanner, return the current file path.
    virtual std::string get_current_scan_range_name() { return "not implemented"; }

#ifdef BE_TEST
    // 根据当前 Scanner 内部最新的 _conjuncts 快照，重新计算并返回条件缓存的哈希摘要。
    static uint64_t TEST_build_condition_cache_digest(uint64_t seed,
                                                      const VExprContextSPtrs& conjuncts);
#endif

protected:
    // Rebuild the condition-cache digest from the scanner's current conjunct snapshot. The local
    // state's digest is used only as a safety gate: zero means condition cache was disabled during
    // scan-node open (for example by TopN or an expression without a reliable digest).
    uint64_t _current_condition_cache_digest() const;
    static uint64_t _build_condition_cache_digest(uint64_t seed,
                                                  const VExprContextSPtrs& conjuncts);
    // prepare() 的子类实现点。默认设置 _has_prepared = true。
    virtual Status _prepare_impl() {
        _has_prepared = true;
        return Status::OK();
    }
    // open() 的子类实现点。默认根据 batch_size 估算初始 _block_avg_bytes。
    virtual Status _open_impl(RuntimeState* state) {
        _block_avg_bytes = state->batch_size() * 8;
        return Status::OK();
    }

    // Subclass should implement this to return data.
    virtual Status _get_block_impl(RuntimeState* state, Block* block, bool* eof) = 0;

    Status _merge_padding_block() {
        if (_padding_block.empty()) {
            _padding_block.swap(_origin_block);
        } else if (_origin_block.rows()) {
            ScopedMutableBlock scoped_mutable_block(&_padding_block);
            auto& mutable_block = scoped_mutable_block.mutable_block();
            RETURN_IF_ERROR(mutable_block.merge(_origin_block));
        }
        return Status::OK();
    }

    // Update the counters before closing this scanner
    virtual void _collect_profile_before_close();

    // Whether rows filtered/unselected by this scanner should be reported to the load
    // counters in RuntimeState. Only the scanner reading the load source data should
    // report, otherwise rows filtered by query predicates (e.g. in INSERT INTO ... SELECT
    // or DELETE FROM ... WHERE) would be mixed into load counters and make
    // num_rows_load_success() negative.
    virtual bool _should_update_load_counters() const { return _is_load; }

    // Check if scanner is already closed, if not, mark it as closed.
    // Returns true if the scanner was successfully marked as closed (first time).
    // Returns false if the scanner was already closed.
    bool _try_close();

    // Filter the output block finally.
    virtual Status _filter_output_block(Block* block);

    Status _do_projections(Block* origin_block, Block* output_block);

private:
    void _start_scan_cpu_timer() {
        _cpu_watch.reset();
        _cpu_watch.start();
    }

    void _update_wait_worker_timer() { _scanner_wait_worker_timer += _watch.elapsed_time(); }
    void _update_scan_cpu_timer();

public:
    // Call start_wait_worker_timer() when submit the scanner to the thread pool.
    // And call update_wait_worker_timer() when it is actually being executed.
    void start_wait_worker_timer() {
        _watch.reset();
        _watch.start();
    }

    void resume() {
        _update_wait_worker_timer();
        _start_scan_cpu_timer();
    }
    // 当 Scanner 被切出线程或暂停执行时调用。更新 CPU 耗时，并重新开启等待计时。
    void pause() {
        _update_scan_cpu_timer();
        start_wait_worker_timer();
    }
    int64_t get_time_cost_ns() const { return _per_scanner_timer; }
    int64_t get_prepare_time_cost_ns() const { return _per_scanner_prepare_timer; }
    int64_t get_open_time_cost_ns() const { return _per_scanner_open_timer; }

    int64_t projection_time() const { return _projection_timer; }
    int64_t get_rows_read() const { return _num_rows_read; }
    // 判断当前 Scanner 是否完成了 prepare 阶段。
    bool has_prepared() const { return _has_prepared; }

    Status try_append_late_arrival_runtime_filter();

    int64_t get_scanner_wait_worker_timer() const { return _scanner_wait_worker_timer; }

    // Some counters need to be updated realtime, for example, workload group policy need
    // scan bytes to cancel the query exceed limit.
    virtual void update_realtime_counters() {}

    RuntimeState* runtime_state() { return _state; }

    bool is_open() const { return _is_open; }
    void set_opened() { _is_open = true; }

    virtual doris::TabletStorageType get_storage_type() {
        return doris::TabletStorageType::STORAGE_TYPE_REMOTE;
    }

    // Returns true if this scanner's partition has been pruned by a runtime filter.
    // Overridden by OlapScanner to check partition pruning state.
    virtual bool check_partition_pruned() const { return false; }

    bool need_to_close() const { return _need_to_close; }

    void mark_to_need_to_close() {
        // If the scanner is failed during init or open, then not need update counters
        // because the query is fail and the counter is useless. And it may core during
        // update counters. For example, update counters depend on scanner's tablet, but
        // the tablet == null when init failed.
        if (_is_open) {
            _collect_profile_before_close();
        }
        _need_to_close = true;
    }

    void set_status_on_failure(const Status& st) { _status = st; }

    int64_t limit() const { return _limit; }

    auto get_block_avg_bytes() const { return _block_avg_bytes; }

    void update_block_avg_bytes(size_t block_avg_bytes) { _block_avg_bytes = block_avg_bytes; }

protected:
    // 当前查询或导入任务的全局运行时状态句柄，包含查询参数、内存 Tracker、Batch Size 等。
    RuntimeState* _state = nullptr;
    // Pipeline 框架中 Scan 算子的本地执行状态（LocalState），用于交互全局信息。
    ScanLocalStateBase* _local_state = nullptr;

    // Set if scan node has sort limit info
    // 下推到该 Scanner 的最大读取行数限制（LIMIT）。初始值为 -1 表示无限制。
    int64_t _limit = -1;
    // 性能分析指标树节点，Scanner 内部的各种耗时计数器会注册并汇报到该对象中。
    RuntimeProfile* _profile = nullptr;
    // 最终输出数据块（Block）的元组描述符。
    const TupleDescriptor* _output_tuple_desc = nullptr;
    // 输出行结构描述符（包含多个 Tuple 的组合关系）。
    const RowDescriptor* _output_row_descriptor = nullptr;

    // If _input_tuple_desc is set, the scanner will read data into
    // this _input_block first, then convert to the output block.
    // 原始数据读取缓存块。
    // 如果存储层数据格式与算子所需输出格式不一致，会先读入 _input_block 再转换为输出 Block。
    Block _input_block;
    // 标记该 Scanner 是否已成功执行完 open() 逻辑。
    bool _is_open = false;
    // 原子布尔变量，标记当前 Scanner 是否已被关闭，确保 close() 调用的幂等性与线程安全。
    std::atomic<bool> _is_closed {false};
    // 标记该 Scanner 是否已被打上“待关闭”标签。
    bool _need_to_close = false;
    // 记录 Scanner 内部发生的异常/错误状态。
    Status _status;

    // If _applied_rf_num == _total_rf_num
    // means all runtime filters are arrived and applied.
    // 当前已经成功生效/应用到该 Scanner 上的 Runtime Filter 数量。
    int _applied_rf_num = 0;
    // 该 Scanner 需要等待或处理的 Runtime Filter 总数。当 _applied_rf_num == _total_rf_num 时，表示所有 RF 已到位。
    int _total_rf_num = 0;
    // Cloned from _conjuncts of scan node.
    // It includes predicate in SQL and runtime filters.
    // 表达式上下文指针数组，存有下推到该 Scanner 的所有过滤谓词（包括 SQL 中的 WHERE 谓词及动态生成的 Runtime Filter）。
    VExprContextSPtrs _conjuncts;
    // 投影表达式上下文指针数组，用于处理 SELECT 中列的计算与映射转换。
    VExprContextSPtrs _projections;
    // Used in common subexpression elimination to compute intermediate results.
    // 公共子表达式消除（CSE）过程中，用于计算中间结果的多阶段投影上下文。
    std::vector<VExprContextSPtrs> _intermediate_projections;
    // 存储过滤/投影前从存储层直接读出的原始 Block。
    Block _origin_block;
    // 填充/合并 Block，主要用于处理补全列或跨 Batch 拼接时的临时缓存 Block。
    Block _padding_block;
    // 下推到存储层的公共表达式上下文。
    VExprContextSPtrs _common_expr_ctxs_push_down;

    // num of rows read from scanner
    // 从存储介质中实际读取的原始行数（过滤前）。
    int64_t _num_rows_read = 0;
    // 从存储介质中实际读取的原始字节数。
    int64_t _num_byte_read = 0;

    // num of rows return from scanner, after filter block
    // 经过谓词过滤和投影处理后，实际返回给上层算子的行数。
    int64_t _num_rows_return = 0;
    // 单个 Block 的平均内存字节数估计值，用于预估分配内存。
    size_t _block_avg_bytes = 0;

    // Set true after counter is updated finally
    // 标记 Profile 计数器是否已经最终更新过，防止重复累加。
    bool _has_updated_counter = false;

    // watch to count the time wait for scanner thread
    // 单调递增计时器，用于测量 Scanner 在工作线程池中的等待排队时间。
    MonotonicStopWatch _watch;
    // Do not use ScopedTimer. There is no guarantee that, the counter
    // 线程 CPU 耗时计时器，精确测量 Scanner 实际占用 CPU 执行计算的时间。
    ThreadCpuStopWatch _cpu_watch;
    // 累加值：Scanner 在线程池中等待被调度执行的总纳秒数。
    int64_t _scanner_wait_worker_timer = 0;
    // 累加值：Scanner 实际消耗 CPU 的总纳秒数。
    int64_t _scan_cpu_timer = 0;
    // 标记当前 Scanner 是否运行在数据导入（Load）场景下（如 INSERT INTO, STREAM LOAD）。
    bool _is_load = false;
    // 标记 prepare() 阶段是否已完成。
    bool _has_prepared = false;
    // 封装了各种度量指标计数器（如过滤行数、位图过滤行数等）的结构体。
    ScannerCounter _counter;
    // 单个 Scanner 对象的总生命周期运行耗时（纳秒）。
    int64_t _per_scanner_timer = 0;
    int64_t _per_scanner_prepare_timer = 0;
    int64_t _per_scanner_open_timer = 0;
    int64_t _projection_timer = 0;
    // 停止标志位，当外部查询取消或 Limit 满足时置为 true，通知底层读取迭代器提前终止。
    bool _should_stop = false;

    // Cached pointer to ScanOperator's remaining-limit counter. Null when
    // this scanner is on the topn path or the query has no LIMIT.
    // 共享的剩余 Limit 计数器指针（多个 Scanner 共享），用于并发 Scan 场景下的快速全局 Limit 扣减。
    std::atomic<int64_t>* _shared_scan_limit = nullptr;
};

using ScannerSPtr = std::shared_ptr<Scanner>;

} // namespace doris
