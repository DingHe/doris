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

#include <memory>

#include "runtime/runtime_state.h"

namespace doris {

class RuntimeState;

// Base class for execution contexts (e.g. PipelineFragmentContext).
//
// For recursive CTE, the PFC (which inherits from this class) is held by external threads
// (scanner threads, brpc callbacks, etc.) via weak_ptr<TaskExecutionContext>.
// 在 Apache Doris 的 MPP 执行引擎中，一个查询任务（如 Fragment / PipelineFragment）通常由主线程（如 Fragment 执行线程）创建并驱动，但执行过程中会有大量的异步线程或子线程参与协作，例如：
// Scanner 线程：从存储引擎读取数据；
// BRPC 回调线程：处理网络传输与 RPC 数据发送；
// 异步 IO / 调度线程。
// TaskExecutionContext 的核心作用是：
// 作为执行上下文基类：为具体的 Fragment 执行上下文（如 PipelineFragmentContext）提供统一的基类类型，并继承 std::enable_shared_from_this。
// 配合弱引用（std::weak_ptr）进行安全访问：工作线程不直接持有上下文的强引用（避免循环引用导致无法释放），而是持有 weak_ptr。
// 工作线程在访问上下文前，通过 lock() 方法提升为 shared_ptr；若锁成功，说明对象仍存活可以安全访问；若返回空，说明上下文已被销毁，线程直接安全退出即可。

class TaskExecutionContext : public std::enable_shared_from_this<TaskExecutionContext> {
public:
    TaskExecutionContext();
    virtual ~TaskExecutionContext();
};

using TaskExecutionContextSPtr = std::shared_ptr<TaskExecutionContext>;

// Task Execution Context maybe plan fragment executor or pipelinefragmentcontext or pipelinexfragmentcontext
// In multi thread scenario, the object is created in main thread (such as FragmentExecThread), but the object
// maybe used in other thread(such as scanner thread, brpc->sender queue). If the main thread stopped and destroy
// the object, then the other thread may core. So the other thread must lock the context to ensure the object exists.
struct HasTaskExecutionCtx {
    using Weak = typename TaskExecutionContextSPtr::weak_type;

    HasTaskExecutionCtx(RuntimeState* state);

    virtual ~HasTaskExecutionCtx();

public:
    inline TaskExecutionContextSPtr task_exec_ctx() const { return task_exec_ctx_.lock(); }
    inline Weak weak_task_exec_ctx() const { return task_exec_ctx_; }

private:
    Weak task_exec_ctx_;
};

} // namespace doris
