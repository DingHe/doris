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

package org.apache.doris.common.lock;

import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.qe.ConnectContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A monitored version of ReentrantReadWriteLock that provides additional
 * monitoring capabilities for read and write locks.
 */
// Apache Doris 对 Java 标准库 java.util.concurrent.locks.ReentrantReadWriteLock（可重入读写锁）的一个扩展与监控增强类。
// 核心作用包括：
// 锁持有时长监控与日志追踪：
// 通过内置的 AbstractMonitoredLock 监控器，在获取锁（lock）和释放锁（unlock）时记录时间戳，用于统计锁的持有时间。当持锁时间过长时，可以输出警告日志或进行耗时分析，帮助开发者定位元数据锁竞争瓶颈。
// 死锁风险预警（锁降级/升级风险检测）：
// 在公平锁模式（isFair()）下，当写锁被获取时，自动检测当前线程是否还持有读锁（getReadHoldCount() > 0）。这种“在持有读锁的同时尝试获取写锁”的行为会导致死锁（即锁升级死锁），类中对此打印警告日志、当前线程堆栈以及 Query ID，帮助排查隐蔽的并发 Bug。
//
public class MonitoredReentrantReadWriteLock extends ReentrantReadWriteLock {

    private static final Logger LOG = LogManager.getLogger(MonitoredReentrantReadWriteLock.class);
    // Monitored read and write lock instances
    // 当前监控读写锁内部持有的增强版读锁实例，在类初始化时创建并绑定到当前对象。
    private final ReadLock readLock = new ReadLock(this);
    // 当前监控读写锁内部持有的增强版写锁实例，在类初始化时创建并绑定到当前对象。
    private final WriteLock writeLock = new WriteLock(this);

    // Constructor for creating a monitored lock with fairness option
    // 调用父类 ReentrantReadWriteLock(fair) 构造函数。参数 fair 为 true 时创建公平锁，为 false 时创建非公平锁。
    public MonitoredReentrantReadWriteLock(boolean fair) {
        super(fair);
    }

    public MonitoredReentrantReadWriteLock() {
    }

    /**
     * Monitored read lock class that extends ReentrantReadWriteLock.ReadLock.
     */
    public class ReadLock extends ReentrantReadWriteLock.ReadLock {
        private static final long serialVersionUID = 1L;
        private final AbstractMonitoredLock monitor = new AbstractMonitoredLock() {};

        /**
         * Constructs a new ReadLock instance.
         *
         * @param lock The ReentrantReadWriteLock this lock is associated with
         */
        protected ReadLock(ReentrantReadWriteLock lock) {
            super(lock);
        }

        /**
         * Acquires the read lock.
         * Records the time when the lock is acquired.
         */
        @Override
        public void lock() {
            super.lock();
            monitor.afterLock();
        }

        /**
         * Releases the read lock.
         * Records the time when the lock is released and logs the duration.
         */
        @Override
        public void unlock() {
            monitor.afterUnlock();
            super.unlock();
        }
    }

    /**
     * Monitored write lock class that extends ReentrantReadWriteLock.WriteLock.
     */
    public class WriteLock extends ReentrantReadWriteLock.WriteLock {
        private static final long serialVersionUID = 1L;
        private final AbstractMonitoredLock monitor = new AbstractMonitoredLock() {};

        /**
         * Constructs a new WriteLock instance.
         *
         * @param lock The ReentrantReadWriteLock this lock is associated with
         */
        protected WriteLock(ReentrantReadWriteLock lock) {
            super(lock);
        }

        /**
         * Acquires the write lock.
         * Records the time when the lock is acquired.
         */
        @Override
        public void lock() {
            super.lock();
            monitor.afterLock();
            if (isFair() && getReadHoldCount() > 0) {
                LOG.warn(" read lock count is {}, write lock count is {}, stack is {}, query id is {}",
                        getReadHoldCount(), getWriteHoldCount(), Thread.currentThread().getStackTrace(),
                        ConnectContext.get() == null ? "" : DebugUtil.printId(ConnectContext.get().queryId()));
            }
        }

        /**
         * Releases the write lock.
         * Records the time when the lock is released and logs the duration.
         */
        @Override
        public void unlock() {
            monitor.afterUnlock();
            super.unlock();
        }
    }

    /**
     * Returns the read lock associated with this lock.
     *
     * @return The monitored read lock
     */
    @Override
    public ReadLock readLock() {
        return readLock;
    }

    /**
     * Returns the write lock associated with this lock.
     *
     * @return The monitored write lock
     */
    @Override
    public WriteLock writeLock() {
        return writeLock;
    }

    @Override
    public Thread getOwner() {
        return super.getOwner();
    }
}
