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

package org.apache.doris.common.util;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.MetaNotFoundException;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * MetaLockUtils is a helper class to lock and unlock all meta object in a list.
 * In order to escape dead lock, meta object in list should be sorted in ascending
 * order by id first, and then MetaLockUtils can lock them.
 */
public class MetaLockUtils {

    public static void readLockDatabases(List<? extends DatabaseIf> databaseList) {
        for (DatabaseIf database : databaseList) {
            database.readLock();
        }
    }

    public static void readUnlockDatabases(List<? extends DatabaseIf> databaseList) {
        for (int i = databaseList.size() - 1; i >= 0; i--) {
            databaseList.get(i).readUnlock();
        }
    }

    public static void readLockTables(List<? extends TableIf> tableList) {
        for (TableIf table : tableList) {
            table.readLock();
        }
    }

    public static void readUnlockTables(List<? extends TableIf> tableList) {
        for (int i = tableList.size() - 1; i >= 0; i--) {
            tableList.get(i).readUnlock();
        }
    }

    public static void writeLockTables(List<? extends TableIf> tableList) {
        for (TableIf table : tableList) {
            table.writeLock();
        }
    }

    public static List<? extends TableIf> writeLockTablesIfExist(List<? extends TableIf> tableList) {
        List<TableIf> lockedTablesList = Lists.newArrayListWithCapacity(tableList.size());
        for (TableIf table : tableList) {
            if (table.writeLockIfExist()) {
                lockedTablesList.add(table);
            }
        }
        return lockedTablesList;
    }

    public static boolean tryWriteLockTablesIfExist(List<? extends TableIf> tableList, long timeout,
            TimeUnit unit) {
        for (int i = 0; i < tableList.size(); i++) {
            if (!tableList.get(i).tryWriteLockIfExist(timeout, unit)) {
                for (int j = i - 1; j >= 0; j--) {
                    tableList.get(j).writeUnlock();
                }
                return false;
            }
        }
        return true;
    }

    public static void writeLockTablesOrMetaException(List<? extends TableIf> tableList) throws MetaNotFoundException {
        for (int i = 0; i < tableList.size(); i++) {
            try {
                tableList.get(i).writeLockOrMetaException();
            } catch (MetaNotFoundException e) {
                for (int j = i - 1; j >= 0; j--) {
                    tableList.get(j).writeUnlock();
                }
                throw e;
            }
        }
    }
    // 主要用于在处理涉及多张表的事务（如多表导入、表锁冲突管理等）时，按顺序尝试对表列表中所有的表批量施加写锁（Write Lock）
    // 最核心的机制在于“全有或全无”（All-or-Nothing）的回滚保护：如果在给某张表加锁超时，或者某张表元数据已被删除/不存在（抛出 MetaNotFoundException）时，方法会自动释放之前已经加锁成功的所有表，防止发生死锁或锁泄漏。
    public static boolean tryWriteLockTablesOrMetaException(List<? extends TableIf> tableList, long timeout,
            TimeUnit unit) throws MetaNotFoundException {
        // 遍历表列表进行逐一加锁
        for (int i = 0; i < tableList.size(); i++) {
            try {
                // 尝试获取第 i 张表的写锁，最多等待 timeout 时间。
                // 若在超时时间内未能获取到第 i 张表的写锁（返回 false），说明发生了写锁竞争或超时。
                if (!tableList.get(i).tryWriteLockOrMetaException(timeout, unit)) {
                    // 回滚所有已拿到的锁之后，向调用方返回 false，表示本次批量加锁操作失败。
                    for (int j = i - 1; j >= 0; j--) {
                        tableList.get(j).writeUnlock();
                    }
                    return false;
                }
            // 异常回滚机制（元数据丢失时释放已加的锁）
            } catch (MetaNotFoundException e) {
                for (int j = i - 1; j >= 0; j--) {
                    tableList.get(j).writeUnlock();
                }
                throw e;
            }
        }
        return true;
    }

    public static void writeUnlockTables(List<? extends TableIf> tableList) {
        for (int i = tableList.size() - 1; i >= 0; i--) {
            tableList.get(i).writeUnlock();
        }
    }

    public static void commitLockTables(List<Table> tableList) {
        for (int i = 0; i < tableList.size(); i++) {
            try {
                tableList.get(i).commitLock();
            } catch (Exception e) {
                for (int j = i - 1; j >= 0; j--) {
                    tableList.get(i).commitUnlock();
                }
            }
        }
    }

    public static void commitUnlockTables(List<Table> tableList) {
        for (int i = tableList.size() - 1; i >= 0; i--) {
            tableList.get(i).commitUnlock();
        }
    }

    public static boolean tryCommitLockTables(List<Table> tableList, long timeout, TimeUnit unit) {
        for (int i = 0; i < tableList.size(); i++) {
            if (!tableList.get(i).tryCommitLock(timeout, unit)) {
                for (int j = i - 1; j >= 0; j--) {
                    tableList.get(j).commitUnlock();
                }
                return false;
            }
        }
        return true;
    }
}
