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

package org.apache.doris.mysql.privilege;

import org.apache.doris.analysis.CompoundPredicate.Operator;
import org.apache.doris.analysis.ResourcePattern;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import java.util.Collection;
import java.util.List;
import java.util.Set;

// ....0000000000
//        ^     ^
//        |     |
//        |     -- first priv(0)
//        |--------last priv(7)
// PrivBitSet 是一个基于位图（BitMap/BitSet）思想实现的权限集合类。它使用一个 64 位长整型（long）变量，以二进制的每一位（Bit）对应代表一种特定的权限（例如 SELECT_PRIV、LOAD_PRIV、ALTER_PRIV 等）。
// 高效的权限存储与传输：将多个不同的权限组合存储在一个简单的 long 整数中，极大地节省了内存空间并提高了序列化/反序列化效率。
// 快速的权限校验与位运算：借助位运算符（&、|、^、~），以 $O(1)$ 的时间复杂度快速执行权限校验（满足/不满足）、权限合并（or）、权限取交集（and）以及权限撤销/移除（remove）。
// 支持复杂的权限谓词判断（PrivPredicate）：能够方便地校验当前用户拥有的权限位图是否满足某个操作所需的单项权限（OR 关系）或联合权限（AND 关系）。
// 分类判断与转换：提供对不同类型权限（节点权限、资源权限、库表权限）的快速检查，以及位图与 Privilege 对象列表/集合之间的相互转换。
public class PrivBitSet {
    // 存储权限集合的核心位图变量。默认值为 0（表示不包含任何权限）。
    // 使用 @SerializedName(value = "set") 标注，用于 Gson 序列化和持久化。64 位的 long 能够支持最多 64 种不同的权限索引（Index 0 ~ 63）。
    @SerializedName(value = "set")
    private long set = 0;

    public PrivBitSet() {
    }
    // 将指定索引位置的权限置为 1（即授予/开启该权限）。
    public void set(int index) {
        Preconditions.checkState(Privilege.privileges.containsKey(index), index);
        set |= 1 << index;
    }
    // 将指定索引位置的权限置为 0（即撤销/关闭该权限）。
    public void unset(int index) {
        Preconditions.checkState(Privilege.privileges.containsKey(index), index);
        set &= ~(1 << index);
    }
    // 检查当前位图中是否包含指定索引位置的权限。
    public boolean get(int index) {
        Preconditions.checkState(Privilege.privileges.containsKey(index), index);
        return (set & (1 << index)) > 0;
    }
    // 将另一个 PrivBitSet 的权限并入当前位图（权限求并集/合并授权）
    public void or(PrivBitSet other) {
        set |= other.set;
    }

    public void and(PrivBitSet other) {
        set &= other.set;
    }

    public void xor(PrivBitSet other) {
        set ^= other.set;
    }

    public void clean() {
        this.set = 0;
    }

    public void remove(PrivBitSet privs) {
        PrivBitSet tmp = copy();
        tmp.xor(privs);
        and(tmp);
    }

    public boolean isEmpty() {
        return set == 0;
    }

    public boolean satisfy(PrivPredicate wantPrivs) {
        if (wantPrivs.getOp() == Operator.AND) {
            return (set & wantPrivs.getPrivs().set) == wantPrivs.getPrivs().set;
        } else {
            return (set & wantPrivs.getPrivs().set) != 0;
        }
    }

    public boolean containsNodePriv() {
        return containsPrivs(Privilege.NODE_PRIV);
    }

    public boolean containsResourcePriv() {
        return containsPrivs(Privilege.USAGE_PRIV, Privilege.CLUSTER_USAGE_PRIV, Privilege.STAGE_USAGE_PRIV);
    }

    public boolean containsDbTablePriv() {
        return containsPrivs(Privilege.SELECT_PRIV, Privilege.LOAD_PRIV, Privilege.ALTER_PRIV,
                Privilege.CREATE_PRIV, Privilege.DROP_PRIV);
    }

    public boolean containsPrivs(Privilege... privs) {
        for (Privilege priv : privs) {
            if (get(priv.getIdx())) {
                return true;
            }
        }
        return false;
    }

    public List<Privilege> toPrivilegeList() {
        List<Privilege> privs = Lists.newArrayList();
        Privilege.privileges.keySet().forEach(idx -> {
            if (get(idx)) {
                privs.add(Privilege.getPriv(idx));
            }
        });
        return privs;
    }

    public static PrivBitSet of(Privilege... privs) {
        PrivBitSet bitSet = new PrivBitSet();
        for (Privilege priv : privs) {
            bitSet.set(priv.getIdx());
        }
        return bitSet;
    }

    public static PrivBitSet of(Collection<Privilege> privs) {
        PrivBitSet bitSet = new PrivBitSet();
        for (Privilege priv : privs) {
            bitSet.set(priv.getIdx());
        }
        return bitSet;
    }

    public PrivBitSet copy() {
        PrivBitSet newSet = new PrivBitSet();
        newSet.set = set;
        return newSet;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        Privilege.privileges.keySet().forEach(idx -> {
            if (get(idx)) {
                sb.append(Privilege.getPriv(idx)).append(",");
            }
        });
        String res = sb.toString();
        if (res.length() > 0) {
            return res.substring(0, res.length() - 1);
        } else {
            return res;
        }
    }

    public static void convertResourcePrivToCloudPriv(ResourcePattern resourcePattern, Set<Privilege> privileges) {
        if (privileges.size() != 1 || !privileges.contains(Privilege.USAGE_PRIV)) {
            return;
        }
        switch (resourcePattern.getResourceType()) {
            case CLUSTER:
                privileges.clear();
                privileges.add(Privilege.CLUSTER_USAGE_PRIV);
                break;
            case STAGE:
                privileges.clear();
                privileges.add(Privilege.STAGE_USAGE_PRIV);
                break;
            default:
                break;
        }
    }
}
