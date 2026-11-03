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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/COW.h
// and modified by Doris

#pragma once

#include <atomic>
#include <initializer_list>
#include <type_traits>
#include <vector>

#include "common/exception.h"
#include "common/status.h"

namespace doris {

/** Copy-on-write shared ptr.
  * Allows to work with shared immutable objects and sometimes unshare and mutate you own unique copy.
  *
  * Usage:
    class Column : public COW<Column>
    {
    private:
        friend class COW<Column>;
        /// Leave all constructors in private section. They will be available through 'create' method.
        Column();
        /// Provide 'clone' method. It can be virtual if you want polymorphic behaviour.
        virtual Column * clone() const;
    public:
        /// Correctly use const qualifiers in your interface.
        virtual ~Column() {}
    };
  * It will provide 'create' and 'mutate' methods.
  * And 'Ptr' and 'MutablePtr' types.
  * Ptr is refcounted pointer to immutable object.
  * MutablePtr is refcounted noncopyable pointer to mutable object.
  * MutablePtr can be assigned to Ptr through move assignment.
  *
  * 'create' method creates MutablePtr: you cannot share mutable objects.
  * To share, move-assign to immutable pointer.
  * 'mutate' method allows to create mutable noncopyable object from immutable object:
  *   either by cloning or by using directly, if it is not shared.
  * These methods are thread-safe.
  *
  * Example:
  *
    /// Creating and assigning to immutable ptr.
    Column::Ptr x = Column::create(1);
    /// Sharing single immutable object in two ptrs.
    Column::Ptr y = x;
    /// Now x and y are shared.
    /// Change value of x.
    {
        /// Creating mutable ptr. It can clone an object under the hood if it was shared.
        Column::MutablePtr mutate_x = std::move(*x).mutate();
        /// Using non-const methods of an object.
        mutate_x->set(2);
        /// Assigning pointer 'x' to mutated object.
        x = std::move(mutate_x);
    }
    /// Now x and y are unshared and have different values.
  * Note. You may have heard that COW is bad practice.
  * Actually it is, if your values are small or if copying is done implicitly.
  * This is the case for string implementations.
  *
  * In contrast, COW is intended for the cases when you need to share states of large objects,
  * (when you usually will use std::shared_ptr) but you also want precise control over modification
  * of this shared state.
  *
  * Caveats:
  * - after a call to 'mutate' method, you can still have a reference to immutable ptr somewhere.
  * - as 'mutable_ptr' should be unique, it's refcount is redundant - probably it would be better
  *   to use std::unique_ptr for it somehow.
  */
// 写时复制模板类
// 提供一套侵入式引用计数（Intrusive Reference Counting）与写时复制（Copy-on-Write）机制。它允许多个不可变指针（Ptr）高效共享同一个大型对象（例如大数据量的列对象）而无需复制；
// 当需要对对象进行修改（Mutate）时，如果该对象被多个指针共享，系统会自动触发 clone() 深拷贝出一个独占的副本，从而保证函数式编程的不可变性与线程安全性。
template <typename Derived>
class COW {
    // 原子引用计数器。记录当前对象被多少个智能指针所共享，确保在多线程环境下的线程安全。
    std::atomic_uint ref_counter;

protected:
    // 默认构造函数
    COW() : ref_counter(0) {}
    // 拷贝构造函数
    COW(COW const&) : ref_counter(0) {}
    // 拷贝赋值运算符
    COW& operator=(COW const&) { return *this; }
    // 增加引用计数
    void add_ref() { ++ref_counter; }
    // 减少引用计数（原子自减 --ref_counter）。当计数归零时，通过 delete static_cast<const Derived*>(this) 自动销毁对象。
    void release_ref() {
        if (--ref_counter == 0) {
            delete static_cast<const Derived*>(this);
        }
    }
    // 向下转型辅助方法，将 COW 基类指针转换为具体的派生类（Derived*）指针。
    Derived* derived() { return static_cast<Derived*>(this); }

    const Derived* derived() const { return static_cast<const Derived*>(this); }
    // 侵入式智能指针
    // 模仿 boost::intrusive_ptr 实现的高性能引用计数智能指针，分为不可变指针 Ptr 和可变指针 MutablePtr 的底层支撑。
    template <typename T>
    class intrusive_ptr {
    public:
        // 默认构造函数，初始化指针为空。
        intrusive_ptr() : t(nullptr) {}
        // 带指针和是否增加引用计数参数的构造函数。
        intrusive_ptr(T* t, bool add_ref = true) : t(t) {
            if (t && add_ref) {
                ((std::remove_const_t<T>*)t)->add_ref();
            }
        }
        // 拷贝构造函数，自动对目标对象调用 add_ref()
        template <typename U>
        intrusive_ptr(intrusive_ptr<U> const& rhs) : t(rhs.get()) {
            if (t) {
                ((std::remove_const_t<T>*)t)->add_ref();
            }
        }

        intrusive_ptr(intrusive_ptr const& rhs) : t(rhs.get()) {
            if (t) {
                ((std::remove_const_t<T>*)t)->add_ref();
            }
        }
        // 析构函数，自动调用 release_ref()
        ~intrusive_ptr() {
            if (t) {
                ((std::remove_const_t<T>*)t)->release_ref();
            }
        }
        // 拷贝赋值与移动赋值运算符，通过惯用法（Copy-and-Swap）保证异常安全。
        template <typename U>
        intrusive_ptr& operator=(intrusive_ptr<U> const& rhs) {
            intrusive_ptr(rhs).swap(*this);
            return *this;
        }
        // 移动构造函数，剥离原指针所有权。
        intrusive_ptr(intrusive_ptr&& rhs) : t(rhs.t) { rhs.t = nullptr; }

        intrusive_ptr& operator=(intrusive_ptr&& rhs) {
            intrusive_ptr(static_cast<intrusive_ptr&&>(rhs)).swap(*this);
            return *this;
        }

        template <class U>
        friend class intrusive_ptr;

        template <class U>
        intrusive_ptr(intrusive_ptr<U>&& rhs) : t(rhs.t) {
            rhs.t = nullptr;
        }

        template <class U>
        intrusive_ptr& operator=(intrusive_ptr<U>&& rhs) {
            intrusive_ptr(static_cast<intrusive_ptr<U>&&>(rhs)).swap(*this);
            return *this;
        }

        intrusive_ptr& operator=(intrusive_ptr const& rhs) {
            intrusive_ptr(rhs).swap(*this);
            return *this;
        }

        intrusive_ptr& operator=(T* rhs) {
            intrusive_ptr(rhs).swap(*this);
            return *this;
        }
        // 重置智能指针，释放旧对象并绑定新对象。
        void reset() { intrusive_ptr().swap(*this); }

        void reset(T* rhs) { intrusive_ptr(rhs).swap(*this); }

        void reset(T* rhs, bool add_ref) { intrusive_ptr(rhs, add_ref).swap(*this); }
        // 返回裸指针 T*
        T* get() const { return t; }
        // 剥离指针所有权但不减少引用计数，返回裸指针。
        T* detach() {
            T* ret = t;
            t = nullptr;
            return ret;
        }
        // 交换两个指针。
        void swap(intrusive_ptr& rhs) {
            T* tmp = t;
            t = rhs.t;
            rhs.t = tmp;
        }
        // 解引用与成员访问操作符（区分 const 与非 const 左值/右值引用）。
        T& operator*() const& { return *t; }

        T&& operator*() const&& { return const_cast<std::remove_const_t<T>&&>(*t); }

        T* operator->() const { return t; }

        operator bool() const { return t != nullptr; }

    private:
        T* t = nullptr;
    };

protected:
    template <typename T>
    class mutable_ptr : public intrusive_ptr<T> {
    private:
        using Base = intrusive_ptr<T>;

        template <typename>
        friend class COW;
        template <typename, typename>
        friend class COWHelper;

        explicit mutable_ptr(T* ptr) : Base(ptr) {}

    public:
        /// Copy: not possible.
        mutable_ptr(const mutable_ptr&) = delete;

        /// Move: ok.
        mutable_ptr(mutable_ptr&&) = default;
        mutable_ptr& operator=(mutable_ptr&&) = default;

        /// Initializing from temporary of compatible type.
        template <typename U>
        mutable_ptr(mutable_ptr<U>&& other) : Base(std::move(other)) {}

        mutable_ptr() = default;

        mutable_ptr(std::nullptr_t) {}
    };

public:
    using MutablePtr = mutable_ptr<Derived>;

    unsigned int use_count() const { return ref_counter.load(); }

protected:
    template <typename T>
    class immutable_ptr : public intrusive_ptr<const T> {
    private:
        using Base = intrusive_ptr<const T>;

        template <typename>
        friend class COW;
        template <typename, typename>
        friend class COWHelper;

        explicit immutable_ptr(const T* ptr) : Base(ptr) {}

    public:
        /// Copy from immutable ptr: ok.
        immutable_ptr(const immutable_ptr&) = default;
        immutable_ptr& operator=(const immutable_ptr&) = default;

        template <typename U>
        immutable_ptr(const immutable_ptr<U>& other) : Base(other) {}

        /// Move: ok.
        immutable_ptr(immutable_ptr&&) = default;
        immutable_ptr& operator=(immutable_ptr&&) = default;

        /// Initializing from temporary of compatible type.
        template <typename U>
        immutable_ptr(immutable_ptr<U>&& other) : Base(std::move(other)) {}

        /// Move from mutable ptr: ok.
        template <typename U>
        immutable_ptr(mutable_ptr<U>&& other) : Base(std::move(other)) {}

        /// Copy from mutable ptr: not possible.
        template <typename U>
        immutable_ptr(const mutable_ptr<U>&) = delete;

        immutable_ptr() = default;

        immutable_ptr(std::nullptr_t) {}
    };

public:
    using Ptr = immutable_ptr<Derived>;

    template <typename... Args>
    static MutablePtr create(Args&&... args) {
        return MutablePtr(new Derived(std::forward<Args>(args)...));
    }

    template <typename T>
    static MutablePtr create(std::initializer_list<T>&& arg) {
        return create(std::forward<std::initializer_list<T>>(arg));
    }

public:
    Ptr get_ptr() const { return Ptr(derived()); }
    MutablePtr get_ptr() { return MutablePtr(derived()); }

protected:
    MutablePtr shallow_mutate() const {
        if (this->use_count() > 1) {
            return derived()->clone();
        } else {
            return assert_mutable();
        }
    }

public:
    MutablePtr mutate() const&& { return shallow_mutate(); }

    // Ownership assertion for callers that have already proved this object is
    // uniquely owned. This does not detach shared owners; use a type-specific
    // COW entry point (for example IColumn::mutate) when the pointer may be
    // shared.
    MutablePtr assert_mutable() const {
        if (this->use_count() > 1) {
            throw Exception(ErrorCode::INTERNAL_ERROR, "COW::assert_mutable: use_count() > 1");
        }
        return const_cast<COW*>(this)->get_ptr();
    }

    // Reference variant of assert_mutable(), with the same ownership contract.
    Derived& assert_mutable_ref() const {
        if (this->use_count() > 1) {
            throw Exception(ErrorCode::INTERNAL_ERROR, "COW::assert_mutable: use_count() > 1");
        }
        return const_cast<Derived&>(*derived());
    }

protected:
    /// It works as immutable_ptr if it is const and as mutable_ptr if it is non const.
    template <typename T>
    class chameleon_ptr {
    private:
        immutable_ptr<T> value;

    public:
        template <typename... Args>
        chameleon_ptr(Args&&... args) : value(std::forward<Args>(args)...) {}

        template <typename U>
        chameleon_ptr(std::initializer_list<U>&& arg)
                : value(std::forward<std::initializer_list<U>>(arg)) {}

        const T* get() const { return value.get(); }
        T* get() { return &static_cast<T&>(value->assert_mutable_ref()); }

        const T* operator->() const { return get(); }
        T* operator->() { return get(); }

        const T& operator*() const { return *value; }
        T& operator*() { return static_cast<T&>(value->assert_mutable_ref()); }

        operator const immutable_ptr<T>&() const { return value; }
        operator immutable_ptr<T>&() { return value; }

        operator bool() const { return value.get() != nullptr; }
        bool operator!() const { return value.get() == nullptr; }

        bool operator==(const chameleon_ptr& rhs) const { return value == rhs.value; }
        bool operator!=(const chameleon_ptr& rhs) const { return value != rhs.value; }
    };

public:
    /** Use this type in class members for compositions.
      *
      * NOTE:
      * For classes with WrappedPtr members,
      * you must reimplement 'mutate' method, so it will call 'mutate' of all subobjects (do deep mutate).
      * It will guarantee, that mutable object have all subobjects unshared.
      *
      * NOTE:
      * If you override 'mutate' method in inherited classes, don't forget to make it virtual in base class or to make it call a virtual method.
      * (COW itself doesn't force any methods to be virtual).
      *
      * See example in "cow_compositions.cpp".
      */
    using WrappedPtr = chameleon_ptr<Derived>;
};

/** Helper class to support inheritance.
  * Example:
  *
  * class IColumn : public COW<IColumn>
  * {
  *     friend class COW<IColumn>;
  *     virtual MutablePtr clone() const = 0;
  *     virtual ~IColumn() {}
  * };
  *
  * class ConcreteColumn : public COWHelper<IColumn, ConcreteColumn>
  * {
  *     friend class COWHelper<IColumn, ConcreteColumn>;
  * };
  *
  * Here is complete inheritance diagram:
  *
  * ConcreteColumn
  *  COWHelper<IColumn, ConcreteColumn>
  *   IColumn
  *    CowPtr<IColumn>
  *     boost::intrusive_ref_counter<IColumn>
  *
  * See example in "cow_columns.cpp".
  */
class IColumn;
class Field;
// 继承辅助类
// 专门配合 Doris 向量化列（IColumn）使用的辅助派生模板类。它简化了具体列类型（如 ColumnInt32、ColumnString 等）的样板代码，自动注入了针对 IColumn 的类型转换、数据切片选择（Selector）、多列插入等重载方法。
template <typename Base, typename Derived>
class COWHelper : public Base {
public:
    static_assert(std::is_base_of_v<doris::IColumn, Base>, "COWHelper only use in IColumn");
    using Ptr = typename Base::template immutable_ptr<Derived>;
    using MutablePtr = typename Base::template mutable_ptr<Derived>;
    using WrappedPtr = typename Base::template chameleon_ptr<Derived>;

#include "common/compile_check_avoid_begin.h"

    //This code uses templates, and errors like the following are likely to occur, mainly due to literal type mismatches:
    // be/src/vec/common/cow.h:409:39: warning: implicit conversion loses integer precision: 'int' to 'value_type' (aka 'unsigned char') [-Wimplicit-int-conversion]
    //   409 |         return MutablePtr(new Derived(std::forward<Args>(args)...));
    //       |                               ~~~~~~~ ^~~~~~~~~~~~~~~~~~~~~~~~
    // ColumnPtr res_data_column = ColumnUInt8::create(1, 1);
    template <typename... Args>
    static MutablePtr create(Args&&... args) {
        return MutablePtr(new Derived(std::forward<Args>(args)...));
    }
#include "common/compile_check_avoid_end.h"

    static Ptr cast_to_column_ptr(const Derived* raw_type_ptr) { return Ptr(raw_type_ptr); }

    static MutablePtr cast_to_column_mutptr(Derived* raw_type_ptr) {
        return MutablePtr(raw_type_ptr);
    }

    typename Base::MutablePtr clone() const override {
        return typename Base::MutablePtr(new Derived(static_cast<const Derived&>(*this)));
    }
    void append_data_by_selector(typename Base::MutablePtr& res,
                                 const typename Base::Selector& selector) const override {
        this->template append_data_by_selector_impl<Derived>(res, selector);
    }
    void insert_duplicate_fields(const Field& x, const size_t n) override {
        this->template insert_impl<Derived>(x, n);
    }

    void append_data_by_selector(typename Base::MutablePtr& res,
                                 const typename Base::Selector& selector, size_t begin,
                                 size_t end) const override {
        this->template append_data_by_selector_impl<Derived>(res, selector, begin, end);
    }

    void insert_from_multi_column(const std::vector<const IColumn*>& srcs,
                                  const std::vector<size_t>& positions) override {
        this->template insert_from_multi_column_impl<Derived>(srcs, positions);
    }

protected:
    MutablePtr shallow_mutate() const {
        return MutablePtr(static_cast<Derived*>(Base::shallow_mutate().get()));
    }
};
} // namespace doris
