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

#include "storage/segment/lazy_init_segment_iterator.h"

#include "storage/segment/segment_loader.h"

namespace doris::segment_v2 {

LazyInitSegmentIterator::LazyInitSegmentIterator(BetaRowsetSharedPtr rowset, int64_t segment_id,
                                                 bool should_use_cache, SchemaSPtr schema,
                                                 const StorageReadOptions& opts)
        : _rowset(std::move(rowset)),
          _segment_id(segment_id),
          _should_use_cache(should_use_cache),
          _schema(std::move(schema)),
          _read_options(opts) {}

/// See where the iterator is created in `BetaRowsetReader::get_segment_iterators`
// 通过 SegmentLoader 缓存加载 与 延迟实例化，将昂贵的物理 Segment 打开与索引加载动作推迟到最后一刻。
Status LazyInitSegmentIterator::init(const StorageReadOptions& opts) {
    // 将 _need_lazy_init 标志置为 false。若 _inner_iterator 已经被创建（例如被显式提前调用了 init），则直接返回 Status::OK()，防止重复加载 Segment。
    _need_lazy_init = false;
    if (_inner_iterator) {
        return Status::OK();
    }
    // 从 SegmentLoader 缓存句柄中加载物理 Segment
    std::shared_ptr<Segment> segment;
    {
        SegmentCacheHandle segment_cache_handle;
        // 调用全局单例 SegmentLoader::instance()->load_segment 获取 Segment。如果该 Segment 已经在内存 Cache（如 SegmentCache）中，直接命中缓存；若未命中，则从磁盘读取 Segment Header 并解包。
        RETURN_IF_ERROR(SegmentLoader::instance()->load_segment(
                _rowset, _segment_id, &segment_cache_handle, _should_use_cache, false, opts.stats,
                &opts.io_ctx));
        const auto& tmp_segments = segment_cache_handle.get_segments();
        segment = tmp_segments[0];
    }
    // 创建底层的物理迭代器 (SegmentIterator)
    RETURN_IF_ERROR(segment->new_iterator(_schema, _read_options, &_inner_iterator));
    return _inner_iterator->init(_read_options);
}

} // namespace doris::segment_v2
