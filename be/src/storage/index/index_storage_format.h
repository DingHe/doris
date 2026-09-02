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

#include "common/status.h"
#include "storage/index/inverted/inverted_index_common.h"

namespace doris::segment_v2 {

class IndexFileWriter;

// 记录待合并处理的倒排索引子文件的基础元数据结构。
class FileInfo {
public:
    // 索引子文件名称（例如 "10001_0.idx"）。
    std::string filename;
    // 索引子文件大小（字节数）。
    int64_t filesize;
};
// IndexStorageFormat 是 Apache Doris 存储层（Segment V2）中针对倒排索引（Inverted Index）存储文件格式写入的抽象基类（Abstract Base Class）。
// 在 Apache Doris 的倒排索引构建过程中，底层索引引擎（CLucene）会在本地目录中生成一系列子索引文件（如 .idx、.til、.tip 等）。为了优化文件系统句柄开销以及存储/读取效率，Doris 提供了不同的存储格式演进：
// 统一索引格式写入抽象：IndexStorageFormat 定义了将这些 CLucene 索引小文件打包合并写入 Doris 存储层文件系统的通用接口。
// V1 格式 (InvertedIndexStorageFormatV1)：将每个倒排索引列的子文件分别独立保存（或者按列分散存储）。
// V2 格式 (InvertedIndexStorageFormatV2)：将同一个 Segment 对应所有列的所有倒排索引子文件合并打包写入一个统一的倒排索引大文件（.idx）中，大幅减少存储层的文件句柄（File Descriptor）数量，提升海量数据列场景下的 IO 性能。
// 文件排序与数据拷贝：提供通用的索引子文件排序机制（按文件名字典序）与大块内存（Buffer）拷贝方法，确保在写入索引元数据和数据页时具有确定性的二进制布局。
class IndexStorageFormat {
public:
    IndexStorageFormat(IndexFileWriter* index_file_writer);
    virtual ~IndexStorageFormat() = default;
    // 倒排索引文件写入的核心执行入口
    // 在 V1 中，按照老版本协议逐列/逐文件输出。
    // 在 V2 中，遍历合并所有索引文件，写入合流的元数据 Compound Directory，生成统一的索引文件。
    virtual Status write() = 0;
    // 按文件名（filename）对传入的 file_infos 容器进行字典序排序（Lexicographical Order Sort）。
    // 应用场景：打包索引文件时，确保存储格式中的索引文件元数据索引表是有序的，以便读取时可以通过二分查找迅速定位指定文件偏移（Offset）。
    void sort_files(std::vector<FileInfo>& file_infos);
    // 扫描 CLucene 的 directory 目录，获取该目录下产生的所有子文件列表。
    // 提取每个文件的大小 filesize 并构建 FileInfo 对象。
    // 内部调用 sort_files 对列表进行字典序排序后返回。
    std::vector<FileInfo> prepare_sorted_files(lucene::store::Directory* directory);
    // dir: lucene::store::Directory*（源文件所在的 CLucene Directory 目录）。
    // output: lucene::store::IndexOutput*（目标写入流，通常对应 Doris 目标的复合索引大文件输出流）。
    // 高效的文件数据分块拷贝辅助函数。从 dir 中打开名为 fileName 的文件，使用传入的 buffer 将其数据内容批量读取并写入到复合索引文件的 output 流中。
    void copy_file(const char* fileName, lucene::store::Directory* dir,
                   lucene::store::IndexOutput* output, uint8_t* buffer, int64_t bufferLength);

protected:
    // 指向索引文件写入器 IndexFileWriter 的原生指针
    IndexFileWriter* _index_file_writer = nullptr;
};
using IndexStorageFormatPtr = std::unique_ptr<IndexStorageFormat>;

} // namespace doris::segment_v2