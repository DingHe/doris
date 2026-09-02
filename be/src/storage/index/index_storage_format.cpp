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

#include "storage/index/index_storage_format.h"

#include "common/cast_set.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/inverted/inverted_index_fs_directory.h"
#include "util/debug_points.h"

namespace doris::segment_v2 {
IndexStorageFormat::IndexStorageFormat(IndexFileWriter* index_file_writer)
        : _index_file_writer(index_file_writer) {}

// 规定倒排索引子文件在最终合成的复合文件（Compound File）中的存储物理顺序。
void IndexStorageFormat::sort_files(std::vector<FileInfo>& file_infos) {
    // 根据文件名后缀/特征计算文件的写入优先级（Priority）
    auto file_priority = [](const std::string& filename) {
        // 遍历 InvertedIndexDescriptor::index_file_info_map（映射表，维护了各类 CLucene 子文件后缀对应的优先级序号，数值越小优先级越高）。
        for (const auto& entry : InvertedIndexDescriptor::index_file_info_map) {
            // 如果 filename 中包含某个已知后缀（例如 .tis, .tii, .frq, .prx 等），则返回对应的优先级整数。
            if (filename.find(entry.first) != std::string::npos) {
                return entry.second;
            }
        }
        // 若未匹配到任何预定义类型，则返回 std::numeric_limits<int32_t>::max()（即最大整数，优先级最低，排在最后）。
        return std::numeric_limits<int32_t>::max(); // Other files
    };
    // 比较函数按照 双重排序规则（二级排序） 进行：
    std::sort(file_infos.begin(), file_infos.end(), [&](const FileInfo& a, const FileInfo& b) {
        int32_t priority_a = file_priority(a.filename);
        int32_t priority_b = file_priority(b.filename);
        // 第一优先级：文件类型优先级 (priority)
        if (priority_a != priority_b) {
            return priority_a < priority_b;
        }
        // 第二优先级：文件大小 (filesize)
        return a.filesize < b.filesize;
    });
}
// 负责提取、过滤并按规则排序倒排索引目录内子文件的核心方法。
std::vector<FileInfo> IndexStorageFormat::prepare_sorted_files(
        lucene::store::Directory* directory) {
    // 获取文件列表：调用 CLucene 的 Directory::list() 接口，读取当前索引目录中保存的所有子文件文件名，装入 files 容器中。
    std::vector<std::string> files;
    directory->list(&files);

    // Remove write.lock file
    // CLucene 在写入索引时会产生写锁文件（通常为 write.lock），防止多线程/多进程并发写入破坏文件。
    // 该锁文件只在写阶段有效，不需要打包合并到最终的索引复合文件（Compound File）中。
    // 此处使用标准的 C++ std::remove 配合容器 erase 方法，将锁文件从待处理列表中彻底剔除。
    files.erase(std::remove(files.begin(), files.end(), DorisFSDirectory::WRITE_LOCK_FILE),
                files.end());

    std::vector<FileInfo> sorted_files;
    for (const auto& file : files) {
        FileInfo file_info;
        file_info.filename = file;
        file_info.filesize = directory->fileLength(file.c_str());
        sorted_files.push_back(std::move(file_info));
    }

    // Sort the files
    sort_files(sorted_files);
    return sorted_files;
}

void IndexStorageFormat::copy_file(const char* fileName, lucene::store::Directory* dir,
                                   lucene::store::IndexOutput* output, uint8_t* buffer,
                                   int64_t bufferLength) {
    try {
        CLuceneError err;
        std::unique_ptr<lucene::store::IndexInput> input(dir->openInput(fileName));
        DBUG_EXECUTE_IF("IndexFileWriter::copyFile_openInput_error", {
            err.set(CL_ERR_IO, "debug point: copyFile_openInput_error");
            throw err;
        });
        int64_t start_ptr = output->getFilePointer();
        int64_t length = input->length();
        int64_t remainder = length;
        int64_t chunk = bufferLength;

        while (remainder > 0) {
            auto len = cast_set<int32_t>(std::min({chunk, length, remainder}));
            input->readBytes(buffer, len);
            output->writeBytes(buffer, len);
            remainder -= len;
        }
        DBUG_EXECUTE_IF("IndexFileWriter::copyFile_remainder_is_not_zero", { remainder = 10; });
        if (remainder != 0) {
            std::ostringstream errMsg;
            errMsg << "Non-zero remainder length after copying: " << remainder
                   << " (id: " << fileName << ", length: " << length << ", buffer size: " << chunk
                   << ")";
            err.set(CL_ERR_IO, errMsg.str().c_str());
            throw err;
        }

        int64_t end_ptr = output->getFilePointer();
        int64_t diff = end_ptr - start_ptr;
        DBUG_EXECUTE_IF("IndexFileWriter::copyFile_diff_not_equals_length",
                        { diff = length - 10; });
        if (diff != length) {
            std::ostringstream errMsg;
            errMsg << "Difference in the output file offsets " << diff
                   << " does not match the original file length " << length;
            err.set(CL_ERR_IO, errMsg.str().c_str());
            throw err;
        }
        input->close();
    } catch (const CLuceneError& e) {
        if (e.number() == CL_ERR_EmptyIndexSegment) {
            LOG(WARNING) << "InvertedIndexFileWriter::copyFile: " << fileName << " is empty";
            return;
        } else {
            throw e;
        }
    }
}

} // namespace doris::segment_v2
