/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef AERON_UTIL_MEMORY_MAPPED_FILE_H
#define AERON_UTIL_MEMORY_MAPPED_FILE_H

#include <cstdint>
#include <memory>
#include "util/Export.h"

#ifdef _WIN32
#include <cstddef>
typedef void * HANDLE;
#else
#include <sys/types.h>
#endif

namespace aeron { namespace util
{

class CLIENT_EXPORT MemoryMappedFile
{
public:
    typedef std::shared_ptr<MemoryMappedFile> ptr_t;

#ifdef _WIN32
    static ptr_t createNew(const char *filename, std::size_t offset, std::size_t length, bool preTouch);
    static ptr_t mapExisting(
        const char *filename, std::size_t offset, std::size_t length, bool readOnly = false, bool preTouch = false);
#else
    static ptr_t createNew(const char *filename, off_t offset, std::size_t length, bool preTouch);
    static ptr_t mapExisting(
        const char *filename, off_t offset, std::size_t length, bool readOnly = false, bool preTouch = false);
#endif

    static ptr_t mapExisting(const char *filename, bool readOnly = false, bool preTouch = false);

    inline static ptr_t mapExistingReadOnly(const char *filename)
    {
        return mapExisting(filename, 0, 0, true, false);
    }

    ~MemoryMappedFile();

    std::uint8_t *getMemoryPtr() const;
    std::size_t getMemorySize() const;

    MemoryMappedFile(MemoryMappedFile const &) = delete;
    MemoryMappedFile& operator=(MemoryMappedFile const &) = delete;

    static std::size_t getPageSize() noexcept;
    static std::int64_t getFileSize(const char *filename);

private:
    struct FileHandle
    {
#ifdef _WIN32
        HANDLE handle;
#else
        int handle = -1;
#endif
    };

#ifdef _WIN32
    MemoryMappedFile(FileHandle fd, std::size_t offset, std::size_t length, bool readOnly, bool preTouch);
#else
    MemoryMappedFile(FileHandle fd, off_t offset, std::size_t length, bool readOnly, bool preTouch);
#endif

    std::uint8_t *doMapping(std::size_t size, FileHandle fd, std::size_t offset, bool readOnly, bool preTouch);

    std::uint8_t *m_memory = nullptr;
    std::size_t m_memorySize = 0;
    static std::size_t m_page_size;
    static bool fill(FileHandle fd, std::size_t sz, std::uint8_t value);

#ifdef _WIN32
    HANDLE m_file = nullptr;
    HANDLE m_mapping = nullptr;
    void cleanUp();
#endif

};

}}

#endif
