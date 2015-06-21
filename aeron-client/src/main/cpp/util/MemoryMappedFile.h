/*
 * Copyright 2014 - 2015 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef INCLUDED_AERON_UTIL_MEMORY_MAPPED_FILE__
#define INCLUDED_AERON_UTIL_MEMORY_MAPPED_FILE__

#include <cstdint>
#include <memory>

#ifdef _WIN32
#include <windows.h>
#endif


namespace aeron { namespace util {

class MemoryMappedFile
{
public:
    typedef std::shared_ptr<MemoryMappedFile> ptr_t;

    static ptr_t createNew(const char* filename, size_t length);
    static ptr_t mapExisting(const char* filename);
    static ptr_t mapExisting(const char *filename, size_t offset, size_t length);

    ~MemoryMappedFile ();

    uint8_t* getMemoryPtr() const;
    size_t getMemorySize() const;

    MemoryMappedFile(MemoryMappedFile const&) = delete;
    MemoryMappedFile& operator=(MemoryMappedFile const&) = delete;

    // some OS specific utility methods
    static size_t getPageSize();
    static std::int64_t getFileSize(const char *filename);

private:
    MemoryMappedFile(const char* filename, size_t length);
    MemoryMappedFile(const char* filename);
    MemoryMappedFile(const char* filename, size_t offset, size_t length);

    struct FileHandle
    {
#ifdef _WIN32
        HANDLE handle;
#else
        int handle;
#endif
    };

    bool fill(FileHandle fd, size_t sz, std::uint8_t);
    uint8_t* doMapping(size_t size, FileHandle fd, size_t offset);

    std::uint8_t* m_memory = 0;
    size_t m_memorySize = 0;
#if !defined(PAGE_SIZE)
    static size_t PAGE_SIZE;
#endif

#ifdef _WIN32
    HANDLE m_file = NULL;
    HANDLE m_mapping = NULL;
    void cleanUp();
#endif

};

}}

#endif