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

#ifndef _WIN32
    #include <sys/mman.h>
    #include <sys/types.h>
    #include <sys/stat.h>
    #include <fcntl.h>
    #include <unistd.h>
#endif

#include <string>
#include <string.h>

#include "MemoryMappedFile.h"
#include "Exceptions.h"
#include "ScopeUtils.h"
#include "StringUtil.h"

namespace aeron { namespace util {

MemoryMappedFile::ptr_t MemoryMappedFile::createNew(const char *filename, size_t size)
{
    return std::shared_ptr<MemoryMappedFile>(new MemoryMappedFile(filename, size));
}

MemoryMappedFile::ptr_t MemoryMappedFile::mapExisting(const char *filename)
{
    return std::shared_ptr<MemoryMappedFile>(new MemoryMappedFile(filename));
}

MemoryMappedFile::ptr_t MemoryMappedFile::mapExisting(const char *filename, size_t offset, size_t length)
{
    return std::shared_ptr<MemoryMappedFile>(new MemoryMappedFile(filename, offset, length));
}

uint8_t* MemoryMappedFile::getMemoryPtr() const
{
    return m_memory;
}

size_t MemoryMappedFile::getMemorySize() const
{
    return m_memorySize;
}

size_t MemoryMappedFile::PAGE_SIZE = getPageSize();

MemoryMappedFile::MemoryMappedFile(const char *filename)
    : MemoryMappedFile(filename, 0, 0)
{
}

#ifdef _WIN32
MemoryMappedFile::MemoryMappedFile(const char *filename, size_t size)
    : m_file(NULL), m_mapping(NULL), m_memory(NULL)
{
    FileHandle fd;
    fd.handle = CreateFile(filename, GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE, NULL, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);

    if (fd.handle == INVALID_HANDLE_VALUE)
    {
        throw IOException(std::string("Failed to create file: ") + filename + " " + toString(GetLastError()), SOURCEINFO);
    }

    if (!fill(fd, size, 0))
    {
        cleanUp();
        throw IOException(std::string("Failed to write to file: ") + filename + " " + toString(GetLastError()), SOURCEINFO);
    }

    m_memorySize = size;
    m_memory = doMapping(m_memorySize, fd, 0);

    if (!m_memory)
    {
        cleanUp();
        throw IOException(std::string("Failed to Map Memory: ") + filename + " " + toString(GetLastError()), SOURCEINFO);
    }
}

MemoryMappedFile::MemoryMappedFile(const char *filename, size_t offset, size_t length)
    : m_file(NULL), m_mapping(NULL)
{
    FileHandle fd;
    fd.handle = CreateFile(filename, GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);

    if (fd.handle == INVALID_HANDLE_VALUE)
    {
        throw IOException(std::string("Failed to open existing file: ") + filename + " " + toString(GetLastError()), SOURCEINFO);
    }

    if (0 == length && 0 == offset)
    {
        LARGE_INTEGER fileSize;
        if (!GetFileSizeEx(fd.handle, &fileSize))
        {
            cleanUp();
            throw IOException(std::string("Failed query size of existing file: ") + filename + " " + toString(GetLastError()), SOURCEINFO);
        }

        length = (size_t)fileSize.QuadPart;
    }

    m_memorySize = length;
    m_memory = doMapping(m_memorySize, fd, offset);

    if (!m_memory)
    {
        cleanUp();
        throw IOException(std::string("Failed to Map Memory: ") + filename + " " + toString(GetLastError()), SOURCEINFO);
    }
}

void MemoryMappedFile::cleanUp()
{
    if (m_file)
        CloseHandle(m_file);

    if (m_memory)
        UnmapViewOfFile(m_memory);

    if (m_mapping)
        CloseHandle(m_mapping);
}

MemoryMappedFile::~MemoryMappedFile()
{
    cleanUp();
}

uint8_t* MemoryMappedFile::doMapping(size_t size, FileHandle fd, size_t offset)
{
    m_mapping = CreateFileMapping(fd.handle, NULL, PAGE_READWRITE, 0, size, NULL);
    if (m_mapping == NULL)
    {
        return NULL;
    }

    void* memory = (LPTSTR)MapViewOfFile(m_mapping, FILE_MAP_ALL_ACCESS, 0,	offset, size);

    return static_cast<uint8_t*>(memory);
}

bool MemoryMappedFile::fill(FileHandle fd, size_t size, uint8_t value)
{
    uint8_t buffer[8196];
    memset(buffer, value, PAGE_SIZE);

    DWORD written = 0;

    while (size >= PAGE_SIZE)
    {
        if (!WriteFile(fd.handle, buffer, PAGE_SIZE, &written, NULL))
        {
            return false;
        }

        size -= written;
    }

    if (size)
    {
        if (!WriteFile(fd.handle, buffer, size, &written, NULL))
        {
            return false;
        }
    }
    return true;
}

size_t MemoryMappedFile::getPageSize()
{
    SYSTEM_INFO sinfo;

    ::GetSystemInfo(&sinfo);
    return static_cast<size_t>(sinfo.dwPageSize);
}

std::int64_t MemoryMappedFile::getFileSize(const char *filename)
{
    WIN32_FILE_ATTRIBUTE_DATA info;

    if (::GetFileAttributesEx(filename, GetFileExInfoStandard, &info) == 0)
    {
        return -1;
    }

    return (info.nFileSizeHigh << 32) | (info.nFileSizeLow);
}

#else
MemoryMappedFile::MemoryMappedFile(const char *filename, size_t length)
{
    FileHandle fd;
    fd.handle = open(filename, O_RDWR|O_CREAT, 0666);
    if (fd.handle < 0)
    {
        throw IOException(std::string("Failed to create file: ") + filename, SOURCEINFO);
    }

    m_memorySize = length;

    OnScopeExit tidy ([&]()
    {
        close(fd.handle);
    });

    if (!fill(fd, length, 0))
    {
        throw IOException(std::string("Failed to write to file: ") + filename, SOURCEINFO);
    }

    m_memory = doMapping(length, fd, 0);
}

MemoryMappedFile::MemoryMappedFile(const char *filename, size_t length, size_t offset)
{
    FileHandle fd;
    fd.handle = ::open(filename, O_RDWR, 0666);
    if (fd.handle < 0)
    {
        throw IOException(std::string("Failed to open existing file: ") + filename, SOURCEINFO);
    }

    OnScopeExit tidy([&]()
    {
        close(fd.handle);
    });

    if (0 == length && 0 == offset)
    {
        struct stat statInfo;
        ::fstat(fd.handle, &statInfo);
        length = statInfo.st_size;
    }

    m_memorySize = length;
    m_memory = doMapping(m_memorySize, fd, offset);
}

MemoryMappedFile::~MemoryMappedFile()
{
    if (m_memory && m_memorySize)
    {
        munmap(m_memory, m_memorySize);
    }
}

uint8_t* MemoryMappedFile::doMapping(size_t length, FileHandle fd, size_t offset)
{
    void* memory = ::mmap(NULL, length, PROT_READ|PROT_WRITE, MAP_SHARED, fd.handle, static_cast<off_t>(offset));

    if (MAP_FAILED == memory)
    {
        throw IOException("Failed to Memory Map File", SOURCEINFO);
    }

    return static_cast<uint8_t*>(memory);
}

bool MemoryMappedFile::fill(FileHandle fd, size_t size, uint8_t value)
{
    uint8_t buffer[PAGE_SIZE];
    memset(buffer, value, PAGE_SIZE);

    while (size >= PAGE_SIZE)
    {
        if (static_cast<size_t>(write(fd.handle, buffer, PAGE_SIZE)) != PAGE_SIZE)
        {
            return false;
        }

        size -= PAGE_SIZE;
    }

    if (size)
    {
        if (static_cast<size_t>(write(fd.handle, buffer, size)) != size)
        {
            return false;
        }
    }
    return true;
}

size_t MemoryMappedFile::getPageSize()
{
    return static_cast<size_t>(::getpagesize());
}

std::int64_t MemoryMappedFile::getFileSize(const char *filename)
{
    struct stat statInfo;

    if (::stat(filename, &statInfo) < 0)
    {
        return -1;
    }

    return statInfo.st_size;
}

#endif

}}