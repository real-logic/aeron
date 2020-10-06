/*
 * Copyright 2014-2020 Real Logic Limited.
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

#ifndef _WIN32
    #include <sys/mman.h>
    #include <sys/stat.h>
    #include <fcntl.h>
    #include <unistd.h>
#else
    #ifndef NOMINMAX
        #define NOMINMAX
    #endif // !NOMINMAX
    #include <Windows.h>
    #include "StringUtil.h"
#endif

#include <string>
#include <cstring>

#include "util/MemoryMappedFile.h"
#include "util/Exceptions.h"
#include "util/ScopeUtils.h"

namespace aeron { namespace util
{

#ifdef _WIN32
bool MemoryMappedFile::fill(FileHandle fd, std::size_t size, std::uint8_t value)
{
    std::uint8_t buffer[8196];
    memset(buffer, value, m_page_size);

    DWORD written = 0;

    while (size >= m_page_size)
    {
        if (!::WriteFile(fd.handle, buffer, (DWORD)m_page_size, &written, nullptr))
        {
            return false;
        }

        size -= written;
    }

    if (size)
    {
        if (!::WriteFile(fd.handle, buffer, (DWORD)size, &written, nullptr))
        {
            return false;
        }
    }

    return true;
}

MemoryMappedFile::ptr_t MemoryMappedFile::createNew(const char *filename, std::size_t offset, std::size_t size)
{
    FileHandle fd{};
    fd.handle = ::CreateFile(
        filename,
        GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE,
        nullptr,
        CREATE_ALWAYS,
        FILE_ATTRIBUTE_NORMAL,
        nullptr);

    if (INVALID_HANDLE_VALUE == fd.handle)
    {
        throw IOException(
            std::string("failed to create file: ") + filename + " " + toString(GetLastError()), SOURCEINFO);
    }

    OnScopeExit tidy(
        [&]()
        {
            if (INVALID_HANDLE_VALUE != fd.handle)
            {
                ::CloseHandle(fd.handle);
            }
        });

    if (!fill(fd, size, 0))
    {
        throw IOException(
            std::string("failed to write to file: ") + filename + " " + toString(GetLastError()), SOURCEINFO);
    }

    auto obj = MemoryMappedFile::ptr_t(new MemoryMappedFile(fd, offset, size, false));
    fd.handle = INVALID_HANDLE_VALUE;

    return obj;
}

MemoryMappedFile::ptr_t MemoryMappedFile::mapExisting(
    const char *filename, std::size_t offset, std::size_t size, bool readOnly)
{
    DWORD dwDesiredAccess = readOnly ? GENERIC_READ : (GENERIC_READ | GENERIC_WRITE);
    DWORD dwSharedMode = FILE_SHARE_READ | FILE_SHARE_WRITE;
    FileHandle fd{};
    fd.handle = ::CreateFile(
        filename,
        dwDesiredAccess,
        dwSharedMode,
        nullptr,
        OPEN_EXISTING,
        FILE_ATTRIBUTE_NORMAL,
        nullptr);

    if (INVALID_HANDLE_VALUE == fd.handle)
    {
        throw IOException(
            std::string("failed to create file: ") + filename + " " + toString(GetLastError()), SOURCEINFO);
    }

    OnScopeExit tidy(
        [&]()
        {
            if (INVALID_HANDLE_VALUE != fd.handle)
            {
                ::CloseHandle(fd.handle);
            }
        });

    auto obj = MemoryMappedFile::ptr_t(new MemoryMappedFile(fd, offset, size, readOnly));
    fd.handle = INVALID_HANDLE_VALUE;

    return obj;
}
#else
bool MemoryMappedFile::fill(FileHandle fd, std::size_t size, std::uint8_t value)
{
    std::unique_ptr<std::uint8_t[]> buffer(new std::uint8_t[m_page_size]);
    memset(buffer.get(), value, m_page_size);

    while (size >= m_page_size)
    {
        if (static_cast<std::size_t>(::write(fd.handle, buffer.get(), m_page_size)) != m_page_size)
        {
            return false;
        }

        size -= m_page_size;
    }

    if (size)
    {
        if (static_cast<std::size_t>(::write(fd.handle, buffer.get(), size)) != size)
        {
            return false;
        }
    }

    return true;
}

MemoryMappedFile::ptr_t MemoryMappedFile::createNew(const char *filename, off_t offset, std::size_t size)
{
    FileHandle fd{};
    fd.handle = ::open(filename, O_RDWR | O_CREAT, 0666);

    if (fd.handle < 0)
    {
        throw IOException(std::string("failed to create file: ") + filename, SOURCEINFO);
    }

    OnScopeExit tidy(
        [&]()
        {
            close(fd.handle);
        });

    if (!fill(fd, size, 0))
    {
        throw IOException(std::string("failed to write to file: ") + filename, SOURCEINFO);
    }

    return MemoryMappedFile::ptr_t(new MemoryMappedFile(fd, offset, size, false));
}

MemoryMappedFile::ptr_t MemoryMappedFile::mapExisting(
    const char *filename, off_t offset, std::size_t length, bool readOnly)
{
    FileHandle fd{};
    fd.handle = ::open(filename, (readOnly ? O_RDONLY : O_RDWR), 0666);

    if (fd.handle < 0)
    {
        throw IOException(std::string("failed to open existing file: ") + filename, SOURCEINFO);
    }

    OnScopeExit tidy(
        [&]()
        {
            close(fd.handle);
        });

    return MemoryMappedFile::ptr_t(new MemoryMappedFile(fd, offset, length, readOnly));
}
#endif

MemoryMappedFile::ptr_t MemoryMappedFile::mapExisting(const char *filename, bool readOnly)
{
    return mapExisting(filename, 0, 0, readOnly);
}

std::uint8_t *MemoryMappedFile::getMemoryPtr() const
{
    return m_memory;
}

std::size_t MemoryMappedFile::getMemorySize() const
{
    return m_memorySize;
}

std::size_t MemoryMappedFile::m_page_size = getPageSize();

#ifdef _WIN32
MemoryMappedFile::MemoryMappedFile(FileHandle fd, std::size_t offset, std::size_t length, bool readOnly)
{
    m_file = fd.handle;

    if (0 == length && 0 == offset)
    {
        LARGE_INTEGER fileSize;
        if (!::GetFileSizeEx(fd.handle, &fileSize))
        {
            cleanUp();
            throw IOException(
                std::string("failed query size of existing file: ") + toString(GetLastError()), SOURCEINFO);
        }

        length = static_cast<std::size_t>(fileSize.QuadPart);
    }

    m_memorySize = length;
    m_memory = doMapping(m_memorySize, fd, offset, readOnly);

    if (!m_memory)
    {
        cleanUp();
        throw IOException(std::string("failed to Map Memory: ") + toString(GetLastError()), SOURCEINFO);
    }
}

void MemoryMappedFile::cleanUp()
{
    if (m_memory)
    {
        ::UnmapViewOfFile(m_memory);
        m_memory = nullptr;
    }

    if (m_mapping)
    {
        ::CloseHandle(m_mapping);
        m_mapping = nullptr;
    }

    if (m_file)
    {
        ::CloseHandle(m_file);
        m_file = nullptr;
    }
}

MemoryMappedFile::~MemoryMappedFile()
{
    cleanUp();
}

uint8_t *MemoryMappedFile::doMapping(std::size_t size, FileHandle fd, std::size_t offset, bool readOnly)
{
    DWORD flProtect = readOnly ? PAGE_READONLY : PAGE_READWRITE;
    m_mapping = ::CreateFileMapping(fd.handle, nullptr, flProtect, 0, static_cast<DWORD>(size), nullptr);
    if (nullptr == m_mapping)
    {
        return nullptr;
    }

    DWORD dwDesiredAccess = readOnly ? FILE_MAP_READ : FILE_MAP_ALL_ACCESS;
    void *memory = (LPTSTR)::MapViewOfFile(m_mapping, dwDesiredAccess, 0, static_cast<DWORD>(offset), size);

    return static_cast<std::uint8_t *>(memory);
}

std::size_t MemoryMappedFile::getPageSize() noexcept
{
    SYSTEM_INFO system_info;
    ::GetSystemInfo(&system_info);

    return static_cast<std::size_t>(system_info.dwPageSize);
}

std::int64_t MemoryMappedFile::getFileSize(const char *filename)
{
    WIN32_FILE_ATTRIBUTE_DATA fad;
    if (::GetFileAttributesEx(filename, GetFileExInfoStandard, &fad) == 0)
    {
        return -1;
    }

    LARGE_INTEGER size;
    size.HighPart = fad.nFileSizeHigh;
    size.LowPart = fad.nFileSizeLow;

    return size.QuadPart;
}

#else
MemoryMappedFile::MemoryMappedFile(FileHandle fd, off_t offset, std::size_t length, bool readOnly)
{
    if (0 == length && 0 == offset)
    {
        struct stat statInfo{};
        ::fstat(fd.handle, &statInfo);
        length = static_cast<std::size_t>(statInfo.st_size);
    }

    m_memorySize = length;
    m_memory = doMapping(m_memorySize, fd, static_cast<std::size_t>(offset), readOnly);
}

MemoryMappedFile::~MemoryMappedFile()
{
    if (m_memory && m_memorySize)
    {
        ::munmap(m_memory, m_memorySize);
    }
}

std::uint8_t *MemoryMappedFile::doMapping(std::size_t length, FileHandle fd, std::size_t offset, bool readOnly)
{
    void *memory = ::mmap(
        nullptr,
        length,
        readOnly ? PROT_READ : (PROT_READ | PROT_WRITE),
        MAP_SHARED,
        fd.handle,
        static_cast<off_t>(offset));

    if (MAP_FAILED == memory)
    {
        throw IOException("failed to Memory Map file", SOURCEINFO);
    }

    return static_cast<std::uint8_t *>(memory);
}

std::size_t MemoryMappedFile::getPageSize() noexcept
{
    return static_cast<std::size_t>(::getpagesize());
}

std::int64_t MemoryMappedFile::getFileSize(const char *filename)
{
    struct stat statInfo{};

    if (::stat(filename, &statInfo) < 0)
    {
        return -1;
    }

    return statInfo.st_size;
}

#endif

}}
