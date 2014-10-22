
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <string>
#include <string.h>

#include "MemoryMappedFile.h"
#include "Exceptions.h"
#include "ScopeUtils.h"

namespace aeron { namespace common { namespace util {

MemoryMappedFile::ptr_t MemoryMappedFile::createNew(const char *filename, size_t size)
{
    return std::shared_ptr<MemoryMappedFile>(new MemoryMappedFile(filename, size));
}

MemoryMappedFile::ptr_t MemoryMappedFile::mapExisting(const char *filename)
{
    return std::shared_ptr<MemoryMappedFile>(new MemoryMappedFile(filename));
}

MemoryMappedFile::MemoryMappedFile(const char *filename, size_t size)
{
    int fd = open(filename, O_RDWR|O_CREAT, 0666);
    if (fd < 0)
        throw IOException(std::string ("Failed to create file: ") + filename, SOURCEINFO);

    m_memorySize = size;

    OnScopeExit tidy ([&]()
    {
        close(fd);
    });

    fill (fd, size, 0);
    m_memory = doMapping(size, fd);
    close(fd);
}

MemoryMappedFile::MemoryMappedFile(const char *filename)
{
    int fd = open(filename, O_RDWR, 0666);
    if (fd < 0)
        throw IOException(std::string ("Failed to open existing file: ") + filename, SOURCEINFO);

    OnScopeExit tidy ([&]()
    {
        close(fd);
    });

    struct stat statInfo;
    fstat(fd, &statInfo);

    m_memorySize = statInfo.st_size;
    m_memory = doMapping(m_memorySize, fd);
}

MemoryMappedFile::~MemoryMappedFile()
{
    if (m_memory && m_memorySize)
        munmap(m_memory, m_memorySize);
}

uint8_t* MemoryMappedFile::getMemoryPtr() const
{
    return m_memory;
}

size_t MemoryMappedFile::getMemorySize() const
{
    return m_memorySize;
}

uint8_t* MemoryMappedFile::doMapping(size_t size, int fd)
{
    void* memory = mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    if (memory == MAP_FAILED)
        throw IOException ("Failed to Memory Map File", SOURCEINFO);

    return static_cast<uint8_t*>(memory);
}

void MemoryMappedFile::fill(int fd, size_t size, uint8_t value)
{
    uint8_t buffer[PAGE_SIZE];
    memset(buffer, value, PAGE_SIZE);

    while (size >= PAGE_SIZE)
    {
        if (static_cast<size_t>(write(fd, buffer, PAGE_SIZE)) != PAGE_SIZE)
            throw IOException("Failed Writing to File", SOURCEINFO);
        size -= PAGE_SIZE;
    }

    if (size)
    {
        if (static_cast<size_t>(write(fd, buffer, size)) != size)
            throw IOException("Failed Writing to File", SOURCEINFO);
    }
}

}}}