
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

namespace aeron { namespace common { namespace util {

MemoryMappedFile::ptr_t MemoryMappedFile::createNew(const char *filename, size_t size)
{
    return std::shared_ptr<MemoryMappedFile>(new MemoryMappedFile(filename, size));
}

MemoryMappedFile::ptr_t MemoryMappedFile::mapExisting(const char *filename)
{
    return std::shared_ptr<MemoryMappedFile>(new MemoryMappedFile(filename));
}

uint8_t* MemoryMappedFile::getMemoryPtr() const
{
	return m_memory;
}

size_t MemoryMappedFile::getMemorySize() const
{
	return m_memorySize;
}

#ifdef _WIN32
MemoryMappedFile::MemoryMappedFile(const char *filename, size_t size)
{
}

MemoryMappedFile::MemoryMappedFile(const char *filename)
	: m_file(NULL), m_mapping(NULL)
{
	FileHandle fd;
	fd.handle = CreateFile(filename, GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);

	if (fd.handle == INVALID_HANDLE_VALUE)
	{
		DWORD err = GetLastError();
		throw IOException(std::string("Failed to open existing file: ") + filename + " " + toString(err) , SOURCEINFO);
	}

	LARGE_INTEGER fileSize;
	GetFileSizeEx(m_file, &fileSize);
		 
	m_memorySize = (size_t)fileSize.QuadPart;

	m_memory = doMapping(m_memorySize, fd);


}

MemoryMappedFile::~MemoryMappedFile()
{
	CloseHandle(m_file);
	UnmapViewOfFile(m_memory);
	CloseHandle(m_mapping);
}

uint8_t* MemoryMappedFile::doMapping(size_t size, FileHandle fd)
{
	m_mapping = CreateFileMapping(fd.handle, NULL, PAGE_READWRITE, 0, size, NULL);                 
	if (m_mapping == NULL)
		throw IOException("Failed to Memory Map File", SOURCEINFO);

	void* memory = (LPTSTR)MapViewOfFile(m_mapping, FILE_MAP_ALL_ACCESS, 0,	0, size);
	if (memory == NULL)
		throw IOException("Failed to Memory Map File", SOURCEINFO);

	return static_cast<uint8_t*>(memory);
}

void MemoryMappedFile::fill(FileHandle fd, size_t size, uint8_t value)
{
// 	uint8_t buffer[PAGE_SIZE];
// 	memset(buffer, value, PAGE_SIZE);
// 
// 	while (size >= PAGE_SIZE)
// 	{
// 		if (static_cast<size_t>(write(fd, buffer, PAGE_SIZE)) != PAGE_SIZE)
// 			throw IOException("Failed Writing to File", SOURCEINFO);
// 		size -= PAGE_SIZE;
// 	}
// 
// 	if (size)
// 	{
// 		if (static_cast<size_t>(write(fd, buffer, size)) != size)
// 			throw IOException("Failed Writing to File", SOURCEINFO);
// 	}
}

#else
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

uint8_t* MemoryMappedFile::doMapping(size_t size, FileHandle fd)
{
    void* memory = mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    if (memory == MAP_FAILED)
        throw IOException ("Failed to Memory Map File", SOURCEINFO);

    return static_cast<uint8_t*>(memory);
}

void MemoryMappedFile::fill(FileHandle fd, size_t size, uint8_t value)
{
    uint8_t buffer[PAGE_SIZE];
    memset(buffer, value, PAGE_SIZE);

    while (size >= PAGE_SIZE)
    {
        if (static_cast<size_t>(write(fd.handle, buffer, PAGE_SIZE)) != PAGE_SIZE)
            throw IOException("Failed Writing to File", SOURCEINFO);
        size -= PAGE_SIZE;
    }

    if (size)
    {
        if (static_cast<size_t>(write(fd.handle, buffer, size)) != size)
            throw IOException("Failed Writing to File", SOURCEINFO);
    }
}

#endif

}}}