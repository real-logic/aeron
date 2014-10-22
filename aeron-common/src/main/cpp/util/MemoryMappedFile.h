#ifndef INCLUDED_AERON_UTIL_MEMORY_MAPPED_FILE__
#define INCLUDED_AERON_UTIL_MEMORY_MAPPED_FILE__

#include <cstdint>
#include <memory>

namespace aeron { namespace common { namespace util {

class MemoryMappedFile
{
public:
    typedef std::shared_ptr<MemoryMappedFile> ptr_t;

    static ptr_t createNew (const char* filename, size_t size);
    static ptr_t mapExisting (const char* filename);

    ~MemoryMappedFile ();

    uint8_t* getMemoryPtr() const;
    size_t getMemorySize() const;

    MemoryMappedFile(MemoryMappedFile const&) = delete;
    MemoryMappedFile& operator=(MemoryMappedFile const&) = delete;

private:
    MemoryMappedFile (const char* filename, size_t size);
    MemoryMappedFile (const char* filename);

    void fill (int fd, size_t sz, std::uint8_t);
    uint8_t* doMapping(size_t size, int fd);

    std::uint8_t* m_memory = 0;
    size_t m_memorySize = 0;
    const size_t PAGE_SIZE = 4096; // TODO: Get this programaticaly?
};

}}}

#endif