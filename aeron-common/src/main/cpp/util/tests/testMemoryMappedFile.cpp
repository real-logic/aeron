#include <stdlib.h>
#include <gtest/gtest.h>
#include <string>

#include <util/MemoryMappedFile.h>

using namespace aeron::common::util;

std::string makeTempFileName ()
{
    char* rawname = tempnam(nullptr, "aeron");
    std::string name = rawname;
    free(rawname);

    return name;
}

TEST(mmfileTest, failToOpen)
{
    ASSERT_ANY_THROW({
        auto m = MemoryMappedFile::mapExisting("this file does no exist");
    });
}

TEST(mmfileTest, createCheck)
{
    MemoryMappedFile::ptr_t m;

    const size_t size = 10000;

    ASSERT_NO_THROW({
        m = MemoryMappedFile::createNew(makeTempFileName().c_str(), size);
    });

    ASSERT_EQ(m->getMemorySize(), size);
    ASSERT_NE(m->getMemoryPtr(), nullptr);

    for (size_t n = 0; n < size; n++)
    {
        ASSERT_EQ(m->getMemoryPtr()[n], 0);
    }
}

TEST(mmfileTest, writeReadCheck)
{
    MemoryMappedFile::ptr_t m;

    const size_t size = 10000;
    std::string name = makeTempFileName();

    ASSERT_NO_THROW({
        m = MemoryMappedFile::createNew(name.c_str(), size);
    });

    for (size_t n = 0; n < size; n++)
    {
        m->getMemoryPtr()[n] = static_cast<uint8_t>(n & 0xff);
    }

    m.reset();

    ASSERT_NO_THROW({
        m = MemoryMappedFile::mapExisting(name.c_str());
    });

    ASSERT_EQ(m->getMemorySize(), size);
    ASSERT_NE(m->getMemoryPtr(), nullptr);

    for (size_t n = 0; n < size; n++)
    {
        ASSERT_EQ(m->getMemoryPtr()[n], static_cast<uint8_t>(n & 0xff));
    }
}