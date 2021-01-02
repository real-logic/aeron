/*
 * Copyright 2014-2021 Real Logic Limited.
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

#define _DISABLE_EXTENDED_ALIGNED_STORAGE

#include "Aeron.h"

#if defined (_WIN32)
    #ifndef NOMINMAX
        #define NOMINMAX
    #endif // !NOMINMAX
    #include <Windows.h>
#endif

using namespace aeron;
using namespace aeron::util;

bool Context::requestDriverTermination(
    const std::string &directory, const std::uint8_t *tokenBuffer, std::size_t tokenLength)
{
    auto minLength = static_cast<std::int64_t>(CncFileDescriptor::META_DATA_LENGTH);
    const std::string cncFilename = directory + std::string(1, AERON_FILE_SEP) + CncFileDescriptor::CNC_FILE;

    if (MemoryMappedFile::getFileSize(cncFilename.c_str()) > minLength)
    {
        MemoryMappedFile::ptr_t cncFile = MemoryMappedFile::mapExisting(cncFilename.c_str());
        std::size_t fileLength = cncFile->getMemorySize();
        if (fileLength > static_cast<std::size_t>(minLength))
        {
            const std::int32_t cncVersion = CncFileDescriptor::cncVersionVolatile(cncFile);
            if (semanticVersionMajor(cncVersion) != semanticVersionMajor(CncFileDescriptor::CNC_VERSION))
            {
                throw AeronException(
                    "Aeron CnC version does not match:"
                    " app=" + semanticVersionToString(CncFileDescriptor::CNC_VERSION) +
                    " file=" + semanticVersionToString(cncVersion),
                    SOURCEINFO);
            }
            
            if (!CncFileDescriptor::isCncFileLengthSufficient(cncFile))
            {
                throw AeronException(
                    "Aeron CnC file length not sufficient: length=" + std::to_string(fileLength), SOURCEINFO);
            }

            AtomicBuffer toDriverBuffer(CncFileDescriptor::createToDriverBuffer(cncFile));
            ManyToOneRingBuffer ringBuffer(toDriverBuffer);
            DriverProxy driverProxy(ringBuffer);

            driverProxy.terminateDriver(tokenBuffer, tokenLength);

            return true;
        }
    }

    return false;
}

#if !defined(__linux__)
inline static std::string tmpDir()
{
#if defined(_MSC_VER)
    static char buff[MAX_PATH + 1];
    std::string dir;

    if (::GetTempPath(MAX_PATH, &buff[0]) > 0)
    {
        dir = buff;
    }

    return dir;
#else
    std::string dir = "/tmp";

    if (::getenv("TMPDIR"))
    {
        dir = ::getenv("TMPDIR");
    }

    return dir;
#endif
}
#endif

inline static std::string getUserName()
{
    const char *username = ::getenv("USER");
#if (_MSC_VER)
    if (nullptr == username)
    {
        username = ::getenv("USERNAME");
        if (nullptr == username)
        {
            username = "default";
        }
    }
#else
    if (nullptr == username)
    {
        username = "default";
    }
#endif
    return username;
}

std::string Context::defaultAeronPath()
{
#if defined(__linux__)
    return "/dev/shm/aeron-" + getUserName();
#elif (_MSC_VER)
    return tmpDir() + "aeron-" + getUserName();
#else
    return tmpDir() + "/aeron-" + getUserName();
#endif
}
