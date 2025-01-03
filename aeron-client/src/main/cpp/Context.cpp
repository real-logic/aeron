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

#define _DISABLE_EXTENDED_ALIGNED_STORAGE

#include "Aeron.h"

#if defined (_WIN32)
    #ifndef NOMINMAX
        #define NOMINMAX
    #endif // !NOMINMAX
    #include <Windows.h>
#else
#include <unistd.h>
#include <sys/types.h>
#include <pwd.h>
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

            if (cncVersion > 0)
            {
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
    }

    return false;
}

#if !defined(__linux__)
inline static std::string tmpDir()
{
#if defined(_MSC_VER)
    char buff[MAX_PATH + 1];
    std::string dir;

    if (::GetTempPath(MAX_PATH, &buff[0]) > 0)
    {
        dir = buff;
    }

    return dir;
#else
    std::string dir = "/tmp";
    const char *tmpDir = ::getenv("TMPDIR");

    if (nullptr != tmpDir)
    {
        dir = tmpDir;
    }

    return dir;
#endif
}
#endif

inline static std::string getUserName()
{
#if (_MSC_VER)
    const char *username = ::getenv("USER");

    if (nullptr == username)
    {
        username = ::getenv("USERNAME");
        if (nullptr == username)
        {
            username = "default";
        }
    }

    return { username };
#else
    char buffer[16384] = {};
    const char *username = ::getenv("USER");

    if (nullptr == username)
    {
        uid_t uid = ::getuid(); // using uid instead of euid as that is what the JVM seems to do.
        struct passwd pw = {}, *pwResult = nullptr;

        int e = ::getpwuid_r(uid, &pw, buffer, sizeof(buffer), &pwResult);
        username = (0 == e && nullptr != pwResult && nullptr != pwResult->pw_name && *(pwResult->pw_name )) ?
            pwResult->pw_name : "default";
    }

    return { username };
#endif
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
