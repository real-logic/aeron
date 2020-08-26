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

    static std::int64_t getFileSize(const char *filename)
    {
        struct stat statInfo{};

        if (::stat(filename, &statInfo) < 0)
        {
            return -1;
        }

        return statInfo.st_size;
    }

};

}}

#endif
