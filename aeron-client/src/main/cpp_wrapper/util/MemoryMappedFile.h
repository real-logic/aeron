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
#include "util/Export.h"

#include "util/aeron_fileutil.h"

namespace aeron { namespace util
{

class CLIENT_EXPORT MemoryMappedFile
{
public:
    typedef std::shared_ptr<MemoryMappedFile> ptr_t;

    static std::int64_t getFileSize(const char *filename)
    {
        return aeron_file_length(filename);
    }
};

}}

#endif
