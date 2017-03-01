/*
 * Copyright 2014-2017 Real Logic Ltd.
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

#ifndef AERON_MAPPEDRAWLOG_H
#define AERON_MAPPEDRAWLOG_H


#include <sys/types.h>
#include <cstdint>
#include <vector>

#include <concurrent/logbuffer/LogBufferDescriptor.h>
#include <concurrent/AtomicBuffer.h>
#include <util/MemoryMappedFile.h>

using namespace aeron::util;
using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;

namespace aeron { namespace driver { namespace buffer {

class MappedRawLog
{
public:
    MappedRawLog(const char* location, bool useSparseFiles, std::int32_t termLength);
    ~MappedRawLog();

    std::int32_t termLength();
    const char* logFileName();

private:
    const char* m_location;
    bool m_useSparseFiles;
    std::int32_t m_termLength;
    std::vector<MemoryMappedFile::ptr_t> m_memoryMappedFiles;
    AtomicBuffer m_buffers[LogBufferDescriptor::PARTITION_COUNT];
    AtomicBuffer m_logMetaDataBuffer;

    static void allocatePages(std::uint8_t *mapping, size_t length);
};

}}}

#endif //AERON_MAPPEDRAWLOG_H
