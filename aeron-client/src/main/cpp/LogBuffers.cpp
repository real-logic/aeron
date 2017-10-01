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

#include "LogBuffers.h"

namespace aeron {

using namespace aeron::util;
using namespace aeron::concurrent::logbuffer;

LogBuffers::LogBuffers(const char *filename)
{
    const std::int64_t logLength = MemoryMappedFile::getFileSize(filename);

    m_memoryMappedFiles = MemoryMappedFile::mapExisting(filename);

    std::uint8_t *basePtr = m_memoryMappedFiles->getMemoryPtr();

    m_buffers[LogBufferDescriptor::LOG_META_DATA_SECTION_INDEX]
        .wrap(basePtr + (logLength - LogBufferDescriptor::LOG_META_DATA_LENGTH),
            LogBufferDescriptor::LOG_META_DATA_LENGTH);

    const std::int32_t termLength =
        LogBufferDescriptor::termLength(m_buffers[LogBufferDescriptor::LOG_META_DATA_SECTION_INDEX]);
    const std::int32_t pageSize =
        LogBufferDescriptor::pageSize(m_buffers[LogBufferDescriptor::LOG_META_DATA_SECTION_INDEX]);

    LogBufferDescriptor::checkTermLength(termLength);
    LogBufferDescriptor::checkPageSize(pageSize);

    for (int i = 0; i < LogBufferDescriptor::PARTITION_COUNT; i++)
    {
        m_buffers[i].wrap(basePtr + (i * termLength), termLength);
    }
}

LogBuffers::LogBuffers(std::uint8_t *address, std::int64_t logLength, std::int32_t termLength)
{
    m_buffers[LogBufferDescriptor::LOG_META_DATA_SECTION_INDEX]
        .wrap(address + (logLength - LogBufferDescriptor::LOG_META_DATA_LENGTH),
            LogBufferDescriptor::LOG_META_DATA_LENGTH);

    for (int i = 0; i < LogBufferDescriptor::PARTITION_COUNT; i++)
    {
        m_buffers[i].wrap(address + (i * termLength), termLength);
    }
}

LogBuffers::~LogBuffers() = default;

}
