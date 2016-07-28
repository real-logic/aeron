/*
 * Copyright 2016 Real Logic Ltd.
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

#include <unistd.h>
#include "MappedRawLog.h"

namespace aeron { namespace driver { namespace buffer {

MappedRawLog::MappedRawLog(
    const char* location,
    bool useSparseFiles,
    std::int32_t termLength) :
    m_location{location},
    m_useSparseFiles{useSparseFiles},
    m_termLength{termLength}
{
    std::int64_t logLength = LogBufferDescriptor::computeLogLength(termLength);

    if (logLength < LogBufferDescriptor::MAX_SINGLE_MAPPING_SIZE)
    {
        m_memoryMappedFiles.push_back(MemoryMappedFile::createNew(m_location, 0, (size_t) logLength));

        std::uint8_t *basePtr = m_memoryMappedFiles[0]->getMemoryPtr();

        if (!useSparseFiles)
        {
            allocatePages(basePtr, m_memoryMappedFiles[0]->getMemorySize());
        }

        for (int i = 0; i < LogBufferDescriptor::PARTITION_COUNT; i++)
        {
            m_buffers[i].wrap(basePtr + (i * termLength), termLength);
        }

        m_logMetaDataBuffer.wrap(
            basePtr + (logLength - LogBufferDescriptor::LOG_META_DATA_LENGTH),
            LogBufferDescriptor::LOG_META_DATA_LENGTH);
    }
    else
    {
        const std::int64_t metaDataSectionOffset = (index_t) (termLength * LogBufferDescriptor::PARTITION_COUNT);
        const std::int64_t metaDataSectionLength = (index_t) (logLength - metaDataSectionOffset);

        m_memoryMappedFiles.push_back(
            MemoryMappedFile::createNew(location, metaDataSectionOffset, metaDataSectionLength));

        std::uint8_t *metaDataBasePtr = m_memoryMappedFiles[0]->getMemoryPtr();

        for (int i = 0; i < LogBufferDescriptor::PARTITION_COUNT; i++)
        {
            // one map for each term
            m_memoryMappedFiles.push_back(MemoryMappedFile::mapExisting(location, i * termLength, termLength));

            std::uint8_t *basePtr = m_memoryMappedFiles[i + 1]->getMemoryPtr();

            if (!useSparseFiles)
            {
                allocatePages(basePtr, m_memoryMappedFiles[i + 1]->getMemorySize());
            }

            m_buffers[i].wrap(basePtr, termLength);
        }

        m_logMetaDataBuffer.wrap(metaDataBasePtr, LogBufferDescriptor::LOG_META_DATA_LENGTH);
    }
}

MappedRawLog::~MappedRawLog()
{
    ::unlink(m_location);
}

std::int32_t MappedRawLog::termLength()
{
    return m_termLength;
}

const char* MappedRawLog::logFileName()
{
    return m_location;
}

void MappedRawLog::allocatePages(std::uint8_t *mapping, size_t length)
{
    size_t pageLength = MemoryMappedFile::getPageSize();

    for (int i = 0; i < length; i += pageLength)
    {
        mapping[i] = 0;
    }
}

}}}