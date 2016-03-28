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
    std::int32_t termLength
) : m_location{location}, m_useSparseFiles{useSparseFiles}, m_termLength{termLength}
{
    std::int64_t logLength = LogBufferDescriptor::computeLogLength(termLength);

    if (logLength < LogBufferDescriptor::MAX_SINGLE_MAPPING_SIZE)
    {
        m_memoryMappedFiles.push_back(MemoryMappedFile::createNew(m_location, 0, (size_t) logLength));

        std::uint8_t *basePtr = m_memoryMappedFiles[0]->getMemoryPtr();

        const index_t metaDataSectionOffset = termLength * LogBufferDescriptor::PARTITION_COUNT;

        for (int i = 0; i < LogBufferDescriptor::PARTITION_COUNT; i++)
        {
            const index_t termOffset = (i * termLength);
            const index_t metaDataOffset = metaDataSectionOffset + (i * LogBufferDescriptor::TERM_META_DATA_LENGTH);

            m_partitions[i].termBuffer().wrap(basePtr + termOffset, termLength);
            m_partitions[i].metaDataBuffer().wrap(basePtr + metaDataOffset, LogBufferDescriptor::TERM_META_DATA_LENGTH);
        }

        m_logMetaDataBuffer.wrap(
            basePtr + (logLength - LogBufferDescriptor::LOG_META_DATA_LENGTH),
            LogBufferDescriptor::LOG_META_DATA_LENGTH);
    }
    else
    {
        const std::int64_t metaDataSectionOffset = (index_t) (termLength * LogBufferDescriptor::PARTITION_COUNT);
        const std::int64_t metaDataSectionLength = (index_t) (logLength - metaDataSectionOffset);

        // one single map for all meta data (terms and log)
        m_memoryMappedFiles.push_back(
            MemoryMappedFile::createNew(location, metaDataSectionOffset, metaDataSectionLength));

        std::uint8_t *metaDataBasePtr = m_memoryMappedFiles[0]->getMemoryPtr();

        for (int i = 0; i < LogBufferDescriptor::PARTITION_COUNT; i++)
        {
            // one map for each term
            m_memoryMappedFiles.push_back(MemoryMappedFile::mapExisting(location, i * termLength, termLength));

            std::uint8_t *basePtr = m_memoryMappedFiles[i + 1]->getMemoryPtr();

            m_partitions[i].termBuffer().wrap(basePtr, termLength);
            m_partitions[i].metaDataBuffer().wrap(
                metaDataBasePtr + (i * LogBufferDescriptor::TERM_META_DATA_LENGTH),
                LogBufferDescriptor::TERM_META_DATA_LENGTH);
        }

        m_logMetaDataBuffer.wrap(
            metaDataBasePtr + (metaDataSectionLength - LogBufferDescriptor::LOG_META_DATA_LENGTH),
            LogBufferDescriptor::LOG_META_DATA_LENGTH);
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

}}}