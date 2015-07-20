/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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

#define MAX_SINGLE_MAPPING_SIZE (0x7FFFFFFF)

LogBuffers::LogBuffers(const char *filename)
{
    const std::int64_t logLength = MemoryMappedFile::getFileSize(filename);
    const std::int64_t termLength = LogBufferDescriptor::computeTermLength(logLength);

    LogBufferDescriptor::checkTermLength(termLength);

    if (logLength < MAX_SINGLE_MAPPING_SIZE)
    {
        m_memoryMappedFiles.push_back(MemoryMappedFile::mapExisting(filename));

        const index_t metaDataSectionOffset = (index_t) (termLength * LogBufferDescriptor::PARTITION_COUNT);

        std::uint8_t *basePtr = m_memoryMappedFiles[0]->getMemoryPtr();

        for (int i = 0; i < LogBufferDescriptor::PARTITION_COUNT; i++)
        {
            const index_t metaDataOffset = metaDataSectionOffset + (i * LogBufferDescriptor::TERM_META_DATA_LENGTH);

            m_buffers[i].wrap(basePtr + (i * termLength), termLength);
            m_buffers[i + LogBufferDescriptor::PARTITION_COUNT]
                .wrap(basePtr + metaDataOffset, LogBufferDescriptor::TERM_META_DATA_LENGTH);
        }

        m_buffers[2 * LogBufferDescriptor::PARTITION_COUNT]
            .wrap(basePtr + (logLength - LogBufferDescriptor::LOG_META_DATA_LENGTH),
                LogBufferDescriptor::LOG_META_DATA_LENGTH);
    }
    else
    {
        const std::int64_t metaDataSectionOffset = (index_t) (termLength * LogBufferDescriptor::PARTITION_COUNT);
        const std::int64_t metaDataSectionLength = (index_t) (logLength - metaDataSectionOffset);

        // one single map for all meta data (terms and log)
        m_memoryMappedFiles.push_back(
            MemoryMappedFile::mapExisting(filename, metaDataSectionOffset, metaDataSectionLength));

        std::uint8_t *metaDataBasePtr = m_memoryMappedFiles[0]->getMemoryPtr();

        for (int i = 0; i < LogBufferDescriptor::PARTITION_COUNT; i++)
        {
            // one map for each term
            m_memoryMappedFiles.push_back(MemoryMappedFile::mapExisting(filename, i * termLength, termLength));

            std::uint8_t *basePtr = m_memoryMappedFiles[i + 1]->getMemoryPtr();

            m_buffers[i].wrap(basePtr, termLength);
            m_buffers[i + LogBufferDescriptor::PARTITION_COUNT]
                .wrap(metaDataBasePtr + (i * LogBufferDescriptor::TERM_META_DATA_LENGTH),
                    LogBufferDescriptor::TERM_META_DATA_LENGTH);
        }

        m_buffers[2 * LogBufferDescriptor::PARTITION_COUNT]
            .wrap(metaDataBasePtr + (metaDataSectionLength - LogBufferDescriptor::LOG_META_DATA_LENGTH),
                LogBufferDescriptor::LOG_META_DATA_LENGTH);
    }
}

LogBuffers::LogBuffers(std::uint8_t *address, index_t length)
{
    const index_t termLength = (index_t)LogBufferDescriptor::computeTermLength(length);
    const index_t metaDataSectionOffset = (index_t) (termLength * LogBufferDescriptor::PARTITION_COUNT);

    for (int i = 0; i < LogBufferDescriptor::PARTITION_COUNT; i++)
    {
        const index_t metaDataOffset = metaDataSectionOffset + (i * LogBufferDescriptor::TERM_META_DATA_LENGTH);

        m_buffers[i].wrap(address + (i * termLength), termLength);
        m_buffers[i + LogBufferDescriptor::PARTITION_COUNT]
            .wrap(address + metaDataOffset, LogBufferDescriptor::TERM_META_DATA_LENGTH);
    }

    m_buffers[2 * LogBufferDescriptor::PARTITION_COUNT]
        .wrap(address + (length - LogBufferDescriptor::LOG_META_DATA_LENGTH),
            LogBufferDescriptor::LOG_META_DATA_LENGTH);
}

LogBuffers::~LogBuffers() = default;

}