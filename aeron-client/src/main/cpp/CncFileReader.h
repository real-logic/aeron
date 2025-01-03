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

#ifndef AERON_CNCFILEREADER_H
#define AERON_CNCFILEREADER_H

#include "util/MemoryMappedFile.h"
#include "concurrent/CountersReader.h"
#include "CncFileDescriptor.h"
#include "concurrent/errors/ErrorLogReader.h"

namespace aeron
{
using namespace aeron::util;
using namespace aeron::concurrent;
using namespace aeron::concurrent::errors;

class CncFileReader
{
public:
    static CncFileReader mapExisting(const char *baseDirectory)
    {
        const std::string cncFilename = baseDirectory + std::string(1, AERON_FILE_SEP) + CncFileDescriptor::CNC_FILE;
        return { util::MemoryMappedFile::mapExistingReadOnly(cncFilename.c_str()) };
    }

    CountersReader countersReader() const
    {
        return
        {
            CncFileDescriptor::createCounterMetadataBuffer(m_mmap),
            CncFileDescriptor::createCounterValuesBuffer(m_mmap)
        };
    }

    int readErrorLog(const ErrorLogReader::error_consumer_t &consumer, std::int64_t sinceTimestamp) const
    {
        AtomicBuffer buffer = CncFileDescriptor::createErrorLogBuffer(m_mmap);

        return ErrorLogReader::read(buffer, consumer, sinceTimestamp);
    }

private:
    MemoryMappedFile::ptr_t m_mmap;

    CncFileReader(MemoryMappedFile::ptr_t mmap) : m_mmap(mmap)
    {
    }
};

}

#endif //AERON_CNCFILEREADER_H
