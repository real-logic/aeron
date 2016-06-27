/*
 * Copyright 2014 - 2016 Real Logic Ltd.
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

#ifndef INCLUDED_AERON_CONCURRENT_LOGBUFFER_LOG_BUFFER__
#define INCLUDED_AERON_CONCURRENT_LOGBUFFER_LOG_BUFFER__

#include <algorithm>
#include <util/Index.h>
#include <concurrent/AtomicBuffer.h>
#include "LogBufferDescriptor.h"

namespace aeron { namespace concurrent { namespace logbuffer {

class LogBufferPartition
{
public:
    inline LogBufferPartition(AtomicBuffer& termBuffer, AtomicBuffer& metaDataBuffer)
    {
        LogBufferDescriptor::checkTermLength(termBuffer.capacity());
        LogBufferDescriptor::checkMetaDataBuffer(metaDataBuffer);

        m_termBuffer.wrap(termBuffer);
        m_metaDataBuffer.wrap(metaDataBuffer);
    }

    inline LogBufferPartition()
    {
    }

    inline void wrap(AtomicBuffer&& termBuffer, AtomicBuffer&& metaDataBuffer)
    {
        LogBufferDescriptor::checkTermLength(termBuffer.capacity());
        LogBufferDescriptor::checkMetaDataBuffer(metaDataBuffer);

        m_termBuffer.wrap(termBuffer);
        m_metaDataBuffer.wrap(metaDataBuffer);
    }

    inline AtomicBuffer& termBuffer()
    {
        return m_termBuffer;
    }

    inline AtomicBuffer& metaDataBuffer()
    {
        return m_metaDataBuffer;
    }

    inline std::int32_t tailVolatile()
    {
        return std::min(m_metaDataBuffer.getInt32Volatile(LogBufferDescriptor::TERM_TAIL_COUNTER_OFFSET),
            m_termBuffer.capacity());
    }

    inline std::int32_t rawTailVolatile()
    {
        return m_metaDataBuffer.getInt32Volatile(LogBufferDescriptor::TERM_TAIL_COUNTER_OFFSET);
    }

    inline std::int32_t tail()
    {
        return std::min(m_metaDataBuffer.getInt32(LogBufferDescriptor::TERM_TAIL_COUNTER_OFFSET),
            m_termBuffer.capacity());
    }

private:
    AtomicBuffer m_termBuffer;
    AtomicBuffer m_metaDataBuffer;
};

}}}

#endif
