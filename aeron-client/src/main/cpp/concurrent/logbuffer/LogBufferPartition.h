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

    inline AtomicBuffer& termBuffer() const
    {
        return m_termBuffer;
    }

    inline AtomicBuffer& metaDataBuffer() const
    {
        return m_metaDataBuffer;
    }

    inline void clean()
    {
        m_termBuffer.setMemory(0, m_termBuffer.capacity(), 0);
        m_metaDataBuffer.setMemory(0, m_metaDataBuffer.capacity(), 0);
        statusOrdered(LogBufferDescriptor::CLEAN);
    }

    inline int status() const
    {
        return m_metaDataBuffer.getInt32Volatile(LogBufferDescriptor::TERM_STATUS_OFFSET);
    }

    inline void statusOrdered(std::int32_t status)
    {
        m_metaDataBuffer.putInt32Ordered(LogBufferDescriptor::TERM_STATUS_OFFSET, status);
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

protected:
    inline LogBufferPartition(AtomicBuffer& termBuffer, AtomicBuffer& metaDataBuffer) :
        m_termBuffer(termBuffer), m_metaDataBuffer(metaDataBuffer)
    {
        LogBufferDescriptor::checkTermBuffer(termBuffer);
        LogBufferDescriptor::checkMetaDataBuffer(metaDataBuffer);
    }

private:
    AtomicBuffer& m_termBuffer;
    AtomicBuffer& m_metaDataBuffer;
};

}}}

#endif
