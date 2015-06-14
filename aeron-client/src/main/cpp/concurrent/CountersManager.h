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
#ifndef INCLUDED_AERON_CONCURRENT_COUNTERS_MANAGER__
#define INCLUDED_AERON_CONCURRENT_COUNTERS_MANAGER__

#include <functional>
#include <cstdint>
#include <deque>
#include <memory>

#include <util/Exceptions.h>
#include <util/StringUtil.h>
#include <util/Index.h>
#include <util/BitUtil.h>

#include "AtomicBuffer.h"

namespace aeron { namespace concurrent {

class CountersManager
{
public:
    inline CountersManager(const AtomicBuffer& labelsBuffer, const AtomicBuffer& countersBuffer) :
        m_countersBuffer(countersBuffer), m_labelsBuffer(labelsBuffer)
    {
    }

    void forEach (const std::function<void(int, const std::string&)>& f)
    {
        util::index_t labelsOffset = 0;
        util::index_t size;
        std::int32_t id = 0;

        while (
            (labelsOffset < (m_labelsBuffer.capacity() - (util::index_t)sizeof(std::int32_t))) &&
                (size = m_labelsBuffer.getInt32(labelsOffset)) != 0)
        {
            if (size != UNREGISTERED_LABEL_LENGTH)
            {
                std::string label = m_labelsBuffer.getStringUtf8(labelsOffset);
                f(id, label);
            }

            labelsOffset += LABEL_LENGTH;
            id++;
        }
    }

    std::int32_t allocate(const std::string& label)
    {
        std::int32_t id = counterId();
        util::index_t labelsOffset = labelOffset(id);

        if (label.length() > (LABEL_LENGTH - sizeof(std::int32_t)))
        {
            throw util::IllegalArgumentException("Label too long", SOURCEINFO);
        }

        if ((counterOffset(id) + COUNTER_LENGTH) > m_countersBuffer.capacity())
        {
            throw util::IllegalArgumentException("Unable to allocated counter, counter buffer is full", SOURCEINFO);
        }

        if ((labelsOffset + LABEL_LENGTH) > m_labelsBuffer.capacity())
        {
            throw util::IllegalArgumentException("Unable to allocate counter, labels buffer is full", SOURCEINFO);
        }

        m_labelsBuffer.putStringUtf8(labelsOffset, label);

        return id;
    }

    inline AtomicBuffer& labelsBuffer()
    {
        return m_labelsBuffer;
    }

    inline AtomicBuffer& valuesBuffer()
    {
        return m_countersBuffer;
    }

    void free(std::int32_t counterId)
    {
        util::index_t lsize = m_labelsBuffer.getInt32(labelOffset(counterId));
        if (lsize == 0 || lsize == UNREGISTERED_LABEL_LENGTH)
        {
            throw util::IllegalArgumentException(
                util::strPrintf("Attempt to free unallocated ID: %d", counterId), SOURCEINFO);
        }

        m_labelsBuffer.putInt32(labelOffset(counterId), UNREGISTERED_LABEL_LENGTH);
        m_countersBuffer.putInt64Ordered(counterOffset(counterId), 0L);
        m_freeList.push_back(counterId);
    }

    inline static util::index_t counterOffset(std::int32_t counterId)
    {
        return counterId * COUNTER_LENGTH;
    }

    inline static util::index_t labelOffset(std::int32_t counterId)
    {
        return counterId * LABEL_LENGTH;
    }

    static const util::index_t LABEL_LENGTH = util::BitUtil::CACHE_LINE_LENGTH * 2;
    static const util::index_t COUNTER_LENGTH = util::BitUtil::CACHE_LINE_LENGTH * 2;

private:
    static const util::index_t UNREGISTERED_LABEL_LENGTH = -1;

    util::index_t m_highwaterMark = 0;

    std::deque<std::int32_t> m_freeList;

    AtomicBuffer m_countersBuffer;
    AtomicBuffer m_labelsBuffer;

    std::int32_t counterId()
    {
        if (m_freeList.empty())
        {
            return m_highwaterMark++;
        }

        std::int32_t id = m_freeList.front();
        m_freeList.pop_front();
        m_countersBuffer.putInt64Ordered(counterOffset(id), 0L);

        return id;
    }
};

}}

#endif