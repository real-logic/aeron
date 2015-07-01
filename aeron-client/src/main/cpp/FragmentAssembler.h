/*
 * Copyright 2015 Real Logic Ltd.
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

#ifndef AERON_FRAGMENTASSEMBLYADAPTER_H
#define AERON_FRAGMENTASSEMBLYADAPTER_H

#include <unordered_map>
#include "Aeron.h"

namespace aeron {

static const std::size_t DEFAULT_FRAGMENT_ASSEMBLY_BUFFER_LENGTH = 4096;

class FragmentAssembler
{
public:
    FragmentAssembler(
        const fragment_handler_t& delegate, size_t initialBufferLength = DEFAULT_FRAGMENT_ASSEMBLY_BUFFER_LENGTH) :
        m_initialBufferLength(initialBufferLength), m_delegate(delegate)
    {
    }

    fragment_handler_t handler()
    {
        return [&](AtomicBuffer& buffer, util::index_t offset, util::index_t length, Header& header)
        {
            this->onFragment(buffer, offset, length, header);
        };
    }

    void deleteSessionBuffer(std::int32_t sessionId)
    {
        m_builderBySessionIdMap.erase(sessionId);
    }

private:
    class BufferBuilder
    {
    public:
        typedef BufferBuilder this_t;

        BufferBuilder(size_t initialLength) :
            m_capacity(BitUtil::findNextPowerOfTwo(initialLength)),
            m_limit(DataFrameHeader::LENGTH),
            m_buffer(new std::uint8_t[m_capacity])
        {
        }

        virtual ~BufferBuilder()
        {
        }

        BufferBuilder(BufferBuilder&& builder) :
            m_capacity(builder.m_capacity), m_limit(builder.m_limit), m_buffer(std::move(builder.m_buffer))
        {
        }

        std::uint8_t* buffer() const
        {
            return &m_buffer[0];
        }

        size_t limit() const
        {
            return m_limit;
        }

        this_t& reset()
        {
            m_limit = DataFrameHeader::LENGTH;
            return *this;
        }

        this_t& append(AtomicBuffer& buffer, util::index_t offset, util::index_t length, Header& header)
        {
            ensureCapacity(length);

            ::memcpy(&m_buffer[0] + m_limit, buffer.buffer() + offset, length);
            m_limit += length;
            return *this;
        }

    private:
        std::size_t m_capacity;
        std::size_t m_limit = 0;
        std::unique_ptr<std::uint8_t[]> m_buffer;

        inline static size_t findSuitableCapacity(size_t capacity, size_t requiredCapacity)
        {
            do
            {
                capacity <<= 1;
            }
            while (capacity < requiredCapacity);

            return capacity;
        }

        void ensureCapacity(size_t additionalCapacity)
        {
            const size_t requiredCapacity = m_limit + additionalCapacity;

            if (requiredCapacity > m_capacity)
            {
                const size_t newCapacity = findSuitableCapacity(m_capacity, requiredCapacity);
                std::unique_ptr<std::uint8_t[]> newBuffer(new std::uint8_t[newCapacity]);

                ::memcpy(&newBuffer[0], &m_buffer[0], m_limit);
                m_buffer = std::move(newBuffer);
                m_capacity = newCapacity;
            }
        }
    };

    std::size_t m_initialBufferLength;
    fragment_handler_t m_delegate;
    std::unordered_map<std::int32_t, BufferBuilder> m_builderBySessionIdMap;

    void onFragment(AtomicBuffer& buffer, util::index_t offset, util::index_t length, Header& header)
    {
        const std::uint8_t flags = header.flags();

        if ((flags & FrameDescriptor::UNFRAGMENTED) == FrameDescriptor::UNFRAGMENTED)
        {
            m_delegate(buffer, offset, length, header);
        }
        else
        {
            if ((flags & FrameDescriptor::BEGIN_FRAG) == FrameDescriptor::BEGIN_FRAG)
            {
                auto result = m_builderBySessionIdMap.emplace(header.sessionId(), m_initialBufferLength);
                BufferBuilder& builder = result.first->second;

                builder
                    .reset()
                    .append(buffer, offset, length, header);
            }
            else
            {
                auto result = m_builderBySessionIdMap.find(header.sessionId());

                if (result != m_builderBySessionIdMap.end())
                {
                    BufferBuilder& builder = result->second;

                    if (builder.limit() != DataFrameHeader::LENGTH)
                    {
                        builder.append(buffer, offset, length, header);

                        if ((flags & FrameDescriptor::END_FRAG) == FrameDescriptor::END_FRAG)
                        {
                            const util::index_t msgLength = builder.limit() - DataFrameHeader::LENGTH;
                            AtomicBuffer msgBuffer(builder.buffer(), builder.limit());
                            Header assemblyHeader(header);

                            assemblyHeader.buffer(msgBuffer);
                            assemblyHeader.offset(0);
                            DataFrameHeader::DataFrameHeaderDefn& frame(
                                msgBuffer.overlayStruct<DataFrameHeader::DataFrameHeaderDefn>());

                            frame.frameLength = DataFrameHeader::LENGTH + msgLength;
                            frame.sessionId = header.sessionId();
                            frame.streamId = header.streamId();
                            frame.termId = header.termId();
                            frame.flags = FrameDescriptor::UNFRAGMENTED;
                            frame.type = DataFrameHeader::HDR_TYPE_DATA;
                            frame.termOffset = header.termOffset() - (frame.frameLength - header.frameLength());

                            m_delegate(msgBuffer, DataFrameHeader::LENGTH, msgLength, assemblyHeader);

                            builder.reset();
                        }
                    }
                }
            }
        }
    }
};

}

#endif //AERON_FRAGMENTASSEMBLYADAPTER_H
