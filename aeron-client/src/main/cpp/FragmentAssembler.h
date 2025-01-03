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

#ifndef AERON_FRAGMENT_ASSEMBLER_H
#define AERON_FRAGMENT_ASSEMBLER_H

#include <unordered_map>
#include "BufferBuilder.h"

namespace aeron
{

static const std::size_t DEFAULT_FRAGMENT_ASSEMBLY_BUFFER_LENGTH = 4096;

/**
 * A handler that sits in a chain-of-responsibility pattern that reassembles fragmented messages
 * so that the next handler in the chain only sees whole messages.
 * <p>
 * Unfragmented messages are delegated without copy. Fragmented messages are copied to a temporary
 * buffer for reassembly before delegation.
 * <p>
 * The Header passed to the delegate on assembling a message will be that of the last fragment.
 * <p>
 * Session based buffers will be allocated and grown as necessary based on the length of messages to be assembled.
 * When sessions go inactive see {@link on_unavailable_image_t}, it is possible to free the buffer by calling
 * {@link #deleteSessionBuffer(std::int32_t)}.
 */
class FragmentAssembler
{
public:

    /**
     * Construct an adapter to reassembly message fragments and delegate on only whole messages.
     *
     * @param delegate            onto which whole messages are forwarded.
     * @param initialBufferLength to be used for each session.
     */
    explicit FragmentAssembler(
        const fragment_handler_t &delegate, std::size_t initialBufferLength = DEFAULT_FRAGMENT_ASSEMBLY_BUFFER_LENGTH) :
        m_initialBufferLength(initialBufferLength),
        m_delegate(delegate)
    {
    }

    /**
     * Compose a fragment_handler_t that calls the this FragmentAssembler instance for reassembly. Suitable for
     * passing to Subscription::poll(fragment_handler_t, int).
     *
     * @return fragment_handler_t composed with the FragmentAssembler instance
     */
    fragment_handler_t handler()
    {
        return [this](AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
        {
            this->onFragment(buffer, offset, length, header);
        };
    }

    /**
     * Free an existing session buffer to reduce memory pressure when an Image goes inactive or no more
     * large messages are expected.
     *
     * @param sessionId to have its buffer freed
     */
    void deleteSessionBuffer(std::int32_t sessionId)
    {
        m_builderBySessionIdMap.erase(sessionId);
    }

private:
    const std::size_t m_initialBufferLength;
    fragment_handler_t m_delegate;
    std::unordered_map<std::int32_t, BufferBuilder> m_builderBySessionIdMap;

    inline void onFragment(AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
    {
        const std::uint8_t flags = header.flags();

        if ((flags & FrameDescriptor::UNFRAGMENTED) == FrameDescriptor::UNFRAGMENTED)
        {
            m_delegate(buffer, offset, length, header);
        }
        else
        {
            handleFragment(buffer, offset, length, header);
        }
    }

    void handleFragment(AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
    {
        const std::uint8_t flags = header.flags();

        if ((flags & FrameDescriptor::BEGIN_FRAG) == FrameDescriptor::BEGIN_FRAG)
        {
            BufferBuilder &builder = getBuffer(header.sessionId());
            builder.reset()
                .captureHeader(header)
                .append(buffer, offset, length)
                .nextTermOffset(header.nextTermOffset());
        }
        else
        {
            auto result = m_builderBySessionIdMap.find(header.sessionId());

            if (result != m_builderBySessionIdMap.end())
            {
                BufferBuilder &builder = result->second;

                if (header.termOffset() == builder.nextTermOffset())
                {
                    builder.append(buffer, offset, length);

                    if ((flags & FrameDescriptor::END_FRAG) == FrameDescriptor::END_FRAG)
                    {
                        AtomicBuffer msgBuffer(builder.buffer(), builder.limit());
                        m_delegate(msgBuffer, 0, (util::index_t)builder.limit(), builder.completeHeader(header));

                        builder.reset();
                    }
                    else
                    {
                        builder.nextTermOffset(header.nextTermOffset());
                    }
                }
                else
                {
                    builder.reset();
                }
            }
        }
    }

    inline BufferBuilder &getBuffer(std::int32_t sessionId)
    {
        auto iter = m_builderBySessionIdMap.find(sessionId);
        if (iter != m_builderBySessionIdMap.end())
        {
            return iter->second;
        }
        else
        {
            auto pair = m_builderBySessionIdMap.emplace(
                sessionId, BufferBuilder(static_cast<std::uint32_t>(m_initialBufferLength)));
            return pair.first->second;
        }
    }
};

}

#endif
