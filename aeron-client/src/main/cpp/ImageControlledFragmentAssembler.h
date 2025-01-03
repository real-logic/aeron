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
#ifndef AERON_IMAGE_CONTROLLEDFRAGMENTASSEMBLER_H
#define AERON_IMAGE_CONTROLLEDFRAGMENTASSEMBLER_H

#include "BufferBuilder.h"

namespace aeron
{

static const std::size_t DEFAULT_IMAGE_CONTROLLED_FRAGMENT_ASSEMBLY_BUFFER_LENGTH = 4096;

/**
 * A handler that sits in a chain-of-responsibility pattern that reassembles fragmented messages
 * so that the next handler in the chain only sees whole messages.
 * <p>
 * Unfragmented messages are delegated without copy. Fragmented messages are copied to a temporary
 * buffer for reassembly before delegation.
 * <p>
 * The Header passed to the delegate on assembling a message will be that of the last fragment.
 * <p>
 * This handler is not session aware and must only be used when polling a single Image.
 */
class ImageControlledFragmentAssembler
{
public:

    /**
     * Construct an adapter to reassembly message fragments and delegate on only whole messages.
     *
     * @param delegate            onto which whole messages are forwarded.
     * @param initialBufferLength to be used for rebuilding.
     */
    explicit ImageControlledFragmentAssembler(
        const controlled_poll_fragment_handler_t &delegate,
        std::size_t initialBufferLength = DEFAULT_IMAGE_CONTROLLED_FRAGMENT_ASSEMBLY_BUFFER_LENGTH) :
        m_delegate(delegate),
        m_builder(static_cast<std::uint32_t>(initialBufferLength))
    {
    }

    /**
     * Compose a controlled_poll_fragment_handler_t that calls the ImageControlledFragmentAssembler instance for
     * reassembly. Suitable for passing to Image::controlledPoll(controlled_poll_fragment_handler_t, int).
     *
     * @return controlled_poll_fragment_handler_t composed with the ImageControlledFragmentAssembler instance
     */
    controlled_poll_fragment_handler_t handler()
    {
        return [this](AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
        {
            return this->onFragment(buffer, offset, length, header);
        };
    }

private:
    controlled_poll_fragment_handler_t m_delegate;
    BufferBuilder m_builder;

    inline ControlledPollAction onFragment(
        AtomicBuffer &buffer,
        util::index_t offset,
        util::index_t length,
        Header &header)
    {
        std::uint8_t flags = header.flags();
        ControlledPollAction action = ControlledPollAction::CONTINUE;

        if ((flags & FrameDescriptor::UNFRAGMENTED) == FrameDescriptor::UNFRAGMENTED)
        {
            action = m_delegate(buffer, offset, length, header);
        }
        else
        {
            action = handleFragment(buffer, offset, length, header);
        }

        return action;
    }

    ControlledPollAction handleFragment(
        AtomicBuffer &buffer,
        util::index_t offset,
        util::index_t length,
        Header &header)
    {
        std::uint8_t flags = header.flags();
        ControlledPollAction action = ControlledPollAction::CONTINUE;

        if ((flags & FrameDescriptor::BEGIN_FRAG) == FrameDescriptor::BEGIN_FRAG)
        {
            m_builder.reset()
                .captureHeader(header)
                .append(buffer, offset, length)
                .nextTermOffset(header.nextTermOffset());
        }
        else if (header.termOffset() == m_builder.nextTermOffset())
        {
            const std::uint32_t limit = m_builder.limit();
            m_builder.append(buffer, offset, length);

            if ((flags & FrameDescriptor::END_FRAG) == FrameDescriptor::END_FRAG)
            {
                AtomicBuffer msgBuffer(m_builder.buffer(), m_builder.limit());
                action = m_delegate(msgBuffer, 0, (util::index_t)m_builder.limit(), m_builder.completeHeader(header));

                if (ControlledPollAction::ABORT == action)
                {
                    m_builder.limit(limit);
                }
                else
                {
                    m_builder.reset();
                }
            }
            else
            {
                m_builder.nextTermOffset(header.nextTermOffset());
            }
        }
        else
        {
            m_builder.reset();
        }

        return action;
    }
};

}
#endif

