/*
 * Copyright 2014-2020 Real Logic Limited.
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

#ifndef AERON_IMAGE_FRAGMENT_ASSEMBLER_H
#define AERON_IMAGE_FRAGMENT_ASSEMBLER_H

#include "Aeron.h"
#include "BufferBuilder.h"
#include "concurrent/logbuffer/FrameDescriptor.h"

namespace aeron
{

static const std::size_t DEFAULT_IMAGE_FRAGMENT_ASSEMBLY_BUFFER_LENGTH = 4096;

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
class ImageFragmentAssembler
{
public:

    /**
     * Construct an adapter to reassembly message fragments and delegate on only whole messages.
     *
     * @param delegate            onto which whole messages are forwarded.
     * @param initialBufferLength to be used for rebuilding.
     */
    explicit ImageFragmentAssembler(
        const fragment_handler_t &delegate,
        size_t initialBufferLength = DEFAULT_IMAGE_FRAGMENT_ASSEMBLY_BUFFER_LENGTH) :
        m_delegate(delegate),
        m_builder(static_cast<int32_t>(initialBufferLength))
    {
    }

    /**
     * Compose a fragment_handler_t that calls the ImageFragmentAssembler instance for reassembly. Suitable for
     * passing to Image::poll(fragment_handler_t, int).
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

private:
    fragment_handler_t m_delegate;
    BufferBuilder m_builder;

    inline void onFragment(AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
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
                m_builder
                    .reset()
                    .append(buffer, offset, length, header);
            }
            else
            {
                if (m_builder.limit() != DataFrameHeader::LENGTH)
                {
                    m_builder.append(buffer, offset, length, header);

                    if ((flags & FrameDescriptor::END_FRAG) == FrameDescriptor::END_FRAG)
                    {
                        const util::index_t msgLength = m_builder.limit() - DataFrameHeader::LENGTH;
                        AtomicBuffer msgBuffer(m_builder.buffer(), m_builder.limit());

                        m_delegate(msgBuffer, DataFrameHeader::LENGTH, msgLength, header);

                        m_builder.reset();
                    }
                }
            }
        }
    }
};

}

#endif
