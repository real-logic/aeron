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
package uk.co.real_logic.aeron;

import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.aeron.protocol.DataHeaderFlyweight;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.collections.Int2ObjectHashMap;

import java.util.function.IntFunction;

import static uk.co.real_logic.aeron.logbuffer.FrameDescriptor.*;

/**
 * A {@link FragmentHandler} that sits in a chain-of-responsibility pattern that reassembles fragmented messages
 * so that the next handler in the chain only sees whole messages.
 * <p>
 * Unfragmented messages are delegated without copy. Fragmented messages are copied to a temporary
 * buffer for reassembly before delegation.
 * <p>
 * Session based buffers will be allocated and grown as necessary based on the length of messages to be assembled.
 * When sessions go inactive see {@link InactiveConnectionHandler}, it is possible to free the buffer by calling
 * {@link #freeSessionBuffer(int)}.
 */
public class FragmentAssemblyAdapter implements FragmentHandler
{
    private final FragmentHandler delegate;
    private final AssemblyHeader assemblyHeader = new AssemblyHeader();
    private final Int2ObjectHashMap<BufferBuilder> builderBySessionIdMap = new Int2ObjectHashMap<>();
    private final IntFunction<BufferBuilder> builderFunc;

    /**
     * Construct an adapter to reassemble message fragments and delegate on only whole messages.
     *
     * @param delegate onto which whole messages are forwarded.
     */
    public FragmentAssemblyAdapter(final FragmentHandler delegate)
    {
        this(delegate, BufferBuilder.INITIAL_CAPACITY);
    }

    /**
     * Construct an adapter to reassembly message fragments and delegate on only whole messages.
     *
     * @param delegate            onto which whole messages are forwarded.
     * @param initialBufferLength to be used for each session.
     */
    public FragmentAssemblyAdapter(final FragmentHandler delegate, final int initialBufferLength)
    {
        this.delegate = delegate;
        builderFunc = (ignore) -> new BufferBuilder(initialBufferLength);
    }

    /**
     * The implementation of {@link FragmentHandler} that reassembles and forwards whole messages.
     *
     * @param buffer containing the data.
     * @param offset at which the data begins.
     * @param length of the data in bytes.
     * @param header representing the meta data for the data.
     */
    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        final byte flags = header.flags();

        if ((flags & UNFRAGMENTED) == UNFRAGMENTED)
        {
            delegate.onFragment(buffer, offset, length, header);
        }
        else
        {
            if ((flags & BEGIN_FRAG) == BEGIN_FRAG)
            {
                final BufferBuilder builder = builderBySessionIdMap.computeIfAbsent(header.sessionId(), builderFunc);
                builder.reset().append(buffer, offset, length);
            }
            else
            {
                final BufferBuilder builder = builderBySessionIdMap.get(header.sessionId());
                if (null != builder && builder.limit() != 0)
                {
                    builder.append(buffer, offset, length);

                    if ((flags & END_FRAG) == END_FRAG)
                    {
                        final int msgLength = builder.limit();
                        delegate.onFragment(builder.buffer(), 0, msgLength, assemblyHeader.reset(header, msgLength));
                        builder.reset();
                    }
                }
            }
        }
    }

    /**
     * Free an existing session buffer to reduce memory pressure when a connection goes inactive or no more
     * large messages are expected.
     *
     * @param sessionId to have its buffer freed
     * @return true if a buffer has been freed otherwise false.
     */
    public boolean freeSessionBuffer(final int sessionId)
    {
        return null != builderBySessionIdMap.remove(sessionId);
    }

    private static class AssemblyHeader extends Header
    {
        private int frameLength;

        public AssemblyHeader reset(final Header base, final int msgLength)
        {
            positionBitsToShift(base.positionBitsToShift());
            initialTermId(base.initialTermId());
            offset(base.offset());
            buffer(base.buffer());
            frameLength = msgLength + DataHeaderFlyweight.HEADER_LENGTH;

            return this;
        }

        public int frameLength()
        {
            return frameLength;
        }

        public byte flags()
        {
            return (byte)(super.flags() | UNFRAGMENTED);
        }

        public int termOffset()
        {
            return offset() - (frameLength - super.frameLength());
        }
    }
}
