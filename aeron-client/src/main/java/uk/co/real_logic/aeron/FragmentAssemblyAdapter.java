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

import uk.co.real_logic.aeron.common.BufferBuilder;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.collections.Int2ObjectHashMap;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.DataHandler;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.Header;

import java.util.function.Supplier;

import static uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor.*;

/**
 * {@link DataHandler} that sits in a chain-of-responsibilities pattern that re-assembles fragmented messages
 * so that next handler in the chain only sees unfragmented messages.
 *
 * Unfragmented messages are delegated without copy. Fragmented messages are copied to a temporary
 * buffer for reassembly before delegation.
 *
 * Session based buffers will be allocated and grown as necessary based on the length of messages to be assembled.
 *
 * When sessions go inactive {@link InactiveConnectionHandler}, it is possible to free the buffer by calling
 * {@link #freeSessionBuffer(int)}.
 */
public class FragmentAssemblyAdapter implements DataHandler
{
    private final DataHandler delegate;
    private final AssemblyHeader assemblyHeader = new AssemblyHeader();
    private final Int2ObjectHashMap<BufferBuilder> builderBySessionIdMap = new Int2ObjectHashMap<>();
    private final Supplier<BufferBuilder> builderSupplier;

    /**
     * Construct an adapter to reassembly message fragments and delegate on only whole messages.
     *
     * @param delegate onto which whole messages are forwarded.
     */
    public FragmentAssemblyAdapter(final DataHandler delegate)
    {
        this(delegate, BufferBuilder.INITIAL_CAPACITY);
    }

    /**
     * Construct an adapter to reassembly message fragments and delegate on only whole messages.
     *
     * @param delegate            onto which whole messages are forwarded.
     * @param initialBufferLength to be used for each session.
     */
    public FragmentAssemblyAdapter(final DataHandler delegate, final int initialBufferLength)
    {
        this.delegate = delegate;
        builderSupplier = () -> new BufferBuilder(initialBufferLength);
    }

    public void onData(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        final byte flags = header.flags();

        if ((flags & UNFRAGMENTED) == UNFRAGMENTED)
        {
            delegate.onData(buffer, offset, length, header);
        }
        else
        {
            if ((flags & BEGIN_FRAG) == BEGIN_FRAG)
            {
                final BufferBuilder builder = builderBySessionIdMap.getOrDefault(header.sessionId(), builderSupplier);
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
                        delegate.onData(builder.buffer(), 0, msgLength, assemblyHeader.reset(header, msgLength));
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
            buffer(base.buffer());
            offset(base.offset());
            frameLength = msgLength + Header.LENGTH;

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
