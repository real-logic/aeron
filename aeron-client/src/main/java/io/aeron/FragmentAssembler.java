/*
 * Copyright 2014-2017 Real Logic Ltd.
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
package io.aeron;

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;

import static io.aeron.logbuffer.FrameDescriptor.*;

/**
 * A {@link FragmentHandler} that sits in a chain-of-responsibility pattern that reassembles fragmented messages
 * so that the next handler in the chain only sees whole messages.
 *
 * Unfragmented messages are delegated without copy. Fragmented messages are copied to a temporary
 * buffer for reassembly before delegation.
 *
 * The {@link Header} passed to the delegate on assembling a message will be that of the last fragment.
 *
 * Session based buffers will be allocated and grown as necessary based on the length of messages to be assembled.
 * When sessions go inactive see {@link UnavailableImageHandler}, it is possible to free the buffer by calling
 * {@link #freeSessionBuffer(int)}.
 */
public class FragmentAssembler implements FragmentHandler
{
    private final int initialBufferLength;
    private final FragmentHandler delegate;
    private final Int2ObjectHashMap<BufferBuilder> builderBySessionIdMap = new Int2ObjectHashMap<>();

    /**
     * Construct an adapter to reassemble message fragments and delegate on whole messages.
     *
     * @param delegate onto which whole messages are forwarded.
     */
    public FragmentAssembler(final FragmentHandler delegate)
    {
        this(delegate, BufferBuilder.INITIAL_CAPACITY);
    }

    /**
     * Construct an adapter to reassemble message fragments and delegate on whole messages.
     *
     * @param delegate            onto which whole messages are forwarded.
     * @param initialBufferLength to be used for each session.
     */
    public FragmentAssembler(final FragmentHandler delegate, final int initialBufferLength)
    {
        this.initialBufferLength = initialBufferLength;
        this.delegate = delegate;
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
            handleFragment(buffer, offset, length, header, flags);
        }
    }

    private void handleFragment(
        final DirectBuffer buffer, final int offset, final int length, final Header header, final byte flags)
    {
        if ((flags & BEGIN_FRAG_FLAG) == BEGIN_FRAG_FLAG)
        {
            final BufferBuilder builder = getBufferBuilder(header.sessionId());
            builder.reset().append(buffer, offset, length);
        }
        else
        {
            final BufferBuilder builder = builderBySessionIdMap.get(header.sessionId());
            if (null != builder && builder.limit() != 0)
            {
                builder.append(buffer, offset, length);

                if ((flags & END_FRAG_FLAG) == END_FRAG_FLAG)
                {
                    final int msgLength = builder.limit();
                    delegate.onFragment(builder.buffer(), 0, msgLength, header);
                    builder.reset();
                }
            }
        }
    }

    /**
     * Free an existing session buffer to reduce memory pressure when an image goes inactive or no more
     * large messages are expected.
     *
     * @param sessionId to have its buffer freed
     * @return true if a buffer has been freed otherwise false.
     */
    public boolean freeSessionBuffer(final int sessionId)
    {
        return null != builderBySessionIdMap.remove(sessionId);
    }

    /**
     * Clear down the cache of buffers by session for reassembling messages.
     */
    public void clear()
    {
        builderBySessionIdMap.clear();
    }

    private BufferBuilder getBufferBuilder(final int sessionId)
    {
        BufferBuilder bufferBuilder = builderBySessionIdMap.get(sessionId);

        if (null == bufferBuilder)
        {
            bufferBuilder = new BufferBuilder(initialBufferLength);
            builderBySessionIdMap.put(sessionId, bufferBuilder);
        }

        return bufferBuilder;
    }
}
