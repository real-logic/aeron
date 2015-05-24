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
package uk.co.real_logic.aeron.common.concurrent.logbuffer;

import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.AtomicBuffer;

/**
 * Represents a claimed range in a buffer to be used for recording a message without copy semantics for later commit.
 * <p>
 * The claimed space is in {@link #buffer()} between {@link #offset()} and {@link #offset()} + {@link #length()}.
 * When the buffer is filled with message data, use {@link #commit()} to make it available to subscribers.
 */
public class BufferClaim
{
    private AtomicBuffer buffer;
    private int offset;
    private int length;

    /**
     * The referenced buffer to be used.
     *
     * @return the referenced buffer to be used..
     */
    public MutableDirectBuffer buffer()
    {
        return buffer;
    }

    /**
     * Set the referenced buffer to be used.
     *
     * @param buffer the referenced buffer to be used.
     * @return this instance for fluent API usage.
     */
    BufferClaim buffer(final AtomicBuffer buffer)
    {
        this.buffer = buffer;
        return this;
    }

    /**
     * The offset in the buffer at which the claimed range begins.
     *
     * @return offset in the buffer at which the range begins.
     */
    public int offset()
    {
        return offset;
    }

    /**
     * Set the offset in the buffer at which the range begins.
     *
     * @param offset the offset in the buffer at which the range begins.
     * @return this instance for fluent API usage.
     */
    BufferClaim offset(final int offset)
    {
        this.offset = offset;
        return this;
    }

    /**
     * The length of the claimed range in the buffer.
     *
     * @return length of the range in the buffer.
     */
    public int length()
     {
        return length;
    }

    /**
     * Set length of the range in the buffer.
     *
     * @param length of the range in the buffer.
     * @return this instance for fluent API usage.
     */
    BufferClaim length(final int length)
    {
        this.length = length;
        return this;
    }

    /**
     * Commit the message to the log buffer so that is it available to subscribers.
     */
    public void commit()
    {
        buffer.putIntOrdered(offset - DataHeaderFlyweight.HEADER_LENGTH, length + DataHeaderFlyweight.HEADER_LENGTH);
    }
}

