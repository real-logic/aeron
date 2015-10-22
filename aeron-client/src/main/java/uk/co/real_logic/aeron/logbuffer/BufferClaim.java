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
package uk.co.real_logic.aeron.logbuffer;

import uk.co.real_logic.aeron.protocol.DataHeaderFlyweight;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteOrder;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static uk.co.real_logic.aeron.protocol.HeaderFlyweight.FRAME_LENGTH_FIELD_OFFSET;
import static uk.co.real_logic.aeron.protocol.HeaderFlyweight.HDR_TYPE_PAD;
import static uk.co.real_logic.aeron.protocol.HeaderFlyweight.TYPE_FIELD_OFFSET;

/**
 * Represents a claimed range in a buffer to be used for recording a message without copy semantics for later commit.
 * <p>
 * The claimed space is in {@link #buffer()} between {@link #offset()} and {@link #offset()} + {@link #length()}.
 * When the buffer is filled with message data, use {@link #commit()} to make it available to subscribers.
 * <p>
 * If the claimed space is no longer required it can be aborted by calling {@link #abort()}.
 */
public class BufferClaim
{
    private final UnsafeBuffer buffer = new UnsafeBuffer(0, 0);

    /**
     * Wrap a region of an underlying log buffer so can can represent a claimed space for use by a publisher.
     *
     * @param buffer to be wrapped.
     * @param offset at which the claimed region begins including space for the header.
     * @param length length of the underlying claimed region including space for the header.
     */
    public void wrap(final AtomicBuffer buffer, final int offset, final int length)
    {
        this.buffer.wrap(buffer, offset, length);
    }

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
     * The offset in the buffer at which the claimed range begins.
     *
     * @return offset in the buffer at which the range begins.
     */
    public int offset()
    {
        return DataHeaderFlyweight.HEADER_LENGTH;
    }

    /**
     * The length of the claimed range in the buffer.
     *
     * @return length of the range in the buffer.
     */
    public int length()
    {
        return buffer.capacity() - DataHeaderFlyweight.HEADER_LENGTH;
    }

    /**
     * Commit the message to the log buffer so that is it available to subscribers.
     */
    public void commit()
    {
        int frameLength = buffer.capacity();
        if (ByteOrder.nativeOrder() != LITTLE_ENDIAN)
        {
            frameLength = Integer.reverseBytes(frameLength);
        }

        buffer.putIntOrdered(FRAME_LENGTH_FIELD_OFFSET, frameLength);
    }

    /**
     * Abort a claim of the message space to the log buffer so that log can progress ignoring this claim.
     */
    public void abort()
    {
        int frameLength = buffer.capacity();
        if (ByteOrder.nativeOrder() != LITTLE_ENDIAN)
        {
            frameLength = Integer.reverseBytes(frameLength);
        }

        buffer.putShort(TYPE_FIELD_OFFSET, (short)HDR_TYPE_PAD, LITTLE_ENDIAN);
        buffer.putIntOrdered(FRAME_LENGTH_FIELD_OFFSET, frameLength);
    }
}

