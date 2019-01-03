/*
 * Copyright 2014-2019 Real Logic Ltd.
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
package io.aeron.logbuffer;

import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;

import java.nio.ByteOrder;

import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.protocol.DataHeaderFlyweight.*;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static io.aeron.logbuffer.LogBufferDescriptor.computePosition;

/**
 * Represents the header of the data frame for accessing meta data fields.
 */
public class Header
{
    private final int positionBitsToShift;
    private final int initialTermId;
    private int offset = 0;
    private DirectBuffer buffer;
    private final Object context;

    /**
     * Construct a header that references a buffer for the log.
     *
     * @param initialTermId       this stream started at.
     * @param positionBitsToShift for calculating positions.
     */
    public Header(final int initialTermId, final int positionBitsToShift)
    {
        this(initialTermId, positionBitsToShift, null);
    }

    /**
     * Construct a header that references a buffer for the log.
     *
     * @param initialTermId       this stream started at.
     * @param positionBitsToShift for calculating positions.
     * @param context             for storing state when which can be accessed with {@link #context()}.
     */
    public Header(final int initialTermId, final int positionBitsToShift, final Object context)
    {
        this.initialTermId = initialTermId;
        this.positionBitsToShift = positionBitsToShift;
        this.context = context;
    }

    /**
     * Context for storing state related to the context of the callback where the header is used.
     *
     * @return context for storing state related to the context of the callback where the header is used.
     */
    public Object context()
    {
        return context;
    }

    /**
     * Get the current position to which the image has advanced on reading this message.
     *
     * @return the current position to which the image has advanced on reading this message.
     */
    public final long position()
    {
        final int resultingOffset = BitUtil.align(termOffset() + frameLength(), FRAME_ALIGNMENT);
        return computePosition(termId(), resultingOffset, positionBitsToShift, initialTermId);
    }

    /**
     * Get the initial term id this stream started at.
     *
     * @return the initial term id this stream started at.
     */
    public final int initialTermId()
    {
        return initialTermId;
    }

    /**
     * Set the offset at which the header begins in the log.
     *
     * @param offset at which the header begins in the log.
     */
    public final void offset(final int offset)
    {
        this.offset = offset;
    }

    /**
     * The offset at which the frame begins.
     *
     * @return offset at which the frame begins.
     */
    public final int offset()
    {
        return offset;
    }

    /**
     * The {@link org.agrona.DirectBuffer} containing the header.
     *
     * @return {@link org.agrona.DirectBuffer} containing the header.
     */
    public final DirectBuffer buffer()
    {
        return buffer;
    }

    /**
     * The {@link org.agrona.DirectBuffer} containing the header.
     *
     * @param buffer {@link org.agrona.DirectBuffer} containing the header.
     */
    public final void buffer(final DirectBuffer buffer)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
    }

    /**
     * The total length of the frame including the header.
     *
     * @return the total length of the frame including the header.
     */
    public int frameLength()
    {
        return buffer.getInt(offset, LITTLE_ENDIAN);
    }

    /**
     * The session ID to which the frame belongs.
     *
     * @return the session ID to which the frame belongs.
     */
    public final int sessionId()
    {
        return buffer.getInt(offset + SESSION_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * The stream ID to which the frame belongs.
     *
     * @return the stream ID to which the frame belongs.
     */
    public final int streamId()
    {
        return buffer.getInt(offset + STREAM_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * The term ID to which the frame belongs.
     *
     * @return the term ID to which the frame belongs.
     */
    public final int termId()
    {
        return buffer.getInt(offset + TERM_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * The offset in the term at which the frame begins. This will be the same as {@link #offset()}
     *
     * @return the offset in the term at which the frame begins.
     */
    public int termOffset()
    {
        return offset;
    }

    /**
     * The type of the the frame which should always be {@link DataHeaderFlyweight#HDR_TYPE_DATA}
     *
     * @return type of the the frame which should always be {@link DataHeaderFlyweight#HDR_TYPE_DATA}
     */
    public final int type()
    {
        return buffer.getShort(offset + TYPE_FIELD_OFFSET, LITTLE_ENDIAN) & 0xFFFF;
    }

    /**
     * The flags for this frame. Valid flags are {@link DataHeaderFlyweight#BEGIN_FLAG}
     * and {@link DataHeaderFlyweight#END_FLAG}. A convenience flag {@link DataHeaderFlyweight#BEGIN_AND_END_FLAGS}
     * can be used for both flags.
     *
     * @return the flags for this frame.
     */
    public byte flags()
    {
        return buffer.getByte(offset + FLAGS_FIELD_OFFSET);
    }

    /**
     * Get the value stored in the reserve space at the end of a data frame header.
     * <p>
     * Note: The value is in {@link ByteOrder#LITTLE_ENDIAN} format.
     *
     * @return the value stored in the reserve space at the end of a data frame header.
     * @see DataHeaderFlyweight
     */
    public long reservedValue()
    {
        return buffer.getLong(offset + RESERVED_VALUE_OFFSET, LITTLE_ENDIAN);
    }
}
