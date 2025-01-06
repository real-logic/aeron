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
package io.aeron.logbuffer;

import io.aeron.Aeron;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;

import java.nio.ByteOrder;

import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.protocol.DataHeaderFlyweight.*;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static io.aeron.logbuffer.LogBufferDescriptor.computePosition;

/**
 * Represents the header of the data frame for accessing metadata fields.
 */
public final class Header
{
    private Object context;
    private int positionBitsToShift;
    private int initialTermId;
    private int offset = 0;
    private DirectBuffer buffer;
    private int fragmentedFrameLength = Aeron.NULL_VALUE;

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
     * Assign context for storing state related to the context of the callback where the header is used.
     *
     * @param context for storing state related to the context of the callback where the header is used.
     * @return this for a fluent API.
     */
    public Header context(final Object context)
    {
        this.context = context;
        return this;
    }

    /**
     * Get the current position to which the image has advanced on reading this message.
     *
     * @return the current position to which the image has advanced on reading this message.
     */
    public long position()
    {
        return computePosition(termId(), nextTermOffset(), positionBitsToShift, initialTermId);
    }

    /**
     * The number of times to left shift the term count to multiply by term length.
     *
     * @return number of times to left shift the term count to multiply by term length.
     */
    public int positionBitsToShift()
    {
        return positionBitsToShift;
    }

    /**
     * Set the number of times to left shift the term count to multiply by term length.
     *
     * @param positionBitsToShift number of times to left shift the term count to multiply by term length.
     * @return this for a fluent API.
     */
    public Header positionBitsToShift(final int positionBitsToShift)
    {
        this.positionBitsToShift = positionBitsToShift;
        return this;
    }

    /**
     * Get the initial term id this stream started at.
     *
     * @return the initial term id this stream started at.
     */
    public int initialTermId()
    {
        return initialTermId;
    }

    /**
     * Get the initial term id this stream started at.
     *
     * @param initialTermId the initial term id this stream started at.
     * @return this for a fluent API.
     */
    public Header initialTermId(final int initialTermId)
    {
        this.initialTermId = initialTermId;
        return this;
    }

    /**
     * Set the offset at which the header begins in the buffer.
     *
     * @param offset at which the header begins in the buffer.
     * @return this for a fluent API.
     */
    public Header offset(final int offset)
    {
        this.offset = offset;
        return this;
    }

    /**
     * The offset at which the frame begins in the buffer.
     *
     * @return offset at which the frame begins in the buffer.
     */
    public int offset()
    {
        return offset;
    }

    /**
     * The {@link org.agrona.DirectBuffer} containing the header.
     *
     * @return {@link org.agrona.DirectBuffer} containing the header.
     */
    public DirectBuffer buffer()
    {
        return buffer;
    }

    /**
     * The {@link org.agrona.DirectBuffer} containing the header.
     *
     * @param buffer {@link org.agrona.DirectBuffer} containing the header.
     * @return this for a fluent API.
     */
    public Header buffer(final DirectBuffer buffer)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        return this;
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
    public int sessionId()
    {
        return buffer.getInt(offset + SESSION_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * The stream ID to which the frame belongs.
     *
     * @return the stream ID to which the frame belongs.
     */
    public int streamId()
    {
        return buffer.getInt(offset + STREAM_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * The term ID to which the frame belongs.
     *
     * @return the term ID to which the frame belongs.
     */
    public int termId()
    {
        return buffer.getInt(offset + TERM_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * The offset in the term at which the frame begins.
     *
     * @return the offset in the term at which the frame begins.
     */
    public int termOffset()
    {
        return buffer.getInt(offset + TERM_OFFSET_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Calculates the offset of the frame immediately after this one.
     *
     * @return the offset of the next frame.
     */
    public int nextTermOffset()
    {
        return BitUtil.align(termOffset() + termOccupancyLength(), FRAME_ALIGNMENT);
    }

    /**
     * The type of the frame which should always be {@link DataHeaderFlyweight#HDR_TYPE_DATA}.
     *
     * @return type of the frame which should always be {@link DataHeaderFlyweight#HDR_TYPE_DATA}.
     */
    public int type()
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

    /**
     * Total amount of space occupied by this message when it is within the term buffer. When fragmented this will
     * include the length of the header for each fragment. Used when doing reassembly of fragmented packets.
     *
     * @param fragmentedFrameLength total fragmented length of the message.
     */
    public void fragmentedFrameLength(final int fragmentedFrameLength)
    {
        this.fragmentedFrameLength = fragmentedFrameLength;
    }

    private int termOccupancyLength()
    {
        return Aeron.NULL_VALUE == fragmentedFrameLength ? frameLength() : fragmentedFrameLength;
    }
}
