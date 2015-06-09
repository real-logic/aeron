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
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteOrder;

/**
 * Represents the header of the data frame for accessing meta data fields.
 */
public class Header
{
    private int positionBitsToShift;
    private int initialTermId;
    private int offset = 0;
    private UnsafeBuffer buffer;

    /**
     * Default constructor to enable inheritance.
     */
    protected Header()
    {
    }

    /**
     * Construct a header that references a buffer for the log.
     *
     * @param initialTermId this stream started at.
     * @param termCapacity for each term in the log buffer.
     */
    public Header(final int initialTermId, final int termCapacity)
    {
        this.initialTermId = initialTermId;
        this.positionBitsToShift = Integer.numberOfTrailingZeros(termCapacity);
    }

    /**
     * Get the current position to which the connection has advanced on reading this message.
     *
     * @return the current position to which the connection has advanced on reading this message.
     */
    public final long position()
    {
        return LogBufferDescriptor.computePosition(termId(), termOffset() + frameLength(), positionBitsToShift, initialTermId);
    }

    /**
     * Get the number of bits the number of terms need to be shifted to get the position.
     *
     * @return the number of bits the number of terms need to be shifted to get the position.
     */
    public final int positionBitsToShift()
    {
        return positionBitsToShift;
    }

    /**
     * Set the number of bits the number of terms need to be shifted to get the position.
     *
     * @param positionBitsToShift the number of bits the number of terms need to be shifted to get the position.
     */
    public final void positionBitsToShift(final int positionBitsToShift)
    {
        this.positionBitsToShift = positionBitsToShift;
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
     * Set the initial term id this stream started at.
     *
     * @param initialTermId this stream started at.
     */
    public final void initialTermId(final int initialTermId)
    {
        this.initialTermId = initialTermId;
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
     * The {@link uk.co.real_logic.agrona.concurrent.UnsafeBuffer} containing the header.
     *
     * @return {@link uk.co.real_logic.agrona.concurrent.UnsafeBuffer} containing the header.
     */
    public final UnsafeBuffer buffer()
    {
        return buffer;
    }

    /**
     * The {@link uk.co.real_logic.agrona.concurrent.UnsafeBuffer} containing the header.
     *
     * @param buffer {@link uk.co.real_logic.agrona.concurrent.UnsafeBuffer} containing the header.
     */
    public final void buffer(final UnsafeBuffer buffer)
    {
        this.buffer = buffer;
    }

    /**
     * The total length of the frame including the header.
     *
     * @return the total length of the frame including the header.
     */
    public int frameLength()
    {
        return buffer.getInt(offset, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * The session ID to which the frame belongs.
     *
     * @return the session ID to which the frame belongs.
     */
    public final int sessionId()
    {
        return buffer.getInt(offset + DataHeaderFlyweight.SESSION_ID_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * The stream ID to which the frame belongs.
     *
     * @return the stream ID to which the frame belongs.
     */
    public final int streamId()
    {
        return buffer.getInt(offset + DataHeaderFlyweight.STREAM_ID_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * The term ID to which the frame belongs.
     *
     * @return the term ID to which the frame belongs.
     */
    public final int termId()
    {
        return buffer.getInt(offset + DataHeaderFlyweight.TERM_ID_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN);
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
        return buffer.getShort(offset + DataHeaderFlyweight.TYPE_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
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
        return buffer.getByte(offset + DataHeaderFlyweight.FLAGS_FIELD_OFFSET);
    }
}
