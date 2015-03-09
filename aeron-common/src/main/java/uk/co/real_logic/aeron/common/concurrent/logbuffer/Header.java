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

import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;

import java.nio.ByteOrder;

import static uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;

/**
 * Represents the header of the data frame for accessing meta data fields.
 */
public class Header
{
    /**
     * The actual length of the header must be aligned to a {@link FrameDescriptor#FRAME_ALIGNMENT} boundary.
     */
    public static final int LENGTH = BitUtil.align(DataHeaderFlyweight.HEADER_LENGTH, FRAME_ALIGNMENT);

    private UnsafeBuffer buffer;
    private int offset = 0;

    /**
     * Default constructor to enable inheritance.
     */
    public Header()
    {
    }

    /**
     * Construct a header that references a buffer for the log.
     *
     * @param termBuffer for the log.
     */
    public Header(final UnsafeBuffer termBuffer)
    {
        this.buffer = termBuffer;
    }

    /**
     * Set the offset at which the header begins in the log.
     *
     * @param offset at which the header begins in the log.
     */
    public void offset(final int offset)
    {
        this.offset = offset;
    }

    /**
     * The offset at which the frame begins.
     *
     * @return offset at which the frame begins.
     */
    public int offset()
    {
        return offset;
    }

    /**
     * The {@link uk.co.real_logic.agrona.concurrent.UnsafeBuffer} containing the header.
     *
     * @return {@link uk.co.real_logic.agrona.concurrent.UnsafeBuffer} containing the header.
     */
    public UnsafeBuffer buffer()
    {
        return buffer;
    }

    /**
     * The {@link uk.co.real_logic.agrona.concurrent.UnsafeBuffer} containing the header.
     *
     * @param buffer {@link uk.co.real_logic.agrona.concurrent.UnsafeBuffer} containing the header.
     */
    public void buffer(final UnsafeBuffer buffer)
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
        return buffer.getInt(offset + DataHeaderFlyweight.FRAME_LENGTH_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * The session ID to which the frame belongs.
     *
     * @return the session ID to which the frame belongs.
     */
    public int sessionId()
    {
        return buffer.getInt(offset + DataHeaderFlyweight.SESSION_ID_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * The stream ID to which the frame belongs.
     *
     * @return the stream ID to which the frame belongs.
     */
    public int streamId()
    {
        return buffer.getInt(offset + DataHeaderFlyweight.STREAM_ID_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * The term ID to which the frame belongs.
     *
     * @return the term ID to which the frame belongs.
     */
    public int termId()
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
    public int type()
    {
        return buffer.getShort(offset + DataHeaderFlyweight.TYPE_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
    }

    /**
     * The flags for this frame.
     *
     * @return the flags for this frame.
     */
    public byte flags()
    {
        return buffer.getByte(offset + DataHeaderFlyweight.FLAGS_FIELD_OFFSET);
    }
}
