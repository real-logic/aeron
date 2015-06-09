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

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static uk.co.real_logic.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;

/**
 * Description of the structure for message framing in a log buffer.
 *
 * All messages are logged in frames that have a minimum header layout as follows plus a reserve then
 * the encoded message follows:
 *
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |R|                       Frame Length                          |
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-------------------------------+
 *  |  Version      |B|E| Flags     |             Type              |
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-------------------------------+
 *  |R|                       Term Offset                           |
 *  +-+-------------------------------------------------------------+
 *  |                      Additional Fields                       ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *  |                        Encoded Message                       ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 * </pre>
 *
 * The (B)egin and (E)nd flags are used for message fragmentation. R is for reserved bit.
 * Both are set for a message that does not span frames.
 */
public class FrameDescriptor
{
    /**
     * Alignment as a multiple of bytes for each frame. The length field will store the unaligned length in bytes.
     */
    public static final int FRAME_ALIGNMENT = 8;

    /**
     * Beginning fragment of a frame.
     */
    public static final byte BEGIN_FRAG = (byte)0b1000_0000;

    /**
     * End fragment of a frame.
     */
    public static final byte END_FRAG = (byte)0b0100_0000;

    /**
     * End fragment of a frame.
     */
    public static final byte UNFRAGMENTED = (byte)(BEGIN_FRAG | END_FRAG);

    /**
     * Offset within a frame at which the verion field begins
     */
    public static final int VERSION_OFFSET = DataHeaderFlyweight.VERSION_FIELD_OFFSET;

    /**
     * Offset within a frame at which the flags field begins
     */
    public static final int FLAGS_OFFSET = DataHeaderFlyweight.FLAGS_FIELD_OFFSET;

    /**
     * Offset within a frame at which the type field begins
     */
    public static final int TYPE_OFFSET = DataHeaderFlyweight.TYPE_FIELD_OFFSET;

    /**
     * Offset within a frame at which the term offset field begins
     */
    public static final int TERM_OFFSET = DataHeaderFlyweight.TERM_OFFSET_FIELD_OFFSET;

    /**
     * Padding frame type to indicate end of the log is not in use.
     */
    public static final int PADDING_FRAME_TYPE = 0;

    /**
     * Compute the maximum supported message length for a buffer of given capacity.
     *
     * @param capacity of the log buffer.
     * @return the maximum supported length for a message.
     */
    public static int computeMaxMessageLength(final int capacity)
    {
        return capacity / 8;
    }

    /**
     * Check the the default header is equal to {@link DataHeaderFlyweight#HEADER_LENGTH}
     *
     * @param length to check.
     * @throws IllegalStateException if the default header is invalid.
     */
    public static void checkHeaderLength(final int length)
    {
        if (length != HEADER_LENGTH)
        {
            final String s = String.format(
                "Frame header length %d must be equal to %d",
                length, HEADER_LENGTH);
            throw new IllegalStateException(s);
        }
    }

    /**
     * Check the max frame length is a multiple of {@link #FRAME_ALIGNMENT}
     *
     * @param length to be applied to all logged frames.
     * @throws IllegalStateException if not a multiple of {@link #FRAME_ALIGNMENT}
     */
    public static void checkMaxFrameLength(final int length)
    {
        if ((length & (FRAME_ALIGNMENT - 1)) != 0)
        {
            final String s = String.format(
                "Max frame length must be a multiple of %d, length=%d",
                FRAME_ALIGNMENT,
                length);
            throw new IllegalStateException(s);
        }
    }

    /**
     * The buffer offset at which the version field begins.
     *
     * @param frameOffset at which the frame begins.
     * @return the offset at which the version field begins.
     */
    public static int versionOffset(final int frameOffset)
    {
        return frameOffset + VERSION_OFFSET;
    }

    /**
     * The buffer offset at which the flags field begins.
     *
     * @param frameOffset at which the frame begins.
     * @return the offset at which the flags field begins.
     */
    public static int flagsOffset(final int frameOffset)
    {
        return frameOffset + FLAGS_OFFSET;
    }

    /**
     * The buffer offset at which the type field begins.
     *
     * @param frameOffset at which the frame begins.
     * @return the offset at which the type field begins.
     */
    public static int typeOffset(final int frameOffset)
    {
        return frameOffset + TYPE_OFFSET;
    }

    /**
     * The buffer offset at which the term offset field begins.
     *
     * @param frameOffset at which the frame begins.
     * @return the offset at which the term offset field begins.
     */
    public static int termOffsetOffset(final int frameOffset)
    {
        return frameOffset + TERM_OFFSET;
    }

    /**
     * Read the type of of the frame from header.
     *
     * @param buffer      containing the frame.
     * @param frameOffset at which a frame begins.
     * @return the value of the frame type header.
     */
    public static int frameVersion(final UnsafeBuffer buffer, final int frameOffset)
    {
        return buffer.getByte(versionOffset(frameOffset));
    }

    /**
     * Read the type of of the frame from header.
     *
     * @param buffer      containing the frame.
     * @param frameOffset at which a frame begins.
     * @return the value of the frame type header.
     */
    public static int frameType(final UnsafeBuffer buffer, final int frameOffset)
    {
        return buffer.getShort(typeOffset(frameOffset), LITTLE_ENDIAN) & 0xFFFF;
    }

    /**
     * Is the frame starting at the frameOffset a padding frame at the end of a buffer?
     *
     * @param buffer      containing the frame.
     * @param frameOffset at which a frame begins.
     * @return true if the frame is a padding frame otherwise false.
     */
    public static boolean isPaddingFrame(final UnsafeBuffer buffer, final int frameOffset)
    {
        return buffer.getShort(typeOffset(frameOffset)) == PADDING_FRAME_TYPE;
    }

    /**
     * Get the length of a frame from the header.
     *
     * @param buffer      containing the frame.
     * @param frameOffset at which a frame begins.
     * @return the value for the frame length.
     */
    public static int frameLength(final UnsafeBuffer buffer, final int frameOffset)
    {
        return buffer.getInt(frameOffset, LITTLE_ENDIAN);
    }

    /**
     * Get the length of a frame from the header as a volatile read.
     *
     * @param buffer      containing the frame.
     * @param frameOffset at which a frame begins.
     * @return the value for the frame length.
     */
    public static int frameLengthVolatile(final UnsafeBuffer buffer, final int frameOffset)
    {
        int frameLength = buffer.getIntVolatile(frameOffset);

        if (ByteOrder.nativeOrder() != LITTLE_ENDIAN)
        {
            frameLength = Integer.reverseBytes(frameLength);
        }

        return frameLength;
    }

    /**
     * Write the length header for a frame in a memory ordered fashion.
     *
     * @param buffer      containing the frame.
     * @param frameOffset at which a frame begins.
     * @param frameLength field to be set for the frame.
     */
    public static void frameLengthOrdered(final UnsafeBuffer buffer, final int frameOffset, int frameLength)
    {
        if (ByteOrder.nativeOrder() != LITTLE_ENDIAN)
        {
            frameLength = Integer.reverseBytes(frameLength);
        }

        buffer.putIntOrdered(frameOffset, frameLength);
    }

    /**
     * Write the type field for a frame.
     *
     * @param buffer      containing the frame.
     * @param frameOffset at which a frame begins.
     * @param type        type value for the frame.
     */
    public static void frameType(final UnsafeBuffer buffer, final int frameOffset, final int type)
    {
        buffer.putShort(typeOffset(frameOffset), (short)type, LITTLE_ENDIAN);
    }

    /**
     * Write the flags field for a frame.
     *
     * @param buffer      containing the frame.
     * @param frameOffset at which a frame begins.
     * @param flags       value for the frame.
     */
    public static void frameFlags(final UnsafeBuffer buffer, final int frameOffset, final byte flags)
    {
        buffer.putByte(flagsOffset(frameOffset), flags);
    }

    /**
     * Write the term offset field for a frame.
     *
     * @param buffer      containing the frame.
     * @param frameOffset at which a frame begins.
     * @param termOffset  value for the frame.
     */
    public static void frameTermOffset(final UnsafeBuffer buffer, final int frameOffset, final int termOffset)
    {
        buffer.putInt(termOffsetOffset(frameOffset), termOffset, LITTLE_ENDIAN);
    }
}
