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

import java.nio.ByteOrder;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

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
 *  |  Version      |B|E| Flags     |             Type              |
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-------------------------------+
 *  |R|                       Frame Length                          |
 *  +-+-------------------------------------------------------------+
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
     * Word alignment for fields.
     */
    public static final int WORD_ALIGNMENT = BitUtil.SIZE_OF_LONG;

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
     * Length in bytes for the base header fields.
     */
    public static final int BASE_HEADER_LENGTH = 12;

    /**
     * Offset within a frame at which the version field begins
     */
    public static final int VERSION_OFFSET = 0;

    /**
     * Offset within a frame at which the flags field begins
     */
    public static final int FLAGS_OFFSET = 1;

    /**
     * Offset within a frame at which the type field begins
     */
    public static final int TYPE_OFFSET = 2;

    /**
     * Offset within a frame at which the length field begins
     */
    public static final int LENGTH_OFFSET = 4;

    /**
     * Offset within a frame at which the term offset field begins
     */
    public static final int TERM_OFFSET = 8;

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
     * Check the the default header is greater than or equal in length to
     * {@link #BASE_HEADER_LENGTH} and a multiple of {@link #WORD_ALIGNMENT}.
     *
     * @param length to check.
     * @throws IllegalStateException if the default header is invalid.
     */
    public static void checkHeaderLength(final int length)
    {
        if (length < BASE_HEADER_LENGTH)
        {
            final String s = String.format(
                "Frame header length must not be less than %d, length=%d",
                BASE_HEADER_LENGTH,
                length);
            throw new IllegalStateException(s);
        }

        if (length % WORD_ALIGNMENT != 0)
        {
            final String s = String.format(
                "Frame header length must be a multiple of %d, length=%d",
                WORD_ALIGNMENT,
                length);
            throw new IllegalStateException(s);
        }
    }

    /**
     * Check that a given offset is at the correct {@link FrameDescriptor#FRAME_ALIGNMENT} for a frame to begin.
     *
     * @param offset to be checked.
     * @throws IllegalArgumentException if the offset is not on a frame alignment boundary.
     */
    public static void checkOffsetAlignment(final int offset)
    {
        if ((offset & (FRAME_ALIGNMENT - 1)) != 0)
        {
            throw new IllegalArgumentException("Cannot seek to an offset that isn't a multiple of " + FRAME_ALIGNMENT);
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
     * The buffer offset at which the length field begins.
     *
     * @param frameOffset at which the frame begins.
     * @return the offset at which the length field begins.
     */
    public static int lengthOffset(final int frameOffset)
    {
        return frameOffset + LENGTH_OFFSET;
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
     * @param termBuffer  containing the frame.
     * @param frameOffset at which a frame begins.
     * @return the value of the frame type header.
     */
    public static int frameType(final UnsafeBuffer termBuffer, final int frameOffset)
    {
        return termBuffer.getShort(typeOffset(frameOffset), LITTLE_ENDIAN) & 0xFFFF;
    }

    /**
     * Is the frame starting at the frameOffset a padding frame at the end of a buffer?
     *
     * @param termBuffer  containing the frame.
     * @param frameOffset at which a frame begins.
     * @return true if the frame is a padding frame otherwise false.
     */
    public static boolean isPaddingFrame(final UnsafeBuffer termBuffer, final int frameOffset)
    {
        return termBuffer.getShort(typeOffset(frameOffset)) == PADDING_FRAME_TYPE;
    }

    /**
     * Get the length of a frame from the header as a volatile read.
     *
     * @param termBuffer  containing the frame.
     * @param frameOffset at which a frame begins.
     * @return the value for the frame length.
     */
    public static int frameLengthVolatile(final UnsafeBuffer termBuffer, final int frameOffset)
    {
        int frameLength = termBuffer.getIntVolatile(lengthOffset(frameOffset));

        if (ByteOrder.nativeOrder() != LITTLE_ENDIAN)
        {
            frameLength = Integer.reverseBytes(frameLength);
        }

        return frameLength;
    }

    /**
     * Write the length header for a frame in a memory ordered fashion.
     *
     * @param termBuffer  ontaining the frame.
     * @param frameOffset at which a frame begins.
     * @param frameLength field to be set for the frame.
     */
    public static void frameLengthOrdered(final UnsafeBuffer termBuffer, final int frameOffset, int frameLength)
    {
        if (ByteOrder.nativeOrder() != LITTLE_ENDIAN)
        {
            frameLength = Integer.reverseBytes(frameLength);
        }

        termBuffer.putIntOrdered(lengthOffset(frameOffset), frameLength);
    }

    /**
     * Write the type field for a frame.
     *
     * @param termBuffer  containing the frame.
     * @param frameOffset at which a frame begins.
     * @param type        type value for the frame.
     */
    public static void frameType(final UnsafeBuffer termBuffer, final int frameOffset, final int type)
    {
        termBuffer.putShort(typeOffset(frameOffset), (short)type, LITTLE_ENDIAN);
    }

    /**
     * Write the flags field for a frame.
     *
     * @param termBuffer  containing the frame.
     * @param frameOffset at which a frame begins.
     * @param flags       value for the frame.
     */
    public static void frameFlags(final UnsafeBuffer termBuffer, final int frameOffset, final byte flags)
    {
        termBuffer.putByte(flagsOffset(frameOffset), flags);
    }

    /**
     * Write the term offset field for a frame.
     *
     * @param termBuffer  containing the frame.
     * @param frameOffset at which a frame begins.
     * @param termOffset  value for the frame.
     */
    public static void frameTermOffset(final UnsafeBuffer termBuffer, final int frameOffset, final int termOffset)
    {
        termBuffer.putInt(termOffsetOffset(frameOffset), termOffset, LITTLE_ENDIAN);
    }
}
