/*
 * Copyright 2014 Real Logic Ltd.
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

import uk.co.real_logic.aeron.common.BitUtil;
import uk.co.real_logic.aeron.common.concurrent.UnsafeBuffer;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor.*;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.HIGH_WATER_MARK_OFFSET;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.TAIL_COUNTER_OFFSET;

/**
 * Rebuild a log buffer based on incoming frames that can be out-of-order.
 *
 * <b>Note:</b> Only one rebuilder should rebuild a log at any given time. This is not thread safe
 * by rebuilder instance or across rebuilder instances.
 */
public class LogRebuilder extends LogBuffer
{
    /**
     * Construct a rebuilder over a log and state buffer.
     *
     * @param logBuffer containing the sequence of frames.
     * @param stateBuffer containing the state of the rebuild process.
     */
    public LogRebuilder(final UnsafeBuffer logBuffer, final UnsafeBuffer stateBuffer)
    {
        super(logBuffer, stateBuffer);
    }

    /**
     * Set the starting tail offset.
     *
     * @param offset to start tail at
     */
    public void tail(final int offset)
    {
        putTailOrdered(stateBuffer(), offset);
    }

    /**
     * Is the rebuild of this log complete?
     *
     * @return true if it is complete otherwise false.
     */
    public boolean isComplete()
    {
        return stateBuffer().getIntVolatile(TAIL_COUNTER_OFFSET) >= capacity();
    }

    /**
     * Insert a packet of frames into the log at the appropriate offset as indicated by the term offset header.
     *
     * The tail and high-water-mark will be updated as appropriate. Data can be consumed up to the the tail. The
     * high-water-mark can be used to detect loss.
     *
     * @param packet containing a sequence of frames.
     * @param srcOffset in the packet at which the frames begin.
     * @param length of the sequence of frames in bytes.
     */
    public void insert(final UnsafeBuffer packet, final int srcOffset, final int length)
    {
        final int termOffset = packet.getInt(termOffsetOffset(srcOffset), LITTLE_ENDIAN);
        final int tail = tail();

        if (termOffset >= tail)
        {
            final int lengthOffset = lengthOffset(srcOffset);
            final int frameLength = packet.getInt(lengthOffset, LITTLE_ENDIAN);
            packet.putInt(lengthOffset, 0, LITTLE_ENDIAN);

            final UnsafeBuffer logBuffer = logBuffer();
            logBuffer.putBytes(termOffset, packet, srcOffset, length);
            FrameDescriptor.frameLengthOrdered(logBuffer, termOffset, frameLength);

            updateCompetitionStatus(logBuffer, termOffset, length, tail);
        }
    }

    private void updateCompetitionStatus(final UnsafeBuffer logBuffer, final int termOffset, final int length, int tail)
    {
        final int capacity = capacity();
        int frameLength;
        while ((tail < capacity) && (frameLength = logBuffer.getInt(lengthOffset(tail), LITTLE_ENDIAN)) != 0)
        {
            final int alignedFrameLength = BitUtil.align(frameLength, FRAME_ALIGNMENT);
            tail += alignedFrameLength;
        }

        final UnsafeBuffer stateBuffer = stateBuffer();
        putTailOrdered(stateBuffer, tail);

        final int endOfFrame = termOffset + length;
        if (endOfFrame > highWaterMark())
        {
            putHighWaterMark(stateBuffer, endOfFrame);
        }
    }

    private static void putTailOrdered(final UnsafeBuffer stateBuffer, final int tail)
    {
        stateBuffer.putIntOrdered(TAIL_COUNTER_OFFSET, tail);
    }

    private static void putHighWaterMark(final UnsafeBuffer stateBuffer, final int highWaterMark)
    {
        stateBuffer.putIntOrdered(HIGH_WATER_MARK_OFFSET, highWaterMark);
    }
}
