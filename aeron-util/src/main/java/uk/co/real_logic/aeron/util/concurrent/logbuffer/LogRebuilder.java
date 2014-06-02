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
package uk.co.real_logic.aeron.util.concurrent.logbuffer;

import uk.co.real_logic.aeron.util.BitUtil;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

import java.nio.ByteOrder;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.BufferDescriptor.*;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.lengthOffset;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.termOffsetOffset;

/**
 * Rebuild a log buffer based on incoming frames that can be out-of-order.
 *
 * <b>Note:</b> Only one rebuilder should rebuild a log at any given time. This is not thread safe
 * by rebuilder instance or across rebuilder instances.
 */
public class LogRebuilder
{
    private final AtomicBuffer stateBuffer;
    private final AtomicBuffer logBuffer;

    /**
     * Construct a rebuilder over a log and state buffer.
     *
     * @param logBuffer containing the sequence of frames.
     * @param stateBuffer containing the state of the rebuild process.
     */
    public LogRebuilder(final AtomicBuffer logBuffer, final AtomicBuffer stateBuffer)
    {
        checkLogBuffer(logBuffer);
        checkStateBuffer(stateBuffer);

        this.stateBuffer = stateBuffer;
        this.logBuffer = logBuffer;
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
    public void insert(final AtomicBuffer packet, final int srcOffset, final int length)
    {
        final int termOffset = packet.getInt(termOffsetOffset(srcOffset), LITTLE_ENDIAN);
        int tail = tail();

        if (termOffset >= tail)
        {
            logBuffer.putBytes(termOffset, packet, srcOffset, length);

            int alignedFrameLength;
            while ((alignedFrameLength = alignedFrameLength(tail)) != 0)
            {
                tail += alignedFrameLength;
            }

            putTailOrdered(tail);

            final int endOfFrame = termOffset + length;
            if (endOfFrame > highWaterMark())
            {
                putHighWaterMark(endOfFrame);
            }
        }
    }

    /**
     * Is the rebuild of this log complete?
     *
     * @return true if it is complete otherwise false.
     */
    public boolean isComplete()
    {
        return stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET) >= logBuffer.capacity();
    }

    private int alignedFrameLength(final int tail)
    {
        return BitUtil.align(logBuffer.getInt(lengthOffset(tail), ByteOrder.LITTLE_ENDIAN), FRAME_ALIGNMENT);
    }

    private void putTailOrdered(int tail)
    {
        stateBuffer.putIntOrdered(TAIL_COUNTER_OFFSET, tail);
    }

    private int highWaterMark()
    {
        return stateBuffer.getInt(HIGH_WATER_MARK_OFFSET);
    }

    private int tail()
    {
        return stateBuffer.getInt(TAIL_COUNTER_OFFSET);
    }

    private void putHighWaterMark(final int highWaterMark)
    {
        stateBuffer.putIntOrdered(HIGH_WATER_MARK_OFFSET, highWaterMark);
    }
}
