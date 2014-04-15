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

import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.lengthOffset;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.termOffsetOffset;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.*;

public class Rebuilder
{

    private final AtomicBuffer stateBuffer;
    private final AtomicBuffer logBuffer;
    private final int capacity;

    public Rebuilder(final AtomicBuffer logBuffer, final AtomicBuffer stateBuffer)
    {
        checkLogBuffer(logBuffer);
        checkStateBuffer(stateBuffer);

        this.stateBuffer = stateBuffer;
        this.logBuffer = logBuffer;
        this.capacity = logBuffer.capacity();
    }

    public void insert(final AtomicBuffer packet, final int srcOffset, final int length)
    {
        final int termOffset = packet.getInt(termOffsetOffset(srcOffset), LITTLE_ENDIAN);
        int tail = tail();

        if (termOffset < tail)
        {
            return;
        }

        logBuffer.putBytes(termOffset, packet, srcOffset, length);

        int frameLength;
        while ((frameLength = logBuffer.getInt(lengthOffset(tail))) != 0)
        {
            tail += frameLength;
        }
        putTailOrdered(tail);

        final int endOfFrame = termOffset + length;
        if (endOfFrame > highWaterMark())
        {
            putHighWaterMark(endOfFrame);
        }
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
