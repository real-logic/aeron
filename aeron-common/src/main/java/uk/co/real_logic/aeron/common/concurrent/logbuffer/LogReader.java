/*
 * Copyright 2013 Real Logic Ltd.
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
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;

import java.nio.ByteOrder;

import static uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor.*;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.PADDING_FRAME_TYPE;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.checkOffset;

/**
 * A log buffer reader.
 *
 * <b>Note:</b> Reading from the log is thread safe, but each thread needs its own instance of this class.
 */
public class LogReader extends LogBuffer
{
    /**
     * Handler for reading data that is coming from a log buffer.
     */
    @FunctionalInterface
    public interface FrameHandler
    {
        void onFrame(final AtomicBuffer buffer, final int offset, final int length);
    }

    private int cursor = 0;

    public LogReader(final AtomicBuffer logBuffer, final AtomicBuffer stateBuffer)
    {
        super(logBuffer, stateBuffer);
    }

    /**
     * Move the read cursor to the specified offset.
     *
     * @param offset the location to move the read cursor to.
     */
    public void seek(final int offset)
    {
        checkOffset(offset, tailVolatile());
        checkOffsetAlignment(offset);

        cursor = offset;
    }

    /**
     * Reads data from the log buffer.
     *
     * @param handler the handler for data that has been read
     * @param framesCountLimit the number of frames read.
     * @return the number of frames read
     */
    public int read(final FrameHandler handler, final int framesCountLimit)
    {
        int framesCounter = 0;
        final int tail = tailVolatile();

        while (tail > cursor && framesCounter < framesCountLimit)
        {
            final int frameLength = waitForFrameLength(logBuffer(), cursor);
            try
            {
                if (frameType(cursor) != PADDING_FRAME_TYPE)
                {
                    ++framesCounter;
                    handler.onFrame(logBuffer(), cursor, frameLength);

                }
            }
            finally
            {
                cursor += BitUtil.align(frameLength, FRAME_ALIGNMENT);
            }
        }

        return framesCounter;
    }

    /**
     * Has the buffer been read right to the end?
     *
     * @return true if the whole buffer has been read otherwise false if read cursor has not yet reached capacity.
     */
    public boolean isComplete()
    {
        return cursor >= capacity();
    }

    private int frameType(final int frameOffset)
    {
        return logBuffer().getShort(typeOffset(frameOffset), ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
    }
}
