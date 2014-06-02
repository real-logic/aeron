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
package uk.co.real_logic.aeron.util.concurrent.logbuffer;

import uk.co.real_logic.aeron.util.BitUtil;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.BufferDescriptor.*;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.*;

/**
 * A log buffer reader.
 *
 * <b>Note:</b> Reading from the log is thread safe, but each thread needs its own instance of this class.
 */
public class LogReader
{
    /**
     * Handler for reading data that is coming from a log buffer.
     */
    @FunctionalInterface
    public interface FrameHandler
    {
        void onFrame(final AtomicBuffer buffer, final int offset, final int length);
    }

    private final AtomicBuffer logBuffer;
    private final StateViewer stateViewer;
    private int cursor = 0;

    public LogReader(final AtomicBuffer logBuffer, final AtomicBuffer stateBuffer)
    {
        checkLogBuffer(logBuffer);
        checkStateBuffer(stateBuffer);

        this.logBuffer = logBuffer;
        this.stateViewer = new StateViewer(stateBuffer);
    }

    /**
     * Move the read cursor to the specified offset.
     *
     * @param offset the location to move the read cursor to.
     */
    public void seek(final int offset)
    {
        checkOffset(offset, stateViewer.tailVolatile());
        checkOffsetAlignment(offset);

        cursor = offset;
    }

    /**
     * Reads data from the log buffer.
     *
     * @param handler the handler for data that has been read
     * @return the number of frames read
     */
    public int read(final FrameHandler handler)
    {
        int counter = 0;
        final int tail = stateViewer.tailVolatile();

        while (tail > cursor)
        {
            final int frameLength = waitForFrameLength(logBuffer, cursor);
            final int length = type(cursor) != PADDING_MSG_TYPE ? frameLength : FRAME_ALIGNMENT;

            try
            {
                handler.onFrame(logBuffer, cursor, length);
            }
            finally
            {
                cursor += BitUtil.align(frameLength, FRAME_ALIGNMENT);
                counter++;
            }
        }

        return counter;
    }

    /**
     * Has the buffer been read right to the end?
     *
     * @return true if the whole buffer has been read otherwise false if read cursor has not yet reached capacity.
     */
    public boolean isComplete()
    {
        return cursor >= logBuffer.capacity();
    }

    /**
     * Returns the current tail of the buffer, using a volatile read.
     *
     * @return the current tail.
     */
    public int tailVolatile()
    {
        return stateViewer.tailVolatile();
    }

    private int type(final int frameOffset)
    {
        return logBuffer.getInt(typeOffset(frameOffset), LITTLE_ENDIAN);
    }
}
