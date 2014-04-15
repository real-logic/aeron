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

import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.checkLogBuffer;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.checkStateBuffer;

/**
 * A log buffer reader.
 *
 * <b>Note:</b> Reading from the log is thread safe, but each thread needs its own instance of this class.
 */
public class Reader
{
    private final AtomicBuffer logBuffer;
    private final StateViewer stateViewer;
    private int cursor;

    public Reader(final AtomicBuffer logBuffer, final AtomicBuffer stateBuffer)
    {
        checkLogBuffer(logBuffer);
        checkStateBuffer(stateBuffer);

        this.logBuffer = logBuffer;
        this.stateViewer = new StateViewer(stateBuffer);
        cursor = 0;
    }

    /**
     * Handler for reading data that is coming off the log.
     */
    @FunctionalInterface
    public interface FrameHandler
    {
        void onFrame(final AtomicBuffer buffer, final int offset, final int length);
    }

    /**
     * Move the read cursor to the specified offset.
     *
     * @param offset the location to move the read cursor to.
     */
    public void seek(final int offset)
    {
        final int tail = stateViewer.tailVolatile();
        if (offset > tail)
        {
            throw new IllegalStateException("Cannot seek to " + offset + ", the tail is only " + tail);
        }

        if ((offset & (FRAME_ALIGNMENT - 1)) != 0)
        {
            throw new IllegalArgumentException("Cannot seek to an offset that isn't a multiple of " + FRAME_ALIGNMENT);
        }

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
        final int tail = stateViewer.tailVolatile();
        int counter = 0;
        while (tail > cursor)
        {
            final int frameLength = FrameDescriptor.waitForFrameLength(cursor, logBuffer);
            final int type = logBuffer.getInt(FrameDescriptor.typeOffset(cursor));

            try
            {
                if (type != LogBufferDescriptor.PADDING_MSG_TYPE)
                {
                    handler.onFrame(logBuffer, cursor, frameLength);
                }
            }
            finally
            {
                cursor += frameLength;
                counter++;
            }
        }
        return counter;
    }

}
