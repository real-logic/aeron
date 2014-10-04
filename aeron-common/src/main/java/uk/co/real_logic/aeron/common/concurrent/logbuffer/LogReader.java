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
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;

import static uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor.*;

/**
 * A log buffer reader.
 *
 * <b>Note:</b> Reading from the log is thread safe, but each thread needs its own instance of this class.
 */
public class LogReader extends LogBuffer
{
    private final Header header;
    private int offset = 0;

    /**
     * Construct a reader for a log and associated state buffer.
     *
     * @param logBuffer containing the data frames.
     * @param stateBuffer containing the state data for the log.
     */
    public LogReader(final AtomicBuffer logBuffer, final AtomicBuffer stateBuffer)
    {
        super(logBuffer, stateBuffer);
        header = new Header(logBuffer);
    }

    /**
     * Return the read offset
     *
     * @return offset
     */
    public int offset()
    {
        return offset;
    }

    /**
     * Move the read offset to the specified offset.
     *
     * @param offset the location to move the read offset to.
     */
    public void seek(final int offset)
    {
        final int capacity = capacity();
        if (offset < 0 || offset > capacity)
        {
            throw new IndexOutOfBoundsException(String.format("Invalid offset %d: range is 0 - %d", offset, capacity));
        }

        checkOffsetAlignment(offset);

        this.offset = offset;
    }

    /**
     * Reads data from the log buffer.
     *
     * @param handler the handler for data that has been read
     * @param framesCountLimit limit the number of frames read.
     * @return the number of frames read
     */
    public int read(final DataHandler handler, final int framesCountLimit)
    {
        int framesCounter = 0;
        int offset = this.offset;
        final int capacity = capacity();
        final AtomicBuffer logBuffer = logBuffer();
        final Header header = this.header;

        while (offset < capacity && framesCounter < framesCountLimit)
        {
            final int frameLength = frameLengthVolatile(logBuffer, offset);
            if (0 == frameLength)
            {
                break;
            }

            try
            {
                if (!isPaddingFrame(logBuffer, offset))
                {
                    header.offset(offset);
                    handler.onData(logBuffer, offset + Header.LENGTH, frameLength - Header.LENGTH, header);

                    ++framesCounter;
                }
            }
            finally
            {
                offset += BitUtil.align(frameLength, FRAME_ALIGNMENT);
                this.offset = offset;
            }
        }

        return framesCounter;
    }

    /**
     * Has the buffer been read right to the end?
     *
     * @return true if the whole buffer has been read otherwise false if read offset has not yet reached capacity.
     */
    public boolean isComplete()
    {
        return offset >= capacity();
    }
}
