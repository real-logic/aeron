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

import static uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor.*;

/**
 * A log buffer reader.
 *
 * <b>Note:</b> Reading from the log is thread safe, but each thread needs its own instance of this class.
 */
public class LogReader extends LogBufferPartition
{
    private final Header header;
    private int offset = 0;

    /**
     * Construct a reader for a log and associated state buffer.
     *
     * @param termBuffer     containing the data frames.
     * @param metaDataBuffer containing the state data for the log.
     */
    public LogReader(final UnsafeBuffer termBuffer, final UnsafeBuffer metaDataBuffer)
    {
        super(termBuffer, metaDataBuffer);
        header = new Header(termBuffer);
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
        final int capacity = termBuffer().capacity();
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
     * @param handler          the handler for data that has been read
     * @param framesCountLimit limit the number of frames read.
     * @return the number of frames read
     */
    public int read(final DataHandler handler, final int framesCountLimit)
    {
        int framesCounter = 0;
        int offset = this.offset;
        final Header header = this.header;
        final UnsafeBuffer termBuffer = termBuffer();
        final int capacity = termBuffer.capacity();

        while (offset < capacity && framesCounter < framesCountLimit)
        {
            final int frameLength = frameLengthVolatile(termBuffer, offset);
            if (0 == frameLength)
            {
                break;
            }

            try
            {
                if (!isPaddingFrame(termBuffer, offset))
                {
                    header.offset(offset);
                    handler.onData(termBuffer, offset + Header.LENGTH, frameLength - Header.LENGTH, header);

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
        return offset >= termBuffer().capacity();
    }
}
