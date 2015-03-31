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
 * <p>
 * <b>Note:</b> Reading from the log is thread safe, but each thread needs its own instance of this class.
 */
public class LogReader extends LogBufferPartition
{
    private int offset = 0;
    private final Header header;

    /**
     * Construct a reader for a log and associated meta data buffer.
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
     * Reads data from the log buffer.
     *
     * @param termOffset       offset within the buffer that the read should begin.
     * @param handler          the handler for data that has been read
     * @param framesCountLimit limit the number of frames read.
     * @return the number of frames read
     */
    public int read(int termOffset, final DataHandler handler, final int framesCountLimit)
    {
        int framesCounter = 0;
        final Header header = this.header;
        final UnsafeBuffer termBuffer = termBuffer();
        final int capacity = termBuffer.capacity();
        offset = termOffset;

        while (framesCounter < framesCountLimit && termOffset < capacity)
        {
            final int frameLength = frameLengthVolatile(termBuffer, termOffset);
            if (0 == frameLength)
            {
                break;
            }

            final int currentTermOffset = termOffset;
            termOffset += BitUtil.align(frameLength, FRAME_ALIGNMENT);
            offset = termOffset;

            if (!isPaddingFrame(termBuffer, currentTermOffset))
            {
                header.offset(currentTermOffset);
                handler.onData(termBuffer, currentTermOffset + Header.LENGTH, frameLength - Header.LENGTH, header);

                ++framesCounter;
            }
        }

        return framesCounter;
    }
}
