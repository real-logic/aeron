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
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.checkTermBuffer;

/**
 * A term buffer reader.
 * <p>
 * <b>Note:</b> Reading from the term is thread safe, but each thread needs its own instance of this class.
 */
public class TermReader
{
    private final UnsafeBuffer termBuffer;
    private final Header header;
    private int offset = 0;

    /**
     * Construct a reader for a log and associated meta data buffer.
     *
     * @param termBuffer containing the data frames.
     */
    public TermReader(final UnsafeBuffer termBuffer)
    {
        checkTermBuffer(termBuffer);

        this.termBuffer = termBuffer;
        header = new Header(termBuffer);
    }

    /**
     * Get the term buffer that the reader reads.
     *
     * @return the term buffer that the reader reads.
     */
    public UnsafeBuffer termBuffer()
    {
        return termBuffer;
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
     * If a framesCountLimit of 0 or less is passed then at least one read will be attempted.
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
        final UnsafeBuffer termBuffer = this.termBuffer;
        final int capacity = termBuffer.capacity();
        offset = termOffset;

        do
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
        while (framesCounter < framesCountLimit && termOffset < capacity);

        return framesCounter;
    }
}
