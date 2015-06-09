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
package uk.co.real_logic.aeron.logbuffer;

import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static uk.co.real_logic.aeron.logbuffer.FrameDescriptor.*;
import static uk.co.real_logic.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;

/**
 * A term buffer reader.
 * <p>
 * <b>Note:</b> Reading from the term is thread safe, but each thread needs its own instance of this class.
 */
public class TermReader
{
    private int offset = 0;

    public TermReader()
    {
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
     * If a fragmentsLimit of 0 or less is passed then at least one read will be attempted.
     *
     * @param termBuffer     to be read for fragments.
     * @param termOffset     offset within the buffer that the read should begin.
     * @param handler        the handler for data that has been read
     * @param fragmentsLimit limit the number of frames read.
     * @param header         to be used for mapping over the header for a given fragment.
     * @return the number of frames read
     */
    public int read(
        final UnsafeBuffer termBuffer,
        int termOffset,
        final FragmentHandler handler,
        final int fragmentsLimit,
        final Header header)
    {
        int fragmentsRead = 0;
        final int capacity = termBuffer.capacity();
        offset = termOffset;

        do
        {
            final int frameLength = frameLengthVolatile(termBuffer, termOffset);
            if (frameLength <= 0)
            {
                break;
            }

            final int currentTermOffset = termOffset;
            termOffset += BitUtil.align(frameLength, FRAME_ALIGNMENT);
            offset = termOffset;

            if (!isPaddingFrame(termBuffer, currentTermOffset))
            {
                header.buffer(termBuffer);
                header.offset(currentTermOffset);

                handler.onFragment(termBuffer, currentTermOffset + HEADER_LENGTH, frameLength - HEADER_LENGTH, header);

                ++fragmentsRead;
            }
        }
        while (fragmentsRead < fragmentsLimit && termOffset < capacity);

        return fragmentsRead;
    }
}
