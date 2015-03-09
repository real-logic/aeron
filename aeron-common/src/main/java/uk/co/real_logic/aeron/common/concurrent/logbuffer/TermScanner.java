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

import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor.frameLengthVolatile;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor.isPaddingFrame;
import static uk.co.real_logic.agrona.BitUtil.align;

/**
 * Scans a term buffer for an availability range of messages.
 *
 * This can be used to concurrently read a term buffer which is being appended to.
 */
public final class TermScanner
{
    private final int alignedHeaderLength;
    private int padding;

    /**
     * Create a scanner for checking availability in a term buffer with a fixed header length for all messages.
     *
     * @param alignedHeaderLength which all messages will have.
     */
    public TermScanner(final int alignedHeaderLength)
    {
        this.alignedHeaderLength = alignedHeaderLength;
    }

    /**
     * Scan the term buffer for availability of new messages from a given offset up to a maxLength of bytes.
     *
     * @param termBuffer to be scanned for new messages
     * @param offset     at which the scan should begin.
     * @param maxLength  in bytes of how much should be scanned.
     * @return number of bytes available
     */
    public int scanForAvailability(final UnsafeBuffer termBuffer, final int offset, int maxLength)
    {
        maxLength = Math.min(maxLength, termBuffer.capacity() - offset);
        int available = 0;
        int padding = 0;

        do
        {
            final int frameOffset = offset + available;
            final int frameLength = frameLengthVolatile(termBuffer, frameOffset);
            if (0 == frameLength)
            {
                break;
            }

            int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);
            if (isPaddingFrame(termBuffer, frameOffset))
            {
                padding = alignedFrameLength - alignedHeaderLength;
                alignedFrameLength = alignedHeaderLength;
            }

            available += alignedFrameLength;

            if (available > maxLength)
            {
                available -= alignedFrameLength;
                padding = 0;
                break;
            }
        }
        while ((available + padding) < maxLength);

        this.padding = padding;

        return available;
    }

    /**
     * The count of bytes that should be added for padding to the position on top of what is available
     *
     * @return the count of bytes that should be added for padding to the position on top of what is available.
     */
    public int padding()
    {
        return padding;
    }
}
