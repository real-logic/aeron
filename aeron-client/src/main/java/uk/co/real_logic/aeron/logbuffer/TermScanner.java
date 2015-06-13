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

import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static uk.co.real_logic.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static uk.co.real_logic.aeron.logbuffer.FrameDescriptor.frameLengthVolatile;
import static uk.co.real_logic.aeron.logbuffer.FrameDescriptor.isPaddingFrame;
import static uk.co.real_logic.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static uk.co.real_logic.agrona.BitUtil.align;

/**
 * Scans a term buffer for an availability range of messages.
 *
 * This can be used to concurrently read a term buffer which is being appended to.
 */
public final class TermScanner
{
    /**
     * Scan the term buffer for availability of new messages from a given offset up to a maxLength of bytes.
     *
     * @param termBuffer to be scanned for new messages
     * @param offset     at which the scan should begin.
     * @param maxLength  in bytes of how much should be scanned.
     * @return resulting status of the scan which packs the available bytes and padding into a long.
     */
    public static long scanForAvailability(final UnsafeBuffer termBuffer, final int offset, int maxLength)
    {
        maxLength = Math.min(maxLength, termBuffer.capacity() - offset);
        int available = 0;
        int padding = 0;

        do
        {
            final int frameOffset = offset + available;
            final int frameLength = frameLengthVolatile(termBuffer, frameOffset);
            if (frameLength <= 0)
            {
                break;
            }

            int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);
            if (isPaddingFrame(termBuffer, frameOffset))
            {
                padding = alignedFrameLength - HEADER_LENGTH;
                alignedFrameLength = HEADER_LENGTH;
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

        return scanOutcome(padding, available);
    }

    /**
     * Pack the values for available and padding into a long for returning on the stack.
     *
     * @param padding   value to be packed.
     * @param available value to be packed.
     * @return a long with both ints packed into it.
     */
    public static long scanOutcome(final int padding, final int available)
    {
        return ((long)padding << 32) | available;
    }

    /**
     * The number of bytes that are available to be read after a scan.
     *
     * @param scanOutcome into which the padding value has been packed.
     * @return the count of bytes that are available to be read.
     */
    public static int available(final long scanOutcome)
    {
        return (int)scanOutcome;
    }

    /**
     * The count of bytes that should be added for padding to the position on top of what is available
     *
     * @param scanOutcome into which the padding value has been packed.
     * @return the count of bytes that should be added for padding to the position on top of what is available.
     */
    public static int padding(final long scanOutcome)
    {
        return (int)(scanOutcome >>> 32);
    }
}
