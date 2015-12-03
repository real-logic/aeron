/*
 * Copyright 2015 Real Logic Ltd.
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

import static uk.co.real_logic.aeron.logbuffer.FrameDescriptor.*;
import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.applyDefaultHeader;
import static uk.co.real_logic.aeron.logbuffer.TermUnblocker.Status.*;
import static uk.co.real_logic.aeron.protocol.HeaderFlyweight.HDR_TYPE_PAD;

/**
 * Unblocks a term buffer if a publisher has died leaving the log with a partial log entry.
 */
public class TermUnblocker
{
    public enum Status
    {
        /** No action has been taken during operation. */
        NO_ACTION,

        /** The term has been unblocked so that the log can progress. */
        UNBLOCKED,

        /** The term has been unblocked from the offset until the end of the term. */
        UNBLOCKED_TO_END,
    }

    /**
     * Attempt to unblock the current term at the current offset.
     *
     * <ol>
     *     <li>Current position length is &gt; 0, then return</li>
     *     <li>Current position length is 0, scan forward by frame alignment until, one of the following:
     *     <ol>
     *         <li>reach a non-0 length, unblock up to indicated position (check original frame length for non-0)</li>
     *         <li>reach end of term and tail position &gt;= end of term, unblock up to end of term (check original
     *             frame length for non-0)
     *         </li>
     *         <li>reach tail position &lt; end of term, do NOT unblock</li>
     *     </ol>
     *     </li>
     * </ol>
     *
     * @param logMetaDataBuffer containing the default headers
     * @param activeIndex       for the default header
     * @param termBuffer        to unblock
     * @param termOffset        to unblock at
     * @param tailOffset        to unblock up to
     * @return whether unblocking was done, not done, or applied to end of term
     */
    public static Status unblock(
        final UnsafeBuffer logMetaDataBuffer,
        final int activeIndex,
        final UnsafeBuffer termBuffer,
        final int termOffset,
        final int tailOffset)
    {
        Status status = NO_ACTION;
        int frameLength = frameLengthVolatile(termBuffer, termOffset);

        if (frameLength < 0)
        {
            applyDefaultHeader(logMetaDataBuffer, activeIndex, termBuffer, termOffset);
            frameType(termBuffer, termOffset, HDR_TYPE_PAD);
            frameTermOffset(termBuffer, termOffset, termOffset);
            frameLengthOrdered(termBuffer, termOffset, -frameLength);
            status = UNBLOCKED;
        }
        else if (0 == frameLength)
        {
            int currentOffset = termOffset + FRAME_ALIGNMENT;

            while (currentOffset < tailOffset)
            {
                frameLength = frameLengthVolatile(termBuffer, currentOffset);

                if (frameLength != 0)
                {
                    if (scanBackToConfirmZeroed(termBuffer, currentOffset, termOffset))
                    {
                        applyDefaultHeader(logMetaDataBuffer, activeIndex, termBuffer, termOffset);
                        frameType(termBuffer, termOffset, HDR_TYPE_PAD);
                        frameTermOffset(termBuffer, termOffset, termOffset);
                        frameLengthOrdered(termBuffer, termOffset, currentOffset - termOffset);
                        status = UNBLOCKED;
                    }

                    break;
                }

                currentOffset += FRAME_ALIGNMENT;
            }

            if (currentOffset == termBuffer.capacity())
            {
                if (0 == frameLengthVolatile(termBuffer, termOffset))
                {
                    applyDefaultHeader(logMetaDataBuffer, activeIndex, termBuffer, termOffset);
                    frameType(termBuffer, termOffset, HDR_TYPE_PAD);
                    frameTermOffset(termBuffer, termOffset, termOffset);
                    frameLengthOrdered(termBuffer, termOffset, currentOffset - termOffset);
                    status = UNBLOCKED_TO_END;
                }
            }
        }

        return status;
    }

    public static boolean scanBackToConfirmZeroed(final UnsafeBuffer buffer, final int from, final int limit)
    {
        int i = from - FRAME_ALIGNMENT;
        boolean allZeros = true;
        while (i >= limit)
        {
            if (0 != frameLengthVolatile(buffer, i))
            {
                allZeros = false;
                break;
            }

            i -= FRAME_ALIGNMENT;
        }

        return allZeros;
    }
}
