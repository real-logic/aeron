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
import static uk.co.real_logic.aeron.logbuffer.TermPatcher.PatchStatus.*;
import static uk.co.real_logic.aeron.protocol.HeaderFlyweight.HDR_TYPE_PAD;

/**
 * A term buffer patcher.
 */
public class TermPatcher
{
    public enum PatchStatus
    {
        /** No action has been taken during patching operation. */
        NO_ACTION,

        /** The term has been patched so that the log can progress. */
        PATCHED,

        /** The term has been patched from the offset until the end of the term. */
        PATCHED_TO_END,
    }


    /**
     * Attempt to patch the current term at the current offset.
     *
     * <ol>
     *     <li>Current position length is &gt; 0, then return</li>
     *     <li>Current position length is 0, scan forward by frame alignment until, one of the following:
     *     <ol>
     *         <li>reach a non-0 length, patch up to indicated position (check original frame length for non-0)</li>
     *         <li>reach end of term and tail position &gt;= end of term, patch up to end of term (check original
     *             frame length for non-0)
     *         </li>
     *         <li>reach tail position &lt; end of term, do NOT patch</li>
     *     </ol>
     *     </li>
     * </ol>
     *
     * @param termBuffer to patch
     * @param termOffset to patch at
     * @param tailOffset to patch up to
     * @return whether patching was done, not done, or patched to end of term
     */
    public static PatchStatus patch(final UnsafeBuffer termBuffer, final int termOffset, final int tailOffset)
    {
        PatchStatus result = NO_ACTION;
        int frameLength = frameLengthVolatile(termBuffer, termOffset);

        if (frameLength < 0)
        {
            frameType(termBuffer, termOffset, HDR_TYPE_PAD);
            frameLengthOrdered(termBuffer, termOffset, -frameLength);
            result = PATCHED;
        }
        else if (0 == frameLength)
        {
            int currentOffset = termOffset + FRAME_ALIGNMENT;

            while (currentOffset < tailOffset)
            {
                frameLength = frameLengthVolatile(termBuffer, currentOffset);

                if (frameLength != 0)
                {
                    if (0 == frameLengthVolatile(termBuffer, termOffset))
                    {
                        frameType(termBuffer, termOffset, HDR_TYPE_PAD);
                        frameLengthOrdered(termBuffer, termOffset, currentOffset - termOffset);
                        result = PATCHED;
                    }

                    break;
                }

                currentOffset += FRAME_ALIGNMENT;
            }

            if (currentOffset == termBuffer.capacity())
            {
                if (0 == frameLengthVolatile(termBuffer, termOffset))
                {
                    frameType(termBuffer, termOffset, HDR_TYPE_PAD);
                    frameLengthOrdered(termBuffer, termOffset, currentOffset - termOffset);
                    result = PATCHED_TO_END;
                }
            }
        }

        return result;
    }
}
