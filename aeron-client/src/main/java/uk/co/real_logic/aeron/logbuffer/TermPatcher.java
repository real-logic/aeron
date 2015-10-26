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
import static uk.co.real_logic.aeron.protocol.HeaderFlyweight.HDR_TYPE_PAD;

/**
 * A term buffer patcher.
 */
public class TermPatcher
{
    public static final int NOT_PATCHED = 0;
    public static final int PATCHED = 1;
    public static final int PATCHED_TO_END = 2;

    /*
    0) Current position length is > 0, then return
    1) Current position length is < 0, just patch with positive value and return
    2) Current position length is 0, scan forward by frame alignment until, one of the following
       a) reach a non-0 length, patch up to indicated position (check original framelength for non-0)
       b) reach end of term and tail position >= end of term, patch up to end of term (check original framelength for non-0)
       c) reach tail position < end of term, do NOT patch
     */

    /**
     * Attempt to patch the current term at the current offset.
     *
     * @param termBuffer to patch
     * @param termOffset to patch at
     * @param tailOffset to patch up to
     * @return whether patching was done, not done, or patched to end of term
     */
    public static int patch(final UnsafeBuffer termBuffer, final int termOffset, final int tailOffset)
    {
        int result = NOT_PATCHED;
        int frameLength = frameLengthVolatile(termBuffer, termOffset);

        if (frameLength > 0)
        {
            result = NOT_PATCHED;
        }
        else if (frameLength < 0)
        {
            frameType(termBuffer, termOffset, HDR_TYPE_PAD);
            frameLengthOrdered(termBuffer, termOffset, -frameLength);
            result = PATCHED;
        }
        else
        {
            final int highOffset = Math.min(termBuffer.capacity(), tailOffset);
            int currentOffset = termOffset + FRAME_ALIGNMENT;

            while (currentOffset < highOffset)
            {
                frameLength = frameLengthVolatile(termBuffer, currentOffset);

                if (frameLength != 0)
                {
                    final int originalFrameLength = frameLengthVolatile(termBuffer, termOffset);

                    if (0 != originalFrameLength)
                    {
                        result = NOT_PATCHED;
                        break;
                    }

                    frameType(termBuffer, termOffset, HDR_TYPE_PAD);
                    frameLengthOrdered(termBuffer, termOffset, currentOffset - termOffset);
                    result = PATCHED;
                    break;
                }

                currentOffset += FRAME_ALIGNMENT;
            }

            if (NOT_PATCHED == result && highOffset >= termBuffer.capacity())
            {
                final int originalFrameLength = frameLengthVolatile(termBuffer, termOffset);

                if (0 != originalFrameLength)
                {
                    result = NOT_PATCHED;
                }
                else
                {
                    frameType(termBuffer, termOffset, HDR_TYPE_PAD);
                    frameLengthOrdered(termBuffer, termOffset, highOffset - termOffset);
                    result = PATCHED_TO_END;
                }
            }
        }

        return result;
    }
}
