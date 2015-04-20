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

import static uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor.*;
import static uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static uk.co.real_logic.aeron.common.protocol.HeaderFlyweight.FRAME_LENGTH_FIELD_OFFSET;
import static uk.co.real_logic.agrona.BitUtil.align;

/**
 * Scans for gaps in the sequence of bytes in a replicated term buffer between the completed rebuild and the
 * high-water-mark. This can be used for detecting loss and generating a NACK message to the source.
 *
 * <b>Note:</b> This class is threadsafe to be used across multiple threads.
 */
public class TermGapScanner
{
    /**
     * Handler for notifying of gaps in the log.
     */
    @FunctionalInterface
    public interface GapHandler
    {
        /**
         * Gap detected in log buffer that is being rebuilt.
         *
         * @param termId active term being scanned.
         * @param buffer containing the gap.
         * @param offset at which the gap begins.
         * @param length of the gap in bytes.
         */
        void onGap(final int termId, UnsafeBuffer buffer, int offset, int length);
    }

    /**
     * Scan for gaps from the rebuildOffset up to the high-water-mark. Each gap will be reported to the {@link GapHandler}.
     *
     * @param termBuffer    to be scanned for a gap.
     * @param termId        of the current term buffer.
     * @param rebuildOffset at which to start scanning.
     * @param hwmOffset     at which to stop scanning.
     * @param handler       to call if a gap is found.
     * @return true if a gap has been found otherwise false.
     */
    public static boolean scanForGap(
        final UnsafeBuffer termBuffer, final int termId, int rebuildOffset, final int hwmOffset, final GapHandler handler)
    {
        do
        {
            final int frameLength = frameLengthVolatile(termBuffer, rebuildOffset);
            if (0 == frameLength)
            {
                break;
            }

            rebuildOffset += align(frameLength, FRAME_ALIGNMENT);
        }
        while (rebuildOffset < hwmOffset);

        boolean gapFound = false;
        if (rebuildOffset < hwmOffset)
        {
            gapFound = true;
            final int limit = hwmOffset - HEADER_LENGTH;
            final int gapBeginOffset = rebuildOffset;
            rebuildOffset += FRAME_LENGTH_FIELD_OFFSET;

            while (rebuildOffset < limit)
            {
                rebuildOffset += FRAME_ALIGNMENT;
                final int frameLength = termBuffer.getIntVolatile(rebuildOffset);

                if (0 != frameLength)
                {
                    rebuildOffset -= HEADER_LENGTH;
                    break;
                }
            }

            final int gapLength = ((rebuildOffset - FRAME_LENGTH_FIELD_OFFSET) - gapBeginOffset) + HEADER_LENGTH;
            handler.onGap(termId, termBuffer, gapBeginOffset, gapLength);
        }

        return gapFound;
    }
}
