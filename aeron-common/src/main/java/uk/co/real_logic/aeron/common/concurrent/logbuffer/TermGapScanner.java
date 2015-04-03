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

import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor.*;
import static uk.co.real_logic.agrona.BitUtil.align;

/**
 * Scans for gaps in the sequence of bytes in a replicated term buffer between the completed and the
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
     * Scan for gaps from the completedOffset up to the high-water-mark. Each gap will be reported to the {@link GapHandler}.
     *
     * @param handler to be notified of gaps.
     * @return true if a gap is found otherwise false.
     */
    public static boolean scanForGap(
        final UnsafeBuffer termBuffer, final int termId, int completedOffset, final int hwmOffset, final GapHandler handler)
    {
        boolean gapFound = false;
        do
        {
            int frameLength = align(termBuffer.getInt(lengthOffset(completedOffset), LITTLE_ENDIAN), FRAME_ALIGNMENT);
            if (frameLength > 0)
            {
                completedOffset += frameLength;
            }
            else
            {
                final int limit = hwmOffset - DataHeaderFlyweight.HEADER_LENGTH;
                int gapLength = 0;
                do
                {
                    gapLength += FRAME_ALIGNMENT;
                    final int lengthOffset = lengthOffset(completedOffset + gapLength);
                    frameLength = align(termBuffer.getInt(lengthOffset, LITTLE_ENDIAN), FRAME_ALIGNMENT);

                    if (0 != frameLength)
                    {
                        gapLength -= DataHeaderFlyweight.HEADER_LENGTH;
                        break;
                    }
                }
                while ((completedOffset + gapLength) < limit);

                gapFound = true;
                handler.onGap(termId, termBuffer, completedOffset, gapLength + DataHeaderFlyweight.HEADER_LENGTH);
                break;
            }
        }
        while (completedOffset < hwmOffset);

        return gapFound;
    }
}
