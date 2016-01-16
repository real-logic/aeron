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

import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.*;

/**
 * Provides the functionality to unblock a log at a given position.
 */
public class LogBufferUnblocker
{
    /**
     * Attempt to unblock a log buffer at given position
     *
     * @param logPartitions     for current blockedOffset
     * @param logMetaDataBuffer for log buffer
     * @param blockedPosition   to attempt to unblock
     * @return whether unblocked or not
     */
    public static boolean unblock(
        final LogBufferPartition[] logPartitions, final UnsafeBuffer logMetaDataBuffer, final long blockedPosition)
    {
        final int termLength = logPartitions[0].termBuffer().capacity();
        final int positionBitsToShift = Integer.numberOfTrailingZeros(termLength);
        final int activeIndex = indexByPosition(blockedPosition, positionBitsToShift);
        final LogBufferPartition activePartition = logPartitions[activeIndex];
        final UnsafeBuffer termBuffer = activePartition.termBuffer();
        final long rawTail = activePartition.rawTailVolatile();
        final int termId = termId(rawTail);
        final int tailOffset = termOffset(rawTail, termLength);
        final int blockedOffset = computeTermOffsetFromPosition(blockedPosition, positionBitsToShift);

        boolean result = false;

        switch (TermUnblocker.unblock(logMetaDataBuffer, termBuffer, blockedOffset, tailOffset, termId))
        {
            case UNBLOCKED_TO_END:
                rotateLog(logPartitions, logMetaDataBuffer, activeIndex, termId + 1);
                // fall through
            case UNBLOCKED:
                result = true;
        }

        return result;
    }
}
