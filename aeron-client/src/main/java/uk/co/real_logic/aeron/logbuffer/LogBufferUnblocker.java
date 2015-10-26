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
import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.indexByTerm;

/**
 * An unblocker for a logbuffer
 */
public class LogBufferUnblocker
{
    /**
     * Attempt to unblock a logbuffer at given blockedOffset
     *
     * @param logPartitions for current blockedOffset
     * @param logMetaDataBuffer for logbuffer
     * @param blockedPosition to attempt to unblock
     * @return whether unblocked or not
     */
    public static boolean unblock(
        final LogBufferPartition[] logPartitions,
        final UnsafeBuffer logMetaDataBuffer,
        final long blockedPosition)
    {
        final int initialTermId = initialTermId(logMetaDataBuffer);
        final int activeTermId = activeTermId(logMetaDataBuffer);
        final int activeIndex = indexByTerm(initialTermId, activeTermId);
        final int currentTail = logPartitions[activeIndex].tailVolatile();
        final int positionBitsToShift = Integer.numberOfTrailingZeros(logPartitions[0].termBuffer().capacity());
        final int blockedOffset = computeTermOffsetFromPosition(blockedPosition, positionBitsToShift);

        final int patchResult = TermPatcher.patch(logPartitions[activeIndex].termBuffer(), blockedOffset, currentTail);
        boolean result = false;

        if (TermPatcher.PATCHED == patchResult)
        {
            result = true;
        }
        else if (TermPatcher.PATCHED_TO_END == patchResult)
        {
            final int newTermId = activeTermId + 1;
            final int nextIndex = nextPartitionIndex(activeIndex);
            final int nextNextIndex = nextPartitionIndex(nextIndex);

            LogBufferDescriptor.defaultHeaderTermId(logMetaDataBuffer, nextIndex, newTermId);

            // Need to advance the term id in case a publication takes an interrupt
            // between reading the active term and incrementing the tail.
            // This covers the case of an interrupt taking longer than
            // the time taken to complete the current term.
            LogBufferDescriptor.defaultHeaderTermId(logMetaDataBuffer, nextNextIndex, newTermId + 1);

            logPartitions[nextNextIndex].statusOrdered(NEEDS_CLEANING);
            LogBufferDescriptor.activeTermId(logMetaDataBuffer, newTermId);

            result = true;
        }

        return result;
    }
}
