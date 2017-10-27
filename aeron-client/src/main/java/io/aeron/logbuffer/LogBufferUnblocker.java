/*
 * Copyright 2014-2017 Real Logic Ltd.
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
package io.aeron.logbuffer;

import org.agrona.concurrent.UnsafeBuffer;

import static io.aeron.logbuffer.LogBufferDescriptor.*;

/**
 * Provides the functionality to unblock a log at a given position.
 */
public class LogBufferUnblocker
{
    /**
     * Attempt to unblock a log buffer at given position
     *
     * @param termBuffers       for current blockedOffset
     * @param logMetaDataBuffer for log buffer
     * @param blockedPosition   to attempt to unblock
     * @return whether unblocked or not
     */
    public static boolean unblock(
        final UnsafeBuffer[] termBuffers, final UnsafeBuffer logMetaDataBuffer, final long blockedPosition)
    {
        final int termLength = termBuffers[0].capacity();
        final int positionBitsToShift = Integer.numberOfTrailingZeros(termLength);
        final int activeTermCount = activeTermCount(logMetaDataBuffer);
        final int expectedTermCount = (int)(blockedPosition >> positionBitsToShift);
        final int index = indexByTermCount(expectedTermCount);
        final long rawTail = rawTailVolatile(logMetaDataBuffer, index);
        final int termId = termId(rawTail);
        final int tailOffset = termOffset(rawTail, termLength);
        final int blockedOffset = computeTermOffsetFromPosition(blockedPosition, positionBitsToShift);

        if (activeTermCount == (expectedTermCount - 1) && blockedOffset == 0)
        {
            final int currentTermId = termId(rawTailVolatile(logMetaDataBuffer, indexByTermCount(activeTermCount)));
            return rotateLog(logMetaDataBuffer, activeTermCount, currentTermId);
        }

        switch (TermUnblocker.unblock(logMetaDataBuffer, termBuffers[index], blockedOffset, tailOffset, termId))
        {
            case UNBLOCKED_TO_END:
                rotateLog(logMetaDataBuffer, activeTermCount, termId);
                // fall through
            case UNBLOCKED:
                return true;
        }

        return false;
    }
}
