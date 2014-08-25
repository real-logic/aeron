/*
 * Copyright 2014 Real Logic Ltd.
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
package uk.co.real_logic.aeron.common;

import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBuffer;

import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.*;

/**
 * Common helper for dealing with term buffers.
 */
public class TermHelper
{
    public static final int BUFFER_COUNT = 3;

    public static int rotateNext(final int current)
    {
        return BitUtil.next(current, BUFFER_COUNT);
    }

    public static int rotatePrevious(final int current)
    {
        return BitUtil.previous(current, BUFFER_COUNT);
    }

    public static int termIdToBufferIndex(final int termId)
    {
        return Math.abs(termId % BUFFER_COUNT);
    }

    /**
     * Calculate the current position in absolute number of bytes.
     *
     * @param activeTermId active term id.
     * @param currentTail in the term.
     * @param positionBitsToShift number of times to left shift the term count
     * @param initialTermId the initial term id that this stream started on
     * @return the absolute position in bytes
     */
    public static long calculatePosition(
        final int activeTermId, final int currentTail, final int positionBitsToShift, final int initialTermId)
    {
        final long termCount = activeTermId - initialTermId; // copes with negative activeTermId on rollover

        return (termCount << positionBitsToShift) + currentTail;
    }

    /**
     * Calculate the term id from a position.
     *
     * @param position             to calculate from
     * @param positionBitsToShift  number of times to right shift the position
     * @param initialTermId        the initial term id that this stream started on
     * @return the term id according to the position
     */
    public static int calculateTermIdFromPosition(final long position, final int positionBitsToShift, final int initialTermId)
    {
        return ((int)(position >>> positionBitsToShift) + initialTermId);
    }

    /**
     * Calculate the term offset from a given position.
     *
     * @param position            to calculate from
     * @param positionBitsToShift number of times to right shift the position
     * @return the offset within the term that represents the position
     */
    public static int calculateTermOffsetFromPosition(final long position, final int positionBitsToShift)
    {
        final int mask = (1 << positionBitsToShift) - 1;

        return (int)(position & mask);
    }

    /**
     * Check that has been cleaned and is ready for use. If it is not clean it will be cleaned on this thread
     * or this thread will wait for the conductor to complete the cleaning.
     *
     * @param logBuffer to be checked.
     * @return true if the buffer was clean otherwise false if you needed cleaning.
     */
    public static boolean ensureClean(final LogBuffer logBuffer)
    {
        if (CLEAN != logBuffer.status())
        {
            if (logBuffer.compareAndSetStatus(NEEDS_CLEANING, IN_CLEANING))
            {
                logBuffer.clean(); // Conductor is not keeping up so do it yourself!!!
            }
            else
            {
                while (CLEAN != logBuffer.status())
                {
                    Thread.yield();
                }
            }

            return false;
        }

        return true;
    }
}
