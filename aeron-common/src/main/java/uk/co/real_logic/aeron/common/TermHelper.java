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

import uk.co.real_logic.agrona.BitUtil;

/**
 * Common helper for dealing with term buffers.
 */
public class TermHelper
{
    public static final int BUFFER_COUNT = 3;

    /**
     * Rotate to the next buffer in sequence for the term id.
     *
     * @param current buffer index
     * @return the next buffer index
     */
    public static int rotateNext(final int current)
    {
        return BitUtil.next(current, BUFFER_COUNT);
    }

    /**
     * Rotate to the previous buffer in sequence for the term id.
     *
     * @param current buffer index
     * @return the previous buffer index
     */
    public static int rotatePrevious(final int current)
    {
        return BitUtil.previous(current, BUFFER_COUNT);
    }

    /**
     * Determine the buffer index to be used given the initial term and active term ids.
     *
     * @param initialTermId at which the log buffer usage began
     * @param activeTermId  that is in current usage
     * @return the index of which buffer should be used
     */
    public static int bufferIndex(final int initialTermId, final int activeTermId)
    {
        return (activeTermId - initialTermId) % BUFFER_COUNT;
    }

    /**
     * Calculate the current position in absolute number of bytes.
     *
     * @param activeTermId        active term id.
     * @param currentTail         in the term.
     * @param positionBitsToShift number of times to left shift the term count
     * @param initialTermId       the initial term id that this stream started on
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
     * @param position            to calculate from
     * @param positionBitsToShift number of times to right shift the position
     * @param initialTermId       the initial term id that this stream started on
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
}
