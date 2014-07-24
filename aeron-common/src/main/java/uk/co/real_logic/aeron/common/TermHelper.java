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

import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.CLEAN;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.IN_CLEANING;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.NEEDS_CLEANING;

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

    public static int termIdToBufferIndex(final long termId)
    {
        return (int)(termId % BUFFER_COUNT);
    }

    /**
     * Calculate the current position in absolute number of bytes.
     *
     * @param activeTermId active term id.
     * @param currentTail in the term.
     * @param positionBitsToShift number of times to left shift the activeTermId
     * @param initialPosition that first activeTermId started at
     * @return the absolute position in bytes
     */
    public static long calculatePosition(final long activeTermId,
                                         final int currentTail,
                                         final int positionBitsToShift,
                                         final long initialPosition)
    {
        // TODO: we need to deal with termId wrapping and going negative.
        return ((activeTermId << positionBitsToShift) - initialPosition) + currentTail;
    }

    /**
     * Check that has been cleaned and is ready for use. If it is not clean it will be cleaned on this thread
     * or this thread will wait for the conductor to complete the cleaning.
     *
     * @param logBuffer to be checked.
     * @param destination used from the client to notify of unclean buffer.
     * @param channelId used from the client to notify of unclean buffer.
     * @param termId used from the client to notify of unclean buffer.
     */
    public static void checkForCleanTerm(final LogBuffer logBuffer,
                                         final String destination,
                                         final long channelId,
                                         final long termId)
    {
        if (CLEAN != logBuffer.status())
        {
            System.err.println(String.format("Term not clean: destination=%s channelId=%d, required termId=%d",
                                             destination, channelId, termId));

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
        }
    }
}
