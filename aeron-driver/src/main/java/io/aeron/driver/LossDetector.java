/*
 * Copyright 2014 - 2016 Real Logic Ltd.
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
package io.aeron.driver;

import io.aeron.logbuffer.TermGapScanner;
import org.agrona.concurrent.UnsafeBuffer;

import static io.aeron.logbuffer.TermGapScanner.scanForGap;

/**
 * Detecting and handling of gaps in a stream
 *
 * This detector only notifies a single run of gap in message stream
 */
public class LossDetector implements TermGapScanner.GapHandler
{
    private static final long TIMER_INACTIVE = -1;

    private final FeedbackDelayGenerator delayGenerator;
    private final GapHandler gapHandler;
    private final Gap scannedGap = new Gap();
    private final Gap activeGap = new Gap();

    private long expire = TIMER_INACTIVE;

    /**
     * Create a loss detector for a channel.
     *
     * @param delayGenerator to use for delay determination
     * @param gapHandler     to call when signalling a gap
     */
    public LossDetector(final FeedbackDelayGenerator delayGenerator, final GapHandler gapHandler)
    {
        this.delayGenerator = delayGenerator;
        this.gapHandler = gapHandler;
    }

    /**
     * Scan for gaps and handle received data.
     *
     * The handler keeps track from scan to scan what is a gap and what must have been repaired.
     *
     * @param termBuffer          to scan
     * @param rebuildPosition     to start scanning from
     * @param hwmPosition         to scan up to
     * @param now                 time in nanoseconds
     * @param termLengthMask      used for offset calculation
     * @param positionBitsToShift used for position calculation
     * @param initialTermId       used by the scanner
     * @return packed outcome of the scan.
     */
    public long scan(
        final UnsafeBuffer termBuffer,
        final long rebuildPosition,
        final long hwmPosition,
        final long now,
        final int termLengthMask,
        final int positionBitsToShift,
        final int initialTermId)
    {
        int workCount = 0;
        int rebuildOffset = (int)rebuildPosition & termLengthMask;

        if (rebuildPosition < hwmPosition)
        {
            final int rebuildTermCount = (int)(rebuildPosition >>> positionBitsToShift);
            final int hwmTermCount = (int)(hwmPosition >>> positionBitsToShift);

            final int activeTermId = initialTermId + rebuildTermCount;
            final int hwmTermOffset = (int)hwmPosition & termLengthMask;
            final int activeTermLimit = rebuildTermCount == hwmTermCount ? hwmTermOffset : termBuffer.capacity();

            rebuildOffset = scanForGap(termBuffer, activeTermId, rebuildOffset, activeTermLimit, this);
            if (rebuildOffset < activeTermLimit)
            {
                final Gap gap = scannedGap;
                if (TIMER_INACTIVE == expire || !gap.matches(activeGap.termId, activeGap.termOffset))
                {
                    activateGap(now, gap.termId, gap.termOffset, gap.length);
                    workCount = 1;
                }

                rebuildOffset = gap.termOffset;
            }
        }
        else if (expire != TIMER_INACTIVE)
        {
            expire = TIMER_INACTIVE;
        }

        workCount += checkTimerExpire(now);

        return pack(rebuildOffset, workCount);
    }

    public void onGap(final int termId, final int offset, final int length)
    {
        scannedGap.reset(termId, offset, length);
    }

    /**
     * Pack the values for workCount and rebuildOffset into a long for returning on the stack.
     *
     * @param rebuildOffset value to be packed.
     * @param workCount     value to be packed.
     * @return a long with both ints packed into it.
     */
    public static long pack(final int rebuildOffset, final int workCount)
    {
        return ((long)rebuildOffset << 32) | workCount;
    }

    /**
     * The work count for the scan.
     *
     * @param scanOutcome into which the fragments read value has been packed.
     * @return the number of fragments that have been read.
     */
    public static int workCount(final long scanOutcome)
    {
        return (int)scanOutcome;
    }

    /**
     * The offset up to which the log has been rebuilt.
     *
     * @param scanOutcome into which the offset value has been packed.
     * @return the offset up to which the log has been rebuilt.
     */
    public static int rebuildOffset(final long scanOutcome)
    {
        return (int)(scanOutcome >>> 32);
    }

    private void activateGap(final long now, final int termId, final int termOffset, final int length)
    {
        activeGap.reset(termId, termOffset, length);
        if (delayGenerator.generateDelay() == -1)
        {
            return;
        }

        expire = now + delayGenerator.generateDelay();

        if (delayGenerator.shouldFeedbackImmediately())
        {
            gapHandler.onLossDetected(activeGap.termId, activeGap.termOffset, activeGap.length);
        }
    }

    private int checkTimerExpire(final long now)
    {
        int result = 0;

        if (TIMER_INACTIVE != expire && now > expire)
        {
            gapHandler.onLossDetected(activeGap.termId, activeGap.termOffset, activeGap.length);
            expire = now + delayGenerator.generateDelay();
            result = 1;
        }

        return result;
    }

    static final class Gap
    {
        int termId;
        int termOffset;
        int length;

        public void reset(final int termId, final int termOffset, final int length)
        {
            this.termId = termId;
            this.termOffset = termOffset;
            this.length = length;
        }

        public boolean matches(final int termId, final int termOffset)
        {
            return termId == this.termId && termOffset == this.termOffset;
        }
    }
}
