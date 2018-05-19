/*
 * Copyright 2014-2018 Real Logic Ltd.
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

import io.aeron.Aeron;
import io.aeron.logbuffer.TermGapScanner;
import org.agrona.concurrent.UnsafeBuffer;

import static io.aeron.logbuffer.TermGapScanner.scanForGap;

/**
 * Detecting and handling of gaps in a message stream.
 * <p>
 * Each detector only notifies a single run of a gap in a message stream.
 */
public class LossDetector implements TermGapScanner.GapHandler
{
    private final FeedbackDelayGenerator delayGenerator;
    private final LossHandler lossHandler;
    private final Gap scannedGap = new Gap();
    private final Gap activeGap = new Gap();

    private long deadlineNs = Aeron.NULL_VALUE;

    /**
     * Create a loss detector for a channel.
     *
     * @param delayGenerator to use for delay determination
     * @param lossHandler    to call when signalling a gap
     */
    public LossDetector(final FeedbackDelayGenerator delayGenerator, final LossHandler lossHandler)
    {
        this.delayGenerator = delayGenerator;
        this.lossHandler = lossHandler;
    }

    /**
     * Scan for gaps and handle received data.
     * <p>
     * The handler keeps track from scan to scan what is a gap and what must have been repaired.
     *
     * @param termBuffer          to scan
     * @param rebuildPosition     to start scanning from
     * @param hwmPosition         to scan up to
     * @param nowNs               time in nanoseconds
     * @param termLengthMask      used for offset calculation
     * @param positionBitsToShift used for position calculation
     * @param initialTermId       used by the scanner
     * @return packed outcome of the scan.
     */
    public long scan(
        final UnsafeBuffer termBuffer,
        final long rebuildPosition,
        final long hwmPosition,
        final long nowNs,
        final int termLengthMask,
        final int positionBitsToShift,
        final int initialTermId)
    {
        boolean lossFound = false;
        int rebuildOffset = (int)rebuildPosition & termLengthMask;

        if (rebuildPosition < hwmPosition)
        {
            final int rebuildTermCount = (int)(rebuildPosition >>> positionBitsToShift);
            final int hwmTermCount = (int)(hwmPosition >>> positionBitsToShift);

            final int rebuildTermId = initialTermId + rebuildTermCount;
            final int hwmTermOffset = (int)hwmPosition & termLengthMask;
            final int limitOffset = rebuildTermCount == hwmTermCount ? hwmTermOffset : termBuffer.capacity();

            rebuildOffset = scanForGap(termBuffer, rebuildTermId, rebuildOffset, limitOffset, this);
            if (rebuildOffset < limitOffset)
            {
                if (!scannedGap.matches(activeGap))
                {
                    activateGap(nowNs, scannedGap);
                    lossFound = true;
                }

                checkTimerExpiry(nowNs);
            }
        }

        return pack(rebuildOffset, lossFound);
    }

    public void onGap(final int termId, final int offset, final int length)
    {
        scannedGap.set(termId, offset, length);
    }

    /**
     * Pack the values for workCount and rebuildOffset into a long for returning on the stack.
     *
     * @param rebuildOffset value to be packed.
     * @param lossFound     value to be packed.
     * @return a long with rebuildOffset and lossFound packed into it.
     */
    public static long pack(final int rebuildOffset, final boolean lossFound)
    {
        return ((long)rebuildOffset << 32) | (lossFound ? 1 : 0);
    }

    /**
     * Has loss been found in the scan?
     *
     * @param scanOutcome into which the fragments read value has been packed.
     * @return if loss has been found or not.
     */
    public static boolean lossFound(final long scanOutcome)
    {
        return ((int)scanOutcome) != 0;
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

    private void activateGap(final long nowNs, final Gap gap)
    {
        activeGap.set(gap.termId, gap.termOffset, gap.length);

        if (delayGenerator.shouldFeedbackImmediately())
        {
            deadlineNs = nowNs;
        }
        else
        {
            deadlineNs = nowNs + delayGenerator.generateDelay();
        }
    }

    private void checkTimerExpiry(final long nowNs)
    {
        if (nowNs >= deadlineNs)
        {
            lossHandler.onGapDetected(activeGap.termId, activeGap.termOffset, activeGap.length);
            deadlineNs = nowNs + delayGenerator.generateDelay();
        }
    }

    static final class Gap
    {
        int termId;
        int termOffset = -1;
        int length;

        public void set(final int termId, final int termOffset, final int length)
        {
            this.termId = termId;
            this.termOffset = termOffset;
            this.length = length;
        }

        public boolean matches(final Gap other)
        {
            return other.termId == this.termId && other.termOffset == this.termOffset;
        }
    }
}
