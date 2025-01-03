/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.driver.ext;

import org.agrona.BitUtil;
import org.agrona.collections.BiInt2ObjectMap;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;

import java.net.InetSocketAddress;

/**
 * A loss generator implementation that will introduce multiple data gaps from a specific term-id/term-offset pair. It
 * will only gaps once for a given stream-id/session-id pair.
 */
public class MultiGapLossGenerator implements LossGenerator
{
    private final int termId;
    private final int gapRadixBits;
    private final int gapRadixMask;
    private final int gapLength;
    private final int lastGapLimit;
    private final BiInt2ObjectMap<MutableInteger> streamAndSessionIdToOffsetMap = new BiInt2ObjectMap<>();

    /**
     * Set the range of messages to be dropped.
     *
     * @param termId     to be dropped
     * @param gapRadix   the initial offset and subsequent interval between gaps - must be a power of 2
     * @param gapLength  length of each gap
     * @param totalGaps  the total number of gaps
     */
    public MultiGapLossGenerator(
        final int termId,
        final int gapRadix,
        final int gapLength,
        final int totalGaps)
    {
        final int actualGapRadix = BitUtil.findNextPositivePowerOfTwo(gapRadix);

        if (gapLength >= actualGapRadix)
        {
            throw new IllegalArgumentException("gapLength must be smaller than gapRadix");
        }

        this.termId = termId;
        this.gapRadixBits = Integer.numberOfTrailingZeros(actualGapRadix);
        this.gapRadixMask = -actualGapRadix;
        this.gapLength = gapLength;
        this.lastGapLimit = (totalGaps * actualGapRadix) + gapLength;
    }

    /**
     * {@inheritDoc}
     */
    public boolean shouldDropFrame(final InetSocketAddress address, final UnsafeBuffer buffer, final int length)
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    public boolean shouldDropFrame(
        final InetSocketAddress address,
        final UnsafeBuffer buffer,
        final int streamId,
        final int sessionId,
        final int termId,
        final int termOffset,
        final int length)
    {
        if (this.termId != termId) // wrong term
        {
            return false;
        }

        if (termOffset > this.lastGapLimit) // this packet is past the last offset we'll drop
        {
            return false;
        }

        MutableInteger maximumDroppedOffset = streamAndSessionIdToOffsetMap.get(streamId, sessionId);
        if (null == maximumDroppedOffset) // first time we've seen this term/stream/session
        {
            maximumDroppedOffset = new MutableInteger(termOffset);
            streamAndSessionIdToOffsetMap.put(streamId, sessionId, maximumDroppedOffset);
        }

        if (maximumDroppedOffset.get() > termOffset) // this is an rx (offset is lower than max dropped)
        {
            return false;
        }

        final int frameLimit = termOffset + length;

        if (termOffset != 0 && Integer.numberOfTrailingZeros(termOffset) >= this.gapRadixBits)
        {
            maximumDroppedOffset.set(frameLimit);
            return true;
        }

        final int previousGapOffset = termOffset & this.gapRadixMask;
        final int previousGapLimit = previousGapOffset + this.gapLength;

        if (previousGapOffset > 0 && termOffset < previousGapLimit)
        {
            maximumDroppedOffset.set(frameLimit);
            return true;
        }

        final int nextGapOffset = ((termOffset >> this.gapRadixBits) + 1) << this.gapRadixBits;
        final int nextGapLimit = nextGapOffset + this.gapLength;

        if (frameLimit > nextGapOffset && termOffset < nextGapLimit)
        {
            maximumDroppedOffset.set(frameLimit);
            return true;
        }

        return false;
    }
}
