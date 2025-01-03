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

import org.agrona.collections.BiInt2ObjectMap;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;

import java.net.InetSocketAddress;

/**
 * A loss generator implementation that will lose a fixed block of data from a specific term-id/term-offset pair. It
 * will only lose the selected block of data once for a given stream-id/session-id pair.
 */
public class FixedLossGenerator implements LossGenerator
{
    private final int termId;
    private final int termOffset;
    private final int length;
    private final BiInt2ObjectMap<MutableInteger> streamAndSessionIdToOffsetMap = new BiInt2ObjectMap<>();

    /**
     * Set the range of messages to be dropped.
     *
     * @param termId        to be dropped
     * @param termOffset    to be dropped
     * @param length        to be dropped
     */
    public FixedLossGenerator(final int termId, final int termOffset, final int length)
    {
        this.termId = termId;
        this.termOffset = termOffset;
        this.length = length;
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
        if (this.termId == termId)
        {
            MutableInteger maximumDroppedOffset = streamAndSessionIdToOffsetMap.get(streamId, sessionId);
            if (null == maximumDroppedOffset)
            {
                maximumDroppedOffset = new MutableInteger(termOffset);
                streamAndSessionIdToOffsetMap.put(streamId, sessionId, maximumDroppedOffset);
            }

            final boolean isRetransmission = maximumDroppedOffset.get() > termOffset;

            if (!isRetransmission)
            {
                final int dropRegionOffset = this.termOffset;
                final int dropRegionLimit = this.termOffset + this.length;
                final int frameLimit = termOffset + length;
                final boolean shouldDrop = frameLimit > dropRegionOffset && termOffset < dropRegionLimit;
                if (shouldDrop)
                {
                    maximumDroppedOffset.set(termOffset + length);
                    return true;
                }
            }
        }

        return false;
    }
}
