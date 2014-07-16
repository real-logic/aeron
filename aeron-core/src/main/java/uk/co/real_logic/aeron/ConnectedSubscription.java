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
package uk.co.real_logic.aeron;

import uk.co.real_logic.aeron.util.BitUtil;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogReader;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;

import java.util.concurrent.atomic.AtomicLong;

import static uk.co.real_logic.aeron.util.TermHelper.rotateNext;
import static uk.co.real_logic.aeron.util.TermHelper.termIdToBufferIndex;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.WORD_ALIGNMENT;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.flagsOffset;

/**
 * A subscription that has been connected to from a publisher session.
 */
public class ConnectedSubscription
{
    private static final int HEADER_LENGTH = BitUtil.align(DataHeaderFlyweight.HEADER_LENGTH, WORD_ALIGNMENT);

    private final LogReader[] logReaders;
    private final long sessionId;
    private final DataHandler dataHandler;
    private final AtomicLong activeTermId;

    private int activeIndex;

    public ConnectedSubscription(final LogReader[] readers,
                                 final long sessionId,
                                 final long initialTermId,
                                 final DataHandler dataHandler)
    {
        this.logReaders = readers;
        this.sessionId = sessionId;
        this.dataHandler = dataHandler;
        this.activeTermId = new AtomicLong(initialTermId);
        this.activeIndex = termIdToBufferIndex(initialTermId);
    }

    public long sessionId()
    {
        return sessionId;
    }

    public int poll(final int frameCountLimit)
    {
        final int activeIndex = this.activeIndex;
        LogReader logReader = logReaders[activeIndex];

        if (logReader.isComplete())
        {
            final int nextIndex = rotateNext(activeIndex);
            logReader = logReaders[nextIndex];
            if (logReader.status() != LogBufferDescriptor.CLEAN)
            {
                return 0;
            }

            activeTermId.lazySet(activeTermId.get() + 1);
            this.activeIndex = nextIndex;
            logReader.seek(0);
        }

        return logReader.read(this::onFrame, frameCountLimit);
    }

    private void onFrame(final AtomicBuffer buffer, final int offset, final int length)
    {
        final byte flags = buffer.getByte(flagsOffset(offset));

        dataHandler.onData(buffer, offset + HEADER_LENGTH, length - HEADER_LENGTH, sessionId, flags);
    }
}
