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

import uk.co.real_logic.aeron.common.BitUtil;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogReader;
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.common.status.PositionReporter;
import uk.co.real_logic.aeron.conductor.ManagedBuffer;

import java.util.concurrent.atomic.AtomicInteger;

import static uk.co.real_logic.aeron.common.TermHelper.*;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor.WORD_ALIGNMENT;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor.flagsOffset;

/**
 * A subscription that has been connected to from a publisher session.
 */
public class ConnectedSubscription
{
    private static final int HEADER_LENGTH = BitUtil.align(DataHeaderFlyweight.HEADER_LENGTH, WORD_ALIGNMENT);

    private final LogReader[] logReaders;
    private final int sessionId;
    private final DataHandler dataHandler;
    private final PositionReporter positionReporter;
    private final ManagedBuffer[] managedBuffers;
    private final AtomicInteger activeTermId;
    private final int positionBitsToShift;
    private final int initialTermId;

    private int activeIndex;

    public ConnectedSubscription(final LogReader[] readers,
                                 final int sessionId,
                                 final int initialTermId,
                                 final DataHandler dataHandler,
                                 final PositionReporter positionReporter,
                                 final ManagedBuffer[] managedBuffers)
    {
        this.logReaders = readers;
        this.sessionId = sessionId;
        this.dataHandler = dataHandler;
        this.positionReporter = positionReporter;
        this.managedBuffers = managedBuffers;
        this.activeTermId = new AtomicInteger(initialTermId);
        this.activeIndex = termIdToBufferIndex(initialTermId);

        this.positionBitsToShift = Integer.numberOfTrailingZeros(logReaders[0].capacity());
        this.initialTermId = initialTermId;
    }

    public int sessionId()
    {
        return sessionId;
    }

    public int poll(final int frameCountLimit)
    {
        final int activeIndex = this.activeIndex;
        LogReader logReader = logReaders[activeIndex];
        final int activeTermId = this.activeTermId.get();

        if (logReader.isComplete())
        {
            final int nextIndex = rotateNext(activeIndex);
            logReader = logReaders[nextIndex];
            if (logReader.status() != LogBufferDescriptor.CLEAN)
            {
                return 0;
            }

            this.activeTermId.lazySet(activeTermId + 1);
            this.activeIndex = nextIndex;
            logReader.seek(0);
        }

        final int messagesRead = logReader.read(this::onFrame, frameCountLimit);
        if (messagesRead > 0)
        {
            positionReporter.position(calculatePosition(activeTermId, logReader.tail(), positionBitsToShift, initialTermId));
        }

        return messagesRead;
    }

    private void onFrame(final AtomicBuffer buffer, final int offset, final int length)
    {
        final byte flags = buffer.getByte(flagsOffset(offset));

        dataHandler.onData(buffer, offset + HEADER_LENGTH, length - HEADER_LENGTH, sessionId, flags);
    }

    public void close()
    {
        for (final ManagedBuffer managedBuffer : managedBuffers)
        {
            try
            {
                managedBuffer.close();
            }
            catch (final Exception ex)
            {
                throw new IllegalStateException(ex);
            }
        }
    }
}
