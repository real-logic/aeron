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
package uk.co.real_logic.aeron.mediadriver;

import uk.co.real_logic.aeron.mediadriver.buffer.BufferRotator;
import uk.co.real_logic.aeron.mediadriver.buffer.LogBuffers;
import uk.co.real_logic.aeron.util.BufferRotationDescriptor;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogRebuilder;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.StateViewer;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicLong;

import static uk.co.real_logic.aeron.util.BufferRotationDescriptor.UNKNOWN_TERM_ID;

/**
 * State maintained for active sessionIds within a channel for receiver processing
 */
public class RcvSessionState
{
    public static final int CLEAN_WINDOW = 2;
    private final InetSocketAddress srcAddr;
    private final long sessionId;

    private final AtomicLong cleanedTermId = new AtomicLong(UNKNOWN_TERM_ID);
    private final AtomicLong currentTermId = new AtomicLong(UNKNOWN_TERM_ID);
    private int currentBufferId = 0;

    private BufferRotator rotator;
    private TermRebuilder[] rebuilders;

    public RcvSessionState(final long sessionId, final InetSocketAddress srcAddr)
    {
        this.srcAddr = srcAddr;
        this.sessionId = sessionId;
    }

    public void termBuffer(final long initialTermId, final BufferRotator rotator)
    {
        currentTermId.lazySet(initialTermId);
        this.rotator = rotator;
        rebuilders = rotator.buffers()
                            .map(TermRebuilder::new)
                            .toArray(TermRebuilder[]::new);
        cleanedTermId.lazySet(initialTermId + 2);
    }

    public InetSocketAddress sourceAddress()
    {
        return srcAddr;
    }

    public long sessionId()
    {
        return sessionId;
    }

    public void rebuildBuffer(final long termId, final DataHeaderFlyweight header)
    {
        final long currentTermId = this.currentTermId.get();
        if (termId == currentTermId)
        {
            final TermRebuilder rebuilder = rebuilders[currentBufferId];
            rebuilder.insert(header);
        }
        else if (termId == (currentTermId + 1))
        {
            this.currentTermId.incrementAndGet(); // TODO: should this be an atomic increment?
            currentBufferId = BufferRotationDescriptor.rotateId(currentBufferId);
            TermRebuilder rebuilder = rebuilders[currentBufferId];
            while (rebuilder.tailVolatile() != 0)
            {
                // TODO:
                Thread.yield();
            }
            rebuilder.insert(header);
        }
        else
        {
            // TODO: log or monitor this case
            System.out.println("Unexpected Term Id " + currentTermId + ":" + termId);
        }
    }

    private class TermRebuilder
    {
        private final LogRebuilder logRebuilder;
        private final StateViewer stateViewer;

        public TermRebuilder(final LogBuffers buffer)
        {
            final AtomicBuffer stateBuffer = buffer.stateBuffer();
            stateViewer = new StateViewer(stateBuffer);
            logRebuilder = new LogRebuilder(buffer.logBuffer(), stateBuffer);
        }

        public int tailVolatile()
        {
            return stateViewer.tailVolatile();
        }

        public void insert(final DataHeaderFlyweight header)
        {
            logRebuilder.insert(header.atomicBuffer(), header.offset(), header.frameLength());
        }
    }

    public void processBufferRotation()
    {
        final long currentTermId = this.currentTermId.get();
        final long expectedTermId = currentTermId + CLEAN_WINDOW;
        if (currentTermId != UNKNOWN_TERM_ID && expectedTermId > cleanedTermId.get())
        {
            try
            {
                rotator.rotate();
                cleanedTermId.incrementAndGet(); // TODO: should this be an atomic increment?
            }
            catch (final IOException ex)
            {
                // TODO; log
                ex.printStackTrace();
            }
        }
    }
}
