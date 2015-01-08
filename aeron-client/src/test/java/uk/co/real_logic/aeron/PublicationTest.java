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

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.verification.VerificationMode;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.BufferClaim;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogAppender;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor;
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;
import uk.co.real_logic.agrona.status.PositionIndicator;

import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.common.TermHelper.BUFFER_COUNT;
import static uk.co.real_logic.aeron.common.TermHelper.bufferIndex;
import static uk.co.real_logic.agrona.concurrent.broadcast.RecordDescriptor.RECORD_ALIGNMENT;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogAppender.ActionStatus.*;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.TERM_MIN_LENGTH;

public class PublicationTest
{
    private static final String CHANNEL = "udp://localhost:40124";
    private static final int STREAM_ID_1 = 2;
    private static final int SESSION_ID_1 = 13;
    private static final int TERM_ID_1 = 1;
    private static final int CORRELATION_ID = 2000;
    private static final int SEND_BUFFER_CAPACITY = 1024;

    private final ByteBuffer sendBuffer = ByteBuffer.allocate(SEND_BUFFER_CAPACITY);
    private final UnsafeBuffer atomicSendBuffer = new UnsafeBuffer(sendBuffer);
    private final DataHeaderFlyweight dataHeaderFlyweight = new DataHeaderFlyweight();

    private Publication publication;
    private PositionIndicator limit;
    private LogAppender[] appenders;
    private MutableDirectBuffer[] headers;
    private ManagedBuffer[] managedBuffers;

    @Before
    public void setUp()
    {
        final ClientConductor conductor = mock(ClientConductor.class);
        limit = mock(PositionIndicator.class);
        when(limit.position()).thenReturn(2L * SEND_BUFFER_CAPACITY);

        appenders = new LogAppender[BUFFER_COUNT];
        headers = new MutableDirectBuffer[BUFFER_COUNT];
        for (int i = 0; i < BUFFER_COUNT; i++)
        {
            appenders[i] = mock(LogAppender.class);
            final MutableDirectBuffer header = DataHeaderFlyweight.createDefaultHeader(0, 0, 0);
            headers[i] = header;
            when(appenders[i].append(any(), anyInt(), anyInt())).thenReturn(SUCCESS);
            when(appenders[i].claim(anyInt(), any())).thenReturn(SUCCESS);
            when(appenders[i].defaultHeader()).thenReturn(header);
            when(appenders[i].capacity()).thenReturn(TERM_MIN_LENGTH);
        }

        managedBuffers = new ManagedBuffer[BUFFER_COUNT * 2];
        for (int i = 0; i < BUFFER_COUNT * 2; i++)
        {
            managedBuffers[i] = mock(ManagedBuffer.class);
        }

        publication = new Publication(
            conductor, CHANNEL, STREAM_ID_1, SESSION_ID_1, TERM_ID_1, appenders, limit, managedBuffers, CORRELATION_ID);
    }

    @Test
    public void shouldOfferAMessageUponConstruction()
    {
        assertTrue(publication.offer(atomicSendBuffer));
    }

    @Test
    public void shouldFailToOfferAMessageWhenLimited()
    {
        when(limit.position()).thenReturn(0L);
        assertFalse(publication.offer(atomicSendBuffer));
    }

    @Test
    public void shouldFailToOfferWhenAppendFails()
    {
        when(appenders[bufferIndex(TERM_ID_1, TERM_ID_1)].append(any(), anyInt(), anyInt())).thenReturn(FAILURE);
        assertFalse(publication.offer(atomicSendBuffer));
    }

    @Test
    public void shouldRotateWhenAppendTrips()
    {
        when(appenders[bufferIndex(TERM_ID_1, TERM_ID_1)].append(any(), anyInt(), anyInt())).thenReturn(TRIPPED);
        when(appenders[bufferIndex(TERM_ID_1, TERM_ID_1)].tailVolatile()).thenReturn(TERM_MIN_LENGTH - RECORD_ALIGNMENT);
        when(limit.position()).thenReturn(Long.MAX_VALUE);

        assertFalse(publication.offer(atomicSendBuffer));
        assertTrue(publication.offer(atomicSendBuffer));

        final InOrder inOrder = inOrder(appenders[0], appenders[1], appenders[2]);
        inOrder.verify(appenders[bufferIndex(TERM_ID_1, TERM_ID_1 + 2)]).statusOrdered(LogBufferDescriptor.NEEDS_CLEANING);

        // written data to the next record
        inOrder.verify(appenders[bufferIndex(TERM_ID_1, TERM_ID_1 + 1)])
               .append(atomicSendBuffer, 0, atomicSendBuffer.capacity());

        // updated the term id in the header
        dataHeaderFlyweight.wrap(headers[bufferIndex(TERM_ID_1, TERM_ID_1 + 1)]);
        assertThat(dataHeaderFlyweight.termId(), is(TERM_ID_1 + 1));
    }

    @Test
    public void shouldRotateWhenClaimTrips()
    {
        when(appenders[bufferIndex(TERM_ID_1, TERM_ID_1)].claim(anyInt(), any())).thenReturn(TRIPPED);
        when(appenders[bufferIndex(TERM_ID_1, TERM_ID_1)].tailVolatile()).thenReturn(TERM_MIN_LENGTH - RECORD_ALIGNMENT);
        when(limit.position()).thenReturn(Long.MAX_VALUE);

        final BufferClaim bufferClaim = new BufferClaim();
        assertFalse(publication.tryClaim(SEND_BUFFER_CAPACITY, bufferClaim));
        assertTrue(publication.tryClaim(SEND_BUFFER_CAPACITY, bufferClaim));

        final InOrder inOrder = inOrder(appenders[0], appenders[1], appenders[2]);
        inOrder.verify(appenders[bufferIndex(TERM_ID_1, TERM_ID_1 + 2)]).statusOrdered(LogBufferDescriptor.NEEDS_CLEANING);

        // written data to the next record
        inOrder.verify(appenders[bufferIndex(TERM_ID_1, TERM_ID_1 + 1)])
               .claim(SEND_BUFFER_CAPACITY, bufferClaim);

        // updated the term id in the header
        dataHeaderFlyweight.wrap(headers[bufferIndex(TERM_ID_1, TERM_ID_1 + 1)]);
        assertThat(dataHeaderFlyweight.termId(), is(TERM_ID_1 + 1));
    }

    @Test
    public void shouldUnmapBuffersWhenReleased() throws Exception
    {
        publication.close();
        verifyBuffersUnmapped(times(1));
    }

    @Test
    public void shouldNotUnmapBuffersBeforeLastRelease() throws Exception
    {
        publication.incRef();
        publication.close();
        verifyBuffersUnmapped(never());
    }

    @Test
    public void shouldUnmapBuffersWithMultipleReferences() throws Exception
    {
        publication.incRef();
        publication.close();

        publication.close();
        verifyBuffersUnmapped(times(1));
    }

    private void verifyBuffersUnmapped(final VerificationMode times) throws Exception
    {
        for (final ManagedBuffer buffer : managedBuffers)
        {
            verify(buffer, times).close();
        }
    }
}
