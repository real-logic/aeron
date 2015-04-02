/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
import uk.co.real_logic.aeron.common.concurrent.logbuffer.*;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;
import uk.co.real_logic.agrona.status.PositionIndicator;

import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.*;
import static uk.co.real_logic.agrona.concurrent.broadcast.RecordDescriptor.RECORD_ALIGNMENT;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogAppender.ActionStatus.*;

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

    private final UnsafeBuffer logMetaDataBuffer = spy(new UnsafeBuffer(new byte[LOG_META_DATA_LENGTH]));

    private Publication publication;
    private PositionIndicator limit;
    private LogAppender[] appenders;
    private MutableDirectBuffer[] headers;
    private LogBuffers logBuffers = mock(LogBuffers.class);
    private UnsafeBuffer termBuffer = mock(UnsafeBuffer.class);

    @Before
    public void setUp()
    {
        final ClientConductor conductor = mock(ClientConductor.class);
        limit = mock(PositionIndicator.class);
        when(limit.position()).thenReturn(2L * SEND_BUFFER_CAPACITY);
        when(termBuffer.capacity()).thenReturn(TERM_MIN_LENGTH);

        appenders = new LogAppender[PARTITION_COUNT];
        headers = new MutableDirectBuffer[PARTITION_COUNT];
        for (int i = 0; i < PARTITION_COUNT; i++)
        {
            appenders[i] = mock(LogAppender.class);
            final MutableDirectBuffer header = DataHeaderFlyweight.createDefaultHeader(0, 0, 0);
            headers[i] = header;
            when(appenders[i].append(any(), anyInt(), anyInt())).thenReturn(SUCCEEDED);
            when(appenders[i].claim(anyInt(), any())).thenReturn(SUCCEEDED);
            when(appenders[i].defaultHeader()).thenReturn(header);
            when(appenders[i].termBuffer()).thenReturn(termBuffer);
            when(appenders[i].maxMessageLength()).thenReturn(FrameDescriptor.computeMaxMessageLength(TERM_MIN_LENGTH));
        }

        initialTermId(logMetaDataBuffer, TERM_ID_1);

        publication = new Publication(
            conductor,
            CHANNEL,
            STREAM_ID_1,
            SESSION_ID_1,
            appenders,
            limit,
            logBuffers,
            logMetaDataBuffer,
            CORRELATION_ID);
    }

    @Test
    public void shouldReportMaxMessageLength()
    {
        assertThat(publication.maxMessageLength(), is(FrameDescriptor.computeMaxMessageLength(TERM_MIN_LENGTH)));
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
        when(appenders[indexByTerm(TERM_ID_1, TERM_ID_1)].append(any(), anyInt(), anyInt())).thenReturn(FAILED);
        assertFalse(publication.offer(atomicSendBuffer));
    }

    @Test
    public void shouldRotateWhenAppendTrips()
    {
        when(appenders[indexByTerm(TERM_ID_1, TERM_ID_1)].append(any(), anyInt(), anyInt())).thenReturn(TRIPPED);
        when(appenders[indexByTerm(TERM_ID_1, TERM_ID_1)].tailVolatile()).thenReturn(TERM_MIN_LENGTH - RECORD_ALIGNMENT);
        when(limit.position()).thenReturn(Long.MAX_VALUE);

        assertFalse(publication.offer(atomicSendBuffer));
        assertTrue(publication.offer(atomicSendBuffer));

        final InOrder inOrder = inOrder(appenders[0], appenders[1], appenders[2], logMetaDataBuffer);
        inOrder.verify(appenders[indexByTerm(TERM_ID_1, TERM_ID_1 + 2)]).statusOrdered(NEEDS_CLEANING);
        inOrder.verify(logMetaDataBuffer).putIntOrdered(LOG_ACTIVE_TERM_ID_OFFSET, TERM_ID_1 + 1);
        inOrder.verify(appenders[indexByTerm(TERM_ID_1, TERM_ID_1 + 1)])
               .append(atomicSendBuffer, 0, atomicSendBuffer.capacity());

        dataHeaderFlyweight.wrap(headers[indexByTerm(TERM_ID_1, TERM_ID_1 + 1)]);
        assertThat(dataHeaderFlyweight.termId(), is(TERM_ID_1 + 1));
    }

    @Test
    public void shouldRotateWhenClaimTrips()
    {
        when(appenders[indexByTerm(TERM_ID_1, TERM_ID_1)].claim(anyInt(), any())).thenReturn(TRIPPED);
        when(appenders[indexByTerm(TERM_ID_1, TERM_ID_1)].tailVolatile()).thenReturn(TERM_MIN_LENGTH - RECORD_ALIGNMENT);
        when(limit.position()).thenReturn(Long.MAX_VALUE);

        final BufferClaim bufferClaim = new BufferClaim();
        assertFalse(publication.tryClaim(SEND_BUFFER_CAPACITY, bufferClaim));
        assertTrue(publication.tryClaim(SEND_BUFFER_CAPACITY, bufferClaim));

        final InOrder inOrder = inOrder(appenders[0], appenders[1], appenders[2], logMetaDataBuffer);
        inOrder.verify(appenders[indexByTerm(TERM_ID_1, TERM_ID_1 + 2)]).statusOrdered(NEEDS_CLEANING);
        inOrder.verify(logMetaDataBuffer).putIntOrdered(LOG_ACTIVE_TERM_ID_OFFSET, TERM_ID_1 + 1);
        inOrder.verify(appenders[indexByTerm(TERM_ID_1, TERM_ID_1 + 1)]).claim(SEND_BUFFER_CAPACITY, bufferClaim);

        dataHeaderFlyweight.wrap(headers[indexByTerm(TERM_ID_1, TERM_ID_1 + 1)]);
        assertThat(dataHeaderFlyweight.termId(), is(TERM_ID_1 + 1));
    }

    @Test
    public void shouldUnmapBuffersWhenReleased() throws Exception
    {
        publication.close();
        verify(logBuffers, times(1)).close();
    }

    @Test
    public void shouldNotUnmapBuffersBeforeLastRelease() throws Exception
    {
        publication.incRef();
        publication.close();
        verify(logBuffers, never()).close();
    }

    @Test
    public void shouldUnmapBuffersWithMultipleReferences() throws Exception
    {
        publication.incRef();
        publication.close();

        publication.close();
        verify(logBuffers, times(1)).close();
    }
}
