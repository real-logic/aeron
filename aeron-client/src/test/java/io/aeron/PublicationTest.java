/*
 * Copyright 2014-2019 Real Logic Ltd.
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
package io.aeron;

import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.status.ChannelEndpointStatus;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.ReadablePosition;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static java.nio.ByteBuffer.allocateDirect;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class PublicationTest
{
    private static final String CHANNEL = "aeron:udp?endpoint=localhost:40124";
    private static final int STREAM_ID_1 = 2;
    private static final int SESSION_ID_1 = 13;
    private static final int TERM_ID_1 = 1;
    private static final int CORRELATION_ID = 2000;
    private static final int SEND_BUFFER_CAPACITY = 1024;
    private static final int PARTITION_INDEX = 0;
    private static final int MTU_LENGTH = 4096;
    private static final int PAGE_SIZE = 4 * 1024;

    private final ByteBuffer sendBuffer = allocateDirect(SEND_BUFFER_CAPACITY);
    private final UnsafeBuffer atomicSendBuffer = new UnsafeBuffer(sendBuffer);
    private final UnsafeBuffer logMetaDataBuffer = spy(new UnsafeBuffer(allocateDirect(LOG_META_DATA_LENGTH)));
    private final UnsafeBuffer[] termBuffers = new UnsafeBuffer[PARTITION_COUNT];

    private final ClientConductor conductor = mock(ClientConductor.class);
    private final LogBuffers logBuffers = mock(LogBuffers.class);
    private final ReadablePosition publicationLimit = mock(ReadablePosition.class);
    private ConcurrentPublication publication;

    @Before
    public void setUp()
    {
        when(publicationLimit.getVolatile()).thenReturn(2L * SEND_BUFFER_CAPACITY);
        when(logBuffers.duplicateTermBuffers()).thenReturn(termBuffers);
        when(logBuffers.termLength()).thenReturn(TERM_MIN_LENGTH);
        when(logBuffers.metaDataBuffer()).thenReturn(logMetaDataBuffer);

        initialTermId(logMetaDataBuffer, TERM_ID_1);
        mtuLength(logMetaDataBuffer, MTU_LENGTH);
        termLength(logMetaDataBuffer, TERM_MIN_LENGTH);
        pageSize(logMetaDataBuffer, PAGE_SIZE);
        isConnected(logMetaDataBuffer, false);

        for (int i = 0; i < PARTITION_COUNT; i++)
        {
            termBuffers[i] = new UnsafeBuffer(allocateDirect(TERM_MIN_LENGTH));
        }

        publication = new ConcurrentPublication(
            conductor,
            CHANNEL,
            STREAM_ID_1,
            SESSION_ID_1,
            publicationLimit,
            ChannelEndpointStatus.NO_ID_ALLOCATED,
            logBuffers,
            CORRELATION_ID,
            CORRELATION_ID);

        initialiseTailWithTermId(logMetaDataBuffer, PARTITION_INDEX, TERM_ID_1);

        doAnswer(
            (invocation) ->
            {
                publication.internalClose();
                return null;
            }).when(conductor).releasePublication(publication);
    }

    @Test
    public void shouldEnsureThePublicationIsOpenBeforeReadingPosition()
    {
        publication.close();
        assertThat(publication.position(), is(Publication.CLOSED));

        verify(conductor).releasePublication(publication);
    }

    @Test
    public void shouldEnsureThePublicationIsOpenBeforeOffer()
    {
        publication.close();
        assertTrue(publication.isClosed());
        assertThat(publication.offer(atomicSendBuffer), is(Publication.CLOSED));
    }

    @Test
    public void shouldEnsureThePublicationIsOpenBeforeClaim()
    {
        publication.close();
        final BufferClaim bufferClaim = new BufferClaim();
        assertThat(publication.tryClaim(SEND_BUFFER_CAPACITY, bufferClaim), is(Publication.CLOSED));
    }

    @Test
    public void shouldReportThatPublicationHasNotBeenConnectedYet()
    {
        when(publicationLimit.getVolatile()).thenReturn(0L);
        isConnected(logMetaDataBuffer, false);
        assertFalse(publication.isConnected());
    }

    @Test
    public void shouldReportThatPublicationHasBeenConnectedYet()
    {
        isConnected(logMetaDataBuffer, true);
        assertTrue(publication.isConnected());
    }

    @Test
    public void shouldReportInitialPosition()
    {
        assertThat(publication.position(), is(0L));
    }

    @Test
    public void shouldReportMaxMessageLength()
    {
        assertThat(publication.maxMessageLength(), is(FrameDescriptor.computeMaxMessageLength(TERM_MIN_LENGTH)));
    }

    @Test
    public void shouldReleasePublicationOnClose()
    {
        publication.close();

        verify(conductor).releasePublication(publication);
    }
}
