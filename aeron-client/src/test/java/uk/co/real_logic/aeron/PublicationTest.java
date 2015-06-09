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
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.aeron.logbuffer.FrameDescriptor;

import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.concurrent.status.ReadablePosition;

import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.*;

public class PublicationTest
{
    private static final String CHANNEL = "udp://localhost:40124";
    private static final int STREAM_ID_1 = 2;
    private static final int SESSION_ID_1 = 13;
    private static final int TERM_ID_1 = 1;
    private static final int CORRELATION_ID = 2000;
    private static final int SEND_BUFFER_CAPACITY = 1024;

    private final ByteBuffer sendBuffer = ByteBuffer.allocateDirect(SEND_BUFFER_CAPACITY);
    private final UnsafeBuffer atomicSendBuffer = new UnsafeBuffer(sendBuffer);
    private final UnsafeBuffer logMetaDataBuffer = spy(new UnsafeBuffer(ByteBuffer.allocateDirect(LOG_META_DATA_LENGTH)));
    private final UnsafeBuffer[] termBuffers = new UnsafeBuffer[PARTITION_COUNT];
    private final UnsafeBuffer[] termMetaDataBuffers = new UnsafeBuffer[PARTITION_COUNT];
    private final UnsafeBuffer[] buffers = new UnsafeBuffer[(PARTITION_COUNT * 2) + 1];

    private Publication publication;
    private ClientConductor conductor = mock(ClientConductor.class);
    private LogBuffers logBuffers = mock(LogBuffers.class);

    @Before
    public void setUp()
    {
        final ReadablePosition limit = mock(ReadablePosition.class);
        when(limit.getVolatile()).thenReturn(2L * SEND_BUFFER_CAPACITY);
        when(logBuffers.atomicBuffers()).thenReturn(buffers);

        initialTermId(logMetaDataBuffer, TERM_ID_1);

        for (int i = 0; i < PARTITION_COUNT; i++)
        {
            termBuffers[i] = new UnsafeBuffer(ByteBuffer.allocateDirect(TERM_MIN_LENGTH));
            termMetaDataBuffers[i] = new UnsafeBuffer(ByteBuffer.allocateDirect(TERM_META_DATA_LENGTH));

            buffers[i] = termBuffers[i];
            buffers[i + PARTITION_COUNT] = termMetaDataBuffers[i];
        }
        buffers[LOG_META_DATA_SECTION_INDEX] = logMetaDataBuffer;

        publication = new Publication(
            conductor,
            CHANNEL,
            STREAM_ID_1,
            SESSION_ID_1,
            limit,
            logBuffers,
            CORRELATION_ID);

        publication.incRef();
    }

    @Test(expected = IllegalStateException.class)
    public void shouldEnsureThePublicationIsOpenBeforeReadingPosition()
    {
        publication.close();
        publication.position();
    }

    @Test(expected = IllegalStateException.class)
    public void shouldEnsureThePublicationIsOpenBeforeOffer()
    {
        publication.close();
        publication.offer(atomicSendBuffer);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldEnsureThePublicationIsOpenBeforeClaim()
    {
        publication.close();
        final BufferClaim bufferClaim = new BufferClaim();
        publication.tryClaim(SEND_BUFFER_CAPACITY, bufferClaim);
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
    public void shouldUnmapBuffersWhenReleased() throws Exception
    {
        publication.close();

        logBuffersClosedOnce();
        releaseSelfOnce();
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
        logBuffersClosedOnce();
    }

    @Test
    public void shouldReleaseResourcesIdempotently() throws Exception
    {
        publication.close();
        publication.close();

        logBuffersClosedOnce();
        releaseSelfOnce();
    }

    private void logBuffersClosedOnce()
    {
        verify(logBuffers, times(1)).close();
    }

    private void releaseSelfOnce()
    {
        verify(conductor, times(1)).releasePublication(publication);
    }
}
