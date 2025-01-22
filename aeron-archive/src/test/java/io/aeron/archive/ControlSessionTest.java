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
package io.aeron.archive;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.archive.codecs.RecordingDescriptorEncoder;
import io.aeron.archive.codecs.RecordingDescriptorHeaderEncoder;
import io.aeron.archive.codecs.RecordingState;
import io.aeron.security.Authenticator;
import org.agrona.BitUtil;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.aeron.archive.ControlSession.State.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ControlSessionTest
{
    private static final long CONTROL_SESSION_ID = -953749534;
    private static final long CORRELATION_ID = 47354;
    private static final long CONTROL_PUBLICATION_ID = 777;
    private static final long CONNECT_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(5);
    private static final long SESSION_LIVENESS_CHECK_INTERVAL_NS = TimeUnit.MILLISECONDS.toNanos(100);

    private final ControlSessionAdapter mockDemuxer = mock(ControlSessionAdapter.class);
    private final ArchiveConductor mockConductor = mock(ArchiveConductor.class);
    private final Aeron mockAeron = mock(Aeron.class);
    private final ExclusivePublication mockControlPublication = mock(ExclusivePublication.class);
    private final ControlResponseProxy mockProxy = mock(ControlResponseProxy.class);
    private final ControlSessionProxy mockSessionProxy = mock(ControlSessionProxy.class);
    private final Authenticator mockAuthenticator = mock(Authenticator.class);
    private final CachedEpochClock cachedEpochClock = new CachedEpochClock();
    private ControlSession session;

    @BeforeEach
    void before()
    {
        session = new ControlSession(
            CONTROL_SESSION_ID,
            CORRELATION_ID,
            CONNECT_TIMEOUT_MS,
            SESSION_LIVENESS_CHECK_INTERVAL_NS,
            CONTROL_PUBLICATION_ID,
            null,
            mockDemuxer,
            mockAeron,
            mockConductor,
            cachedEpochClock,
            mockProxy,
            mockAuthenticator,
            mockSessionProxy);

        when(mockAeron.getExclusivePublication(CONTROL_PUBLICATION_ID)).thenReturn(mockControlPublication);
        when(mockControlPublication.isConnected()).thenReturn(true);
    }

    @Test
    void shouldTimeoutIfConnectSentButPublicationNotConnected()
    {
        when(mockControlPublication.isClosed()).thenReturn(false);
        when(mockControlPublication.isConnected()).thenReturn(false);

        session.doWork();

        cachedEpochClock.update(CONNECT_TIMEOUT_MS + 1L);
        session.doWork();
        assertEquals(DONE, session.state());
        assertTrue(session.isDone());
    }

    @Test
    void shouldTimeoutIfConnectSentButPublicationFailsToSend()
    {
        when(mockControlPublication.isClosed()).thenReturn(false);
        when(mockControlPublication.isConnected()).thenReturn(true);

        session.doWork();
        session.sendOkResponse(1L);
        session.doWork();

        cachedEpochClock.update(CONNECT_TIMEOUT_MS + 1L);
        session.doWork();
        assertEquals(DONE, session.state());
        assertTrue(session.isDone());
    }

    @Test
    void shouldCopyDescriptor()
    {
        final long correlationId = -438682374754L;
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[1024]);
        ThreadLocalRandom.current().nextBytes(buffer.byteArray());

        final RecordingDescriptorEncoder recordingDescriptorEncoder = new RecordingDescriptorEncoder();
        recordingDescriptorEncoder
            .wrap(buffer, RecordingDescriptorHeaderEncoder.BLOCK_LENGTH)
            .strippedChannel("aeron:udp?endpoint=localhost:12345")
            .originalChannel("aeron:udp?mtu=2048|term-length=128k|endpoint=localhost:12345")
            .sourceIdentity("the source of this mess");

        final int fullDescriptorLength = BitUtil.align(
            RecordingDescriptorHeaderEncoder.BLOCK_LENGTH + recordingDescriptorEncoder.encodedLength(),
            BitUtil.CACHE_LINE_LENGTH);

        final RecordingDescriptorHeaderEncoder headerEncoder = new RecordingDescriptorHeaderEncoder();
        headerEncoder
            .wrap(buffer, 0)
            .length(fullDescriptorLength)
            .state(RecordingState.VALID)
            .checksum(ThreadLocalRandom.current().nextInt());

        final int payloadOffset =
            RecordingDescriptorHeaderEncoder.BLOCK_LENGTH + RecordingDescriptorEncoder.recordingIdEncodingOffset();
        final int payloadLength = fullDescriptorLength - RecordingDescriptorEncoder.recordingIdEncodingOffset();

        session.sendDescriptor(correlationId, buffer);

        verify(mockProxy)
            .sendDescriptor(CONTROL_SESSION_ID, correlationId, buffer, payloadOffset, payloadLength, session);

        while (session.state() != CONNECTED)
        {
            session.doWork();
        }

        session.authenticate(buffer.byteArray());
        assertEquals(AUTHENTICATED, session.state());

        session.onArchiveId(777);
        assertEquals(ACTIVE, session.state());

        session.doWork();

        final ArgumentCaptor<UnsafeBuffer> bufferCaptor = ArgumentCaptor.forClass(UnsafeBuffer.class);
        verify(mockProxy).sendDescriptor(
            eq(CONTROL_SESSION_ID), eq(correlationId), bufferCaptor.capture(), eq(0), eq(payloadLength), same(session));
        final UnsafeBuffer tmpBuffer = bufferCaptor.getValue();
        assertNotNull(tmpBuffer);
        assertNotSame(buffer, tmpBuffer);
        for (int i = 0; i < payloadLength; i++)
        {
            assertEquals(buffer.getByte(payloadOffset + i), tmpBuffer.getByte(i));
        }
    }
}
