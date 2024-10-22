/*
 * Copyright 2014-2024 Real Logic Limited.
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

import io.aeron.archive.codecs.RecordingDescriptorDecoder;
import io.aeron.archive.codecs.RecordingDescriptorHeaderDecoder;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

class ListRecordingsForUriSessionTest
{
    private static final long CAPACITY = 1024 * 1024;
    private static final int SEGMENT_FILE_SIZE = 128 * 1024 * 1024;
    public static final byte[] LOCALHOST_BYTES = "localhost".getBytes(StandardCharsets.US_ASCII);
    private final UnsafeBuffer descriptorBuffer = new UnsafeBuffer();
    private final RecordingDescriptorDecoder recordingDescriptorDecoder = new RecordingDescriptorDecoder();
    private final long[] matchingRecordingIds = new long[3];
    private final File archiveDir = ArchiveTests.makeTestDirectory();
    private final EpochClock clock = mock(EpochClock.class);

    private Catalog catalog;
    private final long correlationId = 1;
    private final ControlSession controlSession = mock(ControlSession.class);

    @BeforeEach
    void before()
    {
        catalog = new Catalog(archiveDir, null, 0, CAPACITY, clock, null, null);
        matchingRecordingIds[0] = catalog.addNewRecording(
            0L, 0L, 0, SEGMENT_FILE_SIZE, 4096, 1024, 6, 1, "localhost", "localhost?tag=f", "sourceA");
        catalog.addNewRecording(
            0L, 0L, 0, SEGMENT_FILE_SIZE, 4096, 1024, 7, 1, "channelA", "channel?tag=f", "sourceV");
        matchingRecordingIds[1] = catalog.addNewRecording(
            0L, 0L, 0, SEGMENT_FILE_SIZE, 4096, 1024, 8, 1, "localhost", "localhost?tag=f", "sourceB");
        catalog.addNewRecording(
            0L, 0L, 0, SEGMENT_FILE_SIZE, 4096, 1024, 8, 1, "channelB", "channelB?tag=f", "sourceB");
        matchingRecordingIds[2] = catalog.addNewRecording(
            0L, 0L, 0, SEGMENT_FILE_SIZE, 4096, 1024, 8, 1, "localhost", "localhost?tag=f", "sourceB");
    }

    @AfterEach
    void after()
    {
        CloseHelper.close(catalog);
        IoUtil.delete(archiveDir, false);
    }

    @Test
    void shouldSendAllDescriptors()
    {
        final ListRecordingsForUriSession session = new ListRecordingsForUriSession(
            correlationId,
            0,
            3,
            LOCALHOST_BYTES,
            1,
            catalog,
            controlSession,
            descriptorBuffer,
            recordingDescriptorDecoder);

        final MutableLong counter = new MutableLong(0);
        doAnswer(verifySendDescriptor(counter))
            .when(controlSession)
            .sendDescriptor(eq(correlationId), any());

        assertEquals(5, session.doWork());
        verify(controlSession, times(3)).sendDescriptor(eq(correlationId), any());
    }

    @Test
    void shouldSend2Descriptors()
    {
        final long fromRecordingId = 1;
        final ListRecordingsForUriSession session = new ListRecordingsForUriSession(
            correlationId,
            fromRecordingId,
            2,
            LOCALHOST_BYTES,
            1,
            catalog,
            controlSession,
            descriptorBuffer,
            recordingDescriptorDecoder);

        final MutableLong counter = new MutableLong(fromRecordingId);
        doAnswer(verifySendDescriptor(counter))
            .when(controlSession)
            .sendDescriptor(eq(correlationId), any());

        assertEquals(4, session.doWork());
        verify(controlSession, times(2)).sendDescriptor(eq(correlationId), any());
    }

    @Test
    void shouldSend2DescriptorsAndRecordingUnknown()
    {
        final ListRecordingsForUriSession session = new ListRecordingsForUriSession(
            correlationId,
            1,
            5,
            LOCALHOST_BYTES,
            1,
            catalog,
            controlSession,
            descriptorBuffer,
            recordingDescriptorDecoder);

        final MutableLong counter = new MutableLong(1);
        doAnswer(verifySendDescriptor(counter))
            .when(controlSession)
            .sendDescriptor(eq(correlationId), any());

        assertEquals(4, session.doWork());
        verify(controlSession, times(2)).sendDescriptor(eq(correlationId), any());
        verify(controlSession).sendRecordingUnknown(eq(correlationId), eq(5L));
    }

    @Test
    void shouldSendRecordingUnknown()
    {
        final ListRecordingsForUriSession session = new ListRecordingsForUriSession(
            correlationId,
            1,
            3,
            "notChannel".getBytes(StandardCharsets.US_ASCII),
            1,
            catalog,
            controlSession,
            descriptorBuffer,
            recordingDescriptorDecoder);

        assertEquals(4, session.doWork());

        verify(controlSession, never()).sendDescriptor(eq(correlationId), any());
        verify(controlSession).sendRecordingUnknown(eq(correlationId), eq(5L));
    }

    @Test
    void shouldSendUnknownOnFirst()
    {
        when(controlSession.maxPayloadLength()).thenReturn(4096 - 32);

        final ListRecordingsForUriSession session = new ListRecordingsForUriSession(
            correlationId,
            5,
            3,
            LOCALHOST_BYTES,
            1,
            catalog,
            controlSession,
            descriptorBuffer,
            recordingDescriptorDecoder);

        assertEquals(0, session.doWork());

        verify(controlSession, never()).sendDescriptor(eq(correlationId), any());
        verify(controlSession).sendRecordingUnknown(eq(correlationId), eq(5L));
    }

    private Answer<Object> verifySendDescriptor(final MutableLong counter)
    {
        return (invocation) ->
        {
            final UnsafeBuffer buffer = invocation.getArgument(1);
            recordingDescriptorDecoder.wrap(
                buffer,
                RecordingDescriptorHeaderDecoder.BLOCK_LENGTH,
                RecordingDescriptorDecoder.BLOCK_LENGTH,
                RecordingDescriptorDecoder.SCHEMA_VERSION);

            final int i = counter.intValue();
            assertEquals(matchingRecordingIds[i], recordingDescriptorDecoder.recordingId());
            counter.set(i + 1);

            return buffer.getInt(0);
        };
    }
}