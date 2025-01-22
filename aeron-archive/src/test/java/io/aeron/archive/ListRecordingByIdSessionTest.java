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

import io.aeron.archive.codecs.RecordingDescriptorDecoder;
import io.aeron.archive.codecs.RecordingDescriptorHeaderDecoder;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import java.io.File;

import static org.mockito.Mockito.*;

class ListRecordingByIdSessionTest
{
    private static final long CAPACITY = 1024 * 1024;
    private static final int SEGMENT_FILE_SIZE = 128 * 1024 * 1024;
    private final RecordingDescriptorDecoder recordingDescriptorDecoder = new RecordingDescriptorDecoder();
    private final long[] recordingIds = new long[3];
    private final File archiveDir = ArchiveTests.makeTestDirectory();
    private final EpochClock clock = mock(EpochClock.class);

    private Catalog catalog;
    private final ControlSession controlSession = mock(ControlSession.class);
    private final UnsafeBuffer descriptorBuffer = new UnsafeBuffer();

    @BeforeEach
    void before()
    {
        catalog = new Catalog(archiveDir, null, 0, CAPACITY, clock, null, null);
        recordingIds[0] = catalog.addNewRecording(
            0L, 0L, 0, SEGMENT_FILE_SIZE, 4096, 1024, 6, 1, "channelG", "channelG?tag=f", "sourceA");
        recordingIds[1] = catalog.addNewRecording(
            0L, 0L, 0, SEGMENT_FILE_SIZE, 4096, 1024, 7, 2, "channelH", "channelH?tag=f", "sourceV");
        recordingIds[2] = catalog.addNewRecording(
            0L, 0L, 0, SEGMENT_FILE_SIZE, 4096, 1024, 8, 3, "channelK", "channelK?tag=f", "sourceB");
    }

    @AfterEach
    void after()
    {
        CloseHelper.close(catalog);
        IoUtil.delete(archiveDir, false);
    }

    @Test
    void shouldSendDescriptor()
    {
        final long correlationId = -53465834;
        final ListRecordingByIdSession session =
            new ListRecordingByIdSession(correlationId, recordingIds[1], catalog, controlSession, descriptorBuffer);

        doAnswer(verifySendDescriptor())
            .when(controlSession)
            .sendDescriptor(eq(correlationId), any());

        session.doWork();
        verify(controlSession).sendDescriptor(eq(correlationId), any());
        verifyNoMoreInteractions(controlSession);
    }

    @Test
    void shouldSendRecordingUnknownOnFirst()
    {
        final long correlationId = 42;
        final long unknownRecordingId = 17777;
        final ListRecordingByIdSession session =
            new ListRecordingByIdSession(correlationId, unknownRecordingId, catalog, controlSession, descriptorBuffer);

        session.doWork();

        verify(controlSession).sendRecordingUnknown(eq(correlationId), eq(unknownRecordingId));
        verifyNoMoreInteractions(controlSession);
    }

    private Answer<Object> verifySendDescriptor()
    {
        return (invocation) ->
        {
            final UnsafeBuffer buffer = invocation.getArgument(1);

            recordingDescriptorDecoder.wrap(
                buffer,
                RecordingDescriptorHeaderDecoder.BLOCK_LENGTH,
                RecordingDescriptorDecoder.BLOCK_LENGTH,
                RecordingDescriptorDecoder.SCHEMA_VERSION);

            return true;
        };
    }
}
