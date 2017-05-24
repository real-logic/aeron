/*
 * Copyright 2014-2017 Real Logic Ltd.
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
package io.aeron.archiver;

import io.aeron.archiver.codecs.RecordingDescriptorDecoder;
import org.agrona.*;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.*;

import java.io.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;

public class CatalogTest
{
    public static final int SEGMENT_FILE_SIZE = 128 * 1024 * 1024;
    static final UnsafeBuffer BUFFER = new UnsafeBuffer(BufferUtil.allocateDirectAligned(Catalog.RECORD_LENGTH, 64));
    static final RecordingDescriptorDecoder DECODER = new RecordingDescriptorDecoder();
    static long recordingOneId;
    static long recordingTwoId;
    static long recordingThreeId;
    static RecordingSession mockSession = mock(RecordingSession.class);
    private static File archiveDir;

    @BeforeClass
    public static void before() throws Exception
    {
        DECODER.wrap(
            BUFFER,
            Catalog.CATALOG_FRAME_LENGTH,
            RecordingDescriptorDecoder.BLOCK_LENGTH,
            RecordingDescriptorDecoder.SCHEMA_VERSION);
        archiveDir = TestUtil.makeTempDir();

        try (Catalog catalog = new Catalog(archiveDir))
        {
            recordingOneId = catalog.addNewRecording(
                6, 1, "channelG", "sourceA", 4096, 1024, 0, 0L, mockSession, SEGMENT_FILE_SIZE);
            recordingTwoId = catalog.addNewRecording(
                7, 2, "channelH", "sourceV", 4096, 1024, 0, 0L, mockSession, SEGMENT_FILE_SIZE);
            recordingThreeId = catalog.addNewRecording(
                8, 3, "channelK", "sourceB", 4096, 1024, 0, 0L, mockSession, SEGMENT_FILE_SIZE);
            catalog.removeRecordingSession(recordingOneId);
            catalog.removeRecordingSession(recordingTwoId);
            catalog.removeRecordingSession(recordingThreeId);
        }
    }

    @AfterClass
    public static void after()
    {
        IoUtil.delete(archiveDir, false);
    }

    @Test
    public void shouldReloadExistingIndex() throws Exception
    {
        try (Catalog catalog = new Catalog(archiveDir))
        {
            verifyRecordingForId(catalog, recordingOneId, 6, 1, "channelG", "sourceA");
            verifyRecordingForId(catalog, recordingTwoId, 7, 2, "channelH", "sourceV");
            verifyRecordingForId(catalog, recordingThreeId, 8, 3, "channelK", "sourceB");
        }
    }

    private void verifyRecordingForId(
        final Catalog catalog,
        final long id,
        final int sessionId,
        final int streamId,
        final String channel,
        final String sourceIdentity)
        throws IOException
    {
        BUFFER.byteBuffer().clear();
        catalog.readDescriptor(id, BUFFER.byteBuffer());
        DECODER.limit(Catalog.CATALOG_FRAME_LENGTH + RecordingDescriptorDecoder.BLOCK_LENGTH);
        assertEquals(id, DECODER.recordingId());
        assertEquals(sessionId, DECODER.sessionId());
        assertEquals(streamId, DECODER.streamId());
        assertEquals(channel, DECODER.channel());
        assertEquals(sourceIdentity, DECODER.sourceIdentity());
    }

    @Test
    public void shouldAppendToExistingIndex() throws Exception
    {
        final long newRecordingId;
        try (Catalog catalog = new Catalog(archiveDir))
        {
            newRecordingId = catalog.addNewRecording(
                9, 4, "channelJ", "sourceN", 4096, 1024, 0, 0L, mockSession, SEGMENT_FILE_SIZE);
            catalog.removeRecordingSession(newRecordingId);
        }

        try (Catalog catalog = new Catalog(archiveDir))
        {
            verifyRecordingForId(catalog, recordingOneId, 6, 1, "channelG", "sourceA");
            verifyRecordingForId(catalog, newRecordingId, 9, 4, "channelJ", "sourceN");
        }
    }

    @Test
    public void shouldAllowMultipleInstancesForSameStream() throws Exception
    {
        try (Catalog catalog = new Catalog(archiveDir))
        {
            final long newRecordingId = catalog.addNewRecording(
                6, 1, "channelG", "sourceA", 4096, 1024, 0, 0L, mockSession, SEGMENT_FILE_SIZE);
            catalog.removeRecordingSession(newRecordingId);
            assertNotEquals(recordingOneId, newRecordingId);
        }
    }
}
