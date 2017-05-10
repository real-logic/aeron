/*
 * Copyright 2014-2017 Real Logic Ltd.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
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
    static final UnsafeBuffer BUFFER = new UnsafeBuffer(
        BufferUtil.allocateDirectAligned(Catalog.RECORD_LENGTH, 64));
    static final RecordingDescriptorDecoder DECODER = new RecordingDescriptorDecoder();
    static int recordingAId;
    static int recordingBId;
    static int recordingCId;
    static RecordingSession mockSession = mock(RecordingSession.class);
    private static File archiveDir;

    @BeforeClass
    public static void setup() throws Exception
    {
        DECODER.wrap(
            BUFFER,
            Catalog.CATALOG_FRAME_LENGTH,
            RecordingDescriptorDecoder.BLOCK_LENGTH,
            RecordingDescriptorDecoder.SCHEMA_VERSION);
        archiveDir = TestUtil.makeTempDir();

        try (Catalog catalog = new Catalog(archiveDir))
        {
            recordingAId =
                catalog.addNewRecording("sourceA", 6, "channelG", 1, 4096, 0, mockSession,
                    SEGMENT_FILE_SIZE);
            recordingBId =
                catalog.addNewRecording("sourceV", 7, "channelH", 2, 4096, 0, mockSession,
                    SEGMENT_FILE_SIZE);
            recordingCId =
                catalog.addNewRecording("sourceB", 8, "channelK", 3, 4096, 0, mockSession,
                    SEGMENT_FILE_SIZE);
            catalog.removeRecordingSession(recordingAId);
            catalog.removeRecordingSession(recordingBId);
            catalog.removeRecordingSession(recordingCId);
        }
    }

    @AfterClass
    public static void teardown()
    {
        IoUtil.delete(archiveDir, false);
    }

    @Test
    public void shouldReloadExistingIndex() throws Exception
    {
        try (Catalog catalog = new Catalog(archiveDir))
        {
            verifyRecordingForId(catalog, recordingAId, "sourceA", 6, "channelG", 1);
            verifyRecordingForId(catalog, recordingBId, "sourceV", 7, "channelH", 2);
            verifyRecordingForId(catalog, recordingCId, "sourceB", 8, "channelK", 3);
        }
    }

    private void verifyRecordingForId(
        final Catalog catalog,
        final int id,
        final String source,
        final int sessionId,
        final String channel,
        final int streamId)
        throws IOException
    {
        BUFFER.byteBuffer().clear();
        catalog.readDescriptor(id, BUFFER.byteBuffer());
        DECODER.limit(Catalog.CATALOG_FRAME_LENGTH + RecordingDescriptorDecoder.BLOCK_LENGTH);
        assertEquals(id, DECODER.recordingId());
        assertEquals(sessionId, DECODER.sessionId());
        assertEquals(streamId, DECODER.streamId());
        assertEquals(source, DECODER.source());
        assertEquals(channel, DECODER.channel());
    }

    @Test
    public void shouldAppendToExistingIndex() throws Exception
    {
        final int newRecordingId;
        try (Catalog catalog = new Catalog(archiveDir))
        {
            newRecordingId =
                catalog.addNewRecording("sourceN", 9, "channelJ", 4, 4096, 0, mockSession,
                    SEGMENT_FILE_SIZE);
            catalog.removeRecordingSession(newRecordingId);
        }

        try (Catalog catalog = new Catalog(archiveDir))
        {
            verifyRecordingForId(catalog, recordingAId, "sourceA", 6, "channelG", 1);
            verifyRecordingForId(catalog, newRecordingId, "sourceN", 9, "channelJ", 4);
        }
    }

    @Test
    public void shouldAllowMultipleInstancesForSameStream() throws Exception
    {
        final int newRecordingId;
        try (Catalog catalog = new Catalog(archiveDir))
        {
            newRecordingId =
                catalog.addNewRecording("sourceA", 6, "channelG", 1, 4096, 0, mockSession,
                    SEGMENT_FILE_SIZE);
            catalog.removeRecordingSession(newRecordingId);
            assertNotEquals(recordingAId, newRecordingId);
        }
    }
}
