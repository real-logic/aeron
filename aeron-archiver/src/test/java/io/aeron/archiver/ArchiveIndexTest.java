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

public class ArchiveIndexTest
{
    public static final int ARCHIVE_FILE_SIZE = 128 * 1024 * 1024;
    static final UnsafeBuffer UB = new UnsafeBuffer(
        BufferUtil.allocateDirectAligned(ArchiveIndex.INDEX_RECORD_LENGTH, 64));
    static final RecordingDescriptorDecoder DECODER = new RecordingDescriptorDecoder();
    static int recordingAId;
    static int recordingBId;
    static int recordingCId;
    static RecordingSession mockSession = mock(RecordingSession.class);
    private static File archiveFolder;

    @BeforeClass
    public static void setup() throws Exception
    {
        DECODER.wrap(
            UB,
            ArchiveIndex.INDEX_FRAME_LENGTH,
            RecordingDescriptorDecoder.BLOCK_LENGTH,
            RecordingDescriptorDecoder.SCHEMA_VERSION);
        archiveFolder = TestUtil.makeTempFolder();

        try (ArchiveIndex archiveIndex = new ArchiveIndex(archiveFolder))
        {
            recordingAId =
                archiveIndex.addNewRecording("sourceA", 6, "channelG", 1, 4096, 0, mockSession,
                    ARCHIVE_FILE_SIZE);
            recordingBId =
                archiveIndex.addNewRecording("sourceV", 7, "channelH", 2, 4096, 0, mockSession,
                    ARCHIVE_FILE_SIZE);
            recordingCId =
                archiveIndex.addNewRecording("sourceB", 8, "channelK", 3, 4096, 0, mockSession,
                    ARCHIVE_FILE_SIZE);
            archiveIndex.removeRecordingSession(recordingAId);
            archiveIndex.removeRecordingSession(recordingBId);
            archiveIndex.removeRecordingSession(recordingCId);
        }
    }

    @AfterClass
    public static void teardown()
    {
        IoUtil.delete(archiveFolder, false);
    }

    @Test
    public void shouldReloadExistingIndex() throws Exception
    {
        try (ArchiveIndex archiveIndex = new ArchiveIndex(archiveFolder))
        {
            verifyRecordingForId(archiveIndex, recordingAId, "sourceA", 6, "channelG", 1);
            verifyRecordingForId(archiveIndex, recordingBId, "sourceV", 7, "channelH", 2);
            verifyRecordingForId(archiveIndex, recordingCId, "sourceB", 8, "channelK", 3);
        }
    }

    private void verifyRecordingForId(
        final ArchiveIndex archiveIndex,
        final int id,
        final String source,
        final int sessionId,
        final String channel,
        final int streamId)
        throws IOException
    {
        UB.byteBuffer().clear();
        archiveIndex.readDescriptor(id, UB.byteBuffer());
        DECODER.limit(ArchiveIndex.INDEX_FRAME_LENGTH + RecordingDescriptorDecoder.BLOCK_LENGTH);
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
        try (ArchiveIndex archiveIndex = new ArchiveIndex(archiveFolder))
        {
            newRecordingId =
                archiveIndex.addNewRecording("sourceN", 9, "channelJ", 4, 4096, 0, mockSession,
                    ARCHIVE_FILE_SIZE);
            archiveIndex.removeRecordingSession(newRecordingId);
        }

        try (ArchiveIndex archiveIndex = new ArchiveIndex(archiveFolder))
        {
            verifyRecordingForId(archiveIndex, recordingAId, "sourceA", 6, "channelG", 1);
            verifyRecordingForId(archiveIndex, newRecordingId, "sourceN", 9, "channelJ", 4);
        }
    }

    @Test
    public void shouldAllowMultipleInstancesForSameStream() throws Exception
    {
        final int newRecordingId;
        try (ArchiveIndex archiveIndex = new ArchiveIndex(archiveFolder))
        {
            newRecordingId =
                archiveIndex.addNewRecording("sourceA", 6, "channelG", 1, 4096, 0, mockSession,
                    ARCHIVE_FILE_SIZE);
            archiveIndex.removeRecordingSession(newRecordingId);
            assertNotEquals(recordingAId, newRecordingId);
        }
    }
}
