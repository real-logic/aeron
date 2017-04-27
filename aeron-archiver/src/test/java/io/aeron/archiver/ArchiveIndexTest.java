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

import io.aeron.archiver.codecs.ArchiveDescriptorDecoder;
import org.agrona.*;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.*;

import java.io.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;

public class ArchiveIndexTest
{
    static final UnsafeBuffer UB = new UnsafeBuffer(
        BufferUtil.allocateDirectAligned(ArchiveIndex.INDEX_RECORD_SIZE, 64));
    static final ArchiveDescriptorDecoder DECODER = new ArchiveDescriptorDecoder();
    public static final int ARCHIVE_FILE_SIZE = 128 * 1024 * 1024;


    private static File archiveFolder;

    static int streamInstanceAId;
    static int streamInstanceBId;
    static int streamInstanceCId;

    static ArchivingSession mockSession = mock(ArchivingSession.class);
    @BeforeClass
    public static void setup() throws Exception
    {
        DECODER.wrap(
            UB,
            ArchiveIndex.INDEX_FRAME_LENGTH,
            ArchiveDescriptorDecoder.BLOCK_LENGTH,
            ArchiveDescriptorDecoder.SCHEMA_VERSION);
        archiveFolder = TestUtil.makeTempFolder();

        try (ArchiveIndex archiveIndex = new ArchiveIndex(archiveFolder))
        {
            streamInstanceAId = archiveIndex.addNewStreamInstance("sourceA", 6, "channelG", 1, 4096, 0, mockSession,
                ARCHIVE_FILE_SIZE);
            streamInstanceBId = archiveIndex.addNewStreamInstance("sourceV", 7, "channelH", 2, 4096, 0, mockSession,
                ARCHIVE_FILE_SIZE);
            streamInstanceCId = archiveIndex.addNewStreamInstance("sourceB", 8, "channelK", 3, 4096, 0, mockSession,
                ARCHIVE_FILE_SIZE);
            archiveIndex.removeArchivingSession(streamInstanceAId);
            archiveIndex.removeArchivingSession(streamInstanceBId);
            archiveIndex.removeArchivingSession(streamInstanceCId);
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
            verifyArchiveForId(archiveIndex, streamInstanceAId, "sourceA", 6, "channelG", 1);
            verifyArchiveForId(archiveIndex, streamInstanceBId, "sourceV", 7, "channelH", 2);
            verifyArchiveForId(archiveIndex, streamInstanceCId, "sourceB", 8, "channelK", 3);
        }
    }

    private void verifyArchiveForId(
        final ArchiveIndex archiveIndex,
        final int id,
        final String source,
        final int sessionId, final String channel, final int streamId)
        throws IOException
    {
        UB.byteBuffer().clear();
        archiveIndex.readArchiveDescriptor(id, UB.byteBuffer());
        DECODER.limit(ArchiveIndex.INDEX_FRAME_LENGTH + ArchiveDescriptorDecoder.BLOCK_LENGTH);
        assertEquals(id, DECODER.streamInstanceId());
        assertEquals(sessionId, DECODER.sessionId());
        assertEquals(streamId, DECODER.streamId());
        assertEquals(source, DECODER.source());
        assertEquals(channel, DECODER.channel());
    }

    @Test
    public void shouldAppendToExistingIndex() throws Exception
    {
        final int newStreamInstanceId;
        try (ArchiveIndex archiveIndex = new ArchiveIndex(archiveFolder))
        {
            newStreamInstanceId = archiveIndex.addNewStreamInstance("sourceN", 9, "channelJ", 4, 4096, 0, mockSession,
                ARCHIVE_FILE_SIZE);
            archiveIndex.removeArchivingSession(newStreamInstanceId);
        }

        try (ArchiveIndex archiveIndex = new ArchiveIndex(archiveFolder))
        {
            verifyArchiveForId(archiveIndex, streamInstanceAId, "sourceA", 6, "channelG", 1);
            verifyArchiveForId(archiveIndex, newStreamInstanceId, "sourceN", 9, "channelJ", 4);
        }
    }

    @Test
    public void shouldAllowMultipleInstancesForSameStream() throws Exception
    {
        final int newStreamInstanceId;
        try (ArchiveIndex archiveIndex = new ArchiveIndex(archiveFolder))
        {
            newStreamInstanceId = archiveIndex.addNewStreamInstance("sourceA", 6, "channelG", 1, 4096, 0, mockSession,
                ARCHIVE_FILE_SIZE);
            archiveIndex.removeArchivingSession(newStreamInstanceId);
            assertNotEquals(streamInstanceAId, newStreamInstanceId);
        }
    }
}
