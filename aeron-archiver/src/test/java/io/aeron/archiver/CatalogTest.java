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
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

public class CatalogTest
{
    private static final int SEGMENT_FILE_SIZE = 128 * 1024 * 1024;
    private final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(new byte[0]);
    private final RecordingDescriptorDecoder recordingDescriptorDecoder = new RecordingDescriptorDecoder();
    private long recordingOneId;
    private long recordingTwoId;
    private long recordingThreeId;
    private File archiveDir;

    @Before
    public void before() throws Exception
    {
        recordingDescriptorDecoder.wrap(
            unsafeBuffer,
            Catalog.CATALOG_FRAME_LENGTH,
            RecordingDescriptorDecoder.BLOCK_LENGTH,
            RecordingDescriptorDecoder.SCHEMA_VERSION);
        archiveDir = TestUtil.makeTempDir();

        try (Catalog catalog = new Catalog(archiveDir))
        {
            recordingOneId = catalog.addNewRecording(
                6, 1, "channelG", "sourceA", 4096, 1024, 0, 0L, SEGMENT_FILE_SIZE);
            recordingTwoId = catalog.addNewRecording(
                7, 2, "channelH", "sourceV", 4096, 1024, 0, 0L, SEGMENT_FILE_SIZE);
            recordingThreeId = catalog.addNewRecording(
                8, 3, "channelK", "sourceB", 4096, 1024, 0, 0L, SEGMENT_FILE_SIZE);
        }
    }

    @After
    public void after()
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
        assertTrue(catalog.wrapDescriptor(id, unsafeBuffer));
        recordingDescriptorDecoder.wrap(
            unsafeBuffer,
            Catalog.CATALOG_FRAME_LENGTH,
            RecordingDescriptorDecoder.BLOCK_LENGTH,
            RecordingDescriptorDecoder.SCHEMA_VERSION);

        assertEquals(id, recordingDescriptorDecoder.recordingId());
        assertEquals(sessionId, recordingDescriptorDecoder.sessionId());
        assertEquals(streamId, recordingDescriptorDecoder.streamId());
        assertEquals(channel, recordingDescriptorDecoder.channel());
        assertEquals(sourceIdentity, recordingDescriptorDecoder.sourceIdentity());
    }

    @Test
    public void shouldAppendToExistingIndex() throws Exception
    {
        final long newRecordingId;
        try (Catalog catalog = new Catalog(archiveDir))
        {
            newRecordingId = catalog.addNewRecording(
                9, 4, "channelJ", "sourceN", 4096, 1024, 0, 0L, SEGMENT_FILE_SIZE);
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
                6, 1, "channelG", "sourceA", 4096, 1024, 0, 0L, SEGMENT_FILE_SIZE);
            assertNotEquals(recordingOneId, newRecordingId);
        }
    }

    // TODO: cover refreshCatalog and IO exception cases
}
