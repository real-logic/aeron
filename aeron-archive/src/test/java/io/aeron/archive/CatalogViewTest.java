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
import io.aeron.archive.client.RecordingDescriptorConsumer;
import org.agrona.IoUtil;
import org.agrona.concurrent.CachedEpochClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

class CatalogViewTest
{
    private static final long CAPACITY = 1024 * 1024;
    private static final int TERM_LENGTH = 2 * Catalog.PAGE_SIZE;
    private static final int SEGMENT_LENGTH = 2 * TERM_LENGTH;
    private static final int MTU_LENGTH = 1024;

    private final File archiveDir = ArchiveTests.makeTestDirectory();
    private final CachedEpochClock clock = new CachedEpochClock();

    private long recordingOneId;
    private long recordingTwoId;
    private long recordingThreeId;
    private final RecordingDescriptorConsumer mockRecordingDescriptorConsumer = mock(RecordingDescriptorConsumer.class);

    @BeforeEach
    void before()
    {
        clock.update(1);

        try (Catalog catalog = new Catalog(archiveDir, null, 0, CAPACITY, clock, null, null))
        {
            recordingOneId = catalog.addNewRecording(
                10L, 4L, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 7, 1, "channelG", "channelG?tag=f", "sourceA");
            recordingTwoId = catalog.addNewRecording(
                11L, 5L, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 8, 2, "channelH", "channelH?tag=f", "sourceV");
            recordingThreeId = catalog.addNewRecording(
                12L, 6L, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 9, 3, "channelK", "channelK?tag=f", "sourceB");
        }
    }

    @AfterEach
    void after()
    {
        IoUtil.delete(archiveDir, false);
    }

    @Test
    void shouldListAllRecordingsInCatalog()
    {
        final int count = CatalogView.listRecordings(archiveDir, mockRecordingDescriptorConsumer);
        assertEquals(3, count);

        verify(mockRecordingDescriptorConsumer).onRecordingDescriptor(
            Aeron.NULL_VALUE, Aeron.NULL_VALUE, recordingOneId, 4L, Aeron.NULL_VALUE, 10L,
            Aeron.NULL_VALUE, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 7, 1,
            "channelG", "channelG?tag=f", "sourceA");

        verify(mockRecordingDescriptorConsumer).onRecordingDescriptor(
            Aeron.NULL_VALUE, Aeron.NULL_VALUE, recordingTwoId, 5L, Aeron.NULL_VALUE, 11L,
            Aeron.NULL_VALUE, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 8, 2,
            "channelH", "channelH?tag=f", "sourceV");

        verify(mockRecordingDescriptorConsumer).onRecordingDescriptor(
            Aeron.NULL_VALUE, Aeron.NULL_VALUE, recordingThreeId, 6L, Aeron.NULL_VALUE, 12L,
            Aeron.NULL_VALUE, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 9, 3,
            "channelK", "channelK?tag=f", "sourceB");

        verifyNoMoreInteractions(mockRecordingDescriptorConsumer);
    }

    @Test
    void shouldListRecordingByRecordingId()
    {
        final boolean found = CatalogView.listRecording(archiveDir, recordingTwoId, mockRecordingDescriptorConsumer);
        assertTrue(found);

        verify(mockRecordingDescriptorConsumer).onRecordingDescriptor(
            Aeron.NULL_VALUE, Aeron.NULL_VALUE, recordingTwoId, 5L, Aeron.NULL_VALUE, 11L,
            Aeron.NULL_VALUE, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 8, 2,
            "channelH", "channelH?tag=f", "sourceV");

        verifyNoMoreInteractions(mockRecordingDescriptorConsumer);
    }
}
