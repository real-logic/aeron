package io.aeron.archive;

import java.io.File;

import org.agrona.IoUtil;
import org.agrona.concurrent.EpochClock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;


import io.aeron.Aeron;
import io.aeron.archive.client.RecordingDescriptorConsumer;

public class CatalogViewTest
{
    private static final long MAX_ENTRIES = 1024;
    private static final int TERM_LENGTH = 2 * Catalog.PAGE_SIZE;
    private static final int SEGMENT_LENGTH = 2 * TERM_LENGTH;
    private static final int MTU_LENGTH = 1024;

    private final File archiveDir = TestUtil.makeTestDirectory();

    private long currentTimeMs = 1;
    private final EpochClock clock = () -> currentTimeMs;

    private long recordingOneId;
    private long recordingTwoId;
    private long recordingThreeId;
    private RecordingDescriptorConsumer mockRecordingDescriptorConsumer = mock(RecordingDescriptorConsumer.class);


    @Before
    public void before()
    {
        try (Catalog catalog = new Catalog(archiveDir, null, 0, MAX_ENTRIES, clock))
        {
            recordingOneId = catalog.addNewRecording(
                    10L, 4L, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 7, 1, "channelG", "channelG?tag=f", "sourceA");
            recordingTwoId = catalog.addNewRecording(
                    11L, 5L, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 8, 2, "channelH", "channelH?tag=f", "sourceV");
            recordingThreeId = catalog.addNewRecording(
                    12L, 6L, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 9, 3, "channelK", "channelK?tag=f", "sourceB");
        }
    }

    @After
    public void after()
    {
        IoUtil.delete(archiveDir, false);
    }


    @Test
    public void shouldListAllRecordingsInCatalog()
    {
        CatalogView.listRecordings(archiveDir, mockRecordingDescriptorConsumer);
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
    public void shouldListRecordingByRecordingId()
    {
        CatalogView.listRecording(archiveDir, recordingTwoId, mockRecordingDescriptorConsumer);

        verify(mockRecordingDescriptorConsumer).onRecordingDescriptor(
            Aeron.NULL_VALUE, Aeron.NULL_VALUE, recordingTwoId, 5L, Aeron.NULL_VALUE, 11L,
            Aeron.NULL_VALUE, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 8, 2,
            "channelH", "channelH?tag=f", "sourceV");

        verifyNoMoreInteractions(mockRecordingDescriptorConsumer);
    }
}