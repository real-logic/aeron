package io.aeron.archiver;

import io.aeron.archiver.codecs.RecordingDescriptorDecoder;
import org.agrona.IoUtil;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;

import static io.aeron.archiver.ArchiveUtil.loadRecordingDescriptor;
import static io.aeron.archiver.ArchiveUtil.recordingDescriptorFileName;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

public class RecordingWriterTest
{
    private static final int RECORDING_ID = 1;
    private static final int TERM_BUFFER_LENGTH = 16 * 1024;
    private static final int MTU_LENGTH = 4 * 1024;
    private static final int INITIAL_TERM_ID = 3;
    private static final int JOIN_POSITION = 32;
    private static final int SESSION_ID = 1234;
    private static final int STREAM_ID = 0;
    private static final int SYNC_LEVEL = 2;
    private static final String CHANNEL = "channel";
    private static final String SOURCE = "source";
    private File archiveDir;
    private EpochClock epochClock = Mockito.mock(EpochClock.class);
    private final RecordingWriter.Context recordingCtx = new RecordingWriter.Context();
    private FileChannel mockArchiveDirFileChannel = Mockito.mock(FileChannel.class);
    private FileChannel mockDataFileChannel = Mockito.mock(FileChannel.class);
    private UnsafeBuffer mockTermBuffer = Mockito.mock(UnsafeBuffer.class);

    @Before
    public void before() throws Exception
    {
        archiveDir = TestUtil.makeTempDir();
        recordingCtx
            .archiveDirChannel(mockArchiveDirFileChannel)
            .archiveDir(archiveDir)
            .recordingFileLength(1024 * 1024)
            .epochClock(epochClock)
            .fileSyncLevel(SYNC_LEVEL);
    }

    @After
    public void after()
    {
        IoUtil.delete(archiveDir, false);
    }

    @Test
    public void shouldInitMetaData() throws IOException
    {
        when(epochClock.time()).thenReturn(42L);

        try (RecordingWriter ignored = new RecordingWriter(
            recordingCtx,
            RECORDING_ID,
            TERM_BUFFER_LENGTH,
            MTU_LENGTH,
            INITIAL_TERM_ID,
            JOIN_POSITION,
            SESSION_ID,
            STREAM_ID,
            CHANNEL,
            SOURCE))
        {
            final RecordingDescriptorDecoder descriptorDecoder = loadMetaData();
            assertEquals(RECORDING_ID, descriptorDecoder.recordingId());
            assertEquals(TERM_BUFFER_LENGTH, descriptorDecoder.termBufferLength());
            assertEquals(STREAM_ID, descriptorDecoder.streamId());
            assertEquals(MTU_LENGTH, descriptorDecoder.mtuLength());
            assertEquals(RecordingWriter.NULL_TIME, descriptorDecoder.joinTimestamp());
            assertEquals(JOIN_POSITION, descriptorDecoder.joinPosition());
            assertEquals(RecordingWriter.NULL_TIME, descriptorDecoder.endTimestamp());
            assertEquals(JOIN_POSITION, descriptorDecoder.endPosition());
            assertEquals(CHANNEL, descriptorDecoder.channel());
            assertEquals(SOURCE, descriptorDecoder.sourceIdentity());
            when(epochClock.time()).thenReturn(43L);
        }

        final RecordingDescriptorDecoder descriptorDecoder = loadMetaData();
        assertEquals(RECORDING_ID, descriptorDecoder.recordingId());
        assertEquals(TERM_BUFFER_LENGTH, descriptorDecoder.termBufferLength());
        assertEquals(STREAM_ID, descriptorDecoder.streamId());
        assertEquals(MTU_LENGTH, descriptorDecoder.mtuLength());
        assertEquals(RecordingWriter.NULL_TIME, descriptorDecoder.joinTimestamp());
        assertEquals(JOIN_POSITION, descriptorDecoder.joinPosition());
        assertEquals(43L, descriptorDecoder.endTimestamp());
        assertEquals(JOIN_POSITION, descriptorDecoder.endPosition());
        assertEquals(CHANNEL, descriptorDecoder.channel());
        assertEquals(SOURCE, descriptorDecoder.sourceIdentity());
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void verifyFirstWrite() throws IOException
    {
        when(epochClock.time()).thenReturn(42L);

        try (RecordingWriter writer = Mockito.spy(new RecordingWriter(
            recordingCtx,
            RECORDING_ID,
            TERM_BUFFER_LENGTH,
            MTU_LENGTH,
            INITIAL_TERM_ID,
            JOIN_POSITION,
            SESSION_ID,
            STREAM_ID,
            CHANNEL,
            SOURCE)))
        {
            final RecordingDescriptorDecoder descriptorDecoder = loadMetaData();
            assertEquals(RecordingWriter.NULL_TIME, descriptorDecoder.joinTimestamp());

            when(mockDataFileChannel.transferTo(eq(0L), eq(256L), any(FileChannel.class))).then(
                (invocation) ->
                {
                    final FileChannel dataFileChannel = invocation.getArgument(2);
                    dataFileChannel.position(JOIN_POSITION + 256);
                    return 256L;
                });

            writer.onBlock(
                mockDataFileChannel, 0, mockTermBuffer, JOIN_POSITION, 256, SESSION_ID, INITIAL_TERM_ID);

            when(epochClock.time()).thenReturn(43L);

            final InOrder inOrder = Mockito.inOrder(writer);
            inOrder.verify(writer).forceData(eq(mockArchiveDirFileChannel), eq(SYNC_LEVEL == 2));
            inOrder.verify(writer).forceData(any(FileChannel.class), eq(SYNC_LEVEL == 2));
        }

        final RecordingDescriptorDecoder descriptorDecoder = loadMetaData();
        assertEquals(42L, descriptorDecoder.joinTimestamp());
        assertEquals(43L, descriptorDecoder.endTimestamp());
        assertEquals(JOIN_POSITION, descriptorDecoder.joinPosition());
        assertEquals(JOIN_POSITION + 256, descriptorDecoder.endPosition());
    }

    private RecordingDescriptorDecoder loadMetaData() throws IOException
    {
        return loadRecordingDescriptor(new File(archiveDir, recordingDescriptorFileName(RECORDING_ID)));
    }
}