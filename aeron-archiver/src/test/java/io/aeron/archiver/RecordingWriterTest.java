package io.aeron.archiver;

import io.aeron.archiver.codecs.RecordingDescriptorDecoder;
import org.agrona.IoUtil;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;

import static io.aeron.archiver.ArchiveUtil.loadRecordingDescriptor;
import static io.aeron.archiver.ArchiveUtil.recordingMetaFileName;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

public class RecordingWriterTest
{

    public static final int RECORDING_ID = 1;
    public static final int TERM_BUFFER_LENGTH = 16 * 1024;
    public static final int MTU_LENGTH = 4 * 1024;
    public static final int INITIAL_TERM_ID = 3;
    public static final int JOIN_POSITION = 32;
    public static final int SESSION_ID = 1234;
    public static final int STREAM_ID = 0;
    public static final String CHANNEL = "channel";
    public static final String SOURCE = "source";
    private File archiveDir;
    private EpochClock epochClock = Mockito.mock(EpochClock.class);
    final RecordingWriter.RecordingContext recordingContext = new RecordingWriter.RecordingContext();
    private FileChannel mockFileChannel = Mockito.mock(FileChannel.class);
    private UnsafeBuffer mockTermBuffer = Mockito.mock(UnsafeBuffer.class);

    @Before
    public void before() throws Exception
    {
        archiveDir = TestUtil.makeTempDir();
        recordingContext
            .recordingFileLength(1024 * 1024)
            .archiveDir(archiveDir)
            .epochClock(epochClock)
            .forceWrites(true);
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
        try (RecordingWriter writer = new RecordingWriter(
            recordingContext,
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
            final RecordingDescriptorDecoder metaData = loadMetaData();
            assertEquals(RECORDING_ID, metaData.recordingId());
            assertEquals(TERM_BUFFER_LENGTH, metaData.termBufferLength());
            assertEquals(STREAM_ID, metaData.streamId());
            assertEquals(MTU_LENGTH, metaData.mtuLength());
            assertEquals(RecordingWriter.NULL_TIME, metaData.joinTimestamp());
            assertEquals(JOIN_POSITION, metaData.joinPosition());
            assertEquals(RecordingWriter.NULL_TIME, metaData.endTimestamp());
            assertEquals(JOIN_POSITION, metaData.endPosition());
            assertEquals(CHANNEL, metaData.channel());
            assertEquals(SOURCE, metaData.sourceIdentity());
            when(epochClock.time()).thenReturn(43L);
        }
        final RecordingDescriptorDecoder metaData = loadMetaData();
        assertEquals(RECORDING_ID, metaData.recordingId());
        assertEquals(TERM_BUFFER_LENGTH, metaData.termBufferLength());
        assertEquals(STREAM_ID, metaData.streamId());
        assertEquals(MTU_LENGTH, metaData.mtuLength());
        assertEquals(RecordingWriter.NULL_TIME, metaData.joinTimestamp());
        assertEquals(JOIN_POSITION, metaData.joinPosition());
        assertEquals(43L, metaData.endTimestamp());
        assertEquals(JOIN_POSITION, metaData.endPosition());
        assertEquals(CHANNEL, metaData.channel());
        assertEquals(SOURCE, metaData.sourceIdentity());
    }

    @Test
    public void verifyFirstWrite() throws IOException
    {
        when(epochClock.time()).thenReturn(42L);

        try (RecordingWriter writer = Mockito.spy(new RecordingWriter(
            recordingContext,
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
            final RecordingDescriptorDecoder metaData = loadMetaData();
            assertEquals(RecordingWriter.NULL_TIME, metaData.joinTimestamp());

            when(mockFileChannel.transferTo(eq(0L), eq(256L), any(FileChannel.class))).then(invocation ->
            {
                final FileChannel dataFileChannel = (FileChannel)invocation.getArgument(2);
                dataFileChannel.position(JOIN_POSITION + 256);
                return 256L;
            });
            writer.onBlock(
                mockFileChannel, 0, mockTermBuffer, JOIN_POSITION, 256, SESSION_ID, INITIAL_TERM_ID);
            when(epochClock.time()).thenReturn(43L);
            Mockito.verify(writer).forceData(any(FileChannel.class));
        }

        final RecordingDescriptorDecoder metaData = loadMetaData();
        assertEquals(42L, metaData.joinTimestamp());
        assertEquals(43L, metaData.endTimestamp());
        assertEquals(JOIN_POSITION, metaData.joinPosition());
        assertEquals(JOIN_POSITION + 256, metaData.endPosition());
    }

    private RecordingDescriptorDecoder loadMetaData() throws IOException
    {
        return loadRecordingDescriptor(new File(archiveDir, recordingMetaFileName(RECORDING_ID)));
    }
}