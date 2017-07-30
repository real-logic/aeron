package io.aeron.archive;

import io.aeron.archive.codecs.RecordingDescriptorDecoder;
import io.aeron.archive.codecs.RecordingDescriptorEncoder;
import org.agrona.IoUtil;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;

import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static org.agrona.BufferUtil.allocateDirectAligned;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class RecordingWriterTest
{
    private static final int RECORDING_ID = 1;
    private static final int TERM_BUFFER_LENGTH = 16 * 1024;
    private static final int MTU_LENGTH = 4 * 1024;
    private static final int INITIAL_TERM_ID = 3;
    private static final int START_POSITION = 32;
    private static final int SESSION_ID = 1234;
    private static final int STREAM_ID = 0;
    private static final int SYNC_LEVEL = 2;
    private static final String CHANNEL = "channel";
    private static final String SOURCE = "source";
    private static final long START_TIMESTAMP = 0L;
    public static final long STOP_TIMESTAMP = 43L;
    private File archiveDir = TestUtil.makeTempDir();
    private EpochClock epochClock = Mockito.mock(EpochClock.class);
    private final RecordingWriter.Context recordingCtx = new RecordingWriter.Context();
    private FileChannel mockArchiveDirFileChannel = Mockito.mock(FileChannel.class);
    private FileChannel mockDataFileChannel = Mockito.mock(FileChannel.class);
    private UnsafeBuffer mockTermBuffer = Mockito.mock(UnsafeBuffer.class);
    private final AtomicCounter position = mock(AtomicCounter.class);
    private long positionLong;

    @Before
    public void before() throws Exception
    {
        when(position.getWeak()).then((invocation) -> positionLong);
        when(position.get()).then((invocation) -> positionLong);
        doAnswer(
            (invocation) ->
            {
                positionLong = invocation.getArgument(0);
                return null;
            })
            .when(position).setOrdered(anyLong());

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

    @SuppressWarnings("ConstantConditions")
    @Test
    public void verifyFirstWrite() throws IOException
    {
        when(epochClock.time()).thenReturn(42L);

        final UnsafeBuffer descriptorBuffer =
            new UnsafeBuffer(allocateDirectAligned(Catalog.DEFAULT_RECORD_LENGTH, FRAME_ALIGNMENT));
        final RecordingDescriptorEncoder descriptorEncoder = new RecordingDescriptorEncoder().wrap(
            descriptorBuffer,
            Catalog.DESCRIPTOR_HEADER_LENGTH);
        final RecordingDescriptorDecoder descriptorDecoder = new RecordingDescriptorDecoder().wrap(
            descriptorBuffer,
            Catalog.DESCRIPTOR_HEADER_LENGTH,
            RecordingDescriptorDecoder.BLOCK_LENGTH,
            RecordingDescriptorDecoder.SCHEMA_VERSION);
        Catalog.initDescriptor(
            descriptorEncoder,
            RECORDING_ID,
            START_TIMESTAMP,
            START_POSITION,
            INITIAL_TERM_ID,
            recordingCtx.segmentFileLength,
            TERM_BUFFER_LENGTH,
            MTU_LENGTH,
            SESSION_ID,
            STREAM_ID,
            CHANNEL,
            CHANNEL,
            SOURCE);

        try (RecordingWriter writer = Mockito.spy(new RecordingWriter(recordingCtx, descriptorBuffer, position)))
        {
            assertEquals(START_TIMESTAMP, descriptorDecoder.startTimestamp());

            when(mockDataFileChannel.transferTo(eq(0L), eq(256L), any(FileChannel.class))).then(
                (invocation) ->
                {
                    final FileChannel dataFileChannel = invocation.getArgument(2);
                    dataFileChannel.position(START_POSITION + 256);
                    return 256L;
                });

            writer.onBlock(
                mockDataFileChannel, 0, mockTermBuffer, START_POSITION, 256, SESSION_ID, INITIAL_TERM_ID);

            when(epochClock.time()).thenReturn(STOP_TIMESTAMP);

            final InOrder inOrder = Mockito.inOrder(writer);
            inOrder.verify(writer).forceData(eq(mockArchiveDirFileChannel), eq(SYNC_LEVEL == 2));
            inOrder.verify(writer).forceData(any(FileChannel.class), eq(SYNC_LEVEL == 2));
        }

        assertEquals(START_TIMESTAMP, descriptorDecoder.startTimestamp());
        assertEquals(STOP_TIMESTAMP, descriptorDecoder.stopTimestamp());
        assertEquals(START_POSITION, descriptorDecoder.startPosition());
        assertEquals(START_POSITION + 256, descriptorDecoder.stopPosition());
    }
}