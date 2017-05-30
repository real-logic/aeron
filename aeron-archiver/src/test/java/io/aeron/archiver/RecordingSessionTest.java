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

import io.aeron.*;
import io.aeron.archiver.codecs.RecordingDescriptorDecoder;
import io.aeron.logbuffer.*;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.*;
import org.agrona.concurrent.*;
import org.junit.*;
import org.mockito.Mockito;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static io.aeron.archiver.ArchiveUtil.recordingMetaFileName;
import static java.nio.file.StandardOpenOption.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RecordingSessionTest
{
    private static final int SEGMENT_FILE_SIZE = 128 * 1024 * 1024;
    private static final int RECORDED_BLOCK_LENGTH = 100;
    private final long recordingId = 12345;

    private final String channel = "channel";
    private final String sourceIdentity = "sourceIdentity";
    private final int streamId = 54321;
    private final int sessionId = 12345;

    private final int termBufferLength = 4096;
    private final int termOffset = 1024;
    private final int mtuLength = 1024;
    private final long joiningPosition = termOffset;

    private final File tempDirForTest = TestUtil.makeTempDir();
    private final NotificationsProxy proxy;

    private final Image image;
    private final Catalog catalog;

    private FileChannel mockLogBufferChannel;
    private UnsafeBuffer mockLogBufferMapped;
    private File termFile;

    public RecordingSessionTest() throws IOException
    {
        proxy = mock(NotificationsProxy.class);
        catalog = mock(Catalog.class);
        final int initialTermId = 0;
        when(catalog.addNewRecording(
            eq(sessionId),
            eq(streamId),
            eq(channel),
            eq(sourceIdentity),
            eq(termBufferLength),
            eq(mtuLength),
            eq(initialTermId),
            eq(joiningPosition),
            any(RecordingSession.class),
            eq(SEGMENT_FILE_SIZE))).thenReturn(recordingId);

        image = mockImage(
            sessionId, initialTermId, sourceIdentity, termBufferLength, mockSubscription(channel, streamId));
    }

    @Before
    public void before() throws IOException
    {
        termFile = File.createTempFile("test.rec", "sourceIdentity");

        mockLogBufferChannel = FileChannel.open(termFile.toPath(), CREATE, READ, WRITE);
        mockLogBufferChannel.position(termBufferLength - 1);
        mockLogBufferChannel.write(ByteBuffer.wrap(new byte[1]));

        final ByteBuffer bb = ByteBuffer.allocate(RECORDED_BLOCK_LENGTH);
        mockLogBufferChannel.position(termOffset);
        mockLogBufferChannel.write(bb);
        mockLogBufferMapped = new UnsafeBuffer(
            mockLogBufferChannel.map(FileChannel.MapMode.READ_WRITE, 0, termBufferLength));

        final DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight();
        headerFlyweight.wrap(mockLogBufferMapped);
        headerFlyweight.headerType(DataHeaderFlyweight.HDR_TYPE_DATA).frameLength(RECORDED_BLOCK_LENGTH);
    }

    @After
    public void after()
    {
        IoUtil.unmap(mockLogBufferMapped.byteBuffer());
        CloseHelper.close(mockLogBufferChannel);
        IoUtil.delete(tempDirForTest, false);
        IoUtil.delete(termFile, false);
    }

    @Test
    public void shouldRecordFragmentsFromImage() throws Exception
    {
        final EpochClock epochClock = Mockito.mock(EpochClock.class);
        when(epochClock.time()).thenReturn(42L);

        final Recorder.Builder builder = new Recorder.Builder()
            .recordingFileLength(SEGMENT_FILE_SIZE)
            .archiveDir(tempDirForTest)
            .epochClock(epochClock);
        final RecordingSession session = new RecordingSession(proxy, catalog, image, builder);

        assertEquals(Catalog.NULL_RECORD_ID, session.recordingId());

        session.doWork();

        assertEquals(recordingId, session.recordingId());

        final File recordingMetaFile = new File(tempDirForTest, recordingMetaFileName(session.recordingId()));
        assertTrue(recordingMetaFile.exists());

        RecordingDescriptorDecoder metaData = ArchiveUtil.loadRecordingDescriptor(recordingMetaFile);

        assertEquals(recordingId, metaData.recordingId());
        assertEquals(termBufferLength, metaData.termBufferLength());
        assertEquals(streamId, metaData.streamId());
        assertEquals(mtuLength, metaData.mtuLength());
        assertEquals(Recorder.NULL_TIME, metaData.startTime());
        assertEquals(joiningPosition, metaData.joiningPosition());
        assertEquals(Recorder.NULL_TIME, metaData.endTime());
        assertEquals(Recorder.NULL_POSITION, metaData.lastPosition());
        assertEquals(channel, metaData.channel());
        assertEquals(sourceIdentity, metaData.sourceIdentity());

        when(image.rawPoll(any(), anyInt())).thenAnswer(
            (invocation) ->
            {
                final RawBlockHandler handle = invocation.getArgument(0);
                if (handle == null)
                {
                    return 0;
                }

                handle.onBlock(
                    mockLogBufferChannel,
                    0,
                    mockLogBufferMapped,
                    termOffset,
                    RECORDED_BLOCK_LENGTH,
                    sessionId,
                    0);

                return RECORDED_BLOCK_LENGTH;
            });

        assertNotEquals("Expect some work", 0, session.doWork());



        metaData = ArchiveUtil.loadRecordingDescriptor(recordingMetaFile);

        assertEquals(42L, metaData.startTime());

        assertEquals(Recorder.NULL_TIME, metaData.endTime());
        assertEquals(joiningPosition + RECORDED_BLOCK_LENGTH, metaData.lastPosition());

        final File segmentFile =
            new File(tempDirForTest, ArchiveUtil.recordingDataFileName(recordingId, 0));
        assertTrue(segmentFile.exists());

        try (RecordingFragmentReader reader = new RecordingFragmentReader(session.recordingId(), tempDirForTest))
        {
            final int polled = reader.controlledPoll(
                (buffer, offset, length) ->
                {
                    final int frameOffset = offset - DataHeaderFlyweight.HEADER_LENGTH;
                    assertEquals(termOffset, frameOffset);
                    assertEquals(RECORDED_BLOCK_LENGTH, FrameDescriptor.frameLength(buffer, frameOffset));
                    assertEquals(RECORDED_BLOCK_LENGTH - DataHeaderFlyweight.HEADER_LENGTH, length);
                    return true;
                },
                1);

            assertEquals(1, polled);
        }

        when(image.rawPoll(any(), anyInt())).thenReturn(0);
        assertEquals("Expect no work", 0, session.doWork());

        when(image.isClosed()).thenReturn(true);
        when(epochClock.time()).thenReturn(128L);

        assertNotEquals("Expect some work", 0, session.doWork());
        assertTrue(session.isDone());

        metaData = ArchiveUtil.loadRecordingDescriptor(recordingMetaFile);
        assertEquals(128L, metaData.endTime());
    }

    private Subscription mockSubscription(final String channel, final int streamId)
    {
        final Subscription subscription = mock(Subscription.class);

        when(subscription.channel()).thenReturn(channel);
        when(subscription.streamId()).thenReturn(streamId);

        return subscription;
    }

    private Image mockImage(
        final int sessionId,
        final int initialTermId,
        final String sourceIdentity,
        final int termBufferLength,
        final Subscription subscription)
    {
        final Image image = mock(Image.class);

        when(image.sessionId()).thenReturn(sessionId);
        when(image.initialTermId()).thenReturn(initialTermId);
        when(image.sourceIdentity()).thenReturn(sourceIdentity);
        when(image.termBufferLength()).thenReturn(termBufferLength);
        when(image.subscription()).thenReturn(subscription);
        when(image.mtuLength()).thenReturn(mtuLength);
        when(image.joiningPosition()).thenReturn(joiningPosition);

        return image;
    }
}
