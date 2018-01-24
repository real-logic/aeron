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
package io.aeron.archive;

import io.aeron.Counter;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.logbuffer.RawBlockHandler;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.channels.FileChannel;

import static io.aeron.archive.Archive.segmentFileName;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static java.nio.file.StandardOpenOption.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class RecordingSessionTest
{
    private static final int RECORDED_BLOCK_LENGTH = 100;
    private static final long RECORDING_ID = 12345;
    private static final int TERM_BUFFER_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int SEGMENT_FILE_SIZE = TERM_BUFFER_LENGTH;

    private static final String CHANNEL = "channel";
    private static final String SOURCE_IDENTITY = "sourceIdentity";
    private static final int STREAM_ID = 54321;

    private static final int SESSION_ID = 12345;
    private static final int TERM_OFFSET = 1024;
    private static final int MTU_LENGTH = 1024;
    private static final long START_POSITION = TERM_OFFSET;
    private static final int INITIAL_TERM_ID = 0;
    public static final FileChannel ARCHIVE_CHANNEL = null;

    private final RecordingEventsProxy recordingEventsProxy = mock(RecordingEventsProxy.class);
    private final Counter position = mock(Counter.class);
    private Image image = mockImage(
        SESSION_ID, INITIAL_TERM_ID, SOURCE_IDENTITY, TERM_BUFFER_LENGTH, mockSubscription(CHANNEL, STREAM_ID));
    private File tempDirForTest = TestUtil.makeTempDir();
    private FileChannel mockLogBufferChannel;
    private UnsafeBuffer mockLogBufferMapped;
    private File termFile;
    private EpochClock epochClock = mock(EpochClock.class);
    private Catalog mockCatalog = mock(Catalog.class);
    private Archive.Context context;
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

        termFile = File.createTempFile("test.rec", "sourceIdentity");

        mockLogBufferChannel = FileChannel.open(termFile.toPath(), CREATE, READ, WRITE);
        mockLogBufferMapped = new UnsafeBuffer(
            mockLogBufferChannel.map(FileChannel.MapMode.READ_WRITE, 0, TERM_BUFFER_LENGTH));

        final DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight();
        headerFlyweight.wrap(mockLogBufferMapped, TERM_OFFSET, DataHeaderFlyweight.HEADER_LENGTH);
        headerFlyweight
            .termOffset(TERM_OFFSET)
            .sessionId(SESSION_ID)
            .streamId(STREAM_ID)
            .headerType(DataHeaderFlyweight.HDR_TYPE_DATA)
            .frameLength(RECORDED_BLOCK_LENGTH);

        context = new Archive.Context()
            .segmentFileLength(SEGMENT_FILE_SIZE)
            .archiveDir(tempDirForTest)
            .epochClock(epochClock);
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
        final RecordingSession session = new RecordingSession(
            RECORDING_ID, START_POSITION, CHANNEL, recordingEventsProxy, image, position, ARCHIVE_CHANNEL, context);

        assertEquals(RECORDING_ID, session.sessionId());

        session.doWork();

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
                    TERM_OFFSET,
                    mockLogBufferMapped,
                    TERM_OFFSET,
                    RECORDED_BLOCK_LENGTH,
                    SESSION_ID,
                    0);

                return RECORDED_BLOCK_LENGTH;
            });

        assertNotEquals("Expect some work", 0, session.doWork());

        final File segmentFile = new File(tempDirForTest, segmentFileName(RECORDING_ID, 0));
        assertTrue(segmentFile.exists());

        final RecordingSummary recordingSummary = new RecordingSummary();
        recordingSummary.recordingId = RECORDING_ID;
        recordingSummary.startPosition = START_POSITION;
        recordingSummary.segmentFileLength = context.segmentFileLength();
        recordingSummary.initialTermId = INITIAL_TERM_ID;
        recordingSummary.termBufferLength = TERM_BUFFER_LENGTH;
        recordingSummary.streamId = STREAM_ID;
        recordingSummary.sessionId = SESSION_ID;
        recordingSummary.stopPosition = START_POSITION + RECORDED_BLOCK_LENGTH;

        final RecordingFragmentReader reader = new RecordingFragmentReader(
            mockCatalog,
            recordingSummary,
            tempDirForTest,
            NULL_POSITION,
            RecordingFragmentReader.NULL_LENGTH,
            null);

        final int polled = reader.controlledPoll(
            (buffer, offset, length) ->
            {
                final int frameOffset = offset - DataHeaderFlyweight.HEADER_LENGTH;
                assertEquals(TERM_OFFSET, frameOffset);
                assertEquals(RECORDED_BLOCK_LENGTH, FrameDescriptor.frameLength(buffer, frameOffset));
                assertEquals(RECORDED_BLOCK_LENGTH - DataHeaderFlyweight.HEADER_LENGTH, length);
                return true;
            },
            1);

        assertEquals(1, polled);

        reader.close();

        when(image.rawPoll(any(), anyInt())).thenReturn(0);
        assertEquals("Expect no work", 0, session.doWork());

        when(image.isClosed()).thenReturn(true);
        session.doWork();
        session.doWork();
        assertTrue(session.isDone());
        session.close();
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
        when(image.mtuLength()).thenReturn(MTU_LENGTH);
        when(image.joinPosition()).thenReturn(START_POSITION);

        return image;
    }
}
