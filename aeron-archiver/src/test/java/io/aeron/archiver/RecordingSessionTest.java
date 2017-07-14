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

import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archiver.codecs.RecordingDescriptorDecoder;
import io.aeron.archiver.codecs.RecordingDescriptorEncoder;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.RawBlockHandler;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static io.aeron.archiver.TestUtil.newRecordingFragmentReader;
import static java.nio.file.StandardOpenOption.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RecordingSessionTest
{
    private static final int RECORDED_BLOCK_LENGTH = 100;
    private static final long RECORDING_ID = 12345;
    private static final int TERM_BUFFER_LENGTH = 4096;
    private static final int SEGMENT_FILE_SIZE = TERM_BUFFER_LENGTH;

    private static final String CHANNEL = "channel";
    private static final String SOURCE_IDENTITY = "sourceIdentity";
    private static final int STREAM_ID = 54321;

    private static final int SESSION_ID = 12345;
    private static final int TERM_OFFSET = 1024;
    private static final int MTU_LENGTH = 1024;
    private static final long JOIN_POSITION = TERM_OFFSET;
    private static final int INITIAL_TERM_ID = 0;

    private final RecordingEventsProxy recordingEventsProxy = mock(RecordingEventsProxy.class);
    private Image image = mockImage(
        SESSION_ID, INITIAL_TERM_ID, SOURCE_IDENTITY, TERM_BUFFER_LENGTH, mockSubscription(CHANNEL, STREAM_ID));
    private File tempDirForTest = TestUtil.makeTempDir();
    private FileChannel mockLogBufferChannel;
    private UnsafeBuffer mockLogBufferMapped;
    private File termFile;
    private EpochClock epochClock = Mockito.mock(EpochClock.class);
    private RecordingDescriptorDecoder descriptorDecoder;
    private RecordingWriter.Context context;
    private UnsafeBuffer descriptorBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(Catalog.RECORD_LENGTH));

    @Before
    public void before() throws IOException
    {
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

        context = new RecordingWriter.Context()
            .recordingFileLength(SEGMENT_FILE_SIZE)
            .archiveDir(tempDirForTest)
            .epochClock(epochClock);

        descriptorDecoder = new RecordingDescriptorDecoder().wrap(
            descriptorBuffer,
            Catalog.CATALOG_FRAME_LENGTH,
            RecordingDescriptorDecoder.BLOCK_LENGTH,
            RecordingDescriptorDecoder.SCHEMA_VERSION);

        Catalog.initDescriptor(
            new RecordingDescriptorEncoder().wrap(descriptorBuffer, Catalog.CATALOG_FRAME_LENGTH),
            RECORDING_ID,
            JOIN_POSITION,
            INITIAL_TERM_ID,
            context.segmentFileLength,
            TERM_BUFFER_LENGTH,
            MTU_LENGTH,
            SESSION_ID,
            STREAM_ID,
            CHANNEL,
            SOURCE_IDENTITY,
            CHANNEL);
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
        when(epochClock.time()).thenReturn(42L);

        final RecordingSession session = new RecordingSession(
            RECORDING_ID, descriptorBuffer, recordingEventsProxy, image, context);

        assertEquals(RECORDING_ID, session.sessionId());

        session.doWork();

        assertEquals(Catalog.NULL_TIME, descriptorDecoder.joinTimestamp());
        assertEquals(JOIN_POSITION, descriptorDecoder.joinPosition());
        assertEquals(Catalog.NULL_TIME, descriptorDecoder.endTimestamp());
        assertEquals(JOIN_POSITION, descriptorDecoder.endPosition());

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

        assertEquals(42L, descriptorDecoder.joinTimestamp());

        assertEquals(Catalog.NULL_TIME, descriptorDecoder.endTimestamp());
        assertEquals(JOIN_POSITION + RECORDED_BLOCK_LENGTH, descriptorDecoder.endPosition());

        final File segmentFile = new File(tempDirForTest, ArchiveUtil.recordingDataFileName(RECORDING_ID, 0));
        assertTrue(segmentFile.exists());

        try (RecordingFragmentReader reader = newRecordingFragmentReader(descriptorBuffer, tempDirForTest))
        {
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
        }

        when(image.rawPoll(any(), anyInt())).thenReturn(0);
        assertEquals("Expect no work", 0, session.doWork());

        when(image.isClosed()).thenReturn(true);
        when(epochClock.time()).thenReturn(128L);
        session.doWork();
        assertTrue(session.isDone());
        session.close();

        assertEquals(128L, descriptorDecoder.endTimestamp());
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
        when(image.joinPosition()).thenReturn(JOIN_POSITION);

        return image;
    }
}
