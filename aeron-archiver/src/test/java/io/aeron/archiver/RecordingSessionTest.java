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
    private static final int SEGMENT_FILE_SIZE = 128 * 1024 * 1024;
    private static final int RECORDED_BLOCK_LENGTH = 100;
    private static final long RECORDING_ID = 12345;

    private final String channel = "channel";
    private final String sourceIdentity = "sourceIdentity";
    private final int streamId = 54321;
    private final int sessionId = 12345;

    private final int termBufferLength = 4096;
    private final int termOffset = 1024;
    private final int mtuLength = 1024;
    private final long joinPosition = termOffset;
    private final int initialTermId = 0;

    private final RecordingEventsProxy recordingEventsProxy = mock(RecordingEventsProxy.class);

    private File tempDirForTest;

    private Image image;

    private FileChannel mockLogBufferChannel;
    private UnsafeBuffer mockLogBufferMapped;
    private File termFile;
    private EpochClock epochClock;
    private RecordingDescriptorEncoder descriptorEncoder;
    private RecordingDescriptorDecoder descriptorDecoder;
    private RecordingWriter.Context context;
    private UnsafeBuffer descriptorBuffer;


    @Before
    public void before() throws IOException
    {
        tempDirForTest = TestUtil.makeTempDir();

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

        image = mockImage(
            sessionId, initialTermId, sourceIdentity, termBufferLength, mockSubscription(channel, streamId));

        epochClock = Mockito.mock(EpochClock.class);

        context = new RecordingWriter.Context()
            .recordingFileLength(SEGMENT_FILE_SIZE)
            .archiveDir(tempDirForTest)
            .epochClock(epochClock);
        descriptorBuffer = new UnsafeBuffer(new byte[Catalog.RECORD_LENGTH]);
        descriptorEncoder = new RecordingDescriptorEncoder().wrap(
            descriptorBuffer,
            Catalog.CATALOG_FRAME_LENGTH);
        descriptorDecoder = new RecordingDescriptorDecoder().wrap(
            descriptorBuffer,
            Catalog.CATALOG_FRAME_LENGTH,
            RecordingDescriptorDecoder.BLOCK_LENGTH,
            RecordingDescriptorDecoder.SCHEMA_VERSION);
        Catalog.initDescriptor(
            descriptorEncoder,
            RECORDING_ID,
            termBufferLength,
            context.segmentFileLength,
            mtuLength,
            initialTermId,
            joinPosition,
            sessionId,
            streamId,
            channel,
            sourceIdentity);
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
        assertEquals(joinPosition, descriptorDecoder.joinPosition());
        assertEquals(Catalog.NULL_TIME, descriptorDecoder.endTimestamp());
        assertEquals(joinPosition, descriptorDecoder.endPosition());

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

        assertEquals(42L, descriptorDecoder.joinTimestamp());

        assertEquals(Catalog.NULL_TIME, descriptorDecoder.endTimestamp());
        assertEquals(joinPosition + RECORDED_BLOCK_LENGTH, descriptorDecoder.endPosition());

        final File segmentFile =
            new File(tempDirForTest, ArchiveUtil.recordingDataFileName(RECORDING_ID, 0));
        assertTrue(segmentFile.exists());

        try (RecordingFragmentReader reader = newRecordingFragmentReader(descriptorBuffer, tempDirForTest))
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
        when(image.mtuLength()).thenReturn(mtuLength);
        when(image.joinPosition()).thenReturn(joinPosition);

        return image;
    }
}
