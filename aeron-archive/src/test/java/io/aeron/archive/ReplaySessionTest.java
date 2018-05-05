/*
 * Copyright 2014-2018 Real Logic Ltd.
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
import io.aeron.ExclusivePublication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.ExclusiveBufferClaim;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.Header;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.IoUtil;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;

import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_DATA;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_PAD;
import static org.agrona.BufferUtil.allocateDirectAligned;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ReplaySessionTest
{
    private static final int RECORDING_ID = 0;
    private static final int TERM_BUFFER_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int INITIAL_TERM_ID = 8231773;
    private static final int INITIAL_TERM_OFFSET = 1024;
    private static final long START_POSITION = INITIAL_TERM_OFFSET;
    private static final long JOIN_POSITION = START_POSITION;
    private static final long RECORDING_POSITION = INITIAL_TERM_OFFSET;
    private static final int MTU_LENGTH = 4096;
    private static final long TIME = 0;
    private static final int FRAME_LENGTH = 1024;
    private static final int SESSION_ID = 1;
    private static final int STREAM_ID = 1;
    private static final FileChannel ARCHIVE_DIR_CHANNEL = null;

    private final ExclusivePublication mockReplayPub = mock(ExclusivePublication.class);
    private final ControlSession mockControlSession = mock(ControlSession.class);
    private final ArchiveConductor mockArchiveConductor = mock(ArchiveConductor.class);
    private final Counter position = mock(Counter.class);

    private int messageCounter = 0;

    private final File archiveDir = TestUtil.makeTestDirectory();
    private ControlResponseProxy proxy = mock(ControlResponseProxy.class);
    private EpochClock epochClock = mock(EpochClock.class);
    private Catalog mockCatalog = mock(Catalog.class);
    private Archive.Context context;
    private long positionLong;
    private RecordingSummary recordingSummary = new RecordingSummary();

    @Before
    public void before() throws IOException
    {
        when(position.getWeak()).then((invocation) -> positionLong);
        when(position.get()).then((invocation) -> positionLong);
        when(mockArchiveConductor.catalog()).thenReturn(mockCatalog);

        doAnswer(
            (invocation) ->
            {
                positionLong = invocation.getArgument(0);
                return null;
            })
            .when(position).setOrdered(anyLong());

        doAnswer(
            (invocation) ->
            {
                final long delta = invocation.getArgument(0);
                positionLong += delta;
                return null;
            })
            .when(position).getAndAddOrdered(anyLong());

        context = new Archive.Context()
            .archiveDir(archiveDir)
            .epochClock(epochClock);

        recordingSummary.recordingId = RECORDING_ID;
        recordingSummary.startPosition = START_POSITION;
        recordingSummary.segmentFileLength = context.segmentFileLength();
        recordingSummary.initialTermId = INITIAL_TERM_ID;
        recordingSummary.termBufferLength = TERM_BUFFER_LENGTH;
        recordingSummary.mtuLength = MTU_LENGTH;
        recordingSummary.streamId = STREAM_ID;
        recordingSummary.sessionId = SESSION_ID;

        final RecordingWriter writer = new RecordingWriter(
            RECORDING_ID, START_POSITION, JOIN_POSITION, TERM_BUFFER_LENGTH, context, ARCHIVE_DIR_CHANNEL, position);

        writer.init(INITIAL_TERM_OFFSET);

        final UnsafeBuffer buffer = new UnsafeBuffer(allocateDirectAligned(TERM_BUFFER_LENGTH, 64));

        final DataHeaderFlyweight headerFwt = new DataHeaderFlyweight();
        final Header header = new Header(INITIAL_TERM_ID, Integer.numberOfLeadingZeros(TERM_BUFFER_LENGTH));
        header.buffer(buffer);

        recordFragment(writer, buffer, headerFwt, header, 0, FrameDescriptor.UNFRAGMENTED, HDR_TYPE_DATA);
        recordFragment(writer, buffer, headerFwt, header, 1, FrameDescriptor.BEGIN_FRAG_FLAG, HDR_TYPE_DATA);
        recordFragment(writer, buffer, headerFwt, header, 2, FrameDescriptor.END_FRAG_FLAG, HDR_TYPE_DATA);
        recordFragment(writer, buffer, headerFwt, header, 3, FrameDescriptor.UNFRAGMENTED, HDR_TYPE_PAD);

        writer.close();
        recordingSummary.stopPosition = START_POSITION + 4 * FRAME_LENGTH;
    }

    @After
    public void after()
    {
        IoUtil.delete(archiveDir, false);
    }

    @Test
    public void verifyRecordingFile()
    {
        try (RecordingFragmentReader reader = new RecordingFragmentReader(
            mockCatalog,
            recordingSummary,
            archiveDir,
            NULL_POSITION,
            AeronArchive.NULL_LENGTH,
            null))
        {
            int fragments = reader.controlledPoll(
                (buffer, offset, length, frameType, flags, reservedValue) ->
                {
                    final int frameOffset = offset - DataHeaderFlyweight.HEADER_LENGTH;
                    assertEquals(offset, INITIAL_TERM_OFFSET + HEADER_LENGTH);
                    assertEquals(length, FRAME_LENGTH - HEADER_LENGTH);
                    assertEquals(FrameDescriptor.frameType(buffer, frameOffset), HDR_TYPE_DATA);
                    assertEquals(FrameDescriptor.frameFlags(buffer, frameOffset), FrameDescriptor.UNFRAGMENTED);

                    return true;
                },
                1);

            assertEquals(1, fragments);

            fragments = reader.controlledPoll(
                (buffer, offset, length, frameType, flags, reservedValue) ->
                {
                    final int frameOffset = offset - DataHeaderFlyweight.HEADER_LENGTH;
                    assertEquals(offset, INITIAL_TERM_OFFSET + FRAME_LENGTH + HEADER_LENGTH);
                    assertEquals(length, FRAME_LENGTH - HEADER_LENGTH);
                    assertEquals(FrameDescriptor.frameType(buffer, frameOffset), HDR_TYPE_DATA);
                    assertEquals(FrameDescriptor.frameFlags(buffer, frameOffset), FrameDescriptor.BEGIN_FRAG_FLAG);

                    return true;
                },
                1);

            assertEquals(1, fragments);

            fragments = reader.controlledPoll(
                (buffer, offset, length, frameType, flags, reservedValue) ->
                {
                    final int frameOffset = offset - DataHeaderFlyweight.HEADER_LENGTH;
                    assertEquals(offset, INITIAL_TERM_OFFSET + 2 * FRAME_LENGTH + HEADER_LENGTH);
                    assertEquals(length, FRAME_LENGTH - HEADER_LENGTH);
                    assertEquals(FrameDescriptor.frameType(buffer, frameOffset), HDR_TYPE_DATA);
                    assertEquals(FrameDescriptor.frameFlags(buffer, frameOffset), FrameDescriptor.END_FRAG_FLAG);

                    return true;
                },
                1);

            assertEquals(1, fragments);

            fragments = reader.controlledPoll(
                (buffer, offset, length, frameType, flags, reservedValue) ->
                {
                    final int frameOffset = offset - DataHeaderFlyweight.HEADER_LENGTH;
                    assertEquals(offset, INITIAL_TERM_OFFSET + 3 * FRAME_LENGTH + HEADER_LENGTH);
                    assertEquals(length, FRAME_LENGTH - HEADER_LENGTH);
                    assertEquals(FrameDescriptor.frameType(buffer, frameOffset), HDR_TYPE_PAD);
                    assertEquals(FrameDescriptor.frameFlags(buffer, frameOffset), FrameDescriptor.UNFRAGMENTED);

                    return true;
                },
                1);

            assertEquals(1, fragments);
        }
    }

    @Test
    public void shouldReplayPartialDataFromFile()
    {
        final long correlationId = 1L;

        final ReplaySession replaySession = replaySession(
            RECORDING_POSITION,
            FRAME_LENGTH,
            correlationId,
            mockReplayPub,
            mockControlSession);

        when(mockReplayPub.isClosed()).thenReturn(false);
        when(mockReplayPub.isConnected()).thenReturn(false);

        replaySession.doWork();

        assertEquals(replaySession.state(), ReplaySession.State.INIT);

        when(mockReplayPub.isConnected()).thenReturn(true);

        replaySession.doWork();
        assertEquals(replaySession.state(), ReplaySession.State.REPLAY);

        verify(mockControlSession).sendOkResponse(eq(correlationId), anyLong(), eq(proxy));

        final UnsafeBuffer termBuffer = new UnsafeBuffer(allocateDirectAligned(4096, 64));
        mockPublication(mockReplayPub, termBuffer);
        assertNotEquals(0, replaySession.doWork());
        assertThat(messageCounter, is(1));

        validateFrame(termBuffer, 0, FrameDescriptor.UNFRAGMENTED);

        assertTrue(replaySession.isDone());
        replaySession.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotReplayPartialUnalignedDataFromFile()
    {
        final long correlationId = 1L;
        new ReplaySession(
            RECORDING_POSITION + 1,
            FRAME_LENGTH,
            mockCatalog,
            mockControlSession,
            archiveDir,
            proxy,
            correlationId,
            epochClock,
            mockReplayPub,
            recordingSummary,
            position);
    }

    @Test
    public void shouldReplayFullDataFromFile()
    {
        final long length = 4 * FRAME_LENGTH;
        final long correlationId = 1L;

        final ReplaySession replaySession = replaySession(
            RECORDING_POSITION,
            length,
            correlationId,
            mockReplayPub,
            mockControlSession);

        when(mockReplayPub.isClosed()).thenReturn(false);
        when(mockReplayPub.isConnected()).thenReturn(false);

        replaySession.doWork();

        assertEquals(replaySession.state(), ReplaySession.State.INIT);

        when(mockReplayPub.isConnected()).thenReturn(true);

        replaySession.doWork();
        assertEquals(replaySession.state(), ReplaySession.State.REPLAY);

        verify(mockControlSession).sendOkResponse(eq(correlationId), anyLong(), eq(proxy));

        final UnsafeBuffer termBuffer = new UnsafeBuffer(allocateDirectAligned(4096, 64));
        mockPublication(mockReplayPub, termBuffer);

        assertNotEquals(0, replaySession.doWork());
        assertThat(messageCounter, is(4));

        validateFrame(termBuffer, 0, FrameDescriptor.UNFRAGMENTED);
        validateFrame(termBuffer, 1, FrameDescriptor.BEGIN_FRAG_FLAG);
        validateFrame(termBuffer, 2, FrameDescriptor.END_FRAG_FLAG);

        verify(mockReplayPub).appendPadding(FRAME_LENGTH - HEADER_LENGTH);
        assertTrue(replaySession.isDone());
        replaySession.close();
    }

    @Test
    public void shouldGiveUpIfPublishersAreNotConnectedAfterTimeout()
    {
        final long length = 1024L;
        final long correlationId = 1L;
        final ReplaySession replaySession = replaySession(
            RECORDING_POSITION,
            length,
            correlationId,
            mockReplayPub,
            mockControlSession);

        when(mockReplayPub.isClosed()).thenReturn(false);
        when(mockReplayPub.isConnected()).thenReturn(false);

        replaySession.doWork();

        when(epochClock.time()).thenReturn(ReplaySession.CONNECT_TIMEOUT_MS + TIME + 1L);
        replaySession.doWork();
        assertTrue(replaySession.isDone());
        replaySession.close();
    }

    @Test
    public void shouldReplayFromActiveRecording() throws IOException
    {
        final UnsafeBuffer termBuffer = new UnsafeBuffer(allocateDirectAligned(4096, 64));

        final int recordingId = RECORDING_ID + 1;
        recordingSummary.recordingId = recordingId;
        recordingSummary.stopPosition = NULL_POSITION;

        when(mockCatalog.stopPosition(recordingId)).thenReturn(START_POSITION + FRAME_LENGTH * 4);
        position.setOrdered(START_POSITION);

        final RecordingWriter writer = new RecordingWriter(
            recordingId, START_POSITION, JOIN_POSITION, TERM_BUFFER_LENGTH, context, ARCHIVE_DIR_CHANNEL, position);

        writer.init(INITIAL_TERM_OFFSET);

        when(epochClock.time()).thenReturn(TIME);

        final UnsafeBuffer buffer = new UnsafeBuffer(allocateDirectAligned(TERM_BUFFER_LENGTH, 64));

        final DataHeaderFlyweight headerFwt = new DataHeaderFlyweight();
        final Header header = new Header(INITIAL_TERM_ID, Integer.numberOfLeadingZeros(TERM_BUFFER_LENGTH));
        header.buffer(buffer);

        recordFragment(writer, buffer, headerFwt, header, 0, FrameDescriptor.UNFRAGMENTED, HDR_TYPE_DATA);
        recordFragment(writer, buffer, headerFwt, header, 1, FrameDescriptor.BEGIN_FRAG_FLAG, HDR_TYPE_DATA);

        final long length = 5 * FRAME_LENGTH;
        final long correlationId = 1L;

        final ReplaySession replaySession = replaySession(
            RECORDING_POSITION,
            length,
            correlationId,
            mockReplayPub,
            mockControlSession);

        when(mockReplayPub.isClosed()).thenReturn(false);
        when(mockReplayPub.isConnected()).thenReturn(false);

        replaySession.doWork();

        assertEquals(replaySession.state(), ReplaySession.State.INIT);

        when(mockReplayPub.isConnected()).thenReturn(true);

        replaySession.doWork();
        assertEquals(replaySession.state(), ReplaySession.State.REPLAY);

        verify(mockControlSession).sendOkResponse(eq(correlationId), anyLong(), eq(proxy));

        mockPublication(mockReplayPub, termBuffer);

        assertNotEquals(0, replaySession.doWork());
        assertThat(messageCounter, is(2));

        validateFrame(termBuffer, 0, FrameDescriptor.UNFRAGMENTED);
        validateFrame(termBuffer, 1, FrameDescriptor.BEGIN_FRAG_FLAG);

        assertEquals(0, replaySession.doWork());

        recordFragment(writer, buffer, headerFwt, header, 2, FrameDescriptor.END_FRAG_FLAG, HDR_TYPE_DATA);
        recordFragment(writer, buffer, headerFwt, header, 3, FrameDescriptor.UNFRAGMENTED, HDR_TYPE_PAD);

        writer.close();

        when(position.isClosed()).thenReturn(true);
        when(mockCatalog.stopPosition(recordingId)).thenReturn(START_POSITION + FRAME_LENGTH * 4);
        assertNotEquals(0, replaySession.doWork());

        validateFrame(termBuffer, 2, FrameDescriptor.END_FRAG_FLAG);
        verify(mockReplayPub).appendPadding(FRAME_LENGTH - HEADER_LENGTH);

        assertTrue(replaySession.isDone());
        replaySession.close();
    }

    private void recordFragment(
        final RecordingWriter recordingWriter,
        final UnsafeBuffer buffer,
        final DataHeaderFlyweight headerFlyweight,
        final Header header,
        final int message,
        final byte flags,
        final int type)
    {
        final int offset = INITIAL_TERM_OFFSET + message * FRAME_LENGTH;
        headerFlyweight.wrap(buffer, offset, HEADER_LENGTH);
        headerFlyweight
            .streamId(STREAM_ID)
            .sessionId(SESSION_ID)
            .termOffset(offset)
            .termId(INITIAL_TERM_ID)
            .reservedValue(message)
            .headerType(type)
            .flags(flags)
            .frameLength(FRAME_LENGTH);

        buffer.setMemory(
            offset + HEADER_LENGTH,
            FRAME_LENGTH - HEADER_LENGTH,
            (byte)message);

        header.offset(offset);

        recordingWriter.onBlock(buffer, offset, FRAME_LENGTH, SESSION_ID, INITIAL_TERM_ID);
    }

    private void mockPublication(final ExclusivePublication replay, final UnsafeBuffer termBuffer)
    {
        when(replay.tryClaim(anyInt(), any(ExclusiveBufferClaim.class))).then(
            (invocation) ->
            {
                final int claimedSize = invocation.getArgument(0);
                final ExclusiveBufferClaim buffer = invocation.getArgument(1);
                buffer.wrap(termBuffer, messageCounter * FRAME_LENGTH, claimedSize + HEADER_LENGTH);
                messageCounter++;

                return (long)claimedSize;
            });

        when(replay.appendPadding(anyInt())).then(
            (invocation) ->
            {
                final int claimedSize = invocation.getArgument(0);
                messageCounter++;

                return (long)claimedSize;
            });
    }

    @SuppressWarnings("SameParameterValue")
    private ReplaySession replaySession(
        final long recordingPosition,
        final long length,
        final long correlationId,
        final ExclusivePublication replay,
        final ControlSession control)
    {
        return new ReplaySession(
            recordingPosition,
            length,
            mockCatalog,
            control,
            archiveDir,
            proxy,
            correlationId,
            epochClock,
            replay,
            recordingSummary,
            position);
    }

    private static void validateFrame(final UnsafeBuffer buffer, final int message, final byte flags)
    {
        final int offset = message * FRAME_LENGTH;

        assertEquals(FRAME_LENGTH, FrameDescriptor.frameLength(buffer, offset));
        assertEquals(flags, FrameDescriptor.frameFlags(buffer, offset));
        assertEquals(message, buffer.getLong(offset + DataHeaderFlyweight.RESERVED_VALUE_OFFSET));
        assertEquals(message, buffer.getByte(offset + DataHeaderFlyweight.HEADER_LENGTH));
    }
}
