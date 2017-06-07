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
import io.aeron.archiver.codecs.ControlResponseCode;
import io.aeron.logbuffer.*;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.*;
import org.agrona.concurrent.*;
import org.junit.*;
import org.mockito.Mockito;

import java.io.*;

import static io.aeron.archiver.TestUtil.makeTempDir;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_DATA;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_PAD;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class ReplaySessionTest
{
    private static final String REPLAY_CHANNEL = "aeron:ipc";
    private static final int REPLAY_STREAM_ID = 101;
    private static final int RECORDING_ID = 0;
    private static final int TERM_BUFFER_LENGTH = 4096 * 4;
    private static final int INITIAL_TERM_ID = 8231773;
    private static final int INITIAL_TERM_OFFSET = 1024;
    private static final long JOINING_POSITION = INITIAL_TERM_OFFSET;
    private static final long RECORDING_POSITION = INITIAL_TERM_OFFSET;
    private static final int MTU_LENGTH = 4096;
    private static final long TIME = 0;
    private static final int REPLAY_SESSION_ID = 0;
    private static final int FRAME_LENGTH = 1024;
    private File archiveDir;

    private int messageCounter = 0;
    private ControlSessionProxy proxy;
    private EpochClock epochClock;

    @Before
    public void before() throws Exception
    {
        archiveDir = makeTempDir();
        proxy = Mockito.mock(ControlSessionProxy.class);
        epochClock = mock(EpochClock.class);
        try (RecordingWriter writer = new RecordingWriter(new RecordingWriter.RecordingContext()
            .archiveDir(archiveDir)
            .epochClock(epochClock)
            .forceWrites(true),
            RECORDING_ID,
            TERM_BUFFER_LENGTH,
            MTU_LENGTH,
            INITIAL_TERM_ID,
            JOINING_POSITION,
            1,
            1,
            "channel",
            "sourceIdentity"))
        {
            when(epochClock.time()).thenReturn(TIME);

            final UnsafeBuffer buffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(TERM_BUFFER_LENGTH, 64));

            final DataHeaderFlyweight headerFwt = new DataHeaderFlyweight();
            final Header header = new Header(INITIAL_TERM_ID, Integer.numberOfLeadingZeros(TERM_BUFFER_LENGTH));
            header.buffer(buffer);

            recordFragment(writer, buffer, headerFwt, header, 0, FrameDescriptor.UNFRAGMENTED, HDR_TYPE_DATA);
            recordFragment(writer, buffer, headerFwt, header, 1, FrameDescriptor.BEGIN_FRAG_FLAG, HDR_TYPE_DATA);
            recordFragment(writer, buffer, headerFwt, header, 2, FrameDescriptor.END_FRAG_FLAG, HDR_TYPE_DATA);
            recordFragment(writer, buffer, headerFwt, header, 3, FrameDescriptor.UNFRAGMENTED, HDR_TYPE_PAD);
        }
    }

    @After
    public void after()
    {
        IoUtil.delete(archiveDir, false);
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

        recordingWriter.writeFragment(buffer, header);
    }

    @Test
    public void verifyRecordingFile() throws IOException
    {
        // Verify file reader matches file writer
        try (RecordingFragmentReader reader = new RecordingFragmentReader(RECORDING_ID, archiveDir))
        {
            int polled = reader.controlledPoll(
                (buffer, offset, length) ->
                {
                    final int frameOffset = offset - DataHeaderFlyweight.HEADER_LENGTH;
                    assertEquals(offset, INITIAL_TERM_OFFSET + HEADER_LENGTH);
                    assertEquals(length, FRAME_LENGTH - HEADER_LENGTH);
                    assertEquals(FrameDescriptor.frameType(buffer, frameOffset), HDR_TYPE_DATA);
                    assertEquals(FrameDescriptor.frameFlags(buffer, frameOffset), FrameDescriptor.UNFRAGMENTED);

                    return true;
                },
                1);

            assertEquals(1, polled);

            polled = reader.controlledPoll(
                (buffer, offset, length) ->
                {
                    final int frameOffset = offset - DataHeaderFlyweight.HEADER_LENGTH;
                    assertEquals(offset, INITIAL_TERM_OFFSET + FRAME_LENGTH + HEADER_LENGTH);
                    assertEquals(length, FRAME_LENGTH - HEADER_LENGTH);
                    assertEquals(FrameDescriptor.frameType(buffer, frameOffset), HDR_TYPE_DATA);
                    assertEquals(FrameDescriptor.frameFlags(buffer, frameOffset), FrameDescriptor.BEGIN_FRAG_FLAG);

                    return true;
                },
                1);

            assertEquals(1, polled);

            polled = reader.controlledPoll(
                (buffer, offset, length) ->
                {
                    final int frameOffset = offset - DataHeaderFlyweight.HEADER_LENGTH;
                    assertEquals(offset, INITIAL_TERM_OFFSET + 2 * FRAME_LENGTH + HEADER_LENGTH);
                    assertEquals(length, FRAME_LENGTH - HEADER_LENGTH);
                    assertEquals(FrameDescriptor.frameType(buffer, frameOffset), HDR_TYPE_DATA);
                    assertEquals(FrameDescriptor.frameFlags(buffer, frameOffset), FrameDescriptor.END_FRAG_FLAG);

                    return true;
                },
                1);

            assertEquals(1, polled);

            polled = reader.controlledPoll(
                (buffer, offset, length) ->
                {
                    final int frameOffset = offset - DataHeaderFlyweight.HEADER_LENGTH;
                    assertEquals(offset, INITIAL_TERM_OFFSET + 3 * FRAME_LENGTH + HEADER_LENGTH);
                    assertEquals(length, FRAME_LENGTH - HEADER_LENGTH);
                    assertEquals(FrameDescriptor.frameType(buffer, frameOffset), HDR_TYPE_PAD);
                    assertEquals(FrameDescriptor.frameFlags(buffer, frameOffset), FrameDescriptor.UNFRAGMENTED);

                    return true;
                },
                1);

            assertEquals(1, polled);
        }
    }

    @Test
    public void shouldReplayPartialDataFromFile()
    {
        final long correlationId = 1L;
        final ExclusivePublication replay = Mockito.mock(ExclusivePublication.class);
        final Publication control = Mockito.mock(Publication.class);
        final Replayer conductor = Mockito.mock(Replayer.class);

        final ReplaySession replaySession = replaySession(
            RECORDING_ID, RECORDING_POSITION, FRAME_LENGTH, correlationId, replay, control, conductor);

        when(control.isClosed()).thenReturn(false);
        when(control.isConnected()).thenReturn(true);

        when(replay.isClosed()).thenReturn(false);
        when(replay.isConnected()).thenReturn(false);

        replaySession.doWork();

        assertEquals(replaySession.state(), ReplaySession.State.INIT);

        when(replay.isConnected()).thenReturn(true);
        when(control.isConnected()).thenReturn(true);

        replaySession.doWork();
        assertEquals(replaySession.state(), ReplaySession.State.REPLAY);

        verify(proxy, times(1)).sendOkResponse(control, correlationId);
        verify(conductor).newReplayPublication(
            REPLAY_CHANNEL,
            REPLAY_STREAM_ID,
            RECORDING_POSITION,
            MTU_LENGTH,
            INITIAL_TERM_ID,
            TERM_BUFFER_LENGTH);

        final UnsafeBuffer termBuffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(4096, 64));
        mockPublication(replay, termBuffer);
        assertNotEquals(0, replaySession.doWork());
        assertThat(messageCounter, is(1));

        validateFrame(termBuffer, 0, FrameDescriptor.UNFRAGMENTED);

        assertFalse(replaySession.isDone());

        when(epochClock.time()).thenReturn(ReplaySession.LINGER_LENGTH_MS + TIME + 1L);
        replaySession.doWork();
        assertTrue(replaySession.isDone());
    }

    @Test
    public void shouldReplayPartialUnalignedDataFromFile()
    {
        final long correlationId = 1L;
        final ExclusivePublication replayPublication = Mockito.mock(ExclusivePublication.class);
        final Publication control = Mockito.mock(Publication.class);

        final Replayer conductor = Mockito.mock(Replayer.class);

        final ReplaySession replaySession = replaySession(
            RECORDING_ID, RECORDING_POSITION + 1, FRAME_LENGTH, correlationId, replayPublication, control, conductor);

        when(conductor.newReplayPublication(
            eq(REPLAY_CHANNEL),
            eq(REPLAY_STREAM_ID),
            eq(RECORDING_POSITION + FRAME_LENGTH),
            eq(MTU_LENGTH),
            eq(INITIAL_TERM_ID),
            eq(TERM_BUFFER_LENGTH))).thenReturn(replayPublication);

        when(replayPublication.isClosed()).thenReturn(false);
        when(control.isClosed()).thenReturn(false);

        when(replayPublication.isConnected()).thenReturn(true);
        when(control.isConnected()).thenReturn(true);

        replaySession.doWork();

        replaySession.doWork();
        assertEquals(replaySession.state(), ReplaySession.State.REPLAY);

        verify(proxy, times(1)).sendOkResponse(control, correlationId);
        verify(conductor).newReplayPublication(
            REPLAY_CHANNEL,
            REPLAY_STREAM_ID,
            RECORDING_POSITION + FRAME_LENGTH,
            MTU_LENGTH,
            INITIAL_TERM_ID,
            TERM_BUFFER_LENGTH);

        final UnsafeBuffer termBuffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(4096, 64));
        mockPublication(replayPublication, termBuffer);

        assertNotEquals(0, replaySession.doWork());
        assertThat(messageCounter, is(1));

        assertEquals(FRAME_LENGTH, termBuffer.getInt(DataHeaderFlyweight.FRAME_LENGTH_FIELD_OFFSET));
        assertEquals(FrameDescriptor.BEGIN_FRAG_FLAG, termBuffer.getByte(DataHeaderFlyweight.FLAGS_FIELD_OFFSET));
        assertEquals(1, termBuffer.getLong(DataHeaderFlyweight.RESERVED_VALUE_OFFSET));
        assertEquals(1, termBuffer.getByte(DataHeaderFlyweight.HEADER_LENGTH));

        final int expectedFrameLength = 1024;
        assertEquals(expectedFrameLength, termBuffer.getInt(0));
        assertFalse(replaySession.isDone());

        when(epochClock.time()).thenReturn(ReplaySession.LINGER_LENGTH_MS + TIME + 1L);
        replaySession.doWork();
        assertTrue(replaySession.isDone());
    }

    @Test
    public void shouldReplayFullDataFromFile()
    {
        final long length = 4 * FRAME_LENGTH;
        final long correlationId = 1L;
        final ExclusivePublication replayPublication = Mockito.mock(ExclusivePublication.class);
        final Publication control = Mockito.mock(Publication.class);

        final Replayer conductor = Mockito.mock(Replayer.class);

        final ReplaySession replaySession = replaySession(
            RECORDING_ID, RECORDING_POSITION, length, correlationId, replayPublication, control, conductor);

        when(control.isClosed()).thenReturn(false);
        when(control.isConnected()).thenReturn(true);

        when(replayPublication.isClosed()).thenReturn(false);
        when(replayPublication.isConnected()).thenReturn(false);

        replaySession.doWork();

        assertEquals(replaySession.state(), ReplaySession.State.INIT);

        when(replayPublication.isConnected()).thenReturn(true);
        when(control.isConnected()).thenReturn(true);

        replaySession.doWork();
        assertEquals(replaySession.state(), ReplaySession.State.REPLAY);

        verify(proxy, times(1)).sendOkResponse(control, correlationId);
        verify(conductor).newReplayPublication(
            REPLAY_CHANNEL,
            REPLAY_STREAM_ID,
            RECORDING_POSITION,
            MTU_LENGTH,
            INITIAL_TERM_ID,
            TERM_BUFFER_LENGTH);

        final UnsafeBuffer termBuffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(4096, 64));
        mockPublication(replayPublication, termBuffer);

        assertNotEquals(0, replaySession.doWork());
        assertThat(messageCounter, is(4));

        validateFrame(termBuffer, 0, FrameDescriptor.UNFRAGMENTED);
        validateFrame(termBuffer, 1, FrameDescriptor.BEGIN_FRAG_FLAG);
        validateFrame(termBuffer, 2, FrameDescriptor.END_FRAG_FLAG);

        verify(replayPublication).appendPadding(FRAME_LENGTH - HEADER_LENGTH);
        assertFalse(replaySession.isDone());

        when(epochClock.time()).thenReturn(ReplaySession.LINGER_LENGTH_MS + TIME + 1L);
        replaySession.doWork();
        assertTrue(replaySession.isDone());
    }

    @Test
    public void shouldAbortReplay()
    {
        final long length = 1024L;
        final long correlationId = 1L;
        final ExclusivePublication replayPublication = Mockito.mock(ExclusivePublication.class);
        final Publication control = Mockito.mock(Publication.class);

        final Replayer conductor = Mockito.mock(Replayer.class);

        final ReplaySession replaySession = replaySession(
            RECORDING_ID, RECORDING_POSITION, length, correlationId, replayPublication, control, conductor);

        when(control.isClosed()).thenReturn(false);
        when(replayPublication.isClosed()).thenReturn(false);
        when(replayPublication.isConnected()).thenReturn(true);
        when(control.isConnected()).thenReturn(true);

        replaySession.doWork();

        verify(proxy, times(1)).sendOkResponse(control, correlationId);
        assertEquals(replaySession.state(), ReplaySession.State.REPLAY);

        replaySession.abort();
        assertEquals(replaySession.state(), ReplaySession.State.INACTIVE);
        verify(proxy, times(1))
            .sendReplayAborted(control, correlationId, REPLAY_SESSION_ID, replayPublication.position());

        replaySession.doWork();
        assertTrue(replaySession.isDone());
    }

    @Test
    public void shouldFailToReplayDataForNonExistentStream()
    {
        final long length = 1024L;
        final long correlationId = 1L;
        final ExclusivePublication replayPublication = Mockito.mock(ExclusivePublication.class);
        final Publication control = Mockito.mock(Publication.class);

        final Replayer conductor = Mockito.mock(Replayer.class);
        final ReplaySession replaySession = replaySession(
            RECORDING_ID + 1, RECORDING_POSITION, length, correlationId, replayPublication, control, conductor);

        when(replayPublication.isClosed()).thenReturn(false);
        when(control.isClosed()).thenReturn(false);

        when(replayPublication.isConnected()).thenReturn(true);
        when(control.isConnected()).thenReturn(true);

        assertEquals(1, replaySession.doWork());

        verify(proxy, times(1))
            .sendError(eq(control), eq(ControlResponseCode.ERROR), notNull(), eq(correlationId));

        assertTrue(replaySession.isDone());
    }

    @Test
    public void shouldGiveUpIfPublishersAreNotConnectedAfterOneSecond()
    {
        final long length = 1024L;
        final long correlationId = 1L;
        final ExclusivePublication replay = Mockito.mock(ExclusivePublication.class);
        final Publication control = Mockito.mock(Publication.class);

        final Replayer conductor = Mockito.mock(Replayer.class);
        final ReplaySession replaySession = replaySession(
            RECORDING_ID, RECORDING_POSITION, length, correlationId, replay, control, conductor);

        when(replay.isClosed()).thenReturn(false);
        when(control.isClosed()).thenReturn(false);
        when(replay.isConnected()).thenReturn(false);

        replaySession.doWork();

        when(epochClock.time()).thenReturn(ReplaySession.LINGER_LENGTH_MS + TIME + 1L);
        replaySession.doWork();
        assertTrue(replaySession.isDone());
    }

    private ReplaySession replaySession(
        final long recordingId,
        final long recordingPosition, final long length,
        final long correlationId,
        final ExclusivePublication replay,
        final Publication control,
        final Replayer conductor)
    {
        when(conductor.newReplayPublication(
            eq(REPLAY_CHANNEL),
            eq(REPLAY_STREAM_ID),
            eq(recordingPosition),
            eq(MTU_LENGTH),
            eq(INITIAL_TERM_ID),
            eq(TERM_BUFFER_LENGTH))).thenReturn(replay);

        return new ReplaySession(
            recordingId,
            recordingPosition,
            length,
            conductor,
            control,
            archiveDir,
            proxy,
            REPLAY_SESSION_ID,
            correlationId,
            epochClock,
            REPLAY_CHANNEL,
            REPLAY_STREAM_ID);
    }

    private void validateFrame(
        final UnsafeBuffer buffer,
        final int message,
        final byte flags)
    {
        final int offset = message * FRAME_LENGTH;

        assertEquals(FRAME_LENGTH, FrameDescriptor.frameLength(buffer, offset));
        assertEquals(flags, FrameDescriptor.frameFlags(buffer, offset));
        assertEquals(message, buffer.getLong(offset + DataHeaderFlyweight.RESERVED_VALUE_OFFSET));
        assertEquals(message, buffer.getByte(offset + DataHeaderFlyweight.HEADER_LENGTH));
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
}
