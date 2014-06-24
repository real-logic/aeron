/*
 * Copyright 2014 Real Logic Ltd.
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
package uk.co.real_logic.aeron.mediadriver;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;
import uk.co.real_logic.aeron.mediadriver.buffer.BufferRotator;
import uk.co.real_logic.aeron.util.AtomicArray;
import uk.co.real_logic.aeron.util.TimerWheel;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferDescriptor;
import uk.co.real_logic.aeron.util.event.EventLogger;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.util.BitUtil.align;

public class SenderTest
{
    public static final EventLogger LOGGER = new EventLogger(SenderTest.class);

    public static final long LOG_BUFFER_SIZE = (64 * 1024) + RingBufferDescriptor.TRAILER_LENGTH;
    public static final int MAX_FRAME_LENGTH = 1024;
    public static final long SESSION_ID = 1L;
    public static final long CHANNEL_ID = 2L;
    public static final long INITIAL_TERM_ID = 3L;
    public static final byte[] PAYLOAD = "Payload is here!".getBytes();

    public final byte[] HEADER = DataHeaderFlyweight.createDefaultHeader(SESSION_ID, CHANNEL_ID, INITIAL_TERM_ID);

    private final AtomicArray<Publication> publications = new AtomicArray<>();
    private final Sender sender = new Sender(new MediaDriver.Context().publications(publications));
    private final BufferRotator rotator =
        BufferAndFrameUtils.createTestRotator(LOG_BUFFER_SIZE, LogBufferDescriptor.STATE_BUFFER_LENGTH);

    private LogAppender[] logAppenders;
    private Publication publication;

    private final SenderControlStrategy spySenderControlStrategy = spy(new UnicastSenderControlStrategy());

    private long currentTimestamp;

    private final TimerWheel wheel = new TimerWheel(() -> currentTimestamp,
                                                    MediaDriver.MEDIA_CONDUCTOR_TICK_DURATION_US,
                                                    TimeUnit.MICROSECONDS,
                                                    MediaDriver.MEDIA_CONDUCTOR_TICKS_PER_WHEEL);

    private final Queue<ByteBuffer> receivedFrames = new ArrayDeque<>();

    private final UdpDestination destination = UdpDestination.parse("udp://localhost:40123");
    private final InetSocketAddress rcvAddress = destination.remoteData();

    private ControlFrameHandler mockControlFrameHandler;

    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();

    private Answer<Integer> saveByteBufferAnswer =
            (invocation) ->
            {
                Object args[] = invocation.getArguments();

                final ByteBuffer buffer = (ByteBuffer)args[0];

                final int size = buffer.limit() - buffer.position();
                receivedFrames.add(ByteBuffer.allocate(size).put(buffer));
                // we don't pass on the args, so don't reset buffer.position() back
                return size;
            };

    @Before
    public void setUp() throws Exception
    {
        currentTimestamp = 0;

        logAppenders = rotator.buffers()
                              .map((log) -> new LogAppender(log.logBuffer(), log.stateBuffer(),
                                                            HEADER, MAX_FRAME_LENGTH)).toArray(LogAppender[]::new);

        mockControlFrameHandler = mock(ControlFrameHandler.class);
        when(mockControlFrameHandler.destination()).thenReturn(destination);
        when(mockControlFrameHandler.sendTo(anyObject(), anyObject())).thenAnswer(saveByteBufferAnswer);

        publication = new Publication(mockControlFrameHandler, wheel, spySenderControlStrategy, rotator, SESSION_ID,
                                      CHANNEL_ID, INITIAL_TERM_ID, HEADER.length, MAX_FRAME_LENGTH);
        publications.add(publication);
    }

    @After
    public void tearDown() throws Exception
    {
        sender.close();
    }

    @Test
    public void shouldAddAndRemovePublication()
    {
        publications.remove(publication);
    }

    @Test
    public void shouldSendZeroLengthDataFrameOnChannelWhenTimeoutWithoutStatusMessage() throws Exception
    {
        currentTimestamp += TimeUnit.MILLISECONDS.toNanos(Publication.INITIAL_HEARTBEAT_TIMEOUT_MS) - 1;
        publications.forEach(0, Publication::heartbeatCheck);
        assertThat(receivedFrames.size(), is(0));
        currentTimestamp += 10;
        publications.forEach(0, Publication::heartbeatCheck);
        assertThat(receivedFrames.size(), is(1));

        dataHeader.wrap(receivedFrames.remove(), 0);
        assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH));
        assertThat(dataHeader.termId(), is(INITIAL_TERM_ID));
        assertThat(dataHeader.channelId(), is(CHANNEL_ID));
        assertThat(dataHeader.sessionId(), is(SESSION_ID));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(1)));
        assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(dataHeader.flags(), is(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
        assertThat(dataHeader.version(), is((short)HeaderFlyweight.CURRENT_VERSION));
    }

    @Test
    public void shouldSendMultipleZeroLengthDataFrameOnChannelWhenTimeoutWithoutStatusMessage() throws Exception
    {
        currentTimestamp += TimeUnit.MILLISECONDS.toNanos(Publication.INITIAL_HEARTBEAT_TIMEOUT_MS) - 1;
        publications.forEach(0, Publication::heartbeatCheck);
        assertThat(receivedFrames.size(), is(0));
        currentTimestamp += 10;
        publications.forEach(0, Publication::heartbeatCheck);
        assertThat(receivedFrames.size(), is(1));

        currentTimestamp += TimeUnit.MILLISECONDS.toNanos(Publication.INITIAL_HEARTBEAT_TIMEOUT_MS) - 1;
        publications.forEach(0, Publication::heartbeatCheck);
        currentTimestamp += 10;
        publications.forEach(0, Publication::heartbeatCheck);
        assertThat(receivedFrames.size(), is(2));
    }

    @Test
    public void shouldNotSendZeroLengthDataFrameAfterReceivingStatusMessage() throws Exception
    {
        currentTimestamp += TimeUnit.MILLISECONDS.toNanos(Publication.HEARTBEAT_TIMEOUT_MS);
        publication.onStatusMessage(INITIAL_TERM_ID, 0, 0, rcvAddress);
        publications.forEach(0, Publication::heartbeatCheck);
        assertThat(receivedFrames.size(), is(0));
    }

    @Test
    public void shouldBeAbleToSendOnChannel() throws Exception
    {
        publication.onStatusMessage(INITIAL_TERM_ID, 0, align(PAYLOAD.length, FrameDescriptor.FRAME_ALIGNMENT), rcvAddress);

        final AtomicBuffer buffer = new AtomicBuffer(ByteBuffer.allocate(PAYLOAD.length));
        buffer.putBytes(0, PAYLOAD);

        assertTrue(logAppenders[0].append(buffer, 0, PAYLOAD.length));
        sender.doWork();

        assertThat(receivedFrames.size(), is(1));
        dataHeader.wrap(receivedFrames.remove(), 0);
        assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH + PAYLOAD.length));
        assertThat(dataHeader.termId(), is(INITIAL_TERM_ID));
        assertThat(dataHeader.channelId(), is(CHANNEL_ID));
        assertThat(dataHeader.sessionId(), is(SESSION_ID));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(1)));
        assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(dataHeader.flags(), is(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
        assertThat(dataHeader.version(), is((short)HeaderFlyweight.CURRENT_VERSION));
    }

    @Test
    public void shouldBeAbleToSendOnChannelTwice() throws Exception
    {
        publication.onStatusMessage(INITIAL_TERM_ID, 0, (2 * align(PAYLOAD.length, FrameDescriptor.FRAME_ALIGNMENT)),
                                    rcvAddress);

        final AtomicBuffer buffer = new AtomicBuffer(ByteBuffer.allocate(PAYLOAD.length));
        buffer.putBytes(0, PAYLOAD);

        assertTrue(logAppenders[0].append(buffer, 0, PAYLOAD.length));
        sender.doWork();
        assertTrue(logAppenders[0].append(buffer, 0, PAYLOAD.length));
        sender.doWork();

        assertThat(receivedFrames.size(), is(2));

        dataHeader.wrap(receivedFrames.remove(), 0);
        assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH + PAYLOAD.length));
        assertThat(dataHeader.termId(), is(INITIAL_TERM_ID));
        assertThat(dataHeader.channelId(), is(CHANNEL_ID));
        assertThat(dataHeader.sessionId(), is(SESSION_ID));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(1)));
        assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(dataHeader.flags(), is(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
        assertThat(dataHeader.version(), is((short)HeaderFlyweight.CURRENT_VERSION));

        dataHeader.wrap(receivedFrames.remove(), 0);
        assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH + PAYLOAD.length));
        assertThat(dataHeader.termId(), is(INITIAL_TERM_ID));
        assertThat(dataHeader.channelId(), is(CHANNEL_ID));
        assertThat(dataHeader.sessionId(), is(SESSION_ID));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(2)));
        assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(dataHeader.flags(), is(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
        assertThat(dataHeader.version(), is((short)HeaderFlyweight.CURRENT_VERSION));
    }

    @Test
    public void shouldBeAbleToSendOnChannelTwiceAsBatch() throws Exception
    {
        publication.onStatusMessage(INITIAL_TERM_ID, 0, (2 * align(PAYLOAD.length, FrameDescriptor.FRAME_ALIGNMENT)),
                                    rcvAddress);

        final AtomicBuffer buffer = new AtomicBuffer(ByteBuffer.allocate(PAYLOAD.length));
        buffer.putBytes(0, PAYLOAD);

        assertTrue(logAppenders[0].append(buffer, 0, PAYLOAD.length));
        assertTrue(logAppenders[0].append(buffer, 0, PAYLOAD.length));
        sender.doWork();

        assertThat(receivedFrames.size(), is(1));
        final ByteBuffer frame = receivedFrames.remove();

        dataHeader.wrap(frame, 0);
        assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH + PAYLOAD.length));
        assertThat(dataHeader.termId(), is(INITIAL_TERM_ID));
        assertThat(dataHeader.channelId(), is(CHANNEL_ID));
        assertThat(dataHeader.sessionId(), is(SESSION_ID));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(1)));
        assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(dataHeader.flags(), is(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
        assertThat(dataHeader.version(), is((short)HeaderFlyweight.CURRENT_VERSION));

        dataHeader.wrap(frame, (int)offsetOfMessage(2));
        assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH + PAYLOAD.length));
        assertThat(dataHeader.termId(), is(INITIAL_TERM_ID));
        assertThat(dataHeader.channelId(), is(CHANNEL_ID));
        assertThat(dataHeader.sessionId(), is(SESSION_ID));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(2)));
        assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(dataHeader.flags(), is(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
        assertThat(dataHeader.version(), is((short)HeaderFlyweight.CURRENT_VERSION));
    }

    @Test
    public void shouldNotSendUntilStatusMessageReceived() throws Exception
    {
        final AtomicBuffer buffer = new AtomicBuffer(ByteBuffer.allocate(PAYLOAD.length));
        buffer.putBytes(0, PAYLOAD);
        assertTrue(logAppenders[0].append(buffer, 0, PAYLOAD.length));

        sender.doWork();
        assertThat(receivedFrames.size(), is(0));

        publication.onStatusMessage(INITIAL_TERM_ID, 0, align(PAYLOAD.length, FrameDescriptor.FRAME_ALIGNMENT), rcvAddress);
        sender.doWork();

        assertThat(receivedFrames.size(), is(1));
        dataHeader.wrap(receivedFrames.remove(), 0);
        assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH + PAYLOAD.length));
        assertThat(dataHeader.termId(), is(INITIAL_TERM_ID));
        assertThat(dataHeader.channelId(), is(CHANNEL_ID));
        assertThat(dataHeader.sessionId(), is(SESSION_ID));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(1)));
        assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(dataHeader.flags(), is(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
        assertThat(dataHeader.version(), is((short)HeaderFlyweight.CURRENT_VERSION));
    }

    @Test
    public void shouldNotBeAbleToSendAfterUsingUpYourWindow() throws Exception
    {
        final AtomicBuffer buffer = new AtomicBuffer(ByteBuffer.allocate(PAYLOAD.length));
        buffer.putBytes(0, PAYLOAD);
        assertTrue(logAppenders[0].append(buffer, 0, PAYLOAD.length));
        publication.onStatusMessage(INITIAL_TERM_ID, 0, align(PAYLOAD.length, FrameDescriptor.FRAME_ALIGNMENT), rcvAddress);

        sender.doWork();

        assertThat(receivedFrames.size(), is(1));
        dataHeader.wrap(receivedFrames.remove(), 0);
        assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH + PAYLOAD.length));
        assertThat(dataHeader.termId(), is(INITIAL_TERM_ID));
        assertThat(dataHeader.channelId(), is(CHANNEL_ID));
        assertThat(dataHeader.sessionId(), is(SESSION_ID));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(1)));
        assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(dataHeader.flags(), is(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
        assertThat(dataHeader.version(), is((short)HeaderFlyweight.CURRENT_VERSION));

        assertTrue(logAppenders[0].append(buffer, 0, PAYLOAD.length));
        sender.doWork();

        assertThat(receivedFrames.size(), is(0));
    }

    @Test
    public void shouldSend0LengthDataFrameAsHeartbeatWhenIdle() throws Exception
    {
        publication.onStatusMessage(INITIAL_TERM_ID, 0, align(PAYLOAD.length, FrameDescriptor.FRAME_ALIGNMENT), rcvAddress);

        final AtomicBuffer buffer = new AtomicBuffer(ByteBuffer.allocate(PAYLOAD.length));
        buffer.putBytes(0, PAYLOAD);

        assertTrue(logAppenders[0].append(buffer, 0, PAYLOAD.length));
        sender.doWork();

        assertThat(receivedFrames.size(), is(1));  // should send now
        receivedFrames.remove();                   // skip data frame

        currentTimestamp += TimeUnit.MILLISECONDS.toNanos(Publication.HEARTBEAT_TIMEOUT_MS) - 1;
        publications.forEach(0, Publication::heartbeatCheck);
        assertThat(receivedFrames.size(), is(0));  // should not send yet
        currentTimestamp += 10;
        publications.forEach(0, Publication::heartbeatCheck);
        assertThat(receivedFrames.size(), greaterThanOrEqualTo(1));  // should send now

        dataHeader.wrap(new AtomicBuffer(receivedFrames.remove()), 0);
        assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(2)));
    }

    @Test
    public void shouldSendMultiple0LengthDataFrameAsHeartbeatsWhenIdle()
    {
        publication.onStatusMessage(INITIAL_TERM_ID, 0, align(PAYLOAD.length, FrameDescriptor.FRAME_ALIGNMENT), rcvAddress);

        final AtomicBuffer buffer = new AtomicBuffer(ByteBuffer.allocate(PAYLOAD.length));
        buffer.putBytes(0, PAYLOAD);

        assertTrue(logAppenders[0].append(buffer, 0, PAYLOAD.length));
        sender.doWork();

        assertThat(receivedFrames.size(), is(1));  // should send now
        receivedFrames.remove();                   // skip data frame

        currentTimestamp += TimeUnit.MILLISECONDS.toNanos(Publication.HEARTBEAT_TIMEOUT_MS) - 1;
        publications.forEach(0, Publication::heartbeatCheck);
        assertThat(receivedFrames.size(), is(0));  // should not send yet
        currentTimestamp += 10;
        publications.forEach(0, Publication::heartbeatCheck);
        assertThat(receivedFrames.size(), greaterThanOrEqualTo(1));  // should send now

        dataHeader.wrap(new AtomicBuffer(receivedFrames.remove()), 0);
        assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(2)));

        currentTimestamp += TimeUnit.MILLISECONDS.toNanos(Publication.HEARTBEAT_TIMEOUT_MS) - 1;
        publications.forEach(0, Publication::heartbeatCheck);
        assertThat(receivedFrames.size(), is(0));  // should not send yet
        currentTimestamp += 10;
        publications.forEach(0, Publication::heartbeatCheck);
        assertThat(receivedFrames.size(), greaterThanOrEqualTo(1));  // should send now

        dataHeader.wrap(new AtomicBuffer(receivedFrames.remove()), 0);
        assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(2)));
    }

    private long offsetOfMessage(final int num)
    {
        return (num - 1) * align(PAYLOAD.length, FrameDescriptor.FRAME_ALIGNMENT);
    }
}
