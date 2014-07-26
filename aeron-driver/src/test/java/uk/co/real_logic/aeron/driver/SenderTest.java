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
package uk.co.real_logic.aeron.driver;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;
import uk.co.real_logic.aeron.common.TimerWheel;
import uk.co.real_logic.aeron.common.concurrent.AtomicArray;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogAppender;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor;
import uk.co.real_logic.aeron.common.event.EventLogger;
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.common.protocol.HeaderFlyweight;
import uk.co.real_logic.aeron.common.status.BufferPositionReporter;
import uk.co.real_logic.aeron.driver.buffer.TermBuffers;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.common.BitUtil.align;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogAppender.AppendStatus.SUCCESS;

public class SenderTest
{
    public static final long LOG_BUFFER_SIZE = LogBufferDescriptor.MIN_LOG_SIZE;
    public static final int MAX_FRAME_LENGTH = 1024;
    public static final int SESSION_ID = 1;
    public static final int STREAM_ID = 2;
    public static final int INITIAL_TERM_ID = 3;
    public static final byte[] PAYLOAD = "Payload is here!".getBytes();

    public final byte[] HEADER = DataHeaderFlyweight.createDefaultHeader(SESSION_ID, STREAM_ID, INITIAL_TERM_ID);
    public final int ALIGNED_FRAME_LENGTH = align(HEADER.length + PAYLOAD.length, FRAME_ALIGNMENT);

    private final EventLogger mockLogger = mock(EventLogger.class);

    private final AtomicArray<DriverPublication> publications = new AtomicArray<>();
    private final Sender sender = new Sender(new MediaDriver.Context().publications(publications).senderLogger(mockLogger));

    private final TermBuffers termBuffers =
        BufferAndFrameHelper.newTestTermBuffers(LOG_BUFFER_SIZE, LogBufferDescriptor.STATE_BUFFER_LENGTH);

    private LogAppender[] logAppenders;
    private DriverPublication publication;

    private final SenderControlStrategy spySenderControlStrategy = spy(new UnicastSenderControlStrategy());

    private long currentTimestamp;

    private final TimerWheel wheel = new TimerWheel(() -> currentTimestamp,
                                                    MediaDriver.CONDUCTOR_TICK_DURATION_US,
                                                    TimeUnit.MICROSECONDS,
                                                    MediaDriver.CONDUCTOR_TICKS_PER_WHEEL);

    private final Queue<ByteBuffer> receivedFrames = new ArrayDeque<>();

    private final UdpChannel udpChannel = UdpChannel.parse("udp://localhost:40123");
    private final InetSocketAddress rcvAddress = udpChannel.remoteData();
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

        logAppenders =
            termBuffers.stream()
                       .map((log) -> new LogAppender(log.logBuffer(), log.stateBuffer(), HEADER, MAX_FRAME_LENGTH))
                       .toArray(LogAppender[]::new);

        final ChannelSendEndpoint mockChannelSendEndpoint = mock(ChannelSendEndpoint.class);
        when(mockChannelSendEndpoint.udpChannel()).thenReturn(udpChannel);
        when(mockChannelSendEndpoint.sendTo(anyObject(), anyObject())).thenAnswer(saveByteBufferAnswer);

        publication = new DriverPublication(mockChannelSendEndpoint,
                                            wheel,
                                            spySenderControlStrategy,
                                            termBuffers,
                                            mock(BufferPositionReporter.class),
                                            SESSION_ID,
            STREAM_ID,
                                            INITIAL_TERM_ID,
                                            HEADER.length,
                                            MAX_FRAME_LENGTH,
                                            mockLogger);
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
        EventLogger.logInvocation();

        publications.remove(publication);
    }

    @Test
    public void shouldSendZeroLengthDataFrameOnChannelWhenTimeoutWithoutStatusMessage() throws Exception
    {
        EventLogger.logInvocation();

        currentTimestamp += TimeUnit.MILLISECONDS.toNanos(DriverPublication.INITIAL_HEARTBEAT_TIMEOUT_MS) - 1;
        publications.forEach(DriverPublication::heartbeatCheck);
        assertThat(receivedFrames.size(), is(0));
        currentTimestamp += 10;
        publications.forEach(DriverPublication::heartbeatCheck);
        assertThat(receivedFrames.size(), is(1));

        dataHeader.wrap(receivedFrames.remove(), 0);
        assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH));
        assertThat(dataHeader.termId(), is(INITIAL_TERM_ID));
        assertThat(dataHeader.streamId(), is(STREAM_ID));
        assertThat(dataHeader.sessionId(), is(SESSION_ID));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(1)));
        assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(dataHeader.flags(), is(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
        assertThat(dataHeader.version(), is((short)HeaderFlyweight.CURRENT_VERSION));
    }

    @Test
    public void shouldSendMultipleZeroLengthDataFrameOnChannelWhenTimeoutWithoutStatusMessage() throws Exception
    {
        EventLogger.logInvocation();

        currentTimestamp += TimeUnit.MILLISECONDS.toNanos(DriverPublication.INITIAL_HEARTBEAT_TIMEOUT_MS) - 1;
        publications.forEach(DriverPublication::heartbeatCheck);
        assertThat(receivedFrames.size(), is(0));
        currentTimestamp += 10;
        publications.forEach(DriverPublication::heartbeatCheck);
        assertThat(receivedFrames.size(), is(1));

        currentTimestamp += TimeUnit.MILLISECONDS.toNanos(DriverPublication.INITIAL_HEARTBEAT_TIMEOUT_MS) - 1;
        publications.forEach(DriverPublication::heartbeatCheck);
        currentTimestamp += 10;
        publications.forEach(DriverPublication::heartbeatCheck);

        assertThat(receivedFrames.size(), is(2));
    }

    @Test
    public void shouldNotSendZeroLengthDataFrameAfterReceivingStatusMessage() throws Exception
    {
        EventLogger.logInvocation();

        currentTimestamp += TimeUnit.MILLISECONDS.toNanos(DriverPublication.HEARTBEAT_TIMEOUT_MS);
        publication.onStatusMessage(INITIAL_TERM_ID, 0, 0, rcvAddress);
        publications.forEach(DriverPublication::heartbeatCheck);

        assertThat(receivedFrames.size(), is(0));
    }

    @Test
    public void shouldBeAbleToSendOnChannel() throws Exception
    {
        EventLogger.logInvocation();

        publication.onStatusMessage(INITIAL_TERM_ID, 0, ALIGNED_FRAME_LENGTH, rcvAddress);

        final AtomicBuffer buffer = new AtomicBuffer(ByteBuffer.allocate(PAYLOAD.length));
        buffer.putBytes(0, PAYLOAD);

        assertThat(logAppenders[0].append(buffer, 0, PAYLOAD.length), is(SUCCESS));
        sender.doWork();

        assertThat(receivedFrames.size(), is(1));

        dataHeader.wrap(receivedFrames.remove(), 0);

        assertThat(dataHeader.frameLength(), is(ALIGNED_FRAME_LENGTH));
        assertThat(dataHeader.termId(), is(INITIAL_TERM_ID));
        assertThat(dataHeader.streamId(), is(STREAM_ID));
        assertThat(dataHeader.sessionId(), is(SESSION_ID));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(1)));
        assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(dataHeader.flags(), is(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
        assertThat(dataHeader.version(), is((short)HeaderFlyweight.CURRENT_VERSION));
    }

    @Test
    public void shouldBeAbleToSendOnChannelTwice() throws Exception
    {
        EventLogger.logInvocation();

        publication.onStatusMessage(INITIAL_TERM_ID, 0, (2 * ALIGNED_FRAME_LENGTH), rcvAddress);

        final AtomicBuffer buffer = new AtomicBuffer(ByteBuffer.allocate(PAYLOAD.length));
        buffer.putBytes(0, PAYLOAD);

        assertThat(logAppenders[0].append(buffer, 0, PAYLOAD.length), is(SUCCESS));
        sender.doWork();
        assertThat(logAppenders[0].append(buffer, 0, PAYLOAD.length), is(SUCCESS));
        sender.doWork();

        assertThat(receivedFrames.size(), is(2));

        dataHeader.wrap(receivedFrames.remove(), 0);

        assertThat(dataHeader.frameLength(), is(ALIGNED_FRAME_LENGTH));
        assertThat(dataHeader.termId(), is(INITIAL_TERM_ID));
        assertThat(dataHeader.streamId(), is(STREAM_ID));
        assertThat(dataHeader.sessionId(), is(SESSION_ID));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(1)));
        assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(dataHeader.flags(), is(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
        assertThat(dataHeader.version(), is((short)HeaderFlyweight.CURRENT_VERSION));

        dataHeader.wrap(receivedFrames.remove(), 0);
        assertThat(dataHeader.frameLength(), is(ALIGNED_FRAME_LENGTH));
        assertThat(dataHeader.termId(), is(INITIAL_TERM_ID));
        assertThat(dataHeader.streamId(), is(STREAM_ID));
        assertThat(dataHeader.sessionId(), is(SESSION_ID));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(2)));
        assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(dataHeader.flags(), is(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
        assertThat(dataHeader.version(), is((short)HeaderFlyweight.CURRENT_VERSION));
    }

    @Test
    public void shouldBeAbleToSendOnChannelTwiceAsBatch() throws Exception
    {
        EventLogger.logInvocation();

        publication.onStatusMessage(INITIAL_TERM_ID, 0, (2 * ALIGNED_FRAME_LENGTH), rcvAddress);

        final AtomicBuffer buffer = new AtomicBuffer(ByteBuffer.allocate(PAYLOAD.length));
        buffer.putBytes(0, PAYLOAD);

        assertThat(logAppenders[0].append(buffer, 0, PAYLOAD.length), is(SUCCESS));
        assertThat(logAppenders[0].append(buffer, 0, PAYLOAD.length), is(SUCCESS));
        sender.doWork();

        assertThat(receivedFrames.size(), is(1));
        final ByteBuffer frame = receivedFrames.remove();

        dataHeader.wrap(frame, 0);
        assertThat(dataHeader.frameLength(), is(ALIGNED_FRAME_LENGTH));
        assertThat(dataHeader.termId(), is(INITIAL_TERM_ID));
        assertThat(dataHeader.streamId(), is(STREAM_ID));
        assertThat(dataHeader.sessionId(), is(SESSION_ID));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(1)));
        assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(dataHeader.flags(), is(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
        assertThat(dataHeader.version(), is((short)HeaderFlyweight.CURRENT_VERSION));

        dataHeader.wrap(frame, offsetOfMessage(2));
        assertThat(dataHeader.frameLength(), is(ALIGNED_FRAME_LENGTH));
        assertThat(dataHeader.termId(), is(INITIAL_TERM_ID));
        assertThat(dataHeader.streamId(), is(STREAM_ID));
        assertThat(dataHeader.sessionId(), is(SESSION_ID));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(2)));
        assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(dataHeader.flags(), is(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
        assertThat(dataHeader.version(), is((short)HeaderFlyweight.CURRENT_VERSION));
    }

    @Test
    public void shouldNotSendUntilStatusMessageReceived() throws Exception
    {
        EventLogger.logInvocation();

        final AtomicBuffer buffer = new AtomicBuffer(ByteBuffer.allocate(PAYLOAD.length));
        buffer.putBytes(0, PAYLOAD);
        assertThat(logAppenders[0].append(buffer, 0, PAYLOAD.length), is(SUCCESS));

        sender.doWork();
        assertThat(receivedFrames.size(), is(0));

        publication.onStatusMessage(INITIAL_TERM_ID, 0, ALIGNED_FRAME_LENGTH, rcvAddress);
        sender.doWork();

        assertThat(receivedFrames.size(), is(1));

        dataHeader.wrap(receivedFrames.remove(), 0);

        assertThat(dataHeader.frameLength(), is(ALIGNED_FRAME_LENGTH));
        assertThat(dataHeader.termId(), is(INITIAL_TERM_ID));
        assertThat(dataHeader.streamId(), is(STREAM_ID));
        assertThat(dataHeader.sessionId(), is(SESSION_ID));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(1)));
        assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(dataHeader.flags(), is(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
        assertThat(dataHeader.version(), is((short)HeaderFlyweight.CURRENT_VERSION));
    }

    @Test
    public void shouldNotBeAbleToSendAfterUsingUpYourWindow() throws Exception
    {
        EventLogger.logInvocation();

        final AtomicBuffer buffer = new AtomicBuffer(ByteBuffer.allocate(PAYLOAD.length));
        buffer.putBytes(0, PAYLOAD);
        assertThat(logAppenders[0].append(buffer, 0, PAYLOAD.length), is(SUCCESS));
        publication.onStatusMessage(INITIAL_TERM_ID, 0, ALIGNED_FRAME_LENGTH, rcvAddress);

        sender.doWork();

        assertThat(receivedFrames.size(), is(1));

        dataHeader.wrap(receivedFrames.remove(), 0);

        assertThat(dataHeader.frameLength(), is(ALIGNED_FRAME_LENGTH));
        assertThat(dataHeader.termId(), is(INITIAL_TERM_ID));
        assertThat(dataHeader.streamId(), is(STREAM_ID));
        assertThat(dataHeader.sessionId(), is(SESSION_ID));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(1)));
        assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(dataHeader.flags(), is(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
        assertThat(dataHeader.version(), is((short)HeaderFlyweight.CURRENT_VERSION));

        assertThat(logAppenders[0].append(buffer, 0, PAYLOAD.length), is(SUCCESS));
        sender.doWork();

        assertThat(receivedFrames.size(), is(0));
    }

    @Test
    public void shouldSend0LengthDataFrameAsHeartbeatWhenIdle() throws Exception
    {
        EventLogger.logInvocation();

        publication.onStatusMessage(INITIAL_TERM_ID, 0, ALIGNED_FRAME_LENGTH, rcvAddress);

        final AtomicBuffer buffer = new AtomicBuffer(ByteBuffer.allocate(PAYLOAD.length));
        buffer.putBytes(0, PAYLOAD);

        assertThat(logAppenders[0].append(buffer, 0, PAYLOAD.length), is(SUCCESS));
        sender.doWork();

        assertThat(receivedFrames.size(), is(1));  // should send now
        receivedFrames.remove();                   // skip data frame

        currentTimestamp += TimeUnit.MILLISECONDS.toNanos(DriverPublication.HEARTBEAT_TIMEOUT_MS) - 1;
        publications.forEach(DriverPublication::heartbeatCheck);

        assertThat(receivedFrames.size(), is(0));  // should not send yet
        currentTimestamp += 10;
        publications.forEach(DriverPublication::heartbeatCheck);

        assertThat(receivedFrames.size(), greaterThanOrEqualTo(1));  // should send now

        dataHeader.wrap(new AtomicBuffer(receivedFrames.remove()), 0);
        assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(2)));
    }

    @Test
    public void shouldSendMultiple0LengthDataFrameAsHeartbeatsWhenIdle()
    {
        EventLogger.logInvocation();

        publication.onStatusMessage(INITIAL_TERM_ID, 0, ALIGNED_FRAME_LENGTH, rcvAddress);

        final AtomicBuffer buffer = new AtomicBuffer(ByteBuffer.allocate(PAYLOAD.length));
        buffer.putBytes(0, PAYLOAD);

        assertThat(logAppenders[0].append(buffer, 0, PAYLOAD.length), is(SUCCESS));
        sender.doWork();

        assertThat(receivedFrames.size(), is(1));  // should send now
        receivedFrames.remove();                   // skip data frame

        currentTimestamp += TimeUnit.MILLISECONDS.toNanos(DriverPublication.HEARTBEAT_TIMEOUT_MS) - 1;
        publications.forEach(DriverPublication::heartbeatCheck);
        assertThat(receivedFrames.size(), is(0));  // should not send yet
        currentTimestamp += 10;
        publications.forEach(DriverPublication::heartbeatCheck);
        assertThat(receivedFrames.size(), greaterThanOrEqualTo(1));  // should send now

        dataHeader.wrap(new AtomicBuffer(receivedFrames.remove()), 0);
        assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(2)));

        currentTimestamp += TimeUnit.MILLISECONDS.toNanos(DriverPublication.HEARTBEAT_TIMEOUT_MS) - 1;
        publications.forEach(DriverPublication::heartbeatCheck);
        assertThat(receivedFrames.size(), is(0));  // should not send yet
        currentTimestamp += 10;
        publications.forEach(DriverPublication::heartbeatCheck);
        assertThat(receivedFrames.size(), greaterThanOrEqualTo(1));  // should send now

        dataHeader.wrap(new AtomicBuffer(receivedFrames.remove()), 0);
        assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(2)));
    }

    private int offsetOfMessage(final int num)
    {
        return (num - 1) * align(HEADER.length + PAYLOAD.length, FRAME_ALIGNMENT);
    }
}
