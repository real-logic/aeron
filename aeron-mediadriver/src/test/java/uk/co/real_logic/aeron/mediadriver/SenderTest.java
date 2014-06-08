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

import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.aeron.mediadriver.buffer.BufferRotator;
import uk.co.real_logic.aeron.util.BitUtil;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferDescriptor;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Test Sender in isolation
 */
public class SenderTest
{
    public static final long LOG_BUFFER_SIZE = (64*1024) + RingBufferDescriptor.TRAILER_LENGTH;
    public static final int MAX_FRAME_LENGTH = 1024;
    public static final long SESSION_ID = 1L;
    public static final long CHANNEL_ID = 2L;
    public static final long INITIAL_TERM_ID = 3L;
    public static final long PAYLOAD = 0x7100L;

    private final Sender sender = new Sender();
    private final BufferRotator rotator = BufferAndFrameUtils.createTestRotator(LOG_BUFFER_SIZE,
            LogBufferDescriptor.STATE_BUFFER_LENGTH);
    private final LogAppender[] logAppenders = BufferAndFrameUtils.createLogAppenders(rotator, MAX_FRAME_LENGTH);

    private SenderChannel channel;
    private final ControlFrameHandler mockFrameHandler = mock(ControlFrameHandler.class);
    private final SenderControlStrategy spySenderControlStrategy = spy(new UnicastSenderControlStrategy());

    private long currentTimestamp;

    private final Queue<ByteBuffer> receivedFrames = new ArrayDeque<>();

    private final UdpDestination destination = UdpDestination.parse("udp://localhost:40123");
    private final InetSocketAddress recvAddr = destination.remoteData();

    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();

    private SenderChannel.SendFunction sendFunction = (buffer, addr) ->
    {
        assertThat(addr, is(recvAddr));

        final int size = buffer.limit() - buffer.position();
        final ByteBuffer savedFrame = ByteBuffer.allocate(size);
        savedFrame.put(buffer);
        receivedFrames.add(savedFrame);

        return size;
    };

    private SenderChannel.TimeFunction timeFunction = () -> currentTimestamp;

    @Before
    public void setUp()
    {
        currentTimestamp = 0;

        when(mockFrameHandler.destination()).thenReturn(destination);

        channel = new SenderChannel(mockFrameHandler, spySenderControlStrategy, rotator, SESSION_ID,
                CHANNEL_ID, INITIAL_TERM_ID, logAppenders[0].defaultHeaderLength(), MAX_FRAME_LENGTH,
                sendFunction, timeFunction);
        sender.addChannel(channel);
    }

    @Test
    public void shouldAddAndRemoveSenderChannel()
    {
        sender.removeChannel(channel);
    }

    @Test
    public void shouldSend0LengthDataOnChannelWhenTimeoutWithoutStatusMessage()
    {
        currentTimestamp += TimeUnit.MILLISECONDS.toNanos(SenderChannel.INITIAL_HEARTBEAT_TIMEOUT_MS) - 1;
        sender.heartbeatChecks();
        assertThat(receivedFrames.size(), is(0));  // should not send yet
        currentTimestamp += 10;
        sender.heartbeatChecks();
        assertThat(receivedFrames.size(), greaterThanOrEqualTo(1));  // should send now

        dataHeader.wrap(new AtomicBuffer(receivedFrames.remove()), 0);

        assertThat(dataHeader.version(), is(Short.valueOf(HeaderFlyweight.CURRENT_VERSION)));
        assertThat(dataHeader.flags(), is(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
        assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH));
        assertThat(dataHeader.sessionId(), is(SESSION_ID));
        assertThat(dataHeader.channelId(), is(CHANNEL_ID));
        assertThat(dataHeader.termId(), is(INITIAL_TERM_ID));
        assertThat(dataHeader.termOffset(), is(0L));
    }

    @Test
    public void shouldNotSend0LengthDataFrameAfterReceivingStatusMessage()
    {
        currentTimestamp += TimeUnit.MILLISECONDS.toNanos(SenderChannel.HEARTBEAT_TIMEOUT_MS);
        channel.onStatusMessage(INITIAL_TERM_ID, 0, 0);
        sender.heartbeatChecks();
        assertThat(receivedFrames.size(), is(0));
    }

    @Test
    public void shouldBeAbleToSendOnChannel()
    {
        channel.onStatusMessage(INITIAL_TERM_ID, 0, 64);

        final AtomicBuffer buffer = new AtomicBuffer(ByteBuffer.allocate(BitUtil.SIZE_OF_LONG));
        buffer.putLong(0, PAYLOAD);

        assertTrue(logAppenders[0].append(buffer, 0, BitUtil.SIZE_OF_LONG));
        channel.send();

        assertThat(receivedFrames.size(), greaterThanOrEqualTo(1));  // should send now

        dataHeader.wrap(new AtomicBuffer(receivedFrames.remove()), 0);

        assertThat(dataHeader.version(), is(Short.valueOf(HeaderFlyweight.CURRENT_VERSION)));
        assertThat(dataHeader.flags(), is(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
        assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH + BitUtil.SIZE_OF_LONG));
        assertThat(dataHeader.sessionId(), is(SESSION_ID));
        assertThat(dataHeader.channelId(), is(CHANNEL_ID));
        assertThat(dataHeader.termId(), is(INITIAL_TERM_ID));
        assertThat(dataHeader.termOffset(), is(0L));
    }

}
