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
package io.aeron.driver;

import io.aeron.driver.buffer.RawLog;
import io.aeron.driver.media.ControlTransportPoller;
import io.aeron.driver.media.SendChannelEndpoint;
import io.aeron.driver.media.UdpChannel;
import io.aeron.driver.status.SystemCounters;
import io.aeron.logbuffer.HeaderWriter;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.logbuffer.TermAppender;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.HeaderFlyweight;
import io.aeron.protocol.SetupFlyweight;
import io.aeron.protocol.StatusMessageFlyweight;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.CachedNanoClock;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.AtomicLongPosition;
import org.agrona.concurrent.status.Position;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;

import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.logbuffer.LogBufferDescriptor.PARTITION_COUNT;
import static org.agrona.BitUtil.align;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.*;

public class SenderTest
{
    private static final int TERM_BUFFER_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int MAX_FRAME_LENGTH = 1024;
    private static final int SESSION_ID = 1;
    private static final int STREAM_ID = 2;
    private static final int INITIAL_TERM_ID = 3;
    private static final byte[] PAYLOAD = "Payload is here!".getBytes();

    private static final UnsafeBuffer HEADER = DataHeaderFlyweight.createDefaultHeader(
        SESSION_ID, STREAM_ID, INITIAL_TERM_ID);
    private static final int FRAME_LENGTH = HEADER.capacity() + PAYLOAD.length;
    private static final int ALIGNED_FRAME_LENGTH = align(FRAME_LENGTH, FRAME_ALIGNMENT);

    private final ControlTransportPoller mockTransportPoller = mock(ControlTransportPoller.class);

    private final RawLog rawLog = LogBufferHelper.newTestLogBuffers(TERM_BUFFER_LENGTH);

    private TermAppender[] termAppenders;
    private NetworkPublication publication;
    private Sender sender;

    private final FlowControl flowControl = spy(new UnicastFlowControl());
    private final RetransmitHandler mockRetransmitHandler = mock(RetransmitHandler.class);

    private long currentTimestamp = 0;

    private final Queue<ByteBuffer> receivedFrames = new ArrayDeque<>();

    private final UdpChannel udpChannel = UdpChannel.parse("aeron:udp?endpoint=localhost:40123");
    private final InetSocketAddress rcvAddress = udpChannel.remoteData();
    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();
    private final SetupFlyweight setupHeader = new SetupFlyweight();
    private final SystemCounters mockSystemCounters = mock(SystemCounters.class);
    private final OneToOneConcurrentArrayQueue<Runnable> senderCommandQueue =
        new OneToOneConcurrentArrayQueue<>(Configuration.CMD_QUEUE_CAPACITY);

    private final HeaderWriter headerWriter = HeaderWriter.newInstance(HEADER);

    private Answer<Integer> saveByteBufferAnswer =
        (invocation) ->
        {
            final Object args[] = invocation.getArguments();
            final ByteBuffer buffer = (ByteBuffer)args[0];

            final int length = buffer.limit() - buffer.position();
            receivedFrames.add(ByteBuffer.allocateDirect(length).put(buffer));

            // we don't pass on the args, so don't reset buffer.position() back
            return length;
        };

    @Before
    public void setUp()
    {
        final SendChannelEndpoint mockSendChannelEndpoint = mock(SendChannelEndpoint.class);
        when(mockSendChannelEndpoint.udpChannel()).thenReturn(udpChannel);
        when(mockSendChannelEndpoint.send(any())).thenAnswer(saveByteBufferAnswer);
        when(mockSystemCounters.get(any())).thenReturn(mock(AtomicCounter.class));

        final CachedNanoClock mockCachedNanoClock = mock(CachedNanoClock.class);
        when(mockCachedNanoClock.nanoTime()).thenAnswer((invocation) -> currentTimestamp);

        sender = new Sender(
            new MediaDriver.Context()
                .cachedEpochClock(new CachedEpochClock())
                .cachedNanoClock(mockCachedNanoClock)
                .controlTransportPoller(mockTransportPoller)
                .systemCounters(mockSystemCounters)
                .senderCommandQueue(senderCommandQueue)
                .nanoClock(() -> currentTimestamp));

        LogBufferDescriptor.initialiseTailWithTermId(rawLog.metaData(), 0, INITIAL_TERM_ID);

        termAppenders = new TermAppender[PARTITION_COUNT];
        for (int i = 0; i < PARTITION_COUNT; i++)
        {
            termAppenders[i] = new TermAppender(rawLog.termBuffers()[i], rawLog.metaData(), i);
        }

        publication = new NetworkPublication(
            1,
            101,
            mockSendChannelEndpoint,
            () -> currentTimestamp,
            rawLog,
            mock(Position.class),
            mock(Position.class),
            new AtomicLongPosition(),
            new AtomicLongPosition(),
            SESSION_ID,
            STREAM_ID,
            INITIAL_TERM_ID,
            MAX_FRAME_LENGTH,
            mockSystemCounters,
            flowControl,
            mockRetransmitHandler,
            new NetworkPublicationThreadLocals(),
            Configuration.PUBLICATION_UNBLOCK_TIMEOUT_NS,
            Configuration.PUBLICATION_CONNECTION_TIMEOUT_NS,
            Configuration.PUBLICATION_LINGER_NS,
            false,
            false);

        senderCommandQueue.offer(() -> sender.onNewNetworkPublication(publication));
    }

    @After
    public void tearDown()
    {
        sender.onClose();
    }

    @Test
    public void shouldSendSetupFrameOnChannelWhenTimeoutWithoutStatusMessage()
    {
        sender.doWork();
        assertThat(receivedFrames.size(), is(1));
        currentTimestamp += Configuration.PUBLICATION_SETUP_TIMEOUT_NS - 1;
        sender.doWork();
        assertThat(receivedFrames.size(), is(1));
        currentTimestamp += 10;
        sender.doWork();
        assertThat(receivedFrames.size(), is(2));

        setupHeader.wrap(new UnsafeBuffer(receivedFrames.remove()));
        assertThat(setupHeader.frameLength(), is(SetupFlyweight.HEADER_LENGTH));
        assertThat(setupHeader.initialTermId(), is(INITIAL_TERM_ID));
        assertThat(setupHeader.activeTermId(), is(INITIAL_TERM_ID));
        assertThat(setupHeader.streamId(), is(STREAM_ID));
        assertThat(setupHeader.sessionId(), is(SESSION_ID));
        assertThat(setupHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_SETUP));
        assertThat(setupHeader.flags(), is((short)0));
        assertThat(setupHeader.version(), is((short)HeaderFlyweight.CURRENT_VERSION));
    }

    @Test
    public void shouldSendMultipleSetupFramesOnChannelWhenTimeoutWithoutStatusMessage()
    {
        sender.doWork();
        assertThat(receivedFrames.size(), is(1));

        currentTimestamp += Configuration.PUBLICATION_SETUP_TIMEOUT_NS - 1;
        sender.doWork();
        currentTimestamp += 10;
        sender.doWork();

        assertThat(receivedFrames.size(), is(2));
    }

    @Test
    public void shouldNotSendSetupFrameAfterReceivingStatusMessage()
    {
        final StatusMessageFlyweight msg = mock(StatusMessageFlyweight.class);
        when(msg.consumptionTermId()).thenReturn(INITIAL_TERM_ID);
        when(msg.consumptionTermOffset()).thenReturn(0);
        when(msg.receiverWindowLength()).thenReturn(0);

        publication.onStatusMessage(msg, rcvAddress);
//        publication.senderPositionLimit(flowControl.onStatusMessage(msg, rcvAddress, ));
        sender.doWork();
        assertThat(receivedFrames.size(), is(1));
        receivedFrames.remove();

        currentTimestamp += Configuration.PUBLICATION_SETUP_TIMEOUT_NS + 10;
        sender.doWork();

        assertThat(receivedFrames.size(), is(1));
        dataHeader.wrap(receivedFrames.remove());
        assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA)); // heartbeat
        assertThat(dataHeader.frameLength(), is(0));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(1)));
    }

    @Test
    public void shouldSendSetupFrameAfterReceivingStatusMessageWithSetupBit()
    {
        final StatusMessageFlyweight msg = mock(StatusMessageFlyweight.class);
        when(msg.consumptionTermId()).thenReturn(INITIAL_TERM_ID);
        when(msg.consumptionTermOffset()).thenReturn(0);
        when(msg.receiverWindowLength()).thenReturn(ALIGNED_FRAME_LENGTH);

        publication.onStatusMessage(msg, rcvAddress);
//        publication.senderPositionLimit(
//            flowControl.onStatusMessage(INITIAL_TERM_ID, 0, ALIGNED_FRAME_LENGTH, rcvAddress));

        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(PAYLOAD.length));
        buffer.putBytes(0, PAYLOAD);

        termAppenders[0].appendUnfragmentedMessage(headerWriter, buffer, 0, PAYLOAD.length, null, INITIAL_TERM_ID);
        sender.doWork();

        assertThat(receivedFrames.size(), is(2)); // setup then data
        receivedFrames.remove();
        receivedFrames.remove();

        publication.triggerSendSetupFrame();

        sender.doWork();
        assertThat(receivedFrames.size(), is(0)); // setup has been sent already, have to wait

        currentTimestamp += Configuration.PUBLICATION_SETUP_TIMEOUT_NS + 10;

        sender.doWork();

        assertThat(receivedFrames.size(), is(1));

        setupHeader.wrap(new UnsafeBuffer(receivedFrames.remove()));
        assertThat(setupHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_SETUP));
    }

    @Test
    public void shouldBeAbleToSendOnChannel()
    {
        final StatusMessageFlyweight msg = mock(StatusMessageFlyweight.class);
        when(msg.consumptionTermId()).thenReturn(INITIAL_TERM_ID);
        when(msg.consumptionTermOffset()).thenReturn(0);
        when(msg.receiverWindowLength()).thenReturn(ALIGNED_FRAME_LENGTH);

        publication.onStatusMessage(msg, rcvAddress);
//        publication.senderPositionLimit(
//            flowControl.onStatusMessage(INITIAL_TERM_ID, 0, ALIGNED_FRAME_LENGTH, rcvAddress));

        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(PAYLOAD.length));
        buffer.putBytes(0, PAYLOAD);

        termAppenders[0].appendUnfragmentedMessage(headerWriter, buffer, 0, PAYLOAD.length, null, INITIAL_TERM_ID);
        sender.doWork();

        assertThat(receivedFrames.size(), is(2));
        setupHeader.wrap(new UnsafeBuffer(receivedFrames.remove()));
        assertThat(setupHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_SETUP));

        dataHeader.wrap(new UnsafeBuffer(receivedFrames.remove()));
        assertThat(dataHeader.frameLength(), is(FRAME_LENGTH));
        assertThat(dataHeader.termId(), is(INITIAL_TERM_ID));
        assertThat(dataHeader.streamId(), is(STREAM_ID));
        assertThat(dataHeader.sessionId(), is(SESSION_ID));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(1)));
        assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(dataHeader.flags(), is(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
        assertThat(dataHeader.version(), is((short)HeaderFlyweight.CURRENT_VERSION));
    }

    @Test
    public void shouldBeAbleToSendOnChannelTwice()
    {
        final StatusMessageFlyweight msg = mock(StatusMessageFlyweight.class);
        when(msg.consumptionTermId()).thenReturn(INITIAL_TERM_ID);
        when(msg.consumptionTermOffset()).thenReturn(0);
        when(msg.receiverWindowLength()).thenReturn(2 * ALIGNED_FRAME_LENGTH);

        publication.onStatusMessage(msg, rcvAddress);
//        publication.senderPositionLimit(
//            flowControl.onStatusMessage(INITIAL_TERM_ID, 0, (2 * ALIGNED_FRAME_LENGTH), rcvAddress));

        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(PAYLOAD.length));
        buffer.putBytes(0, PAYLOAD);

        termAppenders[0].appendUnfragmentedMessage(headerWriter, buffer, 0, PAYLOAD.length, null, INITIAL_TERM_ID);
        sender.doWork();
        termAppenders[0].appendUnfragmentedMessage(headerWriter, buffer, 0, PAYLOAD.length, null, INITIAL_TERM_ID);
        sender.doWork();

        assertThat(receivedFrames.size(), is(3));

        setupHeader.wrap(new UnsafeBuffer(receivedFrames.remove()));
        assertThat(setupHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_SETUP));

        dataHeader.wrap(new UnsafeBuffer(receivedFrames.remove()));
        assertThat(dataHeader.frameLength(), is(FRAME_LENGTH));
        assertThat(dataHeader.termId(), is(INITIAL_TERM_ID));
        assertThat(dataHeader.streamId(), is(STREAM_ID));
        assertThat(dataHeader.sessionId(), is(SESSION_ID));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(1)));
        assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(dataHeader.flags(), is(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
        assertThat(dataHeader.version(), is((short)HeaderFlyweight.CURRENT_VERSION));

        dataHeader.wrap(new UnsafeBuffer(receivedFrames.remove()));
        assertThat(dataHeader.frameLength(), is(FRAME_LENGTH));
        assertThat(dataHeader.termId(), is(INITIAL_TERM_ID));
        assertThat(dataHeader.streamId(), is(STREAM_ID));
        assertThat(dataHeader.sessionId(), is(SESSION_ID));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(2)));
        assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(dataHeader.flags(), is(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
        assertThat(dataHeader.version(), is((short)HeaderFlyweight.CURRENT_VERSION));
    }

    @Test
    public void shouldNotSendUntilStatusMessageReceived()
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(PAYLOAD.length));
        buffer.putBytes(0, PAYLOAD);
        termAppenders[0].appendUnfragmentedMessage(headerWriter, buffer, 0, PAYLOAD.length, null, INITIAL_TERM_ID);

        sender.doWork();
        assertThat(receivedFrames.size(), is(1));
        setupHeader.wrap(receivedFrames.remove());
        assertThat(setupHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_SETUP));

        final StatusMessageFlyweight msg = mock(StatusMessageFlyweight.class);
        when(msg.consumptionTermId()).thenReturn(INITIAL_TERM_ID);
        when(msg.consumptionTermOffset()).thenReturn(0);
        when(msg.receiverWindowLength()).thenReturn(ALIGNED_FRAME_LENGTH);

        publication.onStatusMessage(msg, rcvAddress);
//        publication.senderPositionLimit(
//            flowControl.onStatusMessage(INITIAL_TERM_ID, 0, ALIGNED_FRAME_LENGTH, rcvAddress));
        sender.doWork();

        assertThat(receivedFrames.size(), is(1));

        dataHeader.wrap(new UnsafeBuffer(receivedFrames.remove()));

        assertThat(dataHeader.frameLength(), is(FRAME_LENGTH));
        assertThat(dataHeader.termId(), is(INITIAL_TERM_ID));
        assertThat(dataHeader.streamId(), is(STREAM_ID));
        assertThat(dataHeader.sessionId(), is(SESSION_ID));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(1)));
        assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(dataHeader.flags(), is(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
        assertThat(dataHeader.version(), is((short)HeaderFlyweight.CURRENT_VERSION));
    }

    @Test
    public void shouldNotBeAbleToSendAfterUsingUpYourWindow()
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(PAYLOAD.length));
        buffer.putBytes(0, PAYLOAD);
        termAppenders[0].appendUnfragmentedMessage(headerWriter, buffer, 0, PAYLOAD.length, null, INITIAL_TERM_ID);

        final StatusMessageFlyweight msg = mock(StatusMessageFlyweight.class);
        when(msg.consumptionTermId()).thenReturn(INITIAL_TERM_ID);
        when(msg.consumptionTermOffset()).thenReturn(0);
        when(msg.receiverWindowLength()).thenReturn(ALIGNED_FRAME_LENGTH);

        publication.onStatusMessage(msg, rcvAddress);
//        publication.senderPositionLimit(
//            flowControl.onStatusMessage(INITIAL_TERM_ID, 0, ALIGNED_FRAME_LENGTH, rcvAddress));

        sender.doWork();

        assertThat(receivedFrames.size(), is(2));
        receivedFrames.remove();                   // skip setup

        dataHeader.wrap(new UnsafeBuffer(receivedFrames.remove()));
        assertThat(dataHeader.frameLength(), is(FRAME_LENGTH));
        assertThat(dataHeader.termId(), is(INITIAL_TERM_ID));
        assertThat(dataHeader.streamId(), is(STREAM_ID));
        assertThat(dataHeader.sessionId(), is(SESSION_ID));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(1)));
        assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
        assertThat(dataHeader.flags(), is(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
        assertThat(dataHeader.version(), is((short)HeaderFlyweight.CURRENT_VERSION));

        termAppenders[0].appendUnfragmentedMessage(headerWriter, buffer, 0, PAYLOAD.length, null, INITIAL_TERM_ID);
        sender.doWork();

        assertThat(receivedFrames.size(), is(0));
    }

    @Test
    public void shouldSendLastDataFrameAsHeartbeatWhenIdle()
    {
        final StatusMessageFlyweight msg = mock(StatusMessageFlyweight.class);
        when(msg.consumptionTermId()).thenReturn(INITIAL_TERM_ID);
        when(msg.consumptionTermOffset()).thenReturn(0);
        when(msg.receiverWindowLength()).thenReturn(ALIGNED_FRAME_LENGTH);

        publication.onStatusMessage(msg, rcvAddress);
//        publication.senderPositionLimit(
//            flowControl.onStatusMessage(INITIAL_TERM_ID, 0, ALIGNED_FRAME_LENGTH, rcvAddress));

        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(PAYLOAD.length));
        buffer.putBytes(0, PAYLOAD);

        termAppenders[0].appendUnfragmentedMessage(headerWriter, buffer, 0, PAYLOAD.length, null, INITIAL_TERM_ID);
        sender.doWork();

        assertThat(receivedFrames.size(), is(2));  // should send ticks
        receivedFrames.remove();                   // skip setup & data frame
        receivedFrames.remove();

        currentTimestamp += Configuration.PUBLICATION_HEARTBEAT_TIMEOUT_NS - 1;
        sender.doWork();

        assertThat(receivedFrames.size(), is(0));  // should not send yet
        currentTimestamp += 10;
        sender.doWork();

        assertThat(receivedFrames.size(), greaterThanOrEqualTo(1));  // should send ticks

        dataHeader.wrap(receivedFrames.remove());
        assertThat(dataHeader.frameLength(), is(0));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(2)));
    }

    @Test
    public void shouldSendMultipleDataFramesAsHeartbeatsWhenIdle()
    {
        final StatusMessageFlyweight msg = mock(StatusMessageFlyweight.class);
        when(msg.consumptionTermId()).thenReturn(INITIAL_TERM_ID);
        when(msg.consumptionTermOffset()).thenReturn(0);
        when(msg.receiverWindowLength()).thenReturn(ALIGNED_FRAME_LENGTH);

        publication.onStatusMessage(msg, rcvAddress);
//        publication.senderPositionLimit(
//            flowControl.onStatusMessage(INITIAL_TERM_ID, 0, ALIGNED_FRAME_LENGTH, rcvAddress));

        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(PAYLOAD.length));
        buffer.putBytes(0, PAYLOAD);

        termAppenders[0].appendUnfragmentedMessage(headerWriter, buffer, 0, PAYLOAD.length, null, INITIAL_TERM_ID);
        sender.doWork();

        assertThat(receivedFrames.size(), is(2));  // should send ticks
        receivedFrames.remove();
        receivedFrames.remove();                   // skip setup & data frame

        currentTimestamp += Configuration.PUBLICATION_HEARTBEAT_TIMEOUT_NS - 1;
        sender.doWork();
        assertThat(receivedFrames.size(), is(0));  // should not send yet
        currentTimestamp += 10;
        sender.doWork();
        assertThat(receivedFrames.size(), greaterThanOrEqualTo(1));  // should send ticks

        dataHeader.wrap(receivedFrames.remove());
        assertThat(dataHeader.frameLength(), is(0));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(2)));

        currentTimestamp += Configuration.PUBLICATION_HEARTBEAT_TIMEOUT_NS - 1;
        sender.doWork();
        assertThat(receivedFrames.size(), is(0));  // should not send yet
        currentTimestamp += 10;
        sender.doWork();
        assertThat(receivedFrames.size(), greaterThanOrEqualTo(1));  // should send ticks

        dataHeader.wrap(receivedFrames.remove());
        assertThat(dataHeader.frameLength(), is(0));
        assertThat(dataHeader.termOffset(), is(offsetOfMessage(2)));
    }

    private int offsetOfMessage(final int offset)
    {
        return (offset - 1) * align(HEADER.capacity() + PAYLOAD.length, FRAME_ALIGNMENT);
    }
}
