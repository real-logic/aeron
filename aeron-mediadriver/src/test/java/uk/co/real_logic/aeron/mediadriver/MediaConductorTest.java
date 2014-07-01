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
import uk.co.real_logic.aeron.mediadriver.buffer.BufferManagement;
import uk.co.real_logic.aeron.util.AtomicArray;
import uk.co.real_logic.aeron.util.ErrorCode;
import uk.co.real_logic.aeron.util.TimerWheel;
import uk.co.real_logic.aeron.util.command.ControlProtocolEvents;
import uk.co.real_logic.aeron.util.command.PublicationMessageFlyweight;
import uk.co.real_logic.aeron.util.command.SubscriptionMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.OneToOneConcurrentArrayQueue;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.ManyToOneRingBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferDescriptor;
import uk.co.real_logic.aeron.util.event.EventLogger;
import uk.co.real_logic.aeron.util.status.StatusBufferManager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.mediadriver.MediaDriver.MEDIA_CONDUCTOR_TICKS_PER_WHEEL;
import static uk.co.real_logic.aeron.mediadriver.MediaDriver.MEDIA_CONDUCTOR_TICK_DURATION_US;
import static uk.co.real_logic.aeron.util.ErrorCode.CHANNEL_UNKNOWN;
import static uk.co.real_logic.aeron.util.ErrorCode.INVALID_DESTINATION;

/**
 * Test the Media Driver Conductor in isolation
 *
 * Tests for interaction of media conductor with client protocol
 */
public class MediaConductorTest
{
    // TODO: Assert the error log has been notified appropriately

    public static final EventLogger LOGGER = new EventLogger(MediaConductorTest.class);

    private static final String DESTINATION_URI = "udp://localhost:";
    private static final String INVALID_URI = "udp://";
    private static final long[] ONE_CHANNEL = {10};
    private static final long[] ANOTHER_CHANNEL = {20};
    private static final long[] TWO_CHANNELS = {20, 30};
    private static final long[] THREE_CHANNELS = {10, 20, 30};

    private final ByteBuffer toDriverBuffer =
        ByteBuffer.allocate(MediaDriver.COMMAND_BUFFER_SZ + RingBufferDescriptor.TRAILER_LENGTH);

    private final NioSelector nioSelector = mock(NioSelector.class);
    private final BufferManagement mockBufferManagement = mock(BufferManagement.class);

    private final RingBuffer fromClientCommands = new ManyToOneRingBuffer(new AtomicBuffer(toDriverBuffer));
    private final ClientProxy mockClientProxy = mock(ClientProxy.class);

    private final PublicationMessageFlyweight publicationMessage = new PublicationMessageFlyweight();

    private final SubscriptionMessageFlyweight subscriptionMessage = new SubscriptionMessageFlyweight();
    private final AtomicBuffer writeBuffer = new AtomicBuffer(ByteBuffer.allocate(256));

    private final AtomicArray<DriverPublication> publications = new AtomicArray<>();

    private StatusBufferManager statusBufferManager;

    private MediaConductor mediaConductor;
    private Receiver receiver;

    @Before
    public void setUp() throws Exception
    {
        when(mockBufferManagement.addPublication(anyObject(), anyLong(), anyLong()))
                .thenReturn(BufferAndFrameUtils.createTestRotator(65536 + RingBufferDescriptor.TRAILER_LENGTH,
                        LogBufferDescriptor.STATE_BUFFER_LENGTH));

        statusBufferManager = mock(StatusBufferManager.class);

        final MediaDriver.MediaDriverContext ctx = new MediaDriver.MediaDriverContext()
            .conductorCommandBuffer(MediaDriver.COMMAND_BUFFER_SZ)
            .receiverCommandBuffer(MediaDriver.COMMAND_BUFFER_SZ)
            .receiverNioSelector(nioSelector)
            .conductorNioSelector(nioSelector)
            .unicastSenderFlowControl(UnicastSenderControlStrategy::new)
            .multicastSenderFlowControl(DefaultMulticastSenderControlStrategy::new)
            .conductorTimerWheel(new TimerWheel(MEDIA_CONDUCTOR_TICK_DURATION_US,
                                                TimeUnit.MICROSECONDS,
                                                MEDIA_CONDUCTOR_TICKS_PER_WHEEL))
            .newReceiveBufferEventQueue(new OneToOneConcurrentArrayQueue<>(1024))
            .subscribedSessions(new AtomicArray<>())
            .publications(publications)
            .bufferManagement(mockBufferManagement)
            .statusBufferManager(statusBufferManager);

        ctx.fromClientCommands(fromClientCommands);
        ctx.clientProxy(mockClientProxy);

        ctx.receiverProxy(new ReceiverProxy(ctx.receiverCommandBuffer(), ctx.newReceiveBufferEventQueue()));
        ctx.mediaConductorProxy(new MediaConductorProxy(ctx.mediaCommandBuffer()));

        receiver = new Receiver(ctx);
        mediaConductor = new MediaConductor(ctx);
    }

    @After
    public void tearDown() throws Exception
    {
        receiver.close();
        receiver.nioSelector().selectNowWithoutProcessing();
        mediaConductor.close();
        mediaConductor.nioSelector().selectNowWithoutProcessing();
    }

    @Test
    public void shouldBeAbleToAddSingleChannel() throws Exception
    {
        LOGGER.logInvocation();

        writeChannelMessage(ControlProtocolEvents.ADD_PUBLICATION, 1L, 2L, 4000);

        mediaConductor.doWork();

        assertThat(publications.length(), is(1));
        assertNotNull(publications.get(0));
        assertThat(publications.get(0).sessionId(), is(1L));
        assertThat(publications.get(0).channelId(), is(2L));

        verify(mockClientProxy).onNewBuffers(eq(ControlProtocolEvents.NEW_PUBLICATION_BUFFER_EVENT),
                eq(1L), eq(2L), anyLong(), eq(DESTINATION_URI + 4000), any(), anyLong(), anyInt());

    }

    @Test
    public void shouldBeAbleToAddSingleSubscriber() throws Exception
    {
        LOGGER.logInvocation();

        writeSubscriberMessage(ControlProtocolEvents.ADD_SUBSCRIPTION, DESTINATION_URI + 4000, ONE_CHANNEL);

        mediaConductor.doWork();
        receiver.doWork();

        assertNotNull(receiver.frameHandler(UdpDestination.parse(DESTINATION_URI + 4000)));
    }

    @Test
    public void shouldBeAbleToAddAndRemoveSingleSubscriber() throws Exception
    {
        LOGGER.logInvocation();

        writeSubscriberMessage(ControlProtocolEvents.ADD_SUBSCRIPTION, DESTINATION_URI + 4000, ONE_CHANNEL);
        writeSubscriberMessage(ControlProtocolEvents.REMOVE_SUBSCRIPTION, DESTINATION_URI + 4000, ONE_CHANNEL);

        mediaConductor.doWork();
        receiver.doWork();

        assertNull(receiver.frameHandler(UdpDestination.parse(DESTINATION_URI + 4000)));
    }

    @Test
    public void shouldBeAbleToAddMultipleChannels() throws Exception
    {
        LOGGER.logInvocation();

        writeChannelMessage(ControlProtocolEvents.ADD_PUBLICATION, 1L, 2L, 4001);
        writeChannelMessage(ControlProtocolEvents.ADD_PUBLICATION, 1L, 3L, 4002);
        writeChannelMessage(ControlProtocolEvents.ADD_PUBLICATION, 3L, 2L, 4003);
        writeChannelMessage(ControlProtocolEvents.ADD_PUBLICATION, 3L, 4L, 4004);

        mediaConductor.doWork();

        assertThat(publications.length(), is(4));
    }

    @Test
    public void shouldBeAbleToRemoveSingleChannel() throws Exception
    {
        LOGGER.logInvocation();

        writeChannelMessage(ControlProtocolEvents.ADD_PUBLICATION, 1L, 2L, 4005);
        writeChannelMessage(ControlProtocolEvents.REMOVE_PUBLICATION, 1L, 2L, 4005);

        mediaConductor.doWork();

        assertThat(publications.length(), is(0));
        assertNull(mediaConductor.frameHandler(UdpDestination.parse(DESTINATION_URI + 4005)));
    }

    @Test
    public void shouldBeAbleToRemoveMultipleChannels() throws IOException
    {
        LOGGER.logInvocation();

        writeChannelMessage(ControlProtocolEvents.ADD_PUBLICATION, 1L, 2L, 4006);
        writeChannelMessage(ControlProtocolEvents.ADD_PUBLICATION, 1L, 3L, 4007);
        writeChannelMessage(ControlProtocolEvents.ADD_PUBLICATION, 3L, 2L, 4008);
        writeChannelMessage(ControlProtocolEvents.ADD_PUBLICATION, 3L, 4L, 4008);

        writeChannelMessage(ControlProtocolEvents.REMOVE_PUBLICATION, 1L, 2L, 4006);
        writeChannelMessage(ControlProtocolEvents.REMOVE_PUBLICATION, 1L, 3L, 4007);
        writeChannelMessage(ControlProtocolEvents.REMOVE_PUBLICATION, 3L, 2L, 4008);
        writeChannelMessage(ControlProtocolEvents.REMOVE_PUBLICATION, 3L, 4L, 4008);

        mediaConductor.doWork();

        assertThat(publications.length(), is(0));
    }

    @Test
    public void shouldKeepFrameHandlerUponRemovalOfAllButOneSubscriber() throws Exception
    {
        LOGGER.logInvocation();

        final UdpDestination destination = UdpDestination.parse(DESTINATION_URI + 4000);

        writeSubscriberMessage(ControlProtocolEvents.ADD_SUBSCRIPTION, DESTINATION_URI + 4000, THREE_CHANNELS);

        mediaConductor.doWork();
        receiver.doWork();

        final DataFrameHandler frameHandler = receiver.frameHandler(destination);

        assertNotNull(frameHandler);
        assertThat(frameHandler.subscriptionMap().size(), is(3));

        writeSubscriberMessage(ControlProtocolEvents.REMOVE_SUBSCRIPTION, DESTINATION_URI + 4000, TWO_CHANNELS);

        mediaConductor.doWork();
        receiver.doWork();

        assertNotNull(receiver.frameHandler(destination));
        assertThat(frameHandler.subscriptionMap().size(), is(1));
    }

    @Test
    public void shouldOnlyRemoveFrameHandlerUponRemovalOfAllSubscribers() throws Exception
    {
        LOGGER.logInvocation();

        final UdpDestination destination = UdpDestination.parse(DESTINATION_URI + 4000);

        writeSubscriberMessage(ControlProtocolEvents.ADD_SUBSCRIPTION, DESTINATION_URI + 4000, THREE_CHANNELS);

        mediaConductor.doWork();
        receiver.doWork();

        final DataFrameHandler frameHandler = receiver.frameHandler(destination);

        assertNotNull(frameHandler);
        assertThat(frameHandler.subscriptionMap().size(), is(3));

        writeSubscriberMessage(ControlProtocolEvents.REMOVE_SUBSCRIPTION, DESTINATION_URI + 4000, TWO_CHANNELS);

        mediaConductor.doWork();
        receiver.doWork();

        assertNotNull(receiver.frameHandler(destination));
        assertThat(frameHandler.subscriptionMap().size(), is(1));

        writeSubscriberMessage(ControlProtocolEvents.REMOVE_SUBSCRIPTION, DESTINATION_URI + 4000, ONE_CHANNEL);

        mediaConductor.doWork();
        receiver.doWork();

        assertNull(receiver.frameHandler(destination));
    }

    @Test
    public void shouldErrorOnAddDuplicateChannelOnExistingSession() throws Exception
    {
        LOGGER.logInvocation();

        writeChannelMessage(ControlProtocolEvents.ADD_PUBLICATION, 1L, 2L, 4000);
        writeChannelMessage(ControlProtocolEvents.ADD_PUBLICATION, 1L, 2L, 4000);

        mediaConductor.doWork();

        assertThat(publications.length(), is(1));
        assertNotNull(publications.get(0));
        assertThat(publications.get(0).sessionId(), is(1L));
        assertThat(publications.get(0).channelId(), is(2L));

        mockClientProxy.onError(eq(ErrorCode.CHANNEL_ALREADY_EXISTS), argThat(not(isEmptyOrNullString())), any(), anyInt());
    }

    @Test
    public void shouldErrorOnRemoveChannelOnUnknownDestination() throws Exception
    {
        LOGGER.logInvocation();

        writeChannelMessage(ControlProtocolEvents.REMOVE_PUBLICATION, 1L, 2L, 4000);

        mediaConductor.doWork();

        assertThat(publications.length(), is(0));

        mockClientProxy.onError(eq(INVALID_DESTINATION), argThat(not(isEmptyOrNullString())), any(), anyInt());
    }

    @Test
    public void shouldErrorOnRemoveChannelOnUnknownSessionId() throws Exception
    {
        LOGGER.logInvocation();

        writeChannelMessage(ControlProtocolEvents.ADD_PUBLICATION, 1L, 2L, 4000);
        writeChannelMessage(ControlProtocolEvents.REMOVE_PUBLICATION, 2L, 2L, 4000);

        mediaConductor.doWork();

        assertThat(publications.length(), is(1));
        assertNotNull(publications.get(0));
        assertThat(publications.get(0).sessionId(), is(1L));
        assertThat(publications.get(0).channelId(), is(2L));

        mockClientProxy.onError(eq(CHANNEL_UNKNOWN), argThat(not(isEmptyOrNullString())), any(), anyInt());
    }

    @Test
    public void shouldErrorOnRemoveChannelOnUnknownChannelId() throws Exception
    {
        LOGGER.logInvocation();

        writeChannelMessage(ControlProtocolEvents.ADD_PUBLICATION, 1L, 2L, 4000);
        writeChannelMessage(ControlProtocolEvents.REMOVE_PUBLICATION, 1L, 3L, 4000);

        mediaConductor.doWork();

        assertThat(publications.length(), is(1));
        assertNotNull(publications.get(0));
        assertThat(publications.get(0).sessionId(), is(1L));
        assertThat(publications.get(0).channelId(), is(2L));

        mockClientProxy.onError(eq(CHANNEL_UNKNOWN), argThat(not(isEmptyOrNullString())), any(), anyInt());
    }

    @Test
    public void shouldErrorOnAddSubscriberWithInvalidUri() throws Exception
    {
        LOGGER.logInvocation();

        writeSubscriberMessage(ControlProtocolEvents.ADD_SUBSCRIPTION, INVALID_URI, ONE_CHANNEL);

        mediaConductor.doWork();
        receiver.doWork();
        mediaConductor.doWork();

        mockClientProxy.onError(eq(INVALID_DESTINATION), argThat(not(isEmptyOrNullString())), any(), anyInt());
    }

    private void writeChannelMessage(final int msgTypeId, final long sessionId, final long channelId, final int port)
        throws IOException
    {
        publicationMessage.wrap(writeBuffer, 0);
        publicationMessage.channelId(channelId);
        publicationMessage.sessionId(sessionId);
        publicationMessage.destination(DESTINATION_URI + port);

        fromClientCommands.write(msgTypeId, writeBuffer, 0, publicationMessage.length());
    }

    private void writeSubscriberMessage(final int msgTypeId, final String destination, final long[] channelIds)
            throws IOException
    {
        subscriptionMessage.wrap(writeBuffer, 0);
        subscriptionMessage.channelIds(channelIds)
                         .destination(destination);

        fromClientCommands.write(msgTypeId, writeBuffer, 0, subscriptionMessage.length());
    }
}
