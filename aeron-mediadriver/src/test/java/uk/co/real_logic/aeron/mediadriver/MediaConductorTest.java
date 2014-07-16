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
import uk.co.real_logic.aeron.mediadriver.buffer.TermBufferManager;
import uk.co.real_logic.aeron.util.concurrent.AtomicArray;
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
import static uk.co.real_logic.aeron.util.ErrorCode.INVALID_DESTINATION_IN_PUBLICATION;
import static uk.co.real_logic.aeron.util.ErrorCode.PUBLICATION_CHANNEL_UNKNOWN;

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
    private static final long CHANNEL_1 = 10;
    private static final long CHANNEL_2 = 20;
    private static final long CHANNEL_3 = 30;
    private static final int TERM_BUFFER_SZ = MediaDriver.TERM_BUFFER_SZ_DEFAULT;

    private final ByteBuffer toDriverBuffer =
        ByteBuffer.allocate(MediaDriver.COMMAND_BUFFER_SZ + RingBufferDescriptor.TRAILER_LENGTH);

    private final NioSelector nioSelector = mock(NioSelector.class);
    private final TermBufferManager mockTermBufferManager = mock(TermBufferManager.class);

    private final RingBuffer fromClientCommands = new ManyToOneRingBuffer(new AtomicBuffer(toDriverBuffer));
    private final ClientProxy mockClientProxy = mock(ClientProxy.class);

    private final PublicationMessageFlyweight publicationMessage = new PublicationMessageFlyweight();

    private final SubscriptionMessageFlyweight subscriptionMessage = new SubscriptionMessageFlyweight();
    private final AtomicBuffer writeBuffer = new AtomicBuffer(ByteBuffer.allocate(256));

    private final AtomicArray<DriverPublication> publications = new AtomicArray<>();


    private MediaConductor mediaConductor;
    private Receiver receiver;

    @Before
    public void setUp() throws Exception
    {
        when(mockTermBufferManager.addPublication(anyObject(), anyLong(), anyLong()))
            .thenReturn(BufferAndFrameUtils.createTestTermBuffers(TERM_BUFFER_SZ,
                                                                  LogBufferDescriptor.STATE_BUFFER_LENGTH));

        final MediaDriver.MediaDriverContext ctx = new MediaDriver.MediaDriverContext()
            .receiverNioSelector(nioSelector)
            .conductorNioSelector(nioSelector)
            .unicastSenderFlowControl(UnicastSenderControlStrategy::new)
            .multicastSenderFlowControl(DefaultMulticastSenderControlStrategy::new)
            .conductorTimerWheel(new TimerWheel(MEDIA_CONDUCTOR_TICK_DURATION_US,
                                                TimeUnit.MICROSECONDS,
                                                MEDIA_CONDUCTOR_TICKS_PER_WHEEL))
            .conductorCommandQueue(new OneToOneConcurrentArrayQueue<>(1024))
            .receiverCommandQueue(new OneToOneConcurrentArrayQueue<>(1024))
            .connectedSubscriptions(new AtomicArray<>())
            .publications(publications)
            .termBufferManager(mockTermBufferManager)
            .statusBufferManager(mock(StatusBufferManager.class));

        ctx.fromClientCommands(fromClientCommands);
        ctx.clientProxy(mockClientProxy);

        ctx.receiverProxy(new ReceiverProxy(ctx.receiverCommandQueue()));
        ctx.mediaConductorProxy(new MediaConductorProxy(ctx.conductorCommandQueue()));

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

        assertThat(publications.size(), is(1));
        assertNotNull(publications.get(0));
        assertThat(publications.get(0).sessionId(), is(1L));
        assertThat(publications.get(0).channelId(), is(2L));

        verify(mockClientProxy).onNewLogBuffers(eq(ControlProtocolEvents.ON_NEW_PUBLICATION),
                                                eq(1L), eq(2L), anyLong(), eq(DESTINATION_URI + 4000),
                                                any(), anyLong(), anyInt());
    }

    @Test
    public void shouldBeAbleToAddSingleSubscriber() throws Exception
    {
        LOGGER.logInvocation();

        writeSubscriberMessage(ControlProtocolEvents.ADD_SUBSCRIPTION, DESTINATION_URI + 4000, CHANNEL_1);

        mediaConductor.doWork();
        receiver.doWork();

        assertNotNull(receiver.getFrameHandler(UdpDestination.parse(DESTINATION_URI + 4000)));
    }

    @Test
    public void shouldBeAbleToAddAndRemoveSingleSubscriber() throws Exception
    {
        LOGGER.logInvocation();

        writeSubscriberMessage(ControlProtocolEvents.ADD_SUBSCRIPTION, DESTINATION_URI + 4000, CHANNEL_1);
        writeSubscriberMessage(ControlProtocolEvents.REMOVE_SUBSCRIPTION, DESTINATION_URI + 4000, CHANNEL_1);

        mediaConductor.doWork();
        receiver.doWork();

        assertNull(receiver.getFrameHandler(UdpDestination.parse(DESTINATION_URI + 4000)));
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

        assertThat(publications.size(), is(4));
    }

    @Test
    public void shouldBeAbleToRemoveSingleChannel() throws Exception
    {
        LOGGER.logInvocation();

        writeChannelMessage(ControlProtocolEvents.ADD_PUBLICATION, 1L, 2L, 4005);
        writeChannelMessage(ControlProtocolEvents.REMOVE_PUBLICATION, 1L, 2L, 4005);

        mediaConductor.doWork();

        assertThat(publications.size(), is(0));
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

        assertThat(publications.size(), is(0));
    }

    @Test
    public void shouldKeepFrameHandlerUponRemovalOfAllButOneSubscriber() throws Exception
    {
        LOGGER.logInvocation();

        final UdpDestination destination = UdpDestination.parse(DESTINATION_URI + 4000);

        writeSubscriberMessage(ControlProtocolEvents.ADD_SUBSCRIPTION, DESTINATION_URI + 4000, CHANNEL_1);
        writeSubscriberMessage(ControlProtocolEvents.ADD_SUBSCRIPTION, DESTINATION_URI + 4000, CHANNEL_2);
        writeSubscriberMessage(ControlProtocolEvents.ADD_SUBSCRIPTION, DESTINATION_URI + 4000, CHANNEL_3);

        mediaConductor.doWork();
        receiver.doWork();

        final DataFrameHandler frameHandler = receiver.getFrameHandler(destination);

        assertNotNull(frameHandler);
        assertThat(frameHandler.subscriptionMap().size(), is(3));

        writeSubscriberMessage(ControlProtocolEvents.REMOVE_SUBSCRIPTION, DESTINATION_URI + 4000, CHANNEL_1);
        writeSubscriberMessage(ControlProtocolEvents.REMOVE_SUBSCRIPTION, DESTINATION_URI + 4000, CHANNEL_2);

        mediaConductor.doWork();
        receiver.doWork();

        assertNotNull(receiver.getFrameHandler(destination));
        assertThat(frameHandler.subscriptionMap().size(), is(1));
    }

    @Test
    public void shouldOnlyRemoveFrameHandlerUponRemovalOfAllSubscribers() throws Exception
    {
        LOGGER.logInvocation();

        final UdpDestination destination = UdpDestination.parse(DESTINATION_URI + 4000);

        writeSubscriberMessage(ControlProtocolEvents.ADD_SUBSCRIPTION, DESTINATION_URI + 4000, CHANNEL_1);
        writeSubscriberMessage(ControlProtocolEvents.ADD_SUBSCRIPTION, DESTINATION_URI + 4000, CHANNEL_2);
        writeSubscriberMessage(ControlProtocolEvents.ADD_SUBSCRIPTION, DESTINATION_URI + 4000, CHANNEL_3);

        mediaConductor.doWork();
        receiver.doWork();

        final DataFrameHandler frameHandler = receiver.getFrameHandler(destination);

        assertNotNull(frameHandler);
        assertThat(frameHandler.subscriptionMap().size(), is(3));

        writeSubscriberMessage(ControlProtocolEvents.REMOVE_SUBSCRIPTION, DESTINATION_URI + 4000, CHANNEL_2);
        writeSubscriberMessage(ControlProtocolEvents.REMOVE_SUBSCRIPTION, DESTINATION_URI + 4000, CHANNEL_3);

        mediaConductor.doWork();
        receiver.doWork();

        assertNotNull(receiver.getFrameHandler(destination));
        assertThat(frameHandler.subscriptionMap().size(), is(1));

        writeSubscriberMessage(ControlProtocolEvents.REMOVE_SUBSCRIPTION, DESTINATION_URI + 4000, CHANNEL_1);

        mediaConductor.doWork();
        receiver.doWork();

        assertNull(receiver.getFrameHandler(destination));
    }

    @Test
    public void shouldErrorOnAddDuplicateChannelOnExistingSession() throws Exception
    {
        LOGGER.logInvocation();

        writeChannelMessage(ControlProtocolEvents.ADD_PUBLICATION, 1L, 2L, 4000);
        writeChannelMessage(ControlProtocolEvents.ADD_PUBLICATION, 1L, 2L, 4000);

        mediaConductor.doWork();

        assertThat(publications.size(), is(1));
        assertNotNull(publications.get(0));
        assertThat(publications.get(0).sessionId(), is(1L));
        assertThat(publications.get(0).channelId(), is(2L));

        mockClientProxy.onError(eq(ErrorCode.PUBLICATION_CHANNEL_ALREADY_EXISTS), argThat(not(isEmptyOrNullString())), any(), anyInt());
    }

    @Test
    public void shouldErrorOnRemoveChannelOnUnknownDestination() throws Exception
    {
        LOGGER.logInvocation();

        writeChannelMessage(ControlProtocolEvents.REMOVE_PUBLICATION, 1L, 2L, 4000);

        mediaConductor.doWork();

        assertThat(publications.size(), is(0));

        mockClientProxy.onError(eq(INVALID_DESTINATION_IN_PUBLICATION), argThat(not(isEmptyOrNullString())), any(), anyInt());
    }

    @Test
    public void shouldErrorOnRemoveChannelOnUnknownSessionId() throws Exception
    {
        LOGGER.logInvocation();

        writeChannelMessage(ControlProtocolEvents.ADD_PUBLICATION, 1L, 2L, 4000);
        writeChannelMessage(ControlProtocolEvents.REMOVE_PUBLICATION, 2L, 2L, 4000);

        mediaConductor.doWork();

        assertThat(publications.size(), is(1));
        assertNotNull(publications.get(0));
        assertThat(publications.get(0).sessionId(), is(1L));
        assertThat(publications.get(0).channelId(), is(2L));

        mockClientProxy.onError(eq(PUBLICATION_CHANNEL_UNKNOWN), argThat(not(isEmptyOrNullString())), any(), anyInt());
    }

    @Test
    public void shouldErrorOnRemoveChannelOnUnknownChannelId() throws Exception
    {
        LOGGER.logInvocation();

        writeChannelMessage(ControlProtocolEvents.ADD_PUBLICATION, 1L, 2L, 4000);
        writeChannelMessage(ControlProtocolEvents.REMOVE_PUBLICATION, 1L, 3L, 4000);

        mediaConductor.doWork();

        assertThat(publications.size(), is(1));
        assertNotNull(publications.get(0));
        assertThat(publications.get(0).sessionId(), is(1L));
        assertThat(publications.get(0).channelId(), is(2L));

        mockClientProxy.onError(eq(PUBLICATION_CHANNEL_UNKNOWN), argThat(not(isEmptyOrNullString())), any(), anyInt());
    }

    @Test
    public void shouldErrorOnAddSubscriberWithInvalidUri() throws Exception
    {
        LOGGER.logInvocation();

        writeSubscriberMessage(ControlProtocolEvents.ADD_SUBSCRIPTION, INVALID_URI, CHANNEL_1);

        mediaConductor.doWork();
        receiver.doWork();
        mediaConductor.doWork();

        mockClientProxy.onError(eq(INVALID_DESTINATION_IN_PUBLICATION), argThat(not(isEmptyOrNullString())), any(), anyInt());
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

    private void writeSubscriberMessage(final int msgTypeId, final String destination, final long channelId)
        throws IOException
    {
        subscriptionMessage.wrap(writeBuffer, 0);
        subscriptionMessage.channelId(channelId)
                           .destination(destination);

        fromClientCommands.write(msgTypeId, writeBuffer, 0, subscriptionMessage.length());
    }
}
