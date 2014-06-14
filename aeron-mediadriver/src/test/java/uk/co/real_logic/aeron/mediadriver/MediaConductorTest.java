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
import uk.co.real_logic.aeron.util.ConductorShmBuffers;
import uk.co.real_logic.aeron.util.ErrorCode;
import uk.co.real_logic.aeron.util.TimerWheel;
import uk.co.real_logic.aeron.util.command.PublisherMessageFlyweight;
import uk.co.real_logic.aeron.util.command.ControlProtocolEvents;
import uk.co.real_logic.aeron.util.command.NewBufferMessageFlyweight;
import uk.co.real_logic.aeron.util.command.SubscriberMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.OneToOneConcurrentArrayQueue;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.ManyToOneRingBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferDescriptor;
import uk.co.real_logic.aeron.util.protocol.ErrorHeaderFlyweight;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.co.real_logic.aeron.mediadriver.MediaDriver.MEDIA_CONDUCTOR_TICKS_PER_WHEEL;
import static uk.co.real_logic.aeron.mediadriver.MediaDriver.MEDIA_CONDUCTOR_TICK_DURATION_US;

/**
 * Test the Media Driver Conductor in isolation
 *
 * Tests for interaction of media conductor with client protocol
 */
public class MediaConductorTest
{
    private static final String DESTINATION_URI = "udp://localhost:";
    private static final String INVALID_URI = "udp://";
    private static final long[] ONE_CHANNEL = {10};
    private static final long[] ANOTHER_CHANNEL = {20};
    private static final long[] TWO_CHANNELS = {20, 30};
    private static final long[] THREE_CHANNELS = {10, 20, 30};

    private final ConductorShmBuffers mockConductorShmBuffers = mock(ConductorShmBuffers.class);
    private final ByteBuffer toDriverBuffer =
        ByteBuffer.allocate(MediaDriver.COMMAND_BUFFER_SZ + RingBufferDescriptor.TRAILER_LENGTH);
    private final ByteBuffer toClientBuffer =
        ByteBuffer.allocate(MediaDriver.COMMAND_BUFFER_SZ + RingBufferDescriptor.TRAILER_LENGTH);

    private final NioSelector nioSelector = mock(NioSelector.class);
    private final BufferManagement mockBufferManagement = mock(BufferManagement.class);

    private final RingBuffer conductorCommands = new ManyToOneRingBuffer(new AtomicBuffer(toDriverBuffer));
    private final RingBuffer conductorNotifications = new ManyToOneRingBuffer(new AtomicBuffer(toClientBuffer));

    private final PublisherMessageFlyweight publisherMessage = new PublisherMessageFlyweight();
    private final NewBufferMessageFlyweight bufferMessage = new NewBufferMessageFlyweight();
    private final ErrorHeaderFlyweight errorHeader = new ErrorHeaderFlyweight();
    private final SubscriberMessageFlyweight subscriberMessage = new SubscriberMessageFlyweight();

    private final AtomicBuffer writeBuffer = new AtomicBuffer(ByteBuffer.allocate(256));

    private MediaConductor mediaConductor;
    private Sender sender;
    private Receiver receiver;

    @Before
    public void setUp() throws Exception
    {
        when(mockConductorShmBuffers.toDriver()).thenReturn(toDriverBuffer);
        when(mockConductorShmBuffers.toClient()).thenReturn(toClientBuffer);
        when(mockBufferManagement.addPublisherChannel(anyObject(), anyLong(), anyLong()))
                .thenReturn(BufferAndFrameUtils.createTestRotator(65536 + RingBufferDescriptor.TRAILER_LENGTH,
                                                                  LogBufferDescriptor.STATE_BUFFER_LENGTH));

        final MediaDriver.Context ctx = new MediaDriver.Context()
            .conductorCommandBuffer(MediaDriver.COMMAND_BUFFER_SZ)
            .receiverCommandBuffer(MediaDriver.COMMAND_BUFFER_SZ)
            .receiverNioSelector(nioSelector)
            .conductorNioSelector(nioSelector)
            .senderFlowControl(UnicastSenderControlStrategy::new)
            .conductorShmBuffers(mockConductorShmBuffers)
            .conductorTimerWheel(new TimerWheel(MEDIA_CONDUCTOR_TICK_DURATION_US,
                                                TimeUnit.MICROSECONDS,
                                                MEDIA_CONDUCTOR_TICKS_PER_WHEEL))
            .newReceiveBufferEventQueue(new OneToOneConcurrentArrayQueue<>(1024))
            .bufferManagement(mockBufferManagement);

        ctx.receiverProxy(new ReceiverProxy(ctx.receiverCommandBuffer(),
                                            ctx.conductorNioSelector(),
                                            ctx.newReceiveBufferEventQueue()));

        ctx.mediaConductorProxy(new MediaConductorProxy(ctx.mediaCommandBuffer(), ctx.conductorNioSelector()));

        sender = new Sender();
        receiver = new Receiver(ctx);
        mediaConductor = new MediaConductor(ctx, receiver, sender);
    }

    @After
    public void tearDown() throws Exception
    {
        receiver.close();
        receiver.nioSelector().selectNowWithNoProcessing();
        mediaConductor.close();
        mediaConductor.nioSelector().selectNowWithNoProcessing();
    }

    @Test
    public void shouldBeAbleToAddSingleChannel() throws Exception
    {
        writeChannelMessage(ControlProtocolEvents.ADD_CHANNEL, 1L, 2L, 4000);

        mediaConductor.doWork();

        assertThat(sender.channels().length(), is(1));
        assertNotNull(sender.channels().get(0));
        assertThat(sender.channels().get(0).sessionId(), is(1L));
        assertThat(sender.channels().get(0).channelId(), is(2L));

        final int messagesRead = conductorNotifications.read(
            (msgTypeId, buffer, index, length) ->
            {
                assertThat(msgTypeId, is(ControlProtocolEvents.NEW_SEND_BUFFER_NOTIFICATION));

                bufferMessage.wrap(buffer, index);

                assertThat(bufferMessage.sessionId(), is(1L));
                assertThat(bufferMessage.channelId(), is(2L));
                assertThat(bufferMessage.destination(), is(DESTINATION_URI + 4000));
            });

        assertThat(messagesRead, is(1));
    }

    @Test
    public void shouldBeAbleToAddSingleSubscriber() throws Exception
    {
        writeSubscriberMessage(ControlProtocolEvents.ADD_SUBSCRIBER, DESTINATION_URI + 4000, ONE_CHANNEL);

        mediaConductor.doWork();
        receiver.doWork();

        assertNotNull(receiver.frameHandler(UdpDestination.parse(DESTINATION_URI + 4000)));
    }

    @Test
    public void shouldBeAbleToAddAndRemoveSingleSubscriber() throws Exception
    {
        writeSubscriberMessage(ControlProtocolEvents.ADD_SUBSCRIBER, DESTINATION_URI + 4000, ONE_CHANNEL);
        writeSubscriberMessage(ControlProtocolEvents.REMOVE_SUBSCRIBER, DESTINATION_URI + 4000, ONE_CHANNEL);

        mediaConductor.doWork();
        receiver.doWork();

        assertNull(receiver.frameHandler(UdpDestination.parse(DESTINATION_URI + 4000)));
    }

    @Test
    public void shouldBeAbleToAddMultipleChannels() throws Exception
    {
        writeChannelMessage(ControlProtocolEvents.ADD_CHANNEL, 1L, 2L, 4001);
        writeChannelMessage(ControlProtocolEvents.ADD_CHANNEL, 1L, 3L, 4002);
        writeChannelMessage(ControlProtocolEvents.ADD_CHANNEL, 3L, 2L, 4003);
        writeChannelMessage(ControlProtocolEvents.ADD_CHANNEL, 3L, 4L, 4004);

        mediaConductor.doWork();

        assertThat(sender.channels().length(), is(4));
    }

    @Test
    public void shouldBeAbleToRemoveSingleChannel() throws Exception
    {
        writeChannelMessage(ControlProtocolEvents.ADD_CHANNEL, 1L, 2L, 4005);
        writeChannelMessage(ControlProtocolEvents.REMOVE_CHANNEL, 1L, 2L, 4005);

        mediaConductor.doWork();

        assertThat(sender.channels().length(), is(0));
        assertNull(mediaConductor.frameHandler(UdpDestination.parse(DESTINATION_URI + 4005)));
    }

    @Test
    public void shouldBeAbleToRemoveMultipleChannels() throws IOException
    {
        writeChannelMessage(ControlProtocolEvents.ADD_CHANNEL, 1L, 2L, 4006);
        writeChannelMessage(ControlProtocolEvents.ADD_CHANNEL, 1L, 3L, 4007);
        writeChannelMessage(ControlProtocolEvents.ADD_CHANNEL, 3L, 2L, 4008);
        writeChannelMessage(ControlProtocolEvents.ADD_CHANNEL, 3L, 4L, 4008);

        writeChannelMessage(ControlProtocolEvents.REMOVE_CHANNEL, 1L, 2L, 4006);
        writeChannelMessage(ControlProtocolEvents.REMOVE_CHANNEL, 1L, 3L, 4007);
        writeChannelMessage(ControlProtocolEvents.REMOVE_CHANNEL, 3L, 2L, 4008);
        writeChannelMessage(ControlProtocolEvents.REMOVE_CHANNEL, 3L, 4L, 4008);

        mediaConductor.doWork();

        assertThat(sender.channels().length(), is(0));
    }

    @Test
    public void shouldKeepFrameHandlerUponRemovalOfAllButOneSubscriber() throws Exception
    {
        final UdpDestination destination = UdpDestination.parse(DESTINATION_URI + 4000);

        writeSubscriberMessage(ControlProtocolEvents.ADD_SUBSCRIBER, DESTINATION_URI + 4000, THREE_CHANNELS);

        mediaConductor.doWork();
        receiver.doWork();

        final DataFrameHandler frameHandler = receiver.frameHandler(destination);

        assertNotNull(frameHandler);
        assertThat(frameHandler.subscriptionMap().size(), is(3));

        writeSubscriberMessage(ControlProtocolEvents.REMOVE_SUBSCRIBER, DESTINATION_URI + 4000, TWO_CHANNELS);

        mediaConductor.doWork();
        receiver.doWork();

        assertNotNull(receiver.frameHandler(destination));
        assertThat(frameHandler.subscriptionMap().size(), is(1));
    }

    @Test
    public void shouldOnlyRemoveFrameHandlerUponRemovalOfAllSubscribers() throws Exception
    {
        final UdpDestination destination = UdpDestination.parse(DESTINATION_URI + 4000);

        writeSubscriberMessage(ControlProtocolEvents.ADD_SUBSCRIBER, DESTINATION_URI + 4000, THREE_CHANNELS);

        mediaConductor.doWork();
        receiver.doWork();

        final DataFrameHandler frameHandler = receiver.frameHandler(destination);

        assertNotNull(frameHandler);
        assertThat(frameHandler.subscriptionMap().size(), is(3));

        writeSubscriberMessage(ControlProtocolEvents.REMOVE_SUBSCRIBER, DESTINATION_URI + 4000, TWO_CHANNELS);

        mediaConductor.doWork();
        receiver.doWork();

        assertNotNull(receiver.frameHandler(destination));
        assertThat(frameHandler.subscriptionMap().size(), is(1));

        writeSubscriberMessage(ControlProtocolEvents.REMOVE_SUBSCRIBER, DESTINATION_URI + 4000, ONE_CHANNEL);

        mediaConductor.doWork();
        receiver.doWork();

        assertNull(receiver.frameHandler(destination));
    }

    @Test
    public void shouldErrorOnAddDuplicateChannelOnExistingSession() throws Exception
    {
        writeChannelMessage(ControlProtocolEvents.ADD_CHANNEL, 1L, 2L, 4000);
        writeChannelMessage(ControlProtocolEvents.ADD_CHANNEL, 1L, 2L, 4000);

        mediaConductor.doWork();

        assertThat(sender.channels().length(), is(1));
        assertNotNull(sender.channels().get(0));
        assertThat(sender.channels().get(0).sessionId(), is(1L));
        assertThat(sender.channels().get(0).channelId(), is(2L));

        conductorNotifications.read((msgTypeId, buffer, index, length) -> {}, 1); // eat new buffer notification

        final int messagesRead = conductorNotifications.read(
            (msgTypeId, buffer, index, length) ->
            {
                assertThat(msgTypeId, is(ControlProtocolEvents.ERROR_RESPONSE));

                errorHeader.wrap(buffer, index);
                assertThat(errorHeader.errorCode(), is(ErrorCode.CHANNEL_ALREADY_EXISTS));
                assertThat(errorHeader.errorStringLength(), greaterThan(0));

                publisherMessage.wrap(buffer, errorHeader.offendingHeaderOffset());
                assertThat(publisherMessage.sessionId(), is(1L));
                assertThat(publisherMessage.channelId(), is(2L));
                assertThat(publisherMessage.destination(), is(DESTINATION_URI + 4000));
            });

        assertThat(messagesRead, is(1));
    }

    @Test
    public void shouldErrorOnRemoveChannelOnUnknownDestination() throws Exception
    {
        writeChannelMessage(ControlProtocolEvents.REMOVE_CHANNEL, 1L, 2L, 4000);

        mediaConductor.doWork();

        assertThat(sender.channels().length(), is(0));

        final int messagesRead = conductorNotifications.read(
            (msgTypeId, buffer, index, length) ->
            {
                assertThat(msgTypeId, is(ControlProtocolEvents.ERROR_RESPONSE));

                errorHeader.wrap(buffer, index);
                assertThat(errorHeader.errorCode(), is(ErrorCode.INVALID_DESTINATION));
                assertThat(errorHeader.errorStringLength(), greaterThan(0));

                publisherMessage.wrap(buffer, errorHeader.offendingHeaderOffset());
                assertThat(publisherMessage.sessionId(), is(1L));
                assertThat(publisherMessage.channelId(), is(2L));
                assertThat(publisherMessage.destination(), is(DESTINATION_URI + 4000));
            });

        assertThat(messagesRead, is(1));
    }

    @Test
    public void shouldErrorOnRemoveChannelOnUnknownSessionId() throws Exception
    {
        writeChannelMessage(ControlProtocolEvents.ADD_CHANNEL, 1L, 2L, 4000);
        writeChannelMessage(ControlProtocolEvents.REMOVE_CHANNEL, 2L, 2L, 4000);

        mediaConductor.doWork();

        assertThat(sender.channels().length(), is(1));
        assertNotNull(sender.channels().get(0));
        assertThat(sender.channels().get(0).sessionId(), is(1L));
        assertThat(sender.channels().get(0).channelId(), is(2L));

        conductorNotifications.read((msgTypeId, buffer, index, length) -> {}, 1); // eat new buffer notification

        final int messagesRead = conductorNotifications.read(
            (msgTypeId, buffer, index, length) ->
            {
                assertThat(msgTypeId, is(ControlProtocolEvents.ERROR_RESPONSE));

                errorHeader.wrap(buffer, index);
                assertThat(errorHeader.errorCode(), is(ErrorCode.CHANNEL_UNKNOWN));
                assertThat(errorHeader.errorStringLength(), greaterThan(0));

                publisherMessage.wrap(buffer, errorHeader.offendingHeaderOffset());
                assertThat(publisherMessage.sessionId(), is(2L));
                assertThat(publisherMessage.channelId(), is(2L));
                assertThat(publisherMessage.destination(), is(DESTINATION_URI + 4000));
            });

        assertThat(messagesRead, is(1));
    }

    @Test
    public void shouldErrorOnRemoveChannelOnUnknownChannelId() throws Exception
    {
        writeChannelMessage(ControlProtocolEvents.ADD_CHANNEL, 1L, 2L, 4000);
        writeChannelMessage(ControlProtocolEvents.REMOVE_CHANNEL, 1L, 3L, 4000);

        mediaConductor.doWork();

        assertThat(sender.channels().length(), is(1));
        assertNotNull(sender.channels().get(0));
        assertThat(sender.channels().get(0).sessionId(), is(1L));
        assertThat(sender.channels().get(0).channelId(), is(2L));

        conductorNotifications.read((msgTypeId, buffer, index, length) -> {}, 1); // eat new buffer notification

        final int messagesRead = conductorNotifications.read(
            (msgTypeId, buffer, index, length) ->
            {
                assertThat(msgTypeId, is(ControlProtocolEvents.ERROR_RESPONSE));

                errorHeader.wrap(buffer, index);
                assertThat(errorHeader.errorCode(), is(ErrorCode.CHANNEL_UNKNOWN));
                assertThat(errorHeader.errorStringLength(), greaterThan(0));

                publisherMessage.wrap(buffer, errorHeader.offendingHeaderOffset());
                assertThat(publisherMessage.sessionId(), is(1L));
                assertThat(publisherMessage.channelId(), is(3L));
                assertThat(publisherMessage.destination(), is(DESTINATION_URI + 4000));
            });

        assertThat(messagesRead, is(1));
    }

    @Test
    public void shouldErrorOnAddSubscriberWithInvalidUri() throws Exception
    {
        writeSubscriberMessage(ControlProtocolEvents.ADD_SUBSCRIBER, INVALID_URI, ONE_CHANNEL);

        mediaConductor.doWork();
        receiver.doWork();
        mediaConductor.doWork();

        final int messagesRead = conductorNotifications.read(
            (msgTypeId, buffer, index, length) ->
            {
                assertThat(msgTypeId, is(ControlProtocolEvents.ERROR_RESPONSE));

                errorHeader.wrap(buffer, index);
                assertThat(errorHeader.errorCode(), is(ErrorCode.INVALID_DESTINATION));
                assertThat(errorHeader.errorStringLength(), is(0));

                subscriberMessage.wrap(buffer, errorHeader.offendingHeaderOffset());
                assertThat(subscriberMessage.channelIds(), is(ONE_CHANNEL));
                assertThat(subscriberMessage.destination(), is(INVALID_URI));
            });

        assertThat(messagesRead, is(1));
    }

    @Test
    public void shouldSendErrorOnRemoveSubscriberWithNonExistentDestination() throws Exception
    {
        writeSubscriberMessage(ControlProtocolEvents.REMOVE_SUBSCRIBER, DESTINATION_URI + 4000, ONE_CHANNEL);

        mediaConductor.doWork();
        receiver.doWork();
        mediaConductor.doWork();

        final int messagesRead = conductorNotifications.read(
            (msgTypeId, buffer, index, length) ->
            {
                assertThat(msgTypeId, is(ControlProtocolEvents.ERROR_RESPONSE));

                errorHeader.wrap(buffer, index);
                assertThat(errorHeader.errorCode(), is(ErrorCode.SUBSCRIBER_NOT_REGISTERED));
                assertThat(errorHeader.errorStringLength(), is(0));

                subscriberMessage.wrap(buffer, errorHeader.offendingHeaderOffset());
                assertThat(subscriberMessage.channelIds(), is(ONE_CHANNEL));
                assertThat(subscriberMessage.destination(), is(DESTINATION_URI + 4000));
            });

        assertThat(messagesRead, is(1));
    }

    @Test
    public void shouldSendErrorOnRemoveSubscriberFromWrongChannels() throws Exception
    {
        writeSubscriberMessage(ControlProtocolEvents.ADD_SUBSCRIBER, DESTINATION_URI + 4000, ONE_CHANNEL);
        writeSubscriberMessage(ControlProtocolEvents.REMOVE_SUBSCRIBER, DESTINATION_URI + 4000, ANOTHER_CHANNEL);

        mediaConductor.doWork();
        receiver.doWork();
        mediaConductor.doWork();

        final int messagesRead = conductorNotifications.read(
            (msgTypeId, buffer, index, length) ->
            {
                assertThat(msgTypeId, is(ControlProtocolEvents.ERROR_RESPONSE));

                errorHeader.wrap(buffer, index);
                assertThat(errorHeader.errorCode(), is(ErrorCode.SUBSCRIBER_NOT_REGISTERED));
                assertThat(errorHeader.errorStringLength(), is(0));

                subscriberMessage.wrap(buffer, errorHeader.offendingHeaderOffset());
                assertThat(subscriberMessage.channelIds(), is(ANOTHER_CHANNEL));
                assertThat(subscriberMessage.destination(), is(DESTINATION_URI + 4000));
            });

        assertThat(messagesRead, is(1));
    }

    private void writeChannelMessage(final int msgTypeId, final long sessionId, final long channelId, final int port)
        throws IOException
    {
        publisherMessage.wrap(writeBuffer, 0);
        publisherMessage.channelId(channelId);
        publisherMessage.sessionId(sessionId);
        publisherMessage.destination(DESTINATION_URI + port);

        conductorCommands.write(msgTypeId, writeBuffer, 0, publisherMessage.length());
    }

    private void writeSubscriberMessage(final int msgTypeId, final String destination, final long[] channelIds)
            throws IOException
    {
        subscriberMessage.wrap(writeBuffer, 0);
        subscriberMessage.channelIds(channelIds)
                         .destination(destination);

        conductorCommands.write(msgTypeId, writeBuffer, 0, subscriberMessage.length());
    }
}
