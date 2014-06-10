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
import uk.co.real_logic.aeron.util.command.ChannelMessageFlyweight;
import uk.co.real_logic.aeron.util.command.ControlProtocolEvents;
import uk.co.real_logic.aeron.util.command.NewBufferMessageFlyweight;
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
 */
public class MediaConductorTest
{
    private static final String DESTINATION_URI = "udp://localhost:";

    private final ConductorShmBuffers mockConductorShmBuffers = mock(ConductorShmBuffers.class);
    private final ByteBuffer toDriverBuffer = ByteBuffer.allocate(MediaDriver.COMMAND_BUFFER_SZ +
        RingBufferDescriptor.TRAILER_LENGTH);
    private final ByteBuffer toClientBuffer = ByteBuffer.allocate(MediaDriver.COMMAND_BUFFER_SZ +
        RingBufferDescriptor.TRAILER_LENGTH);

    private final NioSelector nioSelector = mock(NioSelector.class);

    private final BufferManagement mockBufferManagement = mock(BufferManagement.class);

    final RingBuffer conductorCommands = new ManyToOneRingBuffer(new AtomicBuffer(toDriverBuffer));
    final ChannelMessageFlyweight channelMessage = new ChannelMessageFlyweight();
    final RingBuffer conductorNotifications = new ManyToOneRingBuffer(new AtomicBuffer(toClientBuffer));
    private final NewBufferMessageFlyweight bufferMessage = new NewBufferMessageFlyweight();
    private final ErrorHeaderFlyweight errorHeader = new ErrorHeaderFlyweight();

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
        sender = new Sender();
        receiver = mock(Receiver.class);
        mediaConductor = new MediaConductor(ctx, receiver, sender);
    }

    @After
    public void teardown() throws Exception
    {
        mediaConductor.close();
        mediaConductor.nioSelector().selectNowWithNoProcessing();
    }

    @Test
    public void shouldNotifySenderThreadOnAddingChannel() throws Exception
    {
        writeChannelMessage(ControlProtocolEvents.ADD_CHANNEL, 1L, 2L, 4000);

        mediaConductor.process();

        assertThat(sender.channels().length(), is(1));
        assertNotNull(sender.channels().get(0));
        assertThat(sender.channels().get(0).sessionId(), is(1L));
        assertThat(sender.channels().get(0).channelId(), is(2L));

        final int msgs = conductorNotifications.read(
            (msgTypeId, buffer, index, length) ->
            {
                assertThat(msgTypeId, is(ControlProtocolEvents.NEW_SEND_BUFFER_NOTIFICATION));

                bufferMessage.wrap(buffer, index);

                assertThat(bufferMessage.sessionId(), is(1L));
                assertThat(bufferMessage.channelId(), is(2L));
                assertThat(bufferMessage.destination(), is(DESTINATION_URI + 4000));
            });
        assertThat(msgs, is(1));
    }

    @Test
    public void shouldNotifySenderThreadUponAddingMultipleChannels() throws Exception
    {

        writeChannelMessage(ControlProtocolEvents.ADD_CHANNEL, 1L, 2L, 4001);
        writeChannelMessage(ControlProtocolEvents.ADD_CHANNEL, 1L, 3L, 4002);
        writeChannelMessage(ControlProtocolEvents.ADD_CHANNEL, 3L, 2L, 4003);
        writeChannelMessage(ControlProtocolEvents.ADD_CHANNEL, 3L, 4L, 4004);

        mediaConductor.process();

        assertThat(sender.channels().length(), is(4));
    }

    @Test
    public void shouldNotifySenderThreadUponRemovingChannel() throws Exception
    {
        writeChannelMessage(ControlProtocolEvents.ADD_CHANNEL, 1L, 2L, 4005);
        writeChannelMessage(ControlProtocolEvents.REMOVE_CHANNEL, 1L, 2L, 4005);

        mediaConductor.process();

        assertThat(sender.channels().length(), is(0));
        assertNull(mediaConductor.frameHandler(UdpDestination.parse(DESTINATION_URI + 4005)));
    }

    @Test
    public void shouldNotifySenderThreadUponRemovingMultipleChannels() throws IOException
    {
        writeChannelMessage(ControlProtocolEvents.ADD_CHANNEL, 1L, 2L, 4006);
        writeChannelMessage(ControlProtocolEvents.ADD_CHANNEL, 1L, 3L, 4007);
        writeChannelMessage(ControlProtocolEvents.ADD_CHANNEL, 3L, 2L, 4008);
        writeChannelMessage(ControlProtocolEvents.ADD_CHANNEL, 3L, 4L, 4008);

        writeChannelMessage(ControlProtocolEvents.REMOVE_CHANNEL, 1L, 2L, 4006);
        writeChannelMessage(ControlProtocolEvents.REMOVE_CHANNEL, 1L, 3L, 4007);
        writeChannelMessage(ControlProtocolEvents.REMOVE_CHANNEL, 3L, 2L, 4008);
        writeChannelMessage(ControlProtocolEvents.REMOVE_CHANNEL, 3L, 4L, 4008);

        mediaConductor.process();

        assertThat(sender.channels().length(), is(0));
    }

    @Test
    public void shouldErrorOnAddDuplicateChannelOnExistingSession() throws Exception
    {
        writeChannelMessage(ControlProtocolEvents.ADD_CHANNEL, 1L, 2L, 4000);
        writeChannelMessage(ControlProtocolEvents.ADD_CHANNEL, 1L, 2L, 4000);

        mediaConductor.process();

        assertThat(sender.channels().length(), is(1));
        assertNotNull(sender.channels().get(0));
        assertThat(sender.channels().get(0).sessionId(), is(1L));
        assertThat(sender.channels().get(0).channelId(), is(2L));

        conductorNotifications.read((msgTypeId, buffer, index, length) -> {}, 0); // eat new buffer notification
        final int msgs = conductorNotifications.read(
            (msgTypeId, buffer, index, length) ->
            {
                assertThat(msgTypeId, is(ControlProtocolEvents.ERROR_RESPONSE));

                errorHeader.wrap(buffer, index);
                assertThat(errorHeader.errorCode(), is(ErrorCode.CHANNEL_ALREADY_EXISTS));
                assertThat(errorHeader.errorStringLength(), greaterThan(0));

                channelMessage.wrap(buffer, errorHeader.offendingHeaderOffset());
                assertThat(channelMessage.sessionId(), is(1L));
                assertThat(channelMessage.channelId(), is(2L));
                assertThat(channelMessage.destination(), is(DESTINATION_URI + 4000));
            });
        assertThat(msgs, is(1));
    }

    @Test
    public void shouldErrorOnRemoveChannelOnUnknownDestination() throws Exception
    {
        writeChannelMessage(ControlProtocolEvents.REMOVE_CHANNEL, 1L, 2L, 4000);

        mediaConductor.process();

        assertThat(sender.channels().length(), is(0));
        final int msgs = conductorNotifications.read(
                (msgTypeId, buffer, index, length) ->
                {
                    assertThat(msgTypeId, is(ControlProtocolEvents.ERROR_RESPONSE));

                    errorHeader.wrap(buffer, index);
                    assertThat(errorHeader.errorCode(), is(ErrorCode.INVALID_DESTINATION));
                    assertThat(errorHeader.errorStringLength(), greaterThan(0));

                    channelMessage.wrap(buffer, errorHeader.offendingHeaderOffset());
                    assertThat(channelMessage.sessionId(), is(1L));
                    assertThat(channelMessage.channelId(), is(2L));
                    assertThat(channelMessage.destination(), is(DESTINATION_URI + 4000));
                });
        assertThat(msgs, is(1));
    }

    @Test
    public void shouldErrorOnRemoveChannelOnUnknownSessionId() throws Exception
    {
        writeChannelMessage(ControlProtocolEvents.ADD_CHANNEL, 1L, 2L, 4000);
        writeChannelMessage(ControlProtocolEvents.REMOVE_CHANNEL, 2L, 2L, 4000);

        mediaConductor.process();

        assertThat(sender.channels().length(), is(1));
        assertNotNull(sender.channels().get(0));
        assertThat(sender.channels().get(0).sessionId(), is(1L));
        assertThat(sender.channels().get(0).channelId(), is(2L));

        conductorNotifications.read((msgTypeId, buffer, index, length) -> {}, 0); // eat new buffer notification
        final int msgs = conductorNotifications.read(
                (msgTypeId, buffer, index, length) ->
                {
                    assertThat(msgTypeId, is(ControlProtocolEvents.ERROR_RESPONSE));

                    errorHeader.wrap(buffer, index);
                    assertThat(errorHeader.errorCode(), is(ErrorCode.CHANNEL_UNKNOWN));
                    assertThat(errorHeader.errorStringLength(), greaterThan(0));

                    channelMessage.wrap(buffer, errorHeader.offendingHeaderOffset());
                    assertThat(channelMessage.sessionId(), is(2L));
                    assertThat(channelMessage.channelId(), is(2L));
                    assertThat(channelMessage.destination(), is(DESTINATION_URI + 4000));
                });
        assertThat(msgs, is(1));
    }

    @Test
    public void shouldErrorOnRemoveChannelOnUnknownChannelId() throws Exception
    {
        writeChannelMessage(ControlProtocolEvents.ADD_CHANNEL, 1L, 2L, 4000);
        writeChannelMessage(ControlProtocolEvents.REMOVE_CHANNEL, 1L, 3L, 4000);

        mediaConductor.process();

        assertThat(sender.channels().length(), is(1));
        assertNotNull(sender.channels().get(0));
        assertThat(sender.channels().get(0).sessionId(), is(1L));
        assertThat(sender.channels().get(0).channelId(), is(2L));

        conductorNotifications.read((msgTypeId, buffer, index, length) -> {}, 0); // eat new buffer notification
        final int msgs = conductorNotifications.read(
                (msgTypeId, buffer, index, length) ->
                {
                    assertThat(msgTypeId, is(ControlProtocolEvents.ERROR_RESPONSE));

                    errorHeader.wrap(buffer, index);
                    assertThat(errorHeader.errorCode(), is(ErrorCode.CHANNEL_UNKNOWN));
                    assertThat(errorHeader.errorStringLength(), greaterThan(0));

                    channelMessage.wrap(buffer, errorHeader.offendingHeaderOffset());
                    assertThat(channelMessage.sessionId(), is(1L));
                    assertThat(channelMessage.channelId(), is(3L));
                    assertThat(channelMessage.destination(), is(DESTINATION_URI + 4000));
                });
        assertThat(msgs, is(1));
    }

    private void writeChannelMessage(final int msgTypeId, final long sessionId, final long channelId, final int port)
        throws IOException
    {
        channelMessage.wrap(writeBuffer, 0);
        channelMessage.channelId(channelId);
        channelMessage.sessionId(sessionId);
        channelMessage.destination(DESTINATION_URI + port);

        conductorCommands.write(msgTypeId, writeBuffer, 0, channelMessage.length());
    }
}
