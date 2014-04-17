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
package uk.co.real_logic.aeron;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import uk.co.real_logic.aeron.admin.ClientAdminThread;
import uk.co.real_logic.aeron.util.AdminBuffers;
import uk.co.real_logic.aeron.util.ErrorCode;
import uk.co.real_logic.aeron.util.MappingAdminBufferStrategy;
import uk.co.real_logic.aeron.util.SharedDirectories;
import uk.co.real_logic.aeron.util.command.ChannelMessageFlyweight;
import uk.co.real_logic.aeron.util.command.CompletelyIdentifiedMessageFlyweight;
import uk.co.real_logic.aeron.util.command.ReceiverMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.EventHandler;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.util.protocol.ErrorHeaderFlyweight;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.*;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferTestUtil.assertEventRead;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferTestUtil.skip;

public class AeronTest
{

    private static final String DESTINATION = "udp://localhost:40124";
    private static final String INVALID_DESTINATION = "udp://lo124";
    private static final long CHANNEL_ID = 2L;
    private static final long CHANNEL_ID_2 = 4L;
    private static final long[] CHANNEL_IDs = { CHANNEL_ID, CHANNEL_ID_2};
    private static final long SESSION_ID = 3L;

    @ClassRule
    public static SharedDirectories directory = new SharedDirectories();

    @ClassRule
    public static AdminBuffers adminBuffers = new AdminBuffers();

    private final InvalidDestinationHandler invalidDestination = mock(InvalidDestinationHandler.class);

    private final ChannelMessageFlyweight message = new ChannelMessageFlyweight();
    private final CompletelyIdentifiedMessageFlyweight identifiedMessage = new CompletelyIdentifiedMessageFlyweight();
    private final ReceiverMessageFlyweight receiverMessage = new ReceiverMessageFlyweight();

    private final ErrorHeaderFlyweight errorHeader = new ErrorHeaderFlyweight();

    private final ByteBuffer sendBuffer = ByteBuffer.allocate(256);
    private final AtomicBuffer atomicSendBuffer = new AtomicBuffer(sendBuffer);

    public AeronTest()
    {
        identifiedMessage.wrap(atomicSendBuffer, 0);
        errorHeader.wrap(atomicSendBuffer, 0);
    }

    @Test
    public void creatingChannelsShouldNotifyMediaDriver() throws Exception
    {
        final Aeron aeron = newAeron();
        newChannel(aeron);
        aeron.adminThread().process();

        assertChannelMessage(adminBuffers.toMediaDriver(), ADD_CHANNEL);
    }

    @Test
    public void cannotOfferOnChannelUntilBuffersMapped() throws Exception
    {
        final Aeron aeron = newAeron();
        final Channel channel = newChannel(aeron);
        assertFalse(channel.offer(atomicSendBuffer));
        assertFalse(channel.offer(atomicSendBuffer, 0, 1));
    }

    @Test(expected=BufferExhaustedException.class)
    public void cannotSendOnChannelUntilBuffersMapped() throws Exception
    {
        final Aeron aeron = newAeron();
        final Channel channel = newChannel(aeron);
        channel.send(atomicSendBuffer);
    }

    @Test
    public void canOfferAMessageOnceBuffersHaveBeenMapped() throws Exception
    {
        final Aeron aeron = newAeron();
        final Channel channel = newChannel(aeron);
        aeron.adminThread().process();
        createTermBuffer(0L);
        aeron.adminThread().process();
        assertTrue(channel.offer(atomicSendBuffer));
    }

    @Test
    public void removingChannelsShouldNotifyMediaDriver() throws Exception
    {
        final RingBuffer buffer = adminBuffers.toMediaDriver();
        final Aeron aeron = newAeron();
        final Channel channel = newChannel(aeron);
        final ClientAdminThread adminThread = aeron.adminThread();

        adminThread.process();
        skip(buffer, 1);

        channel.close();
        adminThread.process();

        assertChannelMessage(buffer, REMOVE_CHANNEL);
    }

    @Test
    public void closingASourceRemovesItsAssociatedChannels() throws Exception
    {
        final RingBuffer buffer = adminBuffers.toMediaDriver();
        final Aeron aeron = newAeron();
        final Source.Builder sourceBuilder = new Source.Builder()
            .sessionId(SESSION_ID)
            .destination(new Destination(DESTINATION));
        final Source source = aeron.newSource(sourceBuilder);
        final Channel channel = source.newChannel(CHANNEL_ID);
        final ClientAdminThread adminThread = aeron.adminThread();

        adminThread.process();
        skip(buffer, 1);

        source.close();
        adminThread.process();

        assertChannelMessage(buffer, REMOVE_CHANNEL);
    }

    @Test
    public void closingASourceDoesNotRemoveOtherChannels() throws Exception
    {
        final RingBuffer buffer = adminBuffers.toMediaDriver();
        final Aeron aeron = newAeron();
        final Source source = aeron.newSource(new Source.Builder()
                .sessionId(SESSION_ID)
                .destination(new Destination(DESTINATION)));
        final Source otherSource = aeron.newSource(new Source.Builder()
                .sessionId(SESSION_ID + 1)
                .destination(new Destination(DESTINATION)));
        final Channel channel = source.newChannel(CHANNEL_ID);
        final ClientAdminThread adminThread = aeron.adminThread();

        adminThread.process();
        skip(buffer, 1);

        otherSource.close();
        adminThread.process();

        skip(buffer, 0);
    }

    @Test
    public void registeringReceiverNotifiesMediaDriver()
    {
        final RingBuffer toMediaDriver = adminBuffers.toMediaDriver();
        final Aeron aeron = newAeron();
        final Receiver.Builder builder = new Receiver.Builder()
                .destination(new Destination(DESTINATION))
                .channel(CHANNEL_ID, emptyDataHandler())
                .channel(CHANNEL_ID_2, emptyDataHandler());

        aeron.newReceiver(builder);

        aeron.adminThread().process();

        assertEventRead(toMediaDriver, assertReceiverMessageOfType(ADD_RECEIVER));
    }

    @Test
    public void removingReceiverNotifiesMediaDriver()
    {
        final RingBuffer toMediaDriver = adminBuffers.toMediaDriver();
        final Aeron aeron = newAeron();
        final Receiver receiver = newReceiver(aeron);

        aeron.adminThread().process();
        skip(toMediaDriver, 1);

        receiver.close();
        aeron.adminThread().process();

        assertEventRead(toMediaDriver, assertReceiverMessageOfType(REMOVE_RECEIVER));
    }

    @Test
    public void clientCodeNotifiedOfAnInvalidDestination()
    {
        final Aeron aeron = newAeron();

        receiverMessage.wrap(atomicSendBuffer, 0);
        receiverMessage.channelIds(CHANNEL_IDs);
        receiverMessage.destination(INVALID_DESTINATION);

        errorHeader.wrap(atomicSendBuffer, receiverMessage.length());
        errorHeader.errorCode(ErrorCode.INVALID_DESTINATION);
        errorHeader.offendingFlyweight(receiverMessage, receiverMessage.length());
        errorHeader.frameLength(ErrorHeaderFlyweight.HEADER_LENGTH + receiverMessage.length());

        adminBuffers.toApi().write(ERROR_RESPONSE,
                                   atomicSendBuffer,
                                   receiverMessage.length(),
                                   errorHeader.frameLength());

        aeron.adminThread().process();

        verify(invalidDestination).onInvalidDestination(INVALID_DESTINATION);
    }

    private void createTermBuffer(final long termId) throws IOException
    {
        final RingBuffer apiBuffer = adminBuffers.toApi();
        directory.createSenderTermFile(DESTINATION, SESSION_ID, CHANNEL_ID, termId);
        identifiedMessage.channelId(CHANNEL_ID)
                .sessionId(SESSION_ID)
                .termId(termId)
                .destination(DESTINATION);
        assertTrue(apiBuffer.write(NEW_SEND_BUFFER_NOTIFICATION, atomicSendBuffer, 0, identifiedMessage.length()));
    }

    private Receiver newReceiver(final Aeron aeron)
    {
        final Receiver.Builder builder = new Receiver.Builder()
                .destination(new Destination(DESTINATION))
                .channel(CHANNEL_ID, emptyDataHandler())
                .channel(CHANNEL_ID_2, emptyDataHandler());

        return aeron.newReceiver(builder);
    }

    private EventHandler assertReceiverMessageOfType(final int expectedEventTypeId)
    {
        return (eventTypeId, buffer, index, length) ->
        {
            assertThat(eventTypeId, is(expectedEventTypeId));
            receiverMessage.wrap(buffer, index);
            assertThat(receiverMessage.channelIds(), is(CHANNEL_IDs));
            assertThat(receiverMessage.destination(), is(DESTINATION));
        };
    }

    private Receiver.DataHandler emptyDataHandler()
    {
        return (buffer, offset, sessionId, flags) ->
        {
        };
    }

    private Channel newChannel(final Aeron aeron)
    {
        final Source.Builder sourceBuilder = new Source.Builder()
            .sessionId(SESSION_ID)
            .destination(new Destination(DESTINATION));
        final Source source = aeron.newSource(sourceBuilder);
        return source.newChannel(CHANNEL_ID);
    }

    private Aeron newAeron()
    {
        final Aeron.Builder builder = new Aeron.Builder()
             .adminBufferStrategy(new MappingAdminBufferStrategy(adminBuffers.adminDir()))
             .invalidDestinationHandler(invalidDestination);

        return Aeron.newSingleMediaDriver(builder);
    }

    private void assertChannelMessage(final RingBuffer mediaDriverBuffer, final int expectedEventTypeId)
    {
        assertEventRead(mediaDriverBuffer, (eventTypeId, buffer, index, length) ->
        {
            assertThat(eventTypeId, is(expectedEventTypeId));
            message.wrap(buffer, index);
            assertThat(message.destination(), is(DESTINATION));
            assertThat(message.channelId(), is(CHANNEL_ID));
            assertThat(message.sessionId(), is(SESSION_ID));
        });
    }

}
