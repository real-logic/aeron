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

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import uk.co.real_logic.aeron.conductor.ClientConductor;
import uk.co.real_logic.aeron.util.*;
import uk.co.real_logic.aeron.util.command.ChannelMessageFlyweight;
import uk.co.real_logic.aeron.util.command.QualifiedMessageFlyweight;
import uk.co.real_logic.aeron.util.command.SubscriberMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.EventHandler;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.BufferDescriptor;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.ManyToOneRingBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.util.protocol.ErrorHeaderFlyweight;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static uk.co.real_logic.aeron.Subscriber.DataHandler;
import static uk.co.real_logic.aeron.util.BitUtil.SIZE_OF_INT;
import static uk.co.real_logic.aeron.util.BufferRotationDescriptor.rotateId;
import static uk.co.real_logic.aeron.util.SharedDirectories.Buffers;
import static uk.co.real_logic.aeron.util.SharedDirectories.mapLoggers;
import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.*;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.BASE_HEADER_LENGTH;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferTestUtil.assertEventRead;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferTestUtil.skip;

public class AeronTest
{
    private static final byte[] DEFAULT_HEADER = new byte[BASE_HEADER_LENGTH + SIZE_OF_INT];
    private static final int MAX_FRAME_LENGTH = 1024;

    private static final String DESTINATION = "udp://localhost:40124";
    private static final String INVALID_DESTINATION = "udp://lo124";
    private static final long CHANNEL_ID = 2L;
    private static final long CHANNEL_ID_2 = 4L;
    private static final long[] CHANNEL_IDs = {CHANNEL_ID, CHANNEL_ID_2};
    private static final long SESSION_ID = 3L;
    private static final long SESSION_ID_2 = 5L;
    private static final int PACKET_VALUE = 37;
    private static final int SEND_BUFFER_CAPACITY = 256;
    private static final int CONDUCTOR_BUFFER_SIZE = (4 * 1024) + BufferDescriptor.TRAILER_LENGTH;

    @ClassRule
    public static CountersFile counters = new CountersFile();

    @ClassRule
    public static SharedDirectories directory = new SharedDirectories();

    @ClassRule
    public static ConductorBuffers conductorBuffers = new ConductorBuffers();

    private final InvalidDestinationHandler invalidDestination = mock(InvalidDestinationHandler.class);

    private DataHandler channel2Handler = emptyDataHandler();

    private final ChannelMessageFlyweight channelMessage = new ChannelMessageFlyweight();
    private final QualifiedMessageFlyweight qualifiedMessage = new QualifiedMessageFlyweight();
    private final SubscriberMessageFlyweight subscriberMessage = new SubscriberMessageFlyweight();

    private final ErrorHeaderFlyweight errorHeader = new ErrorHeaderFlyweight();

    private final ByteBuffer sendBuffer = ByteBuffer.allocate(SEND_BUFFER_CAPACITY);
    private final AtomicBuffer atomicSendBuffer = new AtomicBuffer(sendBuffer);

    private ClientConductorMappedBuffers clientConductorMappedBuffers;
    private MediaDriverConductorMappedBuffers mediaDriverConductorMappedBuffers;

    public AeronTest()
    {
        qualifiedMessage.wrap(atomicSendBuffer, 0);
        errorHeader.wrap(atomicSendBuffer, 0);
    }

    @Before
    public void setUp()
    {
        mediaDriverConductorMappedBuffers =
            new MediaDriverConductorMappedBuffers(conductorBuffers.adminDirName(), CONDUCTOR_BUFFER_SIZE);
        clientConductorMappedBuffers = new ClientConductorMappedBuffers(conductorBuffers.adminDirName());
    }

    @After
    public void tearDown()
    {
        clientConductorMappedBuffers.close();
        mediaDriverConductorMappedBuffers.close();
    }

    @Test
    public void creatingChannelsShouldNotifyMediaDriver() throws Exception
    {
        final Aeron aeron = newAeron();
        newChannel(aeron);
        aeron.conductor().process();

        assertChannelMessage(toMediaDriver(), ADD_CHANNEL);
    }

    @Test
    public void cannotOfferOnChannelUntilBuffersMapped() throws Exception
    {
        final Aeron aeron = newAeron();
        final Channel channel = newChannel(aeron);
        assertFalse(channel.offer(atomicSendBuffer));
        assertFalse(channel.offer(atomicSendBuffer, 0, 1));
    }

    @Test(expected = BufferExhaustedException.class)
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
        aeron.conductor().process();
        createTermBuffer(0L, NEW_SEND_BUFFER_NOTIFICATION, directory.senderDir(), SESSION_ID);
        aeron.conductor().process();
        assertTrue(channel.offer(atomicSendBuffer));
        aeron.conductor().close();
    }

    @Test
    public void shouldRotateBuffersOnceFull() throws Exception
    {
        final RingBuffer toMediaDriver = toMediaDriver();
        final Aeron aeron = newAeron();
        final Channel channel = newChannel(aeron);
        aeron.conductor().process();
        final List<Buffers> buffers =
            createTermBuffer(0L, NEW_SEND_BUFFER_NOTIFICATION, directory.senderDir(), SESSION_ID);

        final int capacity = buffers.get(0).logBuffer().capacity();
        final int eventCount = (4 * capacity) / SEND_BUFFER_CAPACITY;

        aeron.conductor().process();
        skip(toMediaDriver, 1);
        boolean previousAppend = true;
        int bufferId = 0;
        for (int i = 0; i < eventCount; i++)
        {
            final boolean appended = channel.offer(atomicSendBuffer);
            aeron.conductor().process();

            // only two in a row is a failure, because we don't rollover immediately
            assertTrue(previousAppend || appended);
            previousAppend = appended;

            if (!appended)
            {
                assertCleanTermRequested(toMediaDriver);
                cleanBuffer(buffers.get(bufferId));
                bufferId = rotateId(bufferId);
            }
        }

        aeron.conductor().close();
    }

    private void assertCleanTermRequested(final RingBuffer toMediaDriver)
    {
        assertEventRead(toMediaDriver,
                        (eventTypeId, buffer, index, length) -> assertThat(eventTypeId, is(REQUEST_CLEANED_TERM)));
    }

    @Test
    public void removingChannelsShouldNotifyMediaDriver() throws Exception
    {
        final RingBuffer toMediaDriver = toMediaDriver();
        final Aeron aeron = newAeron();
        final Channel channel = newChannel(aeron);
        final ClientConductor adminThread = aeron.conductor();

        adminThread.process();
        skip(toMediaDriver, 1);

        channel.close();
        adminThread.process();

        assertChannelMessage(toMediaDriver, REMOVE_CHANNEL);
    }

    @Test
    public void closingASourceRemovesItsAssociatedChannels() throws Exception
    {
        final Aeron aeron = newAeron();
        final Source.Context sourceContext = new Source.Context()
            .sessionId(SESSION_ID)
            .destination(new Destination(DESTINATION));
        final Source source = aeron.newSource(sourceContext);
        source.newChannel(CHANNEL_ID);
        final ClientConductor adminThread = aeron.conductor();

        adminThread.process();
        skip(toMediaDriver(), 1);

        source.close();
        adminThread.process();

        assertChannelMessage(toMediaDriver(), REMOVE_CHANNEL);
    }

    @Test
    public void closingASourceDoesNotRemoveOtherChannels() throws Exception
    {
        final RingBuffer buffer = conductorBuffers.toMediaDriver();
        final Aeron aeron = newAeron();
        final Source source = aeron.newSource(new Source.Context()
                                                  .sessionId(SESSION_ID)
                                                  .destination(new Destination(DESTINATION)));
        final Source otherSource = aeron.newSource(new Source.Context()
                                                       .sessionId(SESSION_ID + 1)
                                                       .destination(new Destination(DESTINATION)));
        source.newChannel(CHANNEL_ID);
        final ClientConductor clientConductor = aeron.conductor();

        clientConductor.process();
        skip(buffer, 1);

        otherSource.close();
        clientConductor.process();

        skip(buffer, 0);
    }

    @Test
    public void registeringSubscriberNotifiesMediaDriver() throws Exception
    {
        final Aeron aeron = newAeron();
        final Subscriber.Context context = new Subscriber.Context()
            .destination(new Destination(DESTINATION))
            .channel(CHANNEL_ID, emptyDataHandler())
            .channel(CHANNEL_ID_2, emptyDataHandler());

        final Subscriber subscriber = aeron.newSubscriber(context);

        aeron.conductor().process();

        assertEventRead(toMediaDriver(), assertSubscriberMessageOfType(ADD_SUBSCRIBER));

        assertThat(subscriber.read(), is(0));
    }

    @Test
    public void removingSubscriberNotifiesMediaDriver()
    {
        final RingBuffer toMediaDriver = toMediaDriver();
        final Aeron aeron = newAeron();
        final Subscriber subscriber = newSubscriber(aeron);

        aeron.conductor().process();
        skip(toMediaDriver, 1);

        subscriber.close();
        aeron.conductor().process();

        assertEventRead(toMediaDriver, assertSubscriberMessageOfType(REMOVE_SUBSCRIBER));
    }

    @Test
    public void clientCodeNotifiedOfAnInvalidDestination()
    {
        final Aeron aeron = newAeron();

        subscriberMessage.wrap(atomicSendBuffer, 0);
        subscriberMessage.channelIds(CHANNEL_IDs);
        subscriberMessage.destination(INVALID_DESTINATION);

        errorHeader.wrap(atomicSendBuffer, subscriberMessage.length());
        errorHeader.errorCode(ErrorCode.INVALID_DESTINATION);
        errorHeader.offendingFlyweight(subscriberMessage, subscriberMessage.length());
        errorHeader.frameLength(ErrorHeaderFlyweight.HEADER_LENGTH + subscriberMessage.length());

        toClient().write(ERROR_RESPONSE,
                      atomicSendBuffer,
                      subscriberMessage.length(),
                      errorHeader.frameLength());

        aeron.conductor().process();

        verify(invalidDestination).onInvalidDestination(INVALID_DESTINATION);
    }

    @Test
    public void subscriberCanReceiveAMessage() throws Exception
    {
        channel2Handler = assertingHandler();

        final RingBuffer toMediaDriver = conductorBuffers.toMediaDriver();
        final Aeron aeron = newAeron();
        final Subscriber subscriber = newSubscriber(aeron);

        final List<LogAppender> logAppenders = createLogAppenders(SESSION_ID);

        aeron.conductor().process();
        skip(toMediaDriver, 1);

        writePacket(logAppenders.get(0));

        assertThat(subscriber.read(), is(1));

        aeron.conductor().close();
    }

    @Test
    public void subscriberCanReceivePacketsFromMultipleSessions() throws Exception
    {
        channel2Handler = eitherSessionHandler();

        final RingBuffer toMediaDriver = conductorBuffers.toMediaDriver();
        final Aeron aeron = newAeron();
        final Subscriber subscriber = newSubscriber(aeron);

        final List<LogAppender> logAppenders = createLogAppenders(SESSION_ID);
        final List<LogAppender> otherLogAppenders = createLogAppenders(SESSION_ID_2);

        aeron.conductor().process();
        skip(toMediaDriver, 1);

        writePacket(logAppenders.get(0));
        writePacket(otherLogAppenders.get(0));
        assertThat(subscriber.read(), is(2));

        aeron.conductor().close();
    }

    @Test
    public void receivingEnoughPacketsCausesSubscriberBufferRoll() throws Exception
    {
        channel2Handler = assertingHandler();

        final RingBuffer toMediaDriver = conductorBuffers.toMediaDriver();
        final Aeron aeron = newAeron();
        final Subscriber subscriber = newSubscriber(aeron);
        final List<Buffers> termBuffers =
            createTermBuffer(0L, NEW_RECEIVE_BUFFER_NOTIFICATION, directory.receiverDir(), SESSION_ID);

        final List<LogAppender> logAppenders = createLogAppenders(termBuffers);

        aeron.conductor().process();
        skip(toMediaDriver, 1);

        final LogAppender logAppender = logAppenders.get(0);
        final int eventCount = logAppender.capacity() / sendBuffer.capacity();

        writePackets(logAppender, eventCount);
        assertThat(subscriber.read(), is(eventCount));

        // cleaning is triggered by the subscriber and not the subscriber
        // so we clean two ahead of the current buffer
        cleanBuffer(termBuffers.get(2));
        aeron.conductor().process();

        writePackets(logAppenders.get(1), eventCount);
        assertThat(subscriber.read(), is(eventCount));

        cleanBuffer(termBuffers.get(0));
        aeron.conductor().process();

        writePackets(logAppenders.get(2), eventCount);
        assertThat(subscriber.read(), is(eventCount));

        cleanBuffer(termBuffers.get(1));
        aeron.conductor().process();

        writePackets(logAppender, eventCount);
        assertThat(subscriber.read(), is(eventCount));

        aeron.conductor().close();
    }

    private void cleanBuffer(final Buffers buffers)
    {
        cleanBuffer(buffers.logBuffer());
        cleanBuffer(buffers.stateBuffer());
    }

    private void cleanBuffer(final AtomicBuffer buffer)
    {
        buffer.putBytes(0, new byte[buffer.capacity()]);
    }

    private List<LogAppender> createLogAppenders(final List<Buffers> termBuffers)
    {
        return mapLoggers(termBuffers, DEFAULT_HEADER, MAX_FRAME_LENGTH);
    }

    @Test
    public void subscriberBufferRollsDoNotOverflowTheCleanedBuffer() throws Exception
    {
        channel2Handler = assertingHandler();

        final RingBuffer toMediaDriver = conductorBuffers.toMediaDriver();
        final Aeron aeron = newAeron();
        final Subscriber subscriber = newSubscriber(aeron);
        final List<LogAppender> logAppenders = createLogAppenders(SESSION_ID);

        aeron.conductor().process();
        skip(toMediaDriver, 1);

        final LogAppender logAppender = logAppenders.get(0);
        final int eventCount = logAppender.capacity() / SEND_BUFFER_CAPACITY;

        writePackets(logAppender, eventCount);
        assertThat(subscriber.read(), is(eventCount));

        writePackets(logAppenders.get(1), eventCount);
        assertThat(subscriber.read(), is(eventCount));

        writePackets(logAppenders.get(2), eventCount);
        assertThat(subscriber.read(), is(eventCount));

        // force the roll
        assertThat(subscriber.read(), is(eventCount));

        // Now you've hit an unclean buffer and can't proceed
        assertThat(subscriber.read(), is(0));

        aeron.conductor().close();
    }

    @Test
    public void subscriberBufferRollsShouldNotAffectOtherSessions() throws Exception
    {
        channel2Handler = eitherSessionHandler();

        final RingBuffer toMediaDriver = conductorBuffers.toMediaDriver();
        final Aeron aeron = newAeron();
        final Subscriber subscriber = newSubscriber(aeron);
        final List<LogAppender> logAppenders = createLogAppenders(SESSION_ID);
        final List<LogAppender> otherLogAppenders = createLogAppenders(SESSION_ID_2);
        sendNewBufferNotification(NEW_RECEIVE_BUFFER_NOTIFICATION, 1L, SESSION_ID);

        aeron.conductor().process();
        skip(toMediaDriver, 1);

        final LogAppender logAppender = logAppenders.get(0);
        final int eventCount = logAppender.capacity() / sendBuffer.capacity();

        writePackets(logAppender, eventCount);
        assertThat(subscriber.read(), is(eventCount));

        writePackets(logAppenders.get(1), eventCount);
        assertThat(subscriber.read(), is(eventCount));

        writePackets(otherLogAppenders.get(0), 5);
        assertThat(subscriber.read(), is(5));

        aeron.conductor().close();
    }

    private DataHandler eitherSessionHandler()
    {
        return (buffer, offset, sessionId, flags) ->
        {
            assertThat(buffer.getInt(offset), is(PACKET_VALUE));
            assertThat(sessionId, anyOf(is(SESSION_ID), is(SESSION_ID_2)));
        };
    }

    private DataHandler assertingHandler()
    {
        return (buffer, offset, sessionId, flags) ->
        {
            assertThat(buffer.getInt(offset), is(PACKET_VALUE));
            assertThat(sessionId, is(SESSION_ID));
        };
    }

    private void writePacket(final LogAppender logAppender)
    {
        writePackets(logAppender, 1);
    }

    private void writePackets(final LogAppender logAppender, final int events)
    {
        final int bytesToSend = atomicSendBuffer.capacity() - DEFAULT_HEADER.length;
        IntStream.range(0, events).forEach(
            (i) ->
            {
                atomicSendBuffer.putInt(0, PACKET_VALUE);
                assertTrue(logAppender.append(atomicSendBuffer, 0, bytesToSend));
            }
        );
    }

    private List<LogAppender> createLogAppenders(final long sessionId) throws IOException
    {
        final List<Buffers> termBuffers =
            createTermBuffer(0L, NEW_RECEIVE_BUFFER_NOTIFICATION, directory.receiverDir(), sessionId);

        return createLogAppenders(termBuffers);
    }

    private List<Buffers> createTermBuffer(final long termId,
                                           final int eventTypeId,
                                           final File rootDir,
                                           final long sessionId) throws IOException
    {
        final List<Buffers> buffers = directory.createTermFile(rootDir, DESTINATION, sessionId, CHANNEL_ID);
        sendNewBufferNotification(eventTypeId, termId, sessionId);

        return buffers;
    }

    private void sendNewBufferNotification(final int eventTypeId, final long termId, final long sessionId)
    {
        final RingBuffer apiBuffer = toClient();
        qualifiedMessage.channelId(CHANNEL_ID)
                         .sessionId(sessionId)
                         .termId(termId)
                         .destination(DESTINATION);

        assertTrue(apiBuffer.write(eventTypeId, atomicSendBuffer, 0, qualifiedMessage.length()));
    }

    private ManyToOneRingBuffer toClient()
    {
        return new ManyToOneRingBuffer(new AtomicBuffer(mediaDriverConductorMappedBuffers.toClient()));
    }

    private ManyToOneRingBuffer toMediaDriver()
    {
        return new ManyToOneRingBuffer(new AtomicBuffer(mediaDriverConductorMappedBuffers.toMediaDriver()));
    }

    private Subscriber newSubscriber(final Aeron aeron)
    {
        final Subscriber.Context context = new Subscriber.Context()
            .destination(new Destination(DESTINATION))
            .channel(CHANNEL_ID, channel2Handler)
            .channel(CHANNEL_ID_2, emptyDataHandler());

        return aeron.newSubscriber(context);
    }

    private EventHandler assertSubscriberMessageOfType(final int expectedEventTypeId)
    {
        return (eventTypeId, buffer, index, length) ->
        {
            assertThat(eventTypeId, is(expectedEventTypeId));

            subscriberMessage.wrap(buffer, index);
            assertThat(subscriberMessage.channelIds(), is(CHANNEL_IDs));
            assertThat(subscriberMessage.destination(), is(DESTINATION));
        };
    }

    private DataHandler emptyDataHandler()
    {
        return (buffer, offset, sessionId, flags) -> {};
    }

    private Channel newChannel(final Aeron aeron)
    {
        final Source.Context sourceContext = new Source.Context()
            .sessionId(SESSION_ID)
            .destination(new Destination(DESTINATION));
        final Source source = aeron.newSource(sourceContext);

        return source.newChannel(CHANNEL_ID);
    }

    private Aeron newAeron()
    {
        final Aeron.Context context = new Aeron.Context()
            .conductorMappedBuffers(clientConductorMappedBuffers)
            .invalidDestinationHandler(invalidDestination);

        return Aeron.newSingleMediaDriver(context);
    }

    private void assertChannelMessage(final RingBuffer mediaDriverBuffer, final int expectedEventTypeId)
    {
        assertEventRead(mediaDriverBuffer, (eventTypeId, buffer, index, length) ->
        {
            assertThat(eventTypeId, is(expectedEventTypeId));

            channelMessage.wrap(buffer, index);
            assertThat(channelMessage.destination(), is(DESTINATION));
            assertThat(channelMessage.channelId(), is(CHANNEL_ID));
            assertThat(channelMessage.sessionId(), is(SESSION_ID));
        });
    }
}
