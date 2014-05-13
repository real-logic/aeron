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
import uk.co.real_logic.aeron.util.command.CompletelyIdentifiedMessageFlyweight;
import uk.co.real_logic.aeron.util.command.ConsumerMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.EventHandler;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
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
import static uk.co.real_logic.aeron.Consumer.DataHandler;
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
    public static final int PACKET_VALUE = 37;
    public static final int SEND_BUFFER_CAPACITY = 256;

    @ClassRule
    public static CountersResource counters = new CountersResource();

    @ClassRule
    public static SharedDirectories directory = new SharedDirectories();

    @ClassRule
    public static ConductorBuffers conductorBuffers = new ConductorBuffers();

    private final InvalidDestinationHandler invalidDestination = mock(InvalidDestinationHandler.class);

    private DataHandler channel2Handler = emptyDataHandler();

    private final ChannelMessageFlyweight message = new ChannelMessageFlyweight();
    private final CompletelyIdentifiedMessageFlyweight identifiedMessage = new CompletelyIdentifiedMessageFlyweight();
    private final ConsumerMessageFlyweight receiverMessage = new ConsumerMessageFlyweight();

    private final ErrorHeaderFlyweight errorHeader = new ErrorHeaderFlyweight();

    private final ByteBuffer sendBuffer = ByteBuffer.allocate(SEND_BUFFER_CAPACITY);
    private final AtomicBuffer atomicSendBuffer = new AtomicBuffer(sendBuffer);
    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();

    private MappingConductorBufferStrategy adminBufferStrategy;

    public AeronTest()
    {
        identifiedMessage.wrap(atomicSendBuffer, 0);
        errorHeader.wrap(atomicSendBuffer, 0);
        dataHeader.wrap(atomicSendBuffer, 0);
    }

    @Before
    public void setUp()
    {
        adminBufferStrategy = new MappingConductorBufferStrategy(conductorBuffers.adminDir());
    }

    @After
    public void tearDown()
    {
        adminBufferStrategy.close();
    }

    @Test
    public void creatingChannelsShouldNotifyMediaDriver() throws Exception
    {
        final Aeron aeron = newAeron();
        newChannel(aeron);
        aeron.conductor().process();

        assertChannelMessage(conductorBuffers.toMediaDriver(), ADD_CHANNEL);
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
        final RingBuffer toMediaDriver = conductorBuffers.toMediaDriver();
        final Aeron aeron = newAeron();
        final Channel channel = newChannel(aeron);
        aeron.conductor().process();
        List<Buffers> buffers = createTermBuffer(0L, NEW_SEND_BUFFER_NOTIFICATION, directory.senderDir(), SESSION_ID);

        final int capacity = buffers.get(0).logBuffer().capacity();
        final int eventCount = 4 * capacity / SEND_BUFFER_CAPACITY;

        aeron.conductor().process();
        skip(toMediaDriver, 1);
        boolean previousAppend = true;
        int bufferId = 0;
        for (int i = 0; i < eventCount; i++)
        {
            boolean appended = channel.offer(atomicSendBuffer);
            aeron.conductor().process();

            // only two in a row is a failure, because we don't
            // rollover immedately
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
            (eventTypeId, buffer, index, length) ->
            {
                assertThat(eventTypeId, is(REQUEST_CLEANED_TERM));
            });
    }

    @Test
    public void removingChannelsShouldNotifyMediaDriver() throws Exception
    {
        final RingBuffer buffer = conductorBuffers.toMediaDriver();
        final Aeron aeron = newAeron();
        final Channel channel = newChannel(aeron);
        final ClientConductor adminThread = aeron.conductor();

        adminThread.process();
        skip(buffer, 1);

        channel.close();
        adminThread.process();

        assertChannelMessage(buffer, REMOVE_CHANNEL);
    }

    @Test
    public void closingASourceRemovesItsAssociatedChannels() throws Exception
    {
        final RingBuffer buffer = conductorBuffers.toMediaDriver();
        final Aeron aeron = newAeron();
        final Source.Context sourceContext = new Source.Context()
            .sessionId(SESSION_ID)
            .destination(new Destination(DESTINATION));
        final Source source = aeron.newSource(sourceContext);
        final Channel channel = source.newChannel(CHANNEL_ID);
        final ClientConductor adminThread = aeron.conductor();

        adminThread.process();
        skip(buffer, 1);

        source.close();
        adminThread.process();

        assertChannelMessage(buffer, REMOVE_CHANNEL);
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
        final Channel channel = source.newChannel(CHANNEL_ID);
        final ClientConductor adminThread = aeron.conductor();

        adminThread.process();
        skip(buffer, 1);

        otherSource.close();
        adminThread.process();

        skip(buffer, 0);
    }

    @Test
    public void registeringReceiverNotifiesMediaDriver() throws Exception
    {
        final RingBuffer toMediaDriver = conductorBuffers.toMediaDriver();
        final Aeron aeron = newAeron();
        final Consumer.Context context = new Consumer.Context()
            .destination(new Destination(DESTINATION))
            .channel(CHANNEL_ID, emptyDataHandler())
            .channel(CHANNEL_ID_2, emptyDataHandler());

        final Consumer consumer = aeron.newConsumer(context);

        aeron.conductor().process();

        assertEventRead(toMediaDriver, assertReceiverMessageOfType(ADD_CONSUMER));

        assertThat(consumer.consume(), is(0));
    }

    @Test
    public void removingReceiverNotifiesMediaDriver()
    {
        final RingBuffer toMediaDriver = conductorBuffers.toMediaDriver();
        final Aeron aeron = newAeron();
        final Consumer consumer = newConsumer(aeron);

        aeron.conductor().process();
        skip(toMediaDriver, 1);

        consumer.close();
        aeron.conductor().process();

        assertEventRead(toMediaDriver, assertReceiverMessageOfType(REMOVE_CONSUMER));
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

        conductorBuffers.toApi().write(ERROR_RESPONSE,
                                   atomicSendBuffer,
                                   receiverMessage.length(),
                                   errorHeader.frameLength());

        aeron.conductor().process();

        verify(invalidDestination).onInvalidDestination(INVALID_DESTINATION);
    }

    @Test
    public void canReceiveAMessage() throws Exception
    {
        channel2Handler = assertingHandler();

        final RingBuffer toMediaDriver = conductorBuffers.toMediaDriver();
        final Aeron aeron = newAeron();
        final Consumer consumer = newConsumer(aeron);

        final List<LogAppender> logAppenders = createLogAppenders(SESSION_ID);

        aeron.conductor().process();
        skip(toMediaDriver, 1);

        writePacket(logAppenders.get(0));

        assertThat(consumer.consume(), is(1));

        aeron.conductor().close();
    }

    @Test
    public void canReceivePacketsFromMultipleSessions() throws Exception
    {
        channel2Handler = eitherSessionHandler();

        final RingBuffer toMediaDriver = conductorBuffers.toMediaDriver();
        final Aeron aeron = newAeron();
        final Consumer consumer = newConsumer(aeron);

        final List<LogAppender> logAppenders = createLogAppenders(SESSION_ID);
        final List<LogAppender> otherLogAppenders = createLogAppenders(SESSION_ID_2);

        aeron.conductor().process();
        skip(toMediaDriver, 1);

        writePacket(logAppenders.get(0));
        writePacket(otherLogAppenders.get(0));
        assertThat(consumer.consume(), is(1));

        aeron.conductor().close();
    }

    @Test
    public void receivingEnoughPacketsCausesAConsumerBufferRoll() throws Exception
    {
        channel2Handler = assertingHandler();

        final RingBuffer toMediaDriver = conductorBuffers.toMediaDriver();
        final Aeron aeron = newAeron();
        final Consumer consumer = newConsumer(aeron);
        final List<Buffers> termBuffers =
            createTermBuffer(0L, NEW_RECEIVE_BUFFER_NOTIFICATION, directory.receiverDir(), SESSION_ID);

        final List<LogAppender> logAppenders = createLogAppenders(termBuffers);

        aeron.conductor().process();
        skip(toMediaDriver, 1);

        final LogAppender logAppender = logAppenders.get(0);
        final int eventCount = logAppender.capacity() / sendBuffer.capacity();

        writePackets(logAppender, eventCount);
        assertThat(consumer.consume(), is(eventCount));

        // cleaning is triggered by the receiver and not the consumer
        // so we clean two ahead of the current buffer
        cleanBuffer(termBuffers.get(2));
        aeron.conductor().process();

        writePackets(logAppenders.get(1), eventCount);
        assertThat(consumer.consume(), is(eventCount));

        cleanBuffer(termBuffers.get(0));
        aeron.conductor().process();

        writePackets(logAppenders.get(2), eventCount);
        assertThat(consumer.consume(), is(eventCount));

        cleanBuffer(termBuffers.get(1));
        aeron.conductor().process();

        writePackets(logAppender, eventCount);
        assertThat(consumer.consume(), is(eventCount));

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
    public void consumerBufferRollsDoNotOverflowTheCleanedBuffer() throws Exception
    {
        channel2Handler = assertingHandler();

        final RingBuffer toMediaDriver = conductorBuffers.toMediaDriver();
        final Aeron aeron = newAeron();
        final Consumer consumer = newConsumer(aeron);
        final List<LogAppender> logAppenders = createLogAppenders(SESSION_ID);

        aeron.conductor().process();
        skip(toMediaDriver, 1);

        final LogAppender logAppender = logAppenders.get(0);
        final int eventCount = logAppender.capacity() / SEND_BUFFER_CAPACITY;

        writePackets(logAppender, eventCount);
        assertThat(consumer.consume(), is(eventCount));

        writePackets(logAppenders.get(1), eventCount);
        assertThat(consumer.consume(), is(eventCount));

        writePackets(logAppenders.get(2), eventCount);
        assertThat(consumer.consume(), is(eventCount));

        // force the roll
        assertThat(consumer.consume(), is(eventCount));

        // Now you've hit an unclean buffer and can't proceed
        assertThat(consumer.consume(), is(0));

        aeron.conductor().close();
    }

    @Test
    public void consumerBufferRollsShouldNotAffectOtherSessions() throws Exception
    {
        channel2Handler = eitherSessionHandler();

        final RingBuffer toMediaDriver = conductorBuffers.toMediaDriver();
        final Aeron aeron = newAeron();
        final Consumer consumer = newConsumer(aeron);
        final List<LogAppender> logAppenders = createLogAppenders(SESSION_ID);
        final List<LogAppender> otherLogAppenders = createLogAppenders(SESSION_ID_2);
        sendNewBufferNotification(NEW_RECEIVE_BUFFER_NOTIFICATION, 1L, SESSION_ID);

        aeron.conductor().process();
        skip(toMediaDriver, 1);

        final LogAppender logAppender = logAppenders.get(0);
        final int eventCount = logAppender.capacity() / sendBuffer.capacity();

        writePackets(logAppender, eventCount);
        assertThat(consumer.consume(), is(eventCount));

        writePackets(logAppenders.get(1), eventCount);
        assertThat(consumer.consume(), is(eventCount));

        writePackets(otherLogAppenders.get(0), 5);
        assertThat(consumer.consume(), is(5));

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
        final RingBuffer apiBuffer = conductorBuffers.toApi();
        identifiedMessage.channelId(CHANNEL_ID)
                         .sessionId(sessionId)
                         .termId(termId)
                         .destination(DESTINATION);
        assertTrue(apiBuffer.write(eventTypeId, atomicSendBuffer, 0, identifiedMessage.length()));
    }

    private Consumer newConsumer(final Aeron aeron)
    {
        final Consumer.Context context = new Consumer.Context()
            .destination(new Destination(DESTINATION))
            .channel(CHANNEL_ID, channel2Handler)
            .channel(CHANNEL_ID_2, emptyDataHandler());

        return aeron.newConsumer(context);
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

    private DataHandler emptyDataHandler()
    {
        return (buffer, offset, sessionId, flags) ->
        {
        };
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
            .adminBufferStrategy(adminBufferStrategy)
            .invalidDestinationHandler(invalidDestination);

        return Aeron.newSingleMediaDriver(context);
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
