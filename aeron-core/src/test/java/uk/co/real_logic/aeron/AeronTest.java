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

import org.junit.*;
import uk.co.real_logic.aeron.conductor.ClientConductor;
import uk.co.real_logic.aeron.util.*;
import uk.co.real_logic.aeron.util.command.*;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.MessageHandler;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.*;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.ErrorFlyweight;

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
import static uk.co.real_logic.aeron.util.BufferRotationDescriptor.BUFFER_COUNT;
import static uk.co.real_logic.aeron.util.BufferRotationDescriptor.rotateId;
import static uk.co.real_logic.aeron.util.FileMappingConvention.Type;
import static uk.co.real_logic.aeron.util.FileMappingConvention.Type.LOG;
import static uk.co.real_logic.aeron.util.FileMappingConvention.Type.STATE;
import static uk.co.real_logic.aeron.util.FileMappingConvention.termLocation;
import static uk.co.real_logic.aeron.util.SharedDirectoriesExternalResource.Buffers;
import static uk.co.real_logic.aeron.util.SharedDirectoriesExternalResource.mapLoggers;
import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.*;
import static uk.co.real_logic.aeron.util.command.NewBufferMessageFlyweight.PAYLOAD_BUFFER_COUNT;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.LOG_MIN_SIZE;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferTestUtil.assertMsgRead;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferTestUtil.skip;

public class AeronTest
{
    private static final int MAX_FRAME_LENGTH = 1024;

    private static final String DESTINATION = "udp://localhost:40124";
    private static final String INVALID_DESTINATION = "udp://lo124";
    private static final long CHANNEL_ID = 2L;
    private static final long CHANNEL_ID_2 = 4L;
    private static final long[] CHANNEL_IDs = {CHANNEL_ID, CHANNEL_ID_2};
    private static final long SESSION_ID = 3L;
    private static final long SESSION_ID_2 = 5L;
    private static final int PACKET_VALUE = 37;
    private static final int SEND_BUFFER_CAPACITY = 1024;
    private static final int CONDUCTOR_BUFFER_SIZE = (16 * 1024) + RingBufferDescriptor.TRAILER_LENGTH;
    public static final int BUFFER_SZ = 4096 + RingBufferDescriptor.TRAILER_LENGTH;

    @ClassRule
    public static SharedDirectoriesExternalResource directory = new SharedDirectoriesExternalResource();

    @ClassRule
    public static CountersFileExternalResource counters = new CountersFileExternalResource();

    private final InvalidDestinationHandler invalidDestination = mock(InvalidDestinationHandler.class);

    private DataHandler channel2Handler = emptyDataHandler();

    private final PublicationMessageFlyweight publicationMessage = new PublicationMessageFlyweight();
    private final NewBufferMessageFlyweight newBufferMessage = new NewBufferMessageFlyweight();
    private final SubscriptionMessageFlyweight subscriptionMessage = new SubscriptionMessageFlyweight();

    private final ErrorFlyweight errorHeader = new ErrorFlyweight();

    private final ByteBuffer sendBuffer = ByteBuffer.allocate(SEND_BUFFER_CAPACITY);
    private final AtomicBuffer atomicSendBuffer = new AtomicBuffer(sendBuffer);
    private final RingBuffer toClientBuffer = new ManyToOneRingBuffer(new AtomicBuffer(new byte[BUFFER_SZ]));
    private final RingBuffer toDriverBuffer = new ManyToOneRingBuffer(new AtomicBuffer(new byte[BUFFER_SZ]));

    public AeronTest()
    {
        newBufferMessage.wrap(atomicSendBuffer, 0);
        errorHeader.wrap(atomicSendBuffer, 0);
    }

    @Test
    public void creatingChannelsShouldNotifyMediaDriver() throws Exception
    {
        final Aeron aeron = newAeron();
        newChannel(aeron);
        aeron.conductor().doWork();

        assertChannelMessage(toDriverBuffer, ADD_PUBLICATION);
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
        aeron.conductor().doWork();
        createTermBuffer(0L, NEW_PUBLICATION_BUFFER_NOTIFICATION, directory.senderDir(), SESSION_ID);
        aeron.conductor().doWork();
        assertTrue(channel.offer(atomicSendBuffer));
        aeron.conductor().close();
    }

    @Test
    public void shouldRotateBuffersOnceFull() throws Exception
    {
        final RingBuffer toMediaDriver = toDriverBuffer;
        final Aeron aeron = newAeron();
        final Channel channel = newChannel(aeron);
        aeron.conductor().doWork();
        final List<Buffers> buffers =
            createTermBuffer(0L, NEW_PUBLICATION_BUFFER_NOTIFICATION, directory.senderDir(), SESSION_ID);

        final int capacity = buffers.get(0).logBuffer().capacity();
        final int msgCount = (4 * capacity) / SEND_BUFFER_CAPACITY;

        aeron.conductor().doWork();
        skip(toMediaDriver, 1);
        boolean previousAppend = true;
        int bufferId = 0;
        for (int i = 0; i < msgCount; i++)
        {
            final boolean appended = channel.offer(atomicSendBuffer);
            aeron.conductor().doWork();

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
        assertMsgRead(toMediaDriver,
                      (msgTypeId, buffer, index, length) -> assertThat(msgTypeId, is(CLEAN_TERM_BUFFER)));
    }

    @Test
    public void removingChannelsShouldNotifyMediaDriver() throws Exception
    {
        final RingBuffer toMediaDriver = toDriverBuffer;
        final Aeron aeron = newAeron();
        final Channel channel = newChannel(aeron);
        final ClientConductor adminThread = aeron.conductor();

        adminThread.doWork();
        skip(toMediaDriver, 1);

        channel.close();
        adminThread.doWork();

        assertChannelMessage(toMediaDriver, REMOVE_PUBLICATION);
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

        adminThread.doWork();
        skip(toDriverBuffer, 1);

        source.close();
        adminThread.doWork();

        assertChannelMessage(toDriverBuffer, REMOVE_PUBLICATION);
    }

    @Test
    public void closingASourceDoesNotRemoveOtherChannels() throws Exception
    {
        final Aeron aeron = newAeron();
        final Source source = aeron.newSource(new Source.Context()
                                                  .sessionId(SESSION_ID)
                                                  .destination(new Destination(DESTINATION)));
        final Source otherSource = aeron.newSource(new Source.Context()
                                                       .sessionId(SESSION_ID + 1)
                                                       .destination(new Destination(DESTINATION)));
        source.newChannel(CHANNEL_ID);
        final ClientConductor clientConductor = aeron.conductor();

        clientConductor.doWork();
        skip(toDriverBuffer, 1);

        otherSource.close();
        clientConductor.doWork();

        skip(toDriverBuffer, 0);
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

        aeron.conductor().doWork();

        assertMsgRead(toDriverBuffer, assertSubscriberMessageOfType(ADD_SUBSCRIPTION));

        assertThat(subscriber.read(), is(0));
    }

    @Test
    public void removingSubscriberNotifiesMediaDriver()
    {
        final RingBuffer toMediaDriver = toDriverBuffer;
        final Aeron aeron = newAeron();
        final Subscriber subscriber = newSubscriber(aeron);

        aeron.conductor().doWork();
        skip(toMediaDriver, 1);

        subscriber.close();
        aeron.conductor().doWork();

        assertMsgRead(toMediaDriver, assertSubscriberMessageOfType(REMOVE_SUBSCRIPTION));
    }

    @Test
    public void clientCodeNotifiedOfAnInvalidDestination()
    {
        final Aeron aeron = newAeron();

        subscriptionMessage.wrap(atomicSendBuffer, 0);
        subscriptionMessage.channelIds(CHANNEL_IDs);
        subscriptionMessage.destination(INVALID_DESTINATION);

        errorHeader.wrap(atomicSendBuffer, subscriptionMessage.length());
        errorHeader.errorCode(ErrorCode.INVALID_DESTINATION);
        errorHeader.offendingFlyweight(subscriptionMessage, subscriptionMessage.length());
        errorHeader.frameLength(ErrorFlyweight.HEADER_LENGTH + subscriptionMessage.length());

        toClientBuffer.write(ERROR_RESPONSE,
                atomicSendBuffer,
                subscriptionMessage.length(),
                errorHeader.frameLength());

        aeron.conductor().doWork();

        verify(invalidDestination).onInvalidDestination(INVALID_DESTINATION);
    }

    @Test
    public void subscriberCanReceiveAMessage() throws Exception
    {
        channel2Handler = assertingHandler();

        final Aeron aeron = newAeron();
        final Subscriber subscriber = newSubscriber(aeron);

        final List<LogAppender> logAppenders = createLogAppenders(SESSION_ID);

        aeron.conductor().doWork();
        skip(toDriverBuffer, 1);

        writePacket(logAppenders.get(0));

        assertThat(subscriber.read(), is(1));

        aeron.conductor().close();
    }

    @Test
    public void subscriberCanReceivePacketsFromMultipleSessions() throws Exception
    {
        channel2Handler = eitherSessionHandler();

        final Aeron aeron = newAeron();
        final Subscriber subscriber = newSubscriber(aeron);

        final List<LogAppender> logAppenders = createLogAppenders(SESSION_ID);
        final List<LogAppender> otherLogAppenders = createLogAppenders(SESSION_ID_2);

        aeron.conductor().doWork();
        skip(toDriverBuffer, 1);

        writePacket(logAppenders.get(0));
        writePacket(otherLogAppenders.get(0));
        assertThat(subscriber.read(), is(2));

        aeron.conductor().close();
    }

    @Test
    public void receivingEnoughPacketsCausesSubscriberBufferRoll() throws Exception
    {
        channel2Handler = assertingHandler();

        final Aeron aeron = newAeron();
        final Subscriber subscriber = newSubscriber(aeron);
        final List<Buffers> termBuffers =
            createTermBuffer(0L, NEW_SUBSCRIPTION_BUFFER_NOTIFICATION, directory.receiverDir(), SESSION_ID);

        final List<LogAppender> logAppenders = createLogAppenders(termBuffers);

        aeron.conductor().doWork();
        skip(toDriverBuffer, 1);

        final LogAppender logAppender = logAppenders.get(0);
        final int msgCount = logAppender.capacity() / sendBuffer.capacity();

        writePackets(logAppender, msgCount);
        assertThat(subscriber.read(), is(msgCount));

        // cleaning is triggered by the subscriber and not the subscriber
        // so we clean two ahead of the current buffer
        cleanBuffer(termBuffers.get(2));
        aeron.conductor().doWork();

        writePackets(logAppenders.get(1), msgCount);
        assertThat(subscriber.read(), is(msgCount));

        cleanBuffer(termBuffers.get(0));
        aeron.conductor().doWork();

        writePackets(logAppenders.get(2), msgCount);
        assertThat(subscriber.read(), is(msgCount));

        cleanBuffer(termBuffers.get(1));
        aeron.conductor().doWork();

        writePackets(logAppender, msgCount);
        assertThat(subscriber.read(), is(msgCount));

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
        return mapLoggers(termBuffers, DataHeaderFlyweight.DEFAULT_HEADER_NULL_IDS, MAX_FRAME_LENGTH);
    }

    @Test
    public void subscriberBufferRollsDoNotOverflowTheCleanedBuffer() throws Exception
    {
        channel2Handler = assertingHandler();

        final Aeron aeron = newAeron();
        final Subscriber subscriber = newSubscriber(aeron);
        final List<LogAppender> logAppenders = createLogAppenders(SESSION_ID);

        aeron.conductor().doWork();
        skip(toDriverBuffer, 1);

        final LogAppender logAppender = logAppenders.get(0);
        final int msgCount = logAppender.capacity() / SEND_BUFFER_CAPACITY;

        writePackets(logAppender, msgCount);
        assertThat(subscriber.read(), is(msgCount));

        writePackets(logAppenders.get(1), msgCount);
        assertThat(subscriber.read(), is(msgCount));

        writePackets(logAppenders.get(2), msgCount);
        assertThat(subscriber.read(), is(msgCount));

        // force the roll
        assertThat(subscriber.read(), is(msgCount));

        // Now you've hit an unclean buffer and can't proceed
        assertThat(subscriber.read(), is(0));

        aeron.conductor().close();
    }

    @Test
    public void subscriberBufferRollsShouldNotAffectOtherSessions() throws Exception
    {
        channel2Handler = eitherSessionHandler();

        final RingBuffer toMediaDriver = toDriverBuffer;
        final Aeron aeron = newAeron();
        final Subscriber subscriber = newSubscriber(aeron);
        final List<LogAppender> logAppenders = createLogAppenders(SESSION_ID);
        final List<LogAppender> otherLogAppenders = createLogAppenders(SESSION_ID_2);

        sendNewBufferNotification(directory.receiverDir(), NEW_SUBSCRIPTION_BUFFER_NOTIFICATION, 1L, SESSION_ID);

        aeron.conductor().doWork();
        skip(toMediaDriver, 1);

        final LogAppender logAppender = logAppenders.get(0);
        final int msgCount = logAppender.capacity() / sendBuffer.capacity();

        writePackets(logAppender, msgCount);
        assertThat(subscriber.read(), is(msgCount));

        writePackets(logAppenders.get(1), msgCount);
        assertThat(subscriber.read(), is(msgCount));

        writePackets(otherLogAppenders.get(0), 5);
        assertThat(subscriber.read(), is(5));

        aeron.conductor().close();
    }

    private DataHandler eitherSessionHandler()
    {
        return (buffer, offset, length, sessionId) ->
        {
            assertThat(buffer.getInt(offset), is(PACKET_VALUE));
            assertThat(sessionId, anyOf(is(SESSION_ID), is(SESSION_ID_2)));
        };
    }

    private DataHandler assertingHandler()
    {
        return (buffer, offset, length, sessionId) ->
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
        final int bytesToSend = atomicSendBuffer.capacity() - DataHeaderFlyweight.DEFAULT_HEADER_NULL_IDS.length;
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
            createTermBuffer(0L, NEW_SUBSCRIPTION_BUFFER_NOTIFICATION, directory.receiverDir(), sessionId);

        return createLogAppenders(termBuffers);
    }

    private List<Buffers> createTermBuffer(final long termId,
                                           final int msgTypeId,
                                           final File rootDir,
                                           final long sessionId) throws IOException
    {
        final List<Buffers> buffers = directory.createTermFile(rootDir, DESTINATION, sessionId, CHANNEL_ID);
        sendNewBufferNotification(rootDir, msgTypeId, termId, sessionId);

        return buffers;
    }

    private void sendNewBufferNotification(final File rootDir,
                                           final int msgTypeId,
                                           final long termId,
                                           final long sessionId)
    {
        final RingBuffer apiBuffer = toClientBuffer;
        newBufferMessage.channelId(CHANNEL_ID)
                        .sessionId(sessionId)
                        .termId(termId);

        IntStream.range(0, PAYLOAD_BUFFER_COUNT).forEach(
            (i) ->
            {
                newBufferMessage.bufferOffset(i, 0);
                newBufferMessage.bufferLength(i, LOG_MIN_SIZE);
            }
        );
        addBufferLocation(rootDir, termId, sessionId, LOG, 0);
        addBufferLocation(rootDir, termId, sessionId, STATE, BUFFER_COUNT);
        newBufferMessage.destination(DESTINATION);

        assertTrue(apiBuffer.write(msgTypeId, atomicSendBuffer, 0, newBufferMessage.length()));
    }

    private void addBufferLocation(final File dir,
                                   final long termId,
                                   final long sessionId,
                                   final Type type,
                                   final int start)
    {
        IntStream.range(0, BUFFER_COUNT).forEach(
            (i) ->
            {
                File term = termLocation(dir, sessionId, CHANNEL_ID, termId + i, true, DESTINATION, type);
                newBufferMessage.location(i + start, term.getAbsolutePath());
            }
        );
    }

    private Subscriber newSubscriber(final Aeron aeron)
    {
        final Subscriber.Context context = new Subscriber.Context()
            .destination(new Destination(DESTINATION))
            .channel(CHANNEL_ID, channel2Handler)
            .channel(CHANNEL_ID_2, emptyDataHandler());

        return aeron.newSubscriber(context);
    }

    private MessageHandler assertSubscriberMessageOfType(final int expectedMsgTypeId)
    {
        return (msgTypeId, buffer, index, length) ->
        {
            assertThat(msgTypeId, is(expectedMsgTypeId));

            subscriptionMessage.wrap(buffer, index);
            assertThat(subscriptionMessage.channelIds(), is(CHANNEL_IDs));
            assertThat(subscriptionMessage.destination(), is(DESTINATION));
        };
    }

    private DataHandler emptyDataHandler()
    {
        return (buffer, offset, length, sessionId) -> {};
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
            .toClientBuffer(toClientBuffer)
            .toDriverBuffer(toDriverBuffer)
            .invalidDestinationHandler(invalidDestination);

        return Aeron.newSingleMediaDriver(context);
    }

    private void assertChannelMessage(final RingBuffer mediaDriverBuffer, final int expectedMsgTypeId)
    {
        assertMsgRead(mediaDriverBuffer, (msgTypeId, buffer, index, length) ->
        {
            assertThat(msgTypeId, is(expectedMsgTypeId));

            publicationMessage.wrap(buffer, index);
            assertThat(publicationMessage.destination(), is(DESTINATION));
            assertThat(publicationMessage.channelId(), is(CHANNEL_ID));
            assertThat(publicationMessage.sessionId(), is(SESSION_ID));
        });
    }
}
