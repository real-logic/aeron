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
import org.junit.Test;
import uk.co.real_logic.aeron.conductor.BufferUsageStrategy;
import uk.co.real_logic.aeron.util.ErrorCode;
import uk.co.real_logic.aeron.util.command.NewBufferMessageFlyweight;
import uk.co.real_logic.aeron.util.command.SubscriptionMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.MessageHandler;
import uk.co.real_logic.aeron.util.concurrent.broadcast.BroadcastBufferDescriptor;
import uk.co.real_logic.aeron.util.concurrent.broadcast.BroadcastReceiver;
import uk.co.real_logic.aeron.util.concurrent.broadcast.BroadcastTransmitter;
import uk.co.real_logic.aeron.util.concurrent.broadcast.CopyBroadcastReceiver;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.ManyToOneRingBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferDescriptor;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.ErrorFlyweight;

import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.Subscription.DataHandler;
import static uk.co.real_logic.aeron.util.BufferRotationDescriptor.BUFFER_COUNT;
import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.*;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender.AppendStatus.SUCCESS;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.STATE_BUFFER_LENGTH;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferTestUtil.assertMsgRead;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferTestUtil.skip;

public class AeronTest
{
    private static final int MAX_FRAME_LENGTH = 1024;
    private static final int COUNTER_BUFFER_SZ = 1024;

    private static final String DESTINATION = "udp://localhost:40124";
    private static final String INVALID_DESTINATION = "udp://lo124";
    private static final long CHANNEL_ID_1 = 2L;
    private static final long CHANNEL_ID_2 = 4L;
    private static final long[] CHANNEL_IDS = {CHANNEL_ID_1, CHANNEL_ID_2};
    private static final long SESSION_ID_1 = 13L;
    private static final long SESSION_ID_2 = 15L;
    public static final long TERM_ID_1 = 1L;
    public static final long TERM_ID_2 = 11L;
    private static final int PACKET_VALUE = 37;
    private static final int SEND_BUFFER_CAPACITY = 1024;
    private static final int SCRATCH_BUFFER_CAPACITY = 1024;
    private static final DataHandler EMPTY_DATA_HANDLER = (buffer, offset, length, sessionId) -> {};

    public static final int RING_BUFFER_SZ = (16 * 1024) + RingBufferDescriptor.TRAILER_LENGTH;
    public static final int BROADCAST_BUFFER_SZ = (16 * 1024) + BroadcastBufferDescriptor.TRAILER_LENGTH;
    public static final int LOG_BUFFER_SIZE = LogBufferDescriptor.LOG_MIN_SIZE;

    private final InvalidDestinationHandler invalidDestination = mock(InvalidDestinationHandler.class);

    private DataHandler channel1Handler = EMPTY_DATA_HANDLER;

    private final NewBufferMessageFlyweight newBufferMessage = new NewBufferMessageFlyweight();
    private final SubscriptionMessageFlyweight subscriptionMessage = new SubscriptionMessageFlyweight();
    private final ErrorFlyweight errorHeader = new ErrorFlyweight();

    private final ByteBuffer sendBuffer = ByteBuffer.allocate(SEND_BUFFER_CAPACITY);
    private final AtomicBuffer atomicSendBuffer = new AtomicBuffer(sendBuffer);

    private final ByteBuffer scratchBuffer = ByteBuffer.allocate(SCRATCH_BUFFER_CAPACITY);
    private final AtomicBuffer atomicScratchBuffer = new AtomicBuffer(scratchBuffer);

    private final AtomicBuffer toClientBuffer = new AtomicBuffer(new byte[BROADCAST_BUFFER_SZ]);
    private final CopyBroadcastReceiver toClientReceiver =
        new CopyBroadcastReceiver(new BroadcastReceiver(toClientBuffer));
    private final BroadcastTransmitter toClientTransmitter = new BroadcastTransmitter(toClientBuffer);

    private final RingBuffer toDriverBuffer = new ManyToOneRingBuffer(new AtomicBuffer(new byte[RING_BUFFER_SZ]));

    private final AtomicBuffer counterValuesBuffer = new AtomicBuffer(new byte[COUNTER_BUFFER_SZ]);
    private final AtomicBuffer counterLabelsBuffer = new AtomicBuffer(new byte[COUNTER_BUFFER_SZ]);

    private Aeron aeron;

    private AtomicBuffer[] logBuffersSession1 = new AtomicBuffer[BUFFER_COUNT];
    private AtomicBuffer[] logBuffersSession2 = new AtomicBuffer[BUFFER_COUNT];
    private AtomicBuffer[] stateBuffersSession1 = new AtomicBuffer[BUFFER_COUNT];
    private AtomicBuffer[] stateBuffersSession2 = new AtomicBuffer[BUFFER_COUNT];
    private LogAppender[] appendersSession1 = new LogAppender[BUFFER_COUNT];
    private LogAppender[] appendersSession2 = new LogAppender[BUFFER_COUNT];
    private BufferUsageStrategy mockBufferUsage = mock(BufferUsageStrategy.class);

    @Before
    public void setUp() throws Exception
    {
        for (int i = 0; i < BUFFER_COUNT; i++)
        {
            logBuffersSession1[i] = new AtomicBuffer(new byte[LOG_BUFFER_SIZE]);
            stateBuffersSession1[i] = new AtomicBuffer(new byte[STATE_BUFFER_LENGTH]);
            logBuffersSession2[i] = new AtomicBuffer(new byte[LOG_BUFFER_SIZE]);
            stateBuffersSession2[i] = new AtomicBuffer(new byte[STATE_BUFFER_LENGTH]);

            when(mockBufferUsage.newBuffer(eq(SESSION_ID_1 + "-log-" + i), anyInt(), anyInt()))
                .thenReturn(logBuffersSession1[i]);
            when(mockBufferUsage.newBuffer(eq(SESSION_ID_1 + "-state-" + i), anyInt(), anyInt()))
                .thenReturn(stateBuffersSession1[i]);
            when(mockBufferUsage.newBuffer(eq(SESSION_ID_2 + "-log-" + i), anyInt(), anyInt()))
                .thenReturn(logBuffersSession2[i]);
            when(mockBufferUsage.newBuffer(eq(SESSION_ID_2 + "-state-" + i), anyInt(), anyInt()))
                .thenReturn(stateBuffersSession2[i]);

            appendersSession1[i] = new LogAppender(logBuffersSession1[i], stateBuffersSession1[i],
                                                   DataHeaderFlyweight.DEFAULT_HEADER_NULL_IDS, MAX_FRAME_LENGTH);
            appendersSession2[i] = new LogAppender(logBuffersSession2[i], stateBuffersSession2[i],
                                                   DataHeaderFlyweight.DEFAULT_HEADER_NULL_IDS, MAX_FRAME_LENGTH);
        }

        final Aeron.ClientContext ctx =
            new Aeron.ClientContext()
                .toClientBuffer(toClientReceiver)
                .toDriverBuffer(toDriverBuffer)
                .bufferUsageStrategy(mockBufferUsage)
                .invalidDestinationHandler(invalidDestination);

        ctx.counterLabelsBuffer(counterLabelsBuffer)
           .counterValuesBuffer(counterValuesBuffer);

        aeron = Aeron.newSingleMediaDriver(ctx);

        errorHeader.wrap(atomicScratchBuffer, 0);
    }

    @After
    public void tearDown()
    {
        aeron.close();
    }

    @Test
    public void registeringSubscriberNotifiesMediaDriver() throws Exception
    {
        final Subscription subscription = aeron.addSubscription(DESTINATION, CHANNEL_ID_1, EMPTY_DATA_HANDLER);

        aeron.conductor().doWork();

        assertMsgRead(toDriverBuffer, assertSubscriberMessageOfType(ADD_SUBSCRIPTION, CHANNEL_ID_1));

        assertThat(subscription.read(), is(0));
    }

    @Test
    public void removingSubscriberNotifiesMediaDriver()
    {
        final RingBuffer toMediaDriver = toDriverBuffer;
        final Subscription subscription = aeron.addSubscription(DESTINATION, CHANNEL_ID_1, channel1Handler);

        aeron.conductor().doWork();
        skip(toMediaDriver, 1);

        subscription.release();
        aeron.conductor().doWork();

        assertMsgRead(toMediaDriver, assertSubscriberMessageOfType(REMOVE_SUBSCRIPTION, CHANNEL_ID_1));
    }

    @Test
    public void clientCodeNotifiedOfAnInvalidDestination()
    {
        subscriptionMessage.wrap(atomicSendBuffer, 0);
        subscriptionMessage.channelIds(CHANNEL_IDS);
        subscriptionMessage.destination(INVALID_DESTINATION);

        errorHeader.wrap(atomicSendBuffer, subscriptionMessage.length());
        errorHeader.errorCode(ErrorCode.INVALID_DESTINATION);
        errorHeader.offendingFlyweight(subscriptionMessage, subscriptionMessage.length());
        errorHeader.frameLength(ErrorFlyweight.HEADER_LENGTH + subscriptionMessage.length());

        toClientTransmitter.transmit(ERROR_RESPONSE,
                atomicSendBuffer,
                subscriptionMessage.length(),
                errorHeader.frameLength());

        aeron.conductor().doWork();

        verify(invalidDestination).onInvalidDestination(INVALID_DESTINATION);
    }

    @Test
    public void subscriberCanReceiveAMessage() throws Exception
    {
        channel1Handler = sessionAssertingHandler();

        final Subscription subscription = aeron.addSubscription(DESTINATION, CHANNEL_ID_1, channel1Handler);

        sendNewBufferNotification(NEW_SUBSCRIPTION_BUFFER_EVENT, SESSION_ID_1, TERM_ID_1);

        aeron.conductor().doWork();
        skip(toDriverBuffer, 1);

        writePackets(appendersSession1[0], 1);

        assertThat(subscription.read(), is(1));
    }

    @Test
    public void subscriberCanReceivePacketsFromMultipleSessions() throws Exception
    {
        channel1Handler = eitherSessionAssertingHandler();

        final Subscription subscription = aeron.addSubscription(DESTINATION, CHANNEL_ID_1, channel1Handler);

        sendNewBufferNotification(NEW_SUBSCRIPTION_BUFFER_EVENT, SESSION_ID_1, TERM_ID_1);
        sendNewBufferNotification(NEW_SUBSCRIPTION_BUFFER_EVENT, SESSION_ID_2, TERM_ID_2);

        aeron.conductor().doWork();
        skip(toDriverBuffer, 1);

        writePackets(appendersSession1[0], 1);
        writePackets(appendersSession2[0], 1);
        assertThat(subscription.read(), is(2));
    }

    @Test
    public void receivingEnoughPacketsCausesSubscriberBufferRoll() throws Exception
    {
        channel1Handler = sessionAssertingHandler();

        final Subscription subscription = aeron.addSubscription(DESTINATION, CHANNEL_ID_1, channel1Handler);

        sendNewBufferNotification(NEW_SUBSCRIPTION_BUFFER_EVENT, SESSION_ID_1, TERM_ID_1);

        aeron.conductor().doWork();
        skip(toDriverBuffer, 1);

        final LogAppender logAppender = appendersSession1[0];
        final int msgCount = logAppender.capacity() / sendBuffer.capacity();

        writePackets(logAppender, msgCount);
        assertThat(subscription.read(), is(msgCount));

        // cleaning is triggered by the subscriber and not the subscriber
        // so we clean two ahead of the current buffer
        cleanBuffers(2);
        aeron.conductor().doWork();

        writePackets(appendersSession1[1], msgCount);
        assertThat(subscription.read(), is(msgCount));

        cleanBuffers(0);
        aeron.conductor().doWork();

        writePackets(appendersSession1[2], msgCount);
        assertThat(subscription.read(), is(msgCount));

        cleanBuffers(1);
        aeron.conductor().doWork();

        writePackets(logAppender, msgCount);
        assertThat(subscription.read(), is(msgCount));
    }

    @Test
    public void subscriberBufferRollsDoNotOverflowTheCleanedBuffer() throws Exception
    {
        channel1Handler = sessionAssertingHandler();

        final Subscription subscription = aeron.addSubscription(DESTINATION, CHANNEL_ID_1, channel1Handler);

        sendNewBufferNotification(NEW_SUBSCRIPTION_BUFFER_EVENT, SESSION_ID_1, TERM_ID_1);

        aeron.conductor().doWork();
        skip(toDriverBuffer, 1);

        final LogAppender logAppender = appendersSession1[0];
        final int msgCount = logAppender.capacity() / SEND_BUFFER_CAPACITY;

        writePackets(logAppender, msgCount);
        assertThat(subscription.read(), is(msgCount));

        writePackets(appendersSession1[1], msgCount);
        assertThat(subscription.read(), is(msgCount));

        writePackets(appendersSession1[2], msgCount);
        assertThat(subscription.read(), is(msgCount));

        // force the roll
        assertThat(subscription.read(), is(msgCount));

        // Now you've hit an unclean buffer and can't proceed
        assertThat(subscription.read(), is(0));
    }

    @Test
    public void subscriberBufferRollsShouldNotAffectOtherSessions() throws Exception
    {
        channel1Handler = eitherSessionAssertingHandler();

        final RingBuffer toMediaDriver = toDriverBuffer;
        final Subscription subscription = aeron.addSubscription(DESTINATION, CHANNEL_ID_1, channel1Handler);

        sendNewBufferNotification(NEW_SUBSCRIPTION_BUFFER_EVENT, SESSION_ID_1, TERM_ID_1);
        sendNewBufferNotification(NEW_SUBSCRIPTION_BUFFER_EVENT, SESSION_ID_2, TERM_ID_2);

        aeron.conductor().doWork();
        skip(toMediaDriver, 1);

        final LogAppender logAppender = appendersSession1[0];
        final int msgCount = logAppender.capacity() / sendBuffer.capacity();

        writePackets(logAppender, msgCount);
        assertThat(subscription.read(), is(msgCount));

        writePackets(appendersSession1[1], msgCount);
        assertThat(subscription.read(), is(msgCount));

        writePackets(appendersSession2[0], 5);
        assertThat(subscription.read(), is(5));
    }

    private DataHandler eitherSessionAssertingHandler()
    {
        return (buffer, offset, length, sessionId) ->
        {
            assertThat(buffer.getInt(offset), is(PACKET_VALUE));
            assertThat(sessionId, anyOf(is(SESSION_ID_1), is(SESSION_ID_2)));
        };
    }

    private DataHandler sessionAssertingHandler()
    {
        return (buffer, offset, length, sessionId) ->
        {
            assertThat(buffer.getInt(offset), is(PACKET_VALUE));
            assertThat(sessionId, is(SESSION_ID_1));
        };
    }

    private void writePackets(final LogAppender logAppender, final int events)
    {
        final int bytesToSend = atomicSendBuffer.capacity() - DataHeaderFlyweight.DEFAULT_HEADER_NULL_IDS.length;
        for (int i = 0; i < events; i++)
        {
            atomicSendBuffer.putInt(0, PACKET_VALUE);
            assertThat(logAppender.append(atomicSendBuffer, 0, bytesToSend), is(SUCCESS));
        }
    }

    private void sendNewBufferNotification(final int msgTypeId, final long sessionId, final long termId)
    {
        newBufferMessage.wrap(atomicScratchBuffer, 0);
        newBufferMessage.channelId(CHANNEL_ID_1)
                        .sessionId(sessionId)
                        .termId(termId);

        for (int i = 0; i < BUFFER_COUNT; i++)
        {
            newBufferMessage.location(i, sessionId + "-log-" + i);
            newBufferMessage.bufferOffset(i, 0);
            newBufferMessage.bufferLength(i, LOG_BUFFER_SIZE);
        }

        for (int i = 0; i < BUFFER_COUNT; i++)
        {
            newBufferMessage.location(i + BUFFER_COUNT, sessionId + "-state-" + i);
            newBufferMessage.bufferOffset(i + BUFFER_COUNT, 0);
            newBufferMessage.bufferLength(i + BUFFER_COUNT, STATE_BUFFER_LENGTH);
        }

        newBufferMessage.destination(DESTINATION);

        toClientTransmitter.transmit(msgTypeId, atomicScratchBuffer, 0, newBufferMessage.length());
    }

    private void cleanBuffer(final AtomicBuffer buffer)
    {
        buffer.putBytes(0, new byte[buffer.capacity()]);
    }

    private void cleanBuffers(final int index)
    {
        cleanBuffer(logBuffersSession1[index]);
        cleanBuffer(stateBuffersSession1[index]);
    }

    private MessageHandler assertSubscriberMessageOfType(final int expectedMsgTypeId, final long... channelIds)
    {
        return (msgTypeId, buffer, index, length) ->
        {
            assertThat(msgTypeId, is(expectedMsgTypeId));

            subscriptionMessage.wrap(buffer, index);
            assertThat(subscriptionMessage.channelIds(), is(channelIds));
            assertThat(subscriptionMessage.destination(), is(DESTINATION));
        };
    }
}
