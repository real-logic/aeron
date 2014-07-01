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
import org.junit.Ignore;
import org.junit.Test;
import uk.co.real_logic.aeron.conductor.*;
import uk.co.real_logic.aeron.util.AtomicArray;
import uk.co.real_logic.aeron.util.BufferRotationDescriptor;
import uk.co.real_logic.aeron.util.command.NewBufferMessageFlyweight;
import uk.co.real_logic.aeron.util.command.PublicationMessageFlyweight;
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
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.Subscription.DataHandler;
import static uk.co.real_logic.aeron.util.BufferRotationDescriptor.rotateId;
import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.*;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender.AppendStatus.SUCCESS;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferTestUtil.assertMsgRead;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferTestUtil.skip;

public class ClientConductorTest
{
    private static final int MAX_FRAME_LENGTH = 1024;
    private static final int COUNTER_BUFFER_SZ = 1024;

    private static final String DESTINATION = "udp://localhost:40124";
    private static final long CHANNEL_ID_1 = 2L;
    private static final long CHANNEL_ID_2 = 4L;
    private static final long SESSION_ID_1 = 13L;
    private static final long SESSION_ID_2 = 15L;
    public static final long TERM_ID_1 = 1L;
    public static final long TERM_ID_2 = 11L;
    private static final int PACKET_VALUE = 37;
    private static final int SEND_BUFFER_CAPACITY = 1024;
    private static final DataHandler EMPTY_DATA_HANDLER = (buffer, offset, length, sessionId) -> {};

    public static final int RING_BUFFER_SZ = (16 * 1024) + RingBufferDescriptor.TRAILER_LENGTH;
    public static final int BROADCAST_BUFFER_SZ = (16 * 1024) + BroadcastBufferDescriptor.TRAILER_LENGTH;
    public static final int LOG_BUFFER_SIZE = LogBufferDescriptor.LOG_MIN_SIZE;
    private static final long CORRELATION_ID = 2000;
    public static final int AWAIT_TIMEOUT = 100;

    private final InvalidDestinationHandler invalidDestination = mock(InvalidDestinationHandler.class);

    private DataHandler channel1Handler = EMPTY_DATA_HANDLER;

    private final PublicationMessageFlyweight publicationMessage = new PublicationMessageFlyweight();
    private final NewBufferMessageFlyweight newBufferMessage = new NewBufferMessageFlyweight();
    private final SubscriptionMessageFlyweight subscriptionMessage = new SubscriptionMessageFlyweight();
    private final ErrorFlyweight errorHeader = new ErrorFlyweight();

    private final ByteBuffer sendBuffer = ByteBuffer.allocate(SEND_BUFFER_CAPACITY);
    private final AtomicBuffer atomicSendBuffer = new AtomicBuffer(sendBuffer);

    private final AtomicBuffer toClientBuffer = new AtomicBuffer(new byte[BROADCAST_BUFFER_SZ]);
    private final CopyBroadcastReceiver toClientReceiver =
        new CopyBroadcastReceiver(new BroadcastReceiver(toClientBuffer));
    private final BroadcastTransmitter toClientTransmitter = new BroadcastTransmitter(toClientBuffer);

    private final RingBuffer toDriverBuffer = new ManyToOneRingBuffer(new AtomicBuffer(new byte[RING_BUFFER_SZ]));

    private final AtomicBuffer counterValuesBuffer = new AtomicBuffer(new byte[COUNTER_BUFFER_SZ]);
    private final AtomicBuffer counterLabelsBuffer = new AtomicBuffer(new byte[COUNTER_BUFFER_SZ]);

    private Aeron aeron;

    private AtomicBuffer[] logBuffersSession1 = new AtomicBuffer[BufferRotationDescriptor.BUFFER_COUNT];
    private AtomicBuffer[] logBuffersSession2 = new AtomicBuffer[BufferRotationDescriptor.BUFFER_COUNT];
    private AtomicBuffer[] stateBuffersSession1 = new AtomicBuffer[BufferRotationDescriptor.BUFFER_COUNT];
    private AtomicBuffer[] stateBuffersSession2 = new AtomicBuffer[BufferRotationDescriptor.BUFFER_COUNT];
    private LogAppender[] appendersSession1 = new LogAppender[BufferRotationDescriptor.BUFFER_COUNT];
    private LogAppender[] appendersSession2 = new LogAppender[BufferRotationDescriptor.BUFFER_COUNT];
    private BufferUsageStrategy mockBufferUsage = mock(BufferUsageStrategy.class);

    private Signal signal;
    private MediaDriverProxy mediaDriverProxy;
    private ClientConductor conductor;
    private ConductorErrorHandler errorHandler;
    private AtomicArray<Subscription> subscriberChannels;

    @Before
    public void setUp() throws Exception
    {

        for (int i = 0; i < BufferRotationDescriptor.BUFFER_COUNT; i++)
        {
            logBuffersSession1[i] = new AtomicBuffer(new byte[LOG_BUFFER_SIZE]);
            stateBuffersSession1[i] = new AtomicBuffer(new byte[LogBufferDescriptor.STATE_BUFFER_LENGTH]);
            logBuffersSession2[i] = new AtomicBuffer(new byte[LOG_BUFFER_SIZE]);
            stateBuffersSession2[i] = new AtomicBuffer(new byte[LogBufferDescriptor.STATE_BUFFER_LENGTH]);

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

        mediaDriverProxy = mock(MediaDriverProxy.class);
        signal = mock(Signal.class);
        subscriberChannels = new AtomicArray<>();
        errorHandler = mock(ConductorErrorHandler.class);

        when(mediaDriverProxy.addPublication(any(), anyLong(), anyLong())).thenReturn(CORRELATION_ID);

        doAnswer(invocation ->
        {
            sendNewBufferNotification(NEW_PUBLICATION_BUFFER_EVENT, SESSION_ID_1, TERM_ID_1);
            conductor.doWork();
            return null;
        }).when(signal).await(anyLong());

        conductor = new ClientConductor(
                mock(RingBuffer.class),
                toClientReceiver,
                toDriverBuffer,
                subscriberChannels,
                errorHandler,
                mockBufferUsage,
                counterValuesBuffer,
                mediaDriverProxy,
                signal,
                AWAIT_TIMEOUT);

        newBufferMessage.wrap(atomicSendBuffer, 0);
        errorHeader.wrap(atomicSendBuffer, 0);
    }

    @After
    public void tearDown()
    {
        conductor.close();
    }

    @Test
    public void creatingChannelsShouldNotifyMediaDriver() throws Exception
    {
        newPublication(aeron);

        verify(mediaDriverProxy).addPublication(DESTINATION, CHANNEL_ID_1, SESSION_ID_1);
    }

    @Test(expected = MediaDriverTimeoutException.class)
    public void cannotCreatePublisherUntilBuffersMapped() throws Exception
    {
        doAnswer(invocation ->
        {
            Thread.sleep(AWAIT_TIMEOUT + 1);
            return null;
        }).when(signal).await(anyLong());

        newPublication(aeron);
    }

    @Ignore
    @Test
    public void shouldRotateBuffersOnceFull() throws Exception
    {
        final RingBuffer toMediaDriver = toDriverBuffer;
        final Publication publication = newPublication(aeron);
        aeron.conductor().doWork();

        sendNewBufferNotification(NEW_PUBLICATION_BUFFER_EVENT, SESSION_ID_1, TERM_ID_1);

        final int capacity = logBuffersSession1[0].capacity();
        final int msgCount = (4 * capacity) / SEND_BUFFER_CAPACITY;

        aeron.conductor().doWork();
        skip(toMediaDriver, 1);
        boolean previousAppend = true;
        int bufferId = 0;
        for (int i = 0; i < msgCount; i++)
        {
            final boolean appended = publication.offer(atomicSendBuffer);
            aeron.conductor().doWork();

            assertTrue(previousAppend || appended);
            previousAppend = appended;

            if (!appended)
            {
                assertCleanTermRequested(toMediaDriver);
                cleanBuffer(logBuffersSession1[bufferId]);
                cleanBuffer(stateBuffersSession1[bufferId]);
                bufferId = rotateId(bufferId);
            }
        }
    }

    @Ignore
    @Test
    public void removingChannelsShouldNotifyMediaDriver() throws Exception
    {
        final RingBuffer toMediaDriver = toDriverBuffer;
        final Publication publication = newPublication(aeron);
        final ClientConductor adminThread = aeron.conductor();

        adminThread.doWork();
        skip(toMediaDriver, 1);

        publication.close();
        adminThread.doWork();

        assertChannelMessage(toMediaDriver, REMOVE_PUBLICATION);
    }

    @Ignore
    @Test
    public void closingASourceRemovesItsAssociatedChannels() throws Exception
    {
        final Publication publication = aeron.newPublication(DESTINATION, CHANNEL_ID_1, SESSION_ID_1);
        final ClientConductor adminThread = aeron.conductor();

        adminThread.doWork();
        skip(toDriverBuffer, 1);

        publication.close();
        adminThread.doWork();

        assertChannelMessage(toDriverBuffer, REMOVE_PUBLICATION);
    }

    @Ignore
    @Test
    public void closingASourceDoesNotRemoveOtherChannels() throws Exception
    {
        aeron.newPublication(DESTINATION, CHANNEL_ID_1, SESSION_ID_1);
        final Publication otherPublication = aeron.newPublication(DESTINATION, CHANNEL_ID_1, SESSION_ID_1 + 1);
        final ClientConductor clientConductor = aeron.conductor();

        clientConductor.doWork();
        skip(toDriverBuffer, 1);

        otherPublication.close();
        clientConductor.doWork();

        skip(toDriverBuffer, 0);
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
        IntStream.range(0, events).forEach(
            (i) ->
            {
                atomicSendBuffer.putInt(0, PACKET_VALUE);
                assertThat(logAppender.append(atomicSendBuffer, 0, bytesToSend), is(SUCCESS));
            }
        );
    }

    private void sendNewBufferNotification(final int msgTypeId,
                                           final long sessionId,
                                           final long termId)
    {
        newBufferMessage.channelId(CHANNEL_ID_1)
                        .sessionId(sessionId)
                        .correlationId(CORRELATION_ID)
                        .termId(termId);

        IntStream.range(0, BufferRotationDescriptor.BUFFER_COUNT).forEach(
            (i) ->
            {
                newBufferMessage.location(i, sessionId + "-log-" + i);
                newBufferMessage.bufferOffset(i, 0);
                newBufferMessage.bufferLength(i, LOG_BUFFER_SIZE);
            }
        );

        IntStream.range(0, BufferRotationDescriptor.BUFFER_COUNT).forEach(
            (i) ->
            {
                newBufferMessage.location(i + BufferRotationDescriptor.BUFFER_COUNT, sessionId + "-state-" + i);
                newBufferMessage.bufferOffset(i + BufferRotationDescriptor.BUFFER_COUNT, 0);
                newBufferMessage.bufferLength(i + BufferRotationDescriptor.BUFFER_COUNT,
                        LogBufferDescriptor.STATE_BUFFER_LENGTH);
            }
        );

        newBufferMessage.destination(DESTINATION);

        toClientTransmitter.transmit(msgTypeId, atomicSendBuffer, 0, newBufferMessage.length());
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

    private Subscription newSubscriber(final Aeron aeron)
    {
        return aeron.newSubscription(DESTINATION, CHANNEL_ID_1, channel1Handler);
    }

    private MessageHandler assertSubscriberMessageOfType(final int expectedMsgTypeId, final long ... channelIds)
    {
        return (msgTypeId, buffer, index, length) ->
        {
            assertThat(msgTypeId, is(expectedMsgTypeId));

            subscriptionMessage.wrap(buffer, index);
            assertThat(subscriptionMessage.channelIds(), is(channelIds));
            assertThat(subscriptionMessage.destination(), is(DESTINATION));
        };
    }

    private Publication newPublication(final Aeron aeron)
    {
        return conductor.newPublication(DESTINATION, CHANNEL_ID_1, SESSION_ID_1);
    }

    private void assertChannelMessage(final RingBuffer mediaDriverBuffer, final int expectedMsgTypeId)
    {
        assertMsgRead(mediaDriverBuffer, (msgTypeId, buffer, index, length) ->
        {
            assertThat(msgTypeId, is(expectedMsgTypeId));

            publicationMessage.wrap(buffer, index);
            assertThat(publicationMessage.destination(), is(DESTINATION));
            assertThat(publicationMessage.channelId(), is(CHANNEL_ID_1));
            assertThat(publicationMessage.sessionId(), is(SESSION_ID_1));
        });
    }

    private void assertCleanTermRequested(final RingBuffer toMediaDriver)
    {
        assertMsgRead(toMediaDriver,
                (msgTypeId, buffer, index, length) -> assertThat(msgTypeId, is(CLEAN_TERM_BUFFER)));
    }
}
