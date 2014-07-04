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
import uk.co.real_logic.aeron.conductor.*;
import uk.co.real_logic.aeron.util.AtomicArray;
import uk.co.real_logic.aeron.util.BufferRotationDescriptor;
import uk.co.real_logic.aeron.util.command.LogBuffersMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
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

import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.ON_NEW_PUBLICATION;

public class ClientConductorTest
{
    public static final int MAX_FRAME_LENGTH = 1024;
    public static final int COUNTER_BUFFER_SZ = 1024;

    public static final String DESTINATION = "udp://localhost:40124";
    public static final long CHANNEL_ID_1 = 2L;
    public static final long CHANNEL_ID_2 = 4L;
    public static final long SESSION_ID_1 = 13L;
    public static final long SESSION_ID_2 = 15L;
    public static final long TERM_ID_1 = 1L;
    public static final int SEND_BUFFER_CAPACITY = 1024;

    public static final int RING_BUFFER_SZ = (16 * 1024) + RingBufferDescriptor.TRAILER_LENGTH;
    public static final int BROADCAST_BUFFER_SZ = (16 * 1024) + BroadcastBufferDescriptor.TRAILER_LENGTH;
    public static final int LOG_BUFFER_SIZE = LogBufferDescriptor.LOG_MIN_SIZE;
    public static final long CORRELATION_ID = 2000;
    public static final int AWAIT_TIMEOUT = 100;
    public static final long PUBLICATION_WINDOW = 1024;

    private final LogBuffersMessageFlyweight newBufferMessage = new LogBuffersMessageFlyweight();
    private final ErrorFlyweight errorHeader = new ErrorFlyweight();

    private final ByteBuffer sendBuffer = ByteBuffer.allocate(SEND_BUFFER_CAPACITY);
    private final AtomicBuffer atomicSendBuffer = new AtomicBuffer(sendBuffer);

    private final AtomicBuffer toClientBuffer = new AtomicBuffer(new byte[BROADCAST_BUFFER_SZ]);
    private final CopyBroadcastReceiver toClientReceiver =
        new CopyBroadcastReceiver(new BroadcastReceiver(toClientBuffer));
    private final BroadcastTransmitter toClientTransmitter = new BroadcastTransmitter(toClientBuffer);

    private final RingBuffer toDriverBuffer = new ManyToOneRingBuffer(new AtomicBuffer(new byte[RING_BUFFER_SZ]));

    private final AtomicBuffer counterValuesBuffer = new AtomicBuffer(new byte[COUNTER_BUFFER_SZ]);

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

        doAnswer(
            invocation ->
            {
                sendNewBufferNotification(ON_NEW_PUBLICATION, SESSION_ID_1, TERM_ID_1);
                conductor.doWork();
                return null;
            }).when(signal).await(anyLong());

        conductor = new ClientConductor(
            toClientReceiver,
            errorHandler,
            mockBufferUsage,
            counterValuesBuffer,
            mediaDriverProxy,
            signal,
            AWAIT_TIMEOUT,
            PUBLICATION_WINDOW);

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
        addPublication();

        verify(mediaDriverProxy).addPublication(DESTINATION, CHANNEL_ID_1, SESSION_ID_1);
    }

    @Test(expected = MediaDriverTimeoutException.class)
    public void cannotCreatePublisherUntilBuffersMapped()
    {
        doAnswer(
            (invocation) ->
            {
                Thread.sleep(AWAIT_TIMEOUT + 1);
                return null;
            }).when(signal).await(anyLong());

        addPublication();
    }

    @Test
    public void conductorCachesPublicationInstances()
    {
        Publication firstPublication = addPublication();
        Publication secondPublication = addPublication();

        assertThat(firstPublication, sameInstance(secondPublication));
    }

    @Test
    public void closingPublicationShouldNotifyMediaDriver() throws Exception
    {
        Publication publication = addPublication();

        publication.release();

        verify(mediaDriverProxy).removePublication(DESTINATION, CHANNEL_ID_1, SESSION_ID_1);
    }

    @Test
    public void publicationsOnlyClosedOnLastRelease() throws Exception
    {
        Publication publication = addPublication();
        addPublication();

        publication.release();
        verify(mediaDriverProxy, never()).removePublication(DESTINATION, CHANNEL_ID_1, SESSION_ID_1);

        publication.release();
        verify(mediaDriverProxy).removePublication(DESTINATION, CHANNEL_ID_1, SESSION_ID_1);
    }

    @Test
    public void closingAPublicationDoesntRemoveOtherPublications() throws Exception
    {
        Publication publication = conductor.addPublication(DESTINATION, CHANNEL_ID_1, SESSION_ID_1);
        conductor.addPublication(DESTINATION, CHANNEL_ID_2, SESSION_ID_2);

        publication.release();

        verify(mediaDriverProxy).removePublication(DESTINATION, CHANNEL_ID_1, SESSION_ID_1);
        verify(mediaDriverProxy, never()).removePublication(DESTINATION, CHANNEL_ID_2, SESSION_ID_2);
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

    private Publication addPublication()
    {
        return conductor.addPublication(DESTINATION, CHANNEL_ID_1, SESSION_ID_1);
    }
}
