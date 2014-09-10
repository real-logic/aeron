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
import uk.co.real_logic.aeron.common.command.PublicationReadyFlyweight;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.broadcast.BroadcastBufferDescriptor;
import uk.co.real_logic.aeron.common.concurrent.broadcast.BroadcastReceiver;
import uk.co.real_logic.aeron.common.concurrent.broadcast.BroadcastTransmitter;
import uk.co.real_logic.aeron.common.concurrent.broadcast.CopyBroadcastReceiver;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogAppender;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor;
import uk.co.real_logic.aeron.common.concurrent.ringbuffer.ManyToOneRingBuffer;
import uk.co.real_logic.aeron.common.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.common.concurrent.ringbuffer.RingBufferDescriptor;
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.common.protocol.ErrorFlyweight;

import java.nio.ByteBuffer;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static uk.co.real_logic.aeron.common.TermHelper.BUFFER_COUNT;
import static uk.co.real_logic.aeron.common.TermHelper.termIdToBufferIndex;
import static uk.co.real_logic.aeron.common.command.ControlProtocolEvents.ON_CONNECTION_READY;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogAppender.AppendStatus.SUCCESS;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.STATE_BUFFER_LENGTH;

public class AeronTest extends MockBufferUsage
{
    private static final int COUNTER_BUFFER_SZ = 1024;

    private static final String CHANNEL = "udp://localhost:40124";
    private static final int STREAM_ID_1 = 2;
    private static final int SESSION_ID_1 = 13;
    private static final int SESSION_ID_2 = 15;
    private static final int TERM_ID_1 = 1;
    private static final int TERM_ID_2 = 11;
    private static final int PACKET_VALUE = 37;
    private static final int SEND_BUFFER_CAPACITY = 1024;
    private static final int SCRATCH_BUFFER_CAPACITY = 1024;
    private static final DataHandler EMPTY_DATA_HANDLER = (buffer, offset, length, sessionId, flags) -> {
    };

    private static final int RING_BUFFER_SZ = (16 * 1024) + RingBufferDescriptor.TRAILER_LENGTH;
    private static final int BROADCAST_BUFFER_SZ = (16 * 1024) + BroadcastBufferDescriptor.TRAILER_LENGTH;
    private static final int LOG_BUFFER_SIZE = LogBufferDescriptor.MIN_LOG_SIZE;
    private static final int FRAME_COUNT_LIMIT = Integer.MAX_VALUE;

    private DataHandler channel1Handler = EMPTY_DATA_HANDLER;

    private final PublicationReadyFlyweight newBufferMessage = new PublicationReadyFlyweight();
    private final ErrorFlyweight errorHeader = new ErrorFlyweight();

    private final ByteBuffer sendBuffer = ByteBuffer.allocate(SEND_BUFFER_CAPACITY);
    private final AtomicBuffer atomicSendBuffer = new AtomicBuffer(sendBuffer);

    private final ByteBuffer scratchBuffer = ByteBuffer.allocate(SCRATCH_BUFFER_CAPACITY);
    private final AtomicBuffer atomicScratchBuffer = new AtomicBuffer(scratchBuffer);

    private final AtomicBuffer toClientBuffer = new AtomicBuffer(new byte[BROADCAST_BUFFER_SZ]);
    private final CopyBroadcastReceiver toClientReceiver = new CopyBroadcastReceiver(new BroadcastReceiver(toClientBuffer));
    private final BroadcastTransmitter toClientTransmitter = new BroadcastTransmitter(toClientBuffer);

    private final RingBuffer toDriverBuffer = new ManyToOneRingBuffer(new AtomicBuffer(new byte[RING_BUFFER_SZ]));

    private final AtomicBuffer counterValuesBuffer = new AtomicBuffer(new byte[COUNTER_BUFFER_SZ]);
    private final AtomicBuffer counterLabelsBuffer = new AtomicBuffer(new byte[COUNTER_BUFFER_SZ]);

    private Aeron aeron;

    @Before
    public void setUp() throws Exception
    {
        final Aeron.Context ctx = new Aeron.Context()
            .toClientBuffer(toClientReceiver)
            .toDriverBuffer(toDriverBuffer)
            .bufferManager(mockBufferUsage);

        ctx.counterLabelsBuffer(counterLabelsBuffer)
           .countersBuffer(counterValuesBuffer);

        aeron = new Aeron(ctx);

        errorHeader.wrap(atomicScratchBuffer, 0);
    }

    @After
    public void tearDown()
    {
        aeron.close();
    }

    @Ignore("port to a proper unit test")
    @Test
    public void receivingEnoughPacketsCausesSubscriberBufferRoll()
    {
        channel1Handler = sessionAssertingHandler();

        final Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID_1, channel1Handler);

        sendNewBufferNotification(ON_CONNECTION_READY, SESSION_ID_1, TERM_ID_1);

        aeron.conductor().doWork();
        skip(toDriverBuffer, 1);

        final LogAppender logAppender = appendersSession1[termIdToBufferIndex(TERM_ID_1)];
        final int msgCount = logAppender.capacity() / sendBuffer.capacity();

        writePackets(logAppender, msgCount);
        assertThat(subscription.poll(FRAME_COUNT_LIMIT), is(msgCount));

        // cleaning is triggered by the subscriber and not the subscriber
        // so we clean two ahead of the current buffer
        cleanBuffers(termIdToBufferIndex(TERM_ID_1 + 2));
        aeron.conductor().doWork();

        writePackets(appendersSession1[termIdToBufferIndex(TERM_ID_1 + 1)], msgCount);
        assertThat(subscription.poll(FRAME_COUNT_LIMIT), is(msgCount));

        cleanBuffers(termIdToBufferIndex(TERM_ID_1));
        aeron.conductor().doWork();

        writePackets(appendersSession1[termIdToBufferIndex(TERM_ID_1 + 2)], msgCount);
        assertThat(subscription.poll(FRAME_COUNT_LIMIT), is(msgCount));

        cleanBuffers(termIdToBufferIndex(TERM_ID_1 + 1));
        aeron.conductor().doWork();

        writePackets(logAppender, msgCount);
        assertThat(subscription.poll(FRAME_COUNT_LIMIT), is(msgCount));
    }

    @Ignore("port to a proper unit test")
    @Test
    public void subscriberBufferRollsShouldNotAffectOtherSessions()
    {
        channel1Handler = eitherSessionAssertingHandler();

        final RingBuffer toMediaDriver = toDriverBuffer;
        final Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID_1, channel1Handler);

        sendNewBufferNotification(ON_CONNECTION_READY, SESSION_ID_1, TERM_ID_1);
        sendNewBufferNotification(ON_CONNECTION_READY, SESSION_ID_2, TERM_ID_2);

        aeron.conductor().doWork();
        skip(toMediaDriver, 1);

        final LogAppender logAppender = appendersSession1[termIdToBufferIndex(TERM_ID_1)];
        final int msgCount = logAppender.capacity() / sendBuffer.capacity();

        writePackets(logAppender, msgCount);
        assertThat(subscription.poll(FRAME_COUNT_LIMIT), is(msgCount));

        writePackets(appendersSession1[termIdToBufferIndex(TERM_ID_1 + 1)], msgCount);
        assertThat(subscription.poll(FRAME_COUNT_LIMIT), is(msgCount));

        writePackets(appendersSession2[termIdToBufferIndex(TERM_ID_2)], 5);
        assertThat(subscription.poll(FRAME_COUNT_LIMIT), is(5));
    }

    private DataHandler eitherSessionAssertingHandler()
    {
        return (buffer, offset, length, sessionId, flags) ->
        {
            assertThat(buffer.getInt(offset), is(PACKET_VALUE));
            assertThat(sessionId, anyOf(is(SESSION_ID_1), is(SESSION_ID_2)));
        };
    }

    private DataHandler sessionAssertingHandler()
    {
        return (buffer, offset, length, sessionId, flags) ->
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

    private void sendNewBufferNotification(final int msgTypeId, final int sessionId, final int termId)
    {
        newBufferMessage.wrap(atomicScratchBuffer, 0);
        newBufferMessage.streamId(STREAM_ID_1)
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

        newBufferMessage.channel(CHANNEL);

        toClientTransmitter.transmit(msgTypeId, atomicScratchBuffer, 0, newBufferMessage.length());
    }

    private void cleanBuffer(final AtomicBuffer buffer)
    {
        buffer.setMemory(0, buffer.capacity(), (byte)0);
    }

    private void cleanBuffers(final int index)
    {
        cleanBuffer(logBuffersSession1[index]);
        cleanBuffer(stateBuffersSession1[index]);
    }

    private static void skip(final RingBuffer ringBuffer, int count)
    {
        ringBuffer.read((msgTypeId, buffer, index, length) -> {}, count);
    }
}
