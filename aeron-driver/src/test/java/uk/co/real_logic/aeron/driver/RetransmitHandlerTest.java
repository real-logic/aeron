/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
package uk.co.real_logic.aeron.driver;

import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import uk.co.real_logic.aeron.common.FeedbackDelayGenerator;
import uk.co.real_logic.agrona.TimerWheel;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.*;
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.common.protocol.HeaderFlyweight;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.stream.IntStream;

import static org.mockito.Mockito.*;
import static uk.co.real_logic.agrona.BitUtil.align;

@RunWith(Theories.class)
public class RetransmitHandlerTest
{
    private static final int MTU_LENGTH = 1024;
    private static final int TERM_BUFFER_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int META_DATA_BUFFER_LENGTH = LogBufferDescriptor.TERM_META_DATA_LENGTH;
    private static final byte[] DATA = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
    private static final int MESSAGE_LENGTH = DataHeaderFlyweight.HEADER_LENGTH + DATA.length;
    private static final int ALIGNED_FRAME_LENGTH = align(MESSAGE_LENGTH, FrameDescriptor.FRAME_ALIGNMENT);
    private static final int SESSION_ID = 0x5E55101D;
    private static final int STREAM_ID = 0x5400E;
    private static final int TERM_ID = 0x7F003355;

    private static final FeedbackDelayGenerator DELAY_GENERATOR = () -> TimeUnit.MILLISECONDS.toNanos(20);
    private static final FeedbackDelayGenerator ZERO_DELAY_GENERATOR = () -> TimeUnit.MILLISECONDS.toNanos(0);
    private static final FeedbackDelayGenerator LINGER_GENERATOR = () -> TimeUnit.MILLISECONDS.toNanos(40);

    private final UnsafeBuffer termBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(TERM_BUFFER_LENGTH));
    private final UnsafeBuffer metaDataBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(META_DATA_BUFFER_LENGTH));

    private final LogAppender logAppender = new LogAppender(
        termBuffer, metaDataBuffer, DataHeaderFlyweight.createDefaultHeader(0, 0 , 0), 1024);

    private final UnsafeBuffer rcvBuffer = new UnsafeBuffer(new byte[MESSAGE_LENGTH]);
    private DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();

    private long currentTime;

    private final TimerWheel wheel = new TimerWheel(
        () -> currentTime,
        Configuration.CONDUCTOR_TICK_DURATION_US,
        TimeUnit.MICROSECONDS,
        Configuration.CONDUCTOR_TICKS_PER_WHEEL);

    private final RetransmitSender retransmitSender = mock(RetransmitSender.class);
    private final SystemCounters systemCounters = mock(SystemCounters.class);

    private RetransmitHandler handler = new RetransmitHandler(
        wheel, systemCounters, DELAY_GENERATOR, LINGER_GENERATOR, retransmitSender, TERM_ID, TERM_BUFFER_LENGTH);

    @DataPoint
    public static final BiConsumer<RetransmitHandlerTest, Integer> SENDER_ADD_DATA_FRAME =
        (h, i) -> h.addSentDataFrame();

    @DataPoint
    public static final BiConsumer<RetransmitHandlerTest, Integer> RECEIVER_ADD_DATA_FRAME =
        RetransmitHandlerTest::addReceivedDataFrame;

    @Theory
    public void shouldRetransmitOnNak(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 5);
        handler.onNak(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH);
        processTimersUntil(() -> wheel.clock().time() >= TimeUnit.MILLISECONDS.toNanos(100));

        verify(retransmitSender).send(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH);
    }

    @Theory
    public void shouldNotRetransmitOnNakWhileInLinger(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 5);
        handler.onNak(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH);
        processTimersUntil(() -> wheel.clock().time() >= TimeUnit.MILLISECONDS.toNanos(40));
        handler.onNak(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH);
        processTimersUntil(() -> wheel.clock().time() >= TimeUnit.MILLISECONDS.toNanos(100));

        verify(retransmitSender).send(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH);
    }

    @Theory
    public void shouldRetransmitOnNakAfterLinger(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 5);
        handler.onNak(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH);
        processTimersUntil(() -> wheel.clock().time() >= TimeUnit.MILLISECONDS.toNanos(100));
        handler.onNak(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH);
        processTimersUntil(() -> wheel.clock().time() >= TimeUnit.MILLISECONDS.toNanos(200));

        verify(retransmitSender, times(2)).send(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH);
    }

    @Theory
    public void shouldRetransmitOnMultipleNaks(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 5);
        handler.onNak(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH);
        handler.onNak(TERM_ID, offsetOfFrame(1), ALIGNED_FRAME_LENGTH);
        processTimersUntil(() -> wheel.clock().time() >= TimeUnit.MILLISECONDS.toNanos(100));

        final InOrder inOrder = inOrder(retransmitSender);
        inOrder.verify(retransmitSender).send(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH);
        inOrder.verify(retransmitSender).send(TERM_ID, offsetOfFrame(1), ALIGNED_FRAME_LENGTH);
    }

    @Theory
    public void shouldRetransmitOnNakOverMessageLength(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 10);
        handler.onNak(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH * 5);
        processTimersUntil(() -> wheel.clock().time() >= TimeUnit.MILLISECONDS.toNanos(100));

        verify(retransmitSender).send(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH * 5);
    }

    @Theory
    public void shouldRetransmitOnNakOverMtuLength(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        final int numFramesPerMtu = MTU_LENGTH / ALIGNED_FRAME_LENGTH;
        createTermBuffer(creator, numFramesPerMtu * 5);
        handler.onNak(TERM_ID, offsetOfFrame(0), MTU_LENGTH * 2);
        processTimersUntil(() -> wheel.clock().time() >= TimeUnit.MILLISECONDS.toNanos(100));

        verify(retransmitSender).send(TERM_ID, offsetOfFrame(0), MTU_LENGTH * 2);
    }

    @Theory
    public void shouldStopRetransmitOnRetransmitReception(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 5);
        handler.onNak(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH);
        handler.onRetransmitReceived(TERM_ID, offsetOfFrame(0));
        processTimersUntil(() -> wheel.clock().time() >= TimeUnit.MILLISECONDS.toNanos(100));

        verifyZeroInteractions(retransmitSender);
    }

    @Theory
    public void shouldStopOneRetransmitOnRetransmitReception(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 5);
        handler.onNak(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH);
        handler.onNak(TERM_ID, offsetOfFrame(1), ALIGNED_FRAME_LENGTH);
        handler.onRetransmitReceived(TERM_ID, offsetOfFrame(0));
        processTimersUntil(() -> wheel.clock().time() >= TimeUnit.MILLISECONDS.toNanos(100));

        verify(retransmitSender).send(TERM_ID, offsetOfFrame(1), ALIGNED_FRAME_LENGTH);
    }

    @Theory
    public void shouldImmediateRetransmitOnNak(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 5);
        handler = newZeroDelayRetransmitHandler();

        handler.onNak(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH);

        verify(retransmitSender).send(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH);
    }

    @Theory
    public void shouldGoIntoLingerOnImmediateRetransmit(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 5);
        handler = newZeroDelayRetransmitHandler();

        handler.onNak(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH);
        processTimersUntil(() -> wheel.clock().time() >= TimeUnit.MILLISECONDS.toNanos(40));
        handler.onNak(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH);

        verify(retransmitSender).send(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH);
    }

    @Theory
    public void shouldOnlyRetransmitOnNakWhenConfiguredTo(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 5);
        handler.onNak(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH);

        verifyZeroInteractions(retransmitSender);
    }

    private RetransmitHandler newZeroDelayRetransmitHandler()
    {
        return new RetransmitHandler(
            wheel, systemCounters, ZERO_DELAY_GENERATOR, LINGER_GENERATOR, retransmitSender, TERM_ID, TERM_BUFFER_LENGTH);
    }

    private void createTermBuffer(final BiConsumer<RetransmitHandlerTest, Integer> creator, final int num)
    {
        IntStream.range(0, num).forEach((i) -> creator.accept(this, i));
    }

    private static int offsetOfFrame(final int index)
    {
        return index * ALIGNED_FRAME_LENGTH;
    }

    private void addSentDataFrame()
    {
        rcvBuffer.putBytes(0, DATA);
        logAppender.append(rcvBuffer, 0, DATA.length);
    }

    private void addReceivedDataFrame(final int msgNum)
    {
        dataHeader.wrap(rcvBuffer, 0);

        dataHeader.termId(TERM_ID)
                  .streamId(STREAM_ID)
                  .sessionId(SESSION_ID)
                  .termOffset(offsetOfFrame(msgNum))
                  .frameLength(MESSAGE_LENGTH)
                  .headerType(HeaderFlyweight.HDR_TYPE_DATA)
                  .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
                  .version(HeaderFlyweight.CURRENT_VERSION);

        dataHeader.buffer().putBytes(dataHeader.dataOffset(), DATA);

        LogRebuilder.insert(termBuffer, offsetOfFrame(msgNum), rcvBuffer, 0, MESSAGE_LENGTH);
    }

    private long processTimersUntil(final BooleanSupplier condition)
    {
        final long start = wheel.clock().time();

        while (!condition.getAsBoolean())
        {
            if (wheel.computeDelayInMs() > 0)
            {
                currentTime += TimeUnit.MICROSECONDS.toNanos(Configuration.CONDUCTOR_TICK_DURATION_US);
            }

            wheel.expireTimers();
        }

        return wheel.clock().time() - start;
    }
}
