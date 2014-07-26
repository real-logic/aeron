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
package uk.co.real_logic.aeron.driver;

import org.junit.Test;
import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import uk.co.real_logic.aeron.common.FeedbackDelayGenerator;
import uk.co.real_logic.aeron.common.TimerWheel;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.*;
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.common.protocol.HeaderFlyweight;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.stream.IntStream;

import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.common.BitUtil.align;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.STATE_BUFFER_LENGTH;

@RunWith(Theories.class)
public class RetransmitHandlerTest
{
    private static final int MTU_LENGTH = 1024;
    private static final int LOG_BUFFER_SIZE = LogBufferDescriptor.MIN_LOG_SIZE;
    private static final int STATE_BUFFER_SIZE = STATE_BUFFER_LENGTH;
    private static final byte[] DATA = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
    private static final int MESSAGE_LENGTH = DataHeaderFlyweight.HEADER_LENGTH + DATA.length;
    private static final int ALIGNED_FRAME_LENGTH = align(MESSAGE_LENGTH, FrameDescriptor.FRAME_ALIGNMENT);
    private static final int SESSION_ID = 0x5E55101D;
    private static final int STREAM_ID = 0x5400E;
    private static final int TERM_ID = 0x7F003355;

    private static final FeedbackDelayGenerator delayGenerator = () -> TimeUnit.MILLISECONDS.toNanos(20);
    private static final FeedbackDelayGenerator zeroDelayGenerator = () -> TimeUnit.MILLISECONDS.toNanos(0);
    private static final FeedbackDelayGenerator lingerGenerator = () -> TimeUnit.MILLISECONDS.toNanos(40);

    private final AtomicBuffer logBuffer = new AtomicBuffer(ByteBuffer.allocateDirect(LOG_BUFFER_SIZE));
    private final AtomicBuffer stateBuffer = new AtomicBuffer(ByteBuffer.allocateDirect(STATE_BUFFER_SIZE));
    private final LogScanner scanner = new LogScanner(logBuffer, stateBuffer, DataHeaderFlyweight.HEADER_LENGTH);

    private final LogAppender logAppender =
        new LogAppender(logBuffer, stateBuffer, DataHeaderFlyweight.DEFAULT_HEADER_NULL_IDS, 1024);
    private final LogRebuilder logRebuilder = new LogRebuilder(logBuffer, stateBuffer);

    private final AtomicBuffer rcvBuffer = new AtomicBuffer(new byte[MESSAGE_LENGTH]);

    private DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();

    private long currentTime;

    private final TimerWheel wheel = new TimerWheel(() -> currentTime,
                                                    MediaDriver.CONDUCTOR_TICK_DURATION_US,
                                                    TimeUnit.MICROSECONDS,
                                                    MediaDriver.CONDUCTOR_TICKS_PER_WHEEL);

    private final LogScanner.AvailabilityHandler retransmitHandler = mock(LogScanner.AvailabilityHandler.class);

    private RetransmitHandler handler =
        new RetransmitHandler(scanner, wheel, delayGenerator, lingerGenerator, retransmitHandler, MTU_LENGTH);

    @DataPoint
    public static final BiConsumer<RetransmitHandlerTest, Integer> senderAddDataFrame = (h, i) -> h.addSentDataFrame();

    @DataPoint
    public static final BiConsumer<RetransmitHandlerTest, Integer> receiverAddDataFrame = (h, i) -> h.addReceivedDataFrame(i);

    @Theory
    public void shouldRetransmitOnNak(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 5);
        handler.onNak(offsetOfMessage(0), ALIGNED_FRAME_LENGTH);
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(100));

        verify(retransmitHandler).onAvailable(logBuffer, offsetOfMessage(0), ALIGNED_FRAME_LENGTH);
    }

    @Theory
    public void shouldNotRetransmitOnNakWhileInLinger(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 5);
        handler.onNak(offsetOfMessage(0), ALIGNED_FRAME_LENGTH);
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(40));
        handler.onNak(offsetOfMessage(0), ALIGNED_FRAME_LENGTH);
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(100));

        verify(retransmitHandler).onAvailable(logBuffer, offsetOfMessage(0), ALIGNED_FRAME_LENGTH);
    }

    @Theory
    public void shouldRetransmitOnNakAfterLinger(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 5);
        handler.onNak(offsetOfMessage(0), ALIGNED_FRAME_LENGTH);
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(100));
        handler.onNak(offsetOfMessage(0), ALIGNED_FRAME_LENGTH);
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(200));

        verify(retransmitHandler, times(2)).onAvailable(logBuffer, offsetOfMessage(0), ALIGNED_FRAME_LENGTH);
    }

    @Theory
    public void shouldRetransmitOnMultipleNaks(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 5);
        handler.onNak(offsetOfMessage(0), ALIGNED_FRAME_LENGTH);
        handler.onNak(offsetOfMessage(1), ALIGNED_FRAME_LENGTH);
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(100));

        InOrder inOrder = inOrder(retransmitHandler);
        inOrder.verify(retransmitHandler).onAvailable(logBuffer, offsetOfMessage(0), ALIGNED_FRAME_LENGTH);
        inOrder.verify(retransmitHandler).onAvailable(logBuffer, offsetOfMessage(1), ALIGNED_FRAME_LENGTH);
    }

    @Theory
    public void shouldRetransmitOnNakOverMessageLength(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 10);
        handler.onNak(offsetOfMessage(0), ALIGNED_FRAME_LENGTH * 5);
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(100));

        verify(retransmitHandler).onAvailable(logBuffer, offsetOfMessage(0), ALIGNED_FRAME_LENGTH * 5);
    }

    @Theory
    public void shouldRetransmitOnNakOverMtuLength(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        final int numFramesPerMtu = MTU_LENGTH / ALIGNED_FRAME_LENGTH;
        createTermBuffer(creator, numFramesPerMtu * 5);
        handler.onNak(offsetOfMessage(0), MTU_LENGTH * 2);
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(100));

        verify(retransmitHandler).onAvailable(logBuffer, offsetOfMessage(0), numFramesPerMtu * ALIGNED_FRAME_LENGTH);
    }

    @Theory
    public void shouldStopRetransmitOnRetransmitReception(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 5);
        handler.onNak(offsetOfMessage(0), ALIGNED_FRAME_LENGTH);
        handler.onRetransmitReceived(offsetOfMessage(0));
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(100));

        verifyZeroInteractions(retransmitHandler);
    }

    @Theory
    public void shouldStopOneRetransmitOnRetransmitReception(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 5);
        handler.onNak(offsetOfMessage(0), ALIGNED_FRAME_LENGTH);
        handler.onNak(offsetOfMessage(1), ALIGNED_FRAME_LENGTH);
        handler.onRetransmitReceived(offsetOfMessage(0));
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(100));

        verify(retransmitHandler).onAvailable(logBuffer, offsetOfMessage(1), ALIGNED_FRAME_LENGTH);
    }

    @Theory
    public void shouldImmediateRetransmitOnNak(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 5);
        handler = newZeroDelayRetransmitHandler();

        handler.onNak(offsetOfMessage(0), ALIGNED_FRAME_LENGTH);

        verify(retransmitHandler).onAvailable(logBuffer, offsetOfMessage(0), ALIGNED_FRAME_LENGTH);
    }

    @Theory
    public void shouldGoIntoLingerOnImmediateRetransmit(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 5);
        handler = newZeroDelayRetransmitHandler();

        handler.onNak(offsetOfMessage(0), ALIGNED_FRAME_LENGTH);
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(40));
        handler.onNak(offsetOfMessage(0), ALIGNED_FRAME_LENGTH);

        verify(retransmitHandler).onAvailable(logBuffer, offsetOfMessage(0), ALIGNED_FRAME_LENGTH);
    }

    @Theory
    public void shouldOnlyRetransmitOnNakWhenConfiguredTo(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 5);
        handler.onNak(offsetOfMessage(0), ALIGNED_FRAME_LENGTH);

        verifyZeroInteractions(retransmitHandler);
    }

    @Test
    public void shouldNotRetransmitOnNakForMissingFrame()
    {
        createTermBufferWithGap(receiverAddDataFrame, 5, 2);
        handler.onNak(offsetOfMessage(2), ALIGNED_FRAME_LENGTH);

        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(40));
        verifyZeroInteractions(retransmitHandler);
    }

    private RetransmitHandler newZeroDelayRetransmitHandler()
    {
        return new RetransmitHandler(scanner, wheel, zeroDelayGenerator, lingerGenerator, retransmitHandler, MTU_LENGTH);
    }

    private void createTermBuffer(final BiConsumer<RetransmitHandlerTest, Integer> creator, final int num)
    {
        IntStream.range(0, num).forEach((i) -> creator.accept(this, i));
    }

    private void createTermBufferWithGap(final BiConsumer<RetransmitHandlerTest, Integer> creator,
                                         final int num,
                                         final int gap)
    {
        IntStream.range(0, gap).forEach((i) -> creator.accept(this, i));
        IntStream.range(gap + 1, num).forEach((i) -> creator.accept(this, i));
    }

    private static int offsetOfMessage(final int index)
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
                  .termOffset(offsetOfMessage(msgNum))
                  .frameLength(MESSAGE_LENGTH)
                  .headerType(HeaderFlyweight.HDR_TYPE_DATA)
                  .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
                  .version(HeaderFlyweight.CURRENT_VERSION);

        dataHeader.atomicBuffer().putBytes(dataHeader.dataOffset(), DATA);

        logRebuilder.insert(dataHeader.atomicBuffer(), 0, MESSAGE_LENGTH);
    }

    private long processTimersUntil(final BooleanSupplier condition)
    {
        final long start = wheel.now();

        while (!condition.getAsBoolean())
        {
            if (wheel.calculateDelayInMs() > 0)
            {
                currentTime += TimeUnit.MICROSECONDS.toNanos(MediaDriver.CONDUCTOR_TICK_DURATION_US);
            }

            wheel.expireTimers();
        }

        return wheel.now() - start;
    }
}
