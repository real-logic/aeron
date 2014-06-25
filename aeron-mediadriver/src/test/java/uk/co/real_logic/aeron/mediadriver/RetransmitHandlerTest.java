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
package uk.co.real_logic.aeron.mediadriver;

import org.junit.Test;
import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import uk.co.real_logic.aeron.util.FeedbackDelayGenerator;
import uk.co.real_logic.aeron.util.TimerWheel;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogReader;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogRebuilder;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.stream.IntStream;

import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.STATE_BUFFER_LENGTH;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;

@RunWith(Theories.class)
public class RetransmitHandlerTest
{
    private static final int LOG_BUFFER_SIZE = 65536 + TRAILER_LENGTH;
    private static final int STATE_BUFFER_SIZE = STATE_BUFFER_LENGTH;
    private static final byte[] DATA = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
    private static final int MESSAGE_LENGTH = DataHeaderFlyweight.HEADER_LENGTH + DATA.length;
    private static final long SESSION_ID = 0x5E55101DL;
    private static final long CHANNEL_ID = 0x5400EL;
    private static final long TERM_ID = 0x7F003355L;

    public static final FeedbackDelayGenerator delayGenerator = () -> TimeUnit.MILLISECONDS.toNanos(20);
    public static final FeedbackDelayGenerator zeroDelayGenerator = () -> TimeUnit.MILLISECONDS.toNanos(0);
    public static final FeedbackDelayGenerator lingerGenerator = () -> TimeUnit.MILLISECONDS.toNanos(40);

    private final AtomicBuffer logBuffer = new AtomicBuffer(ByteBuffer.allocateDirect(LOG_BUFFER_SIZE));
    private final AtomicBuffer stateBuffer = new AtomicBuffer(ByteBuffer.allocateDirect(STATE_BUFFER_SIZE));
    private final LogReader logReader = new LogReader(logBuffer, stateBuffer);

    private final LogAppender logAppender = new LogAppender(logBuffer, stateBuffer,
        DataHeaderFlyweight.DEFAULT_HEADER_NULL_IDS, 1024);
    private final LogRebuilder logRebuilder = new LogRebuilder(logBuffer, stateBuffer);

    private final AtomicBuffer rcvBuffer = new AtomicBuffer(new byte[MESSAGE_LENGTH]);

    private DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();

    private long currentTime;

    private final TimerWheel wheel = new TimerWheel(() -> currentTime,
                                                    MediaDriver.MEDIA_CONDUCTOR_TICK_DURATION_US,
                                                    TimeUnit.MICROSECONDS,
                                                    MediaDriver.MEDIA_CONDUCTOR_TICKS_PER_WHEEL);

    private final LogReader.FrameHandler retransmitHandler = mock(LogReader.FrameHandler.class);

    private RetransmitHandler handler =
        new RetransmitHandler(logReader, wheel, delayGenerator, lingerGenerator, retransmitHandler);

    @DataPoint
    public static final BiConsumer<RetransmitHandlerTest, Integer> senderAddDataFrame = (h, i) -> addSentDataFrame(h);

    @DataPoint
    public static final BiConsumer<RetransmitHandlerTest, Integer> receiverAddDataFrame =
        (handler, msgNum) -> addReceivedDataFrame(handler, msgNum);

    @Theory
    public void shouldRetransmitOnNak(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 5);
        handler.onNak(offsetOfMessage(0));
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(100));

        verify(retransmitHandler).onFrame(logBuffer, offsetOfMessage(0), MESSAGE_LENGTH);
    }

    @Theory
    public void shouldNotRetransmitOnNakWhileInLinger(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 5);
        handler.onNak(offsetOfMessage(0));
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(40));
        handler.onNak(offsetOfMessage(0));
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(100));

        verify(retransmitHandler).onFrame(logBuffer, offsetOfMessage(0), MESSAGE_LENGTH);
    }

    @Theory
    public void shouldRetransmitOnNakAfterLinger(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 5);
        handler.onNak(offsetOfMessage(0));
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(100));
        handler.onNak(offsetOfMessage(0));
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(200));

        verify(retransmitHandler, times(2)).onFrame(logBuffer, offsetOfMessage(0), MESSAGE_LENGTH);
    }

    @Theory
    public void shouldRetransmitOnMultipleNaks(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 5);
        handler.onNak(offsetOfMessage(0));
        handler.onNak(offsetOfMessage(1));
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(100));

        InOrder inOrder = inOrder(retransmitHandler);
        inOrder.verify(retransmitHandler).onFrame(logBuffer, offsetOfMessage(0), MESSAGE_LENGTH);
        inOrder.verify(retransmitHandler).onFrame(logBuffer, offsetOfMessage(1), MESSAGE_LENGTH);
    }

    @Theory
    public void shouldStopRetransmitOnRetransmitReception(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 5);
        handler.onNak(offsetOfMessage(0));
        handler.onRetransmitReceived(offsetOfMessage(0));
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(100));

        verifyZeroInteractions(retransmitHandler);
    }

    @Theory
    public void shouldStopOneRetransmitOnRetransmitReception(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 5);
        handler.onNak(offsetOfMessage(0));
        handler.onNak(offsetOfMessage(1));
        handler.onRetransmitReceived(offsetOfMessage(0));
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(100));

        verify(retransmitHandler).onFrame(logBuffer, offsetOfMessage(1), MESSAGE_LENGTH);
    }

    @Theory
    public void shouldImmediateRetransmitOnNak(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 5);
        handler = new RetransmitHandler(logReader, wheel, zeroDelayGenerator, lingerGenerator, retransmitHandler);

        handler.onNak(offsetOfMessage(0));

        verify(retransmitHandler).onFrame(logBuffer, offsetOfMessage(0), MESSAGE_LENGTH);
    }

    @Theory
    public void shouldGoIntoLingerOnImmediateRetransmit(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 5);
        handler = new RetransmitHandler(logReader, wheel, zeroDelayGenerator, lingerGenerator, retransmitHandler);

        handler.onNak(offsetOfMessage(0));
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(40));
        handler.onNak(offsetOfMessage(0));

        verify(retransmitHandler).onFrame(logBuffer, offsetOfMessage(0), MESSAGE_LENGTH);
    }

    @Theory
    public void shouldOnlyRetransmitOnNakWhenConfiguredTo(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 5);
        handler.onNak(offsetOfMessage(0));

        verifyZeroInteractions(retransmitHandler);
    }

    @Test
    public void shouldNotRetransmitOnNakForMissingFrame()
    {
        createTermBufferWithGap(receiverAddDataFrame, 5, 2);
        handler.onNak(offsetOfMessage(2));

        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(40));
        verifyZeroInteractions(retransmitHandler);
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
        return index * FrameDescriptor.FRAME_ALIGNMENT;
    }

    private static void addSentDataFrame(final RetransmitHandlerTest handler)
    {
        handler.addSentDataFrame();
    }

    private static void addReceivedDataFrame(final RetransmitHandlerTest handler, final int msgNum)
    {
        handler.addReceivedDataFrame(msgNum);
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
                  .channelId(CHANNEL_ID)
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
                currentTime += TimeUnit.MICROSECONDS.toNanos(MediaDriver.MEDIA_CONDUCTOR_TICK_DURATION_US);
            }

            wheel.expireTimers();
        }

        return wheel.now() - start;
    }
}
