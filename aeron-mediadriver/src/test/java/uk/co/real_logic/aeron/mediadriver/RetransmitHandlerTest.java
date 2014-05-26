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

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.InOrder;
import uk.co.real_logic.aeron.util.FeedbackDelayGenerator;
import uk.co.real_logic.aeron.util.TimerWheel;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogReader;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.IntStream;

import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.BufferDescriptor.STATE_BUFFER_LENGTH;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.BufferDescriptor.TRAILER_LENGTH;

public class RetransmitHandlerTest
{
    private static final int LOG_BUFFER_SIZE = 65536 + TRAILER_LENGTH;
    private static final int STATE_BUFFER_SIZE = STATE_BUFFER_LENGTH;
    private static final byte[] DATA = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    private static final int MESSAGE_LENGTH = DataHeaderFlyweight.HEADER_LENGTH + DATA.length;
    private static final byte[] HEADER = { HeaderFlyweight.CURRENT_VERSION,
                                           FrameDescriptor.UNFRAGMENTED,
                                           0, HeaderFlyweight.HDR_TYPE_DATA,
                                           0, 0, 0, 0,
                                           0, 0, 0, 0,
                                           0x5E, 0x55, 0x10, 0x1D,
                                           0x00, 0x54, 0x00, 0x3F,
                                           0x7F, 0x00, 0x33, 0x55};

    public static final FeedbackDelayGenerator delayGenerator = () -> TimeUnit.MILLISECONDS.toNanos(20);

    public static final FeedbackDelayGenerator zeroDelayGenerator = () -> TimeUnit.MILLISECONDS.toNanos(0);

    public static final FeedbackDelayGenerator lingerGenerator = () -> TimeUnit.MILLISECONDS.toNanos(40);

    private final AtomicBuffer logBuffer = new AtomicBuffer(ByteBuffer.allocateDirect(LOG_BUFFER_SIZE));
    private final AtomicBuffer stateBuffer = new AtomicBuffer(ByteBuffer.allocateDirect(STATE_BUFFER_SIZE));
    private final LogAppender logAppender = new LogAppender(logBuffer, stateBuffer, HEADER, 1024);
    private final LogReader logReader = new LogReader(logBuffer, stateBuffer);

    private final AtomicBuffer rcvBuffer = new AtomicBuffer(new byte[MESSAGE_LENGTH]);

    private long currentTime;

    private final TimerWheel wheel = new TimerWheel(() -> currentTime,
            MediaDriver.MEDIA_CONDUCTOR_TICK_DURATION_MICROS,
            TimeUnit.MICROSECONDS,
            MediaDriver.MEDIA_CONDUCTOR_TICKS_PER_WHEEL);

    private final LogReader.FrameHandler retransmitHandler = mock(LogReader.FrameHandler.class);

    private RetransmitHandler handler = new RetransmitHandler(logReader, wheel,
            delayGenerator, lingerGenerator, retransmitHandler);

    @Before
    public void setUp()
    {
        // add 10 frames to the log to use
        IntStream.range(0, 10).forEach((i) -> rcvDataFrame());
    }

    @Test
    public void shouldRetransmitOnNak()
    {
        handler.onNak(offsetOfMessage(0));
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(100));

        verify(retransmitHandler).onFrame(logBuffer, offsetOfMessage(0), MESSAGE_LENGTH);
    }

    @Test
    public void shouldNotRetransmitOnNakWhileInLinger()
    {
        handler.onNak(offsetOfMessage(0));
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(40));
        handler.onNak(offsetOfMessage(0));
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(100));

        verify(retransmitHandler).onFrame(logBuffer, offsetOfMessage(0), MESSAGE_LENGTH);
    }

    @Test
    public void shouldRetransmitOnNakAfterLinger()
    {
        handler.onNak(offsetOfMessage(0));
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(100));
        handler.onNak(offsetOfMessage(0));
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(200));

        verify(retransmitHandler, times(2)).onFrame(logBuffer, offsetOfMessage(0), MESSAGE_LENGTH);
    }

    @Test
    public void shouldRetransmitOnMultipleNaks()
    {
        handler.onNak(offsetOfMessage(0));
        handler.onNak(offsetOfMessage(1));
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(100));

        InOrder inOrder = inOrder(retransmitHandler);
        inOrder.verify(retransmitHandler).onFrame(logBuffer, offsetOfMessage(0), MESSAGE_LENGTH);
        inOrder.verify(retransmitHandler).onFrame(logBuffer, offsetOfMessage(1), MESSAGE_LENGTH);
    }

    @Test
    public void shouldStopRetransmitOnRetransmitReception()
    {
        handler.onNak(offsetOfMessage(0));
        handler.onRetransmitReceived(offsetOfMessage(0));
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(100));

        verifyZeroInteractions(retransmitHandler);
    }

    @Test
    public void shouldStopOnlyOneRetransmitOnRetransmitReception()
    {
        handler.onNak(offsetOfMessage(0));
        handler.onNak(offsetOfMessage(1));
        handler.onRetransmitReceived(offsetOfMessage(0));
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(100));

        verify(retransmitHandler).onFrame(logBuffer, offsetOfMessage(1), MESSAGE_LENGTH);
    }

    @Test
    public void shouldImmediateRetransmitOnNak()
    {
        handler = new RetransmitHandler(logReader, wheel, zeroDelayGenerator, lingerGenerator, retransmitHandler);

        handler.onNak(offsetOfMessage(0));

        verify(retransmitHandler).onFrame(logBuffer, offsetOfMessage(0), MESSAGE_LENGTH);
    }

    @Test
    public void shouldGoIntoLingerOnImmediateRetransmit()
    {
        handler = new RetransmitHandler(logReader, wheel, zeroDelayGenerator, lingerGenerator, retransmitHandler);

        handler.onNak(offsetOfMessage(0));
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(40));
        handler.onNak(offsetOfMessage(0));

        verify(retransmitHandler).onFrame(logBuffer, offsetOfMessage(0), MESSAGE_LENGTH);
    }

    @Test
    public void shouldOnlyImmediateRetransmitOnNakWhenConfiguredTo()
    {
        handler.onNak(offsetOfMessage(0));

        verifyZeroInteractions(retransmitHandler);
    }

    @Test
    @Ignore
    public void shouldNotRetransmitOnNakForMissingFrame()
    {

    }

    private int offsetOfMessage(final int index)
    {
        return index * FrameDescriptor.FRAME_ALIGNMENT;
    }

    private void rcvDataFrame()
    {
        rcvBuffer.putBytes(0, DATA);
        logAppender.append(rcvBuffer, 0, DATA.length);
    }

    private long processTimersUntil(final BooleanSupplier condition)
    {
        final long startTime = wheel.now();

        while (!condition.getAsBoolean())
        {
            if (wheel.calculateDelayInMs() > 0)
            {
                currentTime += TimeUnit.MICROSECONDS.toNanos(MediaDriver.MEDIA_CONDUCTOR_TICK_DURATION_MICROS);
            }

            wheel.expireTimers();
        }

        return (wheel.now() - startTime);
    }
}
