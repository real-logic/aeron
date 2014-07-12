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
import uk.co.real_logic.aeron.util.BufferRotationDescriptor;
import uk.co.real_logic.aeron.util.StaticDelayGenerator;
import uk.co.real_logic.aeron.util.TimerWheel;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.GapScanner;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogRebuilder;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.util.BitUtil.align;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.STATE_BUFFER_LENGTH;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;

public class LossHandlerTest
{
    private static final int LOG_BUFFER_SIZE = 65536 + TRAILER_LENGTH;
    private static final int STATE_BUFFER_SIZE = STATE_BUFFER_LENGTH;
    private static final byte[] DATA = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    private static final int MESSAGE_LENGTH = DataHeaderFlyweight.HEADER_LENGTH + DATA.length;
    private static final long SESSION_ID = 0x5E55101DL;
    private static final long CHANNEL_ID = 0xC400EL;
    private static final long TERM_ID = 0xEE81D;

    public static final StaticDelayGenerator delayGenerator =
            new StaticDelayGenerator(TimeUnit.MILLISECONDS.toNanos(20), false);

    public static final StaticDelayGenerator delayGeneratorWithImmediate =
            new StaticDelayGenerator(TimeUnit.MILLISECONDS.toNanos(20), true);

    private final AtomicBuffer[] logBuffers = new AtomicBuffer[BufferRotationDescriptor.BUFFER_COUNT];
    private final AtomicBuffer[] stateBuffers = new AtomicBuffer[BufferRotationDescriptor.BUFFER_COUNT];
    private final LogRebuilder[] rebuilders = new LogRebuilder[BufferRotationDescriptor.BUFFER_COUNT];
    private final GapScanner[] scanners = new GapScanner[BufferRotationDescriptor.BUFFER_COUNT];

    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();
    private final AtomicBuffer rcvBuffer = new AtomicBuffer(new byte[MESSAGE_LENGTH]);

    private final TimerWheel wheel;
    private LossHandler handler;
    private LossHandler.SendNakHandler sendNakHandler;
    private long currentTime;

    public LossHandlerTest()
    {
        for (int i = 0; i < BufferRotationDescriptor.BUFFER_COUNT; i++)
        {
            logBuffers[i] = new AtomicBuffer(ByteBuffer.allocateDirect(LOG_BUFFER_SIZE));
            stateBuffers[i] = new AtomicBuffer(ByteBuffer.allocateDirect(STATE_BUFFER_SIZE));
            rebuilders[i] = new LogRebuilder(logBuffers[i], stateBuffers[i]);
            scanners[i] = new GapScanner(logBuffers[i], stateBuffers[i]);
        }

        wheel = new TimerWheel(() -> currentTime,
            MediaDriver.MEDIA_CONDUCTOR_TICK_DURATION_US,
            TimeUnit.MICROSECONDS,
            MediaDriver.MEDIA_CONDUCTOR_TICKS_PER_WHEEL);

        sendNakHandler = mock(LossHandler.SendNakHandler.class);

        handler = new LossHandler(scanners, wheel, delayGenerator, sendNakHandler);
        dataHeader.wrap(rcvBuffer, 0);

        handler.activeTermId(TERM_ID);
    }

    @Before
    public void setUp()
    {
        currentTime = 0;
    }

    @Test
    public void shouldNotSendNakWhenBufferIsEmpty()
    {
        handler.scan();
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(100));
        verifyZeroInteractions(sendNakHandler);
    }

    @Test
    public void shouldNakMissingData()
    {
        rcvDataFrame(offsetOfMessage(0));
        rcvDataFrame(offsetOfMessage(2));

        handler.scan();
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(40));

        verify(sendNakHandler).onSendNak(TERM_ID, offsetOfMessage(1), gapLength());
    }

    @Test
    public void shouldRetransmitNakForMissingData()
    {
        rcvDataFrame(offsetOfMessage(0));
        rcvDataFrame(offsetOfMessage(2));

        handler.scan();
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(60));

        verify(sendNakHandler, atLeast(2)).onSendNak(TERM_ID, offsetOfMessage(1), gapLength());
    }

    @Test
    public void shouldSuppressNakOnReceivingNak()
    {
        rcvDataFrame(offsetOfMessage(0));
        rcvDataFrame(offsetOfMessage(2));

        handler.scan();
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(20));
        handler.onNak(TERM_ID, offsetOfMessage(1));
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(20));

        verifyZeroInteractions(sendNakHandler);
    }

    @Test
    public void shouldStopNakOnReceivingData()
    {
        rcvDataFrame(offsetOfMessage(0));
        rcvDataFrame(offsetOfMessage(2));

        handler.scan();
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(20));
        rcvDataFrame(offsetOfMessage(1));
        handler.scan();
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(100));

        verifyZeroInteractions(sendNakHandler);
    }

    @Test
    public void shouldHandleMoreThan2Gaps()
    {
        rcvDataFrame(offsetOfMessage(0));
        rcvDataFrame(offsetOfMessage(2));
        rcvDataFrame(offsetOfMessage(4));
        rcvDataFrame(offsetOfMessage(6));

        handler.scan();
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(40));
        rcvDataFrame(offsetOfMessage(1));
        handler.scan();
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(80));

        InOrder inOrder = inOrder(sendNakHandler);
        inOrder.verify(sendNakHandler, atLeast(1)).onSendNak(TERM_ID, offsetOfMessage(1), gapLength());
        inOrder.verify(sendNakHandler, atLeast(1)).onSendNak(TERM_ID, offsetOfMessage(3), gapLength());
        inOrder.verify(sendNakHandler, never()).onSendNak(TERM_ID, offsetOfMessage(5), gapLength());
    }

    @Test
    public void shouldReplaceOldNakWithNewNak()
    {
        rcvDataFrame(0);
        rcvDataFrame(offsetOfMessage(2));

        handler.scan();
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(20));
        rcvDataFrame(offsetOfMessage(4));
        rcvDataFrame(offsetOfMessage(1));
        handler.scan();
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(100));

        verify(sendNakHandler, atLeast(1)).onSendNak(TERM_ID, offsetOfMessage(3), gapLength());
    }

    @Test
    public void shouldHandleImmediateNak()
    {
        handler = new LossHandler(scanners, wheel, delayGeneratorWithImmediate, sendNakHandler);
        handler.activeTermId(TERM_ID);

        rcvDataFrame(offsetOfMessage(0));
        rcvDataFrame(offsetOfMessage(2));

        handler.scan();

        verify(sendNakHandler).onSendNak(TERM_ID, offsetOfMessage(1), gapLength());
    }

    @Test
    public void shouldNotNakImmediatelyByDefault()
    {
        rcvDataFrame(offsetOfMessage(0));
        rcvDataFrame(offsetOfMessage(2));

        handler.scan();

        verifyZeroInteractions(sendNakHandler);
    }

    @Test
    public void shouldOnlySendNaksOnceOnMultipleScans()
    {
        handler = new LossHandler(scanners, wheel, delayGeneratorWithImmediate, sendNakHandler);
        handler.activeTermId(TERM_ID);

        rcvDataFrame(offsetOfMessage(0));
        rcvDataFrame(offsetOfMessage(2));

        handler.scan();
        handler.scan();

        verify(sendNakHandler).onSendNak(TERM_ID, offsetOfMessage(1), gapLength());
    }

    @Test
    @Ignore
    public void shouldRotateToNewTermIdCorrectly()
    {

    }

    private void rcvDataFrame(final int offset)
    {
        dataHeader.termId(TERM_ID)
                  .channelId(CHANNEL_ID)
                  .sessionId(SESSION_ID)
                  .termOffset(offset)
                  .frameLength(MESSAGE_LENGTH)
                  .headerType(HeaderFlyweight.HDR_TYPE_DATA)
                  .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
                  .version(HeaderFlyweight.CURRENT_VERSION);

        dataHeader.atomicBuffer().putBytes(dataHeader.dataOffset(), DATA);

        rebuilders[0].insert(dataHeader.atomicBuffer(), 0, MESSAGE_LENGTH);
    }

    private int offsetOfMessage(final int index)
    {
        return index * FrameDescriptor.FRAME_ALIGNMENT;
    }

    private int gapLength()
    {
        return align(MESSAGE_LENGTH, FrameDescriptor.FRAME_ALIGNMENT);
    }

    private long processTimersUntil(final BooleanSupplier condition)
    {
        final long startTime = wheel.now();

        while (!condition.getAsBoolean())
        {
            if (wheel.calculateDelayInMs() > 0)
            {
                currentTime += TimeUnit.MICROSECONDS.toNanos(MediaDriver.MEDIA_CONDUCTOR_TICK_DURATION_US);
            }

            wheel.expireTimers();
        }

        return (wheel.now() - startTime);
    }
}
