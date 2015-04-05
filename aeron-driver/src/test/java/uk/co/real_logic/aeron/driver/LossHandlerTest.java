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

import org.junit.Test;
import org.mockito.InOrder;
import uk.co.real_logic.aeron.common.StaticDelayGenerator;
import uk.co.real_logic.agrona.TimerWheel;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogRebuilder;
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.common.protocol.HeaderFlyweight;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.*;
import static uk.co.real_logic.agrona.BitUtil.align;

public class LossHandlerTest
{
    private static final int TERM_BUFFER_LENGTH = TERM_MIN_LENGTH;
    private static final int POSITION_BITS_TO_SHIFT = Integer.numberOfTrailingZeros(TERM_BUFFER_LENGTH);
    private static final int MASK = TERM_BUFFER_LENGTH - 1;

    private static final byte[] DATA = new byte[36];

    static
    {
        for (int i = 0; i < DATA.length; i++)
        {
            DATA[i] = (byte)i;
        }
    }
    private static final int MESSAGE_LENGTH = DataHeaderFlyweight.HEADER_LENGTH + DATA.length;
    private static final int ALIGNED_FRAME_LENGTH = align(MESSAGE_LENGTH, FrameDescriptor.FRAME_ALIGNMENT);
    private static final int SESSION_ID = 0x5E55101D;
    private static final int STREAM_ID = 0xC400E;
    private static final int TERM_ID = 0xEE81D;
    private static final long ACTIVE_TERM_POSITION = computePosition(TERM_ID, 0, POSITION_BITS_TO_SHIFT, TERM_ID);

    private static final StaticDelayGenerator DELAY_GENERATOR = new StaticDelayGenerator(
        TimeUnit.MILLISECONDS.toNanos(20), false);

    private static final StaticDelayGenerator DELAY_GENERATOR_WITH_IMMEDIATE = new StaticDelayGenerator(
        TimeUnit.MILLISECONDS.toNanos(20), true);

    private final UnsafeBuffer termBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(TERM_BUFFER_LENGTH));

    private final UnsafeBuffer rcvBuffer = new UnsafeBuffer(new byte[MESSAGE_LENGTH]);
    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();

    private final TimerWheel wheel;
    private LossHandler handler;
    private NakMessageSender nakMessageSender;
    private long currentTime = 0;
    private SystemCounters mockSystemCounters = mock(SystemCounters.class);

    public LossHandlerTest()
    {
        when(mockSystemCounters.naksSent()).thenReturn(mock(AtomicCounter.class));

        wheel = new TimerWheel(
            () -> currentTime,
            Configuration.CONDUCTOR_TICK_DURATION_US,
            TimeUnit.MICROSECONDS,
            Configuration.CONDUCTOR_TICKS_PER_WHEEL);

        nakMessageSender = mock(NakMessageSender.class);

        handler = new LossHandler(wheel, DELAY_GENERATOR, nakMessageSender, mockSystemCounters);
        dataHeader.wrap(rcvBuffer, 0);
    }

    @Test
    public void shouldNotSendNakWhenBufferIsEmpty()
    {
        final long completedPosition = ACTIVE_TERM_POSITION;
        final long hwmPosition = ACTIVE_TERM_POSITION;

        handler.scan(termBuffer, completedPosition, hwmPosition, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);

        processTimersUntil(() -> wheel.clock().time() >= TimeUnit.MILLISECONDS.toNanos(100));
        verifyZeroInteractions(nakMessageSender);
    }

    @Test
    public void shouldNakMissingData()
    {
        final long completedPosition = ACTIVE_TERM_POSITION;
        final long hwmPosition = ACTIVE_TERM_POSITION + (ALIGNED_FRAME_LENGTH * 3);

        insertDataFrame(offsetOfMessage(0));
        insertDataFrame(offsetOfMessage(2));

        handler.scan(termBuffer, completedPosition, hwmPosition, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);
        processTimersUntil(() -> wheel.clock().time() >= TimeUnit.MILLISECONDS.toNanos(40));

        verify(nakMessageSender).send(TERM_ID, offsetOfMessage(1), gapLength());
    }

    @Test
    public void shouldRetransmitNakForMissingData()
    {
        final long completedPosition = ACTIVE_TERM_POSITION;
        final long hwmPosition = ACTIVE_TERM_POSITION + (ALIGNED_FRAME_LENGTH * 3);

        insertDataFrame(offsetOfMessage(0));
        insertDataFrame(offsetOfMessage(2));

        handler.scan(termBuffer, completedPosition, hwmPosition, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);
        processTimersUntil(() -> wheel.clock().time() >= TimeUnit.MILLISECONDS.toNanos(60));

        verify(nakMessageSender, atLeast(2)).send(TERM_ID, offsetOfMessage(1), gapLength());
    }

    @Test
    public void shouldSuppressNakOnReceivingNak()
    {
        final long completedPosition = ACTIVE_TERM_POSITION;
        final long hwmPosition = ACTIVE_TERM_POSITION + (ALIGNED_FRAME_LENGTH * 3);

        insertDataFrame(offsetOfMessage(0));
        insertDataFrame(offsetOfMessage(2));

        handler.scan(termBuffer, completedPosition, hwmPosition, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);
        processTimersUntil(() -> wheel.clock().time() >= TimeUnit.MILLISECONDS.toNanos(20));
        handler.onNak(TERM_ID, offsetOfMessage(1));
        processTimersUntil(() -> wheel.clock().time() >= TimeUnit.MILLISECONDS.toNanos(20));

        verifyZeroInteractions(nakMessageSender);
    }

    @Test
    public void shouldStopNakOnReceivingData()
    {
        long completedPosition = ACTIVE_TERM_POSITION;
        final long hwmPosition = ACTIVE_TERM_POSITION + (ALIGNED_FRAME_LENGTH * 3);

        insertDataFrame(offsetOfMessage(0));
        insertDataFrame(offsetOfMessage(2));

        handler.scan(termBuffer, completedPosition, hwmPosition, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);
        processTimersUntil(() -> wheel.clock().time() >= TimeUnit.MILLISECONDS.toNanos(20));
        insertDataFrame(offsetOfMessage(1));
        completedPosition += (ALIGNED_FRAME_LENGTH * 3);
        handler.scan(termBuffer, completedPosition, hwmPosition, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);
        processTimersUntil(() -> wheel.clock().time() >= TimeUnit.MILLISECONDS.toNanos(100));

        verifyZeroInteractions(nakMessageSender);
    }

    @Test
    public void shouldHandleMoreThan2Gaps()
    {
        long completedPosition = ACTIVE_TERM_POSITION;
        final long hwmPosition = ACTIVE_TERM_POSITION + (ALIGNED_FRAME_LENGTH * 7);

        insertDataFrame(offsetOfMessage(0));
        insertDataFrame(offsetOfMessage(2));
        insertDataFrame(offsetOfMessage(4));
        insertDataFrame(offsetOfMessage(6));

        handler.scan(termBuffer, completedPosition, hwmPosition, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);
        processTimersUntil(() -> wheel.clock().time() >= TimeUnit.MILLISECONDS.toNanos(40));
        insertDataFrame(offsetOfMessage(1));
        completedPosition += (3 * ALIGNED_FRAME_LENGTH);
        handler.scan(termBuffer, completedPosition, hwmPosition, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);
        processTimersUntil(() -> wheel.clock().time() >= TimeUnit.MILLISECONDS.toNanos(80));

        final InOrder inOrder = inOrder(nakMessageSender);
        inOrder.verify(nakMessageSender, atLeast(1)).send(TERM_ID, offsetOfMessage(1), gapLength());
        inOrder.verify(nakMessageSender, atLeast(1)).send(TERM_ID, offsetOfMessage(3), gapLength());
        inOrder.verify(nakMessageSender, never()).send(TERM_ID, offsetOfMessage(5), gapLength());
    }

    @Test
    public void shouldReplaceOldNakWithNewNak()
    {
        long completedPosition = ACTIVE_TERM_POSITION;
        long hwmPosition = ACTIVE_TERM_POSITION + (ALIGNED_FRAME_LENGTH * 3);

        insertDataFrame(offsetOfMessage(0));
        insertDataFrame(offsetOfMessage(2));

        handler.scan(termBuffer, completedPosition, hwmPosition, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);
        processTimersUntil(() -> wheel.clock().time() >= TimeUnit.MILLISECONDS.toNanos(20));

        insertDataFrame(offsetOfMessage(4));
        insertDataFrame(offsetOfMessage(1));
        completedPosition += (ALIGNED_FRAME_LENGTH * 3);
        hwmPosition = (ALIGNED_FRAME_LENGTH * 5);

        handler.scan(termBuffer, completedPosition, hwmPosition, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);
        processTimersUntil(() -> wheel.clock().time() >= TimeUnit.MILLISECONDS.toNanos(100));

        verify(nakMessageSender, atLeast(1)).send(TERM_ID, offsetOfMessage(3), gapLength());
    }

    @Test
    public void shouldHandleImmediateNak()
    {
        handler = getLossHandlerWithImmediate();

        long completedPosition = ACTIVE_TERM_POSITION;
        long hwmPosition = ACTIVE_TERM_POSITION + (ALIGNED_FRAME_LENGTH * 3);

        insertDataFrame(offsetOfMessage(0));
        insertDataFrame(offsetOfMessage(2));

        handler.scan(termBuffer, completedPosition, hwmPosition, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);

        verify(nakMessageSender).send(TERM_ID, offsetOfMessage(1), gapLength());
    }

    @Test
    public void shouldNotNakImmediatelyByDefault()
    {
        long completedPosition = ACTIVE_TERM_POSITION;
        long hwmPosition = ACTIVE_TERM_POSITION + (ALIGNED_FRAME_LENGTH * 3);

        insertDataFrame(offsetOfMessage(0));
        insertDataFrame(offsetOfMessage(2));

        handler.scan(termBuffer, completedPosition, hwmPosition, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);

        verifyZeroInteractions(nakMessageSender);
    }

    @Test
    public void shouldOnlySendNaksOnceOnMultipleScans()
    {
        handler = getLossHandlerWithImmediate();

        long completedPosition = ACTIVE_TERM_POSITION;
        long hwmPosition = ACTIVE_TERM_POSITION + (ALIGNED_FRAME_LENGTH * 3);

        insertDataFrame(offsetOfMessage(0));
        insertDataFrame(offsetOfMessage(2));

        handler.scan(termBuffer, completedPosition, hwmPosition, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);
        handler.scan(termBuffer, completedPosition, hwmPosition, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);

        verify(nakMessageSender).send(TERM_ID, offsetOfMessage(1), gapLength());
    }

    @Test
    public void shouldHandleHwmGreaterThanCompletedBuffer()
    {
        handler = getLossHandlerWithImmediate();

        long completedPosition = ACTIVE_TERM_POSITION;
        long hwmPosition = ACTIVE_TERM_POSITION + TERM_BUFFER_LENGTH + ALIGNED_FRAME_LENGTH;

        insertDataFrame(offsetOfMessage(0));
        completedPosition += ALIGNED_FRAME_LENGTH;

        handler.scan(termBuffer, completedPosition, hwmPosition, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);

        verify(nakMessageSender).send(TERM_ID, offsetOfMessage(1), TERM_BUFFER_LENGTH - (int)completedPosition);
    }

    @Test
    public void shouldHandleNonZeroInitialTermOffset()
    {
        handler = getLossHandlerWithImmediate();

        long completedPosition = ACTIVE_TERM_POSITION + (ALIGNED_FRAME_LENGTH * 3);
        long hwmPosition = ACTIVE_TERM_POSITION + (ALIGNED_FRAME_LENGTH * 5);

        insertDataFrame(offsetOfMessage(2));
        insertDataFrame(offsetOfMessage(4));

        handler.scan(termBuffer, completedPosition, hwmPosition, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);

        verify(nakMessageSender).send(TERM_ID, offsetOfMessage(3), gapLength());
        verifyNoMoreInteractions(nakMessageSender);
    }

    private LossHandler getLossHandlerWithImmediate()
    {
        return new LossHandler(wheel, DELAY_GENERATOR_WITH_IMMEDIATE, nakMessageSender, mockSystemCounters);
    }

    private void insertDataFrame(final int offset)
    {
        insertDataFrame(offset, DATA);
    }

    private void insertDataFrame(final int offset, final byte[] payload)
    {
        dataHeader.termId(TERM_ID)
                  .streamId(STREAM_ID)
                  .sessionId(SESSION_ID)
                  .termOffset(offset)
                  .frameLength(payload.length + DataHeaderFlyweight.HEADER_LENGTH)
                  .headerType(HeaderFlyweight.HDR_TYPE_DATA)
                  .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
                  .version(HeaderFlyweight.CURRENT_VERSION);

        dataHeader.buffer().putBytes(dataHeader.dataOffset(), payload);

        LogRebuilder.insert(termBuffer, offset, rcvBuffer, 0, payload.length + DataHeaderFlyweight.HEADER_LENGTH);
    }

    private int offsetOfMessage(final int index)
    {
        return index * ALIGNED_FRAME_LENGTH;
    }

    private int gapLength()
    {
        return ALIGNED_FRAME_LENGTH;
    }

    private long processTimersUntil(final BooleanSupplier condition)
    {
        final long startTime = wheel.clock().time();

        while (!condition.getAsBoolean())
        {
            if (wheel.computeDelayInMs() > 0)
            {
                currentTime += TimeUnit.MICROSECONDS.toNanos(Configuration.CONDUCTOR_TICK_DURATION_US);
            }

            wheel.expireTimers();
        }

        return wheel.clock().time() - startTime;
    }
}
