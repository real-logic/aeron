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

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import uk.co.real_logic.aeron.common.StaticDelayGenerator;
import uk.co.real_logic.aeron.common.TermHelper;
import uk.co.real_logic.aeron.common.TimerWheel;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.GapScanner;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogRebuilder;
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.common.protocol.HeaderFlyweight;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.common.BitUtil.align;

public class LossHandlerTest
{
    private static final int LOG_BUFFER_SIZE = LogBufferDescriptor.MIN_LOG_SIZE;
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
    private static final long SESSION_ID = 0x5E55101DL;
    private static final long CHANNEL_ID = 0xC400EL;
    private static final int TERM_ID = 0xEE81D;

    public static final StaticDelayGenerator delayGenerator =
        new StaticDelayGenerator(TimeUnit.MILLISECONDS.toNanos(20), false);

    public static final StaticDelayGenerator delayGeneratorWithImmediate =
        new StaticDelayGenerator(TimeUnit.MILLISECONDS.toNanos(20), true);

    private final LogRebuilder[] rebuilders = new LogRebuilder[TermHelper.BUFFER_COUNT];
    private final GapScanner[] scanners = new GapScanner[TermHelper.BUFFER_COUNT];

    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();

    private final TimerWheel wheel;
    private LossHandler handler;
    private NakMessageSender nakMessageSender;
    private long currentTime;
    private int activeIndex = TermHelper.termIdToBufferIndex(TERM_ID);

    public LossHandlerTest()
    {
        for (int i = 0; i < TermHelper.BUFFER_COUNT; i++)
        {
            final AtomicBuffer logBuffer = new AtomicBuffer(ByteBuffer.allocateDirect(LOG_BUFFER_SIZE));
            final AtomicBuffer stateBuffer = new AtomicBuffer(ByteBuffer.allocateDirect(LogBufferDescriptor.STATE_BUFFER_LENGTH));
            rebuilders[i] = new LogRebuilder(logBuffer, stateBuffer);
            scanners[i] = new GapScanner(logBuffer, stateBuffer);
        }

        wheel = new TimerWheel(() -> currentTime,
                               MediaDriver.CONDUCTOR_TICK_DURATION_US,
                               TimeUnit.MICROSECONDS,
                               MediaDriver.CONDUCTOR_TICKS_PER_WHEEL);

        nakMessageSender = mock(NakMessageSender.class);

        final AtomicBuffer rcvBuffer = new AtomicBuffer(new byte[MESSAGE_LENGTH]);
        handler = new LossHandler(scanners, wheel, delayGenerator, nakMessageSender, TERM_ID);
        dataHeader.wrap(rcvBuffer, 0);
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
        verifyZeroInteractions(nakMessageSender);
    }

    @Test
    public void shouldNakMissingData()
    {
        insertDataFrame(offsetOfMessage(0));
        insertDataFrame(offsetOfMessage(2));

        handler.scan();
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(40));

        verify(nakMessageSender).send(TERM_ID, offsetOfMessage(1), gapLength());
    }

    @Test
    public void shouldRetransmitNakForMissingData()
    {
        insertDataFrame(offsetOfMessage(0));
        insertDataFrame(offsetOfMessage(2));

        handler.scan();
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(60));

        verify(nakMessageSender, atLeast(2)).send(TERM_ID, offsetOfMessage(1), gapLength());
    }

    @Test
    public void shouldSuppressNakOnReceivingNak()
    {
        insertDataFrame(offsetOfMessage(0));
        insertDataFrame(offsetOfMessage(2));

        handler.scan();
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(20));
        handler.onNak(TERM_ID, offsetOfMessage(1));
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(20));

        verifyZeroInteractions(nakMessageSender);
    }

    @Test
    public void shouldStopNakOnReceivingData()
    {
        insertDataFrame(offsetOfMessage(0));
        insertDataFrame(offsetOfMessage(2));

        handler.scan();
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(20));
        insertDataFrame(offsetOfMessage(1));
        handler.scan();
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(100));

        verifyZeroInteractions(nakMessageSender);
    }

    @Test
    public void shouldHandleMoreThan2Gaps()
    {
        insertDataFrame(offsetOfMessage(0));
        insertDataFrame(offsetOfMessage(2));
        insertDataFrame(offsetOfMessage(4));
        insertDataFrame(offsetOfMessage(6));

        handler.scan();
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(40));
        insertDataFrame(offsetOfMessage(1));
        handler.scan();
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(80));

        InOrder inOrder = inOrder(nakMessageSender);
        inOrder.verify(nakMessageSender, atLeast(1)).send(TERM_ID, offsetOfMessage(1), gapLength());
        inOrder.verify(nakMessageSender, atLeast(1)).send(TERM_ID, offsetOfMessage(3), gapLength());
        inOrder.verify(nakMessageSender, never()).send(TERM_ID, offsetOfMessage(5), gapLength());
    }

    @Test
    public void shouldReplaceOldNakWithNewNak()
    {
        insertDataFrame(0);
        insertDataFrame(offsetOfMessage(2));

        handler.scan();
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(20));
        insertDataFrame(offsetOfMessage(4));
        insertDataFrame(offsetOfMessage(1));
        handler.scan();
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(100));

        verify(nakMessageSender, atLeast(1)).send(TERM_ID, offsetOfMessage(3), gapLength());
    }

    @Test
    public void shouldHandleImmediateNak()
    {
        handler = new LossHandler(scanners, wheel, delayGeneratorWithImmediate, nakMessageSender, TERM_ID);

        insertDataFrame(offsetOfMessage(0));
        insertDataFrame(offsetOfMessage(2));

        handler.scan();

        verify(nakMessageSender).send(TERM_ID, offsetOfMessage(1), gapLength());
    }

    @Test
    public void shouldNotNakImmediatelyByDefault()
    {
        insertDataFrame(offsetOfMessage(0));
        insertDataFrame(offsetOfMessage(2));

        handler.scan();

        verifyZeroInteractions(nakMessageSender);
    }

    @Test
    public void shouldOnlySendNaksOnceOnMultipleScans()
    {
        handler = new LossHandler(scanners, wheel, delayGeneratorWithImmediate, nakMessageSender, TERM_ID);

        insertDataFrame(offsetOfMessage(0));
        insertDataFrame(offsetOfMessage(2));

        handler.scan();
        handler.scan();

        verify(nakMessageSender).send(TERM_ID, offsetOfMessage(1), gapLength());
    }

    @Test
    public void shouldRotateToNewTermIdCorrectlyOnNoGapsNoPadding()
    {
        // use immediate NAKs for simplicity
        handler = new LossHandler(scanners, wheel, delayGeneratorWithImmediate, nakMessageSender, TERM_ID);

        assertThat(handler.activeIndex(), is(activeIndex));

        // fill term buffer (making sure to be exact, i.e. no padding)
        int offset = 0, i = 0;
        while (LOG_BUFFER_SIZE > offset)
        {
            insertDataFrame(offsetOfMessage(i++));
            offset += ALIGNED_FRAME_LENGTH;
        }

        assertThat(offset, is(LOG_BUFFER_SIZE));  // sanity check the fill to make sure it doesn't need padding
        assertTrue(rebuilders[activeIndex].isComplete());
        assertTrue(handler.scan());

        activeIndex = TermHelper.rotateNext(activeIndex);
        assertThat(handler.activeIndex(), is(activeIndex));

        insertDataFrame(offsetOfMessage(0));

        assertFalse(handler.scan());
        verifyZeroInteractions(nakMessageSender);
    }

    @Test
    public void shouldRotateToNewTermIdCorrectlyOnNoGaps()
    {
        // use immediate NAKs for simplicity
        handler = new LossHandler(scanners, wheel, delayGeneratorWithImmediate, nakMessageSender, TERM_ID);

        assertThat(handler.activeIndex(), is(activeIndex));

        // fill term buffer except the padding frame
        int offset = 0, i = 0;
        while ((LOG_BUFFER_SIZE - 1024) > offset)
        {
            insertDataFrame(offsetOfMessage(i++));
            offset += ALIGNED_FRAME_LENGTH;
        }
        insertPaddingFrame(offsetOfMessage(i));  // and now pad to end of buffer

        assertTrue(rebuilders[activeIndex].isComplete());
        assertTrue(handler.scan());

        activeIndex = TermHelper.rotateNext(activeIndex);
        assertThat(handler.activeIndex(), is(activeIndex));

        insertDataFrame(offsetOfMessage(0));

        assertFalse(handler.scan());
        verifyZeroInteractions(nakMessageSender);
    }

    @Test
    public void shouldRotateToNewTermIdCorrectlyOnReceivingDataAfterGap()
    {
        // use immediate NAKs for simplicity
        handler = new LossHandler(scanners, wheel, delayGeneratorWithImmediate, nakMessageSender, TERM_ID);

        assertThat(handler.activeIndex(), is(activeIndex));

        insertDataFrame(offsetOfMessage(0));
        // fill term buffer except the padding frame
        int offset = offsetOfMessage(2), i = 2;
        while (LOG_BUFFER_SIZE > offset)
        {
            insertDataFrame(offsetOfMessage(i++));
            offset += ALIGNED_FRAME_LENGTH;
        }

        assertThat(offset, is(LOG_BUFFER_SIZE));  // sanity check the fill to make sure it doesn't need padding
        assertFalse(rebuilders[activeIndex].isComplete());
        assertFalse(handler.scan());

        verify(nakMessageSender).send(TERM_ID, offsetOfMessage(1), gapLength());

        insertDataFrame(offsetOfMessage(1));

        assertTrue(rebuilders[activeIndex].isComplete());
        assertTrue(handler.scan());

        activeIndex = TermHelper.rotateNext(activeIndex);
        assertThat(handler.activeIndex(), is(activeIndex));

        insertDataFrame(offsetOfMessage(0));

        assertFalse(handler.scan());
        verifyNoMoreInteractions(nakMessageSender);
    }

    private void insertDataFrame(final int offset)
    {
        insertDataFrame(offset, DATA);
    }

    private void insertDataFrame(final int offset, final byte[] payload)
    {
        dataHeader.termId(TERM_ID)
                  .channelId(CHANNEL_ID)
                  .sessionId(SESSION_ID)
                  .termOffset(offset)
                  .frameLength(payload.length + DataHeaderFlyweight.HEADER_LENGTH)
                  .headerType(HeaderFlyweight.HDR_TYPE_DATA)
                  .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
                  .version(HeaderFlyweight.CURRENT_VERSION);

        dataHeader.atomicBuffer().putBytes(dataHeader.dataOffset(), payload);

        rebuilders[activeIndex].insert(dataHeader.atomicBuffer(), 0, payload.length + DataHeaderFlyweight.HEADER_LENGTH);
    }

    private void insertPaddingFrame(final int offset)
    {
        dataHeader.termId(TERM_ID)
                  .channelId(CHANNEL_ID)
                  .sessionId(SESSION_ID)
                  .termOffset(offset)
                  .frameLength(LOG_BUFFER_SIZE - offset)
                  .headerType(LogBufferDescriptor.PADDING_FRAME_TYPE)
                  .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
                  .version(HeaderFlyweight.CURRENT_VERSION);

        rebuilders[activeIndex].insert(dataHeader.atomicBuffer(), 0, FrameDescriptor.BASE_HEADER_LENGTH);
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
        final long startTime = wheel.now();

        while (!condition.getAsBoolean())
        {
            if (wheel.calculateDelayInMs() > 0)
            {
                currentTime += TimeUnit.MICROSECONDS.toNanos(MediaDriver.CONDUCTOR_TICK_DURATION_US);
            }

            wheel.expireTimers();
        }

        return (wheel.now() - startTime);
    }
}
