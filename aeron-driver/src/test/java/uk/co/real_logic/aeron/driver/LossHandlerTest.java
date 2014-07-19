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

import org.junit.*;
import org.mockito.InOrder;
import uk.co.real_logic.aeron.common.*;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.*;
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.common.protocol.HeaderFlyweight;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.common.BitUtil.align;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.STATE_BUFFER_LENGTH;

public class LossHandlerTest
{
    private static final int LOG_BUFFER_SIZE = 64 * 1024;
    private static final int STATE_BUFFER_SIZE = STATE_BUFFER_LENGTH;
    private static final byte[] DATA = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
    private static final int MESSAGE_LENGTH = DataHeaderFlyweight.HEADER_LENGTH + DATA.length;
    private static final long SESSION_ID = 0x5E55101DL;
    private static final long CHANNEL_ID = 0xC400EL;
    private static final long TERM_ID = 0xEE81D;

    public static final StaticDelayGenerator delayGenerator =
        new StaticDelayGenerator(TimeUnit.MILLISECONDS.toNanos(20), false);

    public static final StaticDelayGenerator delayGeneratorWithImmediate =
        new StaticDelayGenerator(TimeUnit.MILLISECONDS.toNanos(20), true);

    private final AtomicBuffer[] logBuffers = new AtomicBuffer[TermHelper.BUFFER_COUNT];
    private final AtomicBuffer[] stateBuffers = new AtomicBuffer[TermHelper.BUFFER_COUNT];
    private final LogRebuilder[] rebuilders = new LogRebuilder[TermHelper.BUFFER_COUNT];
    private final GapScanner[] scanners = new GapScanner[TermHelper.BUFFER_COUNT];

    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();

    private final TimerWheel wheel;
    private LossHandler handler;
    private NakMessageSender nakMessageSender;
    private long currentTime;

    public LossHandlerTest()
    {
        for (int i = 0; i < TermHelper.BUFFER_COUNT; i++)
        {
            logBuffers[i] = new AtomicBuffer(ByteBuffer.allocateDirect(LOG_BUFFER_SIZE));
            stateBuffers[i] = new AtomicBuffer(ByteBuffer.allocateDirect(STATE_BUFFER_SIZE));
            rebuilders[i] = new LogRebuilder(logBuffers[i], stateBuffers[i]);
            scanners[i] = new GapScanner(logBuffers[i], stateBuffers[i]);
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
        rcvDataFrame(offsetOfMessage(0));
        rcvDataFrame(offsetOfMessage(2));

        handler.scan();
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(40));

        verify(nakMessageSender).send(TERM_ID, offsetOfMessage(1), gapLength());
    }

    @Test
    public void shouldRetransmitNakForMissingData()
    {
        rcvDataFrame(offsetOfMessage(0));
        rcvDataFrame(offsetOfMessage(2));

        handler.scan();
        processTimersUntil(() -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(60));

        verify(nakMessageSender, atLeast(2)).send(TERM_ID, offsetOfMessage(1), gapLength());
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

        verifyZeroInteractions(nakMessageSender);
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

        verifyZeroInteractions(nakMessageSender);
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

        InOrder inOrder = inOrder(nakMessageSender);
        inOrder.verify(nakMessageSender, atLeast(1)).send(TERM_ID, offsetOfMessage(1), gapLength());
        inOrder.verify(nakMessageSender, atLeast(1)).send(TERM_ID, offsetOfMessage(3), gapLength());
        inOrder.verify(nakMessageSender, never()).send(TERM_ID, offsetOfMessage(5), gapLength());
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

        verify(nakMessageSender, atLeast(1)).send(TERM_ID, offsetOfMessage(3), gapLength());
    }

    @Test
    public void shouldHandleImmediateNak()
    {
        handler = new LossHandler(scanners, wheel, delayGeneratorWithImmediate, nakMessageSender, TERM_ID);

        rcvDataFrame(offsetOfMessage(0));
        rcvDataFrame(offsetOfMessage(2));

        handler.scan();

        verify(nakMessageSender).send(TERM_ID, offsetOfMessage(1), gapLength());
    }

    @Test
    public void shouldNotNakImmediatelyByDefault()
    {
        rcvDataFrame(offsetOfMessage(0));
        rcvDataFrame(offsetOfMessage(2));

        handler.scan();

        verifyZeroInteractions(nakMessageSender);
    }

    @Test
    public void shouldOnlySendNaksOnceOnMultipleScans()
    {
        handler = new LossHandler(scanners, wheel, delayGeneratorWithImmediate, nakMessageSender, TERM_ID);

        rcvDataFrame(offsetOfMessage(0));
        rcvDataFrame(offsetOfMessage(2));

        handler.scan();
        handler.scan();

        verify(nakMessageSender).send(TERM_ID, offsetOfMessage(1), gapLength());
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
                currentTime += TimeUnit.MICROSECONDS.toNanos(MediaDriver.CONDUCTOR_TICK_DURATION_US);
            }

            wheel.expireTimers();
        }

        return (wheel.now() - startTime);
    }
}
