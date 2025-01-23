/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.driver;

import io.aeron.logbuffer.LogBufferDescriptor;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.TermRebuilder;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.HeaderFlyweight;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static io.aeron.driver.LossDetector.lossFound;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;
import static io.aeron.logbuffer.LogBufferDescriptor.TERM_MIN_LENGTH;
import static io.aeron.logbuffer.LogBufferDescriptor.computePosition;
import static org.agrona.BitUtil.align;

class LossDetectorTest
{
    private static final int TERM_BUFFER_LENGTH = TERM_MIN_LENGTH;
    private static final int POSITION_BITS_TO_SHIFT = LogBufferDescriptor.positionBitsToShift(TERM_BUFFER_LENGTH);
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
        TimeUnit.MILLISECONDS.toNanos(20));

    private static final StaticDelayGenerator DELAY_GENERATOR_WITH_LONGER_RETRY = new StaticDelayGenerator(
        TimeUnit.MILLISECONDS.toNanos(20), TimeUnit.MILLISECONDS.toNanos(200));

    private final UnsafeBuffer termBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(TERM_BUFFER_LENGTH));
    private final UnsafeBuffer rcvBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(MESSAGE_LENGTH));
    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();

    private final LossHandler lossHandler = mock(LossHandler.class);
    private LossDetector lossDetector = new LossDetector(DELAY_GENERATOR, lossHandler);
    private long currentTimeNs = 0;

    {
        dataHeader.wrap(rcvBuffer);
    }

    @Test
    void shouldNotSendNakWhenBufferIsEmpty()
    {
        final long rebuildPosition = ACTIVE_TERM_POSITION;
        final long hwmPosition = ACTIVE_TERM_POSITION;

        lossDetector.scan(
            termBuffer, rebuildPosition, hwmPosition, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);
        currentTimeNs = TimeUnit.MILLISECONDS.toNanos(100);
        lossDetector.scan(
            termBuffer, rebuildPosition, hwmPosition, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);

        verifyNoInteractions(lossHandler);
    }

    @Test
    void shouldNotNakIfNoMissingData()
    {
        final long rebuildPosition = ACTIVE_TERM_POSITION;
        final long hwmPosition = ACTIVE_TERM_POSITION + (ALIGNED_FRAME_LENGTH * 3L);

        insertDataFrame(offsetOfMessage(0));
        insertDataFrame(offsetOfMessage(1));
        insertDataFrame(offsetOfMessage(2));

        lossDetector.scan(
            termBuffer, rebuildPosition, hwmPosition, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);
        currentTimeNs = TimeUnit.MILLISECONDS.toNanos(40);
        lossDetector.scan(
            termBuffer, rebuildPosition, hwmPosition, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);

        verifyNoInteractions(lossHandler);
    }

    @Test
    void shouldNakMissingData()
    {
        final long rebuildPosition = ACTIVE_TERM_POSITION;
        final long hwmPosition = ACTIVE_TERM_POSITION + (ALIGNED_FRAME_LENGTH * 3L);

        insertDataFrame(offsetOfMessage(0));
        insertDataFrame(offsetOfMessage(2));

        lossDetector.scan(
            termBuffer, rebuildPosition, hwmPosition, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);
        currentTimeNs = TimeUnit.MILLISECONDS.toNanos(40);
        lossDetector.scan(
            termBuffer, rebuildPosition, hwmPosition, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);

        verify(lossHandler).onGapDetected(TERM_ID, offsetOfMessage(1), ALIGNED_FRAME_LENGTH);
    }

    @Test
    void shouldRetransmitNakForMissingData()
    {
        final long rebuildPosition = ACTIVE_TERM_POSITION;
        final long hwmPosition = ACTIVE_TERM_POSITION + (ALIGNED_FRAME_LENGTH * 3L);

        insertDataFrame(offsetOfMessage(0));
        insertDataFrame(offsetOfMessage(2));

        lossDetector.scan(
            termBuffer, rebuildPosition, hwmPosition, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);
        currentTimeNs = TimeUnit.MILLISECONDS.toNanos(30);
        lossDetector.scan(
            termBuffer, rebuildPosition, hwmPosition, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);
        currentTimeNs = TimeUnit.MILLISECONDS.toNanos(60);
        lossDetector.scan(
            termBuffer, rebuildPosition, hwmPosition, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);

        verify(lossHandler, atLeast(2)).onGapDetected(TERM_ID, offsetOfMessage(1), ALIGNED_FRAME_LENGTH);
    }

    @Test
    void shouldStopNakOnReceivingData()
    {
        long rebuildPosition = ACTIVE_TERM_POSITION;
        final long hwmPosition = ACTIVE_TERM_POSITION + (ALIGNED_FRAME_LENGTH * 3L);

        insertDataFrame(offsetOfMessage(0));
        insertDataFrame(offsetOfMessage(2));

        lossDetector.scan(
            termBuffer, rebuildPosition, hwmPosition, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);
        currentTimeNs = TimeUnit.MILLISECONDS.toNanos(20);
        insertDataFrame(offsetOfMessage(1));
        rebuildPosition += (ALIGNED_FRAME_LENGTH * 3L);
        lossDetector.scan(
            termBuffer, rebuildPosition, hwmPosition, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);
        currentTimeNs = TimeUnit.MILLISECONDS.toNanos(100);
        lossDetector.scan(
            termBuffer, rebuildPosition, hwmPosition, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);

        verifyNoInteractions(lossHandler);
    }

    @Test
    void shouldHandleMoreThan2Gaps()
    {
        long rebuildPosition = ACTIVE_TERM_POSITION;
        final long hwmPosition = ACTIVE_TERM_POSITION + (ALIGNED_FRAME_LENGTH * 7L);

        insertDataFrame(offsetOfMessage(0));
        insertDataFrame(offsetOfMessage(2));
        insertDataFrame(offsetOfMessage(4));
        insertDataFrame(offsetOfMessage(6));

        lossDetector.scan(
            termBuffer, rebuildPosition, hwmPosition, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);
        currentTimeNs = TimeUnit.MILLISECONDS.toNanos(40);
        lossDetector.scan(
            termBuffer, rebuildPosition, hwmPosition, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);
        insertDataFrame(offsetOfMessage(1));
        rebuildPosition += (ALIGNED_FRAME_LENGTH * 3L);
        lossDetector.scan(
            termBuffer, rebuildPosition, hwmPosition, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);
        currentTimeNs = TimeUnit.MILLISECONDS.toNanos(80);
        lossDetector.scan(
            termBuffer, rebuildPosition, hwmPosition, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);

        final InOrder inOrder = inOrder(lossHandler);
        inOrder.verify(lossHandler, atLeast(1)).onGapDetected(TERM_ID, offsetOfMessage(1), ALIGNED_FRAME_LENGTH);
        inOrder.verify(lossHandler, atLeast(1)).onGapDetected(TERM_ID, offsetOfMessage(3), ALIGNED_FRAME_LENGTH);
        inOrder.verify(lossHandler, never()).onGapDetected(TERM_ID, offsetOfMessage(5), ALIGNED_FRAME_LENGTH);
    }

    @Test
    void shouldReplaceOldNakWithNewNak()
    {
        long rebuildPosition = ACTIVE_TERM_POSITION;
        long hwmPosition = ACTIVE_TERM_POSITION + (ALIGNED_FRAME_LENGTH * 3L);

        insertDataFrame(offsetOfMessage(0));
        insertDataFrame(offsetOfMessage(2));

        lossDetector.scan(
            termBuffer, rebuildPosition, hwmPosition, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);
        currentTimeNs = TimeUnit.MILLISECONDS.toNanos(20);
        lossDetector.scan(
            termBuffer, rebuildPosition, hwmPosition, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);

        insertDataFrame(offsetOfMessage(4));
        insertDataFrame(offsetOfMessage(1));
        rebuildPosition += (ALIGNED_FRAME_LENGTH * 3L);
        hwmPosition = (ALIGNED_FRAME_LENGTH * 5L);

        lossDetector.scan(
            termBuffer, rebuildPosition, hwmPosition, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);
        currentTimeNs = TimeUnit.MILLISECONDS.toNanos(100);
        lossDetector.scan(
            termBuffer, rebuildPosition, hwmPosition, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);

        verify(lossHandler, atLeast(1)).onGapDetected(TERM_ID, offsetOfMessage(3), ALIGNED_FRAME_LENGTH);
    }

    @Test
    void shouldHandleLongerRetryDelay()
    {
        lossDetector = getLossHandlerWithLongRetry();

        final long rebuildPosition = ACTIVE_TERM_POSITION;
        final long hwmPosition = ACTIVE_TERM_POSITION + (ALIGNED_FRAME_LENGTH * 3L);

        insertDataFrame(offsetOfMessage(0));
        insertDataFrame(offsetOfMessage(2));

        lossDetector.scan(
            termBuffer, rebuildPosition, hwmPosition, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);
        verifyNoInteractions(lossHandler);

        currentTimeNs = TimeUnit.MILLISECONDS.toNanos(40);
        lossDetector.scan(
            termBuffer, rebuildPosition, hwmPosition, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);

        verify(lossHandler).onGapDetected(TERM_ID, offsetOfMessage(1), ALIGNED_FRAME_LENGTH);

        currentTimeNs = TimeUnit.MILLISECONDS.toNanos(80);
        lossDetector.scan(
            termBuffer, rebuildPosition, hwmPosition, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);

        verifyNoMoreInteractions(lossHandler);

        currentTimeNs = TimeUnit.MILLISECONDS.toNanos(240);
        lossDetector.scan(
            termBuffer, rebuildPosition, hwmPosition, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);

        verify(lossHandler, times(2)).onGapDetected(TERM_ID, offsetOfMessage(1), ALIGNED_FRAME_LENGTH);
    }

    @Test
    void shouldNotNakImmediatelyByDefault()
    {
        final long rebuildPosition = ACTIVE_TERM_POSITION;
        final long hwmPosition = ACTIVE_TERM_POSITION + (ALIGNED_FRAME_LENGTH * 3L);

        insertDataFrame(offsetOfMessage(0));
        insertDataFrame(offsetOfMessage(2));

        lossDetector.scan(
            termBuffer, rebuildPosition, hwmPosition, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);

        verifyNoInteractions(lossHandler);
    }

    @Test
    void shouldOnlySendNaksOnceOnMultipleScans()
    {
        lossDetector = getLossHandlerWithLongRetry();

        final long rebuildPosition = ACTIVE_TERM_POSITION;
        final long hwmPosition = ACTIVE_TERM_POSITION + (ALIGNED_FRAME_LENGTH * 3L);

        insertDataFrame(offsetOfMessage(0));
        insertDataFrame(offsetOfMessage(2));

        lossDetector.scan(
            termBuffer, rebuildPosition, hwmPosition, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);
        currentTimeNs = TimeUnit.MILLISECONDS.toNanos(40);
        lossDetector.scan(
            termBuffer, rebuildPosition, hwmPosition, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);
        lossDetector.scan(
            termBuffer, rebuildPosition, hwmPosition, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);

        verify(lossHandler).onGapDetected(TERM_ID, offsetOfMessage(1), ALIGNED_FRAME_LENGTH);
    }

    @Test
    void shouldHandleHwmGreaterThanCompletedBuffer()
    {
        lossDetector = getLossHandlerWithLongRetry();

        long rebuildPosition = ACTIVE_TERM_POSITION;
        final long hwmPosition = ACTIVE_TERM_POSITION + TERM_BUFFER_LENGTH + ALIGNED_FRAME_LENGTH;

        insertDataFrame(offsetOfMessage(0));
        rebuildPosition += ALIGNED_FRAME_LENGTH;

        lossDetector.scan(
            termBuffer, rebuildPosition, hwmPosition, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);
        currentTimeNs = TimeUnit.MILLISECONDS.toNanos(40);
        lossDetector.scan(
            termBuffer, rebuildPosition, hwmPosition, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);

        verify(lossHandler).onGapDetected(TERM_ID, offsetOfMessage(1), TERM_BUFFER_LENGTH - (int)rebuildPosition);
    }

    @Test
    void shouldHandleNonZeroInitialTermOffset()
    {
        lossDetector = getLossHandlerWithLongRetry();

        final long rebuildPosition = ACTIVE_TERM_POSITION + (ALIGNED_FRAME_LENGTH * 3L);
        final long hwmPosition = ACTIVE_TERM_POSITION + (ALIGNED_FRAME_LENGTH * 5L);

        insertDataFrame(offsetOfMessage(2));
        insertDataFrame(offsetOfMessage(4));

        lossDetector.scan(
            termBuffer, rebuildPosition, hwmPosition, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);
        currentTimeNs = TimeUnit.MILLISECONDS.toNanos(40);
        lossDetector.scan(
            termBuffer, rebuildPosition, hwmPosition, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID);

        verify(lossHandler).onGapDetected(TERM_ID, offsetOfMessage(3), ALIGNED_FRAME_LENGTH);
        verifyNoMoreInteractions(lossHandler);
    }

    @Test
    void shouldDetectChangesInTheGapLength()
    {
        lossDetector = getLossHandlerWithLongRetry();

        final long rebuildPosition = ACTIVE_TERM_POSITION + (ALIGNED_FRAME_LENGTH * 3L);

        insertDataFrame(offsetOfMessage(2));
        insertDataFrame(offsetOfMessage(5));

        assertTrue(lossFound(lossDetector.scan(
            termBuffer, rebuildPosition, rebuildPosition + 32, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID)));

        currentTimeNs = TimeUnit.MILLISECONDS.toNanos(100);
        assertFalse(lossFound(lossDetector.scan(
            termBuffer, rebuildPosition, rebuildPosition + 32, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID)));

        assertTrue(lossFound(lossDetector.scan(
            termBuffer, rebuildPosition, rebuildPosition + 64, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID)));

        currentTimeNs = TimeUnit.MILLISECONDS.toNanos(200);
        assertFalse(lossFound(lossDetector.scan(
            termBuffer, rebuildPosition, rebuildPosition + 64, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID)));

        currentTimeNs = TimeUnit.MILLISECONDS.toNanos(300);
        assertTrue(lossFound(lossDetector.scan(
            termBuffer,
            rebuildPosition,
            rebuildPosition + ALIGNED_FRAME_LENGTH,
            currentTimeNs,
            MASK,
            POSITION_BITS_TO_SHIFT,
            TERM_ID)));

        assertTrue(lossFound(lossDetector.scan(
            termBuffer,
            rebuildPosition,
            rebuildPosition + ALIGNED_FRAME_LENGTH * 2L,
            currentTimeNs,
            MASK,
            POSITION_BITS_TO_SHIFT,
            TERM_ID)));

        currentTimeNs = TimeUnit.MILLISECONDS.toNanos(400);
        assertFalse(lossFound(lossDetector.scan(
            termBuffer,
            rebuildPosition,
            rebuildPosition + ALIGNED_FRAME_LENGTH * 2L,
            currentTimeNs,
            MASK,
            POSITION_BITS_TO_SHIFT,
            TERM_ID)));

        insertDataFrame(offsetOfMessage(4));
        assertTrue(lossFound(lossDetector.scan(
            termBuffer,
            rebuildPosition,
            rebuildPosition + ALIGNED_FRAME_LENGTH * 2L,
            currentTimeNs,
            MASK,
            POSITION_BITS_TO_SHIFT,
            TERM_ID)));

        currentTimeNs = TimeUnit.MILLISECONDS.toNanos(500);
        assertFalse(lossFound(lossDetector.scan(
            termBuffer,
            rebuildPosition,
            rebuildPosition + ALIGNED_FRAME_LENGTH * 2L,
            currentTimeNs,
            MASK,
            POSITION_BITS_TO_SHIFT,
            TERM_ID)));

        assertTrue(lossFound(lossDetector.scan(
            termBuffer, rebuildPosition, rebuildPosition + 64, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID)));

        currentTimeNs = TimeUnit.MILLISECONDS.toNanos(600);
        assertFalse(lossFound(lossDetector.scan(
            termBuffer, rebuildPosition, rebuildPosition + 64, currentTimeNs, MASK, POSITION_BITS_TO_SHIFT, TERM_ID)));

        final InOrder inOrder = inOrder(lossHandler);
        inOrder.verify(lossHandler).onGapDetected(TERM_ID, offsetOfMessage(3), 32);
        inOrder.verify(lossHandler).onGapDetected(TERM_ID, offsetOfMessage(3), 64);
        inOrder.verify(lossHandler).onGapDetected(TERM_ID, offsetOfMessage(3), ALIGNED_FRAME_LENGTH * 2);
        inOrder.verify(lossHandler).onGapDetected(TERM_ID, offsetOfMessage(3), ALIGNED_FRAME_LENGTH);
        inOrder.verify(lossHandler).onGapDetected(TERM_ID, offsetOfMessage(3), 64);
        inOrder.verifyNoMoreInteractions();
    }

    private LossDetector getLossHandlerWithLongRetry()
    {
        return new LossDetector(DELAY_GENERATOR_WITH_LONGER_RETRY, lossHandler);
    }

    private void insertDataFrame(final int offset)
    {
        insertDataFrame(offset, DATA);
    }

    private void insertDataFrame(final int offset, final byte[] payload)
    {
        dataHeader
            .termId(TERM_ID)
            .streamId(STREAM_ID)
            .sessionId(SESSION_ID)
            .termOffset(offset)
            .frameLength(payload.length + DataHeaderFlyweight.HEADER_LENGTH)
            .headerType(HeaderFlyweight.HDR_TYPE_DATA)
            .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
            .version(HeaderFlyweight.CURRENT_VERSION);

        rcvBuffer.putBytes(dataHeader.dataOffset(), payload);

        TermRebuilder.insert(termBuffer, offset, rcvBuffer, payload.length + DataHeaderFlyweight.HEADER_LENGTH);
    }

    private int offsetOfMessage(final int index)
    {
        return index * ALIGNED_FRAME_LENGTH;
    }
}
