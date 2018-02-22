/*
 * Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron.logbuffer;

import org.junit.Before;
import org.junit.Test;
import org.agrona.concurrent.UnsafeBuffer;
import org.mockito.InOrder;

import static java.nio.ByteBuffer.allocateDirect;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static io.aeron.protocol.HeaderFlyweight.HEADER_LENGTH;

public class LogBufferUnblockerTest
{
    private static final int TERM_LENGTH = TERM_MIN_LENGTH;
    private static final int TERM_ID_1 = 1;
    private static final int TERM_TAIL_COUNTER_OFFSET = TERM_TAIL_COUNTERS_OFFSET;

    private final UnsafeBuffer logMetaDataBuffer = spy(new UnsafeBuffer(allocateDirect(LOG_META_DATA_LENGTH)));
    private final UnsafeBuffer[] termBuffers = new UnsafeBuffer[PARTITION_COUNT];

    private final int positionBitsToShift = LogBufferDescriptor.positionBitsToShift(TERM_LENGTH);

    @Before
    public void setUp()
    {
        initialTermId(logMetaDataBuffer, TERM_ID_1);

        for (int i = 0; i < PARTITION_COUNT; i++)
        {
            termBuffers[i] = spy(new UnsafeBuffer(allocateDirect(TERM_LENGTH)));
        }

        final int initialTermId = TERM_ID_1;
        initialiseTailWithTermId(logMetaDataBuffer, 0, initialTermId);
        for (int i = 1; i < PARTITION_COUNT; i++)
        {
            final int expectedTermId = (initialTermId + i) - PARTITION_COUNT;
            initialiseTailWithTermId(logMetaDataBuffer, i, expectedTermId);
        }
    }

    @Test
    public void shouldNotUnblockWhenPositionHasCompleteMessage()
    {
        final int blockedOffset = HEADER_LENGTH * 4;
        final long blockedPosition = computePosition(TERM_ID_1, blockedOffset, positionBitsToShift, TERM_ID_1);
        final int activeIndex = indexByPosition(blockedPosition, positionBitsToShift);

        when(termBuffers[activeIndex].getIntVolatile(blockedOffset)).thenReturn(HEADER_LENGTH);

        assertFalse(LogBufferUnblocker.unblock(termBuffers, logMetaDataBuffer, blockedPosition, TERM_LENGTH));

        final long rawTail = rawTailVolatile(logMetaDataBuffer);
        assertThat(computePosition(termId(rawTail), blockedOffset, positionBitsToShift, TERM_ID_1),
            is(blockedPosition));
    }

    @Test
    public void shouldUnblockWhenPositionHasNonCommittedMessageAndTailWithinTerm()
    {
        final int blockedOffset = HEADER_LENGTH * 4;
        final int messageLength = HEADER_LENGTH * 4;
        final long blockedPosition = computePosition(TERM_ID_1, blockedOffset, positionBitsToShift, TERM_ID_1);
        final int activeIndex = indexByPosition(blockedPosition, positionBitsToShift);

        when(termBuffers[activeIndex].getIntVolatile(blockedOffset)).thenReturn(-messageLength);

        assertTrue(LogBufferUnblocker.unblock(termBuffers, logMetaDataBuffer, blockedPosition, TERM_LENGTH));

        final long rawTail = rawTailVolatile(logMetaDataBuffer);
        assertThat(computePosition(termId(rawTail), blockedOffset + messageLength, positionBitsToShift, TERM_ID_1),
            is(blockedPosition + messageLength));
    }

    @Test
    public void shouldUnblockWhenPositionHasNonCommittedMessageAndTailAtEndOfTerm()
    {
        final int messageLength = HEADER_LENGTH * 4;
        final int blockedOffset = TERM_LENGTH - messageLength;
        final long blockedPosition = computePosition(TERM_ID_1, blockedOffset, positionBitsToShift, TERM_ID_1);
        final int activeIndex = indexByPosition(blockedPosition, positionBitsToShift);

        when(termBuffers[activeIndex].getIntVolatile(blockedOffset)).thenReturn(0);

        logMetaDataBuffer.getAndAddLong(TERM_TAIL_COUNTER_OFFSET, TERM_LENGTH);

        assertTrue(LogBufferUnblocker.unblock(termBuffers, logMetaDataBuffer, blockedPosition, TERM_LENGTH));

        final long rawTail = rawTailVolatile(logMetaDataBuffer);
        final int termId = termId(rawTail);
        assertThat(computePosition(termId, 0, positionBitsToShift, TERM_ID_1),
            is(blockedPosition + messageLength));

        verify(logMetaDataBuffer).compareAndSetInt(LOG_ACTIVE_TERM_COUNT_OFFSET, 0, 1);
    }

    @Test
    public void shouldUnblockWhenPositionHasCommittedMessageAndTailAtEndOfTermButNotRotated()
    {
        final long blockedPosition = TERM_LENGTH;

        final int termTailCounterTwoOffset = TERM_TAIL_COUNTER_OFFSET + SIZE_OF_LONG;
        logMetaDataBuffer.getAndAddLong(TERM_TAIL_COUNTER_OFFSET, TERM_LENGTH);

        assertTrue(LogBufferUnblocker.unblock(termBuffers, logMetaDataBuffer, blockedPosition, TERM_LENGTH));

        final long rawTail = rawTailVolatile(logMetaDataBuffer);
        final int termId = termId(rawTail);
        assertThat(termId, is(TERM_ID_1 + 1));
        assertThat(computePosition(termId, 0, positionBitsToShift, TERM_ID_1), is(blockedPosition));

        final InOrder inOrder = inOrder(logMetaDataBuffer);
        inOrder.verify(logMetaDataBuffer)
            .compareAndSetLong(termTailCounterTwoOffset, pack(TERM_ID_1 - 2, 0), pack(TERM_ID_1 + 1, 0));
        inOrder.verify(logMetaDataBuffer).compareAndSetInt(LOG_ACTIVE_TERM_COUNT_OFFSET, 0, 1);
    }

    @Test
    public void shouldUnblockWhenPositionHasNonCommittedMessageAndTailPastEndOfTerm()
    {
        final int messageLength = HEADER_LENGTH * 4;
        final int blockedOffset = TERM_LENGTH - messageLength;
        final long blockedPosition = computePosition(TERM_ID_1, blockedOffset, positionBitsToShift, TERM_ID_1);
        final int activeIndex = indexByPosition(blockedPosition, positionBitsToShift);

        when(termBuffers[activeIndex].getIntVolatile(blockedOffset)).thenReturn(0);

        logMetaDataBuffer.putLong(TERM_TAIL_COUNTER_OFFSET, pack(TERM_ID_1, TERM_LENGTH + HEADER_LENGTH));

        assertTrue(LogBufferUnblocker.unblock(termBuffers, logMetaDataBuffer, blockedPosition, TERM_LENGTH));

        final long rawTail = rawTailVolatile(logMetaDataBuffer);
        final int termId = termId(rawTail);
        assertThat(computePosition(termId, 0, positionBitsToShift, TERM_ID_1),
            is(blockedPosition + messageLength));

        verify(logMetaDataBuffer).compareAndSetInt(LOG_ACTIVE_TERM_COUNT_OFFSET, 0, 1);
    }

    private static long pack(final int termId, final int offset)
    {
        return (((long)termId) << 32) | offset;
    }
}
