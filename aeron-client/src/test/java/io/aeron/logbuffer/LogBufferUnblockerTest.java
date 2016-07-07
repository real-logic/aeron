/*
 * Copyright 2015 - 2016 Real Logic Ltd.
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
    private static final int PARTITION_INDEX = 0;
    private static final int TERM_TAIL_COUNTER_OFFSET = TERM_TAIL_COUNTERS_OFFSET + (PARTITION_INDEX * SIZE_OF_LONG);


    private final UnsafeBuffer logMetaDataBuffer = spy(new UnsafeBuffer(allocateDirect(LOG_META_DATA_LENGTH)));
    private final UnsafeBuffer[] termBuffers = new UnsafeBuffer[PARTITION_COUNT];

    private final int positionBitsToShift = Integer.numberOfTrailingZeros(TERM_LENGTH);

    @Before
    public void setUp()
    {
        initialTermId(logMetaDataBuffer, TERM_ID_1);

        for (int i = 0; i < PARTITION_COUNT; i++)
        {
            termBuffers[i] = spy(new UnsafeBuffer(allocateDirect(TERM_LENGTH)));
        }

        initialiseTailWithTermId(logMetaDataBuffer, PARTITION_INDEX, TERM_ID_1);
    }

    @Test
    public void shouldNotUnblockWhenPositionHasCompleteMessage()
    {
        final int blockedOffset = HEADER_LENGTH * 4;
        final long blockedPosition = computePosition(TERM_ID_1, blockedOffset, positionBitsToShift, TERM_ID_1);
        final int activeIndex = indexByPosition(blockedPosition, positionBitsToShift);

        when(termBuffers[activeIndex].getIntVolatile(blockedOffset)).thenReturn(HEADER_LENGTH);

        assertFalse(LogBufferUnblocker.unblock(termBuffers, logMetaDataBuffer, blockedPosition));

        final long rawTail = rawTailVolatile(logMetaDataBuffer);
        assertThat(computePosition(termId(rawTail), blockedOffset, positionBitsToShift, TERM_ID_1), is(blockedPosition));
    }

    @Test
    public void shouldUnblockWhenPositionHasNonCommittedMessageAndTailWithinTerm()
    {
        final int blockedOffset = HEADER_LENGTH * 4;
        final int messageLength = HEADER_LENGTH * 4;
        final long blockedPosition = computePosition(TERM_ID_1, blockedOffset, positionBitsToShift, TERM_ID_1);
        final int activeIndex = indexByPosition(blockedPosition, positionBitsToShift);

        when(termBuffers[activeIndex].getIntVolatile(blockedOffset)).thenReturn(-messageLength);

        assertTrue(LogBufferUnblocker.unblock(termBuffers, logMetaDataBuffer, blockedPosition));

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

        logMetaDataBuffer.putLong(TERM_TAIL_COUNTER_OFFSET, pack(TERM_ID_1, TERM_LENGTH));

        assertTrue(LogBufferUnblocker.unblock(termBuffers, logMetaDataBuffer, blockedPosition));

        verify(logMetaDataBuffer).putIntOrdered(LOG_ACTIVE_PARTITION_INDEX_OFFSET, activeIndex + 1);

        final long rawTail = rawTailVolatile(logMetaDataBuffer);
        final int termId = termId(rawTail);
        assertThat(computePosition(termId, 0, positionBitsToShift, TERM_ID_1), is(blockedPosition + messageLength));
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

        assertTrue(LogBufferUnblocker.unblock(termBuffers, logMetaDataBuffer, blockedPosition));

        verify(logMetaDataBuffer).putIntOrdered(LOG_ACTIVE_PARTITION_INDEX_OFFSET, activeIndex + 1);

        final long rawTail = rawTailVolatile(logMetaDataBuffer);
        assertThat(computePosition(termId(rawTail), 0, positionBitsToShift, TERM_ID_1), is(blockedPosition + messageLength));
    }

    private static long pack(final int termId, final int offset)
    {
        return (((long)termId) << 32) | offset;
    }
}
