/*
 * Copyright 2015 Real Logic Ltd.
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
package uk.co.real_logic.aeron.logbuffer;

import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.*;
import static uk.co.real_logic.aeron.protocol.HeaderFlyweight.HEADER_LENGTH;

public class LogBufferUnblockerTest
{
    private static final int TERM_LENGTH = TERM_MIN_LENGTH;
    private static final int TERM_ID_1 = 1;

    private final UnsafeBuffer logMetaDataBuffer = spy(new UnsafeBuffer(ByteBuffer.allocateDirect(LOG_META_DATA_LENGTH)));
    private final UnsafeBuffer[] termBuffers = new UnsafeBuffer[PARTITION_COUNT];
    private final UnsafeBuffer[] termMetaDataBuffers = new UnsafeBuffer[PARTITION_COUNT];
    private final LogBufferPartition[] partitions = new LogBufferPartition[PARTITION_COUNT];

    private final int activeIndex = indexByTerm(TERM_ID_1, TERM_ID_1);
    private final int positionBitsToShift = Integer.numberOfTrailingZeros(TERM_LENGTH);

    @Before
    public void setUp()
    {
        initialTermId(logMetaDataBuffer, TERM_ID_1);
        activeTermId(logMetaDataBuffer, TERM_ID_1);

        for (int i = 0; i < PARTITION_COUNT; i++)
        {
            termBuffers[i] = spy(new UnsafeBuffer(ByteBuffer.allocateDirect(TERM_LENGTH)));
            termMetaDataBuffers[i] = spy(new UnsafeBuffer(ByteBuffer.allocateDirect(TERM_META_DATA_LENGTH)));

            partitions[i] = spy(new LogBufferPartition(termBuffers[i], termMetaDataBuffers[i]));
        }
    }

    @Test
    public void shouldNotUnblockWhenPositionHasCompleteMessage()
    {
        final int blockedOffset = HEADER_LENGTH * 4;
        final long blockedPosition = computePosition(TERM_ID_1, blockedOffset, positionBitsToShift, TERM_ID_1);

        when(termBuffers[activeIndex].getIntVolatile(blockedOffset)).thenReturn(HEADER_LENGTH);

        assertFalse(LogBufferUnblocker.unblock(partitions, logMetaDataBuffer, blockedPosition));

        assertThat(computePosition(
            activeTermId(logMetaDataBuffer), blockedOffset, positionBitsToShift, TERM_ID_1),
            is(blockedPosition));
    }

    @Test
    public void shouldUnblockWhenPositionHasNonCommittedMessageAndTailWithinTerm()
    {
        final int blockedOffset = HEADER_LENGTH * 4;
        final int messageLength = HEADER_LENGTH * 4;
        final long blockedPosition = computePosition(TERM_ID_1, blockedOffset, positionBitsToShift, TERM_ID_1);

        when(termBuffers[activeIndex].getIntVolatile(blockedOffset)).thenReturn(-messageLength);

        assertTrue(LogBufferUnblocker.unblock(partitions, logMetaDataBuffer, blockedPosition));

        assertThat(computePosition(
                activeTermId(logMetaDataBuffer), blockedOffset + messageLength, positionBitsToShift, TERM_ID_1),
            is(blockedPosition + messageLength));
    }

    @Test
    public void shouldUnblockWhenPositionHasNonCommittedMessageAndTailAtEndOfTerm()
    {
        final int messageLength = HEADER_LENGTH * 4;
        final int blockedOffset = TERM_LENGTH - messageLength;
        final long blockedPosition = computePosition(TERM_ID_1, blockedOffset, positionBitsToShift, TERM_ID_1);

        when(termBuffers[activeIndex].getIntVolatile(blockedOffset)).thenReturn(0);
        when(termMetaDataBuffers[activeIndex].getIntVolatile(TERM_TAIL_COUNTER_OFFSET)).thenReturn(TERM_LENGTH);

        assertTrue(LogBufferUnblocker.unblock(partitions, logMetaDataBuffer, blockedPosition));

        verify(logMetaDataBuffer).putIntOrdered(LOG_ACTIVE_TERM_ID_OFFSET, TERM_ID_1 + 1);

        assertThat(computePosition(
                activeTermId(logMetaDataBuffer), 0, positionBitsToShift, TERM_ID_1),
            is(blockedPosition + messageLength));
    }

    @Test
    public void shouldUnblockWhenPositionHasNonCommittedMessageAndTailPastEndOfTerm()
    {
        final int messageLength = HEADER_LENGTH * 4;
        final int blockedOffset = TERM_LENGTH - messageLength;
        final long blockedPosition = computePosition(TERM_ID_1, blockedOffset, positionBitsToShift, TERM_ID_1);

        when(termBuffers[activeIndex].getIntVolatile(blockedOffset)).thenReturn(0);
        when(termMetaDataBuffers[activeIndex].getIntVolatile(TERM_TAIL_COUNTER_OFFSET)).thenReturn(TERM_LENGTH + HEADER_LENGTH);

        assertTrue(LogBufferUnblocker.unblock(partitions, logMetaDataBuffer, blockedPosition));

        verify(logMetaDataBuffer).putIntOrdered(LOG_ACTIVE_TERM_ID_OFFSET, TERM_ID_1 + 1);

        assertThat(computePosition(
                activeTermId(logMetaDataBuffer), 0, positionBitsToShift, TERM_ID_1),
            is(blockedPosition + messageLength));
    }
}
