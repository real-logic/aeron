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
import org.mockito.InOrder;
import org.agrona.concurrent.UnsafeBuffer;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

import static io.aeron.logbuffer.FrameDescriptor.termOffsetOffset;
import static io.aeron.logbuffer.FrameDescriptor.typeOffset;
import static io.aeron.logbuffer.TermUnblocker.Status.NO_ACTION;
import static io.aeron.logbuffer.TermUnblocker.Status.UNBLOCKED;
import static io.aeron.logbuffer.TermUnblocker.Status.UNBLOCKED_TO_END;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_PAD;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;

public class TermUnblockerTest
{
    private static final int TERM_BUFFER_CAPACITY = 64 * 1014;
    private static final int TERM_ID = 7;

    private final UnsafeBuffer mockTermBuffer = mock(UnsafeBuffer.class);
    private final UnsafeBuffer mockLogMetaDataBuffer = mock(UnsafeBuffer.class);

    @Before
    public void setUp()
    {
        when(mockTermBuffer.capacity()).thenReturn(TERM_BUFFER_CAPACITY);
    }

    @Test
    public void shouldTakeNoActionWhenMessageIsComplete()
    {
        final int termOffset = 0;
        final int tailOffset = TERM_BUFFER_CAPACITY;
        when(mockTermBuffer.getIntVolatile(termOffset)).thenReturn(HEADER_LENGTH);

        assertThat(TermUnblocker.unblock(
            mockLogMetaDataBuffer, mockTermBuffer, termOffset, tailOffset, TERM_ID), is(NO_ACTION));
    }

    @Test
    public void shouldTakeNoActionWhenNoUnblockedMessage()
    {
        final int termOffset = 0;
        final int tailOffset = TERM_BUFFER_CAPACITY / 2;

        assertThat(TermUnblocker.unblock(
            mockLogMetaDataBuffer, mockTermBuffer, termOffset, tailOffset, TERM_ID), is(NO_ACTION));
    }

    @Test
    public void shouldPatchNonCommittedMessage()
    {
        final int termOffset = 0;
        final int messageLength = HEADER_LENGTH * 4;
        final int tailOffset = messageLength;

        when(mockTermBuffer.getIntVolatile(termOffset)).thenReturn(-messageLength);

        assertThat(TermUnblocker.unblock(
            mockLogMetaDataBuffer, mockTermBuffer, termOffset, tailOffset, TERM_ID), is(UNBLOCKED));

        final InOrder inOrder = inOrder(mockTermBuffer);
        inOrder.verify(mockTermBuffer).putShort(typeOffset(termOffset), (short)HDR_TYPE_PAD, LITTLE_ENDIAN);
        inOrder.verify(mockTermBuffer).putInt(termOffsetOffset(termOffset), termOffset, LITTLE_ENDIAN);
        inOrder.verify(mockTermBuffer).putIntOrdered(termOffset, messageLength);
    }

    @Test
    public void shouldPatchToEndOfPartition()
    {
        final int messageLength = HEADER_LENGTH * 4;
        final int termOffset = TERM_BUFFER_CAPACITY - messageLength;
        final int tailOffset = TERM_BUFFER_CAPACITY;

        when(mockTermBuffer.getIntVolatile(termOffset)).thenReturn(0);

        assertThat(TermUnblocker.unblock(
            mockLogMetaDataBuffer, mockTermBuffer, termOffset, tailOffset, TERM_ID), is(UNBLOCKED_TO_END));

        final InOrder inOrder = inOrder(mockTermBuffer);
        inOrder.verify(mockTermBuffer).putShort(typeOffset(termOffset), (short)HDR_TYPE_PAD, LITTLE_ENDIAN);
        inOrder.verify(mockTermBuffer).putInt(termOffsetOffset(termOffset), termOffset, LITTLE_ENDIAN);
        inOrder.verify(mockTermBuffer).putIntOrdered(termOffset, messageLength);
    }

    @Test
    public void shouldScanForwardForNextCompleteMessage()
    {
        final int messageLength = HEADER_LENGTH * 4;
        final int termOffset = 0;
        final int tailOffset = messageLength * 2;

        when(mockTermBuffer.getIntVolatile(messageLength)).thenReturn(messageLength);

        assertThat(TermUnblocker.unblock(
            mockLogMetaDataBuffer, mockTermBuffer, termOffset, tailOffset, TERM_ID), is(UNBLOCKED));

        final InOrder inOrder = inOrder(mockTermBuffer);
        inOrder.verify(mockTermBuffer).putShort(typeOffset(termOffset), (short)HDR_TYPE_PAD, LITTLE_ENDIAN);
        inOrder.verify(mockTermBuffer).putInt(termOffsetOffset(termOffset), termOffset, LITTLE_ENDIAN);
        inOrder.verify(mockTermBuffer).putIntOrdered(termOffset, messageLength);
    }

    @Test
    public void shouldScanForwardForNextNonCommittedMessage()
    {
        final int messageLength = HEADER_LENGTH * 4;
        final int termOffset = 0;
        final int tailOffset = messageLength * 2;

        when(mockTermBuffer.getIntVolatile(messageLength)).thenReturn(-messageLength);

        assertThat(TermUnblocker.unblock(
            mockLogMetaDataBuffer, mockTermBuffer, termOffset, tailOffset, TERM_ID), is(UNBLOCKED));

        final InOrder inOrder = inOrder(mockTermBuffer);
        inOrder.verify(mockTermBuffer).putShort(typeOffset(termOffset), (short)HDR_TYPE_PAD, LITTLE_ENDIAN);
        inOrder.verify(mockTermBuffer).putInt(termOffsetOffset(termOffset), termOffset, LITTLE_ENDIAN);
        inOrder.verify(mockTermBuffer).putIntOrdered(termOffset, messageLength);
    }

    @Test
    public void shouldTakeNoActionIfMessageCompleteAfterScan()
    {
        final int messageLength = HEADER_LENGTH * 4;
        final int termOffset = 0;
        final int tailOffset = messageLength * 2;

        when(mockTermBuffer.getIntVolatile(termOffset))
            .thenReturn(0)
            .thenReturn(messageLength);

        when(mockTermBuffer.getIntVolatile(messageLength))
            .thenReturn(messageLength);

        assertThat(TermUnblocker.unblock(
            mockLogMetaDataBuffer, mockTermBuffer, termOffset, tailOffset, TERM_ID), is(NO_ACTION));
    }

    @Test
    public void shouldTakeNoActionIfMessageNonCommittedAfterScan()
    {
        final int messageLength = HEADER_LENGTH * 4;
        final int termOffset = 0;
        final int tailOffset = messageLength * 2;

        when(mockTermBuffer.getIntVolatile(termOffset))
            .thenReturn(0)
            .thenReturn(-messageLength);

        when(mockTermBuffer.getIntVolatile(messageLength))
            .thenReturn(messageLength);

        assertThat(TermUnblocker.unblock(
            mockLogMetaDataBuffer, mockTermBuffer, termOffset, tailOffset, TERM_ID), is(NO_ACTION));
    }

    @Test
    public void shouldTakeNoActionToEndOfPartitionIfMessageCompleteAfterScan()
    {
        final int messageLength = HEADER_LENGTH * 4;
        final int termOffset = TERM_BUFFER_CAPACITY - messageLength;
        final int tailOffset = TERM_BUFFER_CAPACITY;

        when(mockTermBuffer.getIntVolatile(termOffset))
            .thenReturn(0)
            .thenReturn(messageLength);

        assertThat(TermUnblocker.unblock(
            mockLogMetaDataBuffer, mockTermBuffer, termOffset, tailOffset, TERM_ID), is(NO_ACTION));
    }

    @Test
    public void shouldTakeNoActionToEndOfPartitionIfMessageNonCommittedAfterScan()
    {
        final int messageLength = HEADER_LENGTH * 4;
        final int termOffset = TERM_BUFFER_CAPACITY - messageLength;
        final int tailOffset = TERM_BUFFER_CAPACITY;

        when(mockTermBuffer.getIntVolatile(termOffset))
            .thenReturn(0)
            .thenReturn(-messageLength);

        assertThat(TermUnblocker.unblock(
            mockLogMetaDataBuffer, mockTermBuffer, termOffset, tailOffset, TERM_ID), is(NO_ACTION));
    }

    @Test
    public void shouldNotUnblockGapWithMessageRaceOnSecondMessageIncreasingTailThenInterrupting()
    {
        final int messageLength = HEADER_LENGTH * 4;
        final int termOffset = 0;
        final int tailOffset = messageLength * 3;

        when(mockTermBuffer.getIntVolatile(messageLength))
            .thenReturn(0)
            .thenReturn(messageLength);

        when(mockTermBuffer.getIntVolatile(messageLength * 2))
            .thenReturn(messageLength);

        assertThat(TermUnblocker.unblock(
            mockLogMetaDataBuffer, mockTermBuffer, termOffset, tailOffset, TERM_ID), is(NO_ACTION));
    }

    @Test
    public void shouldNotUnblockGapWithMessageRaceWhenScanForwardTakesAnInterrupt()
    {
        final int messageLength = HEADER_LENGTH * 4;
        final int termOffset = 0;
        final int tailOffset = messageLength * 3;

        when(mockTermBuffer.getIntVolatile(messageLength))
            .thenReturn(0)
            .thenReturn(messageLength);

        when(mockTermBuffer.getIntVolatile(messageLength + HEADER_LENGTH))
            .thenReturn(7);

        assertThat(TermUnblocker.unblock(
            mockLogMetaDataBuffer, mockTermBuffer, termOffset, tailOffset, TERM_ID), is(NO_ACTION));
    }
}