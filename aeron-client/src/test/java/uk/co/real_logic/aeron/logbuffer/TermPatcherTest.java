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
import org.mockito.InOrder;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

import static uk.co.real_logic.aeron.logbuffer.FrameDescriptor.typeOffset;
import static uk.co.real_logic.aeron.logbuffer.TermPatcher.PatchStatus.NO_ACTION;
import static uk.co.real_logic.aeron.logbuffer.TermPatcher.PatchStatus.PATCHED;
import static uk.co.real_logic.aeron.logbuffer.TermPatcher.PatchStatus.PATCHED_TO_END;
import static uk.co.real_logic.aeron.protocol.HeaderFlyweight.HDR_TYPE_PAD;
import static uk.co.real_logic.aeron.protocol.HeaderFlyweight.HEADER_LENGTH;

public class TermPatcherTest
{
    private static final int TERM_BUFFER_CAPACITY = 64 * 1014;

    private final UnsafeBuffer mockBuffer = mock(UnsafeBuffer.class);

    @Before
    public void setUp()
    {
        when(mockBuffer.capacity()).thenReturn(TERM_BUFFER_CAPACITY);
    }

    @Test
    public void shouldTakeNoActionWhenMessageIsComplete()
    {
        final int termOffset = 0;
        final int tailOffset = TERM_BUFFER_CAPACITY;
        when(mockBuffer.getIntVolatile(termOffset)).thenReturn(HEADER_LENGTH);

        assertThat(TermPatcher.patch(mockBuffer, termOffset, tailOffset), is(NO_ACTION));
    }

    @Test
    public void shouldTakeNoActionWhenNoUnblockedMessage()
    {
        final int termOffset = 0;
        final int tailOffset = TERM_BUFFER_CAPACITY / 2;

        assertThat(TermPatcher.patch(mockBuffer, termOffset, tailOffset), is(NO_ACTION));
    }

    @Test
    public void shouldPatchNonCommittedMessage()
    {
        final int termOffset = 0;
        final int messageLength = HEADER_LENGTH * 4;
        final int tailOffset = messageLength;

        when(mockBuffer.getIntVolatile(termOffset)).thenReturn(-messageLength);

        assertThat(TermPatcher.patch(mockBuffer, termOffset, tailOffset), is(PATCHED));

        final InOrder inOrder = inOrder(mockBuffer);
        inOrder.verify(mockBuffer).putShort(typeOffset(termOffset), (short)HDR_TYPE_PAD, LITTLE_ENDIAN);
        inOrder.verify(mockBuffer).putIntOrdered(termOffset, messageLength);
    }

    @Test
    public void shouldPatchToEndOfPartition()
    {
        final int messageLength = HEADER_LENGTH * 4;
        final int termOffset = TERM_BUFFER_CAPACITY - messageLength;
        final int tailOffset = TERM_BUFFER_CAPACITY;

        when(mockBuffer.getIntVolatile(termOffset)).thenReturn(0);

        assertThat(TermPatcher.patch(mockBuffer, termOffset, tailOffset), is(PATCHED_TO_END));

        final InOrder inOrder = inOrder(mockBuffer);
        inOrder.verify(mockBuffer).putShort(typeOffset(termOffset), (short)HDR_TYPE_PAD, LITTLE_ENDIAN);
        inOrder.verify(mockBuffer).putIntOrdered(termOffset, messageLength);
    }

    @Test
    public void shouldScanForwardForNextCompleteMessage()
    {
        final int messageLength = HEADER_LENGTH * 4;
        final int termOffset = 0;
        final int tailOffset = messageLength * 2;

        when(mockBuffer.getIntVolatile(messageLength)).thenReturn(messageLength);

        assertThat(TermPatcher.patch(mockBuffer, termOffset, tailOffset), is(PATCHED));

        final InOrder inOrder = inOrder(mockBuffer);
        inOrder.verify(mockBuffer).putShort(typeOffset(termOffset), (short)HDR_TYPE_PAD, LITTLE_ENDIAN);
        inOrder.verify(mockBuffer).putIntOrdered(termOffset, messageLength);
    }

    @Test
    public void shouldScanForwardForNextNonCommittedMessage()
    {
        final int messageLength = HEADER_LENGTH * 4;
        final int termOffset = 0;
        final int tailOffset = messageLength * 2;

        when(mockBuffer.getIntVolatile(messageLength)).thenReturn(-messageLength);

        assertThat(TermPatcher.patch(mockBuffer, termOffset, tailOffset), is(PATCHED));

        final InOrder inOrder = inOrder(mockBuffer);
        inOrder.verify(mockBuffer).putShort(typeOffset(termOffset), (short)HDR_TYPE_PAD, LITTLE_ENDIAN);
        inOrder.verify(mockBuffer).putIntOrdered(termOffset, messageLength);
    }

    @Test
    public void shouldTakeNoActionIfMessageCompleteAfterScan()
    {
        final int messageLength = HEADER_LENGTH * 4;
        final int termOffset = 0;
        final int tailOffset = messageLength * 2;

        when(mockBuffer.getIntVolatile(termOffset))
            .thenReturn(0)
            .thenReturn(messageLength);

        when(mockBuffer.getIntVolatile(messageLength))
            .thenReturn(messageLength);

        assertThat(TermPatcher.patch(mockBuffer, termOffset, tailOffset), is(NO_ACTION));
    }

    @Test
    public void shouldTakeNoActionIfMessageNonCommittedAfterScan()
    {
        final int messageLength = HEADER_LENGTH * 4;
        final int termOffset = 0;
        final int tailOffset = messageLength * 2;

        when(mockBuffer.getIntVolatile(termOffset))
            .thenReturn(0)
            .thenReturn(-messageLength);

        when(mockBuffer.getIntVolatile(messageLength))
            .thenReturn(messageLength);

        assertThat(TermPatcher.patch(mockBuffer, termOffset, tailOffset), is(NO_ACTION));
    }

    @Test
    public void shouldTakeNoActionToEndOfPartitionIfMessageCompleteAfterScan()
    {
        final int messageLength = HEADER_LENGTH * 4;
        final int termOffset = TERM_BUFFER_CAPACITY - messageLength;
        final int tailOffset = TERM_BUFFER_CAPACITY;

        when(mockBuffer.getIntVolatile(termOffset))
            .thenReturn(0)
            .thenReturn(messageLength);

        assertThat(TermPatcher.patch(mockBuffer, termOffset, tailOffset), is(NO_ACTION));
    }

    @Test
    public void shouldTakeNoActionToEndOfPartitionIfMessageNonCommittedAfterScan()
    {
        final int messageLength = HEADER_LENGTH * 4;
        final int termOffset = TERM_BUFFER_CAPACITY - messageLength;
        final int tailOffset = TERM_BUFFER_CAPACITY;

        when(mockBuffer.getIntVolatile(termOffset))
            .thenReturn(0)
            .thenReturn(-messageLength);

        assertThat(TermPatcher.patch(mockBuffer, termOffset, tailOffset), is(NO_ACTION));
    }
}