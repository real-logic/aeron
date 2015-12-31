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
package uk.co.real_logic.aeron.logbuffer;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import uk.co.real_logic.aeron.protocol.DataHeaderFlyweight;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.logbuffer.FrameDescriptor.*;
import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.TERM_META_DATA_LENGTH;
import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET;
import static uk.co.real_logic.aeron.logbuffer.TermAppender.TRIPPED;
import static uk.co.real_logic.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static uk.co.real_logic.agrona.BitUtil.*;

public class TermAppenderTest
{
    private static final int TERM_BUFFER_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int META_DATA_BUFFER_LENGTH = TERM_META_DATA_LENGTH;
    private static final int MAX_FRAME_LENGTH = 1024;
    private static final int MAX_PAYLOAD_LENGTH = MAX_FRAME_LENGTH - HEADER_LENGTH;
    private static final UnsafeBuffer DEFAULT_HEADER = new UnsafeBuffer(ByteBuffer.allocateDirect(HEADER_LENGTH));

    private final UnsafeBuffer termBuffer = spy(new UnsafeBuffer(ByteBuffer.allocateDirect(TERM_BUFFER_LENGTH)));
    private final UnsafeBuffer metaDataBuffer = mock(UnsafeBuffer.class);

    private final HeaderWriter headerWriter =
        spy(new HeaderWriter(DataHeaderFlyweight.createDefaultHeader(0, 0, 0)));

    private TermAppender termAppender;

    @Before
    public void setUp()
    {
        when(termBuffer.capacity()).thenReturn(TERM_BUFFER_LENGTH);
        when(metaDataBuffer.capacity()).thenReturn(META_DATA_BUFFER_LENGTH);

        termAppender = new TermAppender(termBuffer, metaDataBuffer);
    }

    @Test
    public void shouldAppendFrameToEmptyLog()
    {
        final int headerLength = DEFAULT_HEADER.capacity();
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[128]);
        final int msgLength = 20;
        final int frameLength = msgLength + headerLength;
        final int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);
        final int tail = 0;

        when(metaDataBuffer.getAndAddLong(TERM_TAIL_COUNTER_OFFSET, alignedFrameLength)).thenReturn(0L);

        assertThat(termAppender.appendUnfragmentedMessage(headerWriter, buffer, 0, msgLength), is(alignedFrameLength));

        final InOrder inOrder = inOrder(termBuffer, metaDataBuffer);
        inOrder.verify(metaDataBuffer, times(1)).getAndAddLong(TERM_TAIL_COUNTER_OFFSET, alignedFrameLength);
        verifyDefaultHeader(inOrder, termBuffer, tail);
        inOrder.verify(termBuffer, times(1)).putBytes(headerLength, buffer, 0, msgLength);
        inOrder.verify(termBuffer, times(1)).putIntOrdered(tail, frameLength);
    }

    @Test
    public void shouldAppendFrameTwiceToLog()
    {
        final int headerLength = DEFAULT_HEADER.capacity();
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[128]);
        final int msgLength = 20;
        final int frameLength = msgLength + headerLength;
        final int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);
        int tail = 0;

        when(metaDataBuffer.getAndAddLong(TERM_TAIL_COUNTER_OFFSET, alignedFrameLength))
            .thenReturn(0L)
            .thenReturn((long)alignedFrameLength);

        assertThat(termAppender.appendUnfragmentedMessage(
            headerWriter, buffer, 0, msgLength), is(alignedFrameLength));
        assertThat(termAppender.appendUnfragmentedMessage(
            headerWriter, buffer, 0, msgLength), is(alignedFrameLength * 2));

        final InOrder inOrder = inOrder(termBuffer, metaDataBuffer);
        inOrder.verify(metaDataBuffer, times(1)).getAndAddLong(TERM_TAIL_COUNTER_OFFSET, alignedFrameLength);
        verifyDefaultHeader(inOrder, termBuffer, tail);
        inOrder.verify(termBuffer, times(1)).putBytes(headerLength, buffer, 0, msgLength);
        inOrder.verify(termBuffer, times(1)).putIntOrdered(tail, frameLength);

        tail = alignedFrameLength;
        inOrder.verify(metaDataBuffer, times(1)).getAndAddLong(TERM_TAIL_COUNTER_OFFSET, alignedFrameLength);
        verifyDefaultHeader(inOrder, termBuffer, tail);
        inOrder.verify(termBuffer, times(1)).putBytes(tail + headerLength, buffer, 0, msgLength);
        inOrder.verify(termBuffer, times(1)).putIntOrdered(tail, frameLength);
    }

    @Test
    public void shouldPadLogAndTripWhenAppendingWithInsufficientRemainingCapacity()
    {
        final int msgLength = 120;
        final int headerLength = DEFAULT_HEADER.capacity();
        final int requiredFrameSize = align(headerLength + msgLength, FRAME_ALIGNMENT);
        final int tailValue = TERM_BUFFER_LENGTH - align(msgLength, FRAME_ALIGNMENT);
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[128]);
        final int frameLength = TERM_BUFFER_LENGTH - tailValue;

        when(metaDataBuffer.getAndAddLong(TERM_TAIL_COUNTER_OFFSET, requiredFrameSize))
            .thenReturn((long)tailValue);

        assertThat(termAppender.appendUnfragmentedMessage(headerWriter, buffer, 0, msgLength), is(TRIPPED));

        final InOrder inOrder = inOrder(termBuffer, metaDataBuffer);
        inOrder.verify(metaDataBuffer, times(1)).getAndAddLong(TERM_TAIL_COUNTER_OFFSET, requiredFrameSize);
        verifyDefaultHeader(inOrder, termBuffer, tailValue);
        inOrder.verify(termBuffer, times(1)).putShort(typeOffset(tailValue), (short)PADDING_FRAME_TYPE, LITTLE_ENDIAN);
        inOrder.verify(termBuffer, times(1)).putIntOrdered(tailValue, frameLength);
    }

    @Test
    public void shouldPadLogAndTripWhenAppendingWithInsufficientRemainingCapacityIncludingHeader()
    {
        final int headerLength = DEFAULT_HEADER.capacity();
        final int msgLength = 120;
        final int requiredFrameSize = align(headerLength + msgLength, FRAME_ALIGNMENT);
        final int tailValue = TERM_BUFFER_LENGTH - (requiredFrameSize + (headerLength - FRAME_ALIGNMENT));
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[128]);
        final int frameLength = TERM_BUFFER_LENGTH - tailValue;

        when(metaDataBuffer.getAndAddLong(TERM_TAIL_COUNTER_OFFSET, requiredFrameSize))
            .thenReturn((long)tailValue);

        assertThat(termAppender.appendUnfragmentedMessage(headerWriter, buffer, 0, msgLength), is(TRIPPED));

        final InOrder inOrder = inOrder(termBuffer, metaDataBuffer);
        inOrder.verify(metaDataBuffer, times(1)).getAndAddLong(TERM_TAIL_COUNTER_OFFSET, requiredFrameSize);
        verifyDefaultHeader(inOrder, termBuffer, tailValue);
        inOrder.verify(termBuffer, times(1)).putShort(typeOffset(tailValue), (short)PADDING_FRAME_TYPE, LITTLE_ENDIAN);
        inOrder.verify(termBuffer, times(1)).putIntOrdered(tailValue, frameLength);
    }

    @Test
    public void shouldFragmentMessageOverTwoFrames()
    {
        final int msgLength = MAX_PAYLOAD_LENGTH + 1;
        final int headerLength = DEFAULT_HEADER.capacity();
        final int frameLength = headerLength + 1;
        final int requiredCapacity = align(headerLength + 1, FRAME_ALIGNMENT) + MAX_FRAME_LENGTH;
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[msgLength]);

        when(metaDataBuffer.getAndAddLong(TERM_TAIL_COUNTER_OFFSET, requiredCapacity))
            .thenReturn(0L);

        assertThat(termAppender.appendFragmentedMessage(
            headerWriter, buffer, 0, msgLength, MAX_PAYLOAD_LENGTH), is(requiredCapacity));

        int tail  = 0;
        final InOrder inOrder = inOrder(termBuffer, metaDataBuffer);
        inOrder.verify(metaDataBuffer, times(1)).getAndAddLong(TERM_TAIL_COUNTER_OFFSET, requiredCapacity);

        verifyDefaultHeader(inOrder, termBuffer, tail);
        inOrder.verify(termBuffer, times(1)).putBytes(tail + headerLength, buffer, 0, MAX_PAYLOAD_LENGTH);
        inOrder.verify(termBuffer, times(1)).putByte(flagsOffset(tail), BEGIN_FRAG_FLAG);
        inOrder.verify(termBuffer, times(1)).putIntOrdered(tail, MAX_FRAME_LENGTH);

        tail = MAX_FRAME_LENGTH;
        verifyDefaultHeader(inOrder, termBuffer, tail);
        inOrder.verify(termBuffer, times(1)).putBytes(tail + headerLength, buffer, MAX_PAYLOAD_LENGTH, 1);
        inOrder.verify(termBuffer, times(1)).putByte(flagsOffset(tail), END_FRAG_FLAG);
        inOrder.verify(termBuffer, times(1)).putIntOrdered(tail, frameLength);
    }

    @Test
    public void shouldClaimRegionForZeroCopyEncoding()
    {
        final int headerLength = DEFAULT_HEADER.capacity();
        final int msgLength = 20;
        final int frameLength = msgLength + headerLength;
        final int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);
        final int tail = 0;
        final BufferClaim bufferClaim = new BufferClaim();

        when(metaDataBuffer.getAndAddLong(TERM_TAIL_COUNTER_OFFSET, alignedFrameLength)).thenReturn(0L);

        assertThat(termAppender.claim(headerWriter, msgLength, bufferClaim), is(alignedFrameLength));

        assertThat(bufferClaim.offset(), is(tail + headerLength));
        assertThat(bufferClaim.length(), is(msgLength));

        // Map flyweight or encode to buffer directly then call commit() when done
        bufferClaim.commit();

        final InOrder inOrder = inOrder(termBuffer, metaDataBuffer);
        inOrder.verify(metaDataBuffer, times(1)).getAndAddLong(TERM_TAIL_COUNTER_OFFSET, alignedFrameLength);
        verifyDefaultHeader(inOrder, termBuffer, tail);
    }

    private void verifyDefaultHeader(final InOrder inOrder, final UnsafeBuffer termBuffer, final int frameOffset)
    {
        long headerOffset = 0;
        inOrder.verify(termBuffer, times(1)).putLongOrdered(eq(frameOffset + headerOffset), anyLong());

        headerOffset += SIZE_OF_LONG;
        inOrder.verify(termBuffer, times(1)).putLong(eq(frameOffset + headerOffset), anyLong());

        headerOffset += SIZE_OF_LONG;
        inOrder.verify(termBuffer, times(1)).putLong(eq(frameOffset + headerOffset), anyLong());
    }
}
