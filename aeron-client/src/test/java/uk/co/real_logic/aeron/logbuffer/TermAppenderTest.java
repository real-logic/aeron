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
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.logbuffer.FrameDescriptor.*;
import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.TERM_META_DATA_LENGTH;
import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET;
import static uk.co.real_logic.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static uk.co.real_logic.agrona.BitUtil.*;

public class TermAppenderTest
{
    private static final int TERM_BUFFER_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int META_DATA_BUFFER_LENGTH = TERM_META_DATA_LENGTH;
    private static final int MAX_FRAME_LENGTH = 1024;
    private static final MutableDirectBuffer DEFAULT_HEADER = new UnsafeBuffer(ByteBuffer.allocateDirect(HEADER_LENGTH));

    private final UnsafeBuffer termBuffer = spy(new UnsafeBuffer(ByteBuffer.allocateDirect(TERM_BUFFER_LENGTH)));
    private final UnsafeBuffer metaDataBuffer = mock(UnsafeBuffer.class);

    private TermAppender termAppender;

    @Before
    public void setUp()
    {
        when(termBuffer.capacity()).thenReturn(TERM_BUFFER_LENGTH);
        when(metaDataBuffer.capacity()).thenReturn(META_DATA_BUFFER_LENGTH);

        termAppender = new TermAppender(termBuffer, metaDataBuffer, DEFAULT_HEADER, MAX_FRAME_LENGTH);
    }

    @Test
    public void shouldReportCapacity()
    {
        assertThat(termAppender.termBuffer().capacity(), is(TERM_BUFFER_LENGTH));
    }

    @Test
    public void shouldReportMaxFrameLength()
    {
        assertThat(termAppender.maxFrameLength(), is(MAX_FRAME_LENGTH));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionOnInsufficientCapacityForLog()
    {
        when(termBuffer.capacity()).thenReturn(LogBufferDescriptor.TERM_MIN_LENGTH - 1);

        termAppender = new TermAppender(termBuffer, metaDataBuffer, DEFAULT_HEADER, MAX_FRAME_LENGTH);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionWhenCapacityNotMultipleOfAlignment()
    {
        final int logBufferCapacity = LogBufferDescriptor.TERM_MIN_LENGTH + FRAME_ALIGNMENT + 1;
        when(termBuffer.capacity()).thenReturn(logBufferCapacity);

        termAppender = new TermAppender(termBuffer, metaDataBuffer, DEFAULT_HEADER, MAX_FRAME_LENGTH);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionOnInsufficientMetaDataBufferCapacity()
    {
        when(metaDataBuffer.capacity()).thenReturn(LogBufferDescriptor.TERM_META_DATA_LENGTH - 1);

        termAppender = new TermAppender(termBuffer, metaDataBuffer, DEFAULT_HEADER, MAX_FRAME_LENGTH);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionOnDefaultHeaderLengthLessThanBaseHeaderLength()
    {
        final int length = HEADER_LENGTH - 1;
        termAppender = new TermAppender(termBuffer, metaDataBuffer, new UnsafeBuffer(new byte[length]), MAX_FRAME_LENGTH);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionOnDefaultHeaderLengthNotOnWordSizeBoundary()
    {
        termAppender = new TermAppender(termBuffer, metaDataBuffer, new UnsafeBuffer(new byte[31]), MAX_FRAME_LENGTH);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionOnMaxFrameSizeNotOnWordSizeBoundary()
    {
        final int maxFrameLength = 1001;
        termAppender = new TermAppender(termBuffer, metaDataBuffer, DEFAULT_HEADER, maxFrameLength);
    }

    @Test
    public void shouldReportCurrentTail()
    {
        final int tailValue = 64;

        when(metaDataBuffer.getIntVolatile(TERM_TAIL_COUNTER_OFFSET)).thenReturn(tailValue);

        assertThat(termAppender.tailVolatile(), is(tailValue));
    }

    @Test
    public void shouldReportCurrentTailAtCapacity()
    {
        final int tailValue = TERM_BUFFER_LENGTH + 64;

        when(metaDataBuffer.getIntVolatile(TERM_TAIL_COUNTER_OFFSET)).thenReturn(tailValue);
        when(metaDataBuffer.getInt(TERM_TAIL_COUNTER_OFFSET)).thenReturn(tailValue);

        assertThat(termAppender.tailVolatile(), is(TERM_BUFFER_LENGTH));
        assertThat(termAppender.tail(), is(TERM_BUFFER_LENGTH));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenMaxMessageLengthExceeded()
    {
        final int maxMessageLength = termAppender.maxMessageLength();
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[1024]);

        termAppender.append(srcBuffer, 0, maxMessageLength + 1);
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

        when(metaDataBuffer.getAndAddInt(TERM_TAIL_COUNTER_OFFSET, alignedFrameLength)).thenReturn(0);

        assertThat(termAppender.append(buffer, 0, msgLength), is(alignedFrameLength));

        final InOrder inOrder = inOrder(termBuffer, metaDataBuffer);
        inOrder.verify(metaDataBuffer, times(1)).getAndAddInt(TERM_TAIL_COUNTER_OFFSET, alignedFrameLength);
        verifyDefaultHeader(inOrder, termBuffer, tail, frameLength);
        inOrder.verify(termBuffer, times(1)).putBytes(headerLength, buffer, 0, msgLength);
        inOrder.verify(termBuffer, times(1)).putInt(termOffsetOffset(tail), tail, LITTLE_ENDIAN);
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

        when(metaDataBuffer.getAndAddInt(TERM_TAIL_COUNTER_OFFSET, alignedFrameLength))
            .thenReturn(0)
            .thenReturn(alignedFrameLength);

        assertThat(termAppender.append(buffer, 0, msgLength), is(alignedFrameLength));
        assertThat(termAppender.append(buffer, 0, msgLength), is(alignedFrameLength * 2));

        final InOrder inOrder = inOrder(termBuffer, metaDataBuffer);
        inOrder.verify(metaDataBuffer, times(1)).getAndAddInt(TERM_TAIL_COUNTER_OFFSET, alignedFrameLength);
        verifyDefaultHeader(inOrder, termBuffer, tail, frameLength);
        inOrder.verify(termBuffer, times(1)).putBytes(headerLength, buffer, 0, msgLength);
        inOrder.verify(termBuffer, times(1)).putInt(termOffsetOffset(tail), tail, LITTLE_ENDIAN);
        inOrder.verify(termBuffer, times(1)).putIntOrdered(tail, frameLength);

        tail = alignedFrameLength;
        inOrder.verify(metaDataBuffer, times(1)).getAndAddInt(TERM_TAIL_COUNTER_OFFSET, alignedFrameLength);
        verifyDefaultHeader(inOrder, termBuffer, tail, frameLength);
        inOrder.verify(termBuffer, times(1)).putBytes(tail + headerLength, buffer, 0, msgLength);
        inOrder.verify(termBuffer, times(1)).putInt(termOffsetOffset(tail), tail, LITTLE_ENDIAN);
        inOrder.verify(termBuffer, times(1)).putIntOrdered(tail, frameLength);
    }

    @Test
    public void shouldPadLogAndTripWhenAppendingWithInsufficientRemainingCapacity()
    {
        final int msgLength = 120;
        final int headerLength = DEFAULT_HEADER.capacity();
        final int requiredFrameSize = align(headerLength + msgLength, FRAME_ALIGNMENT);
        final int tailValue = termAppender.termBuffer().capacity() - align(msgLength, FRAME_ALIGNMENT);
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[128]);
        final int frameLength = TERM_BUFFER_LENGTH - tailValue;

        when(metaDataBuffer.getAndAddInt(TERM_TAIL_COUNTER_OFFSET, requiredFrameSize))
            .thenReturn(tailValue);

        assertThat(termAppender.append(buffer, 0, msgLength), is(TermAppender.TRIPPED));

        final InOrder inOrder = inOrder(termBuffer, metaDataBuffer);
        inOrder.verify(metaDataBuffer, times(1)).getAndAddInt(TERM_TAIL_COUNTER_OFFSET, requiredFrameSize);
        verifyDefaultHeader(inOrder, termBuffer, tailValue, frameLength);
        inOrder.verify(termBuffer, times(1)).putShort(typeOffset(tailValue), (short)PADDING_FRAME_TYPE, LITTLE_ENDIAN);
        inOrder.verify(termBuffer, times(1)).putInt(termOffsetOffset(tailValue), tailValue, LITTLE_ENDIAN);
        inOrder.verify(termBuffer, times(1)).putIntOrdered(tailValue, frameLength);
    }

    @Test
    public void shouldPadLogAndTripWhenAppendingWithInsufficientRemainingCapacityIncludingHeader()
    {
        final int headerLength = DEFAULT_HEADER.capacity();
        final int msgLength = 120;
        final int requiredFrameSize = align(headerLength + msgLength, FRAME_ALIGNMENT);
        final int tailValue = termAppender.termBuffer().capacity() - (requiredFrameSize + (headerLength - FRAME_ALIGNMENT));
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[128]);
        final int frameLength = TERM_BUFFER_LENGTH - tailValue;

        when(metaDataBuffer.getAndAddInt(TERM_TAIL_COUNTER_OFFSET, requiredFrameSize))
            .thenReturn(tailValue);

        assertThat(termAppender.append(buffer, 0, msgLength), is(TermAppender.TRIPPED));

        final InOrder inOrder = inOrder(termBuffer, metaDataBuffer);
        inOrder.verify(metaDataBuffer, times(1)).getAndAddInt(TERM_TAIL_COUNTER_OFFSET, requiredFrameSize);
        verifyDefaultHeader(inOrder, termBuffer, tailValue, frameLength);
        inOrder.verify(termBuffer, times(1)).putShort(typeOffset(tailValue), (short)PADDING_FRAME_TYPE, LITTLE_ENDIAN);
        inOrder.verify(termBuffer, times(1)).putInt(termOffsetOffset(tailValue), tailValue, LITTLE_ENDIAN);
        inOrder.verify(termBuffer, times(1)).putIntOrdered(tailValue, frameLength);
    }

    @Test
    public void shouldFragmentMessageOverTwoFrames()
    {
        final int msgLength = termAppender.maxPayloadLength() + 1;
        final int headerLength = DEFAULT_HEADER.capacity();
        final int frameLength = headerLength + 1;
        final int requiredCapacity = align(headerLength + 1, FRAME_ALIGNMENT) + termAppender.maxFrameLength();
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[msgLength]);

        when(metaDataBuffer.getAndAddInt(TERM_TAIL_COUNTER_OFFSET, requiredCapacity))
            .thenReturn(0);

        assertThat(termAppender.append(buffer, 0, msgLength), is(requiredCapacity));

        int tail  = 0;
        final InOrder inOrder = inOrder(termBuffer, metaDataBuffer);
        inOrder.verify(metaDataBuffer, times(1)).getAndAddInt(TERM_TAIL_COUNTER_OFFSET, requiredCapacity);

        verifyDefaultHeader(inOrder, termBuffer, tail, termAppender.maxFrameLength());
        inOrder.verify(termBuffer, times(1)).putBytes(tail + headerLength, buffer, 0, termAppender.maxPayloadLength());
        inOrder.verify(termBuffer, times(1)).putByte(flagsOffset(tail), BEGIN_FRAG);
        inOrder.verify(termBuffer, times(1)).putInt(termOffsetOffset(tail), tail, LITTLE_ENDIAN);
        inOrder.verify(termBuffer, times(1)).putIntOrdered(tail, termAppender.maxFrameLength());

        tail = termAppender.maxFrameLength();
        verifyDefaultHeader(inOrder, termBuffer, tail, frameLength);
        inOrder.verify(termBuffer, times(1)).putBytes(tail + headerLength, buffer, termAppender.maxPayloadLength(), 1);
        inOrder.verify(termBuffer, times(1)).putByte(flagsOffset(tail), END_FRAG);
        inOrder.verify(termBuffer, times(1)).putInt(termOffsetOffset(tail), tail, LITTLE_ENDIAN);
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

        when(metaDataBuffer.getAndAddInt(TERM_TAIL_COUNTER_OFFSET, alignedFrameLength)).thenReturn(0);

        assertThat(termAppender.claim(msgLength, bufferClaim), is(alignedFrameLength));

        assertThat(bufferClaim.offset(), is(tail + headerLength));
        assertThat(bufferClaim.length(), is(msgLength));

        // Map flyweight or encode to buffer directly then call commit() when done
        bufferClaim.commit();

        final InOrder inOrder = inOrder(termBuffer, metaDataBuffer);
        inOrder.verify(metaDataBuffer, times(1)).getAndAddInt(TERM_TAIL_COUNTER_OFFSET, alignedFrameLength);
        verifyDefaultHeader(inOrder, termBuffer, tail, frameLength);
        inOrder.verify(termBuffer, times(1)).putInt(termOffsetOffset(tail), tail, LITTLE_ENDIAN);
    }

    private void verifyDefaultHeader(
        final InOrder inOrder, final UnsafeBuffer termBuffer, final int frameOffset, final int frameLength)
    {
        inOrder.verify(termBuffer, times(1)).putInt(frameOffset, -frameLength, LITTLE_ENDIAN);

        int headerOffset = SIZE_OF_INT;
        inOrder.verify(termBuffer, times(1)).putInt(eq(frameOffset + headerOffset), anyInt());

        headerOffset += SIZE_OF_INT;
        inOrder.verify(termBuffer, times(1)).putLong(eq(frameOffset + headerOffset), anyLong());

        headerOffset += SIZE_OF_LONG;
        inOrder.verify(termBuffer, times(1)).putLong(eq(frameOffset + headerOffset), anyLong());
    }
}
