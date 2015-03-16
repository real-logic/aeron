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
package uk.co.real_logic.aeron.common.concurrent.logbuffer;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.agrona.BitUtil.SIZE_OF_INT;
import static uk.co.real_logic.agrona.BitUtil.align;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor.*;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogAppender.ActionStatus.*;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.*;

public class LogAppenderTest
{
    private static final int TERM_BUFFER_CAPACITY = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int META_DATA_BUFFER_CAPACITY = TERM_META_DATA_LENGTH;
    private static final int MAX_FRAME_LENGTH = 1024;
    private static final MutableDirectBuffer DEFAULT_HEADER = new UnsafeBuffer(new byte[BASE_HEADER_LENGTH + SIZE_OF_INT]);

    private final UnsafeBuffer termBuffer = mock(UnsafeBuffer.class);
    private final UnsafeBuffer metaDataBuffer = mock(UnsafeBuffer.class);

    private LogAppender logAppender;

    @Before
    public void setUp()
    {
        when(termBuffer.capacity()).thenReturn(TERM_BUFFER_CAPACITY);
        when(metaDataBuffer.capacity()).thenReturn(META_DATA_BUFFER_CAPACITY);

        logAppender = new LogAppender(termBuffer, metaDataBuffer, DEFAULT_HEADER, MAX_FRAME_LENGTH);
    }

    @Test
    public void shouldReportCapacity()
    {
        assertThat(logAppender.termBuffer().capacity(), is(TERM_BUFFER_CAPACITY));
    }

    @Test
    public void shouldReportMaxFrameLength()
    {
        assertThat(logAppender.maxFrameLength(), is(MAX_FRAME_LENGTH));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionOnInsufficientCapacityForLog()
    {
        when(termBuffer.capacity()).thenReturn(LogBufferDescriptor.TERM_MIN_LENGTH - 1);

        logAppender = new LogAppender(termBuffer, metaDataBuffer, DEFAULT_HEADER, MAX_FRAME_LENGTH);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionWhenCapacityNotMultipleOfAlignment()
    {
        final int logBufferCapacity = LogBufferDescriptor.TERM_MIN_LENGTH + FRAME_ALIGNMENT + 1;
        when(termBuffer.capacity()).thenReturn(logBufferCapacity);

        logAppender = new LogAppender(termBuffer, metaDataBuffer, DEFAULT_HEADER, MAX_FRAME_LENGTH);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionOnInsufficientMetaDataBufferCapacity()
    {
        when(metaDataBuffer.capacity()).thenReturn(LogBufferDescriptor.TERM_META_DATA_LENGTH - 1);

        logAppender = new LogAppender(termBuffer, metaDataBuffer, DEFAULT_HEADER, MAX_FRAME_LENGTH);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionOnDefaultHeaderLengthLessThanBaseHeaderLength()
    {
        final int length = BASE_HEADER_LENGTH - 1;
        logAppender = new LogAppender(termBuffer, metaDataBuffer, new UnsafeBuffer(new byte[length]), MAX_FRAME_LENGTH);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionOnDefaultHeaderLengthNotOnWordSizeBoundary()
    {
        logAppender = new LogAppender(termBuffer, metaDataBuffer, new UnsafeBuffer(new byte[31]), MAX_FRAME_LENGTH);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionOnMaxFrameSizeNotOnWordSizeBoundary()
    {
        logAppender = new LogAppender(termBuffer, metaDataBuffer, DEFAULT_HEADER, 1001);
    }

    @Test
    public void shouldReportCurrentTail()
    {
        final int tailValue = 64;

        when(metaDataBuffer.getIntVolatile(TERM_TAIL_COUNTER_OFFSET)).thenReturn(tailValue);

        assertThat(logAppender.tailVolatile(), is(tailValue));
    }

    @Test
    public void shouldReportCurrentTailAtCapacity()
    {
        final int tailValue = TERM_BUFFER_CAPACITY + 64;

        when(metaDataBuffer.getIntVolatile(TERM_TAIL_COUNTER_OFFSET)).thenReturn(tailValue);
        when(metaDataBuffer.getInt(TERM_TAIL_COUNTER_OFFSET)).thenReturn(tailValue);

        assertThat(logAppender.tailVolatile(), is(TERM_BUFFER_CAPACITY));
        assertThat(logAppender.tail(), is(TERM_BUFFER_CAPACITY));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenMaxMessageLengthExceeded()
    {
        final int maxMessageLength = logAppender.maxMessageLength();
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[1024]);

        logAppender.append(srcBuffer, 0, maxMessageLength + 1);
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

        assertThat(logAppender.append(buffer, 0, msgLength), is(SUCCEEDED));

        final InOrder inOrder = inOrder(termBuffer, metaDataBuffer);
        inOrder.verify(metaDataBuffer, times(1)).getAndAddInt(TERM_TAIL_COUNTER_OFFSET, alignedFrameLength);
        inOrder.verify(termBuffer, times(1)).putBytes(tail, DEFAULT_HEADER, 0, headerLength);
        inOrder.verify(termBuffer, times(1)).putBytes(headerLength, buffer, 0, msgLength);
        inOrder.verify(termBuffer, times(1)).putByte(flagsOffset(tail), UNFRAGMENTED);
        inOrder.verify(termBuffer, times(1)).putInt(termOffsetOffset(tail), tail, LITTLE_ENDIAN);
        inOrder.verify(termBuffer, times(1)).putIntOrdered(lengthOffset(tail), frameLength);
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

        assertThat(logAppender.append(buffer, 0, msgLength), is(SUCCEEDED));
        assertThat(logAppender.append(buffer, 0, msgLength), is(SUCCEEDED));

        final InOrder inOrder = inOrder(termBuffer, metaDataBuffer);
        inOrder.verify(metaDataBuffer, times(1)).getAndAddInt(TERM_TAIL_COUNTER_OFFSET, alignedFrameLength);
        inOrder.verify(termBuffer, times(1)).putBytes(tail, DEFAULT_HEADER, 0, headerLength);
        inOrder.verify(termBuffer, times(1)).putBytes(headerLength, buffer, 0, msgLength);
        inOrder.verify(termBuffer, times(1)).putByte(flagsOffset(tail), UNFRAGMENTED);
        inOrder.verify(termBuffer, times(1)).putInt(termOffsetOffset(tail), tail, LITTLE_ENDIAN);
        inOrder.verify(termBuffer, times(1)).putIntOrdered(lengthOffset(tail), frameLength);

        tail = alignedFrameLength;
        inOrder.verify(metaDataBuffer, times(1)).getAndAddInt(TERM_TAIL_COUNTER_OFFSET, alignedFrameLength);
        inOrder.verify(termBuffer, times(1)).putBytes(tail, DEFAULT_HEADER, 0, headerLength);
        inOrder.verify(termBuffer, times(1)).putBytes(tail + headerLength, buffer, 0, msgLength);
        inOrder.verify(termBuffer, times(1)).putByte(flagsOffset(tail), UNFRAGMENTED);
        inOrder.verify(termBuffer, times(1)).putInt(termOffsetOffset(tail), tail, LITTLE_ENDIAN);
        inOrder.verify(termBuffer, times(1)).putIntOrdered(lengthOffset(tail), frameLength);
    }

    @Test
    public void shouldTripWhenAppendingToLogAtCapacity()
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[128]);
        final int headerLength = DEFAULT_HEADER.capacity();
        final int msgLength = 20;
        final int frameLength = msgLength + headerLength;
        final int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);

        when(metaDataBuffer.getAndAddInt(TERM_TAIL_COUNTER_OFFSET, alignedFrameLength))
            .thenReturn(TERM_BUFFER_CAPACITY);

        assertThat(logAppender.append(buffer, 0, msgLength), is(TRIPPED));

        verify(metaDataBuffer, times(1)).getAndAddInt(TERM_TAIL_COUNTER_OFFSET, alignedFrameLength);
        verify(termBuffer, atLeastOnce()).capacity();
        verifyNoMoreInteractions(termBuffer);
    }

    @Test
    public void shouldFailWhenTheLogIsAlreadyTripped()
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[128]);
        final int headerLength = DEFAULT_HEADER.capacity();
        final int msgLength = 20;
        final int frameLength = msgLength + headerLength;
        final int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);

        when(metaDataBuffer.getAndAddInt(TERM_TAIL_COUNTER_OFFSET, alignedFrameLength))
            .thenReturn(TERM_BUFFER_CAPACITY)
            .thenReturn(TERM_BUFFER_CAPACITY + alignedFrameLength);

        assertThat(logAppender.append(buffer, 0, msgLength), is(TRIPPED));

        assertThat(logAppender.append(buffer, 0, msgLength), is(FAILED));

        verify(termBuffer, never()).putBytes(anyInt(), eq(buffer), eq(0), eq(msgLength));
    }

    @Test
    public void shouldPadLogAndTripWhenAppendingWithInsufficientRemainingCapacity()
    {
        final int msgLength = 120;
        final int headerLength = DEFAULT_HEADER.capacity();
        final int requiredFrameSize = align(headerLength + msgLength, FRAME_ALIGNMENT);
        final int tailValue = logAppender.termBuffer().capacity() - align(msgLength, FRAME_ALIGNMENT);
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[128]);

        when(metaDataBuffer.getAndAddInt(TERM_TAIL_COUNTER_OFFSET, requiredFrameSize))
            .thenReturn(tailValue);

        assertThat(logAppender.append(buffer, 0, msgLength), is(TRIPPED));

        final InOrder inOrder = inOrder(termBuffer, metaDataBuffer);
        inOrder.verify(metaDataBuffer, times(1)).getAndAddInt(TERM_TAIL_COUNTER_OFFSET, requiredFrameSize);
        inOrder.verify(termBuffer, times(1)).putBytes(tailValue, DEFAULT_HEADER, 0, headerLength);
        inOrder.verify(termBuffer, times(1)).putShort(typeOffset(tailValue), (short)PADDING_FRAME_TYPE, LITTLE_ENDIAN);
        inOrder.verify(termBuffer, times(1)).putByte(flagsOffset(tailValue), UNFRAGMENTED);
        inOrder.verify(termBuffer, times(1)).putInt(termOffsetOffset(tailValue), tailValue, LITTLE_ENDIAN);
        inOrder.verify(termBuffer, times(1)).putIntOrdered(lengthOffset(tailValue), TERM_BUFFER_CAPACITY - tailValue);
    }

    @Test
    public void shouldPadLogAndTripWhenAppendingWithInsufficientRemainingCapacityIncludingHeader()
    {
        final int headerLength = DEFAULT_HEADER.capacity();
        final int msgLength = 120;
        final int requiredFrameSize = align(headerLength + msgLength, FRAME_ALIGNMENT);
        final int tailValue = logAppender.termBuffer().capacity() - (requiredFrameSize + (headerLength - FRAME_ALIGNMENT));
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[128]);

        when(metaDataBuffer.getAndAddInt(TERM_TAIL_COUNTER_OFFSET, requiredFrameSize))
            .thenReturn(tailValue);

        assertThat(logAppender.append(buffer, 0, msgLength), is(TRIPPED));

        final InOrder inOrder = inOrder(termBuffer, metaDataBuffer);
        inOrder.verify(metaDataBuffer, times(1)).getAndAddInt(TERM_TAIL_COUNTER_OFFSET, requiredFrameSize);
        inOrder.verify(termBuffer, times(1)).putBytes(tailValue, DEFAULT_HEADER, 0, headerLength);
        inOrder.verify(termBuffer, times(1)).putShort(typeOffset(tailValue), (short)PADDING_FRAME_TYPE, LITTLE_ENDIAN);
        inOrder.verify(termBuffer, times(1)).putByte(flagsOffset(tailValue), UNFRAGMENTED);
        inOrder.verify(termBuffer, times(1)).putInt(termOffsetOffset(tailValue), tailValue, LITTLE_ENDIAN);
        inOrder.verify(termBuffer, times(1)).putIntOrdered(lengthOffset(tailValue), TERM_BUFFER_CAPACITY - tailValue);
    }

    @Test
    public void shouldFragmentMessageOverTwoFrames()
    {
        final int msgLength = logAppender.maxPayloadLength() + 1;
        final int headerLength = DEFAULT_HEADER.capacity();
        final int frameLength = headerLength + 1;
        final int requiredCapacity = align(headerLength + 1, FRAME_ALIGNMENT) + logAppender.maxFrameLength();
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[msgLength]);

        when(metaDataBuffer.getAndAddInt(TERM_TAIL_COUNTER_OFFSET, requiredCapacity))
            .thenReturn(0);

        assertThat(logAppender.append(buffer, 0, msgLength), is(SUCCEEDED));

        int tail  = 0;
        final InOrder inOrder = inOrder(termBuffer, metaDataBuffer);
        inOrder.verify(metaDataBuffer, times(1)).getAndAddInt(TERM_TAIL_COUNTER_OFFSET, requiredCapacity);

        inOrder.verify(termBuffer, times(1)).putBytes(tail, DEFAULT_HEADER, 0, headerLength);
        inOrder.verify(termBuffer, times(1)).putBytes(tail + headerLength, buffer, 0, logAppender.maxPayloadLength());
        inOrder.verify(termBuffer, times(1)).putByte(flagsOffset(tail), BEGIN_FRAG);
        inOrder.verify(termBuffer, times(1)).putInt(termOffsetOffset(tail), tail, LITTLE_ENDIAN);
        inOrder.verify(termBuffer, times(1)).putIntOrdered(lengthOffset(tail), logAppender.maxFrameLength());

        tail = logAppender.maxFrameLength();
        inOrder.verify(termBuffer, times(1)).putBytes(tail, DEFAULT_HEADER, 0, headerLength);
        inOrder.verify(termBuffer, times(1)).putBytes(tail + headerLength, buffer, logAppender.maxPayloadLength(), 1);
        inOrder.verify(termBuffer, times(1)).putByte(flagsOffset(tail), END_FRAG);
        inOrder.verify(termBuffer, times(1)).putInt(termOffsetOffset(tail), tail, LITTLE_ENDIAN);
        inOrder.verify(termBuffer, times(1)).putIntOrdered(lengthOffset(tail), frameLength);
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

        assertThat(logAppender.claim(msgLength, bufferClaim), is(SUCCEEDED));

        assertThat(bufferClaim.buffer(), is(termBuffer));
        assertThat(bufferClaim.offset(), is(tail + headerLength));
        assertThat(bufferClaim.length(), is(msgLength));

        // Map flyweight or encode to buffer directly then call commit() when done
        bufferClaim.commit();

        final InOrder inOrder = inOrder(termBuffer, metaDataBuffer);
        inOrder.verify(metaDataBuffer, times(1)).getAndAddInt(TERM_TAIL_COUNTER_OFFSET, alignedFrameLength);
        inOrder.verify(termBuffer, times(1)).putBytes(tail, DEFAULT_HEADER, 0, headerLength);
        inOrder.verify(termBuffer, times(1)).putByte(flagsOffset(tail), UNFRAGMENTED);
        inOrder.verify(termBuffer, times(1)).putInt(termOffsetOffset(tail), tail, LITTLE_ENDIAN);
        inOrder.verify(termBuffer, times(1)).putIntOrdered(lengthOffset(tail), frameLength);
    }
}
