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

import io.aeron.DirectBufferVector;
import io.aeron.ReservedValueSupplier;
import io.aeron.exceptions.AeronException;
import org.agrona.BitUtil;
import org.junit.Test;
import org.mockito.InOrder;
import org.agrona.concurrent.UnsafeBuffer;

import static io.aeron.logbuffer.LogBufferDescriptor.TERM_TAIL_COUNTERS_OFFSET;
import static io.aeron.logbuffer.LogBufferDescriptor.packTail;
import static io.aeron.logbuffer.LogBufferDescriptor.rawTailVolatile;
import static io.aeron.logbuffer.TermAppender.FAILED;
import static io.aeron.protocol.DataHeaderFlyweight.RESERVED_VALUE_OFFSET;
import static io.aeron.protocol.DataHeaderFlyweight.createDefaultHeader;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;
import static io.aeron.logbuffer.FrameDescriptor.*;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static org.agrona.BitUtil.*;

public class TermAppenderTest
{
    private static final int TERM_BUFFER_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int META_DATA_BUFFER_LENGTH = LogBufferDescriptor.LOG_META_DATA_LENGTH;
    private static final int MAX_FRAME_LENGTH = 1024;
    private static final int MAX_PAYLOAD_LENGTH = MAX_FRAME_LENGTH - HEADER_LENGTH;
    private static final int PARTITION_INDEX = 0;
    private static final int TERM_TAIL_COUNTER_OFFSET = TERM_TAIL_COUNTERS_OFFSET + (PARTITION_INDEX * SIZE_OF_LONG);
    private static final int TERM_ID = 7;
    private static final long RV = 7777L;
    private static final ReservedValueSupplier RVS = (termBuffer, termOffset, frameLength) -> RV;
    private static final UnsafeBuffer DEFAULT_HEADER = new UnsafeBuffer(allocateDirect(HEADER_LENGTH));

    private final UnsafeBuffer termBuffer = spy(new UnsafeBuffer(allocateDirect(TERM_BUFFER_LENGTH)));
    private final UnsafeBuffer logMetaDataBuffer = new UnsafeBuffer(allocateDirect(META_DATA_BUFFER_LENGTH));
    private final HeaderWriter headerWriter = spy(new HeaderWriter(createDefaultHeader(0, 0, TERM_ID)));

    private final TermAppender termAppender = new TermAppender(termBuffer, logMetaDataBuffer, PARTITION_INDEX);

    @Test
    public void shouldAppendFrameToEmptyLog()
    {
        final int headerLength = DEFAULT_HEADER.capacity();
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[128]);
        final int msgLength = 20;
        final int frameLength = msgLength + headerLength;
        final int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);
        final int tail = 0;

        logMetaDataBuffer.putLong(TERM_TAIL_COUNTER_OFFSET, packTail(TERM_ID, tail));

        assertThat(termAppender.appendUnfragmentedMessage(headerWriter, buffer, 0, msgLength, RVS, TERM_ID),
            is(alignedFrameLength));

        assertThat(rawTailVolatile(logMetaDataBuffer, PARTITION_INDEX),
            is(packTail(TERM_ID, tail + alignedFrameLength)));

        final InOrder inOrder = inOrder(termBuffer, headerWriter);
        inOrder.verify(headerWriter, times(1)).write(termBuffer, tail, frameLength, TERM_ID);
        inOrder.verify(termBuffer, times(1)).putBytes(headerLength, buffer, 0, msgLength);
        inOrder.verify(termBuffer, times(1)).putLong(tail + RESERVED_VALUE_OFFSET, RV, LITTLE_ENDIAN);
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

        logMetaDataBuffer.putLong(TERM_TAIL_COUNTER_OFFSET, packTail(TERM_ID, tail));

        assertThat(termAppender.appendUnfragmentedMessage(
            headerWriter, buffer, 0, msgLength, RVS, TERM_ID), is(alignedFrameLength));
        assertThat(termAppender.appendUnfragmentedMessage(
            headerWriter, buffer, 0, msgLength, RVS, TERM_ID), is(alignedFrameLength * 2));

        assertThat(rawTailVolatile(logMetaDataBuffer, PARTITION_INDEX),
            is(packTail(TERM_ID, tail + (alignedFrameLength * 2))));

        final InOrder inOrder = inOrder(termBuffer, headerWriter);
        inOrder.verify(headerWriter, times(1)).write(termBuffer, tail, frameLength, TERM_ID);
        inOrder.verify(termBuffer, times(1)).putBytes(headerLength, buffer, 0, msgLength);
        inOrder.verify(termBuffer, times(1)).putLong(tail + RESERVED_VALUE_OFFSET, RV, LITTLE_ENDIAN);
        inOrder.verify(termBuffer, times(1)).putIntOrdered(tail, frameLength);

        tail = alignedFrameLength;
        inOrder.verify(headerWriter, times(1)).write(termBuffer, tail, frameLength, TERM_ID);
        inOrder.verify(termBuffer, times(1))
            .putBytes(tail + headerLength, buffer, 0, msgLength);
        inOrder.verify(termBuffer, times(1)).putLong(tail + RESERVED_VALUE_OFFSET, RV, LITTLE_ENDIAN);
        inOrder.verify(termBuffer, times(1)).putIntOrdered(tail, frameLength);
    }

    @Test
    public void shouldPadLogWhenAppendingWithInsufficientRemainingCapacity()
    {
        final int msgLength = 120;
        final int headerLength = DEFAULT_HEADER.capacity();
        final int requiredFrameSize = align(headerLength + msgLength, FRAME_ALIGNMENT);
        final int tailValue = TERM_BUFFER_LENGTH - align(msgLength, FRAME_ALIGNMENT);
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[128]);
        final int frameLength = TERM_BUFFER_LENGTH - tailValue;

        logMetaDataBuffer.putLong(TERM_TAIL_COUNTER_OFFSET, packTail(TERM_ID, tailValue));

        assertThat(termAppender.appendUnfragmentedMessage(headerWriter, buffer, 0, msgLength, RVS, TERM_ID),
            is(FAILED));

        assertThat(rawTailVolatile(logMetaDataBuffer, PARTITION_INDEX),
            is(packTail(TERM_ID, tailValue + requiredFrameSize)));

        final InOrder inOrder = inOrder(termBuffer, headerWriter);
        inOrder.verify(headerWriter, times(1)).write(termBuffer, tailValue, frameLength, TERM_ID);
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
        int tail = 0;

        logMetaDataBuffer.putLong(TERM_TAIL_COUNTER_OFFSET, packTail(TERM_ID, tail));

        assertThat(termAppender.appendFragmentedMessage(
            headerWriter, buffer, 0, msgLength, MAX_PAYLOAD_LENGTH, RVS, TERM_ID), is(requiredCapacity));

        assertThat(rawTailVolatile(logMetaDataBuffer, PARTITION_INDEX),
            is(packTail(TERM_ID, tail + requiredCapacity)));

        final InOrder inOrder = inOrder(termBuffer, headerWriter);
        inOrder.verify(headerWriter, times(1)).write(termBuffer, tail, MAX_FRAME_LENGTH, TERM_ID);
        inOrder.verify(termBuffer, times(1)).putBytes(tail + headerLength, buffer, 0, MAX_PAYLOAD_LENGTH);
        inOrder.verify(termBuffer, times(1)).putByte(flagsOffset(tail), BEGIN_FRAG_FLAG);
        inOrder.verify(termBuffer, times(1)).putLong(tail + RESERVED_VALUE_OFFSET, RV, LITTLE_ENDIAN);
        inOrder.verify(termBuffer, times(1)).putIntOrdered(tail, MAX_FRAME_LENGTH);

        tail = MAX_FRAME_LENGTH;
        inOrder.verify(headerWriter, times(1)).write(termBuffer, tail, frameLength, TERM_ID);
        inOrder.verify(termBuffer, times(1)).putBytes(tail + headerLength, buffer, MAX_PAYLOAD_LENGTH, 1);
        inOrder.verify(termBuffer, times(1)).putByte(flagsOffset(tail), END_FRAG_FLAG);
        inOrder.verify(termBuffer, times(1)).putLong(tail + RESERVED_VALUE_OFFSET, RV, LITTLE_ENDIAN);
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

        logMetaDataBuffer.putLong(TERM_TAIL_COUNTER_OFFSET, packTail(TERM_ID, tail));

        assertThat(termAppender.claim(headerWriter, msgLength, bufferClaim, TERM_ID), is(alignedFrameLength));

        assertThat(bufferClaim.offset(), is(tail + headerLength));
        assertThat(bufferClaim.length(), is(msgLength));

        assertThat(rawTailVolatile(logMetaDataBuffer, PARTITION_INDEX),
            is(packTail(TERM_ID, tail + alignedFrameLength)));

        // Map flyweight or encode to buffer directly then call commit() when done
        bufferClaim.commit();

        final InOrder inOrder = inOrder(headerWriter);
        inOrder.verify(headerWriter, times(1)).write(termBuffer, tail, frameLength, TERM_ID);
    }

    @Test
    public void shouldAppendUnfragmentedFromVectorsToEmptyLog()
    {
        final int headerLength = DEFAULT_HEADER.capacity();
        final UnsafeBuffer bufferOne = new UnsafeBuffer(new byte[64]);
        final UnsafeBuffer bufferTwo = new UnsafeBuffer(new byte[256]);
        bufferOne.setMemory(0, bufferOne.capacity(), (byte)'1');
        bufferTwo.setMemory(0, bufferTwo.capacity(), (byte)'2');
        final int msgLength = bufferOne.capacity() + 200;
        final int frameLength = msgLength + headerLength;
        final int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);
        final int tail = 0;

        logMetaDataBuffer.putLong(TERM_TAIL_COUNTER_OFFSET, packTail(TERM_ID, tail));

        final DirectBufferVector[] vectors = new DirectBufferVector[]
        {
            new DirectBufferVector(bufferOne, 0, bufferOne.capacity()),
            new DirectBufferVector(bufferTwo, 0, 200)
        };

        assertThat(termAppender.appendUnfragmentedMessage(headerWriter, vectors, msgLength, RVS, TERM_ID),
            is(alignedFrameLength));

        assertThat(rawTailVolatile(logMetaDataBuffer, PARTITION_INDEX),
            is(packTail(TERM_ID, tail + alignedFrameLength)));

        final InOrder inOrder = inOrder(termBuffer, headerWriter);
        inOrder.verify(headerWriter, times(1)).write(termBuffer, tail, frameLength, TERM_ID);
        inOrder.verify(termBuffer, times(1)).putBytes(headerLength, bufferOne, 0, bufferOne.capacity());
        inOrder.verify(termBuffer, times(1)).putBytes(headerLength + bufferOne.capacity(), bufferTwo, 0, 200);
        inOrder.verify(termBuffer, times(1)).putLong(tail + RESERVED_VALUE_OFFSET, RV, LITTLE_ENDIAN);
        inOrder.verify(termBuffer, times(1)).putIntOrdered(tail, frameLength);
    }

    @Test
    public void shouldAppendFragmentedFromVectorsToEmptyLog()
    {
        final int mtu = 2048;
        final int headerLength = DEFAULT_HEADER.capacity();
        final int maxPayloadLength = mtu - headerLength;
        final int bufferOneLength = 64;
        final int bufferTwoLength = 3000;
        final UnsafeBuffer bufferOne = new UnsafeBuffer(new byte[bufferOneLength]);
        final UnsafeBuffer bufferTwo = new UnsafeBuffer(new byte[bufferTwoLength]);
        bufferOne.setMemory(0, bufferOne.capacity(), (byte)'1');
        bufferTwo.setMemory(0, bufferTwo.capacity(), (byte)'2');
        final int msgLength = bufferOneLength + bufferTwoLength;
        int tail = 0;
        final int frameOneLength = mtu;
        final int frameTwoLength = (msgLength - (mtu - headerLength)) + headerLength;
        final int resultingOffset = frameOneLength + BitUtil.align(frameTwoLength, FRAME_ALIGNMENT);

        logMetaDataBuffer.putLong(TERM_TAIL_COUNTER_OFFSET, packTail(TERM_ID, tail));

        final DirectBufferVector[] vectors = new DirectBufferVector[]
        {
            new DirectBufferVector(bufferOne, 0, bufferOneLength),
            new DirectBufferVector(bufferTwo, 0, bufferTwoLength)
        };

        assertThat(termAppender.appendFragmentedMessage(
            headerWriter, vectors, msgLength, maxPayloadLength, RVS, TERM_ID), is(resultingOffset));

        final InOrder inOrder = inOrder(termBuffer, headerWriter);

        inOrder.verify(headerWriter, times(1)).write(termBuffer, tail, frameOneLength, TERM_ID);
        inOrder.verify(termBuffer, times(1)).putBytes(headerLength, bufferOne, 0, bufferOneLength);
        inOrder.verify(termBuffer, times(1))
            .putBytes(headerLength + bufferOneLength, bufferTwo, 0, maxPayloadLength - bufferOneLength);
        inOrder.verify(termBuffer, times(1)).putLong(tail + RESERVED_VALUE_OFFSET, RV, LITTLE_ENDIAN);
        inOrder.verify(termBuffer, times(1)).putIntOrdered(tail, frameOneLength);

        tail += frameOneLength;
        final int bufferTwoOffset = maxPayloadLength - bufferOneLength;
        final int fragmentTwoPayloadLength = bufferTwoLength - (maxPayloadLength - bufferOneLength);

        inOrder.verify(headerWriter, times(1)).write(termBuffer, tail, frameTwoLength, TERM_ID);
        inOrder.verify(termBuffer, times(1))
            .putBytes(tail + headerLength, bufferTwo, bufferTwoOffset, fragmentTwoPayloadLength);
        inOrder.verify(termBuffer, times(1)).putLong(tail + RESERVED_VALUE_OFFSET, RV, LITTLE_ENDIAN);
        inOrder.verify(termBuffer, times(1)).putIntOrdered(tail, frameTwoLength);
    }

    @Test
    public void shouldAppendFragmentedFromVectorsWithNonZeroOffsetToEmptyLog()
    {
        final int mtu = 2048;
        final int headerLength = DEFAULT_HEADER.capacity();
        final int maxPayloadLength = mtu - headerLength;
        final int bufferOneLength = 64;
        final int offset = 15;
        final int bufferTwoTotalLength = 3000;
        final int bufferTwoLength = bufferTwoTotalLength - offset;
        final UnsafeBuffer bufferOne = new UnsafeBuffer(new byte[bufferOneLength]);
        final UnsafeBuffer bufferTwo = new UnsafeBuffer(new byte[bufferTwoTotalLength]);
        bufferOne.setMemory(0, bufferOne.capacity(), (byte)'1');
        bufferTwo.setMemory(0, bufferTwo.capacity(), (byte)'2');
        final int msgLength = bufferOneLength + bufferTwoLength;
        int tail = 0;
        final int frameOneLength = mtu;
        final int frameTwoLength = (msgLength - (mtu - headerLength)) + headerLength;
        final int resultingOffset = frameOneLength + BitUtil.align(frameTwoLength, FRAME_ALIGNMENT);

        logMetaDataBuffer.putLong(TERM_TAIL_COUNTER_OFFSET, packTail(TERM_ID, tail));

        final DirectBufferVector[] vectors = new DirectBufferVector[]
        {
            new DirectBufferVector(bufferOne, 0, bufferOneLength),
            new DirectBufferVector(bufferTwo, offset, bufferTwoLength)
        };

        assertThat(termAppender.appendFragmentedMessage(
            headerWriter, vectors, msgLength, maxPayloadLength, RVS, TERM_ID), is(resultingOffset));

        final InOrder inOrder = inOrder(termBuffer, headerWriter);

        inOrder.verify(headerWriter, times(1)).write(termBuffer, tail, frameOneLength, TERM_ID);
        inOrder.verify(termBuffer, times(1)).putBytes(headerLength, bufferOne, 0, bufferOneLength);
        inOrder.verify(termBuffer, times(1))
            .putBytes(headerLength + bufferOneLength, bufferTwo, offset, maxPayloadLength - bufferOneLength);
        inOrder.verify(termBuffer, times(1)).putLong(tail + RESERVED_VALUE_OFFSET, RV, LITTLE_ENDIAN);
        inOrder.verify(termBuffer, times(1)).putIntOrdered(tail, frameOneLength);

        tail += frameOneLength;
        final int bufferTwoOffset = maxPayloadLength - bufferOneLength + offset;
        final int fragmentTwoPayloadLength = bufferTwoLength - (maxPayloadLength - bufferOneLength);

        inOrder.verify(headerWriter, times(1)).write(termBuffer, tail, frameTwoLength, TERM_ID);
        inOrder.verify(termBuffer, times(1))
            .putBytes(tail + headerLength, bufferTwo, bufferTwoOffset, fragmentTwoPayloadLength);
        inOrder.verify(termBuffer, times(1)).putLong(tail + RESERVED_VALUE_OFFSET, RV, LITTLE_ENDIAN);
        inOrder.verify(termBuffer, times(1)).putIntOrdered(tail, frameTwoLength);
    }

    @Test(expected = AeronException.class)
    public void shouldDetectInvalidTerm()
    {
        final int length = 128;
        final int srcOffset = 0;
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[length]);

        logMetaDataBuffer.putLong(TERM_TAIL_COUNTER_OFFSET, packTail(TERM_ID + 1, 0));

        termAppender.appendUnfragmentedMessage(headerWriter, buffer, srcOffset, length, RVS, TERM_ID);
    }
}
