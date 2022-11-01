/*
 * Copyright 2014-2022 Real Logic Limited.
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

import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.HeaderWriter;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.logbuffer.TermRebuilder;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.HeaderFlyweight;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InOrder;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;

import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.logbuffer.FrameDescriptor.frameLengthOrdered;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static java.util.Arrays.asList;
import static org.agrona.BitUtil.align;
import static org.mockito.Mockito.*;

class RetransmitHandlerTest
{
    private static final int MTU_LENGTH = 1024;
    private static final int TERM_BUFFER_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final byte[] DATA = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    private static final int MESSAGE_LENGTH = DataHeaderFlyweight.HEADER_LENGTH + DATA.length;
    private static final int ALIGNED_FRAME_LENGTH = align(MESSAGE_LENGTH, FrameDescriptor.FRAME_ALIGNMENT);
    private static final int SESSION_ID = 0x5E55101D;
    private static final int STREAM_ID = 0x5400E;
    private static final int TERM_ID = 0x7F003355;

    private static final FeedbackDelayGenerator DELAY_GENERATOR =
        new StaticDelayGenerator(TimeUnit.MILLISECONDS.toNanos(20), false);
    private static final FeedbackDelayGenerator ZERO_DELAY_GENERATOR =
        new StaticDelayGenerator(TimeUnit.MILLISECONDS.toNanos(0), false);
    private static final FeedbackDelayGenerator LINGER_GENERATOR =
        new StaticDelayGenerator(TimeUnit.MILLISECONDS.toNanos(40), false);

    private final UnsafeBuffer termBuffer = new UnsafeBuffer(new byte[TERM_BUFFER_LENGTH]);
    private final UnsafeBuffer metaDataBuffer = new UnsafeBuffer(new byte[LogBufferDescriptor.LOG_META_DATA_LENGTH]);

    private final UnsafeBuffer rcvBuffer = new UnsafeBuffer(new byte[MESSAGE_LENGTH]);
    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();

    private long currentTime = 0;

    private final RetransmitSender retransmitSender = mock(RetransmitSender.class);
    private final AtomicCounter invalidPackets = mock(AtomicCounter.class);

    private final HeaderWriter headerWriter = HeaderWriter.newInstance(
        DataHeaderFlyweight.createDefaultHeader(0, 0, 0));

    private RetransmitHandler handler = new RetransmitHandler(
        () -> currentTime, invalidPackets, DELAY_GENERATOR, LINGER_GENERATOR);

    @BeforeEach
    void before()
    {
        LogBufferDescriptor.rawTail(metaDataBuffer, 0, LogBufferDescriptor.packTail(TERM_ID, 0));
    }

    private static List<BiConsumer<RetransmitHandlerTest, Integer>> consumers()
    {
        return asList(
            (retransmitHandlerTest, i) -> retransmitHandlerTest.addSentDataFrame(),
            RetransmitHandlerTest::addReceivedDataFrame);
    }

    @ParameterizedTest
    @MethodSource("consumers")
    void shouldRetransmitOnNak(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 5);
        handler.onNak(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH, TERM_BUFFER_LENGTH, retransmitSender);
        currentTime = TimeUnit.MILLISECONDS.toNanos(100);
        handler.processTimeouts(currentTime, retransmitSender);

        verify(retransmitSender).resend(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH);
    }

    @ParameterizedTest
    @MethodSource("consumers")
    void shouldNotRetransmitOnNakWhileInLinger(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 5);
        handler.onNak(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH, TERM_BUFFER_LENGTH, retransmitSender);
        currentTime = TimeUnit.MILLISECONDS.toNanos(40);
        handler.processTimeouts(currentTime, retransmitSender);
        handler.onNak(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH, TERM_BUFFER_LENGTH, retransmitSender);
        currentTime = TimeUnit.MILLISECONDS.toNanos(100);
        handler.processTimeouts(currentTime, retransmitSender);

        verify(retransmitSender).resend(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH);
    }

    @ParameterizedTest
    @MethodSource("consumers")
    void shouldRetransmitOnNakAfterLinger(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 5);
        handler.onNak(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH, TERM_BUFFER_LENGTH, retransmitSender);
        currentTime = TimeUnit.MILLISECONDS.toNanos(40);
        handler.processTimeouts(currentTime, retransmitSender);
        currentTime = TimeUnit.MILLISECONDS.toNanos(100);
        handler.processTimeouts(currentTime, retransmitSender);
        handler.onNak(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH, TERM_BUFFER_LENGTH, retransmitSender);
        currentTime = TimeUnit.MILLISECONDS.toNanos(200);
        handler.processTimeouts(currentTime, retransmitSender);

        verify(retransmitSender, times(2)).resend(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH);
    }

    @ParameterizedTest
    @MethodSource("consumers")
    void shouldRetransmitOnMultipleNaks(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 5);
        handler.onNak(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH, TERM_BUFFER_LENGTH, retransmitSender);
        handler.onNak(TERM_ID, offsetOfFrame(1), ALIGNED_FRAME_LENGTH, TERM_BUFFER_LENGTH, retransmitSender);
        currentTime = TimeUnit.MILLISECONDS.toNanos(100);
        handler.processTimeouts(currentTime, retransmitSender);

        final InOrder inOrder = inOrder(retransmitSender);
        inOrder.verify(retransmitSender).resend(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH);
        inOrder.verify(retransmitSender).resend(TERM_ID, offsetOfFrame(1), ALIGNED_FRAME_LENGTH);
    }

    @ParameterizedTest
    @MethodSource("consumers")
    void shouldRetransmitOnNakOverMessageLength(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 10);
        handler.onNak(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH * 5, TERM_BUFFER_LENGTH, retransmitSender);
        currentTime = TimeUnit.MILLISECONDS.toNanos(100);
        handler.processTimeouts(currentTime, retransmitSender);

        verify(retransmitSender).resend(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH * 5);
    }

    @ParameterizedTest
    @MethodSource("consumers")
    void shouldRetransmitOnNakOverMtuLength(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        final int numFramesPerMtu = MTU_LENGTH / ALIGNED_FRAME_LENGTH;
        createTermBuffer(creator, numFramesPerMtu * 5);
        handler.onNak(TERM_ID, offsetOfFrame(0), MTU_LENGTH * 2, TERM_BUFFER_LENGTH, retransmitSender);
        currentTime = TimeUnit.MILLISECONDS.toNanos(100);
        handler.processTimeouts(currentTime, retransmitSender);

        verify(retransmitSender).resend(TERM_ID, offsetOfFrame(0), MTU_LENGTH * 2);
    }

    @ParameterizedTest
    @MethodSource("consumers")
    void shouldStopRetransmitOnRetransmitReception(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 5);
        handler.onNak(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH, TERM_BUFFER_LENGTH, retransmitSender);
        handler.onRetransmitReceived(TERM_ID, offsetOfFrame(0));
        currentTime = TimeUnit.MILLISECONDS.toNanos(100);
        handler.processTimeouts(currentTime, retransmitSender);

        verifyNoInteractions(retransmitSender);
    }

    @ParameterizedTest
    @MethodSource("consumers")
    void shouldStopOneRetransmitOnRetransmitReception(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 5);
        handler.onNak(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH, TERM_BUFFER_LENGTH, retransmitSender);
        handler.onNak(TERM_ID, offsetOfFrame(1), ALIGNED_FRAME_LENGTH, TERM_BUFFER_LENGTH, retransmitSender);
        handler.onRetransmitReceived(TERM_ID, offsetOfFrame(0));
        currentTime = TimeUnit.MILLISECONDS.toNanos(100);
        handler.processTimeouts(currentTime, retransmitSender);

        verify(retransmitSender).resend(TERM_ID, offsetOfFrame(1), ALIGNED_FRAME_LENGTH);
    }

    @ParameterizedTest
    @MethodSource("consumers")
    void shouldImmediateRetransmitOnNak(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 5);
        handler = newZeroDelayRetransmitHandler();

        handler.onNak(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH, TERM_BUFFER_LENGTH, retransmitSender);

        verify(retransmitSender).resend(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH);
    }

    @ParameterizedTest
    @MethodSource("consumers")
    void shouldGoIntoLingerOnImmediateRetransmit(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 5);
        handler = newZeroDelayRetransmitHandler();

        handler.onNak(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH, TERM_BUFFER_LENGTH, retransmitSender);
        currentTime = TimeUnit.MILLISECONDS.toNanos(40);
        handler.processTimeouts(currentTime, retransmitSender);
        handler.onNak(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH, TERM_BUFFER_LENGTH, retransmitSender);

        verify(retransmitSender).resend(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH);
    }

    @ParameterizedTest
    @MethodSource("consumers")
    void shouldOnlyRetransmitOnNakWhenConfiguredTo(final BiConsumer<RetransmitHandlerTest, Integer> creator)
    {
        createTermBuffer(creator, 5);
        handler.onNak(TERM_ID, offsetOfFrame(0), ALIGNED_FRAME_LENGTH, TERM_BUFFER_LENGTH, retransmitSender);

        verifyNoInteractions(retransmitSender);
    }

    private RetransmitHandler newZeroDelayRetransmitHandler()
    {
        return new RetransmitHandler(() -> currentTime, invalidPackets, ZERO_DELAY_GENERATOR, LINGER_GENERATOR);
    }

    private void createTermBuffer(final BiConsumer<RetransmitHandlerTest, Integer> creator, final int num)
    {
        IntStream.range(0, num).forEach((i) -> creator.accept(this, i));
    }

    private static int offsetOfFrame(final int index)
    {
        return index * ALIGNED_FRAME_LENGTH;
    }

    private void addSentDataFrame()
    {
        rcvBuffer.putBytes(0, DATA);

        final int frameLength = DATA.length + HEADER_LENGTH;
        final int alignedLength = align(frameLength, FRAME_ALIGNMENT);
        final long rawTail = LogBufferDescriptor.packTail(TERM_ID, alignedLength);

        LogBufferDescriptor.rawTail(metaDataBuffer, 0, rawTail);

        headerWriter.write(termBuffer, 0, frameLength, TERM_ID);
        termBuffer.putBytes(HEADER_LENGTH, rcvBuffer, 0, DATA.length);

        frameLengthOrdered(termBuffer, 0, frameLength);
    }

    private void addReceivedDataFrame(final int msgNum)
    {
        dataHeader.wrap(rcvBuffer);

        dataHeader.termId(TERM_ID)
            .streamId(STREAM_ID)
            .sessionId(SESSION_ID)
            .termOffset(offsetOfFrame(msgNum))
            .frameLength(MESSAGE_LENGTH)
            .headerType(HeaderFlyweight.HDR_TYPE_DATA)
            .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
            .version(HeaderFlyweight.CURRENT_VERSION);

        rcvBuffer.putBytes(dataHeader.dataOffset(), DATA);

        TermRebuilder.insert(termBuffer, offsetOfFrame(msgNum), rcvBuffer, MESSAGE_LENGTH);
    }
}
