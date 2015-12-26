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
package uk.co.real_logic.aeron;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import uk.co.real_logic.aeron.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.protocol.HeaderFlyweight;
import uk.co.real_logic.aeron.logbuffer.*;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.ErrorHandler;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.concurrent.status.AtomicLongPosition;
import uk.co.real_logic.agrona.concurrent.status.Position;

import static java.nio.ByteBuffer.allocateDirect;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.*;
import static uk.co.real_logic.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static uk.co.real_logic.agrona.BitUtil.align;

public class ImageTest
{
    private static final int TERM_BUFFER_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int POSITION_BITS_TO_SHIFT = Integer.numberOfTrailingZeros(TERM_BUFFER_LENGTH);
    private static final byte[] DATA = new byte[36];

    static
    {
        for (int i = 0; i < DATA.length; i++)
        {
            DATA[i] = (byte)i;
        }
    }

    private static final long CORRELATION_ID = 0xC044E1AL;
    private static final int SESSION_ID = 0x5E55101D;
    private static final int STREAM_ID = 0xC400E;
    private static final String SOURCE_IDENTITY = "ipc";
    private static final int INITIAL_TERM_ID = 0xEE81D;
    private static final int MESSAGE_LENGTH = HEADER_LENGTH + DATA.length;
    private static final int ALIGNED_FRAME_LENGTH = align(MESSAGE_LENGTH, FrameDescriptor.FRAME_ALIGNMENT);

    private final UnsafeBuffer rcvBuffer = new UnsafeBuffer(allocateDirect(ALIGNED_FRAME_LENGTH));
    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();
    private final FragmentHandler mockFragmentHandler = mock(FragmentHandler.class);
    private final ControlledFragmentHandler mockControlledFragmentHandler = mock(ControlledFragmentHandler.class);
    private final Position position = spy(new AtomicLongPosition());
    private final LogBuffers logBuffers = mock(LogBuffers.class);
    private final ErrorHandler errorHandler = mock(ErrorHandler.class);
    private final Subscription subscription = mock(Subscription.class);

    private UnsafeBuffer[] atomicBuffers = new UnsafeBuffer[(PARTITION_COUNT * 2) + 1];
    private UnsafeBuffer[] termBuffers = new UnsafeBuffer[PARTITION_COUNT];

    @Before
    public void setUp()
    {
        dataHeader.wrap(rcvBuffer, 0);

        for (int i = 0; i < PARTITION_COUNT; i++)
        {
            atomicBuffers[i] = new UnsafeBuffer(allocateDirect(TERM_BUFFER_LENGTH));
            termBuffers[i] = atomicBuffers[i];

            atomicBuffers[i + PARTITION_COUNT] = new UnsafeBuffer(allocateDirect(TERM_META_DATA_LENGTH));
        }

        atomicBuffers[LOG_META_DATA_SECTION_INDEX] = new UnsafeBuffer(allocateDirect(LOG_META_DATA_LENGTH));

        when(logBuffers.atomicBuffers()).thenReturn(atomicBuffers);
        when(logBuffers.termLength()).thenReturn(TERM_BUFFER_LENGTH);
    }

    @Test
    public void shouldHandleClosedImage()
    {
        final Image image = createImage();

        image.managedResource();

        assertTrue(image.isClosed());
        assertThat(image.poll(mockFragmentHandler, Integer.MAX_VALUE), is(0));
    }

    @Test
    public void shouldReportCorrectPositionOnReception()
    {
        final long initialPosition = computePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
        position.setOrdered(initialPosition);
        final Image image = createImage();

        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(0));

        final int messages = image.poll(mockFragmentHandler, Integer.MAX_VALUE);
        assertThat(messages, is(1));

        verify(mockFragmentHandler).onFragment(
            any(UnsafeBuffer.class),
            eq(HEADER_LENGTH),
            eq(DATA.length),
            any(Header.class));

        final InOrder inOrder = Mockito.inOrder(position);
        inOrder.verify(position).setOrdered(initialPosition);
        inOrder.verify(position).setOrdered(initialPosition + ALIGNED_FRAME_LENGTH);
    }

    @Test
    public void shouldReportCorrectPositionOnReceptionWithNonZeroPositionInInitialTermId()
    {
        final int initialMessageIndex = 5;
        final int initialTermOffset = offsetForFrame(initialMessageIndex);
        final long initialPosition = computePosition(
            INITIAL_TERM_ID, initialTermOffset, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);

        position.setOrdered(initialPosition);
        final Image image = createImage();

        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(initialMessageIndex));

        final int messages = image.poll(mockFragmentHandler, Integer.MAX_VALUE);
        assertThat(messages, is(1));

        verify(mockFragmentHandler).onFragment(
            any(UnsafeBuffer.class),
            eq(initialTermOffset + HEADER_LENGTH),
            eq(DATA.length),
            any(Header.class));

        final InOrder inOrder = Mockito.inOrder(position);
        inOrder.verify(position).setOrdered(initialPosition);
        inOrder.verify(position).setOrdered(initialPosition + ALIGNED_FRAME_LENGTH);
    }

    @Test
    public void shouldReportCorrectPositionOnReceptionWithNonZeroPositionInNonInitialTermId()
    {
        final int activeTermId = INITIAL_TERM_ID + 1;
        final int initialMessageIndex = 5;
        final int initialTermOffset = offsetForFrame(initialMessageIndex);
        final long initialPosition =
            computePosition(activeTermId, initialTermOffset, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);

        position.setOrdered(initialPosition);
        final Image image = createImage();

        insertDataFrame(activeTermId, offsetForFrame(initialMessageIndex));

        final int messages = image.poll(mockFragmentHandler, Integer.MAX_VALUE);
        assertThat(messages, is(1));

        verify(mockFragmentHandler).onFragment(
            any(UnsafeBuffer.class),
            eq(initialTermOffset + HEADER_LENGTH),
            eq(DATA.length),
            any(Header.class));

        final InOrder inOrder = Mockito.inOrder(position);
        inOrder.verify(position).setOrdered(initialPosition);
        inOrder.verify(position).setOrdered(initialPosition + ALIGNED_FRAME_LENGTH);
    }

    @Test
    public void shouldPollNoFragmentsToControlledFragmentHandler()
    {
        final Image image = createImage();
        final int fragmentsRead = image.controlledPoll(mockControlledFragmentHandler, Integer.MAX_VALUE);

        assertThat(fragmentsRead, is(0));
        verify(position, never()).setOrdered(anyLong());
        verify(mockControlledFragmentHandler, never()).onFragment(
            any(UnsafeBuffer.class), anyInt(), anyInt(), any(Header.class));
    }

    @Test
    public void shouldPollOneFragmentToControlledFragmentHandlerOnContinue()
    {
        final long initialPosition = computePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
        position.setOrdered(initialPosition);
        final Image image = createImage();

        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(0));

        when(mockControlledFragmentHandler.onFragment(any(DirectBuffer.class), anyInt(), anyInt(), any(Header.class)))
            .thenReturn(ControlledFragmentHandler.Action.CONTINUE);

        final int fragmentsRead = image.controlledPoll(mockControlledFragmentHandler, Integer.MAX_VALUE);

        assertThat(fragmentsRead, is(1));

        final InOrder inOrder = Mockito.inOrder(position, mockControlledFragmentHandler);
        inOrder.verify(mockControlledFragmentHandler).onFragment(
            any(UnsafeBuffer.class), eq(HEADER_LENGTH), eq(DATA.length), any(Header.class));
        inOrder.verify(position).setOrdered(initialPosition + ALIGNED_FRAME_LENGTH);
    }

    @Test
    public void shouldNotPollOneFragmentToControlledFragmentHandlerOnAbort()
    {
        final long initialPosition = computePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
        position.setOrdered(initialPosition);
        final Image image = createImage();

        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(0));

        when(mockControlledFragmentHandler.onFragment(any(DirectBuffer.class), anyInt(), anyInt(), any(Header.class)))
            .thenReturn(ControlledFragmentHandler.Action.ABORT);

        final int fragmentsRead = image.controlledPoll(mockControlledFragmentHandler, Integer.MAX_VALUE);

        assertThat(fragmentsRead, is(0));
        assertThat(image.position(), is(initialPosition));

        verify(mockControlledFragmentHandler).onFragment(
            any(UnsafeBuffer.class), eq(HEADER_LENGTH), eq(DATA.length), any(Header.class));
    }

    @Test
    public void shouldPollOneFragmentToControlledFragmentHandlerOnBreak()
    {
        final long initialPosition = computePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
        position.setOrdered(initialPosition);
        final Image image = createImage();

        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(0));
        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(1));

        when(mockControlledFragmentHandler.onFragment(any(DirectBuffer.class), anyInt(), anyInt(), any(Header.class)))
            .thenReturn(ControlledFragmentHandler.Action.BREAK);

        final int fragmentsRead = image.controlledPoll(mockControlledFragmentHandler, Integer.MAX_VALUE);

        assertThat(fragmentsRead, is(1));

        final InOrder inOrder = Mockito.inOrder(position, mockControlledFragmentHandler);
        inOrder.verify(mockControlledFragmentHandler).onFragment(
            any(UnsafeBuffer.class), eq(HEADER_LENGTH), eq(DATA.length), any(Header.class));
        inOrder.verify(position).setOrdered(initialPosition + ALIGNED_FRAME_LENGTH);
    }

    @Test
    public void shouldPollFragmentsToControlledFragmentHandlerOnCommit()
    {
        final long initialPosition = computePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
        position.setOrdered(initialPosition);
        final Image image = createImage();

        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(0));
        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(1));

        when(mockControlledFragmentHandler.onFragment(any(DirectBuffer.class), anyInt(), anyInt(), any(Header.class)))
            .thenReturn(ControlledFragmentHandler.Action.COMMIT);

        final int fragmentsRead = image.controlledPoll(mockControlledFragmentHandler, Integer.MAX_VALUE);

        assertThat(fragmentsRead, is(2));

        final InOrder inOrder = Mockito.inOrder(position, mockControlledFragmentHandler);
        inOrder.verify(mockControlledFragmentHandler).onFragment(
            any(UnsafeBuffer.class), eq(HEADER_LENGTH), eq(DATA.length), any(Header.class));
        inOrder.verify(position).setOrdered(initialPosition + ALIGNED_FRAME_LENGTH);

        inOrder.verify(mockControlledFragmentHandler).onFragment(
            any(UnsafeBuffer.class), eq(ALIGNED_FRAME_LENGTH + HEADER_LENGTH), eq(DATA.length), any(Header.class));
        inOrder.verify(position).setOrdered(initialPosition + (ALIGNED_FRAME_LENGTH * 2));
    }

    @Test
    public void shouldPollFragmentsToControlledFragmentHandlerOnContinue()
    {
        final long initialPosition = computePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
        position.setOrdered(initialPosition);
        final Image image = createImage();

        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(0));
        insertDataFrame(INITIAL_TERM_ID, offsetForFrame(1));

        when(mockControlledFragmentHandler.onFragment(any(DirectBuffer.class), anyInt(), anyInt(), any(Header.class)))
            .thenReturn(ControlledFragmentHandler.Action.CONTINUE);

        final int fragmentsRead = image.controlledPoll(mockControlledFragmentHandler, Integer.MAX_VALUE);

        assertThat(fragmentsRead, is(2));

        final InOrder inOrder = Mockito.inOrder(position, mockControlledFragmentHandler);
        inOrder.verify(mockControlledFragmentHandler).onFragment(
            any(UnsafeBuffer.class), eq(HEADER_LENGTH), eq(DATA.length), any(Header.class));
        inOrder.verify(mockControlledFragmentHandler).onFragment(
            any(UnsafeBuffer.class), eq(ALIGNED_FRAME_LENGTH + HEADER_LENGTH), eq(DATA.length), any(Header.class));
        inOrder.verify(position).setOrdered(initialPosition + (ALIGNED_FRAME_LENGTH * 2));
    }

    private Image createImage()
    {
        return new Image(subscription, SESSION_ID, position, logBuffers, errorHandler, SOURCE_IDENTITY, CORRELATION_ID);
    }

    private void insertDataFrame(final int activeTermId, final int termOffset)
    {
        dataHeader
            .termId(INITIAL_TERM_ID)
            .streamId(STREAM_ID)
            .sessionId(SESSION_ID)
            .termOffset(termOffset)
            .frameLength(DATA.length + HEADER_LENGTH)
            .headerType(HeaderFlyweight.HDR_TYPE_DATA)
            .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
            .version(HeaderFlyweight.CURRENT_VERSION);

        rcvBuffer.putBytes(dataHeader.dataOffset(), DATA);

        final int activeIndex = indexByTerm(INITIAL_TERM_ID, activeTermId);
        TermRebuilder.insert(termBuffers[activeIndex], termOffset, rcvBuffer, ALIGNED_FRAME_LENGTH);
    }

    private static int offsetForFrame(final int index)
    {
        return index * ALIGNED_FRAME_LENGTH;
    }
}
