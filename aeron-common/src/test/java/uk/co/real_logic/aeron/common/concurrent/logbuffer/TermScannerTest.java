package uk.co.real_logic.aeron.common.concurrent.logbuffer;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static java.lang.Integer.valueOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor.*;
import static uk.co.real_logic.aeron.common.protocol.HeaderFlyweight.HDR_TYPE_DATA;
import static uk.co.real_logic.agrona.BitUtil.align;

public class TermScannerTest
{
    private static final int TERM_BUFFER_CAPACITY = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int MTU_LENGTH = 1024;
    private static final int HEADER_LENGTH = 24;

    private final UnsafeBuffer termBuffer = mock(UnsafeBuffer.class);
    private final TermScanner scanner = new TermScanner(HEADER_LENGTH);

    @Before
    public void setUp()
    {
        when(termBuffer.capacity()).thenReturn(TERM_BUFFER_CAPACITY);
    }

    @Test
    public void shouldReturnZeroOnEmptyLog()
    {
        assertThat(scanner.scanForAvailability(termBuffer, 0, MTU_LENGTH), is(0));
    }

    @Test
    public void shouldScanSingleMessage()
    {
        final int msgLength = 1;
        final int frameLength = HEADER_LENGTH + msgLength;
        final int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);
        final int frameOffset = 0;

        when(termBuffer.getIntVolatile(lengthOffset(frameOffset))).thenReturn(frameLength);
        when(termBuffer.getShort(typeOffset(frameOffset))).thenReturn((short)HDR_TYPE_DATA);

        assertThat(scanner.scanForAvailability(termBuffer, frameOffset, MTU_LENGTH), is(alignedFrameLength));
        assertThat(scanner.padding(), is(0));

        final InOrder inOrder = inOrder(termBuffer);
        inOrder.verify(termBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(termBuffer).getShort(typeOffset(frameOffset));
    }

    @Test
    public void shouldFailToScanMessageLargerThanMaxLength()
    {
        final int msgLength = 1;
        final int frameLength = HEADER_LENGTH + msgLength;
        final int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);
        final int maxLength = alignedFrameLength - 1;
        final int frameOffset = 0;

        when(termBuffer.getIntVolatile(lengthOffset(frameOffset))).thenReturn(frameLength);
        when(termBuffer.getShort(typeOffset(frameOffset))).thenReturn((short)HDR_TYPE_DATA);

        assertThat(scanner.scanForAvailability(termBuffer, frameOffset, maxLength), is(0));
        assertThat(scanner.padding(), is(0));

        final InOrder inOrder = inOrder(termBuffer);
        inOrder.verify(termBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(termBuffer).getShort(typeOffset(frameOffset));
    }

    @Test
    public void shouldScanTwoMessagesThatFitInSingleMtu()
    {
        final int msgLength = 100;
        final int frameLength = HEADER_LENGTH + msgLength;
        final int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);
        int frameOffset = 0;

        when(termBuffer.getIntVolatile(lengthOffset(frameOffset))).thenReturn(frameLength);
        when(termBuffer.getShort(typeOffset(frameOffset))).thenReturn((short)HDR_TYPE_DATA);
        when(termBuffer.getIntVolatile(lengthOffset(frameOffset + alignedFrameLength))).thenReturn(alignedFrameLength);
        when(termBuffer.getShort(typeOffset(frameOffset + alignedFrameLength))).thenReturn((short)HDR_TYPE_DATA);

        assertThat(scanner.scanForAvailability(termBuffer, frameOffset, MTU_LENGTH), is(alignedFrameLength * 2));
        assertThat(scanner.padding(), is(0));

        final InOrder inOrder = inOrder(termBuffer);
        inOrder.verify(termBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(termBuffer).getShort(typeOffset(frameOffset));

        frameOffset += alignedFrameLength;
        inOrder.verify(termBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(termBuffer).getShort(typeOffset(frameOffset));
    }

    @Test
    public void shouldScanTwoMessagesAndStopAtMtuBoundary()
    {
        final int frameTwoLength = align(HEADER_LENGTH + 1, FRAME_ALIGNMENT);
        final int frameOneLength = MTU_LENGTH - frameTwoLength;

        int frameOffset = 0;

        when(termBuffer.getIntVolatile(lengthOffset(frameOffset))).thenReturn(frameOneLength);
        when(termBuffer.getShort(typeOffset(frameOffset))).thenReturn((short)HDR_TYPE_DATA);
        when(termBuffer.getIntVolatile(lengthOffset(frameOffset + frameOneLength))).thenReturn(frameTwoLength);
        when(termBuffer.getShort(typeOffset(frameOffset + frameOneLength))).thenReturn((short)HDR_TYPE_DATA);

        assertThat(scanner.scanForAvailability(termBuffer, frameOffset, MTU_LENGTH), is(frameOneLength + frameTwoLength));
        assertThat(scanner.padding(), is(0));

        final InOrder inOrder = inOrder(termBuffer);
        inOrder.verify(termBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(termBuffer).getShort(typeOffset(frameOffset));

        frameOffset += frameOneLength;
        inOrder.verify(termBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(termBuffer).getShort(typeOffset(frameOffset));
    }

    @Test
    public void shouldScanTwoMessagesAndStopAtSecondThatSpansMtu()
    {
        final int frameTwoLength = align(HEADER_LENGTH * 2, FRAME_ALIGNMENT);
        final int frameOneLength = MTU_LENGTH - (frameTwoLength / 2);
        int frameOffset = 0;

        when(termBuffer.getIntVolatile(lengthOffset(frameOffset))).thenReturn(frameOneLength);
        when(termBuffer.getShort(typeOffset(frameOffset))).thenReturn((short)HDR_TYPE_DATA);
        when(termBuffer.getIntVolatile(lengthOffset(frameOffset + frameOneLength))).thenReturn(frameTwoLength);
        when(termBuffer.getShort(typeOffset(frameOffset + frameOneLength))).thenReturn((short)HDR_TYPE_DATA);

        assertThat(scanner.scanForAvailability(termBuffer, frameOffset, MTU_LENGTH), is(frameOneLength));
        assertThat(scanner.padding(), is(0));

        final InOrder inOrder = inOrder(termBuffer);
        inOrder.verify(termBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(termBuffer).getShort(typeOffset(frameOffset));

        frameOffset += frameOneLength;
        inOrder.verify(termBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(termBuffer).getShort(typeOffset(frameOffset));
    }

    @Test
    public void shouldScanLastFrameInBuffer()
    {
        final int alignedFrameLength = align(HEADER_LENGTH * 2, FRAME_ALIGNMENT);
        final int frameOffset = TERM_BUFFER_CAPACITY - alignedFrameLength;

        when(termBuffer.getIntVolatile(lengthOffset(frameOffset))).thenReturn(alignedFrameLength);
        when(termBuffer.getShort(typeOffset(frameOffset))).thenReturn((short)HDR_TYPE_DATA);

        assertThat(scanner.scanForAvailability(termBuffer, frameOffset, MTU_LENGTH), is(alignedFrameLength));
        assertThat(scanner.padding(), is(0));
    }

    @Test
    public void shouldScanLastMessageInBufferPlusPadding()
    {
        final int alignedFrameLength = align(HEADER_LENGTH * 2, FRAME_ALIGNMENT);
        final int paddingFrameLength = align(HEADER_LENGTH * 3, FRAME_ALIGNMENT);
        final int frameOffset = TERM_BUFFER_CAPACITY - (alignedFrameLength + paddingFrameLength);

        when(valueOf(termBuffer.getIntVolatile(lengthOffset(frameOffset)))).thenReturn(alignedFrameLength);
        when(termBuffer.getShort(typeOffset(frameOffset))).thenReturn((short)HDR_TYPE_DATA);
        when(termBuffer.getIntVolatile(lengthOffset(frameOffset + alignedFrameLength))).thenReturn(paddingFrameLength);
        when(termBuffer.getShort(typeOffset(frameOffset + alignedFrameLength))).thenReturn((short)PADDING_FRAME_TYPE);

        assertThat(scanner.scanForAvailability(termBuffer, frameOffset, MTU_LENGTH), is(alignedFrameLength + HEADER_LENGTH));
        assertThat(scanner.padding(), is(paddingFrameLength - HEADER_LENGTH));
    }

    @Test
    public void shouldScanLastMessageInBufferMinusPaddingLimitedByMtu()
    {
        final int alignedFrameLength = align(HEADER_LENGTH, FRAME_ALIGNMENT);
        final int frameOffset = TERM_BUFFER_CAPACITY - align(HEADER_LENGTH * 3, FRAME_ALIGNMENT);
        final int mtu = alignedFrameLength + 8;

        when(valueOf(termBuffer.getIntVolatile(lengthOffset(frameOffset)))).thenReturn(alignedFrameLength);
        when(termBuffer.getShort(typeOffset(frameOffset))).thenReturn((short)HDR_TYPE_DATA);
        when(termBuffer.getIntVolatile(lengthOffset(frameOffset + alignedFrameLength))).thenReturn(alignedFrameLength * 2);
        when(termBuffer.getShort(typeOffset(frameOffset + alignedFrameLength))).thenReturn((short)PADDING_FRAME_TYPE);

        assertThat(scanner.scanForAvailability(termBuffer, frameOffset, mtu), is(alignedFrameLength));
        assertThat(scanner.padding(), is(0));
    }
}