package uk.co.real_logic.aeron.logbuffer;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import uk.co.real_logic.aeron.protocol.DataHeaderFlyweight;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static java.lang.Integer.valueOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.logbuffer.FrameDescriptor.*;
import static uk.co.real_logic.aeron.protocol.HeaderFlyweight.HDR_TYPE_DATA;
import static uk.co.real_logic.agrona.BitUtil.align;

public class TermScannerTest
{
    private static final int TERM_BUFFER_CAPACITY = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int MTU_LENGTH = 1024;
    private static final int HEADER_LENGTH = DataHeaderFlyweight.HEADER_LENGTH;

    private final UnsafeBuffer termBuffer = mock(UnsafeBuffer.class);

    @Before
    public void setUp()
    {
        when(termBuffer.capacity()).thenReturn(TERM_BUFFER_CAPACITY);
    }

    @Test
    public void shouldPackPaddingAndOffsetIntoResultingStatus()
    {
        final int padding = 77;
        final int available = 65000;

        final long scanOutcome = TermScanner.scanOutcome(padding, available);

        assertThat(TermScanner.padding(scanOutcome), is(padding));
        assertThat(TermScanner.available(scanOutcome), is(available));
    }

    @Test
    public void shouldReturnZeroOnEmptyLog()
    {
        final long scanOutcome = TermScanner.scanForAvailability(termBuffer, 0, MTU_LENGTH);
        assertThat(TermScanner.available(scanOutcome), is(0));
        assertThat(TermScanner.padding(scanOutcome), is(0));
    }

    @Test
    public void shouldScanSingleMessage()
    {
        final int msgLength = 1;
        final int frameLength = HEADER_LENGTH + msgLength;
        final int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);
        final int frameOffset = 0;

        when(termBuffer.getIntVolatile(frameOffset)).thenReturn(frameLength);
        when(termBuffer.getShort(typeOffset(frameOffset))).thenReturn((short)HDR_TYPE_DATA);

        final long scanOutcome = TermScanner.scanForAvailability(termBuffer, frameOffset, MTU_LENGTH);
        assertThat(TermScanner.available(scanOutcome), is(alignedFrameLength));
        assertThat(TermScanner.padding(scanOutcome), is(0));

        final InOrder inOrder = inOrder(termBuffer);
        inOrder.verify(termBuffer).getIntVolatile(frameOffset);
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

        when(termBuffer.getIntVolatile(frameOffset)).thenReturn(frameLength);
        when(termBuffer.getShort(typeOffset(frameOffset))).thenReturn((short)HDR_TYPE_DATA);

        final long scanOutcome = TermScanner.scanForAvailability(termBuffer, frameOffset, maxLength);
        assertThat(TermScanner.available(scanOutcome), is(0));
        assertThat(TermScanner.padding(scanOutcome), is(0));

        final InOrder inOrder = inOrder(termBuffer);
        inOrder.verify(termBuffer).getIntVolatile(frameOffset);
        inOrder.verify(termBuffer).getShort(typeOffset(frameOffset));
    }

    @Test
    public void shouldScanTwoMessagesThatFitInSingleMtu()
    {
        final int msgLength = 100;
        final int frameLength = HEADER_LENGTH + msgLength;
        final int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);
        int frameOffset = 0;

        when(termBuffer.getIntVolatile(frameOffset)).thenReturn(frameLength);
        when(termBuffer.getShort(typeOffset(frameOffset))).thenReturn((short)HDR_TYPE_DATA);
        when(termBuffer.getIntVolatile(frameOffset + alignedFrameLength)).thenReturn(alignedFrameLength);
        when(termBuffer.getShort(typeOffset(frameOffset + alignedFrameLength))).thenReturn((short)HDR_TYPE_DATA);

        final long scanOutcome = TermScanner.scanForAvailability(termBuffer, frameOffset, MTU_LENGTH);
        assertThat(TermScanner.available(scanOutcome), is(alignedFrameLength * 2));
        assertThat(TermScanner.padding(scanOutcome), is(0));

        final InOrder inOrder = inOrder(termBuffer);
        inOrder.verify(termBuffer).getIntVolatile(frameOffset);
        inOrder.verify(termBuffer).getShort(typeOffset(frameOffset));

        frameOffset += alignedFrameLength;
        inOrder.verify(termBuffer).getIntVolatile(frameOffset);
        inOrder.verify(termBuffer).getShort(typeOffset(frameOffset));
    }

    @Test
    public void shouldScanTwoMessagesAndStopAtMtuBoundary()
    {
        final int frameTwoLength = align(HEADER_LENGTH + 1, FRAME_ALIGNMENT);
        final int frameOneLength = MTU_LENGTH - frameTwoLength;

        int frameOffset = 0;

        when(termBuffer.getIntVolatile(frameOffset)).thenReturn(frameOneLength);
        when(termBuffer.getShort(typeOffset(frameOffset))).thenReturn((short)HDR_TYPE_DATA);
        when(termBuffer.getIntVolatile(frameOffset + frameOneLength)).thenReturn(frameTwoLength);
        when(termBuffer.getShort(typeOffset(frameOffset + frameOneLength))).thenReturn((short)HDR_TYPE_DATA);

        final long scanOutcome = TermScanner.scanForAvailability(termBuffer, frameOffset, MTU_LENGTH);
        assertThat(TermScanner.available(scanOutcome), is(frameOneLength + frameTwoLength));
        assertThat(TermScanner.padding(scanOutcome), is(0));

        final InOrder inOrder = inOrder(termBuffer);
        inOrder.verify(termBuffer).getIntVolatile(frameOffset);
        inOrder.verify(termBuffer).getShort(typeOffset(frameOffset));

        frameOffset += frameOneLength;
        inOrder.verify(termBuffer).getIntVolatile(frameOffset);
        inOrder.verify(termBuffer).getShort(typeOffset(frameOffset));
    }

    @Test
    public void shouldScanTwoMessagesAndStopAtSecondThatSpansMtu()
    {
        final int frameTwoLength = align(HEADER_LENGTH * 2, FRAME_ALIGNMENT);
        final int frameOneLength = MTU_LENGTH - (frameTwoLength / 2);
        int frameOffset = 0;

        when(termBuffer.getIntVolatile(frameOffset)).thenReturn(frameOneLength);
        when(termBuffer.getShort(typeOffset(frameOffset))).thenReturn((short)HDR_TYPE_DATA);
        when(termBuffer.getIntVolatile(frameOffset + frameOneLength)).thenReturn(frameTwoLength);
        when(termBuffer.getShort(typeOffset(frameOffset + frameOneLength))).thenReturn((short)HDR_TYPE_DATA);

        final long scanOutcome = TermScanner.scanForAvailability(termBuffer, frameOffset, MTU_LENGTH);
        assertThat(TermScanner.available(scanOutcome), is(frameOneLength));
        assertThat(TermScanner.padding(scanOutcome), is(0));

        final InOrder inOrder = inOrder(termBuffer);
        inOrder.verify(termBuffer).getIntVolatile(frameOffset);
        inOrder.verify(termBuffer).getShort(typeOffset(frameOffset));

        frameOffset += frameOneLength;
        inOrder.verify(termBuffer).getIntVolatile(frameOffset);
        inOrder.verify(termBuffer).getShort(typeOffset(frameOffset));
    }

    @Test
    public void shouldScanLastFrameInBuffer()
    {
        final int alignedFrameLength = align(HEADER_LENGTH * 2, FRAME_ALIGNMENT);
        final int frameOffset = TERM_BUFFER_CAPACITY - alignedFrameLength;

        when(termBuffer.getIntVolatile(frameOffset)).thenReturn(alignedFrameLength);
        when(termBuffer.getShort(typeOffset(frameOffset))).thenReturn((short)HDR_TYPE_DATA);

        final long scanOutcome = TermScanner.scanForAvailability(termBuffer, frameOffset, MTU_LENGTH);
        assertThat(TermScanner.available(scanOutcome), is(alignedFrameLength));
        assertThat(TermScanner.padding(scanOutcome), is(0));
    }

    @Test
    public void shouldScanLastMessageInBufferPlusPadding()
    {
        final int alignedFrameLength = align(HEADER_LENGTH * 2, FRAME_ALIGNMENT);
        final int paddingFrameLength = align(HEADER_LENGTH * 3, FRAME_ALIGNMENT);
        final int frameOffset = TERM_BUFFER_CAPACITY - (alignedFrameLength + paddingFrameLength);

        when(valueOf(termBuffer.getIntVolatile(frameOffset))).thenReturn(alignedFrameLength);
        when(termBuffer.getShort(typeOffset(frameOffset))).thenReturn((short)HDR_TYPE_DATA);
        when(termBuffer.getIntVolatile(frameOffset + alignedFrameLength)).thenReturn(paddingFrameLength);
        when(termBuffer.getShort(typeOffset(frameOffset + alignedFrameLength))).thenReturn((short)PADDING_FRAME_TYPE);

        final long scanOutcome = TermScanner.scanForAvailability(termBuffer, frameOffset, MTU_LENGTH);
        assertThat(TermScanner.available(scanOutcome), is(alignedFrameLength + HEADER_LENGTH));
        assertThat(TermScanner.padding(scanOutcome), is(paddingFrameLength - HEADER_LENGTH));
    }

    @Test
    public void shouldScanLastMessageInBufferMinusPaddingLimitedByMtu()
    {
        final int alignedFrameLength = align(HEADER_LENGTH, FRAME_ALIGNMENT);
        final int frameOffset = TERM_BUFFER_CAPACITY - align(HEADER_LENGTH * 3, FRAME_ALIGNMENT);
        final int mtu = alignedFrameLength + 8;

        when(valueOf(termBuffer.getIntVolatile(frameOffset))).thenReturn(alignedFrameLength);
        when(termBuffer.getShort(typeOffset(frameOffset))).thenReturn((short)HDR_TYPE_DATA);
        when(termBuffer.getIntVolatile(frameOffset + alignedFrameLength)).thenReturn(alignedFrameLength * 2);
        when(termBuffer.getShort(typeOffset(frameOffset + alignedFrameLength))).thenReturn((short)PADDING_FRAME_TYPE);

        final long scanOutcome = TermScanner.scanForAvailability(termBuffer, frameOffset, mtu);
        assertThat(TermScanner.available(scanOutcome), is(alignedFrameLength));
        assertThat(TermScanner.padding(scanOutcome), is(0));
    }
}