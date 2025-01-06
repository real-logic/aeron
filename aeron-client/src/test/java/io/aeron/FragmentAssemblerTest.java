/*
 * Copyright 2014-2025 Real Logic Limited.
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
package io.aeron;

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.Header;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.BitUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class FragmentAssemblerTest
{
    private static final int SESSION_ID = 777;
    private static final int INITIAL_TERM_ID = 3;

    private final FragmentHandler delegateFragmentHandler = mock(FragmentHandler.class);
    private final Header header = spy(new Header(INITIAL_TERM_ID, LogBufferDescriptor.TERM_MIN_LENGTH));
    private final FragmentAssembler assembler = new FragmentAssembler(delegateFragmentHandler);
    private DataHeaderFlyweight headerFlyweight;

    @BeforeEach
    void setUp()
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[64]);
        headerFlyweight = new DataHeaderFlyweight();
        headerFlyweight.wrap(buffer, 16, HEADER_LENGTH);
        header.buffer(buffer);
        header.offset(headerFlyweight.wrapAdjustment());

        headerFlyweight.sessionId(SESSION_ID);
    }

    @Test
    void shouldPassThroughUnfragmentedMessage()
    {
        headerFlyweight.flags(FrameDescriptor.UNFRAGMENTED);
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[128]);
        final int offset = 8;
        final int length = 32;

        assembler.onFragment(srcBuffer, offset, length, header);

        verify(delegateFragmentHandler, times(1)).onFragment(srcBuffer, offset, length, header);
    }

    @Test
    void shouldAssembleTwoPartMessage()
    {
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[1024 + (2 * HEADER_LENGTH)]);
        final int length = 512;

        int offset = HEADER_LENGTH;
        headerFlyweight.flags(FrameDescriptor.BEGIN_FRAG_FLAG);
        assembler.onFragment(srcBuffer, offset, length, header);
        offset = BitUtil.align(offset + length + HEADER_LENGTH, FRAME_ALIGNMENT);
        headerFlyweight.flags(FrameDescriptor.END_FRAG_FLAG);
        assembler.onFragment(srcBuffer, offset, length, header);

        final ArgumentCaptor<Header> headerArg = ArgumentCaptor.forClass(Header.class);

        verify(delegateFragmentHandler, times(1)).onFragment(
            any(), eq(0), eq(length * 2), headerArg.capture());

        final Header capturedHeader = headerArg.getValue();
        assertEquals(SESSION_ID, capturedHeader.sessionId());
        assertEquals(FrameDescriptor.UNFRAGMENTED, capturedHeader.flags());
    }

    @Test
    void shouldAssembleFourPartMessage()
    {
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[1024 + (4 * HEADER_LENGTH)]);
        final int length = 256;

        int offset = HEADER_LENGTH;
        headerFlyweight.flags(FrameDescriptor.BEGIN_FRAG_FLAG);
        assembler.onFragment(srcBuffer, offset, length, header);
        offset = BitUtil.align(offset + length + HEADER_LENGTH, FRAME_ALIGNMENT);
        headerFlyweight.flags((short)0);
        assembler.onFragment(srcBuffer, offset, length, header);
        offset = BitUtil.align(offset + length + HEADER_LENGTH, FRAME_ALIGNMENT);
        headerFlyweight.flags((short)0);
        assembler.onFragment(srcBuffer, offset, length, header);
        offset = BitUtil.align(offset + length + HEADER_LENGTH, FRAME_ALIGNMENT);
        headerFlyweight.flags((short)(FrameDescriptor.END_FRAG_FLAG | 0x3));
        assembler.onFragment(srcBuffer, offset, length, header);

        final ArgumentCaptor<Header> headerArg = ArgumentCaptor.forClass(Header.class);

        verify(delegateFragmentHandler, times(1)).onFragment(
            any(), eq(0), eq(length * 4), headerArg.capture());

        final Header capturedHeader = headerArg.getValue();
        assertEquals(SESSION_ID, capturedHeader.sessionId());
        assertEquals((byte)(FrameDescriptor.UNFRAGMENTED | 0x3), capturedHeader.flags());
    }

    @Test
    void shouldFreeSessionBuffer()
    {
        when(header.flags())
            .thenReturn(FrameDescriptor.BEGIN_FRAG_FLAG)
            .thenReturn(FrameDescriptor.END_FRAG_FLAG);

        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[1024]);
        final int offset = 0;
        final int length = srcBuffer.capacity() / 2;

        srcBuffer.setMemory(0, length, (byte)65);
        srcBuffer.setMemory(length, length, (byte)66);

        assertFalse(assembler.freeSessionBuffer(SESSION_ID));

        assembler.onFragment(srcBuffer, offset, length, header);
        assembler.onFragment(srcBuffer, length, length, header);

        assertTrue(assembler.freeSessionBuffer(SESSION_ID));
        assertFalse(assembler.freeSessionBuffer(SESSION_ID));
    }

    @Test
    void shouldDoNotingIfEndArrivesWithoutBegin()
    {
        when(header.flags()).thenReturn(FrameDescriptor.END_FRAG_FLAG);
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[1024]);
        final int offset = 0;
        final int length = srcBuffer.capacity() / 2;

        assembler.onFragment(srcBuffer, offset, length, header);

        verify(delegateFragmentHandler, never()).onFragment(any(), anyInt(), anyInt(), any());
    }

    @Test
    void shouldDoNotingIfMidArrivesWithoutBegin()
    {
        when(header.flags())
            .thenReturn((byte)0)
            .thenReturn(FrameDescriptor.END_FRAG_FLAG);

        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[1024]);
        final int offset = 0;
        final int length = srcBuffer.capacity() / 2;

        assembler.onFragment(srcBuffer, offset, length, header);
        assembler.onFragment(srcBuffer, offset, length, header);

        verify(delegateFragmentHandler, never()).onFragment(any(), anyInt(), anyInt(), any());
    }

    @Test
    void shouldSkipOverMessagesWithLoss()
    {
        when(header.flags())
            .thenReturn(FrameDescriptor.BEGIN_FRAG_FLAG)
            .thenReturn(FrameDescriptor.END_FRAG_FLAG)
            .thenReturn(FrameDescriptor.UNFRAGMENTED);

        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[2048]);
        final int length = 256;

        int offset = HEADER_LENGTH;
        assembler.onFragment(srcBuffer, offset, length, header);
        offset = BitUtil.align(offset + length + HEADER_LENGTH, FRAME_ALIGNMENT);
        offset = BitUtil.align(offset + length + HEADER_LENGTH, FRAME_ALIGNMENT);
        offset = BitUtil.align(offset + length + HEADER_LENGTH, FRAME_ALIGNMENT);
        assembler.onFragment(srcBuffer, offset, length, header);
        offset = BitUtil.align(offset + length + HEADER_LENGTH, FRAME_ALIGNMENT);
        assembler.onFragment(srcBuffer, offset, length, header);

        final ArgumentCaptor<Header> headerArg = ArgumentCaptor.forClass(Header.class);

        verify(delegateFragmentHandler, times(1)).onFragment(
            any(), eq(offset), eq(length), headerArg.capture());

        final Header capturedHeader = headerArg.getValue();
        assertEquals(SESSION_ID, capturedHeader.sessionId());
        assertEquals(FrameDescriptor.UNFRAGMENTED, capturedHeader.flags());
    }
}
