/*
 * Copyright 2014-2021 Real Logic Limited.
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
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class FragmentAssemblerTest
{
    private static final int SESSION_ID = 777;
    private static final int INITIAL_TERM_ID = 3;

    private final FragmentHandler delegateFragmentHandler = mock(FragmentHandler.class);
    private final UnsafeBuffer termBuffer = mock(UnsafeBuffer.class);
    private final Header header = spy(new Header(INITIAL_TERM_ID, LogBufferDescriptor.TERM_MIN_LENGTH));
    private final FragmentAssembler adapter = new FragmentAssembler(delegateFragmentHandler);

    @BeforeEach
    void setUp()
    {
        header.buffer(termBuffer);
        when(termBuffer.getInt(anyInt(), any(ByteOrder.class))).thenReturn(SESSION_ID);
    }

    @Test
    void shouldPassThroughUnfragmentedMessage()
    {
        when(header.flags()).thenReturn(FrameDescriptor.UNFRAGMENTED);
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[128]);
        final int offset = 8;
        final int length = 32;

        adapter.onFragment(srcBuffer, offset, length, header);

        verify(delegateFragmentHandler, times(1)).onFragment(srcBuffer, offset, length, header);
    }

    @Test
    void shouldAssembleTwoPartMessage()
    {
        when(header.flags())
            .thenReturn(FrameDescriptor.BEGIN_FRAG_FLAG)
            .thenReturn(FrameDescriptor.END_FRAG_FLAG);

        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[1024]);
        final int offset = 0;
        final int length = srcBuffer.capacity() / 2;

        srcBuffer.setMemory(0, length, (byte)65);
        srcBuffer.setMemory(length, length, (byte)66);

        adapter.onFragment(srcBuffer, offset, length, header);
        adapter.onFragment(srcBuffer, length, length, header);

        final ArgumentCaptor<UnsafeBuffer> bufferArg = ArgumentCaptor.forClass(UnsafeBuffer.class);
        final ArgumentCaptor<Header> headerArg = ArgumentCaptor.forClass(Header.class);

        verify(delegateFragmentHandler, times(1)).onFragment(
            bufferArg.capture(), eq(offset), eq(length * 2), headerArg.capture());

        final UnsafeBuffer capturedBuffer = bufferArg.getValue();
        for (int i = 0; i < srcBuffer.capacity(); i++)
        {
            assertEquals(srcBuffer.getByte(i), capturedBuffer.getByte(i), "same at i=" + i);
        }

        final Header capturedHeader = headerArg.getValue();
        assertEquals(SESSION_ID, capturedHeader.sessionId());
        assertEquals(FrameDescriptor.END_FRAG_FLAG, capturedHeader.flags());
    }

    @Test
    void shouldAssembleFourPartMessage()
    {
        when(header.flags())
            .thenReturn(FrameDescriptor.BEGIN_FRAG_FLAG)
            .thenReturn((byte)0)
            .thenReturn((byte)0)
            .thenReturn(FrameDescriptor.END_FRAG_FLAG);

        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[1024]);
        final int offset = 0;
        final int length = srcBuffer.capacity() / 4;

        for (int i = 0; i < 4; i++)
        {
            srcBuffer.setMemory(i * length, length, (byte)(65 + i));
        }

        adapter.onFragment(srcBuffer, offset, length, header);
        adapter.onFragment(srcBuffer, offset + length, length, header);
        adapter.onFragment(srcBuffer, offset + (length * 2), length, header);
        adapter.onFragment(srcBuffer, offset + (length * 3), length, header);

        final ArgumentCaptor<UnsafeBuffer> bufferArg = ArgumentCaptor.forClass(UnsafeBuffer.class);
        final ArgumentCaptor<Header> headerArg = ArgumentCaptor.forClass(Header.class);

        verify(delegateFragmentHandler, times(1)).onFragment(
            bufferArg.capture(), eq(offset), eq(length * 4), headerArg.capture());

        final UnsafeBuffer capturedBuffer = bufferArg.getValue();
        for (int i = 0; i < srcBuffer.capacity(); i++)
        {
            assertEquals(srcBuffer.getByte(i), capturedBuffer.getByte(i), "same at i=" + i);
        }

        final Header capturedHeader = headerArg.getValue();
        assertEquals(SESSION_ID, capturedHeader.sessionId());
        assertEquals(FrameDescriptor.END_FRAG_FLAG, capturedHeader.flags());
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

        assertFalse(adapter.freeSessionBuffer(SESSION_ID));

        adapter.onFragment(srcBuffer, offset, length, header);
        adapter.onFragment(srcBuffer, length, length, header);

        assertTrue(adapter.freeSessionBuffer(SESSION_ID));
        assertFalse(adapter.freeSessionBuffer(SESSION_ID));
    }

    @Test
    void shouldDoNotingIfEndArrivesWithoutBegin()
    {
        when(header.flags()).thenReturn(FrameDescriptor.END_FRAG_FLAG);
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[1024]);
        final int offset = 0;
        final int length = srcBuffer.capacity() / 2;

        adapter.onFragment(srcBuffer, offset, length, header);

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

        adapter.onFragment(srcBuffer, offset, length, header);
        adapter.onFragment(srcBuffer, offset, length, header);

        verify(delegateFragmentHandler, never()).onFragment(any(), anyInt(), anyInt(), any());
    }

    @Test
    void shouldFreeDirectByteBuffers()
    {
        final FragmentAssembler fragmentAssembler = new FragmentAssembler(delegateFragmentHandler, 100, true);

        when(header.flags()).thenReturn(FrameDescriptor.BEGIN_FRAG_FLAG);
        when(header.sessionId()).thenReturn(1, 2);

        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[1024]);
        srcBuffer.setMemory(0, srcBuffer.capacity(), (byte)1);
        final int length = srcBuffer.capacity() / 2;

        fragmentAssembler.onFragment(srcBuffer, 0, length, header);
        fragmentAssembler.onFragment(srcBuffer, length, length, header);

        final List<BufferBuilder> builders = new ArrayList<>(fragmentAssembler.bufferBuilders());
        assertEquals(2, builders.size());
        for (final BufferBuilder builder : builders)
        {
            BufferUtil.assertDirectByteBufferAllocated(builder.buffer().byteBuffer());
        }

        fragmentAssembler.clear();

        assertTrue(fragmentAssembler.bufferBuilders().isEmpty());
        for (final BufferBuilder builder : builders)
        {
            BufferUtil.assertDirectByteBufferFreed(builder.buffer().byteBuffer());
        }
    }

    @Test
    void shouldFreeDirectSessionBuffer()
    {
        final FragmentAssembler fragmentAssembler = new FragmentAssembler(delegateFragmentHandler, 100, true);

        final int sessionId = -100;
        when(header.flags()).thenReturn(FrameDescriptor.BEGIN_FRAG_FLAG);
        when(header.sessionId()).thenReturn(sessionId);

        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[64]);

        fragmentAssembler.onFragment(srcBuffer, 0, srcBuffer.capacity(), header);

        final Collection<BufferBuilder> builders = fragmentAssembler.bufferBuilders();
        assertEquals(1, builders.size());
        final BufferBuilder builder = builders.iterator().next();
        BufferUtil.assertDirectByteBufferAllocated(builder.buffer().byteBuffer());

        assertTrue(fragmentAssembler.freeSessionBuffer(sessionId));

        assertTrue(fragmentAssembler.bufferBuilders().isEmpty());
        BufferUtil.assertDirectByteBufferFreed(builder.buffer().byteBuffer());
    }
}
