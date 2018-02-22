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
package io.aeron;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.Header;
import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteOrder;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class FragmentAssemblerTest
{
    private static final int SESSION_ID = 777;
    private static final int INITIAL_TERM_ID = 3;

    private final FragmentHandler delegateFragmentHandler = mock(FragmentHandler.class);
    private final UnsafeBuffer termBuffer = mock(UnsafeBuffer.class);
    private final Header header = spy(new Header(INITIAL_TERM_ID, LogBufferDescriptor.TERM_MIN_LENGTH));
    private final FragmentAssembler adapter = new FragmentAssembler(delegateFragmentHandler);

    @Before
    public void setUp()
    {
        header.buffer(termBuffer);
        when(termBuffer.getInt(anyInt(), any(ByteOrder.class))).thenReturn(SESSION_ID);
    }

    @Test
    public void shouldPassThroughUnfragmentedMessage()
    {
        when(header.flags()).thenReturn(FrameDescriptor.UNFRAGMENTED);
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[128]);
        final int offset = 8;
        final int length = 32;

        adapter.onFragment(srcBuffer, offset, length, header);

        verify(delegateFragmentHandler, times(1)).onFragment(srcBuffer, offset, length, header);
    }

    @Test
    public void shouldAssembleTwoPartMessage()
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
            assertThat("same at i=" + i, capturedBuffer.getByte(i), is(srcBuffer.getByte(i)));
        }

        final Header capturedHeader = headerArg.getValue();
        assertThat(capturedHeader.sessionId(), is(SESSION_ID));
        assertThat(capturedHeader.flags(), is(FrameDescriptor.END_FRAG_FLAG));
    }

    @Test
    public void shouldAssembleFourPartMessage()
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
            assertThat("same at i=" + i, capturedBuffer.getByte(i), is(srcBuffer.getByte(i)));
        }

        final Header capturedHeader = headerArg.getValue();
        assertThat(capturedHeader.sessionId(), is(SESSION_ID));
        assertThat(capturedHeader.flags(), is(FrameDescriptor.END_FRAG_FLAG));
    }

    @Test
    public void shouldFreeSessionBuffer()
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
    public void shouldDoNotingIfEndArrivesWithoutBegin()
    {
        when(header.flags()).thenReturn(FrameDescriptor.END_FRAG_FLAG);
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[1024]);
        final int offset = 0;
        final int length = srcBuffer.capacity() / 2;

        adapter.onFragment(srcBuffer, offset, length, header);

        verify(delegateFragmentHandler, never()).onFragment(any(), anyInt(), anyInt(), any());
    }

    @Test
    public void shouldDoNotingIfMidArrivesWithoutBegin()
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
}
