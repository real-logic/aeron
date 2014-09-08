package uk.co.real_logic.aeron;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor;

import static junit.framework.Assert.assertTrue;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class FragmentAssemblyAdapterTest
{
    private final DataHandler delegateDataHandler = mock(DataHandler.class);
    private final FragmentAssemblyAdapter adapter = new FragmentAssemblyAdapter(delegateDataHandler);

    @Test
    public void shouldPassThroughUnfragmentedMessage()
    {
        final AtomicBuffer srcBuffer = new AtomicBuffer(new byte[128]);
        final int offset = 8;
        final int length = 32;
        final int sessionId = 1234;
        final byte flags = FrameDescriptor.UNFRAGMENTED;

        adapter.onData(srcBuffer, offset, length, sessionId, flags);

        verify(delegateDataHandler, times(1)).onData(srcBuffer, offset, length, sessionId, flags);
    }

    @Test
    public void shouldAssembleTwoPartMessage()
    {
        final AtomicBuffer srcBuffer = new AtomicBuffer(new byte[1024]);
        final int offset = 0;
        final int length = srcBuffer.capacity() / 2;
        final int sessionId = 1234;

        srcBuffer.setMemory(0, length, (byte)65);
        srcBuffer.setMemory(length, length, (byte)66);

        adapter.onData(srcBuffer, offset, length, sessionId, FrameDescriptor.BEGIN_FRAG);
        adapter.onData(srcBuffer, length, length, sessionId, FrameDescriptor.END_FRAG);

        final ArgumentCaptor<AtomicBuffer> argument = ArgumentCaptor.forClass(AtomicBuffer.class);

        verify(delegateDataHandler, times(1)).onData(
            argument.capture(), eq(offset), eq(length * 2), eq(sessionId), eq(FrameDescriptor.UNFRAGMENTED));

        final AtomicBuffer capturedBuffer = argument.getValue();
        for (int i = 0; i < srcBuffer.capacity(); i++)
        {
            assertThat("same at i=" + i, capturedBuffer.getByte(i), is(srcBuffer.getByte(i)));
        }
    }

    @Test
    public void shouldAssembleFourPartMessage()
    {
        final AtomicBuffer srcBuffer = new AtomicBuffer(new byte[1024]);
        final int offset = 0;
        final int length = srcBuffer.capacity() / 4;
        final int sessionId = 1234;

        for (int i = 0; i < 4; i++)
        {
            srcBuffer.setMemory(i * length, length, (byte)(65 + i));
        }

        adapter.onData(srcBuffer, offset, length, sessionId, FrameDescriptor.BEGIN_FRAG);
        adapter.onData(srcBuffer, offset + length, length, sessionId, (byte)0);
        adapter.onData(srcBuffer, offset + (length * 2), length, sessionId, (byte)0);
        adapter.onData(srcBuffer, offset + (length * 3), length, sessionId, FrameDescriptor.END_FRAG);

        final ArgumentCaptor<AtomicBuffer> argument = ArgumentCaptor.forClass(AtomicBuffer.class);

        verify(delegateDataHandler, times(1)).onData(
            argument.capture(), eq(offset), eq(length * 4), eq(sessionId), eq(FrameDescriptor.UNFRAGMENTED));

        final AtomicBuffer capturedBuffer = argument.getValue();
        for (int i = 0; i < srcBuffer.capacity(); i++)
        {
            assertThat("same at i=" + i, capturedBuffer.getByte(i), is(srcBuffer.getByte(i)));
        }
    }

    @Test
    public void shouldFreeSessionBuffer()
    {
        final AtomicBuffer srcBuffer = new AtomicBuffer(new byte[1024]);
        final int offset = 0;
        final int length = srcBuffer.capacity() / 2;
        final int sessionId = 1234;

        srcBuffer.setMemory(0, length, (byte)65);
        srcBuffer.setMemory(length, length, (byte)66);

        assertFalse(adapter.freeSessionBuffer(sessionId));

        adapter.onData(srcBuffer, offset, length, sessionId, FrameDescriptor.BEGIN_FRAG);
        adapter.onData(srcBuffer, length, length, sessionId, FrameDescriptor.END_FRAG);

        assertTrue(adapter.freeSessionBuffer(sessionId));
        assertFalse(adapter.freeSessionBuffer(sessionId));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionIfEndFragComesBeforeBeginFrag()
    {
        final AtomicBuffer srcBuffer = new AtomicBuffer(new byte[1024]);
        final int offset = 0;
        final int length = srcBuffer.capacity() / 2;
        final int sessionId = 1234;
        final byte flags = FrameDescriptor.END_FRAG;

        adapter.onData(srcBuffer, offset, length, sessionId, flags);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionIfMidFragComesBeforeBeginFrag()
    {
        final AtomicBuffer srcBuffer = new AtomicBuffer(new byte[1024]);
        final int offset = 0;
        final int length = srcBuffer.capacity() / 2;
        final int sessionId = 1234;
        final byte flags = 0;

        adapter.onData(srcBuffer, offset, length, sessionId, flags);
    }
}
