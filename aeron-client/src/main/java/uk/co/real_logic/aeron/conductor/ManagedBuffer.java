package uk.co.real_logic.aeron.conductor;

import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;

public class ManagedBuffer
{
    private final String location;
    private final int offset;
    private final int length;
    private final AtomicBuffer buffer;
    private final BufferManager bufferManager;

    public ManagedBuffer(final String location,
                         final int offset,
                         final int length,
                         final AtomicBuffer buffer,
                         final BufferManager bufferManager)
    {
        this.location = location;
        this.offset = offset;
        this.length = length;
        this.buffer = buffer;
        this.bufferManager = bufferManager;
    }

    public AtomicBuffer buffer()
    {
        return buffer;
    }

    public int release()
    {
        return bufferManager.releaseBuffers(location, offset, length);
    }
}
