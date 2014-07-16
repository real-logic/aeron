package uk.co.real_logic.aeron.conductor;

import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

public class LogInformation
{
    private final String location;
    private final int offset;
    private final int length;
    private final AtomicBuffer buffer;
    private final BufferLifecycleStrategy strategy;

    public LogInformation(
            final String location,
            final int offset,
            final int length,
            final AtomicBuffer buffer,
            final BufferLifecycleStrategy strategy)
    {
        this.location = location;
        this.offset = offset;
        this.length = length;
        this.buffer = buffer;
        this.strategy = strategy;
    }

    public AtomicBuffer buffer()
    {
        return buffer;
    }

    public int releaseBuffer()
    {
        return strategy.releaseBuffers(location, offset, length);
    }
}
