package uk.co.real_logic.aeron.common.concurrent;

/**
 * Atomic counter that is backed by an {@link AtomicBuffer} that can be read across threads and processes.
 */
public class AtomicCounter implements AutoCloseable
{
    private final AtomicBuffer buffer;
    private final int counterId;
    private final CountersManager countersManager;
    private final int offset;

    AtomicCounter(final AtomicBuffer buffer, final int counterId, final CountersManager countersManager)
    {
        this.buffer = buffer;
        this.counterId = counterId;
        this.countersManager = countersManager;
        this.offset = CountersManager.counterOffset(counterId);
        buffer.putLong(offset, 0);
    }

    /**
     * Perform an atomic increment that will not lose updates across threads.
     */
    public void increment()
    {
        buffer.getAndAddLong(offset, 1);
    }

    /**
     * Perform an atomic increment that is not safe across threads.
     */
    public void orderedIncrement()
    {
        buffer.putLongOrdered(offset, buffer.getLongVolatile(offset) + 1);
    }

    /**
     * Get the latest value for the counter.
     *
     * @return the latest value for the counter.
     */
    public long get()
    {
        return buffer.getLongVolatile(offset);
    }

    /**
     * Free the counter slot for reuse.
     */
    public void close()
    {
        countersManager.free(counterId);
    }
}
