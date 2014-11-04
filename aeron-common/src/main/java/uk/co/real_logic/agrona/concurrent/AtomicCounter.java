package uk.co.real_logic.agrona.concurrent;

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
        buffer.addLongOrdered(offset, 1);
    }

    /**
     * Set the counter with volatile semantics.
     *
     * @param value to be set with volatile semantics.
     */
    public void set(final long value)
    {
        buffer.putLongVolatile(offset, value);
    }

    /**
     * Set the counter with ordered semantics.
     *
     * @param value to be set with ordered semantics.
     */
    public void setOrdered(final long value)
    {
        buffer.putLongOrdered(offset, value);
    }

    /**
     * Add an increment to the counter with ordered store semantics.
     *
     * @param increment to be added with ordered store semantics.
     */
    public void addOrdered(final long increment)
    {
        buffer.addLongOrdered(offset, increment);
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
