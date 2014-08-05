package uk.co.real_logic.aeron.common.concurrent;

public class Counter
{
    private final AtomicBuffer buffer;
    private final int counterId;
    private final CountersManager countersManager;
    private final int offset;

    Counter(final AtomicBuffer buffer, final int counterId, final CountersManager countersManager)
    {
        this.buffer = buffer;
        this.counterId = counterId;
        this.countersManager = countersManager;
        this.offset = CountersManager.counterOffset(counterId);
        buffer.putLong(offset, 0);
    }

    public void increment()
    {
        buffer.getAndAddLong(offset, 1);
    }

    public void close()
    {
        countersManager.free(counterId);
    }
}
