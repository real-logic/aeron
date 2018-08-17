package io.aeron;

final class BufferBuilderUtil
{
    /**
     * Maximum capacity to which the buffer can grow.
     */
    static final int MAX_CAPACITY = Integer.MAX_VALUE - 8;
    /**
     * Initial minimum capacity for the internal buffer when used, zero if not used.
     */
    static final int MIN_ALLOCATED_CAPACITY = 4096;

    private BufferBuilderUtil()
    {
    }

    static int findSuitableCapacity(final int currentCapacity, final int requiredCapacity)
    {
        int capacity = currentCapacity;

        do
        {
            final int candidateCapacity = capacity + (capacity >> 1);
            final int newCapacity = Math.max(candidateCapacity, MIN_ALLOCATED_CAPACITY);

            if (candidateCapacity < 0 || newCapacity > MAX_CAPACITY)
            {
                if (capacity == MAX_CAPACITY)
                {
                    throw new IllegalStateException("max capacity reached: " + MAX_CAPACITY);
                }

                capacity = MAX_CAPACITY;
            }
            else
            {
                capacity = newCapacity;
            }
        }
        while (capacity < requiredCapacity);

        return capacity;
    }
}