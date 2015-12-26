/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
package uk.co.real_logic.aeron;

import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.util.Arrays;

/**
 * Builder for appending buffers that grows capacity as needed.
 */
public class BufferBuilder
{
    public static final int INITIAL_CAPACITY = 4096;

    private final MutableDirectBuffer mutableDirectBuffer;

    private byte[] buffer;
    private int limit = 0;
    private int capacity;

    /**
     * Construct a buffer builder with a default growth increment of {@link #INITIAL_CAPACITY}
     */
    public BufferBuilder()
    {
        this(INITIAL_CAPACITY);
    }

    /**
     * Construct a buffer builder with an initial capacity that will be rounded up to the nearest power of 2.
     *
     * @param initialCapacity at which the capacity will start.
     */
    public BufferBuilder(final int initialCapacity)
    {
        capacity = BitUtil.findNextPositivePowerOfTwo(initialCapacity);
        buffer = new byte[capacity];
        mutableDirectBuffer = new UnsafeBuffer(buffer);
    }

    /**
     * The current capacity of the buffer.
     *
     * @return the current capacity of the buffer.
     */
    public int capacity()
    {
        return capacity;
    }

    /**
     * The current limit of the buffer that has been used by append operations.
     *
     * @return the current limit of the buffer that has been used by append operations.
     */
    public int limit()
    {
        return limit;
    }

    /**
     * Set this limit for this buffer as the position at which the next append operation will occur.
     *
     * @param limit to be the new value.
     */
    public void limit(final int limit)
    {
        if (limit < 0 || limit >= capacity)
        {
            throw new IllegalArgumentException(String.format(
                "Limit outside range: capacity=%d limit=%d", capacity, limit));
        }

        this.limit = limit;
    }

    /**
     * The {@link MutableDirectBuffer} that encapsulates the internal buffer.
     *
     * @return the {@link MutableDirectBuffer} that encapsulates the internal buffer.
     */
    public MutableDirectBuffer buffer()
    {
        return mutableDirectBuffer;
    }

    /**
     * Reset the builder to restart append operations. The internal buffer does not shrink.
     *
     * @return the builder for fluent API usage.
     */
    public BufferBuilder reset()
    {
        limit = 0;
        return this;
    }

    /**
     * Compact the buffer to reclaim unused space above the limit.
     *
     * @return the builder for fluent API usage.
     */
    public BufferBuilder compact()
    {
        capacity = Math.max(INITIAL_CAPACITY, BitUtil.findNextPositivePowerOfTwo(limit));
        buffer = Arrays.copyOf(buffer, capacity);
        mutableDirectBuffer.wrap(buffer);

        return this;
    }

    /**
     * Append a source buffer to the end of the internal buffer, resizing the internal buffer as required.
     *
     * @param srcBuffer from which to copy.
     * @param srcOffset in the source buffer from which to copy.
     * @param length in bytes to copy from the source buffer.
     * @return the builder for fluent API usage.
     */
    public BufferBuilder append(final DirectBuffer srcBuffer, final int srcOffset, final int length)
    {
        ensureCapacity(length);

        srcBuffer.getBytes(srcOffset, buffer, limit, length);
        limit += length;

        return this;
    }

    private void ensureCapacity(final int additionalCapacity)
    {
        final int requiredCapacity = limit + additionalCapacity;

        if (requiredCapacity < 0)
        {
            final String s = String.format("Insufficient capacity: limit=%d additional=%d", limit, additionalCapacity);
            throw new IllegalStateException(s);
        }

        if (requiredCapacity > capacity)
        {
            final int newCapacity = findSuitableCapacity(capacity, requiredCapacity);
            final byte[] newBuffer = Arrays.copyOf(buffer, newCapacity);

            capacity = newCapacity;
            buffer = newBuffer;
            mutableDirectBuffer.wrap(newBuffer);
        }
    }

    private static int findSuitableCapacity(int capacity, final int requiredCapacity)
    {
        do
        {
            capacity <<= 1;
        }
        while (capacity < requiredCapacity);

        return capacity;
    }
}
