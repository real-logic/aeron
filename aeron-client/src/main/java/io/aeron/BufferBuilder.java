/*
 * Copyright 2014-2019 Real Logic Ltd.
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

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Reusable Builder for appending a sequence of buffer fragments which grows internal capacity as needed.
 * <p>
 * The underlying buffer can be byte[] backed or a direct {@link ByteBuffer} if the isDirect param to the constructor
 * is true.
 * <p>
 * Similar in concept to {@link StringBuilder}.
 */
public class BufferBuilder
{
    private final boolean isDirect;
    private int limit = 0;
    private final UnsafeBuffer buffer;

    /**
     * Construct a buffer builder with an initial capacity of zero and isDirect false.
     */
    public BufferBuilder()
    {
        this(0, false);
    }

    /**
     * Construct a buffer builder with an initial capacity and isDirect false.
     *
     * @param initialCapacity at which the capacity will start.
     */
    public BufferBuilder(final int initialCapacity)
    {
        this(initialCapacity, false);
    }

    /**
     * Construct a buffer builder with an initial capacity.
     *
     * @param initialCapacity at which the capacity will start.
     * @param isDirect        is the underlying buffer to be a direct {@link ByteBuffer}
     */
    public BufferBuilder(final int initialCapacity, final boolean isDirect)
    {
        this.isDirect = isDirect;
        if (isDirect)
        {
            final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(initialCapacity);
            byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
            this.buffer = new UnsafeBuffer(byteBuffer);
        }
        else
        {
            buffer = new UnsafeBuffer(new byte[initialCapacity]);
        }
    }

    /**
     * The current capacity of the buffer.
     *
     * @return the current capacity of the buffer.
     */
    public int capacity()
    {
        return buffer.capacity();
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
        if (limit < 0 || limit >= buffer.capacity())
        {
            throw new IllegalArgumentException(
                "limit outside range: capacity=" + buffer.capacity() + " limit=" + limit);
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
        return buffer;
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
        resize(Math.max(BufferBuilderUtil.MIN_ALLOCATED_CAPACITY, limit));

        return this;
    }

    /**
     * Append a source buffer to the end of the internal buffer, resizing the internal buffer as required.
     *
     * @param srcBuffer from which to copy.
     * @param srcOffset in the source buffer from which to copy.
     * @param length    in bytes to copy from the source buffer.
     * @return the builder for fluent API usage.
     */
    public BufferBuilder append(final DirectBuffer srcBuffer, final int srcOffset, final int length)
    {
        ensureCapacity(length);

        buffer.putBytes(limit, srcBuffer, srcOffset, length);
        limit += length;

        return this;
    }

    private void ensureCapacity(final int additionalCapacity)
    {
        final long requiredCapacity = (long)limit + additionalCapacity;

        if (requiredCapacity > BufferBuilderUtil.MAX_CAPACITY)
        {
            throw new IllegalStateException(
                "max capacity exceeded: limit=" + limit + " required=" + requiredCapacity);
        }

        final int capacity = buffer.capacity();
        if (requiredCapacity > capacity)
        {
            resize(BufferBuilderUtil.findSuitableCapacity(capacity, (int)requiredCapacity));
        }
    }

    private void resize(final int newCapacity)
    {
        if (isDirect)
        {
            final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(newCapacity);
            byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
            buffer.getBytes(0, byteBuffer, 0, limit);
            buffer.wrap(byteBuffer);
        }
        else
        {
            buffer.wrap(Arrays.copyOf(buffer.byteArray(), newCapacity));
        }
    }

}
