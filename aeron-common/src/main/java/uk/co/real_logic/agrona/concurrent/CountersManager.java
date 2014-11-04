/*
 * Copyright 2014 Real Logic Ltd.
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
package uk.co.real_logic.agrona.concurrent;

import uk.co.real_logic.agrona.BitUtil;

import java.util.Deque;
import java.util.LinkedList;
import java.util.function.BiConsumer;

import static java.nio.ByteOrder.nativeOrder;
import static uk.co.real_logic.agrona.BitUtil.SIZE_OF_INT;

/**
 * Manages the allocation and freeing of counters.
 */
public class CountersManager
{
    public static final int LABEL_SIZE = 1024;
    public static final int COUNTER_SIZE = BitUtil.CACHE_LINE_SIZE;
    public static final int UNREGISTERED_LABEL_SIZE = -1;

    private final AtomicBuffer labelsBuffer;
    private final AtomicBuffer countersBuffer;
    private final Deque<Integer> freeList = new LinkedList<>();

    private int idHighWaterMark = -1;

    /**
     * Create a new counter buffer manager over two buffers.
     *
     * @param labelsBuffer containing the human readable labels for the counters.
     * @param countersBuffer containing the values of the counters themselves.
     */
    public CountersManager(final AtomicBuffer labelsBuffer, final AtomicBuffer countersBuffer)
    {
        this.labelsBuffer = labelsBuffer;
        this.countersBuffer = countersBuffer;
    }

    /**
     * Allocate a new counter with a given label.
     *
     * @param label to describe the counter.
     * @return the id allocated for the counter.
     */
    public int allocate(final String label)
    {
        final int counterId = counterId();
        final int labelsOffset = labelOffset(counterId);
        if ((counterOffset(counterId) + COUNTER_SIZE) > countersBuffer.capacity())
        {
            throw new IllegalArgumentException("Unable to allocated counter, counter buffer is full");
        }

        if ((labelsOffset + LABEL_SIZE) > labelsBuffer.capacity())
        {
            throw new IllegalArgumentException("Unable to allocate counter, labels buffer is full");
        }

        labelsBuffer.putStringUtf8(labelsOffset, label, nativeOrder(), LABEL_SIZE - SIZE_OF_INT);

        return counterId;
    }

    public AtomicCounter newCounter(final String label)
    {
        return new AtomicCounter(countersBuffer, allocate(label), this);
    }

    /**
     * Free the counter identified by counterId.
     *
     * @param counterId the counter to freed
     */
    public void free(final int counterId)
    {
        labelsBuffer.putInt(labelOffset(counterId), UNREGISTERED_LABEL_SIZE);
        countersBuffer.putLongOrdered(counterOffset(counterId), 0L);
        freeList.push(counterId);
    }

    /**
     * The offset in the counter buffer for a given id.
     *
     * @param id for which the offset should be provided.
     * @return the offset in the counter buffer.
     */
    public static int counterOffset(int id)
    {
        return id * COUNTER_SIZE;
    }

    /**
     * Iterate over all labels in the label buffer.
     *
     * @param consumer function to be called for each label.
     */
    public void forEach(final BiConsumer<Integer, String> consumer)
    {
        int labelsOffset = 0;
        int size;
        int id = 0;

        while ((size = labelsBuffer.getInt(labelsOffset)) != 0)
        {
            if (size != UNREGISTERED_LABEL_SIZE)
            {
                final String label = labelsBuffer.getStringUtf8(labelsOffset, nativeOrder());
                consumer.accept(id, label);
            }

            labelsOffset += LABEL_SIZE;
            id++;
        }
    }

    /**
     * Set an {@link AtomicCounter} value based on counterId.
     *
     * @param counterId to be set.
     * @param value to set for the counter.
     */
    public void setCounterValue(final int counterId, final long value)
    {
        countersBuffer.putLongOrdered(counterOffset(counterId), value);
    }

    private int labelOffset(final int counterId)
    {
        return counterId * LABEL_SIZE;
    }

    private int counterId()
    {
        if (freeList.isEmpty())
        {
            return ++idHighWaterMark;
        }

        return freeList.pop();
    }
}
