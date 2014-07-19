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
package uk.co.real_logic.aeron.common.status;

import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;

import java.util.function.BiConsumer;

import static java.nio.ByteOrder.nativeOrder;
import static uk.co.real_logic.aeron.common.BitUtil.SIZE_OF_INT;
import static uk.co.real_logic.aeron.common.BitUtil.SIZE_OF_LONG;

/**
 * Manages the registration of counters.
 */
public class CountersManager
{
    private final AtomicBuffer labelsBuffer;
    private final int countersCapacity;

    private int labelsOffset;
    private int idCounter;

    /**
     * Create a new counter buffer manager over two buffers.
     *
     * @param labelsBuffer containing the human readable labels for the counters.
     * @param countersBuffer containing the values of the counters themselves.
     */
    public CountersManager(final AtomicBuffer labelsBuffer, final AtomicBuffer countersBuffer)
    {
        this.labelsBuffer = labelsBuffer;
        this.countersCapacity = countersBuffer.capacity();
        labelsOffset = 0;

        // deliberately start at 1, so reading a 0 means the end of the written data
        idCounter = 1;
    }

    /**
     * Register a new counters with a given label.
     *
     * @param label to describe the counter.
     * @return the id allocated for the counter.
     */
    public int registerCounter(final String label)
    {
        if (counterOffset(idCounter) >= countersCapacity)
        {
            throw new IllegalArgumentException("Unable to register counter, counter buffer is full");
        }

        labelsBuffer.putInt(labelsOffset, idCounter);
        labelsOffset += SIZE_OF_INT;
        labelsOffset += labelsBuffer.putString(labelsOffset, label, nativeOrder());

        return idCounter++;
    }

    /**
     * The offset in the counter buffer for a given id.
     *
     * @param id for which the offset should be provided.
     * @return the offset in the counter buffer.
     */
    public static int counterOffset(int id)
    {
        // ids start at 1
        return (id - 1) * SIZE_OF_LONG;
    }

    /**
     * Iterate over all labels in the label buffer.
     *
     * @param consumer function to be called for each label.
     */
    public void forEachLabel(final BiConsumer<Integer, String> consumer)
    {
        int offset = 0;
        int id;

        while ((id = labelsBuffer.getInt(offset)) != 0)
        {
            offset += SIZE_OF_INT;
            final int length = labelsBuffer.getInt(offset, nativeOrder());
            final String label = labelsBuffer.getString(offset, length);
            consumer.accept(id, label);
            offset += SIZE_OF_INT + length;
        }
    }
}
