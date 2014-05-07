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
package uk.co.real_logic.aeron.util.status;

import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

import java.util.function.BiConsumer;

import static java.nio.ByteOrder.nativeOrder;
import static uk.co.real_logic.aeron.util.BitUtil.SIZE_OF_INT;
import static uk.co.real_logic.aeron.util.BitUtil.SIZE_OF_LONG;

/**
 * .
 */
public class StatusBufferManager
{

    private final AtomicBuffer descriptorBuffer;
    private final int counterCapacity;

    private int descriptorOffset;
    private int idCounter;

    public StatusBufferManager(final AtomicBuffer descriptorBuffer, final AtomicBuffer counterBuffer)
    {
        this.descriptorBuffer = descriptorBuffer;
        this.counterCapacity = counterBuffer.capacity();
        descriptorOffset = 0;

        // deliberately start at 1, so reading a 0 means the end of the written data
        idCounter = 1;
    }

    public int registerCounter(String label)
    {
        if (counterOffset(idCounter) >= counterCapacity)
        {
            throw new IllegalArgumentException("Unable to register counter, run out of space in the counter buffer");
        }

        descriptorBuffer.putInt(descriptorOffset, idCounter);
        descriptorOffset += SIZE_OF_INT;
        descriptorOffset += descriptorBuffer.putString(descriptorOffset, label, nativeOrder());
        return idCounter++;
    }

    public int counterOffset(int id)
    {
        // ids start at 1
        return (id - 1) * SIZE_OF_LONG;
    }

    public void listDescriptors(final BiConsumer<Integer, String> consumer)
    {
        int offset = 0;
        int id;
        while ((id = descriptorBuffer.getInt(offset)) != 0)
        {
            offset += SIZE_OF_INT;
            final int length = descriptorBuffer.getInt(offset, nativeOrder());
            final String label = descriptorBuffer.getString(offset, length);
            consumer.accept(id, label);
            offset += SIZE_OF_INT + length;
        }
    }

}
