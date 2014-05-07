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

import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import uk.co.real_logic.aeron.util.BitUtil;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

import java.nio.ByteBuffer;
import java.util.function.BiConsumer;

import static java.nio.ByteBuffer.allocate;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static uk.co.real_logic.aeron.util.BitUtil.SIZE_OF_INT;
import static uk.co.real_logic.aeron.util.BitUtil.SIZE_OF_LONG;

/**
 * .
 */
public class StatusBufferTest
{
    private AtomicBuffer descriptorBuffer = new AtomicBuffer(allocate(512));
    private AtomicBuffer counterBuffer = new AtomicBuffer(allocate(3 * SIZE_OF_LONG));
    private StatusBufferManager manager = new StatusBufferManager(descriptorBuffer, counterBuffer);
    private StatusBufferManager otherManager = new StatusBufferManager(descriptorBuffer, counterBuffer);

    @Test
    public void managerShouldStoreLabels()
    {
        int counterId = manager.registerCounter("abc");
        BiConsumer<Integer, String> consumer = mock(BiConsumer.class);
        otherManager.listDescriptors(consumer);
        verify(consumer).accept(counterId, "abc");
    }

    @Test
    public void managerShouldStoreMultipleLabels()
    {
        int abc = manager.registerCounter("abc");
        int def = manager.registerCounter("def");
        int ghi = manager.registerCounter("ghi");

        BiConsumer<Integer, String> consumer = mock(BiConsumer.class);
        otherManager.listDescriptors(consumer);

        InOrder inOrder = Mockito.inOrder(consumer);
        inOrder.verify(consumer).accept(abc, "abc");
        inOrder.verify(consumer).accept(def, "def");
        inOrder.verify(consumer).accept(ghi, "ghi");
        inOrder.verifyNoMoreInteractions();
    }

    @Test(expected = IllegalArgumentException.class)
    public void managerShouldNotOverAllocateCounters()
    {
        manager.registerCounter("abc");
        manager.registerCounter("def");
        manager.registerCounter("ghi");
        manager.registerCounter("jkl");
    }

    @Test
    public void registeredCountersCanBeMapped()
    {
        int id = manager.registerCounter("abc");
        int offset = manager.counterOffset(id);
        Counter readCounter = new Counter(counterBuffer, offset);
        Counter writeCounter = new Counter(counterBuffer, offset);
        writeCounter.write(0xFFFFFFFFFL);
        assertThat(readCounter.read(), is(0xFFFFFFFFFL));
    }

}
