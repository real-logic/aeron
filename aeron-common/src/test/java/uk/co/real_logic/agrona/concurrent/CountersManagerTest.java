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

import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import uk.co.real_logic.agrona.status.BufferPositionIndicator;
import uk.co.real_logic.agrona.status.BufferPositionReporter;

import java.util.function.BiConsumer;

import static java.nio.ByteBuffer.allocate;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@SuppressWarnings("unchecked")
public class CountersManagerTest
{
    private static final int NUMBER_OF_COUNTERS = 4;

    private UnsafeBuffer labelsBuffer = new UnsafeBuffer(allocate(NUMBER_OF_COUNTERS * CountersManager.LABEL_SIZE));
    private UnsafeBuffer counterBuffer = new UnsafeBuffer(allocate(NUMBER_OF_COUNTERS * CountersManager.COUNTER_SIZE));
    private CountersManager manager = new CountersManager(labelsBuffer, counterBuffer);
    private CountersManager otherManager = new CountersManager(labelsBuffer, counterBuffer);

    @Test
    public void managerShouldStoreLabels()
    {
        final int counterId = manager.allocate("abc");
        final BiConsumer<Integer, String> consumer = mock(BiConsumer.class);
        otherManager.forEach(consumer);
        verify(consumer).accept(counterId, "abc");
    }

    @Test
    public void managerShouldStoreMultipleLabels()
    {
        final int abc = manager.allocate("abc");
        final int def = manager.allocate("def");
        final int ghi = manager.allocate("ghi");

        final BiConsumer<Integer, String> consumer = mock(BiConsumer.class);
        otherManager.forEach(consumer);

        final InOrder inOrder = Mockito.inOrder(consumer);
        inOrder.verify(consumer).accept(abc, "abc");
        inOrder.verify(consumer).accept(def, "def");
        inOrder.verify(consumer).accept(ghi, "ghi");
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void shouldFreeAndReuseCounters()
    {
        final int abc = manager.allocate("abc");
        final int def = manager.allocate("def");
        final int ghi = manager.allocate("ghi");

        manager.free(def);

        final BiConsumer<Integer, String> consumer = mock(BiConsumer.class);
        otherManager.forEach(consumer);

        final InOrder inOrder = Mockito.inOrder(consumer);
        inOrder.verify(consumer).accept(abc, "abc");
        inOrder.verify(consumer).accept(ghi, "ghi");
        inOrder.verifyNoMoreInteractions();

        assertThat(manager.allocate("the next label"), is(def));
    }

    @Test(expected = IllegalArgumentException.class)
    public void managerShouldNotOverAllocateCounters()
    {
        manager.allocate("abc");
        manager.allocate("def");
        manager.allocate("ghi");
        manager.allocate("jkl");
        manager.allocate("mno");
    }

    @Test
    public void allocatedCountersCanBeMapped()
    {
        manager.allocate("def");

        final int id = manager.allocate("abc");
        final BufferPositionIndicator reader = new BufferPositionIndicator(counterBuffer, id);
        final BufferPositionReporter writer = new BufferPositionReporter(counterBuffer, id);
        writer.position(0xFFFFFFFFFL);
        assertThat(reader.position(), is(0xFFFFFFFFFL));
    }
}
