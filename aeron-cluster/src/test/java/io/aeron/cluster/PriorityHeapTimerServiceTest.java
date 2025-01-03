/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.cluster;

import io.aeron.cluster.TimerService.TimerHandler;
import io.aeron.cluster.TimerService.TimerSnapshotTaker;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.MutableInteger;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.*;

import static io.aeron.cluster.TimerService.POLL_LIMIT;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PriorityHeapTimerServiceTest
{
    @Test
    void throwsNullPointerExceptionIfTimerHandlerIsNull()
    {
        assertThrows(NullPointerException.class, () -> new PriorityHeapTimerService(null));
    }

    @Test
    void pollIsANoOpWhenNoTimersWhereScheduled()
    {
        final TimerHandler timerHandler = mock(TimerHandler.class);
        final PriorityHeapTimerService timerService = new PriorityHeapTimerService(timerHandler);

        assertEquals(0, timerService.poll(Long.MIN_VALUE));

        verifyNoInteractions(timerHandler);
    }

    @Test
    void pollIsANoOpWhenNoScheduledTimersAreExpired()
    {
        final TimerHandler timerHandler = mock(TimerHandler.class);
        final PriorityHeapTimerService timerService = new PriorityHeapTimerService(timerHandler);
        timerService.scheduleTimerForCorrelationId(1, 100);

        assertEquals(0, timerService.poll(7));

        verifyNoInteractions(timerHandler);
    }

    @Test
    void pollShouldNotExpireTimerIfHandlerReturnsFalse()
    {
        final TimerHandler timerHandler = mock(TimerHandler.class);
        final PriorityHeapTimerService timerService = new PriorityHeapTimerService(timerHandler);
        timerService.scheduleTimerForCorrelationId(1, 100);

        assertEquals(0, timerService.poll(Long.MAX_VALUE));

        verify(timerHandler).onTimerEvent(1);
        verifyNoMoreInteractions(timerHandler);
    }

    @Test
    void pollShouldExpireSingleTimer()
    {
        final TimerHandler timerHandler = mock(TimerHandler.class);
        when(timerHandler.onTimerEvent(anyLong())).thenReturn(true);
        final PriorityHeapTimerService timerService = new PriorityHeapTimerService(timerHandler);
        timerService.scheduleTimerForCorrelationId(1, 100);

        assertEquals(1, timerService.poll(200));
        assertEquals(0, timerService.poll(200));

        verify(timerHandler).onTimerEvent(1);
        verifyNoMoreInteractions(timerHandler);
    }

    @Test
    void pollShouldRemovedExpiredTimers()
    {
        final TimerHandler timerHandler = mock(TimerHandler.class);
        when(timerHandler.onTimerEvent(anyLong())).thenReturn(true);
        final PriorityHeapTimerService timerService = new PriorityHeapTimerService(timerHandler);
        timerService.scheduleTimerForCorrelationId(1, 100);
        timerService.scheduleTimerForCorrelationId(2, 200);
        timerService.scheduleTimerForCorrelationId(3, 300);
        timerService.scheduleTimerForCorrelationId(4, 400);
        timerService.scheduleTimerForCorrelationId(5, 500);

        assertEquals(2, timerService.poll(200));
        assertEquals(3, timerService.poll(500));

        final InOrder inOrder = inOrder(timerHandler);
        inOrder.verify(timerHandler).onTimerEvent(1);
        inOrder.verify(timerHandler).onTimerEvent(2);
        inOrder.verify(timerHandler).onTimerEvent(3);
        inOrder.verify(timerHandler).onTimerEvent(4);
        inOrder.verify(timerHandler).onTimerEvent(5);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void pollShouldExpireTimersInOrderOfDeadlineButWithinTheDeadlineOrderIsUndefined()
    {
        final TimerHandler timerHandler = mock(TimerHandler.class);
        when(timerHandler.onTimerEvent(anyLong())).thenReturn(true);
        final PriorityHeapTimerService timerService = new PriorityHeapTimerService(timerHandler);
        timerService.scheduleTimerForCorrelationId(1, 10);
        timerService.scheduleTimerForCorrelationId(4, 3);
        timerService.scheduleTimerForCorrelationId(2, 0);
        timerService.scheduleTimerForCorrelationId(3, 3);
        timerService.scheduleTimerForCorrelationId(5, 0);
        timerService.scheduleTimerForCorrelationId(6, 11);

        assertEquals(5, timerService.poll(10));

        final InOrder inOrder = inOrder(timerHandler);
        inOrder.verify(timerHandler).onTimerEvent(2);
        inOrder.verify(timerHandler).onTimerEvent(5);
        inOrder.verify(timerHandler).onTimerEvent(3);
        inOrder.verify(timerHandler).onTimerEvent(4);
        inOrder.verify(timerHandler).onTimerEvent(1);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void cancelTimerByCorrelationIdIsANoOpIfNoTimersRegistered()
    {
        final PriorityHeapTimerService timerService = new PriorityHeapTimerService(mock(TimerHandler.class));

        assertFalse(timerService.cancelTimerByCorrelationId(100));
    }

    @Test
    void cancelTimerByCorrelationIdReturnsFalseForUnknownCorrelationId()
    {
        final PriorityHeapTimerService timerService = new PriorityHeapTimerService(mock(TimerHandler.class));
        timerService.scheduleTimerForCorrelationId(1, 100);
        timerService.scheduleTimerForCorrelationId(7, 50);

        assertFalse(timerService.cancelTimerByCorrelationId(3));
    }

    @Test
    void cancelTimerByCorrelationIdReturnsTrueAfterCancellingTheTimer()
    {
        final TimerHandler timerHandler = mock(TimerHandler.class);
        when(timerHandler.onTimerEvent(anyLong())).thenReturn(true);
        final PriorityHeapTimerService timerService = new PriorityHeapTimerService(timerHandler);
        timerService.scheduleTimerForCorrelationId(1, 100);
        timerService.scheduleTimerForCorrelationId(7, 50);
        timerService.scheduleTimerForCorrelationId(2, 90);

        assertTrue(timerService.cancelTimerByCorrelationId(7));

        assertEquals(2, timerService.poll(111));

        final InOrder inOrder = inOrder(timerHandler);
        inOrder.verify(timerHandler).onTimerEvent(2);
        inOrder.verify(timerHandler).onTimerEvent(1);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void cancelTimerByCorrelationIdReturnsTrueAfterCancellingTheLastTimer()
    {
        final TimerHandler timerHandler = mock(TimerHandler.class);
        when(timerHandler.onTimerEvent(anyLong())).thenReturn(true);
        final PriorityHeapTimerService timerService = new PriorityHeapTimerService(timerHandler);
        timerService.scheduleTimerForCorrelationId(1, 100);
        timerService.scheduleTimerForCorrelationId(7, 50);
        timerService.scheduleTimerForCorrelationId(2, 90);

        assertTrue(timerService.cancelTimerByCorrelationId(1));

        assertEquals(2, timerService.poll(111));

        final InOrder inOrder = inOrder(timerHandler);
        inOrder.verify(timerHandler).onTimerEvent(7);
        inOrder.verify(timerHandler).onTimerEvent(2);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void cancelTimerByCorrelationIdAfterPoll()
    {
        final TimerHandler timerHandler = mock(TimerHandler.class);
        when(timerHandler.onTimerEvent(anyLong())).thenReturn(true);
        final PriorityHeapTimerService timerService = new PriorityHeapTimerService(timerHandler);
        timerService.scheduleTimerForCorrelationId(1, 30);
        timerService.scheduleTimerForCorrelationId(5, 15);
        timerService.scheduleTimerForCorrelationId(2, 20);
        timerService.scheduleTimerForCorrelationId(4, 40);

        assertEquals(1, timerService.poll(19));
        assertTrue(timerService.cancelTimerByCorrelationId(1));

        assertEquals(2, timerService.poll(40));
        final InOrder inOrder = inOrder(timerHandler);
        inOrder.verify(timerHandler).onTimerEvent(5);
        inOrder.verify(timerHandler).onTimerEvent(2);
        inOrder.verify(timerHandler).onTimerEvent(4);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void scheduleTimerForAnExistingCorrelationIdIsANoOpIfDeadlineDoesNotChange()
    {
        final TimerHandler timerHandler = mock(TimerHandler.class);
        when(timerHandler.onTimerEvent(anyLong())).thenReturn(true);
        final PriorityHeapTimerService timerService = new PriorityHeapTimerService(timerHandler);
        timerService.scheduleTimerForCorrelationId(3, 30);
        timerService.scheduleTimerForCorrelationId(5, 50);
        timerService.scheduleTimerForCorrelationId(7, 70);

        timerService.scheduleTimerForCorrelationId(5, 50);

        assertEquals(3, timerService.poll(70));
        final InOrder inOrder = inOrder(timerHandler);
        inOrder.verify(timerHandler).onTimerEvent(3);
        inOrder.verify(timerHandler).onTimerEvent(5);
        inOrder.verify(timerHandler).onTimerEvent(7);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void scheduleTimerForAnExistingCorrelationIdShouldShiftEntryUpWhenDeadlineIsDecreasing()
    {
        final TimerHandler timerHandler = mock(TimerHandler.class);
        when(timerHandler.onTimerEvent(anyLong())).thenReturn(true);
        final PriorityHeapTimerService timerService = new PriorityHeapTimerService(timerHandler);
        timerService.scheduleTimerForCorrelationId(1, 10);
        timerService.scheduleTimerForCorrelationId(2, 10);
        timerService.scheduleTimerForCorrelationId(3, 30);
        timerService.scheduleTimerForCorrelationId(4, 30);
        timerService.scheduleTimerForCorrelationId(5, 50);

        timerService.scheduleTimerForCorrelationId(5, 10);

        assertEquals(3, timerService.poll(10));
        final InOrder inOrder = inOrder(timerHandler);
        inOrder.verify(timerHandler).onTimerEvent(1);
        inOrder.verify(timerHandler).onTimerEvent(2);
        inOrder.verify(timerHandler).onTimerEvent(5);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void scheduleTimerForAnExistingCorrelationIdShouldShiftEntryDownWhenDeadlineIsIncreasing()
    {
        final TimerHandler timerHandler = mock(TimerHandler.class);
        when(timerHandler.onTimerEvent(anyLong())).thenReturn(true);
        final PriorityHeapTimerService timerService = new PriorityHeapTimerService(timerHandler);
        timerService.scheduleTimerForCorrelationId(1, 10);
        timerService.scheduleTimerForCorrelationId(2, 10);
        timerService.scheduleTimerForCorrelationId(3, 30);
        timerService.scheduleTimerForCorrelationId(4, 30);
        timerService.scheduleTimerForCorrelationId(5, 50);

        timerService.scheduleTimerForCorrelationId(1, 30);

        assertEquals(4, timerService.poll(30));
        final InOrder inOrder = inOrder(timerHandler);
        inOrder.verify(timerHandler).onTimerEvent(2);
        inOrder.verify(timerHandler).onTimerEvent(4);
        inOrder.verify(timerHandler).onTimerEvent(1);
        inOrder.verify(timerHandler).onTimerEvent(3);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void pollShouldStopAfterPollLimitIsReached()
    {
        final TimerHandler timerHandler = mock(TimerHandler.class);
        when(timerHandler.onTimerEvent(anyLong())).thenReturn(true);
        final PriorityHeapTimerService timerService = new PriorityHeapTimerService(timerHandler);
        for (int i = 0; i < POLL_LIMIT * 2; i++)
        {
            timerService.scheduleTimerForCorrelationId(i, i);
        }

        assertEquals(POLL_LIMIT, timerService.poll(Long.MAX_VALUE));

        verify(timerHandler, times(POLL_LIMIT)).onTimerEvent(anyLong());
        verifyNoMoreInteractions(timerHandler);
    }

    @Test
    void snapshotProcessesAllScheduledTimers()
    {
        final TimerHandler timerHandler = mock(TimerHandler.class);
        when(timerHandler.onTimerEvent(anyLong())).thenReturn(true);
        final TimerSnapshotTaker snapshotTaker = mock(TimerSnapshotTaker.class);
        final PriorityHeapTimerService timerService = new PriorityHeapTimerService(timerHandler);
        timerService.scheduleTimerForCorrelationId(1, 10);
        timerService.scheduleTimerForCorrelationId(2, 14);
        timerService.scheduleTimerForCorrelationId(3, 30);
        timerService.scheduleTimerForCorrelationId(4, 29);
        timerService.scheduleTimerForCorrelationId(5, 15);

        assertEquals(2, timerService.poll(14));

        timerService.snapshot(snapshotTaker);

        final InOrder inOrder = inOrder(timerHandler, snapshotTaker);
        inOrder.verify(timerHandler).onTimerEvent(1);
        inOrder.verify(timerHandler).onTimerEvent(2);
        inOrder.verify(snapshotTaker).snapshotTimer(5, 15);
        inOrder.verify(snapshotTaker).snapshotTimer(4, 29);
        inOrder.verify(snapshotTaker).snapshotTimer(3, 30);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void expireThanCancelTimer()
    {
        final TimerHandler timerHandler = mock(TimerHandler.class);
        when(timerHandler.onTimerEvent(anyLong())).thenReturn(true);
        final PriorityHeapTimerService timerService = new PriorityHeapTimerService(timerHandler);
        timerService.scheduleTimerForCorrelationId(1, 100);
        timerService.scheduleTimerForCorrelationId(2, 2);
        timerService.scheduleTimerForCorrelationId(3, 30);
        timerService.scheduleTimerForCorrelationId(4, 4);
        timerService.scheduleTimerForCorrelationId(5, 50);

        assertEquals(2, timerService.poll(5));

        assertTrue(timerService.cancelTimerByCorrelationId(1));

        assertEquals(2, timerService.poll(1000));

        final InOrder inOrder = inOrder(timerHandler);
        inOrder.verify(timerHandler).onTimerEvent(2);
        inOrder.verify(timerHandler).onTimerEvent(4);
        inOrder.verify(timerHandler).onTimerEvent(3);
        inOrder.verify(timerHandler).onTimerEvent(5);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void moveUpAnExistingTimerAndCancelAnotherOne()
    {
        final TimerHandler timerHandler = mock(TimerHandler.class);
        when(timerHandler.onTimerEvent(anyLong())).thenReturn(true);
        final PriorityHeapTimerService timerService = new PriorityHeapTimerService(timerHandler);
        timerService.scheduleTimerForCorrelationId(1, 1);
        timerService.scheduleTimerForCorrelationId(2, 1);
        timerService.scheduleTimerForCorrelationId(3, 3);
        timerService.scheduleTimerForCorrelationId(4, 4);
        timerService.scheduleTimerForCorrelationId(5, 5);
        timerService.scheduleTimerForCorrelationId(5, 1);

        assertTrue(timerService.cancelTimerByCorrelationId(3));

        assertEquals(4, timerService.poll(5));

        final InOrder inOrder = inOrder(timerHandler);
        inOrder.verify(timerHandler).onTimerEvent(1);
        inOrder.verify(timerHandler).onTimerEvent(2);
        inOrder.verify(timerHandler).onTimerEvent(5);
        inOrder.verify(timerHandler).onTimerEvent(4);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void moveDownAnExistingTimerAndCancelAnotherOne()
    {
        final TimerHandler timerHandler = mock(TimerHandler.class);
        when(timerHandler.onTimerEvent(anyLong())).thenReturn(true);
        final PriorityHeapTimerService timerService = new PriorityHeapTimerService(timerHandler);
        timerService.scheduleTimerForCorrelationId(1, 1);
        timerService.scheduleTimerForCorrelationId(2, 1);
        timerService.scheduleTimerForCorrelationId(3, 3);
        timerService.scheduleTimerForCorrelationId(4, 4);
        timerService.scheduleTimerForCorrelationId(5, 5);
        timerService.scheduleTimerForCorrelationId(1, 5);

        assertTrue(timerService.cancelTimerByCorrelationId(3));

        assertEquals(4, timerService.poll(5));

        final InOrder inOrder = inOrder(timerHandler);
        inOrder.verify(timerHandler).onTimerEvent(2);
        inOrder.verify(timerHandler).onTimerEvent(4);
        inOrder.verify(timerHandler).onTimerEvent(1);
        inOrder.verify(timerHandler).onTimerEvent(5);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void cancelExpiredTimerIsANoOp()
    {
        final TimerHandler timerHandler = mock(TimerHandler.class);
        when(timerHandler.onTimerEvent(anyLong())).thenReturn(true);
        final PriorityHeapTimerService timerService = new PriorityHeapTimerService(timerHandler);
        timerService.scheduleTimerForCorrelationId(1, 1);
        timerService.scheduleTimerForCorrelationId(2, 1);
        timerService.scheduleTimerForCorrelationId(3, 3);

        assertEquals(2, timerService.poll(1));

        assertFalse(timerService.cancelTimerByCorrelationId(1));
        assertFalse(timerService.cancelTimerByCorrelationId(2));
    }

    @Test
    void scheduleMustRetainOrderBetweenDeadlines()
    {
        final TimerHandler timerHandler = mock(TimerHandler.class);
        when(timerHandler.onTimerEvent(anyLong())).thenReturn(true);
        final PriorityHeapTimerService timerService = new PriorityHeapTimerService(timerHandler);
        timerService.scheduleTimerForCorrelationId(3, 3);
        timerService.scheduleTimerForCorrelationId(4, 4);
        timerService.scheduleTimerForCorrelationId(5, 5);
        timerService.scheduleTimerForCorrelationId(6, 0);

        assertEquals(4, timerService.poll(5));

        final InOrder inOrder = inOrder(timerHandler);
        inOrder.verify(timerHandler).onTimerEvent(6);
        inOrder.verify(timerHandler).onTimerEvent(3);
        inOrder.verify(timerHandler).onTimerEvent(4);
        inOrder.verify(timerHandler).onTimerEvent(5);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldReuseExpiredEntriesFromAFreeList()
    {
        final TimerHandler timerHandler = mock(TimerHandler.class);
        when(timerHandler.onTimerEvent(anyLong())).thenReturn(true);
        final PriorityHeapTimerService timerService = new PriorityHeapTimerService(timerHandler);
        timerService.scheduleTimerForCorrelationId(1, 1);
        timerService.scheduleTimerForCorrelationId(2, 2);
        timerService.scheduleTimerForCorrelationId(3, 3);
        final Set<PriorityHeapTimerService.Timer> entries = Collections.newSetFromMap(new IdentityHashMap<>());

        timerService.forEach(entries::add);
        assertEquals(3, entries.size());

        assertEquals(2, timerService.poll(2));

        timerService.scheduleTimerForCorrelationId(4, 4);
        timerService.scheduleTimerForCorrelationId(5, 5);
        timerService.scheduleTimerForCorrelationId(6, 0);

        timerService.forEach(entries::add);
        assertEquals(4, entries.size());

        assertEquals(4, timerService.poll(5));

        final InOrder inOrder = inOrder(timerHandler);
        inOrder.verify(timerHandler).onTimerEvent(6);
        inOrder.verify(timerHandler).onTimerEvent(3);
        inOrder.verify(timerHandler).onTimerEvent(4);
        inOrder.verify(timerHandler).onTimerEvent(5);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldReuseCanceledTimerEntriesFromAFreeList()
    {
        final TimerHandler timerHandler = mock(TimerHandler.class);
        when(timerHandler.onTimerEvent(anyLong())).thenReturn(true);
        final PriorityHeapTimerService timerService = new PriorityHeapTimerService(timerHandler);
        timerService.scheduleTimerForCorrelationId(1, 1);
        timerService.scheduleTimerForCorrelationId(2, 2);
        timerService.scheduleTimerForCorrelationId(3, 3);
        timerService.scheduleTimerForCorrelationId(4, 4);
        final Set<PriorityHeapTimerService.Timer> entries = Collections.newSetFromMap(new IdentityHashMap<>());

        timerService.forEach(entries::add);
        assertEquals(4, entries.size());

        assertTrue(timerService.cancelTimerByCorrelationId(2));
        assertTrue(timerService.cancelTimerByCorrelationId(4));

        timerService.scheduleTimerForCorrelationId(5, 5);
        timerService.scheduleTimerForCorrelationId(6, 0);

        timerService.forEach(entries::add);
        assertEquals(4, entries.size());

        assertEquals(4, timerService.poll(5));

        final InOrder inOrder = inOrder(timerHandler);
        inOrder.verify(timerHandler).onTimerEvent(6);
        inOrder.verify(timerHandler).onTimerEvent(1);
        inOrder.verify(timerHandler).onTimerEvent(3);
        inOrder.verify(timerHandler).onTimerEvent(5);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void manyRandomOperations()
    {
        final TestHarness testHarness = new TestHarness();
        for (int i = 0; i < 10000; i++)
        {
            testHarness.doRandomOperation();
        }
    }

    private static final class TestHarness
    {
        final PriorityHeapTimerService timerService = new PriorityHeapTimerService(this::onTimerEvent);
        final Long2LongHashMap timerDeadlines = new Long2LongHashMap(-1);
        final Random random = new Random(1);
        long nextCorrelationId = 1;
        long time = 1000;

        void doRandomOperation()
        {
            if (timerDeadlines.size() < 5)
            {
                scheduleTimer();
            }
            else if (timerDeadlines.size() > 20)
            {
                incrementTimeAndPoll();
            }
            else
            {
                switch (random.nextInt(5))
                {
                    case 0:
                        incrementTimeAndPoll();
                        break;
                    case 1:
                        cancelRandomTimer();
                        break;
                    case 2:
                        rescheduleRandomTimer();
                        break;
                    default:
                        scheduleTimer();
                }
            }
            validateOrdering();
        }

        private void scheduleTimer()
        {
            final long correlationId = nextCorrelationId++;
            final long deadline = (time + random.nextInt(20));
            timerService.scheduleTimerForCorrelationId(correlationId, deadline);
            timerDeadlines.put(correlationId, deadline);
        }

        private void cancelRandomTimer()
        {
            final long correlationId = randomScheduledCorrelationId();
            assertNotEquals(-1, timerDeadlines.remove(correlationId));
            assertTrue(timerService.cancelTimerByCorrelationId(correlationId));
        }

        private void rescheduleRandomTimer()
        {
            final long correlationId = randomScheduledCorrelationId();
            final long newDeadline = (time + random.nextInt(20));
            timerService.scheduleTimerForCorrelationId(correlationId, newDeadline);
            assertNotEquals(-1, timerDeadlines.put(correlationId, newDeadline));
        }

        private long randomScheduledCorrelationId()
        {
            final Long2LongHashMap.KeyIterator itr = timerDeadlines.keySet().iterator();
            for (int i = random.nextInt(timerDeadlines.size()); i > 0; i--)
            {
                itr.nextValue();
            }
            return itr.nextValue();
        }

        private void incrementTimeAndPoll()
        {
            time += random.nextInt(2);
            timerService.poll(time);
        }

        private boolean onTimerEvent(final long correlationId)
        {
            final long deadline = timerDeadlines.remove(correlationId);
            assertTrue(deadline >= 1000 && deadline <= time,
                () -> "correlationId=" + correlationId + " deadline=" + deadline);
            return true;
        }

        private void validateOrdering()
        {
            final MutableInteger nextIndex = new MutableInteger();
            final ArrayList<PriorityHeapTimerService.Timer> entries = new ArrayList<>(timerDeadlines.size());
            timerService.forEach(timer ->
            {
                assertEquals(nextIndex.value++, timer.index);
                entries.add(timer);
                if (timer.index > 0)
                {
                    final PriorityHeapTimerService.Timer parent = entries.get((timer.index - 1) / 2);
                    assertTrue(parent.deadline <= timer.deadline, () -> "parent=" + parent + "\n child=" + timer);
                }
            });
            assertEquals(timerDeadlines.size(), entries.size());
        }
    }
}
