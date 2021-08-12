/*
 * Copyright 2014-2021 Real Logic Limited.
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
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class InOrderTimerServiceTest
{
    @Test
    void throwsNullPointerExceptionIfTimerHandlerIsNull()
    {
        assertThrows(NullPointerException.class, () -> new InOrderTimerService(null));
    }

    @Test
    void pollIsANoOpWhenNoTimersWhereScheduled()
    {
        final TimerHandler timerHandler = mock(TimerHandler.class);
        final InOrderTimerService timerService = new InOrderTimerService(timerHandler);

        assertEquals(0, timerService.poll(Long.MIN_VALUE));

        verifyNoInteractions(timerHandler);
    }

    @Test
    void pollIsANoOpWhenNoScheduledTimersAreExpired()
    {
        final TimerHandler timerHandler = mock(TimerHandler.class);
        final InOrderTimerService timerService = new InOrderTimerService(timerHandler);
        timerService.scheduleTimerForCorrelationId(1, 100);

        assertEquals(0, timerService.poll(7));

        verifyNoInteractions(timerHandler);
    }

    @Test
    void pollShouldNotExpireTimerIfHandlerReturnsFalse()
    {
        final TimerHandler timerHandler = mock(TimerHandler.class);
        final InOrderTimerService timerService = new InOrderTimerService(timerHandler);
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
        final InOrderTimerService timerService = new InOrderTimerService(timerHandler);
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
        final InOrderTimerService timerService = new InOrderTimerService(timerHandler);
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
    void pollShouldExpireTimersInOrderWithinTheSameDeadline()
    {
        final TimerHandler timerHandler = mock(TimerHandler.class);
        when(timerHandler.onTimerEvent(anyLong())).thenReturn(true);
        final InOrderTimerService timerService = new InOrderTimerService(timerHandler);
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
        inOrder.verify(timerHandler).onTimerEvent(4);
        inOrder.verify(timerHandler).onTimerEvent(3);
        inOrder.verify(timerHandler).onTimerEvent(1);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void cancelTimerByCorrelationIdIsANoOpIfNoTimersRegistered()
    {
        final InOrderTimerService timerService = new InOrderTimerService(mock(TimerHandler.class));

        assertFalse(timerService.cancelTimerByCorrelationId(100));
    }

    @Test
    void cancelTimerByCorrelationIdReturnsFalseForUnknownCorrelationId()
    {
        final InOrderTimerService timerService = new InOrderTimerService(mock(TimerHandler.class));
        timerService.scheduleTimerForCorrelationId(1, 100);
        timerService.scheduleTimerForCorrelationId(7, 50);

        assertFalse(timerService.cancelTimerByCorrelationId(3));
    }

    @Test
    void cancelTimerByCorrelationIdReturnsTrueAfterCancellingTheTimer()
    {
        final TimerHandler timerHandler = mock(TimerHandler.class);
        when(timerHandler.onTimerEvent(anyLong())).thenReturn(true);
        final InOrderTimerService timerService = new InOrderTimerService(timerHandler);
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
        final InOrderTimerService timerService = new InOrderTimerService(timerHandler);
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
        final InOrderTimerService timerService = new InOrderTimerService(timerHandler);
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
}
