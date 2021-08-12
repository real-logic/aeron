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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
        verify(timerHandler).onTimerEvent(1);
        verifyNoMoreInteractions(timerHandler);
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
}
