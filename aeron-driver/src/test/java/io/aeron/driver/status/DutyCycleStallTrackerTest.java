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
package io.aeron.driver.status;

import org.agrona.concurrent.status.AtomicCounter;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.mockito.Mockito.*;

class DutyCycleStallTrackerTest
{
    @Test
    void throwsNullPointerExceptionIfMaxTimeCounterIsNull()
    {
        assertThrowsExactly(
            NullPointerException.class,
            () -> new DutyCycleStallTracker(null, mock(AtomicCounter.class), 1));
    }

    @Test
    void throwsNullPointerExceptionIfThresholdCounterIsNull()
    {
        assertThrowsExactly(
            NullPointerException.class,
            () -> new DutyCycleStallTracker(mock(AtomicCounter.class), null, 1));
    }

    @Test
    void reportMeasurementIsANoOpIfMaxCounterIsClosed()
    {
        final AtomicCounter maxCycleTime = mock(AtomicCounter.class);
        when(maxCycleTime.isClosed()).thenReturn(true);
        final AtomicCounter cycleTimeThresholdExceededCount = mock(AtomicCounter.class);
        final DutyCycleStallTracker dutyCycleStallTracker =
            new DutyCycleStallTracker(maxCycleTime, cycleTimeThresholdExceededCount, 1);

        dutyCycleStallTracker.reportMeasurement(555);

        verify(maxCycleTime).isClosed();
        verifyNoMoreInteractions(maxCycleTime);
        verifyNoInteractions(cycleTimeThresholdExceededCount);
    }

    @Test
    void reportMeasurementOnlyUpdatesThresholdCounterWhenExceeded()
    {
        final AtomicCounter maxCycleTime = mock(AtomicCounter.class);
        when(maxCycleTime.isClosed()).thenReturn(false);
        final AtomicCounter cycleTimeThresholdExceededCount = mock(AtomicCounter.class);
        final int cycleTimeThresholdNs = 1000;
        final DutyCycleStallTracker dutyCycleStallTracker =
            new DutyCycleStallTracker(maxCycleTime, cycleTimeThresholdExceededCount, cycleTimeThresholdNs);

        dutyCycleStallTracker.reportMeasurement(555);
        dutyCycleStallTracker.reportMeasurement(1000);
        dutyCycleStallTracker.reportMeasurement(1001);

        verify(maxCycleTime, times(3)).isClosed();
        verify(maxCycleTime).proposeMaxRelease(555L);
        verify(maxCycleTime).proposeMaxRelease(1000L);
        verify(maxCycleTime).proposeMaxRelease(1001L);
        verify(cycleTimeThresholdExceededCount).incrementRelease();
        verifyNoMoreInteractions(maxCycleTime, cycleTimeThresholdExceededCount);
    }
}
