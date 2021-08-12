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

import org.agrona.collections.Long2ObjectHashMap;

import java.util.Arrays;
import java.util.Objects;

final class InOrderTimerService implements TimerService
{
    private static final TimerEntry[] NO_TIMERS = new TimerEntry[0];
    private static final int MIN_CAPACITY = 8;

    private final TimerHandler timerHandler;
    private final Long2ObjectHashMap<TimerEntry> timerByCorrelationId = new Long2ObjectHashMap<>();
    private TimerEntry[] timers = NO_TIMERS;
    private int size;

    InOrderTimerService(final TimerHandler timerHandler)
    {
        this.timerHandler = Objects.requireNonNull(timerHandler, "TimerHandler");
    }

    public int poll(final long now)
    {
        int expiredTimers = 0;
        for (int i = 0; i < size; i++)
        {
            final TimerEntry timer = timers[i];
            if (timer.deadline <= now)
            {
                if (!timerHandler.onTimerEvent(timer.correlationId))
                {
                    break;
                }
                expiredTimers++;
            }
            else
            {
                break;
            }
        }

        if (expiredTimers > 0)
        {
            shiftAllUp(timers, expiredTimers, size);
            size -= expiredTimers;
        }

        return expiredTimers;
    }

    public void scheduleTimerForCorrelationId(final long correlationId, final long deadline)
    {
        if (size == timers.length)
        {
            timers = Arrays.copyOf(timers, Math.max(MIN_CAPACITY, timers.length << 1));
        }

        final int index = size++;
        final TimerEntry entry = new TimerEntry(correlationId, deadline, index);
        timers[index] = entry;
        timerByCorrelationId.put(correlationId, entry);

        shiftUp(timers, index);
    }

    public boolean cancelTimerByCorrelationId(final long correlationId)
    {
        final TimerEntry removed = timerByCorrelationId.remove(correlationId);
        if (null == removed)
        {
            return false;
        }

        for (int i = removed.index + 1; i < size; i++)
        {
            final TimerEntry entry = timers[i];
            timers[i - 1] = entry;
            entry.index--;
        }

        size--;
        timers[size] = null;

        return true;
    }

    public void snapshot(final TimerSnapshotTaker snapshotTaker)
    {

    }

    public void currentTime(final long now)
    {
    }

    private static void shiftAllUp(final TimerEntry[] timers, final int expiredTimers, final int size)
    {
        if (expiredTimers < size)
        {
            final int remainingTimers = size - expiredTimers;
            System.arraycopy(timers, expiredTimers, timers, 0, remainingTimers);
            Arrays.fill(timers, remainingTimers, size, null);
            for (int i = 0; i < remainingTimers; i++)
            {
                timers[i].index -= expiredTimers;
            }
        }
        else
        {
            Arrays.fill(timers, null);
        }
    }

    private static void shiftUp(final TimerEntry[] timers, final int index)
    {
        for (int i = index; i > 0; i--)
        {
            final TimerEntry entry = timers[i];
            final TimerEntry prevEntry = timers[i - 1];
            if (entry.deadline < prevEntry.deadline)
            {
                timers[i - 1] = entry;
                entry.index--;
                timers[i] = prevEntry;
                prevEntry.index++;
            }
            else
            {
                break;
            }
        }
    }

    private static final class TimerEntry
    {
        final long correlationId;
        final long deadline;
        int index;

        private TimerEntry(final long correlationId, final long deadline, final int index)
        {
            this.correlationId = correlationId;
            this.deadline = deadline;
            this.index = index;
        }

        public String toString()
        {
            return "TimerEntry{" +
                "correlationId=" + correlationId +
                ", deadline=" + deadline +
                ", index=" + index +
                '}';
        }
    }
}
