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

import io.aeron.Aeron;
import org.agrona.collections.ArrayUtil;
import org.agrona.collections.Long2ObjectHashMap;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Implementation of the {@link TimerService} that uses a priority heap to order the timestamps.
 *
 * <p>
 * <b>Caveats</b>
 * <p>
 * Timers with the same deadline are not be ordered with one another. In contrast, the timers with different deadlines
 * are guaranteed to expire in order even after Cluster restart, i.e. when the deadlines are in the past.
 * <p>
 * <b>Note:</b> Not thread safe.
 */
public final class PriorityHeapTimerService implements TimerService
{
    private static final TimerEntry[] NO_TIMERS = new TimerEntry[0];
    private static final int MIN_CAPACITY = 8;

    private final TimerHandler timerHandler;
    private final Long2ObjectHashMap<TimerEntry> timerByCorrelationId = new Long2ObjectHashMap<>();
    private TimerEntry[] timers = NO_TIMERS;
    private TimerEntry[] freeList = NO_TIMERS;
    private int size;
    private int freeTimers;

    /**
     * Construct a Priority Heap Timer Service using the supplied handler to
     * callback for expired timers
     *
     * @param timerHandler to callback when a timer expires.
     */
    public PriorityHeapTimerService(final TimerHandler timerHandler)
    {
        this.timerHandler = Objects.requireNonNull(timerHandler, "TimerHandler");
    }

    /**
     * Poll for expired timers, firing the callback supplied in the constructor.
     *
     * @param now current time.
     * @return the number of expired timers
     */
    public int poll(final long now)
    {
        int expiredTimers = 0;
        final TimerEntry[] timers = this.timers;
        final TimerHandler timerHandler = this.timerHandler;

        while (size > 0 && expiredTimers < POLL_LIMIT)
        {
            final TimerEntry timer = timers[0];
            if (timer.deadline > now)
            {
                break;
            }

            if (!timerHandler.onTimerEvent(timer.correlationId))
            {
                break;
            }

            expiredTimers++;
            final int lastIndex = --size;
            final TimerEntry lastEntry = timers[lastIndex];
            timers[lastIndex] = null;

            if (0 != lastIndex)
            {
                shiftDown(timers, lastIndex, 0, lastEntry);
            }

            timerByCorrelationId.remove(timer.correlationId);
            addToFreeList(timer);
        }

        return expiredTimers;
    }

    /**
     * {@inheritDoc}
     */
    public void scheduleTimerForCorrelationId(final long correlationId, final long deadline)
    {
        final TimerEntry existingEntry = timerByCorrelationId.get(correlationId);
        if (null != existingEntry)
        {
            if (deadline < existingEntry.deadline)
            {
                existingEntry.deadline = deadline;
                shiftUp(timers, existingEntry.index, existingEntry);
            }
            else if (deadline > existingEntry.deadline)
            {
                existingEntry.deadline = deadline;
                shiftDown(timers, size, existingEntry.index, existingEntry);
            }
        }
        else
        {
            ensureCapacity(size + 1);

            final int index = size++;
            final TimerEntry entry;
            if (freeTimers > 0)
            {
                final int freeIndex = --freeTimers;
                entry = freeList[freeIndex];
                freeList[freeIndex] = null;
                entry.reset(correlationId, deadline, index);
            }
            else
            {
                entry = new TimerEntry(correlationId, deadline, index);
            }

            timerByCorrelationId.put(correlationId, entry);
            shiftUp(timers, index, entry);
        }
    }

    /**
     * {@inheritDoc}
     */
    public boolean cancelTimerByCorrelationId(final long correlationId)
    {
        final TimerEntry removed = timerByCorrelationId.remove(correlationId);
        if (null == removed)
        {
            return false;
        }

        final int lastIndex = --size;
        final TimerEntry last = timers[lastIndex];
        timers[lastIndex] = null;

        if (lastIndex != removed.index)
        {
            shiftDown(timers, lastIndex, removed.index, last);
        }

        addToFreeList(removed);

        return true;
    }

    /**
     * {@inheritDoc}
     */
    public void snapshot(final TimerSnapshotTaker snapshotTaker)
    {
        final TimerEntry[] timers = this.timers;
        for (int i = 0, size = this.size; i < size; i++)
        {
            final TimerEntry entry = timers[i];
            snapshotTaker.snapshotTimer(entry.correlationId, entry.deadline);
        }
    }


    /**
     * {@inheritDoc}
     */
    public void currentTime(final long now)
    {
    }

    void forEach(final Consumer<TimerEntry> consumer)
    {
        final TimerEntry[] timers = this.timers;
        for (int i = 0, size = this.size; i < size; i++)
        {
            consumer.accept(timers[i]);
        }
    }

    private static void shiftUp(final TimerEntry[] timers, final int startIndex, final TimerEntry entry)
    {
        int index = startIndex;
        while (index > 0)
        {
            final int prevIndex = (index - 1) >>> 1;
            final TimerEntry prevEntry = timers[prevIndex];

            if (entry.deadline >= prevEntry.deadline)
            {
                break;
            }

            timers[index] = prevEntry;
            prevEntry.index = index;
            index = prevIndex;
        }

        timers[index] = entry;
        entry.index = index;
    }

    private static void shiftDown(
        final TimerEntry[] timers, final int size, final int startIndex, final TimerEntry entry)
    {
        final int half = size >>> 1;
        int index = startIndex;
        while (index < half)
        {
            int nextIndex = (index << 1) + 1;
            final int right = nextIndex + 1;
            TimerEntry nextEntry = timers[nextIndex];

            if (right < size && nextEntry.deadline > timers[right].deadline)
            {
                nextIndex = right;
                nextEntry = timers[nextIndex];
            }

            if (entry.deadline < nextEntry.deadline)
            {
                break;
            }

            timers[index] = nextEntry;
            nextEntry.index = index;
            index = nextIndex;
        }

        timers[index] = entry;
        entry.index = index;
    }

    private void ensureCapacity(final int requiredCapacity)
    {
        final int currentCapacity = timers.length;

        if (requiredCapacity > currentCapacity)
        {
            if (requiredCapacity > ArrayUtil.MAX_CAPACITY)
            {
                throw new IllegalStateException("max capacity reached: " + ArrayUtil.MAX_CAPACITY);
            }

            if (NO_TIMERS == timers)
            {
                timers = new TimerEntry[MIN_CAPACITY];
                freeList = new TimerEntry[MIN_CAPACITY];
            }
            else
            {
                int newCapacity = currentCapacity + (currentCapacity >> 1);
                if (newCapacity < 0 || newCapacity > ArrayUtil.MAX_CAPACITY)
                {
                    newCapacity = ArrayUtil.MAX_CAPACITY;
                }
                timers = Arrays.copyOf(timers, newCapacity);
                freeList = Arrays.copyOf(freeList, newCapacity);
            }
        }
    }

    private void addToFreeList(final TimerEntry entry)
    {
        entry.reset(Aeron.NULL_VALUE, Aeron.NULL_VALUE, Aeron.NULL_VALUE);
        freeList[freeTimers++] = entry;
    }

    static final class TimerEntry
    {
        long correlationId;
        long deadline;
        int index;

        TimerEntry(final long correlationId, final long deadline, final int index)
        {
            reset(correlationId, deadline, index);
        }

        void reset(final long correlationId, final long deadline, final int index)
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
