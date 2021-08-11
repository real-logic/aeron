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

import org.agrona.DeadlineTimerWheel;
import org.agrona.collections.Long2LongHashMap;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

class TimerWheelTimerService extends DeadlineTimerWheel implements DeadlineTimerWheel.TimerHandler, TimerService
{
    private final TimerService.TimerHandler timerHandler;
    private final Long2LongHashMap timerIdByCorrelationIdMap = new Long2LongHashMap(Long.MAX_VALUE);
    private final Long2LongHashMap correlationIdByTimerIdMap = new Long2LongHashMap(Long.MAX_VALUE);
    private boolean isAbort;

    TimerWheelTimerService(
        final TimerService.TimerHandler timerHandler,
        final TimeUnit timeUnit,
        final long startTime,
        final long tickResolution,
        final int ticksPerWheel)
    {
        super(timeUnit, startTime, tickResolution, ticksPerWheel);
        this.timerHandler = Objects.requireNonNull(timerHandler);
    }

    public int poll(final long now)
    {
        int expired = 0;
        isAbort = false;

        do
        {
            expired += super.poll(now, this, POLL_LIMIT);

            if (isAbort)
            {
                break;
            }
        }
        while (expired < POLL_LIMIT && super.currentTickTime() < now);

        return expired;
    }

    public boolean onTimerExpiry(final TimeUnit timeUnit, final long now, final long timerId)
    {
        final long correlationId = correlationIdByTimerIdMap.get(timerId);

        if (timerHandler.onTimerEvent(correlationId))
        {
            correlationIdByTimerIdMap.remove(timerId);
            timerIdByCorrelationIdMap.remove(correlationId);

            return true;
        }
        else
        {
            isAbort = true;

            return false;
        }
    }

    public void scheduleTimerForCorrelationId(final long correlationId, final long deadline)
    {
        cancelTimerByCorrelationId(correlationId);

        final long timerId = super.scheduleTimer(deadline);
        timerIdByCorrelationIdMap.put(correlationId, timerId);
        correlationIdByTimerIdMap.put(timerId, correlationId);
    }

    public boolean cancelTimerByCorrelationId(final long correlationId)
    {
        final long timerId = timerIdByCorrelationIdMap.remove(correlationId);
        if (Long.MAX_VALUE != timerId)
        {
            super.cancelTimer(timerId);
            correlationIdByTimerIdMap.remove(timerId);

            return true;
        }

        return false;
    }

    public void snapshot(final TimerSnapshotTaker snapshotTaker)
    {
        final Long2LongHashMap.EntryIterator iter = timerIdByCorrelationIdMap.entrySet().iterator();

        while (iter.hasNext())
        {
            iter.next();

            final long correlationId = iter.getLongKey();
            final long deadline = super.deadline(iter.getLongValue());

            snapshotTaker.snapshotTimer(correlationId, deadline);
        }
    }
}
