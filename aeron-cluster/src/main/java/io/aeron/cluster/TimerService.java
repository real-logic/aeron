/*
 * Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron.cluster;

import org.agrona.DeadlineTimerWheel;
import org.agrona.collections.Long2LongHashMap;

import java.util.concurrent.TimeUnit;

import static io.aeron.cluster.ConsensusModule.Configuration.TIMER_POLL_LIMIT;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class TimerService implements DeadlineTimerWheel.TimerHandler
{
    private final ConsensusModuleAgent consensusModuleAgent;
    private final DeadlineTimerWheel timerWheel = new DeadlineTimerWheel(MILLISECONDS, 0, 1, 128);
    private Long2LongHashMap timerIdByCorrelationIdMap = new Long2LongHashMap(Long.MAX_VALUE);
    private Long2LongHashMap correlationIdByTimerIdMap = new Long2LongHashMap(Long.MAX_VALUE);

    TimerService(final ConsensusModuleAgent consensusModuleAgent)
    {
        this.consensusModuleAgent = consensusModuleAgent;
    }

    int poll(final long nowMs)
    {
        return timerWheel.poll(nowMs, this, TIMER_POLL_LIMIT);
    }

    long timerCount()
    {
        return timerWheel.timerCount();
    }

    long currentTickTimeMs()
    {
        return timerWheel.currentTickTime();
    }

    public boolean onTimerExpiry(final TimeUnit timeUnit, final long now, final long timerId)
    {
        final long correlationId = correlationIdByTimerIdMap.remove(timerId);
        timerIdByCorrelationIdMap.remove(correlationId);

        return consensusModuleAgent.onTimerEvent(correlationId, now);
    }

    void scheduleTimer(final long correlationId, final long deadlineMs)
    {
        cancelTimer(correlationId);

        final long timerId = timerWheel.scheduleTimer(deadlineMs);
        timerIdByCorrelationIdMap.put(correlationId, timerId);
        correlationIdByTimerIdMap.put(timerId, correlationId);
    }

    boolean cancelTimer(final long correlationId)
    {
        final long timerId = timerIdByCorrelationIdMap.remove(correlationId);
        if (Long.MAX_VALUE != timerId)
        {
            timerWheel.cancelTimer(timerId);
            correlationIdByTimerIdMap.remove(timerId);

            return true;
        }

        return false;
    }

    void snapshot(final ConsensusModuleSnapshotTaker snapshotTaker)
    {
        final Long2LongHashMap.EntryIterator iter =
            (Long2LongHashMap.EntryIterator)timerIdByCorrelationIdMap.entrySet().iterator();

        while (iter.hasNext())
        {
            iter.next();

            final long correlationId = iter.getLongKey();
            final long deadline = timerWheel.deadline(iter.getLongValue());

            snapshotTaker.snapshotTimer(correlationId, deadline);
        }
    }
}
