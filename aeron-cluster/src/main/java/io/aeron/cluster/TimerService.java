/*
 * Copyright 2017 Real Logic Ltd.
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

import io.aeron.Subscription;
import io.aeron.cluster.codecs.CancelTimerRequestDecoder;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.ScheduleTimerRequestDecoder;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DeadlineTimerWheel;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.EpochClock;

import java.util.concurrent.TimeUnit;

public class TimerService implements FragmentHandler, AutoCloseable, DeadlineTimerWheel.TimerHandler
{
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final ScheduleTimerRequestDecoder scheduleTimerRequestDecoder = new ScheduleTimerRequestDecoder();
    private final CancelTimerRequestDecoder cancelTimerRequestDecoder = new CancelTimerRequestDecoder();

    private final int timerLimit;
    private final int fragmentLimit;
    private final SequencerAgent sequencerAgent;
    private final Subscription subscription;
    private final EpochClock epochClock;
    private final DeadlineTimerWheel timerWheel = new DeadlineTimerWheel(
        TimeUnit.MILLISECONDS, 0, 1, 128);
    private Long2LongHashMap timerIdByCorrelationIdMap = new Long2LongHashMap(Long.MAX_VALUE);
    private Long2LongHashMap correlationIdByTimerIdMap = new Long2LongHashMap(Long.MAX_VALUE);

    public TimerService(
        final int timerPollLimit,
        final int fragmentPollLimit,
        final SequencerAgent sequencerAgent,
        final Subscription subscription,
        final EpochClock epochClock)
    {
        this.timerLimit = timerPollLimit;
        this.fragmentLimit = fragmentPollLimit;
        this.sequencerAgent = sequencerAgent;
        this.subscription = subscription;
        this.epochClock = epochClock;
    }

    public void close()
    {
        CloseHelper.close(subscription);
    }

    public int poll(final long nowMs)
    {
        int workCount = 0;

        workCount += timerWheel.poll(nowMs, this, timerLimit);
        workCount += subscription.poll(this, fragmentLimit);

        return workCount;
    }

    public boolean onExpiry(final TimeUnit timeUnit, final long now, final long timerId)
    {
        final long correlationId = correlationIdByTimerIdMap.remove(timerId);
        timerIdByCorrelationIdMap.remove(correlationId);

        return sequencerAgent.onExpireTimer(correlationId, now);
    }

    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        final int templateId = messageHeaderDecoder.templateId();
        switch (templateId)
        {
            case ScheduleTimerRequestDecoder.TEMPLATE_ID:
                scheduleTimerRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                scheduleTimer(
                    scheduleTimerRequestDecoder.correlationId(),
                    scheduleTimerRequestDecoder.deadline());
                break;

            case CancelTimerRequestDecoder.TEMPLATE_ID:
                cancelTimerRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                cancelTimer(scheduleTimerRequestDecoder.correlationId());
                break;
        }
    }

    private void scheduleTimer(final long correlationId, final long deadlineMs)
    {
        if (epochClock.time() >= deadlineMs)
        {
            sequencerAgent.onExpireTimer(correlationId, epochClock.time());
        }
        else
        {
            cancelTimer(correlationId);

            final long timerId = timerWheel.scheduleTimer(deadlineMs);
            timerIdByCorrelationIdMap.put(correlationId, timerId);
            correlationIdByTimerIdMap.put(timerId, correlationId);
        }
    }

    private void cancelTimer(final long correlationId)
    {
        final long timerId = timerIdByCorrelationIdMap.remove(correlationId);
        if (Long.MAX_VALUE != timerId)
        {
            timerWheel.cancelTimer(timerId);
            correlationIdByTimerIdMap.remove(timerId);
        }
    }
}
