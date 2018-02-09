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
package io.aeron.cluster.service;

import io.aeron.Subscription;
import io.aeron.cluster.codecs.*;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;

public final class ServiceControlAdapter implements FragmentHandler, AutoCloseable
{
    final Subscription subscription;
    final ServiceControlListener serviceControlListener;

    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final ScheduleTimerRequestDecoder scheduleTimerRequestDecoder = new ScheduleTimerRequestDecoder();
    private final CancelTimerRequestDecoder cancelTimerRequestDecoder = new CancelTimerRequestDecoder();
    private final ServiceActionAckDecoder serviceActionAckDecoder = new ServiceActionAckDecoder();
    private final JoinLogRequestDecoder joinLogRequestDecoder = new JoinLogRequestDecoder();

    public ServiceControlAdapter(final Subscription subscription, final ServiceControlListener serviceControlListener)
    {
        this.subscription = subscription;
        this.serviceControlListener = serviceControlListener;
    }

    public void close()
    {
        CloseHelper.close(subscription);
    }

    public int poll()
    {
        return subscription.poll(this, 1);
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

                serviceControlListener.onScheduleTimer(
                    scheduleTimerRequestDecoder.correlationId(),
                    scheduleTimerRequestDecoder.deadline());
                break;

            case CancelTimerRequestDecoder.TEMPLATE_ID:
                cancelTimerRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                serviceControlListener.onCancelTimer(scheduleTimerRequestDecoder.correlationId());
                break;

            case ServiceActionAckDecoder.TEMPLATE_ID:
                serviceActionAckDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                serviceControlListener.onActionAck(
                    serviceActionAckDecoder.logPosition(),
                    serviceActionAckDecoder.leadershipTermId(),
                    serviceActionAckDecoder.serviceId(),
                    serviceActionAckDecoder.action());
                break;

            case JoinLogRequestDecoder.TEMPLATE_ID:
                joinLogRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                serviceControlListener.onJoinLog(
                    joinLogRequestDecoder.leadershipTermId(),
                    joinLogRequestDecoder.commitPositionId(),
                    joinLogRequestDecoder.logSessionId(),
                    joinLogRequestDecoder.logStreamId(),
                    joinLogRequestDecoder.logChannel());
                break;

            default:
                throw new IllegalArgumentException("Unknown template id: " + templateId);
        }
    }
}
