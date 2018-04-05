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
    private final ScheduleTimerDecoder scheduleTimerDecoder = new ScheduleTimerDecoder();
    private final CancelTimerDecoder cancelTimerDecoder = new CancelTimerDecoder();
    private final ClusterActionAckDecoder clusterActionAckDecoder = new ClusterActionAckDecoder();
    private final JoinLogDecoder joinLogDecoder = new JoinLogDecoder();
    private final CloseSessionDecoder closeSessionDecoder = new CloseSessionDecoder();

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
            case ScheduleTimerDecoder.TEMPLATE_ID:
                scheduleTimerDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                serviceControlListener.onScheduleTimer(
                    scheduleTimerDecoder.correlationId(),
                    scheduleTimerDecoder.deadline());
                break;

            case CancelTimerDecoder.TEMPLATE_ID:
                cancelTimerDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                serviceControlListener.onCancelTimer(scheduleTimerDecoder.correlationId());
                break;

            case ClusterActionAckDecoder.TEMPLATE_ID:
                clusterActionAckDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                serviceControlListener.onServiceAck(
                    clusterActionAckDecoder.logPosition(),
                    clusterActionAckDecoder.leadershipTermId(),
                    clusterActionAckDecoder.serviceId(),
                    clusterActionAckDecoder.action());
                break;

            case JoinLogDecoder.TEMPLATE_ID:
                joinLogDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                serviceControlListener.onJoinLog(
                    joinLogDecoder.leadershipTermId(),
                    joinLogDecoder.commitPositionId(),
                    joinLogDecoder.logSessionId(),
                    joinLogDecoder.logStreamId(),
                    joinLogDecoder.ackBeforeImage() == BooleanType.TRUE,
                    joinLogDecoder.logChannel());
                break;

            case CloseSessionDecoder.TEMPLATE_ID:
                closeSessionDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                serviceControlListener.onServiceCloseSession(closeSessionDecoder.clusterSessionId());
                break;

            default:
                throw new IllegalArgumentException("Unknown template id: " + templateId);
        }
    }
}
