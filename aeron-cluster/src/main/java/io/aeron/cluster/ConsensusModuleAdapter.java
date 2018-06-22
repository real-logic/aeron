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

import io.aeron.Subscription;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.*;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;

final class ConsensusModuleAdapter implements FragmentHandler, AutoCloseable
{
    private final Subscription subscription;
    private final ConsensusModuleAgent consensusModuleAgent;
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final ScheduleTimerDecoder scheduleTimerDecoder = new ScheduleTimerDecoder();
    private final CancelTimerDecoder cancelTimerDecoder = new CancelTimerDecoder();
    private final ServiceAckDecoder serviceAckDecoder = new ServiceAckDecoder();
    private final CloseSessionDecoder closeSessionDecoder = new CloseSessionDecoder();

    ConsensusModuleAdapter(final Subscription subscription, final ConsensusModuleAgent consensusModuleAgent)
    {
        this.subscription = subscription;
        this.consensusModuleAgent = consensusModuleAgent;
    }

    public void close()
    {
        CloseHelper.close(subscription);
    }

    int poll()
    {
        return subscription.poll(this, 1);
    }

    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        final int templateId = messageHeaderDecoder.templateId();
        switch (templateId)
        {
            case CloseSessionDecoder.TEMPLATE_ID:
                closeSessionDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onServiceCloseSession(closeSessionDecoder.clusterSessionId());
                break;

            case ScheduleTimerDecoder.TEMPLATE_ID:
                scheduleTimerDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onScheduleTimer(
                    scheduleTimerDecoder.correlationId(),
                    scheduleTimerDecoder.deadline());
                break;

            case CancelTimerDecoder.TEMPLATE_ID:
                cancelTimerDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onCancelTimer(scheduleTimerDecoder.correlationId());
                break;

            case ServiceAckDecoder.TEMPLATE_ID:
                serviceAckDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onServiceAck(
                    serviceAckDecoder.logPosition(),
                    serviceAckDecoder.ackId(),
                    serviceAckDecoder.relevantId(),
                    serviceAckDecoder.serviceId());
                break;

            default:
                throw new ClusterException("unknown template id: " + templateId);
        }
    }
}
