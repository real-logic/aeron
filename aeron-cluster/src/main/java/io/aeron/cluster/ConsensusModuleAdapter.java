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
import io.aeron.cluster.codecs.*;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;

public class ConsensusModuleAdapter implements FragmentHandler, AutoCloseable
{
    private final int fragmentLimit;
    final Subscription subscription;
    final SequencerAgent sequencerAgent;

    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final ScheduleTimerRequestDecoder scheduleTimerRequestDecoder = new ScheduleTimerRequestDecoder();
    private final CancelTimerRequestDecoder cancelTimerRequestDecoder = new CancelTimerRequestDecoder();
    private final ServiceActionAckDecoder serviceActionAckDecoder = new ServiceActionAckDecoder();

    public ConsensusModuleAdapter(
        final int fragmentLimit, final Subscription subscription, final SequencerAgent sequencerAgent)
    {
        this.fragmentLimit = fragmentLimit;
        this.subscription = subscription;
        this.sequencerAgent = sequencerAgent;
    }

    public void close()
    {
        CloseHelper.close(subscription);
    }

    public int poll()
    {
        return subscription.poll(this, fragmentLimit);
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

                sequencerAgent.onScheduleTimer(
                    scheduleTimerRequestDecoder.correlationId(),
                    scheduleTimerRequestDecoder.deadline());
                break;

            case CancelTimerRequestDecoder.TEMPLATE_ID:
                cancelTimerRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                sequencerAgent.onCancelTimer(scheduleTimerRequestDecoder.correlationId());
                break;

            case ServiceActionAckDecoder.TEMPLATE_ID:
                serviceActionAckDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                sequencerAgent.onActionAck(
                    serviceActionAckDecoder.serviceId(),
                    serviceActionAckDecoder.logPosition(),
                    serviceActionAckDecoder.messageIndex(),
                    serviceActionAckDecoder.action());
                break;

            default:
                throw new IllegalArgumentException("Unknown template id: " + templateId);
        }
    }
}
