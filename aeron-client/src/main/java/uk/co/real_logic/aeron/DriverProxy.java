/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
package uk.co.real_logic.aeron;

import uk.co.real_logic.aeron.command.CorrelatedMessageFlyweight;
import uk.co.real_logic.aeron.command.PublicationMessageFlyweight;
import uk.co.real_logic.aeron.command.RemoveMessageFlyweight;
import uk.co.real_logic.aeron.command.SubscriptionMessageFlyweight;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.concurrent.ringbuffer.RingBuffer;

import java.nio.ByteBuffer;

import static uk.co.real_logic.aeron.command.ControlProtocolEvents.*;

/**
 * Separates the concern of communicating with the client conductor away from the rest of the client.
 *
 * Writes messages into the client conductor buffer.
 */
public class DriverProxy
{
    /** Maximum capacity of the write buffer */
    public static final int MSG_BUFFER_CAPACITY = 4096;

    private final long clientId;
    private final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(MSG_BUFFER_CAPACITY));
    private final PublicationMessageFlyweight publicationMessage = new PublicationMessageFlyweight();
    private final SubscriptionMessageFlyweight subscriptionMessage = new SubscriptionMessageFlyweight();

    private final RemoveMessageFlyweight removeMessage = new RemoveMessageFlyweight();
    // the heartbeats come from the client conductor thread, so keep the flyweights and buffer separate
    private final UnsafeBuffer keepaliveBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(MSG_BUFFER_CAPACITY));

    private final CorrelatedMessageFlyweight correlatedMessage = new CorrelatedMessageFlyweight();
    private final RingBuffer toDriverCommandBuffer;

    public DriverProxy(final RingBuffer toDriverCommandBuffer)
    {
        this.toDriverCommandBuffer = toDriverCommandBuffer;

        publicationMessage.wrap(buffer, 0);
        subscriptionMessage.wrap(buffer, 0);

        correlatedMessage.wrap(keepaliveBuffer, 0);
        removeMessage.wrap(buffer, 0);

        clientId = toDriverCommandBuffer.nextCorrelationId();
    }

    public long timeOfLastDriverKeepalive()
    {
        return toDriverCommandBuffer.consumerHeartbeatTime();
    }

    public long addPublication(final String channel, final int streamId)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();

        publicationMessage
            .clientId(clientId)
            .correlationId(correlationId);

        publicationMessage
            .streamId(streamId)
            .channel(channel);

        if (!toDriverCommandBuffer.write(ADD_PUBLICATION, buffer, 0, publicationMessage.length()))
        {
            throw new IllegalStateException("could not write publication message");
        }

        return correlationId;
    }

    public long removePublication(final long registrationId)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();
        removeMessage.correlationId(correlationId);
        removeMessage.registrationId(registrationId);

        if (!toDriverCommandBuffer.write(REMOVE_PUBLICATION, buffer, 0, RemoveMessageFlyweight.length()))
        {
            throw new IllegalStateException("could not write publication remove message");
        }

        return correlationId;
    }

    public long addSubscription(final String channel, final int streamId)
    {
        final long registrationId = -1;
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();

        subscriptionMessage
            .clientId(clientId)
            .correlationId(correlationId);

        subscriptionMessage
            .registrationCorrelationId(registrationId)
            .streamId(streamId)
            .channel(channel);

        if (!toDriverCommandBuffer.write(ADD_SUBSCRIPTION, buffer, 0, subscriptionMessage.length()))
        {
            throw new IllegalStateException("could not write subscription message");
        }

        return correlationId;
    }

    public long removeSubscription(final long registrationId)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();
        removeMessage.correlationId(correlationId);
        removeMessage.registrationId(registrationId);

        if (!toDriverCommandBuffer.write(REMOVE_SUBSCRIPTION, buffer, 0, RemoveMessageFlyweight.length()))
        {
            throw new IllegalStateException("could not write subscription remove message");
        }

        return correlationId;
    }

    public void sendClientKeepalive()
    {
        correlatedMessage
            .clientId(clientId)
            .correlationId(0);

        if (!toDriverCommandBuffer.write(CLIENT_KEEPALIVE, keepaliveBuffer, 0, CorrelatedMessageFlyweight.LENGTH))
        {
            throw new IllegalStateException("could not write keepalive message");
        }
    }
}
