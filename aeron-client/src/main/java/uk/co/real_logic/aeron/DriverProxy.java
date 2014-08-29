/*
 * Copyright 2014 Real Logic Ltd.
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

import uk.co.real_logic.aeron.common.command.CorrelatedMessageFlyweight;
import uk.co.real_logic.aeron.common.command.PublicationMessageFlyweight;
import uk.co.real_logic.aeron.common.command.RemoveMessageFlyweight;
import uk.co.real_logic.aeron.common.command.SubscriptionMessageFlyweight;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.ringbuffer.RingBuffer;

import java.nio.ByteBuffer;

import static uk.co.real_logic.aeron.common.command.ControlProtocolEvents.*;

/**
 * Separates the concern of communicating with the client conductor away from the rest of the client.
 *
 * Writes messages into the client conductor buffer.
 */
class DriverProxy
{
    /** Maximum size of the write buffer */
    public static final int MSG_BUFFER_CAPACITY = 4096;

    private final AtomicBuffer writeBuffer = new AtomicBuffer(ByteBuffer.allocateDirect(MSG_BUFFER_CAPACITY));
    private final PublicationMessageFlyweight publicationMessage = new PublicationMessageFlyweight();
    private final SubscriptionMessageFlyweight subscriptionMessage = new SubscriptionMessageFlyweight();
    private final RemoveMessageFlyweight removeMessage = new RemoveMessageFlyweight();

    // the heartbeats come from the client conductor thread, so keep the flyweights and buffer separate
    private final AtomicBuffer keepaliveBuffer = new AtomicBuffer(ByteBuffer.allocateDirect(MSG_BUFFER_CAPACITY));
    private final CorrelatedMessageFlyweight correlatedMessage = new CorrelatedMessageFlyweight();

    private final RingBuffer driverCommandBuffer;

    private final long clientId;

    public DriverProxy(final RingBuffer driverCommandBuffer)
    {
        this.driverCommandBuffer = driverCommandBuffer;

        publicationMessage.wrap(writeBuffer, 0);
        subscriptionMessage.wrap(writeBuffer, 0);

        correlatedMessage.wrap(keepaliveBuffer, 0);
        removeMessage.wrap(writeBuffer, 0);

        clientId = driverCommandBuffer.nextCorrelationId();
    }

    public long addPublication(final String channel, final int streamId, final int sessionId)
    {
        return sendPublicationMessage(channel, streamId, sessionId, ADD_PUBLICATION);
    }

    public long removePublication(final long registrationId)
    {
        final long correlationId = driverCommandBuffer.nextCorrelationId();
        removeMessage.correlationId(correlationId);
        removeMessage.registrationId(registrationId);

        if (!driverCommandBuffer.write(REMOVE_PUBLICATION, writeBuffer, 0, RemoveMessageFlyweight.length()))
        {
            throw new IllegalStateException("could not write publication remove message");
        }

        return correlationId;
    }

    public long addSubscription(final String channel, final int streamId)
    {
        return sendSubscriptionMessage(ADD_SUBSCRIPTION, channel, streamId, -1);
    }

    public long removeSubscription(final long registrationId)
    {
        final long correlationId = driverCommandBuffer.nextCorrelationId();
        removeMessage.correlationId(correlationId);
        removeMessage.registrationId(registrationId);

        if (!driverCommandBuffer.write(REMOVE_SUBSCRIPTION, writeBuffer, 0, RemoveMessageFlyweight.length()))
        {
            throw new IllegalStateException("could not write subscription remove message");
        }

        return correlationId;
    }

    public void keepaliveClient()
    {
        correlatedMessage.clientId(clientId);
        correlatedMessage.correlationId(0);

        if (!driverCommandBuffer.write(KEEPALIVE_CLIENT, keepaliveBuffer, 0, CorrelatedMessageFlyweight.LENGTH))
        {
            throw new IllegalStateException("could not write keepalive message");
        }
    }

    private long sendPublicationMessage(final String channel, final int streamId, final int sessionId, final int msgTypeId)
    {
        final long correlationId = driverCommandBuffer.nextCorrelationId();

        publicationMessage.clientId(clientId);
        publicationMessage.correlationId(correlationId);
        publicationMessage.streamId(streamId);
        publicationMessage.sessionId(sessionId);
        publicationMessage.channel(channel);

        if (!driverCommandBuffer.write(msgTypeId, writeBuffer, 0, publicationMessage.length()))
        {
            throw new IllegalStateException("could not write publication message");
        }

        return correlationId;
    }

    private long sendSubscriptionMessage(
        final int msgTypeId, final String channel, final int streamId, final long registrationId)
    {
        final long correlationId = driverCommandBuffer.nextCorrelationId();

        subscriptionMessage.clientId(clientId);
        subscriptionMessage.registrationCorrelationId(registrationId);
        subscriptionMessage.correlationId(correlationId);
        subscriptionMessage.streamId(streamId);
        subscriptionMessage.channel(channel);

        if (!driverCommandBuffer.write(msgTypeId, writeBuffer, 0, subscriptionMessage.length()))
        {
            throw new IllegalStateException("could not write subscription message");
        }

        return correlationId;
    }
}
