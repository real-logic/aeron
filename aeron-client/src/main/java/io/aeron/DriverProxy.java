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
package io.aeron;

import io.aeron.command.*;
import io.aeron.exceptions.AeronException;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;

import static io.aeron.command.ControlProtocolEvents.*;

/**
 * Separates the concern of communicating with the client conductor away from the rest of the client.
 * <p>
 * Writes commands into the client conductor buffer.
 * <p>
 * Note: this class is not thread safe and is expecting to be called under the {@link ClientConductor} main lock.
 */
public class DriverProxy
{
    private final MutableDirectBuffer buffer = new ExpandableArrayBuffer(1024);
    private final PublicationMessageFlyweight publicationMessage = new PublicationMessageFlyweight();
    private final SubscriptionMessageFlyweight subscriptionMessage = new SubscriptionMessageFlyweight();
    private final RemoveMessageFlyweight removeMessage = new RemoveMessageFlyweight();
    private final CorrelatedMessageFlyweight correlatedMessage = new CorrelatedMessageFlyweight();
    private final DestinationMessageFlyweight destinationMessage = new DestinationMessageFlyweight();
    private final CounterMessageFlyweight counterMessage = new CounterMessageFlyweight();
    private final RingBuffer toDriverCommandBuffer;

    public DriverProxy(final RingBuffer toDriverCommandBuffer, final long clientId)
    {
        this.toDriverCommandBuffer = toDriverCommandBuffer;

        publicationMessage.wrap(buffer, 0);
        subscriptionMessage.wrap(buffer, 0);
        correlatedMessage.wrap(buffer, 0);
        removeMessage.wrap(buffer, 0);
        destinationMessage.wrap(buffer, 0);
        counterMessage.wrap(buffer, 0);

        correlatedMessage.clientId(clientId);
    }

    public long timeOfLastDriverKeepaliveMs()
    {
        return toDriverCommandBuffer.consumerHeartbeatTime();
    }

    public long addPublication(final String channel, final int streamId)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();

        publicationMessage.correlationId(correlationId);
        publicationMessage
            .streamId(streamId)
            .channel(channel);

        if (!toDriverCommandBuffer.write(ADD_PUBLICATION, buffer, 0, publicationMessage.length()))
        {
            throw new AeronException("could not write add publication command");
        }

        return correlationId;
    }

    public long addExclusivePublication(final String channel, final int streamId)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();

        publicationMessage.correlationId(correlationId);
        publicationMessage
            .streamId(streamId)
            .channel(channel);

        if (!toDriverCommandBuffer.write(ADD_EXCLUSIVE_PUBLICATION, buffer, 0, publicationMessage.length()))
        {
            throw new AeronException("could not write add exclusive publication command");
        }

        return correlationId;
    }

    public long removePublication(final long registrationId)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();

        removeMessage
            .registrationId(registrationId)
            .correlationId(correlationId);

        if (!toDriverCommandBuffer.write(REMOVE_PUBLICATION, buffer, 0, RemoveMessageFlyweight.length()))
        {
            throw new AeronException("could not write remove publication command");
        }

        return correlationId;
    }

    public long addSubscription(final String channel, final int streamId)
    {
        final long registrationId = Aeron.NULL_VALUE;
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();

        subscriptionMessage.correlationId(correlationId);
        subscriptionMessage
            .registrationCorrelationId(registrationId)
            .streamId(streamId)
            .channel(channel);

        if (!toDriverCommandBuffer.write(ADD_SUBSCRIPTION, buffer, 0, subscriptionMessage.length()))
        {
            throw new AeronException("could not write add subscription command");
        }

        return correlationId;
    }

    public long removeSubscription(final long registrationId)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();

        removeMessage
            .registrationId(registrationId)
            .correlationId(correlationId);

        if (!toDriverCommandBuffer.write(REMOVE_SUBSCRIPTION, buffer, 0, RemoveMessageFlyweight.length()))
        {
            throw new AeronException("could not write remove subscription message");
        }

        return correlationId;
    }

    public void sendClientKeepalive()
    {
        correlatedMessage.correlationId(0);

        if (!toDriverCommandBuffer.write(CLIENT_KEEPALIVE, buffer, 0, CorrelatedMessageFlyweight.LENGTH))
        {
            throw new AeronException("could not send client keepalive command");
        }
    }

    public long addDestination(final long registrationId, final String endpointChannel)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();

        destinationMessage
            .registrationCorrelationId(registrationId)
            .channel(endpointChannel)
            .correlationId(correlationId);

        if (!toDriverCommandBuffer.write(ADD_DESTINATION, buffer, 0, destinationMessage.length()))
        {
            throw new AeronException("could not write destination command");
        }

        return correlationId;
    }

    public long removeDestination(final long registrationId, final String endpointChannel)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();

        destinationMessage
            .registrationCorrelationId(registrationId)
            .channel(endpointChannel)
            .correlationId(correlationId);

        if (!toDriverCommandBuffer.write(REMOVE_DESTINATION, buffer, 0, destinationMessage.length()))
        {
            throw new AeronException("could not write destination command");
        }

        return correlationId;
    }

    public long addRcvDestination(final long registrationId, final String endpointChannel)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();

        destinationMessage
            .registrationCorrelationId(registrationId)
            .channel(endpointChannel)
            .correlationId(correlationId);

        if (!toDriverCommandBuffer.write(ADD_RCV_DESTINATION, buffer, 0, destinationMessage.length()))
        {
            throw new AeronException("could not write rcv destination command");
        }

        return correlationId;
    }

    public long removeRcvDestination(final long registrationId, final String endpointChannel)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();

        destinationMessage
            .registrationCorrelationId(registrationId)
            .channel(endpointChannel)
            .correlationId(correlationId);

        if (!toDriverCommandBuffer.write(REMOVE_RCV_DESTINATION, buffer, 0, destinationMessage.length()))
        {
            throw new AeronException("could not write rcv destination command");
        }

        return correlationId;
    }

    public long addCounter(
        final int typeId,
        final DirectBuffer keyBuffer,
        final int keyOffset,
        final int keyLength,
        final DirectBuffer labelBuffer,
        final int labelOffset,
        final int labelLength)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();

        counterMessage
            .typeId(typeId)
            .keyBuffer(keyBuffer, keyOffset, keyLength)
            .labelBuffer(labelBuffer, labelOffset, labelLength)
            .correlationId(correlationId);

        if (!toDriverCommandBuffer.write(ADD_COUNTER, buffer, 0, counterMessage.length()))
        {
            throw new AeronException("could not write add counter command");
        }

        return correlationId;
    }

    public long addCounter(final int typeId, final String label)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();

        counterMessage
            .typeId(typeId)
            .keyBuffer(null, 0, 0)
            .label(label)
            .correlationId(correlationId);

        if (!toDriverCommandBuffer.write(ADD_COUNTER, buffer, 0, counterMessage.length()))
        {
            throw new AeronException("could not write add counter command");
        }

        return correlationId;
    }

    public long removeCounter(final long registrationId)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();

        removeMessage
            .registrationId(registrationId)
            .correlationId(correlationId);

        if (!toDriverCommandBuffer.write(REMOVE_COUNTER, buffer, 0, RemoveMessageFlyweight.length()))
        {
            throw new AeronException("could not write remove counter command");
        }

        return correlationId;
    }

    public void clientClose()
    {
        correlatedMessage.correlationId(toDriverCommandBuffer.nextCorrelationId());
        toDriverCommandBuffer.write(CLIENT_CLOSE, buffer, 0, CorrelatedMessageFlyweight.LENGTH);
    }
}
