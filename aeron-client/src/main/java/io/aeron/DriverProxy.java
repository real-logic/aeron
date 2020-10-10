/*
 * Copyright 2014-2020 Real Logic Limited.
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
 * <b>Note:</b> this class is not thread safe and is expecting to be called within {@link Aeron.Context#clientLock()}.
 */
public final class DriverProxy
{
    private final MutableDirectBuffer buffer = new ExpandableArrayBuffer(1024);
    private final PublicationMessageFlyweight publicationMessage = new PublicationMessageFlyweight();
    private final SubscriptionMessageFlyweight subscriptionMessage = new SubscriptionMessageFlyweight();
    private final RemoveMessageFlyweight removeMessage = new RemoveMessageFlyweight();
    private final CorrelatedMessageFlyweight correlatedMessage = new CorrelatedMessageFlyweight();
    private final DestinationMessageFlyweight destinationMessage = new DestinationMessageFlyweight();
    private final CounterMessageFlyweight counterMessage = new CounterMessageFlyweight();
    private final TerminateDriverFlyweight terminateDriver = new TerminateDriverFlyweight();
    private final RingBuffer toDriverCommandBuffer;

    /**
     * Create a proxy to a media driver which sends commands via a {@link RingBuffer}.
     *
     * @param toDriverCommandBuffer to send commands via.
     * @param clientId              to represent the client.
     */
    public DriverProxy(final RingBuffer toDriverCommandBuffer, final long clientId)
    {
        this.toDriverCommandBuffer = toDriverCommandBuffer;

        publicationMessage.wrap(buffer, 0);
        subscriptionMessage.wrap(buffer, 0);
        correlatedMessage.wrap(buffer, 0);
        removeMessage.wrap(buffer, 0);
        destinationMessage.wrap(buffer, 0);
        counterMessage.wrap(buffer, 0);
        terminateDriver.wrap(buffer, 0);

        correlatedMessage.clientId(clientId);
    }

    /**
     * Time of the last heartbeat to indicate the driver is alive.
     *
     * @return time of the last heartbeat to indicate the driver is alive.
     */
    public long timeOfLastDriverKeepaliveMs()
    {
        return toDriverCommandBuffer.consumerHeartbeatTime();
    }

    /**
     * Instruct the driver to add a concurrent publication.
     *
     * @param channel  uri in string format.
     * @param streamId within the channel.
     * @return the correlation id for the command.
     */
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

    /**
     * Instruct the driver to add a non-concurrent, i.e. exclusive, publication.
     *
     * @param channel  uri in string format.
     * @param streamId within the channel.
     * @return the correlation id for the command.
     */
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

    /**
     * Instruct the driver to remove a publication by its registration id.
     *
     * @param registrationId for the publication to be removed.
     * @return the correlation id for the command.
     */
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

    /**
     * Instruct the driver to add a subscription.
     *
     * @param channel  uri in string format.
     * @param streamId within the channel.
     * @return the correlation id for the command.
     */
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

    /**
     * Instruct the driver to remove a subscription by its registration id.
     *
     * @param registrationId for the subscription to be removed.
     * @return the correlation id for the command.
     */
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

    /**
     * Add a destination to the send channel of an existing MDC Publication.
     *
     * @param registrationId  of the Publication.
     * @param endpointChannel for the destination.
     * @return the correlation id for the command.
     */
    public long addDestination(final long registrationId, final String endpointChannel)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();

        destinationMessage
            .registrationCorrelationId(registrationId)
            .channel(endpointChannel)
            .correlationId(correlationId);

        if (!toDriverCommandBuffer.write(ADD_DESTINATION, buffer, 0, destinationMessage.length()))
        {
            throw new AeronException("could not write add destination command");
        }

        return correlationId;
    }

    /**
     * Remove a destination from the send channel of an existing MDC Publication.
     *
     * @param registrationId  of the Publication.
     * @param endpointChannel used for the {@link #addDestination(long, String)} command.
     * @return the correlation id for the command.
     */
    public long removeDestination(final long registrationId, final String endpointChannel)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();

        destinationMessage
            .registrationCorrelationId(registrationId)
            .channel(endpointChannel)
            .correlationId(correlationId);

        if (!toDriverCommandBuffer.write(REMOVE_DESTINATION, buffer, 0, destinationMessage.length()))
        {
            throw new AeronException("could not write remove destination command");
        }

        return correlationId;
    }

    /**
     * Add a destination to the receive channel of an existing MDS Subscription.
     *
     * @param registrationId  of the Subscription.
     * @param endpointChannel for the destination.
     * @return the correlation id for the command.
     */
    public long addRcvDestination(final long registrationId, final String endpointChannel)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();

        destinationMessage
            .registrationCorrelationId(registrationId)
            .channel(endpointChannel)
            .correlationId(correlationId);

        if (!toDriverCommandBuffer.write(ADD_RCV_DESTINATION, buffer, 0, destinationMessage.length()))
        {
            throw new AeronException("could not write add rcv destination command");
        }

        return correlationId;
    }


    /**
     * Remove a destination from the receive channel of an existing MDS Subscription.
     *
     * @param registrationId  of the Subscription.
     * @param endpointChannel used for the {@link #addRcvDestination(long, String)} command.
     * @return the correlation id for the command.
     */
    public long removeRcvDestination(final long registrationId, final String endpointChannel)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();

        destinationMessage
            .registrationCorrelationId(registrationId)
            .channel(endpointChannel)
            .correlationId(correlationId);

        if (!toDriverCommandBuffer.write(REMOVE_RCV_DESTINATION, buffer, 0, destinationMessage.length()))
        {
            throw new AeronException("could not write remove rcv destination command");
        }

        return correlationId;
    }

    /**
     * Add a new counter with a type id plus the label and key are provided in buffers.
     *
     * @param typeId      for associating with the counter.
     * @param keyBuffer   containing the metadata key.
     * @param keyOffset   offset at which the key begins.
     * @param keyLength   length in bytes for the key.
     * @param labelBuffer containing the label.
     * @param labelOffset offset at which the label begins.
     * @param labelLength length in bytes for the label.
     * @return the correlation id for the command.
     */
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

    /**
     * Add a new counter with a type id and label, the key will be blank.
     *
     * @param typeId for associating with the counter.
     * @param label  that is human readable for the counter.
     * @return the correlation id for the command.
     */
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

    /**
     * Instruct the media driver to remove an existing counter by its registration id.
     *
     * @param registrationId of counter to remove.
     * @return the correlation id for the command.
     */
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

    /**
     * Notify the media driver that this client is closing.
     */
    public void clientClose()
    {
        correlatedMessage.correlationId(Aeron.NULL_VALUE);
        toDriverCommandBuffer.write(CLIENT_CLOSE, buffer, 0, CorrelatedMessageFlyweight.LENGTH);
    }

    /**
     * Instruct the media driver to terminate.
     *
     * @param tokenBuffer containing the authentication token.
     * @param tokenOffset at which the token begins.
     * @param tokenLength in bytes.
     * @return true is successfully sent.
     */
    public boolean terminateDriver(final DirectBuffer tokenBuffer, final int tokenOffset, final int tokenLength)
    {
        correlatedMessage.correlationId(Aeron.NULL_VALUE);
        terminateDriver.tokenBuffer(tokenBuffer, tokenOffset, tokenLength);

        return toDriverCommandBuffer.write(TERMINATE_DRIVER, buffer, 0, terminateDriver.length());
    }
}
