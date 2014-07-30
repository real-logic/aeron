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
package uk.co.real_logic.aeron.conductor;

import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.common.command.PublicationMessageFlyweight;
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
public class DriverProxy
{
    /** Maximum size of the write buffer */
    public static final int MSG_BUFFER_CAPACITY = 4096;

    private final AtomicBuffer writeBuffer = new AtomicBuffer(ByteBuffer.allocateDirect(MSG_BUFFER_CAPACITY));
    private final PublicationMessageFlyweight publicationMessage = new PublicationMessageFlyweight();
    private final SubscriptionMessageFlyweight subscriptionMessage = new SubscriptionMessageFlyweight();

    // the heartbeats come from the client conductor thread, so keep the flyweights and buffer separate
    private final AtomicBuffer keepaliveBuffer = new AtomicBuffer(ByteBuffer.allocateDirect(MSG_BUFFER_CAPACITY));
    private final PublicationMessageFlyweight keepalivePublicationMessage = new PublicationMessageFlyweight();
    private final SubscriptionMessageFlyweight keepaliveSubscriptionMessage = new SubscriptionMessageFlyweight();

    private final RingBuffer mediaDriverCommandBuffer;

    public DriverProxy(final RingBuffer mediaDriverCommandBuffer)
    {
        this.mediaDriverCommandBuffer = mediaDriverCommandBuffer;

        publicationMessage.wrap(writeBuffer, 0);
        subscriptionMessage.wrap(writeBuffer, 0);

        keepalivePublicationMessage.wrap(keepaliveBuffer, 0);
        keepaliveSubscriptionMessage.wrap(keepaliveBuffer, 0);
    }

    public long addPublication(final String channel, final int sessionId, final int streamId)
    {
        return sendPublicationMessage(channel, sessionId, streamId, ADD_PUBLICATION);
    }

    public long removePublication(final String channel, final int sessionId, final int streamId)
    {
        return sendPublicationMessage(channel, sessionId, streamId, REMOVE_PUBLICATION);
    }

    public long addSubscription(final String channel, final int streamId)
    {
        return sendSubscriptionMessage(ADD_SUBSCRIPTION, channel, streamId, -1);
    }

    public long removeSubscription(final String channel, final int streamId, final long registrationCorrelationId)
    {
        return sendSubscriptionMessage(REMOVE_SUBSCRIPTION, channel, streamId, registrationCorrelationId);
    }

    public void keepalivePublication(final Publication publication)
    {
        keepalivePublicationMessage.correlationId(publication.correlationId());
        keepalivePublicationMessage.sessionId(publication.sessionId());
        keepalivePublicationMessage.streamId(publication.streamId());
        keepalivePublicationMessage.channel(publication.channel());

        if (!mediaDriverCommandBuffer.write(KEEPALIVE_PUBLICATION, keepaliveBuffer, 0, keepalivePublicationMessage.length()))
        {
            throw new IllegalStateException("could not write publication heartbeat");
        }
    }

    public void keepaliveSubscription(final Subscription subscription)
    {
        keepaliveSubscriptionMessage.registrationCorrelationId(subscription.correlationId());
        keepaliveSubscriptionMessage.correlationId(subscription.correlationId());
        keepaliveSubscriptionMessage.streamId(subscription.streamId());
        keepaliveSubscriptionMessage.channel(subscription.channel());

        if (!mediaDriverCommandBuffer.write(KEEPALIVE_SUBSCRIPTION, keepaliveBuffer, 0, keepaliveSubscriptionMessage.length()))
        {
            throw new IllegalStateException("could not write subscription heartbeat");
        }
    }

    private long sendPublicationMessage(final String channel, final int sessionId, final int streamId, final int msgTypeId)
    {
        final long correlationId = mediaDriverCommandBuffer.nextCorrelationId();

        publicationMessage.correlationId(correlationId);
        publicationMessage.sessionId(sessionId);
        publicationMessage.streamId(streamId);
        publicationMessage.channel(channel);

        if (!mediaDriverCommandBuffer.write(msgTypeId, writeBuffer, 0, publicationMessage.length()))
        {
            throw new IllegalStateException("could not write publication message");
        }

        return correlationId;
    }

    private long sendSubscriptionMessage(final int msgTypeId,
                                         final String channel,
                                         final int streamId,
                                         final long registrationCorrelationId)
    {
        final long correlationId = mediaDriverCommandBuffer.nextCorrelationId();

        subscriptionMessage.registrationCorrelationId(registrationCorrelationId);
        subscriptionMessage.correlationId(correlationId);
        subscriptionMessage.streamId(streamId);
        subscriptionMessage.channel(channel);

        if (!mediaDriverCommandBuffer.write(msgTypeId, writeBuffer, 0, subscriptionMessage.length()))
        {
            throw new IllegalStateException("could not write subscription message");
        }

        return correlationId;
    }
}
