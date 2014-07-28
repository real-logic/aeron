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
import uk.co.real_logic.aeron.common.command.QualifiedMessageFlyweight;
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
    private final QualifiedMessageFlyweight qualifiedMessage = new QualifiedMessageFlyweight();

    // the heartbeats come from the client conductor thread, so keep the flyweights and buffer separate
    private final AtomicBuffer heartbeatBuffer = new AtomicBuffer(ByteBuffer.allocateDirect(MSG_BUFFER_CAPACITY));
    private final PublicationMessageFlyweight heartbeatPublicationMessage = new PublicationMessageFlyweight();
    private final SubscriptionMessageFlyweight heartbeatSubscriptionMessage = new SubscriptionMessageFlyweight();

    private final RingBuffer mediaDriverCommandBuffer;

    public DriverProxy(final RingBuffer mediaDriverCommandBuffer)
    {
        this.mediaDriverCommandBuffer = mediaDriverCommandBuffer;

        publicationMessage.wrap(writeBuffer, 0);
        subscriptionMessage.wrap(writeBuffer, 0);
        qualifiedMessage.wrap(writeBuffer, 0);

        heartbeatPublicationMessage.wrap(heartbeatBuffer, 0);
        heartbeatSubscriptionMessage.wrap(heartbeatBuffer, 0);
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
        return sendSubscriptionMessage(ADD_SUBSCRIPTION, channel, streamId);
    }

    public long removeSubscription(final String channel, final int streamId)
    {
        return sendSubscriptionMessage(REMOVE_SUBSCRIPTION, channel, streamId);
    }

    public void heartbeatPublication(final Publication.PublicationHeartbeatInfo heartbeatInfo)
    {
        heartbeatPublicationMessage.correlationId(heartbeatInfo.correlationId());
        heartbeatPublicationMessage.sessionId(heartbeatInfo.sessionId());
        heartbeatPublicationMessage.streamId(heartbeatInfo.streamId());
        heartbeatPublicationMessage.channel(heartbeatInfo.channel());

        if (!mediaDriverCommandBuffer.write(HEARTBEAT_PUBLICATION, heartbeatBuffer, 0, heartbeatPublicationMessage.length()))
        {
            throw new IllegalStateException("could not write publication heartbeat");
        }
    }

    public void heartbeatSubscription(final Subscription.SubscriptionHeartbeatInfo heartbeatInfo)
    {
        heartbeatSubscriptionMessage.correlationId(heartbeatInfo.correlationId());
        heartbeatSubscriptionMessage.streamId(heartbeatInfo.streamId());
        heartbeatSubscriptionMessage.channel(heartbeatInfo.channel());

        if (!mediaDriverCommandBuffer.write(HEARTBEAT_SUBSCRIPTION, heartbeatBuffer, 0, heartbeatSubscriptionMessage.length()))
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

    private long sendSubscriptionMessage(final int msgTypeId, final String channel, final int streamId)
    {
        final long correlationId = mediaDriverCommandBuffer.nextCorrelationId();

        subscriptionMessage.correlationId(correlationId);
        subscriptionMessage.streamId(streamId);
        subscriptionMessage.channel(channel);

        if (!mediaDriverCommandBuffer.write(msgTypeId, writeBuffer, 0, subscriptionMessage.length()))
        {
            throw new IllegalStateException("could not write subscription message");
        }

        return correlationId;
    }

    public void requestTerm(final String channel, final int sessionId, final int streamId, final int termId)
    {
        qualifiedMessage.sessionId(sessionId);
        qualifiedMessage.streamId(streamId);
        qualifiedMessage.termId(termId);
        qualifiedMessage.channel(channel);

        if (!mediaDriverCommandBuffer.write(CLEAN_TERM_BUFFER, writeBuffer, 0, qualifiedMessage.length()))
        {
            throw new IllegalStateException("could not write request terms message");
        }
    }
}
