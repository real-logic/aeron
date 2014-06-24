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

import uk.co.real_logic.aeron.util.command.*;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;

import java.nio.ByteBuffer;

import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.*;

/**
 * Separates the concern of communicating with the client conductor away from the rest of the client.
 *
 * Writes messages into the client conductor buffer.
 */
public class ClientConductorProxy
{
    /** Maximum size of the write buffer */
    public static final int MSG_BUFFER_CAPACITY = 1024;

    private final AtomicBuffer writeBuffer = new AtomicBuffer(ByteBuffer.allocateDirect(MSG_BUFFER_CAPACITY));
    private final PublicationMessageFlyweight publicationMessage = new PublicationMessageFlyweight();
    private final SubscriptionMessageFlyweight subscriptionMessage = new SubscriptionMessageFlyweight();
    private final QualifiedMessageFlyweight qualifiedMessage = new QualifiedMessageFlyweight();

    private final RingBuffer conductorBuffer;

    public ClientConductorProxy(final RingBuffer conductorBuffer)
    {
        this.conductorBuffer = conductorBuffer;

        publicationMessage.wrap(writeBuffer, 0);
        subscriptionMessage.wrap(writeBuffer, 0);
        qualifiedMessage.wrap(writeBuffer, 0);
    }

    public void addPublication(final String destination, final long sessionId, final long channelId)
    {
        sendPublicationMessage(destination, sessionId, channelId, ADD_PUBLICATION);
    }

    public void removePublication(final String destination, final long sessionId, final long channelId)
    {
        sendPublicationMessage(destination, sessionId, channelId, REMOVE_PUBLICATION);
    }

    public void addSubscription(final String destination, final long[] channelIds)
    {
        sendSubscriptionMessage(ADD_SUBSCRIPTION, destination, channelIds);
    }

    public void removeSubscription(final String destination, final long[] channelIds)
    {
        sendSubscriptionMessage(REMOVE_SUBSCRIPTION, destination, channelIds);
    }

    private void sendPublicationMessage(final String destination,
                                        final long sessionId,
                                        final long channelId,
                                        final int msgTypeId)
    {
        publicationMessage.sessionId(sessionId);
        publicationMessage.channelId(channelId);
        publicationMessage.destination(destination);

        if (!conductorBuffer.write(msgTypeId, writeBuffer, 0, publicationMessage.length()))
        {
            throw new IllegalStateException("could not write channel message");
        }
    }

    private void sendSubscriptionMessage(final int msgTypeId, final String destination, final long[] channelIds)
    {
        subscriptionMessage.channelIds(channelIds);
        subscriptionMessage.destination(destination);

        if (!conductorBuffer.write(msgTypeId, writeBuffer, 0, subscriptionMessage.length()))
        {
            throw new IllegalStateException("could not write subscription message");
        }
    }

    public void sendRequestTerm(final String destination, final long sessionId, final long channelId, final long termId)
    {
        qualifiedMessage.sessionId(sessionId);
        qualifiedMessage.channelId(channelId);
        qualifiedMessage.termId(termId);
        qualifiedMessage.destination(destination);

        if (!conductorBuffer.write(CLEAN_TERM_BUFFER, writeBuffer, 0, qualifiedMessage.length()))
        {
            throw new IllegalStateException("could not write subscription message");
        }
    }
}
