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
    private final ChannelMessageFlyweight channelMessage = new ChannelMessageFlyweight();
    private final SubscriberMessageFlyweight subscriberMessage = new SubscriberMessageFlyweight();
    private final QualifiedMessageFlyweight qualifiedMessage = new QualifiedMessageFlyweight();

    private final RingBuffer conductorBuffer;

    public ClientConductorProxy(final RingBuffer conductorBuffer)
    {
        this.conductorBuffer = conductorBuffer;

        channelMessage.wrap(writeBuffer, 0);
        subscriberMessage.wrap(writeBuffer, 0);
        qualifiedMessage.wrap(writeBuffer, 0);
    }

    public void sendAddChannel(final String destination, final long sessionId, final long channelId)
    {
        sendChannelMessage(destination, sessionId, channelId, ADD_CHANNEL);
    }

    public void sendRemoveChannel(final String destination, final long sessionId, final long channelId)
    {
        sendChannelMessage(destination, sessionId, channelId, REMOVE_CHANNEL);
    }

    private void sendChannelMessage(final String destination,
                                    final long sessionId,
                                    final long channelId,
                                    final int msgTypeId)
    {
        channelMessage.sessionId(sessionId);
        channelMessage.channelId(channelId);
        channelMessage.destination(destination);
        if (! conductorBuffer.write(msgTypeId, writeBuffer, 0, channelMessage.length()))
        {
            throw new IllegalStateException("could not write channel message");
        }
    }

    public void sendAddSubscriber(final String destination, final long[] channelIds)
    {
        sendReceiverMessage(ADD_SUBSCRIBER, destination, channelIds);
    }

    public void sendRemoveSubscriber(final String destination, final long[] channelIds)
    {
        sendReceiverMessage(REMOVE_SUBSCRIBER, destination, channelIds);
    }

    private void sendReceiverMessage(final int msgTypeId, final String destination, final long[] channelIds)
    {
        subscriberMessage.channelIds(channelIds);
        subscriberMessage.destination(destination);
        if (!conductorBuffer.write(msgTypeId, writeBuffer, 0, subscriberMessage.length()))
        {
            throw new IllegalStateException("could not write receiver message");
        }
    }

    public void sendRequestTerm(final String destination, final long sessionId, final long channelId, final long termId)
    {
        qualifiedMessage.sessionId(sessionId);
        qualifiedMessage.channelId(channelId);
        qualifiedMessage.termId(termId);
        qualifiedMessage.destination(destination);
        conductorBuffer.write(REQUEST_CLEANED_TERM, writeBuffer, 0, qualifiedMessage.length());
    }
}
