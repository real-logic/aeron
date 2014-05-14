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

import uk.co.real_logic.aeron.util.command.ChannelMessageFlyweight;
import uk.co.real_logic.aeron.util.command.QualifiedMessageFlyweight;
import uk.co.real_logic.aeron.util.command.SubscriberMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;

import java.nio.ByteBuffer;

import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.*;

/**
 * Separates the concern of communicating with the media driver away from the rest of the client.
 *
 * Writes messages into the media driver conductor buffer.
 */
public class MediaConductorProxy
{
    /** Maximum size of the write buffer */
    public static final int WRITE_BUFFER_CAPACITY = 256;

    private final RingBuffer conductorBuffer;
    private final AtomicBuffer writeBuffer;
    private final ChannelMessageFlyweight channelMessage;
    private final SubscriberMessageFlyweight subscriberMessage;
    private final QualifiedMessageFlyweight qualifiedMessage;

    public MediaConductorProxy(final RingBuffer conductorBuffer)
    {
        this.conductorBuffer = conductorBuffer;
        this.writeBuffer = new AtomicBuffer(ByteBuffer.allocate(WRITE_BUFFER_CAPACITY));
        this.channelMessage = new ChannelMessageFlyweight();
        this.subscriberMessage = new SubscriberMessageFlyweight();
        this.qualifiedMessage = new QualifiedMessageFlyweight();

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
                                    final int eventTypeId)
    {
        channelMessage.sessionId(sessionId);
        channelMessage.channelId(channelId);
        channelMessage.destination(destination);
        conductorBuffer.write(eventTypeId, writeBuffer, 0, channelMessage.length());
    }

    public void sendAddSubscriber(final String destination, final long[] channelIds)
    {
        sendReceiverMessage(ADD_SUBSCRIBER, destination, channelIds);
    }

    public void sendRemoveSubscriber(final String destination, final long[] channelIds)
    {
        sendReceiverMessage(REMOVE_SUBSCRIBER, destination, channelIds);
    }

    private void sendReceiverMessage(final int eventTypeId, final String destination, final long[] channelIds)
    {
        subscriberMessage.channelIds(channelIds);
        subscriberMessage.destination(destination);
        conductorBuffer.write(eventTypeId, writeBuffer, 0, subscriberMessage.length());
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
