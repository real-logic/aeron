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
import uk.co.real_logic.aeron.util.command.CompletelyIdentifiedMessageFlyweight;
import uk.co.real_logic.aeron.util.command.SubscriberMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;

import java.nio.ByteBuffer;

import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.*;

/**
 * Separates the concern of talking the media driver protocol away from the rest of the API.
 *
 * Writes messages into the Client Admin Thread's conductor buffer.
 */
public class ClientConductorCursor
{
    /** Maximum size of the write buffer */
    public static final int WRITE_BUFFER_CAPACITY = 256;

    private final RingBuffer conductorBuffer;
    private final AtomicBuffer writeBuffer;
    private final ChannelMessageFlyweight channelMessage;
    private final SubscriberMessageFlyweight removeSubscriberMessage;
    private final CompletelyIdentifiedMessageFlyweight requestTermMessage;

    public ClientConductorCursor(final RingBuffer conductorBuffer)
    {
        this.conductorBuffer = conductorBuffer;
        this.writeBuffer = new AtomicBuffer(ByteBuffer.allocate(WRITE_BUFFER_CAPACITY));
        this.channelMessage = new ChannelMessageFlyweight();
        this.removeSubscriberMessage = new SubscriberMessageFlyweight();
        this.requestTermMessage = new CompletelyIdentifiedMessageFlyweight();

        channelMessage.wrap(writeBuffer, 0);
        removeSubscriberMessage.wrap(writeBuffer, 0);
        requestTermMessage.wrap(writeBuffer, 0);
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

    public void sendAddSubscriber(final String destination, final long[] channelIdList)
    {
        sendReceiverMessage(ADD_SUBSCRIBER, destination, channelIdList);
    }

    public void sendRemoveSubscriber(final String destination, final long[] channelIdList)
    {
        sendReceiverMessage(REMOVE_SUBSCRIBER, destination, channelIdList);
    }

    private void sendReceiverMessage(final int eventTypeId, final String destination, final long[] channelIdList)
    {
        removeSubscriberMessage.channelIds(channelIdList);
        removeSubscriberMessage.destination(destination);
        conductorBuffer.write(eventTypeId, writeBuffer, 0, removeSubscriberMessage.length());
    }

    public void sendRequestTerm(final String destination, final long sessionId, final long channelId, final long termId)
    {
        requestTermMessage.sessionId(sessionId);
        requestTermMessage.channelId(channelId);
        requestTermMessage.termId(termId);
        requestTermMessage.destination(destination);
        conductorBuffer.write(REQUEST_CLEANED_TERM, writeBuffer, 0, requestTermMessage.length());
    }
}
