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
package uk.co.real_logic.aeron.admin;

import uk.co.real_logic.aeron.util.command.ChannelMessageFlyweight;
import uk.co.real_logic.aeron.util.command.CompletelyIdentifiedMessageFlyweight;
import uk.co.real_logic.aeron.util.command.ReceiverMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;

import java.nio.ByteBuffer;

import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.*;

/**
 * Separates the concern of talking the media driver protocol away from the rest of the API.
 *
 * Writes messages into the Client Admin Thread's admin buffer.
 */
public class ClientAdminThreadCursor
{
    /** Maximum size of the write buffer */
    public static final int WRITE_BUFFER_CAPACITY = 256;

    private final long sessionId;
    private final RingBuffer adminThreadCommandBuffer;
    private final AtomicBuffer writeBuffer;
    private final ChannelMessageFlyweight channelMessage;
    private final ReceiverMessageFlyweight removeReceiverMessage;
    private final CompletelyIdentifiedMessageFlyweight requestTermMessage;

    public ClientAdminThreadCursor(final long sessionId, final RingBuffer adminThreadCommandBuffer)
    {
        this.sessionId = sessionId;
        this.adminThreadCommandBuffer = adminThreadCommandBuffer;
        this.writeBuffer = new AtomicBuffer(ByteBuffer.allocate(WRITE_BUFFER_CAPACITY));
        this.channelMessage = new ChannelMessageFlyweight();
        this.removeReceiverMessage = new ReceiverMessageFlyweight();
        this.requestTermMessage = new CompletelyIdentifiedMessageFlyweight();

        channelMessage.reset(writeBuffer, 0);
        removeReceiverMessage.reset(writeBuffer, 0);
        requestTermMessage.reset(writeBuffer, 0);
    }

    public void sendAddChannel(final String destination, final long channelId)
    {
        sendChannelMessage(destination, sessionId, channelId, ADD_CHANNEL);
    }

    public void sendRemoveChannel(final String destination, final long channelId)
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
        adminThreadCommandBuffer.write(eventTypeId, writeBuffer, 0, channelMessage.length());
    }

    public void sendAddReceiver(final String destination, final long[] channelIdList)
    {
        sendReceiverMessage(ADD_RECEIVER, destination, channelIdList);
    }

    public void sendRemoveReceiver(final String destination, final long[] channelIdList)
    {
        sendReceiverMessage(REMOVE_RECEIVER, destination, channelIdList);
    }

    private void sendReceiverMessage(final int eventTypeId, final String destination, final long[] channelIdList)
    {
        removeReceiverMessage.channelIds(channelIdList);
        removeReceiverMessage.destination(destination);
        adminThreadCommandBuffer.write(eventTypeId, writeBuffer, 0, removeReceiverMessage.length());
    }

    public void sendRequestTerm(final long channelId, final long termId, final String destination)
    {
        requestTermMessage.sessionId(sessionId);
        requestTermMessage.channelId(channelId);
        requestTermMessage.termId(termId);
        requestTermMessage.destination(destination);
        adminThreadCommandBuffer.write(REQUEST_TERM, writeBuffer, 0, requestTermMessage.length());
    }

}
