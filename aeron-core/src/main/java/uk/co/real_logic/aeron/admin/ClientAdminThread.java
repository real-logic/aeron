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

import uk.co.real_logic.aeron.util.ClosableThread;
import uk.co.real_logic.aeron.util.collections.Long2ObjectHashMap;
import uk.co.real_logic.aeron.util.command.ChannelMessageFlyweight;
import uk.co.real_logic.aeron.util.command.CompletelyIdentifiedMessageFlyweight;
import uk.co.real_logic.aeron.util.command.MediaDriverFacade;
import uk.co.real_logic.aeron.util.command.ReceiverMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;

import java.nio.ByteBuffer;
import java.util.Map;

import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.NEW_RECEIVE_BUFFER_NOTIFICATION;
import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.NEW_SEND_BUFFER_NOTIFICATION;


/**
 * Admin thread to take responses and notifications from mediadriver and act on them. As well as pass commands
 * to the mediadriver.
 */
public final class ClientAdminThread extends ClosableThread implements MediaDriverFacade
{
    /** Maximum size of the write buffer */
    public static final int WRITE_BUFFER_CAPACITY = 256;
    /** Incoming message buffer from media driver */
    private final RingBuffer recvBuffer;
    private final long sessionId;
    /** Incoming message buffer from other Core threads */
    private final RingBuffer commandBuffer;
    /** Outgoing message buffer to media driver */
    private final RingBuffer sendBuffer;

    private final BufferUsageStrategy bufferUsage;
    private final Map<String, Long2ObjectHashMap<TermBufferNotifier>> sendNotifiers;
    private final Map<String, Long2ObjectHashMap<TermBufferNotifier>> recvNotifiers;

    /** Atomic buffer to write message flyweights into before they get sent */
    private final AtomicBuffer writeBuffer = new AtomicBuffer(ByteBuffer.allocate(WRITE_BUFFER_CAPACITY));

    // Control protocol Flyweights
    private final ChannelMessageFlyweight channelMessage = new ChannelMessageFlyweight();
    private final ReceiverMessageFlyweight removeReceiverMessage = new ReceiverMessageFlyweight();
    private final CompletelyIdentifiedMessageFlyweight requestTermMessage = new CompletelyIdentifiedMessageFlyweight();

    private final CompletelyIdentifiedMessageFlyweight bufferNotificationMessage = new CompletelyIdentifiedMessageFlyweight();

    public ClientAdminThread(final long sessionId,
                             final RingBuffer commandBuffer,
                             final RingBuffer recvBuffer,
                             final RingBuffer sendBuffer,
                             final BufferUsageStrategy bufferUsage,
                             final Map<String, Long2ObjectHashMap<TermBufferNotifier>> sendNotifiers,
                             final Map<String, Long2ObjectHashMap<TermBufferNotifier>> recvNotifiers)
    {
        this.sessionId = sessionId;
        this.commandBuffer = commandBuffer;
        this.recvBuffer = recvBuffer;
        this.sendBuffer = sendBuffer;
        this.bufferUsage = bufferUsage;
        this.sendNotifiers = sendNotifiers;
        this.recvNotifiers = recvNotifiers;

        channelMessage.reset(writeBuffer, 0);
        removeReceiverMessage.reset(writeBuffer, 0);
        requestTermMessage.reset(writeBuffer, 0);
    }

    public void process()
    {
        handleReceiveBuffer();
        handleCommandBuffer();
    }

    private void handleCommandBuffer()
    {
        commandBuffer.read((eventTypeId, buffer, index, length) ->
        {
            // TODO
        });
    }

    private void handleReceiveBuffer()
    {
        recvBuffer.read((eventTypeId, buffer, index, length) ->
        {
            switch (eventTypeId)
            {
                case NEW_RECEIVE_BUFFER_NOTIFICATION:
                case NEW_SEND_BUFFER_NOTIFICATION:
                    bufferNotificationMessage.reset(buffer, index);
                    final boolean isSender = eventTypeId == NEW_SEND_BUFFER_NOTIFICATION;
                    onNewBufferNotification(bufferNotificationMessage.sessionId(),
                                            bufferNotificationMessage.channelId(),
                                            bufferNotificationMessage.termId(),
                                            isSender,
                                            bufferNotificationMessage.destination());
                    return;
            }
        });
    }

    /* commands to MediaDriver */

    public void sendAddChannel(final String destination, final long sessionId, final long channelId)
    {

    }

    public void sendRemoveChannel(final String destination, final long sessionId, final long channelId)
    {

    }

    public void sendRemoveTerm(final String destination,
                               final long sessionId,
                               final long channelId,
                               final long termId)
    {

    }

    public void sendAddReceiver(final String destination, final long[] channelIdList)
    {

    }

    public void sendRemoveReceiver(final String destination, final long[] channelIdList)
    {

    }

    public void sendRequestTerm(final long sessionId, final long channelId, final long termId)
    {

    }

    /* callbacks from MediaDriver */

    public void onErrorResponse(final int code, final byte[] message)
    {
    }

    public void onError(final int code, final byte[] message)
    {
    }

    public void onNewBufferNotification(final long sessionId, final long channelId, final long termId, final boolean isSender, final String destination)
    {
        if (sessionId != this.sessionId)
        {
            return;
        }

        try
        {
            bufferUsage.onTermAdded(channelId, termId, isSender);
        }
        catch (Exception e)
        {
            // TODO: establish correct client error handling strategy
            e.printStackTrace();
        }
    }

}
