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
package uk.co.real_logic.aeron;

import uk.co.real_logic.aeron.util.ClosableThread;
import uk.co.real_logic.aeron.util.collections.Long2ObjectHashMap;
import uk.co.real_logic.aeron.util.command.MediaDriverFacade;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.util.control.ChannelMessageFlyweight;
import uk.co.real_logic.aeron.util.control.ControlProtocolEvents;
import uk.co.real_logic.aeron.util.control.RemoveReceiverMessageFlyweight;
import uk.co.real_logic.aeron.util.control.TripleMessageFlyweight;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;

import java.nio.ByteBuffer;
import java.util.Map;

import static uk.co.real_logic.aeron.util.control.ControlProtocolEvents.REMOVE_RECEIVER;
import static uk.co.real_logic.aeron.util.control.ControlProtocolEvents.REQUEST_TERM;


/**
 * Admin thread to take responses and notifications from mediadriver and act on them. As well as pass commands
 * to the mediadriver.
 */
public final class AdminThread extends ClosableThread implements MediaDriverFacade
{
    /** Maximum size of the write buffer */
    public static final int WRITE_BUFFER_CAPACITY = 256;
    /** Incoming message buffer from media driver */
    private final RingBuffer recvBuffer;
    /** Incoming message buffer from other Core threads */
    private final RingBuffer commandBuffer;
    /** Outgoing message buffer to media driver */
    private final RingBuffer sendBuffer;
    /** Maximum size of the write buffer */
    private final Map<Long, Map<Long, ByteBuffer>> termBufferMap = new Long2ObjectHashMap<>();

    /** Atomic buffer to write message flyweights into before they get sent */
    private final AtomicBuffer writeBuffer = new AtomicBuffer(ByteBuffer.allocateDirect(WRITE_BUFFER_CAPACITY));

    // Control protocol Flyweights
    private final ChannelMessageFlyweight channelMessage = new ChannelMessageFlyweight();
    private final RemoveReceiverMessageFlyweight removeReceiverMessage = new RemoveReceiverMessageFlyweight();
    private final TripleMessageFlyweight requestTermMessage = new TripleMessageFlyweight();

    public AdminThread(final RingBuffer commandBuffer,
                       final RingBuffer recvBuffer,
                       final RingBuffer sendBuffer)
    {
        this.commandBuffer = commandBuffer;
        this.recvBuffer = recvBuffer;
        this.sendBuffer = sendBuffer;

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

    }

    private void handleReceiveBuffer()
    {
        recvBuffer.read((eventTypeId, buffer, index, length) ->
        {
            // TODO
        });
    }

    /* commands to MediaDriver */

    @Override
    public void sendAddChannel(final String destination, final long sessionId, final long channelId)
    {
        sendChannelMessage(destination, sessionId, channelId, ControlProtocolEvents.ADD_CHANNEL);
    }

    @Override
    public void sendRemoveChannel(final String destination, final long sessionId, final long channelId)
    {
        sendChannelMessage(destination, sessionId, channelId, ControlProtocolEvents.REMOVE_CHANNEL);
    }

    private void sendChannelMessage(final String destination,
                                    final long sessionId,
                                    final long channelId,
                                    final int eventTypeId)
    {
        channelMessage.sessionId(sessionId);
        channelMessage.channelId(channelId);
        channelMessage.destination(destination);
        sendBuffer.write(eventTypeId, writeBuffer, 0, channelMessage.length());
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

    public void sendRemoveReceiver(final String destination)
    {
        removeReceiverMessage.destination(destination);
        sendBuffer.write(REMOVE_RECEIVER, writeBuffer, 0, removeReceiverMessage.length());
    }

    public void sendRequestTerm(final long sessionId, final long channelId, final long termId)
    {
        requestTermMessage.sessionId(sessionId);
        requestTermMessage.channelId(channelId);
        requestTermMessage.termId(termId);
        sendBuffer.write(REQUEST_TERM, writeBuffer, 0, TripleMessageFlyweight.length());
    }

    /* callbacks from MediaDriver */

    public void onStatusMessage(final HeaderFlyweight header)
    {
    }

    public void onErrorResponse(final int code, final byte[] message)
    {
    }

    public void onError(final int code, final byte[] message)
    {
    }

    public void onNewBufferNotification(final long sessionId, final long channelId, final long termId, final boolean isSender)
    {
    }

}
