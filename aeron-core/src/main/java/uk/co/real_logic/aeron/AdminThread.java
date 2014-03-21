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
import uk.co.real_logic.aeron.util.control.ChannelMessageFlyweight;
import uk.co.real_logic.aeron.util.control.RequestTermFlyweight;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;
import uk.co.real_logic.aeron.util.command.MediaDriverFacade;
import uk.co.real_logic.aeron.util.control.RemoveReceiverMessageFlyweight;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static uk.co.real_logic.aeron.util.protocol.HeaderFlyweight.*;

/**
 * Admin thread to take responses and notifications from mediadriver and act on them. As well as pass commands
 * to the mediadriver.
 */
public final class AdminThread extends ClosableThread implements MediaDriverFacade
{
    // TODO: add correct types once comms buffers are committed, and replace reset calls
    private final ByteBuffer recvBuffer;
    private final ByteBuffer commandBuffer;
    private final ByteBuffer sendBuffer;
    private final Map<Long, Map<Long, ByteBuffer>> termBufferMap;

    // Control protocol Flyweights

    private final ChannelMessageFlyweight channelMessage = new ChannelMessageFlyweight();
    private final RemoveReceiverMessageFlyweight removeReceiverMessage = new RemoveReceiverMessageFlyweight();
    private final RequestTermFlyweight requestTermMessage = new RequestTermFlyweight();

    public AdminThread(final ByteBuffer commandBuffer,
                       final ByteBuffer recvBuffer,
                       final ByteBuffer sendBuffer)
    {
        this.commandBuffer = commandBuffer;
        this.recvBuffer = recvBuffer;
        this.sendBuffer = sendBuffer;
        termBufferMap = new Long2ObjectHashMap<>();
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

    }

    /* commands to MediaDriver */

    @Override
    public void sendAddChannel(final String destination, final long sessionId, final long channelId)
    {
        sendChannelMessage(destination, sessionId, channelId, HDR_TYPE_ADD_CHANNEL);
    }

    @Override
    public void sendRemoveChannel(final String destination, final long sessionId, final long channelId)
    {
        sendChannelMessage(destination, sessionId, channelId, HDR_TYPE_REMOVE_CHANNEL);
    }

    private void sendChannelMessage(final String destination,
                                    final long sessionId,
                                    final long channelId,
                                    final short type)
    {
        channelMessage.reset(sendBuffer);
        channelMessage.currentVersion();
        channelMessage.headerType(type);
        channelMessage.destination(destination);
        channelMessage.sessionId(sessionId);
        channelMessage.channelId(channelId);
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
        removeReceiverMessage.reset(sendBuffer);
        removeReceiverMessage.currentVersion();
        removeReceiverMessage.headerType(HDR_TYPE_REMOVE_RECEIVER);
        removeReceiverMessage.destination(destination);
    }

    public void sendRequestTerm(final long sessionId, final long channelId, final long termId)
    {
        requestTermMessage.reset(sendBuffer);
        requestTermMessage.currentVersion();
        requestTermMessage.headerType(HDR_TYPE_REQUEST_TERM);
        requestTermMessage.sessionId(sessionId);
        requestTermMessage.channelId(channelId);
        requestTermMessage.termId(termId);
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

    public void onLocationResponse(final List<byte[]> filenames)
    {
    }

    public void onNewSession(final long sessionId, final List<byte[]> filenames)
    {
    }
}