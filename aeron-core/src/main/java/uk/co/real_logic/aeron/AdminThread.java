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
import uk.co.real_logic.aeron.util.collections.Long2ObjectOpenAddressingHashMap;
import uk.co.real_logic.aeron.util.protocol.ChannelMessageFlyweight;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;
import uk.co.real_logic.aeron.util.command.MediaDriverFacade;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static uk.co.real_logic.aeron.util.protocol.HeaderFlyweight.HDR_TYPE_ADD_CHANNEL;
import static uk.co.real_logic.aeron.util.protocol.HeaderFlyweight.HDR_TYPE_REMOVE_CHANNEL;

/**
 * Admin thread to take responses and notifications from mediadriver and act on them. As well as pass commands to the mediadriver.
 */
public final class AdminThread extends ClosableThread implements MediaDriverFacade
{
    // TODO: add correct types once comms buffers are committed, and replace reset calls
    private final ByteBuffer recvBuffer;
    private final ByteBuffer commandBuffer;
    private final ByteBuffer sendBuffer;
    private final Map<Long, Map<Long, ByteBuffer>> termBufferMap;

    // Message protocol Flyweights

    private final ChannelMessageFlyweight channelMessage = new ChannelMessageFlyweight();

    public AdminThread(final ByteBuffer commandBuffer,
                       final ByteBuffer recvBuffer,
                       final ByteBuffer sendBuffer)
    {
        this.commandBuffer = commandBuffer;
        this.recvBuffer = recvBuffer;
        this.sendBuffer = sendBuffer;
        termBufferMap = new Long2ObjectOpenAddressingHashMap<>();
    }

    public void process()
    {
        // read from recvBuffer and delegate to event handlers
    }

    /* commands to MediaDriver */

    @Override
    public void sendAddChannel(final String destination, final long sessionId, final long channelId)
    {
        channelMessage.reset(sendBuffer, 0);
        channelMessage.headerType(HDR_TYPE_ADD_CHANNEL);
        channelMessage.destination(destination);
        channelMessage.sessionId(sessionId);
        channelMessage.channelId(channelId);
    }

    @Override
    public void sendRemoveChannel(final String destination, final long sessionId, final long channelId)
    {
        channelMessage.reset(sendBuffer, 0);
        channelMessage.headerType(HDR_TYPE_REMOVE_CHANNEL);
        channelMessage.destination(destination);
        channelMessage.sessionId(sessionId);
        channelMessage.channelId(channelId);
    }

    @Override
    public void sendRemoveTerm(final String destination, final long sessionId, final long channelId, final long termId)
    {

    }

    @Override
    public void sendAddReceiver(final String destination, final long[] channelIdList)
    {

    }

    @Override
    public void sendRemoveReceiver(final String destination)
    {
        
    }

    /* callbacks from MediaDriver */

    @Override
    public void onStatusMessage(final HeaderFlyweight header)
    {

    }

    @Override
    public void onErrorResponse(final int code, final byte[] message)
    {

    }

    @Override
    public void onError(final int code, final byte[] message)
    {

    }

    @Override
    public void onLocationResponse(final List<byte[]> filenames)
    {

    }

    @Override
    public void onNewSession(final long sessionId, final List<byte[]> filenames)
    {

    }

}
