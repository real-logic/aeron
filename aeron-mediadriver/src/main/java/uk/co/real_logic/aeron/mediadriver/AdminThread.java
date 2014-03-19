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
package uk.co.real_logic.aeron.mediadriver;

import uk.co.real_logic.aeron.util.ClosableThread;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;
import uk.co.real_logic.aeron.util.command.ErrorCode;
import uk.co.real_logic.aeron.util.command.LibraryFacade;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Admin thread to take commands from library and act on them. As well as pass control information to library.
 */
public class AdminThread extends ClosableThread implements LibraryFacade
{
    private final Map<UdpDestination, SrcFrameHandler> srcDestinationMap = new HashMap<>();
    private final Map<UdpDestination, RcvFrameHandler> rcvDestinationMap = new HashMap<>();
    private final EventLoop eventLoop;
    private final BufferManagementStrategy bufferManagementStrategy;

    public AdminThread(final EventLoop eventLoop, final BufferManagementStrategy bufferManagementStrategy)
    {
        this.eventLoop = eventLoop;
        this.bufferManagementStrategy = bufferManagementStrategy;
    }

    public void work()
    {
        // TODO: read from control buffer and call onAddChannel, etc.
    }

    public void sendFlowControlResponse(final HeaderFlyweight header)
    {
        // TODO: send FCR on through to control buffer
    }

    public void sendErrorResponse(final int code, final byte[] message)
    {
        // TODO: construct error response for control buffer and write it in
    }

    public void sendError(final int code, final byte[] message)
    {
        // TODO: construct error notification for control buffer and write it in
    }

    public void sendLocationResponse(final List<byte[]> filenames)
    {
        // TODO: construct response for control buffer and write it in
    }

    public void sendNewSession(final long sessionId, final List<byte[]> filenames)
    {
        // TODO: construct notification for control buffer and write it in
    }

    public void onAddChannel(final String destination, final long sessionId, final long channelId)
    {
        try
        {
            final UdpDestination srcDestination = UdpDestination.parse(destination);
            SrcFrameHandler src = srcDestinationMap.get(srcDestination);

            if (null == src)
            {
                src = new SrcFrameHandler(srcDestination, eventLoop);
                srcDestinationMap.put(srcDestination, src);
            }

            final ByteBuffer termBuffer = bufferManagementStrategy.addSourceChannel(sessionId, channelId);

            // TODO: to handle coordination with the event loop, this could use a command queue that causes a wakeup
            // and handled by the SrcFrameHandler

            src.addSessionAndChannel(sessionId, channelId, termBuffer);
        }
        catch (Exception e)
        {
            sendErrorResponse(ErrorCode.GENERIC_ERROR.value(), e.getMessage().getBytes());
            // TODO: log this as well as send the error response
        }
    }

    public void onRemoveChannel(final String destination, final long sessionId, final long channelId)
    {
        try
        {
            final UdpDestination srcDestination = UdpDestination.parse(destination);
            final SrcFrameHandler src = srcDestinationMap.get(srcDestination);

            if (null == src)
            {
                throw new IllegalArgumentException("destination unknown for channel remove: " + destination);
            }

            src.removeSessionAndChannel(sessionId, channelId);

            // TODO: only remove if no more channels.
            //srcDestinationMap.remove(srcDestination);
        }
        catch (Exception e)
        {
            sendErrorResponse(ErrorCode.GENERIC_ERROR.value(), e.getMessage().getBytes());
            // TODO: log this as well as send the error response
        }
    }

    public void onRemoveTerm(final String destination, final long sessionId, final long channelId, final long termId)
    {

    }

    public void onAddReceiver(final String destination, final long[] channelIdList)
    {
        try
        {
            final UdpDestination rcvDestination = UdpDestination.parse(destination);
            RcvFrameHandler rcv = rcvDestinationMap.get(rcvDestination);

            if (null == rcv)
            {
                rcv = new RcvFrameHandler(rcvDestination, eventLoop, channelIdList);
                rcvDestinationMap.put(rcvDestination, rcv);
            }
            else
            {
                // TODO: add new channels to an existing RcvFrameHandler - need to do this via command queue to that running thread
            }

            // this thread does not add buffers. The RcvFrameHandler handle methods will create new buffers on demand
        }
        catch (Exception e)
        {
            sendErrorResponse(ErrorCode.GENERIC_ERROR.value(), e.getMessage().getBytes());
            // TODO: log this as well as send the error response
        }
    }

    public void onRemoveReceiver(final String destination)
    {

    }
}
