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
import uk.co.real_logic.aeron.util.HeaderFlyweight;
import uk.co.real_logic.aeron.util.command.ErrorCode;
import uk.co.real_logic.aeron.util.command.LibraryFacade;

import java.util.List;
import java.util.Map;

/**
 * Admin thread to take commands from library and act on them. As well as pass control information to library.
 */
public class AdminThread extends ClosableThread implements LibraryFacade
{
    private final Map<Long, UdpSession> sessionIdMap;
    private final Map<UdpDestination, SrcFrameHandler> srcDestinationMap;
    private final Map<UdpDestination, RcvFrameHandler> rcvDestinationMap;
    private final EventLoop evLoop;

    public AdminThread(final Map<Long, UdpSession> sessionIdMap,
                       final Map<UdpDestination, SrcFrameHandler> srcDestinationMap,
                       final Map<UdpDestination, RcvFrameHandler> rcvDestinationMap,
                       final EventLoop evLoop)
    {
        this.sessionIdMap = sessionIdMap;
        this.srcDestinationMap = srcDestinationMap;
        this.rcvDestinationMap = rcvDestinationMap;
        this.evLoop = evLoop;
    }

    public void work() {
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
            UdpSession session = sessionIdMap.get(sessionId);
            SrcFrameHandler src = srcDestinationMap.get(srcDestination);

            if (null == session)
            {
                // find destination, create if not found
                if (null == src)
                {
                    src = new SrcFrameHandler(srcDestination, evLoop);
                    srcDestinationMap.put(srcDestination, src);
                }

                session = new UdpSession(sessionId, src);
                sessionIdMap.put(sessionId, session);
            }
            else
            {
                // TODO: validate same destination/SrcFrameHandler
            }

            // TODO: add channel to SrcFrameHandler if not already there
        }
        catch (Exception e)
        {
            byte[] message = String.format("malformed URI: %1$s", destination).getBytes();
            sendErrorResponse(ErrorCode.DESTINATION_MALFORMED.value(), message);
            // TODO: log this as well as send the error response
        }
    }

    public void onRemoveSession(final String destination, final long sessionId)
    {
        final UdpSession session = sessionIdMap.get(sessionId);

        if (session == null)
        {
            // TODO: remove session
        }

    }

    public void onRemoveChannel(final String destination, final long sessionId, final long channelId)
    {

    }

    public void onRemoveTerm(final String destination, final long sessionId, final long channelId, final long termId)
    {

    }

    public void onAddReceiver(final String destination, final List<Long> channelIdList)
    {

    }

    public void onRemoveReceiver(final String destination)
    {

    }
}
