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
import uk.co.real_logic.aeron.util.HeaderFlyweight;
import uk.co.real_logic.aeron.util.command.MediaDriverFacade;

import java.util.List;

/**
 * Admin thread to take responses and notifications from mediadriver and act on them. As well as pass commands to the mediadriver.
 */
public final class AdminThread extends ClosableThread implements MediaDriverFacade
{
    // TODO: add correct types once comms buffers are committed
    private final Object recvBuffer;
    private final Object sendBuffer;

    public AdminThread()
    {
        recvBuffer = null;
        sendBuffer = null;
    }

    public void work()
    {
        // read from recvBuffer and delegate to event handlers
    }

    /* commands to MediaDriver */

    @Override
    public void sendAddChannel(String destination, long sessionId, long channelId)
    {

    }

    @Override
    public void sendRemoveChannel(String destination, long sessionId, long channelId)
    {

    }

    @Override
    public void sendRemoveTerm(String destination, long sessionId, long channelId, long termId)
    {

    }

    @Override
    public void sendAddReceiver(String destination, long[] channelIdList)
    {

    }

    @Override
    public void sendRemoveReceiver(String destination)
    {

    }

    /* callbacks from MediaDriver */

    @Override
    public void onFlowControlResponse(HeaderFlyweight header)
    {

    }

    @Override
    public void onErrorResponse(int code, byte[] message)
    {

    }

    @Override
    public void onError(int code, byte[] message)
    {

    }

    @Override
    public void onLocationResponse(List<byte[]> filenames)
    {

    }

    @Override
    public void onNewSession(long sessionId, List<byte[]> filenames)
    {

    }
}
