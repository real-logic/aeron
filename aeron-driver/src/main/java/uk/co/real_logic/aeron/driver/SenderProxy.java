/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
package uk.co.real_logic.aeron.driver;

import uk.co.real_logic.aeron.driver.cmd.*;
import uk.co.real_logic.aeron.driver.media.SendChannelEndpoint;
import uk.co.real_logic.agrona.concurrent.AtomicCounter;

import java.util.Queue;

import static uk.co.real_logic.aeron.driver.ThreadingMode.SHARED;

/**
 * Proxy for offering into the Sender Thread's command queue.
 */
public class SenderProxy
{
    private final ThreadingMode threadingMode;
    private final Queue<SenderCmd> commandQueue;
    private final AtomicCounter failCount;
    private Sender sender;

    public SenderProxy(final ThreadingMode threadingMode, final Queue<SenderCmd> commandQueue, final AtomicCounter failCount)
    {
        this.threadingMode = threadingMode;
        this.commandQueue = commandQueue;
        this.failCount = failCount;
    }

    public void sender(final Sender sender)
    {
        this.sender = sender;
    }

    public void registerSendChannelEndpoint(final SendChannelEndpoint channelEndpoint)
    {
        if (isSharedThread())
        {
            sender.onRegisterSendChannelEndpoint(channelEndpoint);
        }
        else
        {
            offer(new RegisterSendChannelEndpointCmd(channelEndpoint));
        }
    }

    public void closeSendChannelEndpoint(final SendChannelEndpoint channelEndpoint)
    {
        if (isSharedThread())
        {
            sender.onCloseSendChannelEndpoint(channelEndpoint);
        }
        else
        {
            offer(new CloseSendChannelEndpointCmd(channelEndpoint));
        }
    }

    public void removePublication(final NetworkPublication publication)
    {
        if (isSharedThread())
        {
            sender.onRemovePublication(publication);
        }
        else
        {
            offer(new RemovePublicationCmd(publication));
        }
    }

    public void newPublication(final NetworkPublication publication, final FlowControl flowControl)
    {
        if (isSharedThread())
        {
            sender.onNewPublication(publication, flowControl);
        }
        else
        {
            offer(new NewPublicationCmd(publication, flowControl));
        }
    }

    private boolean isSharedThread()
    {
        return threadingMode == SHARED;
    }

    private void offer(final SenderCmd cmd)
    {
        while (!commandQueue.offer(cmd))
        {
            failCount.orderedIncrement();
            Thread.yield();
        }
    }
}
