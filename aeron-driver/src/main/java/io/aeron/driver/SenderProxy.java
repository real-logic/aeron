/*
 * Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron.driver;

import io.aeron.driver.media.SendChannelEndpoint;
import org.agrona.concurrent.status.AtomicCounter;

import java.net.InetSocketAddress;
import java.util.Queue;

import static io.aeron.driver.ThreadingMode.INVOKER;
import static io.aeron.driver.ThreadingMode.SHARED;

/**
 * Proxy for offering into the Sender Thread's command queue.
 */
public class SenderProxy
{
    private final ThreadingMode threadingMode;
    private final Queue<Runnable> commandQueue;
    private final AtomicCounter failCount;
    private Sender sender;

    public SenderProxy(
        final ThreadingMode threadingMode, final Queue<Runnable> commandQueue, final AtomicCounter failCount)
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
        if (notConcurrent())
        {
            sender.onRegisterSendChannelEndpoint(channelEndpoint);
        }
        else
        {
            offer(() -> sender.onRegisterSendChannelEndpoint(channelEndpoint));
        }
    }

    public void closeSendChannelEndpoint(final SendChannelEndpoint channelEndpoint)
    {
        if (notConcurrent())
        {
            sender.onCloseSendChannelEndpoint(channelEndpoint);
        }
        else
        {
            offer(() -> sender.onCloseSendChannelEndpoint(channelEndpoint));
        }
    }

    public void removeNetworkPublication(final NetworkPublication publication)
    {
        if (notConcurrent())
        {
            sender.onRemoveNetworkPublication(publication);
        }
        else
        {
            offer(() -> sender.onRemoveNetworkPublication(publication));
        }
    }

    public void newNetworkPublication(final NetworkPublication publication)
    {
        if (notConcurrent())
        {
            sender.onNewNetworkPublication(publication);
        }
        else
        {
            offer(() -> sender.onNewNetworkPublication(publication));
        }
    }

    public void addDestination(final SendChannelEndpoint channelEndpoint, final InetSocketAddress address)
    {
        if (notConcurrent())
        {
            sender.onAddDestination(channelEndpoint, address);
        }
        else
        {
            offer(() -> sender.onAddDestination(channelEndpoint, address));
        }
    }

    public void removeDestination(final SendChannelEndpoint channelEndpoint, final InetSocketAddress address)
    {
        if (notConcurrent())
        {
            sender.onRemoveDestination(channelEndpoint, address);
        }
        else
        {
            offer(() -> sender.onRemoveDestination(channelEndpoint, address));
        }
    }

    private boolean notConcurrent()
    {
        return threadingMode == SHARED || threadingMode == INVOKER;
    }

    private void offer(final Runnable cmd)
    {
        while (!commandQueue.offer(cmd))
        {
            failCount.incrementOrdered();
            Thread.yield();
        }
    }
}
