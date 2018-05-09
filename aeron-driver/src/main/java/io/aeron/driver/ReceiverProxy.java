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

import io.aeron.driver.cmd.*;
import io.aeron.driver.media.ReceiveChannelEndpoint;
import io.aeron.driver.media.ReceiveDestinationUdpTransport;
import io.aeron.driver.media.UdpChannel;
import org.agrona.concurrent.status.AtomicCounter;

import java.util.Queue;

import static io.aeron.driver.ThreadingMode.INVOKER;
import static io.aeron.driver.ThreadingMode.SHARED;

/**
 * Proxy for offering into the {@link Receiver} Thread's command queue.
 */
public class ReceiverProxy
{
    private final ThreadingMode threadingMode;
    private final Queue<ReceiverCmd> commandQueue;
    private final AtomicCounter failCount;

    private Receiver receiver;

    public ReceiverProxy(
        final ThreadingMode threadingMode, final Queue<ReceiverCmd> commandQueue, final AtomicCounter failCount)
    {
        this.threadingMode = threadingMode;
        this.commandQueue = commandQueue;
        this.failCount = failCount;
    }

    public void receiver(final Receiver receiver)
    {
        this.receiver = receiver;
    }

    public Receiver receiver()
    {
        return receiver;
    }

    public void addSubscription(final ReceiveChannelEndpoint mediaEndpoint, final int streamId)
    {
        if (notConcurrent())
        {
            receiver.onAddSubscription(mediaEndpoint, streamId);
        }
        else
        {
            offer(new AddSubscriptionCmd(mediaEndpoint, streamId));
        }
    }

    public void addSubscription(final ReceiveChannelEndpoint mediaEndpoint, final int streamId, final int sessionId)
    {
        if (notConcurrent())
        {
            receiver.onAddSubscription(mediaEndpoint, streamId, sessionId);
        }
        else
        {
            offer(new AddSubscriptionAndSessionCmd(mediaEndpoint, streamId, sessionId));
        }
    }

    public void removeSubscription(final ReceiveChannelEndpoint mediaEndpoint, final int streamId)
    {
        if (notConcurrent())
        {
            receiver.onRemoveSubscription(mediaEndpoint, streamId);
        }
        else
        {
            offer(new RemoveSubscriptionCmd(mediaEndpoint, streamId));
        }
    }

    public void removeSubscription(final ReceiveChannelEndpoint mediaEndpoint, final int streamId, final int sessionId)
    {
        if (notConcurrent())
        {
            receiver.onRemoveSubscription(mediaEndpoint, streamId, sessionId);
        }
        else
        {
            offer(new RemoveSubscriptionAndSessionCmd(mediaEndpoint, streamId, sessionId));
        }
    }

    public void newPublicationImage(final ReceiveChannelEndpoint channelEndpoint, final PublicationImage image)
    {
        if (notConcurrent())
        {
            receiver.onNewPublicationImage(channelEndpoint, image);
        }
        else
        {
            offer(new NewPublicationImageCmd(channelEndpoint, image));
        }
    }

    public void registerReceiveChannelEndpoint(final ReceiveChannelEndpoint channelEndpoint)
    {
        if (notConcurrent())
        {
            receiver.onRegisterReceiveChannelEndpoint(channelEndpoint);
        }
        else
        {
            offer(new RegisterReceiveChannelEndpointCmd(channelEndpoint));
        }
    }

    public void closeReceiveChannelEndpoint(final ReceiveChannelEndpoint channelEndpoint)
    {
        if (notConcurrent())
        {
            receiver.onCloseReceiveChannelEndpoint(channelEndpoint);
        }
        else
        {
            offer(new CloseReceiveChannelEndpointCmd(channelEndpoint));
        }
    }

    public void removeCoolDown(final ReceiveChannelEndpoint channelEndpoint, final int sessionId, final int streamId)
    {
        if (notConcurrent())
        {
            receiver.onRemoveCoolDown(channelEndpoint, sessionId, streamId);
        }
        else
        {
            offer(new RemoveCoolDownCmd(channelEndpoint, sessionId, streamId));
        }
    }

    public void addDestination(
        final ReceiveChannelEndpoint channelEndpoint, final ReceiveDestinationUdpTransport transport)
    {
        if (notConcurrent())
        {
            receiver.onAddDestination(channelEndpoint, transport);
        }
        else
        {
            offer(new AddRcvDestinationCmd(channelEndpoint, transport));
        }
    }

    public void removeDestination(final ReceiveChannelEndpoint channelEndpoint, final UdpChannel udpChannel)
    {
        if (notConcurrent())
        {
            receiver.onRemoveDestination(channelEndpoint, udpChannel);
        }
        else
        {
            offer(new RemoveRcvDestinationCmd(channelEndpoint, udpChannel));
        }
    }

    private boolean notConcurrent()
    {
        return threadingMode == SHARED || threadingMode == INVOKER;
    }

    private void offer(final ReceiverCmd cmd)
    {
        while (!commandQueue.offer(cmd))
        {
            failCount.incrementOrdered();
            Thread.yield();
        }
    }
}
