/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.driver;

import io.aeron.ChannelUri;
import io.aeron.driver.media.SendChannelEndpoint;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.agrona.concurrent.status.AtomicCounter;

import java.net.InetSocketAddress;

/**
 * Proxy for offering into the Sender Thread's command queue.
 */
final class SenderProxy extends CommandProxy
{
    private Sender sender;

    SenderProxy(
        final ThreadingMode threadingMode,
        final OneToOneConcurrentArrayQueue<Runnable> commandQueue,
        final AtomicCounter failCount)
    {
        super(threadingMode, commandQueue, failCount);
    }

    void sender(final Sender sender)
    {
        this.sender = sender;
    }

    void registerSendChannelEndpoint(final SendChannelEndpoint channelEndpoint)
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

    void closeSendChannelEndpoint(final SendChannelEndpoint channelEndpoint)
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

    void removeNetworkPublication(final NetworkPublication publication)
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

    void newNetworkPublication(final NetworkPublication publication)
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

    void addDestination(
        final SendChannelEndpoint channelEndpoint,
        final ChannelUri channelUri,
        final InetSocketAddress address,
        final long registrationId)
    {
        if (notConcurrent())
        {
            sender.onAddDestination(channelEndpoint, channelUri, address, registrationId);
        }
        else
        {
            offer(() -> sender.onAddDestination(channelEndpoint, channelUri, address, registrationId));
        }
    }

    void removeDestination(
        final SendChannelEndpoint channelEndpoint, final ChannelUri channelUri, final InetSocketAddress address)
    {
        if (notConcurrent())
        {
            sender.onRemoveDestination(channelEndpoint, channelUri, address);
        }
        else
        {
            offer(() -> sender.onRemoveDestination(channelEndpoint, channelUri, address));
        }
    }

    void removeDestination(
        final SendChannelEndpoint channelEndpoint, final long destinationRegistrationId)
    {
        if (notConcurrent())
        {
            sender.onRemoveDestination(channelEndpoint, destinationRegistrationId);
        }
        else
        {
            offer(() -> sender.onRemoveDestination(channelEndpoint, destinationRegistrationId));
        }
    }

    void onResolutionChange(
        final SendChannelEndpoint channelEndpoint, final String endpoint, final InetSocketAddress newAddress)
    {
        if (notConcurrent())
        {
            sender.onResolutionChange(channelEndpoint, endpoint, newAddress);
        }
        else
        {
            offer(() -> sender.onResolutionChange(channelEndpoint, endpoint, newAddress));
        }
    }
}
