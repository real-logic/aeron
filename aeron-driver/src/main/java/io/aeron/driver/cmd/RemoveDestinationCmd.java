/*
 * Copyright 2017 Real Logic Ltd.
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
package io.aeron.driver.cmd;

import io.aeron.driver.Sender;
import io.aeron.driver.media.SendChannelEndpoint;

import java.net.InetSocketAddress;

public class RemoveDestinationCmd implements SenderCmd
{
    private final SendChannelEndpoint channelEndpoint;
    private final InetSocketAddress address;

    public RemoveDestinationCmd(final SendChannelEndpoint channelEndpoint, final InetSocketAddress address)
    {
        this.channelEndpoint = channelEndpoint;
        this.address = address;
    }

    public void execute(final Sender sender)
    {
        sender.onRemoveDestination(channelEndpoint, address);
    }
}
