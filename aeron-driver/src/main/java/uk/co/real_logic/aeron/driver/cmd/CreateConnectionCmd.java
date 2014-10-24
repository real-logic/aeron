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
package uk.co.real_logic.aeron.driver.cmd;

import uk.co.real_logic.aeron.driver.DriverConductor;
import uk.co.real_logic.aeron.driver.ReceiveChannelEndpoint;

import java.net.InetSocketAddress;

public class CreateConnectionCmd implements DriverConductorCmd
{
    private final int sessionId;
    private final int streamId;
    private final int termId;
    private final int termOffset;
    private final int termSize;
    private final int senderMtuLength;
    private final InetSocketAddress controlAddress;
    private final InetSocketAddress srcAddress;
    private final ReceiveChannelEndpoint channelEndpoint;

    public CreateConnectionCmd(
        final int sessionId,
        final int streamId,
        final int termId,
        final int termOffet,
        final int termSize,
        final int senderMtuLength,
        final InetSocketAddress controlAddress,
        final InetSocketAddress srcAddress,
        final ReceiveChannelEndpoint channelEndpoint)
    {
        this.sessionId = sessionId;
        this.streamId = streamId;
        this.termId = termId;
        this.termOffset = termOffet;
        this.termSize = termSize;
        this.senderMtuLength = senderMtuLength;
        this.controlAddress = controlAddress;
        this.srcAddress = srcAddress;
        this.channelEndpoint = channelEndpoint;
    }

    public void execute(final DriverConductor conductor)
    {
        conductor.onCreateConnection(
            sessionId,
            streamId,
            termId,
            termOffset,
            termSize,
            senderMtuLength,
            controlAddress,
            srcAddress,
            channelEndpoint);
    }

    public ReceiveChannelEndpoint channelEndpoint()
    {
        return channelEndpoint;
    }

    public int streamId()
    {
        return streamId;
    }

    public int sessionId()
    {
        return sessionId;
    }

    public int termId()
    {
        return termId;
    }
}
