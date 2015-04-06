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
package uk.co.real_logic.aeron.driver.cmd;

import uk.co.real_logic.aeron.driver.DriverConductor;
import uk.co.real_logic.aeron.driver.ReceiveChannelEndpoint;

import java.net.InetSocketAddress;

public class ElicitSetupMessageFromSourceCmd implements DriverConductorCmd
{
    private final int sessionId;
    private final int streamId;
    private final InetSocketAddress controlAddress;
    private final ReceiveChannelEndpoint channelEndpoint;

    private long timeOfStatusMessage;

    public ElicitSetupMessageFromSourceCmd(
        final int sessionId,
        final int streamId,
        final InetSocketAddress controlAddress,
        final ReceiveChannelEndpoint channelEndpoint)
    {
        this.sessionId = sessionId;
        this.streamId = streamId;
        this.controlAddress = controlAddress;
        this.channelEndpoint = channelEndpoint;
    }

    public void execute(final DriverConductor conductor)
    {
        conductor.onElicitSetupMessageFromSender(this);
    }

    public int sessionId()
    {
        return sessionId;
    }

    public int streamId()
    {
        return streamId;
    }

    public InetSocketAddress controlAddress()
    {
        return controlAddress;
    }

    public ReceiveChannelEndpoint channelEndpoint()
    {
        return channelEndpoint;
    }

    public long timeOfStatusMessage()
    {
        return timeOfStatusMessage;
    }

    public void timeOfStatusMessage(final long now)
    {
        timeOfStatusMessage = now;
    }
}
