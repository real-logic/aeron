/*
 * Copyright 2016 Real Logic Ltd.
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

import uk.co.real_logic.aeron.driver.media.SendChannelEndpoint;
import uk.co.real_logic.aeron.driver.media.UdpChannel;

/**
 * Supply a debug implementation of a {@link SendChannelEndpoint}.
 */
public class DebugSendChannelEndpointSupplier implements SendChannelEndpointSupplier
{
    public SendChannelEndpoint newInstance(final UdpChannel udpChannel, final MediaDriver.Context context)
    {
        return new DebugSendChannelEndpoint(udpChannel, context);
    }
}
