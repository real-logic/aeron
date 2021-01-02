/*
 * Copyright 2014-2021 Real Logic Limited.
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
package io.aeron.test.driver;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ext.DebugChannelEndpointConfiguration;
import io.aeron.driver.ext.DebugReceiveChannelEndpoint;
import io.aeron.driver.ext.LossGenerator;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.status.CountersManager;

public final class JavaTestMediaDriver implements TestMediaDriver
{
    private final MediaDriver mediaDriver;

    private JavaTestMediaDriver(final MediaDriver mediaDriver)
    {
        this.mediaDriver = mediaDriver;
    }

    public void close()
    {
        mediaDriver.close();
    }

    public static JavaTestMediaDriver launch(final MediaDriver.Context context)
    {
        final MediaDriver mediaDriver = MediaDriver.launch(context);
        return new JavaTestMediaDriver(mediaDriver);
    }

    public MediaDriver.Context context()
    {
        return mediaDriver.context();
    }

    public String aeronDirectoryName()
    {
        return mediaDriver.aeronDirectoryName();
    }

    public AgentInvoker sharedAgentInvoker()
    {
        return mediaDriver.sharedAgentInvoker();
    }

    public CountersManager counters()
    {
        return mediaDriver.context().countersManager();
    }

    public static void enableLossGenerationOnReceive(
        final MediaDriver.Context context,
        final double rate,
        final long seed,
        final boolean loseDataMessages,
        final boolean loseControlMessages)
    {
        final LossGenerator dataLossGenerator = loseDataMessages ?
            DebugChannelEndpointConfiguration.lossGeneratorSupplier(rate, seed) :
            DebugChannelEndpointConfiguration.lossGeneratorSupplier(0, 0);

        final LossGenerator controlLossGenerator = loseControlMessages ?
            DebugChannelEndpointConfiguration.lossGeneratorSupplier(rate, seed) :
            DebugChannelEndpointConfiguration.lossGeneratorSupplier(0, 0);

        context.receiveChannelEndpointSupplier((udpChannel, dispatcher, statusIndicator, ctx) ->
            new DebugReceiveChannelEndpoint(
            udpChannel, dispatcher, statusIndicator, ctx, dataLossGenerator, controlLossGenerator));
    }
}
