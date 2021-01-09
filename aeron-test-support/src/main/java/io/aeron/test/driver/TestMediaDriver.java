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
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.status.CountersReader;

import static org.agrona.Strings.isEmpty;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

public interface TestMediaDriver extends AutoCloseable
{
    String AERONMD_PATH_PROP_NAME = "aeron.test.system.aeronmd.path";
    String DRIVER_AGENT_PATH_PROP_NAME = "aeron.test.system.driver.agent.path";

    static boolean shouldRunCMediaDriver()
    {
        return !isEmpty(System.getProperty(AERONMD_PATH_PROP_NAME));
    }

    static void notSupportedOnCMediaDriver(final String reason)
    {
        assumeFalse(shouldRunCMediaDriver(), () -> "not support by C Media Driver: " + reason);
    }

    static TestMediaDriver launch(final MediaDriver.Context context, final DriverOutputConsumer driverOutputConsumer)
    {
        return shouldRunCMediaDriver() ?
            CTestMediaDriver.launch(context, driverOutputConsumer) : JavaTestMediaDriver.launch(context);
    }

    static void enableLossGenerationOnReceive(
        final MediaDriver.Context context,
        final double rate,
        final long seed,
        final boolean loseDataMessages,
        final boolean loseControlMessages)
    {
        if (shouldRunCMediaDriver())
        {
            CTestMediaDriver.enableLossGenerationOnReceive(context, rate, seed, loseDataMessages, loseControlMessages);
        }
        else
        {
            JavaTestMediaDriver.enableLossGenerationOnReceive(
                context, rate, seed, loseDataMessages, loseControlMessages);
        }
    }

    static void enableCsvNameLookupConfiguration(final MediaDriver.Context context, final String csvLookupTable)
    {
        if (shouldRunCMediaDriver())
        {
            CTestMediaDriver.enableCsvNameLookupConfiguration(context, csvLookupTable);
        }
        else
        {
            context.nameResolver(new StubCsvNameResolver(csvLookupTable));
        }
    }

    MediaDriver.Context context();

    String aeronDirectoryName();

    void close();

    AgentInvoker sharedAgentInvoker();

    CountersReader counters();
}
