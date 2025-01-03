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
package io.aeron.samples;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;

import static org.agrona.SystemUtil.loadPropertiesFiles;

/**
 * Sample setup for a {@link MediaDriver} that is configured for low latency communications. This configuration
 * requires sufficient CPU resource to delivery low latency performance, i.e. 3 active polling threads.
 */
public class LowLatencyMediaDriver
{
    /**
     * Main method for launching the process.
     *
     * @param args passed to the process which will be used for loading properties files.
     */
    @SuppressWarnings("try")
    public static void main(final String[] args)
    {
        loadPropertiesFiles(args);

        final MediaDriver.Context ctx = new MediaDriver.Context()
            .termBufferSparseFile(false)
            .useWindowsHighResTimer(true)
            .threadingMode(ThreadingMode.DEDICATED)
            .conductorIdleStrategy(BusySpinIdleStrategy.INSTANCE)
            .receiverIdleStrategy(NoOpIdleStrategy.INSTANCE)
            .senderIdleStrategy(NoOpIdleStrategy.INSTANCE);

        try (MediaDriver ignored = MediaDriver.launch(ctx))
        {
            new ShutdownSignalBarrier().await();

            System.out.println("Shutdown Driver...");
        }
    }
}
