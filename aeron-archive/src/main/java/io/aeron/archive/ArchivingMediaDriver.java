/*
 * Copyright 2014-2017 Real Logic Ltd.
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
package io.aeron.archive;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.concurrent.ShutdownSignalBarrier;

import static io.aeron.driver.MediaDriver.loadPropertiesFiles;

/**
 * Archiving media driver which has dedicated threads for high throughput.
 */
public class ArchivingMediaDriver
{
    /**
     * Start an {@link ArchiveConductor} as a stand-alone process, with a {@link MediaDriver}.
     *
     * @param args command line arguments
     * @throws Exception if an error occurs
     */
    @SuppressWarnings("unused")
    public static void main(final String[] args) throws Exception
    {
        loadPropertiesFiles(args);

        launchDriverAndArchiver(ThreadingMode.DEDICATED, ArchiverThreadingMode.DEDICATED);
    }

    static void launchDriverAndArchiver(
        final ThreadingMode mediaDriverThreadingMode,
        final ArchiverThreadingMode archiverThreadingMode) throws Exception
    {
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .threadingMode(mediaDriverThreadingMode)
            .useConcurrentCounterManager(mediaDriverThreadingMode != ThreadingMode.INVOKER);
        final MediaDriver mediaDriver = MediaDriver.launch(driverCtx);

        final Archiver.Context archiverCtx = new Archiver.Context()
            .mediaDriverAgentInvoker(mediaDriver.sharedAgentInvoker())
            .threadingMode(archiverThreadingMode);

        archiverCtx
            .countersManager(driverCtx.countersManager())
            .errorHandler(driverCtx.errorHandler());

        final Archiver archiver = Archiver.launch(archiverCtx);

        new ShutdownSignalBarrier().await();
        System.out.println("Shutdown Archiver...");

        archiver.close();
        mediaDriver.close();
    }
}
