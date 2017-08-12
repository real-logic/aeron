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
     * @param args command line argument which is a list for properties files as URLs or filenames.
     */
    public static void main(final String[] args)
    {
        loadPropertiesFiles(args);

        launch(ThreadingMode.DEDICATED, ArchiveThreadingMode.DEDICATED);
    }

    /**
     * Launch an {@link Archive} with embedded {@link MediaDriver} and await a shutdown signal.
     *
     * @param mediaDriverThreadingMode for the {@link MediaDriver}
     * @param archiveThreadingMode     for the {@link Archive}
     */
    public static void launch(
        final ThreadingMode mediaDriverThreadingMode,
        final ArchiveThreadingMode archiveThreadingMode)
    {
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .threadingMode(mediaDriverThreadingMode)
            .useConcurrentCounterManager(mediaDriverThreadingMode != ThreadingMode.INVOKER);
        final MediaDriver mediaDriver = MediaDriver.launch(driverCtx);

        final Archive.Context archiveCtx = new Archive.Context()
            .mediaDriverAgentInvoker(mediaDriver.sharedAgentInvoker())
            .threadingMode(archiveThreadingMode);

        archiveCtx
            .countersManager(driverCtx.countersManager())
            .errorHandler(driverCtx.errorHandler());

        final Archive archive = Archive.launch(archiveCtx);

        new ShutdownSignalBarrier().await();
        System.out.println("Shutdown Archive...");

        archive.close();
        mediaDriver.close();
    }
}
