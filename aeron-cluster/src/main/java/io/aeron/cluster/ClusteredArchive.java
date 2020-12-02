/*
 * Copyright 2014-2020 Real Logic Limited.
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
package io.aeron.cluster;

import io.aeron.archive.Archive;
import io.aeron.driver.MediaDriver;
import org.agrona.CloseHelper;

import static org.agrona.SystemUtil.loadPropertiesFiles;

/**
 * Clustered media driver which is an aggregate of a {@link MediaDriver}, {@link Archive},
 * and a {@link ConsensusModule}.
 */
public class ClusteredArchive implements AutoCloseable
{
    private final Archive archive;
    private final ConsensusModule consensusModule;

    ClusteredArchive(final Archive archive, final ConsensusModule consensusModule)
    {
        this.archive = archive;
        this.consensusModule = consensusModule;
    }

    /**
     * Launch the clustered media driver aggregate and await a shutdown signal.
     *
     * @param args command line argument which is a list for properties files as URLs or filenames.
     */
    public static void main(final String[] args)
    {
        loadPropertiesFiles(args);

        try (ClusteredArchive driver = launch())
        {
            driver.consensusModule().context().shutdownSignalBarrier().await();
            System.out.println("Shutdown ClusteredMediaDriver...");
        }
    }

    /**
     * Launch a new {@link ClusteredArchive} with default contexts.
     *
     * @return a new {@link ClusteredArchive} with default contexts.
     */
    public static ClusteredArchive launch()
    {
        return launch(
            new MediaDriver.Context().aeronDirectoryName(), new Archive.Context(), new ConsensusModule.Context());
    }

    /**
     * Launch a new {@link ClusteredArchive} with provided contexts.
     *
     * @param aeronDirectoryName for connecting to the {@link MediaDriver}.
     * @param archiveCtx         for configuring the {@link Archive}.
     * @param consensusModuleCtx for the configuration of the {@link ConsensusModule}.
     * @return a new {@link ClusteredArchive} with the provided contexts.
     */
    public static ClusteredArchive launch(
        final String aeronDirectoryName,
        final Archive.Context archiveCtx,
        final ConsensusModule.Context consensusModuleCtx)
    {
        Archive archive = null;
        ConsensusModule consensusModule = null;

        try
        {
//            final AtomicCounter errorCounter = Objects.requireNonNull(
//                archiveCtx.errorCounter(), "You must supply an errorCounter for the archive");
//            final ErrorHandler errorHandler = Objects.requireNonNull(
//                archiveCtx.errorHandler(), "You must supply an errorHandler for the archive");

            archive = Archive.launch(archiveCtx
                .aeronDirectoryName(aeronDirectoryName));
//                .errorHandler(errorHandler)
//                .errorCounter(errorCounter));

            consensusModule = ConsensusModule.launch(consensusModuleCtx
                .aeronDirectoryName(aeronDirectoryName));

            return new ClusteredArchive(archive, consensusModule);
        }
        catch (final Exception ex)
        {
            CloseHelper.quietCloseAll(consensusModule, archive);
            throw ex;
        }
    }

    /**
     * Get the {@link Archive} used in the aggregate.
     *
     * @return the {@link Archive} used in the aggregate.
     */
    public Archive archive()
    {
        return archive;
    }

    /**
     * Get the {@link ConsensusModule} used in the aggregate.
     *
     * @return the {@link ConsensusModule} used in the aggregate.
     */
    public ConsensusModule consensusModule()
    {
        return consensusModule;
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        CloseHelper.closeAll(consensusModule, archive);
    }
}
