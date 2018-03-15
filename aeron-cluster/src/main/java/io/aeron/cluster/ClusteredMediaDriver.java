/*
 * Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron.cluster;

import io.aeron.driver.MediaDriver;
import io.aeron.archive.Archive;
import io.aeron.driver.status.SystemCounterDescriptor;
import org.agrona.CloseHelper;

import static org.agrona.SystemUtil.loadPropertiesFiles;

/**
 * Clustered media driver which is an aggregate of a {@link MediaDriver}, {@link Archive},
 * and a {@link ConsensusModule}.
 */
public class ClusteredMediaDriver implements AutoCloseable
{
    private final MediaDriver driver;
    private final Archive archive;
    private final ConsensusModule consensusModule;

    ClusteredMediaDriver(final MediaDriver driver, final Archive archive, final ConsensusModule consensusModule)
    {
        this.driver = driver;
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

        try (ClusteredMediaDriver driver = launch())
        {
            driver.consensusModule().context().shutdownSignalBarrier().await();

            System.out.println("Shutdown ClusteredMediaDriver...");
        }
    }

    /**
     * Launch a new {@link ClusteredMediaDriver} with default contexts.
     *
     * @return a new {@link ClusteredMediaDriver} with default contexts.
     */
    public static ClusteredMediaDriver launch()
    {
        return launch(new MediaDriver.Context(), new Archive.Context(), new ConsensusModule.Context());
    }

    /**
     * Launch a new {@link ClusteredMediaDriver} with provided contexts.
     *
     * @param driverCtx          for configuring the {@link MediaDriver}.
     * @param archiveCtx         for configuring the {@link Archive}.
     * @param consensusModuleCtx for the configuration of the {@link ConsensusModule}.
     * @return a new {@link ClusteredMediaDriver} with the provided contexts.
     */
    public static ClusteredMediaDriver launch(
        final MediaDriver.Context driverCtx,
        final Archive.Context archiveCtx,
        final ConsensusModule.Context consensusModuleCtx)
    {
        final MediaDriver driver = MediaDriver.launch(driverCtx
            .spiesSimulateConnection(true));

        final Archive archive = Archive.launch(archiveCtx
            .mediaDriverAgentInvoker(driver.sharedAgentInvoker())
            .errorHandler(driverCtx.errorHandler())
            .errorCounter(driverCtx.systemCounters().get(SystemCounterDescriptor.ERRORS)));

        final ConsensusModule consensusModule = ConsensusModule.launch(consensusModuleCtx);

        return new ClusteredMediaDriver(driver, archive, consensusModule);
    }

    /**
     * Get the {@link MediaDriver} used in the aggregate.
     *
     * @return the {@link MediaDriver} used in the aggregate.
     */
    public MediaDriver mediaDriver()
    {
        return driver;
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

    public void close()
    {
        CloseHelper.close(consensusModule);
        CloseHelper.close(archive);
        CloseHelper.close(driver);
    }
}
