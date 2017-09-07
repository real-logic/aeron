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
package io.aeron.cluster;

import io.aeron.driver.MediaDriver;
import org.agrona.concurrent.ShutdownSignalBarrier;

import static io.aeron.driver.MediaDriver.loadPropertiesFiles;

public class ClusterMediaDriver implements AutoCloseable
{
    public void close()
    {
    }

    /**
     * Launch a {@link Cluster} with an embedded {@link MediaDriver} and await a shutdown signal.
     *
     * @param args command line argument which is a list for properties files as URLs or filenames.
     */
    public static void main(final String[] args)
    {
        loadPropertiesFiles(args);

        try (ClusterMediaDriver ignore = launch())
        {
            new ShutdownSignalBarrier().await();

            System.out.println("Shutdown ClusterMediaDriver...");
        }
    }

    /**
     * Launch a new {@link ClusterMediaDriver} with default contexts.
     *
     * @return a new {@link ClusterMediaDriver} with default contexts.
     */
    public static ClusterMediaDriver launch()
    {
        return new ClusterMediaDriver();
    }
}
