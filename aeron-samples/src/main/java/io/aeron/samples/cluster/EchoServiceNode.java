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
package io.aeron.samples.cluster;

import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.samples.cluster.tutorial.BasicAuctionClusteredService;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.ShutdownSignalBarrier;

import java.util.Arrays;
import java.util.List;

import static java.lang.Integer.parseInt;

/**
 * Node that launches the service for the {@link BasicAuctionClusteredService}.
 */
public final class EchoServiceNode
{
    private static ErrorHandler errorHandler(final String context)
    {
        return
            (Throwable throwable) ->
            {
                System.err.println(context);
                throwable.printStackTrace(System.err);
            };
    }

    private static final int PORT_BASE = 9000;

    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     */
    @SuppressWarnings("try")
    public static void main(final String[] args)
    {
        final int nodeId = parseInt(System.getProperty("aeron.cluster.tutorial.nodeId"));
        final String hostnamesStr = System.getProperty(
            "aeron.cluster.tutorial.hostnames", "localhost,localhost,localhost");
        final String internalHostnamesStr = System.getProperty(
            "aeron.cluster.tutorial.hostnames.internal", hostnamesStr);
        final List<String> hostnames = Arrays.asList(hostnamesStr.split(","));
        final List<String> internalHostnames = Arrays.asList(internalHostnamesStr.split(","));

        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();

        final ClusterConfig clusterConfig = ClusterConfig.create(
            nodeId, hostnames, internalHostnames, PORT_BASE, new EchoService());

        clusterConfig.mediaDriverContext().errorHandler(EchoServiceNode.errorHandler("Media Driver"));
        clusterConfig.archiveContext()
            .errorHandler(EchoServiceNode.errorHandler("Archive"));
        clusterConfig.aeronArchiveContext()
            .errorHandler(EchoServiceNode.errorHandler("Aeron Archive"));
        clusterConfig.consensusModuleContext()
            .errorHandler(errorHandler("Consensus Module"));
        clusterConfig.clusteredServiceContext()
            .errorHandler(errorHandler("Clustered Service"));

        try (
            ClusteredMediaDriver ignore = ClusteredMediaDriver.launch(
                clusterConfig.mediaDriverContext(),
                clusterConfig.archiveContext(),
                clusterConfig.consensusModuleContext());
            ClusteredServiceContainer ignore2 = ClusteredServiceContainer.launch(
                clusterConfig.clusteredServiceContext()))
        {
            System.out.println("[" + nodeId + "] Started Cluster Node on " + hostnames.get(nodeId) + "...");
            barrier.await();
            System.out.println("[" + nodeId + "] Exiting");
        }
    }
}
