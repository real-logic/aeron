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
package io.aeron.samples.cluster;

import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MinMulticastFlowControlSupplier;
import io.aeron.driver.ThreadingMode;
import io.aeron.samples.cluster.tutorial.BasicAuctionClusteredService;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.ShutdownSignalBarrier;

import java.io.File;

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
    private static final int PORTS_PER_NODE = 100;
    private static final int ARCHIVE_CONTROL_REQUEST_PORT_OFFSET = 1;
    private static final int CLIENT_FACING_PORT_OFFSET = 2;
    private static final int MEMBER_FACING_PORT_OFFSET = 3;
    private static final int LOG_PORT_OFFSET = 4;
    private static final int TRANSFER_PORT_OFFSET = 5;
    private static final int TERM_LENGTH = 64 * 1024;

    static int calculatePort(final int nodeId, final int offset)
    {
        return PORT_BASE + (nodeId * PORTS_PER_NODE) + offset;
    }

    private static String udpChannel(final int nodeId, final String hostname)
    {
        final int port = calculatePort(nodeId, ARCHIVE_CONTROL_REQUEST_PORT_OFFSET);
        return new ChannelUriStringBuilder()
            .media(CommonContext.UDP_MEDIA)
            .termLength(TERM_LENGTH)
            .endpoint(hostname + ":" + port)
            .build();
    }

    /**
     * String representing the cluster members configuration which can be used for
     * {@link io.aeron.cluster.ClusterMember#parse(String)}.
     *
     * @param hostnames of the cluster members.
     * @param internalHostnames of the cluster members internal address (can be the same as 'hostnames'}).
     * @return the String which can be used for {@link io.aeron.cluster.ClusterMember#parse(String)}.
     */
    public static String clusterMembers(final String[] hostnames, final String[] internalHostnames)
    {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < hostnames.length; i++)
        {
            sb.append(i);
            sb.append(',').append(hostnames[i]).append(':').append(calculatePort(i, CLIENT_FACING_PORT_OFFSET));
            sb.append(',').append(internalHostnames[i]).append(':').append(calculatePort(i, MEMBER_FACING_PORT_OFFSET));
            sb.append(',').append(internalHostnames[i]).append(':').append(calculatePort(i, LOG_PORT_OFFSET));
            sb.append(',').append(internalHostnames[i]).append(':').append(calculatePort(i, TRANSFER_PORT_OFFSET));
            sb.append(',').append(internalHostnames[i]).append(':')
                .append(calculatePort(i, ARCHIVE_CONTROL_REQUEST_PORT_OFFSET));
            sb.append('|');
        }

        return sb.toString();
    }

    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     */
    public static void main(final String[] args)
    {
        final int nodeId = parseInt(System.getProperty("aeron.cluster.tutorial.nodeId"));
        final String hostnamesStr = System.getProperty(
            "aeron.cluster.tutorial.hostnames", "localhost,localhost,localhost");
        final String internalHostnamesStr = System.getProperty(
            "aeron.cluster.tutorial.hostnames.internal", hostnamesStr);
        final String[] hostnames = hostnamesStr.split(",");
        final String[] internalHostnames = internalHostnamesStr.split(",");
        final String hostname = hostnames[nodeId];

        final File baseDir = new File(System.getProperty("user.dir"), "node" + nodeId);
        final String aeronDirName = CommonContext.getAeronDirectoryName() + "-" + nodeId + "-driver";

        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();

        final MediaDriver.Context mediaDriverContext = new MediaDriver.Context()
            .aeronDirectoryName(aeronDirName)
            .threadingMode(ThreadingMode.SHARED)
            .termBufferSparseFile(true)
            .multicastFlowControlSupplier(new MinMulticastFlowControlSupplier())
            .terminationHook(barrier::signal)
            .errorHandler(EchoServiceNode.errorHandler("Media Driver"));

        final Archive.Context archiveContext = new Archive.Context()
            .aeronDirectoryName(aeronDirName)
            .archiveDir(new File(baseDir, "archive"))
            .controlChannel(udpChannel(nodeId, hostname))
            .localControlChannel("aeron:ipc?term-length=64k")
            .recordingEventsEnabled(false)
            .threadingMode(ArchiveThreadingMode.SHARED);

        final AeronArchive.Context aeronArchiveContext = new AeronArchive.Context()
            .lock(NoOpLock.INSTANCE)
            .controlRequestChannel(archiveContext.localControlChannel())
            .controlRequestStreamId(archiveContext.localControlStreamId())
            .controlResponseChannel(archiveContext.localControlChannel())
            .aeronDirectoryName(aeronDirName);

        final ConsensusModule.Context consensusModuleContext = new ConsensusModule.Context()
            .errorHandler(errorHandler("Consensus Module"))
            .clusterMemberId(nodeId)
            .clusterMembers(clusterMembers(hostnames, internalHostnames))
            .clusterDir(new File(baseDir, "consensus-module"))
            .archiveContext(aeronArchiveContext.clone());

        final ClusteredServiceContainer.Context clusteredServiceContext =
            new ClusteredServiceContainer.Context()
            .aeronDirectoryName(aeronDirName)
            .archiveContext(aeronArchiveContext.clone())
            .clusterDir(new File(baseDir, "service"))
            .clusteredService(new EchoService())
            .errorHandler(errorHandler("Clustered Service"));

        try (
            ClusteredMediaDriver clusteredMediaDriver = ClusteredMediaDriver.launch(
                mediaDriverContext, archiveContext, consensusModuleContext);
            ClusteredServiceContainer container = ClusteredServiceContainer.launch(
                clusteredServiceContext))
        {
            System.out.println("[" + nodeId + "] Started Cluster Node on " + hostname + "...");
            barrier.await();
            System.out.println("[" + nodeId + "] Exiting");
        }
    }
}
