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
package io.aeron.samples.cluster.tutorial;

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
import org.agrona.ErrorHandler;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.ShutdownSignalBarrier;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static java.lang.Integer.parseInt;

/**
 * Node that launches the service for the {@link BasicAuctionClusteredService}.
 */
// tag::new_service[]
public class BasicAuctionClusteredServiceNode
// end::new_service[]
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

    // tag::ports[]
    private static final int PORT_BASE = 9000;
    private static final int PORTS_PER_NODE = 100;
    private static final int ARCHIVE_CONTROL_PORT_OFFSET = 1;
    static final int CLIENT_FACING_PORT_OFFSET = 2;
    private static final int MEMBER_FACING_PORT_OFFSET = 3;
    private static final int LOG_PORT_OFFSET = 4;
    private static final int TRANSFER_PORT_OFFSET = 5;
    private static final int LOG_CONTROL_PORT_OFFSET = 6;
    private static final int TERM_LENGTH = 64 * 1024;

    static int calculatePort(final int nodeId, final int offset)
    {
        return PORT_BASE + (nodeId * PORTS_PER_NODE) + offset;
    }
    // end::ports[]

    // tag::udp_channel[]
    private static String udpChannel(final int nodeId, final String hostname, final int portOffset)
    {
        final int port = calculatePort(nodeId, portOffset);
        return new ChannelUriStringBuilder()
            .media("udp")
            .termLength(TERM_LENGTH)
            .endpoint(hostname + ":" + port)
            .build();
    }
    // end::udp_channel[]

    private static String logControlChannel(final int nodeId, final String hostname, final int portOffset)
    {
        final int port = calculatePort(nodeId, portOffset);
        return new ChannelUriStringBuilder()
            .media("udp")
            .termLength(TERM_LENGTH)
            .controlMode(CommonContext.MDC_CONTROL_MODE_MANUAL)
            .controlEndpoint(hostname + ":" + port)
            .build();
    }

    private static String logReplicationChannel(final String hostname)
    {
        return new ChannelUriStringBuilder()
            .media("udp")
            .endpoint(hostname + ":0")
            .build();
    }

    private static String clusterMembers(final List<String> hostnames)
    {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < hostnames.size(); i++)
        {
            sb.append(i);
            sb.append(',').append(hostnames.get(i)).append(':').append(calculatePort(i, CLIENT_FACING_PORT_OFFSET));
            sb.append(',').append(hostnames.get(i)).append(':').append(calculatePort(i, MEMBER_FACING_PORT_OFFSET));
            sb.append(',').append(hostnames.get(i)).append(':').append(calculatePort(i, LOG_PORT_OFFSET));
            sb.append(',').append(hostnames.get(i)).append(':').append(calculatePort(i, TRANSFER_PORT_OFFSET));
            sb.append(',').append(hostnames.get(i)).append(':')
                .append(calculatePort(i, ARCHIVE_CONTROL_PORT_OFFSET));
            sb.append('|');
        }

        return sb.toString();
    }

    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     */
    @SuppressWarnings("try")
    // tag::main[]
    public static void main(final String[] args)
    {
        final int nodeId = parseInt(System.getProperty("aeron.cluster.tutorial.nodeId"));               // <1>
        final String[] hostnames = System.getProperty(
            "aeron.cluster.tutorial.hostnames", "localhost,localhost,localhost").split(",");            // <2>
        final String hostname = hostnames[nodeId];

        final File baseDir = new File(System.getProperty("user.dir"), "node" + nodeId);                 // <3>
        final String aeronDirName = CommonContext.getAeronDirectoryName() + "-" + nodeId + "-driver";

        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();                              // <4>
        // end::main[]

        // tag::media_driver[]
        final MediaDriver.Context mediaDriverContext = new MediaDriver.Context()
            .aeronDirectoryName(aeronDirName)
            .threadingMode(ThreadingMode.SHARED)
            .termBufferSparseFile(true)
            .multicastFlowControlSupplier(new MinMulticastFlowControlSupplier())
            .terminationHook(barrier::signal)
            .errorHandler(BasicAuctionClusteredServiceNode.errorHandler("Media Driver"));
        // end::media_driver[]

        final AeronArchive.Context replicationArchiveContext = new AeronArchive.Context()
            .controlResponseChannel("aeron:udp?endpoint=" + hostname + ":0");

        // tag::archive[]
        final Archive.Context archiveContext = new Archive.Context()
            .aeronDirectoryName(aeronDirName)
            .archiveDir(new File(baseDir, "archive"))
            .controlChannel(udpChannel(nodeId, hostname, ARCHIVE_CONTROL_PORT_OFFSET))
            .archiveClientContext(replicationArchiveContext)
            .localControlChannel("aeron:ipc?term-length=64k")
            .recordingEventsEnabled(false)
            .threadingMode(ArchiveThreadingMode.SHARED)
            .replicationChannel("aeron:udp?endpoint=" + hostname + ":0");
        // end::archive[]

        // tag::archive_client[]
        final AeronArchive.Context aeronArchiveContext = new AeronArchive.Context()
            .lock(NoOpLock.INSTANCE)
            .controlRequestChannel(archiveContext.localControlChannel())
            .controlResponseChannel(archiveContext.localControlChannel())
            .aeronDirectoryName(aeronDirName);
        // end::archive_client[]

        // tag::consensus_module[]
        final ConsensusModule.Context consensusModuleContext = new ConsensusModule.Context()
            .errorHandler(errorHandler("Consensus Module"))
            .clusterMemberId(nodeId)                                                                     // <1>
            .clusterMembers(clusterMembers(Arrays.asList(hostnames)))                                    // <2>
            .clusterDir(new File(baseDir, "cluster"))                                                    // <3>
            .ingressChannel("aeron:udp?term-length=64k")                                                 // <4>
            .replicationChannel(logReplicationChannel(hostname))                                         // <5>
            .archiveContext(aeronArchiveContext.clone());                                                // <6>
        // end::consensus_module[]

        // tag::clustered_service[]
        final ClusteredServiceContainer.Context clusteredServiceContext =
            new ClusteredServiceContainer.Context()
            .aeronDirectoryName(aeronDirName)                                                            // <1>
            .archiveContext(aeronArchiveContext.clone())                                                 // <2>
            .clusterDir(new File(baseDir, "cluster"))
            .clusteredService(new BasicAuctionClusteredService())                                        // <3>
            .errorHandler(errorHandler("Clustered Service"));
        // end::clustered_service[]

        // tag::running[]
        try (
            ClusteredMediaDriver clusteredMediaDriver = ClusteredMediaDriver.launch(
                mediaDriverContext, archiveContext, consensusModuleContext);                             // <1>
            ClusteredServiceContainer container = ClusteredServiceContainer.launch(
                clusteredServiceContext))                                                                // <2>
        {
            System.out.println("[" + nodeId + "] Started Cluster Node on " + hostname + "...");
            barrier.await();                                                                             // <3>
            System.out.println("[" + nodeId + "] Exiting");
        }
        // end::running[]
    }
}
