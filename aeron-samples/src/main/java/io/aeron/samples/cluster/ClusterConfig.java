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

import io.aeron.CommonContext;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MinMulticastFlowControlSupplier;
import io.aeron.driver.ThreadingMode;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.NoOpLock;

import java.io.File;
import java.util.List;

/**
 * Wrapper class to simplify cluster configuration. This is sample code and is intended to show a mechanism for
 * managing the configuration of the components required for Aeron Cluster. This code may change between versions
 * and API compatibility is not guaranteed.
 */
public final class ClusterConfig
{
    public static final int PORTS_PER_NODE = 100;
    public static final int ARCHIVE_CONTROL_PORT_OFFSET = 1;
    public static final int CLIENT_FACING_PORT_OFFSET = 2;
    public static final int MEMBER_FACING_PORT_OFFSET = 3;
    public static final int LOG_PORT_OFFSET = 4;
    public static final int TRANSFER_PORT_OFFSET = 5;
    public static final String ARCHIVE_SUB_DIR = "archive";
    public static final String CLUSTER_SUB_DIR = "cluster";

    private final MediaDriver.Context mediaDriverContext;
    private final Archive.Context archiveContext;
    private final AeronArchive.Context aeronArchiveContext;
    private final ConsensusModule.Context consensusModuleContext;
    private final ClusteredServiceContainer.Context clusteredServiceContext;

    ClusterConfig(
        final MediaDriver.Context mediaDriverContext,
        final Archive.Context archiveContext,
        final AeronArchive.Context aeronArchiveContext,
        final ConsensusModule.Context consensusModuleContext,
        final ClusteredServiceContainer.Context clusteredServiceContext)
    {
        this.mediaDriverContext = mediaDriverContext;
        this.archiveContext = archiveContext;
        this.aeronArchiveContext = aeronArchiveContext;
        this.consensusModuleContext = consensusModuleContext;
        this.clusteredServiceContext = clusteredServiceContext;
    }

    /**
     * Create a new ClusterConfig. This call allows for 2 separate lists of hostnames, so that there can be 'external'
     * addresses for ingress requests and 'internal' addresses that will handle all the cluster replication and
     * control traffic.
     *
     * @param nodeId            id for this node.
     * @param ingressHostnames  list of hostnames that will receive ingress request traffic.
     * @param clusterHostnames  list of hostnames that will receive cluster traffic.
     * @param portBase          base port to derive remaining ports from.
     * @param clusteredService  instance of the clustered service that will run on this node.
     * @return                  configuration that wraps all aeron service configuration.
     */
    public static ClusterConfig create(
        final int nodeId,
        final List<String> ingressHostnames,
        final List<String> clusterHostnames,
        final int portBase,
        final ClusteredService clusteredService)
    {
        if (nodeId >= ingressHostnames.size())
        {
            throw new IllegalArgumentException(
                "nodeId=" + nodeId + " >= ingressHostnames.size()=" + ingressHostnames.size());
        }

        final String clusterMembers = clusterMembers(ingressHostnames, clusterHostnames, portBase);

        final String aeronDirName = CommonContext.getAeronDirectoryName() + "-" + nodeId + "-driver";
        final File baseDir = new File(System.getProperty("user.dir"), "aeron-cluster-" + nodeId);

        final String hostname = clusterHostnames.get(nodeId);

        final MediaDriver.Context mediaDriverContext = new MediaDriver.Context()
            .aeronDirectoryName(aeronDirName)
            .threadingMode(ThreadingMode.SHARED)
            .termBufferSparseFile(true)
            .multicastFlowControlSupplier(new MinMulticastFlowControlSupplier());

        final AeronArchive.Context replicationArchiveContext = new AeronArchive.Context()
            .controlResponseChannel("aeron:udp?endpoint=" + hostname + ":0");

        final Archive.Context archiveContext = new Archive.Context()
            .aeronDirectoryName(aeronDirName)
            .archiveDir(new File(baseDir, ARCHIVE_SUB_DIR))
            .controlChannel(udpChannel(nodeId, hostname, portBase, ARCHIVE_CONTROL_PORT_OFFSET))
            .archiveClientContext(replicationArchiveContext)
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
            .clusterMemberId(nodeId)
            .clusterMembers(clusterMembers)
            .clusterDir(new File(baseDir, CLUSTER_SUB_DIR))
            .archiveContext(aeronArchiveContext.clone())
            .replicationChannel("aeron:udp?endpoint=" + hostname + ":0");

        final ClusteredServiceContainer.Context clusteredServiceContext = new ClusteredServiceContainer.Context()
            .aeronDirectoryName(aeronDirName)
            .archiveContext(aeronArchiveContext.clone())
            .clusterDir(new File(baseDir, CLUSTER_SUB_DIR))
            .clusteredService(clusteredService);

        return new ClusterConfig(
            mediaDriverContext, archiveContext, aeronArchiveContext, consensusModuleContext, clusteredServiceContext);
    }

    /**
     * Create a new ClusterConfig. This only supports a single lists of hostnames.
     *
     * @param nodeId            id for this node.
     * @param hostnames         list of hostnames that will receive ingress request and cluster traffic.
     * @param portBase          base port to derive remaining ports from.
     * @param clusteredService  instance of the clustered service that will run on this node.
     * @return                  configuration that wraps all of the detail aeron service configuration.
     */
    public static ClusterConfig create(
        final int nodeId,
        final List<String> hostnames,
        final int portBase,
        final ClusteredService clusteredService)
    {
        return create(nodeId, hostnames, hostnames, portBase, clusteredService);
    }

    /**
     * Set the same error handler for all contexts.
     *
     * @param errorHandler to receive errors.
     */
    public void errorHandler(final ErrorHandler errorHandler)
    {
        this.mediaDriverContext.errorHandler(errorHandler);
        this.archiveContext.errorHandler(errorHandler);
        this.aeronArchiveContext.errorHandler(errorHandler);
        this.consensusModuleContext.errorHandler(errorHandler);
        this.clusteredServiceContext.errorHandler(errorHandler);
    }

    /**
     * Set the aeron directory for all configuration contexts.
     *
     * @param aeronDir directory to use for aeron.
     */
    public void aeronDirectoryName(final String aeronDir)
    {
        this.mediaDriverContext.aeronDirectoryName(aeronDir);
        this.archiveContext.aeronDirectoryName(aeronDir);
        this.aeronArchiveContext.aeronDirectoryName(aeronDir);
        this.consensusModuleContext.aeronDirectoryName(aeronDir);
        this.clusteredServiceContext.aeronDirectoryName(aeronDir);
    }

    /**
     * Set the base directory for cluster and archive.
     *
     * @param baseDir parent directory to be used for archive and cluster stored data.
     */
    public void baseDir(final File baseDir)
    {
        this.archiveContext.archiveDir(new File(baseDir, ARCHIVE_SUB_DIR));
        this.consensusModuleContext.clusterDir(new File(baseDir, CLUSTER_SUB_DIR));
        this.clusteredServiceContext.clusterDir(new File(baseDir, CLUSTER_SUB_DIR));
    }

    /**
     * Gets the configuration's media driver context.
     *
     * @return configured {@link io.aeron.driver.MediaDriver.Context}.
     * @see io.aeron.driver.MediaDriver.Context
     */
    public MediaDriver.Context mediaDriverContext()
    {
        return mediaDriverContext;
    }

    /**
     * Gets the configuration's archive context.
     *
     * @return configured {@link io.aeron.archive.Archive.Context}.
     * @see io.aeron.archive.Archive.Context
     */
    public Archive.Context archiveContext()
    {
        return archiveContext;
    }

    /**
     * Gets the configuration's aeron archive context.
     *
     * @return configured {@link io.aeron.archive.Archive.Context}.
     * @see io.aeron.archive.client.AeronArchive.Context
     */
    public AeronArchive.Context aeronArchiveContext()
    {
        return aeronArchiveContext;
    }

    /**
     * Gets the configuration's consensus module context.
     *
     * @return configured {@link io.aeron.cluster.ConsensusModule.Context}.
     * @see io.aeron.cluster.ConsensusModule.Context
     */
    public ConsensusModule.Context consensusModuleContext()
    {
        return consensusModuleContext;
    }

    /**
     * Gets the configuration's clustered service container context.
     *
     * @return configured {@link io.aeron.cluster.service.ClusteredServiceContainer.Context}.
     * @see io.aeron.cluster.service.ClusteredServiceContainer.Context
     */
    public ClusteredServiceContainer.Context clusteredServiceContext()
    {
        return clusteredServiceContext;
    }

    /**
     * String representing the cluster members configuration which can be used for
     * {@link io.aeron.cluster.ClusterMember#parse(String)}.
     *
     * @param ingressHostnames of the cluster members.
     * @param clusterHostnames of the cluster members internal address (can be the same as 'hostnames').
     * @param portBase         initial port to derive other port from via appropriate node id and offset.
     * @return the String which can be used for {@link io.aeron.cluster.ClusterMember#parse(String)}.
     */
    public static String clusterMembers(
        final List<String> ingressHostnames, final List<String> clusterHostnames, final int portBase)
    {
        if (ingressHostnames.size() != clusterHostnames.size())
        {
            throw new IllegalArgumentException("ingressHostnames and clusterHostnames must be the same size");
        }

        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ingressHostnames.size(); i++)
        {
            sb.append(i);
            sb.append(',').append(endpoint(i, ingressHostnames.get(i), portBase, CLIENT_FACING_PORT_OFFSET));
            sb.append(',').append(endpoint(i, clusterHostnames.get(i), portBase, MEMBER_FACING_PORT_OFFSET));
            sb.append(',').append(endpoint(i, clusterHostnames.get(i), portBase, LOG_PORT_OFFSET));
            sb.append(',').append(endpoint(i, clusterHostnames.get(i), portBase, TRANSFER_PORT_OFFSET));
            sb.append(',').append(endpoint(i, clusterHostnames.get(i), portBase, ARCHIVE_CONTROL_PORT_OFFSET));
            sb.append('|');
        }

        return sb.toString();
    }

    /**
     * Ingress endpoints generated from a list of hostnames.
     *
     * @param hostnames for the cluster members.
     * @param portBase Base port for the cluster
     * @param clientFacingPortOffset Offset for the client facing port
     * @return a formatted string of ingress endpoints for connecting to a cluster.
     */
    public static String ingressEndpoints(
        final List<String> hostnames,
        final int portBase,
        final int clientFacingPortOffset)
    {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < hostnames.size(); i++)
        {
            sb.append(i).append('=');
            sb.append(hostnames.get(i)).append(':').append(
                calculatePort(i, portBase, clientFacingPortOffset));
            sb.append(',');
        }

        sb.setLength(sb.length() - 1);

        return sb.toString();
    }

    static int calculatePort(final int nodeId, final int portBase, final int offset)
    {
        return portBase + (nodeId * PORTS_PER_NODE) + offset;
    }

    private static String udpChannel(final int nodeId, final String hostname, final int portBase, final int portOffset)
    {
        final int port = calculatePort(nodeId, portBase, portOffset);
        return "aeron:udp?endpoint=" + hostname + ":" + port;
    }

    private static String endpoint(final int nodeId, final String hostname, final int portBase, final int portOffset)
    {
        return hostname + ":" + calculatePort(nodeId, portBase, portOffset);
    }
}
