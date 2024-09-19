/*
 * Copyright 2014-2024 Real Logic Limited.
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
import java.util.ArrayList;
import java.util.List;

/**
 * Wrapper class to simplify cluster configuration. This is sample code and is intended to show a mechanism for
 * managing the configuration of the components required for Aeron Cluster. This code may change between versions
 * and API compatibility is not guaranteed.
 */
public final class ClusterConfig
{
    /**
     * Number of ports per node reserved.
     */
    public static final int PORTS_PER_NODE = 100;

    /**
     * Offset from base port that the archive control channel is on.
     */
    public static final int ARCHIVE_CONTROL_PORT_OFFSET = 1;

    /**
     * Offset from base port that the client facing port is on for ingress.
     */
    public static final int CLIENT_FACING_PORT_OFFSET = 2;

    /**
     * Offset from base port that the member listens on for consensus traffic.
     */
    public static final int MEMBER_FACING_PORT_OFFSET = 3;

    /**
     * Offset from base port that the cluster log is on.
     */
    public static final int LOG_PORT_OFFSET = 4;

    /**
     * Offset from base port that the transfer of files is on.
     */
    public static final int TRANSFER_PORT_OFFSET = 5;

    /**
     * Subdirectory into which archive files are stored.
     */
    public static final String ARCHIVE_SUB_DIR = "archive";

    /**
     * Subdirectory into which cluster files are stored.
     */
    public static final String CLUSTER_SUB_DIR = "cluster";

    private final int memberId;
    private final String ingressHostname;
    private final String clusterHostname;
    private final MediaDriver.Context mediaDriverContext;
    private final Archive.Context archiveContext;
    private final AeronArchive.Context aeronArchiveContext;
    private final ConsensusModule.Context consensusModuleContext;
    private final List<ClusteredServiceContainer.Context> clusteredServiceContexts;

    ClusterConfig(
        final int memberId,
        final String ingressHostname,
        final String clusterHostname,
        final MediaDriver.Context mediaDriverContext,
        final Archive.Context archiveContext,
        final AeronArchive.Context aeronArchiveContext,
        final ConsensusModule.Context consensusModuleContext,
        final List<ClusteredServiceContainer.Context> clusteredServiceContexts)
    {
        this.memberId = memberId;
        this.ingressHostname = ingressHostname;
        this.clusterHostname = clusterHostname;
        this.mediaDriverContext = mediaDriverContext;
        this.archiveContext = archiveContext;
        this.aeronArchiveContext = aeronArchiveContext;
        this.consensusModuleContext = consensusModuleContext;
        this.clusteredServiceContexts = clusteredServiceContexts;
    }

    /**
     * Create a new ClusterConfig. This call allows for 2 separate lists of hostnames, so that there can be 'external'
     * addresses for ingress requests and 'internal' addresses that will handle all the cluster replication and
     * control traffic.
     *
     * @param startingMemberId   id for the first member in the list of entries.
     * @param memberId           id for this node.
     * @param ingressHostnames   list of hostnames that will receive ingress request traffic.
     * @param clusterHostnames   list of hostnames that will receive cluster traffic.
     * @param portBase           base port to derive remaining ports from.
     * @param parentDir          directory under which the persistent directories will be created.
     * @param clusteredService   instance of the clustered service that will run with this configuration.
     * @param additionalServices instances of additional clustered services that will run with this configuration.
     * @return configuration that wraps all aeron service configuration.
     */
    public static ClusterConfig create(
        final int startingMemberId,
        final int memberId,
        final List<String> ingressHostnames,
        final List<String> clusterHostnames,
        final int portBase,
        final File parentDir,
        final ClusteredService clusteredService,
        final ClusteredService... additionalServices)
    {
        if (memberId < startingMemberId || (startingMemberId + ingressHostnames.size()) <= memberId)
        {
            throw new IllegalArgumentException(
                "memberId=" + memberId + " is invalid, should be " + startingMemberId +
                " <= memberId < " + startingMemberId + ingressHostnames.size());
        }

        final String clusterMembers = clusterMembers(startingMemberId, ingressHostnames, clusterHostnames, portBase);

        final String aeronDirName = CommonContext.getAeronDirectoryName() + "-" + memberId + "-driver";
        final File baseDir = new File(parentDir, "aeron-cluster-" + memberId);

        final String ingressHostname = ingressHostnames.get(memberId - startingMemberId);
        final String hostname = clusterHostnames.get(memberId - startingMemberId);

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
            .controlChannel(udpChannel(memberId, hostname, portBase, ARCHIVE_CONTROL_PORT_OFFSET))
            .archiveClientContext(replicationArchiveContext)
            .localControlChannel("aeron:ipc?term-length=64k")
            .replicationChannel("aeron:udp?endpoint=" + hostname + ":0")
            .recordingEventsEnabled(false)
            .threadingMode(ArchiveThreadingMode.SHARED);

        final AeronArchive.Context aeronArchiveContext = new AeronArchive.Context()
            .lock(NoOpLock.INSTANCE)
            .controlRequestChannel(archiveContext.localControlChannel())
            .controlRequestStreamId(archiveContext.localControlStreamId())
            .controlResponseChannel(archiveContext.localControlChannel())
            .aeronDirectoryName(aeronDirName);

        final ConsensusModule.Context consensusModuleContext = new ConsensusModule.Context()
            .clusterMemberId(memberId)
            .clusterMembers(clusterMembers)
            .clusterDir(new File(baseDir, CLUSTER_SUB_DIR))
            .archiveContext(aeronArchiveContext.clone())
            .serviceCount(1 + additionalServices.length)
            .replicationChannel("aeron:udp?endpoint=" + hostname + ":0");

        final List<ClusteredServiceContainer.Context> serviceContexts = new ArrayList<>();

        final ClusteredServiceContainer.Context clusteredServiceContext = new ClusteredServiceContainer.Context()
            .aeronDirectoryName(aeronDirName)
            .archiveContext(aeronArchiveContext.clone())
            .clusterDir(new File(baseDir, CLUSTER_SUB_DIR))
            .clusteredService(clusteredService)
            .serviceId(0);
        serviceContexts.add(clusteredServiceContext);

        for (int i = 0; i < additionalServices.length; i++)
        {
            final ClusteredServiceContainer.Context additionalServiceContext = new ClusteredServiceContainer.Context()
                .aeronDirectoryName(aeronDirName)
                .archiveContext(aeronArchiveContext.clone())
                .clusterDir(new File(baseDir, CLUSTER_SUB_DIR))
                .clusteredService(additionalServices[i])
                .serviceId(i + 1);
            serviceContexts.add(additionalServiceContext);
        }

        return new ClusterConfig(
            memberId,
            ingressHostname,
            hostname,
            mediaDriverContext,
            archiveContext,
            aeronArchiveContext,
            consensusModuleContext,
            serviceContexts);
    }

    /**
     * Create a new ClusterConfig. This call allows for 2 separate lists of hostnames, so that there can be 'external'
     * addresses for ingress requests and 'internal' addresses that will handle all the cluster replication and
     * control traffic.
     *
     * @param nodeId             id for this node.
     * @param ingressHostnames   list of hostnames that will receive ingress request traffic.
     * @param clusterHostnames   list of hostnames that will receive cluster traffic.
     * @param portBase           base port to derive remaining ports from.
     * @param clusteredService   instance of the clustered service that will run with this configuration.
     * @param additionalServices instances of additional clustered services that will run with this configuration.
     * @return configuration that wraps all aeron service configuration.
     */
    public static ClusterConfig create(
        final int nodeId,
        final List<String> ingressHostnames,
        final List<String> clusterHostnames,
        final int portBase,
        final ClusteredService clusteredService,
        final ClusteredService... additionalServices)
    {
        return create(
            0,
            nodeId,
            ingressHostnames,
            clusterHostnames,
            portBase,
            new File(System.getProperty("user.dir")),
            clusteredService,
            additionalServices);
    }

    /**
     * Create a new ClusterConfig. This only supports a single lists of hostnames.
     *
     * @param nodeId           id for this node.
     * @param hostnames        list of hostnames that will receive ingress request and cluster traffic.
     * @param portBase         base port to derive remaining ports from.
     * @param clusteredService instance of the clustered service that will run on this node.
     * @return configuration that wraps the detailed aeron service configuration.
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
        this.clusteredServiceContexts.forEach((ctx) -> ctx.errorHandler(errorHandler));
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
        this.clusteredServiceContexts.forEach(ctx -> ctx.aeronDirectoryName(aeronDir));
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
        this.clusteredServiceContexts.forEach((ctx) -> ctx.clusterDir(new File(baseDir, CLUSTER_SUB_DIR)));
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
        return clusteredServiceContexts.get(0);
    }

    /**
     * Gets the configuration's list of clustered service container contexts.
     *
     * @return configured list of {@link io.aeron.cluster.service.ClusteredServiceContainer.Context}.
     * @see io.aeron.cluster.service.ClusteredServiceContainer.Context
     */
    public List<ClusteredServiceContainer.Context> clusteredServiceContexts()
    {
        return clusteredServiceContexts;
    }

    /**
     * memberId of this node.
     *
     * @return memberId.
     */
    public int memberId()
    {
        return memberId;
    }

    /**
     * Hostname of this node that will receive ingress traffic.
     *
     * @return ingress hostname.
     */
    public String ingressHostname()
    {
        return ingressHostname;
    }

    /**
     * Hostname of this node that will receive cluster traffic.
     *
     * @return cluster hostname
     */
    public String clusterHostname()
    {
        return clusterHostname;
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
        return clusterMembers(0, ingressHostnames, clusterHostnames, portBase);
    }

    /**
     * String representing the cluster members configuration which can be used for
     * {@link io.aeron.cluster.ClusterMember#parse(String)}.
     *
     * @param startingMemberId first memberId to be used in the list of clusterMembers. The memberId will increment by 1
     *                         from that value for each entry.
     * @param ingressHostnames of the cluster members.
     * @param clusterHostnames of the cluster members internal address (can be the same as 'hostnames').
     * @param portBase         initial port to derive other port from via appropriate node id and offset.
     * @return the String which can be used for {@link io.aeron.cluster.ClusterMember#parse(String)}.
     */
    public static String clusterMembers(
        final int startingMemberId,
        final List<String> ingressHostnames,
        final List<String> clusterHostnames,
        final int portBase)
    {
        if (ingressHostnames.size() != clusterHostnames.size())
        {
            throw new IllegalArgumentException("ingressHostnames and clusterHostnames must be the same size");
        }

        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ingressHostnames.size(); i++)
        {
            final int memberId = i + startingMemberId;
            sb.append(memberId);
            sb.append(',').append(endpoint(memberId, ingressHostnames.get(i), portBase, CLIENT_FACING_PORT_OFFSET));
            sb.append(',').append(endpoint(memberId, clusterHostnames.get(i), portBase, MEMBER_FACING_PORT_OFFSET));
            sb.append(',').append(endpoint(memberId, clusterHostnames.get(i), portBase, LOG_PORT_OFFSET));
            sb.append(',').append(endpoint(memberId, clusterHostnames.get(i), portBase, TRANSFER_PORT_OFFSET));
            sb.append(',').append(endpoint(memberId, clusterHostnames.get(i), portBase, ARCHIVE_CONTROL_PORT_OFFSET));
            sb.append('|');
        }

        return sb.toString();
    }

    /**
     * Ingress endpoints generated from a list of hostnames.
     *
     * @param hostnames              for the cluster members.
     * @param portBase               Base port for the cluster
     * @param clientFacingPortOffset Offset for the client facing port
     * @return a formatted string of ingress endpoints for connecting to a cluster.
     */
    public static String ingressEndpoints(
        final List<String> hostnames,
        final int portBase,
        final int clientFacingPortOffset)
    {
        return ingressEndpoints(0, hostnames, portBase, clientFacingPortOffset);
    }


    /**
     * Ingress endpoints generated from a list of hostnames.
     *
     * @param startingMemberId       first memberId to be used when generating the ports.
     * @param hostnames              for the cluster members.
     * @param portBase               Base port for the cluster
     * @param clientFacingPortOffset Offset for the client facing port
     * @return a formatted string of ingress endpoints for connecting to a cluster.
     */
    public static String ingressEndpoints(
        final int startingMemberId,
        final List<String> hostnames,
        final int portBase,
        final int clientFacingPortOffset)
    {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < hostnames.size(); i++)
        {
            final int memberId = i + startingMemberId;
            sb.append(memberId).append('=');
            sb.append(hostnames.get(i)).append(':').append(calculatePort(memberId, portBase, clientFacingPortOffset));
            sb.append(',');
        }

        sb.setLength(sb.length() - 1);

        return sb.toString();
    }

    /**
     * Calculates a port for use with a node based on a specific offset.  Can be used with the predefined offsets, e.g.
     * {@link ClusterConfig#ARCHIVE_CONTROL_PORT_OFFSET} or with custom offsets. For custom offsets select a value
     * larger than largest predefined offsets.  A value larger than the largest predefined offset, but less than
     * {@link ClusterConfig#PORTS_PER_NODE} is required.
     *
     * @param nodeId   The id for the member of the cluster.
     * @param portBase The port base to be used.
     * @param offset   The offset to add onto the port base
     * @return a calculated port, which should be unique for the specified criteria.
     */
    public static int calculatePort(final int nodeId, final int portBase, final int offset)
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
