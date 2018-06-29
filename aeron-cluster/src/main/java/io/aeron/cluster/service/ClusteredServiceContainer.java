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
package io.aeron.cluster.service;

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.mark.ClusterComponentType;
import io.aeron.cluster.codecs.mark.MarkFileHeaderEncoder;
import io.aeron.exceptions.ConfigurationException;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.IoUtil;
import org.agrona.LangUtil;
import org.agrona.concurrent.*;
import org.agrona.concurrent.errors.DistinctErrorLog;
import org.agrona.concurrent.errors.LoggingErrorHandler;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.StatusIndicator;

import java.io.File;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;

import static io.aeron.driver.status.SystemCounterDescriptor.SYSTEM_COUNTER_TYPE_ID;
import static org.agrona.SystemUtil.getSizeAsInt;
import static org.agrona.SystemUtil.loadPropertiesFiles;

public final class ClusteredServiceContainer implements AutoCloseable
{
    /**
     * Type of snapshot for this service.
     */
    public static final long SNAPSHOT_TYPE_ID = 2;

    private final Context ctx;
    private final AgentRunner serviceAgentRunner;

    /**
     * Launch the clustered service container and await a shutdown signal.
     *
     * @param args command line argument which is a list for properties files as URLs or filenames.
     */
    public static void main(final String[] args)
    {
        loadPropertiesFiles(args);

        try (ClusteredServiceContainer container = launch())
        {
            container.context().shutdownSignalBarrier().await();

            System.out.println("Shutdown ClusteredServiceContainer...");
        }
    }

    private ClusteredServiceContainer(final Context ctx)
    {
        this.ctx = ctx;
        ctx.conclude();

        final ClusteredServiceAgent agent = new ClusteredServiceAgent(ctx);
        serviceAgentRunner = new AgentRunner(ctx.idleStrategy(), ctx.errorHandler(), ctx.errorCounter(), agent);
    }

    private ClusteredServiceContainer start()
    {
        AgentRunner.startOnThread(serviceAgentRunner, ctx.threadFactory());
        return this;
    }

    /**
     * Launch an ClusteredServiceContainer using a default configuration.
     *
     * @return a new instance of a ClusteredServiceContainer.
     */
    public static ClusteredServiceContainer launch()
    {
        return launch(new Context());
    }

    /**
     * Launch a ClusteredServiceContainer by providing a configuration context.
     *
     * @param ctx for the configuration parameters.
     * @return a new instance of a ClusteredServiceContainer.
     */
    public static ClusteredServiceContainer launch(final Context ctx)
    {
        return new ClusteredServiceContainer(ctx).start();
    }

    /**
     * Get the {@link Context} that is used by this {@link ClusteredServiceContainer}.
     *
     * @return the {@link Context} that is used by this {@link ClusteredServiceContainer}.
     */
    public Context context()
    {
        return ctx;
    }

    public void close()
    {
        CloseHelper.close(serviceAgentRunner);
        CloseHelper.close(ctx);
    }

    /**
     * Configuration options for the consensus module and service container within a cluster.
     */
    public static class Configuration
    {
        /**
         * Identity for a clustered service. Services should be numbered from 0 and be contiguous.
         */
        public static final String SERVICE_ID_PROP_NAME = "aeron.cluster.service.id";

        /**
         * Default identity for a clustered service.
         */
        public static final int SERVICE_ID_DEFAULT = 0;

        /**
         * Name for a clustered service to be the role of the {@link Agent}.
         */
        public static final String SERVICE_NAME_PROP_NAME = "aeron.cluster.service.name";

        /**
         * Name for a clustered service to be the role of the {@link Agent}.
         */
        public static final String SERVICE_NAME_DEFAULT = "clustered-service";

        /**
         * Class name for dynamically loading a {@link ClusteredService}. This is used if
         * {@link Context#clusteredService()} is not set.
         */
        public static final String SERVICE_CLASS_NAME_PROP_NAME = "aeron.cluster.service.class.name";

        /**
         * Channel to be used for log or snapshot replay on startup.
         */
        public static final String REPLAY_CHANNEL_PROP_NAME = "aeron.cluster.replay.channel";

        /**
         * Default channel to be used for log or snapshot replay on startup.
         */
        public static final String REPLAY_CHANNEL_DEFAULT = CommonContext.IPC_CHANNEL;

        /**
         * Stream id within a channel for the clustered log or snapshot replay.
         */
        public static final String REPLAY_STREAM_ID_PROP_NAME = "aeron.cluster.replay.stream.id";

        /**
         * Default stream id for the log or snapshot replay within a channel.
         */
        public static final int REPLAY_STREAM_ID_DEFAULT = 103;

        /**
         * Channel for communications between the local consensus module and services.
         */
        public static final String SERVICE_CONTROL_CHANNEL_PROP_NAME = "aeron.cluster.service.control.channel";

        /**
         * Default channel for communications between the local consensus module and services. This should be IPC.
         */
        public static final String SERVICE_CONTROL_CHANNEL_DEFAULT = "aeron:ipc?term-length=64k|mtu=8k";

        /**
         * Stream id within a channel for communications from the consensus module to the services.
         */
        public static final String SERVICE_STREAM_ID_PROP_NAME = "aeron.cluster.service.stream.id";

        /**
         * Default stream id within a channel for communications from the consensus module to the services.
         */
        public static final int SERVICE_CONTROL_STREAM_ID_DEFAULT = 104;

        /**
         * Stream id within a channel for communications from the services to the consensus module.
         */
        public static final String CONSENSUS_MODULE_STREAM_ID_PROP_NAME = "aeron.cluster.consensus.module.stream.id";

        /**
         * Default stream id within a channel for communications from the services to the consensus module.
         */
        public static final int CONSENSUS_MODULE_STREAM_ID_DEFAULT = 105;

        /**
         * Channel to be used for archiving snapshots.
         */
        public static final String SNAPSHOT_CHANNEL_PROP_NAME = "aeron.cluster.snapshot.channel";

        /**
         * Default channel to be used for archiving snapshots.
         */
        public static final String SNAPSHOT_CHANNEL_DEFAULT = CommonContext.IPC_CHANNEL;

        /**
         * Stream id within a channel for archiving snapshots.
         */
        public static final String SNAPSHOT_STREAM_ID_PROP_NAME = "aeron.cluster.snapshot.stream.id";

        /**
         * Default stream id for the archived snapshots within a channel.
         */
        public static final int SNAPSHOT_STREAM_ID_DEFAULT = 106;

        /**
         * Directory to use for the aeron cluster.
         */
        public static final String CLUSTER_DIR_PROP_NAME = "aeron.cluster.dir";

        /**
         * Default directory to use for the aeron cluster.
         */
        public static final String CLUSTER_DIR_DEFAULT = "aeron-cluster";

        /**
         * Length in bytes of the error buffer for the cluster container.
         */
        public static final String ERROR_BUFFER_LENGTH_PROP_NAME = "aeron.cluster.service.error.buffer.length";

        /**
         * Default length in bytes of the error buffer for the cluster container.
         */
        public static final int ERROR_BUFFER_LENGTH_DEFAULT = 1024 * 1024;

        /**
         * The value {@link #SERVICE_ID_DEFAULT} or system property {@link #SERVICE_ID_PROP_NAME} if set.
         *
         * @return {@link #SERVICE_ID_DEFAULT} or system property {@link #SERVICE_ID_PROP_NAME} if set.
         */
        public static int serviceId()
        {
            return Integer.getInteger(SERVICE_ID_PROP_NAME, SERVICE_ID_DEFAULT);
        }

        /**
         * The value {@link #SERVICE_NAME_DEFAULT} or system property {@link #SERVICE_NAME_PROP_NAME} if set.
         *
         * @return {@link #SERVICE_NAME_DEFAULT} or system property {@link #SERVICE_NAME_PROP_NAME} if set.
         */
        public static String serviceName()
        {
            return System.getProperty(SERVICE_NAME_PROP_NAME, SERVICE_NAME_DEFAULT);
        }

        /**
         * The value {@link #REPLAY_CHANNEL_DEFAULT} or system property {@link #REPLAY_CHANNEL_PROP_NAME} if set.
         *
         * @return {@link #REPLAY_CHANNEL_DEFAULT} or system property {@link #REPLAY_CHANNEL_PROP_NAME} if set.
         */
        public static String replayChannel()
        {
            return System.getProperty(REPLAY_CHANNEL_PROP_NAME, REPLAY_CHANNEL_DEFAULT);
        }

        /**
         * The value {@link #REPLAY_STREAM_ID_DEFAULT} or system property {@link #REPLAY_STREAM_ID_PROP_NAME}
         * if set.
         *
         * @return {@link #REPLAY_STREAM_ID_DEFAULT} or system property {@link #REPLAY_STREAM_ID_PROP_NAME}
         * if set.
         */
        public static int replayStreamId()
        {
            return Integer.getInteger(REPLAY_STREAM_ID_PROP_NAME, REPLAY_STREAM_ID_DEFAULT);
        }

        /**
         * The value {@link #SERVICE_CONTROL_CHANNEL_DEFAULT} or system property
         * {@link #SERVICE_CONTROL_CHANNEL_PROP_NAME} if set.
         *
         * @return {@link #SERVICE_CONTROL_CHANNEL_DEFAULT} or system property
         * {@link #SERVICE_CONTROL_CHANNEL_PROP_NAME} if set.
         */
        public static String serviceControlChannel()
        {
            return System.getProperty(SERVICE_CONTROL_CHANNEL_PROP_NAME, SERVICE_CONTROL_CHANNEL_DEFAULT);
        }

        /**
         * The value {@link #CONSENSUS_MODULE_STREAM_ID_DEFAULT} or system property
         * {@link #CONSENSUS_MODULE_STREAM_ID_PROP_NAME} if set.
         *
         * @return {@link #CONSENSUS_MODULE_STREAM_ID_DEFAULT} or system property
         * {@link #CONSENSUS_MODULE_STREAM_ID_PROP_NAME} if set.
         */
        public static int consensusModuleStreamId()
        {
            return Integer.getInteger(CONSENSUS_MODULE_STREAM_ID_PROP_NAME, CONSENSUS_MODULE_STREAM_ID_DEFAULT);
        }

        /**
         * The value {@link #SERVICE_CONTROL_STREAM_ID_DEFAULT} or system property
         * {@link #SERVICE_STREAM_ID_PROP_NAME} if set.
         *
         * @return {@link #SERVICE_CONTROL_STREAM_ID_DEFAULT} or system property
         * {@link #SERVICE_STREAM_ID_PROP_NAME} if set.
         */
        public static int serviceStreamId()
        {
            return Integer.getInteger(SERVICE_STREAM_ID_PROP_NAME, SERVICE_CONTROL_STREAM_ID_DEFAULT);
        }

        /**
         * The value {@link #SNAPSHOT_CHANNEL_DEFAULT} or system property {@link #SNAPSHOT_CHANNEL_PROP_NAME} if set.
         *
         * @return {@link #SNAPSHOT_CHANNEL_DEFAULT} or system property {@link #SNAPSHOT_CHANNEL_PROP_NAME} if set.
         */
        public static String snapshotChannel()
        {
            return System.getProperty(SNAPSHOT_CHANNEL_PROP_NAME, SNAPSHOT_CHANNEL_DEFAULT);
        }

        /**
         * The value {@link #SNAPSHOT_STREAM_ID_DEFAULT} or system property {@link #SNAPSHOT_STREAM_ID_PROP_NAME}
         * if set.
         *
         * @return {@link #SNAPSHOT_STREAM_ID_DEFAULT} or system property {@link #SNAPSHOT_STREAM_ID_PROP_NAME} if set.
         */
        public static int snapshotStreamId()
        {
            return Integer.getInteger(SNAPSHOT_STREAM_ID_PROP_NAME, SNAPSHOT_STREAM_ID_DEFAULT);
        }

        public static final String DEFAULT_IDLE_STRATEGY = "org.agrona.concurrent.BackoffIdleStrategy";
        public static final String CLUSTER_IDLE_STRATEGY_PROP_NAME = "aeron.cluster.idle.strategy";

        /**
         * Create a supplier of {@link IdleStrategy}s that will use the system property.
         *
         * @param controllableStatus if a {@link org.agrona.concurrent.ControllableIdleStrategy} is required.
         * @return the new idle strategy
         */
        public static Supplier<IdleStrategy> idleStrategySupplier(final StatusIndicator controllableStatus)
        {
            return () ->
            {
                final String name = System.getProperty(CLUSTER_IDLE_STRATEGY_PROP_NAME, DEFAULT_IDLE_STRATEGY);
                return io.aeron.driver.Configuration.agentIdleStrategy(name, controllableStatus);
            };
        }

        /**
         * The value {@link #CLUSTER_DIR_DEFAULT} or system property {@link #CLUSTER_DIR_PROP_NAME} if set.
         *
         * @return {@link #CLUSTER_DIR_DEFAULT} or system property {@link #CLUSTER_DIR_PROP_NAME} if set.
         */
        public static String clusterDirName()
        {
            return System.getProperty(CLUSTER_DIR_PROP_NAME, CLUSTER_DIR_DEFAULT);
        }

        /**
         * Size in bytes of the error buffer in the mark file.
         *
         * @return length of error buffer in bytes.
         * @see #ERROR_BUFFER_LENGTH_PROP_NAME
         */
        public static int errorBufferLength()
        {
            return getSizeAsInt(ERROR_BUFFER_LENGTH_PROP_NAME, ERROR_BUFFER_LENGTH_DEFAULT);
        }
    }

    public static class Context implements AutoCloseable, Cloneable
    {
        private int serviceId = Configuration.serviceId();
        private String serviceName = Configuration.serviceName();
        private String replayChannel = Configuration.replayChannel();
        private int replayStreamId = Configuration.replayStreamId();
        private String serviceControlChannel = Configuration.serviceControlChannel();
        private int consensusModuleStreamId = Configuration.consensusModuleStreamId();
        private int serviceStreamId = Configuration.serviceStreamId();
        private String snapshotChannel = Configuration.snapshotChannel();
        private int snapshotStreamId = Configuration.snapshotStreamId();
        private int errorBufferLength = Configuration.errorBufferLength();

        private ThreadFactory threadFactory;
        private Supplier<IdleStrategy> idleStrategySupplier;
        private EpochClock epochClock;
        private DistinctErrorLog errorLog;
        private ErrorHandler errorHandler;
        private AtomicCounter errorCounter;
        private CountedErrorHandler countedErrorHandler;
        private AeronArchive.Context archiveContext;
        private String clusterDirectoryName = Configuration.clusterDirName();
        private File clusterDir;
        private String aeronDirectoryName = CommonContext.getAeronDirectoryName();
        private Aeron aeron;
        private boolean ownsAeronClient;

        private ClusteredService clusteredService;
        private ShutdownSignalBarrier shutdownSignalBarrier;
        private Runnable terminationHook;
        private ClusterMarkFile markFile;

        /**
         * Perform a shallow copy of the object.
         *
         * @return a shallow copy of the object.
         */
        public Context clone()
        {
            try
            {
                return (Context)super.clone();
            }
            catch (final CloneNotSupportedException ex)
            {
                throw new RuntimeException(ex);
            }
        }

        @SuppressWarnings("MethodLength")
        public void conclude()
        {
            if (serviceId < 0)
            {
                throw new ConfigurationException("service id must be not be negative: " + serviceId);
            }

            if (null == threadFactory)
            {
                threadFactory = Thread::new;
            }

            if (null == idleStrategySupplier)
            {
                idleStrategySupplier = Configuration.idleStrategySupplier(null);
            }

            if (null == epochClock)
            {
                epochClock = new SystemEpochClock();
            }

            if (null == clusterDir)
            {
                clusterDir = new File(clusterDirectoryName);
            }

            if (!clusterDir.exists() && !clusterDir.mkdirs())
            {
                throw new ClusterException(
                    "failed to create cluster dir: " + clusterDir.getAbsolutePath());
            }

            if (null == markFile)
            {
                markFile = new ClusterMarkFile(
                    new File(clusterDir, ClusterMarkFile.markFilenameForService(serviceId)),
                    ClusterComponentType.CONTAINER,
                    errorBufferLength,
                    epochClock,
                    0);
            }

            if (null == errorLog)
            {
                errorLog = new DistinctErrorLog(markFile.errorBuffer(), epochClock);
            }

            if (null == errorHandler)
            {
                errorHandler = new LoggingErrorHandler(errorLog);
            }

            if (null == aeron)
            {
                aeron = Aeron.connect(
                    new Aeron.Context()
                        .aeronDirectoryName(aeronDirectoryName)
                        .errorHandler(errorHandler)
                        .epochClock(epochClock));

                ownsAeronClient = true;
            }

            if (null == errorCounter)
            {
                errorCounter = aeron.addCounter(SYSTEM_COUNTER_TYPE_ID, "Cluster errors - service " + serviceId);
            }

            if (null == countedErrorHandler)
            {
                countedErrorHandler = new CountedErrorHandler(errorHandler, errorCounter);
                if (ownsAeronClient)
                {
                    aeron.context().errorHandler(countedErrorHandler);
                }
            }

            if (null == archiveContext)
            {
                archiveContext = new AeronArchive.Context()
                    .controlRequestChannel(AeronArchive.Configuration.localControlChannel())
                    .controlResponseChannel(AeronArchive.Configuration.localControlChannel())
                    .controlRequestStreamId(AeronArchive.Configuration.localControlStreamId());
            }

            archiveContext
                .aeron(aeron)
                .ownsAeronClient(false)
                .lock(new NoOpLock());

            if (null == shutdownSignalBarrier)
            {
                shutdownSignalBarrier = new ShutdownSignalBarrier();
            }

            if (null == terminationHook)
            {
                terminationHook = () -> shutdownSignalBarrier.signal();
            }

            if (null == clusteredService)
            {
                final String className = System.getProperty(Configuration.SERVICE_CLASS_NAME_PROP_NAME);
                if (null == className)
                {
                    throw new ClusterException(
                        "either a ClusteredService instance or class name for the service must be provided");
                }

                try
                {
                    clusteredService = (ClusteredService)Class.forName(className).getConstructor().newInstance();
                }
                catch (final Exception ex)
                {
                    LangUtil.rethrowUnchecked(ex);
                }
            }

            concludeMarkFile();
        }

        /**
         * Set the id for this clustered service. Services should be numbered from 0 and be contiguous.
         *
         * @param serviceId for this clustered service.
         * @return this for a fluent API
         * @see Configuration#SERVICE_ID_PROP_NAME
         */
        public Context serviceId(final int serviceId)
        {
            this.serviceId = serviceId;
            return this;
        }

        /**
         * Get the id for this clustered service.Services should be numbered from 0 and be contiguous.
         *
         * @return the id for this clustered service.
         * @see Configuration#SERVICE_ID_PROP_NAME
         */
        public int serviceId()
        {
            return serviceId;
        }

        /**
         * Set the name for a clustered service to be the {@link Agent#roleName()} for the {@link Agent}.
         *
         * @param serviceName for a clustered service to be the role for the {@link Agent}.
         * @return this for a fluent API.
         * @see Configuration#SERVICE_NAME_PROP_NAME
         */
        public Context serviceName(final String serviceName)
        {
            this.serviceName = serviceName;
            return this;
        }

        /**
         * Get the name for a clustered service to be the {@link Agent#roleName()} for the {@link Agent}.
         *
         * @return the name for a clustered service to be the role of the {@link Agent}.
         * @see Configuration#SERVICE_NAME_PROP_NAME
         */
        public String serviceName()
        {
            return serviceName;
        }

        /**
         * Set the channel parameter for the cluster log and snapshot replay channel.
         *
         * @param channel parameter for the cluster log replay channel.
         * @return this for a fluent API.
         * @see Configuration#REPLAY_CHANNEL_PROP_NAME
         */
        public Context replayChannel(final String channel)
        {
            replayChannel = channel;
            return this;
        }

        /**
         * Get the channel parameter for the cluster log and snapshot replay channel.
         *
         * @return the channel parameter for the cluster replay channel.
         * @see Configuration#REPLAY_CHANNEL_PROP_NAME
         */
        public String replayChannel()
        {
            return replayChannel;
        }

        /**
         * Set the stream id for the cluster log and snapshot replay channel.
         *
         * @param streamId for the cluster log replay channel.
         * @return this for a fluent API
         * @see Configuration#REPLAY_STREAM_ID_PROP_NAME
         */
        public Context replayStreamId(final int streamId)
        {
            replayStreamId = streamId;
            return this;
        }

        /**
         * Get the stream id for the cluster log and snapshot replay channel.
         *
         * @return the stream id for the cluster log replay channel.
         * @see Configuration#REPLAY_STREAM_ID_PROP_NAME
         */
        public int replayStreamId()
        {
            return replayStreamId;
        }

        /**
         * Set the channel parameter for bi-directional communications between the consensus module and services.
         *
         * @param channel parameter for sending messages to the Consensus Module.
         * @return this for a fluent API.
         * @see Configuration#SERVICE_CONTROL_CHANNEL_PROP_NAME
         */
        public Context serviceControlChannel(final String channel)
        {
            serviceControlChannel = channel;
            return this;
        }

        /**
         * Get the channel parameter for bi-directional communications between the consensus module and services.
         *
         * @return the channel parameter for sending messages to the Consensus Module.
         * @see Configuration#SERVICE_CONTROL_CHANNEL_PROP_NAME
         */
        public String serviceControlChannel()
        {
            return serviceControlChannel;
        }

        /**
         * Set the stream id for communications from the consensus module and to the services.
         *
         * @param streamId for communications from the consensus module and to the services.
         * @return this for a fluent API
         * @see Configuration#SERVICE_STREAM_ID_PROP_NAME
         */
        public Context serviceStreamId(final int streamId)
        {
            serviceStreamId = streamId;
            return this;
        }

        /**
         * Get the stream id for communications from the consensus module and to the services.
         *
         * @return the stream id for communications from the consensus module and to the services.
         * @see Configuration#SERVICE_STREAM_ID_PROP_NAME
         */
        public int serviceStreamId()
        {
            return serviceStreamId;
        }

        /**
         * Set the stream id for communications from the services to the consensus module.
         *
         * @param streamId for communications from the services to the consensus module.
         * @return this for a fluent API
         * @see Configuration#CONSENSUS_MODULE_STREAM_ID_PROP_NAME
         */
        public Context consensusModuleStreamId(final int streamId)
        {
            consensusModuleStreamId = streamId;
            return this;
        }

        /**
         * Get the stream id for communications from the services to the consensus module.
         *
         * @return the stream id for communications from the services to the consensus module.
         * @see Configuration#CONSENSUS_MODULE_STREAM_ID_PROP_NAME
         */
        public int consensusModuleStreamId()
        {
            return consensusModuleStreamId;
        }

        /**
         * Set the channel parameter for snapshot recordings.
         *
         * @param channel parameter for snapshot recordings
         * @return this for a fluent API.
         * @see Configuration#SNAPSHOT_CHANNEL_PROP_NAME
         */
        public Context snapshotChannel(final String channel)
        {
            snapshotChannel = channel;
            return this;
        }

        /**
         * Get the channel parameter for snapshot recordings.
         *
         * @return the channel parameter for snapshot recordings.
         * @see Configuration#SNAPSHOT_CHANNEL_PROP_NAME
         */
        public String snapshotChannel()
        {
            return snapshotChannel;
        }

        /**
         * Set the stream id for snapshot recordings.
         *
         * @param streamId for snapshot recordings.
         * @return this for a fluent API
         * @see Configuration#SNAPSHOT_STREAM_ID_PROP_NAME
         */
        public Context snapshotStreamId(final int streamId)
        {
            snapshotStreamId = streamId;
            return this;
        }

        /**
         * Get the stream id for snapshot recordings.
         *
         * @return the stream id for snapshot recordings.
         * @see Configuration#SNAPSHOT_STREAM_ID_PROP_NAME
         */
        public int snapshotStreamId()
        {
            return snapshotStreamId;
        }

        /**
         * Get the thread factory used for creating threads.
         *
         * @return thread factory used for creating threads.
         */
        public ThreadFactory threadFactory()
        {
            return threadFactory;
        }

        /**
         * Set the thread factory used for creating threads.
         *
         * @param threadFactory used for creating threads
         * @return this for a fluent API.
         */
        public Context threadFactory(final ThreadFactory threadFactory)
        {
            this.threadFactory = threadFactory;
            return this;
        }

        /**
         * Provides an {@link IdleStrategy} supplier for the thread responsible for publication/subscription backoff.
         *
         * @param idleStrategySupplier supplier of thread idle strategy for publication/subscription backoff.
         * @return this for a fluent API.
         */
        public Context idleStrategySupplier(final Supplier<IdleStrategy> idleStrategySupplier)
        {
            this.idleStrategySupplier = idleStrategySupplier;
            return this;
        }

        /**
         * Get a new {@link IdleStrategy} based on configured supplier.
         *
         * @return a new {@link IdleStrategy} based on configured supplier.
         */
        public IdleStrategy idleStrategy()
        {
            return idleStrategySupplier.get();
        }

        /**
         * Set the {@link EpochClock} to be used for tracking wall clock time when interacting with the archive.
         *
         * @param clock {@link EpochClock} to be used for tracking wall clock time when interacting with the archive.
         * @return this for a fluent API.
         */
        public Context epochClock(final EpochClock clock)
        {
            this.epochClock = clock;
            return this;
        }

        /**
         * Get the {@link EpochClock} to used for tracking wall clock time within the archive.
         *
         * @return the {@link EpochClock} to used for tracking wall clock time within the archive.
         */
        public EpochClock epochClock()
        {
            return epochClock;
        }

        /**
         * Get the {@link ErrorHandler} to be used by the Archive.
         *
         * @return the {@link ErrorHandler} to be used by the Archive.
         */
        public ErrorHandler errorHandler()
        {
            return errorHandler;
        }

        /**
         * Set the {@link ErrorHandler} to be used by the Archive.
         *
         * @param errorHandler the error handler to be used by the Archive.
         * @return this for a fluent API
         */
        public Context errorHandler(final ErrorHandler errorHandler)
        {
            this.errorHandler = errorHandler;
            return this;
        }

        /**
         * Get the error counter that will record the number of errors the archive has observed.
         *
         * @return the error counter that will record the number of errors the archive has observed.
         */
        public AtomicCounter errorCounter()
        {
            return errorCounter;
        }

        /**
         * Set the error counter that will record the number of errors the cluster node has observed.
         *
         * @param errorCounter the error counter that will record the number of errors the cluster node has observed.
         * @return this for a fluent API.
         */
        public Context errorCounter(final AtomicCounter errorCounter)
        {
            this.errorCounter = errorCounter;
            return this;
        }

        /**
         * Non-default for context.
         *
         * @param countedErrorHandler to override the default.
         * @return this for a fluent API.
         */
        public Context countedErrorHandler(final CountedErrorHandler countedErrorHandler)
        {
            this.countedErrorHandler = countedErrorHandler;
            return this;
        }

        /**
         * The {@link #errorHandler()} that will increment {@link #errorCounter()} by default.
         *
         * @return {@link #errorHandler()} that will increment {@link #errorCounter()} by default.
         */
        public CountedErrorHandler countedErrorHandler()
        {
            return countedErrorHandler;
        }

        /**
         * Set the top level Aeron directory used for communication between the Aeron client and Media Driver.
         *
         * @param aeronDirectoryName the top level Aeron directory.
         * @return this for a fluent API.
         */
        public Context aeronDirectoryName(final String aeronDirectoryName)
        {
            this.aeronDirectoryName = aeronDirectoryName;
            return this;
        }

        /**
         * Get the top level Aeron directory used for communication between the Aeron client and Media Driver.
         *
         * @return The top level Aeron directory.
         */
        public String aeronDirectoryName()
        {
            return aeronDirectoryName;
        }

        /**
         * An {@link Aeron} client for the container.
         *
         * @return {@link Aeron} client for the container
         */
        public Aeron aeron()
        {
            return aeron;
        }

        /**
         * Provide an {@link Aeron} client for the container
         * <p>
         * If not provided then one will be created.
         *
         * @param aeron client for the container
         * @return this for a fluent API.
         */
        public Context aeron(final Aeron aeron)
        {
            this.aeron = aeron;
            return this;
        }

        /**
         * Does this context own the {@link #aeron()} client and this takes responsibility for closing it?
         *
         * @param ownsAeronClient does this context own the {@link #aeron()} client.
         * @return this for a fluent API.
         */
        public Context ownsAeronClient(final boolean ownsAeronClient)
        {
            this.ownsAeronClient = ownsAeronClient;
            return this;
        }

        /**
         * Does this context own the {@link #aeron()} client and this takes responsibility for closing it?
         *
         * @return does this context own the {@link #aeron()} client and this takes responsibility for closing it?
         */
        public boolean ownsAeronClient()
        {
            return ownsAeronClient;
        }

        /**
         * The service this container holds.
         *
         * @return service this container holds.
         */
        public ClusteredService clusteredService()
        {
            return clusteredService;
        }

        /**
         * Set the service this container is to hold.
         *
         * @param clusteredService this container is to hold.
         * @return this for fluent API.
         */
        public Context clusteredService(final ClusteredService clusteredService)
        {
            this.clusteredService = clusteredService;
            return this;
        }

        /**
         * Set the {@link AeronArchive.Context} that should be used for communicating with the local Archive.
         *
         * @param archiveContext that should be used for communicating with the local Archive.
         * @return this for a fluent API.
         */
        public Context archiveContext(final AeronArchive.Context archiveContext)
        {
            this.archiveContext = archiveContext;
            return this;
        }

        /**
         * Get the {@link AeronArchive.Context} that should be used for communicating with the local Archive.
         *
         * @return the {@link AeronArchive.Context} that should be used for communicating with the local Archive.
         */
        public AeronArchive.Context archiveContext()
        {
            return archiveContext;
        }

        /**
         * Set the directory name to use for the consensus module directory.
         *
         * @param clusterDirectoryName to use.
         * @return this for a fluent API.
         * @see Configuration#CLUSTER_DIR_PROP_NAME
         */
        public Context clusterDirectoryName(final String clusterDirectoryName)
        {
            this.clusterDirectoryName = clusterDirectoryName;
            return this;
        }

        /**
         * The directory name to use for the cluster directory.
         *
         * @return directory name for the cluster directory.
         * @see Configuration#CLUSTER_DIR_PROP_NAME
         */
        public String clusterDirectoryName()
        {
            return clusterDirectoryName;
        }

        /**
         * Set the directory to use for the cluster directory.
         *
         * @param clusterDir to use.
         * @return this for a fluent API.
         * @see ClusteredServiceContainer.Configuration#CLUSTER_DIR_PROP_NAME
         */
        public Context clusterDir(final File clusterDir)
        {
            this.clusterDir = clusterDir;
            return this;
        }

        /**
         * The directory used for for the cluster directory.
         *
         * @return directory for for the cluster directory.
         * @see ClusteredServiceContainer.Configuration#CLUSTER_DIR_PROP_NAME
         */
        public File clusterDir()
        {
            return clusterDir;
        }

        /**
         * Set the {@link ShutdownSignalBarrier} that can be used to shutdown a clustered service.
         *
         * @param barrier that can be used to shutdown a clustered service.
         * @return this for a fluent API.
         */
        public Context shutdownSignalBarrier(final ShutdownSignalBarrier barrier)
        {
            shutdownSignalBarrier = barrier;
            return this;
        }

        /**
         * Get the {@link ShutdownSignalBarrier} that can be used to shutdown a clustered service.
         *
         * @return the {@link ShutdownSignalBarrier} that can be used to shutdown a clustered service.
         */
        public ShutdownSignalBarrier shutdownSignalBarrier()
        {
            return shutdownSignalBarrier;
        }

        /**
         * Set the {@link Runnable} that is called when processing a
         * {@link io.aeron.cluster.codecs.ClusterAction#SHUTDOWN} or {@link io.aeron.cluster.codecs.ClusterAction#ABORT}
         *
         * @param terminationHook that can be used to terminate a service container.
         * @return this for a fluent API.
         */
        public Context terminationHook(final Runnable terminationHook)
        {
            this.terminationHook = terminationHook;
            return this;
        }

        /**
         * Get the {@link Runnable} that is called when processing a
         * {@link io.aeron.cluster.codecs.ClusterAction#SHUTDOWN} or {@link io.aeron.cluster.codecs.ClusterAction#ABORT}
         * <p>
         * The default action is to call signal on the {@link #shutdownSignalBarrier()}.

         * @return the {@link Runnable} that can be used to terminate a service container.
         */
        public Runnable terminationHook()
        {
            return terminationHook;
        }

        /**
         * Set the {@link ClusterMarkFile} in use.
         *
         * @param markFile to use.
         * @return this for a fluent API.
         */
        public Context clusterMarkFile(final ClusterMarkFile markFile)
        {
            this.markFile = markFile;
            return this;
        }

        /**
         * The {@link ClusterMarkFile} in use.
         *
         * @return {@link ClusterMarkFile} in use.
         */
        public ClusterMarkFile clusterMarkFile()
        {
            return markFile;
        }

        /**
         * Set the error buffer length in bytes to use.
         *
         * @param errorBufferLength in bytes to use.
         * @return this for a fluent API.
         */
        public Context errorBufferLength(final int errorBufferLength)
        {
            this.errorBufferLength = errorBufferLength;
            return this;
        }

        /**
         * The error buffer length in bytes.
         *
         * @return error buffer length in bytes.
         */
        public int errorBufferLength()
        {
            return errorBufferLength;
        }

        /**
         * Set the {@link DistinctErrorLog} in use.
         *
         * @param errorLog to use.
         * @return this for a fluent API.
         */
        public Context errorLog(final DistinctErrorLog errorLog)
        {
            this.errorLog = errorLog;
            return this;
        }

        /**
         * The {@link DistinctErrorLog} in use.
         *
         * @return {@link DistinctErrorLog} in use.
         */
        public DistinctErrorLog errorLog()
        {
            return errorLog;
        }

        /**
         * Delete the cluster container directory.
         */
        public void deleteDirectory()
        {
            if (null != clusterDir)
            {
                IoUtil.delete(clusterDir, false);
            }
        }

        /**
         * Close the context and free applicable resources.
         * <p>
         * If {@link #ownsAeronClient()} is true then the {@link #aeron()} client will be closed.
         */
        public void close()
        {
            CloseHelper.quietClose(markFile);

            if (ownsAeronClient)
            {
                CloseHelper.close(aeron);
            }
        }

        private void concludeMarkFile()
        {
            ClusterMarkFile.checkHeaderLength(
                aeron.context().aeronDirectoryName(),
                archiveContext.controlRequestChannel(),
                serviceControlChannel(),
                null,
                serviceName,
                null);

            final MarkFileHeaderEncoder encoder = markFile.encoder();

            encoder
                .archiveStreamId(archiveContext.controlRequestStreamId())
                .serviceStreamId(serviceStreamId)
                .consensusModuleStreamId(consensusModuleStreamId)
                .ingressStreamId(0)
                .memberId(Aeron.NULL_VALUE)
                .serviceId(serviceId)
                .aeronDirectory(aeron.context().aeronDirectoryName())
                .archiveChannel(archiveContext.controlRequestChannel())
                .serviceControlChannel(serviceControlChannel)
                .ingressChannel("")
                .serviceName(serviceName)
                .authenticator("");

            markFile.updateActivityTimestamp(epochClock.time());
            markFile.signalReady();
        }
    }
}
