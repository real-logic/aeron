/*
 * Copyright 2017 Real Logic Ltd.
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
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.IoUtil;
import org.agrona.concurrent.*;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.StatusIndicator;

import java.io.File;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;

import static io.aeron.driver.status.SystemCounterDescriptor.SYSTEM_COUNTER_TYPE_ID;
import static org.agrona.SystemUtil.loadPropertiesFiles;

public final class ClusteredServiceContainer implements AutoCloseable
{
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

            System.out.println("Shutdown ClusteredMediaDriver...");
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
         * Identity for a clustered service.
         */
        public static final String SERVICE_ID_PROP_NAME = "aeron.cluster.service.id";

        /**
         * Identity for a clustered service. Default to 0.
         */
        public static final long SERVICE_ID_DEFAULT = 0;

        /**
         * Channel for the clustered log.
         */
        public static final String LOG_CHANNEL_PROP_NAME = "aeron.cluster.log.channel";

        /**
         * Channel for the clustered log. Default to localhost:9030.
         */
        public static final String LOG_CHANNEL_DEFAULT = "aeron:udp?endpoint=localhost:9030";

        /**
         * Stream id within a channel for the clustered log.
         */
        public static final String LOG_STREAM_ID_PROP_NAME = "aeron.cluster.log.stream.id";

        /**
         * Stream id within a channel for the clustered log. Default to stream id of 3.
         */
        public static final int LOG_STREAM_ID_DEFAULT = 3;

        /**
         * Channel to be used for log and snapshot replay on startup.
         */
        public static final String REPLAY_CHANNEL_PROP_NAME = "aeron.cluster.replay.channel";

        /**
         * Channel to be used for log and snapshot replay on startup.
         */
        public static final String REPLAY_CHANNEL_DEFAULT = CommonContext.IPC_CHANNEL;

        /**
         * Stream id within a channel for the clustered log and snapshot replay.
         */
        public static final String REPLAY_STREAM_ID_PROP_NAME = "aeron.cluster.replay.stream.id";

        /**
         * Stream id for the log and snapshot replay within a channel.
         */
        public static final int REPLAY_STREAM_ID_DEFAULT = 4;

        /**
         * Channel for sending messages to the Consensus Module.
         */
        public static final String CONSENSUS_MODULE_CHANNEL_PROP_NAME = "aeron.consensus.module.channel";

        /**
         * Channel for for sending messages to the Consensus Module. This should be IPC.
         */
        public static final String CONSENSUS_MODULE_CHANNEL_DEFAULT = CommonContext.IPC_CHANNEL;

        /**
         * Stream id within a channel for sending messages to the Consensus Module.
         */
        public static final String CONSENSUS_MODULE_STREAM_ID_PROP_NAME = "aeron.consensus.module.stream.id";

        /**
         * Stream id within a channel for sending messages to the Consensus Module. Default to stream id of 5.
         */
        public static final int CONSENSUS_MODULE_STREAM_ID_DEFAULT = 5;

        /**
         * Channel to be used for archiving snapshots.
         */
        public static final String SNAPSHOT_CHANNEL_PROP_NAME = "aeron.cluster.snapshot.channel";

        /**
         * Channel to be used for archiving snapshots.
         */
        public static final String SNAPSHOT_CHANNEL_DEFAULT = CommonContext.IPC_CHANNEL;

        /**
         * Stream id within a channel for archiving snapshots.
         */
        public static final String SNAPSHOT_STREAM_ID_PROP_NAME = "aeron.cluster.snapshot.stream.id";

        /**
         * Stream id for the archived snapshots within a channel.
         */
        public static final int SNAPSHOT_STREAM_ID_DEFAULT = 6;

        /**
         * Directory to use for the clustered service.
         */
        public static final String CLUSTERED_SERVICE_DIR_PROP_NAME = "aeron.clustered.service.dir";

        /**
         * Directory to use for the cluster container.
         */
        public static final String CLUSTERED_SERVICE_DIR_DEFAULT = "clustered-service";

        /**
         * The value {@link #SERVICE_ID_DEFAULT} or system property {@link #SERVICE_ID_PROP_NAME} if set.
         *
         * @return {@link #SERVICE_ID_DEFAULT} or system property {@link #SERVICE_ID_PROP_NAME} if set.
         */
        public static long serviceId()
        {
            return Long.getLong(SERVICE_ID_PROP_NAME, SERVICE_ID_DEFAULT);
        }

        /**
         * The value {@link #LOG_CHANNEL_DEFAULT} or system property {@link #LOG_CHANNEL_PROP_NAME} if set.
         *
         * @return {@link #LOG_CHANNEL_DEFAULT} or system property {@link #LOG_CHANNEL_PROP_NAME} if set.
         */
        public static String logChannel()
        {
            return System.getProperty(LOG_CHANNEL_PROP_NAME, LOG_CHANNEL_DEFAULT);
        }

        /**
         * The value {@link #LOG_STREAM_ID_DEFAULT} or system property {@link #LOG_STREAM_ID_PROP_NAME} if set.
         *
         * @return {@link #LOG_STREAM_ID_DEFAULT} or system property {@link #LOG_STREAM_ID_PROP_NAME} if set.
         */
        public static int logStreamId()
        {
            return Integer.getInteger(LOG_STREAM_ID_PROP_NAME, LOG_STREAM_ID_DEFAULT);
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
         * The value {@link #CONSENSUS_MODULE_CHANNEL_DEFAULT} or system property
         * {@link #CONSENSUS_MODULE_CHANNEL_PROP_NAME} if set.
         *
         * @return {@link #CONSENSUS_MODULE_CHANNEL_DEFAULT} or system property
         * {@link #CONSENSUS_MODULE_CHANNEL_PROP_NAME} if set.
         */
        public static String consensusModuleChannel()
        {
            return System.getProperty(CONSENSUS_MODULE_CHANNEL_PROP_NAME, CONSENSUS_MODULE_CHANNEL_DEFAULT);
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
         * The value {@link #CLUSTERED_SERVICE_DIR_DEFAULT} or system property {@link #CLUSTERED_SERVICE_DIR_PROP_NAME} if set.
         *
         * @return {@link #CLUSTERED_SERVICE_DIR_DEFAULT} or system property {@link #CLUSTERED_SERVICE_DIR_PROP_NAME} if set.
         */
        public static String clusteredServiceDirName()
        {
            return System.getProperty(CLUSTERED_SERVICE_DIR_PROP_NAME, CLUSTERED_SERVICE_DIR_DEFAULT);
        }
    }

    public static class Context implements AutoCloseable
    {
        private long serviceId = Configuration.serviceId();
        private String logChannel = Configuration.logChannel();
        private int logStreamId = Configuration.logStreamId();
        private String replayChannel = Configuration.replayChannel();
        private int replayStreamId = Configuration.replayStreamId();
        private String consensusModuleChannel = Configuration.consensusModuleChannel();
        private int consensusModuleStreamId = Configuration.consensusModuleStreamId();
        private String snapshotChannel = Configuration.snapshotChannel();
        private int snapshotStreamId = Configuration.snapshotStreamId();
        private boolean deleteDirOnStart = false;

        private ThreadFactory threadFactory;
        private Supplier<IdleStrategy> idleStrategySupplier;
        private EpochClock epochClock;
        private ErrorHandler errorHandler;
        private AtomicCounter errorCounter;
        private CountedErrorHandler countedErrorHandler;
        private AeronArchive.Context archiveContext;
        private File clusteredServiceDir;
        private String aeronDirectoryName = CommonContext.AERON_DIR_PROP_DEFAULT;
        private Aeron aeron;
        private boolean ownsAeronClient;

        private ClusteredService clusteredService;
        private RecordingIndex recordingIndex;
        private ShutdownSignalBarrier shutdownSignalBarrier;

        public void conclude()
        {
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

            if (null == errorHandler)
            {
                throw new IllegalStateException("Error handler must be supplied");
            }

            if (null == aeron)
            {
                aeron = Aeron.connect(
                    new Aeron.Context()
                        .aeronDirectoryName(aeronDirectoryName)
                        .errorHandler(countedErrorHandler)
                        .epochClock(epochClock));

                if (null == errorCounter)
                {
                    errorCounter = aeron.addCounter(SYSTEM_COUNTER_TYPE_ID, "Cluster errors - service " + serviceId);
                }

                ownsAeronClient = true;
            }

            if (null == errorCounter)
            {
                throw new IllegalStateException("Error counter must be supplied");
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
                    .aeron(aeron)
                    .lock(new NoOpLock());
            }

            if (deleteDirOnStart)
            {
                if (null != clusteredServiceDir)
                {
                    IoUtil.delete(clusteredServiceDir, true);
                }
                else
                {
                    IoUtil.delete(new File(Configuration.clusteredServiceDirName()), true);
                }
            }

            if (null == clusteredServiceDir)
            {
                clusteredServiceDir = new File(Configuration.clusteredServiceDirName());
            }

            if (!clusteredServiceDir.exists() && !clusteredServiceDir.mkdirs())
            {
                throw new IllegalStateException(
                    "Failed to create clustered service dir: " + clusteredServiceDir.getAbsolutePath());
            }

            if (null == recordingIndex)
            {
                recordingIndex = new RecordingIndex(clusteredServiceDir);
            }

            if (null == shutdownSignalBarrier)
            {
                shutdownSignalBarrier = new ShutdownSignalBarrier();
            }
        }

        /**
         * Set the id for this clustered service.
         *
         * @param serviceId for this clustered service.
         * @return this for a fluent API
         * @see ClusteredServiceContainer.Configuration#SERVICE_ID_PROP_NAME
         */
        public Context serviceId(final long serviceId)
        {
            this.serviceId = serviceId;
            return this;
        }

        /**
         * Get the id for this clustered service.
         *
         * @return the id for this clustered service.
         * @see ClusteredServiceContainer.Configuration#SERVICE_ID_PROP_NAME
         */
        public long serviceId()
        {
            return serviceId;
        }

        /**
         * Set the channel parameter for the cluster log channel.
         *
         * @param channel parameter for the cluster log channel.
         * @return this for a fluent API.
         * @see ClusteredServiceContainer.Configuration#LOG_CHANNEL_PROP_NAME
         */
        public Context logChannel(final String channel)
        {
            logChannel = channel;
            return this;
        }

        /**
         * Get the channel parameter for the cluster log channel.
         *
         * @return the channel parameter for the cluster channel.
         * @see ClusteredServiceContainer.Configuration#LOG_CHANNEL_PROP_NAME
         */
        public String logChannel()
        {
            return logChannel;
        }

        /**
         * Set the stream id for the cluster log channel.
         *
         * @param streamId for the cluster log channel.
         * @return this for a fluent API
         * @see ClusteredServiceContainer.Configuration#LOG_STREAM_ID_PROP_NAME
         */
        public Context logStreamId(final int streamId)
        {
            logStreamId = streamId;
            return this;
        }

        /**
         * Get the stream id for the cluster log channel.
         *
         * @return the stream id for the cluster log channel.
         * @see ClusteredServiceContainer.Configuration#LOG_STREAM_ID_PROP_NAME
         */
        public int logStreamId()
        {
            return logStreamId;
        }

        /**
         * Set the channel parameter for the cluster log and snapshot replay channel.
         *
         * @param channel parameter for the cluster log replay channel.
         * @return this for a fluent API.
         * @see ClusteredServiceContainer.Configuration#REPLAY_CHANNEL_PROP_NAME
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
         * @see ClusteredServiceContainer.Configuration#REPLAY_CHANNEL_PROP_NAME
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
         * @see ClusteredServiceContainer.Configuration#REPLAY_STREAM_ID_PROP_NAME
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
         * @see ClusteredServiceContainer.Configuration#REPLAY_STREAM_ID_PROP_NAME
         */
        public int replayStreamId()
        {
            return replayStreamId;
        }

        /**
         * Set the channel parameter for sending messages to the Consensus Module.
         *
         * @param channel parameter for sending messages to the Consensus Module.
         * @return this for a fluent API.
         * @see Configuration#CONSENSUS_MODULE_CHANNEL_PROP_NAME
         */
        public Context consensusModuleChannel(final String channel)
        {
            consensusModuleChannel = channel;
            return this;
        }

        /**
         * Get the channel parameter for sending messages to the Consensus Module.
         *
         * @return the channel parameter for sending messages to the Consensus Module.
         * @see Configuration#CONSENSUS_MODULE_CHANNEL_PROP_NAME
         */
        public String consensusModuleChannel()
        {
            return consensusModuleChannel;
        }

        /**
         * Set the stream id for sending messages to the Consensus Module.
         *
         * @param streamId for sending messages to the Consensus Module.
         * @return this for a fluent API
         * @see Configuration#CONSENSUS_MODULE_STREAM_ID_PROP_NAME
         */
        public Context consensusModuleStreamId(final int streamId)
        {
            consensusModuleStreamId = streamId;
            return this;
        }

        /**
         * Get the stream id for sending messages to the Consensus Module.
         *
         * @return the stream id for sending messages to the Consensus Module.
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
         * Should the container attempt to immediately delete {@link #clusteredServiceDir()} on startup.
         *
         * @param deleteDirOnStart Attempt deletion.
         * @return this for a fluent API.
         */
        public Context deleteDirOnStart(final boolean deleteDirOnStart)
        {
            this.deleteDirOnStart = deleteDirOnStart;
            return this;
        }

        /**
         * Will the container attempt to immediately delete {@link #clusteredServiceDir()} on startup.
         *
         * @return true when directory will be deleted, otherwise false.
         */
        public boolean deleteDirOnStart()
        {
            return deleteDirOnStart;
        }

        /**
         * Set the directory to use for the clustered service container.
         *
         * @param dir to use.
         * @return this for a fluent API.
         * @see Configuration#CLUSTERED_SERVICE_DIR_PROP_NAME
         */
        public Context clusteredServiceDir(final File dir)
        {
            this.clusteredServiceDir = dir;
            return this;
        }

        /**
         * The directory used for the clustered service container.
         *
         * @return directory for the cluster container.
         * @see Configuration#CLUSTERED_SERVICE_DIR_PROP_NAME
         */
        public File clusteredServiceDir()
        {
            return clusteredServiceDir;
        }

        /**
         * Set the {@link RecordingIndex} for the  log terms and snapshots.
         *
         * @param recordingIndex to use.
         * @return this for a fluent API.
         */
        public Context recordingIndex(final RecordingIndex recordingIndex)
        {
            this.recordingIndex = recordingIndex;
            return this;
        }

        /**
         * The {@link RecordingIndex} for the  log terms and snapshots.
         *
         * @return {@link RecordingIndex} for the  log terms and snapshots.
         */
        public RecordingIndex recordingIndex()
        {
            return recordingIndex;
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
         * Delete the cluster container directory.
         */
        public void deleteDirectory()
        {
            if (null != clusteredServiceDir)
            {
                IoUtil.delete(clusteredServiceDir, false);
            }
        }

        /**
         * Close the context and free applicable resources.
         * <p>
         * If {@link #ownsAeronClient()} is true then the {@link #aeron()} client will be closed.
         */
        public void close()
        {
            if (ownsAeronClient)
            {
                aeron.close();
            }
        }
    }
}
