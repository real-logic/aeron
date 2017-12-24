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

import io.aeron.Aeron;
import io.aeron.Counter;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.control.ClusterControl;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.cluster.service.RecordingIndex;
import org.agrona.*;
import org.agrona.concurrent.*;
import org.agrona.concurrent.status.AtomicCounter;

import java.io.File;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.aeron.cluster.ConsensusModule.Configuration.CONSENSUS_MODULE_STATE_TYPE_ID;
import static io.aeron.cluster.control.ClusterControl.CONTROL_TOGGLE_TYPE_ID;
import static io.aeron.driver.status.SystemCounterDescriptor.SYSTEM_COUNTER_TYPE_ID;
import static org.agrona.SystemUtil.getDurationInNanos;

public class ConsensusModule implements
    AutoCloseable,
    IngressAdapterSupplier,
    TimerServiceSupplier,
    ClusterSessionSupplier,
    ConsensusModuleAdapterSupplier
{
    /**
     * Possible states for the consensus module. These will be reflected in the {@link Context#moduleState()} counter.
     */
    public enum State
    {
        INIT(0), ACTIVE(1), SUSPENDED(2), SNAPSHOT(3), SHUTDOWN(4), ABORT(5), CLOSED(6);

        private final int code;

        static final State[] STATES;
        static
        {
            final State[] states = values();
            STATES = new State[states.length];
            for (final State state : states)
            {
                STATES[state.code()] = state;
            }
        }

        State(final int code)
        {
            this.code = code;
        }

        public final int code()
        {
            return code;
        }

        /**
         * Get the {@link State} encoded in an {@link AtomicCounter}.
         *
         * @param counter to get the current state for.
         * @return the state for the {@link ConsensusModule}.
         * @throws IllegalStateException if the counter is not one of the valid values.
         */
        public static State get(final AtomicCounter counter)
        {
            final long value = counter.get();

            if (value < 0 || value > (STATES.length - 1))
            {
                throw new IllegalStateException("Invalid state counter value: " + value);
            }

            return STATES[(int)value];
        }
    }

    private static final int FRAGMENT_POLL_LIMIT = 10;
    private static final int TIMER_POLL_LIMIT = 10;

    private final Context ctx;
    private final LogAppender logAppender;
    private final AgentRunner conductorRunner;

    ConsensusModule(final Context ctx)
    {
        this.ctx = ctx;
        ctx.conclude();

        try (AeronArchive archive = AeronArchive.connect(ctx.archiveContext()))
        {
            archive.startRecording(ctx.logChannel(), ctx.logStreamId(), SourceLocation.LOCAL);
        }

        logAppender = new LogAppender(ctx.aeron().addExclusivePublication(ctx.logChannel(), ctx.logStreamId()));

        final SequencerAgent conductor = new SequencerAgent(
            ctx,
            new EgressPublisher(),
            logAppender,
            this,
            this,
            this,
            this);

        conductorRunner = new AgentRunner(ctx.idleStrategy(), ctx.errorHandler(), ctx.errorCounter(), conductor);
    }

    private ConsensusModule start()
    {
        AgentRunner.startOnThread(conductorRunner, ctx.threadFactory());
        return this;
    }

    /**
     * Launch an {@link ConsensusModule} using a default configuration.
     *
     * @return a new instance of an {@link ConsensusModule}.
     */
    public static ConsensusModule launch()
    {
        return launch(new Context());
    }

    /**
     * Launch an {@link ConsensusModule} by providing a configuration context.
     *
     * @param ctx for the configuration parameters.
     * @return a new instance of an {@link ConsensusModule}.
     */
    public static ConsensusModule launch(final Context ctx)
    {
        return new ConsensusModule(ctx).start();
    }

    /**
     * Get the {@link ConsensusModule.Context} that is used by this {@link ConsensusModule}.
     *
     * @return the {@link ConsensusModule.Context} that is used by this {@link ConsensusModule}.
     */
    public Context context()
    {
        return ctx;
    }

    public void close()
    {
        CloseHelper.close(conductorRunner);
        CloseHelper.close(logAppender);
        CloseHelper.close(ctx);
    }

    public IngressAdapter newIngressAdapter(final SequencerAgent sequencerAgent)
    {
        return new IngressAdapter(
            sequencerAgent,
            ctx.aeron().addSubscription(ctx.ingressChannel(), ctx.ingressStreamId()),
            FRAGMENT_POLL_LIMIT);
    }

    public TimerService newTimerService(final SequencerAgent sequencerAgent)
    {
        return new TimerService(TIMER_POLL_LIMIT, sequencerAgent);
    }

    public ClusterSession newClusterSession(
        final long sessionId, final int responseStreamId, final String responseChannel)
    {
        return new ClusterSession(
            sessionId, ctx.aeron().addPublication(responseChannel, responseStreamId));
    }

    public ConsensusModuleAdapter newConsensusModuleAdapter(final SequencerAgent sequencerAgent)
    {
        return new ConsensusModuleAdapter(
            FRAGMENT_POLL_LIMIT,
            ctx.aeron().addSubscription(ctx.consensusModuleChannel(), ctx.consensusModuleStreamId()),
            sequencerAgent);
    }

    /**
     * Configuration options for cluster.
     */
    public static class Configuration
    {
        /**
         * Message detail to be sent when max concurrent session limit is reached.
         */
        public static final String SESSION_LIMIT_MSG = "Concurrent session limit";

        /**
         * Message detail to be sent when a session timeout occurs.
         */
        public static final String SESSION_TIMEOUT_MSG = "Session inactive";

        /**
         * Message detail to be sent when a session is rejected due to authentication.
         */
        public static final String SESSION_REJECTED_MSG = "Session failed authentication";

        /**
         * Counter type id for the consensus module state.
         */
        public static final int CONSENSUS_MODULE_STATE_TYPE_ID = 200;

        /**
         * Directory to use for the aeron cluster.
         */
        public static final String CLUSTER_DIR_PROP_NAME = "aeron.cluster.dir";

        /**
         * Directory to use for the aeron cluster.
         */
        public static final String CLUSTER_DIR_DEFAULT = "aeron-cluster";

        /**
         * The number of services in this cluster instance.
         */
        public static final String SERVICE_COUNT_PROP_NAME = "aeron.cluster.service.count";

        /**
         * The number of services in this cluster instance. Default to 1.
         */
        public static final int SERVICE_COUNT_DEFAULT = 1;

        /**
         * Maximum number of cluster sessions that can be active concurrently.
         */
        public static final String MAX_CONCURRENT_SESSIONS_PROP_NAME = "aeron.cluster.max.sessions";

        /**
         * Maximum number of cluster sessions that can be active concurrently. Default to 10.
         */
        public static final int MAX_CONCURRENT_SESSIONS_DEFAULT = 10;

        /**
         * Timeout for a session if no activity is observed.
         */
        public static final String SESSION_TIMEOUT_PROP_NAME = "aeron.cluster.session.timeout";

        /**
         * Timeout for a session if no activity is observed. Default to 5 seconds in nanoseconds.
         */
        public static final long SESSION_TIMEOUT_DEFAULT_NS = TimeUnit.SECONDS.toNanos(5);

        /**
         * Name of class to use as a supplier of {@link Authenticator} for the cluster.
         */
        public static final String AUTHENTICATOR_SUPPLIER_PROP_NAME = "aeron.cluster.Authenticator.supplier";

        /**
         * Name of the class to use as a supplier of {@link Authenticator} for the cluster. Default is
         * a non-authenticating option.
         */
        public static final String AUTHENTICATOR_SUPPLIER_DEFAULT = "io.aeron.cluster.DefaultAuthenticatorSupplier";

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
         * The value {@link #SERVICE_COUNT_DEFAULT} or system property
         * {@link #SERVICE_COUNT_PROP_NAME} if set.
         *
         * @return {@link #SERVICE_COUNT_DEFAULT} or system property
         * {@link #SERVICE_COUNT_PROP_NAME} if set.
         */
        public static int serviceCount()
        {
            return Integer.getInteger(SERVICE_COUNT_PROP_NAME, SERVICE_COUNT_DEFAULT);
        }

        /**
         * The value {@link #MAX_CONCURRENT_SESSIONS_DEFAULT} or system property
         * {@link #MAX_CONCURRENT_SESSIONS_PROP_NAME} if set.
         *
         * @return {@link #MAX_CONCURRENT_SESSIONS_DEFAULT} or system property
         * {@link #MAX_CONCURRENT_SESSIONS_PROP_NAME} if set.
         */
        public static int maxConcurrentSessions()
        {
            return Integer.getInteger(MAX_CONCURRENT_SESSIONS_PROP_NAME, MAX_CONCURRENT_SESSIONS_DEFAULT);
        }

        /**
         * Timeout for a session if no activity is observed.
         *
         * @return timeout in nanoseconds to wait for activity
         * @see #SESSION_TIMEOUT_PROP_NAME
         */
        public static long sessionTimeoutNs()
        {
            return getDurationInNanos(SESSION_TIMEOUT_PROP_NAME, SESSION_TIMEOUT_DEFAULT_NS);
        }

        /**
         * The value {@link #AUTHENTICATOR_SUPPLIER_DEFAULT} or system property
         * {@link #AUTHENTICATOR_SUPPLIER_PROP_NAME} if set.
         *
         * @return {@link #AUTHENTICATOR_SUPPLIER_DEFAULT} or system property
         * {@link #AUTHENTICATOR_SUPPLIER_PROP_NAME} if set.
         */
        public static AuthenticatorSupplier authenticatorSupplier()
        {
            final String supplierClassName =
                System.getProperty(AUTHENTICATOR_SUPPLIER_PROP_NAME, AUTHENTICATOR_SUPPLIER_DEFAULT);
            AuthenticatorSupplier supplier = null;

            try
            {
                supplier = (AuthenticatorSupplier)Class.forName(supplierClassName).newInstance();
            }
            catch (final Exception ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }

            return supplier;
        }
    }

    public static class Context implements AutoCloseable
    {
        private boolean ownsAeronClient = false;
        private Aeron aeron;

        private boolean deleteDirOnStart = false;
        private File clusterDir;
        private RecordingIndex recordingIndex;

        private String ingressChannel = AeronCluster.Configuration.ingressChannel();
        private int ingressStreamId = AeronCluster.Configuration.ingressStreamId();
        private String logChannel = ClusteredServiceContainer.Configuration.logChannel();
        private int logStreamId = ClusteredServiceContainer.Configuration.logStreamId();
        private String consensusModuleChannel = ClusteredServiceContainer.Configuration.consensusModuleChannel();
        private int consensusModuleStreamId = ClusteredServiceContainer.Configuration.consensusModuleStreamId();

        private int serviceCount = Configuration.serviceCount();
        private int maxConcurrentSessions = Configuration.maxConcurrentSessions();
        private long sessionTimeoutNs = Configuration.sessionTimeoutNs();

        private ThreadFactory threadFactory;
        private Supplier<IdleStrategy> idleStrategySupplier;
        private EpochClock epochClock;
        private CachedEpochClock cachedEpochClock;

        private ErrorHandler errorHandler;
        private AtomicCounter errorCounter;
        private CountedErrorHandler countedErrorHandler;

        private Counter messageIndex;
        private Counter moduleState;
        private Counter controlToggle;
        private ShutdownSignalBarrier shutdownSignalBarrier;

        private AeronArchive.Context archiveContext;
        private AuthenticatorSupplier authenticatorSupplier;

        @SuppressWarnings("MethodLength")
        public void conclude()
        {
            if (deleteDirOnStart)
            {
                if (null != clusterDir)
                {
                    IoUtil.delete(clusterDir, true);
                }
                else
                {
                    IoUtil.delete(new File(Configuration.clusterDirName()), true);
                }
            }

            if (null == clusterDir)
            {
                clusterDir = new File(Configuration.clusterDirName());
            }

            if (!clusterDir.exists() && !clusterDir.mkdirs())
            {
                throw new IllegalStateException(
                    "Failed to create cluster dir: " + clusterDir.getAbsolutePath());
            }

            if (null == recordingIndex)
            {
                recordingIndex = new RecordingIndex(clusterDir);
            }

            if (null == errorHandler)
            {
                throw new IllegalStateException("Error handler must be supplied");
            }

            if (null == epochClock)
            {
                epochClock = new SystemEpochClock();
            }

            if (null == cachedEpochClock)
            {
                cachedEpochClock = new CachedEpochClock();
            }

            if (null == aeron)
            {
                ownsAeronClient = true;

                aeron = Aeron.connect(
                    new Aeron.Context()
                        .errorHandler(errorHandler)
                        .epochClock(epochClock)
                        .useConductorAgentInvoker(true)
                        .clientLock(new NoOpLock()));

                if (null == errorCounter)
                {
                    errorCounter = aeron.addCounter(SYSTEM_COUNTER_TYPE_ID, "Cluster errors");
                }
            }

            if (null == errorCounter)
            {
                throw new IllegalStateException("Error counter must be supplied if aeron client is");
            }

            if (null == countedErrorHandler)
            {
                countedErrorHandler = new CountedErrorHandler(errorHandler, errorCounter);
                if (ownsAeronClient)
                {
                    aeron.context().errorHandler(countedErrorHandler);
                }
            }

            if (null == messageIndex)
            {
                messageIndex = aeron.addCounter(SYSTEM_COUNTER_TYPE_ID, "Log message index");
            }

            if (null == moduleState)
            {
                moduleState = aeron.addCounter(CONSENSUS_MODULE_STATE_TYPE_ID, "Consensus Module state");
            }

            if (null == controlToggle)
            {
                controlToggle = aeron.addCounter(CONTROL_TOGGLE_TYPE_ID, "Control toggle");
            }

            if (null == threadFactory)
            {
                threadFactory = Thread::new;
            }

            if (null == idleStrategySupplier)
            {
                idleStrategySupplier = ClusteredServiceContainer.Configuration.idleStrategySupplier(null);
            }

            if (null == archiveContext)
            {
                archiveContext = new AeronArchive.Context().lock(new NoOpLock());
            }

            if (null == shutdownSignalBarrier)
            {
                shutdownSignalBarrier = new ShutdownSignalBarrier();
            }

            if (null == authenticatorSupplier)
            {
                authenticatorSupplier = Configuration.authenticatorSupplier();
            }
        }

        /**
         * Should the consensus module attempt to immediately delete {@link #clusterDir()} on startup.
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
         * Will the consensus module attempt to immediately delete {@link #clusterDir()} on startup.
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
         * @param clusterDir to use.
         * @return this for a fluent API.
         * @see Configuration#CLUSTER_DIR_PROP_NAME
         */
        public Context clusterDir(final File clusterDir)
        {
            this.clusterDir = clusterDir;
            return this;
        }

        /**
         * The directory used for the clustered service container.
         *
         * @return directory for the cluster container.
         * @see Configuration#CLUSTER_DIR_PROP_NAME
         */
        public File clusterDir()
        {
            return clusterDir;
        }

        /**
         * Set the {@link RecordingIndex} for the log terms and snapshots.
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
         * The {@link RecordingIndex} for the log terms and snapshots.
         *
         * @return {@link RecordingIndex} for the  log terms and snapshots.
         */
        public RecordingIndex recordingIndex()
        {
            return recordingIndex;
        }

        /**
         * Set the channel parameter for the ingress channel.
         *
         * @param channel parameter for the ingress channel.
         * @return this for a fluent API.
         * @see AeronCluster.Configuration#INGRESS_CHANNEL_PROP_NAME
         */
        public Context ingressChannel(final String channel)
        {
            ingressChannel = channel;
            return this;
        }

        /**
         * Get the channel parameter for the ingress channel.
         *
         * @return the channel parameter for the ingress channel.
         * @see AeronCluster.Configuration#INGRESS_CHANNEL_PROP_NAME
         */
        public String ingressChannel()
        {
            return ingressChannel;
        }

        /**
         * Set the stream id for the ingress channel.
         *
         * @param streamId for the ingress channel.
         * @return this for a fluent API
         * @see AeronCluster.Configuration#INGRESS_STREAM_ID_PROP_NAME
         */
        public Context ingressStreamId(final int streamId)
        {
            ingressStreamId = streamId;
            return this;
        }

        /**
         * Get the stream id for the ingress channel.
         *
         * @return the stream id for the ingress channel.
         * @see AeronCluster.Configuration#INGRESS_STREAM_ID_PROP_NAME
         */
        public int ingressStreamId()
        {
            return ingressStreamId;
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
         * Set the channel parameter for sending messages to the Consensus Module.
         *
         * @param channel parameter for sending messages to the Consensus Module.
         * @return this for a fluent API.
         * @see ClusteredServiceContainer.Configuration#CONSENSUS_MODULE_CHANNEL_PROP_NAME
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
         * @see ClusteredServiceContainer.Configuration#CONSENSUS_MODULE_CHANNEL_PROP_NAME
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
         * @see ClusteredServiceContainer.Configuration#CONSENSUS_MODULE_STREAM_ID_PROP_NAME
         */
        public Context consensusModuleStreamId(final int streamId)
        {
            consensusModuleStreamId = streamId;
            return this;
        }

        /**
         * Get the stream id for sending messages to the Consensus Module.
         *
         * @return the stream id for the scheduling timer events channel.
         * @see ClusteredServiceContainer.Configuration#CONSENSUS_MODULE_STREAM_ID_PROP_NAME
         */
        public int consensusModuleStreamId()
        {
            return consensusModuleStreamId;
        }

        /**
         * Set the number of clustered services in this cluster instance.
         *
         * @param serviceCount the number of clustered services in this cluster instance.
         * @return this for a fluent API
         * @see Configuration#SERVICE_COUNT_PROP_NAME
         */
        public Context serviceCount(final int serviceCount)
        {
            this.serviceCount = serviceCount;
            return this;
        }

        /**
         * Get the number of clustered services in this cluster instance.
         *
         * @return the number of clustered services in this cluster instance.
         * @see Configuration#SERVICE_COUNT_PROP_NAME
         */
        public int serviceCount()
        {
            return serviceCount;
        }

        /**
         * Set the limit for the maximum number of concurrent cluster sessions.
         *
         * @param maxSessions after which new sessions will be rejected.
         * @return this for a fluent API
         * @see Configuration#MAX_CONCURRENT_SESSIONS_PROP_NAME
         */
        public Context maxConcurrentSessions(final int maxSessions)
        {
            this.maxConcurrentSessions = maxSessions;
            return this;
        }

        /**
         * Get the limit for the maximum number of concurrent cluster sessions.
         *
         * @return the limit for the maximum number of concurrent cluster sessions.
         * @see Configuration#MAX_CONCURRENT_SESSIONS_PROP_NAME
         */
        public int maxConcurrentSessions()
        {
            return maxConcurrentSessions;
        }

        /**
         * Timeout for a session if no activity is observed.
         *
         * @param sessionTimeoutNs to wait for activity on a session.
         * @return this for a fluent API.
         * @see Configuration#SESSION_TIMEOUT_PROP_NAME
         */
        public Context sessionTimeoutNs(final long sessionTimeoutNs)
        {
            this.sessionTimeoutNs = sessionTimeoutNs;
            return this;
        }

        /**
         * Timeout for a session if no activity is observed.
         *
         * @return the timeout for a session if no activity is observed.
         * @see Configuration#SESSION_TIMEOUT_PROP_NAME
         */
        public long sessionTimeoutNs()
        {
            return sessionTimeoutNs;
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
         * Set the {@link EpochClock} to be used for tracking wall clock time.
         *
         * @param clock {@link EpochClock} to be used for tracking wall clock time.
         * @return this for a fluent API.
         */
        public Context epochClock(final EpochClock clock)
        {
            this.epochClock = clock;
            return this;
        }

        /**
         * Get the {@link EpochClock} to used for tracking wall clock time.
         *
         * @return the {@link EpochClock} to used for tracking wall clock time.
         */
        public EpochClock epochClock()
        {
            return epochClock;
        }

        /**
         * Set the {@link CachedEpochClock} to be used for tracking wall clock time.
         *
         * @param clock {@link CachedEpochClock} to be used for tracking wall clock time.
         * @return this for a fluent API.
         */
        public Context cachedEpochClock(final CachedEpochClock clock)
        {
            this.cachedEpochClock = clock;
            return this;
        }

        /**
         * Get the {@link CachedEpochClock} to used for tracking wall clock time.
         *
         * @return the {@link CachedEpochClock} to used for tracking wall clock time.
         */
        public CachedEpochClock cachedEpochClock()
        {
            return cachedEpochClock;
        }

        /**
         * Get the {@link ErrorHandler} to be used by the Consensus Module.
         *
         * @return the {@link ErrorHandler} to be used by the Consensus Module.
         */
        public ErrorHandler errorHandler()
        {
            return errorHandler;
        }

        /**
         * Set the {@link ErrorHandler} to be used by the Consensus Module.
         *
         * @param errorHandler the error handler to be used by the Consensus Module.
         * @return this for a fluent API
         */
        public Context errorHandler(final ErrorHandler errorHandler)
        {
            this.errorHandler = errorHandler;
            return this;
        }

        /**
         * Get the error counter that will record the number of errors observed.
         *
         * @return the error counter that will record the number of errors observed.
         */
        public AtomicCounter errorCounter()
        {
            return errorCounter;
        }

        /**
         * Set the error counter that will record the number of errors observed.
         *
         * @param errorCounter the error counter that will record the number of errors observed.
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
         * Get the counter for the message index of the log.
         *
         * @return the counter for the message index of the log.
         */
        public Counter messageIndex()
        {
            return messageIndex;
        }

        /**
         * Set the counter for the message index of the log.
         *
         * @param messageIndex the counter for the message index of the log.
         * @return this for a fluent API.
         */
        public Context messageIndex(final Counter messageIndex)
        {
            this.messageIndex = messageIndex;
            return this;
        }

        /**
         * Get the counter for the current state of the consensus module.
         *
         * @return the counter for the current state of the consensus module.
         * @see State
         */
        public Counter moduleState()
        {
            return moduleState;
        }

        /**
         * Set the counter for the current state of the consensus module.
         *
         * @param moduleState the counter for the current state of the consensus module.t
         * @return this for a fluent API.
         * @see State
         */
        public Context moduleState(final Counter moduleState)
        {
            this.moduleState = moduleState;
            return this;
        }

        /**
         * Get the counter for the control toggle for triggering actions on the cluster node.
         *
         * @return the counter for triggering cluster node actions.
         * @see ClusterControl
         */
        public Counter controlToggle()
        {
            return controlToggle;
        }

        /**
         * Set the counter for the control toggle for triggering actions on the cluster node.
         *
         * @param controlToggle the counter for triggering cluster node actions.
         * @return this for a fluent API.
         * @see ClusterControl
         */
        public Context controlToggle(final Counter controlToggle)
        {
            this.controlToggle = controlToggle;
            return this;
        }

        /**
         * {@link Aeron} client for communicating with the local Media Driver.
         * <p>
         * This client will be closed when the {@link ConsensusModule#close()} or {@link #close()} methods are called
         * if {@link #ownsAeronClient()} is true.
         *
         * @param aeron client for communicating with the local Media Driver.
         * @return this for a fluent API.
         * @see Aeron#connect()
         */
        public Context aeron(final Aeron aeron)
        {
            this.aeron = aeron;
            return this;
        }

        /**
         * {@link Aeron} client for communicating with the local Media Driver.
         * <p>
         * If not provided then a default will be established during {@link #conclude()} by calling
         * {@link Aeron#connect()}.
         *
         * @return client for communicating with the local Media Driver.
         */
        public Aeron aeron()
        {
            return aeron;
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
         * Get the {@link AuthenticatorSupplier} that should be used for the consensus module.
         *
         * @return the {@link AuthenticatorSupplier} to be used for the consensus module.
         */
        public AuthenticatorSupplier authenticatorSupplier()
        {
            return authenticatorSupplier;
        }

        /**
         * Set the {@link AuthenticatorSupplier} that will be used for the consensus module.
         *
         * @param authenticatorSupplier {@link AuthenticatorSupplier} to use for the consensus module.
         * @return this for a fluent API.
         */
        public Context authenticatorSupplier(final AuthenticatorSupplier authenticatorSupplier)
        {
            this.authenticatorSupplier = authenticatorSupplier;
            return this;
        }

        /**
         * Set the {@link ShutdownSignalBarrier} that can be used to shutdown a consensus module.
         *
         * @param barrier that can be used to shutdown a consensus module.
         * @return this for a fluent API.
         */
        public Context shutdownSignalBarrier(final ShutdownSignalBarrier barrier)
        {
            shutdownSignalBarrier = barrier;
            return this;
        }

        /**
         * Get the {@link ShutdownSignalBarrier} that can be used to shutdown a consensus module.
         *
         * @return the {@link ShutdownSignalBarrier} that can be used to shutdown a consensus module.
         */
        public ShutdownSignalBarrier shutdownSignalBarrier()
        {
            return shutdownSignalBarrier;
        }

        /**
         * Delete the cluster directory.
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
            if (ownsAeronClient)
            {
                CloseHelper.close(aeron);
            }
            else
            {
                CloseHelper.close(messageIndex);
                CloseHelper.close(moduleState);
                CloseHelper.close(controlToggle);
            }
        }
    }
}
