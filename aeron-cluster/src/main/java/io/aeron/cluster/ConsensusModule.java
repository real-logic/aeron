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

import io.aeron.*;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.codecs.ClusterAction;
import io.aeron.cluster.service.*;
import org.agrona.*;
import org.agrona.concurrent.*;
import org.agrona.concurrent.status.AtomicCounter;

import java.io.File;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.aeron.cluster.ConsensusModule.Configuration.*;
import static io.aeron.cluster.service.ClusteredServiceContainer.Configuration.SNAPSHOT_CHANNEL_PROP_NAME;
import static io.aeron.cluster.service.ClusteredServiceContainer.Configuration.SNAPSHOT_STREAM_ID_PROP_NAME;
import static io.aeron.driver.status.SystemCounterDescriptor.SYSTEM_COUNTER_TYPE_ID;
import static org.agrona.SystemUtil.getDurationInNanos;
import static org.agrona.concurrent.status.CountersReader.METADATA_LENGTH;

public class ConsensusModule implements AutoCloseable
{
    /**
     * Type of snapshot for this agent.
     */
    public static final long SNAPSHOT_TYPE_ID = 1;

    /**
     * Possible states for the consensus module. These will be reflected in the {@link Context#moduleStateCounter()} counter.
     */
    public enum State
    {
        /**
         * Starting up.
         */
        INIT(0, ClusterAction.INIT),

        /**
         * Active state with ingress and expired timers appended to the log.
         */
        ACTIVE(1, null),

        /**
         * Suspended processing of ingress and expired timers.
         */
        SUSPENDED(2, ClusterAction.SUSPEND),

        /**
         * In the process of taking a snapshot.
         */
        SNAPSHOT(3, ClusterAction.SNAPSHOT),

        /**
         * In the process of doing an orderly shutdown taking a snapshot first.
         */
        SHUTDOWN(4, ClusterAction.SHUTDOWN),

        /**
         * Aborting processing and shutting down as soon as services ack without taking a snapshot.
         */
        ABORT(5, ClusterAction.ABORT),

        /**
         * Terminal state.
         */
        CLOSED(6, null);

        static final State[] STATES;

        static
        {
            final State[] states = values();
            STATES = new State[states.length];
            for (final State state : states)
            {
                final int code = state.code();
                if (null != STATES[code])
                {
                    throw new IllegalStateException("Code already in use: " + code);
                }

                STATES[code] = state;
            }
        }

        private final int code;
        private final ClusterAction validClusterAction;

        State(final int code, final ClusterAction clusterAction)
        {
            this.code = code;
            this.validClusterAction = clusterAction;
        }

        public final int code()
        {
            return code;
        }

        /**
         * Is the {@link ClusterAction} valid for the current state?
         *
         * @param action to check if valid.
         * @return true if the action is valid for the current state otherwise false.
         */
        public final boolean isValid(final ClusterAction action)
        {
            return action == validClusterAction;
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
            final long code = counter.get();

            if (code < 0 || code > (STATES.length - 1))
            {
                throw new IllegalStateException("Invalid state counter code: " + code);
            }

            return STATES[(int)code];
        }
    }

    private final Context ctx;
    private final AgentRunner conductorRunner;

    ConsensusModule(final Context ctx)
    {
        this.ctx = ctx;
        ctx.conclude();

        final SequencerAgent conductor = new SequencerAgent(ctx, new EgressPublisher(), new LogAppender());

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
        CloseHelper.close(ctx);
    }

    /**
     * Configuration options for cluster.
     */
    public static class Configuration
    {
        static final int TIMER_POLL_LIMIT = 10;

        /**
         * Property name for the identity of the cluster member.
         */
        public static final String CLUSTER_MEMBER_ID_PROP_NAME = "aeron.cluster.member.id";

        /**
         * Default property for the cluster member identity.
         */
        public static final int CLUSTER_MEMBER_ID_DEFAULT = 0;

        /**
         * Property name for the identity of the appointed leader. This is when automated leader elections are
         * not employed.
         */
        public static final String APPOINTED_LEADER_ID_PROP_NAME = "aeron.cluster.appointed.leader.id";

        /**
         * Default property for the appointed cluster leader id. A value of -1 means no leader has been appointed
         * and thus an automated leader election should occur.
         */
        public static final int APPOINTED_LEADER_ID_DEFAULT = -1;

        /**
         * Property name for the comma separated list of cluster member endpoints.
         */
        public static final String CLUSTER_MEMBERS_PROP_NAME = "aeron.cluster.members";

        /**
         * Default property for the list of cluster member endpoints.
         */
        public static final String CLUSTER_MEMBERS_DEFAULT = "0,localhost:9010,localhost:8001,localhost:7001";

        /**
         * Channel to be used for archiving snapshots.
         */
        public static final String SNAPSHOT_CHANNEL_DEFAULT = CommonContext.IPC_CHANNEL;

        /**
         * Stream id for the archived snapshots within a channel.
         */
        public static final int SNAPSHOT_STREAM_ID_DEFAULT = 6;

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
         * Channel to be used communicating cluster member status to each other. This can be used for default
         * configuration with the endpoints replaced with those provided by {@link #CLUSTER_MEMBERS_PROP_NAME}.
         */
        public static final String MEMBER_STATUS_CHANNEL_PROP_NAME = "aeron.cluster.member.status.channel";

        /**
         * Channel to be used for communicating cluster member status to each other. This can be used for default
         * configuration with the endpoints replaced with those provided by {@link #CLUSTER_MEMBERS_PROP_NAME}.
         */
        public static final String MEMBER_STATUS_CHANNEL_DEFAULT = "aeron:udp?term-length=64k";

        /**
         * Stream id within a channel for communicating cluster member status.
         */
        public static final String MEMBER_STATUS_STREAM_ID_PROP_NAME = "aeron.cluster.member.status.stream.id";

        /**
         * Stream id for the archived snapshots within a channel.
         */
        public static final int MEMBER_STATUS_STREAM_ID_DEFAULT = 8;

        /**
         * Counter type id for the consensus module state.
         */
        public static final int CONSENSUS_MODULE_STATE_TYPE_ID = 200;

        /**
         * Counter type id for the cluster node role.
         */
        public static final int CLUSTER_NODE_ROLE_TYPE_ID = ClusterNodeRole.CLUSTER_NODE_ROLE_TYPE_ID;

        /**
         * Counter type id for the control toggle.
         */
        public static final int CONTROL_TOGGLE_TYPE_ID = ClusterControl.CONTROL_TOGGLE_TYPE_ID;

        /**
         * Type id of a commit position counter.
         */
        public static final int COMMIT_POSITION_TYPE_ID = CommitPos.COMMIT_POSITION_TYPE_ID;

        /**
         * Type id of a recovery state counter.
         */
        public static final int RECOVERY_STATE_TYPE_ID = RecoveryState.RECOVERY_STATE_TYPE_ID;

        /**
         * Counter type id for count of snapshots taken.
         */
        public static final int SNAPSHOT_COUNTER_TYPE_ID = 205;

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
         * Timeout for a leader if no heartbeat is received by an other member.
         */
        public static final String HEARTBEAT_TIMEOUT_PROP_NAME = "aeron.cluster.heartbeat.timeout";

        /**
         * Timeout for a leader if no heartbeat is received by an other member. Default to 10 seconds in nanoseconds.
         */
        public static final long HEARTBEAT_TIMEOUT_DEFAULT_NS = TimeUnit.SECONDS.toNanos(10);

        /**
         * Interval at which a leader will send heartbeats if the log is not progressing.
         */
        public static final String HEARTBEAT_INTERVAL_PROP_NAME = "aeron.cluster.heartbeat.interval";

        /**
         * Interval at which a leader will send heartbeats if the log is not progressing.
         * Default to 500 milliseconds in nanoseconds.
         */
        public static final long HEARTBEAT_INTERVAL_DEFAULT_NS = TimeUnit.MILLISECONDS.toNanos(500);

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
         * The value {@link #CLUSTER_MEMBER_ID_DEFAULT} or system property
         * {@link #CLUSTER_MEMBER_ID_PROP_NAME} if set.
         *
         * @return {@link #CLUSTER_MEMBER_ID_DEFAULT} or system property
         * {@link #CLUSTER_MEMBER_ID_PROP_NAME} if set.
         */
        public static int clusterMemberId()
        {
            return Integer.getInteger(CLUSTER_MEMBER_ID_PROP_NAME, CLUSTER_MEMBER_ID_DEFAULT);
        }

        /**
         * The value {@link #APPOINTED_LEADER_ID_DEFAULT} or system property
         * {@link #APPOINTED_LEADER_ID_PROP_NAME} if set.
         *
         * @return {@link #APPOINTED_LEADER_ID_DEFAULT} or system property
         * {@link #APPOINTED_LEADER_ID_PROP_NAME} if set.
         */
        public static int appointedLeaderId()
        {
            return Integer.getInteger(APPOINTED_LEADER_ID_PROP_NAME, APPOINTED_LEADER_ID_DEFAULT);
        }

        /**
         * The value {@link #CLUSTER_MEMBERS_DEFAULT} or system property
         * {@link #CLUSTER_MEMBERS_PROP_NAME} if set.
         *
         * @return {@link #CLUSTER_MEMBERS_DEFAULT} or system property
         * {@link #CLUSTER_MEMBERS_PROP_NAME} if set.
         */
        public static String clusterMembers()
        {
            return System.getProperty(CLUSTER_MEMBERS_PROP_NAME, CLUSTER_MEMBERS_DEFAULT);
        }

        /**
         * The value {@link #SNAPSHOT_CHANNEL_DEFAULT} or system property
         * {@link ClusteredServiceContainer.Configuration#SNAPSHOT_CHANNEL_PROP_NAME} if set.
         *
         * @return {@link #SNAPSHOT_CHANNEL_DEFAULT} or system property
         * {@link ClusteredServiceContainer.Configuration#SNAPSHOT_CHANNEL_PROP_NAME} if set.
         */
        public static String snapshotChannel()
        {
            return System.getProperty(SNAPSHOT_CHANNEL_PROP_NAME, SNAPSHOT_CHANNEL_DEFAULT);
        }

        /**
         * The value {@link #SNAPSHOT_STREAM_ID_DEFAULT} or system property
         * {@link ClusteredServiceContainer.Configuration#SNAPSHOT_STREAM_ID_PROP_NAME} if set.
         *
         * @return {@link #SNAPSHOT_STREAM_ID_DEFAULT} or system property
         * {@link ClusteredServiceContainer.Configuration#SNAPSHOT_STREAM_ID_PROP_NAME} if set.
         */
        public static int snapshotStreamId()
        {
            return Integer.getInteger(SNAPSHOT_STREAM_ID_PROP_NAME, SNAPSHOT_STREAM_ID_DEFAULT);
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
         * Timeout for a leader if no heartbeat is received by an other member.
         *
         * @return timeout in nanoseconds to wait for heartbeat from a leader.
         * @see #HEARTBEAT_TIMEOUT_PROP_NAME
         */
        public static long leaderHeartbeatTimeoutNs()
        {
            return getDurationInNanos(HEARTBEAT_TIMEOUT_PROP_NAME, HEARTBEAT_TIMEOUT_DEFAULT_NS);
        }

        /**
         * Interval at which a leader will send a heartbeat if the log is not progressing.
         *
         * @return timeout in nanoseconds to for leader heartbeats when no log being appended.
         * @see #HEARTBEAT_INTERVAL_PROP_NAME
         */
        public static long leaderHeartbeatIntervalNs()
        {
            return getDurationInNanos(HEARTBEAT_INTERVAL_PROP_NAME, HEARTBEAT_INTERVAL_DEFAULT_NS);
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
            final String supplierClassName = System.getProperty(
                AUTHENTICATOR_SUPPLIER_PROP_NAME, AUTHENTICATOR_SUPPLIER_DEFAULT);

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

        /**
         * The value {@link #MEMBER_STATUS_CHANNEL_DEFAULT} or system property
         * {@link #MEMBER_STATUS_CHANNEL_PROP_NAME} if set.
         *
         * @return {@link #MEMBER_STATUS_CHANNEL_DEFAULT} or system property
         * {@link #MEMBER_STATUS_CHANNEL_PROP_NAME} if set.
         */
        public static String memberStatusChannel()
        {
            return System.getProperty(MEMBER_STATUS_CHANNEL_PROP_NAME, MEMBER_STATUS_CHANNEL_DEFAULT);
        }

        /**
         * The value {@link #MEMBER_STATUS_STREAM_ID_DEFAULT} or system property
         * {@link #MEMBER_STATUS_STREAM_ID_PROP_NAME} if set.
         *
         * @return {@link #MEMBER_STATUS_STREAM_ID_DEFAULT} or system property
         * {@link #MEMBER_STATUS_STREAM_ID_PROP_NAME} if set.
         */
        public static int memberStatusStreamId()
        {
            return Integer.getInteger(MEMBER_STATUS_STREAM_ID_PROP_NAME, MEMBER_STATUS_STREAM_ID_DEFAULT);
        }
    }

    public static class Context implements AutoCloseable
    {
        private boolean ownsAeronClient = false;
        private Aeron aeron;

        private boolean deleteDirOnStart = false;
        private File clusterDir;
        private RecordingLog recordingLog;

        private int clusterMemberId = Configuration.clusterMemberId();
        private int appointedLeaderId = Configuration.appointedLeaderId();
        private String clusterMembers = Configuration.clusterMembers();
        private String ingressChannel = AeronCluster.Configuration.ingressChannel();
        private int ingressStreamId = AeronCluster.Configuration.ingressStreamId();
        private String logChannel = ClusteredServiceContainer.Configuration.logChannel();
        private int logStreamId = ClusteredServiceContainer.Configuration.logStreamId();
        private String replayChannel = ClusteredServiceContainer.Configuration.replayChannel();
        private int replayStreamId = ClusteredServiceContainer.Configuration.replayStreamId();
        private String consensusModuleChannel = ClusteredServiceContainer.Configuration.consensusModuleChannel();
        private int consensusModuleStreamId = ClusteredServiceContainer.Configuration.consensusModuleStreamId();
        private String snapshotChannel = Configuration.snapshotChannel();
        private int snapshotStreamId = Configuration.snapshotStreamId();
        private String memberStatusChannel = Configuration.memberStatusChannel();
        private int memberStatusStreamId = Configuration.memberStatusStreamId();

        private int serviceCount = Configuration.serviceCount();
        private int maxConcurrentSessions = Configuration.maxConcurrentSessions();
        private long sessionTimeoutNs = Configuration.sessionTimeoutNs();
        private long heartbeatTimeoutNs = Configuration.leaderHeartbeatTimeoutNs();
        private long heartbeatIntervalNs = Configuration.leaderHeartbeatIntervalNs();

        private ThreadFactory threadFactory;
        private Supplier<IdleStrategy> idleStrategySupplier;
        private EpochClock epochClock;
        private CachedEpochClock cachedEpochClock;
        private MutableDirectBuffer tempBuffer = new UnsafeBuffer(new byte[METADATA_LENGTH]);

        private ErrorHandler errorHandler;
        private AtomicCounter errorCounter;
        private CountedErrorHandler countedErrorHandler;

        private Counter moduleState;
        private Counter clusterNodeRole;
        private Counter controlToggle;
        private Counter snapshotCounter;
        private Counter invalidRequestCounter;
        private ShutdownSignalBarrier shutdownSignalBarrier;
        private Runnable terminationHook;

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

            if (null == recordingLog)
            {
                recordingLog = new RecordingLog(clusterDir);
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

            if (null == moduleState)
            {
                moduleState = aeron.addCounter(CONSENSUS_MODULE_STATE_TYPE_ID, "Consensus module state");
            }

            if (null == clusterNodeRole)
            {
                clusterNodeRole = aeron.addCounter(Configuration.CLUSTER_NODE_ROLE_TYPE_ID, "Cluster node role");
            }

            if (null == controlToggle)
            {
                controlToggle = aeron.addCounter(CONTROL_TOGGLE_TYPE_ID, "Cluster control toggle");
            }

            if (null == snapshotCounter)
            {
                snapshotCounter = aeron.addCounter(SNAPSHOT_COUNTER_TYPE_ID, "Snapshot count");
            }

            if (null == invalidRequestCounter)
            {
                invalidRequestCounter = aeron.addCounter(SYSTEM_COUNTER_TYPE_ID, "Invalid cluster request count");
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
                archiveContext = new AeronArchive.Context()
                    .aeron(aeron)
                    .ownsAeronClient(false)
                    .controlRequestChannel(AeronArchive.Configuration.localControlChannel())
                    .controlRequestStreamId(AeronArchive.Configuration.localControlStreamId())
                    .lock(new NoOpLock());
            }

            if (null == shutdownSignalBarrier)
            {
                shutdownSignalBarrier = new ShutdownSignalBarrier();
            }

            if (null == terminationHook)
            {
                terminationHook = () -> shutdownSignalBarrier.signal();
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
         * Set the {@link RecordingLog} for the log terms and snapshots.
         *
         * @param recordingLog to use.
         * @return this for a fluent API.
         */
        public Context recordingLog(final RecordingLog recordingLog)
        {
            this.recordingLog = recordingLog;
            return this;
        }

        /**
         * The {@link RecordingLog} for the log terms and snapshots.
         *
         * @return {@link RecordingLog} for the  log terms and snapshots.
         */
        public RecordingLog recordingLog()
        {
            return recordingLog;
        }

        /**
         * This cluster member identity.
         *
         * @param clusterMemberId for this member.
         * @return this for a fluent API.
         * @see Configuration#CLUSTER_MEMBER_ID_PROP_NAME
         */
        public Context clusterMemberId(final int clusterMemberId)
        {
            this.clusterMemberId = clusterMemberId;
            return this;
        }

        /**
         * This cluster member identity.
         *
         * @return this cluster member identity.
         * @see Configuration#CLUSTER_MEMBER_ID_PROP_NAME
         */
        public int clusterMemberId()
        {
            return clusterMemberId;
        }

        /**
         * The cluster member id of the appointed cluster leader.
         * <p>
         * -1 means no leader has been appointed and an automated leader election should occur.
         *
         * @param appointedLeaderId for the cluster.
         * @return this for a fluent API.
         * @see Configuration#APPOINTED_LEADER_ID_PROP_NAME
         */
        public Context appointedLeaderId(final int appointedLeaderId)
        {
            this.appointedLeaderId = appointedLeaderId;
            return this;
        }

        /**
         * The cluster member id of the appointed cluster leader.
         * <p>
         * -1 means no leader has been appointed and an automated leader election should occur.
         *
         * @return cluster member id of the appointed cluster leader.
         * @see Configuration#APPOINTED_LEADER_ID_PROP_NAME
         */
        public int appointedLeaderId()
        {
            return appointedLeaderId;
        }

        /**
         * String representing the cluster members.
         * <p>
         * <code>
         *     0,client-facing:port,member-facing:port,log:port|1,client-facing:port,member-facing:port,log:port| ...
         * </code>
         * <p>
         * The client facing endpoints will be used as the endpoint in {@link #ingressChannel()} if the endpoint is
         * not provided in that when it is not multicast.
         *
         * @param clusterMembers which are all candidates to be leader.
         * @return this for a fluent API.
         * @see Configuration#CLUSTER_MEMBERS_PROP_NAME
         */
        public Context clusterMembers(final String clusterMembers)
        {
            this.clusterMembers = clusterMembers;
            return this;
        }

        /**
         * The endpoints representing members of the cluster which are all candidates to be leader.
         * <p>
         * The client facing endpoints will be used as the endpoint in {@link #ingressChannel()} if the endpoint is
         * not provided in that when it is not multicast.
         *
         * @return members of the cluster which are all candidates to be leader.
         * @see Configuration#CLUSTER_MEMBERS_PROP_NAME
         */
        public String clusterMembers()
        {
            return clusterMembers;
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
         * Set the channel parameter for snapshot recordings.
         *
         * @param channel parameter for snapshot recordings
         * @return this for a fluent API.
         * @see ClusteredServiceContainer.Configuration#SNAPSHOT_CHANNEL_PROP_NAME
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
         * @see ClusteredServiceContainer.Configuration#SNAPSHOT_CHANNEL_PROP_NAME
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
         * @see ClusteredServiceContainer.Configuration#SNAPSHOT_STREAM_ID_PROP_NAME
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
         * @see ClusteredServiceContainer.Configuration#SNAPSHOT_STREAM_ID_PROP_NAME
         */
        public int snapshotStreamId()
        {
            return snapshotStreamId;
        }

        /**
         * Set the channel parameter for the member status communication channel.
         *
         * @param channel parameter for the member status communication channel.
         * @return this for a fluent API.
         * @see Configuration#MEMBER_STATUS_CHANNEL_PROP_NAME
         */
        public Context memberStatusChannel(final String channel)
        {
            memberStatusChannel = channel;
            return this;
        }

        /**
         * Get the channel parameter for the member status communication channel.
         *
         * @return the channel parameter for the member status communication channel.
         * @see Configuration#MEMBER_STATUS_CHANNEL_PROP_NAME
         */
        public String memberStatusChannel()
        {
            return memberStatusChannel;
        }

        /**
         * Set the stream id for the member status channel.
         *
         * @param streamId for the ingress channel.
         * @return this for a fluent API
         * @see Configuration#MEMBER_STATUS_STREAM_ID_PROP_NAME
         */
        public Context memberStatusStreamId(final int streamId)
        {
            memberStatusStreamId = streamId;
            return this;
        }

        /**
         * Get the stream id for the member status channel.
         *
         * @return the stream id for the member status channel.
         * @see Configuration#MEMBER_STATUS_STREAM_ID_PROP_NAME
         */
        public int memberStatusStreamId()
        {
            return memberStatusStreamId;
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
         * Timeout for a leader if no heartbeat is received by an other member.
         *
         * @param heartbeatTimeoutNs to wait for heartbeat from a leader.
         * @return this for a fluent API.
         * @see Configuration#HEARTBEAT_TIMEOUT_PROP_NAME
         */
        public Context heartbeatTimeoutNs(final long heartbeatTimeoutNs)
        {
            this.heartbeatTimeoutNs = heartbeatTimeoutNs;
            return this;
        }

        /**
         * Timeout for a leader if no heartbeat is received by an other member.
         *
         * @return the timeout for a leader if no heartbeat is received by an other member.
         * @see Configuration#HEARTBEAT_TIMEOUT_PROP_NAME
         */
        public long heartbeatTimeoutNs()
        {
            return heartbeatTimeoutNs;
        }

        /**
         * Interval at which a leader will send heartbeats if the log is not progressing.
         *
         * @param heartbeatIntervalNs between leader heartbeats.
         * @return this for a fluent API.
         * @see Configuration#HEARTBEAT_INTERVAL_PROP_NAME
         */
        public Context heartbeatIntervalNs(final long heartbeatIntervalNs)
        {
            this.heartbeatIntervalNs = heartbeatIntervalNs;
            return this;
        }

        /**
         * Interval at which a leader will send heartbeats if the log is not progressing.
         *
         * @return the interval at which a leader will send heartbeats if the log is not progressing.
         * @see Configuration#HEARTBEAT_INTERVAL_PROP_NAME
         */
        public long heartbeatIntervalNs()
        {
            return heartbeatIntervalNs;
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
         * The temporary buffer than can be used to build up counter labels to avoid allocation.
         *
         * @return the temporary buffer than can be used to build up counter labels to avoid allocation.
         */
        public MutableDirectBuffer tempBuffer()
        {
            return tempBuffer;
        }

        /**
         * Set the temporary buffer than can be used to build up counter labels to avoid allocation.
         *
         * @param tempBuffer to be used to avoid allocation.
         * @return the temporary buffer than can be used to build up counter labels to avoid allocation.
         */
        public Context tempBuffer(final MutableDirectBuffer tempBuffer)
        {
            this.tempBuffer = tempBuffer;
            return this;
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
         * Get the counter for the current state of the consensus module.
         *
         * @return the counter for the current state of the consensus module.
         * @see State
         */
        public Counter moduleStateCounter()
        {
            return moduleState;
        }

        /**
         * Set the counter for the current state of the consensus module.
         *
         * @param moduleState the counter for the current state of the consensus module.
         * @return this for a fluent API.
         * @see State
         */
        public Context moduleStateCounter(final Counter moduleState)
        {
            this.moduleState = moduleState;
            return this;
        }

        /**
         * Get the counter for representing the current {@link Cluster.Role} of the consensus module node.
         *
         * @return the counter for representing the current {@link Cluster.Role} of the cluster node.
         * @see Cluster.Role
         */
        public Counter clusterNodeCounter()
        {
            return clusterNodeRole;
        }

        /**
         * Set the counter for representing the current {@link Cluster.Role} of the cluster node.
         *
         * @param moduleRole the counter for representing the current {@link Cluster.Role} of the cluster node.
         * @return this for a fluent API.
         * @see Cluster.Role
         */
        public Context clusterNodeCounter(final Counter moduleRole)
        {
            this.clusterNodeRole = moduleRole;
            return this;
        }

        /**
         * Get the counter for the control toggle for triggering actions on the cluster node.
         *
         * @return the counter for triggering cluster node actions.
         * @see ClusterControl
         */
        public Counter controlToggleCounter()
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
        public Context controlToggleCounter(final Counter controlToggle)
        {
            this.controlToggle = controlToggle;
            return this;
        }

        /**
         * Get the counter for the count of snapshots taken.
         *
         * @return the counter for the count of snapshots taken.
         */
        public Counter snapshotCounter()
        {
            return snapshotCounter;
        }

        /**
         * Set the counter for the count of snapshots taken.
         *
         * @param snapshotCounter the count of snapshots taken.
         * @return this for a fluent API.
         */
        public Context snapshotCounter(final Counter snapshotCounter)
        {
            this.snapshotCounter = snapshotCounter;
            return this;
        }

        /**
         * Get the counter for the count of invalid client requests.
         *
         * @return the counter for the count of invalid client requests.
         */
        public Counter invalidRequestCounter()
        {
            return invalidRequestCounter;
        }

        /**
         * Set the counter for the count of invalid client requests.
         *
         * @param invalidRequestCounter the count of invalid client requests.
         * @return this for a fluent API.
         */
        public Context invalidRequestCounter(final Counter invalidRequestCounter)
        {
            this.invalidRequestCounter = invalidRequestCounter;
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
         * Set the {@link Runnable} that is called when the {@link ConsensusModule} processes a termination action such
         * as a {@link ConsensusModule.State#SHUTDOWN} or {@link ConsensusModule.State#ABORT}.
         *
         * @param terminationHook that can be used to terminate a consensus module.
         * @return this for a fluent API.
         */
        public Context terminationHook(final Runnable terminationHook)
        {
            this.terminationHook = terminationHook;
            return this;
        }

        /**
         * Get the {@link Runnable} that is called when the {@link ConsensusModule} processes a termination action such
         * as a {@link ConsensusModule.State#SHUTDOWN} or {@link ConsensusModule.State#ABORT}.
         * <p>
         * The default action is to call signal on the {@link #shutdownSignalBarrier()}.
         *
         * @return the {@link Runnable} that can be used to terminate a consensus module.
         */
        public Runnable terminationHook()
        {
            return terminationHook;
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
                CloseHelper.close(moduleState);
                CloseHelper.close(clusterNodeRole);
                CloseHelper.close(controlToggle);
                CloseHelper.close(snapshotCounter);
            }
        }
    }
}
