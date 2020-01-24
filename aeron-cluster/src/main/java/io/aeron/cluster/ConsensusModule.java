/*
 * Copyright 2014-2020 Real Logic Limited.
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
package io.aeron.cluster;

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.Counter;
import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.ClusterClock;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.mark.ClusterComponentType;
import io.aeron.cluster.service.*;
import io.aeron.exceptions.ConcurrentConcludeException;
import io.aeron.security.Authenticator;
import io.aeron.security.AuthenticatorSupplier;
import org.agrona.*;
import org.agrona.concurrent.*;
import org.agrona.concurrent.errors.DistinctErrorLog;
import org.agrona.concurrent.errors.LoggingErrorHandler;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.util.Random;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Supplier;

import static io.aeron.cluster.ConsensusModule.Configuration.*;
import static io.aeron.cluster.service.ClusteredServiceContainer.Configuration.SNAPSHOT_CHANNEL_PROP_NAME;
import static io.aeron.cluster.service.ClusteredServiceContainer.Configuration.SNAPSHOT_STREAM_ID_PROP_NAME;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;
import static org.agrona.SystemUtil.*;
import static org.agrona.concurrent.status.CountersReader.*;

/**
 * Component which resides on each node and is responsible for coordinating consensus within a cluster in concert
 * with the lifecycle of clustered services.
 */
@SuppressWarnings("unused")
public class ConsensusModule implements AutoCloseable
{
    /**
     * Possible states for the {@link ConsensusModule}.
     * These will be reflected in the {@link Context#moduleStateCounter()} counter.
     */
    public enum State
    {
        /**
         * Starting up and recovering state.
         */
        INIT(0),

        /**
         * Active state with ingress and expired timers appended to the log.
         */
        ACTIVE(1),

        /**
         * Suspended processing of ingress and expired timers.
         */
        SUSPENDED(2),

        /**
         * In the process of taking a snapshot.
         */
        SNAPSHOT(3),

        /**
         * Quitting the cluster and shutting down as soon as services ack without taking a snapshot.
         */
        QUITTING(4),

        /**
         * In the process of terminating the node.
         */
        TERMINATING(5),

        /**
         * Terminal state.
         */
        CLOSED(6);

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
                    throw new ClusterException("code already in use: " + code);
                }

                STATES[code] = state;
            }
        }

        private final int code;

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
         * @throws ClusterException if the counter is not one of the valid values.
         */
        public static State get(final AtomicCounter counter)
        {
            final long code = counter.get();
            return get((int)code);
        }

        /**
         * Get the {@link State} corresponding to a particular code.
         *
         * @param code representing a {@link State}.
         * @return the {@link State} corresponding to the provided code.
         * @throws ClusterException if the code does not correspond to a valid State.
         */
        public static State get(final int code)
        {
            if (code < 0 || code > (STATES.length - 1))
            {
                throw new ClusterException("invalid state counter code: " + code);
            }

            return STATES[code];
        }
    }

    /**
     * Get the current state of the {@link ConsensusModule}.
     *
     * @param counters to search for the control toggle.
     * @return the state of the ConsensusModule or null if not found.
     */
    public static State findState(final CountersReader counters)
    {
        final AtomicBuffer buffer = counters.metaDataBuffer();

        for (int i = 0, size = counters.maxCounterId(); i < size; i++)
        {
            final int recordOffset = CountersReader.metaDataOffset(i);

            if (counters.getCounterState(i) == RECORD_ALLOCATED &&
                buffer.getInt(recordOffset + TYPE_ID_OFFSET) == CONSENSUS_MODULE_STATE_TYPE_ID)
            {
                final AtomicCounter atomicCounter = new AtomicCounter(counters.valuesBuffer(), i, null);
                return State.get(atomicCounter);
            }
        }

        return null;
    }

    /**
     * Launch an {@link ConsensusModule} with that communicates with an out of process {@link io.aeron.archive.Archive}
     * and {@link io.aeron.driver.MediaDriver} then awaits shutdown signal.
     *
     * @param args command line argument which is a list for properties files as URLs or filenames.
     */
    public static void main(final String[] args)
    {
        loadPropertiesFiles(args);

        try (ConsensusModule consensusModule = launch())
        {
            consensusModule.context().shutdownSignalBarrier().await();
            System.out.println("Shutdown ConsensusModule...");
        }
    }

    private final Context ctx;
    private final AgentRunner conductorRunner;

    ConsensusModule(final Context ctx)
    {
        this.ctx = ctx;

        try
        {
            ctx.conclude();
        }
        catch (final Throwable ex)
        {
            if (null != ctx.markFile)
            {
                ctx.markFile.signalFailedStart();
            }

            ctx.close();
            throw ex;
        }

        final ConsensusModuleAgent conductor = new ConsensusModuleAgent(ctx);
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
    }

    /**
     * Configuration options for cluster.
     */
    public static class Configuration
    {
        /**
         * Property name for the limit for fragments to be consumed on each poll of ingress.
         */
        public static final String CLUSTER_INGRESS_FRAGMENT_LIMIT_PROP_NAME = "aeron.cluster.ingress.fragment.limit";

        /**
         * Default for the limit for fragments to be consumed on each poll of ingress.
         */
        public static final int CLUSTER_INGRESS_FRAGMENT_LIMIT_DEFAULT = 50;

        /**
         * Type of snapshot for this component.
         */
        public static final long SNAPSHOT_TYPE_ID = 1;

        /**
         * Service ID to identify a snapshot in the {@link RecordingLog}.
         */
        public static final int SERVICE_ID = Aeron.NULL_VALUE;

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
         * Default property for the appointed cluster leader id. A value of {@link Aeron#NULL_VALUE} means no leader
         * has been appointed and thus an automated leader election should occur.
         */
        public static final int APPOINTED_LEADER_ID_DEFAULT = Aeron.NULL_VALUE;

        /**
         * Property name for the comma separated list of cluster member endpoints.
         * <p>
         * <code>
         *     0,client-facing:port,member-facing:port,log:port,transfer:port,archive:port| \
         *     1,client-facing:port,member-facing:port,log:port,transfer:port,archive:port| ...
         * </code>
         * <p>
         * The client facing endpoints will be used as the endpoint substituted into the
         * {@link io.aeron.cluster.client.AeronCluster.Configuration#INGRESS_CHANNEL_PROP_NAME} if the endpoint
         * is not provided when unicast.
         */
        public static final String CLUSTER_MEMBERS_PROP_NAME = "aeron.cluster.members";

        /**
         * Default property for the list of cluster member endpoints.
         */
        public static final String CLUSTER_MEMBERS_DEFAULT =
            "0,localhost:10000,localhost:20000,localhost:30000,localhost:40000,localhost:8010";

        /**
         * Property name for the comma separated list of cluster member status endpoints used for adding passive
         * followers as well as dynamic join of a cluster.
         */
        public static final String CLUSTER_MEMBERS_STATUS_ENDPOINTS_PROP_NAME =
            "aeron.cluster.members.status.endpoints";

        /**
         * Default property for the list of cluster member status endpoints.
         */
        public static final String CLUSTER_MEMBERS_STATUS_ENDPOINTS_DEFAULT = "";

        /**
         * Property name for whether cluster member information in snapshots should be ignored on load or not.
         */
        public static final String CLUSTER_MEMBERS_IGNORE_SNAPSHOT_PROP_NAME = "aeron.cluster.members.ignore.snapshot";

        /**
         * Default property for whether cluster member information in snapshots should be ignored or not.
         */
        public static final String CLUSTER_MEMBERS_IGNORE_SNAPSHOT_DEFAULT = "false";

        /**
         * Channel for the clustered log.
         */
        public static final String LOG_CHANNEL_PROP_NAME = "aeron.cluster.log.channel";

        /**
         * Channel for the clustered log.
         */
        public static final String LOG_CHANNEL_DEFAULT = "aeron:udp?endpoint=localhost:9030|group=true";

        /**
         * Property name for the comma separated list of member endpoints.
         * <p>
         * <code>
         *     client-facing:port,member-facing:port,log:port,transfer:port,archive:port
         * </code>
         * @see #CLUSTER_MEMBERS_PROP_NAME
         */
        public static final String MEMBER_ENDPOINTS_PROP_NAME = "aeron.cluster.member.endpoints";

        /**
         * Default property for member endpoints.
         */
        public static final String MEMBER_ENDPOINTS_DEFAULT = "";

        /**
         * Stream id within a channel for the clustered log.
         */
        public static final String LOG_STREAM_ID_PROP_NAME = "aeron.cluster.log.stream.id";

        /**
         * Stream id within a channel for the clustered log.
         */
        public static final int LOG_STREAM_ID_DEFAULT = 100;

        /**
         * Channel to be used for archiving snapshots.
         */
        public static final String SNAPSHOT_CHANNEL_DEFAULT = CommonContext.IPC_CHANNEL + "?alias=snapshot";

        /**
         * Stream id for the archived snapshots within a channel.
         */
        public static final int SNAPSHOT_STREAM_ID_DEFAULT = 107;

        /**
         * Message detail to be sent when max concurrent session limit is reached.
         */
        public static final String SESSION_LIMIT_MSG = "concurrent session limit";

        /**
         * Message detail to be sent when a session timeout occurs.
         */
        public static final String SESSION_TIMEOUT_MSG = "session inactive";

        /**
         * Message detail to be sent when a session is terminated by a service.
         */
        public static final String SESSION_TERMINATED_MSG = "session terminated";

        /**
         * Message detail to be sent when a session is rejected due to authentication.
         */
        public static final String SESSION_REJECTED_MSG = "session failed authentication";

        /**
         * Message detail to be sent when a session has an invalid client version.
         */
        public static final String SESSION_INVALID_VERSION_MSG = "invalid client version";

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
        public static final int MEMBER_STATUS_STREAM_ID_DEFAULT = 108;

        /**
         * Counter type id for the consensus module state.
         */
        public static final int CONSENSUS_MODULE_STATE_TYPE_ID = 200;

        /**
         * Counter type id for the consensus module error count.
         */
        public static final int CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID = 212;

        /**
         * Counter type id for the number of cluster clients which have been timed out.
         */
        public static final int CLUSTER_CLIENT_TIMEOUT_COUNT_TYPE_ID = 213;

        /**
         * Counter type id for the number of invalid requests which the cluster has received.
         */
        public static final int CLUSTER_INVALID_REQUEST_COUNT_TYPE_ID = 214;

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
         * Type id for election state counter.
         */
        public static final int ELECTION_STATE_TYPE_ID = Election.ELECTION_STATE_TYPE_ID;

        /**
         * The number of services in this cluster instance.
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#SERVICE_ID_PROP_NAME
         */
        public static final String SERVICE_COUNT_PROP_NAME = "aeron.cluster.service.count";

        /**
         * The number of services in this cluster instance.
         */
        public static final int SERVICE_COUNT_DEFAULT = 1;

        /**
         * Maximum number of cluster sessions that can be active concurrently.
         */
        public static final String MAX_CONCURRENT_SESSIONS_PROP_NAME = "aeron.cluster.max.sessions";

        /**
         * Maximum number of cluster sessions that can be active concurrently.
         */
        public static final int MAX_CONCURRENT_SESSIONS_DEFAULT = 10;

        /**
         * Timeout for a session if no activity is observed.
         */
        public static final String SESSION_TIMEOUT_PROP_NAME = "aeron.cluster.session.timeout";

        /**
         * Timeout for a session if no activity is observed.
         */
        public static final long SESSION_TIMEOUT_DEFAULT_NS = TimeUnit.SECONDS.toNanos(5);

        /**
         * Timeout for a leader if no heartbeat is received by an other member.
         */
        public static final String LEADER_HEARTBEAT_TIMEOUT_PROP_NAME = "aeron.cluster.leader.heartbeat.timeout";

        /**
         * Timeout for a leader if no heartbeat is received by an other member.
         */
        public static final long LEADER_HEARTBEAT_TIMEOUT_DEFAULT_NS = TimeUnit.SECONDS.toNanos(10);

        /**
         * Interval at which a leader will send heartbeats if the log is not progressing.
         */
        public static final String LEADER_HEARTBEAT_INTERVAL_PROP_NAME = "aeron.cluster.leader.heartbeat.interval";

        /**
         * Interval at which a leader will send heartbeats if the log is not progressing.
         */
        public static final long LEADER_HEARTBEAT_INTERVAL_DEFAULT_NS = TimeUnit.MILLISECONDS.toNanos(200);

        /**
         * Timeout after which an election vote will be attempted after startup while waiting to canvass the status
         * of members if a majority has been heard from.
         */
        public static final String STARTUP_CANVASS_TIMEOUT_PROP_NAME = "aeron.cluster.startup.canvass.timeout";

        /**
         * Default timeout after which an election vote will be attempted on startup when waiting to canvass the
         * status of all members before going for a majority if possible.
         */
        public static final long STARTUP_CANVASS_TIMEOUT_DEFAULT_NS = TimeUnit.SECONDS.toNanos(60);

        /**
         * Timeout after which an election fails if the candidate does not get a majority of votes.
         */
        public static final String ELECTION_TIMEOUT_PROP_NAME = "aeron.cluster.election.timeout";

        /**
         * Default timeout after which an election fails if the candidate does not get a majority of votes.
         */
        public static final long ELECTION_TIMEOUT_DEFAULT_NS = TimeUnit.SECONDS.toNanos(1);

        /**
         * Interval at which a member will send out status updates during election phases.
         */
        public static final String ELECTION_STATUS_INTERVAL_PROP_NAME = "aeron.cluster.election.status.interval";

        /**
         * Default interval at which a member will send out status updates during election phases.
         */
        public static final long ELECTION_STATUS_INTERVAL_DEFAULT_NS = TimeUnit.MILLISECONDS.toNanos(20);

        /**
         * Interval at which a dynamic joining member will send add cluster member and snapshot recording
         * queries.
         */
        public static final String DYNAMIC_JOIN_INTERVAL_PROP_NAME = "aeron.cluster.dynamic.join.interval";

        /**
         * Default interval at which a dynamic joining member will send add cluster member and snapshot recording
         * queries.
         */
        public static final long DYNAMIC_JOIN_INTERVAL_DEFAULT_NS = TimeUnit.SECONDS.toNanos(1);

        /**
         * Name of class to use as a supplier of {@link Authenticator} for the cluster.
         */
        public static final String AUTHENTICATOR_SUPPLIER_PROP_NAME = "aeron.cluster.authenticator.supplier";

        /**
         * Name of the class to use as a supplier of {@link Authenticator} for the cluster. Default is
         * a non-authenticating option.
         */
        public static final String AUTHENTICATOR_SUPPLIER_DEFAULT = "io.aeron.security.DefaultAuthenticatorSupplier";

        /**
         * Size in bytes of the error buffer for the cluster.
         */
        public static final String ERROR_BUFFER_LENGTH_PROP_NAME = "aeron.cluster.error.buffer.length";

        /**
         * Size in bytes of the error buffer for the cluster.
         */
        public static final int ERROR_BUFFER_LENGTH_DEFAULT = 1024 * 1024;

        /**
         * Timeout waiting for follower termination by leader.
         */
        public static final String TERMINATION_TIMEOUT_PROP_NAME = "aeron.cluster.termination.timeout";

        /**
         * Timeout waiting for follower termination by leader default value.
         */
        public static final long TERMINATION_TIMEOUT_DEFAULT_NS = TimeUnit.SECONDS.toNanos(5);

        /**
         * Resolution in nanoseconds for each tick of the timer wheel for scheduling deadlines.
         */
        public static final String WHEEL_TICK_RESOLUTION_PROP_NAME = "aeron.cluster.wheel.tick.resolution";

        /**
         * Resolution in nanoseconds for each tick of the timer wheel for scheduling deadlines. Defaults to 8ms.
         */
        public static final long WHEEL_TICK_RESOLUTION_DEFAULT_NS = TimeUnit.MILLISECONDS.toNanos(8);

        /**
         * Number of ticks, or spokes, on the timer wheel. Higher number of ticks reduces potential conflicts
         * traded off against memory usage.
         */
        public static final String TICKS_PER_WHEEL_PROP_NAME = "aeron.cluster.ticks.per.wheel";

        /**
         * Number of ticks, or spokes, on the timer wheel. Higher number of ticks reduces potential conflicts
         * traded off against memory usage. Defaults to 128 per wheel.
         */
        public static final int TICKS_PER_WHEEL_DEFAULT = 128;

        /**
         * The level at which files should be sync'ed to disk.
         * <ul>
         * <li>0 - normal writes.</li>
         * <li>1 - sync file data.</li>
         * <li>2 - sync file data + metadata.</li>
         * </ul>
         */
        public static final String FILE_SYNC_LEVEL_PROP_NAME = "aeron.cluster.file.sync.level";

        /**
         * Default file sync level of normal writes.
         */
        public static final int FILE_SYNC_LEVEL_DEFAULT = 0;

        /**
         * The value {@link #CLUSTER_INGRESS_FRAGMENT_LIMIT_DEFAULT} or system property
         * {@link #CLUSTER_INGRESS_FRAGMENT_LIMIT_PROP_NAME} if set.
         *
         * @return {@link #CLUSTER_INGRESS_FRAGMENT_LIMIT_DEFAULT} or system property
         * {@link #CLUSTER_INGRESS_FRAGMENT_LIMIT_PROP_NAME} if set.
         */
        public static int ingressFragmentLimit()
        {
            return Integer.getInteger(CLUSTER_INGRESS_FRAGMENT_LIMIT_PROP_NAME, CLUSTER_INGRESS_FRAGMENT_LIMIT_DEFAULT);
        }

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
         * The value {@link #CLUSTER_MEMBERS_STATUS_ENDPOINTS_DEFAULT} or system property
         * {@link #CLUSTER_MEMBERS_STATUS_ENDPOINTS_PROP_NAME} if set.
         *
         * @return {@link #CLUSTER_MEMBERS_STATUS_ENDPOINTS_DEFAULT} or system property
         * {@link #CLUSTER_MEMBERS_STATUS_ENDPOINTS_PROP_NAME} it set.
         */
        public static String clusterMembersStatusEndpoints()
        {
            return System.getProperty(
                CLUSTER_MEMBERS_STATUS_ENDPOINTS_PROP_NAME, CLUSTER_MEMBERS_STATUS_ENDPOINTS_DEFAULT);
        }

        /**
         * The value {@link #CLUSTER_MEMBERS_IGNORE_SNAPSHOT_DEFAULT} or system property
         * {@link #CLUSTER_MEMBERS_IGNORE_SNAPSHOT_PROP_NAME} if set.
         *
         * @return {@link #CLUSTER_MEMBERS_IGNORE_SNAPSHOT_DEFAULT} or system property
         * {@link #CLUSTER_MEMBERS_IGNORE_SNAPSHOT_PROP_NAME} it set.
         */
        public static boolean clusterMembersIgnoreSnapshot()
        {
            return "true".equalsIgnoreCase(System.getProperty(
                CLUSTER_MEMBERS_IGNORE_SNAPSHOT_PROP_NAME, CLUSTER_MEMBERS_IGNORE_SNAPSHOT_DEFAULT));
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
         * The value {@link #MEMBER_ENDPOINTS_DEFAULT} or system property {@link #MEMBER_ENDPOINTS_PROP_NAME} if set.
         *
         * @return {@link #MEMBER_ENDPOINTS_DEFAULT} or system property {@link #MEMBER_ENDPOINTS_PROP_NAME} if set.
         */
        public static String memberEndpoints()
        {
            return System.getProperty(MEMBER_ENDPOINTS_PROP_NAME, MEMBER_ENDPOINTS_DEFAULT);
        }

        /**
         * The value {@link #SNAPSHOT_CHANNEL_DEFAULT} or system property
         * {@link io.aeron.cluster.service.ClusteredServiceContainer.Configuration#SNAPSHOT_CHANNEL_PROP_NAME} if set.
         *
         * @return {@link #SNAPSHOT_CHANNEL_DEFAULT} or system property
         * {@link io.aeron.cluster.service.ClusteredServiceContainer.Configuration#SNAPSHOT_CHANNEL_PROP_NAME} if set.
         */
        public static String snapshotChannel()
        {
            return System.getProperty(SNAPSHOT_CHANNEL_PROP_NAME, SNAPSHOT_CHANNEL_DEFAULT);
        }

        /**
         * The value {@link #SNAPSHOT_STREAM_ID_DEFAULT} or system property
         * {@link io.aeron.cluster.service.ClusteredServiceContainer.Configuration#SNAPSHOT_STREAM_ID_PROP_NAME} if set.
         *
         * @return {@link #SNAPSHOT_STREAM_ID_DEFAULT} or system property
         * {@link io.aeron.cluster.service.ClusteredServiceContainer.Configuration#SNAPSHOT_STREAM_ID_PROP_NAME} if set.
         */
        public static int snapshotStreamId()
        {
            return Integer.getInteger(SNAPSHOT_STREAM_ID_PROP_NAME, SNAPSHOT_STREAM_ID_DEFAULT);
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
         * @see #LEADER_HEARTBEAT_TIMEOUT_PROP_NAME
         */
        public static long leaderHeartbeatTimeoutNs()
        {
            return getDurationInNanos(LEADER_HEARTBEAT_TIMEOUT_PROP_NAME, LEADER_HEARTBEAT_TIMEOUT_DEFAULT_NS);
        }

        /**
         * Interval at which a leader will send a heartbeat if the log is not progressing.
         *
         * @return timeout in nanoseconds to for leader heartbeats when no log being appended.
         * @see #LEADER_HEARTBEAT_INTERVAL_PROP_NAME
         */
        public static long leaderHeartbeatIntervalNs()
        {
            return getDurationInNanos(LEADER_HEARTBEAT_INTERVAL_PROP_NAME, LEADER_HEARTBEAT_INTERVAL_DEFAULT_NS);
        }

        /**
         * Timeout waiting to canvass the status of cluster members before voting if a majority have been heard from.
         *
         * @return timeout in nanoseconds to wait for the status of other cluster members before voting.
         * @see #STARTUP_CANVASS_TIMEOUT_PROP_NAME
         */
        public static long startupCanvassTimeoutNs()
        {
            return getDurationInNanos(STARTUP_CANVASS_TIMEOUT_PROP_NAME, STARTUP_CANVASS_TIMEOUT_DEFAULT_NS);
        }

        /**
         * Timeout waiting for votes to become leader in an election.
         *
         * @return timeout in nanoseconds to wait for votes to become leader in an election.
         * @see #ELECTION_TIMEOUT_PROP_NAME
         */
        public static long electionTimeoutNs()
        {
            return getDurationInNanos(ELECTION_TIMEOUT_PROP_NAME, ELECTION_TIMEOUT_DEFAULT_NS);
        }

        /**
         * Interval at which a member will send out status messages during the election phases.
         *
         * @return interval at which a member will send out status messages during the election phases.
         * @see #ELECTION_STATUS_INTERVAL_PROP_NAME
         */
        public static long electionStatusIntervalNs()
        {
            return getDurationInNanos(ELECTION_STATUS_INTERVAL_PROP_NAME, ELECTION_STATUS_INTERVAL_DEFAULT_NS);
        }

        /**
         * Interval at which a dynamic joining member will send out add cluster members and snapshot recording
         * queries.
         *
         * @return Interval at which a dynamic joining member will send out add cluster members and snapshot recording
         * queries.
         * @see #DYNAMIC_JOIN_INTERVAL_PROP_NAME
         */
        public static long dynamicJoinIntervalNs()
        {
            return getDurationInNanos(DYNAMIC_JOIN_INTERVAL_PROP_NAME, DYNAMIC_JOIN_INTERVAL_DEFAULT_NS);
        }

        /**
         * Timeout waiting for follower termination by leader.
         *
         * @return timeout in nanoseconds to wait followers to terminate.
         * @see #TERMINATION_TIMEOUT_PROP_NAME
         */
        public static long terminationTimeoutNs()
        {
            return getDurationInNanos(TERMINATION_TIMEOUT_PROP_NAME, TERMINATION_TIMEOUT_DEFAULT_NS);
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
                supplier = (AuthenticatorSupplier)Class.forName(supplierClassName).getConstructor().newInstance();
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

        /**
         * The value {@link #WHEEL_TICK_RESOLUTION_DEFAULT_NS} or system property
         * {@link #WHEEL_TICK_RESOLUTION_PROP_NAME} if set.
         *
         * @return {@link #WHEEL_TICK_RESOLUTION_DEFAULT_NS} or system property
         * {@link #WHEEL_TICK_RESOLUTION_PROP_NAME} if set.
         */
        public static long wheelTickResolutionNs()
        {
            return getDurationInNanos(WHEEL_TICK_RESOLUTION_PROP_NAME, WHEEL_TICK_RESOLUTION_DEFAULT_NS);
        }

        /**
         * The value {@link #TICKS_PER_WHEEL_DEFAULT} or system property
         * {@link #CLUSTER_MEMBER_ID_PROP_NAME} if set.
         *
         * @return {@link #TICKS_PER_WHEEL_DEFAULT} or system property
         * {@link #TICKS_PER_WHEEL_PROP_NAME} if set.
         */
        public static int ticksPerWheel()
        {
            return Integer.getInteger(TICKS_PER_WHEEL_PROP_NAME, TICKS_PER_WHEEL_DEFAULT);
        }

        /**
         * The level at which files should be sync'ed to disk.
         * <ul>
         * <li>0 - normal writes.</li>
         * <li>1 - sync file data.</li>
         * <li>2 - sync file data + metadata.</li>
         * </ul>
         *
         * @return level at which files should be sync'ed to disk.
         */
        public static int fileSyncLevel()
        {
            return Integer.getInteger(FILE_SYNC_LEVEL_PROP_NAME, FILE_SYNC_LEVEL_DEFAULT);
        }
    }

    /**
     * Programmable overrides for configuring the {@link ConsensusModule} in a cluster.
     * <p>
     * The context will be owned by {@link ConsensusModuleAgent} after a successful
     * {@link ConsensusModule#launch(Context)} and closed via {@link ConsensusModule#close()}.
     */
    public static class Context implements Cloneable
    {
        /**
         * Using an integer because there is no support for boolean. 1 is concluded, 0 is not concluded.
         */
        private static final AtomicIntegerFieldUpdater<Context> IS_CONCLUDED_UPDATER = newUpdater(
            Context.class, "isConcluded");
        private volatile int isConcluded;

        private boolean ownsAeronClient = false;
        private String aeronDirectoryName = CommonContext.getAeronDirectoryName();
        private Aeron aeron;

        private boolean deleteDirOnStart = false;
        private String clusterDirectoryName = ClusteredServiceContainer.Configuration.clusterDirName();
        private File clusterDir;
        private RecordingLog recordingLog;
        private ClusterMarkFile markFile;
        private MutableDirectBuffer tempBuffer;
        private int fileSyncLevel = Archive.Configuration.fileSyncLevel();

        private int appVersion = SemanticVersion.compose(0, 0, 1);
        private int clusterMemberId = Configuration.clusterMemberId();
        private int appointedLeaderId = Configuration.appointedLeaderId();
        private String clusterMembers = Configuration.clusterMembers();
        private String clusterMembersStatusEndpoints = Configuration.clusterMembersStatusEndpoints();
        private boolean clusterMembersIgnoreSnapshot = Configuration.clusterMembersIgnoreSnapshot();
        private String ingressChannel = AeronCluster.Configuration.ingressChannel();
        private int ingressStreamId = AeronCluster.Configuration.ingressStreamId();
        private int ingressFragmentLimit = Configuration.ingressFragmentLimit();
        private String logChannel = Configuration.logChannel();
        private int logStreamId = Configuration.logStreamId();
        private String memberEndpoints = Configuration.memberEndpoints();
        private String replayChannel = ClusteredServiceContainer.Configuration.replayChannel();
        private int replayStreamId = ClusteredServiceContainer.Configuration.replayStreamId();
        private String serviceControlChannel = ClusteredServiceContainer.Configuration.serviceControlChannel();
        private int consensusModuleStreamId = ClusteredServiceContainer.Configuration.consensusModuleStreamId();
        private int serviceStreamId = ClusteredServiceContainer.Configuration.serviceStreamId();
        private String snapshotChannel = Configuration.snapshotChannel();
        private int snapshotStreamId = Configuration.snapshotStreamId();
        private String memberStatusChannel = Configuration.memberStatusChannel();
        private int memberStatusStreamId = Configuration.memberStatusStreamId();

        private int serviceCount = Configuration.serviceCount();
        private int errorBufferLength = Configuration.errorBufferLength();
        private int maxConcurrentSessions = Configuration.maxConcurrentSessions();
        private int ticksPerWheel = Configuration.ticksPerWheel();
        private long wheelTickResolutionNs = Configuration.wheelTickResolutionNs();
        private long sessionTimeoutNs = Configuration.sessionTimeoutNs();
        private long leaderHeartbeatTimeoutNs = Configuration.leaderHeartbeatTimeoutNs();
        private long leaderHeartbeatIntervalNs = Configuration.leaderHeartbeatIntervalNs();
        private long startupCanvassTimeoutNs = Configuration.startupCanvassTimeoutNs();
        private long electionTimeoutNs = Configuration.electionTimeoutNs();
        private long electionStatusIntervalNs = Configuration.electionStatusIntervalNs();
        private long dynamicJoinIntervalNs = Configuration.dynamicJoinIntervalNs();
        private long terminationTimeoutNs = Configuration.terminationTimeoutNs();

        private ThreadFactory threadFactory;
        private Supplier<IdleStrategy> idleStrategySupplier;
        private ClusterClock clusterClock;
        private EpochClock epochClock;
        private Random random;

        private DistinctErrorLog errorLog;
        private ErrorHandler errorHandler;
        private AtomicCounter errorCounter;
        private CountedErrorHandler countedErrorHandler;

        private Counter moduleState;
        private Counter clusterNodeRole;
        private Counter commitPosition;
        private Counter controlToggle;
        private Counter snapshotCounter;
        private Counter invalidRequestCounter;
        private Counter timedOutClientCounter;
        private ShutdownSignalBarrier shutdownSignalBarrier;
        private Runnable terminationHook;

        private AeronArchive.Context archiveContext;
        private AuthenticatorSupplier authenticatorSupplier;
        private LogPublisher logPublisher;
        private EgressPublisher egressPublisher;

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
            if (0 != IS_CONCLUDED_UPDATER.getAndSet(this, 1))
            {
                throw new ConcurrentConcludeException();
            }

            if (null == clusterDir)
            {
                clusterDir = new File(clusterDirectoryName);
            }

            if (deleteDirOnStart && clusterDir.exists())
            {
                IoUtil.delete(clusterDir, false);
            }

            if (!clusterDir.exists() && !clusterDir.mkdirs())
            {
                throw new ClusterException("failed to create cluster dir: " + clusterDir.getAbsolutePath());
            }

            if (null == tempBuffer)
            {
                tempBuffer = new UnsafeBuffer(new byte[METADATA_LENGTH]);
            }

            if (null == clusterClock)
            {
                clusterClock = new MillisecondClusterClock();
            }

            if (null == epochClock)
            {
                epochClock = SystemEpochClock.INSTANCE;
            }

            if (null == markFile)
            {
                markFile = new ClusterMarkFile(
                    new File(clusterDir, ClusterMarkFile.FILENAME),
                    ClusterComponentType.CONSENSUS_MODULE,
                    errorBufferLength,
                    epochClock,
                    0);
            }

            if (null == errorLog)
            {
                errorLog = new DistinctErrorLog(markFile.errorBuffer(), epochClock, US_ASCII);
            }

            if (null == errorHandler)
            {
                errorHandler = new LoggingErrorHandler(errorLog);
            }

            if (null == recordingLog)
            {
                recordingLog = new RecordingLog(clusterDir);
            }

            if (null == aeron)
            {
                ownsAeronClient = true;

                aeron = Aeron.connect(
                    new Aeron.Context()
                        .aeronDirectoryName(aeronDirectoryName)
                        .errorHandler(errorHandler)
                        .epochClock(epochClock)
                        .useConductorAgentInvoker(true)
                        .awaitingIdleStrategy(YieldingIdleStrategy.INSTANCE)
                        .clientLock(NoOpLock.INSTANCE));

                if (null == errorCounter)
                {
                    errorCounter = aeron.addCounter(CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID, "Cluster errors");
                }
            }

            if (null == aeron.conductorAgentInvoker())
            {
                throw new ClusterException("Aeron client must use conductor agent invoker");
            }

            if (null == errorCounter)
            {
                throw new ClusterException("error counter must be supplied if aeron client is");
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

            if (null == commitPosition)
            {
                commitPosition = CommitPos.allocate(aeron);
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
                invalidRequestCounter = aeron.addCounter(
                    CLUSTER_INVALID_REQUEST_COUNT_TYPE_ID, "Invalid cluster request count");
            }

            if (null == timedOutClientCounter)
            {
                timedOutClientCounter = aeron.addCounter(
                    CLUSTER_CLIENT_TIMEOUT_COUNT_TYPE_ID, "Timed out cluster client count");
            }

            if (null == clusterNodeRole)
            {
                clusterNodeRole = aeron.addCounter(Configuration.CLUSTER_NODE_ROLE_TYPE_ID, "Cluster node role");
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
                    .controlRequestChannel(AeronArchive.Configuration.localControlChannel())
                    .controlResponseChannel(AeronArchive.Configuration.localControlChannel())
                    .controlRequestStreamId(AeronArchive.Configuration.localControlStreamId());
            }

            archiveContext
                .aeron(aeron)
                .errorHandler(countedErrorHandler)
                .ownsAeronClient(false)
                .lock(NoOpLock.INSTANCE);

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

            if (null == random)
            {
                random = new Random();
            }

            if (null == logPublisher)
            {
                logPublisher = new LogPublisher();
            }

            if (null == egressPublisher)
            {
                egressPublisher = new EgressPublisher();
            }

            concludeMarkFile();
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
         * Set the directory name to use for the consensus module directory.
         *
         * @param clusterDirectoryName to use.
         * @return this for a fluent API.
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#CLUSTER_DIR_PROP_NAME
         */
        public Context clusterDirectoryName(final String clusterDirectoryName)
        {
            this.clusterDirectoryName = clusterDirectoryName;
            return this;
        }

        /**
         * The directory name to use for the consensus module directory.
         *
         * @return directory name for the consensus module directory.
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#CLUSTER_DIR_PROP_NAME
         */
        public String clusterDirectoryName()
        {
            return clusterDirectoryName;
        }

        /**
         * Set the directory to use for the consensus module directory.
         *
         * @param clusterDir to use.
         * @return this for a fluent API.
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#CLUSTER_DIR_PROP_NAME
         */
        public Context clusterDir(final File clusterDir)
        {
            this.clusterDir = clusterDir;
            return this;
        }

        /**
         * The directory used for for the consensus module directory.
         *
         * @return directory for for the consensus module directory.
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#CLUSTER_DIR_PROP_NAME
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
         * User assigned application version which appended to the log as the appVersion in new leadership events.
         * <p>
         * This can be validated using {@link org.agrona.SemanticVersion} to ensure only application nodes of the same
         * major version communicate with each other.
         *
         * @param appVersion for user application.
         * @return this for a fluent API.
         */
        public Context appVersion(final int appVersion)
        {
            this.appVersion = appVersion;
            return this;
        }

        /**
         * User assigned application version which appended to the log as the appVersion in new leadership events.
         * <p>
         * This can be validated using {@link org.agrona.SemanticVersion} to ensure only application nodes of the same
         * major version communicate with each other.
         *
         * @return appVersion for user application.
         */
        public int appVersion()
        {
            return appVersion;
        }

        /**
         * Get level at which files should be sync'ed to disk.
         * <ul>
         * <li>0 - normal writes.</li>
         * <li>1 - sync file data.</li>
         * <li>2 - sync file data + metadata.</li>
         * </ul>
         *
         * @return the level to be applied for file write.
         * @see Configuration#FILE_SYNC_LEVEL_PROP_NAME
         */
        int fileSyncLevel()
        {
            return fileSyncLevel;
        }

        /**
         * Set level at which files should be sync'ed to disk.
         * <ul>
         * <li>0 - normal writes.</li>
         * <li>1 - sync file data.</li>
         * <li>2 - sync file data + metadata.</li>
         * </ul>
         *
         * @param syncLevel to be applied for file writes.
         * @return this for a fluent API.
         * @see Configuration#FILE_SYNC_LEVEL_PROP_NAME
         */
        public Context fileSyncLevel(final int syncLevel)
        {
            this.fileSyncLevel = syncLevel;
            return this;
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
         *     0,client-facing:port,member-facing:port,log:port,transfer:port,archive:port| \
         *     1,client-facing:port,member-facing:port,log:port,transfer:port,archive:port| ...
         * </code>
         * <p>
         * The client facing endpoints will be used as the endpoint substituted into the {@link #ingressChannel()}
         * if the endpoint is not provided unless it is multicast.
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
         * String representing the cluster members member status endpoints used to request to join the cluster.
         * <p>
         * {@code "endpoint,endpoint,endpoint"}
         * <p>
         *
         * @param endpoints which are to be contacted for joining the cluster.
         * @return this for a fluent API.
         * @see Configuration#CLUSTER_MEMBERS_STATUS_ENDPOINTS_PROP_NAME
         */
        public Context clusterMembersStatusEndpoints(final String endpoints)
        {
            this.clusterMembersStatusEndpoints = endpoints;
            return this;
        }

        /**
         * The endpoints representing cluster members of the cluster to attempt to contact to join the cluster.
         *
         * @return members of the cluster to attempt to request to join from.
         * @see Configuration#CLUSTER_MEMBERS_STATUS_ENDPOINTS_PROP_NAME
         */
        public String clusterMembersStatusEndpoints()
        {
            return clusterMembersStatusEndpoints;
        }

        /**
         * Whether the cluster members in the snapshot should be ignored or not.
         *
         * @param ignore or not the cluster members in the snapshot.
         * @return this for a fluent API.
         * @see Configuration#CLUSTER_MEMBERS_IGNORE_SNAPSHOT_PROP_NAME
         */
        public Context clusterMembersIgnoreSnapshot(final boolean ignore)
        {
            this.clusterMembersIgnoreSnapshot = ignore;
            return this;
        }

        /**
         * Whether the cluster members in the snapshot should be ignored or not.
         *
         * @return ignore or not the cluster members in the snapshot.
         * @see Configuration#CLUSTER_MEMBERS_IGNORE_SNAPSHOT_PROP_NAME
         */
        public boolean clusterMembersIgnoreSnapshot()
        {
            return clusterMembersIgnoreSnapshot;
        }

        /**
         * Set the channel parameter for the ingress channel.
         *
         * @param channel parameter for the ingress channel.
         * @return this for a fluent API.
         * @see io.aeron.cluster.client.AeronCluster.Configuration#INGRESS_CHANNEL_PROP_NAME
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
         * @see io.aeron.cluster.client.AeronCluster.Configuration#INGRESS_CHANNEL_PROP_NAME
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
         * @see io.aeron.cluster.client.AeronCluster.Configuration#INGRESS_STREAM_ID_PROP_NAME
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
         * @see io.aeron.cluster.client.AeronCluster.Configuration#INGRESS_STREAM_ID_PROP_NAME
         */
        public int ingressStreamId()
        {
            return ingressStreamId;
        }

        /**
         * Set limit for fragments to be consumed on each poll of ingress.
         *
         * @param ingressFragmentLimit for the ingress channel.
         * @return this for a fluent API
         * @see Configuration#CLUSTER_INGRESS_FRAGMENT_LIMIT_PROP_NAME
         */
        public Context ingressFragmentLimit(final int ingressFragmentLimit)
        {
            this.ingressFragmentLimit = ingressFragmentLimit;
            return this;
        }

        /**
         * The limit for fragments to be consumed on each poll of ingress.
         *
         * @return the limit for fragments to be consumed on each poll of ingress.
         * @see Configuration#CLUSTER_INGRESS_FRAGMENT_LIMIT_PROP_NAME
         */
        public int ingressFragmentLimit()
        {
            return ingressFragmentLimit;
        }

        /**
         * Set the channel parameter for the cluster log channel.
         *
         * @param channel parameter for the cluster log channel.
         * @return this for a fluent API.
         * @see Configuration#LOG_CHANNEL_PROP_NAME
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
         * @see Configuration#LOG_CHANNEL_PROP_NAME
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
         * @see Configuration#LOG_STREAM_ID_PROP_NAME
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
         * @see Configuration#LOG_STREAM_ID_PROP_NAME
         */
        public int logStreamId()
        {
            return logStreamId;
        }

        /**
         * Set the endpoints for this cluster node.
         *
         * @param endpoints for the cluster node.
         * @return this for a fluent API.
         * @see Configuration#MEMBER_ENDPOINTS_PROP_NAME
         */
        public Context memberEndpoints(final String endpoints)
        {
            memberEndpoints = endpoints;
            return this;
        }

        /**
         * Get the endpoints for this cluster node.
         *
         * @return the endpoints for the cluster node.
         * @see Configuration#MEMBER_ENDPOINTS_PROP_NAME
         */
        public String memberEndpoints()
        {
            return memberEndpoints;
        }

        /**
         * Set the channel parameter for the cluster log and snapshot replay channel.
         *
         * @param channel parameter for the cluster log replay channel.
         * @return this for a fluent API.
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#REPLAY_CHANNEL_PROP_NAME
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
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#REPLAY_CHANNEL_PROP_NAME
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
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#REPLAY_STREAM_ID_PROP_NAME
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
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#REPLAY_STREAM_ID_PROP_NAME
         */
        public int replayStreamId()
        {
            return replayStreamId;
        }

        /**
         * Set the channel parameter for bi-directional communications between the consensus module and services.
         *
         * @param channel parameter for bi-directional communications between the consensus module and services.
         * @return this for a fluent API.
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#SERVICE_CONTROL_CHANNEL_PROP_NAME
         */
        public Context serviceControlChannel(final String channel)
        {
            serviceControlChannel = channel;
            return this;
        }

        /**
         * Get the channel parameter for bi-directional communications between the consensus module and services.
         *
         * @return the channel parameter for bi-directional communications between the consensus module and services.
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#SERVICE_CONTROL_CHANNEL_PROP_NAME
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
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#SERVICE_STREAM_ID_PROP_NAME
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
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#SERVICE_STREAM_ID_PROP_NAME
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
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#CONSENSUS_MODULE_STREAM_ID_PROP_NAME
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
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#CONSENSUS_MODULE_STREAM_ID_PROP_NAME
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
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#SNAPSHOT_CHANNEL_PROP_NAME
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
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#SNAPSHOT_CHANNEL_PROP_NAME
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
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#SNAPSHOT_STREAM_ID_PROP_NAME
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
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#SNAPSHOT_STREAM_ID_PROP_NAME
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
         * Resolution in nanoseconds for each tick of the timer wheel for scheduling deadlines.
         *
         * @param wheelTickResolutionNs the resolution in nanoseconds of each tick on the timer wheel.
         * @return this for a fluent API
         * @see Configuration#WHEEL_TICK_RESOLUTION_PROP_NAME
         */
        public Context wheelTickResolutionNs(final long wheelTickResolutionNs)
        {
            this.wheelTickResolutionNs = wheelTickResolutionNs;
            return this;
        }

        /**
         * Resolution in nanoseconds for each tick of the timer wheel for scheduling deadlines.
         *
         * @return the resolution in nanoseconds for each tick on the timer wheel.
         * @see Configuration#WHEEL_TICK_RESOLUTION_PROP_NAME
         */
        public long wheelTickResolutionNs()
        {
            return wheelTickResolutionNs;
        }

        /**
         * Number of ticks, or spokes, on the timer wheel. Higher number of ticks reduces potential conflicts
         * traded off against memory usage.
         *
         * @param ticksPerWheel the number of ticks on the timer wheel.
         * @return this for a fluent API
         * @see Configuration#TICKS_PER_WHEEL_PROP_NAME
         */
        public Context ticksPerWheel(final int ticksPerWheel)
        {
            this.ticksPerWheel = ticksPerWheel;
            return this;
        }

        /**
         * Number of ticks, or spokes, on the timer wheel. Higher number of ticks reduces potential conflicts
         * traded off against memory usage.
         *
         * @return the number of ticks on the timer wheel.
         * @see Configuration#TICKS_PER_WHEEL_PROP_NAME
         */
        public int ticksPerWheel()
        {
            return ticksPerWheel;
        }

        /**
         * Set the number of clustered services in this cluster instance.
         *
         * @param serviceCount the number of clustered services in this cluster instance.
         * @return this for a fluent API
         * @see Configuration#SERVICE_COUNT_PROP_NAME
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#SERVICE_ID_PROP_NAME
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
         * @see io.aeron.cluster.service.ClusteredServiceContainer.Configuration#SERVICE_ID_PROP_NAME
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
         * @see Configuration#LEADER_HEARTBEAT_TIMEOUT_PROP_NAME
         */
        public Context leaderHeartbeatTimeoutNs(final long heartbeatTimeoutNs)
        {
            this.leaderHeartbeatTimeoutNs = heartbeatTimeoutNs;
            return this;
        }

        /**
         * Timeout for a leader if no heartbeat is received by an other member.
         *
         * @return the timeout for a leader if no heartbeat is received by an other member.
         * @see Configuration#LEADER_HEARTBEAT_TIMEOUT_PROP_NAME
         */
        public long leaderHeartbeatTimeoutNs()
        {
            return leaderHeartbeatTimeoutNs;
        }

        /**
         * Interval at which a leader will send heartbeats if the log is not progressing.
         *
         * @param heartbeatIntervalNs between leader heartbeats.
         * @return this for a fluent API.
         * @see Configuration#LEADER_HEARTBEAT_INTERVAL_PROP_NAME
         */
        public Context leaderHeartbeatIntervalNs(final long heartbeatIntervalNs)
        {
            this.leaderHeartbeatIntervalNs = heartbeatIntervalNs;
            return this;
        }

        /**
         * Interval at which a leader will send heartbeats if the log is not progressing.
         *
         * @return the interval at which a leader will send heartbeats if the log is not progressing.
         * @see Configuration#LEADER_HEARTBEAT_INTERVAL_PROP_NAME
         */
        public long leaderHeartbeatIntervalNs()
        {
            return leaderHeartbeatIntervalNs;
        }

        /**
         * Timeout to wait for hearing the status of all cluster members on startup after recovery before commencing
         * an election if a majority of members has been heard from.
         *
         * @param timeoutNs to wait on startup after recovery before commencing an election.
         * @return this for a fluent API.
         * @see Configuration#STARTUP_CANVASS_TIMEOUT_PROP_NAME
         */
        public Context startupCanvassTimeoutNs(final long timeoutNs)
        {
            this.startupCanvassTimeoutNs = timeoutNs;
            return this;
        }

        /**
         * Timeout to wait for hearing the status of all cluster members on startup after recovery before commencing
         * an election if a majority of members has been heard from.
         *
         * @return the timeout to wait on startup after recovery before commencing an election.
         * @see Configuration#STARTUP_CANVASS_TIMEOUT_PROP_NAME
         */
        public long startupCanvassTimeoutNs()
        {
            return startupCanvassTimeoutNs;
        }

        /**
         * Timeout to wait for votes in an election before declaring the election void and starting over.
         *
         * @param timeoutNs to wait for votes in an elections.
         * @return this for a fluent API.
         * @see Configuration#ELECTION_TIMEOUT_PROP_NAME
         */
        public Context electionTimeoutNs(final long timeoutNs)
        {
            this.electionTimeoutNs = timeoutNs;
            return this;
        }

        /**
         * Timeout to wait for votes in an election before declaring the election void and starting over.
         *
         * @return the timeout to wait for votes in an elections.
         * @see Configuration#ELECTION_TIMEOUT_PROP_NAME
         */
        public long electionTimeoutNs()
        {
            return electionTimeoutNs;
        }

        /**
         * Interval at which a member will send out status messages during the election phases.
         *
         * @param electionStatusIntervalNs between status message updates.
         * @return this for a fluent API.
         * @see Configuration#ELECTION_STATUS_INTERVAL_PROP_NAME
         * @see Configuration#ELECTION_STATUS_INTERVAL_DEFAULT_NS
         */
        public Context electionStatusIntervalNs(final long electionStatusIntervalNs)
        {
            this.electionStatusIntervalNs = electionStatusIntervalNs;
            return this;
        }

        /**
         * Interval at which a member will send out status messages during the election phases.
         *
         * @return the interval at which a member will send out status messages during the election phases.
         * @see Configuration#ELECTION_STATUS_INTERVAL_PROP_NAME
         * @see Configuration#ELECTION_STATUS_INTERVAL_DEFAULT_NS
         */
        public long electionStatusIntervalNs()
        {
            return electionStatusIntervalNs;
        }

        /**
         * Interval at which a dynamic joining member will send add cluster member and snapshot recording queries.
         *
         * @param dynamicJoinIntervalNs between add cluster members and snapshot recording queries.
         * @return this for a fluent API.
         * @see Configuration#DYNAMIC_JOIN_INTERVAL_PROP_NAME
         * @see Configuration#DYNAMIC_JOIN_INTERVAL_DEFAULT_NS
         */
        public Context dynamicJoinIntervalNs(final long dynamicJoinIntervalNs)
        {
            this.dynamicJoinIntervalNs = dynamicJoinIntervalNs;
            return this;
        }

        /**
         * Interval at which a dynamic joining member will send add cluster member and snapshot recording queries.
         *
         * @return the interval at which a dynamic joining member will send add cluster member and snapshot recording
         * queries.
         * @see Configuration#DYNAMIC_JOIN_INTERVAL_PROP_NAME
         * @see Configuration#DYNAMIC_JOIN_INTERVAL_DEFAULT_NS
         */
        public long dynamicJoinIntervalNs()
        {
            return dynamicJoinIntervalNs;
        }

        /**
         * Timeout to wait for follower termination by leader.
         *
         * @param terminationTimeoutNs to wait for follower termination.
         * @return this for a fluent API.
         * @see Configuration#TERMINATION_TIMEOUT_PROP_NAME
         * @see Configuration#TERMINATION_TIMEOUT_DEFAULT_NS
         */
        public Context terminationTimeoutNs(final long terminationTimeoutNs)
        {
            this.terminationTimeoutNs = terminationTimeoutNs;
            return this;
        }

        /**
         * Timeout to wait for follower termination by leader.
         *
         * @return timeout to wait for follower termination by leader.
         * @see Configuration#TERMINATION_TIMEOUT_PROP_NAME
         * @see Configuration#TERMINATION_TIMEOUT_DEFAULT_NS
         */
        public long terminationTimeoutNs()
        {
            return terminationTimeoutNs;
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
         * Provides an {@link IdleStrategy} supplier for the idle strategy for the agent duty cycle.
         *
         * @param idleStrategySupplier supplier for the idle strategy for the agent duty cycle.
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
         * Set the {@link ClusterClock} to be used for timestamping messages and timers.
         *
         * @param clock {@link ClusterClock} to be used for timestamping messages and timers.
         * @return this for a fluent API.
         */
        public Context clusterClock(final ClusterClock clock)
        {
            this.clusterClock = clock;
            return this;
        }

        /**
         * Get the {@link ClusterClock} to used for timestamping messages and timers.
         *
         * @return the {@link ClusterClock} to used for timestamping messages and timers.
         */
        public ClusterClock clusterClock()
        {
            return clusterClock;
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
         * @see ConsensusModule.State
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
         * @see ConsensusModule.State
         */
        public Context moduleStateCounter(final Counter moduleState)
        {
            this.moduleState = moduleState;
            return this;
        }

        /**
         * Get the counter for the commit position the cluster has reached for consensus.
         *
         * @return the counter for the commit position the cluster has reached for consensus.
         * @see CommitPos
         */
        public Counter commitPositionCounter()
        {
            return commitPosition;
        }

        /**
         * Set the counter for the commit position the cluster has reached for consensus.
         *
         * @param commitPosition counter for the commit position the cluster has reached for consensus.
         * @return this for a fluent API.
         * @see CommitPos
         */
        public Context commitPositionCounter(final Counter commitPosition)
        {
            this.commitPosition = commitPosition;
            return this;
        }

        /**
         * Get the counter for representing the current {@link io.aeron.cluster.service.Cluster.Role} of the
         * consensus module node.
         *
         * @return the counter for representing the current {@link io.aeron.cluster.service.Cluster.Role} of the
         * cluster node.
         * @see io.aeron.cluster.service.Cluster.Role
         */
        public Counter clusterNodeCounter()
        {
            return clusterNodeRole;
        }

        /**
         * Set the counter for representing the current {@link io.aeron.cluster.service.Cluster.Role} of the
         * cluster node.
         *
         * @param moduleRole the counter for representing the current {@link io.aeron.cluster.service.Cluster.Role}
         *                   of the cluster node.
         * @return this for a fluent API.
         * @see io.aeron.cluster.service.Cluster.Role
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
         * Get the counter for the count of clients that have been timed out and disconnected.
         *
         * @return the counter for the count of clients that have been timed out and disconnected.
         */
        public Counter timedOutClientCounter()
        {
            return timedOutClientCounter;
        }

        /**
         * Set the counter for the count of clients that have been timed out and disconnected.
         *
         * @param timedOutClientCounter the count of clients that have been timed out and disconnected.
         * @return this for a fluent API.
         */
        public Context timedOutClientCounter(final Counter timedOutClientCounter)
        {
            this.timedOutClientCounter = timedOutClientCounter;
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
         * Set the {@link io.aeron.archive.client.AeronArchive.Context} that should be used for communicating with the
         * local Archive.
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
         * Get the {@link io.aeron.archive.client.AeronArchive.Context} that should be used for communicating with
         * the local Archive.
         *
         * @return the {@link io.aeron.archive.client.AeronArchive.Context} that should be used for communicating
         * with the local Archive.
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
         * Set the {@link Runnable} that is called when the {@link ConsensusModule} processes a termination action.
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
         * Get the {@link Runnable} that is called when the {@link ConsensusModule} processes a termination action.
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
         * @see Configuration#ERROR_BUFFER_LENGTH_PROP_NAME
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
         * @see Configuration#ERROR_BUFFER_LENGTH_PROP_NAME
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
         * The source of random values for timeouts used in elections.
         *
         * @param random source of random values for timeouts used in elections.
         * @return this for a fluent API.
         */
        public Context random(final Random random)
        {
            this.random = random;
            return this;
        }

        /**
         * The source of random values for timeouts used in elections.
         *
         * @return source of random values for timeouts used in elections.
         */
        public Random random()
        {
            return random;
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
            CloseHelper.close(recordingLog);
            CloseHelper.close(markFile);
            if (errorHandler instanceof AutoCloseable)
            {
                CloseHelper.close((AutoCloseable)errorHandler);
            }

            if (ownsAeronClient)
            {
                CloseHelper.close(aeron);
            }
            else if (!aeron.isClosed())
            {
                CloseHelper.close(moduleState);
                CloseHelper.close(commitPosition);
                CloseHelper.close(clusterNodeRole);
                CloseHelper.close(controlToggle);
                CloseHelper.close(snapshotCounter);
            }
        }

        Context logPublisher(final LogPublisher logPublisher)
        {
            this.logPublisher = logPublisher;
            return this;
        }

        LogPublisher logPublisher()
        {
            return logPublisher;
        }

        Context egressPublisher(final EgressPublisher egressPublisher)
        {
            this.egressPublisher = egressPublisher;
            return this;
        }

        EgressPublisher egressPublisher()
        {
            return egressPublisher;
        }

        private void concludeMarkFile()
        {
            ClusterMarkFile.checkHeaderLength(
                aeron.context().aeronDirectoryName(),
                archiveContext.controlRequestChannel(),
                serviceControlChannel(),
                ingressChannel,
                null,
                authenticatorSupplier.getClass().toString());

            markFile.encoder()
                .archiveStreamId(archiveContext.controlRequestStreamId())
                .serviceStreamId(serviceStreamId)
                .consensusModuleStreamId(consensusModuleStreamId)
                .ingressStreamId(ingressStreamId)
                .memberId(clusterMemberId)
                .serviceId(SERVICE_ID)
                .aeronDirectory(aeron.context().aeronDirectoryName())
                .archiveChannel(archiveContext.controlRequestChannel())
                .serviceControlChannel(serviceControlChannel)
                .ingressChannel(ingressChannel)
                .serviceName("")
                .authenticator(authenticatorSupplier.getClass().toString());

            markFile.updateActivityTimestamp(epochClock.time());
            markFile.signalReady();
        }
    }
}
