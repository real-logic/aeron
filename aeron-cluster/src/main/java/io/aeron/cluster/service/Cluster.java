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
package io.aeron.cluster.service;

import io.aeron.Aeron;
import io.aeron.DirectBufferVector;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.status.AtomicCounter;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Interface for a {@link ClusteredService} to interact with cluster hosting it.
 * <p>
 * This object should only be used to send messages to the cluster or schedule timers in response to other messages
 * and timers. Sending messages and timers should not happen from cluster lifecycle methods like
 * {@link ClusteredService#onStart(Cluster, Image)}, {@link ClusteredService#onRoleChange(Cluster.Role)} or
 * {@link ClusteredService#onTakeSnapshot(ExclusivePublication)}, or {@link ClusteredService#onTerminate(Cluster)},
 * except the session lifecycle methods {@link ClusteredService#onSessionOpen(ClientSession, long)},
 * {@link ClusteredService#onSessionClose(ClientSession, long, CloseReason)},
 * and {@link ClusteredService#onNewLeadershipTermEvent(long, long, long, long, int, int, TimeUnit, int)}.
 */
public interface Cluster
{
    /**
     * Role of the node in the cluster.
     */
    enum Role
    {
        /**
         * The cluster node is a follower in the current leadership term.
         */
        FOLLOWER(0),

        /**
         * The cluster node is a candidate to become a leader in an election.
         */
        CANDIDATE(1),

        /**
         * The cluster node is the leader for the current leadership term.
         */
        LEADER(2);

        static final Role[] ROLES = values();

        private final int code;

        Role(final int code)
        {
            if (code != ordinal())
            {
                throw new IllegalArgumentException(name() + " - code must equal ordinal value: code=" + code);
            }

            this.code = code;
        }

        /**
         * The code which matches the role in the cluster.
         *
         * @return the code which matches the role in the cluster.
         */
        public final int code()
        {
            return code;
        }

        /**
         * Get the role from a code read from a counter.
         *
         * @param code for the {@link Role}.
         * @return the {@link Role} of the cluster node.
         */
        public static Role get(final long code)
        {
            if (code < 0 || code > (ROLES.length - 1))
            {
                throw new IllegalStateException("Invalid role counter code: " + code);
            }

            return ROLES[(int)code];
        }

        /**
         * Get the role by reading the code from a counter.
         *
         * @param counter containing the value of the role.
         * @return the role for the cluster member.
         */
        public static Role get(final AtomicCounter counter)
        {
            if (counter.isClosed())
            {
                return FOLLOWER;
            }

            return get(counter.get());
        }
    }

    /**
     * The unique id for the hosting member of the cluster. Useful only for debugging purposes.
     *
     * @return unique id for the hosting member of the cluster.
     */
    int memberId();

    /**
     * The role the cluster node is playing.
     *
     * @return the role the cluster node is playing.
     */
    Role role();

    /**
     * Position the log has reached in bytes as of the current message.
     *
     * @return position the log has reached in bytes as of the current message.
     */
    long logPosition();

    /**
     * Get the {@link Aeron} client used by the cluster.
     *
     * @return the {@link Aeron} client used by the cluster.
     */
    Aeron aeron();

    /**
     * Get the {@link ClusteredServiceContainer.Context} under which the container is running.
     *
     * @return the {@link ClusteredServiceContainer.Context} under which the container is running.
     */
    ClusteredServiceContainer.Context context();

    /**
     * Get the {@link ClientSession} for a given cluster session id.
     *
     * @param clusterSessionId to be looked up.
     * @return the {@link ClientSession} that matches the clusterSessionId.
     */
    ClientSession getClientSession(long clusterSessionId);

    /**
     * Get the current collection of cluster client sessions.
     * <p>
     * The {@link java.util.Iterator} on this class does not support nested iteration. It reuses the iterator to
     * avoid allocation.
     *
     * @return the current collection of cluster client sessions.
     */
    Collection<ClientSession> clientSessions();

    /**
     * For each iterator over {@link ClientSession}s using the most efficient method possible.
     *
     * @param action to be taken for each {@link ClientSession} in turn.
     */
    void forEachClientSession(Consumer<? super ClientSession> action);

    /**
     * Request the close of a {@link ClientSession} by sending the request to the consensus module.
     *
     * @param clusterSessionId to be closed.
     * @return true if the event to close a session was sent or false if back pressure was applied.
     * @throws ClusterException if the clusterSessionId is not recognised.
     */
    boolean closeClientSession(long clusterSessionId);

    /**
     * Cluster time as {@link #timeUnit()}s since 1 Jan 1970 UTC.
     *
     * @return time as {@link #timeUnit()}s since 1 Jan 1970 UTC.
     */
    long time();

    /**
     * The unit of time applied when timestamping and {@link #time()} operations.
     *
     * @return the unit of time applied when timestamping and {@link #time()} operations.
     */
    TimeUnit timeUnit();

    /**
     * Schedule a timer for a given deadline and provide a correlation id to identify the timer when it expires or
     * for cancellation. This action is asynchronous, when rescheduling it will race with the timer expiring.
     * <p>
     * If the correlationId is for an existing scheduled timer then it will be rescheduled to the new deadline. However,
     * it is best to generate correlationIds in a monotonic fashion and be aware of potential clashes with other
     * services in the same cluster. Service isolation can be achieved by using the upper bits for service id.
     * <p>
     * Timers should only be scheduled or cancelled in the context of processing a
     * {@link ClusteredService#onSessionMessage(ClientSession, long, DirectBuffer, int, int, Header)},
     * {@link ClusteredService#onTimerEvent(long, long)},
     * {@link ClusteredService#onSessionOpen(ClientSession, long)}, or
     * {@link ClusteredService#onSessionClose(ClientSession, long, CloseReason)}.
     * If applied to other events then they are not guaranteed to be reliable.
     * <p>
     * Callers of this method must loop until the method succeeds.
     *
     * <pre>{@code
     * private Cluster cluster;
     * // Lines omitted...
     *
     * cluster.idleStrategy().reset();
     * while (!cluster.scheduleTimer(correlationId, deadline))
     * {
     *     cluster.idleStrategy().idle();
     * }
     * }</pre>
     *
     * The cluster's idle strategy must be used in the body of the loop to allow for the clustered service to be
     * shutdown if required.
     *
     * @param correlationId to identify the timer when it expires. {@link Long#MAX_VALUE} not supported.
     * @param deadline      time after which the timer will fire. {@link Long#MAX_VALUE} not supported.
     * @return true if the request to schedule a timer has been sent or false if back-pressure is applied.
     * @see #cancelTimer(long)
     */
    boolean scheduleTimer(long correlationId, long deadline);

    /**
     * Cancel a previously scheduled timer. This action is asynchronous and will race with the timer expiring.
     * <p>
     * Timers should only be scheduled or cancelled in the context of processing a
     * {@link ClusteredService#onSessionMessage(ClientSession, long, DirectBuffer, int, int, Header)},
     * {@link ClusteredService#onTimerEvent(long, long)},
     * {@link ClusteredService#onSessionOpen(ClientSession, long)}, or
     * {@link ClusteredService#onSessionClose(ClientSession, long, CloseReason)}.
     * If applied to other events then they are not guaranteed to be reliable.
     * <p>
     * Callers of this method must loop until the method succeeds.
     *
     * <pre>{@code
     * private Cluster cluster;
     * // Lines omitted...
     *
     * cluster.idleStrategy().reset();
     * while (!cluster.cancelTimer(correlationId))
     * {
     *     cluster.idleStrategy().idle();
     * }
     * }</pre>
     *
     * @param correlationId for the timer provided when it was scheduled. {@link Long#MAX_VALUE} not supported.
     * @return true if the request to cancel a timer has been sent or false if back-pressure is applied.
     * @see #scheduleTimer(long, long)
     */
    boolean cancelTimer(long correlationId);

    /**
     * Offer a message as ingress to the cluster for sequencing. This will happen efficiently over IPC to the
     * consensus module and have the cluster session of as the
     * {@link io.aeron.cluster.service.ClusteredServiceContainer.Configuration#SERVICE_ID_PROP_NAME}.
     * <p>
     * Callers of this method must loop until the method succeeds.
     *
     * <pre>{@code
     * private Cluster cluster;
     * // Lines omitted...
     *
     * cluster.idleStrategy().reset();
     * while(true)
     * {
     *     final long position = cluster.offer(buffer, offset, length);
     *     if (position > 0)
     *     {
     *         break;
     *     }
     *     else if (Publication.ADMIN_ACTION != position && Publication.BACK_PRESSURED != position)
     *     {
     *         throw new ClusterException("Internal offer failed: " + position);
     *     }
     *
     *     cluster.idleStrategy.idle();
     * }
     * }</pre>
     *
     * The cluster's idle strategy must be used in the body of the loop to allow for the clustered service to be
     * shutdown if required.
     *
     * @param buffer containing the message to be offered.
     * @param offset in the buffer at which the encoded message begins.
     * @param length in the buffer of the encoded message.
     * @return positive value if successful.
     * @see io.aeron.Publication#offer(DirectBuffer, int, int)
     */
    long offer(DirectBuffer buffer, int offset, int length);

    /**
     * Offer a message as ingress to the cluster for sequencing. This will happen efficiently over IPC to the
     * consensus module and have the cluster session of as the
     * {@link io.aeron.cluster.service.ClusteredServiceContainer.Configuration#SERVICE_ID_PROP_NAME}.
     * <p>
     * The first vector must be left free to be filled in for the session message header.
     * <p>
     * Callers of this method should loop until the method succeeds, see
     * {@link io.aeron.cluster.service.Cluster#offer(DirectBuffer, int, int)} for an example.
     *
     * @param vectors containing the message parts with the first left to be filled.
     * @return positive value if successful.
     * @see io.aeron.Publication#offer(DirectBufferVector[])
     */
    long offer(DirectBufferVector[] vectors);

    /**
     * Try to claim a range in the publication log into which a message can be written with zero copy semantics.
     * Once the message has been written then {@link BufferClaim#commit()} should be called thus making it available.
     * <p>
     * On successful claim, the Cluster session header will be written to the start of the claimed buffer section.
     * Clients <b>MUST</b> write into the claimed buffer region at offset + {@link AeronCluster#SESSION_HEADER_LENGTH}.
     * <p>
     * Callers of this method must loop until the method succeeds.
     *
     * <pre>{@code
     * private final BufferClaim bufferClaim = new BufferClaim();
     * private Cluster cluster;
     * // Lines omitted...
     *
     * final DirectBuffer srcBuffer = acquireMessage();
     * cluster.idleStrategy().reset();
     * while(true)
     * {
     *     final long position = cluster.tryClaim(length, bufferClaim);
     *     if (position > 0)
     *     {
     *         final MutableDirectBuffer buffer = bufferClaim.buffer();
     *         final int offset = bufferClaim.offset();
     *         // ensure that data is written after the session header
     *         buffer.putBytes(offset + AeronCluster.SESSION_HEADER_LENGTH, srcBuffer, 0, length);
     *         bufferClaim.commit();
     *         break;
     *     }
     *     else if (Publication.ADMIN_ACTION != position && Publication.BACK_PRESSURED != position)
     *     {
     *         throw new ClusterException("Internal tryClaim failed: " + position);
     *     }
     *
     *     cluster.idleStrategy.idle();
     * }
     * }</pre>
     *
     * @param length      of the range to claim, in bytes.
     * @param bufferClaim to be populated if the claim succeeds.
     * @return positive value if successful.
     * @throws IllegalArgumentException if the length is greater than {@link io.aeron.Publication#maxPayloadLength()}.
     * @see io.aeron.Publication#tryClaim(int, BufferClaim)
     * @see BufferClaim#commit()
     * @see BufferClaim#abort()
     */
    long tryClaim(int length, BufferClaim bufferClaim);

    /**
     * {@link IdleStrategy} which should be used by the service when it experiences back-pressure on egress,
     * closing sessions, making timer requests, or any long-running actions.
     *
     * @return the {@link IdleStrategy} which should be used by the service when it experiences back-pressure.
     */
    IdleStrategy idleStrategy();
}
