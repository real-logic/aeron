/*
 * Copyright 2014-2019 Real Logic Ltd.
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
import io.aeron.DirectBufferVector;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

import java.util.Collection;

/**
 * Interface for a {@link ClusteredService} to interact with cluster hosting it.
 */
public interface Cluster
{
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

        static final Role[] ROLES;

        static
        {
            final Role[] roles = values();
            ROLES = new Role[roles.length];
            for (final Role role : roles)
            {
                final int code = role.code();
                if (null != ROLES[code])
                {
                    throw new ClusterException("code already in use: " + code);
                }

                ROLES[code] = role;
            }
        }

        private final int code;

        Role(final int code)
        {
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
        public static Role get(final int code)
        {
            if (code < 0 || code > (ROLES.length - 1))
            {
                throw new IllegalStateException("Invalid role counter code: " + code);
            }

            return ROLES[code];
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
     * Get the {@link Aeron} client used by the cluster.
     *
     * @return the {@link Aeron} client used by the cluster.
     */
    Aeron aeron();

    /**
     * Get the  {@link ClusteredServiceContainer.Context} under which the container is running.
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
     *
     * @return the current collection of cluster client sessions.
     */
    Collection<ClientSession> clientSessions();

    /**
     * Request the close of a {@link ClientSession} by sending the request to the consensus module.
     *
     * @param clusterSessionId to be closed.
     * @return true if the event to close a session was sent or false if back pressure was applied.
     * @throws ClusterException if the clusterSessionId is not recognised.
     */
    boolean closeSession(long clusterSessionId);

    /**
     * Current Epoch time in milliseconds.
     *
     * @return Epoch time in milliseconds.
     */
    long timeMs();

    /**
     * Position the log has reached in bytes as of the current message.
     *
     * @return position the log has reached in bytes as of the current message.
     */
    long logPosition();

    /**
     * Schedule a timer for a given deadline and provide a correlation id to identify the timer when it expires or
     * for cancellation.
     * <p>
     * If the correlationId is for an existing scheduled timer then it will be reschedule to the new deadline. However
     * it is best do generate correlationIds in a monotonic fashion and be aware of potential clashes with other
     * services in the same cluster. Service isolation can be achieved by using the upper bits for service id.
     * <p>
     * Timers should only be scheduled or cancelled in the context of processing a
     * {@link ClusteredService#onSessionMessage(ClientSession, long, DirectBuffer, int, int, Header)},
     * {@link ClusteredService#onTimerEvent(long, long)},
     * {@link ClusteredService#onSessionOpen(ClientSession, long)}, or
     * {@link ClusteredService#onSessionClose(ClientSession, long, CloseReason)}.
     * If applied to other events then they are not guaranteed to be reliable.
     *
     * @param correlationId to identify the timer when it expires.
     * @param deadlineMs    epoch time in milliseconds after which the timer will fire.
     * @return true if the event to schedule a timer request has been sent or false if back pressure is applied.
     * @see #cancelTimer(long)
     */
    boolean scheduleTimer(long correlationId, long deadlineMs);

    /**
     * Cancel a previous scheduled timer.
     * <p>
     * Timers should only be scheduled or cancelled in the context of processing a
     * {@link ClusteredService#onSessionMessage(ClientSession, long, DirectBuffer, int, int, Header)},
     * {@link ClusteredService#onTimerEvent(long, long)},
     * {@link ClusteredService#onSessionOpen(ClientSession, long)}, or
     * {@link ClusteredService#onSessionClose(ClientSession, long, CloseReason)}.
     * If applied to other events then they are not guaranteed to be reliable.
     *
     * @param correlationId for the timer provided when it was scheduled.
     * @return true if the event to cancel request has been sent or false if back pressure is applied.
     * @see #scheduleTimer(long, long)
     */
    boolean cancelTimer(long correlationId);

    /**
     * Offer a message as ingress to the cluster for sequencing. This will happen efficiently over IPC to the
     * consensus module and have the cluster session of as the negative value of the
     * {@link io.aeron.cluster.service.ClusteredServiceContainer.Configuration#SERVICE_ID_PROP_NAME}.
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
     * consensus module and have the cluster session of as the negative value of the
     * {@link io.aeron.cluster.service.ClusteredServiceContainer.Configuration#SERVICE_ID_PROP_NAME}.
     * <p>
     * The first vector must be left free to be filled in for the session message header.
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
     * <pre>{@code
     *     final DirectBuffer srcBuffer = acquireMessage();
     *
     *     if (cluster.tryClaim(length, bufferClaim))
     *     {
     *         try
     *         {
     *              final MutableDirectBuffer buffer = bufferClaim.buffer();
     *              final int offset = bufferClaim.offset();
     *              // ensure that data is written at the correct offset
     *              buffer.putBytes(offset + ClientSession.SESSION_HEADER_LENGTH, srcBuffer, 0, length);
     *         }
     *         finally
     *         {
     *             bufferClaim.commit();
     *         }
     *     }
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
     * Should be called by the service when it experiences back-pressure on egress, closing sessions, or making
     * timer requests.
     */
    void idle();

    /**
     * Should be called by the service when it experiences back-pressure on egress, closing sessions, or making
     * timer requests.
     *
     * @param workCount a value of 0 will reset the idle strategy is a progressive back-off has been applied.
     */
    void idle(int workCount);
}
