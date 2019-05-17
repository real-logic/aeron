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
package io.aeron.cluster.client;

import io.aeron.*;
import io.aeron.cluster.codecs.*;
import io.aeron.exceptions.ConcurrentConcludeException;
import io.aeron.exceptions.ConfigurationException;
import io.aeron.exceptions.TimeoutException;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.security.AuthenticationException;
import io.aeron.security.CredentialsSupplier;
import io.aeron.security.NullCredentialsSupplier;
import org.agrona.*;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.*;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;
import static org.agrona.SystemUtil.getDurationInNanos;

/**
 * Client for interacting with an Aeron Cluster.
 * <p>
 * A client will attempt to open a session and then offer ingress messages which are replicated to clustered services
 * for reliability. If the clustered service responds then response messages and events come back via the egress stream.
 * <p>
 * <b>Note:</b> Instances of this class are not threadsafe.
 */
@SuppressWarnings("unused")
public final class AeronCluster implements AutoCloseable
{
    /**
     * Length of a session message header for an ingress or egress with a cluster.
     */
    public static final int SESSION_HEADER_LENGTH =
        MessageHeaderEncoder.ENCODED_LENGTH + SessionMessageHeaderEncoder.BLOCK_LENGTH;


    static final int SEND_ATTEMPTS = 3;
    static final int FRAGMENT_LIMIT = 10;

    private final long clusterSessionId;
    private long leadershipTermId;
    private int leaderMemberId;
    private final Context ctx;
    private final Subscription subscription;
    private Publication publication;
    private final IdleStrategy idleStrategy;
    private final BufferClaim bufferClaim = new BufferClaim();
    private final UnsafeBuffer headerBuffer = new UnsafeBuffer(new byte[SESSION_HEADER_LENGTH]);
    private final DirectBufferVector headerVector = new DirectBufferVector(headerBuffer, 0, SESSION_HEADER_LENGTH);
    private final UnsafeBuffer keepaliveMsgBuffer;
    private final MessageHeaderEncoder messageHeaderEncoder;
    private final SessionMessageHeaderEncoder sessionMessageHeaderEncoder = new SessionMessageHeaderEncoder();
    private final SessionKeepAliveEncoder sessionKeepAliveEncoder = new SessionKeepAliveEncoder();
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final SessionMessageHeaderDecoder sessionMessageHeaderDecoder = new SessionMessageHeaderDecoder();
    private final NewLeaderEventDecoder newLeaderEventDecoder = new NewLeaderEventDecoder();
    private final SessionEventDecoder sessionEventDecoder = new SessionEventDecoder();
    private final FragmentAssembler fragmentAssembler;
    private final EgressListener egressListener;
    private final ControlledFragmentAssembler controlledFragmentAssembler;
    private final ControlledEgressListener controlledEgressListener;
    private Int2ObjectHashMap<MemberEndpoint> endpointByMemberIdMap;

    /**
     * Connect to the cluster using default configuration.
     *
     * @return allocated cluster client if the connection is successful.
     */
    public static AeronCluster connect()
    {
        return connect(new Context());
    }

    /**
     * Connect to the cluster providing {@link Context} for configuration.
     *
     * @param ctx for configuration.
     * @return allocated cluster client if the connection is successful.
     */
    public static AeronCluster connect(final AeronCluster.Context ctx)
    {
        Subscription subscription = null;
        AsyncConnect asyncConnect = null;
        try
        {
            ctx.conclude();

            final Aeron aeron = ctx.aeron();
            final long deadlineNs = aeron.context().nanoClock().nanoTime() + ctx.messageTimeoutNs();
            subscription = aeron.addSubscription(ctx.egressChannel(), ctx.egressStreamId());

            final IdleStrategy idleStrategy = ctx.idleStrategy();
            asyncConnect = new AsyncConnect(ctx, subscription, deadlineNs);
            final AgentInvoker aeronClientInvoker = aeron.conductorAgentInvoker();

            AeronCluster aeronCluster;
            while (null == (aeronCluster = asyncConnect.poll()))
            {
                if (null != aeronClientInvoker)
                {
                    aeronClientInvoker.invoke();
                }
                idleStrategy.idle();
            }

            return aeronCluster;
        }
        catch (final ConcurrentConcludeException ex)
        {
            throw ex;
        }
        catch (final Exception ex)
        {
            if (!ctx.ownsAeronClient())
            {
                CloseHelper.close(subscription);
                CloseHelper.close(asyncConnect);
            }

            ctx.close();

            throw ex;
        }
    }

    /**
     * Begin an attempt at creating a connection which can be completed by calling {@link AsyncConnect#poll()} until
     * it returns the client, before complete it will return null.
     *
     * @return the {@link AsyncConnect} that can be polled for completion.
     */
    public static AsyncConnect asyncConnect()
    {
        return asyncConnect(new Context());
    }

    /**
     * Begin an attempt at creating a connection which can be completed by calling {@link AsyncConnect#poll()} until
     * it returns the client, before complete it will return null.
     *
     * @param ctx for the cluster.
     * @return the {@link AsyncConnect} that can be polled for completion.
     */
    public static AsyncConnect asyncConnect(final Context ctx)
    {
        Subscription subscription = null;
        try
        {
            ctx.conclude();

            final long deadlineNs = ctx.aeron().context().nanoClock().nanoTime() + ctx.messageTimeoutNs();
            subscription = ctx.aeron().addSubscription(ctx.egressChannel(), ctx.egressStreamId());

            return new AsyncConnect(ctx, subscription, deadlineNs);
        }
        catch (final Exception ex)
        {
            if (!ctx.ownsAeronClient())
            {
                CloseHelper.quietClose(subscription);
            }

            ctx.close();

            throw ex;
        }
    }

    private AeronCluster(
        final Context ctx,
        final MessageHeaderEncoder messageHeaderEncoder,
        final Publication publication,
        final Subscription subscription,
        final Int2ObjectHashMap<MemberEndpoint> endpointByMemberIdMap,
        final long clusterSessionId,
        final long leadershipTermId,
        final int leaderMemberId)
    {
        this.ctx = ctx;
        this.messageHeaderEncoder = messageHeaderEncoder;
        this.subscription = subscription;
        this.endpointByMemberIdMap = endpointByMemberIdMap;
        this.clusterSessionId = clusterSessionId;
        this.leadershipTermId = leadershipTermId;
        this.leaderMemberId = leaderMemberId;
        this.publication = publication;

        this.idleStrategy = ctx.idleStrategy();
        this.egressListener = ctx.egressListener();
        this.fragmentAssembler = new FragmentAssembler(this::onFragment, 0, ctx.isDirectAssemblers());
        this.controlledEgressListener = ctx.controlledEgressListener();
        this.controlledFragmentAssembler = new ControlledFragmentAssembler(
            this::onControlledFragment, 0, ctx.isDirectAssemblers());

        sessionMessageHeaderEncoder
            .wrapAndApplyHeader(headerBuffer, 0, messageHeaderEncoder)
            .clusterSessionId(clusterSessionId)
            .leadershipTermId(leadershipTermId);

        keepaliveMsgBuffer = new UnsafeBuffer(new byte[
            MessageHeaderEncoder.ENCODED_LENGTH + SessionKeepAliveEncoder.BLOCK_LENGTH]);

        sessionKeepAliveEncoder
            .wrapAndApplyHeader(keepaliveMsgBuffer, 0, messageHeaderEncoder)
            .leadershipTermId(leadershipTermId)
            .clusterSessionId(clusterSessionId);
    }

    /**
     * Close session and release associated resources.
     */
    public void close()
    {
        if (null != publication && publication.isConnected())
        {
            closeSession();
        }

        if (!ctx.ownsAeronClient())
        {
            CloseHelper.close(subscription);
            CloseHelper.close(publication);
        }

        ctx.close();
    }

    /**
     * Get the context used to launch this cluster client.
     *
     * @return the context used to launch this cluster client.
     */
    public Context context()
    {
        return ctx;
    }

    /**
     * Cluster session id for the session that was opened as the result of a successful connect.
     *
     * @return session id for the session that was opened as the result of a successful connect.
     */
    public long clusterSessionId()
    {
        return clusterSessionId;
    }

    /**
     * Leadership term identity for the cluster. Advances with changing leadership.
     *
     * @return leadership term identity for the cluster.
     */
    public long leadershipTermId()
    {
        return leadershipTermId;
    }

    /**
     * Get the current leader member id for the cluster.
     *
     * @return the current leader member id for the cluster.
     */
    public int leaderMemberId()
    {
        return leaderMemberId;
    }

    /**
     * Get the raw {@link Publication} for sending to the cluster.
     * <p>
     * This can be wrapped with a {@link IngressSessionDecorator} for pre-pending the cluster session header to
     * messages.
     * {@link io.aeron.cluster.codecs.SessionMessageHeaderEncoder} or should be used for raw access.
     *
     * @return the raw {@link Publication} for connecting to the cluster.
     */
    public Publication ingressPublication()
    {
        return publication;
    }

    /**
     * Get the raw {@link Subscription} for receiving from the cluster.
     * <p>
     * The can be wrapped with a {@link EgressAdapter} for dispatching events from the cluster.
     * {@link io.aeron.cluster.codecs.SessionMessageHeaderDecoder} or should be used for raw access.
     *
     * @return the raw {@link Subscription} for receiving from the cluster.
     */
    public Subscription egressSubscription()
    {
        return subscription;
    }

    /**
     * Try to claim a range in the publication log into which a message can be written with zero copy semantics.
     * Once the message has been written then {@link BufferClaim#commit()} should be called thus making it available.
     * <p>
     * On successful claim, the Cluster ingress header will be written to the start of the claimed buffer section.
     * Clients <b>MUST</b> write into the claimed buffer region at offset + {@link AeronCluster#SESSION_HEADER_LENGTH}.
     * <pre>{@code
     *     final DirectBuffer srcBuffer = acquireMessage();
     *
     *     if (aeronCluster.tryClaim(length, bufferClaim) > 0L)
     *     {
     *         try
     *         {
     *              final MutableDirectBuffer buffer = bufferClaim.buffer();
     *              final int offset = bufferClaim.offset();
     *              // ensure that data is written at the correct offset
     *              buffer.putBytes(offset + AeronCluster.SESSION_HEADER_LENGTH, srcBuffer, 0, length);
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
     * @return The new stream position, otherwise a negative error value as specified in
     *         {@link io.aeron.Publication#tryClaim(int, BufferClaim)}.
     * @throws IllegalArgumentException if the length is greater than {@link io.aeron.Publication#maxPayloadLength()}.
     * @see Publication#tryClaim(int, BufferClaim)
     * @see BufferClaim#commit()
     * @see BufferClaim#abort()
     */
    public long tryClaim(final int length, final BufferClaim bufferClaim)
    {
        final long offset = publication.tryClaim(length + SESSION_HEADER_LENGTH, bufferClaim);
        if (offset > 0)
        {
            bufferClaim.putBytes(headerBuffer, 0, SESSION_HEADER_LENGTH);
        }

        return offset;
    }

    /**
     * Non-blocking publish of a partial buffer containing a message plus session header to a cluster.
     * <p>
     * This version of the method will set the timestamp value in the header to zero.
     *
     * @param buffer containing message.
     * @param offset offset in the buffer at which the encoded message begins.
     * @param length in bytes of the encoded message.
     * @return the same as {@link Publication#offer(DirectBuffer, int, int)}.
     */
    public long offer(final DirectBuffer buffer, final int offset, final int length)
    {
        return publication.offer(headerBuffer, 0, SESSION_HEADER_LENGTH, buffer, offset, length, null);
    }

    /**
     * Non-blocking publish by gathering buffer vectors into a message. The first vector will be replaced by the cluster
     * session message header so must be left unused.
     *
     * @param vectors which make up the message.
     * @return the same as {@link Publication#offer(DirectBufferVector[])}.
     * @see Publication#offer(DirectBufferVector[])
     */
    public long offer(final DirectBufferVector[] vectors)
    {
        vectors[0] = headerVector;

        return publication.offer(vectors, null);
    }

    /**
     * Send a keep alive message to the cluster to keep this session open.
     * <p>
     * <b>Note:</b> keepalives can fail during a leadership transition. The consumer should continue to call
     * {@link #pollEgress()} to ensure a connection to the new leader is established.
     *
     * @return true if successfully sent otherwise false.
     */
    public boolean sendKeepAlive()
    {
        idleStrategy.reset();
        int attempts = SEND_ATTEMPTS;

        while (true)
        {
            final long result = publication.offer(keepaliveMsgBuffer, 0, keepaliveMsgBuffer.capacity(), null);
            if (result > 0)
            {
                return true;
            }

            if (result == Publication.NOT_CONNECTED || result == Publication.CLOSED)
            {
                return false;
            }

            if (result == Publication.MAX_POSITION_EXCEEDED)
            {
                throw new ClusterException("unexpected publication state: " + result);
            }

            if (--attempts <= 0)
            {
                break;
            }

            idleStrategy.idle();
        }

        return false;
    }

    /**
     * Poll the {@link #egressSubscription()} for session messages which are dispatched to
     * {@link Context#egressListener()}.
     * <p>
     * <b>Note:</b> if {@link Context#egressListener()} is not set then a {@link ConfigurationException} could result.
     *
     * @return the number of fragments processed.
     */
    public int pollEgress()
    {
        return subscription.poll(fragmentAssembler, FRAGMENT_LIMIT);
    }

    /**
     * Poll the {@link #egressSubscription()} for session messages which are dispatched to
     * {@link Context#controlledEgressListener()}.
     * <p>
     * <b>Note:</b> if {@link Context#controlledEgressListener()} is not set then a {@link ConfigurationException}
     * could result.
     *
     * @return the number of fragments processed.
     */
    public int controlledPollEgress()
    {
        return subscription.controlledPoll(controlledFragmentAssembler, FRAGMENT_LIMIT);
    }

    /**
     * To be called when a new leader event is delivered. This method needs to be called when using the
     * {@link EgressAdapter} or {@link EgressPoller} rather than {@link #pollEgress()} method.
     *
     * @param clusterSessionId which must match {@link #clusterSessionId()}.
     * @param leadershipTermId that identifies the term for which the new leader has been elected.
     * @param leaderMemberId   which has become the new leader.
     * @param memberEndpoints  comma separated list of cluster members endpoints to connect to with the leader first.
     */
    public void onNewLeader(
        final long clusterSessionId,
        final long leadershipTermId,
        final int leaderMemberId,
        final String memberEndpoints)
    {
        if (clusterSessionId != this.clusterSessionId)
        {
            throw new ClusterException(
                "invalid clusterSessionId=" + clusterSessionId + " expected " + this.clusterSessionId);
        }

        this.leadershipTermId = leadershipTermId;
        this.leaderMemberId = leaderMemberId;
        sessionMessageHeaderEncoder.leadershipTermId(leadershipTermId);
        sessionKeepAliveEncoder.leadershipTermId(leadershipTermId);

        if (ctx.clusterMemberEndpoints() != null)
        {
            CloseHelper.close(publication);
            ctx.clusterMemberEndpoints(memberEndpoints);
            updateMemberEndpoints(memberEndpoints, leaderMemberId);
        }

        fragmentAssembler.clear();
        controlledFragmentAssembler.clear();
        egressListener.newLeader(clusterSessionId, leadershipTermId, leaderMemberId, memberEndpoints);
        controlledEgressListener.newLeader(clusterSessionId, leadershipTermId, leaderMemberId, memberEndpoints);
    }

    static Int2ObjectHashMap<MemberEndpoint> parseMemberEndpoints(final String memberEndpoints)
    {
        final Int2ObjectHashMap<MemberEndpoint> endpointByMemberIdMap = new Int2ObjectHashMap<>();

        if (null != memberEndpoints)
        {
            for (final String endpoint : memberEndpoints.split(","))
            {
                final int i = endpoint.indexOf('=');
                if (-1 == i)
                {
                    throw new ConfigurationException("endpoint missing '=' separator: " + memberEndpoints);
                }

                final int memberId = AsciiEncoding.parseIntAscii(endpoint, 0, i);
                endpointByMemberIdMap.put(memberId, new MemberEndpoint(memberId, endpoint.substring(i + 1)));
            }
        }

        return endpointByMemberIdMap;
    }

    private void updateMemberEndpoints(final String memberEndpoints, final int leaderMemberId)
    {
        final Int2ObjectHashMap<MemberEndpoint> tempMap = parseMemberEndpoints(memberEndpoints);
        final MemberEndpoint existingLeaderEndpoint = endpointByMemberIdMap.get(leaderMemberId);
        final MemberEndpoint leaderEndpoint = tempMap.get(leaderMemberId);

        if (null != existingLeaderEndpoint && null != existingLeaderEndpoint.publication &&
            existingLeaderEndpoint.endpoint.equals(leaderEndpoint.endpoint))
        {
            leaderEndpoint.publication = existingLeaderEndpoint.publication;
            publication = existingLeaderEndpoint.publication;
            existingLeaderEndpoint.publication = null;
        }

        if (null == leaderEndpoint.publication)
        {
            final ChannelUri channelUri = ChannelUri.parse(ctx.ingressChannel());
            channelUri.put(CommonContext.ENDPOINT_PARAM_NAME, leaderEndpoint.endpoint);
            publication = addIngressPublication(ctx, channelUri.toString(), ctx.ingressStreamId());
            leaderEndpoint.publication = publication;
        }

        endpointByMemberIdMap.values().forEach(MemberEndpoint::disconnect);
        endpointByMemberIdMap = tempMap;
    }

    private void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        final int templateId = messageHeaderDecoder.templateId();
        if (SessionMessageHeaderDecoder.TEMPLATE_ID == templateId)
        {
            sessionMessageHeaderDecoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                messageHeaderDecoder.blockLength(),
                messageHeaderDecoder.version());

            final long sessionId = sessionMessageHeaderDecoder.clusterSessionId();
            if (sessionId == clusterSessionId)
            {
                egressListener.onMessage(
                    sessionId,
                    sessionMessageHeaderDecoder.timestamp(),
                    buffer,
                    offset + SESSION_HEADER_LENGTH,
                    length - SESSION_HEADER_LENGTH,
                    header);
            }
        }
        else if (NewLeaderEventDecoder.TEMPLATE_ID == templateId)
        {
            newLeaderEventDecoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                messageHeaderDecoder.blockLength(),
                messageHeaderDecoder.version());

            final long sessionId = newLeaderEventDecoder.clusterSessionId();
            if (sessionId == clusterSessionId)
            {
                onNewLeader(
                    sessionId,
                    newLeaderEventDecoder.leadershipTermId(),
                    newLeaderEventDecoder.leaderMemberId(),
                    newLeaderEventDecoder.memberEndpoints());
            }
        }
        else if (SessionEventDecoder.TEMPLATE_ID == templateId)
        {
            sessionEventDecoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                messageHeaderDecoder.blockLength(),
                messageHeaderDecoder.version());

            final long sessionId = sessionEventDecoder.clusterSessionId();
            if (sessionId == clusterSessionId)
            {
                egressListener.sessionEvent(
                    sessionEventDecoder.correlationId(),
                    sessionId,
                    sessionEventDecoder.leadershipTermId(),
                    sessionEventDecoder.leaderMemberId(),
                    sessionEventDecoder.code(),
                    sessionEventDecoder.detail());
            }
        }
    }

    private ControlledFragmentHandler.Action onControlledFragment(
        final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        final int templateId = messageHeaderDecoder.templateId();
        if (SessionMessageHeaderDecoder.TEMPLATE_ID == templateId)
        {
            sessionMessageHeaderDecoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                messageHeaderDecoder.blockLength(),
                messageHeaderDecoder.version());

            final long sessionId = sessionMessageHeaderDecoder.clusterSessionId();
            if (sessionId == clusterSessionId)
            {
                return controlledEgressListener.onMessage(
                    sessionId,
                    sessionMessageHeaderDecoder.timestamp(),
                    buffer,
                    offset + SESSION_HEADER_LENGTH,
                    length - SESSION_HEADER_LENGTH,
                    header);
            }
        }
        else if (NewLeaderEventDecoder.TEMPLATE_ID == templateId)
        {
            newLeaderEventDecoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                messageHeaderDecoder.blockLength(),
                messageHeaderDecoder.version());

            final long sessionId = newLeaderEventDecoder.clusterSessionId();
            if (sessionId == clusterSessionId)
            {
                onNewLeader(
                    sessionId,
                    newLeaderEventDecoder.leadershipTermId(),
                    newLeaderEventDecoder.leaderMemberId(),
                    newLeaderEventDecoder.memberEndpoints());

                return ControlledFragmentHandler.Action.COMMIT;
            }
        }
        else if (SessionEventDecoder.TEMPLATE_ID == templateId)
        {
            sessionEventDecoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                messageHeaderDecoder.blockLength(),
                messageHeaderDecoder.version());

            final long sessionId = sessionEventDecoder.clusterSessionId();
            if (sessionId == clusterSessionId)
            {
                controlledEgressListener.sessionEvent(
                    sessionEventDecoder.correlationId(),
                    sessionId,
                    sessionEventDecoder.leadershipTermId(),
                    sessionEventDecoder.leaderMemberId(),
                    sessionEventDecoder.code(),
                    sessionEventDecoder.detail());
            }
        }

        return ControlledFragmentHandler.Action.CONTINUE;
    }

    private void closeSession()
    {
        idleStrategy.reset();
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + SessionCloseRequestEncoder.BLOCK_LENGTH;
        final SessionCloseRequestEncoder sessionCloseRequestEncoder = new SessionCloseRequestEncoder();
        int attempts = SEND_ATTEMPTS;

        while (true)
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                sessionCloseRequestEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .clusterSessionId(clusterSessionId);

                bufferClaim.commit();
                break;
            }

            checkResult(result);

            if (--attempts <= 0)
            {
                break;
            }

            idleStrategy.idle();
        }
    }

    private static Publication addIngressPublication(final Context ctx, final String channel, final int streamId)
    {
        if (ctx.isIngressExclusive())
        {
            return ctx.aeron().addExclusivePublication(channel, streamId);
        }
        else
        {
            return ctx.aeron().addPublication(channel, streamId);
        }
    }

    private static void checkResult(final long result)
    {
        if (result == Publication.NOT_CONNECTED ||
            result == Publication.CLOSED ||
            result == Publication.MAX_POSITION_EXCEEDED)
        {
            throw new ClusterException("unexpected publication state: " + result);
        }
    }

    /**
     * Configuration options for cluster client.
     */
    public static class Configuration
    {
        public static final int MAJOR_VERSION = 0;
        public static final int MINOR_VERSION = 0;
        public static final int PATCH_VERSION = 1;
        public static final int SEMANTIC_VERSION = SemanticVersion.compose(MAJOR_VERSION, MINOR_VERSION, PATCH_VERSION);

        /**
         * Timeout when waiting on a message to be sent or received.
         */
        public static final String MESSAGE_TIMEOUT_PROP_NAME = "aeron.cluster.message.timeout";

        /**
         * Default timeout when waiting on a message to be sent or received.
         */
        public static final long MESSAGE_TIMEOUT_DEFAULT_NS = TimeUnit.SECONDS.toNanos(5);

        /**
         * Property name for the comma separated list of cluster member endpoints for use with unicast. This is the
         * endpoint values which get substituted into the {@link #INGRESS_CHANNEL_PROP_NAME} when using UDP unicast.
         * <p>
         * {@code "0=endpoint,1=endpoint,2=endpoint"}
         * <p>
         * Each member of the list will be substituted for the endpoint in the {@link #INGRESS_CHANNEL_PROP_NAME} value.
         */
        public static final String CLUSTER_MEMBER_ENDPOINTS_PROP_NAME = "aeron.cluster.member.endpoints";

        /**
         * Default comma separated list of cluster member endpoints.
         */
        public static final String CLUSTER_MEMBER_ENDPOINTS_DEFAULT = null;

        /**
         * Channel for sending messages to a cluster. Ideally this will be a multicast address otherwise unicast will
         * be required and the {@link #CLUSTER_MEMBER_ENDPOINTS_PROP_NAME} is used to substitute the endpoints from
         * the {@link #CLUSTER_MEMBER_ENDPOINTS_PROP_NAME} list.
         */
        public static final String INGRESS_CHANNEL_PROP_NAME = "aeron.cluster.ingress.channel";

        /**
         * Channel for sending messages to a cluster.
         */
        public static final String INGRESS_CHANNEL_DEFAULT = "aeron:udp?endpoint=localhost:9010";

        /**
         * Stream id within a channel for sending messages to a cluster.
         */
        public static final String INGRESS_STREAM_ID_PROP_NAME = "aeron.cluster.ingress.stream.id";

        /**
         * Default stream id within a channel for sending messages to a cluster.
         */
        public static final int INGRESS_STREAM_ID_DEFAULT = 101;

        /**
         * Channel for receiving response messages from a cluster.
         */
        public static final String EGRESS_CHANNEL_PROP_NAME = "aeron.cluster.egress.channel";

        /**
         * Channel for receiving response messages from a cluster.
         */
        public static final String EGRESS_CHANNEL_DEFAULT = "aeron:udp?endpoint=localhost:9020";

        /**
         * Stream id within a channel for receiving messages from a cluster.
         */
        public static final String EGRESS_STREAM_ID_PROP_NAME = "aeron.cluster.egress.stream.id";

        /**
         * Default stream id within a channel for receiving messages from a cluster.
         */
        public static final int EGRESS_STREAM_ID_DEFAULT = 102;

        /**
         * The timeout in nanoseconds to wait for a message.
         *
         * @return timeout in nanoseconds to wait for a message.
         * @see #MESSAGE_TIMEOUT_PROP_NAME
         */
        public static long messageTimeoutNs()
        {
            return getDurationInNanos(MESSAGE_TIMEOUT_PROP_NAME, MESSAGE_TIMEOUT_DEFAULT_NS);
        }

        /**
         * The value {@link #CLUSTER_MEMBER_ENDPOINTS_DEFAULT} or system property
         * {@link #CLUSTER_MEMBER_ENDPOINTS_PROP_NAME} if set.
         *
         * @return {@link #CLUSTER_MEMBER_ENDPOINTS_DEFAULT} or system property
         * {@link #CLUSTER_MEMBER_ENDPOINTS_PROP_NAME} if set.
         */
        public static String clusterMemberEndpoints()
        {
            return System.getProperty(CLUSTER_MEMBER_ENDPOINTS_PROP_NAME, CLUSTER_MEMBER_ENDPOINTS_DEFAULT);
        }

        /**
         * The value {@link #INGRESS_CHANNEL_DEFAULT} or system property
         * {@link #INGRESS_CHANNEL_PROP_NAME} if set.
         *
         * @return {@link #INGRESS_CHANNEL_DEFAULT} or system property
         * {@link #INGRESS_CHANNEL_PROP_NAME} if set.
         */
        public static String ingressChannel()
        {
            return System.getProperty(INGRESS_CHANNEL_PROP_NAME, INGRESS_CHANNEL_DEFAULT);
        }

        /**
         * The value {@link #INGRESS_STREAM_ID_DEFAULT} or system property
         * {@link #INGRESS_STREAM_ID_PROP_NAME} if set.
         *
         * @return {@link #INGRESS_STREAM_ID_DEFAULT} or system property
         * {@link #INGRESS_STREAM_ID_PROP_NAME} if set.
         */
        public static int ingressStreamId()
        {
            return Integer.getInteger(INGRESS_STREAM_ID_PROP_NAME, INGRESS_STREAM_ID_DEFAULT);
        }

        /**
         * The value {@link #EGRESS_CHANNEL_DEFAULT} or system property
         * {@link #EGRESS_CHANNEL_PROP_NAME} if set.
         *
         * @return {@link #EGRESS_CHANNEL_DEFAULT} or system property
         * {@link #EGRESS_CHANNEL_PROP_NAME} if set.
         */
        public static String egressChannel()
        {
            return System.getProperty(EGRESS_CHANNEL_PROP_NAME, EGRESS_CHANNEL_DEFAULT);
        }

        /**
         * The value {@link #EGRESS_STREAM_ID_DEFAULT} or system property
         * {@link #EGRESS_STREAM_ID_PROP_NAME} if set.
         *
         * @return {@link #EGRESS_STREAM_ID_DEFAULT} or system property
         * {@link #EGRESS_STREAM_ID_PROP_NAME} if set.
         */
        public static int egressStreamId()
        {
            return Integer.getInteger(EGRESS_STREAM_ID_PROP_NAME, EGRESS_STREAM_ID_DEFAULT);
        }
    }

    /**
     * Context for cluster session and connection.
     */
    public static class Context implements Cloneable
    {
        /**
         * Using an integer because there is no support for boolean. 1 is concluded, 0 is not concluded.
         */
        private static final AtomicIntegerFieldUpdater<Context> IS_CONCLUDED_UPDATER = newUpdater(
            Context.class, "isConcluded");
        private volatile int isConcluded;

        private long messageTimeoutNs = Configuration.messageTimeoutNs();
        private String clusterMemberEndpoints = Configuration.clusterMemberEndpoints();
        private String ingressChannel = Configuration.ingressChannel();
        private int ingressStreamId = Configuration.ingressStreamId();
        private String egressChannel = Configuration.egressChannel();
        private int egressStreamId = Configuration.egressStreamId();
        private IdleStrategy idleStrategy;
        private String aeronDirectoryName = CommonContext.getAeronDirectoryName();
        private Aeron aeron;
        private CredentialsSupplier credentialsSupplier;
        private boolean ownsAeronClient = false;
        private boolean isIngressExclusive = false;
        private ErrorHandler errorHandler = Aeron.Configuration.DEFAULT_ERROR_HANDLER;
        private boolean isDirectAssemblers = false;
        private EgressListener egressListener;
        private ControlledEgressListener controlledEgressListener;

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

        public void conclude()
        {
            if (0 != IS_CONCLUDED_UPDATER.getAndSet(this, 1))
            {
                throw new ConcurrentConcludeException();
            }

            if (null == aeron)
            {
                aeron = Aeron.connect(
                    new Aeron.Context()
                        .aeronDirectoryName(aeronDirectoryName)
                        .errorHandler(errorHandler));

                ownsAeronClient = true;
            }

            if (null == idleStrategy)
            {
                idleStrategy = new BackoffIdleStrategy(1, 10, 1000, 1000);
            }

            if (null == credentialsSupplier)
            {
                credentialsSupplier = new NullCredentialsSupplier();
            }

            if (null == egressListener)
            {
                egressListener =
                    (clusterSessionId, timestamp, buffer, offset, length, header) ->
                    {
                        throw new ConfigurationException(
                            "egressListener must be specified on AeronCluster.Context");
                    };
            }

            if (null == controlledEgressListener)
            {
                controlledEgressListener =
                    (clusterSessionId, timestamp, buffer, offset, length, header) ->
                    {
                        throw new ConfigurationException(
                            "controlledEgressListener must be specified on AeronCluster.Context");
                    };
            }
        }

        /**
         * Set the message timeout in nanoseconds to wait for sending or receiving a message.
         *
         * @param messageTimeoutNs to wait for sending or receiving a message.
         * @return this for a fluent API.
         * @see Configuration#MESSAGE_TIMEOUT_PROP_NAME
         */
        public Context messageTimeoutNs(final long messageTimeoutNs)
        {
            this.messageTimeoutNs = messageTimeoutNs;
            return this;
        }

        /**
         * The message timeout in nanoseconds to wait for sending or receiving a message.
         *
         * @return the message timeout in nanoseconds to wait for sending or receiving a message.
         * @see Configuration#MESSAGE_TIMEOUT_PROP_NAME
         */
        public long messageTimeoutNs()
        {
            return messageTimeoutNs;
        }

        /**
         * The endpoints representing members for use with unicast to be substituted into the {@link #ingressChannel()}
         * for endpoints. A null value can be used when multicast where the {@link #ingressChannel()} contains the
         * multicast endpoint.
         *
         * @param clusterMembers which are all candidates to be leader.
         * @return this for a fluent API.
         * @see Configuration#CLUSTER_MEMBER_ENDPOINTS_PROP_NAME
         */
        public Context clusterMemberEndpoints(final String clusterMembers)
        {
            this.clusterMemberEndpoints = clusterMembers;
            return this;
        }

        /**
         * The endpoints representing members for use with unicast to be substituted into the {@link #ingressChannel()}
         * for endpoints. A null value can be used when multicast where the {@link #ingressChannel()} contains the
         * multicast endpoint.
         *
         * @return members of the cluster which are all candidates to be leader.
         * @see Configuration#CLUSTER_MEMBER_ENDPOINTS_PROP_NAME
         */
        public String clusterMemberEndpoints()
        {
            return clusterMemberEndpoints;
        }

        /**
         * Set the channel parameter for the ingress channel.
         * <p>
         * The endpoints representing members for use with unicast are substituted from the
         * {@link #clusterMemberEndpoints()} for endpoints. A null value can be used when multicast
         * where this contains the multicast endpoint.
         *
         * @param channel parameter for the ingress channel.
         * @return this for a fluent API.
         * @see Configuration#INGRESS_CHANNEL_PROP_NAME
         */
        public Context ingressChannel(final String channel)
        {
            ingressChannel = channel;
            return this;
        }

        /**
         * Get the channel parameter for the ingress channel.
         * <p>
         * The endpoints representing members for use with unicast are substituted from the
         * {@link #clusterMemberEndpoints()} for endpoints. A null value can be used when multicast
         * where this contains the multicast endpoint.
         *
         * @return the channel parameter for the ingress channel.
         * @see Configuration#INGRESS_CHANNEL_PROP_NAME
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
         * @see Configuration#INGRESS_STREAM_ID_PROP_NAME
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
         * @see Configuration#INGRESS_STREAM_ID_PROP_NAME
         */
        public int ingressStreamId()
        {
            return ingressStreamId;
        }

        /**
         * Set the channel parameter for the egress channel.
         *
         * @param channel parameter for the egress channel.
         * @return this for a fluent API.
         * @see Configuration#EGRESS_CHANNEL_PROP_NAME
         */
        public Context egressChannel(final String channel)
        {
            egressChannel = channel;
            return this;
        }

        /**
         * Get the channel parameter for the egress channel.
         *
         * @return the channel parameter for the egress channel.
         * @see Configuration#EGRESS_CHANNEL_PROP_NAME
         */
        public String egressChannel()
        {
            return egressChannel;
        }

        /**
         * Set the stream id for the egress channel.
         *
         * @param streamId for the egress channel.
         * @return this for a fluent API
         * @see Configuration#EGRESS_STREAM_ID_PROP_NAME
         */
        public Context egressStreamId(final int streamId)
        {
            egressStreamId = streamId;
            return this;
        }

        /**
         * Get the stream id for the egress channel.
         *
         * @return the stream id for the egress channel.
         * @see Configuration#EGRESS_STREAM_ID_PROP_NAME
         */
        public int egressStreamId()
        {
            return egressStreamId;
        }

        /**
         * Set the {@link IdleStrategy} used when waiting for responses.
         *
         * @param idleStrategy used when waiting for responses.
         * @return this for a fluent API.
         */
        public Context idleStrategy(final IdleStrategy idleStrategy)
        {
            this.idleStrategy = idleStrategy;
            return this;
        }

        /**
         * Get the {@link IdleStrategy} used when waiting for responses.
         *
         * @return the {@link IdleStrategy} used when waiting for responses.
         */
        public IdleStrategy idleStrategy()
        {
            return idleStrategy;
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
         * {@link Aeron} client for communicating with the local Media Driver.
         * <p>
         * This client will be closed when the {@link AeronCluster#close()} or {@link #close()} methods are called if
         * {@link #ownsAeronClient()} is true.
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
         * Is ingress to the cluster exclusively from a single thread for this client?
         *
         * @param isIngressExclusive true if ingress to the cluster is exclusively from a single thread for this client?
         * @return this for a fluent API.
         */
        public Context isIngressExclusive(final boolean isIngressExclusive)
        {
            this.isIngressExclusive = isIngressExclusive;
            return this;
        }

        /**
         * Is ingress to the cluster exclusively from a single thread for this client?
         *
         * @return true if ingress to the cluster exclusively from a single thread for this client?
         */
        public boolean isIngressExclusive()
        {
            return isIngressExclusive;
        }

        /**
         * Set the {@link CredentialsSupplier} to be used for authentication with the cluster.
         *
         * @param credentialsSupplier to be used for authentication with the cluster.
         * @return this for fluent API.
         */
        public Context credentialsSupplier(final CredentialsSupplier credentialsSupplier)
        {
            this.credentialsSupplier = credentialsSupplier;
            return this;
        }

        /**
         * Get the {@link CredentialsSupplier} to be used for authentication with the cluster.
         *
         * @return the {@link CredentialsSupplier} to be used for authentication with the cluster.
         */
        public CredentialsSupplier credentialsSupplier()
        {
            return credentialsSupplier;
        }

        /**
         * Set the {@link ErrorHandler} to be used for handling any exceptions.
         *
         * @param errorHandler Method to handle objects of type Throwable.
         * @return this for fluent API.
         */
        public Context errorHandler(final ErrorHandler errorHandler)
        {
            this.errorHandler = errorHandler;
            return this;
        }

        /**
         * Get the {@link ErrorHandler} to be used for handling any exceptions.
         *
         * @return The {@link ErrorHandler} to be used for handling any exceptions.
         */
        public ErrorHandler errorHandler()
        {
            return errorHandler;
        }

        /**
         * Are direct buffers used for fragment assembly on egress?
         *
         * @param isDirectAssemblers true if direct buffers used for fragment assembly on egress.
         * @return this for a fluent API.
         */
        public Context isDirectAssemblers(final boolean isDirectAssemblers)
        {
            this.isDirectAssemblers = isDirectAssemblers;
            return this;
        }

        /**
         * Are direct buffers used for fragment assembly on egress?
         *
         * @return true if direct buffers used for fragment assembly on egress.
         */
        public boolean isDirectAssemblers()
        {
            return isDirectAssemblers;
        }

        /**
         * Set the {@link EgressListener} function that will be called when polling for egress via
         * {@link AeronCluster#pollEgress()}.
         * <p>
         * Only {@link EgressListener#onMessage(long, long, DirectBuffer, int, int, Header)} will be dispatched
         * when using {@link AeronCluster#pollEgress()}.
         *
         * @param listener function that will be called when polling for egress via {@link AeronCluster#pollEgress()}.
         * @return this for a fluent API.
         */
        public Context egressListener(final EgressListener listener)
        {
            this.egressListener = listener;
            return this;
        }

        /**
         * Get the {@link EgressListener} function that will be called when polling for egress via
         * {@link AeronCluster#pollEgress()}.
         *
         * @return the {@link EgressListener} function that will be called when polling for egress via
         * {@link AeronCluster#pollEgress()}.
         */
        public EgressListener egressListener()
        {
            return egressListener;
        }

        /**
         * Set the {@link ControlledEgressListener} function that will be called when polling for egress via
         * {@link AeronCluster#controlledPollEgress()}.
         * <p>
         * Only {@link ControlledEgressListener#onMessage(long, long, DirectBuffer, int, int, Header)} will be
         * dispatched when using {@link AeronCluster#controlledPollEgress()}.
         *
         * @param listener function that will be called when polling for egress via
         *                 {@link AeronCluster#controlledPollEgress()}.
         * @return this for a fluent API.
         */
        public Context controlledEgressListener(final ControlledEgressListener listener)
        {
            this.controlledEgressListener = listener;
            return this;
        }

        /**
         * Get the {@link ControlledEgressListener} function that will be called when polling for egress via
         * {@link AeronCluster#controlledPollEgress()}.
         *
         * @return the {@link ControlledEgressListener} function that will be called when polling for egress via
         * {@link AeronCluster#controlledPollEgress()}.
         */
        public ControlledEgressListener controlledEgressListener()
        {
            return controlledEgressListener;
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
        }
    }

    /**
     * Allows for the async establishment of a cluster session. {@link #poll()} should be called repeatedly until
     * it returns a non-null value with the new {@link AeronCluster} client. On error {@link #close()} should be called
     * to clean up allocated resources.
     */
    public static class AsyncConnect implements AutoCloseable
    {
        private final long deadlineNs;
        private long correlationId;
        private long clusterSessionId;
        private long leadershipTermId;
        private int leaderMemberId;
        private int step = 0;

        private final Context ctx;
        private final NanoClock nanoClock;
        private final EgressPoller egressPoller;
        private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
        private Int2ObjectHashMap<MemberEndpoint> endpointByMemberIdMap;
        private Publication ingressPublication;

        AsyncConnect(final Context ctx, final Subscription egressSubscription, final long deadlineNs)
        {
            this.ctx = ctx;

            endpointByMemberIdMap = parseMemberEndpoints(ctx.clusterMemberEndpoints());
            egressPoller = new EgressPoller(egressSubscription, FRAGMENT_LIMIT);
            nanoClock = ctx.aeron().context().nanoClock();
            this.deadlineNs = deadlineNs;
        }

        /**
         * Close allocated resources. Must be called on error. On success this is a no op.
         */
        public void close()
        {
            if (5 != step)
            {
                CloseHelper.close(ingressPublication);
                endpointByMemberIdMap.values().forEach(MemberEndpoint::disconnect);
                ctx.close();
            }
        }

        /**
         * Indicates which step in the connect process has been reached.
         *
         * @return which step in the connect process has been reached.
         */
        public int step()
        {
            return step;
        }

        private void step(final int step)
        {
            //System.out.println(this.step + " -> " + step);
            this.step = step;
        }

        /**
         * Poll to advance steps in the connection until complete or error.
         *
         * @return null if not yet complete then {@link AeronCluster} when complete.
         */
        public AeronCluster poll()
        {
            AeronCluster aeronCluster = null;
            checkDeadline();

            switch (step)
            {
                case 0:
                    createIngressPublications();
                    break;

                case 1:
                    awaitPublicationConnected();
                    break;

                case 2:
                    sendMessage();
                    break;

                case 3:
                    pollResponse();
                    break;
            }

            if (4 == step)
            {
                aeronCluster = newInstance();
                ingressPublication = null;
                final MemberEndpoint endpoint = endpointByMemberIdMap.get(leaderMemberId);
                if (null != endpoint)
                {
                    endpoint.publication = null;
                }
                endpointByMemberIdMap.values().forEach(MemberEndpoint::disconnect);

                step(5);
            }

            return aeronCluster;
        }

        private void checkDeadline()
        {
            if (Thread.interrupted())
            {
                LangUtil.rethrowUnchecked(new InterruptedException());
            }

            if (deadlineNs - nanoClock.nanoTime() < 0)
            {
                throw new TimeoutException("connect timeout, step=" + step);
            }
        }

        private void createIngressPublications()
        {
            if (ctx.clusterMemberEndpoints() == null)
            {
                ingressPublication = addIngressPublication(ctx, ctx.ingressChannel(), ctx.ingressStreamId());
            }
            else
            {
                final ChannelUri channelUri = ChannelUri.parse(ctx.ingressChannel());
                for (final MemberEndpoint member : endpointByMemberIdMap.values())
                {
                    channelUri.put(CommonContext.ENDPOINT_PARAM_NAME, member.endpoint);
                    member.publication = addIngressPublication(ctx, channelUri.toString(), ctx.ingressStreamId());
                }
            }

            step(1);
        }

        private void awaitPublicationConnected()
        {
            if (null != ingressPublication && ingressPublication.isConnected())
            {
                prepareConnectRequest();
            }
            else
            {
                for (final MemberEndpoint member : endpointByMemberIdMap.values())
                {
                    if (null != member.publication && member.publication.isConnected())
                    {
                        ingressPublication = member.publication;
                        prepareConnectRequest();
                        break;
                    }
                }
            }
        }

        private void prepareConnectRequest()
        {
            correlationId = ctx.aeron().nextCorrelationId();
            final byte[] encodedCredentials = ctx.credentialsSupplier().encodedCredentials();

            new SessionConnectRequestEncoder()
                .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
                .correlationId(correlationId)
                .responseStreamId(ctx.egressStreamId())
                .version(Configuration.SEMANTIC_VERSION)
                .responseChannel(ctx.egressChannel())
                .putEncodedCredentials(encodedCredentials, 0, encodedCredentials.length);

            step(2);
        }

        private void sendMessage()
        {
            final long result = ingressPublication.offer(buffer);
            if (result > 0)
            {
                step(3);
            }
            else if (Publication.CLOSED == result)
            {
                throw new ClusterException("unexpected close from cluster");
            }
        }

        private void pollResponse()
        {
            if (egressPoller.poll() > 0 &&
                egressPoller.isPollComplete() &&
                egressPoller.correlationId() == correlationId)
            {
                if (egressPoller.isChallenged())
                {
                    clusterSessionId = egressPoller.clusterSessionId();
                    prepareChallengeResponse(ctx.credentialsSupplier().onChallenge(egressPoller.encodedChallenge()));
                    step(2);
                    return;
                }

                switch (egressPoller.eventCode())
                {
                    case OK:
                        leadershipTermId = egressPoller.leadershipTermId();
                        leaderMemberId = egressPoller.leaderMemberId();
                        clusterSessionId = egressPoller.clusterSessionId();
                        step(4);
                        break;

                    case ERROR:
                        throw new ClusterException(egressPoller.detail());

                    case REDIRECT:
                        updateMembers();
                        break;

                    case AUTHENTICATION_REJECTED:
                        throw new AuthenticationException(egressPoller.detail());
                }
            }
        }

        private void prepareChallengeResponse(final byte[] encodedCredentials)
        {
            correlationId = ctx.aeron().nextCorrelationId();

            new ChallengeResponseEncoder()
                .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
                .correlationId(correlationId)
                .clusterSessionId(clusterSessionId)
                .putEncodedCredentials(encodedCredentials, 0, encodedCredentials.length);

            step(2);
        }

        private void updateMembers()
        {
            leaderMemberId = egressPoller.leaderMemberId();
            final MemberEndpoint leaderEndpoint = endpointByMemberIdMap.get(leaderMemberId);
            if (null != leaderEndpoint)
            {
                ingressPublication = leaderEndpoint.publication;
                leaderEndpoint.publication = null;
                endpointByMemberIdMap.values().forEach(MemberEndpoint::disconnect);
                endpointByMemberIdMap = parseMemberEndpoints(egressPoller.detail());
            }
            else
            {
                endpointByMemberIdMap.values().forEach(MemberEndpoint::disconnect);
                endpointByMemberIdMap = parseMemberEndpoints(egressPoller.detail());

                final MemberEndpoint memberEndpoint = endpointByMemberIdMap.get(leaderMemberId);
                final ChannelUri channelUri = ChannelUri.parse(ctx.ingressChannel());
                channelUri.put(CommonContext.ENDPOINT_PARAM_NAME, memberEndpoint.endpoint);
                memberEndpoint.publication = addIngressPublication(ctx, channelUri.toString(), ctx.ingressStreamId());
                ingressPublication = memberEndpoint.publication;
            }

            step(1);
        }

        private AeronCluster newInstance()
        {
            return new AeronCluster(
                ctx,
                messageHeaderEncoder,
                ingressPublication,
                egressPoller.subscription(),
                endpointByMemberIdMap,
                clusterSessionId,
                leadershipTermId,
                leaderMemberId);
        }
    }

    static final class MemberEndpoint
    {
        final int memberId;
        final String endpoint;
        Publication publication;

        MemberEndpoint(final int memberId, final String endpoint)
        {
            this.memberId = memberId;
            this.endpoint = endpoint;
        }

        void disconnect()
        {
            CloseHelper.close(publication);
            publication = null;
        }

        public String toString()
        {
            return "MemberEndpoint{" +
                "memberId=" + memberId +
                ", endpoint='" + endpoint + '\'' +
                ", publication=" + publication +
                '}';
        }
    }
}

