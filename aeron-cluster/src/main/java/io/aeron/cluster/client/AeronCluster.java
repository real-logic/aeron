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
package io.aeron.cluster.client;

import io.aeron.*;
import io.aeron.cluster.codecs.*;
import io.aeron.exceptions.AeronException;
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
 * for reliability. If the clustered service responds then response messages and events are sent via the egress stream.
 * <p>
 * <b>Note:</b> Instances of this class are not threadsafe.
 */
public final class AeronCluster implements AutoCloseable
{
    /**
     * Length of a session message header for cluster ingress or egress.
     */
    public static final int SESSION_HEADER_LENGTH =
        MessageHeaderEncoder.ENCODED_LENGTH + SessionMessageHeaderEncoder.BLOCK_LENGTH;

    private static final int SEND_ATTEMPTS = 3;
    private static final int FRAGMENT_LIMIT = 10;

    private final long clusterSessionId;
    private long leadershipTermId;
    private int leaderMemberId;
    private boolean isClosed;
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
    private Int2ObjectHashMap<MemberIngress> endpointByIdMap;

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
            subscription = aeron.addSubscription(ctx.egressChannel(), ctx.egressStreamId());

            final long deadlineNs = aeron.context().nanoClock().nanoTime() + ctx.messageTimeoutNs();
            final IdleStrategy idleStrategy = ctx.idleStrategy();
            asyncConnect = new AsyncConnect(ctx, subscription, deadlineNs);
            final AgentInvoker aeronClientInvoker = aeron.conductorAgentInvoker();

            AeronCluster aeronCluster;
            int step = asyncConnect.step();
            while (null == (aeronCluster = asyncConnect.poll()))
            {
                if (null != aeronClientInvoker)
                {
                    aeronClientInvoker.invoke();
                }

                if (step != asyncConnect.step())
                {
                    step = asyncConnect.step();
                    idleStrategy.reset();
                }
                else
                {
                    idleStrategy.idle();
                }
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
                CloseHelper.quietCloseAll(subscription, asyncConnect);
            }

            CloseHelper.quietClose(ctx::close);

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

    AeronCluster(
        final Context ctx,
        final MessageHeaderEncoder messageHeaderEncoder,
        final Publication publication,
        final Subscription subscription,
        final Int2ObjectHashMap<MemberIngress> endpointByIdMap,
        final long clusterSessionId,
        final long leadershipTermId,
        final int leaderMemberId)
    {
        this.ctx = ctx;
        this.messageHeaderEncoder = messageHeaderEncoder;
        this.subscription = subscription;
        this.endpointByIdMap = endpointByIdMap;
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
        if (null != publication && publication.isConnected() && !isClosed)
        {
            closeSession();
        }

        if (!ctx.ownsAeronClient())
        {
            final ErrorHandler errorHandler = ctx.errorHandler();
            CloseHelper.close(errorHandler, subscription);
            CloseHelper.close(errorHandler, publication);
        }

        isClosed = true;
        ctx.close();
    }

    /**
     * Is the client closed? The client can be closed by calling {@link #close()} or the cluster sending an event.
     *
     * @return true if closed otherwise false.
     */
    public boolean isClosed()
    {
        return isClosed;
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
     * {@link io.aeron.cluster.codecs.SessionMessageHeaderEncoder} should be used for raw access.
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
     * {@link io.aeron.cluster.codecs.SessionMessageHeaderDecoder} should be used for raw access.
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
     * @param length      of the range to claim in bytes. The additional bytes for the session header will be added.
     * @param bufferClaim to be populated if the claim succeeds.
     * @return The new stream position, otherwise a negative error value as specified in
     * {@link io.aeron.Publication#tryClaim(int, BufferClaim)}.
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
     * <b>Note:</b> Sending keep-alive can fail during a leadership transition. The application should continue to call
     * {@link #pollEgress()} to ensure a connection to the new leader is established.
     *
     * @return true if successfully sent otherwise false if back pressured.
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

            if (result == Publication.CLOSED)
            {
                throw new ClusterException("ingress publication is closed");
            }

            if (result == Publication.MAX_POSITION_EXCEEDED)
            {
                throw new ClusterException("max position exceeded");
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
        final int fragments = subscription.poll(fragmentAssembler, FRAGMENT_LIMIT);
        if (isClosed)
        {
            close();
        }

        return fragments;
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
        final int fragments = subscription.controlledPoll(controlledFragmentAssembler, FRAGMENT_LIMIT);
        if (isClosed)
        {
            close();
        }

        return fragments;
    }

    /**
     * To be called when a new leader event is delivered. This method needs to be called when using the
     * {@link EgressAdapter} or {@link EgressPoller} rather than {@link #pollEgress()} method.
     *
     * @param clusterSessionId which must match {@link #clusterSessionId()}.
     * @param leadershipTermId that identifies the term for which the new leader has been elected.
     * @param leaderMemberId   which has become the new leader.
     * @param ingressEndpoints comma separated list of cluster ingress endpoints to connect to with the leader first.
     */
    public void onNewLeader(
        final long clusterSessionId,
        final long leadershipTermId,
        final int leaderMemberId,
        final String ingressEndpoints)
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

        if (ctx.ingressEndpoints() != null)
        {
            CloseHelper.close(publication);
            ctx.ingressEndpoints(ingressEndpoints);
            updateMemberEndpoints(ingressEndpoints, leaderMemberId);
        }

        fragmentAssembler.clear();
        controlledFragmentAssembler.clear();
        egressListener.onNewLeader(clusterSessionId, leadershipTermId, leaderMemberId, ingressEndpoints);
        controlledEgressListener.onNewLeader(clusterSessionId, leadershipTermId, leaderMemberId, ingressEndpoints);
    }

    static Int2ObjectHashMap<MemberIngress> parseIngressEndpoints(final String endpoints)
    {
        final Int2ObjectHashMap<MemberIngress> endpointByIdMap = new Int2ObjectHashMap<>();

        if (null != endpoints)
        {
            for (final String endpoint : endpoints.split(","))
            {
                final int i = endpoint.indexOf('=');
                if (-1 == i)
                {
                    throw new ConfigurationException("endpoint missing '=' separator: " + endpoints);
                }

                final int memberId = AsciiEncoding.parseIntAscii(endpoint, 0, i);
                endpointByIdMap.put(memberId, new MemberIngress(memberId, endpoint.substring(i + 1)));
            }
        }

        return endpointByIdMap;
    }

    private void updateMemberEndpoints(final String ingressEndpoints, final int leaderMemberId)
    {
        final Int2ObjectHashMap<MemberIngress> map = parseIngressEndpoints(ingressEndpoints);
        final MemberIngress existingLeader = endpointByIdMap.get(leaderMemberId);
        final MemberIngress newLeader = map.get(leaderMemberId);

        if (null != existingLeader && null != existingLeader.publication &&
            existingLeader.endpoint.equals(newLeader.endpoint))
        {
            newLeader.publication = existingLeader.publication;
            publication = existingLeader.publication;
            existingLeader.publication = null;
        }

        if (null == newLeader.publication)
        {
            final ChannelUri channelUri = ChannelUri.parse(ctx.ingressChannel());
            channelUri.put(CommonContext.ENDPOINT_PARAM_NAME, newLeader.endpoint);
            publication = addIngressPublication(ctx, channelUri.toString(), ctx.ingressStreamId());
            newLeader.publication = publication;
        }

        CloseHelper.closeAll(endpointByIdMap.values());
        endpointByIdMap = map;
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
                    newLeaderEventDecoder.ingressEndpoints());
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
                final EventCode code = sessionEventDecoder.code();
                if (EventCode.CLOSED == code)
                {
                    isClosed = true;
                }

                egressListener.onSessionEvent(
                    sessionEventDecoder.correlationId(),
                    sessionId,
                    sessionEventDecoder.leadershipTermId(),
                    sessionEventDecoder.leaderMemberId(),
                    code,
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
                    newLeaderEventDecoder.ingressEndpoints());

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
                final EventCode code = sessionEventDecoder.code();
                if (EventCode.CLOSED == code)
                {
                    isClosed = true;
                }

                controlledEgressListener.onSessionEvent(
                    sessionEventDecoder.correlationId(),
                    sessionId,
                    sessionEventDecoder.leadershipTermId(),
                    sessionEventDecoder.leaderMemberId(),
                    code,
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
                    .leadershipTermId(leadershipTermId)
                    .clusterSessionId(clusterSessionId);

                bufferClaim.commit();
                break;
            }

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

    /**
     * Configuration options for cluster client.
     */
    public static final class Configuration
    {
        /**
         * Major version of the network protocol from client to consensus module. If these don't match then client
         * and archive consensus module are not compatible.
         */
        public static final int PROTOCOL_MAJOR_VERSION = 0;

        /**
         * Minor version of the network protocol from client to consensus module. If these don't match then some
         * features may not be available.
         */
        public static final int PROTOCOL_MINOR_VERSION = 1;

        /**
         * Patch version of the network protocol from client to consensus module. If these don't match then bug fixes
         * may not have been applied.
         */
        public static final int PROTOCOL_PATCH_VERSION = 1;

        /**
         * Combined semantic version for the client to consensus module protocol.
         *
         * @see SemanticVersion
         */
        public static final int PROTOCOL_SEMANTIC_VERSION = SemanticVersion.compose(
            PROTOCOL_MAJOR_VERSION, PROTOCOL_MINOR_VERSION, PROTOCOL_PATCH_VERSION);

        /**
         * Timeout when waiting on a message to be sent or received.
         */
        public static final String MESSAGE_TIMEOUT_PROP_NAME = "aeron.cluster.message.timeout";

        /**
         * Default timeout when waiting on a message to be sent or received.
         */
        public static final long MESSAGE_TIMEOUT_DEFAULT_NS = TimeUnit.SECONDS.toNanos(5);

        /**
         * Property name for the comma separated list of cluster ingress endpoints for use with unicast. This is the
         * endpoint values which get substituted into the {@link #INGRESS_CHANNEL_PROP_NAME} when using UDP unicast.
         * <p>
         * {@code "0=endpoint,1=endpoint,2=endpoint"}
         * <p>
         * Each member of the list will be substituted for the endpoint in the {@link #INGRESS_CHANNEL_PROP_NAME} value.
         */
        public static final String INGRESS_ENDPOINTS_PROP_NAME = "aeron.cluster.ingress.endpoints";

        /**
         * Default comma separated list of cluster ingress endpoints.
         */
        public static final String INGRESS_ENDPOINTS_DEFAULT = null;

        /**
         * Channel for sending messages to a cluster. Ideally this will be a multicast address otherwise unicast will
         * be required and the {@link #INGRESS_ENDPOINTS_PROP_NAME} is used to substitute the endpoints from the
         * {@link #INGRESS_ENDPOINTS_PROP_NAME} list.
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
         *
         * <p>
         * Channel's <em>endpoint</em> can be specified explicitly (i.e. by providing address and port pair) or
         * by using zero as a port number. Here is an example of valid response channels:
         * <ul>
         *     <li>{@code aeron:udp?endpoint=localhost:9020} - listen on port {@code 9020} on localhost.</li>
         *     <li>{@code aeron:udp?endpoint=192.168.10.10:9020} - listen on port {@code 9020} on
         *     {@code 192.168.10.10}.</li>
         *     <li>{@code aeron:udp?endpoint=localhost:0} - in this case the port is unspecified and the OS
         *     will assign a free port from the
         *     <a href="https://en.wikipedia.org/wiki/Ephemeral_port">ephemeral port range</a>.</li>
         * </ul>
         */
        public static final String EGRESS_CHANNEL_PROP_NAME = "aeron.cluster.egress.channel";

        /**
         * Channel for receiving response messages from a cluster.
         */
        public static final String EGRESS_CHANNEL_DEFAULT = "aeron:udp?endpoint=localhost:0";

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
         * The value {@link #INGRESS_ENDPOINTS_DEFAULT} or system property {@link #INGRESS_ENDPOINTS_PROP_NAME} if set.
         *
         * @return {@link #INGRESS_ENDPOINTS_DEFAULT} or system property {@link #INGRESS_ENDPOINTS_PROP_NAME} if set.
         */
        public static String ingressEndpoints()
        {
            return System.getProperty(INGRESS_ENDPOINTS_PROP_NAME, INGRESS_ENDPOINTS_DEFAULT);
        }

        /**
         * The value {@link #INGRESS_CHANNEL_DEFAULT} or system property {@link #INGRESS_CHANNEL_PROP_NAME} if set.
         *
         * @return {@link #INGRESS_CHANNEL_DEFAULT} or system property {@link #INGRESS_CHANNEL_PROP_NAME} if set.
         */
        public static String ingressChannel()
        {
            return System.getProperty(INGRESS_CHANNEL_PROP_NAME, INGRESS_CHANNEL_DEFAULT);
        }

        /**
         * The value {@link #INGRESS_STREAM_ID_DEFAULT} or system property {@link #INGRESS_STREAM_ID_PROP_NAME} if set.
         *
         * @return {@link #INGRESS_STREAM_ID_DEFAULT} or system property {@link #INGRESS_STREAM_ID_PROP_NAME} if set.
         */
        public static int ingressStreamId()
        {
            return Integer.getInteger(INGRESS_STREAM_ID_PROP_NAME, INGRESS_STREAM_ID_DEFAULT);
        }

        /**
         * The value {@link #EGRESS_CHANNEL_DEFAULT} or system property {@link #EGRESS_CHANNEL_PROP_NAME} if set.
         *
         * @return {@link #EGRESS_CHANNEL_DEFAULT} or system property {@link #EGRESS_CHANNEL_PROP_NAME} if set.
         */
        public static String egressChannel()
        {
            return System.getProperty(EGRESS_CHANNEL_PROP_NAME, EGRESS_CHANNEL_DEFAULT);
        }

        /**
         * The value {@link #EGRESS_STREAM_ID_DEFAULT} or system property {@link #EGRESS_STREAM_ID_PROP_NAME} if set.
         *
         * @return {@link #EGRESS_STREAM_ID_DEFAULT} or system property {@link #EGRESS_STREAM_ID_PROP_NAME} if set.
         */
        public static int egressStreamId()
        {
            return Integer.getInteger(EGRESS_STREAM_ID_PROP_NAME, EGRESS_STREAM_ID_DEFAULT);
        }
    }

    /**
     * Context for cluster session and connection.
     */
    public static final class Context implements Cloneable
    {
        /**
         * Using an integer because there is no support for boolean. 1 is concluded, 0 is not concluded.
         */
        private static final AtomicIntegerFieldUpdater<Context> IS_CONCLUDED_UPDATER = newUpdater(
            Context.class, "isConcluded");
        private volatile int isConcluded;

        private long messageTimeoutNs = Configuration.messageTimeoutNs();
        private String ingressEndpoints = Configuration.ingressEndpoints();
        private String ingressChannel = Configuration.ingressChannel();
        private int ingressStreamId = Configuration.ingressStreamId();
        private String egressChannel = Configuration.egressChannel();
        private int egressStreamId = Configuration.egressStreamId();
        private IdleStrategy idleStrategy;
        private String aeronDirectoryName = CommonContext.getAeronDirectoryName();
        private Aeron aeron;
        private CredentialsSupplier credentialsSupplier;
        private boolean ownsAeronClient = false;
        private boolean isIngressExclusive = true;
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

        /**
         * Conclude configuration by setting up defaults when specifics are not provided.
         */
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
            return CommonContext.checkDebugTimeout(messageTimeoutNs, TimeUnit.NANOSECONDS);
        }

        /**
         * The endpoints representing members for use with unicast to be substituted into the {@link #ingressChannel()}
         * for endpoints. A null value can be used when multicast where the {@link #ingressChannel()} contains the
         * multicast endpoint.
         *
         * @param clusterMembers which are all candidates to be leader.
         * @return this for a fluent API.
         * @see Configuration#INGRESS_ENDPOINTS_PROP_NAME
         */
        public Context ingressEndpoints(final String clusterMembers)
        {
            this.ingressEndpoints = clusterMembers;
            return this;
        }

        /**
         * The endpoints representing members for use with unicast to be substituted into the {@link #ingressChannel()}
         * for endpoints. A null value can be used when multicast where the {@link #ingressChannel()} contains the
         * multicast endpoint.
         *
         * @return member endpoints of the cluster which are all candidates to be leader.
         * @see Configuration#INGRESS_ENDPOINTS_PROP_NAME
         */
        public String ingressEndpoints()
        {
            return ingressEndpoints;
        }

        /**
         * Set the channel parameter for the ingress channel.
         * <p>
         * The endpoints representing members for use with unicast are substituted from {@link #ingressEndpoints()}
         * for endpoints. If this channel contains a multicast endpoint, then {@link #ingressEndpoints()} should
         * be set to null.
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
         * The endpoints representing members for use with unicast are substituted from {@link #ingressEndpoints()}
         * for endpoints. A null value can be used when multicast where this contains the multicast endpoint.
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
         * Is ingress to the cluster exclusively from a single thread to this client? The client should not be used
         * from another thread, e.g. a separate thread calling {@link AeronCluster#sendKeepAlive()} - which is a really
         * bad design by the way!
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
         * Is ingress the {@link Publication} to the cluster used exclusively from a single thread to this client?
         *
         * @return true if the ingress {@link Publication} is to be used exclusively from a single thread?
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
    public static final class AsyncConnect implements AutoCloseable
    {
        private final Subscription egressSubscription;
        private final long deadlineNs;
        private long correlationId = Aeron.NULL_VALUE;
        private long clusterSessionId;
        private long leadershipTermId;
        private int leaderMemberId;
        private int step = 0;
        private int messageLength = 0;

        private final Context ctx;
        private final NanoClock nanoClock;
        private final EgressPoller egressPoller;
        private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
        private Int2ObjectHashMap<MemberIngress> memberByIdMap;
        private Publication ingressPublication;

        AsyncConnect(final Context ctx, final Subscription egressSubscription, final long deadlineNs)
        {
            this.ctx = ctx;

            memberByIdMap = parseIngressEndpoints(ctx.ingressEndpoints());
            this.egressSubscription = egressSubscription;
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
                final ErrorHandler errorHandler = ctx.errorHandler();
                CloseHelper.close(errorHandler, ingressPublication);
                CloseHelper.close(errorHandler, egressSubscription);
                CloseHelper.closeAll(errorHandler, memberByIdMap.values());

                ctx.close();
            }
        }

        /**
         * Indicates which step in the connect process has been reached.
         *
         * @return which step in the connect process has reached.
         */
        public int step()
        {
            return step;
        }

        private void step(final int newStep)
        {
            //System.out.println("AeronCluster.AsyncConnect " + step + " -> " + newStep);
            step = newStep;
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
                memberByIdMap.remove(leaderMemberId);
                CloseHelper.closeAll(memberByIdMap.values());

                step(5);
            }

            return aeronCluster;
        }

        private void checkDeadline()
        {
            if (deadlineNs - nanoClock.nanoTime() < 0)
            {
                throw new TimeoutException(
                    "connect timeout, step=" + step + " egress.isConnected=" + egressSubscription.isConnected() +
                    " responseChannel=" + egressSubscription.tryResolveChannelEndpointPort());
            }

            if (Thread.currentThread().isInterrupted())
            {
                throw new AeronException("unexpected interrupt");
            }
        }

        private void createIngressPublications()
        {
            if (null == ctx.ingressEndpoints())
            {
                ingressPublication = addIngressPublication(ctx, ctx.ingressChannel(), ctx.ingressStreamId());
            }
            else
            {
                final ChannelUri channelUri = ChannelUri.parse(ctx.ingressChannel());
                for (final MemberIngress member : memberByIdMap.values())
                {
                    channelUri.put(CommonContext.ENDPOINT_PARAM_NAME, member.endpoint);
                    member.publication = addIngressPublication(ctx, channelUri.toString(), ctx.ingressStreamId());
                }
            }

            step(1);
        }

        private void awaitPublicationConnected()
        {
            final String responseChannel = egressSubscription.tryResolveChannelEndpointPort();
            if (null != responseChannel)
            {
                if (null == ingressPublication)
                {
                    for (final MemberIngress member : memberByIdMap.values())
                    {
                        if (member.publication.isConnected())
                        {
                            ingressPublication = member.publication;
                            prepareConnectRequest(responseChannel);
                            break;
                        }
                    }
                }
                else if (ingressPublication.isConnected())
                {
                    prepareConnectRequest(responseChannel);
                }
            }
        }

        private void prepareConnectRequest(final String responseChannel)
        {
            correlationId = ctx.aeron().nextCorrelationId();
            final byte[] encodedCredentials = ctx.credentialsSupplier().encodedCredentials();

            final SessionConnectRequestEncoder encoder = new SessionConnectRequestEncoder();
            encoder
                .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
                .correlationId(correlationId)
                .responseStreamId(ctx.egressStreamId())
                .version(Configuration.PROTOCOL_SEMANTIC_VERSION)
                .responseChannel(responseChannel)
                .putEncodedCredentials(encodedCredentials, 0, encodedCredentials.length);

            messageLength = MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();

            step(2);
        }

        private void sendMessage()
        {
            final long result = ingressPublication.offer(buffer, 0, messageLength);
            if (result > 0)
            {
                step(3);
            }
            else if (Publication.CLOSED == result || Publication.NOT_CONNECTED == result)
            {
                throw new ClusterException("unexpected loss of connection to cluster");
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
                    correlationId = Aeron.NULL_VALUE;
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

            final ChallengeResponseEncoder encoder = new ChallengeResponseEncoder();
            encoder
                .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
                .correlationId(correlationId)
                .clusterSessionId(clusterSessionId)
                .putEncodedCredentials(encodedCredentials, 0, encodedCredentials.length);

            messageLength = MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();

            step(2);
        }

        private void updateMembers()
        {
            leaderMemberId = egressPoller.leaderMemberId();
            final MemberIngress leader = memberByIdMap.get(leaderMemberId);
            if (null != leader)
            {
                ingressPublication = leader.publication;
                leader.publication = null;
                CloseHelper.closeAll(memberByIdMap.values());
                memberByIdMap = parseIngressEndpoints(egressPoller.detail());
            }
            else
            {
                CloseHelper.closeAll(memberByIdMap.values());
                memberByIdMap = parseIngressEndpoints(egressPoller.detail());

                final MemberIngress member = memberByIdMap.get(leaderMemberId);
                final ChannelUri channelUri = ChannelUri.parse(ctx.ingressChannel());
                channelUri.put(CommonContext.ENDPOINT_PARAM_NAME, member.endpoint);
                member.publication = addIngressPublication(ctx, channelUri.toString(), ctx.ingressStreamId());
                ingressPublication = member.publication;
            }

            step(1);
        }

        private AeronCluster newInstance()
        {
            return new AeronCluster(
                ctx,
                messageHeaderEncoder,
                ingressPublication,
                egressSubscription,
                memberByIdMap,
                clusterSessionId,
                leadershipTermId,
                leaderMemberId);
        }
    }

    static final class MemberIngress implements AutoCloseable
    {
        final int memberId;
        final String endpoint;
        Publication publication;

        MemberIngress(final int memberId, final String endpoint)
        {
            this.memberId = memberId;
            this.endpoint = endpoint;
        }

        public void close()
        {
            CloseHelper.close(publication);
            publication = null;
        }

        public String toString()
        {
            return "MemberIngress{" +
                "memberId=" + memberId +
                ", endpoint='" + endpoint + '\'' +
                ", publication=" + publication +
                '}';
        }
    }
}
