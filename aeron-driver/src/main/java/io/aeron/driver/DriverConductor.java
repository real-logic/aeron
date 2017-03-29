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
package io.aeron.driver;

import io.aeron.CommonContext;
import io.aeron.driver.buffer.RawLogFactory;
import io.aeron.driver.MediaDriver.Context;
import io.aeron.driver.buffer.RawLog;
import io.aeron.driver.cmd.DriverConductorCmd;
import io.aeron.driver.exceptions.ControlProtocolException;
import io.aeron.driver.media.ReceiveChannelEndpoint;
import io.aeron.driver.media.SendChannelEndpoint;
import io.aeron.driver.media.UdpChannel;
import io.aeron.driver.status.*;
import io.aeron.driver.uri.AeronUri;
import org.agrona.BitUtil;
import org.agrona.concurrent.*;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.agrona.concurrent.status.*;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;

import static io.aeron.CommonContext.*;
import static io.aeron.driver.Configuration.*;
import static io.aeron.driver.status.SystemCounterDescriptor.CLIENT_KEEP_ALIVES;
import static io.aeron.driver.status.SystemCounterDescriptor.ERRORS;
import static io.aeron.driver.status.SystemCounterDescriptor.UNBLOCKED_COMMANDS;
import static io.aeron.ErrorCode.*;
import static io.aeron.logbuffer.FrameDescriptor.computeExclusiveMaxMessageLength;
import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static io.aeron.protocol.DataHeaderFlyweight.createDefaultHeader;
import static org.agrona.collections.ArrayListUtil.fastUnorderedRemove;

/**
 * Driver Conductor that takes commands from publishers and subscribers and orchestrates the media driver.
 */
public class DriverConductor implements Agent
{
    private final long imageLivenessTimeoutNs;
    private final long clientLivenessTimeoutNs;
    private final long publicationUnblockTimeoutNs;
    private final long statusMessageTimeoutNs;
    private long timeOfLastToDriverPositionChange;
    private long lastConsumerCommandPosition;
    private long timeOfLastTimeoutCheck;
    private volatile long timeInMs;
    private int nextSessionId = BitUtil.generateRandomisedId();

    private final NetworkPublicationThreadLocals networkPublicationThreadLocals = new NetworkPublicationThreadLocals();
    private final Context context;
    private final RawLogFactory rawLogFactory;
    private final ReceiverProxy receiverProxy;
    private final SenderProxy senderProxy;
    private final ClientProxy clientProxy;
    private final DriverConductorProxy fromReceiverConductorProxy;
    private final RingBuffer toDriverCommands;
    private final ClientListenerAdapter clientListenerAdapter;
    private final OneToOneConcurrentArrayQueue<DriverConductorCmd> fromReceiverDriverConductorCmdQueue;
    private final OneToOneConcurrentArrayQueue<DriverConductorCmd> fromSenderDriverConductorCmdQueue;
    private final HashMap<String, SendChannelEndpoint> sendChannelEndpointByChannelMap = new HashMap<>();
    private final HashMap<String, ReceiveChannelEndpoint> receiveChannelEndpointByChannelMap = new HashMap<>();
    private final ArrayList<PublicationLink> publicationLinks = new ArrayList<>();
    private final ArrayList<NetworkPublication> networkPublications = new ArrayList<>();
    private final ArrayList<SubscriptionLink> subscriptionLinks = new ArrayList<>();
    private final ArrayList<PublicationImage> publicationImages = new ArrayList<>();
    private final ArrayList<AeronClient> clients = new ArrayList<>();
    private final ArrayList<IpcPublication> ipcPublications = new ArrayList<>();

    private final EpochClock epochClock;
    private final NanoClock nanoClock;
    private final EpochClock cachedEpochClock = () -> timeInMs;

    private final Consumer<DriverConductorCmd> onDriverConductorCmdFunc = this::onDriverConductorCmd;

    private final CountersManager countersManager;
    private final AtomicCounter clientKeepAlives;

    public DriverConductor(final Context ctx)
    {
        context = ctx;
        imageLivenessTimeoutNs = ctx.imageLivenessTimeoutNs();
        clientLivenessTimeoutNs = ctx.clientLivenessTimeoutNs();
        publicationUnblockTimeoutNs = ctx.publicationUnblockTimeoutNs();
        statusMessageTimeoutNs = ctx.statusMessageTimeout();
        fromReceiverDriverConductorCmdQueue = ctx.toConductorFromReceiverCommandQueue();
        fromSenderDriverConductorCmdQueue = ctx.toConductorFromSenderCommandQueue();
        receiverProxy = ctx.receiverProxy();
        senderProxy = ctx.senderProxy();
        rawLogFactory = ctx.rawLogBuffersFactory();
        epochClock = ctx.epochClock();
        nanoClock = ctx.nanoClock();
        toDriverCommands = ctx.toDriverCommands();
        clientProxy = ctx.clientProxy();
        fromReceiverConductorProxy = ctx.fromReceiverDriverConductorProxy();

        countersManager = context.countersManager();
        clientKeepAlives = context.systemCounters().get(CLIENT_KEEP_ALIVES);

        clientListenerAdapter = new ClientListenerAdapter(
            context.systemCounters().get(ERRORS),
            ctx.errorLog(),
            toDriverCommands,
            clientProxy,
            this);

        timeInMs = epochClock.time();
        toDriverCommands.consumerHeartbeatTime(timeInMs);

        final long now = nanoClock.nanoTime();
        timeOfLastTimeoutCheck = now;
        timeOfLastToDriverPositionChange = now;
        lastConsumerCommandPosition = toDriverCommands.consumerPosition();
    }

    public void onClose()
    {
        networkPublications.forEach(NetworkPublication::close);
        publicationImages.forEach(PublicationImage::close);
        ipcPublications.forEach(IpcPublication::close);
        sendChannelEndpointByChannelMap.values().forEach(SendChannelEndpoint::close);
        receiveChannelEndpointByChannelMap.values().forEach(ReceiveChannelEndpoint::close);
    }

    public String roleName()
    {
        return "driver-conductor";
    }

    public int doWork() throws Exception
    {
        int workCount = 0;

        workCount += clientListenerAdapter.receive();
        workCount += fromReceiverDriverConductorCmdQueue.drain(onDriverConductorCmdFunc);
        workCount += fromSenderDriverConductorCmdQueue.drain(onDriverConductorCmdFunc);

        final long nowNs = nanoClock.nanoTime();
        workCount += processTimers(nowNs);

        final ArrayList<PublicationImage> publicationImages = this.publicationImages;
        for (int i = 0, size = publicationImages.size(); i < size; i++)
        {
            publicationImages.get(i).trackRebuild(nowNs, statusMessageTimeoutNs);
        }

        final ArrayList<NetworkPublication> networkPublications = this.networkPublications;
        for (int i = 0, size = networkPublications.size(); i < size; i++)
        {
            workCount += networkPublications.get(i).updatePublishersLimit();
        }

        final ArrayList<IpcPublication> ipcPublications = this.ipcPublications;
        for (int i = 0, size = ipcPublications.size(); i < size; i++)
        {
            workCount += ipcPublications.get(i).updatePublishersLimit();
        }

        return workCount;
    }

    public void onClosePublication(final NetworkPublication publication)
    {
        publication.close();
    }

    public void onCreatePublicationImage(
        final int sessionId,
        final int streamId,
        final int initialTermId,
        final int activeTermId,
        final int initialTermOffset,
        final int termBufferLength,
        final int senderMtuLength,
        final InetSocketAddress controlAddress,
        final InetSocketAddress sourceAddress,
        final ReceiveChannelEndpoint channelEndpoint)
    {
        channelEndpoint.validateSenderMtuLength(senderMtuLength);
        channelEndpoint.validateWindowMaxLength(context.initialWindowLength());

        final UdpChannel udpChannel = channelEndpoint.udpChannel();
        final String channel = udpChannel.originalUriString();
        final long registrationId = nextImageCorrelationId();

        final long joiningPosition = computePosition(
            activeTermId, initialTermOffset, Integer.numberOfTrailingZeros(termBufferLength), initialTermId);

        final List<SubscriberPosition> subscriberPositions = createSubscriberPositions(
            sessionId, streamId, channelEndpoint, joiningPosition);

        if (subscriberPositions.size() > 0)
        {
            final RawLog rawLog = newPublicationImageLog(
                sessionId, streamId, initialTermId, termBufferLength, senderMtuLength, udpChannel, registrationId);

            final CongestionControl congestionControl = context.congestionControlSupplier().newInstance(
                registrationId,
                udpChannel,
                streamId,
                sessionId,
                termBufferLength,
                senderMtuLength,
                nanoClock,
                context,
                countersManager);

            final PublicationImage image = new PublicationImage(
                registrationId,
                imageLivenessTimeoutNs,
                channelEndpoint,
                controlAddress,
                sessionId,
                streamId,
                initialTermId,
                activeTermId,
                initialTermOffset,
                rawLog,
                udpChannel.isMulticast() ? NAK_MULTICAST_DELAY_GENERATOR : NAK_UNICAST_DELAY_GENERATOR,
                positionArray(subscriberPositions),
                ReceiverHwm.allocate(countersManager, registrationId, sessionId, streamId, channel),
                ReceiverPos.allocate(countersManager, registrationId, sessionId, streamId, channel),
                nanoClock,
                context.epochClock(),
                context.systemCounters(),
                sourceAddress,
                congestionControl,
                context.lossReport(),
                subscriberPositions.get(0).subscription().isReliable());

            for (int i = 0, size = subscriberPositions.size(); i < size; i++)
            {
                subscriberPositions.get(i).addLink(image);
            }

            publicationImages.add(image);
            receiverProxy.newPublicationImage(channelEndpoint, image);

            clientProxy.onAvailableImage(
                registrationId,
                streamId,
                sessionId,
                rawLog.fileName(),
                subscriberPositions,
                generateSourceIdentity(sourceAddress));
        }
    }

    SendChannelEndpoint senderChannelEndpoint(final UdpChannel channel)
    {
        return sendChannelEndpointByChannelMap.get(channel.canonicalForm());
    }

    ReceiveChannelEndpoint receiverChannelEndpoint(final UdpChannel channel)
    {
        return receiveChannelEndpointByChannelMap.get(channel.canonicalForm());
    }

    IpcPublication getIpcSharedPublication(final long streamId)
    {
        return findIpcSharedPublication(ipcPublications, streamId);
    }

    IpcPublication getIpcPublication(final long registrationId)
    {
        for (int i = 0, size = ipcPublications.size(); i < size; i++)
        {
            final IpcPublication publication = ipcPublications.get(i);
            if (publication.registrationId() == registrationId)
            {
                return publication;
            }
        }

        return null;
    }

    void onAddNetworkPublication(
        final String channel,
        final int streamId,
        final long registrationId,
        final long clientId,
        final boolean isExclusive)
    {
        final UdpChannel udpChannel = UdpChannel.parse(channel);
        final PublicationParams params = getPublicationParams(udpChannel.aeronUri(), isExclusive, false);
        final SendChannelEndpoint channelEndpoint = getOrCreateSendChannelEndpoint(udpChannel);

        NetworkPublication publication = null;
        if (!isExclusive)
        {
            publication = findPublication(networkPublications, streamId, channelEndpoint);
        }

        if (null == publication)
        {
            publication = newNetworkPublication(
                registrationId, streamId, channel, udpChannel, channelEndpoint, params, isExclusive);
        }
        else if (publication.mtuLength() != params.mtuLength)
        {
            throw new IllegalStateException("Existing publication has different MTU length: existing=" +
                publication.mtuLength() + " requested=" + params.mtuLength);
        }

        publicationLinks.add(new PublicationLink(registrationId, publication, getOrAddClient(clientId)));

        clientProxy.onPublicationReady(
            registrationId,
            streamId,
            publication.sessionId(),
            publication.rawLog().fileName(),
            publication.publisherLimitId(),
            isExclusive);
    }

    private NetworkPublication newNetworkPublication(
        final long registrationId,
        final int streamId,
        final String channel,
        final UdpChannel udpChannel,
        final SendChannelEndpoint channelEndpoint,
        final PublicationParams params,
        final boolean isExclusive)
    {
        final int sessionId = nextSessionId++;
        final UnsafeBufferPosition senderPosition = SenderPos.allocate(
            countersManager, registrationId, sessionId, streamId, channel);
        final UnsafeBufferPosition senderLimit = SenderLimit.allocate(
            countersManager, registrationId, sessionId, streamId, channel);

        int initialTermId = BitUtil.generateRandomisedId();
        if (params.isReplay)
        {
            initialTermId = params.initialTermId;
            final int bits = Integer.numberOfTrailingZeros(params.termLength);
            final long position = computePosition(params.termId, params.termOffset, bits, initialTermId);
            senderLimit.setOrdered(position);
            senderPosition.setOrdered(position);
        }

        final RetransmitHandler retransmitHandler = new RetransmitHandler(
            nanoClock,
            context.systemCounters(),
            RETRANSMIT_UNICAST_DELAY_GENERATOR,
            RETRANSMIT_UNICAST_LINGER_GENERATOR);

        final FlowControl flowControl =
            udpChannel.isMulticast() || udpChannel.hasExplicitControl() ?
                context.multicastFlowControlSupplier().newInstance(udpChannel, streamId, registrationId) :
                context.unicastFlowControlSupplier().newInstance(udpChannel, streamId, registrationId);

        final NetworkPublication publication = new NetworkPublication(
            registrationId,
            channelEndpoint,
            nanoClock,
            cachedEpochClock,
            newNetworkPublicationLog(sessionId, streamId, initialTermId, udpChannel, registrationId, params.termLength),
            PublisherLimit.allocate(countersManager, registrationId, sessionId, streamId, channel),
            senderPosition,
            senderLimit,
            sessionId,
            streamId,
            initialTermId,
            params.mtuLength,
            context.systemCounters(),
            flowControl,
            retransmitHandler,
            networkPublicationThreadLocals,
            publicationUnblockTimeoutNs,
            isExclusive);

        if (params.isReplay)
        {
            final int activeIndex = indexByTerm(params.initialTermId, params.termId);
            final UnsafeBuffer logMetaDataBuffer = publication.rawLog().metaData();
            rawTail(logMetaDataBuffer, activeIndex, packTail(params.termId, params.termOffset));
            activePartitionIndex(logMetaDataBuffer, activeIndex);
        }

        channelEndpoint.incRef();
        networkPublications.add(publication);
        senderProxy.newNetworkPublication(publication);
        linkSpies(subscriptionLinks, publication);

        return publication;
    }

    void cleanupPublication(final NetworkPublication publication)
    {
        if (publication.hasSpies())
        {
            clientProxy.onUnavailableImage(
                correlationId(publication.rawLog().metaData()),
                publication.streamId(),
                publication.channelEndpoint().originalUriString());

            for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
            {
                subscriptionLinks.get(i).unlink(publication);
            }
        }

        senderProxy.removeNetworkPublication(publication);

        final SendChannelEndpoint channelEndpoint = publication.channelEndpoint();
        if (channelEndpoint.shouldBeClosed())
        {
            channelEndpoint.closeStatusIndicator();
            sendChannelEndpointByChannelMap.remove(channelEndpoint.udpChannel().canonicalForm());
            senderProxy.closeSendChannelEndpoint(channelEndpoint);
        }
    }

    void cleanupSubscriptionLink(final SubscriptionLink subscription)
    {
        final ReceiveChannelEndpoint channelEndpoint = subscription.channelEndpoint();

        if (null != channelEndpoint)
        {
            final int streamId = subscription.streamId();

            if (0 == channelEndpoint.decRefToStream(subscription.streamId()))
            {
                receiverProxy.removeSubscription(channelEndpoint, streamId);
            }

            if (channelEndpoint.shouldBeClosed())
            {
                channelEndpoint.closeStatusIndicator();
                receiveChannelEndpointByChannelMap.remove(channelEndpoint.udpChannel().canonicalForm());
                receiverProxy.closeReceiveChannelEndpoint(channelEndpoint);
            }
        }
    }

    void transitionToLinger(final PublicationImage image)
    {
        clientProxy.onUnavailableImage(image.correlationId(), image.streamId(), image.channelUriString());
        receiverProxy.removeCoolDown(image.channelEndpoint(), image.sessionId(), image.streamId());
    }

    void transitionToLinger(final IpcPublication publication)
    {
        clientProxy.onUnavailableImage(publication.registrationId(), publication.streamId(), CommonContext.IPC_CHANNEL);
    }

    void cleanupImage(final PublicationImage image)
    {
        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            subscriptionLinks.get(i).unlink(image);
        }
    }

    void cleanupIpcPublication(final IpcPublication publication)
    {
        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            subscriptionLinks.get(i).unlink(publication);
        }
    }

    private List<SubscriberPosition> createSubscriberPositions(
        final int sessionId,
        final int streamId,
        final ReceiveChannelEndpoint channelEndpoint,
        final long joiningPosition)
    {
        final ArrayList<SubscriberPosition> subscriberPositions = new ArrayList<>();

        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            final SubscriptionLink subscription = subscriptionLinks.get(i);
            if (subscription.matches(channelEndpoint, streamId))
            {
                final Position position = SubscriberPos.allocate(
                    countersManager,
                    subscription.registrationId(),
                    sessionId,
                    streamId,
                    subscription.uri(),
                    joiningPosition);

                position.setOrdered(joiningPosition);
                subscriberPositions.add(new SubscriberPosition(subscription, position));
            }
        }

        return subscriberPositions;
    }

    void onAddIpcPublication(
        final String channel,
        final int streamId,
        final long registrationId,
        final long clientId,
        final boolean isExclusive)
    {
        final IpcPublication ipcPublication = getOrAddIpcPublication(registrationId, streamId, channel, isExclusive);
        publicationLinks.add(new PublicationLink(registrationId, ipcPublication, getOrAddClient(clientId)));

        clientProxy.onPublicationReady(
            registrationId,
            streamId,
            ipcPublication.sessionId(),
            ipcPublication.rawLog().fileName(),
            ipcPublication.publisherLimitId(),
            isExclusive);

        linkIpcSubscriptions(ipcPublication);
    }

    void onRemovePublication(final long registrationId, final long correlationId)
    {
        PublicationLink publicationLink = null;
        final ArrayList<PublicationLink> publicationLinks = this.publicationLinks;
        for (int i = 0, size = publicationLinks.size(), lastIndex = size - 1; i < size; i++)
        {
            final PublicationLink publication = publicationLinks.get(i);
            if (registrationId == publication.registrationId())
            {
                publicationLink = publication;
                fastUnorderedRemove(publicationLinks, i, lastIndex);
                break;
            }
        }

        if (null == publicationLink)
        {
            throw new ControlProtocolException(UNKNOWN_PUBLICATION, "Unknown publication: " + registrationId);
        }

        publicationLink.close();

        clientProxy.operationSucceeded(correlationId);
    }

    void onAddDestination(final long registrationId, final String destinationChannel, final long correlationId)
    {
        SendChannelEndpoint sendChannelEndpoint = null;

        for (int i = 0, size = networkPublications.size(); i < size; i++)
        {
            final NetworkPublication publication = networkPublications.get(i);

            if (registrationId == publication.registrationId())
            {
                sendChannelEndpoint = publication.channelEndpoint();
                break;
            }
        }

        if (null == sendChannelEndpoint)
        {
            throw new ControlProtocolException(UNKNOWN_PUBLICATION, "Unknown publication: " + registrationId);
        }

        sendChannelEndpoint.validateAllowsManualControl();

        final AeronUri aeronUri = AeronUri.parse(destinationChannel);
        final InetSocketAddress dstAddress = UdpChannel.destinationAddress(aeronUri);
        senderProxy.addDestination(sendChannelEndpoint, dstAddress);
        clientProxy.operationSucceeded(correlationId);
    }

    void onRemoveDestination(final long registrationId, final String destinationChannel, final long correlationId)
    {
        SendChannelEndpoint sendChannelEndpoint = null;

        for (int i = 0, size = networkPublications.size(); i < size; i++)
        {
            final NetworkPublication publication = networkPublications.get(i);

            if (registrationId == publication.registrationId())
            {
                sendChannelEndpoint = publication.channelEndpoint();
                break;
            }
        }

        if (null == sendChannelEndpoint)
        {
            throw new ControlProtocolException(UNKNOWN_PUBLICATION, "Unknown publication: " + registrationId);
        }

        sendChannelEndpoint.validateAllowsManualControl();

        final AeronUri aeronUri = AeronUri.parse(destinationChannel);
        final InetSocketAddress dstAddress = UdpChannel.destinationAddress(aeronUri);
        senderProxy.removeDestination(sendChannelEndpoint, dstAddress);
        clientProxy.operationSucceeded(correlationId);
    }

    void onAddNetworkSubscription(
        final String channel, final int streamId, final long registrationId, final long clientId)
    {
        final UdpChannel udpChannel = UdpChannel.parse(channel);
        final String reliableParam = udpChannel.aeronUri().get(RELIABLE_STREAM_PARAM_NAME, "true");
        final boolean isReliable = !"false".equals(reliableParam);

        checkForClashingSubscription(isReliable, udpChannel, streamId);

        final ReceiveChannelEndpoint channelEndpoint = getOrCreateReceiveChannelEndpoint(udpChannel);
        final int refCount = channelEndpoint.incRefToStream(streamId);
        if (1 == refCount)
        {
            receiverProxy.addSubscription(channelEndpoint, streamId);
        }

        final AeronClient client = getOrAddClient(clientId);
        final SubscriptionLink subscription = new NetworkSubscriptionLink(
            registrationId, channelEndpoint, streamId, channel, client, context.clientLivenessTimeoutNs(), isReliable);

        subscriptionLinks.add(subscription);
        clientProxy.operationSucceeded(registrationId);

        linkMatchingImages(channelEndpoint, subscription);
    }

    void onAddIpcSubscription(final String channel, final int streamId, final long registrationId, final long clientId)
    {
        final IpcSubscriptionLink subscription = new IpcSubscriptionLink(
            registrationId, streamId, channel, getOrAddClient(clientId), context.clientLivenessTimeoutNs());

        subscriptionLinks.add(subscription);
        clientProxy.operationSucceeded(registrationId);

        for (int i = 0, size = ipcPublications.size(); i < size; i++)
        {
            final IpcPublication publication = ipcPublications.get(i);
            if (publication.streamId() == streamId && IpcPublication.Status.ACTIVE == publication.status())
            {
                linkIpcSubscription(subscription, publication);
            }
        }
    }

    void onAddSpySubscription(final String channel, final int streamId, final long registrationId, final long clientId)
    {
        final UdpChannel udpChannel = UdpChannel.parse(channel);
        final AeronClient client = getOrAddClient(clientId);
        final SpySubscriptionLink subscriptionLink = new SpySubscriptionLink(
            registrationId, udpChannel, streamId, client, context.clientLivenessTimeoutNs());

        subscriptionLinks.add(subscriptionLink);
        clientProxy.operationSucceeded(registrationId);

        final SendChannelEndpoint channelEndpoint = sendChannelEndpointByChannelMap.get(udpChannel.canonicalForm());

        for (int i = 0, size = networkPublications.size(); i < size; i++)
        {
            final NetworkPublication publication = networkPublications.get(i);

            if (streamId == publication.streamId() &&
                channelEndpoint == publication.channelEndpoint() &&
                NetworkPublication.Status.ACTIVE == publication.status())
            {
                linkSpy(publication, subscriptionLink);
            }
        }
    }

    void onRemoveSubscription(final long registrationId, final long correlationId)
    {
        final SubscriptionLink subscription = removeSubscriptionLink(subscriptionLinks, registrationId);
        if (null == subscription)
        {
            throw new ControlProtocolException(UNKNOWN_SUBSCRIPTION, "Unknown Subscription: " + registrationId);
        }

        subscription.close();
        final ReceiveChannelEndpoint channelEndpoint = subscription.channelEndpoint();

        if (null != channelEndpoint)
        {
            final int refCount = channelEndpoint.decRefToStream(subscription.streamId());
            if (0 == refCount)
            {
                receiverProxy.removeSubscription(channelEndpoint, subscription.streamId());
            }

            if (channelEndpoint.shouldBeClosed())
            {
                channelEndpoint.closeStatusIndicator();
                receiveChannelEndpointByChannelMap.remove(channelEndpoint.udpChannel().canonicalForm());
                receiverProxy.closeReceiveChannelEndpoint(channelEndpoint);

                while (!channelEndpoint.isClosed())
                {
                    Thread.yield();
                }
            }
        }

        clientProxy.operationSucceeded(correlationId);
    }

    void onClientKeepalive(final long clientId)
    {
        clientKeepAlives.addOrdered(1);

        final AeronClient client = findClient(clients, clientId);
        if (null != client)
        {
            client.timeOfLastKeepalive(nanoClock.nanoTime());
        }
    }

    private void onHeartbeatCheckTimeouts(final long nowNs)
    {
        final long nowMs = epochClock.time();
        timeInMs = nowMs;
        toDriverCommands.consumerHeartbeatTime(nowMs);

        onCheckManagedResources(clients, nowNs, nowMs);
        onCheckManagedResources(publicationLinks, nowNs, nowMs);
        onCheckManagedResources(networkPublications, nowNs, nowMs);
        onCheckManagedResources(subscriptionLinks, nowNs, nowMs);
        onCheckManagedResources(publicationImages, nowNs, nowMs);
        onCheckManagedResources(ipcPublications, nowNs, nowMs);
    }

    private void onCheckForBlockedToDriverCommands(final long nanoTimeNow)
    {
        final long consumerPosition = toDriverCommands.consumerPosition();

        if (consumerPosition == lastConsumerCommandPosition)
        {
            if (toDriverCommands.producerPosition() > consumerPosition &&
                nanoTimeNow > (timeOfLastToDriverPositionChange + clientLivenessTimeoutNs))
            {
                if (toDriverCommands.unblock())
                {
                    context.systemCounters().get(UNBLOCKED_COMMANDS).orderedIncrement();
                }
            }
        }
        else
        {
            timeOfLastToDriverPositionChange = nanoTimeNow;
            lastConsumerCommandPosition = consumerPosition;
        }
    }

    private static NetworkPublication findPublication(
        final ArrayList<NetworkPublication> publications,
        final int streamId,
        final SendChannelEndpoint channelEndpoint)
    {
        for (int i = 0, size = publications.size(); i < size; i++)
        {
            final NetworkPublication publication = publications.get(i);

            if (streamId == publication.streamId() &&
                channelEndpoint == publication.channelEndpoint() &&
                NetworkPublication.Status.ACTIVE == publication.status() &&
                !publication.isExclusive())
            {
                return publication;
            }
        }

        return null;
    }

    private RawLog newNetworkPublicationLog(
        final int sessionId,
        final int streamId,
        final int initialTermId,
        final UdpChannel udpChannel,
        final long registrationId,
        final int termBufferLength)
    {
        final RawLog rawLog = rawLogFactory.newNetworkPublication(
            udpChannel.canonicalForm(), sessionId, streamId, registrationId, termBufferLength);

        final UnsafeBuffer logMetaData = rawLog.metaData();
        storeDefaultFrameHeader(logMetaData, createDefaultHeader(sessionId, streamId, initialTermId));
        initialiseTailWithTermId(logMetaData, 0, initialTermId);

        initialTermId(logMetaData, initialTermId);
        mtuLength(logMetaData, context.mtuLength());
        correlationId(logMetaData, registrationId);
        timeOfLastStatusMessage(logMetaData, 0);

        return rawLog;
    }

    private RawLog newIpcPublicationLog(
        final int termBufferLength,
        final int sessionId,
        final int streamId,
        final int initialTermId,
        final long registrationId)
    {
        final RawLog rawLog = rawLogFactory.newIpcPublication(sessionId, streamId, registrationId, termBufferLength);

        final UnsafeBuffer logMetaData = rawLog.metaData();
        storeDefaultFrameHeader(logMetaData, createDefaultHeader(sessionId, streamId, initialTermId));
        initialiseTailWithTermId(logMetaData, 0, initialTermId);
        initialTermId(logMetaData, initialTermId);

        mtuLength(logMetaData, computeExclusiveMaxMessageLength(termBufferLength));
        correlationId(logMetaData, registrationId);
        timeOfLastStatusMessage(logMetaData, 0);

        return rawLog;
    }

    private RawLog newPublicationImageLog(
        final int sessionId,
        final int streamId,
        final int initialTermId,
        final int termBufferLength,
        final int senderMtuLength,
        final UdpChannel udpChannel,
        final long correlationId)
    {
        final RawLog rawLog = rawLogFactory.newNetworkedImage(
            udpChannel.canonicalForm(), sessionId, streamId, correlationId, termBufferLength);

        final UnsafeBuffer logMetaData = rawLog.metaData();
        storeDefaultFrameHeader(logMetaData, createDefaultHeader(sessionId, streamId, initialTermId));
        initialTermId(logMetaData, initialTermId);
        mtuLength(logMetaData, senderMtuLength);
        correlationId(logMetaData, correlationId);
        timeOfLastStatusMessage(logMetaData, 0);

        return rawLog;
    }

    private SendChannelEndpoint getOrCreateSendChannelEndpoint(final UdpChannel udpChannel)
    {
        SendChannelEndpoint channelEndpoint = sendChannelEndpointByChannelMap.get(udpChannel.canonicalForm());
        if (null == channelEndpoint)
        {
            channelEndpoint = context.sendChannelEndpointSupplier().newInstance(
                udpChannel,
                SendChannelStatus.allocate(countersManager, udpChannel.originalUriString()),
                context);

            sendChannelEndpointByChannelMap.put(udpChannel.canonicalForm(), channelEndpoint);
            senderProxy.registerSendChannelEndpoint(channelEndpoint);
        }

        return channelEndpoint;
    }

    private void checkForClashingSubscription(final boolean isReliable, final UdpChannel udpChannel, final int streamId)
    {
        final ReceiveChannelEndpoint channelEndpoint = receiveChannelEndpointByChannelMap.get(
            udpChannel.canonicalForm());
        if (null != channelEndpoint)
        {
            final ArrayList<SubscriptionLink> existingLinks = subscriptionLinks;
            for (int i = 0, size = existingLinks.size(); i < size; i++)
            {
                final SubscriptionLink subscription = existingLinks.get(i);
                if (subscription.matches(channelEndpoint, streamId) && isReliable != subscription.isReliable())
                {
                    throw new IllegalStateException(
                        "Option conflicts with existing subscriptions: reliable=" + isReliable);
                }
            }
        }
    }

    private void linkMatchingImages(final ReceiveChannelEndpoint channelEndpoint, final SubscriptionLink subscription)
    {
        final long registrationId = subscription.registrationId();
        final int streamId = subscription.streamId();
        final String channel = subscription.uri();

        for (int i = 0, size = publicationImages.size(); i < size; i++)
        {
            final PublicationImage image = publicationImages.get(i);
            if (image.matches(channelEndpoint, streamId) && image.isAcceptingSubscriptions())
            {
                final long rebuildPosition = image.rebuildPosition();
                final int sessionId = image.sessionId();
                final Position position = SubscriberPos.allocate(
                    countersManager, registrationId, sessionId, streamId, channel, rebuildPosition);

                position.setOrdered(rebuildPosition);

                image.addSubscriber(position);
                subscription.link(image, position);

                clientProxy.onAvailableImage(
                    image.correlationId(),
                    streamId,
                    sessionId,
                    image.rawLog().fileName(),
                    Collections.singletonList(new SubscriberPosition(subscription, position)),
                    generateSourceIdentity(image.sourceAddress()));
            }
        }
    }

    private void linkIpcSubscriptions(final IpcPublication publication)
    {
        final int streamId = publication.streamId();
        final ArrayList<SubscriptionLink> subscriptionLinks = this.subscriptionLinks;

        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            final SubscriptionLink subscription = subscriptionLinks.get(i);
            if (subscription.matches(streamId) && !subscription.isLinked(publication))
            {
                linkIpcSubscription((IpcSubscriptionLink)subscription, publication);
            }
        }
    }

    private static ReadablePosition[] positionArray(final List<SubscriberPosition> subscriberPositions)
    {
        final int size = subscriberPositions.size();
        final ReadablePosition[] positions = new ReadablePosition[subscriberPositions.size()];

        for (int i = 0; i < size; i++)
        {
            positions[i] = subscriberPositions.get(i).position();
        }

        return positions;
    }

    private void linkIpcSubscription(final IpcSubscriptionLink subscription, final IpcPublication publication)
    {
        final long joiningPosition = publication.joiningPosition();
        final long registrationId = subscription.registrationId();
        final int sessionId = publication.sessionId();
        final int streamId = subscription.streamId();
        final String channel = subscription.uri();

        final Position position = SubscriberPos.allocate(
            countersManager, registrationId, sessionId, streamId, channel, joiningPosition);

        position.setOrdered(joiningPosition);
        subscription.link(publication, position);
        publication.addSubscriber(position);

        clientProxy.onAvailableImage(
            publication.registrationId(),
            streamId,
            sessionId,
            publication.rawLog().fileName(),
            Collections.singletonList(new SubscriberPosition(subscription, position)),
            channel);
    }

    private void linkSpy(final NetworkPublication publication, final SubscriptionLink subscription)
    {
        final long joiningPosition = publication.spyJoiningPosition();
        final long registrationId = subscription.registrationId();
        final int streamId = publication.streamId();
        final int sessionId = publication.sessionId();
        final String channel = subscription.uri();

        final Position position = SubscriberPos.allocate(
            countersManager, registrationId, sessionId, streamId, channel, joiningPosition);

        position.setOrdered(joiningPosition);
        publication.addSubscriber(position);
        subscription.link(publication, position);

        clientProxy.onAvailableImage(
            correlationId(publication.rawLog().metaData()),
            streamId,
            sessionId,
            publication.rawLog().fileName(),
            Collections.singletonList(new SubscriberPosition(subscription, position)),
            channel);
    }

    private ReceiveChannelEndpoint getOrCreateReceiveChannelEndpoint(final UdpChannel udpChannel)
    {
        ReceiveChannelEndpoint channelEndpoint = receiveChannelEndpointByChannelMap.get(udpChannel.canonicalForm());
        if (null == channelEndpoint)
        {
            channelEndpoint = context.receiveChannelEndpointSupplier().newInstance(
                udpChannel,
                new DataPacketDispatcher(fromReceiverConductorProxy, receiverProxy.receiver()),
                ReceiveChannelStatus.allocate(countersManager, udpChannel.originalUriString()),
                context);

            receiveChannelEndpointByChannelMap.put(udpChannel.canonicalForm(), channelEndpoint);
            receiverProxy.registerReceiveChannelEndpoint(channelEndpoint);
        }

        return channelEndpoint;
    }

    private void onDriverConductorCmd(final DriverConductorCmd cmd)
    {
        cmd.execute(this);
    }

    private AeronClient getOrAddClient(final long clientId)
    {
        AeronClient client = findClient(clients, clientId);
        if (null == client)
        {
            client = new AeronClient(clientId, clientLivenessTimeoutNs, nanoClock.nanoTime());
            clients.add(client);
        }

        return client;
    }

    private IpcPublication getOrAddIpcPublication(
        final long registrationId, final int streamId, final String channel, final boolean isExclusive)
    {
        IpcPublication publication = null;

        if (!isExclusive)
        {
            publication = findIpcSharedPublication(ipcPublications, streamId);
        }

        if (null == publication)
        {
            publication = addIpcPublication(registrationId, streamId, channel, isExclusive);
        }

        return publication;
    }

    private IpcPublication addIpcPublication(
        final long registrationId, final int streamId, final String channel, final boolean isExclusive)
    {
        final PublicationParams params = getPublicationParams(AeronUri.parse(channel), isExclusive, true);

        final int sessionId = nextSessionId++;
        final int initialTermId = params.isReplay ? params.initialTermId : BitUtil.generateRandomisedId();

        final RawLog rawLog = newIpcPublicationLog(
            params.termLength, sessionId, streamId, initialTermId, registrationId);

        if (params.isReplay)
        {
            final int activeIndex = indexByTerm(initialTermId, params.termId);
            rawTail(rawLog.metaData(), activeIndex, packTail(params.termId, params.termOffset));
            activePartitionIndex(rawLog.metaData(), activeIndex);
        }

        final IpcPublication publication = new IpcPublication(
            registrationId,
            sessionId,
            streamId,
            PublisherLimit.allocate(countersManager, registrationId, sessionId, streamId, channel),
            rawLog,
            publicationUnblockTimeoutNs,
            context.systemCounters(),
            isExclusive);

        ipcPublications.add(publication);

        return publication;
    }

    private long nextImageCorrelationId()
    {
        return toDriverCommands.nextCorrelationId();
    }

    private static AeronClient findClient(final ArrayList<AeronClient> clients, final long clientId)
    {
        AeronClient aeronClient = null;

        for (int i = 0, size = clients.size(); i < size; i++)
        {
            final AeronClient client = clients.get(i);
            if (client.clientId() == clientId)
            {
                aeronClient = client;
                break;
            }
        }

        return aeronClient;
    }

    private static SubscriptionLink removeSubscriptionLink(
        final ArrayList<SubscriptionLink> subscriptionLinks, final long registrationId)
    {
        SubscriptionLink subscriptionLink = null;

        for (int i = 0, size = subscriptionLinks.size(), lastIndex = size - 1; i < size; i++)
        {
            final SubscriptionLink subscription = subscriptionLinks.get(i);
            if (subscription.registrationId() == registrationId)
            {
                subscriptionLink = subscription;
                fastUnorderedRemove(subscriptionLinks, i, lastIndex);
                break;
            }
        }

        return subscriptionLink;
    }

    private static IpcPublication findIpcSharedPublication(
        final ArrayList<IpcPublication> ipcPublications, final long streamId)
    {
        IpcPublication ipcPublication = null;

        for (int i = 0, size = ipcPublications.size(); i < size; i++)
        {
            final IpcPublication publication = ipcPublications.get(i);
            if (publication.streamId() == streamId &&
                !publication.isExclusive() &&
                IpcPublication.Status.ACTIVE == publication.status())
            {
                ipcPublication = publication;
                break;
            }
        }

        return ipcPublication;
    }

    private <T extends DriverManagedResource> void onCheckManagedResources(
        final ArrayList<T> list, final long nowNs, final long nowMs)
    {
        for (int lastIndex = list.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final DriverManagedResource resource = list.get(i);

            resource.onTimeEvent(nowNs, nowMs, this);

            if (resource.hasReachedEndOfLife())
            {
                fastUnorderedRemove(list, i, lastIndex);
                lastIndex--;
                resource.delete();
            }
        }
    }

    private void linkSpies(final ArrayList<SubscriptionLink> links, final NetworkPublication publication)
    {
        for (int i = 0, size = links.size(); i < size; i++)
        {
            final SubscriptionLink subscription = links.get(i);
            if (subscription.matches(publication) && !subscription.isLinked(publication))
            {
                linkSpy(publication, subscription);
            }
        }
    }

    private int processTimers(final long nowNs)
    {
        int workCount = 0;

        if (nowNs > (timeOfLastTimeoutCheck + HEARTBEAT_TIMEOUT_NS))
        {
            onHeartbeatCheckTimeouts(nowNs);
            onCheckForBlockedToDriverCommands(nowNs);
            timeOfLastTimeoutCheck = nowNs;
            workCount = 1;
        }

        return workCount;
    }

    private static String generateSourceIdentity(final InetSocketAddress address)
    {
        return address.getHostString() + ':' + address.getPort();
    }

    private static int getTermBufferLength(final AeronUri aeronUri, final int defaultTermLength)
    {
        final String termLengthParam = aeronUri.get(CommonContext.TERM_LENGTH_PARAM_NAME);
        int termLength = defaultTermLength;
        if (null != termLengthParam)
        {
            termLength = Integer.parseInt(termLengthParam);
            Configuration.validateTermBufferLength(termLength);
        }

        return termLength;
    }

    private static int getMtuLength(final AeronUri aeronUri, final int defaultMtuLength)
    {
        int mtuLength = defaultMtuLength;
        final String mtu = aeronUri.get(CommonContext.MTU_LENGTH_URI_PARAM_NAME);
        if (null != mtu)
        {
            mtuLength = Integer.parseInt(mtu);
            Configuration.validateMtuLength(mtuLength);
        }

        return mtuLength;
    }

    private PublicationParams getPublicationParams(
        final AeronUri aeronUri, final boolean isExclusive, final boolean isIpc)
    {
        final PublicationParams params = new PublicationParams();

        params.mtuLength = getMtuLength(aeronUri, context.mtuLength());
        params.termLength = getTermBufferLength(
            aeronUri, isIpc ? context.ipcTermBufferLength() : context.publicationTermBufferLength());

        if (isExclusive)
        {
            int count = 0;

            final String initTermIdStr = aeronUri.get(INITIAL_TERM_ID_PARAM_NAME);
            count = initTermIdStr != null ? count + 1 : count;

            final String termIdStr = aeronUri.get(TERM_ID_PARAM_NAME);
            count = termIdStr != null ? count + 1 : count;

            final String termOffsetStr = aeronUri.get(TERM_OFFSET_PARAM_NAME);
            count = termOffsetStr != null ? count + 1 : count;

            if (count > 0)
            {
                if (count < 3)
                {
                    throw new IllegalStateException("Params must be used as a set: " +
                        INITIAL_TERM_ID_PARAM_NAME + " " + TERM_ID_PARAM_NAME + " " + TERM_OFFSET_PARAM_NAME);
                }

                params.initialTermId = Integer.parseInt(initTermIdStr);
                params.termId = Integer.parseInt(termIdStr);
                params.termOffset = Integer.parseInt(termOffsetStr);

                if (params.termOffset > params.termLength)
                {
                    throw new IllegalStateException(
                        TERM_OFFSET_PARAM_NAME + "=" + params.termOffset + " > " +
                            TERM_LENGTH_PARAM_NAME + "=" + params.termLength);
                }

                params.isReplay = true;
            }
        }

        return params;
    }

    static class PublicationParams
    {
        int mtuLength = 0;
        int termLength = 0;
        int initialTermId = 0;
        int termId = 0;
        int termOffset = 0;
        boolean isReplay = false;
    }
}
