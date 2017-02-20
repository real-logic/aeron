/*
 * Copyright 2014 - 2016 Real Logic Ltd.
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

import io.aeron.ArrayListUtil;
import io.aeron.CommonContext;
import io.aeron.driver.buffer.RawLogFactory;
import io.aeron.command.CorrelatedMessageFlyweight;
import io.aeron.command.PublicationMessageFlyweight;
import io.aeron.command.RemoveMessageFlyweight;
import io.aeron.command.SubscriptionMessageFlyweight;
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
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.*;
import org.agrona.concurrent.errors.DistinctErrorLog;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.Position;
import org.agrona.concurrent.status.ReadablePosition;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;

import static io.aeron.CommonContext.SPY_PREFIX;
import static io.aeron.driver.Configuration.*;
import static io.aeron.driver.status.SystemCounterDescriptor.CLIENT_KEEP_ALIVES;
import static io.aeron.driver.status.SystemCounterDescriptor.ERRORS;
import static io.aeron.driver.status.SystemCounterDescriptor.UNBLOCKED_COMMANDS;
import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.ErrorCode.*;
import static io.aeron.command.ControlProtocolEvents.*;
import static io.aeron.logbuffer.FrameDescriptor.computeMaxMessageLength;
import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static io.aeron.protocol.DataHeaderFlyweight.createDefaultHeader;

/**
 * Driver Conductor that takes commands from publishers and subscribers and orchestrates the media driver.
 */
public class DriverConductor implements Agent
{
    private final long imageLivenessTimeoutNs;
    private final long clientLivenessTimeoutNs;
    private final long publicationUnblockTimeoutNs;
    private long timeOfLastToDriverPositionChange;
    private long lastConsumerCommandPosition;
    private long timeOfLastTimeoutCheck;
    private int nextSessionId = BitUtil.generateRandomisedId();

    private final NetworkPublicationThreadLocals networkPublicationThreadLocals = new NetworkPublicationThreadLocals();
    private final Context context;
    private final RawLogFactory rawLogFactory;
    private final ReceiverProxy receiverProxy;
    private final SenderProxy senderProxy;
    private final ClientProxy clientProxy;
    private final DriverConductorProxy fromReceiverConductorProxy;
    private final RingBuffer toDriverCommands;
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

    private final PublicationMessageFlyweight publicationMsgFlyweight = new PublicationMessageFlyweight();
    private final SubscriptionMessageFlyweight subscriptionMsgFlyweight = new SubscriptionMessageFlyweight();
    private final CorrelatedMessageFlyweight correlatedMsgFlyweight = new CorrelatedMessageFlyweight();
    private final RemoveMessageFlyweight removeMsgFlyweight = new RemoveMessageFlyweight();

    private final EpochClock epochClock;
    private final NanoClock nanoClock;

    private final DistinctErrorLog errorLog;
    private final Consumer<DriverConductorCmd> onDriverConductorCmdFunc = this::onDriverConductorCmd;
    private final MessageHandler onClientCommandFunc = this::onClientCommand;

    private final CountersManager countersManager;
    private final AtomicCounter clientKeepAlives;
    private final AtomicCounter errors;

    public DriverConductor(final Context ctx)
    {
        context = ctx;
        imageLivenessTimeoutNs = ctx.imageLivenessTimeoutNs();
        clientLivenessTimeoutNs = ctx.clientLivenessTimeoutNs();
        publicationUnblockTimeoutNs = ctx.publicationUnblockTimeoutNs();
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
        errorLog = ctx.errorLog();

        countersManager = context.countersManager();
        clientKeepAlives = context.systemCounters().get(CLIENT_KEEP_ALIVES);
        errors = context.systemCounters().get(ERRORS);

        toDriverCommands.consumerHeartbeatTime(epochClock.time());

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

    SendChannelEndpoint senderChannelEndpoint(final UdpChannel channel)
    {
        return sendChannelEndpointByChannelMap.get(channel.canonicalForm());
    }

    ReceiveChannelEndpoint receiverChannelEndpoint(final UdpChannel channel)
    {
        return receiveChannelEndpointByChannelMap.get(channel.canonicalForm());
    }

    IpcPublication getIpcPublication(final long streamId)
    {
        return findIpcPublication(ipcPublications, streamId);
    }

    public int doWork() throws Exception
    {
        int workCount = 0;

        workCount += toDriverCommands.read(onClientCommandFunc);
        workCount += fromReceiverDriverConductorCmdQueue.drain(onDriverConductorCmdFunc);
        workCount += fromSenderDriverConductorCmdQueue.drain(onDriverConductorCmdFunc);

        final long now = nanoClock.nanoTime();
        workCount += processTimers(now);

        final ArrayList<PublicationImage> publicationImages = this.publicationImages;
        for (int i = 0, size = publicationImages.size(); i < size; i++)
        {
            publicationImages.get(i).trackRebuild(now, context.statusMessageTimeout());
        }

        final ArrayList<NetworkPublication> networkPublications = this.networkPublications;
        for (int i = 0, size = networkPublications.size(); i < size; i++)
        {
            workCount += networkPublications.get(i).updatePublishersLimit();
        }

        final ArrayList<IpcPublication> ipcPublications = this.ipcPublications;
        for (int i = 0, size = ipcPublications.size(); i < size; i++)
        {
            workCount += ipcPublications.get(i).updatePublishersLimit(toDriverCommands.consumerHeartbeatTime());
        }

        return workCount;
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

    public void onClosePublication(final NetworkPublication publication)
    {
        publication.close();
    }

    void cleanupPublication(final NetworkPublication publication)
    {
        if (publication.hasSpies())
        {
            clientProxy.onUnavailableImage(
                correlationId(publication.rawLog().metaData()),
                publication.streamId(),
                publication.sendChannelEndpoint().originalUriString());

            for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
            {
                final SubscriptionLink subscription = subscriptionLinks.get(i);
                if (subscription.matches(publication))
                {
                    subscription.unlink(publication);
                }
            }
        }

        senderProxy.removeNetworkPublication(publication);

        final SendChannelEndpoint channelEndpoint = publication.sendChannelEndpoint();
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
        clientProxy.onUnavailableImage(publication.correlationId(), publication.streamId(), CommonContext.IPC_CHANNEL);
    }

    void cleanupImage(final PublicationImage image)
    {
        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            final SubscriptionLink subscription = subscriptionLinks.get(i);
            if (image.matches(subscription.channelEndpoint(), subscription.streamId()))
            {
                subscription.unlink(image);
            }
        }
    }

    void cleanupIpcPublication(final IpcPublication publication)
    {
        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            final SubscriptionLink subscription = subscriptionLinks.get(i);
            if (subscription.matches(publication.streamId()))
            {
                subscription.unlink(publication);
            }
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

    private <T extends DriverManagedResource> void onCheckManagedResources(final ArrayList<T> list, final long time)
    {
        for (int lastIndex = list.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final DriverManagedResource resource = list.get(i);

            resource.onTimeEvent(time, this);

            if (resource.hasReachedEndOfLife())
            {
                ArrayListUtil.fastUnorderedRemove(list, i, lastIndex);
                lastIndex--;
                resource.delete();
            }
        }
    }

    private void onHeartbeatCheckTimeouts(final long nanoTimeNow)
    {
        toDriverCommands.consumerHeartbeatTime(epochClock.time());

        onCheckManagedResources(clients, nanoTimeNow);
        onCheckManagedResources(publicationLinks, nanoTimeNow);
        onCheckManagedResources(networkPublications, nanoTimeNow);
        onCheckManagedResources(subscriptionLinks, nanoTimeNow);
        onCheckManagedResources(publicationImages, nanoTimeNow);
        onCheckManagedResources(ipcPublications, nanoTimeNow);
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

    private void onClientCommand(
        final int msgTypeId,
        final MutableDirectBuffer buffer,
        final int index,
        @SuppressWarnings("unused") final int length)
    {
        long correlationId = 0;

        try
        {
            switch (msgTypeId)
            {
                case ADD_PUBLICATION:
                {
                    final PublicationMessageFlyweight publicationMessageFlyweight = publicationMsgFlyweight;
                    publicationMessageFlyweight.wrap(buffer, index);

                    correlationId = publicationMessageFlyweight.correlationId();
                    final int streamId = publicationMessageFlyweight.streamId();
                    final long clientId = publicationMessageFlyweight.clientId();
                    final String channel = publicationMessageFlyweight.channel();

                    if (channel.startsWith(IPC_CHANNEL))
                    {
                        onAddIpcPublication(channel, streamId, correlationId, clientId);
                    }
                    else
                    {
                        onAddNetworkPublication(channel, streamId, correlationId, clientId);
                    }
                    break;
                }

                case REMOVE_PUBLICATION:
                {
                    final RemoveMessageFlyweight removeMessageFlyweight = removeMsgFlyweight;
                    removeMessageFlyweight.wrap(buffer, index);
                    correlationId = removeMessageFlyweight.correlationId();
                    onRemovePublication(removeMessageFlyweight.registrationId(), correlationId);
                    break;
                }

                case ADD_SUBSCRIPTION:
                {
                    final SubscriptionMessageFlyweight subscriptionMessageFlyweight = subscriptionMsgFlyweight;
                    subscriptionMessageFlyweight.wrap(buffer, index);

                    correlationId = subscriptionMessageFlyweight.correlationId();
                    final int streamId = subscriptionMessageFlyweight.streamId();
                    final long clientId = subscriptionMessageFlyweight.clientId();
                    final String channel = subscriptionMessageFlyweight.channel();

                    if (channel.startsWith(IPC_CHANNEL))
                    {
                        onAddIpcSubscription(channel, streamId, correlationId, clientId);
                    }
                    else if (channel.startsWith(SPY_PREFIX))
                    {
                        onAddSpySubscription(channel.substring(SPY_PREFIX.length()), streamId, correlationId, clientId);
                    }
                    else
                    {
                        onAddNetworkSubscription(channel, streamId, correlationId, clientId);
                    }
                    break;
                }

                case REMOVE_SUBSCRIPTION:
                {
                    final RemoveMessageFlyweight removeMessageFlyweight = removeMsgFlyweight;
                    removeMessageFlyweight.wrap(buffer, index);
                    correlationId = removeMessageFlyweight.correlationId();
                    onRemoveSubscription(removeMessageFlyweight.registrationId(), correlationId);
                    break;
                }

                case CLIENT_KEEPALIVE:
                {
                    final CorrelatedMessageFlyweight correlatedMessageFlyweight = correlatedMsgFlyweight;
                    correlatedMessageFlyweight.wrap(buffer, index);
                    onClientKeepalive(correlatedMessageFlyweight.clientId());
                    break;
                }
            }
        }
        catch (final ControlProtocolException ex)
        {
            clientProxy.onError(ex.errorCode(), ex.getMessage(), correlationId);
            recordError(ex);
        }
        catch (final Exception ex)
        {
            clientProxy.onError(GENERIC_ERROR, ex.getMessage(), correlationId);
            recordError(ex);
        }
    }

    private void recordError(final Exception ex)
    {
        errors.increment();
        errorLog.record(ex);
    }

    private int processTimers(final long now)
    {
        int workCount = 0;

        if (now > (timeOfLastTimeoutCheck + HEARTBEAT_TIMEOUT_NS))
        {
            onHeartbeatCheckTimeouts(now);
            onCheckForBlockedToDriverCommands(now);
            timeOfLastTimeoutCheck = now;
            workCount = 1;
        }

        return workCount;
    }

    private void onAddNetworkPublication(
        final String channel, final int streamId, final long registrationId, final long clientId)
    {
        final UdpChannel udpChannel = UdpChannel.parse(channel);
        final AeronUri aeronUri = udpChannel.aeronUri();
        final int mtuLength = getMtuLength(aeronUri, context.mtuLength());
        final int termLength = getTermBufferLength(aeronUri, context.publicationTermBufferLength());
        final SendChannelEndpoint channelEndpoint = getOrCreateSendChannelEndpoint(udpChannel);

        NetworkPublication publication = channelEndpoint.getPublication(streamId);
        if (null == publication)
        {
            final int sessionId = nextSessionId++;
            final int initialTermId = BitUtil.generateRandomisedId();

            final RetransmitHandler retransmitHandler = new RetransmitHandler(
                nanoClock,
                context.systemCounters(),
                RETRANSMIT_UNICAST_DELAY_GENERATOR,
                RETRANSMIT_UNICAST_LINGER_GENERATOR);

            final FlowControl flowControl =
                udpChannel.isMulticast() ?
                    context.multicastFlowControlSupplier().newInstance(udpChannel, streamId, registrationId) :
                    context.unicastFlowControlSupplier().newInstance(udpChannel, streamId, registrationId);

            publication = new NetworkPublication(
                channelEndpoint,
                nanoClock,
                toDriverCommands::consumerHeartbeatTime,
                newNetworkPublicationLog(sessionId, streamId, initialTermId, udpChannel, registrationId, termLength),
                PublisherLimit.allocate(countersManager, registrationId, sessionId, streamId, channel),
                SenderPos.allocate(countersManager, registrationId, sessionId, streamId, channel),
                SenderLimit.allocate(countersManager, registrationId, sessionId, streamId, channel),
                sessionId,
                streamId,
                initialTermId,
                mtuLength,
                context.systemCounters(),
                flowControl,
                retransmitHandler,
                networkPublicationThreadLocals,
                publicationUnblockTimeoutNs);

            channelEndpoint.addPublication(publication);
            networkPublications.add(publication);
            senderProxy.newNetworkPublication(publication);
            linkSpies(publication);
        }
        else if (publication.mtuLength() != mtuLength)
        {
            throw new IllegalStateException("Existing publication has different MTU length: existing=" +
                publication.mtuLength() + " requested=" + mtuLength);
        }

        publicationLinks.add(new PublicationLink(registrationId, publication, getOrAddClient(clientId)));

        clientProxy.onPublicationReady(
            registrationId,
            streamId,
            publication.sessionId(),
            publication.rawLog().fileName(),
            publication.publisherLimitId());
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

    private void linkSpies(final NetworkPublication publication)
    {
        final ArrayList<SubscriptionLink> links = this.subscriptionLinks;
        for (int i = 0, size = links.size(); i < size; i++)
        {
            final SubscriptionLink subscription = links.get(i);
            if (subscription.matches(publication))
            {
                linkSpy(publication, subscription);
            }
        }
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

    private void onAddIpcPublication(
        final String channel, final int streamId, final long registrationId, final long clientId)
    {
        final IpcPublication ipcPublication = getOrAddIpcPublication(streamId, channel);
        publicationLinks.add(new PublicationLink(registrationId, ipcPublication, getOrAddClient(clientId)));

        clientProxy.onPublicationReady(
            registrationId,
            streamId,
            ipcPublication.sessionId(),
            ipcPublication.rawLog().fileName(),
            ipcPublication.publisherLimitId());

        linkIpcSubscriptions(ipcPublication);
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

        mtuLength(logMetaData, computeMaxMessageLength(termBufferLength));
        correlationId(logMetaData, registrationId);
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

    private void onRemovePublication(final long registrationId, final long correlationId)
    {
        PublicationLink publicationLink = null;
        final ArrayList<PublicationLink> publicationLinks = this.publicationLinks;
        for (int i = 0, size = publicationLinks.size(), lastIndex = size - 1; i < size; i++)
        {
            final PublicationLink publication = publicationLinks.get(i);
            if (registrationId == publication.registrationId())
            {
                publicationLink = publication;
                ArrayListUtil.fastUnorderedRemove(publicationLinks, i, lastIndex);
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

    private void onAddNetworkSubscription(
        final String channel, final int streamId, final long registrationId, final long clientId)
    {
        final UdpChannel udpChannel = UdpChannel.parse(channel);
        final String reliableParam = udpChannel.aeronUri().get("reliable", "true");
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

    private void onAddIpcSubscription(
        final String channel, final int streamId, final long registrationId, final long clientId)
    {
        final IpcSubscriptionLink subscription = new IpcSubscriptionLink(
            registrationId, streamId, channel, getOrAddClient(clientId), context.clientLivenessTimeoutNs());

        subscriptionLinks.add(subscription);
        clientProxy.operationSucceeded(registrationId);

        final IpcPublication publication = findIpcPublication(ipcPublications, streamId);
        if (null != publication)
        {
            linkIpcSubscription(subscription, publication);
        }
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
        publication.addSubscription(position);

        clientProxy.onAvailableImage(
            publication.correlationId(),
            streamId,
            sessionId,
            publication.rawLog().fileName(),
            Collections.singletonList(new SubscriberPosition(subscription, position)),
            channel);
    }

    private void linkIpcSubscriptions(final IpcPublication publication)
    {
        final int streamId = publication.streamId();
        final ArrayList<SubscriptionLink> subscriptionLinks = this.subscriptionLinks;

        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            final SubscriptionLink subscription = subscriptionLinks.get(i);
            if (subscription.matches(streamId))
            {
                linkIpcSubscription((IpcSubscriptionLink)subscription, publication);
            }
        }
    }

    private void onAddSpySubscription(
        final String channel, final int streamId, final long registrationId, final long clientId)
    {
        final UdpChannel udpChannel = UdpChannel.parse(channel);
        final AeronClient client = getOrAddClient(clientId);
        final SpySubscriptionLink subscriptionLink = new SpySubscriptionLink(
            registrationId, udpChannel, streamId, client, context.clientLivenessTimeoutNs());

        subscriptionLinks.add(subscriptionLink);
        clientProxy.operationSucceeded(registrationId);

        final SendChannelEndpoint channelEndpoint = senderChannelEndpoint(udpChannel);
        final NetworkPublication publication =
            null == channelEndpoint ? null : channelEndpoint.getPublication(streamId);
        if (null != publication)
        {
            linkSpy(publication, subscriptionLink);
        }
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
        publication.addSpyPosition(position);
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

    private void onRemoveSubscription(final long registrationId, final long correlationId)
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

    private void onClientKeepalive(final long clientId)
    {
        clientKeepAlives.addOrdered(1);

        final AeronClient client = findClient(clients, clientId);
        if (null != client)
        {
            client.timeOfLastKeepalive(nanoClock.nanoTime());
        }
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

    private IpcPublication getOrAddIpcPublication(final int streamId, final String channel)
    {
        IpcPublication publication = findIpcPublication(ipcPublications, streamId);

        if (null == publication)
        {
            publication = addIpcPublication(streamId, channel);
        }

        return publication;
    }

    private IpcPublication addIpcPublication(final int streamId, final String channel)
    {
        final int termLength = getTermBufferLength(AeronUri.parse(channel), context.ipcTermBufferLength());
        final long registrationId = nextImageCorrelationId();
        final int sessionId = nextSessionId++;
        final int initialTermId = BitUtil.generateRandomisedId();
        final RawLog rawLog = newIpcPublicationLog(
            termLength, sessionId, streamId, initialTermId, registrationId);

        final Position publisherLimit = PublisherLimit.allocate(
            countersManager, registrationId, sessionId, streamId, channel);

        final IpcPublication publication = new IpcPublication(
            registrationId,
            sessionId,
            streamId,
            publisherLimit,
            rawLog,
            publicationUnblockTimeoutNs,
            context.systemCounters());

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
                ArrayListUtil.fastUnorderedRemove(subscriptionLinks, i, lastIndex);
                break;
            }
        }

        return subscriptionLink;
    }

    private static IpcPublication findIpcPublication(
        final ArrayList<IpcPublication> ipcPublications, final long streamId)
    {
        IpcPublication ipcPublication = null;

        for (int i = 0, size = ipcPublications.size(); i < size; i++)
        {
            final IpcPublication publication = ipcPublications.get(i);
            if (publication.streamId() == streamId && IpcPublication.Status.ACTIVE == publication.status())
            {
                ipcPublication = publication;
                break;
            }
        }

        return ipcPublication;
    }

    private static String generateSourceIdentity(final InetSocketAddress address)
    {
        return address.getHostString() + ':' + address.getPort();
    }
}
