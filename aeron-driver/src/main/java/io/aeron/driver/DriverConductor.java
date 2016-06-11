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
import static java.util.stream.Collectors.toList;
import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.ErrorCode.*;
import static io.aeron.command.ControlProtocolEvents.*;
import static io.aeron.logbuffer.FrameDescriptor.computeMaxMessageLength;
import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static io.aeron.protocol.DataHeaderFlyweight.createDefaultHeader;

/**
 * Driver Conductor to take commands from publishers and subscribers as well as determining if loss has occurred.
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
    private final ArrayList<DirectPublication> directPublications = new ArrayList<>();

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
        directPublications.forEach(DirectPublication::close);
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

    DirectPublication getDirectPublication(final long streamId)
    {
        return findDirectPublication(directPublications, streamId);
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
            workCount += publicationImages.get(i).trackRebuild(now);
        }

        final ArrayList<NetworkPublication> networkPublications = this.networkPublications;
        for (int i = 0, size = networkPublications.size(); i < size; i++)
        {
            final NetworkPublication publication = networkPublications.get(i);
            workCount += publication.updatePublishersLimit() + publication.cleanLogBuffer();
        }

        final ArrayList<DirectPublication> directPublications = this.directPublications;
        for (int i = 0, size = directPublications.size(); i < size; i++)
        {
            final DirectPublication publication = directPublications.get(i);
            workCount +=
                publication.updatePublishersLimit(toDriverCommands.consumerHeartbeatTime()) + publication.cleanLogBuffer();
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

        final List<SubscriberPosition> subscriberPositions = listSubscriberPositions(
            sessionId, streamId, channelEndpoint, channel, joiningPosition);

        if (subscriberPositions.size() > 0)
        {
            final RawLog rawLog = newPublicationImageLog(
                sessionId, streamId, initialTermId, termBufferLength, senderMtuLength, udpChannel, registrationId);

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
                context.initialWindowLength(),
                rawLog,
                udpChannel.isMulticast() ? NAK_MULTICAST_DELAY_GENERATOR : NAK_UNICAST_DELAY_GENERATOR,
                subscriberPositions.stream().map(SubscriberPosition::position).collect(toList()),
                ReceiverHwm.allocate(countersManager, registrationId, sessionId, streamId, channel),
                nanoClock,
                context.systemCounters(),
                sourceAddress);

            subscriberPositions.forEach(
                (subscriberPosition) -> subscriberPosition.subscription().addImage(image, subscriberPosition.position()));

            publicationImages.add(image);
            receiverProxy.newPublicationImage(channelEndpoint, image);

            clientProxy.onAvailableImage(
                registrationId,
                streamId,
                sessionId,
                rawLog.logFileName(),
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
        final SendChannelEndpoint channelEndpoint = publication.sendChannelEndpoint();

        senderProxy.removeNetworkPublication(publication);

        if (publication.hasSpies())
        {
            clientProxy.onUnavailableImage(
                correlationId(publication.rawLog().logMetaData()),
                publication.streamId(),
                publication.sendChannelEndpoint().originalUriString());

            subscriptionLinks
                .stream()
                .filter((link) -> link.matches(channelEndpoint, publication.streamId()))
                .forEach((link) -> link.removeSpiedPublication(publication));
        }

        if (channelEndpoint.sessionCount() == 0)
        {
            sendChannelEndpointByChannelMap.remove(channelEndpoint.udpChannel().canonicalForm());
            senderProxy.closeSendChannelEndpoint(channelEndpoint);
        }
    }

    void cleanupSubscriptionLink(final SubscriptionLink link)
    {
        final ReceiveChannelEndpoint channelEndpoint = link.channelEndpoint();

        if (null != channelEndpoint)
        {
            final int streamId = link.streamId();

            if (0 == channelEndpoint.decRefToStream(link.streamId()))
            {
                receiverProxy.removeSubscription(channelEndpoint, streamId);
            }

            if (channelEndpoint.streamCount() == 0)
            {
                receiveChannelEndpointByChannelMap.remove(channelEndpoint.udpChannel().canonicalForm());
                receiverProxy.closeReceiveChannelEndpoint(channelEndpoint);
            }
        }
    }

    void imageTransitionToLinger(final PublicationImage image)
    {
        clientProxy.onUnavailableImage(
            image.correlationId(),
            image.streamId(),
            image.channelUriString());

        receiverProxy.removeCoolDown(image.channelEndpoint(), image.sessionId(), image.streamId());
    }

    void cleanupImage(final PublicationImage image)
    {
        subscriptionLinks
            .stream()
            .filter((link) -> image.matches(link.channelEndpoint(), link.streamId()))
            .forEach((link) -> link.removeImage(image));
    }

    private List<SubscriberPosition> listSubscriberPositions(
        final int sessionId,
        final int streamId,
        final ReceiveChannelEndpoint channelEndpoint,
        final String channel,
        final long joiningPosition)
    {
        return subscriptionLinks
            .stream()
            .filter((subscription) -> subscription.matches(channelEndpoint, streamId))
            .map(
                (subscription) ->
                {
                    final Position position = SubscriberPos.allocate(
                        countersManager, subscription.registrationId(), sessionId, streamId, channel);

                    position.setOrdered(joiningPosition);

                    return new SubscriberPosition(subscription, position);
                })
            .collect(toList());
    }

    private <T extends DriverManagedResource> void onCheckManagedResources(final ArrayList<T> list, final long time)
    {
        for (int i = list.size() - 1; i >= 0; i--)
        {
            final DriverManagedResource resource = list.get(i);

            resource.onTimeEvent(time, this);

            if (resource.hasReachedEndOfLife())
            {
                resource.delete();
                list.remove(i);
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
        onCheckManagedResources(directPublications, nanoTimeNow);
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

    private void onClientCommand(final int msgTypeId, final MutableDirectBuffer buffer, final int index, final int length)
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
                        onAddDirectPublication(channel, streamId, correlationId, clientId);
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
                        onAddDirectSubscription(channel, streamId, correlationId, clientId);
                    }
                    else
                    {
                        if (channel.startsWith(SPY_PREFIX))
                        {
                            onAddSpySubscription(channel.substring(SPY_PREFIX.length()), streamId, correlationId, clientId);
                        }
                        else
                        {
                            onAddNetworkSubscription(channel, streamId, correlationId, clientId);
                        }
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
                    correlationId = correlatedMessageFlyweight.correlationId();
                    onClientKeepalive(correlatedMessageFlyweight.clientId());
                    break;
                }
            }
        }
        catch (final ControlProtocolException ex)
        {
            clientProxy.onError(ex.errorCode(), ex.getMessage(), correlationId);
            errors.increment();
            errorLog.record(ex);
        }
        catch (final Exception ex)
        {
            clientProxy.onError(GENERIC_ERROR, ex.getMessage(), correlationId);
            errors.increment();
            errorLog.record(ex);
        }
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
        final SendChannelEndpoint channelEndpoint = getOrCreateSendChannelEndpoint(udpChannel);

        NetworkPublication publication = channelEndpoint.getPublication(streamId);
        if (null == publication)
        {
            final int termLength = getPublicationTermLength(udpChannel.aeronUri(), context.publicationTermBufferLength());
            final int sessionId = nextSessionId();
            final int initialTermId = BitUtil.generateRandomisedId();

            final RetransmitHandler retransmitHandler = new RetransmitHandler(
                nanoClock,
                context.systemCounters(),
                RETRANSMIT_UNICAST_DELAY_GENERATOR,
                RETRANSMIT_UNICAST_LINGER_GENERATOR,
                initialTermId,
                termLength);

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
                sessionId,
                streamId,
                initialTermId,
                context.mtuLength(),
                context.systemCounters(),
                flowControl,
                retransmitHandler,
                networkPublicationThreadLocals);

            channelEndpoint.addPublication(publication);
            networkPublications.add(publication);
            senderProxy.newNetworkPublication(publication);

            final NetworkPublication networkPublication = publication;

            subscriptionLinks
                .stream()
                .filter((link) -> link.matches(channelEndpoint, streamId))
                .forEach((link) -> linkSpy(networkPublication, link));
        }

        linkPublication(registrationId, publication, getOrAddClient(clientId));

        clientProxy.onPublicationReady(
            registrationId,
            streamId,
            publication.sessionId(),
            publication.rawLog().logFileName(),
            publication.publisherLimitId());
    }

    private static int getPublicationTermLength(final AeronUri aeronUri, final int defaultTermLength)
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

    private void onAddDirectPublication(final String channel, final int streamId, final long registrationId, final long clientId)
    {
        final DirectPublication directPublication = getOrAddDirectPublication(streamId, channel);
        final AeronClient client = getOrAddClient(clientId);

        linkPublication(registrationId, directPublication, client);

        clientProxy.onPublicationReady(
            registrationId,
            streamId,
            directPublication.sessionId(),
            directPublication.rawLog().logFileName(),
            directPublication.publisherLimitId());
    }

    private int nextSessionId()
    {
        return nextSessionId++;
    }

    private void linkPublication(final long registrationId, final DriverManagedResource publication, final AeronClient client)
    {
        if (null != findPublicationLink(publicationLinks, registrationId))
        {
            throw new ControlProtocolException(GENERIC_ERROR, "registration id already in use.");
        }

        publicationLinks.add(new PublicationLink(
            registrationId,
            publication,
            client,
            nanoClock.nanoTime(),
            publicationUnblockTimeoutNs,
            context.systemCounters()));
    }

    private RawLog newNetworkPublicationLog(
        final int sessionId,
        final int streamId,
        final int initialTermId,
        final UdpChannel udpChannel,
        final long registrationId,
        final int termBufferLength)
    {
        final String canonicalForm = udpChannel.canonicalForm();
        final RawLog rawLog = rawLogFactory.newNetworkPublication(
            canonicalForm, sessionId, streamId, registrationId, termBufferLength);

        final UnsafeBuffer header = createDefaultHeader(sessionId, streamId, initialTermId);
        final UnsafeBuffer logMetaData = rawLog.logMetaData();
        storeDefaultFrameHeader(logMetaData, header);

        final UnsafeBuffer termMetaData = rawLog.partitions()[0].metaDataBuffer();
        initialiseTailWithTermId(termMetaData, initialTermId);

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
        final String canonicalForm = udpChannel.canonicalForm();
        final RawLog rawLog = rawLogFactory.newNetworkedImage(
            canonicalForm, sessionId, streamId, correlationId, termBufferLength);

        final UnsafeBuffer header = createDefaultHeader(sessionId, streamId, initialTermId);
        final UnsafeBuffer logMetaData = rawLog.logMetaData();
        storeDefaultFrameHeader(logMetaData, header);
        initialTermId(logMetaData, initialTermId);
        mtuLength(logMetaData, senderMtuLength);
        correlationId(logMetaData, correlationId);
        timeOfLastStatusMessage(logMetaData, 0);

        return rawLog;
    }

    private RawLog newDirectPublicationLog(
        final int termBufferLength, final int sessionId, final int streamId, final int initialTermId, final long registrationId)
    {
        final RawLog rawLog = rawLogFactory.newDirectPublication(sessionId, streamId, registrationId, termBufferLength);

        final UnsafeBuffer header = createDefaultHeader(sessionId, streamId, initialTermId);
        final UnsafeBuffer logMetaData = rawLog.logMetaData();
        storeDefaultFrameHeader(logMetaData, header);

        final UnsafeBuffer termMetaData = rawLog.partitions()[0].metaDataBuffer();
        initialiseTailWithTermId(termMetaData, initialTermId);

        initialTermId(logMetaData, initialTermId);

        final int mtuLength = computeMaxMessageLength(termBufferLength);
        mtuLength(logMetaData, mtuLength);
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
        for (int i = 0, size = publicationLinks.size(); i < size; i++)
        {
            final PublicationLink link = publicationLinks.get(i);
            if (registrationId == link.registrationId())
            {
                publicationLink = link;
                publicationLinks.remove(i);
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
        final ReceiveChannelEndpoint channelEndpoint = getOrCreateReceiveChannelEndpoint(UdpChannel.parse(channel));

        final int refCount = channelEndpoint.incRefToStream(streamId);
        if (1 == refCount)
        {
            receiverProxy.addSubscription(channelEndpoint, streamId);
        }

        final AeronClient client = getOrAddClient(clientId);
        final SubscriptionLink subscription = new SubscriptionLink(registrationId, channelEndpoint, streamId, client);

        subscriptionLinks.add(subscription);
        clientProxy.operationSucceeded(registrationId);

        publicationImages
            .stream()
            .filter((image) -> image.matches(channelEndpoint, streamId) && (image.subscriberCount() > 0))
            .forEach(
                (image) ->
                {
                    final int sessionId = image.sessionId();
                    final Position position = SubscriberPos.allocate(
                        countersManager, registrationId, sessionId, streamId, channel);

                    position.setOrdered(image.rebuildPosition());

                    image.addSubscriber(position);
                    subscription.addImage(image, position);

                    clientProxy.onAvailableImage(
                        image.correlationId(),
                        streamId,
                        sessionId,
                        image.rawLog().logFileName(),
                        Collections.singletonList(new SubscriberPosition(subscription, position)),
                        generateSourceIdentity(image.sourceAddress()));
                });
    }

    private void onAddDirectSubscription(final String channel, final int streamId, final long registrationId, final long clientId)
    {
        final DirectPublication publication = getOrAddDirectPublication(streamId, channel);
        final AeronClient client = getOrAddClient(clientId);

        final int sessionId = publication.sessionId();
        final Position position = SubscriberPos.allocate(countersManager, registrationId, sessionId, streamId, IPC_CHANNEL);
        position.setOrdered(publication.joiningPosition());

        final SubscriptionLink subscriptionLink = new SubscriptionLink(registrationId, streamId, publication, position, client);

        subscriptionLinks.add(subscriptionLink);
        publication.addSubscription(position);

        clientProxy.operationSucceeded(registrationId);

        final List<SubscriberPosition> subscriberPositions = new ArrayList<>();
        subscriberPositions.add(new SubscriberPosition(subscriptionLink, position));

        clientProxy.onAvailableImage(
            publication.correlationId(),
            streamId,
            sessionId,
            publication.rawLog().logFileName(),
            subscriberPositions,
            IPC_CHANNEL);
    }

    private void onAddSpySubscription(final String channel, final int streamId, final long registrationId, final long clientId)
    {
        final UdpChannel udpChannel = UdpChannel.parse(channel);
        final SendChannelEndpoint channelEndpoint = senderChannelEndpoint(udpChannel);
        final NetworkPublication publication = null == channelEndpoint ? null : channelEndpoint.getPublication(streamId);
        final AeronClient client = getOrAddClient(clientId);
        final SubscriptionLink subscriptionLink = new SubscriptionLink(registrationId, udpChannel, streamId, client);

        subscriptionLinks.add(subscriptionLink);

        clientProxy.operationSucceeded(registrationId);

        if (null != publication)
        {
            linkSpy(publication, subscriptionLink);
        }
    }

    private void linkSpy(final NetworkPublication publication, final SubscriptionLink subscriptionLink)
    {
        final int streamId = publication.streamId();
        final int sessionId = publication.sessionId();
        final String channel = subscriptionLink.spiedChannel().originalUriString();
        final Position position =
            SubscriberPos.allocate(countersManager, subscriptionLink.registrationId(), sessionId, streamId, channel);
        position.setOrdered(publication.spyJoiningPosition());

        final List<SubscriberPosition> subscriberPositions = new ArrayList<>();
        subscriberPositions.add(new SubscriberPosition(subscriptionLink, position));

        publication.addSpyPosition(position);
        subscriptionLink.addSpiedPublication(publication, position);

        clientProxy.onAvailableImage(
            correlationId(publication.rawLog().logMetaData()),
            streamId,
            sessionId,
            publication.rawLog().logFileName(),
            subscriberPositions,
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
        final SubscriptionLink link = removeSubscriptionLink(subscriptionLinks, registrationId);
        if (null == link)
        {
            throw new ControlProtocolException(UNKNOWN_SUBSCRIPTION, "Unknown subscription link: " + registrationId);
        }

        link.close();
        final ReceiveChannelEndpoint channelEndpoint = link.channelEndpoint();

        if (null != channelEndpoint)
        {
            final int refCount = channelEndpoint.decRefToStream(link.streamId());
            if (0 == refCount)
            {
                receiverProxy.removeSubscription(channelEndpoint, link.streamId());
            }

            if (0 == channelEndpoint.streamCount())
            {
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

    private DirectPublication getOrAddDirectPublication(final int streamId, final String channel)
    {
        DirectPublication publication = findDirectPublication(directPublications, streamId);

        if (null == publication)
        {
            final int termLength = getPublicationTermLength(AeronUri.parse(channel), context.ipcTermBufferLength());
            final long registrationId = nextImageCorrelationId();
            final int sessionId = nextSessionId();
            final int initialTermId = BitUtil.generateRandomisedId();
            final RawLog rawLog = newDirectPublicationLog(termLength, sessionId, streamId, initialTermId, registrationId);

            final Position publisherLimit =
                PublisherLimit.allocate(countersManager, registrationId, sessionId, streamId, IPC_CHANNEL);

            publication = new DirectPublication(registrationId, sessionId, streamId, publisherLimit, rawLog);

            directPublications.add(publication);
        }

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

    private static PublicationLink findPublicationLink(
        final ArrayList<PublicationLink> publicationLinks, final long registrationId)
    {
        PublicationLink publicationLink = null;

        for (int i = 0, size = publicationLinks.size(); i < size; i++)
        {
            final PublicationLink link = publicationLinks.get(i);
            if (registrationId == link.registrationId())
            {
                publicationLink = link;
                break;
            }
        }

        return publicationLink;
    }

    private static SubscriptionLink removeSubscriptionLink(
        final ArrayList<SubscriptionLink> subscriptionLinks, final long registrationId)
    {
        SubscriptionLink subscriptionLink = null;

        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            final SubscriptionLink link = subscriptionLinks.get(i);
            if (link.registrationId() == registrationId)
            {
                subscriptionLink = link;
                subscriptionLinks.remove(i);
                break;
            }
        }

        return subscriptionLink;
    }

    private static DirectPublication findDirectPublication(
        final ArrayList<DirectPublication> directPublications, final long streamId)
    {
        DirectPublication directPublication = null;

        for (int i = 0, size = directPublications.size(); i < size; i++)
        {
            final DirectPublication log = directPublications.get(i);
            if (log.streamId() == streamId)
            {
                directPublication = log;
                break;
            }
        }

        return directPublication;
    }

    private static String generateSourceIdentity(final InetSocketAddress address)
    {
        return address.getHostString() + ':' + address.getPort();
    }
}
