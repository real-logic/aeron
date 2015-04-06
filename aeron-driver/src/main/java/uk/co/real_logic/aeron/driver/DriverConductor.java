/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
package uk.co.real_logic.aeron.driver;

import uk.co.real_logic.aeron.common.*;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor;
import uk.co.real_logic.aeron.driver.buffer.*;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.TimerWheel;
import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;
import uk.co.real_logic.aeron.common.command.CorrelatedMessageFlyweight;
import uk.co.real_logic.aeron.common.command.PublicationMessageFlyweight;
import uk.co.real_logic.aeron.common.command.RemoveMessageFlyweight;
import uk.co.real_logic.aeron.common.command.SubscriptionMessageFlyweight;
import uk.co.real_logic.agrona.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.common.event.EventCode;
import uk.co.real_logic.aeron.common.event.EventConfiguration;
import uk.co.real_logic.aeron.common.event.EventLogger;
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;
import uk.co.real_logic.agrona.status.BufferPositionIndicator;
import uk.co.real_logic.agrona.status.BufferPositionReporter;
import uk.co.real_logic.aeron.driver.cmd.DriverConductorCmd;
import uk.co.real_logic.aeron.driver.cmd.ElicitSetupMessageFromSourceCmd;
import uk.co.real_logic.aeron.driver.exceptions.ControlProtocolException;
import uk.co.real_logic.aeron.driver.exceptions.InvalidChannelException;
import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.concurrent.*;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toList;
import static uk.co.real_logic.aeron.common.ErrorCode.*;
import static uk.co.real_logic.aeron.common.command.ControlProtocolEvents.*;
import static uk.co.real_logic.aeron.driver.Configuration.CONNECTION_LIVENESS_TIMEOUT_NS;
import static uk.co.real_logic.aeron.driver.Configuration.RETRANS_UNICAST_DELAY_DEFAULT_NS;
import static uk.co.real_logic.aeron.driver.Configuration.RETRANS_UNICAST_LINGER_DEFAULT_NS;
import static uk.co.real_logic.aeron.driver.MediaDriver.Context;

/**
 * Driver Conductor to take commands from publishers and subscribers as well as handle NAKs and retransmissions
 */
public class DriverConductor implements Agent
{
    public static final int HEARTBEAT_TIMEOUT_MS = 1000;  // how often to check liveness & cleanup

    /**
     * Unicast NAK delay is immediate initial with delayed subsequent delay
     */
    public static final StaticDelayGenerator NAK_UNICAST_DELAY_GENERATOR = new StaticDelayGenerator(
        Configuration.NAK_UNICAST_DELAY_DEFAULT_NS, true);

    public static final OptimalMulticastDelayGenerator NAK_MULTICAST_DELAY_GENERATOR = new OptimalMulticastDelayGenerator(
        Configuration.NAK_MAX_BACKOFF_DEFAULT, Configuration.NAK_GROUPSIZE_DEFAULT, Configuration.NAK_GRTT_DEFAULT);

    /**
     * Source uses same for unicast and multicast. For ticks.
     */
    public static final FeedbackDelayGenerator RETRANS_UNICAST_DELAY_GENERATOR = () -> RETRANS_UNICAST_DELAY_DEFAULT_NS;
    public static final FeedbackDelayGenerator RETRANS_UNICAST_LINGER_GENERATOR = () -> RETRANS_UNICAST_LINGER_DEFAULT_NS;

    private final OneToOneConcurrentArrayQueue<DriverConductorCmd> driverConductorCmdQueue;
    private final ReceiverProxy receiverProxy;
    private final SenderProxy senderProxy;
    private final ClientProxy clientProxy;
    private final DriverConductorProxy conductorProxy;
    private final TransportPoller transportPoller;
    private final RawLogFactory rawLogFactory;
    private final RingBuffer toDriverCommands;
    private final RingBuffer toEventReader;
    private final HashMap<String, SendChannelEndpoint> sendChannelEndpointByChannelMap = new HashMap<>();
    private final HashMap<String, ReceiveChannelEndpoint> receiveChannelEndpointByChannelMap = new HashMap<>();
    private final TimerWheel timerWheel;
    private final ArrayList<DriverPublication> publications = new ArrayList<>();
    private final Long2ObjectHashMap<PublicationRegistration> publicationRegistrations = new Long2ObjectHashMap<>();
    private final ArrayList<DriverSubscription> subscriptions = new ArrayList<>();
    private final ArrayList<DriverConnection> connections = new ArrayList<>();
    private final ArrayList<AeronClient> clients = new ArrayList<>();
    private final ArrayList<ElicitSetupMessageFromSourceCmd> pendingSetups = new ArrayList<>();

    private final Supplier<SenderFlowControl> unicastSenderFlowControl;
    private final Supplier<SenderFlowControl> multicastSenderFlowControl;

    private final PublicationMessageFlyweight publicationMessage = new PublicationMessageFlyweight();
    private final SubscriptionMessageFlyweight subscriptionMessage = new SubscriptionMessageFlyweight();
    private final CorrelatedMessageFlyweight correlatedMessage = new CorrelatedMessageFlyweight();
    private final RemoveMessageFlyweight removeMessage = new RemoveMessageFlyweight();

    private final int mtuLength;
    private final int capacity;
    private final int initialWindowLength;
    private final long statusMessageTimeout;
    private final long dataLossSeed;
    private final long controlLossSeed;
    private final double dataLossRate;
    private final double controlLossRate;
    private final TimerWheel.Timer checkTimeoutTimer;
    private final CountersManager countersManager;
    private final UnsafeBuffer countersBuffer;
    private final EventLogger logger;

    private final SystemCounters systemCounters;
    private final Consumer<DriverConductorCmd> onDriverConductorCmdFunc = this::onDriverConductorCmd;
    private final MessageHandler onClientCommandFunc  = this::onClientCommand;
    private final MessageHandler onEventFunc;
    private final NanoClock clock;

    public DriverConductor(final Context ctx)
    {
        this.driverConductorCmdQueue = ctx.conductorCommandQueue();
        this.receiverProxy = ctx.receiverProxy();
        this.senderProxy = ctx.senderProxy();
        this.rawLogFactory = ctx.rawLogBuffersFactory();
        this.transportPoller = ctx.conductorNioSelector();
        this.mtuLength = ctx.mtuLength();
        this.initialWindowLength = ctx.initialWindowLength();
        this.capacity = ctx.termBufferLength();
        this.statusMessageTimeout = ctx.statusMessageTimeout();
        this.unicastSenderFlowControl = ctx.unicastSenderFlowControl();
        this.multicastSenderFlowControl = ctx.multicastSenderFlowControl();
        this.countersManager = ctx.countersManager();
        this.countersBuffer = ctx.countersBuffer();

        timerWheel = ctx.conductorTimerWheel();
        this.clock = timerWheel.clock();
        checkTimeoutTimer = timerWheel.newTimeout(HEARTBEAT_TIMEOUT_MS, TimeUnit.MILLISECONDS, this::onHeartbeatCheckTimeouts);

        toDriverCommands = ctx.toDriverCommands();
        toEventReader = ctx.toEventReader();
        clientProxy = ctx.clientProxy();
        conductorProxy = ctx.driverConductorProxy();
        logger = ctx.eventLogger();
        dataLossRate = ctx.dataLossRate();
        dataLossSeed = ctx.dataLossSeed();
        controlLossRate = ctx.controlLossRate();
        controlLossSeed = ctx.controlLossSeed();

        systemCounters = ctx.systemCounters();

        final Consumer<String> eventConsumer = ctx.eventConsumer();
        onEventFunc =
            (typeId, buffer, offset, length) -> eventConsumer.accept(EventCode.get(typeId).decode(buffer, offset, length));

        final AtomicBuffer buffer = toDriverCommands.buffer();
        publicationMessage.wrap(buffer, 0);
        subscriptionMessage.wrap(buffer, 0);
        correlatedMessage.wrap(buffer, 0);
        removeMessage.wrap(buffer, 0);

        toDriverCommands.consumerHeartbeatTimeNs(clock.time());
    }

    public void onClose()
    {
        rawLogFactory.close();
        publications.forEach(DriverPublication::close);
        connections.forEach(DriverConnection::close);
        sendChannelEndpointByChannelMap.values().forEach(SendChannelEndpoint::close);
        receiveChannelEndpointByChannelMap.values().forEach(ReceiveChannelEndpoint::close);
    }

    public String roleName()
    {
        return "driver-conductor";
    }

    public SendChannelEndpoint senderChannelEndpoint(final UdpChannel channel)
    {
        return sendChannelEndpointByChannelMap.get(channel.canonicalForm());
    }

    public ReceiveChannelEndpoint receiverChannelEndpoint(final UdpChannel channel)
    {
        return receiveChannelEndpointByChannelMap.get(channel.canonicalForm());
    }

    public int doWork() throws Exception
    {
        int workCount = 0;

        workCount += transportPoller.pollTransports();
        workCount += toDriverCommands.read(onClientCommandFunc);
        workCount += driverConductorCmdQueue.drain(onDriverConductorCmdFunc);
        workCount += toEventReader.read(onEventFunc, EventConfiguration.EVENT_READER_FRAME_LIMIT);
        workCount += processTimers();

        final ArrayList<DriverConnection> connections = this.connections;
        final long now = clock.time();
        for (int i = 0, size = connections.size(); i < size; i++)
        {
            final DriverConnection connection = connections.get(i);
            workCount += connection.trackCompletion() + connection.sendPendingStatusMessage(now, statusMessageTimeout);
        }

        final ArrayList<DriverPublication> publications = this.publications;
        for (int i = 0, size = publications.size(); i < size; i++)
        {
            final DriverPublication publication = publications.get(i);
            workCount += publication.updatePublishersLimit() + publication.cleanLogBuffer();
        }

        return workCount;
    }

    private void onHeartbeatCheckTimeouts()
    {
        final long now = clock.time();

        toDriverCommands.consumerHeartbeatTimeNs(now);

        onCheckClients(now);
        onCheckPublications(now);
        onCheckPublicationRegistrations(now);
        onCheckSubscriptions(now);
        onCheckConnections(now);
        onCheckPendingSetups(now);

        timerWheel.rescheduleTimeout(HEARTBEAT_TIMEOUT_MS, TimeUnit.MILLISECONDS, checkTimeoutTimer);
    }

    private void onClientCommand(final int msgTypeId, final MutableDirectBuffer buffer, final int index, final int length)
    {
        Flyweight flyweight = null;

        try
        {
            switch (msgTypeId)
            {
                case ADD_PUBLICATION:
                {
                    logger.log(EventCode.CMD_IN_ADD_PUBLICATION, buffer, index, length);

                    final PublicationMessageFlyweight publicationMessageFlyweight = publicationMessage;
                    publicationMessageFlyweight.offset(index);
                    flyweight = publicationMessageFlyweight;

                    onAddPublication(
                        publicationMessageFlyweight.channel(),
                        publicationMessageFlyweight.sessionId(),
                        publicationMessageFlyweight.streamId(),
                        publicationMessageFlyweight.correlationId(),
                        publicationMessageFlyweight.clientId());
                    break;
                }

                case REMOVE_PUBLICATION:
                {
                    logger.log(EventCode.CMD_IN_REMOVE_PUBLICATION, buffer, index, length);

                    final RemoveMessageFlyweight removeMessageFlyweight = removeMessage;
                    removeMessageFlyweight.offset(index);
                    flyweight = removeMessageFlyweight;

                    onRemovePublication(removeMessageFlyweight.registrationId(), removeMessageFlyweight.correlationId());
                    break;
                }

                case ADD_SUBSCRIPTION:
                {
                    logger.log(EventCode.CMD_IN_ADD_SUBSCRIPTION, buffer, index, length);

                    final SubscriptionMessageFlyweight subscriptionMessageFlyweight = subscriptionMessage;
                    subscriptionMessageFlyweight.offset(index);
                    flyweight = subscriptionMessageFlyweight;

                    onAddSubscription(
                        subscriptionMessageFlyweight.channel(),
                        subscriptionMessageFlyweight.streamId(),
                        subscriptionMessageFlyweight.correlationId(),
                        subscriptionMessageFlyweight.clientId());
                    break;
                }

                case REMOVE_SUBSCRIPTION:
                {
                    logger.log(EventCode.CMD_IN_REMOVE_SUBSCRIPTION, buffer, index, length);

                    final RemoveMessageFlyweight removeMessageFlyweight = removeMessage;
                    removeMessageFlyweight.offset(index);
                    flyweight = removeMessageFlyweight;

                    onRemoveSubscription(removeMessageFlyweight.registrationId(), removeMessageFlyweight.correlationId());
                    break;
                }

                case CLIENT_KEEPALIVE:
                {
                    logger.log(EventCode.CMD_IN_KEEPALIVE_CLIENT, buffer, index, length);

                    final CorrelatedMessageFlyweight correlatedMessageFlyweight = correlatedMessage;
                    correlatedMessageFlyweight.offset(index);
                    flyweight = correlatedMessageFlyweight;

                    onClientKeepalive(correlatedMessageFlyweight.clientId());
                    break;
                }
            }
        }
        catch (final ControlProtocolException ex)
        {
            clientProxy.onError(ex.errorCode(), ex.getMessage(), flyweight, length);
            logger.logException(ex);
        }
        catch (final InvalidChannelException ex)
        {
            clientProxy.onError(INVALID_CHANNEL, ex.getMessage(), flyweight, length);
            logger.logException(ex);
        }
        catch (final Exception ex)
        {
            clientProxy.onError(GENERIC_ERROR, ex.getMessage(), flyweight, length);
            logger.logException(ex);
        }
    }

    private int processTimers()
    {
        int workCount = 0;

        if (timerWheel.computeDelayInMs() <= 0)
        {
            workCount = timerWheel.expireTimers();
        }

        return workCount;
    }

    private void onAddPublication(
        final String channel, final int sessionId, final int streamId, final long correlationId, final long clientId)
    {
        final UdpChannel udpChannel = UdpChannel.parse(channel);
        logger.logChannelCreated(udpChannel.description());

        SendChannelEndpoint channelEndpoint = sendChannelEndpointByChannelMap.get(udpChannel.canonicalForm());
        if (null == channelEndpoint)
        {
            channelEndpoint = new SendChannelEndpoint(
                udpChannel,
                transportPoller,
                logger,
                Configuration.createLossGenerator(controlLossRate, controlLossSeed),
                systemCounters);
            transportPoller.selectNowWithoutProcessing();

            channelEndpoint.validateMtuLength(mtuLength);
            sendChannelEndpointByChannelMap.put(udpChannel.canonicalForm(), channelEndpoint);
        }

        final AeronClient aeronClient = getOrAddClient(clientId);
        DriverPublication publication = channelEndpoint.getPublication(sessionId, streamId);
        if (publication == null)
        {
            final int initialTermId = BitUtil.generateRandomisedId();
            final String canonicalForm = udpChannel.canonicalForm();
            final RawLog rawLog = rawLogFactory.newPublication(canonicalForm, sessionId, streamId, correlationId);

            final MutableDirectBuffer header = DataHeaderFlyweight.createDefaultHeader(sessionId, streamId, initialTermId);
            final UnsafeBuffer logMetaData = rawLog.logMetaData();
            LogBufferDescriptor.storeDefaultFrameHeaders(logMetaData, header);
            LogBufferDescriptor.initialTermId(logMetaData, initialTermId);

            final int senderPositionId = allocatePositionCounter("sender pos", channel, sessionId, streamId);
            final int publisherLimitId = allocatePositionCounter("publisher limit", channel, sessionId, streamId);
            final SenderFlowControl senderFlowControl =
                udpChannel.isMulticast() ? multicastSenderFlowControl.get() : unicastSenderFlowControl.get();

            publication = new DriverPublication(
                channelEndpoint,
                clock,
                rawLog,
                new BufferPositionReporter(countersBuffer, senderPositionId, countersManager),
                new BufferPositionReporter(countersBuffer, publisherLimitId, countersManager),
                sessionId,
                streamId,
                initialTermId,
                DataHeaderFlyweight.HEADER_LENGTH,
                mtuLength,
                senderFlowControl.initialPositionLimit(initialTermId, capacity),
                systemCounters);

            final RetransmitHandler retransmitHandler = new RetransmitHandler(
                timerWheel,
                systemCounters,
                DriverConductor.RETRANS_UNICAST_DELAY_GENERATOR,
                DriverConductor.RETRANS_UNICAST_LINGER_GENERATOR,
                composeNewRetransmitSender(publication),
                initialTermId,
                capacity);

            channelEndpoint.addPublication(publication, retransmitHandler, senderFlowControl);
            publications.add(publication);

            senderProxy.newPublication(publication);
        }

        final PublicationRegistration existingRegistration = publicationRegistrations.put(
            correlationId, new PublicationRegistration(publication, aeronClient));
        if (null != existingRegistration)
        {
            publicationRegistrations.put(correlationId, existingRegistration);
            throw new ControlProtocolException(GENERIC_ERROR, "registration id already in use.");
        }

        publication.incRef();

        clientProxy.onPublicationReady(
            channel,
            streamId,
            sessionId,
            publication.rawLogBuffers(),
            correlationId,
            publication.publisherLimitCounterId(),
            mtuLength);
    }

    private void onRemovePublication(final long registrationId, final long correlationId)
    {
        final PublicationRegistration registration = publicationRegistrations.remove(registrationId);
        if (registration == null)
        {
            throw new ControlProtocolException(UNKNOWN_PUBLICATION, "Unknown publication: " + registrationId);
        }

        registration.remove();
        clientProxy.operationSucceeded(correlationId);
    }

    private void onAddSubscription(final String channel, final int streamId, final long correlationId, final long clientId)
    {
        final UdpChannel udpChannel = UdpChannel.parse(channel);
        ReceiveChannelEndpoint channelEndpoint = receiveChannelEndpointByChannelMap.get(udpChannel.canonicalForm());

        if (null == channelEndpoint)
        {
            final LossGenerator lossGenerator = Configuration.createLossGenerator(dataLossRate, dataLossSeed);
            channelEndpoint = new ReceiveChannelEndpoint(udpChannel, conductorProxy, logger, systemCounters, lossGenerator);

            receiveChannelEndpointByChannelMap.put(udpChannel.canonicalForm(), channelEndpoint);
            receiverProxy.registerMediaEndpoint(channelEndpoint);
        }

        channelEndpoint.incRefToStream(streamId);
        receiverProxy.addSubscription(channelEndpoint, streamId);

        final AeronClient client = getOrAddClient(clientId);
        final DriverSubscription subscription = new DriverSubscription(correlationId, channelEndpoint, client, streamId);

        subscriptions.add(subscription);
        clientProxy.operationSucceeded(correlationId);

        for (final DriverConnection connection : connections)
        {
            if (connection.matches(channelEndpoint, streamId))
            {
                final int subscriberPositionCounterId = allocatePositionCounter(
                    "subscriber pos", channel, connection.sessionId(), streamId);
                final BufferPositionIndicator indicator = new BufferPositionIndicator(
                    countersBuffer, subscriberPositionCounterId, countersManager);
                final String sourceInfo = generateSourceInfo(connection.sourceAddress());
                connection.addSubscription(indicator);
                subscription.addConnection(connection, indicator);

                clientProxy.onConnectionReady(
                    channel,
                    streamId,
                    connection.sessionId(),
                    connection.completedPosition(),
                    connection.rawLogBuffers(),
                    correlationId,
                    Collections.singletonList(new SubscriberPosition(subscription, subscriberPositionCounterId, indicator)),
                    sourceInfo);
            }
        }
    }

    private void onRemoveSubscription(final long registrationId, final long correlationId)
    {
        final DriverSubscription subscription = removeSubscription(subscriptions, registrationId);
        if (null == subscription)
        {
            throw new ControlProtocolException(UNKNOWN_SUBSCRIPTION, "Unknown subscription: " + registrationId);
        }

        subscription.close();
        final ReceiveChannelEndpoint channelEndpoint = subscription.receiveChannelEndpoint();

        final int refCount = channelEndpoint.decRefToStream(subscription.streamId());
        if (0 == refCount)
        {
            receiverProxy.removeSubscription(channelEndpoint, subscription.streamId());
        }

        if (channelEndpoint.streamCount() == 0)
        {
            receiveChannelEndpointByChannelMap.remove(channelEndpoint.udpChannel().canonicalForm());
            receiverProxy.closeReceiveChannelEndpoint(channelEndpoint);

            while (!channelEndpoint.isClosed())
            {
                Thread.yield();
            }
        }

        clientProxy.operationSucceeded(correlationId);
    }

    public void onCreateConnection(
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
        channelEndpoint.validateWindowMaxLength(initialWindowLength);

        final UdpChannel udpChannel = channelEndpoint.udpChannel();
        final String channel = udpChannel.originalUriString();
        final long correlationId = generateCreationCorrelationId();

        final RawLog rawLog = rawLogFactory.newConnection(
            udpChannel.canonicalForm(), sessionId, streamId, correlationId, termBufferLength);
        final long joiningPosition = LogBufferDescriptor.computePosition(
            activeTermId, initialTermOffset, Integer.numberOfTrailingZeros(termBufferLength), initialTermId);

        final List<SubscriberPosition> subscriberPositions = subscriptions
            .stream()
            .filter((subscription) -> subscription.matches(streamId, channelEndpoint))
            .map(
                (subscription) ->
                {
                    final int positionCounterId = allocatePositionCounter("subscriber pos", channel, sessionId, streamId);
                    final BufferPositionIndicator indicator = new BufferPositionIndicator(
                        countersBuffer, positionCounterId, countersManager);
                    countersManager.setCounterValue(positionCounterId, joiningPosition);

                    return new SubscriberPosition(subscription, positionCounterId, indicator);
                })
            .collect(toList());

        final int receiverHwmCounterId = allocatePositionCounter("receiver hwm", channel, sessionId, streamId);
        final String sourceInfo = generateSourceInfo(sourceAddress);

        clientProxy.onConnectionReady(
            channel,
            streamId,
            sessionId,
            joiningPosition,
            rawLog,
            correlationId,
            subscriberPositions,
            sourceInfo);

        final LossHandler lossHandler = new LossHandler(
            timerWheel,
            udpChannel.isMulticast() ? NAK_MULTICAST_DELAY_GENERATOR : NAK_UNICAST_DELAY_GENERATOR,
            channelEndpoint.composeNakMessageSender(controlAddress, sessionId, streamId),
            systemCounters);

        final DriverConnection connection = new DriverConnection(
            channelEndpoint,
            correlationId,
            sessionId,
            streamId,
            initialTermId,
            activeTermId,
            initialTermOffset,
            initialWindowLength,
            rawLog,
            lossHandler,
            channelEndpoint.composeStatusMessageSender(controlAddress, sessionId, streamId),
            subscriberPositions.stream().map(SubscriberPosition::positionIndicator).collect(toList()),
            new BufferPositionReporter(countersBuffer, receiverHwmCounterId, countersManager),
            clock,
            systemCounters,
            sourceAddress,
            logger);

        connections.add(connection);

        subscriberPositions.forEach(
            (subscriberPosition) ->
                subscriberPosition.subscription().addConnection(connection, subscriberPosition.positionIndicator()));

        receiverProxy.newConnection(channelEndpoint, connection);
    }

    private void onClientKeepalive(final long clientId)
    {
        systemCounters.clientKeepAlives().addOrdered(1);

        final AeronClient aeronClient = findClient(clients, clientId);
        if (null != aeronClient)
        {
            aeronClient.timeOfLastKeepalive(clock.time());
        }
    }

    private void onCheckPublicationRegistrations(final long now)
    {
        final Iterator<PublicationRegistration> iter = publicationRegistrations.values().iterator();
        while (iter.hasNext())
        {
            final PublicationRegistration registration = iter.next();
            if (registration.hasClientTimedOut(now))
            {
                iter.remove();
            }
        }
    }

    private void onCheckPublications(final long now)
    {
        final ArrayList<DriverPublication> publications = this.publications;
        for (int i = publications.size() - 1; i >= 0; i--)
        {
            final DriverPublication publication = publications.get(i);

            if (publication.isUnreferencedAndFlushed(now) &&
                now > (publication.timeOfFlush() + Configuration.PUBLICATION_LINGER_NS))
            {
                final SendChannelEndpoint channelEndpoint = publication.sendChannelEndpoint();

                logger.logPublicationRemoval(
                    channelEndpoint.udpChannel().originalUriString(), publication.sessionId(), publication.streamId());

                channelEndpoint.removePublication(publication.sessionId(), publication.streamId());
                publications.remove(i);

                senderProxy.closePublication(publication);

                if (channelEndpoint.sessionCount() == 0)
                {
                    sendChannelEndpointByChannelMap.remove(channelEndpoint.udpChannel().canonicalForm());
                    channelEndpoint.close();
                    transportPoller.selectNowWithoutProcessing();
                }
            }
        }
    }

    private void onCheckSubscriptions(final long now)
    {
        final ArrayList<DriverSubscription> subscriptions = this.subscriptions;
        for (int i = subscriptions.size() - 1; i >= 0; i--)
        {
            final DriverSubscription subscription = subscriptions.get(i);

            if (now > (subscription.timeOfLastKeepaliveFromClient() + Configuration.CLIENT_LIVENESS_TIMEOUT_NS))
            {
                final ReceiveChannelEndpoint channelEndpoint = subscription.receiveChannelEndpoint();
                final int streamId = subscription.streamId();

                logger.logSubscriptionRemoval(
                    channelEndpoint.udpChannel().originalUriString(),
                    subscription.streamId(),
                    subscription.registrationId());

                subscriptions.remove(i);
                subscription.close();

                if (0 == channelEndpoint.decRefToStream(subscription.streamId()))
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
    }

    private void onCheckConnections(final long now)
    {
        final ArrayList<DriverConnection> connections = this.connections;
        for (int i = connections.size() - 1; i >= 0; i--)
        {
            final DriverConnection connection = connections.get(i);
            final String uriString = connection.receiveChannelEndpoint().udpChannel().originalUriString();

            switch (connection.status())
            {
                case INACTIVE:
                    if (connection.remaining() == 0 ||
                        now > (connection.timeOfLastStatusChange() + CONNECTION_LIVENESS_TIMEOUT_NS))
                    {
                        connection.status(DriverConnection.Status.LINGER);

                        clientProxy.onInactiveConnection(
                            connection.correlationId(), connection.sessionId(), connection.streamId(), uriString);
                    }
                    break;

                case LINGER:
                    if (now > (connection.timeOfLastStatusChange() + CONNECTION_LIVENESS_TIMEOUT_NS))
                    {
                        logger.logConnectionRemoval(uriString, connection.sessionId(), connection.streamId());

                        connections.remove(i);
                        connection.close();
                    }
                    break;
            }
        }
    }

    private void onCheckClients(final long now)
    {
        for (int i = clients.size() - 1; i >= 0; i--)
        {
            final AeronClient aeronClient = clients.get(i);

            if (now > (aeronClient.timeOfLastKeepalive() + CONNECTION_LIVENESS_TIMEOUT_NS))
            {
                clients.remove(i);
            }
        }
    }

    private void onCheckPendingSetups(final long now)
    {
        for (int i = pendingSetups.size() - 1; i >= 0; i--)
        {
            final ElicitSetupMessageFromSourceCmd cmd = pendingSetups.get(i);

            if (now > (cmd.timeOfStatusMessage() + Configuration.PENDING_SETUPS_TIMEOUT_NS))
            {
                pendingSetups.remove(i);
                receiverProxy.removePendingSetup(cmd.channelEndpoint(), cmd.sessionId(), cmd.streamId());
            }
        }
    }

    public void onElicitSetupMessageFromSender(final ElicitSetupMessageFromSourceCmd cmd)
    {
        final int sessionId = cmd.sessionId();
        final int streamId = cmd.streamId();
        final InetSocketAddress controlAddress = cmd.controlAddress();
        final ReceiveChannelEndpoint channelEndpoint = cmd.channelEndpoint();

        channelEndpoint.sendSetupElicitingStatusMessage(controlAddress, sessionId, streamId);
        cmd.timeOfStatusMessage(clock.time());

        pendingSetups.add(cmd);
    }

    private void onDriverConductorCmd(final DriverConductorCmd cmd)
    {
        cmd.execute(this);
    }

    private AeronClient getOrAddClient(final long clientId)
    {
        AeronClient aeronClient = findClient(clients, clientId);
        if (null == aeronClient)
        {
            aeronClient = new AeronClient(clientId, clock.time());
            clients.add(aeronClient);
        }

        return aeronClient;
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

    private int allocatePositionCounter(final String type, final String dirName, final int sessionId, final int streamId)
    {
        return countersManager.allocate(String.format("%s: %s %x %x", type, dirName, sessionId, streamId));
    }

    private static String generateSourceInfo(final InetSocketAddress address)
    {
        return String.format("%s:%d", address.getHostString(), address.getPort());
    }

    private static DriverSubscription removeSubscription(
        final ArrayList<DriverSubscription> subscriptions, final long registrationId)
    {
        DriverSubscription subscription = null;
        for (int i = 0, size = subscriptions.size(); i < size; i++)
        {
            subscription = subscriptions.get(i);
            if (subscription.registrationId() == registrationId)
            {
                subscriptions.remove(i);
                break;
            }
        }

        return subscription;
    }

    private RetransmitSender composeNewRetransmitSender(final DriverPublication publication)
    {
        return (termId, termOffset, length) -> senderProxy.retransmit(publication, termId, termOffset, length);
    }

    private long generateCreationCorrelationId()
    {
        return toDriverCommands.nextCorrelationId();
    }
}
