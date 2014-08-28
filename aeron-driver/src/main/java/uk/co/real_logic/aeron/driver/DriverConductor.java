/*
 * Copyright 2014 Real Logic Ltd.
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
import uk.co.real_logic.aeron.common.collections.Long2ObjectHashMap;
import uk.co.real_logic.aeron.common.command.CorrelatedMessageFlyweight;
import uk.co.real_logic.aeron.common.command.PublicationMessageFlyweight;
import uk.co.real_logic.aeron.common.command.RemoveMessageFlyweight;
import uk.co.real_logic.aeron.common.command.SubscriptionMessageFlyweight;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.CountersManager;
import uk.co.real_logic.aeron.common.concurrent.MessageHandler;
import uk.co.real_logic.aeron.common.concurrent.OneToOneConcurrentArrayQueue;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.GapScanner;
import uk.co.real_logic.aeron.common.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.common.event.EventCode;
import uk.co.real_logic.aeron.common.event.EventLogger;
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.common.status.BufferPositionIndicator;
import uk.co.real_logic.aeron.common.status.BufferPositionReporter;
import uk.co.real_logic.aeron.common.status.PositionReporter;
import uk.co.real_logic.aeron.driver.buffer.TermBuffers;
import uk.co.real_logic.aeron.driver.buffer.TermBuffersFactory;
import uk.co.real_logic.aeron.driver.cmd.*;
import uk.co.real_logic.aeron.driver.exceptions.ControlProtocolException;
import uk.co.real_logic.aeron.driver.exceptions.InvalidChannelException;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static uk.co.real_logic.aeron.common.ErrorCode.*;
import static uk.co.real_logic.aeron.common.command.ControlProtocolEvents.*;
import static uk.co.real_logic.aeron.driver.Configuration.RETRANS_UNICAST_DELAY_DEFAULT_NS;
import static uk.co.real_logic.aeron.driver.Configuration.RETRANS_UNICAST_LINGER_DEFAULT_NS;
import static uk.co.real_logic.aeron.driver.MediaDriver.Context;

/**
 * Driver Conductor to take commands from publishers and subscribers as well as handle NAKs and retransmissions
 */
public class DriverConductor extends Agent
{
    public static final int CHECK_TIMEOUT_MS = 1000;  // how often to check liveness & cleanup

    /**
     * Unicast NAK delay is immediate initial with delayed subsequent delay
     */
    public static final StaticDelayGenerator NAK_UNICAST_DELAY_GENERATOR = new StaticDelayGenerator(
        Configuration.NAK_UNICAST_DELAY_DEFAULT_NS, true);

    public static final OptimalMulticastDelayGenerator NAK_MULTICAST_DELAY_GENERATOR = new OptimalMulticastDelayGenerator(
        Configuration.NAK_MAX_BACKOFF_DEFAULT, Configuration.NAK_GROUPSIZE_DEFAULT, Configuration.NAK_GRTT_DEFAULT);

    /**
     * Source uses same for unicast and multicast. For now.
     */
    public static final FeedbackDelayGenerator RETRANS_UNICAST_DELAY_GENERATOR = () -> RETRANS_UNICAST_DELAY_DEFAULT_NS;
    public static final FeedbackDelayGenerator RETRANS_UNICAST_LINGER_GENERATOR = () -> RETRANS_UNICAST_LINGER_DEFAULT_NS;

    private final OneToOneConcurrentArrayQueue<Object> commandQueue;
    private final ReceiverProxy receiverProxy;
    private final SenderProxy senderProxy;
    private final ClientProxy clientProxy;
    private final DriverConductorProxy conductorProxy;
    private final NioSelector nioSelector;
    private final TermBuffersFactory termBuffersFactory;
    private final RingBuffer fromClientCommands;
    private final HashMap<String, SendChannelEndpoint> sendChannelEndpointByChannelMap = new HashMap<>();
    private final HashMap<String, ReceiveChannelEndpoint> receiveChannelEndpointByChannelMap = new HashMap<>();
    private final TimerWheel timerWheel;
    private final ArrayList<DriverPublication> publications = new ArrayList<>();
    private final Long2ObjectHashMap<PublicationRegistration> publicationRegistrations = new Long2ObjectHashMap<>();
    private final ArrayList<DriverSubscription> subscriptions = new ArrayList<>();
    private final ArrayList<DriverConnection> connections = new ArrayList<>();
    private final ArrayList<AeronClient> clients = new ArrayList<>();
    private final ArrayList<ElicitSetupFromSourceCmd> pendingSetups = new ArrayList<>();

    private final Supplier<SenderFlowControl> unicastSenderFlowControl;
    private final Supplier<SenderFlowControl> multicastSenderFlowControl;

    private final PublicationMessageFlyweight publicationMessage = new PublicationMessageFlyweight();
    private final SubscriptionMessageFlyweight subscriptionMessage = new SubscriptionMessageFlyweight();
    private final CorrelatedMessageFlyweight correlatedMessage = new CorrelatedMessageFlyweight();
    private final RemoveMessageFlyweight removeMessage = new RemoveMessageFlyweight();

    private final int mtuLength;
    private final int capacity;
    private final int initialWindowSize;
    private final long statusMessageTimeout;
    private final long dataLossSeed;
    private final long controlLossSeed;
    private final double dataLossRate;
    private final double controlLossRate;
    private final TimerWheel.Timer publicationCheckTimer;
    private final TimerWheel.Timer publicationRegistrationCheckTimer;
    private final TimerWheel.Timer subscriptionCheckTimer;
    private final TimerWheel.Timer connectionCheckTimer;
    private final TimerWheel.Timer clientCheckTimer;
    private final TimerWheel.Timer pendingSetupsTimer;
    private final CountersManager countersManager;
    private final AtomicBuffer countersBuffer;
    private final EventLogger logger;

    private final SystemCounters systemCounters;
    private final Consumer<Object> onReceiverCommandFunc;
    private final MessageHandler onClientCommandFunc;

    public DriverConductor(final Context ctx)
    {
        super(ctx.conductorIdleStrategy(), ctx.eventLoggerException());

        this.commandQueue = ctx.conductorCommandQueue();
        this.receiverProxy = ctx.receiverProxy();
        this.senderProxy = ctx.senderProxy();
        this.termBuffersFactory = ctx.termBuffersFactory();
        this.nioSelector = ctx.conductorNioSelector();
        this.mtuLength = ctx.mtuLength();
        this.initialWindowSize = ctx.initialWindowSize();
        this.capacity = ctx.termBufferSize();
        this.statusMessageTimeout = ctx.statusMessageTimeout();
        this.unicastSenderFlowControl = ctx.unicastSenderFlowControl();
        this.multicastSenderFlowControl = ctx.multicastSenderFlowControl();
        this.countersManager = ctx.countersManager();
        this.countersBuffer = ctx.countersBuffer();

        timerWheel = ctx.conductorTimerWheel();
        // TODO: can these become a single timer?
        publicationCheckTimer = newTimeout(CHECK_TIMEOUT_MS, TimeUnit.MILLISECONDS, this::onCheckPublications);
        publicationRegistrationCheckTimer = newTimeout(CHECK_TIMEOUT_MS, TimeUnit.MILLISECONDS, this::onCheckPublicationRegistrations);
        subscriptionCheckTimer = newTimeout(CHECK_TIMEOUT_MS, TimeUnit.MILLISECONDS, this::onCheckSubscriptions);
        connectionCheckTimer = newTimeout(CHECK_TIMEOUT_MS, TimeUnit.MILLISECONDS, this::onCheckConnections);
        clientCheckTimer = newTimeout(CHECK_TIMEOUT_MS, TimeUnit.MILLISECONDS, this::onCheckClients);
        pendingSetupsTimer = newTimeout(CHECK_TIMEOUT_MS, TimeUnit.MILLISECONDS, this::onCheckPendingSetups);

        fromClientCommands = ctx.fromClientCommands();
        clientProxy = ctx.clientProxy();
        conductorProxy = ctx.driverConductorProxy();
        logger = ctx.eventLogger();
        dataLossRate = ctx.dataLossRate();
        dataLossSeed = ctx.dataLossSeed();
        controlLossRate = ctx.controlLossRate();
        controlLossSeed = ctx.controlLossSeed();

        systemCounters = new SystemCounters(countersManager);

        onReceiverCommandFunc = this::onReceiverCommand;
        onClientCommandFunc = this::onClientCommand;
    }

    public void onClose()
    {
        termBuffersFactory.close();
        publications.forEach(DriverPublication::close);
        connections.forEach(DriverConnection::close);
        sendChannelEndpointByChannelMap.values().forEach(SendChannelEndpoint::close);
        receiveChannelEndpointByChannelMap.values().forEach(ReceiveChannelEndpoint::close);
        systemCounters.close();
    }

    // TODO fix test to use proper collaboration assertions.
    public List<DriverSubscription> subscriptions()
    {
        return subscriptions;
    }

    // TODO fix test to use proper collaboration assertions.
    public List<DriverPublication> publications()
    {
        return publications;
    }

    public SendChannelEndpoint senderChannelEndpoint(final UdpChannel channel)
    {
        return sendChannelEndpointByChannelMap.get(channel.canonicalForm());
    }

    public ReceiveChannelEndpoint receiverChannelEndpoint(final UdpChannel channel)
    {
        return receiveChannelEndpointByChannelMap.get(channel.canonicalForm());
    }

    public void onReceiverCommand(Object obj)
    {
        if (obj instanceof CreateConnectionCmd)
        {
            onCreateConnection((CreateConnectionCmd)obj);
        }
        else if (obj instanceof ElicitSetupFromSourceCmd)
        {
            onElicitSetupFromSender((ElicitSetupFromSourceCmd)obj);
        }
    }

    public int doWork() throws Exception
    {
        int workCount = 0;

        workCount += nioSelector.processKeys();
        workCount += fromClientCommands.read(onClientCommandFunc);
        workCount += commandQueue.drain(onReceiverCommandFunc);
        workCount += processTimers();

        final ArrayList<DriverConnection> connections = this.connections;
        final long now = timerWheel.now();
        for (int i = 0, size = connections.size(); i < size; i++)
        {
            workCount += connections.get(i).sendPendingStatusMessages(now);
        }

        for (int i = 0, size = connections.size(); i < size; i++)
        {
            workCount += connections.get(i).scanForGaps();
        }

        for (int i = 0, size = connections.size(); i < size; i++)
        {
            workCount += connections.get(i).cleanLogBuffer();
        }

        final ArrayList<DriverPublication> publications = this.publications;
        for (int i = 0, size = publications.size(); i < size; i++)
        {
            workCount += publications.get(i).cleanLogBuffer();
        }

        return workCount;
    }

    private void onClientCommand(final int msgTypeId, final AtomicBuffer buffer, final int index, final int length)
    {
        Flyweight flyweight = null;
        final PublicationMessageFlyweight publicationMessageFlyweight = publicationMessage;
        final SubscriptionMessageFlyweight subscriptionMessageFlyweight = subscriptionMessage;
        final CorrelatedMessageFlyweight correlatedMessageFlyweight = correlatedMessage;
        final RemoveMessageFlyweight removeMessageFlyweight = removeMessage;

        try
        {
            switch (msgTypeId)
            {
                case ADD_PUBLICATION:
                    publicationMessageFlyweight.wrap(buffer, index);
                    logger.log(EventCode.CMD_IN_ADD_PUBLICATION, buffer, index, length);
                    flyweight = publicationMessageFlyweight;
                    onAddPublication(
                        publicationMessageFlyweight.channel(),
                        publicationMessageFlyweight.sessionId(),
                        publicationMessageFlyweight.streamId(),
                        publicationMessageFlyweight.correlationId(),
                        publicationMessageFlyweight.clientId());
                    break;

                case REMOVE_PUBLICATION:
                    removeMessageFlyweight.wrap(buffer, index);
                    logger.log(EventCode.CMD_IN_REMOVE_PUBLICATION, buffer, index, length);
                    flyweight = removeMessageFlyweight;
                    onRemovePublication(
                        removeMessageFlyweight.registrationCorrelationId(),
                        removeMessageFlyweight.correlationId());
                    break;

                case ADD_SUBSCRIPTION:
                    subscriptionMessageFlyweight.wrap(buffer, index);
                    logger.log(EventCode.CMD_IN_ADD_SUBSCRIPTION, buffer, index, length);
                    flyweight = subscriptionMessageFlyweight;
                    onAddSubscription(
                        subscriptionMessageFlyweight.channel(),
                        subscriptionMessageFlyweight.streamId(),
                        subscriptionMessageFlyweight.correlationId(),
                        subscriptionMessageFlyweight.clientId());
                    break;

                case REMOVE_SUBSCRIPTION:
                    subscriptionMessageFlyweight.wrap(buffer, index);
                    logger.log(EventCode.CMD_IN_REMOVE_SUBSCRIPTION, buffer, index, length);
                    flyweight = subscriptionMessageFlyweight;
                    onRemoveSubscription(
                        subscriptionMessageFlyweight.registrationCorrelationId(),
                        subscriptionMessageFlyweight.correlationId());
                    break;

                case KEEPALIVE_CLIENT:
                    correlatedMessageFlyweight.wrap(buffer, index);
                    logger.log(EventCode.CMD_IN_KEEPALIVE_CLIENT, buffer, index, length);
                    flyweight = correlatedMessageFlyweight;
                    onClientKeepalive(correlatedMessageFlyweight.clientId());
                    break;
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

        if (timerWheel.calculateDelayInMs() <= 0)
        {
            workCount = timerWheel.expireTimers();
        }

        return workCount;
    }

    private void onAddPublication(
        final String channel, final int sessionId, final int streamId, final long correlationId, final long clientId)
    {
        final UdpChannel udpChannel = UdpChannel.parse(channel);
        SendChannelEndpoint channelEndpoint = sendChannelEndpointByChannelMap.get(udpChannel.canonicalForm());
        if (null == channelEndpoint)
        {
            channelEndpoint = new SendChannelEndpoint(
                udpChannel,
                nioSelector,
                logger,
                Configuration.createLossGenerator(controlLossRate, controlLossSeed),
                systemCounters);

            sendChannelEndpointByChannelMap.put(udpChannel.canonicalForm(), channelEndpoint);
        }

        final AeronClient aeronClient = getOrAddClient(clientId);

        DriverPublication publication = channelEndpoint.getPublication(sessionId, streamId);
        if (publication == null)
        {
            final int initialTermId = BitUtil.generateRandomisedId();
            final String canonicalForm = udpChannel.canonicalForm();
            final TermBuffers termBuffers = termBuffersFactory.newPublication(canonicalForm, sessionId, streamId);

            final int positionCounterId = allocatePositionCounter("publisher limit", channel, sessionId, streamId);
            final PositionReporter positionReporter = new BufferPositionReporter(
                countersBuffer, positionCounterId, countersManager);

            final SenderFlowControl senderFlowControl =
                udpChannel.isMulticast() ? multicastSenderFlowControl.get() : unicastSenderFlowControl.get();

            publication = new DriverPublication(
                correlationId,
                channelEndpoint,
                timerWheel,
                termBuffers,
                positionReporter,
                sessionId,
                streamId,
                initialTermId,
                DataHeaderFlyweight.HEADER_LENGTH,
                mtuLength,
                senderFlowControl.initialPositionLimit(initialTermId, capacity),
                logger,
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

            final NewPublicationCmd cmd = new NewPublicationCmd(publication);
            while (!senderProxy.newPublication(cmd))
            {
                systemCounters.senderProxyFails().orderedIncrement();
                Thread.yield();
            }
        }

        publication.incRef();
        final int initialTermId = publication.initialTermId();
        final TermBuffers termBuffers = publication.termBuffers();
        final int positionCounterId = publication.positionCounterId();

        clientProxy.onNewTermBuffers(
            ON_NEW_PUBLICATION, channel, streamId, sessionId, initialTermId, termBuffers, correlationId, positionCounterId);

        publicationRegistrations.put(correlationId, new PublicationRegistration(publication, aeronClient, correlationId));
    }

    private void onRemovePublication(final long registrationId, final long correlationId)
    {
        final PublicationRegistration registration = publicationRegistrations.remove(registrationId);
        if (registration == null)
        {
            throw new ControlProtocolException(PUBLICATION_UNKNOWN, "unknown registration");
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
            channelEndpoint = new ReceiveChannelEndpoint(
                udpChannel, conductorProxy, logger, Configuration.createLossGenerator(dataLossRate, dataLossSeed));

            receiveChannelEndpointByChannelMap.put(udpChannel.canonicalForm(), channelEndpoint);

            while (!receiverProxy.registerMediaEndpoint(channelEndpoint))
            {
                systemCounters.receiverProxyFails().orderedIncrement();
                Thread.yield();
            }
        }

        final AeronClient aeronClient = getOrAddClient(clientId);
        final int refCount = channelEndpoint.incRefToStream(streamId);

        if (1 == refCount)
        {
            while (!receiverProxy.addSubscription(channelEndpoint, streamId))
            {
                systemCounters.receiverProxyFails().orderedIncrement();
                Thread.yield();
            }
        }

        final DriverSubscription subscription = new DriverSubscription(correlationId, channelEndpoint, aeronClient, streamId);
        subscriptions.add(subscription);

        clientProxy.operationSucceeded(correlationId);
    }

    private void onRemoveSubscription(final long registrationCorrelationId, final long correlationId)
    {
        final DriverSubscription subscription = removeSubscription(subscriptions, registrationCorrelationId);
        if (null == subscription)
        {
            throw new ControlProtocolException(SUBSCRIBER_NOT_REGISTERED, "subscription not registered");
        }

        final ReceiveChannelEndpoint channelEndpoint = subscription.receiveChannelEndpoint();

        final int refCount = channelEndpoint.decRefToStream(subscription);
        if (0 == refCount)
        {
            while (!receiverProxy.removeSubscription(channelEndpoint, subscription.streamId()))
            {
                systemCounters.receiverProxyFails().orderedIncrement();
                Thread.yield();
            }
        }

        if (channelEndpoint.streamCount() == 0)
        {
            receiveChannelEndpointByChannelMap.remove(channelEndpoint.udpTransport().udpChannel().canonicalForm());
            channelEndpoint.close();
        }

        clientProxy.operationSucceeded(correlationId);
    }

    private void onCreateConnection(final CreateConnectionCmd cmd)
    {
        final int sessionId = cmd.sessionId();
        final int streamId = cmd.streamId();
        final int initialTermId = cmd.termId();
        final InetSocketAddress controlAddress = cmd.controlAddress();
        final ReceiveChannelEndpoint channelEndpoint = cmd.channelEndpoint();
        final UdpChannel udpChannel = channelEndpoint.udpChannel();

        final String canonicalForm = udpChannel.canonicalForm();
        final TermBuffers termBuffers = termBuffersFactory.newConnection(canonicalForm, sessionId, streamId);

        final String channel = udpChannel.originalUriAsString();
        final int subscriberPositionCounterId = allocatePositionCounter("subscriber", channel, sessionId, streamId);
        final int receivedCompleteCounterId = allocatePositionCounter("receiver complete", channel, sessionId, streamId);
        final int receivedHwmCounterId = allocatePositionCounter("receiver hwm", channel, sessionId, streamId);

        clientProxy.onNewTermBuffers(
            ON_NEW_CONNECTION, channel, streamId, sessionId, initialTermId, termBuffers, 0, subscriberPositionCounterId);

        final GapScanner[] gapScanners =
            termBuffers.stream()
                       .map((rawLog) -> new GapScanner(rawLog.logBuffer(), rawLog.stateBuffer()))
                       .toArray(GapScanner[]::new);

        final LossHandler lossHandler = new LossHandler(
            gapScanners,
            timerWheel,
            udpChannel.isMulticast() ? NAK_MULTICAST_DELAY_GENERATOR : NAK_UNICAST_DELAY_GENERATOR,
            channelEndpoint.composeNakMessageSender(controlAddress, sessionId, streamId),
            initialTermId,
            systemCounters);

        final DriverConnection connection = new DriverConnection(
            channelEndpoint,
            sessionId,
            streamId,
            initialTermId,
            initialWindowSize,
            statusMessageTimeout,
            termBuffers,
            lossHandler,
            channelEndpoint.composeStatusMessageSender(controlAddress, sessionId, streamId),
            new BufferPositionIndicator(countersBuffer, subscriberPositionCounterId, countersManager),
            new BufferPositionReporter(countersBuffer, receivedCompleteCounterId, countersManager),
            new BufferPositionReporter(countersBuffer, receivedHwmCounterId, countersManager),
            timerWheel::now,
            systemCounters,
            logger);

        connections.add(connection);

        final NewConnectionCmd newConnectionCmd = new NewConnectionCmd(channelEndpoint, connection);
        while (!receiverProxy.newConnection(newConnectionCmd))
        {
            systemCounters.receiverProxyFails().orderedIncrement();
            Thread.yield();
        }
    }

    private void onClientKeepalive(final long clientId)
    {
        final AeronClient aeronClient = findClient(clients, clientId);

        if (null != aeronClient)
        {
            aeronClient.timeOfLastKeepalive(timerWheel.now());
        }
    }

    private void onCheckPublicationRegistrations()
    {
        final long now = timerWheel.now();

        // TODO: remove values() iteration
        final Iterator<PublicationRegistration> iter = publicationRegistrations.values().iterator();
        while (iter.hasNext())
        {
            final PublicationRegistration registration = iter.next();
            if (registration.checkKeepaliveTimeout(now))
            {
                iter.remove();
            }
        }

        rescheduleTimeout(CHECK_TIMEOUT_MS, TimeUnit.MILLISECONDS, publicationRegistrationCheckTimer);
    }

    private void onCheckPublications()
    {
        final long now = timerWheel.now();
        final ArrayList<DriverPublication> publications = this.publications;

        for (int i = publications.size() - 1; i >= 0; i--)
        {
            final DriverPublication publication = publications.get(i);

            if (publication.isUnreferencedAndFlushed(now) &&
                now > (publication.timeOfFlush() + Configuration.PUBLICATION_LINGER_NS))
            {
                final SendChannelEndpoint channelEndpoint = publication.sendChannelEndpoint();

                logger.log(
                    EventCode.REMOVE_PUBLICATION_CLEANUP,
                    "%s %x:%x",
                    channelEndpoint.udpChannel().originalUriAsString(),
                    publication.sessionId(),
                    publication.streamId());

                channelEndpoint.removePublication(publication.sessionId(), publication.streamId());
                publications.remove(i);

                final ClosePublicationCmd cmd = new ClosePublicationCmd(publication);
                while (!senderProxy.closePublication(cmd))
                {
                    systemCounters.senderProxyFails().orderedIncrement();
                    Thread.yield();
                }

                if (channelEndpoint.sessionCount() == 0)
                {
                    sendChannelEndpointByChannelMap.remove(channelEndpoint.udpChannel().canonicalForm());
                    channelEndpoint.close();
                }
            }
        }

        rescheduleTimeout(CHECK_TIMEOUT_MS, TimeUnit.MILLISECONDS, publicationCheckTimer);
    }

    private void onCheckSubscriptions()
    {
        final long now = timerWheel.now();
        final ArrayList<DriverSubscription> subscriptions = this.subscriptions;

        for (int i = subscriptions.size() - 1; i >= 0; i--)
        {
            final DriverSubscription subscription = subscriptions.get(i);

            if (subscription.timeOfLastKeepaliveFromClient() + Configuration.CLIENT_LIVENESS_TIMEOUT_NS < now)
            {
                final ReceiveChannelEndpoint channelEndpoint = subscription.receiveChannelEndpoint();
                final int streamId = subscription.streamId();

                logger.log(
                    EventCode.REMOVE_SUBSCRIPTION_CLEANUP,
                    "%s %x [%x]",
                    channelEndpoint.udpChannel().originalUriAsString(),
                    subscription.streamId(),
                    subscription.id());

                subscriptions.remove(i);

                if (0 == channelEndpoint.decRefToStream(subscription))
                {
                    while (!receiverProxy.removeSubscription(channelEndpoint, streamId))
                    {
                        systemCounters.receiverProxyFails().orderedIncrement();
                        Thread.yield();
                    }
                }

                if (channelEndpoint.streamCount() == 0)
                {
                    receiveChannelEndpointByChannelMap.remove(channelEndpoint.udpChannel().canonicalForm());
                    channelEndpoint.close();
                }
            }
        }

        rescheduleTimeout(CHECK_TIMEOUT_MS, TimeUnit.MILLISECONDS, subscriptionCheckTimer);
    }

    private void onCheckConnections()
    {
        final long now = timerWheel.now();
        final ArrayList<DriverConnection> connections = this.connections;

        for (int i = connections.size() - 1; i >= 0; i--)
        {
            final DriverConnection connection = connections.get(i);

            switch (connection.status())
            {
                case ACTIVE:
                    if (connection.timeOfLastFrame() + Configuration.CONNECTION_LIVENESS_TIMEOUT_NS < now)
                    {
                        while (!receiverProxy.removeConnection(connection))
                        {
                            systemCounters.receiverProxyFails().orderedIncrement();
                            Thread.yield();
                        }

                        connection.status(DriverConnection.Status.INACTIVE);
                        connection.timeOfLastStatusChange(now);

                        if (connection.remaining() == 0)
                        {
                            connection.status(DriverConnection.Status.LINGER);
                            connection.timeOfLastStatusChange(now);

                            clientProxy.onInactiveConnection(
                                connection.sessionId(),
                                connection.streamId(),
                                connection.receiveChannelEndpoint().udpChannel().originalUriAsString());
                        }
                    }
                    break;

                case INACTIVE:
                    if (connection.remaining() == 0 ||
                        connection.timeOfLastStatusChange() + Configuration.CONNECTION_LIVENESS_TIMEOUT_NS < now)
                    {
                        connection.status(DriverConnection.Status.LINGER);
                        connection.timeOfLastStatusChange(now);

                        clientProxy.onInactiveConnection(
                            connection.sessionId(),
                            connection.streamId(),
                            connection.receiveChannelEndpoint().udpChannel().originalUriAsString());
                    }
                    break;

                case LINGER:
                    if (connection.timeOfLastStatusChange() + Configuration.CONNECTION_LIVENESS_TIMEOUT_NS < now)
                    {
                        logger.log(
                            EventCode.REMOVE_CONNECTION_CLEANUP,
                            "%s %x:%x",
                            connection.receiveChannelEndpoint().udpChannel().originalUriAsString(),
                            connection.sessionId(),
                            connection.streamId());

                        connections.remove(i);
                        connection.close();
                    }
                    break;
            }
        }

        rescheduleTimeout(CHECK_TIMEOUT_MS, TimeUnit.MILLISECONDS, connectionCheckTimer);
    }

    private void onCheckClients()
    {
        final long now = timerWheel.now();

        for (int i = clients.size() - 1; i >= 0; i--)
        {
            final AeronClient aeronClient = clients.get(i);

            if (aeronClient.timeOfLastKeepalive() + Configuration.CONNECTION_LIVENESS_TIMEOUT_NS < now)
            {
                clients.remove(i);
            }
        }

        rescheduleTimeout(CHECK_TIMEOUT_MS, TimeUnit.MILLISECONDS, clientCheckTimer);
    }

    private void onCheckPendingSetups()
    {
        final long now = timerWheel.now();

        for (int i = pendingSetups.size() - 1; i >= 0; i--)
        {
            final ElicitSetupFromSourceCmd cmd = pendingSetups.get(i);

            if (cmd.timeOfStatusMessage() + Configuration.PENDING_SETUPS_TIMEOUT_NS < now)
            {
                pendingSetups.remove(i);

                final RemovePendingSetupCmd removeCmd = new RemovePendingSetupCmd(
                    cmd.channelEndpoint(), cmd.sessionId(), cmd.streamId());

                while (!receiverProxy.removePendingSetup(removeCmd))
                {
                    systemCounters.receiverProxyFails().orderedIncrement();
                    Thread.yield();
                }
            }
        }

        rescheduleTimeout(CHECK_TIMEOUT_MS, TimeUnit.MILLISECONDS, pendingSetupsTimer);
    }

    private void onElicitSetupFromSender(final ElicitSetupFromSourceCmd cmd)
    {
        final int sessionId = cmd.sessionId();
        final int streamId = cmd.streamId();
        final InetSocketAddress controlAddress = cmd.controlAddress();
        final ReceiveChannelEndpoint channelEndpoint = cmd.channelEndpoint();

        channelEndpoint.sendSetupElicitingStatusMessage(controlAddress, sessionId, streamId);
        cmd.timeOfStatusMessage(timerWheel.now());

        pendingSetups.add(cmd);
    }

    private TimerWheel.Timer newTimeout(final long delay, final TimeUnit timeUnit, final Runnable task)
    {
        return timerWheel.newTimeout(delay, timeUnit, task);
    }

    private void rescheduleTimeout(final long delay, final TimeUnit timeUnit, final TimerWheel.Timer timer)
    {
        timerWheel.rescheduleTimeout(delay, timeUnit, timer);
    }

    private AeronClient getOrAddClient(final long clientId)
    {
        AeronClient aeronClient = findClient(clients, clientId);

        if (null == aeronClient)
        {
            aeronClient = new AeronClient(clientId, timerWheel.now());
            clients.add(aeronClient);
        }

        return aeronClient;
    }

    private static AeronClient findClient(final ArrayList<AeronClient> clients, final long clientId)
    {
        for (int i = 0, size = clients.size(); i < size; i++)
        {
            final AeronClient aeronClient = clients.get(i);
            if (aeronClient.clientId() == clientId)
            {
                return aeronClient;
            }
        }

        return null;
    }

    private int allocatePositionCounter(final String type, final String dirName, final int sessionId, final int streamId)
    {
        return countersManager.allocate(String.format("%s position: %s %x %x", type, dirName, sessionId, streamId));
    }

    private DriverSubscription removeSubscription(
        final ArrayList<DriverSubscription> subscriptions, final long registrationCorrelationId)
    {
        for (int i = 0, size = subscriptions.size(); i < size; i++)
        {
            final DriverSubscription subscription = subscriptions.get(i);

            if (subscription.id() == registrationCorrelationId)
            {
                subscriptions.remove(i);
                return subscription;
            }
        }

        return null;
    }

    private RetransmitSender composeNewRetransmitSender(final DriverPublication publication)
    {
        return (termId, termOffset, length) ->
        {
            final RetransmitPublicationCmd cmd = new RetransmitPublicationCmd(publication, termId, termOffset, length);

            while (!senderProxy.retransmit(cmd))
            {
                systemCounters.senderProxyFails().orderedIncrement();
                Thread.yield();
            }
        };
    }
}
