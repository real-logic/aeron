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
import uk.co.real_logic.aeron.common.command.SubscriptionMessageFlyweight;
import uk.co.real_logic.aeron.common.concurrent.*;
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
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

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
    public static final int HEADER_LENGTH = DataHeaderFlyweight.HEADER_LENGTH;
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
    private final Long2ObjectHashMap<ClientLiveness> clientLivenessByClientId = new Long2ObjectHashMap<>();
    private final HashMap<String, SendChannelEndpoint> sendChannelEndpointByChannelMap = new HashMap<>();
    private final HashMap<String, ReceiveChannelEndpoint> receiveChannelEndpointByChannelMap = new HashMap<>();
    private final Long2ObjectHashMap<DriverSubscription> subscriptionByCorrelationIdMap = new Long2ObjectHashMap<>();
    private final TimerWheel timerWheel;
    private final AtomicArray<DriverConnection> connections = new AtomicArray<>();
    private final AtomicArray<ClientLiveness> clients = new AtomicArray<>();
    private final AtomicArray<DriverSubscription> subscriptions;
    private final AtomicArray<DriverPublication> publications;
    private final AtomicArray<ElicitSetupFromSourceCmd> pendingSetups;

    private final Supplier<SenderFlowControl> unicastSenderFlowControl;
    private final Supplier<SenderFlowControl> multicastSenderFlowControl;

    private final PublicationMessageFlyweight publicationMessage = new PublicationMessageFlyweight();
    private final SubscriptionMessageFlyweight subscriptionMessage = new SubscriptionMessageFlyweight();
    private final CorrelatedMessageFlyweight correlatedMessage = new CorrelatedMessageFlyweight();

    private final int mtuLength;
    private final int capacity;
    private final int initialWindowSize;
    private final long statusMessageTimeout;
    private final long dataLossSeed;
    private final long controlLossSeed;
    private final double dataLossRate;
    private final double controlLossRate;
    private final TimerWheel.Timer publicationCheckTimer;
    private final TimerWheel.Timer subscriptionCheckTimer;
    private final TimerWheel.Timer connectionCheckTimer;
    private final TimerWheel.Timer clientLivenessCheckTimer;
    private final TimerWheel.Timer pendingSetupsTimer;
    private final CountersManager countersManager;
    private final AtomicBuffer countersBuffer;
    private final EventLogger logger;
    private final SystemCounters systemCounters;

    private final Consumer<Object> onReceiverCommandFunc;
    private final MessageHandler onClientCommandFunc;
    private final ToIntFunction<DriverConnection> sendPendingStatusMessagesFunc;

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
        publicationCheckTimer = newTimeout(CHECK_TIMEOUT_MS, TimeUnit.MILLISECONDS, this::onCheckPublications);
        subscriptionCheckTimer = newTimeout(CHECK_TIMEOUT_MS, TimeUnit.MILLISECONDS, this::onCheckSubscriptions);
        connectionCheckTimer = newTimeout(CHECK_TIMEOUT_MS, TimeUnit.MILLISECONDS, this::onCheckConnections);
        clientLivenessCheckTimer = newTimeout(CHECK_TIMEOUT_MS, TimeUnit.MILLISECONDS, this::onLivenessCheckClients);
        pendingSetupsTimer = newTimeout(CHECK_TIMEOUT_MS, TimeUnit.MILLISECONDS, this::onPendingSetupsCheck);

        publications = ctx.publications();
        subscriptions = ctx.subscriptions();
        pendingSetups = new AtomicArray<>();
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
        sendPendingStatusMessagesFunc = (connection) -> connection.sendPendingStatusMessages(timerWheel.now());
    }

    public void onReceiverCommand(Object obj)
    {
        if (obj instanceof CreateConnectionCmd)
        {
            onCreateConnection((CreateConnectionCmd)obj);
        }
        else if (obj instanceof ElicitSetupFromSourceCmd)
        {
            onElicitSetupFromSource((ElicitSetupFromSourceCmd)obj);
        }
    }

    public void onClientCommand(final int msgTypeId, final AtomicBuffer buffer, final int index, final int length)
    {
        Flyweight flyweight = null;
        final PublicationMessageFlyweight publicationMessageFlyweight = publicationMessage;
        final SubscriptionMessageFlyweight subscriptionMessageFlyweight = subscriptionMessage;
        final CorrelatedMessageFlyweight correlatedMessageFlyweight = correlatedMessage;

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
                    publicationMessageFlyweight.wrap(buffer, index);
                    logger.log(EventCode.CMD_IN_REMOVE_PUBLICATION, buffer, index, length);
                    flyweight = publicationMessageFlyweight;
                    onRemovePublication(
                        publicationMessageFlyweight.channel(),
                        publicationMessageFlyweight.sessionId(),
                        publicationMessageFlyweight.streamId());
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
                        subscriptionMessageFlyweight.channel(),
                        subscriptionMessageFlyweight.streamId(),
                        subscriptionMessageFlyweight.registrationCorrelationId());
                    break;

                case KEEPALIVE_CLIENT:
                    correlatedMessageFlyweight.wrap(buffer, index);
                    logger.log(EventCode.CMD_IN_KEEPALIVE_CLIENT, buffer, index, length);
                    flyweight = correlatedMessageFlyweight;
                    onKeepaliveClient(correlatedMessageFlyweight.clientId());
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
        int workCount = nioSelector.processKeys();

        workCount += processFromClientCommandBuffer();
        workCount += processFromReceiverCommandQueue();
        workCount += processTimers();

        workCount += connections.doAction(sendPendingStatusMessagesFunc);
        workCount += connections.doAction(DriverConnection::scanForGaps);
        workCount += connections.doAction(DriverConnection::cleanLogBuffer);

        workCount += publications.doAction(DriverPublication::cleanLogBuffer);

        return workCount;
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

    /**
     * Return the {@link NioSelector} in use by this conductor thread.
     *
     * @return the {@link NioSelector} in use by this conductor thread
     */
    public NioSelector nioSelector()
    {
        return nioSelector;
    }

    private int processFromReceiverCommandQueue()
    {
        return commandQueue.drain(onReceiverCommandFunc);
    }

    private int processFromClientCommandBuffer()
    {
        return fromClientCommands.read(onClientCommandFunc);
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

    private TimerWheel.Timer newTimeout(final long delay, final TimeUnit timeUnit, final Runnable task)
    {
        return timerWheel.newTimeout(delay, timeUnit, task);
    }

    private void rescheduleTimeout(final long delay, final TimeUnit timeUnit, final TimerWheel.Timer timer)
    {
        timerWheel.rescheduleTimeout(delay, timeUnit, timer);
    }

    private ClientLiveness getOrAddClient(final long clientId)
    {
        ClientLiveness clientLiveness = clientLivenessByClientId.get(clientId);

        if (null == clientLiveness)
        {
            clientLiveness = new ClientLiveness(clientId, timerWheel.now());
            clientLivenessByClientId.put(clientId, clientLiveness);
            clients.add(clientLiveness);
        }

        return clientLiveness;
    }

    private void onAddPublication(
        final String channel, final int sessionId,  final int streamId, final long correlationId, final long clientId)
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

        if (null != channelEndpoint.getPublication(sessionId, streamId))
        {
            throw new ControlProtocolException(
                ErrorCode.PUBLICATION_STREAM_ALREADY_EXISTS, "publication and session already exist on channel");
        }

        final ClientLiveness clientLiveness = getOrAddClient(clientId);
        final int initialTermId = BitUtil.generateRandomisedId();
        final String canonicalForm = udpChannel.canonicalForm();
        final TermBuffers termBuffers = termBuffersFactory.newPublication(canonicalForm, sessionId, streamId);

        final int positionCounterId = allocatePositionCounter("publisher limit", channel, sessionId, streamId);
        final PositionReporter positionReporter = new BufferPositionReporter(countersBuffer, positionCounterId, countersManager);

        final SenderFlowControl senderFlowControl =
            udpChannel.isMulticast() ? multicastSenderFlowControl.get() : unicastSenderFlowControl.get();

        final DriverPublication publication = new DriverPublication(
            channelEndpoint,
            timerWheel,
            termBuffers,
            positionReporter,
            clientLiveness,
            sessionId,
            streamId,
            initialTermId,
            HEADER_LENGTH,
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

        clientProxy.onNewTermBuffers(
            ON_NEW_PUBLICATION, channel, streamId, sessionId, initialTermId, termBuffers, correlationId, positionCounterId);

        publications.add(publication);
    }

    private void onRemovePublication(final String channel, final int sessionId, final int streamId)
    {
        final UdpChannel udpChannel = UdpChannel.parse(channel);
        final SendChannelEndpoint channelEndpoint = sendChannelEndpointByChannelMap.get(udpChannel.canonicalForm());
        if (null == channelEndpoint)
        {
            throw new ControlProtocolException(INVALID_CHANNEL, "channel unknown");
        }

        final DriverPublication publication = channelEndpoint.getPublication(sessionId, streamId);
        if (null == publication)
        {
            throw new ControlProtocolException(PUBLICATION_STREAM_UNKNOWN, "session and publication unknown for channel");
        }

        publication.status(DriverPublication.Status.EOF);

        if (publication.isFlushed() || publication.statusMessagesSeenCount() == 0)
        {
            publication.status(DriverPublication.Status.FLUSHED);
            publication.timeOfFlush(timerWheel.now());
        }

        clientProxy.operationSucceeded(publicationMessage.correlationId());
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

        final ClientLiveness clientLiveness = getOrAddClient(clientId);
        final int refCount = channelEndpoint.incRefToStream(streamId);

        if (1 == refCount)
        {
            while (!receiverProxy.addSubscription(channelEndpoint, streamId))
            {
                systemCounters.receiverProxyFails().orderedIncrement();
                Thread.yield();
            }
        }

        final DriverSubscription subscription = new DriverSubscription(channelEndpoint, clientLiveness, streamId, correlationId);
        subscriptionByCorrelationIdMap.put(correlationId, subscription);
        subscriptions.add(subscription);

        clientProxy.operationSucceeded(correlationId);
    }

    private void onRemoveSubscription(final String channel, final int streamId, final long registrationCorrelationId)
    {
        final UdpChannel udpChannel = UdpChannel.parse(channel);
        final ReceiveChannelEndpoint channelEndpoint = receiveChannelEndpointByChannelMap.get(udpChannel.canonicalForm());
        if (null == channelEndpoint)
        {
            throw new ControlProtocolException(INVALID_CHANNEL, "channel unknown");
        }

        if (channelEndpoint.getRefCountToStream(streamId) == 0)
        {
            throw new ControlProtocolException(SUBSCRIBER_NOT_REGISTERED, "subscriptions unknown for stream");
        }

        final DriverSubscription subscription = subscriptionByCorrelationIdMap.remove(registrationCorrelationId);
        if (null == subscription)
        {
            throw new ControlProtocolException(SUBSCRIBER_NOT_REGISTERED, "subscription not registered");
        }
        subscriptions.remove(subscription);

        final int refCount = channelEndpoint.decRefToStream(streamId);
        if (0 == refCount)
        {
            while (!receiverProxy.removeSubscription(channelEndpoint, streamId))
            {
                systemCounters.receiverProxyFails().orderedIncrement();
                Thread.yield();
            }
        }

        if (channelEndpoint.streamCount() == 0)
        {
            receiveChannelEndpointByChannelMap.remove(udpChannel.canonicalForm());
            channelEndpoint.close();
        }

        clientProxy.operationSucceeded(subscriptionMessage.correlationId());
    }

    private void onKeepaliveClient(final long clientId)
    {
        final ClientLiveness clientLiveness = clientLivenessByClientId.get(clientId);

        if (null != clientLiveness)
        {
            clientLiveness.timeOfLastKeepalive(timerWheel.now());
        }
    }

    private void onCheckPublications()
    {
        final long now = timerWheel.now();

        publications.forEach(
            (publication) ->
            {
                switch (publication.status())
                {
                    case ACTIVE:
                        if (publication.timeOfLastKeepaliveFromClient() + Configuration.CLIENT_LIVENESS_TIMEOUT_NS < now)
                        {
                            publication.status(DriverPublication.Status.EOF);
                            if (publication.isFlushed() || publication.statusMessagesSeenCount() == 0)
                            {
                                publication.status(DriverPublication.Status.FLUSHED);
                                publication.timeOfFlush(now);
                            }
                        }
                        break;

                    case EOF:
                        if (publication.isFlushed() || publication.statusMessagesSeenCount() == 0)
                        {
                            publication.status(DriverPublication.Status.FLUSHED);
                            publication.timeOfFlush(now);
                        }
                        break;

                    case FLUSHED:
                        if (publication.timeOfFlush() + Configuration.PUBLICATION_LINGER_NS < now)
                        {
                            final SendChannelEndpoint channelEndpoint = publication.sendChannelEndpoint();

                            logger.log(
                                EventCode.REMOVE_PUBLICATION_CLEANUP,
                                "%s %x:%x",
                                channelEndpoint.udpChannel().originalUriAsString(),
                                publication.sessionId(),
                                publication.streamId());

                            channelEndpoint.removePublication(publication.sessionId(), publication.streamId());
                            publications.remove(publication);

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
                        break;
                }
            }
        );

        rescheduleTimeout(CHECK_TIMEOUT_MS, TimeUnit.MILLISECONDS, publicationCheckTimer);
    }

    // check of Subscriptions
    // remove from dispatcher, but leave connections to timeout independently
    private void onCheckSubscriptions()
    {
        final long now = timerWheel.now();

        // the array should be removed from safely
        subscriptions.forEach(
            (subscription) ->
            {
                if (subscription.timeOfLastKeepaliveFromClient() + Configuration.CLIENT_LIVENESS_TIMEOUT_NS < now)
                {
                    final ReceiveChannelEndpoint channelEndpoint = subscription.receiveChannelEndpoint();
                    final int streamId = subscription.streamId();

                    logger.log(
                        EventCode.REMOVE_SUBSCRIPTION_CLEANUP,
                        "%s %x [%x]",
                        channelEndpoint.udpChannel().originalUriAsString(),
                        subscription.streamId(),
                        subscription.correlationId());

                    subscriptions.remove(subscription);
                    subscriptionByCorrelationIdMap.remove(subscription.correlationId());

                    if (0 == channelEndpoint.decRefToStream(streamId))
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
        );

        rescheduleTimeout(CHECK_TIMEOUT_MS, TimeUnit.MILLISECONDS, subscriptionCheckTimer);
    }

    private void onCheckConnections()
    {
        final long now = timerWheel.now();

        connections.forEach(
            (connection) ->
            {
                /*
                 *  Stage 1: initial timeout of publisher (after timeout): ACTIVE -> INACTIVE
                 *  Stage 2: check for remaining == 0 OR timeout: INACTIVE -> LINGER (ON_INACTIVE_CONNECTION sent)
                 *  Stage 3: timeout (cleanup)
                 */

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

                            connections.remove(connection);
                            connection.close();
                        }
                        break;
                }
            }
        );

        rescheduleTimeout(CHECK_TIMEOUT_MS, TimeUnit.MILLISECONDS, connectionCheckTimer);
    }

    private void onLivenessCheckClients()
    {
        final long now = timerWheel.now();

        clients.forEach(
            (clientLiveness) ->
            {
                if (clientLiveness.timeOfLastKeepalive() + Configuration.CONNECTION_LIVENESS_TIMEOUT_NS < now)
                {
                    clientLivenessByClientId.remove(clientLiveness.clientId());
                    clients.remove(clientLiveness);
                }
            }
        );

        rescheduleTimeout(CHECK_TIMEOUT_MS, TimeUnit.MILLISECONDS, clientLivenessCheckTimer);
    }

    private void onPendingSetupsCheck()
    {
        final long now = timerWheel.now();

        pendingSetups.forEach(
            (cmd) ->
            {
                if (cmd.timeOfStatusMessage() +  Configuration.PENDING_SETUPS_TIMEOUT_NS < now)
                {
                    pendingSetups.remove(cmd);

                    final RemovePendingSetupCmd removeCmd =
                        new RemovePendingSetupCmd(cmd.channelEndpoint(), cmd.sessionId(), cmd.streamId());

                    while (!receiverProxy.removePendingSetup(removeCmd))
                    {
                        systemCounters.receiverProxyFails().orderedIncrement();
                        Thread.yield();
                    }
                }
            }
        );

        rescheduleTimeout(CHECK_TIMEOUT_MS, TimeUnit.MILLISECONDS, pendingSetupsTimer);
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
        final int receivedCompleteCounterId = allocatePositionCounter("received complete", channel, sessionId, streamId);
        final int receivedHwmCounterId = allocatePositionCounter("received hwm", channel, sessionId, streamId);

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

    private void onElicitSetupFromSource(final ElicitSetupFromSourceCmd cmd)
    {
        final int sessionId = cmd.sessionId();
        final int streamId = cmd.streamId();
        final InetSocketAddress controlAddress = cmd.controlAddress();
        final ReceiveChannelEndpoint channelEndpoint = cmd.channelEndpoint();

        channelEndpoint.sendSetupElicitingSm(controlAddress, sessionId, streamId);
        cmd.timeOfStatusMessage(timerWheel.now());

        pendingSetups.add(cmd);
    }

    private int allocatePositionCounter(final String type, final String dirName, final int sessionId, final int streamId)
    {
        return countersManager.allocate(String.format("%s position: %s %x %x", type, dirName, sessionId, streamId));
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
