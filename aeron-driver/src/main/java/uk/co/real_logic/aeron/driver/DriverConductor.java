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
import uk.co.real_logic.aeron.common.command.PublicationMessageFlyweight;
import uk.co.real_logic.aeron.common.command.SubscriptionMessageFlyweight;
import uk.co.real_logic.aeron.common.concurrent.AtomicArray;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.CountersManager;
import uk.co.real_logic.aeron.common.concurrent.OneToOneConcurrentArrayQueue;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.GapScanner;
import uk.co.real_logic.aeron.common.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.common.event.EventCode;
import uk.co.real_logic.aeron.common.event.EventLogger;
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.common.status.BufferPositionIndicator;
import uk.co.real_logic.aeron.common.status.BufferPositionReporter;
import uk.co.real_logic.aeron.driver.buffer.TermBuffers;
import uk.co.real_logic.aeron.driver.buffer.TermBuffersFactory;
import uk.co.real_logic.aeron.driver.cmd.CreateConnectionCmd;
import uk.co.real_logic.aeron.driver.cmd.NewConnectionCmd;
import uk.co.real_logic.aeron.driver.cmd.SubscriptionRemovedCmd;
import uk.co.real_logic.aeron.driver.exceptions.ControlProtocolException;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static uk.co.real_logic.aeron.common.ErrorCode.*;
import static uk.co.real_logic.aeron.common.command.ControlProtocolEvents.*;
import static uk.co.real_logic.aeron.driver.MediaDriver.*;

/**
 * Driver Conductor to take commands from publishers and subscribers as well as handle NAKs and retransmissions
 */
public class DriverConductor extends Agent
{
    public static final int HEADER_LENGTH = DataHeaderFlyweight.HEADER_LENGTH;
    public static final int HEARTBEAT_TIMEOUT_MS = 100;
    public static final int LIVENESS_CHECK_TIMEOUT_MS = 1000;  // how often to check liveness

    // how long without keepalive/heartbeat before remove
    public static final long LIVENESS_CLIENT_TIMEOUT_NS = TimeUnit.MILLISECONDS.toNanos(2000);
    // how long without frames before removing
    public static final long LIVENESS_FRAME_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(10);

    /**
     * Unicast NAK delay is immediate initial with delayed subsequent delay
     */
    public static final StaticDelayGenerator NAK_UNICAST_DELAY_GENERATOR =
        new StaticDelayGenerator(TimeUnit.MILLISECONDS.toNanos(NAK_UNICAST_DELAY_DEFAULT_NS), true);

    public static final OptimalMulticastDelayGenerator NAK_MULTICAST_DELAY_GENERATOR =
        new OptimalMulticastDelayGenerator(MediaDriver.NAK_MAX_BACKOFF_DEFAULT,
                                           MediaDriver.NAK_GROUPSIZE_DEFAULT,
                                           MediaDriver.NAK_GRTT_DEFAULT);

    /**
     * Source uses same for unicast and multicast. For now.
     */
    public static final FeedbackDelayGenerator RETRANS_UNICAST_DELAY_GENERATOR = () -> RETRANS_UNICAST_DELAY_DEFAULT_NS;
    public static final FeedbackDelayGenerator RETRANS_UNICAST_LINGER_GENERATOR = () -> RETRANS_UNICAST_LINGER_DEFAULT_NS;

    private final OneToOneConcurrentArrayQueue<? super Object> commandQueue;
    private final ReceiverProxy receiverProxy;
    private final ClientProxy clientProxy;
    private final DriverConductorProxy conductorProxy;
    private final NioSelector nioSelector;
    private final TermBuffersFactory termBuffersFactory;
    private final RingBuffer fromClientCommands;
    private final Long2ObjectHashMap<SendChannelEndpoint> sendChannelEndpointByHash = new Long2ObjectHashMap<>();
    private final Long2ObjectHashMap<ReceiveChannelEndpoint> receiveChannelEndpointByHash = new Long2ObjectHashMap<>();
    private final Long2ObjectHashMap<DriverSubscription> subscriptionByCorrelationId = new Long2ObjectHashMap<>();
    private final TimerWheel timerWheel;
    private final AtomicArray<DriverConnection> connections = new AtomicArray<>();
    private final AtomicArray<DriverSubscription> subscriptions;
    private final AtomicArray<DriverPublication> publications;

    private final Supplier<SenderControlStrategy> unicastSenderFlowControl;
    private final Supplier<SenderControlStrategy> multicastSenderFlowControl;

    private final PublicationMessageFlyweight publicationMessage = new PublicationMessageFlyweight();
    private final SubscriptionMessageFlyweight subscriptionMessage = new SubscriptionMessageFlyweight();

    private final int mtuLength;
    private final int initialWindowSize;
    private final long statusMessageTimeout;
    private final TimerWheel.Timer heartbeatTimer;
    private final TimerWheel.Timer publicationLivenessCheckTimer;
    private final TimerWheel.Timer subscriptionLivenessCheckTimer;
    private final TimerWheel.Timer connectionLivenessCheckTimer;
    private final CountersManager countersManager;
    private final AtomicBuffer countersBuffer;

    private final EventLogger logger;

    public DriverConductor(final Context ctx)
    {
        super(ctx.conductorIdleStrategy(), ctx.eventLoggerException());

        this.commandQueue = ctx.conductorCommandQueue();
        this.receiverProxy = ctx.receiverProxy();
        this.termBuffersFactory = ctx.termBuffersFactory();
        this.nioSelector = ctx.conductorNioSelector();
        this.mtuLength = ctx.mtuLength();
        this.initialWindowSize = ctx.initialWindowSize();
        this.statusMessageTimeout = ctx.statusMessageTimeout();
        this.unicastSenderFlowControl = ctx.unicastSenderFlowControl();
        this.multicastSenderFlowControl = ctx.multicastSenderFlowControl();
        this.countersManager = ctx.countersManager();
        this.countersBuffer = ctx.countersBuffer();

        timerWheel = ctx.conductorTimerWheel();
        heartbeatTimer = newTimeout(HEARTBEAT_TIMEOUT_MS, TimeUnit.MILLISECONDS, this::onHeartbeatCheck);
        publicationLivenessCheckTimer =
            newTimeout(LIVENESS_CHECK_TIMEOUT_MS, TimeUnit.MILLISECONDS, this::onLivenessCheckPublications);
        subscriptionLivenessCheckTimer =
            newTimeout(LIVENESS_CHECK_TIMEOUT_MS, TimeUnit.MILLISECONDS, this::onLivenessCheckSubscriptions);
        connectionLivenessCheckTimer =
            newTimeout(LIVENESS_CHECK_TIMEOUT_MS, TimeUnit.MILLISECONDS, this::onLivenessCheckConnections);

        publications = ctx.publications();
        subscriptions = ctx.subscriptions();
        fromClientCommands = ctx.fromClientCommands();
        clientProxy = ctx.clientProxy();
        conductorProxy = ctx.driverConductorProxy();
        logger = ctx.eventLogger();
    }

    public SendChannelEndpoint senderChannelEndpoint(final UdpChannel channel)
    {
        return sendChannelEndpointByHash.get(channel.consistentHash());
    }

    public ReceiveChannelEndpoint receiverChannelEndpoint(final UdpChannel channel)
    {
        return receiveChannelEndpointByHash.get(channel.consistentHash());
    }

    public int doWork() throws Exception
    {
        int workCount = nioSelector.processKeys();

        workCount += publications.doAction(DriverPublication::cleanLogBuffer);

        final long now = timerWheel.now();
        for (final DriverConnection connection : connections)
        {
            workCount += connection.cleanLogBuffer();
            workCount += connection.scanForGaps();
            workCount += connection.sendPendingStatusMessages(now);
        }

        workCount += processFromClientCommandBuffer();
        workCount += processFromReceiverCommandQueue();
        workCount += processTimers();

        return workCount;
    }

    public void close()
    {
        stop();

        termBuffersFactory.close();
        publications.forEach(DriverPublication::close);
        connections.forEach(DriverConnection::close);
        sendChannelEndpointByHash.forEach((hash, endpoint) -> endpoint.close());
        receiveChannelEndpointByHash.forEach((hash, endpoint) -> endpoint.close());
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
        return commandQueue.drain(
            (obj) ->
            {
                try
                {
                    if (obj instanceof CreateConnectionCmd)
                    {
                        onCreateConnection((CreateConnectionCmd)obj);
                    }
                    else if (obj instanceof SubscriptionRemovedCmd)
                    {
                        onRemovedSubscription((SubscriptionRemovedCmd)obj);
                    }
                }
                catch (final Exception ex)
                {
                    logger.logException(ex);
                }
            });
    }

    private int processFromClientCommandBuffer()
    {
        return fromClientCommands.read(
            (msgTypeId, buffer, index, length) ->
            {
                Flyweight flyweight = publicationMessage;

                try
                {
                    switch (msgTypeId)
                    {
                        case ADD_PUBLICATION:
                            publicationMessage.wrap(buffer, index);
                            logger.log(EventCode.CMD_IN_ADD_PUBLICATION, buffer, index, length);
                            flyweight = publicationMessage;
                            onAddPublication(publicationMessage);
                            break;

                        case REMOVE_PUBLICATION:
                            publicationMessage.wrap(buffer, index);
                            logger.log(EventCode.CMD_IN_REMOVE_PUBLICATION, buffer, index, length);
                            flyweight = publicationMessage;
                            onRemovePublication(publicationMessage);
                            break;

                        case ADD_SUBSCRIPTION:
                            subscriptionMessage.wrap(buffer, index);
                            logger.log(EventCode.CMD_IN_ADD_SUBSCRIPTION, buffer, index, length);
                            flyweight = subscriptionMessage;
                            onAddSubscription(subscriptionMessage);
                            break;

                        case REMOVE_SUBSCRIPTION:
                            subscriptionMessage.wrap(buffer, index);
                            logger.log(EventCode.CMD_IN_REMOVE_SUBSCRIPTION, buffer, index, length);
                            flyweight = subscriptionMessage;
                            onRemoveSubscription(subscriptionMessage);
                            break;

                        case KEEPALIVE_PUBLICATION:
                            publicationMessage.wrap(buffer, index);
                            logger.log(EventCode.CMD_IN_KEEPALIVE_PUBLICATION, buffer, index, length);
                            flyweight = publicationMessage;
                            onKeepalivePublication(publicationMessage);
                            break;

                        case KEEPALIVE_SUBSCRIPTION:
                            subscriptionMessage.wrap(buffer, index);
                            logger.log(EventCode.CMD_IN_KEEPALIVE_SUBSCRIPTION, buffer, index, length);
                            flyweight = subscriptionMessage;
                            onKeepaliveSubscription(subscriptionMessage);
                            break;
                    }
                }
                catch (final ControlProtocolException ex)
                {
                    clientProxy.onError(ex.errorCode(), ex.getMessage(), flyweight, length);
                    logger.logException(ex);
                }
                catch (final Exception ex)
                {
                    clientProxy.onError(GENERIC_ERROR, ex.getMessage(), flyweight, length);
                    logger.logException(ex);
                }
            });
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

    private void onAddPublication(final PublicationMessageFlyweight publicationMessage)
    {
        final String channel = publicationMessage.channel();
        final int sessionId = publicationMessage.sessionId();
        final int streamId = publicationMessage.streamId();
        final long correlationId = publicationMessage.correlationId();

        try
        {
            final UdpChannel udpChannel = UdpChannel.parse(channel);
            SendChannelEndpoint channelEndpoint = sendChannelEndpointByHash.get(udpChannel.consistentHash());
            if (null == channelEndpoint)
            {
                channelEndpoint = new SendChannelEndpoint(udpChannel, nioSelector, logger);
                sendChannelEndpointByHash.put(udpChannel.consistentHash(), channelEndpoint);
            }
            else if (!channelEndpoint.udpChannel().equals(udpChannel))
            {
                throw new ControlProtocolException(ErrorCode.PUBLICATION_STREAM_ALREADY_EXISTS,
                                                   "channels hash same, but channels actually different");
            }

            if (null != channelEndpoint.findPublication(sessionId, streamId))
            {
                throw new ControlProtocolException(ErrorCode.PUBLICATION_STREAM_ALREADY_EXISTS,
                                                   "publication and session already exist on channel");
            }

            final int initialTermId = BitUtil.generateRandomizedId();
            final String canonicalForm = udpChannel.canonicalForm();
            final TermBuffers termBuffers = termBuffersFactory.newPublication(canonicalForm, sessionId, streamId);
            final SenderControlStrategy flowControlStrategy =
                udpChannel.isMulticast() ? multicastSenderFlowControl.get() : unicastSenderFlowControl.get();

            final int positionCounterId = allocatePositionCounter("publication", channel, sessionId, streamId);
            final BufferPositionReporter positionReporter =
                new BufferPositionReporter(countersBuffer, positionCounterId, countersManager);

            final DriverPublication publication =
                new DriverPublication(channelEndpoint,
                                      timerWheel,
                                      flowControlStrategy,
                                      termBuffers,
                                      positionReporter,
                                      sessionId,
                                      streamId,
                                      initialTermId,
                                      HEADER_LENGTH,
                                      mtuLength,
                                      logger);

            channelEndpoint.addPublication(publication);

            clientProxy.onNewTermBuffers(ON_NEW_PUBLICATION, sessionId, streamId, initialTermId, channel,
                                         termBuffers, correlationId, positionCounterId);

            publications.add(publication);
        }
        catch (final ControlProtocolException ex)
        {
            throw ex;
        }
        catch (final Exception ex)
        {
            throw new ControlProtocolException(GENERIC_ERROR_MESSAGE, ex.getMessage(), ex);
        }
    }

    private void onRemovePublication(final PublicationMessageFlyweight publicationMessage)
    {
        final String channel = publicationMessage.channel();
        final int sessionId = publicationMessage.sessionId();
        final int streamId = publicationMessage.streamId();

        try
        {
            final UdpChannel udpChannel = UdpChannel.parse(channel);
            final SendChannelEndpoint channelEndpoint = sendChannelEndpointByHash.get(udpChannel.consistentHash());
            if (null == channelEndpoint)
            {
                throw new ControlProtocolException(INVALID_CHANNEL, "channel unknown");
            }

            final DriverPublication publication = channelEndpoint.removePublication(sessionId, streamId);
            if (null == publication)
            {
                throw new ControlProtocolException(PUBLICATION_STREAM_UNKNOWN, "session and publication unknown for channel");
            }

            publications.remove(publication);
            publication.close();

            if (channelEndpoint.sessionCount() == 0)
            {
                sendChannelEndpointByHash.remove(udpChannel.consistentHash());
                channelEndpoint.close();
            }

            clientProxy.operationSucceeded(publicationMessage.correlationId());
        }
        catch (final ControlProtocolException ex)
        {
            throw ex;
        }
        catch (final Exception ex)
        {
            throw new ControlProtocolException(GENERIC_ERROR_MESSAGE, ex.getMessage(), ex);
        }
    }

    private void onAddSubscription(final SubscriptionMessageFlyweight subscriptionMessage)
    {
        final String channel = subscriptionMessage.channel();
        final int streamId = subscriptionMessage.streamId();
        final long correlationId = subscriptionMessage.correlationId();

        try
        {
            final UdpChannel udpChannel = UdpChannel.parse(channel);
            ReceiveChannelEndpoint channelEndpoint = receiveChannelEndpointByHash.get(udpChannel.consistentHash());

            if (null == channelEndpoint)
            {
                channelEndpoint = new ReceiveChannelEndpoint(udpChannel, conductorProxy, logger);
                receiveChannelEndpointByHash.put(udpChannel.consistentHash(), channelEndpoint);

                while (!receiverProxy.registerMediaEndpoint(channelEndpoint))
                {
                    System.out.println("Error adding a subscription - registering media channelEndpoint");
                }
            }

            final int initialCount = channelEndpoint.incRefToStream(streamId);

            if (1 == initialCount)
            {
                while (!receiverProxy.addSubscription(channelEndpoint, streamId))
                {
                    System.out.println("Error adding a subscription - add subscription");
                }
            }

            final DriverSubscription subscription =
                new DriverSubscription(channelEndpoint, streamId, correlationId, timerWheel.now());
            subscriptionByCorrelationId.put(correlationId, subscription);
            subscriptions.add(subscription);

            clientProxy.operationSucceeded(correlationId);
        }
        catch (final IllegalArgumentException ex)
        {
            clientProxy.onError(INVALID_CHANNEL, ex.getMessage(), subscriptionMessage, subscriptionMessage.length());
            logger.logException(ex);
        }
        catch (final Exception ex)
        {
            throw new ControlProtocolException(GENERIC_ERROR_MESSAGE, ex.getMessage(), ex);
        }
    }

    private void onRemoveSubscription(final SubscriptionMessageFlyweight subscriptionMessage)
    {
        final String channel = subscriptionMessage.channel();
        final int streamId = subscriptionMessage.streamId();
        final long registrationCorrelationId = subscriptionMessage.registrationCorrelationId();

        try
        {
            final UdpChannel udpChannel = UdpChannel.parse(channel);
            final ReceiveChannelEndpoint channelEndpoint = receiveChannelEndpointByHash.get(udpChannel.consistentHash());
            if (null == channelEndpoint)
            {
                throw new ControlProtocolException(INVALID_CHANNEL, "channel unknown");
            }

            if (channelEndpoint.getRefCountToStream(streamId) == 0)
            {
                throw new ControlProtocolException(SUBSCRIBER_NOT_REGISTERED, "subscriptions unknown for stream");
            }

            final DriverSubscription subscription = subscriptionByCorrelationId.remove(registrationCorrelationId);
            if (null == subscription)
            {
                throw new ControlProtocolException(SUBSCRIBER_NOT_REGISTERED, "subscription not registered");
            }
            subscriptions.remove(subscription);

            final int count = channelEndpoint.decRefToStream(streamId);
            if (0 == count)
            {
                while (!receiverProxy.removeSubscription(channelEndpoint, streamId))
                {
                    System.out.println("Error removing a subscription");
                }
            }

            if (channelEndpoint.streamCount() == 0)
            {
                receiveChannelEndpointByHash.remove(udpChannel.consistentHash());
                channelEndpoint.close();
            }

            clientProxy.operationSucceeded(subscriptionMessage.correlationId());
        }
        catch (final ControlProtocolException ex)
        {
            throw ex;
        }
        catch (final Exception ex)
        {
            throw new ControlProtocolException(GENERIC_ERROR_MESSAGE, ex.getMessage(), ex);
        }
    }

    // ----------------------- Heartbeats from Clients -----------------------

    private void onKeepalivePublication(final PublicationMessageFlyweight publicationMessage)
    {
        final String channel = publicationMessage.channel();
        final int sessionId = publicationMessage.sessionId();
        final int streamId = publicationMessage.streamId();

        try
        {
            // TODO: use lookup via correlationId instead.
            final UdpChannel udpChannel = UdpChannel.parse(channel);
            final SendChannelEndpoint channelEndpoint = sendChannelEndpointByHash.get(udpChannel.consistentHash());
            if (null == channelEndpoint)
            {
                return; // channel unknown
            }

            final DriverPublication publication = channelEndpoint.findPublication(sessionId, streamId);
            if (null == publication)
            {
                return; // publication (sessionId and streamId) unknown
            }

            // sessionId and streamId must be specific to a client publication, so correlation Id should not be needed.
            // keep publication alive for this correlationId by passing timerWheel.now() to it as last active time
            publication.timeOfLastKeepaliveFromClient(timerWheel.now());
        }
        catch (final Exception ex)
        {
            logger.logException(ex);
        }
    }

    private void onKeepaliveSubscription(final SubscriptionMessageFlyweight subscriptionMessage)
    {
        final String channel = subscriptionMessage.channel();
        final long correlationId = subscriptionMessage.registrationCorrelationId();

        try
        {
            // TODO: channel lookup is probably immaterial and can be removed.
            final UdpChannel udpChannel = UdpChannel.parse(channel);
            final ReceiveChannelEndpoint channelEndpoint = receiveChannelEndpointByHash.get(udpChannel.consistentHash());
            if (null == channelEndpoint)
            {
                return; // unknown channel
            }

            final DriverSubscription subscription = subscriptionByCorrelationId.get(correlationId);

            if (null == subscription)
            {
                return; // subscription unknown
            }

            // keep this subscription alive by updating the time
            subscription.timeOfLastKeepaliveFromClient(timerWheel.now());
        }
        catch (final Exception ex)
        {
            logger.logException(ex);
        }
    }

    // ----------------------- End Heartbeats -----------------------


    private void onHeartbeatCheck()
    {
        publications.forEach(DriverPublication::heartbeatCheck);
        rescheduleTimeout(HEARTBEAT_TIMEOUT_MS, TimeUnit.MILLISECONDS, heartbeatTimer);
    }

    private void onLivenessCheckPublications()
    {
        final long now = timerWheel.now();

        publications.forEach(
            (publication) ->
            {
                if (publication.timeOfLastKeepaliveFromClient() + LIVENESS_CLIENT_TIMEOUT_NS < now)
                {
                    final SendChannelEndpoint channelEndpoint = publication.sendChannelEndpoint();

                    logger.log(EventCode.REMOVE_PUBLICATION_TIMEOUT,
                               "%s %x:%x",
                               channelEndpoint.udpChannel().originalUriAsString(),
                               publication.sessionId(),
                               publication.streamId());

                    channelEndpoint.removePublication(publication.sessionId(), publication.streamId());
                    publications.remove(publication);
                    publication.close();

                    if (channelEndpoint.sessionCount() == 0)
                    {
                        sendChannelEndpointByHash.remove(channelEndpoint.udpChannel().consistentHash());
                        channelEndpoint.close();
                    }
                }
            }
        );

        rescheduleTimeout(LIVENESS_CHECK_TIMEOUT_MS, TimeUnit.MILLISECONDS, publicationLivenessCheckTimer);
    }

    // liveness check for Subscriptions from clients
    private void onLivenessCheckSubscriptions()
    {
        final long now = timerWheel.now();

        // the array should be removed from safely
        subscriptions.forEach(
            (subscription) ->
            {
                if (subscription.timeOfLastKeepaliveFromClient() + LIVENESS_CLIENT_TIMEOUT_NS < now)
                {
                    final ReceiveChannelEndpoint channelEndpoint = subscription.receiveChannelEndpoint();
                    final int streamId = subscription.streamId();

                    logger.log(EventCode.REMOVE_SUBSCRIPTION_TIMEOUT,
                               "%s %x [%x]",
                               channelEndpoint.udpChannel().originalUriAsString(),
                               subscription.streamId(),
                               subscription.correlationId());

                    subscriptions.remove(subscription);

                    if (0 == channelEndpoint.decRefToStream(streamId))
                    {
                        while (!receiverProxy.removeSubscription(channelEndpoint, streamId))
                        {
                            System.out.println("Error removing a subscription");
                        }
                    }

                    if (channelEndpoint.streamCount() == 0)
                    {
                        receiveChannelEndpointByHash.remove(channelEndpoint.udpTransport().udpChannel().consistentHash());
                        channelEndpoint.close();
                    }
                }
            }
        );

        rescheduleTimeout(LIVENESS_CHECK_TIMEOUT_MS, TimeUnit.MILLISECONDS, subscriptionLivenessCheckTimer);
    }

    // liveness check for Connections
    private void onLivenessCheckConnections()
    {
        final long now = timerWheel.now();

        connections.forEach(
            (connection) ->
            {
                if (connection.timeOfLastFrame() + LIVENESS_FRAME_TIMEOUT_NS < now)
                {
                    logger.log(EventCode.REMOVE_CONNECTION_TIMEOUT,
                               "%s %x:%x",
                               connection.receiveChannelEndpoint().udpChannel().originalUriAsString(),
                               connection.sessionId(),
                               connection.streamId());

                    clientProxy.onInactiveConnection(connection.sessionId(),
                                                     connection.streamId(),
                                                     connection.receiveChannelEndpoint().udpChannel().originalUriAsString());

                    while (!receiverProxy.removeConnection(connection))
                    {
                        System.out.println("Error removing connection");
                    }

                    connections.remove(connection);
                    connection.close();
                }
            }
        );

        rescheduleTimeout(LIVENESS_CHECK_TIMEOUT_MS, TimeUnit.MILLISECONDS, connectionLivenessCheckTimer);
    }

    private void onCreateConnection(final CreateConnectionCmd cmd)
    {
        final int sessionId = cmd.sessionId();
        final int streamId = cmd.streamId();
        final int initialTermId = cmd.termId();
        final InetSocketAddress controlAddress = cmd.controlAddress();
        final ReceiveChannelEndpoint channelEndpoint = cmd.channelEndpoint();
        final UdpChannel udpChannel = channelEndpoint.udpTransport().udpChannel();

        try
        {
            final String canonicalForm = udpChannel.canonicalForm();
            final TermBuffers termBuffers = termBuffersFactory.newConnection(canonicalForm, sessionId, streamId);

            final int positionCounterId =
                allocatePositionCounter("subscription", udpChannel.originalUriAsString(), sessionId, streamId);

            clientProxy.onNewTermBuffers(ON_NEW_CONNECTED_SUBSCRIPTION, sessionId, streamId, initialTermId,
                                         udpChannel.originalUriAsString(), termBuffers, 0, positionCounterId);

            final GapScanner[] gapScanners =
                termBuffers.stream()
                           .map((rawLog) -> new GapScanner(rawLog.logBuffer(), rawLog.stateBuffer()))
                           .toArray(GapScanner[]::new);

            final LossHandler lossHandler =
                new LossHandler(
                    gapScanners,
                    timerWheel,
                    udpChannel.isMulticast() ? NAK_MULTICAST_DELAY_GENERATOR : NAK_UNICAST_DELAY_GENERATOR,
                    channelEndpoint.composeNakMessageSender(controlAddress, sessionId, streamId),
                    initialTermId);

            final DriverConnection connection =
                new DriverConnection(
                    channelEndpoint,
                    sessionId,
                    streamId,
                    initialTermId,
                    initialWindowSize,
                    statusMessageTimeout,
                    termBuffers,
                    lossHandler,
                    channelEndpoint.composeStatusMessageSender(controlAddress, sessionId, streamId),
                    new BufferPositionIndicator(countersBuffer, positionCounterId, countersManager),
                    timerWheel::now);

            connections.add(connection);

            final NewConnectionCmd newConnectionCmd = new NewConnectionCmd(channelEndpoint, connection);
            while (!receiverProxy.newConnection(newConnectionCmd))
            {
                // TODO: count errors
                System.out.println("Error adding a connected subscription");
            }
        }
        catch (final Exception ex)
        {
            logger.logException(ex);
        }
    }

    private void onRemovedSubscription(final SubscriptionRemovedCmd cmd)
    {
        final DispatcherSubscription subscription = cmd.dispatcherSubscription();

        for (final DriverConnection connection : subscription.connections())
        {
            try
            {
                connections.remove(connection);
                connection.close();
            }
            catch (final Exception ex)
            {
                logger.logException(ex);
            }
        }
    }

    private int allocatePositionCounter(final String type, final String dirName, final int sessionId, final int streamId)
    {
        return countersManager.allocate(String.format("%s: %s %x %x", type, dirName, sessionId, streamId));
    }
}
