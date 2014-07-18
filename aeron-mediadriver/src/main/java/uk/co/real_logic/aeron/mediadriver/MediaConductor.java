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
package uk.co.real_logic.aeron.mediadriver;

import uk.co.real_logic.aeron.mediadriver.buffer.TermBuffersFactory;
import uk.co.real_logic.aeron.mediadriver.buffer.TermBuffers;
import uk.co.real_logic.aeron.mediadriver.cmd.CreateConnectedSubscriptionCmd;
import uk.co.real_logic.aeron.mediadriver.cmd.NewConnectedSubscriptionCmd;
import uk.co.real_logic.aeron.mediadriver.cmd.SubscriptionRemovedCmd;
import uk.co.real_logic.aeron.util.*;
import uk.co.real_logic.aeron.util.collections.Long2ObjectHashMap;
import uk.co.real_logic.aeron.util.command.PublicationMessageFlyweight;
import uk.co.real_logic.aeron.util.command.SubscriptionMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicArray;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.OneToOneConcurrentArrayQueue;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.GapScanner;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.util.event.EventCode;
import uk.co.real_logic.aeron.util.event.EventLogger;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.status.*;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static uk.co.real_logic.aeron.mediadriver.MediaDriver.*;
import static uk.co.real_logic.aeron.util.ErrorCode.*;
import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.*;

/**
 * Media Conductor to take commands from Publishers and Subscribers as well as handle NAKs and retransmissions
 */
public class MediaConductor extends Agent
{
    private static final EventLogger LOGGER = new EventLogger(MediaConductor.class);

    public static final int HEADER_LENGTH = DataHeaderFlyweight.HEADER_LENGTH;
    public static final int HEARTBEAT_TIMEOUT_MS = 100;

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
    public static final FeedbackDelayGenerator RETRANS_UNICAST_DELAY_GENERATOR =
        () -> RETRANS_UNICAST_DELAY_DEFAULT_NS;
    public static final FeedbackDelayGenerator RETRANS_UNICAST_LINGER_GENERATOR =
        () -> RETRANS_UNICAST_LINGER_DEFAULT_NS;

    private final OneToOneConcurrentArrayQueue<? super Object> commandQueue;
    private final ReceiverProxy receiverProxy;
    private final ClientProxy clientProxy;
    private final NioSelector nioSelector;
    private final TermBuffersFactory termBuffersFactory;
    private final RingBuffer fromClientCommands;
    private final Long2ObjectHashMap<ControlFrameHandler> srcDestinationMap = new Long2ObjectHashMap<>();
    private final TimerWheel timerWheel;
    private final ArrayList<DriverConnectedSubscription> connectedSubscriptions = new ArrayList<>();
    private final AtomicArray<DriverPublication> publications;

    private final Supplier<SenderControlStrategy> unicastSenderFlowControl;
    private final Supplier<SenderControlStrategy> multicastSenderFlowControl;

    private final PublicationMessageFlyweight publicationMessage = new PublicationMessageFlyweight();
    private final SubscriptionMessageFlyweight subscriptionMessage = new SubscriptionMessageFlyweight();

    private final int mtuLength;
    private final int initialWindowSize;
    private final TimerWheel.Timer heartbeatTimer;
    private final StatusBufferManager statusBufferManager;
    private final AtomicBuffer counterValuesBuffer;

    public MediaConductor(final MediaDriverContext ctx)
    {
        super(ctx.conductorIdleStrategy(), LOGGER::logException);

        this.commandQueue = ctx.conductorCommandQueue();
        this.receiverProxy = ctx.receiverProxy();
        this.termBuffersFactory = ctx.termBuffersFactory();
        this.nioSelector = ctx.conductorNioSelector();
        this.mtuLength = ctx.mtuLength();
        this.initialWindowSize = ctx.initialWindowSize();
        this.unicastSenderFlowControl = ctx.unicastSenderFlowControl();
        this.multicastSenderFlowControl = ctx.multicastSenderFlowControl();
        this.statusBufferManager = ctx.statusBufferManager();
        this.counterValuesBuffer = ctx.counterValuesBuffer();

        timerWheel = ctx.conductorTimerWheel();
        heartbeatTimer = newTimeout(HEARTBEAT_TIMEOUT_MS, TimeUnit.MILLISECONDS, this::onHeartbeatCheck);

        publications = ctx.publications();
        fromClientCommands = ctx.fromClientCommands();
        clientProxy = ctx.clientProxy();
    }

    public ControlFrameHandler getFrameHandler(final UdpDestination destination)
    {
        return srcDestinationMap.get(destination.consistentHash());
    }

    public int doWork() throws Exception
    {
        int workCount = nioSelector.processKeys();

        workCount += publications.doAction(DriverPublication::cleanLogBuffer);

        final long now = timerWheel.now();
        for (final DriverConnectedSubscription connectedSubscription : connectedSubscriptions)
        {
            workCount += connectedSubscription.cleanLogBuffer();
            workCount += connectedSubscription.scanForGaps();
            workCount += connectedSubscription.sendPendingStatusMessages(now);
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
        connectedSubscriptions.forEach(DriverConnectedSubscription::close);
        srcDestinationMap.forEach((hash, frameHandler) -> frameHandler.close());
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
                    if (obj instanceof CreateConnectedSubscriptionCmd)
                    {
                        onCreateConnectedSubscription((CreateConnectedSubscriptionCmd)obj);
                    }
                    else if (obj instanceof SubscriptionRemovedCmd)
                    {
                        onRemovedSubscription((SubscriptionRemovedCmd)obj);
                    }
                }
                catch (final Exception ex)
                {
                    LOGGER.logException(ex);
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
                            LOGGER.log(EventCode.CMD_IN_ADD_PUBLICATION, buffer, index, length);
                            flyweight = publicationMessage;
                            onAddPublication(publicationMessage);
                            break;

                        case REMOVE_PUBLICATION:
                            publicationMessage.wrap(buffer, index);
                            LOGGER.log(EventCode.CMD_IN_REMOVE_PUBLICATION, buffer, index, length);
                            flyweight = publicationMessage;
                            onRemovePublication(publicationMessage);
                            break;

                        case ADD_SUBSCRIPTION:
                            subscriptionMessage.wrap(buffer, index);
                            LOGGER.log(EventCode.CMD_IN_ADD_SUBSCRIPTION, buffer, index, length);
                            flyweight = subscriptionMessage;
                            onAddSubscription(subscriptionMessage);
                            break;

                        case REMOVE_SUBSCRIPTION:
                            subscriptionMessage.wrap(buffer, index);
                            LOGGER.log(EventCode.CMD_IN_REMOVE_SUBSCRIPTION, buffer, index, length);
                            flyweight = subscriptionMessage;
                            onRemoveSubscription(subscriptionMessage);
                            break;
                    }
                }
                catch (final ControlProtocolException ex)
                {
                    clientProxy.onError(ex.errorCode(), ex.getMessage(), flyweight, length);
                    LOGGER.logException(ex);
                }
                catch (final Exception ex)
                {
                    clientProxy.onError(GENERIC_ERROR, ex.getMessage(), flyweight, length);
                    LOGGER.logException(ex);
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
        final String destination = publicationMessage.destination();
        final long sessionId = publicationMessage.sessionId();
        final long channelId = publicationMessage.channelId();
        final long correlationId = publicationMessage.correlationId();

        try
        {
            final UdpDestination srcDestination = UdpDestination.parse(destination);
            ControlFrameHandler frameHandler = srcDestinationMap.get(srcDestination.consistentHash());
            if (null == frameHandler)
            {
                frameHandler = new ControlFrameHandler(srcDestination, nioSelector);
                srcDestinationMap.put(srcDestination.consistentHash(), frameHandler);
            }
            else if (!frameHandler.destination().equals(srcDestination))
            {
                throw new ControlProtocolException(ErrorCode.PUBLICATION_CHANNEL_ALREADY_EXISTS,
                                                   "destinations hash same, but destinations different");
            }

            DriverPublication publication = frameHandler.findPublication(sessionId, channelId);
            if (null != publication)
            {
                throw new ControlProtocolException(ErrorCode.PUBLICATION_CHANNEL_ALREADY_EXISTS,
                                                   "publication and session already exist on destination");
            }

            final long initialTermId = generateTermId();
            final TermBuffers termBuffers = termBuffersFactory.newPublication(srcDestination, sessionId, channelId);
            final SenderControlStrategy flowControlStrategy =
                srcDestination.isMulticast() ? multicastSenderFlowControl.get() : unicastSenderFlowControl.get();

            final int positionCounterOffset = registerPositionCounter("publication", destination, sessionId, channelId);
            final BufferPositionReporter positionReporter =
                new BufferPositionReporter(counterValuesBuffer, positionCounterOffset);

            publication = new DriverPublication(frameHandler,
                                                timerWheel,
                                                flowControlStrategy,
                                                termBuffers,
                                                positionReporter,
                                                sessionId,
                                                channelId,
                                                initialTermId,
                                                HEADER_LENGTH,
                                                mtuLength);

            frameHandler.addPublication(publication);

            clientProxy.onNewTermBuffers(ON_NEW_PUBLICATION, sessionId, channelId, initialTermId, destination,
                                         termBuffers, correlationId, positionCounterOffset);

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
        final String destination = publicationMessage.destination();
        final long sessionId = publicationMessage.sessionId();
        final long channelId = publicationMessage.channelId();

        try
        {
            final UdpDestination srcDestination = UdpDestination.parse(destination);
            final ControlFrameHandler frameHandler = srcDestinationMap.get(srcDestination.consistentHash());
            if (null == frameHandler)
            {
                throw new ControlProtocolException(INVALID_DESTINATION, "destination unknown");
            }

            final DriverPublication publication = frameHandler.removePublication(sessionId, channelId);
            if (null == publication)
            {
                throw new ControlProtocolException(PUBLICATION_CHANNEL_UNKNOWN,
                                                   "session and publication unknown for destination");
            }

            publications.remove(publication);
            publication.close();

            if (frameHandler.sessionCount() == 0)
            {
                srcDestinationMap.remove(srcDestination.consistentHash());
                frameHandler.close();
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

    private void onAddSubscription(final SubscriptionMessageFlyweight subscriberMessage)
    {
        final String destination = subscriberMessage.destination();
        try
        {
            final UdpDestination udpDestination = UdpDestination.parse(destination);
            if (!receiverProxy.addSubscription(udpDestination, subscriberMessage.channelId()))
            {
                // TODO: should we error here?
            }

            clientProxy.operationSucceeded(subscriberMessage.correlationId());
        }
        catch (final IllegalArgumentException ex)
        {
            clientProxy.onError(INVALID_DESTINATION, ex.getMessage(), subscriberMessage, subscriberMessage.length());
        }
    }

    private void onRemoveSubscription(final SubscriptionMessageFlyweight subscriberMessage)
    {
        final String destination = subscriberMessage.destination();
        final UdpDestination udpDestination = UdpDestination.parse(destination);

        if (!receiverProxy.removeSubscription(udpDestination, subscriberMessage.channelId()))
        {
            // TODO: should we error here?
        }
    }

    private void onCreateConnectedSubscription(final CreateConnectedSubscriptionCmd cmd)
    {
        final UdpDestination udpDestination = cmd.udpDestination();
        final long sessionId = cmd.sessionId();
        final long channelId = cmd.channelId();
        final long initialTermId = cmd.termId();

        try
        {
            final TermBuffers termBuffers =
                termBuffersFactory.newConnectedSubscription(udpDestination, sessionId, channelId);

            final int positionCounterOffset = registerPositionCounter("subscription", udpDestination
                .clientAwareUri(), sessionId, channelId);

            clientProxy.onNewTermBuffers(ON_NEW_CONNECTED_SUBSCRIPTION, sessionId, channelId, initialTermId,
                                         udpDestination.clientAwareUri(), termBuffers, 0, positionCounterOffset);

            final GapScanner[] gapScanners =
                termBuffers.stream()
                           .map((rawLog) -> new GapScanner(rawLog.logBuffer(), rawLog.stateBuffer()))
                           .toArray(GapScanner[]::new);

            final FeedbackDelayGenerator delayGenerator =
                udpDestination.isMulticast() ? NAK_MULTICAST_DELAY_GENERATOR : NAK_UNICAST_DELAY_GENERATOR;

            final LossHandler lossHandler =
                new LossHandler(gapScanners, timerWheel, delayGenerator, cmd.sendNakHandler(), initialTermId);

            final PositionIndicator indicator = new BufferPositionIndicator(counterValuesBuffer, positionCounterOffset);

            final DriverConnectedSubscription connectedSubscription =
                new DriverConnectedSubscription(udpDestination,
                                                sessionId,
                                                channelId,
                                                initialTermId,
                                                initialWindowSize,
                                                termBuffers,
                                                lossHandler,
                                                cmd.sendSmHandler(),
                                                indicator);

            connectedSubscriptions.add(connectedSubscription);

            final NewConnectedSubscriptionCmd newConnectedSubscriptionCmd =
                new NewConnectedSubscriptionCmd(connectedSubscription);

            while (!receiverProxy.newConnectedSubscription(newConnectedSubscriptionCmd))
            {
                // TODO: count errors
                System.out.println("Error adding a connected subscription");
            }
        }
        catch (final Exception ex)
        {
            LOGGER.logException(ex);
        }
    }

    private void onRemovedSubscription(final SubscriptionRemovedCmd cmd)
    {
        final DriverSubscription subscription = cmd.driverSubscription();

        for (final DriverConnectedSubscription connectedSubscription : subscription.connectedSubscriptions())
        {
            try
            {
                connectedSubscriptions.remove(connectedSubscription);
                connectedSubscription.close();
            }
            catch (final Exception ex)
            {
                LOGGER.logException(ex);
            }
        }
    }

    private void onHeartbeatCheck()
    {
        publications.forEach(DriverPublication::heartbeatCheck);
        rescheduleTimeout(HEARTBEAT_TIMEOUT_MS, TimeUnit.MILLISECONDS, heartbeatTimer);
    }

    private long generateTermId()
    {
        // term Id can be psuedo-random. Doesn't have to be perfect. But must be in the range [0, 0x7FFFFFFF]
        return (int)(Math.random() * (double)0x7FFFFFFF);
    }

    private int registerPositionCounter(final String type,
                                        final String destination,
                                        final long sessionId,
                                        final long channelId)
    {
        final String label = String.format("%s: %s %d %d", type, destination, sessionId, channelId);
        final int id = statusBufferManager.registerCounter(label);

        return StatusBufferManager.counterOffset(id);
    }
}
