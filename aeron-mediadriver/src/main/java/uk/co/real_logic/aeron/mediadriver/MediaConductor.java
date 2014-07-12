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

import uk.co.real_logic.aeron.mediadriver.buffer.BufferManagement;
import uk.co.real_logic.aeron.mediadriver.buffer.BufferRotator;
import uk.co.real_logic.aeron.mediadriver.cmd.NewConnectedSubscriptionCmd;
import uk.co.real_logic.aeron.util.*;
import uk.co.real_logic.aeron.util.collections.Long2ObjectHashMap;
import uk.co.real_logic.aeron.util.command.PublicationMessageFlyweight;
import uk.co.real_logic.aeron.util.command.QualifiedMessageFlyweight;
import uk.co.real_logic.aeron.util.command.SubscriptionMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.util.event.EventCode;
import uk.co.real_logic.aeron.util.event.EventLogger;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.status.StatusBufferManager;

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
        new OptimalMulticastDelayGenerator(MediaDriver.NAK_MAX_BACKOFF_DEFAULT, MediaDriver.NAK_GROUPSIZE_DEFAULT,
                                           MediaDriver.NAK_GRTT_DEFAULT);

    /**
     * Source uses same for unicast and multicast. For now.
     */
    public static final FeedbackDelayGenerator RETRANS_UNICAST_DELAY_GENERATOR =
        () -> RETRANS_UNICAST_DELAY_DEFAULT_NS;
    public static final FeedbackDelayGenerator RETRANS_UNICAST_LINGER_GENERATOR =
        () -> RETRANS_UNICAST_LINGER_DEFAULT_NS;

    private final RingBuffer driverCommandBuffer;
    private final ReceiverProxy receiverProxy;
    private final ClientProxy clientProxy;
    private final NioSelector nioSelector;
    private final BufferManagement bufferManagement;
    private final RingBuffer fromClientCommands;
    private final Long2ObjectHashMap<ControlFrameHandler> srcDestinationMap = new Long2ObjectHashMap<>();
    private final TimerWheel timerWheel;
    private final AtomicArray<DriverConnectedSubscription> connectedSubscriptions;
    private final AtomicArray<DriverPublication> publications;

    private final Supplier<SenderControlStrategy> unicastSenderFlowControl;
    private final Supplier<SenderControlStrategy> multicastSenderFlowControl;

    private final PublicationMessageFlyweight publicationMessage = new PublicationMessageFlyweight();
    private final SubscriptionMessageFlyweight subscriptionMessage = new SubscriptionMessageFlyweight();
    private final QualifiedMessageFlyweight qualifiedMessage = new QualifiedMessageFlyweight();

    private final int mtuLength;
    private final TimerWheel.Timer heartbeatTimer;
    private final StatusBufferManager statusBufferManager;

    public MediaConductor(final MediaDriverContext ctx)
    {
        super(ctx.conductorIdleStrategy());

        this.driverCommandBuffer = ctx.driverCommandBuffer();
        this.receiverProxy = ctx.receiverProxy();
        this.bufferManagement = ctx.bufferManagement();
        this.nioSelector = ctx.conductorNioSelector();
        this.mtuLength = ctx.mtuLength();
        this.unicastSenderFlowControl = ctx.unicastSenderFlowControl();
        this.multicastSenderFlowControl = ctx.multicastSenderFlowControl();
        this.statusBufferManager = ctx.statusBufferManager();

        timerWheel = ctx.conductorTimerWheel();
        heartbeatTimer = newTimeout(HEARTBEAT_TIMEOUT_MS, TimeUnit.MILLISECONDS, this::onHeartbeatCheck);

        connectedSubscriptions = ctx.connectedSubscriptions();
        publications = ctx.publications();
        fromClientCommands = ctx.fromClientCommands();
        clientProxy = ctx.clientProxy();
    }

    public ControlFrameHandler frameHandler(final UdpDestination destination)
    {
        return srcDestinationMap.get(destination.consistentHash());
    }

    public int doWork()
    {
        int workCount = 0;

        try
        {
            workCount += nioSelector.processKeys();
        }
        catch (final Exception ex)
        {
            LOGGER.logException(ex);
        }

        workCount += publications.doAction(DriverPublication::cleanLogBuffer);
        workCount += connectedSubscriptions.doAction(DriverConnectedSubscription::cleanLogBuffer);
        workCount += connectedSubscriptions.doAction(DriverConnectedSubscription::scanForGaps);
        workCount += connectedSubscriptions.doAction((subscription) -> subscription.sendAnyPendingSm(timerWheel.now()));

        workCount += processFromClientCommandBuffer();
        workCount += processMediaCommandBuffer();
        workCount += processTimers();

        return workCount;
    }

    public void close()
    {
        stop();

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

    private int processMediaCommandBuffer()
    {
        return driverCommandBuffer.read(
            (msgTypeId, buffer, index, length) ->
            {
                switch (msgTypeId)
                {
                    case CREATE_CONNECTED_SUBSCRIPTION:
                        qualifiedMessage.wrap(buffer, index);
                        onCreateConnectedSubscription(qualifiedMessage);
                        break;

                    case REMOVE_CONNECTED_SUBSCRIPTION:
                        qualifiedMessage.wrap(buffer, index);
                        onRemoveConnectedSubscription(qualifiedMessage);
                        break;

                    case ERROR_RESPONSE:
                        clientProxy.onError(msgTypeId, buffer, index, length);
                        break;
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
            final BufferRotator bufferRotator = bufferManagement.addPublication(srcDestination, sessionId, channelId);
            final SenderControlStrategy flowControlStrategy =
                srcDestination.isMulticast() ? multicastSenderFlowControl.get() : unicastSenderFlowControl.get();

            publication = new DriverPublication(frameHandler,
                                                timerWheel,
                                                flowControlStrategy,
                                                bufferRotator,
                                                sessionId,
                                                channelId,
                                                initialTermId,
                                                HEADER_LENGTH,
                                                mtuLength);

            frameHandler.addPublication(publication);
            final int positionCounterId = positionCounterId("publication", destination, sessionId, channelId);

            clientProxy.onNewLogBuffers(ON_NEW_PUBLICATION, sessionId, channelId,
                                        initialTermId, destination, bufferRotator, correlationId, positionCounterId);
            publications.add(publication);
        }
        catch (final ControlProtocolException ex)
        {
            throw ex;
        }
        catch (final Exception ex)
        {
            throw new ControlProtocolException(GENERIC_ERROR_MESSAGE, ex.getMessage());
        }
    }

    private int positionCounterId(final String type,
                                  final String destination,
                                  final long sessionId,
                                  final long channelId)
    {
        final String label = String.format("%s: %s %d %d", type, destination, sessionId, channelId);
        final int id = statusBufferManager.registerCounter(label);

        return StatusBufferManager.counterOffset(id);
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
                throw new ControlProtocolException(INVALID_DESTINATION_IN_PUBLICATION, "destination unknown");
            }

            final DriverPublication publication = frameHandler.removePublication(sessionId, channelId);
            if (null == publication)
            {
                throw new ControlProtocolException(PUBLICATION_CHANNEL_UNKNOWN,
                                                   "session and publication unknown for destination");
            }

            publications.remove(publication);

            if (frameHandler.sessionCount() == 0)
            {
                srcDestinationMap.remove(srcDestination.consistentHash());
                frameHandler.close();
            }

            bufferManagement.removePublication(srcDestination, sessionId, channelId);

            clientProxy.operationSucceeded(publicationMessage.correlationId());
        }
        catch (final ControlProtocolException ex)
        {
            throw ex;
        }
        catch (final Exception ex)
        {
            throw new ControlProtocolException(GENERIC_ERROR_MESSAGE, ex.getMessage());
        }
    }

    private void onAddSubscription(final SubscriptionMessageFlyweight subscriberMessage)
    {
        receiverProxy.addSubscription(subscriberMessage.destination(), subscriberMessage.channelIds());
    }

    private void onRemoveSubscription(final SubscriptionMessageFlyweight subscriberMessage)
    {
        receiverProxy.removeSubscription(subscriberMessage.destination(), subscriberMessage.channelIds());
    }

    private void onCreateConnectedSubscription(final QualifiedMessageFlyweight qualifiedMessage)
    {
        final String destination = qualifiedMessage.destination();
        final long sessionId = qualifiedMessage.sessionId();
        final long channelId = qualifiedMessage.channelId();
        final long termId = qualifiedMessage.termId();

        try
        {
            final UdpDestination udpDst = UdpDestination.parse(destination);
            final BufferRotator bufferRotator = bufferManagement.addConnectedSubscription(udpDst, sessionId, channelId);

            clientProxy.onNewLogBuffers(ON_NEW_CONNECTED_SUBSCRIPTION, sessionId, channelId, termId,
                                        destination, bufferRotator, 0, 0);

            final NewConnectedSubscriptionCmd event =
                new NewConnectedSubscriptionCmd(udpDst, sessionId, channelId, termId, bufferRotator);

            while (!receiverProxy.newConnectedSubscription(event))
            {
                // TODO: count errors
                System.out.println("Error adding to connected subscription");
            }
        }
        catch (final Exception ex)
        {
            LOGGER.logException(ex);
        }
    }

    private void onRemoveConnectedSubscription(final QualifiedMessageFlyweight qualifiedMessage)
    {
        final String destination = qualifiedMessage.destination();
        final long sessionId = qualifiedMessage.sessionId();
        final long channelId = qualifiedMessage.channelId();

        try
        {
            final UdpDestination udpDst = UdpDestination.parse(destination);
            bufferManagement.removeConnectedSubscription(udpDst, sessionId, channelId);
        }
        catch (final Exception ex)
        {
            LOGGER.logException(ex);
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
}
