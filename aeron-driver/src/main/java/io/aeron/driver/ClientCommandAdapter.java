/*
 * Copyright 2014-2018 Real Logic Ltd.
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

import io.aeron.command.*;
import io.aeron.driver.exceptions.ControlProtocolException;
import org.agrona.ErrorHandler;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.agrona.concurrent.status.AtomicCounter;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.CommonContext.SPY_PREFIX;
import static io.aeron.ErrorCode.GENERIC_ERROR;
import static io.aeron.command.ControlProtocolEvents.*;

/**
 * Receives commands from Aeron clients and dispatches them to the {@link DriverConductor} for processing.
 */
class ClientCommandAdapter implements MessageHandler
{
    private final PublicationMessageFlyweight publicationMsgFlyweight = new PublicationMessageFlyweight();
    private final SubscriptionMessageFlyweight subscriptionMsgFlyweight = new SubscriptionMessageFlyweight();
    private final CorrelatedMessageFlyweight correlatedMsgFlyweight = new CorrelatedMessageFlyweight();
    private final RemoveMessageFlyweight removeMsgFlyweight = new RemoveMessageFlyweight();
    private final DestinationMessageFlyweight destinationMsgFlyweight = new DestinationMessageFlyweight();
    private final CounterMessageFlyweight counterMsgFlyweight = new CounterMessageFlyweight();
    private final DriverConductor conductor;
    private final RingBuffer toDriverCommands;
    private final ClientProxy clientProxy;
    private final AtomicCounter errors;
    private final ErrorHandler errorHandler;

    ClientCommandAdapter(
        final AtomicCounter errors,
        final ErrorHandler errorHandler,
        final RingBuffer toDriverCommands,
        final ClientProxy clientProxy,
        final DriverConductor driverConductor)
    {
        this.errors = errors;
        this.errorHandler = errorHandler;
        this.toDriverCommands = toDriverCommands;
        this.clientProxy = clientProxy;
        this.conductor = driverConductor;
    }

    public int receive()
    {
        return toDriverCommands.read(this, Configuration.COMMAND_DRAIN_LIMIT);
    }

    @SuppressWarnings("MethodLength")
    public void onMessage(
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
                    publicationMsgFlyweight.wrap(buffer, index);

                    correlationId = publicationMsgFlyweight.correlationId();
                    addPublication(correlationId, false);
                    break;
                }

                case REMOVE_PUBLICATION:
                {
                    removeMsgFlyweight.wrap(buffer, index);

                    correlationId = removeMsgFlyweight.correlationId();
                    conductor.onRemovePublication(removeMsgFlyweight.registrationId(), correlationId);
                    break;
                }

                case ADD_EXCLUSIVE_PUBLICATION:
                {
                    publicationMsgFlyweight.wrap(buffer, index);

                    correlationId = publicationMsgFlyweight.correlationId();
                    addPublication(correlationId, true);
                    break;
                }

                case ADD_SUBSCRIPTION:
                {
                    subscriptionMsgFlyweight.wrap(buffer, index);

                    correlationId = subscriptionMsgFlyweight.correlationId();
                    final int streamId = subscriptionMsgFlyweight.streamId();
                    final long clientId = subscriptionMsgFlyweight.clientId();
                    final String channel = subscriptionMsgFlyweight.channel();

                    if (channel.startsWith(IPC_CHANNEL))
                    {
                        conductor.onAddIpcSubscription(channel, streamId, correlationId, clientId);
                    }
                    else if (channel.startsWith(SPY_PREFIX))
                    {
                        conductor.onAddSpySubscription(channel, streamId, correlationId, clientId);
                    }
                    else
                    {
                        conductor.onAddNetworkSubscription(channel, streamId, correlationId, clientId);
                    }
                    break;
                }

                case REMOVE_SUBSCRIPTION:
                {
                    removeMsgFlyweight.wrap(buffer, index);

                    correlationId = removeMsgFlyweight.correlationId();
                    conductor.onRemoveSubscription(removeMsgFlyweight.registrationId(), correlationId);
                    break;
                }

                case ADD_DESTINATION:
                {
                    destinationMsgFlyweight.wrap(buffer, index);

                    correlationId = destinationMsgFlyweight.correlationId();
                    final long channelRegistrationId = destinationMsgFlyweight.registrationCorrelationId();
                    final String channel = destinationMsgFlyweight.channel();

                    conductor.onAddDestination(channelRegistrationId, channel, correlationId);
                    break;
                }

                case REMOVE_DESTINATION:
                {
                    destinationMsgFlyweight.wrap(buffer, index);

                    correlationId = destinationMsgFlyweight.correlationId();
                    final long channelRegistrationId = destinationMsgFlyweight.registrationCorrelationId();
                    final String channel = destinationMsgFlyweight.channel();

                    conductor.onRemoveDestination(channelRegistrationId, channel, correlationId);
                    break;
                }

                case CLIENT_KEEPALIVE:
                {
                    correlatedMsgFlyweight.wrap(buffer, index);

                    conductor.onClientKeepalive(correlatedMsgFlyweight.clientId());
                    break;
                }

                case ADD_COUNTER:
                {
                    counterMsgFlyweight.wrap(buffer, index);

                    correlationId = counterMsgFlyweight.correlationId();
                    conductor.onAddCounter(
                        counterMsgFlyweight.typeId(),
                        buffer,
                        index + counterMsgFlyweight.keyBufferOffset(),
                        counterMsgFlyweight.keyBufferLength(),
                        buffer,
                        index + counterMsgFlyweight.labelBufferOffset(),
                        counterMsgFlyweight.labelBufferLength(),
                        correlationId,
                        counterMsgFlyweight.clientId());
                    break;
                }

                case REMOVE_COUNTER:
                {
                    removeMsgFlyweight.wrap(buffer, index);

                    correlationId = removeMsgFlyweight.correlationId();
                    conductor.onRemoveCounter(removeMsgFlyweight.registrationId(), correlationId);
                    break;
                }

                case CLIENT_CLOSE:
                {
                    correlatedMsgFlyweight.wrap(buffer, index);

                    correlationId = correlatedMsgFlyweight.correlationId();
                    conductor.onClientClose(correlatedMsgFlyweight.clientId(), correlationId);
                    break;
                }

                case ADD_RCV_DESTINATION:
                {
                    destinationMsgFlyweight.wrap(buffer, index);

                    correlationId = destinationMsgFlyweight.correlationId();
                    final long channelRegistrationId = destinationMsgFlyweight.registrationCorrelationId();
                    final String channel = destinationMsgFlyweight.channel();

                    conductor.onAddRcvDestination(channelRegistrationId, channel, correlationId);
                    break;
                }

                case REMOVE_RCV_DESTINATION:
                {
                    destinationMsgFlyweight.wrap(buffer, index);

                    correlationId = destinationMsgFlyweight.correlationId();
                    final long channelRegistrationId = destinationMsgFlyweight.registrationCorrelationId();
                    final String channel = destinationMsgFlyweight.channel();

                    conductor.onRemoveRcvDestination(channelRegistrationId, channel, correlationId);
                    break;
                }
            }
        }
        catch (final ControlProtocolException ex)
        {
            clientProxy.onError(correlationId, ex.errorCode(), ex.getMessage());
            recordError(ex);
        }
        catch (final Exception ex)
        {
            final String errorMessage = ex.getClass().getSimpleName() + " : " + ex.getMessage();
            clientProxy.onError(correlationId, GENERIC_ERROR, errorMessage);
            recordError(ex);
        }
    }

    public void addPublication(final long correlationId, final boolean isExclusive)
    {
        final int streamId = publicationMsgFlyweight.streamId();
        final long clientId = publicationMsgFlyweight.clientId();
        final String channel = publicationMsgFlyweight.channel();

        if (channel.startsWith(IPC_CHANNEL))
        {
            conductor.onAddIpcPublication(channel, streamId, correlationId, clientId, isExclusive);
        }
        else
        {
            conductor.onAddNetworkPublication(channel, streamId, correlationId, clientId, isExclusive);
        }
    }

    private void recordError(final Exception ex)
    {
        errors.increment();
        errorHandler.onError(ex);
    }
}
