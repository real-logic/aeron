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
package uk.co.real_logic.aeron;

import uk.co.real_logic.aeron.common.ErrorCode;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.aeron.common.command.ConnectionMessageFlyweight;
import uk.co.real_logic.aeron.common.command.ConnectionBuffersReadyFlyweight;
import uk.co.real_logic.aeron.common.command.CorrelatedMessageFlyweight;
import uk.co.real_logic.aeron.common.command.PublicationBuffersReadyFlyweight;
import uk.co.real_logic.agrona.concurrent.MessageHandler;
import uk.co.real_logic.agrona.concurrent.broadcast.CopyBroadcastReceiver;
import uk.co.real_logic.aeron.common.protocol.ErrorFlyweight;

import static uk.co.real_logic.aeron.common.command.ControlProtocolEvents.*;

/**
 * Analogue of {@link DriverProxy} on the client side
 */
class DriverListenerAdapter implements MessageHandler
{
    private final CopyBroadcastReceiver broadcastReceiver;

    private final ErrorFlyweight errorHeader = new ErrorFlyweight();
    private final PublicationBuffersReadyFlyweight publicationReady = new PublicationBuffersReadyFlyweight();
    private final ConnectionBuffersReadyFlyweight connectionReady = new ConnectionBuffersReadyFlyweight();
    private final CorrelatedMessageFlyweight correlatedMessage = new CorrelatedMessageFlyweight();
    private final ConnectionMessageFlyweight connectionMessage = new ConnectionMessageFlyweight();
    private final DriverListener listener;

    private long activeCorrelationId;

    public DriverListenerAdapter(final CopyBroadcastReceiver broadcastReceiver, final DriverListener listener)
    {
        this.broadcastReceiver = broadcastReceiver;
        this.listener = listener;
    }

    public int receiveMessages(final long correlationId)
    {
        activeCorrelationId = correlationId;

        return broadcastReceiver.receive(this);
    }

    public void onMessage(final int msgTypeId, final MutableDirectBuffer buffer, final int index, final int length)
    {
        switch (msgTypeId)
        {
            case ON_PUBLICATION_READY:
            {
                publicationReady.wrap(buffer, index);

                final long correlationId = publicationReady.correlationId();

                if (activeCorrelationId == correlationId)
                {
                    final int sessionId = publicationReady.sessionId();
                    final int streamId = publicationReady.streamId();
                    final int positionCounterId = publicationReady.positionCounterId();
                    final int mtuLength = publicationReady.mtuLength();
                    final String channel = publicationReady.channel();
                    final String logFileName = publicationReady.logFileName();


                    listener.onNewPublication(
                        channel, streamId, sessionId, positionCounterId, mtuLength, logFileName, correlationId);
                }
                break;
            }

            case ON_CONNECTION_READY:
            {
                connectionReady.wrap(buffer, index);

                final int sessionId = connectionReady.sessionId();
                final int streamId = connectionReady.streamId();
                final long joiningPosition = connectionReady.joiningPosition();
                final String channel = connectionReady.channel();
                final String logFileName = connectionReady.logFileName();
                final long correlationId = connectionReady.correlationId();

                listener.onNewConnection(
                    channel, streamId, sessionId, joiningPosition, logFileName, connectionReady, correlationId);
                break;
            }

            case ON_OPERATION_SUCCESS:
                correlatedMessage.wrap(buffer, index);
                if (correlatedMessage.correlationId() == activeCorrelationId)
                {
                    listener.operationSucceeded();
                }
                break;

            case ON_INACTIVE_CONNECTION:
                connectionMessage.wrap(buffer, index);
                listener.onInactiveConnection(
                    connectionMessage.channel(),
                    connectionMessage.streamId(),
                    connectionMessage.sessionId(),
                    connectionMessage.correlationId());
                break;

            case ON_ERROR:
                onError(buffer, index, listener, activeCorrelationId);
                break;

            default:
                break;
        }
    }

    private void onError(
        final MutableDirectBuffer buffer, final int index, final DriverListener listener, final long activeCorrelationId)
    {
        errorHeader.wrap(buffer, index);
        final ErrorCode errorCode = errorHeader.errorCode();
        if (activeCorrelationId == correlationId(buffer, errorHeader.offendingHeaderOffset()))
        {
            listener.onError(errorCode, errorHeader.errorMessage());
        }
    }

    private long correlationId(final MutableDirectBuffer buffer, final int offset)
    {
        correlatedMessage.wrap(buffer, offset);

        return correlatedMessage.correlationId();
    }
}
