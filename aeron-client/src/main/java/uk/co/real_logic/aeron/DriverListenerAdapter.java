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

import uk.co.real_logic.aeron.command.ConnectionBuffersReadyFlyweight;
import uk.co.real_logic.aeron.command.ConnectionMessageFlyweight;
import uk.co.real_logic.aeron.command.CorrelatedMessageFlyweight;
import uk.co.real_logic.aeron.command.PublicationBuffersReadyFlyweight;
import uk.co.real_logic.aeron.protocol.ErrorFlyweight;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.MessageHandler;
import uk.co.real_logic.agrona.concurrent.broadcast.CopyBroadcastReceiver;

import static uk.co.real_logic.aeron.command.ControlProtocolEvents.*;

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
    private long lastReceivedCorrelationId;
    private String expectedChannel;

    public DriverListenerAdapter(final CopyBroadcastReceiver broadcastReceiver, final DriverListener listener)
    {
        this.broadcastReceiver = broadcastReceiver;
        this.listener = listener;
    }

    public int pollMessage(final long activeCorrelationId, final String expectedChannel)
    {
        this.activeCorrelationId = activeCorrelationId;
        this.lastReceivedCorrelationId = -1;
        this.expectedChannel = expectedChannel;

        return broadcastReceiver.receive(this);
    }

    public long lastReceivedCorrelationId()
    {
        return lastReceivedCorrelationId;
    }

    public void onMessage(final int msgTypeId, final MutableDirectBuffer buffer, final int index, final int length)
    {
        switch (msgTypeId)
        {
            case ON_PUBLICATION_READY:
            {
                publicationReady.wrap(buffer, index);

                final long correlationId = publicationReady.correlationId();

                if (correlationId == activeCorrelationId)
                {
                    final int sessionId = publicationReady.sessionId();
                    final int streamId = publicationReady.streamId();
                    final int publicationLimitCounterId = publicationReady.publicationLimitCounterId();
                    final String logFileName = publicationReady.logFileName();

                    listener.onNewPublication(
                        expectedChannel, streamId, sessionId, publicationLimitCounterId, logFileName, correlationId);

                    lastReceivedCorrelationId = correlationId;
                }
                break;
            }

            case ON_CONNECTION_READY:
            {
                connectionReady.wrap(buffer, index);

                final int sessionId = connectionReady.sessionId();
                final int streamId = connectionReady.streamId();
                final long joiningPosition = connectionReady.joiningPosition();
                final String logFileName = connectionReady.logFileName();
                final long correlationId = connectionReady.correlationId();

                listener.onNewConnection(
                    streamId, sessionId, joiningPosition, logFileName, connectionReady, correlationId);
                break;
            }

            case ON_OPERATION_SUCCESS:
            {
                correlatedMessage.wrap(buffer, index);

                final long correlationId = correlatedMessage.correlationId();
                if (correlationId == activeCorrelationId)
                {
                    lastReceivedCorrelationId = correlationId;
                }
                break;
            }

            case ON_INACTIVE_CONNECTION:
            {
                connectionMessage.wrap(buffer, index);

                listener.onInactiveConnection(
                    connectionMessage.streamId(),
                    connectionMessage.sessionId(),
                    connectionMessage.position(),
                    connectionMessage.correlationId());
                break;
            }

            case ON_ERROR:
            {
                errorHeader.wrap(buffer, index);

                correlatedMessage.wrap(buffer, errorHeader.offendingHeaderOffset());

                final long correlationId = correlatedMessage.correlationId();

                if (correlationId == activeCorrelationId)
                {
                    listener.onError(errorHeader.errorCode(), errorHeader.errorMessage(), correlatedMessage.correlationId());

                    lastReceivedCorrelationId = correlationId;
                }
                break;
            }

            default:
                break;
        }
    }
}
