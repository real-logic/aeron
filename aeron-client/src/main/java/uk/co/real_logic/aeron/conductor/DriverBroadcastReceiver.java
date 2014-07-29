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
package uk.co.real_logic.aeron.conductor;

import uk.co.real_logic.aeron.common.ErrorCode;
import uk.co.real_logic.aeron.common.command.CorrelatedMessageFlyweight;
import uk.co.real_logic.aeron.common.command.LogBuffersMessageFlyweight;
import uk.co.real_logic.aeron.common.command.PublicationMessageFlyweight;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.broadcast.CopyBroadcastReceiver;
import uk.co.real_logic.aeron.common.protocol.ErrorFlyweight;

import java.util.function.Consumer;

import static uk.co.real_logic.aeron.common.command.ControlProtocolEvents.*;

/**
 * Analogue of {@link DriverProxy} on the client side
 */
public class DriverBroadcastReceiver
{
    private final CopyBroadcastReceiver broadcastReceiver;
    private final Consumer<Exception> errorHandler;

    private final PublicationMessageFlyweight publicationMessage = new PublicationMessageFlyweight();
    private final ErrorFlyweight errorHeader = new ErrorFlyweight();
    private final LogBuffersMessageFlyweight logBuffersMessage = new LogBuffersMessageFlyweight();
    private final CorrelatedMessageFlyweight correlatedMessage = new CorrelatedMessageFlyweight();

    public DriverBroadcastReceiver(final CopyBroadcastReceiver broadcastReceiver, final Consumer<Exception> errorHandler)
    {
        this.broadcastReceiver = broadcastReceiver;
        this.errorHandler = errorHandler;
    }

    public int receive(final DriverListener listener, final long activeCorrelationId)
    {
        return broadcastReceiver.receive(
            (msgTypeId, buffer, index, length) ->
            {
                try
                {
                    switch (msgTypeId)
                    {
                        case ON_NEW_CONNECTED_SUBSCRIPTION:
                        case ON_NEW_PUBLICATION:
                            logBuffersMessage.wrap(buffer, index);

                            final String channel = logBuffersMessage.channel();
                            final int sessionId = logBuffersMessage.sessionId();
                            final int streamId = logBuffersMessage.streamId();
                            final int termId = logBuffersMessage.termId();
                            final int positionCounterId = logBuffersMessage.positionCounterId();

                            if (msgTypeId == ON_NEW_PUBLICATION && logBuffersMessage.correlationId() == activeCorrelationId)
                            {
                                listener.onNewPublication(
                                    channel, sessionId, streamId, termId, positionCounterId, logBuffersMessage);
                            }
                            else
                            {
                                listener.onNewConnection(channel, sessionId, streamId, termId, logBuffersMessage);
                            }
                            break;

                        case ON_OPERATION_SUCCESS:
                            correlatedMessage.wrap(buffer, index);
                            if (correlatedMessage.correlationId() == activeCorrelationId)
                            {
                                listener.operationSucceeded();
                            }
                            break;

                        case ON_ERROR:
                            onError(buffer, index, listener, activeCorrelationId);
                            break;

                        default:
                            break;
                    }
                }
                catch (final Exception ex)
                {
                    errorHandler.accept(ex);
                }
            }
        );
    }

    private void onError(final AtomicBuffer buffer, final int index,
                         final DriverListener listener, final long activeCorrelationId)
    {
        errorHeader.wrap(buffer, index);
        final ErrorCode errorCode = errorHeader.errorCode();
        if (activeCorrelationId == correlationId(buffer, errorHeader.offendingHeaderOffset()))
        {
            listener.onError(errorCode, errorHeader.errorMessage());
        }
    }

    private long correlationId(final AtomicBuffer buffer, final int offset)
    {
        correlatedMessage.wrap(buffer, offset);

        return correlatedMessage.correlationId();
    }
}
