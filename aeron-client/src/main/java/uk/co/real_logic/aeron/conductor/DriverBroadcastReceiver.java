package uk.co.real_logic.aeron.conductor;

import uk.co.real_logic.aeron.common.ErrorCode;
import uk.co.real_logic.aeron.common.command.*;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.broadcast.CopyBroadcastReceiver;
import uk.co.real_logic.aeron.common.event.EventLogger;
import uk.co.real_logic.aeron.common.protocol.ErrorFlyweight;

import static uk.co.real_logic.aeron.common.command.ControlProtocolEvents.*;

/**
 * Analogue of {@see MediaDriverProxy} on the poll side
 */
public class DriverBroadcastReceiver
{
    private final CopyBroadcastReceiver broadcastReceiver;

    private final PublicationMessageFlyweight publicationMessage = new PublicationMessageFlyweight();
    private final ErrorFlyweight errorHeader = new ErrorFlyweight();
    private final LogBuffersMessageFlyweight logBuffersMessage = new LogBuffersMessageFlyweight();
    private final CorrelatedMessageFlyweight correlatedMessage = new CorrelatedMessageFlyweight();

    public DriverBroadcastReceiver(final CopyBroadcastReceiver broadcastReceiver)
    {
        this.broadcastReceiver = broadcastReceiver;
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

                            final String destination = logBuffersMessage.destination();

                            final long sessionId = logBuffersMessage.sessionId();
                            final long channelId = logBuffersMessage.channelId();
                            final long termId = logBuffersMessage.termId();
                            final int positionIndicatorOffset = logBuffersMessage.positionCounterOffset();

                            if (msgTypeId == ON_NEW_PUBLICATION)
                            {
                                if (logBuffersMessage.correlationId() != activeCorrelationId)
                                {
                                    break;
                                }

                                listener.onNewPublication(destination, sessionId, channelId, termId,
                                                          positionIndicatorOffset, logBuffersMessage);
                            }
                            else
                            {
                                listener.onNewConnectedSubscription(destination, sessionId, channelId, termId,
                                                                    logBuffersMessage);
                            }
                            break;

                        case ON_OPERATION_SUCCESS:
                            correlatedMessage.wrap(buffer, index);
                            if (correlatedMessage.correlationId() == activeCorrelationId)
                            {
                                listener.operationSucceeded();
                            }
                            break;

                        case ERROR_RESPONSE:
                            handleErrorResponse(buffer, index, listener, activeCorrelationId);
                            break;

                        default:
                            break;
                    }
                }
                catch (final Exception ex)
                {
                    EventLogger.logException(ex);
                }
            }
        );
    }

    private void handleErrorResponse(final AtomicBuffer buffer,
                                     final int index,
                                     final DriverListener listener,
                                     final long activeCorrelationId)
    {
        errorHeader.wrap(buffer, index);
        final ErrorCode errorCode = errorHeader.errorCode();

        switch (errorCode)
        {
            // Publication errors
            case PUBLICATION_CHANNEL_ALREADY_EXISTS:
            case GENERIC_ERROR_MESSAGE:
            case INVALID_DESTINATION:
            case PUBLICATION_CHANNEL_UNKNOWN:
                final long correlationId = correlationId(buffer, errorHeader.offendingHeaderOffset());
                if (correlationId == activeCorrelationId)
                {
                    listener.onError(errorCode, errorHeader.errorMessage());
                }
                break;

            default:
                // TODO
                break;
        }
    }

    private long correlationId(final AtomicBuffer buffer, final int offset)
    {
        publicationMessage.wrap(buffer, offset);

        return publicationMessage.correlationId();
    }
}
