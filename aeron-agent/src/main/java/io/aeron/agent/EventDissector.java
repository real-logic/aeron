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
package io.aeron.agent;

import io.aeron.command.*;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.protocol.*;
import org.agrona.MutableDirectBuffer;

import java.net.InetAddress;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Dissect encoded log events. The event consumer of the log should be single threaded.
 */
public class EventDissector
{
    private static final DataHeaderFlyweight DATA_HEADER = new DataHeaderFlyweight();
    private static final StatusMessageFlyweight SM_HEADER = new StatusMessageFlyweight();
    private static final NakFlyweight NAK_HEADER = new NakFlyweight();
    private static final SetupFlyweight SETUP_HEADER = new SetupFlyweight();
    private static final RttMeasurementFlyweight RTT_MEASUREMENT = new RttMeasurementFlyweight();
    private static final PublicationMessageFlyweight PUB_MSG = new PublicationMessageFlyweight();
    private static final SubscriptionMessageFlyweight SUB_MSG = new SubscriptionMessageFlyweight();
    private static final PublicationBuffersReadyFlyweight PUB_READY = new PublicationBuffersReadyFlyweight();
    private static final ImageBuffersReadyFlyweight IMAGE_READY = new ImageBuffersReadyFlyweight();
    private static final CorrelatedMessageFlyweight CORRELATED_MSG = new CorrelatedMessageFlyweight();
    private static final ImageMessageFlyweight IMAGE_MSG = new ImageMessageFlyweight();
    private static final RemoveMessageFlyweight REMOVE_MSG = new RemoveMessageFlyweight();
    private static final DestinationMessageFlyweight DESTINATION_MSG = new DestinationMessageFlyweight();
    private static final ErrorResponseFlyweight ERROR_MSG = new ErrorResponseFlyweight();
    private static final CounterMessageFlyweight COUNTER_MSG = new CounterMessageFlyweight();
    private static final CounterUpdateFlyweight COUNTER_UPDATE = new CounterUpdateFlyweight();
    private static final OperationSucceededFlyweight OPERATION_SUCCEEDED = new OperationSucceededFlyweight();
    private static final SubscriptionReadyFlyweight SUBSCRIPTION_READY = new SubscriptionReadyFlyweight();

    public static String dissectAsFrame(final EventCode code, final MutableDirectBuffer buffer, final int offset)
    {
        final StringBuilder builder = new StringBuilder();
        int relativeOffset = dissectLogHeader(code, buffer, offset, builder);

        builder.append(": ");

        relativeOffset += dissectSocketAddress(buffer, offset + relativeOffset, builder);

        builder.append(" ");

        final int frameOffset = offset + relativeOffset;
        switch (frameType(buffer, frameOffset))
        {
            case HeaderFlyweight.HDR_TYPE_PAD:
            case HeaderFlyweight.HDR_TYPE_DATA:
                final DataHeaderFlyweight dataFrame = DATA_HEADER;
                dataFrame.wrap(buffer, frameOffset, buffer.capacity() - frameOffset);
                builder.append(dissect(dataFrame));
                break;

            case HeaderFlyweight.HDR_TYPE_SM:
                final StatusMessageFlyweight smFrame = SM_HEADER;
                smFrame.wrap(buffer, frameOffset, buffer.capacity() - frameOffset);
                builder.append(dissect(smFrame));
                break;

            case HeaderFlyweight.HDR_TYPE_NAK:
                final NakFlyweight nakFrame = NAK_HEADER;
                nakFrame.wrap(buffer, frameOffset, buffer.capacity() - frameOffset);
                builder.append(dissect(nakFrame));
                break;

            case HeaderFlyweight.HDR_TYPE_SETUP:
                final SetupFlyweight setupFrame = SETUP_HEADER;
                setupFrame.wrap(buffer, frameOffset, buffer.capacity() - frameOffset);
                builder.append(dissect(setupFrame));
                break;

            case HeaderFlyweight.HDR_TYPE_RTTM:
                final RttMeasurementFlyweight rttMeasurementFlyweight = RTT_MEASUREMENT;
                rttMeasurementFlyweight.wrap(buffer, frameOffset, buffer.capacity() - frameOffset);
                builder.append(dissect(rttMeasurementFlyweight));
                break;

            default:
                builder.append("FRAME_UNKNOWN");
                break;
        }

        return builder.toString();
    }

    @SuppressWarnings("MethodLength")
    public static String dissectAsCommand(final EventCode code, final MutableDirectBuffer buffer, final int offset)
    {
        final StringBuilder builder = new StringBuilder();
        final int relativeOffset = dissectLogHeader(code, buffer, offset, builder);

        builder.append(": ");

        switch (code)
        {
            case CMD_IN_ADD_PUBLICATION:
            case CMD_IN_ADD_EXCLUSIVE_PUBLICATION:
                final PublicationMessageFlyweight pubCommand = PUB_MSG;
                pubCommand.wrap(buffer, offset + relativeOffset);
                builder.append(dissect(pubCommand));
                break;

            case CMD_IN_ADD_SUBSCRIPTION:
                final SubscriptionMessageFlyweight subCommand = SUB_MSG;
                subCommand.wrap(buffer, offset + relativeOffset);
                builder.append(dissect(subCommand));
                break;

            case CMD_IN_REMOVE_PUBLICATION:
            case CMD_IN_REMOVE_SUBSCRIPTION:
            case CMD_IN_REMOVE_COUNTER:
                final RemoveMessageFlyweight removeCmd = REMOVE_MSG;
                removeCmd.wrap(buffer, offset + relativeOffset);
                builder.append(dissect(removeCmd));
                break;

            case CMD_OUT_PUBLICATION_READY:
            case CMD_OUT_EXCLUSIVE_PUBLICATION_READY:
                final PublicationBuffersReadyFlyweight publicationReadyEvent = PUB_READY;
                publicationReadyEvent.wrap(buffer, offset + relativeOffset);
                builder.append(dissect(publicationReadyEvent));
                break;

            case CMD_OUT_AVAILABLE_IMAGE:
                final ImageBuffersReadyFlyweight imageAvailableEvent = IMAGE_READY;
                imageAvailableEvent.wrap(buffer, offset + relativeOffset);
                builder.append(dissect(imageAvailableEvent));
                break;

            case CMD_OUT_ON_OPERATION_SUCCESS:
                final OperationSucceededFlyweight operationSucceeded = OPERATION_SUCCEEDED;
                operationSucceeded.wrap(buffer, offset + relativeOffset);
                builder.append(dissect(operationSucceeded));
                break;

            case CMD_IN_KEEPALIVE_CLIENT:
            case CMD_IN_CLIENT_CLOSE:
                final CorrelatedMessageFlyweight correlatedEvent = CORRELATED_MSG;
                correlatedEvent.wrap(buffer, offset + relativeOffset);
                builder.append(dissect(correlatedEvent));
                break;

            case CMD_OUT_ON_UNAVAILABLE_IMAGE:
                final ImageMessageFlyweight imageUnavailableEvent = IMAGE_MSG;
                imageUnavailableEvent.wrap(buffer, offset + relativeOffset);
                builder.append(dissect(imageUnavailableEvent));
                break;

            case CMD_IN_ADD_DESTINATION:
            case CMD_IN_REMOVE_DESTINATION:
            case CMD_IN_ADD_RCV_DESTINATION:
            case CMD_IN_REMOVE_RCV_DESTINATION:
                final DestinationMessageFlyweight destinationMessageFlyweight = DESTINATION_MSG;
                destinationMessageFlyweight.wrap(buffer, offset + relativeOffset);
                builder.append(dissect(destinationMessageFlyweight));
                break;

            case CMD_OUT_ERROR:
                final ErrorResponseFlyweight errorResponseFlyweight = ERROR_MSG;
                errorResponseFlyweight.wrap(buffer, offset + relativeOffset);
                builder.append(dissect(errorResponseFlyweight));
                break;

            case CMD_IN_ADD_COUNTER:
                final CounterMessageFlyweight counterMessage = COUNTER_MSG;
                counterMessage.wrap(buffer, offset + relativeOffset);
                builder.append(dissect(counterMessage));
                break;

            case CMD_OUT_SUBSCRIPTION_READY:
                final SubscriptionReadyFlyweight subscriptionReady = SUBSCRIPTION_READY;
                subscriptionReady.wrap(buffer, offset + relativeOffset);
                builder.append(dissect(subscriptionReady));
                break;

            case CMD_OUT_COUNTER_READY:
            case CMD_OUT_ON_UNAVAILABLE_COUNTER:
                final CounterUpdateFlyweight counterUpdate = COUNTER_UPDATE;
                counterUpdate.wrap(buffer, offset + relativeOffset);
                builder.append(dissect(counterUpdate));
                break;

            default:
                builder.append("COMMAND_UNKNOWN");
                break;
        }

        return builder.toString();
    }

    public static String dissectAsInvocation(
        final EventCode code, final MutableDirectBuffer buffer, final int initialOffset)
    {
        final StringBuilder builder = new StringBuilder();
        final int relativeOffset = dissectLogHeader(code, buffer, initialOffset, builder);
        builder.append(": ");

        readStackTraceElement(buffer, initialOffset + relativeOffset, builder);

        return builder.toString();
    }

    public static String dissectAsString(final EventCode code, final MutableDirectBuffer buffer, final int offset)
    {
        final StringBuilder builder = new StringBuilder();
        final int relativeOffset = dissectLogHeader(code, buffer, offset, builder);
        builder.append(": ");
        builder.append(buffer.getStringUtf8(offset + relativeOffset, LITTLE_ENDIAN));

        return builder.toString();
    }

    private static void readStackTraceElement(
        final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        int i = offset;

        final int lineNumber = buffer.getInt(i, LITTLE_ENDIAN);
        i += SIZE_OF_INT;

        int length = buffer.getInt(i);
        final String className = buffer.getStringUtf8(i, length);
        i += SIZE_OF_INT + length;

        length = buffer.getInt(i);
        final String methodName = buffer.getStringUtf8(i, length);
        i += SIZE_OF_INT + length;

        length = buffer.getInt(i);
        final String fileName = buffer.getStringUtf8(i, length);

        builder.append(String.format("%s.%s %s:%d", className, methodName, fileName, lineNumber));
    }

    private static int dissectLogHeader(
        final EventCode code, final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        int relativeOffset = 0;

        final int captureLength = buffer.getInt(offset + relativeOffset, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        final int bufferLength = buffer.getInt(offset + relativeOffset, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        final long timestamp = buffer.getLong(offset + relativeOffset, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_LONG;

        builder.append(String.format(
            "[%1$f] %2$s [%3$d/%4$d]",
            ((double)timestamp) / 1_000_000_000.0, code.name(), captureLength, bufferLength));

        return relativeOffset;
    }

    private static int dissectSocketAddress(
        final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        int relativeOffset = 0;

        final int port = buffer.getInt(offset + relativeOffset, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        final byte[] addressBuffer = new byte[buffer.getInt(offset + relativeOffset)];
        relativeOffset += SIZE_OF_INT;

        buffer.getBytes(offset + relativeOffset, addressBuffer);
        relativeOffset += addressBuffer.length;

        try
        {
            builder.append(String.format("%s.%d", InetAddress.getByAddress(addressBuffer).getHostAddress(), port));
        }
        catch (final Exception ex)
        {
            ex.printStackTrace();
        }

        return relativeOffset;
    }

    private static String dissect(final DataHeaderFlyweight msg)
    {
        return String.format(
            "%s 0x%x len %d %d:%d:%d @%x",
            msg.headerType() == HeaderFlyweight.HDR_TYPE_PAD ? "PAD" : "DATA",
            msg.flags(),
            msg.frameLength(),
            msg.sessionId(),
            msg.streamId(),
            msg.termId(),
            msg.termOffset());
    }

    private static String dissect(final StatusMessageFlyweight msg)
    {
        return String.format(
            "SM 0x%x len %d %d:%d:%d @%x %d %d",
            msg.flags(),
            msg.frameLength(),
            msg.sessionId(),
            msg.streamId(),
            msg.consumptionTermId(),
            msg.consumptionTermOffset(),
            msg.receiverWindowLength(),
            msg.receiverId());
    }

    private static String dissect(final NakFlyweight msg)
    {
        return String.format(
            "NAK 0x%x len %d %d:%d:%d @%x %d",
            msg.flags(),
            msg.frameLength(),
            msg.sessionId(),
            msg.streamId(),
            msg.termId(),
            msg.termOffset(),
            msg.length());
    }

    private static String dissect(final SetupFlyweight msg)
    {
        return String.format(
            "SETUP 0x%x len %d %d:%d:%d %d @%x %d MTU %d TTL %d",
            msg.flags(),
            msg.frameLength(),
            msg.sessionId(),
            msg.streamId(),
            msg.activeTermId(),
            msg.initialTermId(),
            msg.termOffset(),
            msg.termLength(),
            msg.mtuLength(),
            msg.ttl());
    }

    private static String dissect(final RttMeasurementFlyweight msg)
    {
        return String.format(
            "RTT 0x%x len %d %d:%d %d %d %d",
            msg.flags(),
            msg.frameLength(),
            msg.sessionId(),
            msg.streamId(),
            msg.echoTimestampNs(),
            msg.receptionDelta(),
            msg.receiverId());
    }

    private static String dissect(final PublicationMessageFlyweight msg)
    {
        return String.format(
            "%2$s %1$d [%4$d:%3$d]",
            msg.streamId(),
            msg.channel(),
            msg.correlationId(),
            msg.clientId());
    }

    private static String dissect(final SubscriptionMessageFlyweight msg)
    {
        return String.format(
            "%s %d [%d][%d:%d]",
            msg.channel(),
            msg.streamId(),
            msg.registrationCorrelationId(),
            msg.clientId(),
            msg.correlationId());
    }

    private static String dissect(final PublicationBuffersReadyFlyweight msg)
    {
        return String.format(
            "%d:%d %d %d [%d %d]%n    %s",
            msg.sessionId(),
            msg.streamId(),
            msg.publicationLimitCounterId(),
            msg.channelStatusCounterId(),
            msg.correlationId(),
            msg.registrationId(),
            msg.logFileName());
    }

    private static String dissect(final ImageBuffersReadyFlyweight msg)
    {
        return String.format(
            "%d:%d [%d:%d] \"%s\" [%d]%n    %s",
            msg.sessionId(),
            msg.streamId(),
            msg.subscriberPositionId(),
            msg.subscriptionRegistrationId(),
            msg.sourceIdentity(),
            msg.correlationId(),
            msg.logFileName());
    }

    private static String dissect(final CorrelatedMessageFlyweight msg)
    {
        return String.format(
            "[%d:%d]",
            msg.clientId(),
            msg.correlationId());
    }

    private static String dissect(final ImageMessageFlyweight msg)
    {
        return String.format(
            "%s %d [%d %d]",
            msg.channel(),
            msg.streamId(),
            msg.correlationId(),
            msg.subscriptionRegistrationId());
    }

    private static String dissect(final RemoveMessageFlyweight msg)
    {
        return String.format(
            "%d [%d:%d]",
            msg.registrationId(),
            msg.clientId(),
            msg.correlationId());
    }

    private static String dissect(final DestinationMessageFlyweight msg)
    {
        return String.format(
            "%s %d [%d:%d]",
            msg.channel(),
            msg.registrationCorrelationId(),
            msg.clientId(),
            msg.correlationId());
    }

    private static String dissect(final ErrorResponseFlyweight msg)
    {
        return String.format(
            "%d %s %s",
            msg.offendingCommandCorrelationId(),
            msg.errorCode(),
            msg.errorMessage());
    }

    private static String dissect(final CounterMessageFlyweight msg)
    {
        return String.format(
            "%d [%d %d][%d %d][%d:%d]",
            msg.typeId(),
            msg.keyBufferOffset(),
            msg.keyBufferLength(),
            msg.labelBufferOffset(),
            msg.labelBufferLength(),
            msg.clientId(),
            msg.correlationId());
    }

    private static String dissect(final CounterUpdateFlyweight msg)
    {
        return String.format(
            "%d %d",
            msg.correlationId(),
            msg.counterId());
    }

    private static String dissect(final OperationSucceededFlyweight msg)
    {
        return String.format("%d", msg.correlationId());
    }

    private static String dissect(final SubscriptionReadyFlyweight msg)
    {
        return String.format(
            "%d %d",
            msg.correlationId(),
            msg.channelStatusCounterId());
    }

    public static int frameType(final MutableDirectBuffer buffer, final int termOffset)
    {
        return buffer.getShort(FrameDescriptor.typeOffset(termOffset), LITTLE_ENDIAN) & 0xFFFF;
    }
}
