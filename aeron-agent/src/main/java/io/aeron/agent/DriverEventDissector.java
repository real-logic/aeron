/*
 * Copyright 2014-2019 Real Logic Ltd.
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

import static io.aeron.protocol.HeaderFlyweight.flagsToChars;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Dissect encoded log events and append them to a provided {@link StringBuilder}.
 * <p>
 * <b>Note:</b>The event consumer of the log should be single threaded.
 */
public class DriverEventDissector
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
    private static final ClientTimeoutFlyweight CLIENT_TIMEOUT = new ClientTimeoutFlyweight();
    private static final TerminateDriverFlyweight TERMINATE_DRIVER = new TerminateDriverFlyweight();

    public static void dissectAsFrame(
        final DriverEventCode code, final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
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
                dissect(dataFrame, builder);
                break;

            case HeaderFlyweight.HDR_TYPE_SM:
                final StatusMessageFlyweight smFrame = SM_HEADER;
                smFrame.wrap(buffer, frameOffset, buffer.capacity() - frameOffset);
                dissect(smFrame, builder);
                break;

            case HeaderFlyweight.HDR_TYPE_NAK:
                final NakFlyweight nakFrame = NAK_HEADER;
                nakFrame.wrap(buffer, frameOffset, buffer.capacity() - frameOffset);
                dissect(nakFrame, builder);
                break;

            case HeaderFlyweight.HDR_TYPE_SETUP:
                final SetupFlyweight setupFrame = SETUP_HEADER;
                setupFrame.wrap(buffer, frameOffset, buffer.capacity() - frameOffset);
                dissect(setupFrame, builder);
                break;

            case HeaderFlyweight.HDR_TYPE_RTTM:
                final RttMeasurementFlyweight rttMeasurementFlyweight = RTT_MEASUREMENT;
                rttMeasurementFlyweight.wrap(buffer, frameOffset, buffer.capacity() - frameOffset);
                dissect(rttMeasurementFlyweight, builder);
                break;

            default:
                builder.append("FRAME_UNKNOWN: ").append(frameType(buffer, frameOffset));
                break;
        }
    }

    @SuppressWarnings("MethodLength")
    public static void dissectAsCommand(
        final DriverEventCode code, final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        final int relativeOffset = dissectLogHeader(code, buffer, offset, builder);

        builder.append(": ");

        switch (code)
        {
            case CMD_IN_ADD_PUBLICATION:
            case CMD_IN_ADD_EXCLUSIVE_PUBLICATION:
                final PublicationMessageFlyweight pubCommand = PUB_MSG;
                pubCommand.wrap(buffer, offset + relativeOffset);
                dissect(pubCommand, builder);
                break;

            case CMD_IN_ADD_SUBSCRIPTION:
                final SubscriptionMessageFlyweight subCommand = SUB_MSG;
                subCommand.wrap(buffer, offset + relativeOffset);
                dissect(subCommand, builder);
                break;

            case CMD_IN_REMOVE_PUBLICATION:
            case CMD_IN_REMOVE_SUBSCRIPTION:
            case CMD_IN_REMOVE_COUNTER:
                final RemoveMessageFlyweight removeCmd = REMOVE_MSG;
                removeCmd.wrap(buffer, offset + relativeOffset);
                dissect(removeCmd, builder);
                break;

            case CMD_OUT_PUBLICATION_READY:
            case CMD_OUT_EXCLUSIVE_PUBLICATION_READY:
                final PublicationBuffersReadyFlyweight publicationReadyEvent = PUB_READY;
                publicationReadyEvent.wrap(buffer, offset + relativeOffset);
                dissect(publicationReadyEvent, builder);
                break;

            case CMD_OUT_AVAILABLE_IMAGE:
                final ImageBuffersReadyFlyweight imageAvailableEvent = IMAGE_READY;
                imageAvailableEvent.wrap(buffer, offset + relativeOffset);
                dissect(imageAvailableEvent, builder);
                break;

            case CMD_OUT_ON_OPERATION_SUCCESS:
                final OperationSucceededFlyweight operationSucceeded = OPERATION_SUCCEEDED;
                operationSucceeded.wrap(buffer, offset + relativeOffset);
                dissect(operationSucceeded, builder);
                break;

            case CMD_IN_KEEPALIVE_CLIENT:
            case CMD_IN_CLIENT_CLOSE:
                final CorrelatedMessageFlyweight correlatedEvent = CORRELATED_MSG;
                correlatedEvent.wrap(buffer, offset + relativeOffset);
                dissect(correlatedEvent, builder);
                break;

            case CMD_OUT_ON_UNAVAILABLE_IMAGE:
                final ImageMessageFlyweight imageUnavailableEvent = IMAGE_MSG;
                imageUnavailableEvent.wrap(buffer, offset + relativeOffset);
                dissect(imageUnavailableEvent, builder);
                break;

            case CMD_IN_ADD_DESTINATION:
            case CMD_IN_REMOVE_DESTINATION:
            case CMD_IN_ADD_RCV_DESTINATION:
            case CMD_IN_REMOVE_RCV_DESTINATION:
                final DestinationMessageFlyweight destinationMessageFlyweight = DESTINATION_MSG;
                destinationMessageFlyweight.wrap(buffer, offset + relativeOffset);
                dissect(destinationMessageFlyweight, builder);
                break;

            case CMD_OUT_ERROR:
                final ErrorResponseFlyweight errorResponseFlyweight = ERROR_MSG;
                errorResponseFlyweight.wrap(buffer, offset + relativeOffset);
                dissect(errorResponseFlyweight, builder);
                break;

            case CMD_IN_ADD_COUNTER:
                final CounterMessageFlyweight counterMessage = COUNTER_MSG;
                counterMessage.wrap(buffer, offset + relativeOffset);
                dissect(counterMessage, builder);
                break;

            case CMD_OUT_SUBSCRIPTION_READY:
                final SubscriptionReadyFlyweight subscriptionReady = SUBSCRIPTION_READY;
                subscriptionReady.wrap(buffer, offset + relativeOffset);
                dissect(subscriptionReady, builder);
                break;

            case CMD_OUT_COUNTER_READY:
            case CMD_OUT_ON_UNAVAILABLE_COUNTER:
                final CounterUpdateFlyweight counterUpdate = COUNTER_UPDATE;
                counterUpdate.wrap(buffer, offset + relativeOffset);
                dissect(counterUpdate, builder);
                break;

            case CMD_OUT_ON_CLIENT_TIMEOUT:
                final ClientTimeoutFlyweight clientTimeout = CLIENT_TIMEOUT;
                clientTimeout.wrap(buffer, offset + relativeOffset);
                dissect(clientTimeout, builder);
                break;

            case CMD_IN_TERMINATE_DRIVER:
                final TerminateDriverFlyweight terminateDriver = TERMINATE_DRIVER;
                terminateDriver.wrap(buffer, offset + relativeOffset);
                dissect(terminateDriver, builder);
                break;

            default:
                builder.append("COMMAND_UNKNOWN: ").append(code);
                break;
        }
    }

    public static void dissectAsInvocation(
        final DriverEventCode code,
        final MutableDirectBuffer buffer,
        final int initialOffset,
        final StringBuilder builder)
    {
        final int relativeOffset = dissectLogHeader(code, buffer, initialOffset, builder);
        builder.append(": ");

        readStackTraceElement(buffer, initialOffset + relativeOffset, builder);
    }

    public static void dissectAsString(
        final DriverEventCode code, final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        final int relativeOffset = dissectLogHeader(code, buffer, offset, builder);
        builder.append(": ").append(buffer.getStringUtf8(offset + relativeOffset, LITTLE_ENDIAN));
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

        builder
            .append(className)
            .append('.')
            .append(methodName)
            .append(' ')
            .append(fileName)
            .append(':')
            .append(lineNumber);
    }

    private static int dissectLogHeader(
        final DriverEventCode code, final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        int relativeOffset = 0;

        final int captureLength = buffer.getInt(offset + relativeOffset, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        final int bufferLength = buffer.getInt(offset + relativeOffset, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        final long timestampNs = buffer.getLong(offset + relativeOffset, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_LONG;

        builder
            .append('[')
            .append(((double)timestampNs) / 1_000_000_000.0)
            .append("] ")
            .append(code.name())
            .append(" [")
            .append(captureLength)
            .append('/')
            .append(bufferLength)
            .append(']');

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
            builder.append(InetAddress.getByAddress(addressBuffer).getHostAddress()).append('.').append(port);
        }
        catch (final Exception ex)
        {
            ex.printStackTrace();
        }

        return relativeOffset;
    }

    private static void dissect(final DataHeaderFlyweight msg, final StringBuilder builder)
    {
        builder
            .append(msg.headerType() == HeaderFlyweight.HDR_TYPE_PAD ? "PAD" : "DATA")
            .append(' ')
            .append(flagsToChars(msg.flags()))
            .append(" len ")
            .append(msg.frameLength())
            .append(' ')
            .append(msg.sessionId())
            .append(':')
            .append(msg.streamId())
            .append(':')
            .append(msg.termId())
            .append(" @")
            .append(msg.termOffset());
    }

    private static void dissect(final StatusMessageFlyweight msg, final StringBuilder builder)
    {
        builder
            .append("SM ")
            .append(flagsToChars(msg.flags()))
            .append(" len ")
            .append(msg.frameLength())
            .append(' ')
            .append(msg.sessionId())
            .append(':')
            .append(msg.streamId())
            .append(':')
            .append(msg.consumptionTermId())
            .append(" @")
            .append(msg.consumptionTermOffset())
            .append(' ')
            .append(msg.receiverWindowLength())
            .append(' ')
            .append(msg.receiverId());
    }

    private static void dissect(final NakFlyweight msg, final StringBuilder builder)
    {
        builder
            .append("NAK ")
            .append(flagsToChars(msg.flags()))
            .append(" len ")
            .append(msg.frameLength())
            .append(' ')
            .append(msg.sessionId())
            .append(':')
            .append(msg.streamId())
            .append(':')
            .append(msg.termId())
            .append(" @")
            .append(msg.termOffset())
            .append(' ')
            .append(msg.length());
    }

    private static void dissect(final SetupFlyweight msg, final StringBuilder builder)
    {
        builder
            .append("SETUP ")
            .append(flagsToChars(msg.flags()))
            .append(" len ")
            .append(msg.frameLength())
            .append(' ')
            .append(msg.sessionId())
            .append(':')
            .append(msg.streamId())
            .append(':')
            .append(msg.activeTermId())
            .append(' ')
            .append(msg.initialTermId())
            .append(" @")
            .append(msg.termOffset())
            .append(' ')
            .append(msg.termLength())
            .append(" MTU ")
            .append(msg.mtuLength())
            .append(" TTL ")
            .append(msg.ttl());
    }

    private static void dissect(final RttMeasurementFlyweight msg, final StringBuilder builder)
    {
        builder
            .append("RTT ")
            .append(flagsToChars(msg.flags()))
            .append(" len ")
            .append(msg.frameLength())
            .append(' ')
            .append(msg.sessionId())
            .append(':')
            .append(msg.streamId())
            .append(' ')
            .append(msg.echoTimestampNs())
            .append(' ')
            .append(msg.receptionDelta())
            .append(' ')
            .append(msg.receiverId());
    }

    private static void dissect(final PublicationMessageFlyweight msg, final StringBuilder builder)
    {
        builder
            .append(msg.channel())
            .append(' ')
            .append(msg.streamId())
            .append(" [")
            .append(msg.clientId())
            .append(':')
            .append(msg.correlationId())
            .append(']');
    }

    private static void dissect(final SubscriptionMessageFlyweight msg, final StringBuilder builder)
    {
        builder
            .append(msg.channel())
            .append(' ')
            .append(msg.streamId())
            .append(" [")
            .append(msg.registrationCorrelationId())
            .append("][")
            .append(msg.clientId())
            .append(':')
            .append(msg.correlationId())
            .append(']');
    }

    private static void dissect(final PublicationBuffersReadyFlyweight msg, final StringBuilder builder)
    {
        builder
            .append(msg.sessionId())
            .append(':')
            .append(msg.streamId())
            .append(' ')
            .append(msg.publicationLimitCounterId())
            .append(' ')
            .append(msg.channelStatusCounterId())
            .append(" [")
            .append(msg.correlationId())
            .append(' ')
            .append(msg.registrationId())
            .append("] ")
            .append(msg.logFileName());
    }

    private static void dissect(final ImageBuffersReadyFlyweight msg, final StringBuilder builder)
    {
        builder
            .append(msg.sessionId())
            .append(':')
            .append(msg.streamId())
            .append(" [")
            .append(msg.subscriberPositionId())
            .append(':')
            .append(msg.subscriptionRegistrationId())
            .append("] \"")
            .append(msg.sourceIdentity())
            .append("\" [")
            .append(msg.correlationId())
            .append("] ")
            .append(msg.logFileName());
    }

    private static void dissect(final CorrelatedMessageFlyweight msg, final StringBuilder builder)
    {
        builder
            .append('[')
            .append(msg.clientId())
            .append(':')
            .append(msg.correlationId())
            .append(']');
    }

    private static void dissect(final ImageMessageFlyweight msg, final StringBuilder builder)
    {
        builder
            .append(msg.channel())
            .append(' ')
            .append(msg.streamId())
            .append(" [")
            .append(msg.correlationId())
            .append(' ')
            .append(msg.subscriptionRegistrationId())
            .append(']');
    }

    private static void dissect(final RemoveMessageFlyweight msg, final StringBuilder builder)
    {
        builder
            .append(msg.registrationId())
            .append(" [")
            .append(msg.clientId())
            .append(':')
            .append(msg.correlationId())
            .append(']');
    }

    private static void dissect(final DestinationMessageFlyweight msg, final StringBuilder builder)
    {
        builder
            .append(msg.channel())
            .append(' ')
            .append(msg.registrationCorrelationId())
            .append(" [")
            .append(msg.clientId())
            .append(':')
            .append(msg.correlationId())
            .append(']');
    }

    private static void dissect(final ErrorResponseFlyweight msg, final StringBuilder builder)
    {
        builder
            .append(msg.offendingCommandCorrelationId())
            .append(' ')
            .append(msg.errorCode().toString())
            .append(' ')
            .append(msg.errorMessage());
    }

    private static void dissect(final CounterMessageFlyweight msg, final StringBuilder builder)
    {
        builder
            .append(msg.typeId())
            .append(" [")
            .append(msg.keyBufferOffset()).append(' ').append(msg.keyBufferLength())
            .append("][")
            .append(msg.labelBufferOffset()).append(' ').append(msg.labelBufferLength())
            .append("][")
            .append(msg.clientId())
            .append(':')
            .append(msg.correlationId())
            .append(']');
    }

    private static void dissect(final CounterUpdateFlyweight msg, final StringBuilder builder)
    {
        builder.append(msg.correlationId()).append(' ').append(msg.counterId());
    }

    private static void dissect(final OperationSucceededFlyweight msg, final StringBuilder builder)
    {
        builder.append(msg.correlationId());
    }

    private static void dissect(final SubscriptionReadyFlyweight msg, final StringBuilder builder)
    {
        builder.append(msg.correlationId()).append(' ').append(msg.channelStatusCounterId());
    }

    private static void dissect(final ClientTimeoutFlyweight msg, final StringBuilder builder)
    {
        builder.append(msg.clientId());
    }

    private static void dissect(final TerminateDriverFlyweight msg, final StringBuilder builder)
    {
        builder.append(msg.clientId()).append(' ').append(msg.tokenBufferLength());
    }

    public static int frameType(final MutableDirectBuffer buffer, final int termOffset)
    {
        return buffer.getShort(FrameDescriptor.typeOffset(termOffset), LITTLE_ENDIAN) & 0xFFFF;
    }
}
