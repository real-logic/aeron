/*
 * Copyright 2014-2020 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
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

import java.text.SimpleDateFormat;
import java.util.Date;

import static java.lang.Integer.toHexString;
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
                DATA_HEADER.wrap(buffer, frameOffset, buffer.capacity() - frameOffset);
                dissectDataFrame(builder);
                break;

            case HeaderFlyweight.HDR_TYPE_SM:
                SM_HEADER.wrap(buffer, frameOffset, buffer.capacity() - frameOffset);
                dissectStatusFrame(builder);
                break;

            case HeaderFlyweight.HDR_TYPE_NAK:
                NAK_HEADER.wrap(buffer, frameOffset, buffer.capacity() - frameOffset);
                dissectNakFrame(builder);
                break;

            case HeaderFlyweight.HDR_TYPE_SETUP:
                SETUP_HEADER.wrap(buffer, frameOffset, buffer.capacity() - frameOffset);
                dissectSetupFrame(builder);
                break;

            case HeaderFlyweight.HDR_TYPE_RTTM:
                RTT_MEASUREMENT.wrap(buffer, frameOffset, buffer.capacity() - frameOffset);
                dissectRttFrame(builder);
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
                PUB_MSG.wrap(buffer, offset + relativeOffset);
                dissectPublication(builder);
                break;

            case CMD_IN_ADD_SUBSCRIPTION:
                SUB_MSG.wrap(buffer, offset + relativeOffset);
                dissectSubscription(builder);
                break;

            case CMD_IN_REMOVE_PUBLICATION:
            case CMD_IN_REMOVE_SUBSCRIPTION:
            case CMD_IN_REMOVE_COUNTER:
                REMOVE_MSG.wrap(buffer, offset + relativeOffset);
                dissectRemoveEvent(builder);
                break;

            case CMD_OUT_PUBLICATION_READY:
            case CMD_OUT_EXCLUSIVE_PUBLICATION_READY:
                PUB_READY.wrap(buffer, offset + relativeOffset);
                dissectPublicationReady(builder);
                break;

            case CMD_OUT_AVAILABLE_IMAGE:
                IMAGE_READY.wrap(buffer, offset + relativeOffset);
                dissectImageReady(builder);
                break;

            case CMD_OUT_ON_OPERATION_SUCCESS:
                OPERATION_SUCCEEDED.wrap(buffer, offset + relativeOffset);
                dissectOperationSuccess(builder);
                break;

            case CMD_IN_KEEPALIVE_CLIENT:
            case CMD_IN_CLIENT_CLOSE:
                CORRELATED_MSG.wrap(buffer, offset + relativeOffset);
                dissectCorrelationEvent(builder);
                break;

            case CMD_OUT_ON_UNAVAILABLE_IMAGE:
                IMAGE_MSG.wrap(buffer, offset + relativeOffset);
                dissectImage(builder);
                break;

            case CMD_IN_ADD_DESTINATION:
            case CMD_IN_REMOVE_DESTINATION:
            case CMD_IN_ADD_RCV_DESTINATION:
            case CMD_IN_REMOVE_RCV_DESTINATION:
                DESTINATION_MSG.wrap(buffer, offset + relativeOffset);
                dissectDestination(builder);
                break;

            case CMD_OUT_ERROR:
                ERROR_MSG.wrap(buffer, offset + relativeOffset);
                dissectError(builder);
                break;

            case CMD_IN_ADD_COUNTER:
                COUNTER_MSG.wrap(buffer, offset + relativeOffset);
                dissectCounter(builder);
                break;

            case CMD_OUT_SUBSCRIPTION_READY:
                SUBSCRIPTION_READY.wrap(buffer, offset + relativeOffset);
                dissectSubscriptionReady(builder);
                break;

            case CMD_OUT_COUNTER_READY:
            case CMD_OUT_ON_UNAVAILABLE_COUNTER:
                COUNTER_UPDATE.wrap(buffer, offset + relativeOffset);
                dissectCounterUpdate(builder);
                break;

            case CMD_OUT_ON_CLIENT_TIMEOUT:
                CLIENT_TIMEOUT.wrap(buffer, offset + relativeOffset);
                dissectClientTimeout(builder);
                break;

            case CMD_IN_TERMINATE_DRIVER:
                TERMINATE_DRIVER.wrap(buffer, offset + relativeOffset);
                dissectTerminateDriver(builder);
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

    public static void dissectLogStartMessage(
        final long timestampNs, final long timestampMs, final StringBuilder builder)
    {
        final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");

        builder
            .append('[')
            .append(((double)timestampNs) / 1_000_000_000.0)
            .append("] log started ")
            .append(format.format(new Date(timestampMs)));
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

        final int addressLength = buffer.getInt(offset + relativeOffset);
        relativeOffset += SIZE_OF_INT;

        if (4 == addressLength)
        {
            final int i = offset + relativeOffset;
            builder
                .append(buffer.getByte(i) & 0xFF)
                .append('.')
                .append(buffer.getByte(i + 1) & 0xFF)
                .append('.')
                .append(buffer.getByte(i + 2) & 0xFF)
                .append('.')
                .append(buffer.getByte(i + 3) & 0xFF)
                .append(':')
                .append(port);
        }
        else if (16 == addressLength)
        {
            final int i = offset + relativeOffset;
            builder
                .append(toHexString(((buffer.getByte(i) << 8) & 0xFF00) | buffer.getByte(i + 1) & 0xFF))
                .append(':')
                .append(toHexString(((buffer.getByte(i + 2) << 8) & 0xFF00) | buffer.getByte(i + 3) & 0xFF))
                .append(':')
                .append(toHexString(((buffer.getByte(i + 4) << 8) & 0xFF00) | buffer.getByte(i + 5) & 0xFF))
                .append(':')
                .append(toHexString(((buffer.getByte(i + 6) << 8) & 0xFF00) | buffer.getByte(i + 7) & 0xFF))
                .append(':')
                .append(toHexString(((buffer.getByte(i + 8) << 8) & 0xFF00) | buffer.getByte(i + 9) & 0xFF))
                .append(':')
                .append(toHexString(((buffer.getByte(i + 10) << 8) & 0xFF00) | buffer.getByte(i + 11) & 0xFF))
                .append(':')
                .append(toHexString(((buffer.getByte(i + 12) << 8) & 0xFF00) | buffer.getByte(i + 13) & 0xFF))
                .append(':')
                .append(toHexString(((buffer.getByte(i + 14) << 8) & 0xFF00) | buffer.getByte(i + 15) & 0xFF))
                .append(':')
                .append(port);
        }
        else
        {
            builder.append("unknown-address:").append(port);
        }

        relativeOffset += addressLength;

        return relativeOffset;
    }

    private static void dissectDataFrame(final StringBuilder builder)
    {
        builder
            .append(DATA_HEADER.headerType() == HeaderFlyweight.HDR_TYPE_PAD ? "PAD" : "DATA")
            .append(' ');

        HeaderFlyweight.appendFlagsAsChars(DATA_HEADER.flags(), builder);

        builder
            .append(" len ")
            .append(DATA_HEADER.frameLength())
            .append(' ')
            .append(DATA_HEADER.sessionId())
            .append(':')
            .append(DATA_HEADER.streamId())
            .append(':')
            .append(DATA_HEADER.termId())
            .append(" @")
            .append(DATA_HEADER.termOffset());
    }

    private static void dissectStatusFrame(final StringBuilder builder)
    {
        builder.append("SM ");

        HeaderFlyweight.appendFlagsAsChars(SM_HEADER.flags(), builder);

        builder
            .append(" len ")
            .append(SM_HEADER.frameLength())
            .append(' ')
            .append(SM_HEADER.sessionId())
            .append(':')
            .append(SM_HEADER.streamId())
            .append(':')
            .append(SM_HEADER.consumptionTermId())
            .append(" @")
            .append(SM_HEADER.consumptionTermOffset())
            .append(' ')
            .append(SM_HEADER.receiverWindowLength())
            .append(' ')
            .append(SM_HEADER.receiverId());
    }

    private static void dissectNakFrame(final StringBuilder builder)
    {
        builder.append("NAK ");

        HeaderFlyweight.appendFlagsAsChars(NAK_HEADER.flags(), builder);

        builder
            .append(" len ")
            .append(NAK_HEADER.frameLength())
            .append(' ')
            .append(NAK_HEADER.sessionId())
            .append(':')
            .append(NAK_HEADER.streamId())
            .append(':')
            .append(NAK_HEADER.termId())
            .append(" @")
            .append(NAK_HEADER.termOffset())
            .append(' ')
            .append(NAK_HEADER.length());
    }

    private static void dissectSetupFrame(final StringBuilder builder)
    {
        builder.append("SETUP ");

        HeaderFlyweight.appendFlagsAsChars(SETUP_HEADER.flags(), builder);

        builder
            .append(" len ")
            .append(SETUP_HEADER.frameLength())
            .append(' ')
            .append(SETUP_HEADER.sessionId())
            .append(':')
            .append(SETUP_HEADER.streamId())
            .append(':')
            .append(SETUP_HEADER.activeTermId())
            .append(' ')
            .append(SETUP_HEADER.initialTermId())
            .append(" @")
            .append(SETUP_HEADER.termOffset())
            .append(' ')
            .append(SETUP_HEADER.termLength())
            .append(" MTU ")
            .append(SETUP_HEADER.mtuLength())
            .append(" TTL ")
            .append(SETUP_HEADER.ttl());
    }

    private static void dissectRttFrame(final StringBuilder builder)
    {
        builder.append("RTT ");

        HeaderFlyweight.appendFlagsAsChars(RTT_MEASUREMENT.flags(), builder);

        builder
            .append(" len ")
            .append(RTT_MEASUREMENT.frameLength())
            .append(' ')
            .append(RTT_MEASUREMENT.sessionId())
            .append(':')
            .append(RTT_MEASUREMENT.streamId())
            .append(' ')
            .append(RTT_MEASUREMENT.echoTimestampNs())
            .append(' ')
            .append(RTT_MEASUREMENT.receptionDelta())
            .append(' ')
            .append(RTT_MEASUREMENT.receiverId());
    }

    private static void dissectPublication(final StringBuilder builder)
    {
        PUB_MSG.appendChannel(builder);

        builder
            .append(' ')
            .append(PUB_MSG.streamId())
            .append(" [")
            .append(PUB_MSG.clientId())
            .append(':')
            .append(PUB_MSG.correlationId())
            .append(']');
    }

    private static void dissectSubscription(final StringBuilder builder)
    {
        SUB_MSG.appendChannel(builder);

        builder
            .append(' ')
            .append(SUB_MSG.streamId())
            .append(" [")
            .append(SUB_MSG.registrationCorrelationId())
            .append("][")
            .append(SUB_MSG.clientId())
            .append(':')
            .append(SUB_MSG.correlationId())
            .append(']');
    }

    private static void dissectPublicationReady(final StringBuilder builder)
    {
        builder
            .append(PUB_READY.sessionId())
            .append(':')
            .append(PUB_READY.streamId())
            .append(' ')
            .append(PUB_READY.publicationLimitCounterId())
            .append(' ')
            .append(PUB_READY.channelStatusCounterId())
            .append(" [")
            .append(PUB_READY.correlationId())
            .append(' ')
            .append(PUB_READY.registrationId())
            .append("] ");

        PUB_READY.appendLogFileName(builder);
    }

    private static void dissectImageReady(final StringBuilder builder)
    {
        builder
            .append(IMAGE_READY.sessionId())
            .append(':')
            .append(IMAGE_READY.streamId())
            .append(" [")
            .append(IMAGE_READY.subscriberPositionId())
            .append(':')
            .append(IMAGE_READY.subscriptionRegistrationId())
            .append("] \"")
            .append(IMAGE_READY.sourceIdentity())
            .append("\" [")
            .append(IMAGE_READY.correlationId())
            .append("] ");

        IMAGE_READY.appendLogFileName(builder);
    }

    private static void dissectCorrelationEvent(final StringBuilder builder)
    {
        builder
            .append('[')
            .append(CORRELATED_MSG.clientId())
            .append(':')
            .append(CORRELATED_MSG.correlationId())
            .append(']');
    }

    private static void dissectImage(final StringBuilder builder)
    {
        IMAGE_MSG.appendChannel(builder);

        builder
            .append(' ')
            .append(IMAGE_MSG.streamId())
            .append(" [")
            .append(IMAGE_MSG.correlationId())
            .append(' ')
            .append(IMAGE_MSG.subscriptionRegistrationId())
            .append(']');
    }

    private static void dissectRemoveEvent(final StringBuilder builder)
    {
        builder
            .append(REMOVE_MSG.registrationId())
            .append(" [")
            .append(REMOVE_MSG.clientId())
            .append(':')
            .append(REMOVE_MSG.correlationId())
            .append(']');
    }

    private static void dissectDestination(final StringBuilder builder)
    {
        DESTINATION_MSG.appendChannel(builder);

        builder
            .append(' ')
            .append(DESTINATION_MSG.registrationCorrelationId())
            .append(" [")
            .append(DESTINATION_MSG.clientId())
            .append(':')
            .append(DESTINATION_MSG.correlationId())
            .append(']');
    }

    private static void dissectError(final StringBuilder builder)
    {
        builder
            .append(ERROR_MSG.offendingCommandCorrelationId())
            .append(' ')
            .append(ERROR_MSG.errorCode().toString())
            .append(' ')
            .append(ERROR_MSG.errorMessage());
    }

    private static void dissectCounter(final StringBuilder builder)
    {
        builder
            .append(COUNTER_MSG.typeId())
            .append(" [")
            .append(COUNTER_MSG.keyBufferOffset()).append(' ').append(COUNTER_MSG.keyBufferLength())
            .append("][")
            .append(COUNTER_MSG.labelBufferOffset()).append(' ').append(COUNTER_MSG.labelBufferLength())
            .append("][")
            .append(COUNTER_MSG.clientId())
            .append(':')
            .append(COUNTER_MSG.correlationId())
            .append(']');
    }

    private static void dissectCounterUpdate(final StringBuilder builder)
    {
        builder.append(COUNTER_UPDATE.correlationId()).append(' ').append(COUNTER_UPDATE.counterId());
    }

    private static void dissectOperationSuccess(final StringBuilder builder)
    {
        builder.append(OPERATION_SUCCEEDED.correlationId());
    }

    private static void dissectSubscriptionReady(final StringBuilder builder)
    {
        builder
            .append(SUBSCRIPTION_READY.correlationId())
            .append(' ')
            .append(SUBSCRIPTION_READY.channelStatusCounterId());
    }

    private static void dissectClientTimeout(final StringBuilder builder)
    {
        builder.append(CLIENT_TIMEOUT.clientId());
    }

    private static void dissectTerminateDriver(final StringBuilder builder)
    {
        builder.append(TERMINATE_DRIVER.clientId()).append(' ').append(TERMINATE_DRIVER.tokenBufferLength());
    }

    public static int frameType(final MutableDirectBuffer buffer, final int termOffset)
    {
        return buffer.getShort(FrameDescriptor.typeOffset(termOffset), LITTLE_ENDIAN) & 0xFFFF;
    }
}
