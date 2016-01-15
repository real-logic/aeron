/*
 * Copyright 2015 Real Logic Ltd.
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
package uk.co.real_logic.aeron.driver.event;

import uk.co.real_logic.aeron.command.*;
import uk.co.real_logic.aeron.logbuffer.FrameDescriptor;
import uk.co.real_logic.aeron.protocol.*;
import uk.co.real_logic.agrona.MutableDirectBuffer;

import java.net.InetAddress;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static uk.co.real_logic.agrona.BitUtil.SIZE_OF_INT;
import static uk.co.real_logic.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Dissect encoded log events. The event consumer of the log should be single threaded.
 */
public class EventDissector
{
    private static final DataHeaderFlyweight DATA_HEADER = new DataHeaderFlyweight();
    private static final StatusMessageFlyweight SM_HEADER = new StatusMessageFlyweight();
    private static final NakFlyweight NAK_HEADER = new NakFlyweight();
    private static final SetupFlyweight SETUP_HEADER = new SetupFlyweight();
    private static final PublicationMessageFlyweight PUB_MESSAGE = new PublicationMessageFlyweight();
    private static final SubscriptionMessageFlyweight SUB_MESSAGE = new SubscriptionMessageFlyweight();
    private static final PublicationBuffersReadyFlyweight PUBLICATION_READY = new PublicationBuffersReadyFlyweight();
    private static final ImageBuffersReadyFlyweight IMAGE_READY = new ImageBuffersReadyFlyweight();
    private static final CorrelatedMessageFlyweight CORRELATED_MSG = new CorrelatedMessageFlyweight();
    private static final ImageMessageFlyweight IMAGE_MSG = new ImageMessageFlyweight();
    private static final RemoveMessageFlyweight REMOVE_MSG = new RemoveMessageFlyweight();

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
                smFrame.wrap(buffer,  frameOffset, buffer.capacity() - frameOffset);
                builder.append(dissect(smFrame));
                break;

            case HeaderFlyweight.HDR_TYPE_NAK:
                final NakFlyweight nakFrame = NAK_HEADER;
                nakFrame.wrap(buffer,  frameOffset, buffer.capacity() - frameOffset);
                builder.append(dissect(nakFrame));
                break;

            case HeaderFlyweight.HDR_TYPE_SETUP:
                final SetupFlyweight setupFrame = SETUP_HEADER;
                setupFrame.wrap(buffer,  frameOffset, buffer.capacity() - frameOffset);
                builder.append(dissect(setupFrame));
                break;

            default:
                builder.append("FRAME_UNKNOWN");
                break;
        }

        return builder.toString();
    }

    public static String dissectAsCommand(final EventCode code, final MutableDirectBuffer buffer, final int offset)
    {
        final StringBuilder builder = new StringBuilder();
        final int relativeOffset = dissectLogHeader(code, buffer, offset, builder);

        builder.append(": ");

        switch (code)
        {
            case CMD_IN_ADD_PUBLICATION:
                final PublicationMessageFlyweight pubCommand = PUB_MESSAGE;
                pubCommand.wrap(buffer, offset + relativeOffset);
                builder.append(dissect(pubCommand));
                break;

            case CMD_IN_ADD_SUBSCRIPTION:
                final SubscriptionMessageFlyweight subCommand = SUB_MESSAGE;
                subCommand.wrap(buffer, offset + relativeOffset);
                builder.append(dissect(subCommand));
                break;

            case CMD_IN_REMOVE_PUBLICATION:
            case CMD_IN_REMOVE_SUBSCRIPTION:
                final RemoveMessageFlyweight removeCmd = REMOVE_MSG;
                removeCmd.wrap(buffer, offset + relativeOffset);
                builder.append(dissect(removeCmd));
                break;

            case CMD_OUT_PUBLICATION_READY:
                final PublicationBuffersReadyFlyweight publicationReadyEvent = PUBLICATION_READY;
                publicationReadyEvent.wrap(buffer, offset + relativeOffset);
                builder.append(dissect(publicationReadyEvent));
                break;

            case CMD_OUT_AVAILABLE_IMAGE:
                final ImageBuffersReadyFlyweight imageAvailableEvent = IMAGE_READY;
                imageAvailableEvent.wrap(buffer, offset + relativeOffset);
                builder.append(dissect(imageAvailableEvent));
                break;

            case CMD_OUT_ON_OPERATION_SUCCESS:
            case CMD_IN_KEEPALIVE_CLIENT:
                final CorrelatedMessageFlyweight correlatedEvent = CORRELATED_MSG;
                correlatedEvent.wrap(buffer, offset + relativeOffset);
                builder.append(dissect(correlatedEvent));
                break;

            case CMD_OUT_ON_UNAVAILABLE_IMAGE:
                final ImageMessageFlyweight imageUnavailableEvent = IMAGE_MSG;
                imageUnavailableEvent.wrap(buffer, offset + relativeOffset);
                builder.append(dissect(imageUnavailableEvent));
                break;

            default:
                builder.append("COMMAND_UNKNOWN");
                break;
        }

        return builder.toString();
    }

    public static String dissectAsInvocation(final EventCode code, final MutableDirectBuffer buffer, final int initialOffset)
    {
        final StringBuilder builder = new StringBuilder();
        final int relativeOffset = dissectLogHeader(code, buffer, initialOffset, builder);
        builder.append(": ");

        readStackTraceElement(buffer, initialOffset + relativeOffset, builder);

        return builder.toString();
    }

    public static String dissectAsException(final EventCode code, final MutableDirectBuffer buffer, final int initialOffset)
    {
        final StringBuilder builder = new StringBuilder();
        int offset = initialOffset + dissectLogHeader(code, buffer, initialOffset, builder);
        builder.append(": ");

        int strLength = buffer.getInt(offset, LITTLE_ENDIAN);
        builder.append(buffer.getStringUtf8(offset, strLength));
        offset += strLength + SIZE_OF_INT;

        builder.append('(');
        strLength = buffer.getInt(offset, LITTLE_ENDIAN);
        builder.append(buffer.getStringUtf8(offset, strLength));
        offset += strLength + SIZE_OF_INT;
        builder.append(')');

        for (int i = 0; i < EventEncoder.STACK_DEPTH; i++)
        {
            builder.append('\n');
            offset = readStackTraceElement(buffer, offset, builder);
        }

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

    private static int readStackTraceElement(final MutableDirectBuffer buffer, int offset, final StringBuilder builder)
    {
        final int lineNumber = buffer.getInt(offset, LITTLE_ENDIAN);
        offset += SIZE_OF_INT;

        int length = buffer.getInt(offset);
        final String className = buffer.getStringUtf8(offset, length);
        offset += SIZE_OF_INT + length;

        length = buffer.getInt(offset);
        final String methodName = buffer.getStringUtf8(offset, length);
        offset += SIZE_OF_INT + length;

        length = buffer.getInt(offset);
        final String fileName = buffer.getStringUtf8(offset, length);
        offset += SIZE_OF_INT + length;

        builder.append(String.format("%s.%s %s:%d", className, methodName, fileName, lineNumber));

        return offset;
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
            (double)timestamp / 1000000000.0, code.name(), captureLength, bufferLength));

        return relativeOffset;
    }

    private static int dissectSocketAddress(final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
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
            "SM 0x%x len %d %d:%d:%d @%x %d",
            msg.flags(),
            msg.frameLength(),
            msg.sessionId(),
            msg.streamId(),
            msg.consumptionTermId(),
            msg.consumptionTermOffset(),
            msg.receiverWindowLength());
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
            "SETUP 0x%x len %d %d:%d:%d %d @%x %d MTU %d",
            msg.flags(),
            msg.frameLength(),
            msg.sessionId(),
            msg.streamId(),
            msg.activeTermId(),
            msg.initialTermId(),
            msg.termOffset(),
            msg.termLength(),
            msg.mtuLength());
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
            "%d:%d %d [%d]\n    %s",
            msg.sessionId(),
            msg.streamId(),
            msg.publicationLimitCounterId(),
            msg.correlationId(),
            msg.logFileName());
    }

    private static String dissect(final ImageBuffersReadyFlyweight msg)
    {
        final StringBuilder positions = new StringBuilder();

        for (int i = 0; i < msg.subscriberPositionCount(); i++)
        {
            positions.append(String.format(
                "[%d:%d:%d]",
                i,
                msg.subscriberPositionId(i),
                msg.positionIndicatorRegistrationId(i)));
        }

        return String.format(
            "%d:%d %s \"%s\" [%d]\n    %s",
            msg.sessionId(),
            msg.streamId(),
            positions.toString(),
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
            "%s %d [%d]",
            msg.channel(),
            msg.streamId(),
            msg.correlationId());
    }

    private static String dissect(final RemoveMessageFlyweight msg)
    {
        return String.format(
            "%d [%d:%d]",
            msg.registrationId(),
            msg.clientId(),
            msg.correlationId());
    }

    public static int frameType(final MutableDirectBuffer buffer, final int termOffset)
    {
        return buffer.getShort(FrameDescriptor.typeOffset(termOffset), LITTLE_ENDIAN) & 0xFFFF;
    }
}
