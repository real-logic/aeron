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
package uk.co.real_logic.aeron.common.event;

import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.aeron.common.command.*;
import uk.co.real_logic.aeron.common.protocol.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static uk.co.real_logic.agrona.BitUtil.SIZE_OF_INT;

/**
 * Encoding/Dissecting of event types
 */
public class EventCodec
{
    private static final ThreadLocal<HeaderFlyweight> HEADER_FLYWEIGHT =
        ThreadLocal.withInitial(HeaderFlyweight::new);
    private static final ThreadLocal<DataHeaderFlyweight> DATA_HEADER =
        ThreadLocal.withInitial(DataHeaderFlyweight::new);
    private static final ThreadLocal<StatusMessageFlyweight> SM_HEADER =
        ThreadLocal.withInitial(StatusMessageFlyweight::new);
    private static final ThreadLocal<NakFlyweight> NAK_HEADER =
        ThreadLocal.withInitial(NakFlyweight::new);
    private static final ThreadLocal<SetupFlyweight> SETUP_HEADER =
        ThreadLocal.withInitial(SetupFlyweight::new);

    private static final ThreadLocal<PublicationMessageFlyweight> PUB_MESSAGE =
        ThreadLocal.withInitial(PublicationMessageFlyweight::new);
    private static final ThreadLocal<SubscriptionMessageFlyweight> SUB_MESSAGE =
        ThreadLocal.withInitial(SubscriptionMessageFlyweight::new);
    private static final ThreadLocal<PublicationBuffersReadyFlyweight> PUBLICATION_READY =
        ThreadLocal.withInitial(PublicationBuffersReadyFlyweight::new);
    private static final ThreadLocal<ConnectionBuffersReadyFlyweight> CONNECTION_READY =
        ThreadLocal.withInitial(ConnectionBuffersReadyFlyweight::new);
    private static final ThreadLocal<CorrelatedMessageFlyweight> CORRELATED_MSG =
        ThreadLocal.withInitial(CorrelatedMessageFlyweight::new);
    private static final ThreadLocal<ConnectionMessageFlyweight> CONNECTION_MSG =
        ThreadLocal.withInitial(ConnectionMessageFlyweight::new);

    private static final int LOG_HEADER_LENGTH = 16;
    private static final int SOCKET_ADDRESS_MAX_LENGTH = 24;
    public static final int STACK_DEPTH = 5;

    public static int encode(
        final MutableDirectBuffer encodingBuffer, final MutableDirectBuffer buffer, final int offset, final int bufferLength)
    {
        final int captureLength = determineCaptureLength(bufferLength);
        int relativeOffset = encodeLogHeader(encodingBuffer, captureLength, bufferLength);

        encodingBuffer.putBytes(relativeOffset, buffer, offset, captureLength);
        relativeOffset += captureLength;

        return relativeOffset;
    }

    public static int encode(
        final MutableDirectBuffer encodingBuffer,
        final ByteBuffer buffer,
        final int offset,
        final int bufferLength,
        final InetSocketAddress dstAddress)
    {
        final int captureLength = determineCaptureLength(bufferLength);
        int relativeOffset = encodeLogHeader(encodingBuffer, captureLength, bufferLength);

        relativeOffset += encodeSocketAddress(encodingBuffer, relativeOffset, dstAddress);
        encodingBuffer.putBytes(relativeOffset, buffer, offset, captureLength);
        relativeOffset += captureLength;

        return relativeOffset;
    }

    public static int encode(
        final MutableDirectBuffer encodingBuffer, final byte[] buffer, final int offset, final int bufferLength)
    {
        final int captureLength = determineCaptureLength(bufferLength);
        int relativeOffset = encodeLogHeader(encodingBuffer, captureLength, bufferLength);

        encodingBuffer.putBytes(relativeOffset, buffer, offset, captureLength);
        relativeOffset += captureLength;

        return relativeOffset;
    }

    public static int encode(final MutableDirectBuffer encodingBuffer, final String value)
    {
        final int length = encodingBuffer.putStringUtf8(LOG_HEADER_LENGTH, value, LITTLE_ENDIAN);
        final int recordLength = LOG_HEADER_LENGTH + length;
        encodeLogHeader(encodingBuffer, recordLength, recordLength);

        return recordLength;
    }

    public static int encode(final MutableDirectBuffer encodingBuffer, final StackTraceElement stack)
    {
        final int relativeOffset = putStackTraceElement(encodingBuffer, stack, LOG_HEADER_LENGTH);
        final int captureLength = relativeOffset;
        encodeLogHeader(encodingBuffer, captureLength, captureLength);

        return relativeOffset;
    }

    public static int encode(final MutableDirectBuffer encodingBuffer, final Throwable ex)
    {
        final String msg = null != ex.getMessage() ? ex.getMessage() : "exception message not set";

        int relativeOffset = LOG_HEADER_LENGTH;
        relativeOffset += encodingBuffer.putStringUtf8(relativeOffset, ex.getClass().getName(), LITTLE_ENDIAN);
        relativeOffset += encodingBuffer.putStringUtf8(relativeOffset, msg, LITTLE_ENDIAN);

        final StackTraceElement[] stackTrace = ex.getStackTrace();
        for (int i = 0; i < Math.min(STACK_DEPTH, stackTrace.length); i++)
        {
            relativeOffset = putStackTraceElement(encodingBuffer, stackTrace[i], relativeOffset);
        }

        final int recordLength = relativeOffset - LOG_HEADER_LENGTH;
        encodeLogHeader(encodingBuffer, recordLength, recordLength);

        return relativeOffset;
    }

    private static int putStackTraceElement(
        final MutableDirectBuffer encodingBuffer, final StackTraceElement stack, int relativeOffset)
    {
        encodingBuffer.putInt(relativeOffset, stack.getLineNumber(), LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;
        relativeOffset += encodingBuffer.putStringUtf8(relativeOffset, stack.getClassName(), LITTLE_ENDIAN);
        relativeOffset += encodingBuffer.putStringUtf8(relativeOffset, stack.getMethodName(), LITTLE_ENDIAN);
        relativeOffset += encodingBuffer.putStringUtf8(relativeOffset, stack.getFileName(), LITTLE_ENDIAN);

        return relativeOffset;
    }

    public static String dissectAsFrame(
        final EventCode code, final MutableDirectBuffer buffer, final int offset, final int length)
    {
        final StringBuilder builder = new StringBuilder();
        final HeaderFlyweight frame = HEADER_FLYWEIGHT.get();
        int relativeOffset = dissectLogHeader(code, buffer, offset, builder);

        builder.append(": ");

        relativeOffset += dissectSocketAddress(buffer, offset + relativeOffset, builder);

        builder.append(" ");

        frame.wrap(buffer, offset + relativeOffset);
        switch (frame.headerType())
        {
            case HeaderFlyweight.HDR_TYPE_PAD:
            case HeaderFlyweight.HDR_TYPE_DATA:
                final DataHeaderFlyweight dataFrame = DATA_HEADER.get();
                dataFrame.wrap(buffer, offset + relativeOffset);
                builder.append(dissect(dataFrame));
                break;

            case HeaderFlyweight.HDR_TYPE_SM:
                final StatusMessageFlyweight smFrame = SM_HEADER.get();
                smFrame.wrap(buffer, offset + relativeOffset);
                builder.append(dissect(smFrame));
                break;

            case HeaderFlyweight.HDR_TYPE_NAK:
                final NakFlyweight nakFrame = NAK_HEADER.get();
                nakFrame.wrap(buffer, offset + relativeOffset);
                builder.append(dissect(nakFrame));
                break;

            case HeaderFlyweight.HDR_TYPE_SETUP:
                final SetupFlyweight setupFrame = SETUP_HEADER.get();
                setupFrame.wrap(buffer, offset + relativeOffset);
                builder.append(dissect(setupFrame));
                break;

            default:
                builder.append("FRAME_UNKNOWN");
                break;
        }

        return builder.toString();
    }

    public static String dissectAsCommand(
        final EventCode code, final MutableDirectBuffer buffer, final int offset, final int length)
    {
        final StringBuilder builder = new StringBuilder();
        final int relativeOffset = dissectLogHeader(code, buffer, offset, builder);

        builder.append(": ");

        switch (code)
        {
            case CMD_IN_ADD_PUBLICATION:
            case CMD_IN_REMOVE_PUBLICATION:
                final PublicationMessageFlyweight pubCommand = PUB_MESSAGE.get();
                pubCommand.wrap(buffer, offset + relativeOffset);
                builder.append(dissect(pubCommand));
                break;

            case CMD_IN_ADD_SUBSCRIPTION:
            case CMD_IN_REMOVE_SUBSCRIPTION:
                final SubscriptionMessageFlyweight subCommand = SUB_MESSAGE.get();
                subCommand.wrap(buffer, offset + relativeOffset);
                builder.append(dissect(subCommand));
                break;

            case CMD_OUT_PUBLICATION_READY:
                final PublicationBuffersReadyFlyweight newBuffer = PUBLICATION_READY.get();
                newBuffer.wrap(buffer, offset + relativeOffset);
                builder.append(dissect(newBuffer));
                break;

            case CMD_OUT_CONNECTION_READY:
                final ConnectionBuffersReadyFlyweight connectionReadyCommand = CONNECTION_READY.get();
                connectionReadyCommand.wrap(buffer, offset + relativeOffset);
                builder.append(dissect(connectionReadyCommand));
                break;

            case CMD_OUT_ON_OPERATION_SUCCESS:
            case CMD_IN_KEEPALIVE_CLIENT:
                final CorrelatedMessageFlyweight correlatedCmd = CORRELATED_MSG.get();
                correlatedCmd.wrap(buffer, offset + relativeOffset);
                builder.append(dissect(correlatedCmd));
                break;

            case CMD_OUT_ON_INACTIVE_CONNECTION:
                final ConnectionMessageFlyweight connectionCmd = CONNECTION_MSG.get();
                connectionCmd.wrap(buffer, offset + relativeOffset);
                builder.append(dissect(connectionCmd));
                break;

            default:
                builder.append("COMMAND_UNKNOWN");
                break;
        }

        return builder.toString();
    }

    public static String dissectAsInvocation(
        final EventCode code, final MutableDirectBuffer buffer, final int initialOffset, final int length)
    {
        final StringBuilder builder = new StringBuilder();
        final int relativeOffset = dissectLogHeader(code, buffer, initialOffset, builder);
        builder.append(": ");

        readStackTraceElement(buffer, initialOffset + relativeOffset, builder);

        return builder.toString();
    }

    public static String dissectAsException(
        final EventCode code, final MutableDirectBuffer buffer, final int initialOffset, final int length)
    {
        final StringBuilder builder = new StringBuilder();
        int offset = initialOffset + dissectLogHeader(code, buffer, initialOffset, builder);
        builder.append(": ");

        int strLength = buffer.getInt(offset, LITTLE_ENDIAN);
        builder.append(buffer.getStringUtf8(offset, strLength));
        offset += strLength + SIZE_OF_INT;

        builder.append("(");
        strLength = buffer.getInt(offset, LITTLE_ENDIAN);
        builder.append(buffer.getStringUtf8(offset, strLength));
        offset += strLength + SIZE_OF_INT;
        builder.append(")");

        for (int i = 0; i < STACK_DEPTH; i++)
        {
            builder.append('\n');
            offset = readStackTraceElement(buffer, offset, builder);
        }

        return builder.toString();
    }

    public static String dissectAsString(
        final EventCode code, final MutableDirectBuffer buffer, final int offset, final int length)
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

    private static int encodeLogHeader(final MutableDirectBuffer encodingBuffer, final int captureLength, final int bufferLength)
    {
        int relativeOffset = 0;
        /*
         * Stream of values:
         * - capture buffer length (int)
         * - total buffer length (int)
         * - timestamp (long)
         * - buffer (until end)
         */

        encodingBuffer.putInt(relativeOffset, captureLength, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        encodingBuffer.putInt(relativeOffset, bufferLength, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        encodingBuffer.putLong(relativeOffset, System.nanoTime(), LITTLE_ENDIAN);
        relativeOffset += BitUtil.SIZE_OF_LONG;

        return relativeOffset;
    }

    private static int encodeSocketAddress(
        final MutableDirectBuffer encodingBuffer, final int offset, final InetSocketAddress dstAddress)
    {
        int relativeOffset = 0;
        /*
         * Stream of values:
         * - port (int) (unsigned short int)
         * - IP address length (int) (4 or 16)
         * - IP address (4 or 16 bytes)
         */

        encodingBuffer.putInt(offset + relativeOffset, dstAddress.getPort(), LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        final byte[] addrBuffer = dstAddress.getAddress().getAddress();
        encodingBuffer.putInt(offset + relativeOffset, addrBuffer.length, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        encodingBuffer.putBytes(offset + relativeOffset, addrBuffer);
        relativeOffset += addrBuffer.length;

        return relativeOffset;
    }

    private static int determineCaptureLength(final int bufferLength)
    {
        return Math.min(bufferLength, EventConfiguration.MAX_EVENT_LENGTH - LOG_HEADER_LENGTH - SOCKET_ADDRESS_MAX_LENGTH);
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
        relativeOffset += BitUtil.SIZE_OF_LONG;

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

    private static String dissect(final DataHeaderFlyweight header)
    {
        return String.format(
            "%s %x len %d %x:%x:%x @%x",
            header.headerType() == HeaderFlyweight.HDR_TYPE_PAD ? "PAD" : "DATA",
            header.flags(),
            header.frameLength(),
            header.sessionId(),
            header.streamId(),
            header.termId(),
            header.termOffset());
    }

    private static String dissect(final StatusMessageFlyweight header)
    {
        return String.format(
            "SM %x len %d %x:%x:%x @%x %d",
            header.flags(),
            header.frameLength(),
            header.sessionId(),
            header.streamId(),
            header.termId(),
            header.completedTermOffset(),
            header.receiverWindowLength());
    }

    private static String dissect(final NakFlyweight header)
    {
        return String.format(
            "NAK %x len %d %x:%x:%x @%x %d",
            header.flags(),
            header.frameLength(),
            header.sessionId(),
            header.streamId(),
            header.termId(),
            header.termOffset(),
            header.length());
    }

    private static String dissect(final SetupFlyweight header)
    {
        return String.format(
            "SETUP %x len %d %x:%x:%x %x @%x %d MTU %d",
            header.flags(),
            header.frameLength(),
            header.sessionId(),
            header.streamId(),
            header.activeTermId(),
            header.initialTermId(),
            header.termOffset(),
            header.termLength(),
            header.mtuLength());
    }

    private static String dissect(final PublicationMessageFlyweight command)
    {
        return String.format(
            "%3$s %1$x:%2$x [%5$x:%4$x]",
            command.sessionId(),
            command.streamId(),
            command.channel(),
            command.correlationId(),
            command.clientId());
    }

    private static String dissect(final SubscriptionMessageFlyweight command)
    {
        return String.format(
            "%s %d [%x][%x:%x]",
            command.channel(),
            command.streamId(),
            command.registrationCorrelationId(),
            command.clientId(),
            command.correlationId());
    }

    private static String dissect(final PublicationBuffersReadyFlyweight command)
    {
        return String.format(
            "%s %x:%x:%x [%x]\n    %s",
            command.channel(),
            command.sessionId(),
            command.streamId(),
            command.positionCounterId(),
            command.correlationId(),
            command.logFileName());
    }

    private static String dissect(final ConnectionBuffersReadyFlyweight command)
    {
        return String.format(
            "%s %x:%x %x %s [%x]\n    %s",
             command.channel(),
             command.sessionId(),
             command.streamId(),
             command.positionIndicatorCount(),
             command.sourceInfo(),
             command.correlationId(),
             command.logFileName());
    }

    private static String dissect(final CorrelatedMessageFlyweight command)
    {
        return String.format("[%x:%x]", command.clientId(), command.correlationId());
    }

    private static String dissect(final ConnectionMessageFlyweight command)
    {
        return String.format(
            "%s %x:%x [%x]",
            command.channel(),
            command.sessionId(),
            command.streamId(),
            command.correlationId());
    }
}
