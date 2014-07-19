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
package uk.co.real_logic.aeron.common.event;

import uk.co.real_logic.aeron.common.BitUtil;
import uk.co.real_logic.aeron.common.command.*;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.protocol.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static uk.co.real_logic.aeron.common.BitUtil.SIZE_OF_INT;

/**
 * Encoding/Dissecting of event types
 */
public class EventCodec
{
    private final static ThreadLocal<HeaderFlyweight> headerFlyweight =
            ThreadLocal.withInitial(HeaderFlyweight::new);
    private final static ThreadLocal<DataHeaderFlyweight> dataHeader =
            ThreadLocal.withInitial(DataHeaderFlyweight::new);
    private final static ThreadLocal<StatusMessageFlyweight> smHeader =
            ThreadLocal.withInitial(StatusMessageFlyweight::new);
    private final static ThreadLocal<NakFlyweight> nakHeader =
            ThreadLocal.withInitial(NakFlyweight::new);

    private final static ThreadLocal<PublicationMessageFlyweight> pubMessage =
            ThreadLocal.withInitial(PublicationMessageFlyweight::new);
    private final static ThreadLocal<SubscriptionMessageFlyweight> subMessage =
            ThreadLocal.withInitial(SubscriptionMessageFlyweight::new);
    private final static ThreadLocal<LogBuffersMessageFlyweight> newBufferMessage =
            ThreadLocal.withInitial(LogBuffersMessageFlyweight::new);

    private final static int LOG_HEADER_LENGTH = 16;
    private final static int SOCKET_ADDRESS_MAX_LENGTH = 24;

    public static int encode(final AtomicBuffer encodingBuffer, final AtomicBuffer buffer,
                             final int offset, final int bufferLength)
    {
        final int captureLength = determineCaptureLength(bufferLength);
        int relativeOffset = encodeLogHeader(encodingBuffer, captureLength, bufferLength);

        encodingBuffer.putBytes(relativeOffset, buffer, offset, captureLength);
        relativeOffset += captureLength;

        return relativeOffset;
    }

    public static int encode(final AtomicBuffer encodingBuffer, final ByteBuffer buffer,
                             final int bufferLength, final InetSocketAddress dstAddr)
    {
        final int captureLength = determineCaptureLength(bufferLength);
        int relativeOffset = encodeLogHeader(encodingBuffer, captureLength, bufferLength);

        relativeOffset += encodeSocketAddress(encodingBuffer, relativeOffset, dstAddr);
        encodingBuffer.putBytes(relativeOffset, buffer, 0, captureLength);
        relativeOffset += captureLength;

        return relativeOffset;
    }

    public static int encode(final AtomicBuffer encodingBuffer, final byte[] buffer,
                             final int offset, final int bufferLength)
    {
        final int captureLength = determineCaptureLength(bufferLength);
        int relativeOffset = encodeLogHeader(encodingBuffer, captureLength, bufferLength);

        relativeOffset += encodingBuffer.putBytes(relativeOffset, buffer, offset, captureLength);

        return relativeOffset;
    }

    public static int encode(final AtomicBuffer encodingBuffer, final String value)
    {
        final int length = encodingBuffer.putString(LOG_HEADER_LENGTH, value, LITTLE_ENDIAN);
        final int recordLength = LOG_HEADER_LENGTH + length;
        encodeLogHeader(encodingBuffer, recordLength, recordLength);
        return recordLength;
    }

    public static int encode(final AtomicBuffer encodingBuffer, final StackTraceElement stack)
    {
        final int relativeOffset = putStackTraceElement(encodingBuffer, stack, LOG_HEADER_LENGTH);
        final int captureLength = relativeOffset - LOG_HEADER_LENGTH;
        encodeLogHeader(encodingBuffer, captureLength, captureLength);
        return relativeOffset;
    }

    public static int encode(final AtomicBuffer encodingBuffer, final Exception ex)
    {
        final StackTraceElement stack = ex.getStackTrace()[0];

        int relativeOffset = LOG_HEADER_LENGTH;
        relativeOffset += encodingBuffer.putString(relativeOffset, ex.getClass().getName(), LITTLE_ENDIAN);
        relativeOffset += encodingBuffer.putString(relativeOffset, ex.getMessage(), LITTLE_ENDIAN);
        relativeOffset = putStackTraceElement(encodingBuffer, stack, relativeOffset);

        final int recordLength = relativeOffset - LOG_HEADER_LENGTH;
        encodeLogHeader(encodingBuffer, recordLength, recordLength);
        return relativeOffset;
    }

    private static int putStackTraceElement(final AtomicBuffer encodingBuffer, final StackTraceElement stack, int relativeOffset)
    {
        encodingBuffer.putInt(relativeOffset, stack.getLineNumber(), LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;
        relativeOffset += encodingBuffer.putString(relativeOffset, stack.getClassName(), LITTLE_ENDIAN);
        relativeOffset += encodingBuffer.putString(relativeOffset, stack.getMethodName(), LITTLE_ENDIAN);
        relativeOffset += encodingBuffer.putString(relativeOffset, stack.getFileName(), LITTLE_ENDIAN);
        return relativeOffset;
    }

    public static String dissectAsFrame(final EventCode code, final AtomicBuffer buffer,
                                        final int offset, final int length)
    {
        final StringBuilder builder = new StringBuilder();
        final HeaderFlyweight frame = headerFlyweight.get();
        int relativeOffset = dissectLogHeader(code, buffer, offset, builder);

        builder.append(": ");

        relativeOffset += dissectSocketAddress(buffer, offset + relativeOffset, builder);

        builder.append(" ");

        frame.wrap(buffer, offset + relativeOffset);
        switch (frame.headerType())
        {
            case HeaderFlyweight.HDR_TYPE_DATA:
                final DataHeaderFlyweight dataFrame = dataHeader.get();
                dataFrame.wrap(buffer, offset + relativeOffset);
                builder.append(dissect(dataFrame));
                break;

            case HeaderFlyweight.HDR_TYPE_SM:
                final StatusMessageFlyweight smFrame = smHeader.get();
                smFrame.wrap(buffer, offset + relativeOffset);
                builder.append(dissect(smFrame));
                break;

            case HeaderFlyweight.HDR_TYPE_NAK:
                final NakFlyweight nakFrame = nakHeader.get();
                nakFrame.wrap(buffer, offset + relativeOffset);
                builder.append(dissect(nakFrame));
                break;

            default:
                builder.append("FRAME_UNKNOWN");
                break;
        }

        return builder.toString();
    }

    public static String dissectAsCommand(final EventCode code, final AtomicBuffer buffer,
                                          final int offset, final int length)
    {
        final StringBuilder builder = new StringBuilder();
        int relativeOffset = dissectLogHeader(code, buffer, offset, builder);

        builder.append(": ");

        switch (code)
        {
            case CMD_IN_ADD_PUBLICATION:
            case CMD_IN_REMOVE_PUBLICATION:
                final PublicationMessageFlyweight pubCommand = pubMessage.get();
                pubCommand.wrap(buffer, offset + relativeOffset);
                builder.append(dissect(pubCommand));
                break;

            case CMD_IN_ADD_SUBSCRIPTION:
            case CMD_IN_REMOVE_SUBSCRIPTION:
                final SubscriptionMessageFlyweight subCommand = subMessage.get();
                subCommand.wrap(buffer, offset + relativeOffset);
                builder.append(dissect(subCommand));
                break;

            case CMD_OUT_NEW_PUBLICATION_BUFFER_NOTIFICATION:
            case CMD_OUT_NEW_SUBSCRIPTION_BUFFER_NOTIFICATION:
                final LogBuffersMessageFlyweight newBuffer = newBufferMessage.get();
                newBuffer.wrap(buffer, offset + relativeOffset);
                builder.append(dissect(newBuffer));
                break;

            default:
                builder.append("COMMAND_UNKNOWN");
                break;
        }

        return builder.toString();
    }

    public static String dissectAsInvocation(final EventCode code, final AtomicBuffer buffer,
                                             final int initialOffset, final int length)
    {
        final StringBuilder builder = new StringBuilder();
        int relativeOffset = dissectLogHeader(code, buffer, initialOffset, builder);
        builder.append(": ");

        readStackTraceElement(buffer, relativeOffset, builder);

        return builder.toString();
    }

    public static String dissectAsException(
            final EventCode code,
            final AtomicBuffer buffer,
            final int initialOffset,
            final int length)
    {
        final StringBuilder builder = new StringBuilder();
        int offset = initialOffset + dissectLogHeader(code, buffer, initialOffset, builder);
        builder.append(": ");

        int strLength = buffer.getInt(offset, LITTLE_ENDIAN);
        builder.append(buffer.getString(offset, strLength));
        offset += strLength + SIZE_OF_INT;

        builder.append("(");
        strLength = buffer.getInt(offset, LITTLE_ENDIAN);
        builder.append(buffer.getString(offset, strLength));
        offset += strLength + SIZE_OF_INT;
        builder.append(")");

        readStackTraceElement(buffer, offset, builder);

        return builder.toString();
    }

    public static String dissectAsString(
            final EventCode code,
            final AtomicBuffer buffer,
            final int offset,
            final int length)
    {
        final StringBuilder builder = new StringBuilder();
        final int relativeOffset = dissectLogHeader(code, buffer, offset, builder);
        builder.append(": ");
        builder.append(buffer.getString(offset + relativeOffset, LITTLE_ENDIAN));
        return builder.toString();
    }

    private static int readStackTraceElement(
            final AtomicBuffer buffer,
            int offset,
            final StringBuilder builder)
    {
        final int lineNumber = buffer.getInt(offset, LITTLE_ENDIAN);
        offset += SIZE_OF_INT;

        int length = buffer.getInt(offset);
        final String className = buffer.getString(offset, length);
        offset += SIZE_OF_INT + length;

        length = buffer.getInt(offset);
        final String methodName = buffer.getString(offset, length);
        offset += SIZE_OF_INT + length;

        length = buffer.getInt(offset);
        final String fileName = buffer.getString(offset, length);
        offset += SIZE_OF_INT + length;

        builder.append(String.format("%s.%s %s:%d", className, methodName, fileName, lineNumber));

        return offset;
    }

    private static int encodeLogHeader(
            final AtomicBuffer encodingBuffer,
            final int captureLength,
            final int bufferLength)
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

    private static int encodeSocketAddress(final AtomicBuffer encodingBuffer, final int offset,
                                           final InetSocketAddress dstAddr)
    {
        int relativeOffset = 0;
        /*
         * Stream of values:
         * - port (int) (unsigned short int)
         * - IP address length (int) (4 or 16)
         * - IP address (4 or 16 bytes)
         */

        encodingBuffer.putInt(offset + relativeOffset, dstAddr.getPort(), LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        final byte[] addrBuffer = dstAddr.getAddress().getAddress();
        encodingBuffer.putInt(offset + relativeOffset, addrBuffer.length, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        relativeOffset += encodingBuffer.putBytes(offset + relativeOffset, addrBuffer);

        return relativeOffset;
    }

    private static int determineCaptureLength(final int bufferLength)
    {
        return Math.min(bufferLength, EventConfiguration.MAX_EVENT_LENGTH - LOG_HEADER_LENGTH - SOCKET_ADDRESS_MAX_LENGTH);
    }

    private static int dissectLogHeader(final EventCode code, final AtomicBuffer buffer, final int offset,
                                        final StringBuilder builder)
    {
        int relativeOffset = 0;

        final int captureLength = buffer.getInt(offset + relativeOffset, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        final int bufferLength = buffer.getInt(offset + relativeOffset, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        final long timestamp = buffer.getLong(offset + relativeOffset, LITTLE_ENDIAN);
        relativeOffset += BitUtil.SIZE_OF_LONG;

        builder.append(String.format("[%1$f] %2$s [%3$d/%4$d]", (double)timestamp / 1000000000.0, code.name(),
                captureLength, bufferLength));

        return relativeOffset;
    }

    private static int dissectSocketAddress(final AtomicBuffer buffer, final int offset, final StringBuilder builder)
    {
        int relativeOffset = 0;

        final int port = buffer.getInt(offset + relativeOffset, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        final byte[] addrBuffer = new byte[buffer.getInt(offset + relativeOffset)];
        relativeOffset += SIZE_OF_INT;

        relativeOffset += buffer.getBytes(offset + relativeOffset, addrBuffer);

        try
        {
            builder.append(String.format("%s.%d", InetAddress.getByAddress(addrBuffer).getHostAddress(), port));
        }
        catch (final Exception ex)
        {
            ex.printStackTrace();
        }

        return relativeOffset;
    }

    private static String dissect(final DataHeaderFlyweight header)
    {
        return String.format("DATA %x len %d %x:%x:%x @%x", header.flags(), header.frameLength(),
                header.sessionId(), header.channelId(), header.termId(), header.termOffset());
    }

    private static String dissect(final StatusMessageFlyweight header)
    {
        return String.format("SM %x len %d %x:%x:%x @%x %d", header.flags(), header.frameLength(),
                header.sessionId(), header.channelId(), header.termId(), header.highestContiguousTermOffset(),
                header.receiverWindow());
    }

    private static String dissect(final NakFlyweight header)
    {
        return String.format("NAK %x len %d %x:%x:%x @%x %d", header.flags(), header.frameLength(),
                header.sessionId(), header.channelId(), header.termId(), header.termOffset(), header.length());
    }

    private static String dissect(final PublicationMessageFlyweight command)
    {
        return String.format("%3$s %1$x:%2$x", command.sessionId(), command.channelId(), command.destination());
    }

    private static String dissect(final SubscriptionMessageFlyweight command)
    {
        return String.format("%s %d", command.destination(), command.channelId());
    }

    private static String dissect(final LogBuffersMessageFlyweight command)
    {
        final String locations = IntStream.range(0, 6)
                .mapToObj((i) -> String.format("{%s, %d, %d}", command.location(i),
                        command.bufferLength(i), command.bufferOffset(i)))
                .collect(Collectors.joining("\n    "));

        return String.format("%x:%x:%x\n    %s", command.sessionId(), command.channelId(), command.termId(), locations);
    }
}
