/*
 * Copyright 2014-2022 Real Logic Limited.
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

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static io.aeron.agent.CommonEventEncoder.*;
import static io.aeron.agent.DriverEventLogger.MAX_HOST_NAME_LENGTH;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Encoding of event types to a {@link UnsafeBuffer} for logging.
 */
final class DriverEventEncoder
{
    private DriverEventEncoder()
    {
    }

    static void encode(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final ByteBuffer srcBuffer,
        final int srcOffset,
        final InetSocketAddress dstAddress)
    {
        int encodedLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        final int encodedSocketLength = encodeSocketAddress(encodingBuffer, offset + encodedLength, dstAddress);
        encodedLength += encodedSocketLength;

        final int bufferCaptureLength = captureLength - encodedSocketLength;
        encodingBuffer.putBytes(offset + encodedLength, srcBuffer, srcOffset, bufferCaptureLength);
    }

    static void encode(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final DirectBuffer srcBuffer,
        final int srcOffset,
        final InetSocketAddress dstAddress)
    {
        int encodedLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        final int encodedSocketLength = encodeSocketAddress(encodingBuffer, offset + encodedLength, dstAddress);
        encodedLength += encodedSocketLength;

        final int bufferCaptureLength = captureLength - encodedSocketLength;
        encodingBuffer.putBytes(offset + encodedLength, srcBuffer, srcOffset, bufferCaptureLength);
    }

    static void encode(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final DirectBuffer srcBuffer,
        final int srcOffset,
        final InetAddress dstAddress)
    {
        int encodedLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        final int encodedInetAddressLength = encodeInetAddress(encodingBuffer, offset + encodedLength, dstAddress);
        encodedLength += encodedInetAddressLength;

        final int bufferCaptureLength = captureLength - encodedInetAddressLength;
        encodingBuffer.putBytes(offset + encodedLength, srcBuffer, srcOffset, bufferCaptureLength);
    }

    public static void encode(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final String value)
    {
        final int encodedLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);
        encodeTrailingString(encodingBuffer, offset + encodedLength, captureLength, value);
    }

    static void encode(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final InetSocketAddress address)
    {
        final int encodedLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);
        encodeSocketAddress(encodingBuffer, offset + encodedLength, address);
    }

    static void encodePublicationRemoval(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final String channel,
        final int sessionId,
        final int streamId)
    {
        int encodedLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putInt(offset + encodedLength, sessionId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        encodingBuffer.putInt(offset + encodedLength, streamId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        encodeTrailingString(encodingBuffer, offset + encodedLength, captureLength - SIZE_OF_INT * 2, channel);
    }

    static void encodeSubscriptionRemoval(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final String channel,
        final int streamId,
        final long id)
    {
        int encodedLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putInt(offset + encodedLength, streamId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        encodingBuffer.putLong(offset + encodedLength, id, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodeTrailingString(
            encodingBuffer, offset + encodedLength, captureLength - SIZE_OF_INT - SIZE_OF_LONG, channel);
    }

    static void encodeImageRemoval(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final String channel,
        final int sessionId,
        final int streamId,
        final long id)
    {
        int encodedLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putInt(offset + encodedLength, sessionId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        encodingBuffer.putInt(offset + encodedLength, streamId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        encodingBuffer.putLong(offset + encodedLength, id, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodeTrailingString(
            encodingBuffer, offset + encodedLength, captureLength - SIZE_OF_INT * 2 - SIZE_OF_LONG, channel);
    }

    static <E extends Enum<E>> int untetheredSubscriptionStateChangeLength(final E from, final E to)
    {
        return stateTransitionStringLength(from, to) + SIZE_OF_LONG + 2 * SIZE_OF_INT;
    }

    static <E extends Enum<E>> void encodeUntetheredSubscriptionStateChange(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final E from,
        final E to,
        final long subscriptionId,
        final int streamId,
        final int sessionId)
    {
        int encodedLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putLong(offset + encodedLength, subscriptionId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putInt(offset + encodedLength, streamId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        encodingBuffer.putInt(offset + encodedLength, sessionId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        encodeTrailingStateChange(encodingBuffer, offset, encodedLength, captureLength, from, to);
    }

    static void encodeFlowControlReceiver(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final long receiverId,
        final int sessionId,
        final int streamId,
        final String channel,
        final int receiverCount)
    {
        int encodedLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putInt(offset + encodedLength, receiverCount, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        encodingBuffer.putLong(offset + encodedLength, receiverId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putInt(offset + encodedLength, sessionId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        encodingBuffer.putInt(offset + encodedLength, streamId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        encodeTrailingString(
            encodingBuffer, offset + encodedLength, captureLength - SIZE_OF_INT * 3 - SIZE_OF_LONG, channel);
    }

    static void encodeResolve(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int length,
        final int captureLength,
        final String resolverName,
        final String hostName,
        final InetAddress inetAddress)
    {
        int encodedLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);
        encodedLength += encodeTrailingString(
            encodingBuffer, offset + encodedLength, SIZE_OF_INT + MAX_HOST_NAME_LENGTH, resolverName);
        encodedLength += encodeTrailingString(
            encodingBuffer, offset + encodedLength, SIZE_OF_INT + MAX_HOST_NAME_LENGTH, hostName);
        encodeInetAddress(encodingBuffer, offset + encodedLength, inetAddress);
    }
}
