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

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static io.aeron.agent.CommonEventEncoder.*;
import static io.aeron.agent.EventConfiguration.MAX_EVENT_LENGTH;
import static java.lang.Math.min;
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

    static int encode(
        final UnsafeBuffer encodingBuffer,
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

    static int encode(
        final UnsafeBuffer encodingBuffer,
        final DirectBuffer buffer,
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

    static int encode(final UnsafeBuffer encodingBuffer, final String value)
    {
        final int encodedLength = encodeTrailingString(encodingBuffer, LOG_HEADER_LENGTH, value);
        encodeLogHeader(encodingBuffer, encodedLength, value.length() + SIZE_OF_INT);

        return LOG_HEADER_LENGTH + encodedLength;
    }

    static int encodePublicationRemoval(
        final UnsafeBuffer encodingBuffer, final String uri, final int sessionId, final int streamId)
    {
        int offset = LOG_HEADER_LENGTH;
        encodingBuffer.putInt(offset, sessionId, LITTLE_ENDIAN);
        offset += SIZE_OF_INT;

        encodingBuffer.putInt(offset, streamId, LITTLE_ENDIAN);
        offset += SIZE_OF_INT;

        final int encodedUriLength = encodeTrailingString(encodingBuffer, offset, uri);

        encodeLogHeader(encodingBuffer, offset + encodedUriLength - LOG_HEADER_LENGTH,
            offset + SIZE_OF_INT + uri.length() - LOG_HEADER_LENGTH);

        return offset + encodedUriLength;
    }

    static int encodeSubscriptionRemoval(
        final UnsafeBuffer encodingBuffer, final String uri, final int streamId, final long id)
    {
        int offset = LOG_HEADER_LENGTH;
        encodingBuffer.putInt(offset, streamId, LITTLE_ENDIAN);
        offset += SIZE_OF_INT;

        encodingBuffer.putLong(offset, id, LITTLE_ENDIAN);
        offset += SIZE_OF_LONG;

        final int encodedUriLength = encodeTrailingString(encodingBuffer, offset, uri);

        encodeLogHeader(encodingBuffer, offset + encodedUriLength - LOG_HEADER_LENGTH,
            offset + SIZE_OF_INT + uri.length() - LOG_HEADER_LENGTH);

        return offset + encodedUriLength;
    }

    static int encodeImageRemoval(
        final UnsafeBuffer encodingBuffer,
        final String uri,
        final int sessionId,
        final int streamId,
        final long id)
    {
        int offset = LOG_HEADER_LENGTH;
        encodingBuffer.putInt(offset, sessionId, LITTLE_ENDIAN);
        offset += SIZE_OF_INT;

        encodingBuffer.putInt(offset, streamId, LITTLE_ENDIAN);
        offset += SIZE_OF_INT;

        encodingBuffer.putLong(offset, id, LITTLE_ENDIAN);
        offset += SIZE_OF_LONG;

        final int encodedUriLength = encodeTrailingString(encodingBuffer, offset, uri);

        encodeLogHeader(encodingBuffer, offset + encodedUriLength - LOG_HEADER_LENGTH,
            offset + SIZE_OF_INT + uri.length() - LOG_HEADER_LENGTH);

        return offset + encodedUriLength;
    }

    private static int determineCaptureLength(final int bufferLength)
    {
        return min(bufferLength, MAX_EVENT_LENGTH - LOG_HEADER_LENGTH - SOCKET_ADDRESS_MAX_LENGTH);
    }
}
