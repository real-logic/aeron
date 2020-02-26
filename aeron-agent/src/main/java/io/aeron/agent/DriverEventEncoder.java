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
        int relativeOffset = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        final int encodedSocketLength = encodeSocketAddress(encodingBuffer, offset + relativeOffset, dstAddress);
        relativeOffset += encodedSocketLength;

        final int bufferCaptureLength = captureLength - encodedSocketLength;
        encodingBuffer.putBytes(offset + relativeOffset, srcBuffer, srcOffset, bufferCaptureLength);
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
        int relativeOffset = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        final int encodedSocketLength = encodeSocketAddress(encodingBuffer, offset + relativeOffset, dstAddress);
        relativeOffset += encodedSocketLength;

        final int bufferCaptureLength = captureLength - encodedSocketLength;
        encodingBuffer.putBytes(offset + relativeOffset, srcBuffer, srcOffset, bufferCaptureLength);
    }

    static void encode(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final String value)
    {
        final int relativeOffset = encodeLogHeader(encodingBuffer, offset, captureLength, length);
        encodeTrailingString(encodingBuffer, offset + relativeOffset, captureLength, value);
    }

    static void encodePublicationRemoval(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final String uri,
        final int sessionId,
        final int streamId)
    {
        int relativeOffset = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putInt(offset + relativeOffset, sessionId, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        encodingBuffer.putInt(offset + relativeOffset, streamId, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        encodeTrailingString(encodingBuffer, offset + relativeOffset, captureLength - (SIZE_OF_INT * 2), uri);
    }

    static void encodeSubscriptionRemoval(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final String uri,
        final int streamId,
        final long id)
    {
        int relativeOffset = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putInt(offset + relativeOffset, streamId, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        encodingBuffer.putLong(offset + relativeOffset, id, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_LONG;

        encodeTrailingString(encodingBuffer, offset + relativeOffset, captureLength - SIZE_OF_INT - SIZE_OF_LONG, uri);
    }

    static void encodeImageRemoval(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final String uri,
        final int sessionId,
        final int streamId,
        final long id)
    {
        int relativeOffset = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putInt(offset + relativeOffset, sessionId, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        encodingBuffer.putInt(offset + relativeOffset, streamId, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        encodingBuffer.putLong(offset + relativeOffset, id, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_LONG;

        encodeTrailingString(
            encodingBuffer, offset + relativeOffset, captureLength - (SIZE_OF_INT * 2) - SIZE_OF_LONG, uri);
    }
}
