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

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static io.aeron.agent.EventConfiguration.MAX_EVENT_LENGTH;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Encoding of event types to a {@link MutableDirectBuffer} for logging.
 */
public class DriverEventEncoder
{
    private static final int LOG_HEADER_LENGTH = 16;
    private static final int SOCKET_ADDRESS_MAX_LENGTH = 24;

    public static int encode(
        final MutableDirectBuffer encodingBuffer, final DirectBuffer buffer, final int offset, final int length)
    {
        final int captureLength = determineCaptureLength(length);
        int relativeOffset = encodeLogHeader(encodingBuffer, captureLength, length);

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
        final MutableDirectBuffer encodingBuffer,
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

    public static int encode(final MutableDirectBuffer encodingBuffer, final String value)
    {
        final int length = encodingBuffer.putStringUtf8(LOG_HEADER_LENGTH, value, LITTLE_ENDIAN);
        final int recordLength = LOG_HEADER_LENGTH + length;
        encodeLogHeader(encodingBuffer, recordLength, recordLength);

        return recordLength;
    }

    private static int encodeLogHeader(
        final MutableDirectBuffer encodingBuffer, final int captureLength, final int length)
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

        encodingBuffer.putInt(relativeOffset, length, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        encodingBuffer.putLong(relativeOffset, System.nanoTime(), LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_LONG;

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

        final byte[] addressBytes = dstAddress.getAddress().getAddress();
        encodingBuffer.putInt(offset + relativeOffset, addressBytes.length, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        encodingBuffer.putBytes(offset + relativeOffset, addressBytes);
        relativeOffset += addressBytes.length;

        return relativeOffset;
    }

    private static int determineCaptureLength(final int bufferLength)
    {
        return Math.min(bufferLength, MAX_EVENT_LENGTH - LOG_HEADER_LENGTH - SOCKET_ADDRESS_MAX_LENGTH);
    }
}
