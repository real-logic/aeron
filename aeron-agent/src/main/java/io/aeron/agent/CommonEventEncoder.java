/*
 * Copyright 2014-2021 Real Logic Limited.
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
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.net.InetSocketAddress;

import static io.aeron.agent.EventConfiguration.MAX_EVENT_LENGTH;
import static java.lang.Math.min;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

final class CommonEventEncoder
{
    static final int LOG_HEADER_LENGTH = 16;
    static final int MAX_CAPTURE_LENGTH = MAX_EVENT_LENGTH - LOG_HEADER_LENGTH;
    static final String STATE_SEPARATOR = " -> ";

    private CommonEventEncoder()
    {
    }

    static int encodeLogHeader(
        final UnsafeBuffer encodingBuffer, final int offset, final int captureLength, final int length)
    {
        return internalEncodeLogHeader(encodingBuffer, offset, captureLength, length, SystemNanoClock.INSTANCE);
    }

    static int internalEncodeLogHeader(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final NanoClock nanoClock)
    {
        if (captureLength < 0 || captureLength > length || captureLength > MAX_CAPTURE_LENGTH)
        {
            throw new IllegalArgumentException("invalid input: captureLength=" + captureLength + ", length=" + length);
        }

        int relativeOffset = 0;
        /*
         * Stream of values:
         * - capture buffer length (int)
         * - total buffer length (int)
         * - timestamp (long)
         * - buffer (until end)
         */

        encodingBuffer.putInt(offset + relativeOffset, captureLength, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        encodingBuffer.putInt(offset + relativeOffset, length, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        encodingBuffer.putLong(offset + relativeOffset, nanoClock.nanoTime(), LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_LONG;

        return relativeOffset;
    }

    static int encodeSocketAddress(
        final UnsafeBuffer encodingBuffer, final int offset, final InetSocketAddress address)
    {
        int relativeOffset = 0;
        /*
         * Stream of values:
         * - port (int) (unsigned short int)
         * - IP address length (int) (4 or 16)
         * - IP address (4 or 16 bytes)
         */

        encodingBuffer.putInt(offset + relativeOffset, address.getPort(), LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        final byte[] addressBytes = address.getAddress().getAddress();
        encodingBuffer.putInt(offset + relativeOffset, addressBytes.length, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        encodingBuffer.putBytes(offset + relativeOffset, addressBytes);
        relativeOffset += addressBytes.length;

        return relativeOffset;
    }

    static int encodeTrailingString(
        final UnsafeBuffer encodingBuffer, final int offset, final int remainingCapacity, final String value)
    {
        final int maxLength = remainingCapacity - SIZE_OF_INT;
        if (value.length() <= maxLength)
        {
            return encodingBuffer.putStringAscii(offset, value, LITTLE_ENDIAN);
        }
        else
        {
            encodingBuffer.putInt(offset, maxLength, LITTLE_ENDIAN);
            encodingBuffer.putStringWithoutLengthAscii(offset + SIZE_OF_INT, value, 0, maxLength - 3);
            encodingBuffer.putStringWithoutLengthAscii(offset + SIZE_OF_INT + maxLength - 3, "...");
            return remainingCapacity;
        }
    }

    static int encode(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final DirectBuffer srcBuffer,
        final int srcOffset)
    {
        int relativeOffset = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putBytes(offset + relativeOffset, srcBuffer, srcOffset, captureLength);
        relativeOffset += captureLength;

        return relativeOffset;
    }

    static int captureLength(final int length)
    {
        return min(length, MAX_CAPTURE_LENGTH);
    }

    static int encodedLength(final int captureLength)
    {
        return LOG_HEADER_LENGTH + captureLength;
    }

    static int socketAddressLength(final InetSocketAddress address)
    {
        return 2 * SIZE_OF_INT + address.getAddress().getAddress().length;
    }

    static <E extends Enum<E>> int stateTransitionStringLength(final E from, final E to)
    {
        return SIZE_OF_INT + from.name().length() + STATE_SEPARATOR.length() + to.name().length();
    }
}
