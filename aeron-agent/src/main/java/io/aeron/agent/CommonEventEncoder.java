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
    static final int SOCKET_ADDRESS_MAX_LENGTH = 24;
    static final int MAX_CAPTURE_LENGTH = MAX_EVENT_LENGTH - LOG_HEADER_LENGTH;

    private CommonEventEncoder()
    {
    }

    static int encodeLogHeader(final UnsafeBuffer encodingBuffer, final int captureLength, final int length)
    {
        return internalEncodeLogHeader(encodingBuffer, captureLength, length, SystemNanoClock.INSTANCE);
    }

    static int internalEncodeLogHeader(
        final UnsafeBuffer encodingBuffer, final int captureLength, final int length, final NanoClock nanoClock)
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

        encodingBuffer.putLong(relativeOffset, nanoClock.nanoTime(), LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_LONG;

        return relativeOffset;
    }

    static int encodeSocketAddress(
        final UnsafeBuffer encodingBuffer, final int offset, final InetSocketAddress dstAddress)
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

    static int encodeTrailingString(final UnsafeBuffer encodingBuffer, final int offset, final String value)
    {
        int relativeOffset = 0;
        final int maxLength = MAX_EVENT_LENGTH - offset - SIZE_OF_INT;
        if (value.length() <= maxLength)
        {
            relativeOffset += encodingBuffer.putStringAscii(relativeOffset + offset, value, LITTLE_ENDIAN);
        }
        else
        {
            encodingBuffer.putInt(relativeOffset + offset, maxLength, LITTLE_ENDIAN);
            relativeOffset += SIZE_OF_INT;
            relativeOffset += encodingBuffer.putStringWithoutLengthAscii(
                relativeOffset + offset, value, 0, maxLength - 3);
            relativeOffset += encodingBuffer.putStringWithoutLengthAscii(relativeOffset + offset, "...");
        }

        return relativeOffset;
    }

    static int encode(final UnsafeBuffer encodingBuffer, final DirectBuffer buffer, final int offset, final int length)
    {
        final int captureLength = min(length, MAX_CAPTURE_LENGTH);
        int relativeOffset = encodeLogHeader(encodingBuffer, captureLength, length);

        encodingBuffer.putBytes(relativeOffset, buffer, offset, captureLength);
        relativeOffset += captureLength;

        return relativeOffset;
    }
}
