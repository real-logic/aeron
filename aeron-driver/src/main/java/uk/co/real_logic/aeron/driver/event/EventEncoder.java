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
package uk.co.real_logic.aeron.driver.event;

import uk.co.real_logic.agrona.MutableDirectBuffer;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static uk.co.real_logic.agrona.BitUtil.SIZE_OF_INT;
import static uk.co.real_logic.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Encoding of event types to a buffer for logging.
 */
public class EventEncoder
{
    static final int STACK_DEPTH = 5;
    private static final int LOG_HEADER_LENGTH = 16;
    private static final int SOCKET_ADDRESS_MAX_LENGTH = 24;

    public static int encode(
        final MutableDirectBuffer encodingBuffer, final MutableDirectBuffer buffer, final int offset, final int length)
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

    private static int encodeLogHeader(final MutableDirectBuffer encodingBuffer, final int captureLength, final int length)
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
        return Math.min(bufferLength, EventConfiguration.MAX_EVENT_LENGTH - LOG_HEADER_LENGTH - SOCKET_ADDRESS_MAX_LENGTH);
    }
}
