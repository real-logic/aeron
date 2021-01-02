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

import org.agrona.MutableDirectBuffer;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static java.lang.Integer.toHexString;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.time.Instant.ofEpochMilli;
import static java.time.OffsetDateTime.ofInstant;
import static java.time.format.DateTimeFormatter.ofPattern;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

final class CommonEventDissector
{
    private static final DateTimeFormatter DATE_TIME_FORMATTER = ofPattern("uuuu-MM-dd HH:mm:ss.SSSZ");
    private static final double NANOS_PER_SECOND = 1_000_000_000.0;

    private CommonEventDissector()
    {
    }

    static void dissectLogStartMessage(
        final long timestampNs,
        final long timestampMs,
        final ZoneId zone,
        final StringBuilder builder)
    {
        builder
            .append('[')
            .append(((double)timestampNs) / NANOS_PER_SECOND)
            .append("] log started ")
            .append(DATE_TIME_FORMATTER.format(ofInstant(ofEpochMilli(timestampMs), zone)));
    }

    static int dissectLogHeader(
        final String context,
        final Enum<?> code,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int relativeOffset = 0;

        final int captureLength = buffer.getInt(offset + relativeOffset, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        final int bufferLength = buffer.getInt(offset + relativeOffset, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        final long timestampNs = buffer.getLong(offset + relativeOffset, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_LONG;

        builder
            .append('[')
            .append(((double)timestampNs) / NANOS_PER_SECOND)
            .append("] ")
            .append(context)
            .append(": ")
            .append(code.name())
            .append(" [")
            .append(captureLength)
            .append('/')
            .append(bufferLength)
            .append(']');

        return relativeOffset;
    }

    static int dissectSocketAddress(final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        int relativeOffset = 0;

        final int port = buffer.getInt(offset + relativeOffset, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        final int addressLength = buffer.getInt(offset + relativeOffset);
        relativeOffset += SIZE_OF_INT;

        if (4 == addressLength)
        {
            final int i = offset + relativeOffset;
            builder
                .append(buffer.getByte(i) & 0xFF)
                .append('.')
                .append(buffer.getByte(i + 1) & 0xFF)
                .append('.')
                .append(buffer.getByte(i + 2) & 0xFF)
                .append('.')
                .append(buffer.getByte(i + 3) & 0xFF)
                .append(':')
                .append(port);
        }
        else if (16 == addressLength)
        {
            final int i = offset + relativeOffset;
            builder
                .append(toHexString(((buffer.getByte(i) << 8) & 0xFF00) | buffer.getByte(i + 1) & 0xFF))
                .append(':')
                .append(toHexString(((buffer.getByte(i + 2) << 8) & 0xFF00) | buffer.getByte(i + 3) & 0xFF))
                .append(':')
                .append(toHexString(((buffer.getByte(i + 4) << 8) & 0xFF00) | buffer.getByte(i + 5) & 0xFF))
                .append(':')
                .append(toHexString(((buffer.getByte(i + 6) << 8) & 0xFF00) | buffer.getByte(i + 7) & 0xFF))
                .append(':')
                .append(toHexString(((buffer.getByte(i + 8) << 8) & 0xFF00) | buffer.getByte(i + 9) & 0xFF))
                .append(':')
                .append(toHexString(((buffer.getByte(i + 10) << 8) & 0xFF00) | buffer.getByte(i + 11) & 0xFF))
                .append(':')
                .append(toHexString(((buffer.getByte(i + 12) << 8) & 0xFF00) | buffer.getByte(i + 13) & 0xFF))
                .append(':')
                .append(toHexString(((buffer.getByte(i + 14) << 8) & 0xFF00) | buffer.getByte(i + 15) & 0xFF))
                .append(':')
                .append(port);
        }
        else
        {
            builder.append("unknown-address:").append(port);
        }

        relativeOffset += addressLength;

        return relativeOffset;
    }
}
