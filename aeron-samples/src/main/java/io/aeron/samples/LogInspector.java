/*
 * Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron.samples;

import io.aeron.LogBuffers;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.PrintStream;
import java.util.Date;

import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static java.lang.Math.min;

/**
 * Command line utility for inspecting a log buffer to see what terms and messages it contains.
 */
public class LogInspector
{
    /**
     * Data format for fragments which can be ASCII or HEX.
     */
    public static final String AERON_LOG_DATA_FORMAT_PROP_NAME = "aeron.log.inspector.data.format";
    public static final String AERON_LOG_DATA_FORMAT = System.getProperty(
        AERON_LOG_DATA_FORMAT_PROP_NAME, "hex").toLowerCase();

    /**
     * Should the default header be skipped for output.
     */
    public static final String AERON_LOG_SKIP_DEFAULT_HEADER_PROP_NAME = "aeron.log.inspector.skipDefaultHeader";
    public static final boolean AERON_LOG_SKIP_DEFAULT_HEADER = Boolean.getBoolean(
        AERON_LOG_SKIP_DEFAULT_HEADER_PROP_NAME);

    /**
     * Should zeros be skipped in the output to reduce noise.
     */
    public static final String AERON_LOG_SCAN_OVER_ZEROES_PROP_NAME = "aeron.log.inspector.scanOverZeroes";
    public static final boolean AERON_LOG_SCAN_OVER_ZEROES = Boolean.getBoolean(AERON_LOG_SCAN_OVER_ZEROES_PROP_NAME);

    public static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    public static void main(final String[] args)
    {
        final PrintStream out = System.out;
        if (args.length < 1)
        {
            out.println("Usage: LogInspector <logFileName> [dump limit in bytes per message]");
            return;
        }

        final String logFileName = args[0];
        final int messageDumpLimit = args.length >= 2 ? Integer.parseInt(args[1]) : Integer.MAX_VALUE;

        try (LogBuffers logBuffers = new LogBuffers(logFileName))
        {
            out.println("======================================================================");
            out.format("%s Inspection dump for %s%n", new Date(), logFileName);
            out.println("======================================================================");

            final DataHeaderFlyweight dataHeaderFlyweight = new DataHeaderFlyweight();
            final UnsafeBuffer[] termBuffers = logBuffers.duplicateTermBuffers();
            final int termLength = logBuffers.termLength();
            final UnsafeBuffer metaDataBuffer = logBuffers.metaDataBuffer();
            final int initialTermId = initialTermId(metaDataBuffer);

            out.format("   Is Connected: %s%n", isConnected(metaDataBuffer));
            out.format("Initial term id: %d%n", initialTermId);
            out.format("     Term Count: %d%n", activeTermCount(metaDataBuffer));
            out.format("   Active index: %d%n", indexByTermCount(activeTermCount(metaDataBuffer)));
            out.format("    Term length: %d%n", termLength);
            out.format("     MTU length: %d%n", mtuLength(metaDataBuffer));
            out.format("      Page Size: %d%n", pageSize(metaDataBuffer));
            out.format("   EOS Position: %d%n%n", endOfStreamPosition(metaDataBuffer));

            if (!AERON_LOG_SKIP_DEFAULT_HEADER)
            {
                dataHeaderFlyweight.wrap(defaultFrameHeader(metaDataBuffer));
                out.format("default %s%n", dataHeaderFlyweight);
            }
            out.println();

            for (int i = 0; i < PARTITION_COUNT; i++)
            {
                final long rawTail = rawTailVolatile(metaDataBuffer, i);
                final long termOffset = rawTail & 0xFFFF_FFFFL;
                final int termId = termId(rawTail);
                final int offset = (int)Math.min(termOffset, termLength);
                final int positionBitsToShift = LogBufferDescriptor.positionBitsToShift(termLength);
                out.format(
                    "Index %d Term Meta Data termOffset=%d termId=%d rawTail=%d position=%d%n",
                    i,
                    termOffset,
                    termId,
                    rawTail,
                    LogBufferDescriptor.computePosition(termId, offset, positionBitsToShift, initialTermId));
            }

            for (int i = 0; i < PARTITION_COUNT; i++)
            {
                out.println();
                out.println("======================================================================");
                out.format("Index %d Term Data%n%n", i);

                final UnsafeBuffer termBuffer = termBuffers[i];
                int offset = 0;
                do
                {
                    dataHeaderFlyweight.wrap(termBuffer, offset, termLength - offset);
                    out.println(offset + ": " + dataHeaderFlyweight.toString());

                    final int frameLength = dataHeaderFlyweight.frameLength();
                    if (frameLength < DataHeaderFlyweight.HEADER_LENGTH)
                    {
                        if (0 == frameLength && AERON_LOG_SCAN_OVER_ZEROES)
                        {
                            offset += FrameDescriptor.FRAME_ALIGNMENT;
                            continue;
                        }

                        try
                        {
                            final int limit = min(termLength - (offset + HEADER_LENGTH), messageDumpLimit);
                            out.println(formatBytes(termBuffer, offset + HEADER_LENGTH, limit));
                        }
                        catch (final Exception ex)
                        {
                            System.err.printf("frameLength=%d offset=%d%n", frameLength, offset);
                            ex.printStackTrace();
                        }

                        break;
                    }

                    final int limit = min(frameLength - HEADER_LENGTH, messageDumpLimit);
                    out.println(formatBytes(termBuffer, offset + HEADER_LENGTH, limit));

                    offset += BitUtil.align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
                }
                while (offset < termLength);
            }
        }
    }

    public static char[] formatBytes(final DirectBuffer buffer, final int offset, final int length)
    {
        switch (AERON_LOG_DATA_FORMAT)
        {
            case "us-ascii":
            case "us_ascii":
            case "ascii":
                return bytesToAscii(buffer, offset, length);

            default:
                return bytesToHex(buffer, offset, length);
        }
    }

    private static char[] bytesToAscii(final DirectBuffer buffer, final int offset, final int length)
    {
        final char[] chars = new char[length];

        for (int i = 0; i < length; i++)
        {
            byte b = buffer.getByte(offset + i);

            if (b < 0)
            {
                b = 0;
            }

            chars[i] = (char)b;
        }

        return chars;
    }

    public static char[] bytesToHex(final DirectBuffer buffer, final int offset, final int length)
    {
        final char[] chars = new char[length * 2];

        for (int i = 0; i < length; i++)
        {
            final int b = buffer.getByte(offset + i) & 0xFF;

            chars[i * 2] = HEX_ARRAY[b >>> 4];
            chars[i * 2 + 1] = HEX_ARRAY[b & 0x0F];
        }

        return chars;
    }
}
