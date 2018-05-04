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
package io.aeron.samples.archive;

import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.samples.LogInspector;
import org.agrona.BitUtil;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Date;

import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static java.lang.Math.min;

/**
 * Command line utility for inspecting the data contents of a archive segment file.
 * <p>
 * Supports {@link LogInspector#AERON_LOG_DATA_FORMAT_PROP_NAME}
 */
public class SegmentInspector
{
    public static void main(final String[] args)
    {
        final PrintStream out = System.out;
        if (args.length < 1)
        {
            out.println("Usage: SegmentInspector <segmentFileName> [dump limit in bytes per message]");
            return;
        }

        final String fileName = args[0];
        final int messageDumpLimit = args.length >= 2 ? Integer.parseInt(args[1]) : Integer.MAX_VALUE;
        final File file = new File(fileName);
        final ByteBuffer byteBuffer = IoUtil.mapExistingFile(file, "Archive Segment File");
        final UnsafeBuffer segmentBuffer = new UnsafeBuffer(byteBuffer);

        out.println("======================================================================");
        out.format("%s Inspection dump for %s%n", new Date(), fileName);
        out.println("======================================================================");
        out.println();

        dumpSegment(out, messageDumpLimit, segmentBuffer);
    }

    /**
     * Dump the contents of a segment file to a {@link PrintStream}.
     *
     * @param out              for the dumped contents.
     * @param messageDumpLimit for the number of bytes per message fragment to dump.
     * @param buffer           the wraps the segment file.
     */
    public static void dumpSegment(final PrintStream out, final int messageDumpLimit, final UnsafeBuffer buffer)
    {
        final DataHeaderFlyweight dataHeaderFlyweight = new DataHeaderFlyweight();
        final int length = buffer.capacity();
        int offset = 0;

        while (offset < length)
        {
            dataHeaderFlyweight.wrap(buffer, offset, length - offset);
            out.println(offset + ": " + dataHeaderFlyweight.toString());

            final int frameLength = dataHeaderFlyweight.frameLength();
            if (frameLength < DataHeaderFlyweight.HEADER_LENGTH)
            {
                break;
            }

            final int limit = min(frameLength - HEADER_LENGTH, messageDumpLimit);
            out.println(LogInspector.formatBytes(buffer, offset + HEADER_LENGTH, limit));
            offset += BitUtil.align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
        }
    }
}
