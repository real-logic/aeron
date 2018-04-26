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
package io.aeron.archive;

import io.aeron.archive.codecs.mark.MarkFileHeaderDecoder;
import io.aeron.archive.codecs.mark.MarkFileHeaderEncoder;
import io.aeron.archive.codecs.mark.VarAsciiEncodingEncoder;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.MarkFile;
import org.agrona.SystemUtil;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.util.function.Consumer;

/**
 * Used to mark the presence of a running {@link Archive} in a directory to guard it.
 */
public class ArchiveMarkFile implements AutoCloseable
{
    public static final String FILENAME = "archive-mark.dat";
    public static final int ALIGNMENT = 1024;

    private final MarkFileHeaderDecoder headerDecoder = new MarkFileHeaderDecoder();
    private final MarkFileHeaderEncoder headerEncoder = new MarkFileHeaderEncoder();
    private final MarkFile markFile;
    private final UnsafeBuffer buffer;

    public ArchiveMarkFile(final Archive.Context ctx)
    {
        this(new File(ctx.archiveDir(), FILENAME), alignedTotalFileLength(ctx), ctx.epochClock(), 0);

        encode(ctx);
        updateActivityTimestamp(ctx.epochClock().time());
        signalReady();
    }

    public ArchiveMarkFile(
        final File file,
        final int totalFileLength,
        final EpochClock epochClock,
        final long timeoutMs)
    {
        this.markFile = new MarkFile(
            file,
            file.exists(),
            MarkFileHeaderDecoder.versionEncodingOffset(),
            MarkFileHeaderDecoder.activityTimestampEncodingOffset(),
            totalFileLength,
            timeoutMs,
            epochClock,
            (version) ->
            {
                if (version != MarkFileHeaderDecoder.SCHEMA_VERSION)
                {
                    throw new IllegalArgumentException("mark file version " + version +
                        " does not match software:" + MarkFileHeaderDecoder.SCHEMA_VERSION);
                }
            },
            null);

        this.buffer = markFile.buffer();

        headerEncoder.wrap(buffer, 0);
        headerDecoder.wrap(buffer, 0, MarkFileHeaderDecoder.BLOCK_LENGTH, MarkFileHeaderDecoder.SCHEMA_VERSION);

        headerEncoder.pid(SystemUtil.getPid());
    }

    public ArchiveMarkFile(
        final File directory,
        final String filename,
        final EpochClock epochClock,
        final long timeoutMs,
        final Consumer<String> logger)
    {
        markFile = new MarkFile(
            directory,
            filename,
            MarkFileHeaderDecoder.versionEncodingOffset(),
            MarkFileHeaderDecoder.activityTimestampEncodingOffset(),
            timeoutMs,
            epochClock,
            (version) ->
            {
                if (version != MarkFileHeaderDecoder.SCHEMA_VERSION)
                {
                    throw new IllegalArgumentException("mark file version " + version +
                        " does not match software:" + MarkFileHeaderDecoder.SCHEMA_VERSION);
                }
            },
            logger);

        this.buffer = markFile.buffer();

        headerDecoder.wrap(buffer, 0, MarkFileHeaderDecoder.BLOCK_LENGTH, MarkFileHeaderDecoder.SCHEMA_VERSION);
    }

    public void close()
    {
        CloseHelper.close(markFile);
    }

    public void signalReady()
    {
        markFile.signalReady(MarkFileHeaderEncoder.SCHEMA_VERSION);
    }

    public void updateActivityTimestamp(final long nowMs)
    {
        markFile.timestampOrdered(nowMs);
    }

    public long activityTimestampVolatile()
    {
        return markFile.timestampVolatile();
    }

    public MarkFileHeaderEncoder encoder()
    {
        return headerEncoder;
    }

    public MarkFileHeaderDecoder decoder()
    {
        return headerDecoder;
    }

    public void encode(final Archive.Context ctx)
    {
        headerEncoder
            .startTimestamp(ctx.epochClock().time())
            .controlStreamId(ctx.controlStreamId())
            .localControlStreamId(ctx.localControlStreamId())
            .eventsStreamId(ctx.recordingEventsStreamId())
            .controlChannel(ctx.controlChannel())
            .localControlChannel(ctx.localControlChannel())
            .eventsChannel(ctx.recordingEventsChannel())
            .aeronDirectory(ctx.aeron().context().aeronDirectoryName());
    }

    public static int alignedTotalFileLength(final Archive.Context ctx)
    {
        return alignedTotalFileLength(
            ALIGNMENT,
            ctx.controlChannel(),
            ctx.localControlChannel(),
            ctx.recordingEventsChannel(),
            ctx.aeron().context().aeronDirectoryName());
    }

    private static int alignedTotalFileLength(
        final int alignment,
        final String controlChannel,
        final String localControlChannel,
        final String eventsChannel,
        final String aeronDirectory)
    {
        return BitUtil.align(
            MarkFileHeaderEncoder.BLOCK_LENGTH +
            (4 * VarAsciiEncodingEncoder.lengthEncodingLength()) +
            controlChannel.length() +
            localControlChannel.length() +
            eventsChannel.length() +
            aeronDirectory.length(), alignment);
    }
}
