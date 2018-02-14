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

import io.aeron.archive.codecs.CncHeaderDecoder;
import io.aeron.archive.codecs.CncHeaderEncoder;
import io.aeron.archive.codecs.VarAsciiEncodingEncoder;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.CncFile;
import org.agrona.SystemUtil;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.util.function.Consumer;

public class ArchiveCncFile implements AutoCloseable
{
    public static final String FILENAME = "cnc.dat";
    public static final int ALIGNMENT = 1024;

    private final CncHeaderDecoder cncHeaderDecoder;
    private final CncHeaderEncoder cncHeaderEncoder;
    private final CncFile cncFile;
    private final UnsafeBuffer cncBuffer;

    public ArchiveCncFile(final Archive.Context ctx)
    {
        this(new File(ctx.archiveDir(), FILENAME), alignedTotalFileLength(ctx), ctx.epochClock(), 0);

        encode(ctx);
        signalCncReady();
        updateActivityTimestamp(ctx.epochClock().time());
    }

    public ArchiveCncFile(
        final File file,
        final int totalFileLength,
        final EpochClock epochClock,
        final long timeoutMs)
    {
        this.cncFile = new CncFile(
            file,
            file.exists(),
            CncHeaderDecoder.versionEncodingOffset(),
            CncHeaderDecoder.activityTimestampEncodingOffset(),
            totalFileLength,
            timeoutMs,
            epochClock,
            (version) ->
            {
                if (version != CncHeaderDecoder.SCHEMA_VERSION)
                {
                    throw new IllegalArgumentException("CnC file version " + version +
                        " does not match software:" + CncHeaderDecoder.SCHEMA_VERSION);
                }
            },
            null);

        this.cncBuffer = cncFile.buffer();

        cncHeaderDecoder = new CncHeaderDecoder();
        cncHeaderEncoder = new CncHeaderEncoder();

        cncHeaderEncoder.wrap(cncBuffer, 0);
        cncHeaderDecoder.wrap(cncBuffer, 0, CncHeaderDecoder.BLOCK_LENGTH, CncHeaderDecoder.SCHEMA_VERSION);

        cncHeaderEncoder.pid(SystemUtil.getpid());
    }

    public ArchiveCncFile(
        final File directory,
        final String filename,
        final EpochClock epochClock,
        final long timeoutMs,
        final Consumer<String> logger)
    {
        cncFile = new CncFile(
            directory,
            filename,
            CncHeaderDecoder.versionEncodingOffset(),
            CncHeaderDecoder.activityTimestampEncodingOffset(),
            timeoutMs,
            epochClock,
            (version) ->
            {
                if (version != CncHeaderDecoder.SCHEMA_VERSION)
                {
                    throw new IllegalArgumentException("CnC file version " + version +
                        " does not match software:" + CncHeaderDecoder.SCHEMA_VERSION);
                }
            },
            logger);

        this.cncBuffer = cncFile.buffer();

        cncHeaderDecoder = new CncHeaderDecoder();
        cncHeaderEncoder = null;

        cncHeaderDecoder.wrap(cncBuffer, 0, CncHeaderDecoder.BLOCK_LENGTH, CncHeaderDecoder.SCHEMA_VERSION);
    }

    public void close()
    {
        CloseHelper.quietClose(cncFile);
    }

    public void signalCncReady()
    {
        cncFile.signalCncReady(CncHeaderEncoder.SCHEMA_VERSION);
    }

    public void updateActivityTimestamp(final long nowMs)
    {
        cncFile.timestampOrdered(nowMs);
    }

    public long activityTimestampVolatile()
    {
        return cncFile.timestampVolatile();
    }

    public CncHeaderEncoder encoder()
    {
        return cncHeaderEncoder;
    }

    public CncHeaderDecoder decoder()
    {
        return cncHeaderDecoder;
    }

    public void encode(final Archive.Context ctx)
    {
        cncHeaderEncoder
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
            CncHeaderEncoder.BLOCK_LENGTH +
            (4 * VarAsciiEncodingEncoder.lengthEncodingLength()) +
            controlChannel.length() +
            localControlChannel.length() +
            eventsChannel.length() +
            aeronDirectory.length(), alignment);
    }
}
