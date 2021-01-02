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
package io.aeron.archive;

import io.aeron.CommonContext;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.codecs.mark.MarkFileHeaderDecoder;
import io.aeron.archive.codecs.mark.MarkFileHeaderEncoder;
import io.aeron.archive.codecs.mark.VarAsciiEncodingEncoder;
import org.agrona.*;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

/**
 * Used to mark the presence of a running {@link Archive} in a directory to guard it.
 */
public class ArchiveMarkFile implements AutoCloseable
{
    /**
     * Major version for the archive files stored on disk. A change to this requires migration.
     */
    public static final int MAJOR_VERSION = 3;

    /**
     * Minor version for the archive files stored on disk. A change to this indicates new features.
     */
    public static final int MINOR_VERSION = 1;

    /**
     * Patch version for the archive files stored on disk. A change to this indicates feature parity bug fixes.
     */
    public static final int PATCH_VERSION = 0;

    /**
     * Combined semantic version for the stored files.
     *
     * @see SemanticVersion
     */
    public static final int SEMANTIC_VERSION = SemanticVersion.compose(MAJOR_VERSION, MINOR_VERSION, PATCH_VERSION);

    /**
     * Header length for the {@link MarkFile} containing the metadata.
     */
    public static final int HEADER_LENGTH = 8 * 1024;

    /**
     * Name for the archive {@link MarkFile} stored in the {@link Archive.Configuration#ARCHIVE_DIR_PROP_NAME}.
     */
    public static final String FILENAME = "archive-mark.dat";

    private final MarkFileHeaderDecoder headerDecoder = new MarkFileHeaderDecoder();
    private final MarkFileHeaderEncoder headerEncoder = new MarkFileHeaderEncoder();
    private final MarkFile markFile;
    private final UnsafeBuffer buffer;
    private final UnsafeBuffer errorBuffer;

    ArchiveMarkFile(final Archive.Context ctx)
    {
        this(
            new File(ctx.archiveDir(), FILENAME),
            alignedTotalFileLength(ctx),
            ctx.errorBufferLength(),
            ctx.epochClock(),
            0);

        encode(ctx);
        updateActivityTimestamp(ctx.epochClock().time());
    }

    ArchiveMarkFile(
        final File file,
        final int totalFileLength,
        final int errorBufferLength,
        final EpochClock epochClock,
        final long timeoutMs)
    {
        final boolean markFileExists = file.exists();

        markFile = new MarkFile(
            file,
            file.exists(),
            MarkFileHeaderDecoder.versionEncodingOffset(),
            MarkFileHeaderDecoder.activityTimestampEncodingOffset(),
            totalFileLength,
            timeoutMs,
            epochClock,
            (version) ->
            {
                if (SemanticVersion.major(version) != MAJOR_VERSION)
                {
                    throw new IllegalArgumentException("mark file major version " + SemanticVersion.major(version) +
                        " does not match software:" + MAJOR_VERSION);
                }
            },
            null);

        buffer = markFile.buffer();

        errorBuffer = errorBufferLength > 0 ?
            new UnsafeBuffer(buffer, HEADER_LENGTH, errorBufferLength) : new UnsafeBuffer(buffer, 0, 0);

        headerEncoder.wrap(buffer, 0);
        headerDecoder.wrap(buffer, 0, MarkFileHeaderDecoder.BLOCK_LENGTH, MarkFileHeaderDecoder.SCHEMA_VERSION);

        if (markFileExists && headerDecoder.errorBufferLength() > 0)
        {
            final UnsafeBuffer existingErrorBuffer = new UnsafeBuffer(
                buffer, headerDecoder.headerLength(), headerDecoder.errorBufferLength());

            saveExistingErrors(file, existingErrorBuffer, System.err);
            existingErrorBuffer.setMemory(0, headerDecoder.errorBufferLength(), (byte)0);
        }

        headerEncoder.pid(SystemUtil.getPid());
    }

    /**
     * Construct the {@link MarkFile} based on an existing directory containing a mark file of a running archive.
     *
     * @param directory  containing the archive files.
     * @param filename   for the mark file.
     * @param epochClock to be used for checking liveness.
     * @param timeoutMs  after which the opening will be aborted if no archive starts.
     * @param logger     to detail any discoveries.
     */
    public ArchiveMarkFile(
        final File directory,
        final String filename,
        final EpochClock epochClock,
        final long timeoutMs,
        final Consumer<String> logger)
    {
        this(
            directory,
            filename,
            epochClock,
            timeoutMs,
            (version) ->
            {
                if (SemanticVersion.major(version) != MAJOR_VERSION)
                {
                    throw new IllegalArgumentException("mark file major version " + SemanticVersion.major(version) +
                        " does not match software:" + MAJOR_VERSION);
                }
            },
            logger);
    }

    /**
     * Open an existing {@link MarkFile} or create a new one to be used in migration.
     *
     * @param directory    containing the archive files.
     * @param filename     for the mark file.
     * @param epochClock   to be used for checking liveness.
     * @param timeoutMs    after which the opening will be aborted if no archive starts.
     * @param versionCheck for confirming the correct version.
     * @param logger       to detail any discoveries.
     */
    public ArchiveMarkFile(
        final File directory,
        final String filename,
        final EpochClock epochClock,
        final long timeoutMs,
        final IntConsumer versionCheck,
        final Consumer<String> logger)
    {
        markFile = new MarkFile(
            directory,
            filename,
            MarkFileHeaderDecoder.versionEncodingOffset(),
            MarkFileHeaderDecoder.activityTimestampEncodingOffset(),
            timeoutMs,
            epochClock,
            versionCheck,
            logger);

        buffer = markFile.buffer();
        headerEncoder.wrap(buffer, 0);
        headerDecoder.wrap(buffer, 0, MarkFileHeaderDecoder.BLOCK_LENGTH, MarkFileHeaderDecoder.SCHEMA_VERSION);

        errorBuffer = headerDecoder.errorBufferLength() > 0 ?
            new UnsafeBuffer(buffer, headerDecoder.headerLength(), headerDecoder.errorBufferLength()) :
            new UnsafeBuffer(buffer, 0, 0);
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        CloseHelper.close(markFile);
    }

    /**
     * Signal the archive has concluded successfully and ready to start.
     */
    public void signalReady()
    {
        markFile.signalReady(SEMANTIC_VERSION);
    }

    /**
     * Update the activity timestamp as a proof of life.
     *
     * @param nowMs activity timestamp as a proof of life.
     */
    public void updateActivityTimestamp(final long nowMs)
    {
        markFile.timestampOrdered(nowMs);
    }

    /**
     * Read the activity timestamp of the archive with volatile semantics.
     *
     * @return the activity timestamp of the archive with volatile semantics.
     */
    public long activityTimestampVolatile()
    {
        return markFile.timestampVolatile();
    }

    /**
     * The encoder for writing the {@link MarkFile} header.
     *
     * @return the encoder for writing the {@link MarkFile} header.
     */
    public MarkFileHeaderEncoder encoder()
    {
        return headerEncoder;
    }

    /**
     * The decoder for reading the {@link MarkFile} header.
     *
     * @return the decoder for reading the {@link MarkFile} header.
     */
    public MarkFileHeaderDecoder decoder()
    {
        return headerDecoder;
    }

    /**
     * The direct buffer which wraps the region of the {@link MarkFile} which contains the error log.
     *
     * @return the direct buffer which wraps the region of the {@link MarkFile} which contains the error log.
     */
    public AtomicBuffer errorBuffer()
    {
        return errorBuffer;
    }

    /**
     * Save the existing errors from a {@link MarkFile} to a {@link PrintStream} for logging.
     *
     * @param markFile    which contains the error buffer.
     * @param errorBuffer which wraps the error log.
     * @param logger      to which the existing errors will be printed.
     */
    public static void saveExistingErrors(final File markFile, final AtomicBuffer errorBuffer, final PrintStream logger)
    {
        try
        {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final int observations = CommonContext.printErrorLog(errorBuffer, new PrintStream(baos, false, "US-ASCII"));
            if (observations > 0)
            {
                final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SSSZ");
                final String errorLogFilename =
                    markFile.getParent() + '-' + dateFormat.format(new Date()) + "-error.log";

                if (null != logger)
                {
                    logger.println("WARNING: existing errors saved to: " + errorLogFilename);
                }

                try (FileOutputStream out = new FileOutputStream(errorLogFilename))
                {
                    baos.writeTo(out);
                }
            }
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private static int alignedTotalFileLength(final Archive.Context ctx)
    {
        final int headerLength =
            MarkFileHeaderEncoder.BLOCK_LENGTH +
            (4 * VarAsciiEncodingEncoder.lengthEncodingLength()) +
            ctx.controlChannel().length() +
            ctx.localControlChannel().length() +
            ctx.recordingEventsChannel().length() +
            ctx.aeronDirectoryName().length();

        if (headerLength > HEADER_LENGTH)
        {
            throw new ArchiveException("ArchiveMarkFile required headerLength=" + headerLength + " > " + HEADER_LENGTH);
        }

        return HEADER_LENGTH + ctx.errorBufferLength();
    }

    private void encode(final Archive.Context ctx)
    {
        headerEncoder
            .startTimestamp(ctx.epochClock().time())
            .controlStreamId(ctx.controlStreamId())
            .localControlStreamId(ctx.localControlStreamId())
            .eventsStreamId(ctx.recordingEventsStreamId())
            .headerLength(HEADER_LENGTH)
            .errorBufferLength(ctx.errorBufferLength())
            .controlChannel(ctx.controlChannel())
            .localControlChannel(ctx.localControlChannel())
            .eventsChannel(ctx.recordingEventsChannel())
            .aeronDirectory(ctx.aeronDirectoryName());
    }
}
