/*
 * Copyright 2014-2019 Real Logic Ltd.
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

import io.aeron.CncFileDescriptor;
import io.aeron.CommonContext;
import io.aeron.archive.codecs.RecordingDescriptorDecoder;
import io.aeron.archive.codecs.RecordingDescriptorEncoder;
import io.aeron.archive.codecs.RecordingDescriptorHeaderDecoder;
import io.aeron.archive.codecs.RecordingDescriptorHeaderEncoder;
import io.aeron.archive.codecs.mark.MarkFileHeaderDecoder;
import io.aeron.exceptions.AeronException;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.PrintBufferUtil;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.aeron.archive.Catalog.*;
import static io.aeron.archive.MigrationUtils.fullVersionString;
import static io.aeron.archive.ReplaySession.isInvalidHeader;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.client.AeronArchive.segmentFileBasePosition;
import static io.aeron.logbuffer.FrameDescriptor.*;
import static io.aeron.logbuffer.LogBufferDescriptor.computeTermIdFromPosition;
import static io.aeron.logbuffer.LogBufferDescriptor.positionBitsToShift;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_DATA;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_PAD;
import static java.lang.Math.min;
import static java.nio.file.StandardOpenOption.READ;
import static org.agrona.BitUtil.align;
import static org.agrona.BufferUtil.allocateDirectAligned;

/**
 * Tool for inspecting and performing administrative tasks on an {@link Archive} and its contents which is described in
 * the {@link Catalog}.
 */
public class ArchiveTool
{
    /**
     * Allows user to confirm or reject an action.
     *
     * @param <T> type of the context data
     */
    @FunctionalInterface
    interface ActionConfirmation<T>
    {
        /**
         * Confirm or reject the action.
         *
         * @param context context data
         * @return <tt>true</tt> confirms the action and <tt>false</tt> to reject the action
         */
        boolean confirm(T context);
    }

    public static int maxEntries(final File archiveDir)
    {
        try (Catalog catalog = openCatalogReadOnly(archiveDir, SystemEpochClock.INSTANCE))
        {
            return catalog.maxEntries();
        }
    }

    public static int maxEntries(final File archiveDir, final long newMaxEntries)
    {
        try (Catalog catalog = new Catalog(archiveDir, null, 0, newMaxEntries, SystemEpochClock.INSTANCE))
        {
            return catalog.maxEntries();
        }
    }

    public static void describe(final PrintStream out, final File archiveDir)
    {
        try (Catalog catalog = openCatalogReadOnly(archiveDir, SystemEpochClock.INSTANCE);
            ArchiveMarkFile markFile = openMarkFile(archiveDir, SystemEpochClock.INSTANCE, out::println))
        {
            printMarkInformation(markFile, out);
            out.println("Catalog Max Entries: " + catalog.maxEntries());
            catalog.forEach((he, hd, e, d) -> out.println(d));
        }
    }

    public static void describeRecording(final PrintStream out, final File archiveDir, final long recordingId)
    {
        try (Catalog catalog = openCatalogReadOnly(archiveDir, SystemEpochClock.INSTANCE))
        {
            catalog.forEntry(recordingId, (he, hd, e, d) -> out.println(d));
        }
    }

    public static int countEntries(final File archiveDir)
    {
        try (Catalog catalog = openCatalogReadOnly(archiveDir, SystemEpochClock.INSTANCE))
        {
            return catalog.countEntries();
        }
    }

    public static long pid(final File archiveDir)
    {
        try (ArchiveMarkFile markFile = openMarkFile(archiveDir, SystemEpochClock.INSTANCE, null))
        {
            return markFile.decoder().pid();
        }
    }

    public static void printErrors(final PrintStream out, final File archiveDir)
    {
        try (ArchiveMarkFile markFile = openMarkFile(archiveDir, SystemEpochClock.INSTANCE, null))
        {
            printErrors(out, markFile);
        }
    }

    public static void dump(
        final PrintStream out,
        final File archiveDir,
        final long dataFragmentLimit,
        final ActionConfirmation<Long> continueOnFragmentLimit)
    {
        try (Catalog catalog = openCatalog(archiveDir, SystemEpochClock.INSTANCE);
            ArchiveMarkFile markFile = openMarkFile(archiveDir, SystemEpochClock.INSTANCE, out::println))
        {
            printMarkInformation(markFile, out);
            out.println("Catalog Max Entries: " + catalog.maxEntries());

            out.println();
            out.println("Dumping " + dataFragmentLimit + " fragments per recording");
            catalog.forEach((he, headerDecoder, e, descriptorDecoder) -> dump(out, archiveDir, catalog,
                dataFragmentLimit, continueOnFragmentLimit, headerDecoder, descriptorDecoder));
        }
    }

    /**
     * Verify descriptors in the catalog, checking recording files availability and contents.
     * Faulty entries are marked as unusable
     *
     * @param out                        output stream to print results and errors to
     * @param archiveDir                 that contains Markfile, Catalog, and recordings.
     * @param validateAllSegmentFiles    when <tt>true</tt> then all of the segment files will be validated, otherwise
     *                                   only the last segment file will be validated.
     * @param truncateFileOnPageStraddle action to perform if last fragment in the max segment file straddles the page
     *                                   boundary, i.e. if <tt>true</tt> the file will be truncated (last fragment
     */
    public static void verify(
        final PrintStream out,
        final File archiveDir,
        final boolean validateAllSegmentFiles,
        final ActionConfirmation<File> truncateFileOnPageStraddle)
    {
        verify(out, archiveDir, validateAllSegmentFiles, SystemEpochClock.INSTANCE, truncateFileOnPageStraddle);
    }

    static void verify(
        final PrintStream out,
        final File archiveDir,
        final boolean validateAllSegmentFiles,
        final EpochClock epochClock,
        final ActionConfirmation<File> truncateFileOnPageStraddle)
    {
        try (Catalog catalog = openCatalog(archiveDir, epochClock))
        {
            catalog.forEach(createVerifyEntryProcessor(out, archiveDir, validateAllSegmentFiles, epochClock,
                truncateFileOnPageStraddle));
        }
    }

    private static CatalogEntryProcessor createVerifyEntryProcessor(
        final PrintStream out,
        final File archiveDir,
        final boolean validateAllSegmentFiles,
        final EpochClock epochClock,
        final ActionConfirmation<File> truncateFileOnPageStraddle)
    {
        final ByteBuffer buffer = allocateDirectAligned(HEADER_LENGTH, FRAME_ALIGNMENT);
        buffer.order(RecordingDescriptorDecoder.BYTE_ORDER);
        final UnsafeBuffer tempBuffer = new UnsafeBuffer(buffer);
        final DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight(tempBuffer);
        return (headerEncoder, headerDecoder, encoder, decoder) ->
            verifyRecording(out, archiveDir, validateAllSegmentFiles, epochClock, truncateFileOnPageStraddle,
                tempBuffer, headerFlyweight, headerEncoder, encoder, decoder);
    }

    /**
     * Verify descriptor in the catalog according to recordingId, checking recording files availability and contents.
     * Faulty entries are marked as unusable
     *
     * @param out                        output stream to print results and errors to
     * @param archiveDir                 that contains Markfile, Catalog, and recordings.
     * @param recordingId                to verify.
     * @param validateAllSegmentFiles    when <tt>true</tt> then all of the segment files will be validated, otherwise
     *                                   only the last segment file will be validated.
     * @param truncateFileOnPageStraddle action to perform if last fragment in the max segment file straddles the page
     *                                   boundary, i.e. if <tt>true</tt> the file will be truncated (last fragment
     *                                   will be deleted), if <tt>false</tt> the fragment if considered complete.
     * @throws AeronException if there is no recording with <tt>recordingId</tt> in the archive
     */
    public static void verifyRecording(
        final PrintStream out,
        final File archiveDir,
        final long recordingId,
        final boolean validateAllSegmentFiles,
        final ActionConfirmation<File> truncateFileOnPageStraddle)
    {
        verifyRecording(out, archiveDir, recordingId, validateAllSegmentFiles, SystemEpochClock.INSTANCE,
            truncateFileOnPageStraddle);
    }

    static void verifyRecording(
        final PrintStream out,
        final File archiveDir,
        final long recordingId,
        final boolean validateAllSegmentFiles,
        final EpochClock epochClock,
        final ActionConfirmation<File> truncateFileOnPageStraddle)
    {
        try (Catalog catalog = openCatalog(archiveDir, epochClock))
        {
            if (!catalog.forEntry(recordingId, createVerifyEntryProcessor(out, archiveDir, validateAllSegmentFiles,
                epochClock, truncateFileOnPageStraddle)))
            {
                throw new AeronException("No recording found with recordinId: " + recordingId);
            }
        }
    }

    /**
     * Migrate previous archive MarkFile, Catalog, and recordings from previous version to latest version.
     *
     * @param out        output stream to print results and errors to
     * @param archiveDir that contains MarkFile, Catalog and recordings.
     */
    public static void migrate(final PrintStream out, final File archiveDir)
    {
        final EpochClock epochClock = SystemEpochClock.INSTANCE;
        try (ArchiveMarkFile markFile = openMarkFileReadWrite(archiveDir, epochClock);
            Catalog catalog = openCatalogReadWrite(archiveDir, epochClock))
        {
            out.println("MarkFile version=" + fullVersionString(markFile.decoder().version()));
            out.println("Catalog version=" + fullVersionString(catalog.version()));
            out.println("Latest version=" + fullVersionString(ArchiveMarkFile.SEMANTIC_VERSION));

            final List<ArchiveMigrationStep> steps = ArchiveMigrationPlanner.createPlan(markFile.decoder().version());

            for (final ArchiveMigrationStep step : steps)
            {
                out.println("Migration step " + step.toString());
                step.migrate(out, markFile, catalog, archiveDir);
            }
        }
        catch (final Exception ex)
        {
            ex.printStackTrace(out);
        }
    }

    private static ArchiveMarkFile openMarkFile(
        final File archiveDir, final EpochClock epochClock, final Consumer<String> logger)
    {
        return new ArchiveMarkFile(archiveDir, ArchiveMarkFile.FILENAME, epochClock, TimeUnit.SECONDS.toMillis(5),
            logger);
    }

    static Catalog openCatalogReadOnly(final File archiveDir, final EpochClock epochClock)
    {
        return new Catalog(archiveDir, epochClock);
    }

    private static Catalog openCatalog(final File archiveDir, final EpochClock epochClock)
    {
        return new Catalog(archiveDir, epochClock, true, null);
    }

    private static ArchiveMarkFile openMarkFileReadWrite(final File archiveDir, final EpochClock epochClock)
    {
        return new ArchiveMarkFile(
            archiveDir,
            ArchiveMarkFile.FILENAME,
            epochClock,
            TimeUnit.SECONDS.toMillis(5),
            (version) ->
            {
            },
            null);
    }

    private static Catalog openCatalogReadWrite(final File archiveDir, final EpochClock epochClock)
    {
        return new Catalog(archiveDir, epochClock, true, (version) ->
        {
        });
    }

    private static void dump(
        final PrintStream out,
        final File archiveDir,
        final Catalog catalog,
        final long dataFragmentLimit,
        final ActionConfirmation<Long> continueOnFragmentLimit,
        final RecordingDescriptorHeaderDecoder header,
        final RecordingDescriptorDecoder descriptor)
    {
        final long stopPosition = descriptor.stopPosition();
        final long streamLength = stopPosition - descriptor.startPosition();

        out.printf("%n%nRecording %d %n  channel: %s%n  streamId: %d%n  stream length: %d%n",
            descriptor.recordingId(), descriptor.strippedChannel(), descriptor.streamId(),
            NULL_POSITION == stopPosition ? NULL_POSITION : streamLength);
        out.println(header);
        out.println(descriptor);

        if (0 == streamLength)
        {
            out.println("Recording is empty");
            return;
        }

        final RecordingReader reader = new RecordingReader(
            catalog.recordingSummary(descriptor.recordingId(), new RecordingSummary()),
            archiveDir, descriptor.startPosition(), NULL_POSITION);

        boolean isContinue = true;
        long fragmentCount = dataFragmentLimit;
        do
        {
            out.println();
            out.print("Frame at position [" + reader.replayPosition() + "] ");
            reader.poll((buffer, offset, length, frameType, flags, reservedValue) ->
            {
                out.println("data at offset [" + offset + "] with length = " + length);
                if (HDR_TYPE_PAD == frameType)
                {
                    out.println("PADDING FRAME");
                }
                else if (HDR_TYPE_DATA == frameType)
                {
                    if ((flags & UNFRAGMENTED) != UNFRAGMENTED)
                    {
                        String suffix = (flags & BEGIN_FRAG_FLAG) == BEGIN_FRAG_FLAG ? "BEGIN_FRAGMENT" : "";
                        suffix += (flags & END_FRAG_FLAG) == END_FRAG_FLAG ? "END_FRAGMENT" : "";
                        out.println("Fragmented frame. " + suffix);
                    }
                    out.println(PrintBufferUtil.prettyHexDump(buffer, offset, length));
                }
                else
                {
                    out.println("Unexpected frame type " + frameType);
                }
            }, 1);

            if (--fragmentCount == 0)
            {
                fragmentCount = dataFragmentLimit;
                if (NULL_POSITION != stopPosition)
                {
                    out.printf("%d bytes (from %d) remaining in recording %d%n",
                        streamLength - reader.replayPosition(), streamLength, descriptor.recordingId());
                }
                isContinue = continueOnFragmentLimit.confirm(dataFragmentLimit);
            }
        }
        while (!reader.isDone() && isContinue);
    }

    private static void printMarkInformation(final ArchiveMarkFile markFile, final PrintStream out)
    {
        out.format("%1$tH:%1$tM:%1$tS (start: %2tF %2$tH:%2$tM:%2$tS, activity: %3tF %3$tH:%3$tM:%3$tS)%n",
            new Date(), new Date(markFile.decoder().startTimestamp()),
            new Date(markFile.activityTimestampVolatile()));
        out.println(markFile.decoder());
    }

    private static void verifyRecording(
        final PrintStream out,
        final File archiveDir,
        final boolean validateAllSegmentFiles,
        final EpochClock epochClock,
        final ActionConfirmation<File> truncateFileOnPageStraddle,
        final UnsafeBuffer tempBuffer,
        final DataHeaderFlyweight headerFlyweight,
        final RecordingDescriptorHeaderEncoder headerEncoder,
        final RecordingDescriptorEncoder encoder,
        final RecordingDescriptorDecoder decoder)
    {
        final long recordingId = decoder.recordingId();
        final int segmentFileLength = decoder.segmentFileLength();
        final int termBufferLength = decoder.termBufferLength();
        final long startPosition = decoder.startPosition();

        final String[] segmentFiles = listSegmentFiles(archiveDir, recordingId);
        final String maxSegmentFile;
        final long stopPosition;
        try
        {
            maxSegmentFile = findSegmentFileWithHighestPosition(segmentFiles);
            stopPosition = computeStopPosition(archiveDir, maxSegmentFile, startPosition, termBufferLength,
                segmentFileLength, truncateFileOnPageStraddle::confirm);
        }
        catch (final Exception ex)
        {
            final String message = ex.getMessage();
            out.println("(recordingId=" + recordingId + ") ERR: " + (null != message ? message : ex.toString()));
            headerEncoder.valid(INVALID);
            return;
        }

        if (maxSegmentFile != null)
        {
            final int streamId = decoder.streamId();
            if (validateAllSegmentFiles)
            {
                for (final String filename : segmentFiles)
                {
                    if (validateSegmentFile(out, archiveDir, recordingId, filename, startPosition, termBufferLength,
                        segmentFileLength, streamId, decoder.initialTermId(), tempBuffer, headerFlyweight))
                    {
                        headerEncoder.valid(INVALID);
                        return;
                    }
                }
            }
            else if (validateSegmentFile(out, archiveDir, recordingId, maxSegmentFile, startPosition, termBufferLength,
                segmentFileLength, streamId, decoder.initialTermId(), tempBuffer, headerFlyweight))
            {
                headerEncoder.valid(INVALID);
                return;
            }
        }
        if (stopPosition != decoder.stopPosition())
        {
            encoder.stopPosition(stopPosition);
            encoder.stopTimestamp(epochClock.time());
        }
        headerEncoder.valid(VALID);
        out.println("(recordingId=" + recordingId + ") OK");
    }

    private static boolean validateSegmentFile(
        final PrintStream out,
        final File archiveDir,
        final long recordingId,
        final String fileName,
        final long startPosition,
        final int termBufferLength,
        final int segmentFileLength,
        final int streamId,
        final int initialTermId,
        final UnsafeBuffer tempBuffer,
        final DataHeaderFlyweight headerFlyweight)
    {
        final File file = new File(archiveDir, fileName);
        try (FileChannel channel = FileChannel.open(file.toPath(), READ))
        {
            final long maxSize = min(segmentFileLength, channel.size());
            int position = 0;
            long streamPosition = segmentFileBasePosition(startPosition, parseSegmentFilePosition(fileName),
                termBufferLength, segmentFileLength);
            do
            {
                tempBuffer.byteBuffer().clear();
                if (HEADER_LENGTH != channel.read(tempBuffer.byteBuffer(), position))
                {
                    out.println("(recordingId=" + recordingId + ") ERR: failed to read fragment header.");
                    return true;
                }
                if (0 == headerFlyweight.frameLength())
                {
                    break;
                }
                final int termId = computeTermIdFromPosition(streamPosition, positionBitsToShift(termBufferLength),
                    initialTermId);
                final int termOffset = (int)(streamPosition & (termBufferLength - 1));
                if (isInvalidHeader(tempBuffer, streamId, termId, termOffset))
                {
                    out.println("(recordingId=" + recordingId + ") ERR: fragment " +
                        "termOffset=" + headerFlyweight.termOffset() + " (expected=" + termOffset + "), " +
                        "termId=" + headerFlyweight.termId() + " (expected=" + termId + "), " +
                        "sessionId=" + headerFlyweight.streamId() + " (expected=" + streamId + ")"
                    );
                    return true;
                }

                position += align(headerFlyweight.frameLength(), FRAME_ALIGNMENT);
                streamPosition += position;
            }
            while (position < maxSize);
        }
        catch (final IOException ex)
        {
            out.println("(recordingId=" + recordingId + ") ERR: failed to verify file:" + file);
            ex.printStackTrace(out);
            return true;
        }

        return false;
    }

    private static void printErrors(final PrintStream out, final ArchiveMarkFile markFile)
    {
        out.println("Archive error log:");
        CommonContext.printErrorLog(markFile.errorBuffer(), out);

        final MarkFileHeaderDecoder decoder = markFile.decoder();
        decoder.skipControlChannel();
        decoder.skipLocalControlChannel();
        decoder.skipEventsChannel();
        final String aeronDirectory = decoder.aeronDirectory();

        out.println();
        out.println("Aeron driver error log (directory: " + aeronDirectory + "):");
        final File cncFile = new File(aeronDirectory, CncFileDescriptor.CNC_FILE);

        final MappedByteBuffer cncByteBuffer = IoUtil.mapExistingFile(cncFile, FileChannel.MapMode.READ_ONLY, "cnc");
        final DirectBuffer cncMetaDataBuffer = CncFileDescriptor.createMetaDataBuffer(cncByteBuffer);
        final int cncVersion = cncMetaDataBuffer.getInt(CncFileDescriptor.cncVersionOffset(0));

        CncFileDescriptor.checkVersion(cncVersion);
        CommonContext.printErrorLog(CncFileDescriptor.createErrorLogBuffer(cncByteBuffer, cncMetaDataBuffer), out);
    }

}
