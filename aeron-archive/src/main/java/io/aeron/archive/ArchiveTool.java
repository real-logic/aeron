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

import io.aeron.Aeron;
import io.aeron.CncFileDescriptor;
import io.aeron.CommonContext;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.RecordingDescriptorDecoder;
import io.aeron.archive.codecs.RecordingDescriptorEncoder;
import io.aeron.archive.codecs.RecordingDescriptorHeaderDecoder;
import io.aeron.archive.codecs.RecordingDescriptorHeaderEncoder;
import io.aeron.archive.codecs.mark.MarkFileHeaderDecoder;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.*;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.SystemEpochClock;

import java.io.File;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.aeron.archive.Archive.Configuration.RECORDING_SEGMENT_SUFFIX;
import static io.aeron.archive.Archive.segmentFileName;
import static io.aeron.archive.Catalog.INVALID;
import static io.aeron.archive.Catalog.VALID;
import static io.aeron.archive.MigrationUtils.fullVersionString;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.logbuffer.FrameDescriptor.*;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_DATA;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_PAD;
import static java.nio.file.StandardOpenOption.READ;

/**
 * Tool for inspecting and performing administrative tasks on an {@link Archive} and its contents which is described in
 * the {@link Catalog}.
 */
public class ArchiveTool
{
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

    public static void dump(final PrintStream out, final File archiveDir, final long dataFragmentLimit, final Supplier<Boolean> continueOnFragmentLimit)
    {
        try (Catalog catalog = openCatalog(archiveDir, SystemEpochClock.INSTANCE);
             ArchiveMarkFile markFile = openMarkFile(archiveDir, SystemEpochClock.INSTANCE, out::println))
        {
            printMarkInformation(markFile, out);
            out.println("Catalog Max Entries: " + catalog.maxEntries());

            out.println();
            out.println("Dumping " + dataFragmentLimit + " fragments per recording");
            catalog.forEach((he, headerDecoder, e, descriptorDecoder) ->
                                dump(out, archiveDir, catalog, dataFragmentLimit, headerDecoder, descriptorDecoder, continueOnFragmentLimit));
        }
    }

    /**
     * Verify descriptors in the catalog, checking recording files availability and contents.
     * Faulty entries are marked as unusable
     *
     * @param out        output stream to print results and errors to
     * @param archiveDir that contains Markfile, Catalog, and recordings.
     */
    public static void verify(final PrintStream out, final File archiveDir)
    {
        try (Catalog catalog = openCatalog(archiveDir, SystemEpochClock.INSTANCE))
        {
            catalog.forEach(createVerifyEntryProcessor(out, archiveDir));
        }
    }

    private static Catalog.CatalogEntryProcessor createVerifyEntryProcessor(final PrintStream out, final File archiveDir)
    {
        final ByteBuffer tempBuffer = BufferUtil.allocateDirectAligned(4096, FRAME_ALIGNMENT);
        final DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight(tempBuffer);
        return (headerEncoder, headerDecoder, encoder, decoder) ->
                   verifyRecording(out, archiveDir, tempBuffer, headerFlyweight, headerEncoder, headerDecoder, encoder, decoder);
    }

    /**
     * Verify descriptor in the catalog according to recordingId, checking recording files availability and contents.
     * Faulty entries are marked as unusable
     *
     * @param out         output stream to print results and errors to
     * @param archiveDir  that contains Markfile, Catalog, and recordings.
     * @param recordingId to verify.
     */
    public static void verifyRecording(final PrintStream out, final File archiveDir, final long recordingId)
    {
        try (Catalog catalog = openCatalog(archiveDir, SystemEpochClock.INSTANCE))
        {
            catalog.forEntry(recordingId, createVerifyEntryProcessor(out, archiveDir));
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
            out.println(
                "MarkFile version=" + fullVersionString(markFile.decoder().version()));
            out.println(
                "Catalog version=" + fullVersionString(catalog.version()));
            out.println(
                "Latest version=" + fullVersionString(ArchiveMarkFile.SEMANTIC_VERSION));

            final List<ArchiveMigrationStep> steps = ArchiveMigrationPlanner.createPlan(
                markFile.decoder().version());

            for (final ArchiveMigrationStep step : steps)
            {
                out.println("Migration step " + step.toString());
                step.migrate(markFile, catalog, archiveDir, out);
            }
        }
        catch (final Exception ex)
        {
            ex.printStackTrace(out);
        }
    }

    private static ArchiveMarkFile openMarkFile(File archiveDir, final EpochClock epochClock, final Consumer<String> logger)
    {
        return new ArchiveMarkFile(
            archiveDir, ArchiveMarkFile.FILENAME, epochClock, TimeUnit.SECONDS.toMillis(5), logger);
    }

    private static Catalog openCatalogReadOnly(File archiveDir, final EpochClock epochClock)
    {
        return new Catalog(archiveDir, epochClock);
    }

    private static Catalog openCatalog(File archiveDir, final EpochClock epochClock)
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
        final RecordingDescriptorHeaderDecoder header,
        final RecordingDescriptorDecoder descriptor,
        final Supplier<Boolean> continueOnFragmentLimit)
    {
        final long stopPosition = descriptor.stopPosition();
        final long streamLength = stopPosition - descriptor.startPosition();

        out.printf(
            "%n%nRecording %d %n  channel: %s%n  streamId: %d%n  stream length: %d%n",
            descriptor.recordingId(),
            descriptor.strippedChannel(),
            descriptor.streamId(),
            AeronArchive.NULL_POSITION == stopPosition ? AeronArchive.NULL_POSITION : streamLength);
        out.println(header);
        out.println(descriptor);

        if (0 == streamLength)
        {
            out.println("Recording is empty");
            return;
        }

        final RecordingReader reader = new RecordingReader(
            catalog.recordingSummary(descriptor.recordingId(), new RecordingSummary()),
            archiveDir,
            descriptor.startPosition(),
            AeronArchive.NULL_POSITION);

        boolean isContinue = true;
        long fragmentCount = dataFragmentLimit;
        do
        {
            out.println();
            out.print("Frame at position [" + reader.replayPosition() + "] ");
            reader.poll(
                (buffer, offset, length, frameType, flags, reservedValue) ->
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
                },
                1);

            if (--fragmentCount == 0)
            {
                fragmentCount = dataFragmentLimit;
                if (NULL_POSITION != stopPosition)
                {
                    out.printf(
                        "%d bytes (from %d) remaining in recording %d%n",
                        streamLength - reader.replayPosition(),
                        streamLength,
                        descriptor.recordingId());
                }
                isContinue = continueOnFragmentLimit.get();
            }
        }
        while (!reader.isDone() && isContinue);
    }

    private static void printMarkInformation(final ArchiveMarkFile markFile, final PrintStream out)
    {
        out.format(
            "%1$tH:%1$tM:%1$tS (start: %2tF %2$tH:%2$tM:%2$tS, activity: %3tF %3$tH:%3$tM:%3$tS)%n",
            new Date(),
            new Date(markFile.decoder().startTimestamp()),
            new Date(markFile.activityTimestampVolatile()));
        out.println(markFile.decoder());
    }

    private static void verifyRecording(
        final PrintStream out,
        final File archiveDir,
        final ByteBuffer tempBuffer,
        final DataHeaderFlyweight headerFlyweight,
        final RecordingDescriptorHeaderEncoder headerEncoder,
        final RecordingDescriptorHeaderDecoder headerDecoder,
        final RecordingDescriptorEncoder encoder,
        final RecordingDescriptorDecoder decoder)
    {
        final long recordingId = decoder.recordingId();
        final int segmentFileLength = decoder.segmentFileLength();
        final int termBufferLength = decoder.termBufferLength();
        final long startPosition = decoder.startPosition();
        final long startSegmentOffset = startPosition & (termBufferLength - 1);
        final long stopSegmentOffset;
        final File maxSegmentFile;

        long stopPosition = decoder.stopPosition();
        long maxSegmentPosition = Aeron.NULL_VALUE;

        if (NULL_POSITION == stopPosition)
        {
            final String prefix = recordingId + "-";
            String[] segmentFiles = archiveDir.list(
                (dir, name) -> name.startsWith(prefix) && name.endsWith(RECORDING_SEGMENT_SUFFIX));

            if (null == segmentFiles)
            {
                segmentFiles = ArrayUtil.EMPTY_STRING_ARRAY;
            }

            for (final String filename : segmentFiles)
            {
                final int length = filename.length();
                final int offset = prefix.length();
                final int remaining = length - offset - RECORDING_SEGMENT_SUFFIX.length();

                if (remaining > 0)
                {
                    try
                    {
                        maxSegmentPosition = Math.max(
                            AsciiEncoding.parseLongAscii(filename, offset, remaining),
                            maxSegmentPosition);
                    }
                    catch (final Exception ignore)
                    {
                        out.println(
                            "(recordingId=" + recordingId + ") ERR: malformed recording filename:" + filename);
                        headerEncoder.valid(INVALID);
                        return;
                    }
                }
            }

            if (maxSegmentPosition < 0)
            {
                out.println(
                    "(recordingId=" + recordingId + ") ERR: no recording segment files");
                headerEncoder.valid(INVALID);
                return;
            }

            maxSegmentFile = new File(archiveDir, segmentFileName(recordingId, maxSegmentPosition));
            stopSegmentOffset = Catalog.recoverStopOffset(maxSegmentFile, segmentFileLength);

            final long recordingLength = maxSegmentPosition + stopSegmentOffset - startSegmentOffset;

            stopPosition = startPosition + recordingLength;

            encoder.stopPosition(stopPosition);
            encoder.stopTimestamp(System.currentTimeMillis());
        }
        else
        {
            final long recordingLength = stopPosition - startPosition;
            final long dataLength = startSegmentOffset + recordingLength;

            stopSegmentOffset = dataLength & (segmentFileLength - 1);
            maxSegmentPosition = stopPosition - (stopPosition & (segmentFileLength - 1));
            maxSegmentFile = new File(archiveDir, segmentFileName(recordingId, maxSegmentPosition));
        }

        if (!maxSegmentFile.exists())
        {
            out.println("(recordingId=" + recordingId + ") ERR: missing last recording file: " + maxSegmentFile);
            headerEncoder.valid(INVALID);
            return;
        }

        final long startOffset = ((stopPosition - startPosition) > segmentFileLength) ? 0L : startSegmentOffset;

        if (verifyLastFile(tempBuffer, headerFlyweight, recordingId, maxSegmentFile, startOffset, stopSegmentOffset, decoder, out))
        {
            headerEncoder.valid(INVALID);
            return;
        }

        headerEncoder.valid(VALID);
        out.println("(recordingId=" + recordingId + ") OK");
    }

    private static boolean verifyLastFile(
        final ByteBuffer tempBuffer,
        final DataHeaderFlyweight headerFlyweight,
        final long recordingId,
        final File lastSegmentFile,
        final long startOffset,
        final long endSegmentOffset,
        final RecordingDescriptorDecoder decoder,
        final PrintStream out)
    {
        try (FileChannel lastFile = FileChannel.open(lastSegmentFile.toPath(), READ))
        {
            tempBuffer.clear();
            long position = startOffset;
            do
            {
                tempBuffer.clear().limit(DataHeaderFlyweight.HEADER_LENGTH);
                if (lastFile.read(tempBuffer, position) != DataHeaderFlyweight.HEADER_LENGTH)
                {
                    out.println("(recordingId=" + recordingId + ") ERR: failed to read fragment header.");
                    return true;
                }

                if (headerFlyweight.frameLength() != 0)
                {
                    if (headerFlyweight.sessionId() != decoder.sessionId())
                    {
                        out.println("(recordingId=" + recordingId + ") ERR: fragment sessionId=" +
                                        headerFlyweight.sessionId() + " (expected=" + decoder.sessionId() + ")");
                        return true;
                    }

                    if (headerFlyweight.streamId() != decoder.streamId())
                    {
                        out.println("(recordingId=" + recordingId + ") ERR: fragment sessionId=" +
                                        headerFlyweight.streamId() + " (expected=" + decoder.streamId() + ")");
                        return true;
                    }
                }

                position += BitUtil.align(headerFlyweight.frameLength(), FrameDescriptor.FRAME_ALIGNMENT);
            }
            while (headerFlyweight.frameLength() != 0);

            if (position != endSegmentOffset)
            {
                out.println("(recordingId=" + recordingId + ") ERR: end segment offset=" +
                                position + " (expected=" + endSegmentOffset + ")");
                return true;
            }
        }
        catch (final Exception ex)
        {
            out.println("(recordingId=" + recordingId + ") ERR: failed to verify file:" + lastSegmentFile);
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
