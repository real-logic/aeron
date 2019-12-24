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

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static io.aeron.archive.ArchiveTool.VerifyOption.*;
import static io.aeron.archive.Catalog.*;
import static io.aeron.archive.MigrationUtils.fullVersionString;
import static io.aeron.archive.ReplaySession.isInvalidHeader;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.logbuffer.FrameDescriptor.*;
import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_DATA;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_PAD;
import static java.lang.Math.min;
import static java.nio.file.StandardOpenOption.READ;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toMap;
import static org.agrona.BitUtil.align;
import static org.agrona.BufferUtil.allocateDirectAligned;
import static org.agrona.Checksums.crc32;

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
         * @return {@code true} confirms the action and {@code false} to reject the action
         */
        boolean confirm(T context);
    }

    @SuppressWarnings("MethodLength")
    public static void main(final String[] args)
    {
        if (args.length == 0 || args.length > 5)
        {
            printHelp();
            System.exit(-1);
        }

        final File archiveDir = new File(args[0]);
        if (!archiveDir.exists())
        {
            System.err.println("ERR: Archive folder not found: " + archiveDir.getAbsolutePath());
            printHelp();
            System.exit(-1);
        }

        if (args.length == 2 && args[1].equals("describe"))
        {
            describe(System.out, archiveDir);
        }
        else if (args.length == 3 && args[1].equals("describe"))
        {
            describeRecording(System.out, archiveDir, Long.parseLong(args[2]));
        }
        else if (args.length >= 2 && args[1].equals("dump"))
        {
            dump(
                System.out,
                archiveDir,
                args.length >= 3 ? Long.parseLong(args[2]) : Long.MAX_VALUE,
                ArchiveTool::continueOnFrameLimit);
        }
        else if (args.length == 2 && args[1].equals("errors"))
        {
            printErrors(System.out, archiveDir);
        }
        else if (args.length == 2 && args[1].equals("pid"))
        {
            System.out.println(pid(archiveDir));
        }
        else if (args.length >= 2 && args[1].equals("verify"))
        {
            if (args.length == 2)
            {
                verify(System.out, archiveDir, emptySet(), ArchiveTool::truncateFileOnPageStraddle);
            }
            else
            {
                if (!isValidOption(args, 2)) // starts with recordingId
                {
                    verifyRecording(
                        System.out,
                        archiveDir,
                        Long.parseLong(args[2]),
                        parseOptions(args, 3),
                        ArchiveTool::truncateFileOnPageStraddle);
                }
                else
                {
                    verify(
                        System.out,
                        archiveDir,
                        parseOptions(args, 3),
                        ArchiveTool::truncateFileOnPageStraddle);
                }
            }
        }
        else if (args.length == 2 && args[1].equals("count-entries"))
        {
            System.out.println(countEntries(archiveDir));
        }
        else if (args.length == 2 && args[1].equals("max-entries"))
        {
            System.out.println(maxEntries(archiveDir));
        }
        else if (args.length == 3 && args[1].equals("max-entries"))
        {
            System.out.println(maxEntries(archiveDir, Long.parseLong(args[2])));
        }
        else if (args.length == 2 && args[1].equals("migrate"))
        {
            System.out.print("WARNING: please ensure archive is not running and that a backup has been taken of " +
                "archive directory before attempting migration(s).");

            if (readContinueAnswer("Continue? (y/n)"))
            {
                migrate(System.out, archiveDir);
            }
        }
    }

    /**
     * Get the maximum number of entries supported by a {@link Catalog}.
     *
     * @param archiveDir containing the {@link Catalog}.
     * @return the maximum number of entries supported by a {@link Catalog}.
     */
    public static int maxEntries(final File archiveDir)
    {
        try (Catalog catalog = openCatalogReadOnly(archiveDir, SystemEpochClock.INSTANCE))
        {
            return catalog.maxEntries();
        }
    }

    /**
     * Set the maximum number of entries supported by a {@link Catalog}.
     *
     * @param archiveDir    containing the {@link Catalog}.
     * @param newMaxEntries value to set.
     * @return the maximum number of entries supported by a {@link Catalog} after update.
     */
    public static int maxEntries(final File archiveDir, final long newMaxEntries)
    {
        try (Catalog catalog = new Catalog(archiveDir, null, 0, newMaxEntries, SystemEpochClock.INSTANCE))
        {
            return catalog.maxEntries();
        }
    }

    /**
     * Describe the metadata for entries in the {@link Catalog}.
     *
     * @param out        to which the entries will be printed.
     * @param archiveDir containing the {@link Catalog}.
     */
    public static void describe(final PrintStream out, final File archiveDir)
    {
        try (Catalog catalog = openCatalogReadOnly(archiveDir, SystemEpochClock.INSTANCE);
            ArchiveMarkFile markFile = openMarkFile(archiveDir, out::println))
        {
            printMarkInformation(markFile, out);
            out.println("Catalog Max Entries: " + catalog.maxEntries());
            catalog.forEach((he, hd, e, d) -> out.println(d));
        }
    }

    /**
     * Describe the metadata for a entry in the {@link Catalog} identified by recording id.
     *
     * @param out         to which the entry will be printed.
     * @param archiveDir  containing the {@link Catalog}.
     * @param recordingId to identify the entry.
     */
    public static void describeRecording(final PrintStream out, final File archiveDir, final long recordingId)
    {
        try (Catalog catalog = openCatalogReadOnly(archiveDir, SystemEpochClock.INSTANCE))
        {
            catalog.forEntry(recordingId, (he, hd, e, d) -> out.println(d));
        }
    }

    /**
     * Count the number of entries in the {@link Catalog}.
     *
     * @param archiveDir containing the {@link Catalog}.
     * @return the number of entries in the {@link Catalog}.
     */
    public static int countEntries(final File archiveDir)
    {
        try (Catalog catalog = openCatalogReadOnly(archiveDir, SystemEpochClock.INSTANCE))
        {
            return catalog.countEntries();
        }
    }

    /**
     * Get the pid of the process for the {@link Archive}.
     *
     * @param archiveDir containing the {@link org.agrona.MarkFile}.
     * @return the pid of the process for the {@link Archive}.
     */
    public static long pid(final File archiveDir)
    {
        try (ArchiveMarkFile markFile = openMarkFile(archiveDir, null))
        {
            return markFile.decoder().pid();
        }
    }

    /**
     * Print the errors in the {@link org.agrona.MarkFile}.
     *
     * @param out        stream to which the errors will be printed.
     * @param archiveDir containing the {@link org.agrona.MarkFile}.
     */
    public static void printErrors(final PrintStream out, final File archiveDir)
    {
        try (ArchiveMarkFile markFile = openMarkFile(archiveDir, null))
        {
            printErrors(out, markFile);
        }
    }

    /**
     * Dump the contents of an {@link Archive} so it can be inspected or debugged. Each recording will have its
     * contents dumped up to fragmentCountLimit before requesting to continue.
     *
     * @param out                               to which the contents will be printed.
     * @param archiveDir                        containing the {@link Archive}.
     * @param fragmentCountLimit                limit of data fragments to print from each recording before continue.
     * @param confirmActionOnFragmentCountLimit confirm continue to dump at limit.
     */
    public static void dump(
        final PrintStream out,
        final File archiveDir,
        final long fragmentCountLimit,
        final ActionConfirmation<Long> confirmActionOnFragmentCountLimit)
    {
        try (Catalog catalog = openCatalog(archiveDir, SystemEpochClock.INSTANCE);
            ArchiveMarkFile markFile = openMarkFile(archiveDir, out::println))
        {
            printMarkInformation(markFile, out);
            out.println("Catalog Max Entries: " + catalog.maxEntries());

            out.println();
            out.println("Dumping up to " + fragmentCountLimit + " fragments per recording");
            catalog.forEach((headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) -> dump(
                out,
                archiveDir,
                catalog,
                fragmentCountLimit,
                confirmActionOnFragmentCountLimit,
                headerDecoder,
                descriptorDecoder));
        }
    }

    /**
     * Set of options for {@link #verify(PrintStream, File, Set, ActionConfirmation)} and
     * {@link #verify(PrintStream, File, Set, ActionConfirmation)} methods.
     */
    public enum VerifyOption
    {
        /**
         * Enables validation for all segment files of a given recording.
         * By default only last segment file is validated.
         */
        VALIDATE_ALL_SEGMENT_FILES("all"),
        /**
         * Perform CRC for each data frame within a segment file being validated.
         */
        APPLY_CRC("crc");

        private final String flag;

        VerifyOption(final String flag)
        {
            this.flag = flag;
        }

        private static final Map<String, VerifyOption> BY_FLAG = Stream.of(values())
            .collect(toMap(opt -> opt.flag, opt -> opt));

        public static boolean isValidOption(final String[] args, final int index)
        {
            return null != BY_FLAG.get(args[index]);
        }

        public static Set<VerifyOption> parseOptions(final String[] args, final int index)
        {
            final EnumSet<VerifyOption> options = EnumSet.noneOf(VerifyOption.class);
            for (int i = index; i < args.length; i++)
            {
                final VerifyOption option = BY_FLAG.get(args[index]);
                if (null != option)
                {
                    options.add(option);
                }
            }
            return options;
        }
    }

    /**
     * Verify descriptors in the catalog, checking recording files availability and contents.
     * Faulty entries are marked as unusable
     *
     * @param out                        output stream to print results and errors to
     * @param archiveDir                 that contains {@link org.agrona.MarkFile}, {@link Catalog}, and recordings.
     * @param options                    set of options that control verification behavior.
     * @param truncateFileOnPageStraddle action to perform if last fragment in the max segment file straddles the page
     *                                   boundary, i.e. if {@code true} the file will be truncated (last fragment
     */
    public static void verify(
        final PrintStream out,
        final File archiveDir,
        final Set<VerifyOption> options,
        final ActionConfirmation<File> truncateFileOnPageStraddle)
    {
        verify(out, archiveDir, options, SystemEpochClock.INSTANCE, truncateFileOnPageStraddle);
    }

    /**
     * Verify descriptor in the catalog according to recordingId, checking recording files availability and contents.
     * <p>
     * Faulty entries are marked as unusable.
     *
     * @param out                        output stream to print results and errors to
     * @param archiveDir                 that contains {@link org.agrona.MarkFile}, {@link Catalog}, and recordings.
     * @param recordingId                to verify.
     * @param options                    set of options that control verification behavior.
     * @param truncateFileOnPageStraddle action to perform if last fragment in the max segment file straddles the page
     *                                   boundary, i.e. if {@code true} the file will be truncated (last fragment
     *                                   will be deleted), if {@code false} the fragment if considered complete.
     * @throws AeronException if there is no recording with {@code recordingId} in the archive
     */
    public static void verifyRecording(
        final PrintStream out,
        final File archiveDir,
        final long recordingId,
        final Set<VerifyOption> options,
        final ActionConfirmation<File> truncateFileOnPageStraddle)
    {
        verifyRecording(
            out,
            archiveDir,
            recordingId,
            options,
            SystemEpochClock.INSTANCE,
            truncateFileOnPageStraddle);
    }

    /**
     * Migrate previous archive {@link org.agrona.MarkFile}, {@link Catalog}, and recordings from previous version
     * to latest version.
     *
     * @param out        output stream to print results and errors to.
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

    static void verify(
        final PrintStream out,
        final File archiveDir,
        final Set<VerifyOption> options,
        final EpochClock epochClock,
        final ActionConfirmation<File> truncateFileOnPageStraddle)
    {
        try (Catalog catalog = openCatalog(archiveDir, epochClock))
        {
            catalog.forEach(createVerifyEntryProcessor(
                out, archiveDir, options, epochClock, truncateFileOnPageStraddle));
        }
    }

    static void verifyRecording(
        final PrintStream out,
        final File archiveDir,
        final long recordingId,
        final Set<VerifyOption> options,
        final EpochClock epochClock,
        final ActionConfirmation<File> truncateFileOnPageStraddle)
    {
        try (Catalog catalog = openCatalog(archiveDir, epochClock))
        {
            if (!catalog.forEntry(recordingId, createVerifyEntryProcessor(
                out, archiveDir, options, epochClock, truncateFileOnPageStraddle)))
            {
                throw new AeronException("no recording found with recordingId: " + recordingId);
            }
        }
    }

    static Catalog openCatalogReadOnly(final File archiveDir, final EpochClock epochClock)
    {
        return new Catalog(archiveDir, epochClock);
    }

    private static Catalog openCatalog(final File archiveDir, final EpochClock epochClock)
    {
        return new Catalog(archiveDir, epochClock, true, null);
    }

    private static CatalogEntryProcessor createVerifyEntryProcessor(
        final PrintStream out,
        final File archiveDir,
        final Set<VerifyOption> options,
        final EpochClock epochClock,
        final ActionConfirmation<File> truncateFileOnPageStraddle)
    {
        final ByteBuffer buffer = allocateDirectAligned(computeMaxMessageLength(TERM_MAX_LENGTH), FRAME_ALIGNMENT);
        buffer.order(RecordingDescriptorDecoder.BYTE_ORDER);
        final DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight(buffer);

        return (headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) -> verifyRecording(
            out,
            archiveDir,
            options,
            epochClock,
            truncateFileOnPageStraddle,
            headerFlyweight,
            headerEncoder,
            descriptorEncoder,
            descriptorDecoder);
    }

    private static boolean truncateFileOnPageStraddle(final File maxSegmentFile)
    {
        return readContinueAnswer(String.format("Last fragment in the segment file: %s straddles the page boundary,%n" +
                "i.e. it is not possible to verify if it was written correctly.%n%n" +
                "Please choose the corrective action: (y) - to truncate the file and " +
                "(n) - to do nothing",
            maxSegmentFile.getAbsolutePath()));
    }

    private static boolean continueOnFrameLimit(final Long frameLimit)
    {
        return readContinueAnswer(String.format("Specified frame limit %d reached. Continue? (y/n)", frameLimit));
    }

    private static boolean readContinueAnswer(final String msg)
    {
        System.out.printf("%n" + msg + ": ");
        final String answer = new Scanner(System.in).nextLine();

        return answer.isEmpty() || answer.equalsIgnoreCase("y") || answer.equalsIgnoreCase("yes");
    }

    private static ArchiveMarkFile openMarkFile(final File archiveDir, final Consumer<String> logger)
    {
        return new ArchiveMarkFile(
            archiveDir, ArchiveMarkFile.FILENAME, SystemEpochClock.INSTANCE, TimeUnit.SECONDS.toMillis(5), logger);
    }

    private static ArchiveMarkFile openMarkFileReadWrite(final File archiveDir, final EpochClock epochClock)
    {
        return new ArchiveMarkFile(
            archiveDir,
            ArchiveMarkFile.FILENAME,
            epochClock,
            TimeUnit.SECONDS.toMillis(5),
            (version) -> {},
            null);
    }

    private static Catalog openCatalogReadWrite(final File archiveDir, final EpochClock epochClock)
    {
        return new Catalog(archiveDir, epochClock, true, (version) -> {});
    }

    private static void dump(
        final PrintStream out,
        final File archiveDir,
        final Catalog catalog,
        final long fragmentCountLimit,
        final ActionConfirmation<Long> continueActionOnFragmentLimit,
        final RecordingDescriptorHeaderDecoder header,
        final RecordingDescriptorDecoder descriptor)
    {
        final long stopPosition = descriptor.stopPosition();
        final long streamLength = stopPosition - descriptor.startPosition();

        out.printf("%n%nRecording %d %n  channel: %s%n  streamId: %d%n  stream length: %d%n",
            descriptor.recordingId(),
            descriptor.strippedChannel(),
            descriptor.streamId(),
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
            archiveDir,
            descriptor.startPosition(),
            NULL_POSITION);

        boolean isActive = true;
        long fragmentCount = fragmentCountLimit;
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
                fragmentCount = fragmentCountLimit;
                if (NULL_POSITION != stopPosition)
                {
                    out.printf("%d bytes (from %d) remaining in recording %d%n",
                        streamLength - reader.replayPosition(), streamLength, descriptor.recordingId());
                }

                isActive = continueActionOnFragmentLimit.confirm(fragmentCountLimit);
            }
        }
        while (!reader.isDone() && isActive);
    }

    private static void printMarkInformation(final ArchiveMarkFile markFile, final PrintStream out)
    {
        out.format("%1$tH:%1$tM:%1$tS (start: %2tF %2$tH:%2$tM:%2$tS, activity: %3tF %3$tH:%3$tM:%3$tS)%n",
            new Date(), new Date(markFile.decoder().startTimestamp()),
            new Date(markFile.activityTimestampVolatile()));
        out.println(markFile.decoder());
    }

    @SuppressWarnings("MethodLength")
    private static void verifyRecording(
        final PrintStream out,
        final File archiveDir,
        final Set<VerifyOption> options,
        final EpochClock epochClock,
        final ActionConfirmation<File> truncateFileOnPageStraddle,
        final DataHeaderFlyweight headerFlyweight,
        final RecordingDescriptorHeaderEncoder headerEncoder,
        final RecordingDescriptorEncoder encoder,
        final RecordingDescriptorDecoder decoder)
    {
        final long recordingId = decoder.recordingId();
        final long startPosition = decoder.startPosition();
        final long stopPosition = decoder.stopPosition();
        if (isPositionInvariantViolated(out, recordingId, startPosition, stopPosition))
        {
            headerEncoder.valid(INVALID);
            return;
        }

        final int segmentLength = decoder.segmentFileLength();
        final int termLength = decoder.termBufferLength();
        final String[] segmentFiles = listSegmentFiles(archiveDir, recordingId);
        final String maxSegmentFile;
        final long computedStopPosition;

        try
        {
            maxSegmentFile = findSegmentFileWithHighestPosition(segmentFiles);
            if (maxSegmentFile != null)
            {
                final long maxSegmentPosition = parseSegmentFilePosition(maxSegmentFile) + (segmentLength - 1);
                if (startPosition > maxSegmentPosition || stopPosition > maxSegmentPosition)
                {
                    out.println("(recordingId=" + recordingId + ") ERR: Invariant violation: startPosition=" +
                        startPosition + " and/or stopPosition=" + stopPosition + " exceed max segment file position=" +
                        maxSegmentPosition);
                    headerEncoder.valid(INVALID);
                    return;
                }
            }

            computedStopPosition = computeStopPosition(
                archiveDir,
                maxSegmentFile,
                startPosition,
                termLength,
                segmentLength,
                truncateFileOnPageStraddle::confirm);
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
            final boolean performCrc = options.contains(APPLY_CRC);
            if (options.contains(VALIDATE_ALL_SEGMENT_FILES))
            {
                for (final String filename : segmentFiles)
                {
                    if (isInvalidSegmentFile(
                        out,
                        archiveDir,
                        recordingId,
                        filename,
                        startPosition,
                        termLength,
                        segmentLength,
                        streamId,
                        decoder.initialTermId(),
                        performCrc,
                        headerFlyweight
                    ))
                    {
                        headerEncoder.valid(INVALID);
                        return;
                    }
                }
            }
            else if (isInvalidSegmentFile(
                out,
                archiveDir,
                recordingId,
                maxSegmentFile,
                startPosition,
                termLength,
                segmentLength,
                streamId,
                decoder.initialTermId(),
                performCrc,
                headerFlyweight
            ))
            {
                headerEncoder.valid(INVALID);
                return;
            }
        }

        if (computedStopPosition != stopPosition)
        {
            encoder.stopPosition(computedStopPosition);
            encoder.stopTimestamp(epochClock.time());
        }

        headerEncoder.valid(VALID);
        out.println("(recordingId=" + recordingId + ") OK");
    }

    private static boolean isPositionInvariantViolated(
        final PrintStream out, final long recordingId, final long startPosition, final long stopPosition)
    {
        if (startPosition < 0)
        {
            out.println("(recordingId=" + recordingId + ") ERR: Negative startPosition=" + startPosition);
            return true;
        }
        else if (isNotFrameAligned(startPosition))
        {
            out.println("(recordingId=" + recordingId + ") ERR: Non-aligned startPosition=" + startPosition);
            return true;
        }
        else if (stopPosition != NULL_POSITION)
        {
            if (stopPosition < startPosition)
            {
                out.println("(recordingId=" + recordingId + ") ERR: Invariant violation stopPosition=" +
                    stopPosition + " is before startPosition=" + startPosition);
                return true;
            }
            else if (isNotFrameAligned(stopPosition))
            {
                out.println("(recordingId=" + recordingId + ") ERR: Non-aligned stopPosition=" + stopPosition);
                return true;
            }
        }
        return false;
    }

    private static boolean isNotFrameAligned(final long position)
    {
        return 0 != (position & (FRAME_ALIGNMENT - 1));
    }

    private static boolean isInvalidSegmentFile(
        final PrintStream out,
        final File archiveDir,
        final long recordingId,
        final String fileName,
        final long startPosition,
        final int termLength,
        final int segmentLength,
        final int streamId,
        final int initialTermId,
        final boolean applyCrc,
        final DataHeaderFlyweight headerFlyweight)
    {
        final File file = new File(archiveDir, fileName);
        try (FileChannel channel = FileChannel.open(file.toPath(), READ))
        {
            final long offsetLimit = min(segmentLength, channel.size());
            final int positionBitsToShift = positionBitsToShift(termLength);
            final long startTermOffset = startPosition & (termLength - 1);
            final long startTermBasePosition = startPosition - startTermOffset;
            final long segmentFileBasePosition = parseSegmentFilePosition(fileName);

            final ByteBuffer byteBuffer = headerFlyweight.byteBuffer();
            final long bufferAddress = headerFlyweight.addressOffset();

            long fileOffset = segmentFileBasePosition == startTermBasePosition ? startTermOffset : 0;
            long position = segmentFileBasePosition + fileOffset;
            do
            {
                byteBuffer.clear().limit(HEADER_LENGTH);
                if (HEADER_LENGTH != channel.read(byteBuffer, fileOffset))
                {
                    out.println("(recordingId=" + recordingId + ", file=" + file +
                        ") ERR: failed to read fragment header");
                    return true;
                }

                final int frameLength = headerFlyweight.frameLength();
                if (0 == frameLength)
                {
                    break;
                }

                final int termId = computeTermIdFromPosition(position, positionBitsToShift, initialTermId);
                final int termOffset = (int)(position & (termLength - 1));
                if (isInvalidHeader(headerFlyweight, streamId, termId, termOffset))
                {
                    out.println("(recordingId=" + recordingId + ", file=" + file + ") ERR: fragment " +
                        "termOffset=" + headerFlyweight.termOffset() + " (expected=" + termOffset + "), " +
                        "termId=" + headerFlyweight.termId() + " (expected=" + termId + "), " +
                        "streamId=" + headerFlyweight.streamId() + " (expected=" + streamId + ")");
                    return true;
                }
                final int frameType = frameType(headerFlyweight, 0);
                final int sessionId = frameSessionId(headerFlyweight, 0);

                final int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);
                final int dataLength = alignedFrameLength - HEADER_LENGTH;
                byteBuffer.clear().limit(dataLength);
                if (dataLength != channel.read(byteBuffer, fileOffset + HEADER_LENGTH))
                {
                    out.println("(recordingId=" + recordingId + ", file=" + file + ") ERR: failed to read " +
                        dataLength + " byte(s) of data at offset " + (fileOffset + HEADER_LENGTH));
                    return true;
                }

                if (applyCrc && HDR_TYPE_DATA == frameType)
                {
                    final int checksum = crc32(0, bufferAddress, 0, dataLength);
                    if (checksum != sessionId)
                    {
                        out.println("(recordingId=" + recordingId + ", file=" + file + ") ERR: CRC failed " +
                            "recorder=" + sessionId + " (expected=" + checksum + ")");
                        return true;
                    }
                }

                fileOffset += alignedFrameLength;
                position += alignedFrameLength;
            }
            while (fileOffset < offsetLimit);
        }
        catch (final IOException ex)
        {
            out.println("(recordingId=" + recordingId + ", file=" + file + ") ERR: failed to verify file:" + file);
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

    private static void printHelp()
    {
        System.out.println("Usage: <archive-dir> <command>");
        System.out.println("  describe <optional recordingId>: prints out descriptor(s) in the catalog.");
        System.out.println("  dump <optional data fragment limit per recording>: prints descriptor(s)");
        System.out.println("     in the catalog and associated recorded data.");
        System.out.println("  errors: prints errors for the archive and media driver.");
        System.out.println("  pid: prints just PID of archive.");
        System.out.println("  verify <optional recordingId> <optional 'all'> <optional 'crc'>: verifies descriptor(s)" +
            " in the catalog");
        System.out.println("     checking recording files availability and contents. Only the last segment file is");
        System.out.println("     validated unless flag 'all' is specified, i.e. meaning validate all segment files.");
        System.out.println("     In order to perform CRC for each data frame specify the 'crc' flag.");
        System.out.println("     Faulty entries are marked as unusable.");
        System.out.println("  count-entries: queries the number of recording entries in the catalog.");
        System.out.println("  max-entries <optional number of entries>: gets or increases the maximum number of");
        System.out.println("     recording entries the catalog can store.");
        System.out.println("  migrate: migrate archive MarkFile, Catalog, and recordings to the latest version.");
    }
}
