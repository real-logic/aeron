/*
 * Copyright 2014-2019 Real Logic Ltd.
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

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.RecordingDescriptorDecoder;
import io.aeron.archive.codecs.RecordingDescriptorEncoder;
import io.aeron.archive.codecs.RecordingDescriptorHeaderDecoder;
import io.aeron.archive.codecs.RecordingDescriptorHeaderEncoder;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.AsciiEncoding;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.PrintBufferUtil;
import org.agrona.collections.ArrayUtil;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Date;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.aeron.archive.Archive.Configuration.RECORDING_SEGMENT_POSTFIX;
import static io.aeron.archive.Archive.segmentFileName;
import static io.aeron.archive.Catalog.INVALID;
import static io.aeron.archive.Catalog.VALID;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.logbuffer.FrameDescriptor.*;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_DATA;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_PAD;
import static java.nio.file.StandardOpenOption.READ;

/**
 * Tool for getting a listing from or verifying the archive catalog.
 */
public class CatalogTool
{
    private static final ByteBuffer TEMP_BUFFER =
        BufferUtil.allocateDirectAligned(4096, FrameDescriptor.FRAME_ALIGNMENT);
    private static final DataHeaderFlyweight HEADER_FLYWEIGHT = new DataHeaderFlyweight(TEMP_BUFFER);

    private static File archiveDir;

    public static void main(final String[] args)
    {
        if (args.length == 0 || args.length > 3)
        {
            printHelp();
            System.exit(-1);
        }

        archiveDir = new File(args[0]);
        if (!archiveDir.exists())
        {
            System.err.println("ERR: Archive folder not found: " + archiveDir.getAbsolutePath());
            printHelp();
            System.exit(-1);
        }

        if (args.length == 2 && args[1].equals("describe"))
        {
            try (Catalog catalog = openCatalogReadOnly();
                ArchiveMarkFile markFile = openMarkFile(System.out::println))
            {
                printMarkInformation(markFile);
                System.out.println("Catalog Max Entries: " + catalog.maxEntries());
                catalog.forEach((he, hd, e, d) -> System.out.println(d));
            }
        }
        else if (args.length >= 2 && args[1].equals("dump"))
        {
            try (Catalog catalog = openCatalog();
                ArchiveMarkFile markFile = openMarkFile(System.out::println))
            {
                printMarkInformation(markFile);
                System.out.println("Catalog Max Entries: " + catalog.maxEntries());

                System.out.println();
                final long dataFragmentLimit = args.length >= 3 ? Long.parseLong(args[2]) : Long.MAX_VALUE;
                System.out.println("Dumping " + dataFragmentLimit + " fragments per recording");
                catalog.forEach((he, headerDecoder, e, descriptorDecoder) ->
                    dump(catalog, dataFragmentLimit, headerDecoder, descriptorDecoder));
            }
        }
        else if (args.length == 2 && args[1].equals("pid"))
        {
            try (ArchiveMarkFile markFile = openMarkFile(null))
            {
                System.out.println(markFile.decoder().pid());
            }
        }
        else if (args.length == 3 && args[1].equals("describe"))
        {
            try (Catalog catalog = openCatalogReadOnly())
            {
                catalog.forEntry(Long.valueOf(args[2]), (he, hd, e, d) -> System.out.println(d));
            }
        }
        else if (args.length == 2 && args[1].equals("verify"))
        {
            try (Catalog catalog = openCatalog())
            {
                catalog.forEach(CatalogTool::verify);
            }
        }
        else if (args.length == 3 && args[1].equals("verify"))
        {
            try (Catalog catalog = openCatalog())
            {
                catalog.forEntry(Long.valueOf(args[2]), CatalogTool::verify);
            }
        }
        else if (args.length == 2 && args[1].equals("count-entries"))
        {
            try (Catalog catalog = openCatalogReadOnly())
            {
                System.out.println(catalog.countEntries());
            }
        }
        else if (args.length == 2 && args[1].equals("max-entries"))
        {
            try (Catalog catalog = openCatalogReadOnly())
            {
                System.out.println(catalog.maxEntries());
            }
        }
        else if (args.length == 3 && args[1].equals("max-entries"))
        {
            final long newMaxEntries = Long.parseLong(args[2]);

            try (Catalog catalog = new Catalog(archiveDir, null, 0, newMaxEntries, System::currentTimeMillis))
            {
                System.out.println(catalog.maxEntries());
            }
        }
    }

    private static void dump(
        final Catalog catalog,
        final long dataFragmentLimit,
        final RecordingDescriptorHeaderDecoder header,
        final RecordingDescriptorDecoder descriptor)
    {
        final long stopPosition = descriptor.stopPosition();
        final long streamLength = stopPosition - descriptor.startPosition();

        System.out.printf(
            "%n%nRecording %d %n  channel: %s%n  streamId: %d%n  stream length: %d%n",
            descriptor.recordingId(),
            descriptor.strippedChannel(),
            descriptor.streamId(),
            AeronArchive.NULL_POSITION == stopPosition ? AeronArchive.NULL_POSITION : streamLength);
        System.out.println(header);
        System.out.println(descriptor);

        if (0 == streamLength)
        {
            System.out.println("Recording is empty");
            return;
        }

        final RecordingReader reader = new RecordingReader(
            catalog,
            catalog.recordingSummary(descriptor.recordingId(), new RecordingSummary()),
            archiveDir,
            descriptor.startPosition(),
            AeronArchive.NULL_POSITION,
            null);

        boolean isContinue = true;
        long fragmentCount = dataFragmentLimit;
        do
        {
            System.out.println();
            System.out.print("Frame at position [" + reader.replayPosition() + "] ");
            reader.poll(
                (buffer, offset, length, frameType, flags, reservedValue) ->
                {
                    System.out.println("data at offset [" + offset + "] with length = " + length);
                    if (HDR_TYPE_PAD == frameType)
                    {
                        System.out.println("PADDING FRAME");
                    }
                    else if (HDR_TYPE_DATA == frameType)
                    {
                        if ((flags & UNFRAGMENTED) != UNFRAGMENTED)
                        {
                            String suffix = (flags & BEGIN_FRAG_FLAG) == BEGIN_FRAG_FLAG ? "BEGIN_FRAGMENT" : "";
                            suffix += (flags & END_FRAG_FLAG) == END_FRAG_FLAG ? "END_FRAGMENT" : "";
                            System.out.println("Fragmented frame. " + suffix);
                        }
                        System.out.println(PrintBufferUtil.prettyHexDump(buffer, offset, length));
                    }
                    else
                    {
                        System.out.println("Unexpected frame type " + frameType);
                    }
                },
                1);

            if (--fragmentCount == 0)
            {
                fragmentCount = dataFragmentLimit;
                if (NULL_POSITION != stopPosition)
                {
                    System.out.printf(
                        "%d bytes (from %d) remaining in recording %d%n",
                        streamLength - reader.replayPosition(),
                        streamLength,
                        descriptor.recordingId());
                }
                isContinue = readContinueAnswer();
            }
        }
        while (!reader.isDone() && isContinue);
    }

    private static boolean readContinueAnswer()
    {
        System.out.printf("%nContinue? (y/n): ");
        final String answer = new Scanner(System.in).nextLine();

        return answer.isEmpty() || answer.equalsIgnoreCase("y") || answer.equalsIgnoreCase("yes");
    }

    private static ArchiveMarkFile openMarkFile(final Consumer<String> logger)
    {
        return new ArchiveMarkFile(
            archiveDir, ArchiveMarkFile.FILENAME, System::currentTimeMillis, TimeUnit.SECONDS.toMillis(5), logger);
    }

    private static Catalog openCatalogReadOnly()
    {
        return new Catalog(archiveDir, System::currentTimeMillis);
    }

    private static Catalog openCatalog()
    {
        return new Catalog(archiveDir, System::currentTimeMillis, true);
    }

    private static void printMarkInformation(final ArchiveMarkFile markFile)
    {
        System.out.format(
            "%1$tH:%1$tM:%1$tS (start: %2tF %2$tH:%2$tM:%2$tS, activity: %3tF %3$tH:%3$tM:%3$tS)%n",
            new Date(),
            new Date(markFile.decoder().startTimestamp()),
            new Date(markFile.activityTimestampVolatile()));
        System.out.println(markFile.decoder());
    }

    private static void verify(
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
        int maxSegmentIndex = Aeron.NULL_VALUE;

        if (NULL_POSITION == stopPosition)
        {
            final String prefix = recordingId + "-";
            String[] segmentFiles = archiveDir.list(
                (dir, name) -> name.startsWith(prefix) && name.endsWith(RECORDING_SEGMENT_POSTFIX));

            if (null == segmentFiles)
            {
                segmentFiles = ArrayUtil.EMPTY_STRING_ARRAY;
            }

            for (final String filename : segmentFiles)
            {
                final int length = filename.length();
                final int offset = prefix.length();
                final int remaining = length - offset - RECORDING_SEGMENT_POSTFIX.length();

                if (remaining > 0)
                {
                    try
                    {
                        maxSegmentIndex = Math.max(
                            AsciiEncoding.parseIntAscii(filename, offset, remaining),
                            maxSegmentIndex);
                    }
                    catch (final Exception ignore)
                    {
                        System.err.println(
                            "(recordingId=" + recordingId + ") ERR: malformed recording filename:" + filename);
                        headerEncoder.valid(INVALID);
                        return;
                    }
                }
            }

            if (maxSegmentIndex < 0)
            {
                System.err.println(
                    "(recordingId=" + recordingId + ") ERR: no recording segment files");
                headerEncoder.valid(INVALID);
                return;
            }

            maxSegmentFile = new File(archiveDir, segmentFileName(recordingId, maxSegmentIndex));
            stopSegmentOffset = Catalog.recoverStopOffset(maxSegmentFile, segmentFileLength);

            final long recordingLength =
                startSegmentOffset + (maxSegmentIndex * (long)segmentFileLength) + stopSegmentOffset;

            stopPosition = startPosition + recordingLength;

            encoder.stopPosition(stopPosition);
            encoder.stopTimestamp(System.currentTimeMillis());
        }
        else
        {
            final long recordingLength = stopPosition - startPosition;
            final long dataLength = startSegmentOffset + recordingLength;

            stopSegmentOffset = dataLength & (segmentFileLength - 1);
            maxSegmentIndex = (int)((recordingLength - startSegmentOffset - stopSegmentOffset) / segmentFileLength);
            maxSegmentFile = new File(archiveDir, segmentFileName(recordingId, maxSegmentIndex));
        }

        if (!maxSegmentFile.exists())
        {
            System.err.println("(recordingId=" + recordingId + ") ERR: missing last recording file: " + maxSegmentFile);
            headerEncoder.valid(INVALID);
            return;
        }

        final long startOffset = ((stopPosition - startPosition) > segmentFileLength) ? 0L : startSegmentOffset;
        if (verifyLastFile(recordingId, maxSegmentFile, startOffset, stopSegmentOffset, decoder))
        {
            headerEncoder.valid(INVALID);
            return;
        }

        headerEncoder.valid(VALID);
        System.out.println("(recordingId=" + recordingId + ") OK");
    }

    private static boolean verifyLastFile(
        final long recordingId,
        final File lastSegmentFile,
        final long startOffset,
        final long endSegmentOffset,
        final RecordingDescriptorDecoder decoder)
    {
        try (FileChannel lastFile = FileChannel.open(lastSegmentFile.toPath(), READ))
        {
            TEMP_BUFFER.clear();
            long position = startOffset;
            do
            {
                TEMP_BUFFER.clear().limit(DataHeaderFlyweight.HEADER_LENGTH);
                if (lastFile.read(TEMP_BUFFER, position) != DataHeaderFlyweight.HEADER_LENGTH)
                {
                    System.err.println("(recordingId=" + recordingId + ") ERR: failed to read fragment header.");
                    return true;
                }

                if (HEADER_FLYWEIGHT.frameLength() != 0)
                {
                    if (HEADER_FLYWEIGHT.sessionId() != decoder.sessionId())
                    {
                        System.err.println("(recordingId=" + recordingId + ") ERR: fragment sessionId=" +
                            HEADER_FLYWEIGHT.sessionId() + " (expected=" + decoder.sessionId() + ")");
                        return true;
                    }

                    if (HEADER_FLYWEIGHT.streamId() != decoder.streamId())
                    {
                        System.err.println("(recordingId=" + recordingId + ") ERR: fragment sessionId=" +
                            HEADER_FLYWEIGHT.streamId() + " (expected=" + decoder.streamId() + ")");
                        return true;
                    }
                }

                position += BitUtil.align(HEADER_FLYWEIGHT.frameLength(), FrameDescriptor.FRAME_ALIGNMENT);
            }
            while (HEADER_FLYWEIGHT.frameLength() != 0);

            if (position != endSegmentOffset)
            {
                System.err.println("(recordingId=" + recordingId + ") ERR: end segment offset=" +
                    position + " (expected=" + endSegmentOffset + ")");
                return true;
            }
        }
        catch (final Exception ex)
        {
            System.err.println("(recordingId=" + recordingId + ") ERR: failed to verify file:" + lastSegmentFile);
            ex.printStackTrace(System.err);
            return true;
        }

        return false;
    }

    private static void printHelp()
    {
        System.out.println("Usage: <archive-dir> <command>");
        System.out.println("  describe <optional recordingId>: prints out descriptor(s) in the catalog.");
        System.out.println("  dump <optional data fragment limit per recording>: prints descriptor(s)");
        System.out.println("     in the catalog and associated recorded data.");
        System.out.println("  pid: prints just PID of archive.");
        System.out.println("  verify <optional recordingId>: verifies descriptor(s) in the catalog, checking");
        System.out.println("     recording files availability and contents. Faulty entries are marked as unusable.");
        System.out.println("  count-entries: queries the number of recording entries in the catalog.");
        System.out.println("  max-entries <optional number of entries>: gets or increases the maximum number of");
        System.out.println("     recording entries the catalog can store.");
    }
}
