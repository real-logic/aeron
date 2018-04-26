package io.aeron.archive;

import io.aeron.archive.codecs.RecordingDescriptorDecoder;
import io.aeron.archive.codecs.RecordingDescriptorEncoder;
import io.aeron.archive.codecs.RecordingDescriptorHeaderDecoder;
import io.aeron.archive.codecs.RecordingDescriptorHeaderEncoder;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.collections.ArrayUtil;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.aeron.archive.Archive.Configuration.RECORDING_SEGMENT_POSTFIX;
import static io.aeron.archive.Archive.segmentFileName;
import static io.aeron.archive.Catalog.INVALID;
import static io.aeron.archive.Catalog.VALID;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
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
            try (Catalog catalog = openCatalog();
                ArchiveMarkFile markFile = openMarkFile(System.out::println))
            {
                printMarkInformation(markFile);
                System.out.println("Catalog Max Entries: " + catalog.maxEntries());
                catalog.forEach((he, hd, e, d) -> System.out.println(d));
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
            try (Catalog catalog = openCatalog())
            {
                catalog.forEntry((he, hd, e, d) -> System.out.println(d), Long.valueOf(args[2]));
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
                catalog.forEntry(CatalogTool::verify, Long.valueOf(args[2]));
            }
        }
        // TODO: add a manual override tool to force mark entries as unusable
    }

    private static ArchiveMarkFile openMarkFile(final Consumer<String> logger)
    {
        return new ArchiveMarkFile(
            archiveDir, ArchiveMarkFile.FILENAME, System::currentTimeMillis, TimeUnit.SECONDS.toMillis(5), logger);
    }

    private static Catalog openCatalog()
    {
        return new Catalog(archiveDir, System::currentTimeMillis);
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
        int maxSegmentIndex = -1;

        if (NULL_POSITION == stopPosition)
        {
            final String prefix = recordingId + "-";
            String[] segmentFiles =
                archiveDir.list((dir, name) -> name.endsWith(RECORDING_SEGMENT_POSTFIX));

            if (null == segmentFiles)
            {
                segmentFiles = ArrayUtil.EMPTY_STRING_ARRAY;
            }

            for (final String filename : segmentFiles)
            {
                try
                {
                    final int index = Integer.valueOf(
                        filename.substring(prefix.length(), filename.length() - RECORDING_SEGMENT_POSTFIX.length()));
                    maxSegmentIndex = Math.max(index, maxSegmentIndex);
                }
                catch (final Exception ignore)
                {
                    System.err.println(
                        "(recordingId=" + recordingId + ") ERR: malformed recording filename:" + filename);
                    headerEncoder.valid(INVALID);
                    return;
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
                startSegmentOffset + (maxSegmentIndex * segmentFileLength) + stopSegmentOffset;

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

    private static boolean verifyFirstFile(
        final long recordingId, final RecordingDescriptorDecoder decoder, final long joinSegmentOffset)
    {
        final File firstSegmentFile = new File(archiveDir, segmentFileName(recordingId, 0));
        try (FileChannel firstFile = FileChannel.open(firstSegmentFile.toPath(), READ))
        {
            TEMP_BUFFER.clear();
            TEMP_BUFFER.limit(DataHeaderFlyweight.HEADER_LENGTH);
            if (firstFile.read(TEMP_BUFFER, joinSegmentOffset) != DataHeaderFlyweight.HEADER_LENGTH)
            {
                System.err.println("(recordingId=" + recordingId + ") ERR: missing reading first fragment header.");
                return true;
            }

            if (HEADER_FLYWEIGHT.sessionId() != decoder.sessionId())
            {
                System.err.println("(recordingId=" + recordingId + ") ERR: first fragment sessionId=" +
                    HEADER_FLYWEIGHT.sessionId() + " (expected=" + decoder.sessionId() + ")");
                return true;
            }

            if (HEADER_FLYWEIGHT.streamId() != decoder.streamId())
            {
                System.err.println("(recordingId=" + recordingId + ") ERR: first fragment sessionId=" +
                    HEADER_FLYWEIGHT.streamId() + " (expected=" + decoder.streamId() + ")");
                return true;
            }

            final int joinTermOffset = (int)joinSegmentOffset;
            if (HEADER_FLYWEIGHT.termOffset() != joinTermOffset)
            {
                System.err.println("(recordingId=" + recordingId + ") ERR: first fragment termOffset=" +
                    HEADER_FLYWEIGHT.termOffset() + " (expected=" + joinTermOffset + ")");
                return true;
            }

            final long joinTermId = decoder.initialTermId() + (decoder.startPosition() / decoder.termBufferLength());
            if (HEADER_FLYWEIGHT.termId() != joinTermId)
            {
                System.err.println("(recordingId=" + recordingId + ") ERR: first fragment termId=" +
                    HEADER_FLYWEIGHT.termId() + " (expected=" + joinTermId + ")");
                return true;
            }
        }
        catch (final Exception ex)
        {
            System.err.println("(recordingId=" + recordingId + ") ERR: fail to verify file:" +
                segmentFileName(recordingId, 0));
            ex.printStackTrace(System.err);
            return true;
        }

        return false;
    }

    private static void printHelp()
    {
        System.out.println("Usage: <archive-dir> <command> <optional recordingId>");
        System.out.println("  describe: prints out all descriptors in the file. Optionally specify a recording id" +
            " to describe a single recording.");
        System.out.println("  pid: prints just PID of archive.");
        System.out.println("  verify: verifies all descriptors in the file, checking recording files availability %n" +
            "and contents. Faulty entries are marked as unusable. Optionally specify a recording id%n" +
            "to verify a single recording.");
    }
}
