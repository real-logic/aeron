package io.aeron.archive;

import io.aeron.archive.codecs.RecordingDescriptorDecoder;
import io.aeron.archive.codecs.RecordingDescriptorEncoder;
import io.aeron.archive.codecs.RecordingDescriptorHeaderDecoder;
import io.aeron.archive.codecs.RecordingDescriptorHeaderEncoder;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static io.aeron.archive.Archive.Configuration.RECORDING_SEGMENT_POSTFIX;
import static io.aeron.archive.Archive.segmentFileName;
import static io.aeron.archive.Catalog.INVALID;
import static io.aeron.archive.Catalog.VALID;
import static java.nio.file.StandardOpenOption.READ;

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
            try (Catalog catalog = openCatalog())
            {
                catalog.forEach((he, hd, e, d) -> System.out.println(d));
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

    private static Catalog openCatalog()
    {
        return new Catalog(archiveDir, null, 0, System::currentTimeMillis, false);
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
        final long stopPosition = decoder.stopPosition();
        final long recordingLength = stopPosition - startPosition;
        final long startSegmentOffset = startPosition & (termBufferLength - 1);
        final long dataLength = startSegmentOffset + recordingLength;
        final long endSegmentOffset = dataLength & (segmentFileLength - 1);

        final int recordingFileCount = (int)((dataLength + segmentFileLength - 1) / segmentFileLength);

        final String prefix = recordingId + ".";
        final boolean[] filesFound = new boolean[recordingFileCount];
        for (final String fileName : archiveDir.list((dir, name) -> name.startsWith(prefix)))
        {
            try
            {
                final int index = Integer.valueOf(
                    fileName.substring(prefix.length(), fileName.length() - RECORDING_SEGMENT_POSTFIX.length()));
                filesFound[index] = true;
            }
            catch (final Exception ex)
            {
                System.err.println("(recordingId=" + recordingId + ") ERR: malformed recording filename:" + fileName);
                ex.printStackTrace(System.err);
                headerEncoder.valid(INVALID);
                return;
            }
        }

        for (int i = 0; i < filesFound.length; i++)
        {
            if (!filesFound[i])
            {
                System.err.println("(recordingId=" + recordingId + ") ERR: missing recording file :" + i);
                headerEncoder.valid(INVALID);
                return;
            }
        }

        if (verifyFirstFile(recordingId, decoder, startSegmentOffset))
        {
            headerEncoder.valid(INVALID);
            return;
        }

        if (verifyLastFile(recordingId, recordingFileCount, endSegmentOffset))
        {
            headerEncoder.valid(INVALID);
            return;
        }

        headerEncoder.valid(VALID);
        System.out.println("(recordingId=" + recordingId + ") OK");
    }

    private static boolean verifyLastFile(
        final long recordingId, final int recordingFileCount, final long endSegmentOffset)
    {
        final File lastSegmentFile = new File(archiveDir, segmentFileName(recordingId, recordingFileCount - 1));
        try (FileChannel lastFile = FileChannel.open(lastSegmentFile.toPath(), READ))
        {
            TEMP_BUFFER.clear();
            long position = 0L;
            do
            {
                TEMP_BUFFER.clear().limit(DataHeaderFlyweight.HEADER_LENGTH);
                if (lastFile.read(TEMP_BUFFER, position) != DataHeaderFlyweight.HEADER_LENGTH)
                {
                    System.err.println("(recordingId=" + recordingId + ") ERR: failed to read fragment header.");
                    return true;
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
            System.err.println("(recordingId=" + recordingId + ") ERR: failed to verify file:" +
                segmentFileName(recordingId, recordingFileCount - 1));
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
        System.out.println("  verify: verifies all descriptors in the file, checking recording files availability %n" +
            "and contents. Faulty entries are marked as unusable. Optionally specify a recording id%n" +
            "to verify a single recording.");
    }
}
