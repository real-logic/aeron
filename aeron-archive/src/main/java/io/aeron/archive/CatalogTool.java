package io.aeron.archive;

import io.aeron.archive.codecs.RecordingDescriptorDecoder;
import io.aeron.archive.codecs.RecordingDescriptorEncoder;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static io.aeron.archive.ArchiveUtil.recordingDataFileName;
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
            try (Catalog catalog = new Catalog(archiveDir, null, 0))
            {
                catalog.forEach((e, d) -> System.out.println(d));
            }
        }
        else if (args.length == 3 && args[1].equals("describe"))
        {
            try (Catalog catalog = new Catalog(archiveDir, null, 0))
            {
                catalog.forEntry(Long.valueOf(args[2]), (e, d) -> System.out.println(d));
            }
        }
        else if (args.length == 2 && args[1].equals("verify"))
        {
            try (Catalog catalog = new Catalog(archiveDir, null, 0))
            {
                catalog.forEach((e, d) -> verify(e, d));
            }
        }
        else if (args.length == 3 && args[1].equals("verify"))
        {
            try (Catalog catalog = new Catalog(archiveDir, null, 0))
            {
                catalog.forEntry(Long.valueOf(args[2]), (e, d) -> verify(e, d));
            }
        }
    }

    private static void verify(final RecordingDescriptorEncoder encoder, final RecordingDescriptorDecoder decoder)
    {
        final long recordingId = decoder.recordingId();
        final int segmentFileLength = decoder.segmentFileLength();
        final int termBufferLength = decoder.termBufferLength();
        final long startPosition = decoder.startPosition();
        final long stopPosition = decoder.stopPosition();
        final long recordingLength = stopPosition - startPosition;
        final long joinSegmentOffset = startPosition & (termBufferLength - 1);
        final long dataLength = joinSegmentOffset + recordingLength;
        final long endSegmentOffset = dataLength & (segmentFileLength - 1);

        final int recordingFileCount = (int) ((dataLength + segmentFileLength - 1) / segmentFileLength);

        final String prefix = recordingId + ".";
        final String[] recordingFileNames = archiveDir.list((dir, name) -> name.startsWith(prefix));
        final boolean[] filesFound = new boolean[recordingFileCount];
        for (final String recordingFileName : recordingFileNames)
        {
            try
            {
                final int index = Integer.valueOf(
                    recordingFileName.substring(
                        prefix.length(),
                        recordingFileName.length() - ArchiveUtil.RECORDING_SEGMENT_POSTFIX.length()));
                filesFound[index] = true;
            }
            catch (final Exception ex)
            {
                System.err.println("(recordingId=" + recordingId + ") ERR: malformed recording filename:" +
                    recordingFileName);
                ex.printStackTrace(System.err);
                return;
            }
        }

        for (int i = 0; i < filesFound.length; i++)
        {
            if (!filesFound[i])
            {
                System.err.println("(recordingId=" + recordingId + ") ERR: missing recording file :" + i);
                return;
            }
        }

        if (verifyFirstFile(recordingId, decoder, joinSegmentOffset))
        {
            return;
        }

        if (verifyLastFile(recordingId, recordingFileCount, endSegmentOffset))
        {
            return;
        }

        System.out.println("(recordingId=" + recordingId + ") OK");
    }

    private static boolean verifyLastFile(
        final long recordingId,
        final int recordingFileCount, final long endSegmentOffset)
    {
        final String recordingDataFileName = recordingDataFileName(recordingId, recordingFileCount - 1);
        try (FileChannel lastFile = FileChannel.open(new File(
            archiveDir,
            recordingDataFileName).toPath(), READ))
        {
            TEMP_BUFFER.clear();
            long position = 0L;
            do
            {
                TEMP_BUFFER.clear().limit(DataHeaderFlyweight.HEADER_LENGTH);
                if (lastFile.read(TEMP_BUFFER, position) != DataHeaderFlyweight.HEADER_LENGTH)
                {
                    System.err.println("(recordingId=" + recordingId + ") ERR: failed to read fragment header.");
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
            System.err.println("(recordingId=" + recordingId + ") ERR: failed to verify file:" + recordingDataFileName);
            ex.printStackTrace(System.err);
            return true;
        }
        return false;
    }

    private static boolean verifyFirstFile(
        final long recordingId,
        final RecordingDescriptorDecoder decoder,
        final long joinSegmentOffset)
    {
        final String recordingDataFileName = recordingDataFileName(recordingId, 0);
        try (FileChannel firstFile = FileChannel.open(new File(
            archiveDir,
            recordingDataFileName).toPath(), READ))
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

            final int joinTermOffset = (int) joinSegmentOffset;
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
            System.err.println("(recordingId=" + recordingId + ") ERR: fail to verify file:" + recordingDataFileName);
            ex.printStackTrace(System.err);
            return true;
        }
        return false;
    }

    private static void printHelp()
    {
        System.out.println("Usage: <archive-dir> <command> <optional recordingId>");
        System.out.println("  describe: prints out all descriptors in the file. Optionally specify a recording id as" +
            " second argument to describe a single recording.");
        System.out.println("  verify: verifies all descriptors in the file, checking recording files availability %n" +
            "and contents. Faulty entries are marked as unusable. Optionally specify a recording id as second%n" +
            "argument to verify a single recording.");
        System.out.println("  reverify: verify a descriptor and, if successful, mark it as usable.");
    }
}
