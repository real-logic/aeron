/*
 * Copyright 2014-2025 Real Logic Limited.
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

import io.aeron.archive.client.ArchiveException;
import org.agrona.BufferUtil;
import org.agrona.LangUtil;
import org.agrona.SemanticVersion;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.aeron.archive.Catalog.MIN_CAPACITY;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.*;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

final class ArchiveMigrationUtils
{
    static final int VERSION_2_1_0 = SemanticVersion.compose(2, 1, 0);

    private static final int MARK_FILE_HEADER_LENGTH_V2 = 8 * 1024;
    static final int RECORDING_FRAME_LENGTH_V2 = 1024;

    private ArchiveMigrationUtils()
    {
    }

    static ArchiveMarkFile createArchiveMarkFileInVersion2(final File archiveDir, final EpochClock epochClock)
    {
        final Path markFile = archiveDir.toPath().resolve(ArchiveMarkFile.FILENAME);
        try (FileChannel channel = FileChannel.open(markFile, CREATE_NEW, READ, WRITE, SPARSE))
        {
            final int errorBufferLength = 100;
            final String controlChannel = "ctx.controlChannel()";
            final String localControlChannel = "ctx.localControlChannel()";
            final String eventsChannel = "ctx.recordingEventsChannel()";
            final String aeronDirectory = "ctx.aeronDirectoryName()";

            final MappedByteBuffer mappedByteBuffer = channel.map(
                READ_WRITE,
                0,
                totalMarkFileLengthInVersion2(
                controlChannel, localControlChannel, eventsChannel, aeronDirectory, errorBufferLength));
            mappedByteBuffer.order(LITTLE_ENDIAN);
            try
            {
                final UnsafeBuffer buffer = new UnsafeBuffer(mappedByteBuffer);
                final long currentTime = epochClock.time();

                int index = 0;
                buffer.putInt(index, VERSION_2_1_0); // version
                index += SIZE_OF_INT;

                buffer.putLong(index, currentTime, LITTLE_ENDIAN); // activityTimestamp
                index += SIZE_OF_LONG;

                buffer.putLong(index, currentTime, LITTLE_ENDIAN); // startTimestamp
                index += SIZE_OF_LONG;

                buffer.putLong(index, 1, LITTLE_ENDIAN); // pid
                index += SIZE_OF_LONG;

                buffer.putInt(index, 777, LITTLE_ENDIAN); // controlStreamId
                index += SIZE_OF_INT;

                buffer.putInt(index, 42, LITTLE_ENDIAN); // localControlStreamId
                index += SIZE_OF_INT;

                buffer.putInt(index, 13, LITTLE_ENDIAN); // eventsStreamId
                index += SIZE_OF_INT;

                buffer.putInt(index, MARK_FILE_HEADER_LENGTH_V2, LITTLE_ENDIAN);
                index += SIZE_OF_INT;

                buffer.putInt(index, errorBufferLength, LITTLE_ENDIAN);
                index += SIZE_OF_INT;

                index += buffer.putStringAscii(index, controlChannel, LITTLE_ENDIAN);

                index += buffer.putStringAscii(index, localControlChannel, LITTLE_ENDIAN);

                index += buffer.putStringAscii(index, eventsChannel, LITTLE_ENDIAN);

                // aeronDirectory
                buffer.putStringAscii(index, aeronDirectory, LITTLE_ENDIAN);
            }
            finally
            {
                BufferUtil.free(mappedByteBuffer);
            }
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return new ArchiveMarkFile(
            archiveDir,
            ArchiveMarkFile.FILENAME,
            epochClock,
            TimeUnit.SECONDS.toMillis(5),
            (version) -> {},
            null);
    }

    static Catalog createCatalogInVersion2(
        final File archiveDir, final EpochClock epochClock, final List<RecordingDescriptorV2> recordings)
    {
        final Path catalogFile = archiveDir.toPath().resolve(Archive.Configuration.CATALOG_FILE_NAME);
        try (FileChannel channel = FileChannel.open(catalogFile, CREATE_NEW, READ, WRITE, SPARSE))
        {
            final MappedByteBuffer mappedByteBuffer = channel.map(
                READ_WRITE,
                0,
                RECORDING_FRAME_LENGTH_V2 + (recordings.size() * (long)RECORDING_FRAME_LENGTH_V2));
            mappedByteBuffer.order(LITTLE_ENDIAN);
            try
            {
                final UnsafeBuffer buffer = new UnsafeBuffer(mappedByteBuffer);

                int index = 0;
                buffer.putInt(index, VERSION_2_1_0); // version
                index += SIZE_OF_INT;

                buffer.putInt(index, RECORDING_FRAME_LENGTH_V2); // entryLength

                index = RECORDING_FRAME_LENGTH_V2;

                for (final RecordingDescriptorV2 recording : recordings)
                {
                    writeRecording(buffer, index, recording);
                    index += RECORDING_FRAME_LENGTH_V2;
                }
            }
            finally
            {
                BufferUtil.free(mappedByteBuffer);
            }
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return new Catalog(archiveDir, epochClock, MIN_CAPACITY, true, null, (version) -> {});
    }

    private static int totalMarkFileLengthInVersion2(
        final String controlChannel,
        final String localControlChannel,
        final String eventsChannel,
        final String aeronDirectory,
        final int errorBufferLength)
    {
        final int headerContentLength =
            128 + 4 * SIZE_OF_INT +
            controlChannel.length() +
            localControlChannel.length() +
            eventsChannel.length() +
            aeronDirectory.length();

        if (headerContentLength > MARK_FILE_HEADER_LENGTH_V2)
        {
            throw new ArchiveException(
                "MarkFile length required " + headerContentLength + " greater than " + MARK_FILE_HEADER_LENGTH_V2);
        }

        return MARK_FILE_HEADER_LENGTH_V2 + errorBufferLength;
    }

    private static void writeRecording(
        final UnsafeBuffer buffer, final int index, final RecordingDescriptorV2 recording)
    {
        int offset = index + 32;

        // RecordingDescriptor
        buffer.putLong(offset, recording.controlSessionId, LITTLE_ENDIAN);
        offset += SIZE_OF_LONG;

        buffer.putLong(offset, recording.correlationId, LITTLE_ENDIAN);
        offset += SIZE_OF_LONG;

        buffer.putLong(offset, recording.recordingId, LITTLE_ENDIAN);
        offset += SIZE_OF_LONG;

        buffer.putLong(offset, recording.startTimestamp, LITTLE_ENDIAN);
        offset += SIZE_OF_LONG;

        buffer.putLong(offset, recording.stopTimestamp, LITTLE_ENDIAN);
        offset += SIZE_OF_LONG;

        buffer.putLong(offset, recording.startPosition, LITTLE_ENDIAN);
        offset += SIZE_OF_LONG;

        buffer.putLong(offset, recording.stopPosition, LITTLE_ENDIAN);
        offset += SIZE_OF_LONG;

        buffer.putInt(offset, recording.initialTermId, LITTLE_ENDIAN);
        offset += SIZE_OF_INT;

        buffer.putInt(offset, recording.segmentFileLength, LITTLE_ENDIAN);
        offset += SIZE_OF_INT;

        buffer.putInt(offset, recording.termBufferLength, LITTLE_ENDIAN);
        offset += SIZE_OF_INT;

        buffer.putInt(offset, recording.mtuLength, LITTLE_ENDIAN);
        offset += SIZE_OF_INT;

        buffer.putInt(offset, recording.sessionId, LITTLE_ENDIAN);
        offset += SIZE_OF_INT;

        buffer.putInt(offset, recording.streamId, LITTLE_ENDIAN);
        offset += SIZE_OF_INT;

        offset += buffer.putStringAscii(offset, recording.strippedChannel, LITTLE_ENDIAN);

        offset += buffer.putStringAscii(offset, recording.originalChannel, LITTLE_ENDIAN);

        offset += buffer.putStringAscii(offset, recording.sourceIdentity, LITTLE_ENDIAN);

        final int totalLength = offset - index;
        if (totalLength > RECORDING_FRAME_LENGTH_V2)
        {
            throw new IllegalArgumentException("recordingId=" + recording.recordingId + "encoded length is " +
                totalLength + " bytes which exceeds max recording length of " + RECORDING_FRAME_LENGTH_V2 + " bytes");
        }

        // RecordingDescriptorHeader
        buffer.putInt(index, totalLength - 32, LITTLE_ENDIAN); // Length of the recording w/o header
        buffer.putByte(index + SIZE_OF_INT, recording.valid);
    }

    static final class RecordingDescriptorV2
    {
        // RecordingDescriptorHeader
        final byte valid;

        // RecordingDescriptor
        final long controlSessionId;
        final long correlationId;
        final long recordingId;
        final long startTimestamp;
        final long stopTimestamp;
        final long startPosition;
        final long stopPosition;
        final int initialTermId;
        final int segmentFileLength;
        final int termBufferLength;
        final int mtuLength;
        final int sessionId;
        final int streamId;
        final String strippedChannel;
        final String originalChannel;
        final String sourceIdentity;

        RecordingDescriptorV2(
            final byte valid,
            final long controlSessionId,
            final long correlationId,
            final long recordingId,
            final long startTimestamp,
            final long stopTimestamp,
            final long startPosition,
            final long stopPosition,
            final int initialTermId,
            final int segmentFileLength,
            final int termBufferLength,
            final int mtuLength,
            final int sessionId,
            final int streamId,
            final String strippedChannel,
            final String originalChannel,
            final String sourceIdentity)
        {
            this.valid = valid;
            this.controlSessionId = controlSessionId;
            this.correlationId = correlationId;
            this.recordingId = recordingId;
            this.startTimestamp = startTimestamp;
            this.stopTimestamp = stopTimestamp;
            this.startPosition = startPosition;
            this.stopPosition = stopPosition;
            this.initialTermId = initialTermId;
            this.segmentFileLength = segmentFileLength;
            this.termBufferLength = termBufferLength;
            this.mtuLength = mtuLength;
            this.sessionId = sessionId;
            this.streamId = streamId;
            this.strippedChannel = strippedChannel;
            this.originalChannel = originalChannel;
            this.sourceIdentity = sourceIdentity;
        }
    }
}
