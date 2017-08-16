/*
 * Copyright 2014-2017 Real Logic Ltd.
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

import io.aeron.archive.codecs.*;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.IoUtil;
import org.agrona.LangUtil;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static java.nio.file.StandardOpenOption.*;
import static org.agrona.BitUtil.align;
import static org.agrona.BufferUtil.allocateDirectAligned;

/**
 * Catalog for the archive keeps details of recorded images, past and present, and used for browsing.
 * The format is simple, allocating a fixed 1KB record for each record descriptor. This allows offset
 * based look up of a descriptor in the file.
 * <p>
 * Catalog file format:
 * <pre>
 *  # |---------------- 32b --------------|
 *  0 |desc-length 4b|------24b-unused----|
 *  1 |RecordingDescriptor (length < 1024)|
 *  2 |...continues...                    |
 * 128|------------- repeat --------------|
 * </pre>
 * <p>
 */
class Catalog implements AutoCloseable
{
    @FunctionalInterface
    interface CatalogEntryProcessor
    {
        void accept(
            RecordingDescriptorHeaderEncoder headerEncoder,
            RecordingDescriptorHeaderDecoder headerDecoder,
            RecordingDescriptorEncoder descriptorEncoder,
            RecordingDescriptorDecoder descriptorDecoder);
    }

    static final long NULL_TIME = -1L;
    static final long NULL_POSITION = -1;
    static final int PAGE_SIZE = 4096;
    static final int NULL_RECORD_ID = -1;

    static final int DEFAULT_RECORD_LENGTH = 1024;
    static final byte VALID = 1;
    static final byte INVALID = 0;
    static final int DESCRIPTOR_HEADER_LENGTH = RecordingDescriptorHeaderDecoder.BLOCK_LENGTH;

    private static final int SCHEMA_VERSION = RecordingDescriptorHeaderDecoder.SCHEMA_VERSION;
    private static final int DESCRIPTOR_BLOCK_LENGTH = RecordingDescriptorDecoder.BLOCK_LENGTH;

    private final RecordingDescriptorHeaderDecoder descriptorHeaderDecoder = new RecordingDescriptorHeaderDecoder();
    private final RecordingDescriptorHeaderEncoder descriptorHeaderEncoder = new RecordingDescriptorHeaderEncoder();

    private final RecordingDescriptorEncoder descriptorEncoder = new RecordingDescriptorEncoder();
    private final RecordingDescriptorDecoder descriptorDecoder = new RecordingDescriptorDecoder();

    private final UnsafeBuffer indexUBuffer;
    private final MappedByteBuffer indexMappedBBuffer;

    private final int recordLength;

    private final int maxDescriptorStringsCombinedLength;
    private final int maxRecordingId;
    private final File archiveDir;
    private final int fileSyncLevel;
    private final EpochClock epochClock;
    private long nextRecordingId = 0;

    Catalog(
        final File archiveDir,
        final FileChannel archiveDirChannel,
        final int fileSyncLevel,
        final EpochClock epochClock)
    {
        this(archiveDir, archiveDirChannel, fileSyncLevel, epochClock, true);
    }

    Catalog(
        final File archiveDir,
        final FileChannel archiveDirChannel,
        final int fileSyncLevel,
        final EpochClock epochClock,
        final boolean fixOnRefresh)
    {
        this.archiveDir = archiveDir;
        this.fileSyncLevel = fileSyncLevel;
        this.epochClock = epochClock;
        final File indexFile = new File(archiveDir, Archive.Configuration.CATALOG_FILE_NAME);
        final boolean indexPreExists = indexFile.exists();

        try (FileChannel channel = FileChannel.open(indexFile.toPath(), CREATE, READ, WRITE, SPARSE))
        {
            indexMappedBBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, Integer.MAX_VALUE);
            indexUBuffer = new UnsafeBuffer(indexMappedBBuffer);

            if (!indexPreExists && archiveDirChannel != null && fileSyncLevel > 0)
            {
                archiveDirChannel.force(fileSyncLevel > 1);
            }
        }
        catch (final IOException ex)
        {
            throw new RuntimeException(ex);
        }

        try
        {
            if (indexPreExists)
            {
                final CatalogHeaderDecoder catalogHeaderDecoder = new CatalogHeaderDecoder()
                    .wrap(indexUBuffer, 0, CatalogHeaderDecoder.BLOCK_LENGTH, SCHEMA_VERSION);

                if (catalogHeaderDecoder.version() != SCHEMA_VERSION)
                {
                    throw new IllegalArgumentException("Catalog file version" + catalogHeaderDecoder.version() +
                        " does not match software:" + SCHEMA_VERSION);
                }
                recordLength = catalogHeaderDecoder.entryLength();
            }
            else
            {
                new CatalogHeaderEncoder()
                    .wrap(indexUBuffer, 0)
                    .version(SCHEMA_VERSION)
                    .entryLength(DEFAULT_RECORD_LENGTH);

                recordLength = DEFAULT_RECORD_LENGTH;
            }

            maxDescriptorStringsCombinedLength =
                recordLength - (DESCRIPTOR_HEADER_LENGTH + RecordingDescriptorEncoder.BLOCK_LENGTH + 12);
            maxRecordingId = (Integer.MAX_VALUE - (2 * recordLength - 1)) / recordLength;

            refreshCatalog(fixOnRefresh);
        }
        catch (final Throwable ex)
        {
            close();
            throw ex;
        }
    }

    public void close()
    {
        IoUtil.unmap(indexMappedBBuffer);
    }

    long addNewRecording(
        final long startPosition,
        final long startTimestamp,
        final int imageInitialTermId,
        final int segmentFileLength,
        final int termBufferLength,
        final int mtuLength,
        final int sessionId,
        final int streamId,
        final String strippedChannel,
        final String originalChannel,
        final String sourceIdentity)
    {
        if (nextRecordingId > maxRecordingId)
        {
            throw new IllegalStateException("Catalog is full, max recordings reached: " + maxRecordingId);
        }

        final int combinedStringsLen = strippedChannel.length() + sourceIdentity.length() + originalChannel.length();
        if (combinedStringsLen > maxDescriptorStringsCombinedLength)
        {
            throw new IllegalArgumentException("Combined length of channel:'" + strippedChannel +
                "' and sourceIdentity:'" + sourceIdentity +
                "' and originalChannel:'" + originalChannel +
                "' exceeds max allowed:" + maxDescriptorStringsCombinedLength);
        }

        final long newRecordingId = nextRecordingId;

        indexUBuffer.wrap(indexMappedBBuffer, recordingDescriptorOffset(newRecordingId), recordLength);
        descriptorEncoder.wrap(indexUBuffer, DESCRIPTOR_HEADER_LENGTH);

        initDescriptor(
            descriptorEncoder,
            newRecordingId,
            startTimestamp,
            startPosition,
            imageInitialTermId,
            segmentFileLength,
            termBufferLength,
            mtuLength,
            sessionId,
            streamId,
            strippedChannel,
            originalChannel,
            sourceIdentity);

        descriptorHeaderEncoder.wrap(indexUBuffer, 0);
        descriptorHeaderEncoder
            .length(descriptorEncoder.encodedLength())
            .valid(VALID);

        nextRecordingId++;

        if (fileSyncLevel > 0)
        {
            indexMappedBBuffer.force();
        }

        return newRecordingId;
    }

    private int recordingDescriptorOffset(final long newRecordingId)
    {
        return (int)(newRecordingId * recordLength) + recordLength;
    }

    boolean wrapDescriptor(final long recordingId, final UnsafeBuffer buffer)
    {
        if (recordingId < 0 || recordingId >= maxRecordingId)
        {
            return false;
        }

        buffer.wrap(indexMappedBBuffer, recordingDescriptorOffset(recordingId), recordLength);
        descriptorHeaderDecoder.wrap(buffer, 0, DESCRIPTOR_HEADER_LENGTH, SCHEMA_VERSION);

        return descriptorHeaderDecoder.length() != 0;
    }

    UnsafeBuffer wrapDescriptor(final long recordingId)
    {
        final UnsafeBuffer unsafeBuffer = new UnsafeBuffer();
        return wrapDescriptor(recordingId, unsafeBuffer) ? unsafeBuffer : null;
    }

    /**
     * On catalog load we verify entries are in coherent state and attempt to recover entries data where untimely
     * termination of recording has resulted in an unaccounted for stopPosition/stopTimestamp. This operation may be
     * expensive for large catalogs.
     */
    private void refreshCatalog(final boolean fixOnRefresh)
    {
        if (fixOnRefresh)
        {
            forEach(this::refreshAndFixDescriptor);
        }
        else
        {
            forEach(((headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) -> nextRecordingId++));
        }
    }

    void forEach(final CatalogEntryProcessor consumer)
    {
        long recordingId = 0L;
        while (recordingId < maxRecordingId && wrapDescriptor(recordingId, indexUBuffer))
        {
            descriptorHeaderDecoder.wrap(indexUBuffer, 0, DESCRIPTOR_HEADER_LENGTH, SCHEMA_VERSION);
            descriptorHeaderEncoder.wrap(indexUBuffer, 0);
            wrapDescriptorDecoder(descriptorDecoder, indexUBuffer);
            descriptorEncoder.wrap(indexUBuffer, DESCRIPTOR_HEADER_LENGTH);
            consumer.accept(descriptorHeaderEncoder, descriptorHeaderDecoder, descriptorEncoder, descriptorDecoder);
            ++recordingId;
        }
    }

    boolean forEntry(final CatalogEntryProcessor consumer, final long recordingId)
    {
        if (wrapDescriptor(recordingId, indexUBuffer))
        {
            descriptorHeaderDecoder.wrap(indexUBuffer, 0, DESCRIPTOR_HEADER_LENGTH, SCHEMA_VERSION);
            descriptorHeaderEncoder.wrap(indexUBuffer, 0);
            wrapDescriptorDecoder(descriptorDecoder, indexUBuffer);
            descriptorEncoder.wrap(indexUBuffer, DESCRIPTOR_HEADER_LENGTH);
            consumer.accept(descriptorHeaderEncoder, descriptorHeaderDecoder, descriptorEncoder, descriptorDecoder);

            return true;
        }

        return false;
    }

    private void refreshAndFixDescriptor(
        @SuppressWarnings("unused") final RecordingDescriptorHeaderEncoder unused,
        final RecordingDescriptorHeaderDecoder headerDecoder,
        final RecordingDescriptorEncoder encoder,
        final RecordingDescriptorDecoder decoder)
    {
        if (headerDecoder.valid() == VALID && decoder.stopTimestamp() == NULL_TIME)
        {
            final long stopPosition = decoder.stopPosition();
            final long recordingLength = stopPosition - decoder.startPosition();
            final int segmentFileLength = decoder.segmentFileLength();
            final int segmentIndex = (int)(recordingLength / segmentFileLength);
            final long stoppedSegmentOffset =
                ((decoder.startPosition() % decoder.termBufferLength()) + recordingLength) % segmentFileLength;

            final File segmentFile = new File(archiveDir, Archive.segmentFileName(decoder.recordingId(), segmentIndex));
            if (!segmentFile.exists())
            {
                if (recordingLength != 0 || stoppedSegmentOffset != 0)
                {
                    throw new IllegalStateException("Failed to open recording: " + segmentFile.getAbsolutePath() +
                        " - Please use the CatalogTool to fix the archive.");
                }
            }
            else
            {
                recoverStopPosition(encoder, segmentFile, segmentFileLength, stopPosition, stoppedSegmentOffset);
            }

            encoder.stopTimestamp(epochClock.time());
        }

        nextRecordingId = decoder.recordingId() + 1;
    }

    private void recoverStopPosition(
        final RecordingDescriptorEncoder encoder,
        final File segmentFile,
        final int segmentFileLength,
        final long stopPosition,
        final long stoppedSegmentOffset)
    {
        try (FileChannel segment = FileChannel.open(segmentFile.toPath(), READ))
        {
            final ByteBuffer headerBB = allocateDirectAligned(HEADER_LENGTH, FRAME_ALIGNMENT);
            final DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight(headerBB);
            long lastFragmentSegmentOffset = stoppedSegmentOffset;
            long nextFragmentSegmentOffset = stoppedSegmentOffset;
            do
            {
                headerBB.clear();
                if (HEADER_LENGTH != segment.read(headerBB, nextFragmentSegmentOffset))
                {
                    throw new IllegalStateException("Unexpected read failure from file: " +
                        segmentFile.getAbsolutePath() + " at position:" + nextFragmentSegmentOffset);
                }

                if (headerFlyweight.frameLength() == 0)
                {
                    break;
                }

                lastFragmentSegmentOffset = nextFragmentSegmentOffset;
                nextFragmentSegmentOffset += align(headerFlyweight.frameLength(), FRAME_ALIGNMENT);
            }
            while (nextFragmentSegmentOffset != segmentFileLength);

            // since we know descriptor buffers are forced on file rollover we don't need to handle rollover
            // beyond segment boundary (see RecordingWriter#onFileRollover)

            if ((nextFragmentSegmentOffset / PAGE_SIZE) == (lastFragmentSegmentOffset / PAGE_SIZE))
            {
                // if fragment does not straddle page boundaries we need not drop the last fragment
                lastFragmentSegmentOffset = nextFragmentSegmentOffset;
            }

            if (lastFragmentSegmentOffset != stoppedSegmentOffset)
            {
                // process has failed between transferring the data to updating the stop position, we can't trust
                // the last fragment, so take the position of the previous fragment as the stop position
                encoder.stopPosition(stopPosition + (lastFragmentSegmentOffset - stoppedSegmentOffset));
            }
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    static void initDescriptor(
        final RecordingDescriptorEncoder recordingDescriptorEncoder,
        final long recordingId,
        final long startTimestamp,
        final long startPosition,
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
        recordingDescriptorEncoder
            .recordingId(recordingId)
            .startTimestamp(startTimestamp)
            .stopTimestamp(NULL_TIME)
            .startPosition(startPosition)
            .stopPosition(startPosition)
            .initialTermId(initialTermId)
            .segmentFileLength(segmentFileLength)
            .termBufferLength(termBufferLength)
            .mtuLength(mtuLength)
            .sessionId(sessionId)
            .streamId(streamId)
            .strippedChannel(strippedChannel)
            .originalChannel(originalChannel)
            .sourceIdentity(sourceIdentity);
    }

    static void wrapDescriptorDecoder(final RecordingDescriptorDecoder decoder, final UnsafeBuffer descriptorBuffer)
    {
        decoder.wrap(
            descriptorBuffer,
            DESCRIPTOR_HEADER_LENGTH,
            DESCRIPTOR_BLOCK_LENGTH,
            SCHEMA_VERSION);
    }
}
