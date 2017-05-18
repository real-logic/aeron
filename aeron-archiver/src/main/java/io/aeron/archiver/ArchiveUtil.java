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
package io.aeron.archiver;

import io.aeron.archiver.codecs.RecordingDescriptorDecoder;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Date;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class ArchiveUtil
{
    static String recordingMetaFileName(final long recordingId)
    {
        return recordingId + ".inf";
    }

    static String recordingDataFileName(
        final long recordingId,
        final int segmentIndex)
    {
        return recordingId + "." + segmentIndex + ".rec";
    }

    static String recordingDataFileName(
        final long recordingId,
        final int initialTermId,
        final int termBufferLength,
        final int termId,
        final int segmentFileLength)
    {
        final int segmentIndex = segmentFileIndex(initialTermId, termId, termBufferLength, segmentFileLength);

        return recordingDataFileName(recordingId, segmentIndex);
    }

    private static int segmentFileIndex(
        final int initialTermId,
        final int termId,
        final int termBufferLength,
        final int segmentFileLength)
    {
        return (int) (((termId - initialTermId) * (long)termBufferLength) / segmentFileLength);
    }

    static int segmentFileIndex(
        final long joiningPosition,
        final long position,
        final int segmentFileLength)
    {
        return (int)((position - joiningPosition) / segmentFileLength);
    }

    static void printMetaFile(final File metaFile) throws IOException
    {
        final RecordingDescriptorDecoder formatDecoder = recordingMetaFileFormatDecoder(metaFile);
        System.out.println("recordingId: " + formatDecoder.recordingId());
        System.out.println("termBufferLength: " + formatDecoder.termBufferLength());
        System.out.println("start time: " + new Date(formatDecoder.startTime()));
        System.out.println("joining position: " + formatDecoder.joiningPosition());
        System.out.println("last position: " + formatDecoder.lastPosition());
        System.out.println("end time: " + new Date(formatDecoder.endTime()));
        System.out.println("source: " + formatDecoder.source());
        System.out.println("sessionId: " + formatDecoder.sessionId());
        System.out.println("channel: " + formatDecoder.channel());
        System.out.println("streamId: " + formatDecoder.streamId());
        IoUtil.unmap(formatDecoder.buffer().byteBuffer());
    }

    static RecordingDescriptorDecoder recordingMetaFileFormatDecoder(final File metaFile)
        throws IOException
    {
        try (FileChannel metadataFileChannel = FileChannel.open(metaFile.toPath(), READ, WRITE))
        {
            final ByteBuffer metaDataBuffer = ByteBuffer.allocate(Catalog.RECORD_LENGTH);
            metadataFileChannel.read(metaDataBuffer);
            final RecordingDescriptorDecoder decoder = new RecordingDescriptorDecoder();
            return decoder.wrap(
                new UnsafeBuffer(metaDataBuffer),
                Catalog.CATALOG_FRAME_LENGTH,
                RecordingDescriptorDecoder.BLOCK_LENGTH,
                RecordingDescriptorDecoder.SCHEMA_VERSION);
        }
    }

    static int recordingOffset(
        final int termOffset,
        final int termId,
        final int initialTermId,
        final int termsMask,
        final int termBufferLength)
    {
        return ((termId - initialTermId) & termsMask) * termBufferLength + termOffset;
    }

    static long recordingFileFullLength(final RecordingDescriptorDecoder metaDecoder)
    {
        return metaDecoder.lastPosition() - metaDecoder.joiningPosition();
    }

    public static long recordingLength(
        final int termBufferLength,
        final int initialTermId,
        final int initialTermOffset,
        final int lastTermId, final int lastTermOffset)
    {
        return ((long)(lastTermId - initialTermId)) * termBufferLength +
            (lastTermOffset - initialTermOffset);
    }
}
