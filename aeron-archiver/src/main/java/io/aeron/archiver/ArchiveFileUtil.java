/*
 * Copyright 2014-2017 Real Logic Ltd.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.aeron.archiver;

import io.aeron.archiver.messages.ArchiveDescriptorDecoder;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Date;

class ArchiveFileUtil
{
    // TODO: make configurable
    static final int ARCHIVE_FILE_SIZE = 1024 * 1024 * 128;

    static String archiveMetaFileName(final int streamInstanceId)
    {
        return streamInstanceId + ".meta";
    }

    static String archiveDataFileName(final int streamInstanceId, final int index)
    {
        return streamInstanceId + "." + index + ".aaf";
    }

    static String archiveDataFileName(
        final int streamInstanceId,
        final int initialTermId,
        final int termBufferLength,
        final int termId)
    {
        final int index = archiveDataFileIndex(initialTermId, termBufferLength, termId);

        return archiveDataFileName(streamInstanceId, index);
    }

    static int archiveDataFileIndex(final int initialTermId, final int termBufferLength, final int termId)
    {
        final int termsPerFile = ARCHIVE_FILE_SIZE / termBufferLength;
        return (termId - initialTermId) / termsPerFile;
    }

    static void printMetaFile(final File metaFile) throws IOException
    {
        final ArchiveDescriptorDecoder formatDecoder = archiveMetaFileFormatDecoder(metaFile);
        System.out.println("streamInstanceId: " + formatDecoder.streamInstanceId());
        System.out.println("termBufferLength: " + formatDecoder.termBufferLength());
        System.out.println("start time: " + new Date(formatDecoder.startTime()));
        System.out.println("initialTermId: " + formatDecoder.initialTermId());
        System.out.println("initial term offset: " + formatDecoder.initialTermOffset());
        System.out.println("last term: " + formatDecoder.lastTermId());
        System.out.println("last term offset: " + formatDecoder.lastTermOffset());
        System.out.println("end time: " + new Date(formatDecoder.endTime()));
        System.out.println("source: " + formatDecoder.source());
        System.out.println("sessionId: " + formatDecoder.sessionId());
        System.out.println("channel: " + formatDecoder.channel());
        System.out.println("streamId: " + formatDecoder.streamId());
        IoUtil.unmap(formatDecoder.buffer().byteBuffer());
    }

    static ArchiveDescriptorDecoder archiveMetaFileFormatDecoder(final File metaFile)
        throws IOException
    {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(metaFile, "rw");
             FileChannel metadataFileChannel = randomAccessFile.getChannel())
        {
            final MappedByteBuffer metaDataBuffer = metadataFileChannel.map(
                FileChannel.MapMode.READ_WRITE, 0, ArchiveIndex.INDEX_RECORD_SIZE);
            final ArchiveDescriptorDecoder decoder = new ArchiveDescriptorDecoder();

            return decoder.wrap(
                new UnsafeBuffer(metaDataBuffer),
                ArchiveIndex.INDEX_FRAME_LENGTH,
                ArchiveDescriptorDecoder.BLOCK_LENGTH,
                ArchiveDescriptorDecoder.SCHEMA_VERSION);
        }
    }

    static int archiveOffset(
        final int termOffset, final int termId, final int initialTermId, final int termBufferLength)
    {
        final int termsMask = ((ARCHIVE_FILE_SIZE / termBufferLength) - 1);
        return archiveOffset(termOffset, termId, initialTermId, termsMask, termBufferLength);
    }

    static int archiveOffset(
        final int termOffset,
        final int termId,
        final int initialTermId,
        final int termsMask,
        final int termBufferLength)
    {
        return ((termId - initialTermId) & termsMask) * termBufferLength + termOffset;
    }

    static long archiveFullLength(final ArchiveDescriptorDecoder metaDecoder)
    {
        return ((long)(metaDecoder.lastTermId() - metaDecoder.initialTermId())) * metaDecoder.termBufferLength() +
            (metaDecoder.lastTermOffset() - metaDecoder.initialTermOffset());
    }
}
