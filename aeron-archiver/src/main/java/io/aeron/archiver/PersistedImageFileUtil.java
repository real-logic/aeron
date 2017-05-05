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

import io.aeron.archiver.codecs.ArchiveDescriptorDecoder;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Date;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

class PersistedImageFileUtil
{
    static String archiveMetaFileName(final int persistedImageId)
    {
        return persistedImageId + ".meta";
    }

    static String archiveDataFileName(final int persistedImageId, final int index)
    {
        return persistedImageId + "." + index + ".aaf";
    }

    static String archiveDataFileName(
        final int persistedImageId,
        final int initialTermId,
        final int termBufferLength,
        final int termId,
        final int archiveFileSize)
    {
        final int index = persistedImageDataFileIndex(initialTermId, termBufferLength, termId, archiveFileSize);

        return archiveDataFileName(persistedImageId, index);
    }

    static int persistedImageDataFileIndex(
        final int initialTermId,
        final int termBufferLength,
        final int termId,
        final int archiveFileSize)
    {
        final int termsPerFile = archiveFileSize / termBufferLength;
        return (termId - initialTermId) / termsPerFile;
    }

    static void printMetaFile(final File metaFile) throws IOException
    {
        final ArchiveDescriptorDecoder formatDecoder = archiveMetaFileFormatDecoder(metaFile);
        System.out.println("persistedImageId: " + formatDecoder.persistedImageId());
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
        try (FileChannel metadataFileChannel = FileChannel.open(metaFile.toPath(), READ, WRITE))
        {
            final MappedByteBuffer metaDataBuffer = metadataFileChannel.map(
                FileChannel.MapMode.READ_WRITE, 0, PersistedImagesIndex.INDEX_RECORD_SIZE);
            final ArchiveDescriptorDecoder decoder = new ArchiveDescriptorDecoder();

            return decoder.wrap(
                new UnsafeBuffer(metaDataBuffer),
                PersistedImagesIndex.INDEX_FRAME_LENGTH,
                ArchiveDescriptorDecoder.BLOCK_LENGTH,
                ArchiveDescriptorDecoder.SCHEMA_VERSION);
        }
    }

    static int offsetInPersistedImageFile(
        final int termOffset,
        final int termId,
        final int initialTermId,
        final int termBufferLength,
        final int archiveFileSize)
    {
        final int termsMask = ((archiveFileSize / termBufferLength) - 1);
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
