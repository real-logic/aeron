/*
 * Copyright 2014 - 2017 Real Logic Ltd.
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

import io.aeron.archiver.messages.ArchiveMetaFileFormatDecoder;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Date;

class ArchiveFileUtil
{
    static final int META_FILE_SIZE = 64;
    static final int ARCHIVE_FILE_SIZE = 1 << 30;

    static String archiveMetaFileName(int streamInstanceId)
    {
        return streamInstanceId + ".meta";
    }

    static String archiveDataFileName(int streamInstanceId, int index)
    {
        return streamInstanceId + "." + index + ".aaf";
    }

    static String archiveDataFileName(int streamInstanceId, int initialTermId, int termBufferLength, int termId)
    {
        final int termsPerFile = ARCHIVE_FILE_SIZE / termBufferLength;
        final int index = (termId - initialTermId) / termsPerFile;

        return archiveDataFileName(streamInstanceId, index);
    }

    static void printMetaFile(File metaFile) throws IOException
    {
        final ArchiveMetaFileFormatDecoder formatDecoder = archiveMetaFileFormatDecoder(metaFile);
        System.out.println("streamInstanceId: " + formatDecoder.streamInstanceId());
        System.out.println("termBufferLength: " + formatDecoder.termBufferLength());
        System.out.println("start time: " + new Date(formatDecoder.startTime()));
        System.out.println("initialTermId: " + formatDecoder.initialTermId());
        System.out.println("initial term offset: " + formatDecoder.initialTermOffset());
        System.out.println("last term: " + formatDecoder.lastTermId());
        System.out.println("last term offset: " + formatDecoder.lastTermOffset());
        System.out.println("end time: " + new Date(formatDecoder.endTime()));
    }

    static ArchiveMetaFileFormatDecoder archiveMetaFileFormatDecoder(File metaFile) throws IOException
    {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(metaFile, "rw");
             FileChannel metadataFileChannel = randomAccessFile.getChannel();)
        {
            final MappedByteBuffer metaDataBuffer = metadataFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 64);
            return new ArchiveMetaFileFormatDecoder().wrap(new UnsafeBuffer(metaDataBuffer), 0, 64, 0);
        }
    }

    static int archiveOffset(int termOffset, int termId, int initialTermId, int termBufferLength)
    {
        final int termsMask = ((ARCHIVE_FILE_SIZE / termBufferLength) - 1);
        return archiveOffset(termOffset, termId, initialTermId, termsMask, termBufferLength);
    }

    static int archiveOffset(int termOffset, int termId, int initialTermId, int termsMask, int termBufferLength)
    {
        return ((termId - initialTermId) & termsMask) * termBufferLength + termOffset;
    }
}
