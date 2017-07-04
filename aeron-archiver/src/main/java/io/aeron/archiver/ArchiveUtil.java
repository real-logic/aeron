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
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Date;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

class ArchiveUtil
{

    static final String RECORDING_SEGMENT_POSTFIX = ".rec";
    static final String RECORDING_DESCRIPTOR_POSTFIX = ".inf";

    static String recordingDescriptorFileName(final long recordingId)
    {
        return recordingId + RECORDING_DESCRIPTOR_POSTFIX;
    }

    static String recordingDataFileName(final long recordingId, final int segmentIndex)
    {
        return recordingId + "." + segmentIndex + RECORDING_SEGMENT_POSTFIX;
    }

    static int segmentFileIndex(final long joinPosition, final long position, final int segmentFileLength)
    {
        return (int)((position - joinPosition) / segmentFileLength);
    }

    static void printDescriptorFile(final File descriptorFile) throws IOException
    {
        final RecordingDescriptorDecoder descriptorDecoder = loadRecordingDescriptor(descriptorFile);

        System.out.println("recordingId: " + descriptorDecoder.recordingId());
        System.out.println("termBufferLength: " + descriptorDecoder.termBufferLength());
        System.out.println("joinTimestamp: " + new Date(descriptorDecoder.joinTimestamp()));
        System.out.println("joinPosition: " + descriptorDecoder.joinPosition());
        System.out.println("endPosition: " + descriptorDecoder.endPosition());
        System.out.println("endTimestamp: " + new Date(descriptorDecoder.endTimestamp()));
        System.out.println("sessionId: " + descriptorDecoder.sessionId());
        System.out.println("streamId: " + descriptorDecoder.streamId());
        System.out.println("channel: " + descriptorDecoder.channel());
        System.out.println("sourceIdentity: " + descriptorDecoder.sourceIdentity());
    }

    static RecordingDescriptorDecoder loadRecordingDescriptor(final File descriptorFile)
        throws IOException
    {
        // assume this is called for short lived buffers, in which case let GC take the hit rather than relying
        // on DirectByteBuffer cleaner
        final ByteBuffer descriptorBuffer = ByteBuffer.allocate(Catalog.RECORD_LENGTH);
        return loadRecordingDescriptor(descriptorFile, descriptorBuffer);
    }

    static RecordingDescriptorDecoder loadRecordingDescriptor(
        final File descriptorFile,
        final ByteBuffer descriptorBuffer) throws IOException
    {
        try (FileChannel descriptorFileChannel = FileChannel.open(descriptorFile.toPath(), READ, WRITE))
        {
            descriptorFileChannel.read(descriptorBuffer);

            return new RecordingDescriptorDecoder().wrap(
                new UnsafeBuffer(descriptorBuffer),
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

    static long recordingLength(final RecordingDescriptorDecoder descriptorDecoder)
    {
        return descriptorDecoder.endPosition() - descriptorDecoder.joinPosition();
    }

    static String[] listRecordingSegments(final File archiveDir, final long recordingId)
    {
        return archiveDir.list(new RecordingSegmentsFilter(Long.toString(recordingId)));
    }

    static int segmentIndexFromFileName(final String segmentName)
    {
        final String index = segmentName.split(".")[1];
        return Integer.valueOf(index);
    }

    private static class RecordingSegmentsFilter implements FilenameFilter
    {
        private final String recordingPrefix;

        RecordingSegmentsFilter(final String recordingPrefix)
        {
            this.recordingPrefix = recordingPrefix;
        }

        public boolean accept(final File dir, final String name)
        {
            return name.startsWith(recordingPrefix) && name.endsWith(RECORDING_SEGMENT_POSTFIX);
        }
    }
}
