/*
 * Copyright 2017 Real Logic Ltd.
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
package io.aeron.cluster.service;

import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.LangUtil;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.LongConsumer;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.*;
import static org.agrona.BitUtil.SIZE_OF_LONG;

public class ClusterRecordingEventLog implements AutoCloseable
{
    private static final int RECORD_LENGTH = SIZE_OF_LONG;

    private final File eventFile;
    private final File newEventFile;
    private final ByteBuffer buffer = ByteBuffer.allocate(RECORD_LENGTH);

    private FileChannel fileChannel;

    public ClusterRecordingEventLog(final File clusterDir)
    {
        eventFile = new File(clusterDir, ClusteredServiceContainer.Configuration.RECORDING_IDS_LOG_FILE_NAME);
        newEventFile = new File(
            clusterDir, ClusteredServiceContainer.Configuration.RECORDING_IDS_LOG_FILE_NAME + ".tmp");

        try
        {
            fileChannel = FileChannel.open(eventFile.toPath(), CREATE, READ, WRITE);
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    public long recordCount()
    {
        long result = 0;

        try
        {
            result = fileChannel.size() / RECORD_LENGTH;
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return result;
    }

    public int forEach(final LongConsumer handler)
    {
        MappedByteBuffer mappedByteBuffer = null;
        int numIds = 0;

        try
        {
            final long length = fileChannel.size();

            mappedByteBuffer = fileChannel.map(READ_WRITE, 0, length);
            final UnsafeBuffer buffer = new UnsafeBuffer(mappedByteBuffer);

            for (int i = 0; i < (int)length; i += RECORD_LENGTH)
            {
                handler.accept(buffer.getLong(i));
                numIds++;
            }
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
        finally
        {
            IoUtil.unmap(mappedByteBuffer);
        }

        return numIds;
    }

    public void append(final long recordingId)
    {
        try
        {
            final Path eventFilePath = eventFile.toPath();
            final Path newEventFilePath = newEventFile.toPath();

            fileChannel.close();

            Files.copy(eventFilePath, newEventFilePath);

            final FileChannel newFileChannel = FileChannel.open(newEventFilePath, WRITE, APPEND);

            buffer.putLong(0, recordingId);
            buffer.position(0);
            newFileChannel.write(buffer);
            newFileChannel.force(true);
            newFileChannel.close();

            Files.move(newEventFilePath, eventFilePath, REPLACE_EXISTING);

            fileChannel = FileChannel.open(eventFilePath, READ);
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    public void close()
    {
        CloseHelper.quietClose(fileChannel);
    }
}
