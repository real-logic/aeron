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

import static io.aeron.cluster.service.ClusteredServiceContainer.Configuration.RECORDING_EVENTS_LOG_FILE_NAME;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.*;
import static org.agrona.BitUtil.SIZE_OF_LONG;

public class ClusterRecordingEventLog implements AutoCloseable
{
    public static final long RECORDING_TYPE_LOG = 0;
    public static final long RECORDING_TYPE_SNAPSHOT = 1;

    private static final int RECORDING_ID_OFFSET = 0;
    private static final int RECORD_TYPE_OFFSET = RECORDING_ID_OFFSET + SIZE_OF_LONG;
    private static final int POSITION_OFFSET = RECORD_TYPE_OFFSET + SIZE_OF_LONG;
    private static final int ABSOLUTE_POSITION_OFFSET = POSITION_OFFSET + SIZE_OF_LONG;
    private static final int MESSAGE_INDEX_OFFSET = ABSOLUTE_POSITION_OFFSET + SIZE_OF_LONG;

    private static final int RECORD_LENGTH = MESSAGE_INDEX_OFFSET + SIZE_OF_LONG;

    @FunctionalInterface
    public interface RecordingEventHandler
    {
        void onRecord(long type, long recordingId, long position, long absolutePosition, long messageIndex);
    }

    private final File eventFile;
    private final File newEventFile;
    private final ByteBuffer buffer = ByteBuffer.allocate(RECORD_LENGTH);

    private FileChannel fileChannel;

    public ClusterRecordingEventLog(final File clusterDir, final long serviceId)
    {
        eventFile = recordingEventsLogLocation(clusterDir, serviceId, false);
        newEventFile = recordingEventsLogLocation(clusterDir, serviceId, true);

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

    public int forEachFromLastSnapshot(final RecordingEventHandler handler)
    {
        MappedByteBuffer mappedByteBuffer = null;
        int numIds = 0;

        try
        {
            final long length = fileChannel.size();

            mappedByteBuffer = fileChannel.map(READ_WRITE, 0, length);
            final UnsafeBuffer buffer = new UnsafeBuffer(mappedByteBuffer);

            final int snapshotOffset = findOffsetOfLatestSnapshot(buffer);

            for (int i = snapshotOffset; i < (int)length; i += RECORD_LENGTH)
            {
                handler.onRecord(
                    buffer.getLong(i + RECORD_TYPE_OFFSET),
                    buffer.getLong(i + RECORDING_ID_OFFSET),
                    buffer.getLong(i + POSITION_OFFSET),
                    buffer.getLong(i + ABSOLUTE_POSITION_OFFSET),
                    buffer.getLong(i + MESSAGE_INDEX_OFFSET));
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

    public void appendLog(
        final long recordingId, final long position, final long absolutePosition, final long messageIndex)
    {
        append(RECORDING_TYPE_LOG, recordingId, position, absolutePosition, messageIndex);
    }

    public void appendSnapshot(
        final long recordingId, final long position, final long absolutePosition, final long messageIndex)
    {
        append(RECORDING_TYPE_SNAPSHOT, recordingId, position, absolutePosition, messageIndex);
    }

    public static File recordingEventsLogLocation(
        final File clusterDirectory, final long serviceId, final boolean isTmp)
    {
        final String suffix = isTmp ? ".tmp" : "";

        return new File(
            clusterDirectory, Long.toString(serviceId) + "-" + RECORDING_EVENTS_LOG_FILE_NAME + suffix);
    }

    public void close()
    {
        CloseHelper.quietClose(fileChannel);
    }

    private void append(
        final long type,
        final long recordingId,
        final long position,
        final long absolutePosition,
        final long messageIndex)
    {
        try
        {
            final Path eventFilePath = eventFile.toPath();
            final Path newEventFilePath = newEventFile.toPath();

            fileChannel.close();

            Files.copy(eventFilePath, newEventFilePath);

            final FileChannel newFileChannel = FileChannel.open(newEventFilePath, WRITE, APPEND);

            buffer.putLong(RECORDING_ID_OFFSET, recordingId);
            buffer.putLong(RECORD_TYPE_OFFSET, type);
            buffer.putLong(POSITION_OFFSET, position);
            buffer.putLong(ABSOLUTE_POSITION_OFFSET, absolutePosition);
            buffer.putLong(MESSAGE_INDEX_OFFSET, messageIndex);
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

    private static int findOffsetOfLatestSnapshot(final UnsafeBuffer unsafeBuffer)
    {
        for (int i = unsafeBuffer.capacity() - RECORD_LENGTH; i >= 0; i -= RECORD_LENGTH)
        {
            if (unsafeBuffer.getLong(i + RECORD_TYPE_OFFSET) == RECORDING_TYPE_SNAPSHOT)
            {
                return i;
            }
        }

        return 0;
    }
}
