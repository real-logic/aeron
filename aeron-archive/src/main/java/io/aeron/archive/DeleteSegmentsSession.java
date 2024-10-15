/*
 * Copyright 2014-2024 Real Logic Limited.
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

import io.aeron.archive.client.ArchiveEvent;
import io.aeron.archive.client.ArchiveException;
import org.agrona.ErrorHandler;

import java.io.File;
import java.util.ArrayDeque;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.ArchiveConductor.DELETE_SUFFIX;
import static io.aeron.archive.codecs.RecordingSignal.DELETE;
import static org.agrona.AsciiEncoding.digitCount;
import static org.agrona.AsciiEncoding.parseLongAscii;

class DeleteSegmentsSession implements Session
{
    private final long recordingId;
    private final long correlationId;
    private final long maxDeletePosition;
    private final ArrayDeque<File> files;
    private final ControlSession controlSession;
    private final ErrorHandler errorHandler;

    DeleteSegmentsSession(
        final long recordingId,
        final long correlationId,
        final ArrayDeque<File> files,
        final ControlSession controlSession,
        final ErrorHandler errorHandler)
    {
        this.recordingId = recordingId;
        this.correlationId = correlationId;
        this.files = files;
        this.controlSession = controlSession;
        this.errorHandler = errorHandler;

        long maxSegmentPosition = Long.MIN_VALUE;
        final int prefixLength = digitCount(recordingId) + 1;
        for (final File file : files)
        {
            final String name = file.getName();
            final int dotIndex = name.indexOf('.');
            maxSegmentPosition = Math.max(
                maxSegmentPosition, parseLongAscii(name, prefixLength, dotIndex - prefixLength));
        }
        maxDeletePosition = maxSegmentPosition;
    }

    long maxDeletePosition()
    {
        return maxDeletePosition;
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        controlSession.archiveConductor().removeDeleteSegmentsSession(this);
        controlSession.sendSignal(correlationId, recordingId, NULL_VALUE, NULL_VALUE, DELETE);
    }

    /**
     * {@inheritDoc}
     */
    public void abort()
    {
    }

    /**
     * {@inheritDoc}
     */
    public boolean isDone()
    {
        return files.isEmpty();
    }

    /**
     * {@inheritDoc}
     */
    public long sessionId()
    {
        return recordingId;
    }

    /**
     * {@inheritDoc}
     */
    public int doWork()
    {
        int workCount = 0;
        final File file = files.pollFirst();
        if (null != file)
        {
            if (!file.delete())
            {
                if (file.exists())
                {
                    onDeleteError(file);
                }
                else if (!file.getName().endsWith(DELETE_SUFFIX))
                {
                    final File renamedFile = new File(file.getParent(), file.getName() + DELETE_SUFFIX);
                    if (!renamedFile.delete() && renamedFile.exists())
                    {
                        onDeleteError(renamedFile);
                    }
                }
            }

            workCount = 1;
        }

        return workCount;
    }

    private void onDeleteError(final File file)
    {
        final String errorMessage = "unable to delete segment file: " + file;
        controlSession.sendErrorResponse(correlationId, ArchiveException.GENERIC, errorMessage);
        errorHandler.onError(new ArchiveEvent(errorMessage));
    }
}
