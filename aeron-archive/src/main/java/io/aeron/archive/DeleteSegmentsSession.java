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

import io.aeron.Aeron;
import io.aeron.archive.client.ArchiveEvent;
import io.aeron.archive.codecs.RecordingSignal;
import org.agrona.ErrorHandler;

import java.io.File;
import java.util.ArrayDeque;


class DeleteSegmentsSession implements Session
{
    private final long recordingId;
    private final long correlationId;
    private final File archiveDir;
    private final ArrayDeque<File> deleteList;
    private final ArrayDeque<File> pendingDeleteList;
    private final ControlSession controlSession;
    private final ControlResponseProxy controlResponseProxy;
    private final ErrorHandler errorHandler;
    private static final String DELETE_SUFFIX = ".del";
    private boolean isAborted = false;

    DeleteSegmentsSession(
        final long recordingId,
        final long correlationId,
        final File archiveDir,
        final ArrayDeque<File> deleteList,
        final ArrayDeque<File> pendingDeleteList,
        final ControlSession controlSession,
        final ControlResponseProxy controlResponseProxy,
        final ErrorHandler errorHandler)
    {
        this.recordingId = recordingId;
        this.correlationId = correlationId;
        this.archiveDir = archiveDir;
        this.deleteList = deleteList;
        this.pendingDeleteList = pendingDeleteList;
        this.controlSession = controlSession;
        this.controlResponseProxy = controlResponseProxy;
        this.errorHandler = errorHandler;
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        if (pendingDeleteList.isEmpty())
        {
            while (!deleteList.isEmpty())
            {
                final File file = deleteList.pollFirst();
                if (null != file && file.exists() && !file.delete())
                {
                    errorHandler.onError(new ArchiveEvent("segment delete failed for recording: " + recordingId));
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    public void abort()
    {
        isAborted = true;
    }

    /**
     * {@inheritDoc}
     */
    public boolean isDone()
    {
        return isAborted || (pendingDeleteList.isEmpty() && deleteList.isEmpty());
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
        if (isAborted)
        {
            return 0;
        }

        int workCount = 0;

        if (!pendingDeleteList.isEmpty())
        {
            final File file = pendingDeleteList.pollFirst();
            if (null != file)
            {
                final File toDelete = new File(archiveDir, file.getName() + DELETE_SUFFIX);
                if (!file.renameTo(toDelete))
                {
                    isAborted = true;
                    final String msg = "failed to rename " + file + " to " + toDelete;
                    controlSession.sendErrorResponse(correlationId, msg, controlResponseProxy);
                }
                else
                {
                    deleteList.add(toDelete);
                }
                workCount += 1;
            }
        }
        else
        {
            final File file = deleteList.pollFirst();
            if (null != file)
            {
                if (file.exists() && !file.delete())
                {
                    final String errorMessage = "unable to delete segment file: " + file;
                    controlSession.attemptErrorResponse(correlationId, errorMessage, controlResponseProxy);
                    errorHandler.onError(new ArchiveEvent("segment delete failed for recording: " + recordingId));
                }

                if (deleteList.isEmpty())
                {
                    controlSession.sendSignal(
                        correlationId, recordingId, Aeron.NULL_VALUE, Aeron.NULL_VALUE, RecordingSignal.DELETE);
                }

                workCount += 1;
            }
        }

        return workCount;
    }
}
