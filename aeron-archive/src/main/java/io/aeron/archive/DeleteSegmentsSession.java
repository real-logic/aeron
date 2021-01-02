/*
 * Copyright 2014-2021 Real Logic Limited.
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
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.codecs.RecordingSignal;
import org.agrona.ErrorHandler;

import java.io.File;
import java.util.ArrayDeque;

class DeleteSegmentsSession implements Session
{
    private final long recordingId;
    private final long correlationId;
    private final ArrayDeque<String> files;
    private final File archiveDir;
    private final ControlSession controlSession;
    private final ControlResponseProxy controlResponseProxy;
    private final ErrorHandler errorHandler;

    DeleteSegmentsSession(
        final long recordingId,
        final long correlationId,
        final ArrayDeque<String> files,
        final File archiveDir,
        final ControlSession controlSession,
        final ControlResponseProxy controlResponseProxy,
        final ErrorHandler errorHandler)
    {
        this.recordingId = recordingId;
        this.correlationId = correlationId;
        this.files = files;
        this.archiveDir = archiveDir;
        this.controlSession = controlSession;
        this.controlResponseProxy = controlResponseProxy;
        this.errorHandler = errorHandler;
    }

    public void close()
    {
    }

    public void abort()
    {
    }

    public boolean isDone()
    {
        return files.isEmpty();
    }

    public long sessionId()
    {
        return recordingId;
    }

    public int doWork()
    {
        int workCount = 0;
        final String fileName = files.pollFirst();

        if (null != fileName)
        {
            final File file = new File(archiveDir, fileName);
            if (file.exists() && !file.delete())
            {
                final String errorMessage = "unable to delete segment file: " + file;
                controlSession.attemptErrorResponse(correlationId, errorMessage, controlResponseProxy);
                errorHandler.onError(new ArchiveException("segment delete failed for recording: " + recordingId));
            }

            if (files.isEmpty())
            {
                controlSession.attemptSignal(
                    correlationId, recordingId, Aeron.NULL_VALUE, Aeron.NULL_VALUE, RecordingSignal.DELETE);
            }

            workCount += 1;
        }

        return workCount;
    }
}
