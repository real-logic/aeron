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

import io.aeron.*;
import io.aeron.archiver.codecs.ControlResponseCode;
import org.agrona.CloseHelper;
import org.agrona.concurrent.IdleStrategy;

import java.io.File;

/**
 * Runs recording sessions and descriptor queries sessions. Joining these activities allows the catalog to be single
 * threaded access.
 */
class Recorder extends SessionWorker
{
    private final Catalog catalog;
    private final ControlSessionProxy controlSessionProxy;
    private final NotificationsProxy notificationsProxy;
    private final RecordingWriter.RecordingContext recordingContext;

    Recorder(
        final Aeron aeron,
        final File archiveDir,
        final IdleStrategy idleStrategy,
        final String eventsChannel,
        final int eventsStreamId,
        final RecordingWriter.RecordingContext recordingContext)
    {
        catalog = new Catalog(archiveDir);
        this.controlSessionProxy = new ControlSessionProxy(idleStrategy);
        final Publication notificationPublication = aeron.addPublication(eventsChannel, eventsStreamId);
        this.notificationsProxy = new NotificationsProxy(idleStrategy, notificationPublication);
        this.recordingContext = recordingContext;
    }

    public String roleName()
    {
        return "archiver-recorder";
    }

    protected void sessionCleanup(final long sessionId)
    {
        catalog.removeRecordingSession(sessionId);
    }

    protected void postSessionsClose()
    {
        CloseHelper.close(catalog);
    }

    void startRecording(final Image image)
    {
        addSession(new RecordingSession(notificationsProxy, catalog, image, recordingContext));
    }

    void stopRecording(
        final long correlationId,
        final Publication controlPublication,
        final long recordingId)
    {
        final RecordingSession recordingSession = catalog.getRecordingSession(recordingId);

        if (recordingSession != null)
        {
            recordingSession.abort();
            controlSessionProxy.sendOkResponse(controlPublication, correlationId);
        }
        else
        {
            controlSessionProxy.sendError(
                controlPublication,
                ControlResponseCode.RECORDING_NOT_FOUND,
                null,
                correlationId);
        }
    }

    void listRecordings(
        final long correlationId,
        final Publication controlPublication,
        final long fromId,
        final int count)
    {
        addSession(new ListRecordingsSession(
            correlationId, controlPublication, fromId, count, catalog, controlSessionProxy));
    }
}
