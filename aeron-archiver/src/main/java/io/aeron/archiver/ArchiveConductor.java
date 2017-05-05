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
package io.aeron.archiver;

import io.aeron.*;
import org.agrona.CloseHelper;
import org.agrona.collections.*;
import org.agrona.concurrent.*;

import java.io.File;
import java.util.ArrayList;
import java.util.function.Consumer;

class ArchiveConductor implements Agent
{
    interface Session
    {
        void abort();

        boolean isDone();

        void remove(ArchiveConductor conductor);

        int doWork();
    }

    private final Aeron aeron;
    private final Subscription serviceRequestSubscription;
    private final ArrayList<Session> sessions = new ArrayList<>();
    private final Long2ObjectHashMap<ReplaySession> replaySession2IdMap = new Long2ObjectHashMap<>();

    private final ObjectHashSet<Subscription> archiveSubscriptionSet = new ObjectHashSet<>(128);
    private final ArchiveIndex archiveIndex;
    private final OneToOneConcurrentArrayQueue<Image> imageNotificationQueue =
        new OneToOneConcurrentArrayQueue<>(1024);
    private final File archiveDir;

    private final Consumer<Image> newImageConsumer = this::handleNewImageNotification;
    private final AvailableImageHandler availableImageHandler = this::onAvailableImage;

    private final ClientProxy proxy;
    private volatile boolean isClosed = false;
    private final ImageRecorder.Builder imageRecorderBuilder = new ImageRecorder.Builder();
    private int replaySessionId;

    ArchiveConductor(final Aeron aeron, final Archiver.Context ctx)
    {
        this.aeron = aeron;

        archiveDir = ctx.archiveFolder();
        archiveIndex = new ArchiveIndex(archiveDir);

        imageRecorderBuilder
            .recordingFileLength(ctx.recordingFileLength())
            .archiveFolder(ctx.archiveFolder())
            .epochClock(ctx.epochClock())
            .forceMetadataUpdates(ctx.forceMetadataUpdates())
            .forceWrites(ctx.forceWrites());

        serviceRequestSubscription = aeron.addSubscription(
            ctx.serviceRequestChannel(),
            ctx.serviceRequestStreamId(),
            availableImageHandler,
            null);

        final Publication archiverNotificationPublication = aeron.addPublication(
            ctx.archiverNotificationsChannel(), ctx.archiverNotificationsStreamId());

        proxy = new ClientProxy(ctx.idleStrategy(), archiverNotificationPublication);
    }

    public String roleName()
    {
        return "archive-conductor";
    }

    public int doWork() throws Exception
    {
        int workDone = 0;

        workDone += imageNotificationQueue.drain(newImageConsumer, 10);
        workDone += doSessionsWork();

        return workDone;
    }

    public void onClose()
    {
        if (isClosed)
        {
            return;
        }

        isClosed = true;

        for (final Session session : sessions)
        {
            session.abort();
            while (!session.isDone())
            {
                session.doWork();
            }
            session.remove(this);
        }
        sessions.clear();

        if (!replaySession2IdMap.isEmpty())
        {
            // TODO: Use an error log.
            System.err.println("ERROR: expected empty replaySession2IdMap");
        }

        CloseHelper.close(archiveIndex);
    }


    void removeReplaySession(final int sessionId)
    {
        replaySession2IdMap.remove(sessionId);
    }

    private int doSessionsWork()
    {
        int workDone = 0;
        final ArrayList<Session> sessions = this.sessions;
        for (int lastIndex = sessions.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final Session session = sessions.get(i);
            workDone += session.doWork();
            if (session.isDone())
            {
                session.remove(this);
                ArrayListUtil.fastUnorderedRemove(sessions, i, lastIndex);
                lastIndex--;
            }
        }

        return workDone;
    }

    private void handleNewImageNotification(final Image image)
    {
        final Session session;
        if (image.subscription() == serviceRequestSubscription)
        {
            session = new ArchiveClientSession(image, proxy, this);
        }
        else
        {
            session = new RecordingSession(proxy, archiveIndex, image, imageRecorderBuilder);
        }
        sessions.add(session);
    }

    private void onAvailableImage(final Image image)
    {
        if (!isClosed && imageNotificationQueue.offer(image))
        {
            return;
        }
        // This is required since image available handler is called from the client conductor thread
        // we can either bridge via a queue or protect access to the sessions list, which seems clumsy.
        while (!isClosed && !imageNotificationQueue.offer(image))
        {
            Thread.yield();
        }
    }

    void stopRecording(final String channel, final int streamId)
    {
        for (final Subscription subscription : archiveSubscriptionSet)
        {
            if (subscription.streamId() == streamId && subscription.channel().equals(channel))
            {
                subscription.close();
                archiveSubscriptionSet.remove(subscription);
                break;
                // image archiving sessions will sort themselves out naturally
            }
        }
    }

    public void startRecording(final String channel, final int streamId)
    {
        for (final Subscription subscription : archiveSubscriptionSet)
        {
            if (subscription.streamId() == streamId && subscription.channel().equals(channel))
            {
                // we're already subscribed, don't bother
                return;
            }
        }

        final Subscription archiveSubscription = aeron.addSubscription(
            channel, streamId, availableImageHandler, null);

        // as subscription images are created they will get picked up and archived
        archiveSubscriptionSet.add(archiveSubscription);
    }

    public void listRecordings(
        final int correlationId,
        final ExclusivePublication reply,
        final int from,
        final int to)
    {
        final Session listSession = new ListRecordingsSession(
            correlationId, reply, from, to, archiveIndex, proxy);

        sessions.add(listSession);
    }

    public void stopReplay(final int sessionId)
    {
        final ReplaySession session = replaySession2IdMap.get(sessionId);
        if (session == null)
        {
            throw new IllegalStateException("Trying to abort an unknown replay session:" + sessionId);
        }

        session.abort();
    }

    void startReplay(
        final int correlationId,
        final ExclusivePublication reply,
        final int replayStreamId,
        final String replayChannel,
        final int recordingId,
        final int termId,
        final int termOffset,
        final long length)
    {
        final int newId = replaySessionId++;
        final ExclusivePublication replayPublication = aeron.addExclusivePublication(replayChannel, replayStreamId);
        final ReplaySession replaySession = new ReplaySession(
            recordingId,
            termId,
            termOffset,
            length,
            replayPublication,
            reply,
            archiveDir,
            proxy,
            newId,
            correlationId);

        replaySession2IdMap.put(newId, replaySession);
        sessions.add(replaySession);
    }

    ExclusivePublication clientConnect(final String channel, final int streamId)
    {
        return aeron.addExclusivePublication(channel, streamId);
    }
}
