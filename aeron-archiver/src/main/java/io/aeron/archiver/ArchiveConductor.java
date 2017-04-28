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

class ArchiveConductor implements Agent, ArchiverProtocolListener
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
    private final Int2ObjectHashMap<ReplaySession> replaySessionBySessionIdMap = new Int2ObjectHashMap<>();

    private final ObjectHashSet<Subscription> archiveSubscriptionSet = new ObjectHashSet<>(128);
    private final ArchiveIndex archiveIndex;

    // TODO: arguably this is a good fit for a linked array queue so that we can have minimal footprint
    private final OneToOneConcurrentArrayQueue<Image> imageNotificationQueue =
        new OneToOneConcurrentArrayQueue<>(1024);
    private final File archiveFolder;

    private final Consumer<Image> newImageConsumer = this::handleNewImageNotification;
    private final AvailableImageHandler availableImageHandler = this::onAvailableImage;
    private final ArchiverProtocolAdapter adapter = new ArchiverProtocolAdapter(this);
    private final ArchiverProtocolProxy proxy;
    private volatile boolean isClosed = false;
    private final ArchiveStreamWriter.Builder archiveWriterBuilder = new ArchiveStreamWriter.Builder();

    ArchiveConductor(final Aeron aeron, final Archiver.Context ctx)
    {
        this.aeron = aeron;

        archiveFolder = ctx.archiveFolder();
        archiveIndex = new ArchiveIndex(archiveFolder);

        archiveWriterBuilder.archiveFileSize(ctx.archiveFileSize());
        archiveWriterBuilder.archiveFolder(ctx.archiveFolder());
        archiveWriterBuilder.epochClock(ctx.epochClock());
        archiveWriterBuilder.forceMetadataUpdates(ctx.forceMetadataUpdates());
        archiveWriterBuilder.forceWrites(ctx.forceWrites());

        serviceRequestSubscription = aeron.addSubscription(ctx.serviceRequestChannel(), ctx.serviceRequestStreamId());

        final Publication archiverNotificationPublication = aeron.addPublication(
            ctx.archiverNotificationsChannel(), ctx.archiverNotificationsStreamId());

        proxy = new ArchiverProtocolProxy(ctx.idleStrategy(), archiverNotificationPublication);
    }

    public String roleName()
    {
        return "archive-conductor";
    }

    public int doWork() throws Exception
    {
        int workDone = 0;

        // TODO: control tasks balance? shard/distribute tasks across threads?

        // this will trigger callbacks into handleNewImageNotification
        workDone += imageNotificationQueue.drain(newImageConsumer);
        // this will trigger callbacks into ArchiverProtocolListener::on* methods
        workDone += serviceRequestSubscription.poll(adapter, 16);
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

        if (!replaySessionBySessionIdMap.isEmpty())
        {
            System.err.println("ERROR: expected empty replaySessionBySessionIdMap");
        }

        CloseHelper.close(archiveIndex);
    }

    public void onArchiveStop(final String channel, final int streamId)
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

    public void onArchiveStart(final String channel, final int streamId)
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
            channel, streamId, availableImageHandler, (image) -> {});

        // as subscription images are created they will get picked up and archived
        archiveSubscriptionSet.add(archiveSubscription);
    }

    public void onListStreamInstances(final int from, final int to, final String replyChannel, final int replyStreamId)
    {
        final Publication reply = aeron.addPublication(replyChannel, replyStreamId);
        final Session listSession = new ListDescriptorsSession(reply, from, to, archiveIndex, proxy);

        sessions.add(listSession);
    }

    public void onReplayStop(final int sessionId)
    {
        final ReplaySession session = replaySessionBySessionIdMap.get(sessionId);
        if (session == null)
        {
            throw new IllegalStateException("Trying to abort an unknown replay session:" + sessionId);
        }

        session.abort();
    }

    public void onReplayStart(
        final int sessionId,
        final int replayStreamId,
        final String replayChannel,
        final int controlStreamId,
        final String controlChannel,
        final int streamInstanceId,
        final int termId,
        final int termOffset,
        final long length)
    {
        if (replaySessionBySessionIdMap.containsKey(sessionId))
        {
            throw new IllegalStateException("Trying to request a second replay from same session:" + sessionId);
        }

        // TODO: need to control construction of publications to handle errors
        final Image image = serviceRequestSubscription.imageBySessionId(sessionId);
        final ExclusivePublication replayPublication = aeron.addExclusivePublication(replayChannel, replayStreamId);
        final ExclusivePublication controlPublication = aeron.addExclusivePublication(controlChannel, controlStreamId);
        final ReplaySession replaySession = new ReplaySession(
            streamInstanceId,
            termId,
            termOffset,
            length,
            replayPublication,
            controlPublication,
            image,
            archiveFolder,
            proxy);

        replaySessionBySessionIdMap.put(sessionId, replaySession);
        sessions.add(replaySession);
    }

    void removeReplaySession(final int sessionId)
    {
        replaySessionBySessionIdMap.remove(sessionId);
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
        final ArchivingSession session = new ArchivingSession(proxy, archiveIndex, image, archiveWriterBuilder);
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
}
