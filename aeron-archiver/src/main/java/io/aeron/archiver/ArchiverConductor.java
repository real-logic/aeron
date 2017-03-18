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

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.function.Consumer;

class ArchiverConductor implements Agent, ArchiverProtocolListener
{
    interface Session
    {
        void abort();
        boolean isDone();
        void remove(ArchiverConductor conductor);
        int doWork();
    }

    private final Aeron aeron;
    private final Subscription serviceRequests;
    private final Publication archiverNotifications;

    private final ArrayList<Session> sessions = new ArrayList<>();

    private final Int2ObjectHashMap<ReplaySession> image2ReplaySession = new Int2ObjectHashMap<>();

    // TODO: archiving sessions index should be managed by the archive index
    private final Int2ObjectHashMap<ImageArchivingSession> instance2ArchivingSession = new Int2ObjectHashMap<>();

    private final ObjectHashSet<Subscription> archiveSubscriptions = new ObjectHashSet<>(128);

    private final ArchiveIndex archiveIndex;


    // TODO: arguably this is a good fit for a linked array queue so that we can have minimal footprint
    // TODO: this makes for awkward construction as we need Aeron setup before the archiver.
    // TODO: Verify this is a valid SPSC usage
    private final OneToOneConcurrentArrayQueue<Image> imageNotifications =
        new OneToOneConcurrentArrayQueue<>(1024);
    private final AvailableImageHandler availableImageHandler;
    private final File archiveFolder;
    private final EpochClock epochClock;

    private final Consumer<Image> handleNewImageNotification = this::handleNewImageNotification;
    private final ArchiverProtocolAdapter adapter = new ArchiverProtocolAdapter(this);
    private final ArchiverProtocolProxy proxy;
    private final IdleStrategy idleStrategy;

    ArchiverConductor(
        final Aeron aeron,
        final Archiver.Context ctx)
    {
        this.aeron = aeron;
        serviceRequests = aeron.addSubscription(
            ctx.serviceRequestChannel(),
            ctx.serviceRequestStreamId());

        archiverNotifications = aeron.addPublication(
            ctx.archiverNotificationsChannel(),
            ctx.archiverNotificationsStreamId());

        this.archiveFolder = ctx.archiveFolder();
        idleStrategy = ctx.idleStrategy();
        availableImageHandler = image ->
        {
            while (!imageNotifications.offer(image))
            {
                idleStrategy.idle(0);
            }
        };
        this.proxy = new ArchiverProtocolProxy(idleStrategy, archiverNotifications);
        this.epochClock = ctx.epochClock();

        archiveIndex = new ArchiveIndex(archiveFolder);
    }

    public String roleName()
    {
        return "ArchiverConductor";
    }

    public int doWork() throws Exception
    {
        int workDone = 0;

        // TODO: control tasks balance? shard/distribute tasks across threads?
        workDone += imageNotifications.drain(handleNewImageNotification);
        workDone += serviceRequests.poll(adapter, 16);
        workDone += doSessionsWork();

        return workDone;
    }

    public void onClose()
    {
        for (final Session session : sessions)
        {
            session.abort();
        }
        doSessionsWork();

        if (!sessions.isEmpty())
        {
            System.err.println("ERROR: expected empty sessions");
        }

        if (!image2ReplaySession.isEmpty())
        {
            System.err.println("ERROR: expected empty image2ReplaySession");
        }

        if (!instance2ArchivingSession.isEmpty())
        {
            System.err.println("ERROR: expected empty instance2ArchivingSession");
        }

        for (final Subscription subscription : archiveSubscriptions)
        {
            subscription.close();
        }
        archiveSubscriptions.clear();
        imageNotifications.clear();

        archiverNotifications.close();
        serviceRequests.close();
        CloseHelper.quietClose(archiveIndex);
    }

    private void handleNewImageNotification(final Image image)
    {
        final ImageArchivingSession session =
            new ImageArchivingSession(proxy, archiveIndex, archiveFolder, image, this.epochClock);
        sessions.add(session);
        instance2ArchivingSession.put(session.streamInstanceId(), session);
    }

    public void onArchiveStop(final String channel, final int streamId)
    {
        for (final Subscription archiveSubscription : archiveSubscriptions)
        {
            if (archiveSubscription.streamId() == streamId &&
                archiveSubscription.channel().equals(channel))
            {
                archiveSubscription.close();
                archiveSubscriptions.remove(archiveSubscription);
                // image archiving sessions will sort themselves out naturally
            }
        }
    }

    public void onArchiveStart(final String channel, final int streamId)
    {
        final Subscription archiveSubscription =
            aeron.addSubscription(
                channel,
                streamId,
                availableImageHandler,
                image -> {});

        // as subscription images are created they will get picked up and archived
        archiveSubscriptions.add(archiveSubscription);
    }

    public void onListStreamInstances(final int from, final int to, final String replyChannel, final int replyStreamId)
    {
        final Publication reply = aeron.addPublication(replyChannel, replyStreamId);
        final Session listSession =
            new ListDescriptorsSession(reply, from, to, this, proxy);

        sessions.add(listSession);
    }

    public void onReplayStop(final int sessionId)
    {
        final ReplaySession session = image2ReplaySession.get(sessionId);
        if (session == null)
        {
            throw new IllegalStateException("Trying to abort an unknown replay session:" + sessionId);
        }
        session.abort();
    }

    public void onReplayStart(
        final int sessionId,
        final int replyStreamId,
        final String replyChannel,
        final int streamInstanceId, final int termId, final int termOffset, final long length)
    {
        if (image2ReplaySession.containsKey(sessionId))
        {
            throw new IllegalStateException(
                "Trying to request a second replay from same session:" + sessionId);
        }
        // TODO: need to control construction of publications to handle errors
        final Image image = serviceRequests.imageBySessionId(sessionId);
        final Publication replyPublication = aeron.addPublication(replyChannel, replyStreamId);
        final ReplaySession replaySession = new ReplaySession(
            streamInstanceId,
            termId,
            termOffset,
            length,
            replyPublication,
            image,
            archiveFolder,
            proxy);

        image2ReplaySession.put(sessionId, replaySession);
        sessions.add(replaySession);
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

    ImageArchivingSession getArchivingSession(final int streamInstanceId)
    {
        return instance2ArchivingSession.get(streamInstanceId);
    }

    void removeArchivingSession(final int streamInstanceId)
    {
        instance2ArchivingSession.remove(streamInstanceId);
    }

    void removeReplaySession(final int sessionId)
    {
        image2ReplaySession.remove(sessionId);
    }

    boolean readArchiveDescriptor(final int streamInstanceId, final ByteBuffer byteBuffer) throws IOException
    {
        return archiveIndex.readArchiveDescriptor(streamInstanceId, byteBuffer);
    }

    int maxStreamInstanceId()
    {
        return archiveIndex.maxStreamInstanceId();
    }
}
