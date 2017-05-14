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

import io.aeron.*;
import org.agrona.CloseHelper;
import org.agrona.collections.*;
import org.agrona.concurrent.*;

import java.io.File;
import java.util.ArrayList;
import java.util.function.Consumer;

class ArchiveConductor implements Agent
{
    /**
     * Limit on the number of items drained from the available image queue per work cycle.
     */
    private static final int QUEUE_DRAIN_LIMIT = 10;

    interface Session
    {
        void abort();

        boolean isDone();

        void remove(ArchiveConductor conductor);

        int doWork();
    }

    private final Aeron aeron;
    private final AgentInvoker aeronClientAgentInvoker;
    private final AgentInvoker driverAgentInvoker;
    private final Subscription controlSubscription;
    private final ArrayList<Session> sessions = new ArrayList<>();
    private final Long2ObjectHashMap<ReplaySession> replaySession2IdMap = new Long2ObjectHashMap<>();

    private final ObjectHashSet<Subscription> recordingSubscriptionSet = new ObjectHashSet<>(128);
    private final Catalog catalog;
    private final OneToOneConcurrentArrayQueue<Image> availableImageQueue = new OneToOneConcurrentArrayQueue<>(512);
    private final File archiveDir;

    private final Consumer<Image> newImageConsumer = this::availableImageHandler;
    private final AvailableImageHandler availableImageHandler = this::onAvailableImage;

    private final NotificationsProxy notificationsProxy;
    private final ClientSessionProxy clientProxy;
    private final EpochClock epochClock;
    private volatile boolean isClosed = false;
    private final Recorder.Builder imageRecorderBuilder = new Recorder.Builder();
    private int replaySessionId;

    ArchiveConductor(final Aeron aeron, final Archiver.Context ctx)
    {
        this.aeron = aeron;
        this.aeronClientAgentInvoker = ctx.clientContext().conductorAgentInvoker();
        this.driverAgentInvoker = ctx.driverAgentInvoker();

        archiveDir = ctx.archiveDir();
        catalog = new Catalog(archiveDir);

        imageRecorderBuilder
            .recordingFileLength(ctx.segmentFileLength())
            .archiveDir(ctx.archiveDir())
            .epochClock(ctx.epochClock())
            .forceMetadataUpdates(ctx.forceMetadataUpdates())
            .forceWrites(ctx.forceWrites());

        controlSubscription = aeron.addSubscription(
            ctx.controlRequestChannel(),
            ctx.controlRequestStreamId(),
            availableImageHandler,
            null);

        final Publication archiverNotificationPublication = aeron.addPublication(
            ctx.recordingEventsChannel(), ctx.recordingEventsStreamId());

        notificationsProxy = new NotificationsProxy(ctx.idleStrategy(), archiverNotificationPublication);
        clientProxy = new ClientSessionProxy(ctx.idleStrategy());
        epochClock = ctx.epochClock();
    }

    public String roleName()
    {
        return "archiver";
    }

    public int doWork() throws Exception
    {
        int workDone = 0;

        if (null != driverAgentInvoker)
        {
            workDone += driverAgentInvoker.invoke();
        }

        workDone += aeronClientAgentInvoker.invoke();
        workDone += availableImageQueue.drain(newImageConsumer, QUEUE_DRAIN_LIMIT);
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

        CloseHelper.close(catalog);
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

    private void availableImageHandler(final Image image)
    {
        final Session session;
        if (image.subscription() == controlSubscription)
        {
            session = new ControlSession(image, clientProxy, this);
        }
        else
        {
            session = new RecordingSession(notificationsProxy, catalog, image, imageRecorderBuilder);
        }

        sessions.add(session);
    }

    private void onAvailableImage(final Image image)
    {
        if (!isClosed && availableImageQueue.offer(image))
        {
            return;
        }
        // This is required since image available handler is called from the client conductor thread
        // we can either bridge via a queue or protect access to the sessions list, which seems clumsy.
        while (!isClosed && !availableImageQueue.offer(image))
        {
            Thread.yield();
        }
    }

    void stopRecording(final String channel, final int streamId)
    {
        for (final Subscription subscription : recordingSubscriptionSet)
        {
            if (subscription.streamId() == streamId && subscription.channel().equals(channel))
            {
                subscription.close();
                recordingSubscriptionSet.remove(subscription);
                break;
                // image archiving sessions will sort themselves out naturally
            }
        }
    }

    public void startRecording(final String channel, final int streamId)
    {
        for (final Subscription subscription : recordingSubscriptionSet)
        {
            if (subscription.streamId() == streamId && subscription.channel().equals(channel))
            {
                // we're already subscribed, don't bother
                return;
            }
        }

        final Subscription recordingSubscription = aeron.addSubscription(
            channel, streamId, availableImageHandler, null);

        // as subscription images are created they will get picked up and recorded
        recordingSubscriptionSet.add(recordingSubscription);
    }

    public void listRecordings(
        final long correlationId,
        final ExclusivePublication replyPublication,
        final int fromId,
        final int toId)
    {
        final Session listSession = new ListRecordingsSession(
            correlationId, replyPublication, fromId, toId, catalog, clientProxy);

        sessions.add(listSession);
    }

    public void stopReplay(final int sessionId)
    {
        final ReplaySession session = replaySession2IdMap.get(sessionId);
        if (session == null)
        {
            throw new IllegalStateException("Trying to abort an unknown replay session: " + sessionId);
        }

        session.abort();
    }

    void startReplay(
        final long correlationId,
        final ExclusivePublication reply,
        final int replayStreamId,
        final String replayChannel,
        final int recordingId,
        final int termId,
        final int termOffset,
        final long length)
    {
        final int newId = replaySessionId++;
        // TODO: Replay channel not setup with the correct term-length, init-term-id, term-id, term-offset, and mtu
        final ExclusivePublication replayPublication = aeron.addExclusivePublication(replayChannel, replayStreamId);
        final ReplaySession replaySession = new ReplaySession(
            recordingId,
            termId,
            termOffset,
            length,
            replayPublication,
            reply,
            archiveDir,
            clientProxy,
            newId,
            correlationId,
            this.epochClock);

        replaySession2IdMap.put(newId, replaySession);
        sessions.add(replaySession);
    }

    ExclusivePublication clientConnect(final String channel, final int streamId)
    {
        return aeron.addExclusivePublication(channel, streamId);
    }
}
