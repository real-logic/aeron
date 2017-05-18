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

    private final Catalog catalog;
    private final OneToOneConcurrentArrayQueue<Image> availableImageQueue = new OneToOneConcurrentArrayQueue<>(512);
    private final File archiveDir;

    private final Consumer<Image> newImageConsumer = this::availableImageHandler;
    private final AvailableImageHandler availableImageHandler = this::onAvailableImage;

    private final NotificationsProxy notificationsProxy;
    private final ControlSessionProxy clientProxy;
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
        clientProxy = new ControlSessionProxy(ctx.idleStrategy());
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

    void removeReplaySession(final long sessionId)
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
            // TODO: What happens to the state of the builder if two recordings are added before their
            // TODO: init() get called? Should the setup not happen in the constructor?
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

    void stopRecording(final long recordingId)
    {
        catalog.getRecordingSession(recordingId).abort();
    }

    public void startRecording(final String channel, final int streamId)
    {

        final Subscription recordingSubscription = aeron.addSubscription(
            channel, streamId, availableImageHandler, null);
    }

    public void listRecordings(
        final long correlationId,
        final ExclusivePublication replyPublication,
        final long fromId,
        final long toId)
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
        // TODO: replace with host/port?
        final String replayChannel,
        final long recordingId,
        final long position,
        final long length)
    {
        final int newId = replaySessionId++;
        final ReplaySession replaySession = new ReplaySession(
            recordingId,
            position,
            length,
            this,
            reply,
            archiveDir,
            clientProxy,
            newId,
            correlationId,
            this.epochClock,
            replayChannel,
            replayStreamId);

        replaySession2IdMap.put(newId, replaySession);
        sessions.add(replaySession);
    }

    ExclusivePublication clientConnect(final String channel, final int streamId)
    {
        return aeron.addExclusivePublication(channel, streamId);
    }

    ExclusivePublication newReplayPublication(
        final String replayChannel,
        final int replayStreamId,
        final long fromPosition,
        final int mtuLength,
        final int initialTermId,
        final int termBufferLength)
    {
        final int termId = (int) (fromPosition / termBufferLength + initialTermId);
        final int termOffset = (int) (fromPosition % termBufferLength);
        // TODO: can cache and reuse builder
        final StringBuilder builder = new StringBuilder(replayChannel.length() + 128);
        builder.append(replayChannel);
        if (replayChannel.contains("?"))
        {
            builder.append("|");
        }
        else
        {
            builder.append("?");
        }
        builder
            .append(CommonContext.INITIAL_TERM_ID_PARAM_NAME).append("=").append(initialTermId)
            .append("|")
            .append(CommonContext.MTU_LENGTH_URI_PARAM_NAME).append("=").append(mtuLength)
            .append("|")
            .append(CommonContext.TERM_LENGTH_PARAM_NAME).append("=").append(termBufferLength)
            .append("|")
            .append(CommonContext.TERM_ID_PARAM_NAME).append("=").append(termId)
            .append("|")
            .append(CommonContext.TERM_OFFSET_PARAM_NAME).append("=").append(termOffset);

        return aeron.addExclusivePublication(builder.toString(), replayStreamId);
    }
}
