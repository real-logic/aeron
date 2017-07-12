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
import io.aeron.archiver.codecs.ControlResponseCode;
import org.agrona.BufferUtil;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.LangUtil;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static io.aeron.CommonContext.SPY_PREFIX;
import static io.aeron.archiver.Catalog.PAGE_SIZE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

abstract class ArchiveConductor extends SessionWorker<Session>
{
    /**
     * Low term length for control channel reflect expected low bandwidth usage.
     */
    private static final int DEFAULT_CONTROL_TERM_LENGTH = 64 * 1024;

    private final ByteBuffer threadLocalDescriptorBBuffer =
        BufferUtil.allocateDirectAligned(Catalog.RECORD_LENGTH, PAGE_SIZE);
    private final UnsafeBuffer threadLocalDescriptorUBuffer = new UnsafeBuffer(threadLocalDescriptorBBuffer);
    private final ChannelUriStringBuilder channelBuilder = new ChannelUriStringBuilder();
    private final Long2ObjectHashMap<ReplaySession> replaySessionByIdMap = new Long2ObjectHashMap<>();
    private final Long2ObjectHashMap<RecordingSession> recordingSessionByIdMap = new Long2ObjectHashMap<>();
    private final Map<String, Subscription> subscriptionMap = new HashMap<>();
    private final ReplayPublicationSupplier newReplayPublication = this::newReplayPublication;
    private final AvailableImageHandler availableImageHandler = this::onAvailableImage;

    private final Aeron aeron;
    private final AgentInvoker aeronClientAgentInvoker;
    private final AgentInvoker driverAgentInvoker;
    private final EpochClock epochClock;
    private final File archiveDir;
    private final FileChannel archiveDirChannel;
    private final ErrorHandler errorHandler;
    private final RecordingWriter.Context recordingCtx;

    private final Subscription controlSubscription;
    private final Catalog catalog;
    private final RecordingEventsProxy recordingEventsProxy;
    private final int maxConcurrentRecordings;
    private final int maxConcurrentReplays;

    protected final Archiver.Context ctx;
    protected final ControlSessionProxy controlSessionProxy;
    protected SessionWorker<ReplaySession> replayer;
    protected SessionWorker<RecordingSession> recorder;

    private int replaySessionId;


    ArchiveConductor(final Aeron aeron, final Archiver.Context ctx)
    {
        super("archive-conductor");

        this.aeron = aeron;
        this.ctx = ctx;
        aeronClientAgentInvoker = aeron.conductorAgentInvoker();
        Objects.requireNonNull(aeronClientAgentInvoker, "An aeron invoker should be present in the archiver context");

        epochClock = ctx.epochClock();
        driverAgentInvoker = ctx.mediaDriverAgentInvoker();

        controlSessionProxy = new ControlSessionProxy(ctx.idleStrategy());

        controlSubscription = aeron.addSubscription(
            ctx.controlChannel(),
            ctx.controlStreamId(),
            availableImageHandler,
            null);

        catalog = new Catalog(ctx.archiveDir());

        final Publication notificationPublication = aeron.addPublication(
            ctx.recordingEventsChannel(), ctx.recordingEventsStreamId());
        recordingEventsProxy = new RecordingEventsProxy(ctx.idleStrategy(), notificationPublication);

        archiveDir = ctx.archiveDir();

        FileChannel channel = null;
        if (ctx.fileSyncLevel() > 0)
        {
            try
            {
                channel = FileChannel.open(archiveDir.toPath(), READ, WRITE);
            }
            catch (final IOException ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }
        }

        archiveDirChannel = channel;

        recordingCtx = new RecordingWriter.Context()
            .archiveDirChannel(archiveDirChannel)
            .archiveDir(ctx.archiveDir())
            .recordingFileLength(ctx.segmentFileLength())
            .epochClock(ctx.epochClock())
            .fileSyncLevel(ctx.fileSyncLevel());

        maxConcurrentRecordings = ctx.maxConcurrentRecordings();
        maxConcurrentReplays = ctx.maxConcurrentReplays();
        errorHandler = ctx.errorHandler();
    }

    public void onStart()
    {
        replayer = constructReplayer();
        recorder = constructRecorder();
    }

    protected abstract SessionWorker<RecordingSession> constructRecorder();

    protected abstract SessionWorker<ReplaySession> constructReplayer();

    protected final void preSessionsClose()
    {
        closeSessionWorkers();
        subscriptionMap.values().forEach(Subscription::close);
        subscriptionMap.clear();
    }

    protected abstract void closeSessionWorkers();

    protected void postSessionsClose()
    {
        CloseHelper.quietClose(aeronClientAgentInvoker);
        CloseHelper.quietClose(driverAgentInvoker);
        CloseHelper.quietClose(catalog);
        CloseHelper.quietClose(archiveDirChannel);

        if (!recordingSessionByIdMap.isEmpty())
        {
            System.err.println("ERROR: expected no active recording sessions");
        }

        if (!replaySessionByIdMap.isEmpty())
        {
            System.err.println("ERROR: expected no active replay sessions");
        }
    }

    protected int preWork()
    {
        int workDone = null != driverAgentInvoker ? driverAgentInvoker.invoke() : 0;
        workDone += aeronClientAgentInvoker.invoke();

        return workDone;
    }

    /**
     * Note: this is only a thread safe interaction because we are running the aeron client as an invoked agent so the
     * available image notifications are run from this agent thread.
     */
    private void onAvailableImage(final Image image)
    {
        if (image.subscription() == controlSubscription)
        {
            addSession(new ControlSession(image, this, epochClock));
        }
        else
        {
            startImageRecording(image);
        }
    }

    void stopRecording(
        final long correlationId,
        final Publication controlPublication,
        final String channel,
        final int streamId)
    {
        try
        {
            final String minimalChannel = minimalChannelBuilder(channel).build();
            final String key = makeKey(streamId, minimalChannel);
            final Subscription oldSubscription = subscriptionMap.remove(key);
            if (oldSubscription != null)
            {
                oldSubscription.close();
                controlSessionProxy.sendOkResponse(correlationId, controlPublication);
            }
            else
            {
                controlSessionProxy.sendError(
                    correlationId,
                    ControlResponseCode.ERROR,
                    "No recording found for: " + key,
                    controlPublication);
            }
        }
        catch (final Exception ex)
        {
            errorHandler.onError(ex);
            controlSessionProxy.sendError(
                correlationId, ControlResponseCode.ERROR, ex.getMessage(), controlPublication);
        }
    }

    void startRecordingSubscription(
        final long correlationId,
        final Publication controlPublication,
        final String channel,
        final int streamId)
    {
        // note that since a subscription may trigger multiple images, and therefore multiple recordings this is a soft
        // limit.
        if (recordingSessionByIdMap.size() >= maxConcurrentRecordings)
        {
            controlSessionProxy.sendError(
                correlationId,
                ControlResponseCode.ERROR,
                "Max concurrent recordings reached: " + maxConcurrentRecordings,
                controlPublication);

            return;
        }

        try
        {
            if (!channel.startsWith(SPY_PREFIX) && !channel.contains("aeron:ipc"))
            {
                controlSessionProxy.sendError(
                    correlationId,
                    ControlResponseCode.ERROR,
                    "Only IPC and spy subscriptions are supported.",
                    controlPublication);
                return;
            }
            final String minimalChannel = minimalChannelBuilder(channel).build();
            final String key = makeKey(streamId, minimalChannel);
            final Subscription oldSubscription = subscriptionMap.get(key);
            if (oldSubscription == null)
            {
                final Subscription subscription = aeron.addSubscription(
                    minimalChannel,
                    streamId,
                    availableImageHandler,
                    null);

                subscriptionMap.put(key, subscription);
                controlSessionProxy.sendOkResponse(correlationId, controlPublication);
            }
            else
            {
                controlSessionProxy.sendError(
                    correlationId,
                    ControlResponseCode.ERROR,
                    "Subscription is already being recorded: " + key,
                    controlPublication);
            }
        }
        catch (final Exception ex)
        {
            errorHandler.onError(ex);
            controlSessionProxy.sendError(
                correlationId, ControlResponseCode.ERROR, ex.getMessage(), controlPublication);
        }
    }

    ListRecordingsSession newListRecordingsSession(
        final long correlationId,
        final Publication controlPublication,
        final long fromId,
        final int count,
        final ControlSession controlSession)
    {
        return new ListRecordingsSession(
            correlationId,
            threadLocalDescriptorBBuffer,
            threadLocalDescriptorUBuffer,
            controlPublication,
            fromId,
            count,
            catalog,
            controlSessionProxy,
            controlSession);
    }

    void stopReplay(final long correlationId, final Publication controlPublication, final long replayId)
    {
        final ReplaySession session = replaySessionByIdMap.remove(replayId);
        if (session != null)
        {
            replayer.abortSession(session);
            controlSessionProxy.sendOkResponse(correlationId, controlPublication);
        }
        else
        {
            controlSessionProxy.sendError(
                correlationId,
                ControlResponseCode.REPLAY_NOT_FOUND,
                null,
                controlPublication);
        }
    }

    void startReplay(
        final long correlationId,
        final Publication controlPublication,
        final int replayStreamId,
        final String replayChannel,
        final long recordingId,
        final long position,
        final long length)
    {
        if (replaySessionByIdMap.size() >= maxConcurrentReplays)
        {
            controlSessionProxy.sendError(
                correlationId,
                ControlResponseCode.ERROR,
                "Max concurrent replays reached: " + maxConcurrentReplays,
                controlPublication);

            return;
        }

        final int newId = replaySessionId++;
        final ReplaySession replaySession = new ReplaySession(
            recordingId,
            position,
            length,
            newReplayPublication,
            controlPublication,
            archiveDir,
            controlSessionProxy,
            newId,
            correlationId,
            epochClock,
            replayChannel,
            replayStreamId,
            threadLocalDescriptorBBuffer);

        replaySessionByIdMap.put(newId, replaySession);
        replayer.addSession(replaySession);
    }

    Publication newControlPublication(final String channel, final int streamId)
    {
        final String controlChannel;
        if (!channel.contains(CommonContext.TERM_LENGTH_PARAM_NAME))
        {
            controlChannel = minimalChannelBuilder(channel)
                .termLength(DEFAULT_CONTROL_TERM_LENGTH)
                .build();
        }
        else
        {
            controlChannel = channel;
        }

        return aeron.addPublication(controlChannel, streamId);
    }

    private String makeKey(final int streamId, final String minimalChannel)
    {
        return streamId + ':' + minimalChannel;
    }

    private ChannelUriStringBuilder minimalChannelBuilder(final String channel)
    {
        final ChannelUri channelUri = ChannelUri.parse(channel);
        channelBuilder
            .clear()
            .prefix(channelUri.prefix())
            .media(channelUri.media())
            .endpoint(channelUri.get(CommonContext.ENDPOINT_PARAM_NAME))
            .networkInterface(channelUri.get(CommonContext.INTERFACE_PARAM_NAME))
            .controlEndpoint(channelUri.get(CommonContext.MDC_CONTROL_PARAM_NAME));

        return channelBuilder;
    }

    private void startImageRecording(final Image image)
    {
        final Subscription subscription = image.subscription();
        final int sessionId = image.sessionId();
        final int streamId = subscription.streamId();
        final String channel = subscription.channel();
        final String sourceIdentity = image.sourceIdentity();
        final int termBufferLength = image.termBufferLength();
        final int mtuLength = image.mtuLength();
        final int initialTermId = image.initialTermId();
        final long joinPosition = image.joinPosition();

        final long recordingId = catalog.addNewRecording(
            sessionId,
            streamId,
            channel,
            sourceIdentity,
            termBufferLength,
            mtuLength,
            initialTermId,
            joinPosition,
            recordingCtx.recordingFileLength());

        final RecordingSession session = new RecordingSession(
            recordingId,
            recordingEventsProxy,
            image,
            recordingCtx);

        recordingSessionByIdMap.put(recordingId, session);
        recorder.addSession(session);
    }

    interface ReplayPublicationSupplier
    {
        ExclusivePublication newReplayPublication(
            String replayChannel,
            int replayStreamId,
            long fromPosition,
            int mtuLength,
            int initialTermId,
            int termBufferLength);
    }

    private ExclusivePublication newReplayPublication(
        final String replayChannel,
        final int replayStreamId,
        final long fromPosition,
        final int mtuLength,
        final int initialTermId,
        final int termBufferLength)
    {
        final int termId = (int)((fromPosition / termBufferLength) + initialTermId);
        final int termOffset = (int)(fromPosition % termBufferLength);

        final String channel = minimalChannelBuilder(replayChannel)
            .mtu(mtuLength)
            .termLength(termBufferLength)
            .initialTermId(initialTermId)
            .termId(termId)
            .termOffset(termOffset)
            .build();

        return aeron.addExclusivePublication(channel, replayStreamId);
    }

    void closeRecordingSession(final RecordingSession session)
    {
        closeSession(session);
        final long recordingId = session.sessionId();
        recordingSessionByIdMap.remove(recordingId);
        try
        {
            catalog.updateRecordingMetaDataInCatalog(
                recordingId, session.endPosition(), session.joinTimestamp(), session.endTimestamp());
        }
        catch (final IOException ex)
        {
            errorHandler.onError(ex);
        }
    }

    void closeReplaySession(final ReplaySession session)
    {
        closeSession(session);
        replaySessionByIdMap.remove(session.sessionId());
    }
}
