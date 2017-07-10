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
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static io.aeron.archiver.Catalog.PAGE_SIZE;

abstract class ArchiveConductor extends SessionWorker<Session>
{
    /**
     * Low term length for control channel reflect expected low bandwidth usage.
     */
    private static final String DEFAULT_CONTROL_CHANNEL_TERM_LENGTH_PARAM =
        CommonContext.TERM_LENGTH_PARAM_NAME + "=" + Integer.toString(64 * 1024);

    private final ByteBuffer threadLocalDescriptorBBuffer =
        BufferUtil.allocateDirectAligned(Catalog.RECORD_LENGTH, PAGE_SIZE);
    private final UnsafeBuffer threadLocalDescriptorUBuffer = new UnsafeBuffer(threadLocalDescriptorBBuffer);
    private final StringBuilder uriBuilder = new StringBuilder(1024);
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
    private final ErrorHandler errorHandler;
    private final RecordingWriter.RecordingContext recordingContext;

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
        super("archiver-conductor");

        this.aeron = aeron;
        this.ctx = ctx;
        aeronClientAgentInvoker = aeron.conductorAgentInvoker();
        Objects.requireNonNull(aeronClientAgentInvoker, "In the archiver context an aeron invoker should be present");

        epochClock = ctx.epochClock();
        this.driverAgentInvoker = ctx.mediaDriverAgentInvoker();

        controlSessionProxy = new ControlSessionProxy(ctx.idleStrategy());

        controlSubscription = aeron.addSubscription(
            ctx.controlChannel(),
            ctx.controlStreamId(),
            availableImageHandler,
            null);

        catalog = new Catalog(ctx.archiveDir());

        final Publication notificationPublication =
            aeron.addPublication(ctx.recordingEventsChannel(), ctx.recordingEventsStreamId());
        recordingEventsProxy = new RecordingEventsProxy(ctx.idleStrategy(), notificationPublication);

        this.recordingContext = new RecordingWriter.RecordingContext()
            .recordingFileLength(ctx.segmentFileLength())
            .archiveDir(ctx.archiveDir())
            .epochClock(ctx.epochClock())
            .forceWrites(ctx.forceDataWrites());

        this.archiveDir = ctx.archiveDir();
        this.maxConcurrentRecordings = ctx.maxConcurrentRecordings();
        this.maxConcurrentReplays = ctx.maxConcurrentReplays();
        this.errorHandler = ctx.errorHandler();
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
        subscriptionMap.forEach((k, v) -> v.close());
        subscriptionMap.clear();
    }

    protected abstract void closeSessionWorkers();

    protected void postSessionsClose()
    {
        CloseHelper.quietClose(aeronClientAgentInvoker);
        CloseHelper.quietClose(driverAgentInvoker);
        CloseHelper.quietClose(catalog);

        if (!recordingSessionByIdMap.isEmpty())
        {
            System.err.println("ERROR: expected empty recordingSessionByIdMap");
        }

        if (!replaySessionByIdMap.isEmpty())
        {
            System.err.println("ERROR: expected empty replaySessionByIdMap");
        }
    }

    protected int preWork()
    {
        int workDone = (null != driverAgentInvoker) ? driverAgentInvoker.invoke() : 0;
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
            final ChannelUri channelUri = ChannelUri.parse(channel);

            final ChannelUriStringBuilder channelBuilder = new ChannelUriStringBuilder()
                .prefix(channelUri.prefix())
                .media(channelUri.media())
                .endpoint(channelUri.get(CommonContext.ENDPOINT_PARAM_NAME))
                .controlEndpoint(channelUri.get(CommonContext.MDC_CONTROL_PARAM_NAME));

            final String minimalChannel = channelBuilder.build();
            final String key = "channel=" + minimalChannel + " streamId=" + streamId;
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
                    "The subscription " + key + " is not being recorded.",
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
                "Max concurrent recording reached: " + maxConcurrentRecordings,
                controlPublication);
            return;
        }


        try
        {
            final ChannelUri channelUri = ChannelUri.parse(channel);
            if (!ChannelUri.SPY_QUALIFIER.equals(channelUri.prefix()) && !"ipc".equals(channelUri.media()))
            {
                controlSessionProxy.sendError(
                    correlationId,
                    ControlResponseCode.ERROR,
                    "Can only record IPC or spy subscriptions at this time",
                    controlPublication);
                return;
            }

            final ChannelUriStringBuilder channelBuilder = new ChannelUriStringBuilder()
                .prefix(channelUri.prefix())
                .media(channelUri.media())
                .endpoint(channelUri.get(CommonContext.ENDPOINT_PARAM_NAME))
                .controlEndpoint(channelUri.get(CommonContext.MDC_CONTROL_PARAM_NAME));

            final String minimalChannel = channelBuilder.build();
            final String key = "channel=" + minimalChannel + " streamId=" + streamId;
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
                    "The subscription " + key + " is already being recorded.",
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
            recordingContext.recordingFileLength());

        final RecordingSession session = new RecordingSession(
            recordingId,
            recordingEventsProxy,
            image,
            recordingContext);

        recordingSessionByIdMap.put(recordingId, session);
        recorder.addSession(session);
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
            initUriBuilder(channel);
            uriBuilder.append(DEFAULT_CONTROL_CHANNEL_TERM_LENGTH_PARAM);
            controlChannel = uriBuilder.toString();
        }
        else
        {
            controlChannel = channel;
        }

        return aeron.addPublication(controlChannel, streamId);
    }

    private void initUriBuilder(final String channel)
    {
        uriBuilder.setLength(0);
        uriBuilder.append(channel);

        if (channel.indexOf('?', 0) > -1)
        {
            uriBuilder.append('|');
        }
        else
        {
            uriBuilder.append('?');
        }
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
        // TODO: strip channel to minimal set of media/endpoint? maybe change API to specify only those 2?
        initUriBuilder(replayChannel);

        uriBuilder
            .append(CommonContext.INITIAL_TERM_ID_PARAM_NAME).append('=').append(initialTermId)
            .append('|')
            .append(CommonContext.MTU_LENGTH_PARAM_NAME).append('=').append(mtuLength)
            .append('|')
            .append(CommonContext.TERM_LENGTH_PARAM_NAME).append('=').append(termBufferLength)
            .append('|')
            .append(CommonContext.TERM_ID_PARAM_NAME).append('=').append(termId)
            .append('|')
            .append(CommonContext.TERM_OFFSET_PARAM_NAME).append('=').append(termOffset);

        return aeron.addExclusivePublication(uriBuilder.toString(), replayStreamId);
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
