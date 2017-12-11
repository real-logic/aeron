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
package io.aeron.archive;

import io.aeron.*;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.archive.codecs.RecordingDescriptorDecoder;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import org.agrona.CloseHelper;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static io.aeron.CommonContext.SPY_PREFIX;
import static io.aeron.archive.Catalog.NULL_POSITION;
import static io.aeron.archive.codecs.ControlResponseCode.ERROR;
import static org.agrona.concurrent.status.CountersReader.METADATA_LENGTH;

abstract class ArchiveConductor extends SessionWorker<Session> implements AvailableImageHandler
{
    private static final int CONTROL_TERM_LENGTH = AeronArchive.Configuration.controlTermBufferLength();
    private static final int CONTROL_MTU = AeronArchive.Configuration.controlMtuLength();

    private final ChannelUriStringBuilder channelBuilder = new ChannelUriStringBuilder();
    private final Long2ObjectHashMap<ReplaySession> replaySessionByIdMap = new Long2ObjectHashMap<>();
    private final Long2ObjectHashMap<RecordingSession> recordingSessionByIdMap = new Long2ObjectHashMap<>();
    private final Map<String, Subscription> subscriptionMap = new HashMap<>();
    private final UnsafeBuffer descriptorBuffer = new UnsafeBuffer();
    private final RecordingDescriptorDecoder recordingDescriptorDecoder = new RecordingDescriptorDecoder();
    private final RecordingSummary recordingSummary = new RecordingSummary();
    private final UnsafeBuffer tempBuffer = new UnsafeBuffer(new byte[METADATA_LENGTH]);

    private final Aeron aeron;
    private final AgentInvoker aeronAgentInvoker;
    private final AgentInvoker driverAgentInvoker;
    private final EpochClock epochClock;
    private final File archiveDir;
    private final FileChannel archiveDirChannel;

    private final Catalog catalog;
    private final RecordingEventsProxy recordingEventsProxy;
    private final int maxConcurrentRecordings;
    private final int maxConcurrentReplays;

    protected final Archive.Context ctx;
    protected final ControlResponseProxy controlResponseProxy;
    protected SessionWorker<ReplaySession> replayer;
    protected SessionWorker<RecordingSession> recorder;

    private long replaySessionId = ThreadLocalRandom.current().nextInt();
    private long controlSessionId = ThreadLocalRandom.current().nextInt();

    ArchiveConductor(final Aeron aeron, final Archive.Context ctx)
    {
        super("archive-conductor", ctx.countedErrorHandler());

        this.aeron = aeron;
        this.ctx = ctx;

        aeronAgentInvoker = ctx.ownsAeronClient() ? aeron.conductorAgentInvoker() : null;
        driverAgentInvoker = ctx.mediaDriverAgentInvoker();
        epochClock = ctx.epochClock();
        archiveDir = ctx.archiveDir();
        archiveDirChannel = channelForDirectorySync(archiveDir, ctx.fileSyncLevel());
        controlResponseProxy = new ControlResponseProxy();
        maxConcurrentRecordings = ctx.maxConcurrentRecordings();
        maxConcurrentReplays = ctx.maxConcurrentReplays();

        aeron.addSubscription(ctx.controlChannel(), ctx.controlStreamId(), this, null);

        recordingEventsProxy = new RecordingEventsProxy(
            ctx.idleStrategy(),
            aeron.addExclusivePublication(ctx.recordingEventsChannel(), ctx.recordingEventsStreamId()));

        catalog = new Catalog(archiveDir, archiveDirChannel, ctx.fileSyncLevel(), epochClock);
    }

    public void onStart()
    {
        replayer = newReplayer();
        recorder = newRecorder();
    }

    public void onAvailableImage(final Image image)
    {
        addSession(new ControlSessionDemuxer(image, this));
    }

    protected abstract SessionWorker<RecordingSession> newRecorder();

    protected abstract SessionWorker<ReplaySession> newReplayer();

    protected final void preSessionsClose()
    {
        closeSessionWorkers();
        subscriptionMap.values().forEach(Subscription::close);
        subscriptionMap.clear();
    }

    protected abstract void closeSessionWorkers();

    protected void postSessionsClose()
    {
        CloseHelper.quietClose(catalog);
        CloseHelper.quietClose(archiveDirChannel);
        CloseHelper.quietClose(aeronAgentInvoker);
        CloseHelper.quietClose(driverAgentInvoker);
    }

    protected int preWork()
    {
        int workCount = 0;

        workCount += null != driverAgentInvoker ? driverAgentInvoker.invoke() : 0;
        workCount += null != aeronAgentInvoker ? aeronAgentInvoker.invoke() : 0;

        return workCount;
    }

    Catalog catalog()
    {
        return catalog;
    }

    void startRecordingSubscription(
        final long correlationId,
        final ControlSession controlSession,
        final int streamId,
        final String originalChannel,
        final SourceLocation sourceLocation)
    {
        if (recordingSessionByIdMap.size() >= maxConcurrentRecordings)
        {
            controlSession.sendResponse(
                correlationId,
                ERROR,
                "Max concurrent recordings reached: " + maxConcurrentRecordings,
                controlResponseProxy);

            return;
        }

        try
        {
            final String strippedChannel = strippedChannelBuilder(originalChannel).build();
            final String key = makeKey(streamId, strippedChannel);
            final Subscription oldSubscription = subscriptionMap.get(key);

            if (oldSubscription == null)
            {
                final String channel = originalChannel.contains("udp") && sourceLocation == SourceLocation.LOCAL ?
                    SPY_PREFIX + strippedChannel : strippedChannel;

                final long controlSessionId = controlSession.sessionId();

                final Subscription subscription = aeron.addSubscription(
                    channel,
                    streamId,
                    (image) -> startRecordingSession(
                        controlSessionId, correlationId, strippedChannel, originalChannel, image),
                    null);

                subscriptionMap.put(key, subscription);
                controlSession.sendOkResponse(correlationId, controlResponseProxy);
            }
            else
            {
                controlSession.sendResponse(
                    correlationId,
                    ERROR,
                    "Recording already setup for subscription: " + key,
                    controlResponseProxy);
            }
        }
        catch (final Exception ex)
        {
            errorHandler.onError(ex);
            controlSession.sendResponse(correlationId, ERROR, ex.getMessage(), controlResponseProxy);
        }
    }

    void stopRecording(
        final long correlationId,
        final ControlSession controlSession,
        final int streamId,
        final String channel)
    {
        try
        {
            final String key = makeKey(streamId, strippedChannelBuilder(channel).build());
            final Subscription oldSubscription = subscriptionMap.remove(key);

            if (oldSubscription != null)
            {
                oldSubscription.close();
                controlSession.sendOkResponse(correlationId, controlResponseProxy);
            }
            else
            {
                controlSession.sendResponse(
                    correlationId,
                    ERROR,
                    "No recording subscription found for: " + key,
                    controlResponseProxy);
            }
        }
        catch (final Exception ex)
        {
            errorHandler.onError(ex);
            controlSession.sendResponse(correlationId, ERROR, ex.getMessage(), controlResponseProxy);
        }
    }

    ListRecordingsSession newListRecordingsSession(
        final long correlationId,
        final long fromId,
        final int count,
        final ControlSession controlSession)
    {
        return new ListRecordingsSession(
            correlationId,
            fromId,
            count,
            catalog,
            controlResponseProxy,
            controlSession,
            descriptorBuffer);
    }

    ListRecordingsForUriSession newListRecordingsForUriSession(
        final long correlationId,
        final long fromRecordingId,
        final int count,
        final int streamId,
        final String channel,
        final ControlSession controlSession)
    {
        return new ListRecordingsForUriSession(
            correlationId,
            fromRecordingId,
            count,
            channel,
            streamId,
            catalog,
            controlResponseProxy,
            controlSession,
            descriptorBuffer,
            recordingDescriptorDecoder);
    }

    void startReplay(
        final long correlationId,
        final ControlSession controlSession,
        final long recordingId,
        final long position,
        final long length,
        final int replayStreamId,
        final String replayChannel)
    {
        if (replaySessionByIdMap.size() >= maxConcurrentReplays)
        {
            controlSession.sendResponse(
                correlationId,
                ERROR,
                "Max concurrent replays reached: " + maxConcurrentReplays,
                controlResponseProxy);

            return;
        }

        if (!catalog.hasRecording(recordingId))
        {
            controlSession.sendResponse(
                correlationId,
                ERROR,
                "Unknown recording : " + recordingId,
                controlResponseProxy);

            return;
        }

        catalog.recordingSummary(recordingId, recordingSummary);

        final long startPosition = recordingSummary.startPosition;
        if (position - startPosition < 0)
        {
            controlSession.sendResponse(
                correlationId,
                ControlResponseCode.ERROR,
                "requested replay start position(=" + position +
                    ") is before recording start position(=" + startPosition + ")",
                controlResponseProxy);

            return;
        }

        final long stopPosition = recordingSummary.stopPosition;
        if (stopPosition != NULL_POSITION && position >= stopPosition)
        {
            controlSession.sendResponse(
                correlationId,
                ControlResponseCode.ERROR,
                "requested replay start position(=" + position +
                    ") must be before current highest recorded position(=" + stopPosition + ")",
                controlResponseProxy);

            return;
        }

        final RecordingSession recordingSession = recordingSessionByIdMap.get(recordingId);
        final long newId = replaySessionId++;

        final ReplaySession replaySession = new ReplaySession(
            position,
            length,
            this,
            controlSession,
            archiveDir,
            controlResponseProxy,
            newId,
            correlationId,
            epochClock,
            replayChannel,
            replayStreamId,
            recordingSummary,
            null == recordingSession ? null : recordingSession.recordingPosition());

        replaySessionByIdMap.put(newId, replaySession);
        replayer.addSession(replaySession);
    }

    public void stopReplay(final long correlationId, final ControlSession controlSession, final long replaySessionId)
    {
        final ReplaySession replaySession = replaySessionByIdMap.get(replaySessionId);
        if (null == replaySession)
        {
            controlSession.sendResponse(
                correlationId, ERROR, "Replay session not known: id=" + replaySessionId, controlResponseProxy);
        }
        else
        {
            replaySession.abort();
            controlSession.sendOkResponse(correlationId, controlResponseProxy);
        }
    }

    ControlSession newControlSession(
        final long correlationId,
        final int streamId,
        final String channel,
        final ControlSessionDemuxer demuxer)
    {
        final String controlChannel;
        if (!channel.contains(CommonContext.TERM_LENGTH_PARAM_NAME))
        {
            controlChannel = strippedChannelBuilder(channel)
                .termLength(CONTROL_TERM_LENGTH)
                .mtu(CONTROL_MTU)
                .build();
        }
        else
        {
            controlChannel = channel;
        }

        final Publication publication = aeron.addPublication(controlChannel, streamId);

        final ControlSession controlSession = new ControlSession(
            controlSessionId++,
            correlationId,
            demuxer,
            publication,
            this,
            epochClock,
            controlResponseProxy);
        addSession(controlSession);

        return controlSession;
    }

    ChannelUriStringBuilder strippedChannelBuilder(final String channel)
    {
        final ChannelUri channelUri = ChannelUri.parse(channel);
        channelBuilder
            .clear()
            .media(channelUri.media())
            .endpoint(channelUri.get(CommonContext.ENDPOINT_PARAM_NAME))
            .networkInterface(channelUri.get(CommonContext.INTERFACE_PARAM_NAME))
            .controlEndpoint(channelUri.get(CommonContext.MDC_CONTROL_PARAM_NAME));

        return channelBuilder;
    }

    void closeRecordingSession(final RecordingSession session)
    {
        final long sessionId = session.sessionId();

        recordingSessionByIdMap.remove(sessionId);
        catalog.recordingStopped(sessionId, session.recordingPosition().get(), epochClock.time());

        closeSession(session);
    }

    void closeReplaySession(final ReplaySession session)
    {
        replaySessionByIdMap.remove(session.sessionId());
        closeSession(session);
    }

    private static String makeKey(final int streamId, final String strippedChannel)
    {
        return streamId + ":" + strippedChannel;
    }

    private void startRecordingSession(
        final long controlSessionId,
        final long correlationId,
        final String strippedChannel,
        final String originalChannel,
        final Image image)
    {
        if (recordingSessionByIdMap.size() >= 2 * maxConcurrentRecordings)
        {
            throw new IllegalStateException(
                "Too many recordings, can't record: " + originalChannel + ":" + image.subscription().streamId());
        }

        final int sessionId = image.sessionId();
        final int streamId = image.subscription().streamId();
        final String sourceIdentity = image.sourceIdentity();
        final int termBufferLength = image.termBufferLength();
        final int mtuLength = image.mtuLength();
        final int initialTermId = image.initialTermId();
        final long startPosition = image.joinPosition();

        final long recordingId = catalog.addNewRecording(
            startPosition,
            epochClock.time(),
            initialTermId,
            ctx.segmentFileLength(),
            termBufferLength,
            mtuLength,
            sessionId,
            streamId,
            strippedChannel,
            originalChannel,
            sourceIdentity);

        final Counter position = RecordingPos.allocate(
            aeron, tempBuffer, recordingId, controlSessionId, correlationId, sessionId, streamId, strippedChannel);
        position.setOrdered(startPosition);

        final RecordingSession session = new RecordingSession(
            recordingId,
            originalChannel,
            recordingEventsProxy,
            image,
            position,
            archiveDirChannel,
            ctx);

        recordingSessionByIdMap.put(recordingId, session);
        recorder.addSession(session);
    }

    ExclusivePublication newReplayPublication(
        final String replayChannel,
        final int replayStreamId,
        final long fromPosition,
        final int mtuLength,
        final int initialTermId,
        final int termBufferLength)
    {
        final int termId = (int)((fromPosition / termBufferLength) + initialTermId);
        final int termOffset = (int)(fromPosition % termBufferLength);

        final String channel = strippedChannelBuilder(replayChannel)
            .mtu(mtuLength)
            .termLength(termBufferLength)
            .initialTermId(initialTermId)
            .termId(termId)
            .termOffset(termOffset)
            .build();

        return aeron.addExclusivePublication(channel, replayStreamId);
    }

    private static FileChannel channelForDirectorySync(final File directory, final int fileSyncLevel)
    {
        if (fileSyncLevel > 0)
        {
            try
            {
                return FileChannel.open(directory.toPath());
            }
            catch (final IOException ignore)
            {
            }
        }

        return null;
    }
}
