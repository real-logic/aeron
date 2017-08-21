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
import io.aeron.archive.codecs.RecordingDescriptorDecoder;
import io.aeron.archive.codecs.RecordingDescriptorEncoder;
import io.aeron.archive.codecs.SourceLocation;
import org.agrona.CloseHelper;
import org.agrona.UnsafeAccess;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

import static io.aeron.CommonContext.SPY_PREFIX;
import static io.aeron.archive.codecs.ControlResponseCode.ERROR;

abstract class ArchiveConductor extends SessionWorker<Session>
{
    private static final int CONTROL_TERM_LENGTH = AeronArchive.Configuration.controlTermBufferLength();
    private static final int CONTROL_MTU = AeronArchive.Configuration.controlMtuLength();

    private final ChannelUriStringBuilder channelBuilder = new ChannelUriStringBuilder();
    private final Long2ObjectHashMap<ReplaySession> replaySessionByIdMap = new Long2ObjectHashMap<>();
    private final Long2ObjectHashMap<RecordingSession> recordingSessionByIdMap = new Long2ObjectHashMap<>();
    private final Long2ObjectHashMap<AtomicCounter> recordingPositionByIdMap = new Long2ObjectHashMap<>();
    private final Map<String, Subscription> subscriptionMap = new HashMap<>();
    private final ReplayPublicationSupplier newReplayPublication = this::newReplayPublication;
    private final UnsafeBuffer descriptorBuffer = new UnsafeBuffer();
    private final RecordingDescriptorDecoder recordingDescriptorDecoder = new RecordingDescriptorDecoder();
    private final RecordingDescriptorEncoder recordingDescriptorEncoder = new RecordingDescriptorEncoder();

    private final Aeron aeron;
    private final AgentInvoker aeronAgentInvoker;
    private final AgentInvoker driverAgentInvoker;
    private final EpochClock epochClock;
    private final File archiveDir;
    private final FileChannel archiveDirChannel;
    private final RecordingWriter.Context recordingCtx;

    private final Catalog catalog;
    private final RecordingEventsProxy recordingEventsProxy;
    private final int maxConcurrentRecordings;
    private final int maxConcurrentReplays;
    private final CountersManager countersManager;

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

        aeronAgentInvoker = aeron.conductorAgentInvoker();
        Objects.requireNonNull(aeronAgentInvoker, "An aeron invoker should be present in the context");

        maxConcurrentRecordings = ctx.maxConcurrentRecordings();
        maxConcurrentReplays = ctx.maxConcurrentReplays();
        epochClock = ctx.epochClock();
        driverAgentInvoker = ctx.mediaDriverAgentInvoker();
        archiveDir = ctx.archiveDir();
        final int fileSyncLevel = ctx.fileSyncLevel();
        archiveDirChannel = channelForDirectorySync(archiveDir, fileSyncLevel);

        controlResponseProxy = new ControlResponseProxy();

        aeron.addSubscription(ctx.controlChannel(), ctx.controlStreamId(), this::onControlConnection, null);

        recordingEventsProxy = new RecordingEventsProxy(
            ctx.idleStrategy(), aeron.addPublication(ctx.recordingEventsChannel(), ctx.recordingEventsStreamId()));

        catalog = new Catalog(archiveDir, archiveDirChannel, fileSyncLevel, epochClock);
        countersManager = ctx.countersManager();

        recordingCtx = new RecordingWriter.Context()
            .archiveDirChannel(archiveDirChannel)
            .archiveDir(archiveDir)
            .recordingFileLength(ctx.segmentFileLength())
            .epochClock(ctx.epochClock())
            .fileSyncLevel(fileSyncLevel);
    }

    public void onStart()
    {
        replayer = newReplayer();
        recorder = newRecorder();
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
        workCount += aeronAgentInvoker.invoke();

        return workCount;
    }

    /**
     * Note: this is only a thread safe interaction because we are running the aeron client as an invoked agent so the
     * available image notifications are run from this agent thread.
     */
    private void onControlConnection(final Image image)
    {
        addSession(new ControlSessionDemuxer(image, this));
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

    void startRecordingSubscription(
        final long correlationId,
        final ControlSession controlSession,
        final int streamId,
        final String originalChannel,
        final SourceLocation sourceLocation)
    {
        // note that since a subscription may trigger multiple images, and therefore multiple recordings this is a soft
        // limit.
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

                final Subscription subscription = aeron.addSubscription(
                    channel,
                    streamId,
                    (image) -> startImageRecording(strippedChannel, originalChannel, image),
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

        final UnsafeBuffer descriptorBuffer = catalog.wrapDescriptor(recordingId);
        if (descriptorBuffer == null)
        {
            controlSession.sendResponse(
                correlationId,
                ERROR,
                "Unknown recording : " + recordingId,
                controlResponseProxy);

            return;
        }

        final long newId = replaySessionId++;
        final ReplaySession replaySession = new ReplaySession(
            position,
            length,
            newReplayPublication,
            controlSession,
            archiveDir,
            controlResponseProxy,
            newId,
            correlationId,
            epochClock,
            replayChannel,
            replayStreamId,
            descriptorBuffer,
            recordingPositionByIdMap.get(recordingId));

        replaySessionByIdMap.put(newId, replaySession);
        replayer.addSession(replaySession);
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

    private static String makeKey(final int streamId, final String strippedChannel)
    {
        return streamId + ':' + strippedChannel;
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

    private void startImageRecording(final String strippedChannel, final String originalChannel, final Image image)
    {
        if (recordingSessionByIdMap.size() >= 2 * maxConcurrentRecordings)
        {
            throw new IllegalStateException("Too many recordings, can't record: '" + originalChannel + ":" +
                image.subscription().streamId() + "'");
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
            recordingCtx.recordingFileLength(),
            termBufferLength,
            mtuLength,
            sessionId,
            streamId,
            strippedChannel,
            originalChannel,
            sourceIdentity);

        final AtomicCounter position = newRecordingPositionCounter(recordingId, sessionId, streamId, strippedChannel);

        final RecordingSession session = new RecordingSession(
            recordingId,
            catalog.wrapDescriptor(recordingId),
            recordingEventsProxy,
            strippedChannel,
            image,
            position,
            recordingCtx);

        recordingSessionByIdMap.put(recordingId, session);
        recordingPositionByIdMap.put(recordingId, position);

        recorder.addSession(session);
    }

    void closeRecordingSession(final RecordingSession session)
    {
        recordingSessionByIdMap.remove(session.sessionId());
        closeSession(session);

        final AtomicCounter position = recordingPositionByIdMap.remove(session.sessionId());
        Catalog.wrapDescriptorEncoder(recordingDescriptorEncoder, session.descriptorBuffer());
        recordingDescriptorEncoder.stopPosition(position.get());
        recordingDescriptorEncoder.stopTimestamp(epochClock.time());

        UnsafeAccess.UNSAFE.storeFence();

        position.close();
    }

    void closeReplaySession(final ReplaySession session)
    {
        replaySessionByIdMap.remove(session.sessionId());
        closeSession(session);
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

    private AtomicCounter newRecordingPositionCounter(
        final long recordingId,
        final int sessionId,
        final int streamId,
        final String strippedChannel)
    {
        final String label = "rec-pos: " + recordingId + ' ' + sessionId + ' ' + streamId + ' ' + strippedChannel;

        return countersManager.newCounter(
            label,
            Archive.Configuration.ARCHIVE_RECORDING_POSITION_TYPE_ID,
            (buffer) -> buffer.putLong(0, recordingId));
    }
}
