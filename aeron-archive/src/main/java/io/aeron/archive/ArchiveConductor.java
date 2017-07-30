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
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.archive.codecs.RecordingDescriptorDecoder;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.collections.IntArrayList;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.aeron.CommonContext.SPY_PREFIX;
import static java.nio.file.StandardOpenOption.*;
import static org.agrona.concurrent.status.CountersReader.COUNTER_LENGTH;
import static org.agrona.concurrent.status.CountersReader.METADATA_LENGTH;

abstract class ArchiveConductor extends SessionWorker<Session>
{
    /**
     * Low term length for control channel reflects expected low bandwidth usage.
     */
    private static final int DEFAULT_CONTROL_TERM_LENGTH = 64 * 1024;
    private static final String POSITIONS_FILE_NAME = "archive.positions";

    private final ChannelUriStringBuilder channelBuilder = new ChannelUriStringBuilder();
    private final Long2ObjectHashMap<ReplaySession> replaySessionByIdMap = new Long2ObjectHashMap<>();
    private final Long2ObjectHashMap<RecordingSession> recordingSessionByIdMap = new Long2ObjectHashMap<>();
    private final Long2ObjectHashMap<AtomicCounter> recordingPositionByIdMap = new Long2ObjectHashMap<>();
    private final Map<String, Subscription> subscriptionMap = new HashMap<>();
    private final ReplayPublicationSupplier newReplayPublication = this::newReplayPublication;
    private final UnsafeBuffer descriptorBuffer = new UnsafeBuffer();
    private final RecordingDescriptorDecoder recordingDescriptorDecoder = new RecordingDescriptorDecoder();

    private final Aeron aeron;
    private final AgentInvoker aeronClientAgentInvoker;
    private final AgentInvoker driverAgentInvoker;
    private final EpochClock epochClock;
    private final File archiveDir;
    private final FileChannel archiveDirChannel;
    private final RecordingWriter.Context recordingCtx;

    private final Subscription controlSubscription;

    private final Catalog catalog;
    private final RecordingEventsProxy recordingEventsProxy;
    private final int maxConcurrentRecordings;
    private final int maxConcurrentReplays;

    private final CountersManager recordingPositionsManager;
    private final MappedByteBuffer countersMappedBBuffer;

    protected final Archive.Context ctx;
    protected final ControlSessionProxy controlSessionProxy;
    protected SessionWorker<ReplaySession> replayer;
    protected SessionWorker<RecordingSession> recorder;

    private int replaySessionId;

    ArchiveConductor(final Aeron aeron, final Archive.Context ctx)
    {
        super("archive-conductor", ctx.errorHandler());

        this.aeron = aeron;
        this.ctx = ctx;

        aeronClientAgentInvoker = aeron.conductorAgentInvoker();
        Objects.requireNonNull(aeronClientAgentInvoker, "An aeron invoker should be present in the archive context");

        maxConcurrentRecordings = ctx.maxConcurrentRecordings();
        maxConcurrentReplays = ctx.maxConcurrentReplays();
        epochClock = ctx.epochClock();
        driverAgentInvoker = ctx.mediaDriverAgentInvoker();
        archiveDir = ctx.archiveDir();
        final int fileSyncLevel = ctx.fileSyncLevel();
        archiveDirChannel = getDirectoryChannel(archiveDir, fileSyncLevel);

        controlSessionProxy = new ControlSessionProxy(ctx.idleStrategy());

        controlSubscription = aeron.addSubscription(
            ctx.controlChannel(),
            ctx.controlStreamId(),
            this::onAvailableControlImage,
            null);

        final Publication notificationPublication = aeron.addPublication(
            ctx.recordingEventsChannel(), ctx.recordingEventsStreamId());
        recordingEventsProxy = new RecordingEventsProxy(ctx.idleStrategy(), notificationPublication);

        catalog = new Catalog(archiveDir, archiveDirChannel, fileSyncLevel, epochClock);

        final File countersFile = new File(archiveDir, POSITIONS_FILE_NAME);
        final boolean filePreExists = countersFile.exists();

        try (FileChannel channel = FileChannel.open(countersFile.toPath(), CREATE, READ, WRITE, SPARSE))
        {
            final int maxPositionCounters = this.maxConcurrentRecordings * 2;
            countersMappedBBuffer = channel.map(
                MapMode.READ_WRITE, 0, maxPositionCounters * (METADATA_LENGTH + COUNTER_LENGTH));
            final UnsafeBuffer countersMetaBuffer =
                new UnsafeBuffer(countersMappedBBuffer, 0, maxPositionCounters * METADATA_LENGTH);
            final UnsafeBuffer countersValuesBuffer = new UnsafeBuffer(
                countersMappedBBuffer,
                maxPositionCounters * METADATA_LENGTH,
                maxPositionCounters * COUNTER_LENGTH);
            recordingPositionsManager = new CountersManager(countersMetaBuffer, countersValuesBuffer);
            if (!filePreExists && archiveDirChannel != null && fileSyncLevel > 0)
            {
                archiveDirChannel.force(fileSyncLevel > 1);
            }
        }
        catch (final IOException ex)
        {
            throw new RuntimeException(ex);
        }

        recordingCtx = new RecordingWriter.Context()
            .archiveDirChannel(archiveDirChannel)
            .archiveDir(archiveDir)
            .recordingFileLength(ctx.segmentFileLength())
            .epochClock(ctx.epochClock())
            .fileSyncLevel(fileSyncLevel);
    }

    public void onStart()
    {
        replayer = constructReplayer();
        recorder = constructRecorder();

        final List<String> presetChannels = ctx.presetRecordingChannels();
        final IntArrayList presetStreamIds = ctx.presetRecordingStreamIds();

        for (int i = 0, size = presetChannels.size(); i < size; i++)
        {
            internalStartRecordingSubscription(presetChannels.get(i), presetStreamIds.get(i));
        }
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
        CloseHelper.quietClose(archiveDirChannel);
        CloseHelper.quietClose(aeronClientAgentInvoker);
        CloseHelper.quietClose(driverAgentInvoker);
        CloseHelper.quietClose(catalog);
        IoUtil.unmap(countersMappedBBuffer);
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
    private void onAvailableControlImage(final Image image)
    {
        addSession(new ControlSession(image, this, epochClock));
    }

    void stopRecording(
        final long correlationId,
        final Publication controlPublication,
        final String channel,
        final int streamId)
    {
        try
        {
            final String key = makeKey(streamId, strippedChannelBuilder(channel).build());
            final Subscription oldSubscription = subscriptionMap.remove(key);
            if (oldSubscription != null)
            {
                oldSubscription.close();
                controlSessionProxy.sendOkResponse(correlationId, controlPublication);
            }
            else
            {
                controlSessionProxy.sendResponse(
                    correlationId,
                    ControlResponseCode.ERROR,
                    "No recording subscription found for: " + key,
                    controlPublication);
            }
        }
        catch (final Exception ex)
        {
            errorHandler.onError(ex);
            controlSessionProxy.sendResponse(
                correlationId, ControlResponseCode.ERROR, ex.getMessage(), controlPublication);
        }
    }

    void startRecordingSubscription(
        final long correlationId,
        final Publication controlPublication,
        final String originalChannel,
        final int streamId)
    {
        // note that since a subscription may trigger multiple images, and therefore multiple recordings this is a soft
        // limit.
        if (recordingSessionByIdMap.size() >= maxConcurrentRecordings)
        {
            controlSessionProxy.sendResponse(
                correlationId,
                ControlResponseCode.ERROR,
                "Max concurrent recordings reached: " + maxConcurrentRecordings,
                controlPublication);

            return;
        }

        try
        {
            if (!originalChannel.startsWith(SPY_PREFIX) && !originalChannel.contains("aeron:ipc"))
            {
                controlSessionProxy.sendResponse(
                    correlationId,
                    ControlResponseCode.ERROR,
                    "Only IPC and spy subscriptions are supported.",
                    controlPublication);

                return;
            }

            final String strippedChannel = strippedChannelBuilder(originalChannel).build();
            final String key = makeKey(streamId, strippedChannel);
            final Subscription oldSubscription = subscriptionMap.get(key);
            if (oldSubscription == null)
            {
                final Subscription subscription = aeron.addSubscription(
                    strippedChannel,
                    streamId,
                    (image) -> startImageRecording(originalChannel, image),
                    null);

                subscriptionMap.put(key, subscription);
                controlSessionProxy.sendOkResponse(correlationId, controlPublication);
            }
            else
            {
                controlSessionProxy.sendResponse(
                    correlationId,
                    ControlResponseCode.ERROR,
                    "Recording already setup for subscription: " + key,
                    controlPublication);
            }
        }
        catch (final Exception ex)
        {
            errorHandler.onError(ex);
            controlSessionProxy.sendResponse(
                correlationId, ControlResponseCode.ERROR, ex.getMessage(), controlPublication);
        }
    }

    private void internalStartRecordingSubscription(final String originalChannel, final int streamId)
    {
        // note that since a subscription may trigger multiple images, and therefore multiple recordings this is a soft
        // limit.
        if (recordingSessionByIdMap.size() >= maxConcurrentRecordings)
        {
            errorHandler.onError(new IllegalArgumentException("Max concurrent recordings reached: " +
                maxConcurrentRecordings + ". Not recording: " + originalChannel + " : " + streamId));
            return;
        }

        try
        {
            if (!originalChannel.startsWith(SPY_PREFIX) && !originalChannel.contains("aeron:ipc"))
            {
                errorHandler.onError(new IllegalArgumentException("Only IPC and spy subscriptions are supported." +
                    "Not recording: " + originalChannel + " : " + streamId));

                return;
            }

            final String strippedChannel = strippedChannelBuilder(originalChannel).build();
            final String key = makeKey(streamId, strippedChannel);
            final Subscription oldSubscription = subscriptionMap.get(key);
            if (oldSubscription == null)
            {
                final Subscription subscription = aeron.addSubscription(
                    strippedChannel,
                    streamId,
                    (image) -> startImageRecording(originalChannel, image),
                    null);

                subscriptionMap.put(key, subscription);
            }
        }
        catch (final Exception ex)
        {
            errorHandler.onError(new IllegalArgumentException("Error while creating recording subscription." +
                "Not recording: " + originalChannel + " : " + streamId, ex));
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
            controlPublication,
            fromId,
            count,
            catalog,
            controlSessionProxy,
            controlSession,
            descriptorBuffer);
    }

    ListRecordingsForUriSession newListRecordingsForUriSession(
        final long correlationId,
        final Publication controlPublication,
        final long fromRecordingId,
        final int count,
        final String channel,
        final int streamId,
        final ControlSession controlSession)
    {
        return new ListRecordingsForUriSession(
            correlationId,
            controlPublication,
            fromRecordingId,
            count,
            channel,
            streamId,
            catalog,
            controlSessionProxy,
            controlSession,
            descriptorBuffer,
            recordingDescriptorDecoder);
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
            controlSessionProxy.sendResponse(
                correlationId,
                ControlResponseCode.ERROR,
                "Max concurrent replays reached: " + maxConcurrentReplays,
                controlPublication);

            return;
        }

        final UnsafeBuffer descriptorBuffer = catalog.wrapDescriptor(recordingId);
        if (descriptorBuffer == null)
        {
            controlSessionProxy.sendResponse(
                correlationId,
                ControlResponseCode.ERROR,
                "Unknown recording : " + recordingId,
                controlPublication);

            return;
        }

        final int newId = replaySessionId++;
        final AtomicCounter recordingPosition = recordingPositionByIdMap.get(recordingId);
        final ReplaySession replaySession = new ReplaySession(
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
            descriptorBuffer,
            recordingPosition);

        replaySessionByIdMap.put(newId, replaySession);
        replayer.addSession(replaySession);
    }

    Publication newControlPublication(final String channel, final int streamId)
    {
        final String controlChannel;
        if (!channel.contains(CommonContext.TERM_LENGTH_PARAM_NAME))
        {
            controlChannel = strippedChannelBuilder(channel)
                .termLength(DEFAULT_CONTROL_TERM_LENGTH)
                .build();
        }
        else
        {
            controlChannel = channel;
        }

        return aeron.addPublication(controlChannel, streamId);
    }

    private static String makeKey(final int streamId, final String minimalChannel)
    {
        return streamId + ':' + minimalChannel;
    }

    ChannelUriStringBuilder strippedChannelBuilder(final String channel)
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

    private void startImageRecording(final String originalChannel, final Image image)
    {
        if (recordingSessionByIdMap.size() >= 2 * maxConcurrentRecordings)
        {
            throw new IllegalStateException("Too many recordings, can't record: '" + originalChannel + ":" +
                image.subscription().streamId() + "'");
        }

        final Subscription subscription = image.subscription();
        final int sessionId = image.sessionId();
        final int streamId = subscription.streamId();
        final String channel = subscription.channel();
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
            channel,
            originalChannel,
            sourceIdentity);

        final AtomicCounter position =
            recordingPositionsManager.newCounter(makeKey(streamId, channel) + ":" + recordingId);
        final RecordingSession session = new RecordingSession(
            recordingId,
            catalog.wrapDescriptor(recordingId),
            recordingEventsProxy,
            image,
            position,
            recordingCtx);

        recordingSessionByIdMap.put(recordingId, session);
        recordingPositionByIdMap.put(recordingId, position);

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

        final String channel = strippedChannelBuilder(replayChannel)
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
        recordingSessionByIdMap.remove(session.sessionId());
        closeSession(session);
        final AtomicCounter position = recordingPositionByIdMap.remove(session.sessionId());
        position.close();
    }

    void closeReplaySession(final ReplaySession session)
    {
        replaySessionByIdMap.remove(session.sessionId());
        closeSession(session);
    }

    private static FileChannel getDirectoryChannel(final File directory, final int fileSyncLevel)
    {
        FileChannel dirChannel = null;
        if (fileSyncLevel > 0)
        {
            try
            {
                dirChannel = FileChannel.open(directory.toPath());
            }
            catch (final IOException ignore)
            {
                // If directories cannot be opened then we ignore.
            }
        }

        return dirChannel;
    }
}
