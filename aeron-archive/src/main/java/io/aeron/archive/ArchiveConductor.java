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
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.CloseHelper;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static io.aeron.ChannelUriStringBuilder.integerValueOf;
import static io.aeron.CommonContext.SPY_PREFIX;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.codecs.ControlResponseCode.ERROR;
import static org.agrona.concurrent.status.CountersReader.METADATA_LENGTH;

abstract class ArchiveConductor extends SessionWorker<Session> implements AvailableImageHandler
{
    private static final int CONTROL_TERM_LENGTH = AeronArchive.Configuration.controlTermBufferLength();
    private static final int CONTROL_MTU = AeronArchive.Configuration.controlMtuLength();

    private final ChannelUriStringBuilder channelBuilder = new ChannelUriStringBuilder();
    private final Long2ObjectHashMap<ReplaySession> replaySessionByIdMap = new Long2ObjectHashMap<>();
    private final Long2ObjectHashMap<RecordingSession> recordingSessionByIdMap = new Long2ObjectHashMap<>();
    private final Map<String, Subscription> recordingSubscriptionMap = new HashMap<>();
    private final UnsafeBuffer descriptorBuffer = new UnsafeBuffer();
    private final RecordingDescriptorDecoder recordingDescriptorDecoder = new RecordingDescriptorDecoder();
    private final RecordingSummary recordingSummary = new RecordingSummary();
    private final UnsafeBuffer tempBuffer = new UnsafeBuffer(new byte[METADATA_LENGTH]);

    private final Aeron aeron;
    private final AgentInvoker aeronAgentInvoker;
    private final AgentInvoker driverAgentInvoker;
    private final EpochClock epochClock;
    private final NanoClock nanoClock;
    private final File archiveDir;
    private final FileChannel archiveDirChannel;
    private final Subscription controlSubscription;
    private final Subscription localControlSubscription;

    private final Catalog catalog;
    private final RecordingEventsProxy recordingEventsProxy;
    private final int maxConcurrentRecordings;
    private final int maxConcurrentReplays;

    protected final Archive.Context ctx;
    protected final ControlResponseProxy controlResponseProxy;
    protected SessionWorker<ReplaySession> replayer;
    protected SessionWorker<RecordingSession> recorder;

    private long nextControlSessionId = ThreadLocalRandom.current().nextInt();
    private long clockUpdateDeadlineNs;

    ArchiveConductor(final Aeron aeron, final Archive.Context ctx)
    {
        super("archive-conductor", ctx.countedErrorHandler());

        this.aeron = aeron;
        this.ctx = ctx;

        aeronAgentInvoker = ctx.ownsAeronClient() ? aeron.conductorAgentInvoker() : null;
        driverAgentInvoker = ctx.mediaDriverAgentInvoker();
        epochClock = ctx.epochClock();
        nanoClock = ctx.nanoClock();
        archiveDir = ctx.archiveDir();
        archiveDirChannel = channelForDirectorySync(archiveDir, ctx.fileSyncLevel());
        controlResponseProxy = new ControlResponseProxy();
        maxConcurrentRecordings = ctx.maxConcurrentRecordings();
        maxConcurrentReplays = ctx.maxConcurrentReplays();

        controlSubscription = aeron.addSubscription(
            ctx.controlChannel(), ctx.controlStreamId(), this, null);

        localControlSubscription = aeron.addSubscription(
            ctx.localControlChannel(), ctx.localControlStreamId(), this, null);

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
        recordingSubscriptionMap.values().forEach(Subscription::close);
        recordingSubscriptionMap.clear();
    }

    protected abstract void closeSessionWorkers();

    protected void postSessionsClose()
    {
        if (!ctx.ownsAeronClient())
        {
            CloseHelper.close(localControlSubscription);
            CloseHelper.close(controlSubscription);
        }

        CloseHelper.quietClose(catalog);
        CloseHelper.quietClose(archiveDirChannel);
    }

    protected int preWork()
    {
        int workCount = 0;

        workCount += null != driverAgentInvoker ? driverAgentInvoker.invoke() : 0;
        workCount += null != aeronAgentInvoker ? aeronAgentInvoker.invoke() : 0;

        updateClockAndCatalogTimestamp(nanoClock.nanoTime());

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
            final Subscription oldSubscription = recordingSubscriptionMap.get(key);

            if (oldSubscription == null)
            {
                final String channel = originalChannel.contains("udp") && sourceLocation == SourceLocation.LOCAL ?
                    SPY_PREFIX + strippedChannel : strippedChannel;

                final AvailableImageHandler handler = (image) ->
                    startRecordingSession(controlSession, correlationId, strippedChannel, originalChannel, image);

                final Subscription subscription = aeron.addSubscription(channel, streamId, handler, null);

                recordingSubscriptionMap.put(key, subscription);
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
            final Subscription oldSubscription = recordingSubscriptionMap.remove(key);

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

    public void listRecording(final long correlationId, final ControlSession controlSession, final long recordingId)
    {
        if (catalog.wrapAndValidateDescriptor(recordingId, descriptorBuffer))
        {
            controlSession.sendDescriptor(correlationId, descriptorBuffer, controlResponseProxy);
        }
        else
        {
            controlSession.sendRecordingUnknown(correlationId, recordingId, controlResponseProxy);
        }
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
                correlationId, ERROR, "Unknown recording : " + recordingId, controlResponseProxy);

            return;
        }

        catalog.recordingSummary(recordingId, recordingSummary);
        long replayPosition = recordingSummary.startPosition;

        if (position != NULL_POSITION)
        {
            if (!validateReplayPosition(correlationId, controlSession, position, recordingSummary))
            {
                return;
            }

            replayPosition = position;
        }

        final ExclusivePublication replayPublication = newReplayPublication(
            correlationId, controlSession, replayChannel, replayStreamId, replayPosition, recordingSummary);

        final RecordingSession recordingSession = recordingSessionByIdMap.get(recordingId);
        final ReplaySession replaySession = new ReplaySession(
            replayPosition,
            length,
            catalog,
            controlSession,
            archiveDir,
            controlResponseProxy,
            correlationId,
            epochClock,
            replayPublication,
            recordingSummary,
            null == recordingSession ? null : recordingSession.recordingPosition());

        replaySessionByIdMap.put(replaySession.sessionId(), replaySession);
        replayer.addSession(replaySession);
    }

    private boolean validateReplayPosition(
        final long correlationId,
        final ControlSession controlSession,
        final long position,
        final RecordingSummary recordingSummary)
    {
        if ((position & (FrameDescriptor.FRAME_ALIGNMENT - 1)) != 0)
        {
            final String msg = "requested replay start position(=" + position +
                ") is not a multiple of FRAME_ALIGNMENT (=" + FrameDescriptor.FRAME_ALIGNMENT + ")";

            controlSession.sendResponse(correlationId, ERROR, msg, controlResponseProxy);

            return false;
        }

        final long startPosition = recordingSummary.startPosition;
        if (position - startPosition < 0)
        {
            final String msg = "requested replay start position(=" + position +
                ") is before recording start position(=" + startPosition + ")";

            controlSession.sendResponse(correlationId, ERROR, msg, controlResponseProxy);

            return false;
        }

        final long stopPosition = recordingSummary.stopPosition;
        if (stopPosition != NULL_POSITION && position >= stopPosition)
        {
            final String msg = "requested replay start position(=" + position +
                ") must be before current highest recorded position(=" + stopPosition + ")";

            controlSession.sendResponse(correlationId, ERROR, msg, controlResponseProxy);

            return false;
        }

        return true;
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

    public void extendRecording(
        final long correlationId,
        final ControlSession controlSession,
        final long recordingId,
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

        if (!catalog.hasRecording(recordingId))
        {
            controlSession.sendResponse(
                correlationId, ERROR, "Unknown recording : " + recordingId, controlResponseProxy);

            return;
        }

        if (recordingSessionByIdMap.containsKey(recordingId))
        {
            controlSession.sendResponse(
                correlationId, ERROR, "Can not extend active recording : " + recordingId, controlResponseProxy);

            return;
        }

        final RecordingSummary originalRecordingSummary = new RecordingSummary();
        catalog.recordingSummary(recordingId, originalRecordingSummary);

        final ChannelUri channelUri = ChannelUri.parse(originalChannel);
        final String sessionIdStr = channelUri.get(CommonContext.SESSION_ID_PARAM_NAME);

        if (null == sessionIdStr || originalRecordingSummary.sessionId != Integer.parseInt(sessionIdStr))
        {
            controlSession.sendResponse(
                correlationId,
                ERROR,
                "Extend recording channel must contain correct sessionId: " + recordingId,
                controlResponseProxy);

            return;
        }

        try
        {
            final String strippedChannel = strippedChannelBuilder(channelUri).build();
            final String key = makeKey(streamId, strippedChannel);
            final Subscription oldSubscription = recordingSubscriptionMap.get(key);

            if (oldSubscription == null)
            {
                final String channel = originalChannel.contains("udp") && sourceLocation == SourceLocation.LOCAL ?
                    SPY_PREFIX + strippedChannel : strippedChannel;

                final AvailableImageHandler handler = (image) -> extendRecordingSession(
                    controlSession,
                    correlationId,
                    recordingId,
                    strippedChannel,
                    originalChannel,
                    originalRecordingSummary,
                    image);

                final Subscription subscription = aeron.addSubscription(channel, streamId, handler, null);

                recordingSubscriptionMap.put(key, subscription);
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
            nextControlSessionId++,
            correlationId,
            demuxer,
            publication,
            this,
            epochClock,
            controlResponseProxy);
        addSession(controlSession);

        return controlSession;
    }

    ChannelUriStringBuilder strippedChannelBuilder(final ChannelUri channelUri)
    {
        channelBuilder
            .clear()
            .media(channelUri.media())
            .endpoint(channelUri.get(CommonContext.ENDPOINT_PARAM_NAME))
            .networkInterface(channelUri.get(CommonContext.INTERFACE_PARAM_NAME))
            .controlEndpoint(channelUri.get(CommonContext.MDC_CONTROL_PARAM_NAME))
            .sessionId(integerValueOf(channelUri.get(CommonContext.SESSION_ID_PARAM_NAME)));

        return channelBuilder;
    }

    ChannelUriStringBuilder strippedChannelBuilder(final String channel)
    {
        return strippedChannelBuilder(ChannelUri.parse(channel));
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

    private void startRecordingSession(
        final ControlSession controlSession,
        final long correlationId,
        final String strippedChannel,
        final String originalChannel,
        final Image image)
    {
        validateMaxConcurrentRecordings(controlSession, correlationId, originalChannel, image);

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

        final long controlSessionId = controlSession.sessionId();
        final Counter position = RecordingPos.allocate(
            aeron, tempBuffer, recordingId, controlSessionId, correlationId, sessionId, streamId, strippedChannel);
        position.setOrdered(startPosition);

        final RecordingSession session = new RecordingSession(
            recordingId,
            startPosition,
            originalChannel,
            recordingEventsProxy,
            image,
            position,
            archiveDirChannel,
            ctx);

        recordingSessionByIdMap.put(recordingId, session);
        recorder.addSession(session);
    }

    private void extendRecordingSession(
        final ControlSession controlSession,
        final long correlationId,
        final long recordingId,
        final String strippedChannel,
        final String originalChannel,
        final RecordingSummary originalRecordingSummary,
        final Image image)
    {
        validateMaxConcurrentRecordings(controlSession, correlationId, originalChannel, image);
        validateImageForExtendRecording(correlationId, controlSession, image, originalRecordingSummary);

        final int sessionId = image.sessionId();
        final int streamId = image.subscription().streamId();
        final long newStartPosition = image.joinPosition();
        final long controlSessionId = controlSession.sessionId();

        final Counter position = RecordingPos.allocate(
            aeron, tempBuffer, recordingId, controlSessionId, correlationId, sessionId, streamId, strippedChannel);
        position.setOrdered(newStartPosition);

        final RecordingSession session = new RecordingSession(
            recordingId,
            originalRecordingSummary.startPosition,
            originalChannel,
            recordingEventsProxy,
            image,
            position,
            archiveDirChannel,
            ctx);

        catalog.extendRecording(recordingId);

        recordingSessionByIdMap.put(recordingId, session);
        recorder.addSession(session);
    }

    private ExclusivePublication newReplayPublication(
        final long correlationId,
        final ControlSession controlSession,
        final String replayChannel,
        final int replayStreamId,
        final long position,
        final RecordingSummary recording)
    {
        final int initialTermId = recording.initialTermId;
        final int termBufferLength = recording.termBufferLength;

        final int positionBitsToShift = LogBufferDescriptor.positionBitsToShift(termBufferLength);
        final int termId = LogBufferDescriptor.computeTermIdFromPosition(position, positionBitsToShift, initialTermId);
        final int termOffset = (int)(position & (termBufferLength - 1));

        final String channel = strippedChannelBuilder(replayChannel)
            .mtu(recording.mtuLength)
            .termLength(termBufferLength)
            .initialTermId(initialTermId)
            .termId(termId)
            .termOffset(termOffset)
            .build();

        try
        {
            return aeron.addExclusivePublication(channel, replayStreamId);
        }
        catch (final Exception ex)
        {
            final String msg = "Failed to create replay publication - " + ex;
            controlSession.sendResponse(correlationId, ERROR, msg, controlResponseProxy);
            throw ex;
        }
    }

    private void validateMaxConcurrentRecordings(
        final ControlSession controlSession,
        final long correlationId,
        final String originalChannel,
        final Image image)
    {
        if (recordingSessionByIdMap.size() >= maxConcurrentRecordings)
        {
            final String msg = "Max concurrent recordings reached, can't record: " +
                image.subscription().streamId() + ":" + originalChannel;

            controlSession.attemptErrorResponse(correlationId, msg, controlResponseProxy);
            throw new IllegalStateException(msg);
        }
    }

    private void validateImageForExtendRecording(
        final long correlationId,
        final ControlSession controlSession,
        final Image image,
        final RecordingSummary originalRecordingSummary)
    {
        if (image.joinPosition() != originalRecordingSummary.stopPosition)
        {
            final String msg = "Can't extend recording: " + originalRecordingSummary.recordingId +
                ": image joinPosition=" + image.joinPosition() +
                " not equal to recording stopPosition=" + originalRecordingSummary.stopPosition;

            controlSession.attemptErrorResponse(correlationId, msg, controlResponseProxy);
            throw new IllegalStateException(msg);
        }

        if (image.termBufferLength() != originalRecordingSummary.termBufferLength)
        {
            final String msg = "Can't extend recording: " + originalRecordingSummary.recordingId +
                ": image termBufferLength=" + image.termBufferLength() +
                " not equal to recording termBufferLength=" + originalRecordingSummary.termBufferLength;

            controlSession.attemptErrorResponse(correlationId, msg, controlResponseProxy);
            throw new IllegalStateException(msg);
        }

        if (image.mtuLength() != originalRecordingSummary.mtuLength)
        {
            final String msg = "Can't extend recording: " + originalRecordingSummary.recordingId +
                ": image mtuLength=" + image.mtuLength() +
                " not equal to recording mtuLength=" + originalRecordingSummary.mtuLength;

            controlSession.attemptErrorResponse(correlationId, msg, controlResponseProxy);
            throw new IllegalStateException(msg);
        }
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

    private static String makeKey(final int streamId, final String strippedChannel)
    {
        return streamId + ":" + strippedChannel;
    }

    private void updateClockAndCatalogTimestamp(final long nowNs)
    {
        if (nowNs >= clockUpdateDeadlineNs)
        {
            clockUpdateDeadlineNs = nowNs + 1_000_000;
            // TODO: update cached epochClock
            // TODO: update cached nanoClock
            catalog.updateTimestamp(epochClock.time());
        }
    }
}
