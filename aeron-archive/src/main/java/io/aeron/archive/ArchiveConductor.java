/*
 * Copyright 2014-2018 Real Logic Ltd.
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
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static io.aeron.ChannelUriStringBuilder.integerValueOf;
import static io.aeron.CommonContext.SPY_PREFIX;
import static io.aeron.CommonContext.UDP_MEDIA;
import static io.aeron.archive.Archive.segmentFileIndex;
import static io.aeron.archive.Archive.segmentFileName;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.codecs.ControlResponseCode.ERROR;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.agrona.concurrent.status.CountersReader.METADATA_LENGTH;

abstract class ArchiveConductor extends SessionWorker<Session> implements AvailableImageHandler
{
    private static final EnumSet<StandardOpenOption> FILE_OPTIONS = EnumSet.of(READ, WRITE);
    private static final FileAttribute<?>[] NO_ATTRIBUTES = new FileAttribute[0];

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
    private final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(DataHeaderFlyweight.HEADER_LENGTH);
    private final DataHeaderFlyweight dataHeaderFlyweight = new DataHeaderFlyweight(byteBuffer);

    private final Aeron aeron;
    private final AgentInvoker aeronAgentInvoker;
    private final AgentInvoker driverAgentInvoker;
    private final EpochClock epochClock;
    private final CachedEpochClock cachedEpochClock = new CachedEpochClock();
    private final File archiveDir;
    private final FileChannel archiveDirChannel;
    private final Subscription controlSubscription;
    private final Subscription localControlSubscription;

    private final Catalog catalog;
    private final ArchiveMarkFile markFile;
    private final RecordingEventsProxy recordingEventsProxy;
    private final int maxConcurrentRecordings;
    private final int maxConcurrentReplays;

    protected final Archive.Context ctx;
    protected final ControlResponseProxy controlResponseProxy;
    protected SessionWorker<ReplaySession> replayer;
    protected SessionWorker<RecordingSession> recorder;

    private long nextControlSessionId = ThreadLocalRandom.current().nextInt();

    ArchiveConductor(final Aeron aeron, final Archive.Context ctx)
    {
        super("archive-conductor", ctx.countedErrorHandler());

        this.aeron = aeron;
        this.ctx = ctx;

        aeronAgentInvoker = aeron.conductorAgentInvoker();
        driverAgentInvoker = ctx.mediaDriverAgentInvoker();
        epochClock = ctx.epochClock();
        archiveDir = ctx.archiveDir();
        archiveDirChannel = ctx.archiveDirChannel();
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

        cachedEpochClock.update(epochClock.time());
        catalog = ctx.catalog();
        markFile = ctx.archiveMarkFile();
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
    }

    protected abstract void closeSessionWorkers();

    protected void postSessionsClose()
    {
        if (!ctx.ownsAeronClient())
        {
            for (final Subscription subscription : recordingSubscriptionMap.values())
            {
                subscription.close();
            }

            CloseHelper.close(localControlSubscription);
            CloseHelper.close(controlSubscription);
        }
    }

    protected int preWork()
    {
        int workCount = 0;

        final long nowMs = epochClock.time();
        if (cachedEpochClock.time() != nowMs)
        {
            cachedEpochClock.update(nowMs);
            markFile.updateActivityTimestamp(nowMs);
            workCount += aeronAgentInvoker.invoke();
        }

        workCount += invokeDriverConductor();

        return workCount;
    }

    protected final int invokeDriverConductor()
    {
        return null != driverAgentInvoker ? driverAgentInvoker.invoke() : 0;
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
            final String errorMessage = "max concurrent recordings reached " + maxConcurrentRecordings;
            controlSession.sendResponse(correlationId, ERROR, errorMessage, controlResponseProxy);

            return;
        }

        try
        {
            final ChannelUri channelUri = ChannelUri.parse(originalChannel);
            final String strippedChannel = strippedChannelBuilder(channelUri).build();
            final String key = makeKey(streamId, strippedChannel);
            final Subscription oldSubscription = recordingSubscriptionMap.get(key);

            if (oldSubscription == null)
            {
                final String channel = channelUri.media().equals(UDP_MEDIA) && sourceLocation == SourceLocation.LOCAL ?
                    SPY_PREFIX + strippedChannel : strippedChannel;

                final AvailableImageHandler handler = (image) ->
                    startRecordingSession(controlSession, correlationId, strippedChannel, originalChannel, image);

                final Subscription subscription = aeron.addSubscription(channel, streamId, handler, null);

                recordingSubscriptionMap.put(key, subscription);
                controlSession.sendOkResponse(correlationId, controlResponseProxy);
            }
            else
            {
                final String errorMessage = "recording already started for subscription " + key;
                controlSession.sendResponse(correlationId, ERROR, errorMessage, controlResponseProxy);
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
            final String key = makeKey(streamId, strippedChannelBuilder(ChannelUri.parse(channel)).build());
            final Subscription oldSubscription = recordingSubscriptionMap.remove(key);

            if (oldSubscription != null)
            {
                oldSubscription.close();
                controlSession.sendOkResponse(correlationId, controlResponseProxy);
            }
            else
            {
                final String errorMessage = "no recording subscription found for " + key;
                controlSession.sendResponse(correlationId, ERROR, errorMessage, controlResponseProxy);
            }
        }
        catch (final Exception ex)
        {
            errorHandler.onError(ex);
            controlSession.sendResponse(correlationId, ERROR, ex.getMessage(), controlResponseProxy);
        }
    }

    void newListRecordingsSession(
        final long correlationId,
        final long fromId,
        final int count,
        final ControlSession controlSession)
    {
        if (controlSession.activeListRecordingsSession() != null)
        {
            controlSession.sendResponse(
                correlationId, ERROR, "active listing already in progress", controlResponseProxy);
        }
        else
        {
            final ListRecordingsSession session = new ListRecordingsSession(
                correlationId,
                fromId,
                count,
                catalog,
                controlResponseProxy,
                controlSession,
                descriptorBuffer);

            addSession(session);
            controlSession.activeListRecordingsSession(session);
        }
    }

    void newListRecordingsForUriSession(
        final long correlationId,
        final long fromRecordingId,
        final int count,
        final int streamId,
        final String channel,
        final ControlSession controlSession)
    {
        if (controlSession.activeListRecordingsSession() != null)
        {
            controlSession.sendResponse(
                correlationId, ERROR, "active listing already in progress", controlResponseProxy);
        }
        else
        {
            final ListRecordingsForUriSession session = new ListRecordingsForUriSession(
                correlationId,
                fromRecordingId,
                count,
                strippedChannelBuilder(ChannelUri.parse(channel)).build(),
                streamId,
                catalog,
                controlResponseProxy,
                controlSession,
                descriptorBuffer,
                recordingDescriptorDecoder);

            addSession(session);
            controlSession.activeListRecordingsSession(session);
        }
    }

    public void listRecording(final long correlationId, final ControlSession controlSession, final long recordingId)
    {
        if (controlSession.activeListRecordingsSession() != null)
        {
            controlSession.sendResponse(
                correlationId, ERROR, "active listing already in progress", controlResponseProxy);
        }
        else if (catalog.wrapAndValidateDescriptor(recordingId, descriptorBuffer))
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
            final String errorMessage = "max concurrent replays reached " + maxConcurrentReplays;
            controlSession.sendResponse(correlationId, ERROR, errorMessage, controlResponseProxy);

            return;
        }

        if (!catalog.hasRecording(recordingId))
        {
            controlSession.sendResponse(
                correlationId, ERROR, "unknown recording id " + recordingId, controlResponseProxy);

            return;
        }

        catalog.recordingSummary(recordingId, recordingSummary);
        long replayPosition = recordingSummary.startPosition;

        if (position != NULL_POSITION)
        {
            if (!validateReplayPosition(correlationId, controlSession, recordingId, position, recordingSummary))
            {
                return;
            }

            replayPosition = position;
        }

        if (!RecordingFragmentReader.hasInitialSegmentFile(recordingSummary, archiveDir, replayPosition))
        {
            controlSession.sendResponse(
                correlationId,
                ERROR,
                "initial segment file does not exist for replay recording id: " + recordingId,
                controlResponseProxy);

            return;
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
            cachedEpochClock,
            replayPublication,
            recordingSummary,
            null == recordingSession ? null : recordingSession.recordingPosition());

        replaySessionByIdMap.put(replaySession.sessionId(), replaySession);
        replayer.addSession(replaySession);
    }

    private boolean validateReplayPosition(
        final long correlationId,
        final ControlSession controlSession,
        final long recordingId,
        final long position,
        final RecordingSummary recordingSummary)
    {
        if ((position & (FRAME_ALIGNMENT - 1)) != 0)
        {
            final String msg = "requested replay start position " + position +
                " is not a multiple of FRAME_ALIGNMENT (" + FRAME_ALIGNMENT + ") for recording " + recordingId;
            controlSession.sendResponse(correlationId, ERROR, msg, controlResponseProxy);

            return false;
        }

        final long startPosition = recordingSummary.startPosition;
        if (position - startPosition < 0)
        {
            final String msg = "requested replay start position " + position +
                ") is less than recording start position " + startPosition + " for recording " + recordingId;
            controlSession.sendResponse(correlationId, ERROR, msg, controlResponseProxy);

            return false;
        }

        final long stopPosition = recordingSummary.stopPosition;
        if (stopPosition != NULL_POSITION && position >= stopPosition)
        {
            final String msg = "requested replay start position " + position +
                " must be less than highest recorded position " + stopPosition + " for recording " + recordingId;
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
            final String errorMessage = "replay session not known: " + replaySessionId;
            controlSession.sendResponse(correlationId, ERROR, errorMessage, controlResponseProxy);
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
            final String errorMessage = "max concurrent recordings reached: " + maxConcurrentRecordings;
            controlSession.sendResponse(correlationId, ERROR, errorMessage, controlResponseProxy);

            return;
        }

        if (!catalog.hasRecording(recordingId))
        {
            final String errorMessage = "unknown recording : " + recordingId;
            controlSession.sendResponse(correlationId, ERROR, errorMessage, controlResponseProxy);

            return;
        }

        if (recordingSessionByIdMap.containsKey(recordingId))
        {
            final String errorMessage = "cannot extend active recording : " + recordingId;
            controlSession.sendResponse(correlationId, ERROR, errorMessage, controlResponseProxy);

            return;
        }

        final RecordingSummary originalRecordingSummary = new RecordingSummary();
        catalog.recordingSummary(recordingId, originalRecordingSummary);

        final ChannelUri channelUri = ChannelUri.parse(originalChannel);
        final String sessionIdStr = channelUri.get(CommonContext.SESSION_ID_PARAM_NAME);

        if (null == sessionIdStr || originalRecordingSummary.sessionId != Integer.parseInt(sessionIdStr))
        {
            final String errorMessage = "extend recording channel must contain correct sessionId: " + recordingId;
            controlSession.sendResponse(correlationId, ERROR, errorMessage, controlResponseProxy);

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
                final String errorMessage = "recording already setup for subscription: " + key;
                controlSession.sendResponse(correlationId, ERROR, errorMessage, controlResponseProxy);
            }
        }
        catch (final Exception ex)
        {
            errorHandler.onError(ex);
            controlSession.sendResponse(correlationId, ERROR, ex.getMessage(), controlResponseProxy);
        }
    }

    public void getRecordingPosition(
        final long correlationId, final ControlSession controlSession, final long recordingId)
    {
        final RecordingSession recordingSession = recordingSessionByIdMap.get(recordingId);
        final long position = null == recordingSession ? NULL_POSITION : recordingSession.recordingPosition().get();

        controlSession.sendOkResponse(correlationId, position, controlResponseProxy);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void truncateRecording(
        final long correlationId, final ControlSession controlSession, final long recordingId, final long position)
    {
        final RecordingSummary summary = validateFramePosition(correlationId, controlSession, recordingId, position);
        if (null != summary)
        {
            final long stopPosition = summary.stopPosition;
            if (stopPosition == position)
            {
                controlSession.sendOkResponse(correlationId, controlResponseProxy);
                return;
            }

            final long startPosition = summary.startPosition;
            final int segmentLength = summary.segmentFileLength;
            final int segmentIndex = segmentFileIndex(startPosition, position, segmentLength);
            final File file = new File(archiveDir, segmentFileName(recordingId, segmentIndex));

            if (position >= startPosition)
            {
                final long segmentOffset = position & (segmentLength - 1);
                final int termLength = summary.termBufferLength;
                final int termOffset = (int)(position & (termLength - 1));

                if (termOffset > 0)
                {
                    try (FileChannel fileChannel = FileChannel.open(file.toPath(), FILE_OPTIONS, NO_ATTRIBUTES))
                    {
                        byteBuffer.clear();
                        if (DataHeaderFlyweight.HEADER_LENGTH != fileChannel.read(byteBuffer, segmentOffset))
                        {
                            throw new IllegalStateException("failed to read fragment header");
                        }

                        final long termCount = position >> LogBufferDescriptor.positionBitsToShift(termLength);
                        final int termId = summary.initialTermId + (int)termCount;

                        if (dataHeaderFlyweight.termOffset() != termOffset ||
                            dataHeaderFlyweight.termId() != termId ||
                            dataHeaderFlyweight.streamId() != summary.streamId)
                        {
                            final String msg = "position " + position + " does not match header " + dataHeaderFlyweight;
                            controlSession.sendResponse(correlationId, ERROR, msg, controlResponseProxy);
                            return;
                        }

                        catalog.recordingStopped(recordingId, position);

                        fileChannel.truncate(segmentOffset);
                        byteBuffer.put(0, (byte)0).limit(1).position(0);
                        fileChannel.write(byteBuffer, segmentLength - 1);
                    }
                    catch (final IOException ex)
                    {
                        LangUtil.rethrowUnchecked(ex);
                    }
                }
                else
                {
                    catalog.recordingStopped(recordingId, position);
                    file.delete();
                }

                for (int i = segmentIndex + 1; (i * (long)segmentLength) <= stopPosition; i++)
                {
                    new File(archiveDir, segmentFileName(recordingId, i)).delete();
                }
            }

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
            controlChannel = strippedChannelBuilder(ChannelUri.parse(channel))
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
            cachedEpochClock,
            controlResponseProxy);
        addSession(controlSession);

        return controlSession;
    }

    void closeRecordingSession(final RecordingSession session)
    {
        final long recordingId = session.sessionId();
        catalog.recordingStopped(recordingId, session.recordingPosition().get(), epochClock.time());
        recordingSessionByIdMap.remove(recordingId);
        closeSession(session);
    }

    void closeReplaySession(final ReplaySession session)
    {
        replaySessionByIdMap.remove(session.sessionId());
        closeSession(session);
    }

    private ChannelUriStringBuilder strippedChannelBuilder(final ChannelUri channelUri)
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
            cachedEpochClock.time(),
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

        try
        {
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
        catch (final Exception ex)
        {
            errorHandler.onError(ex);
            position.close();
            controlSession.sendResponse(correlationId, ERROR, ex.getMessage(), controlResponseProxy);
        }
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

        try
        {
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
        catch (final Exception ex)
        {
            errorHandler.onError(ex);
            position.close();
            controlSession.sendResponse(correlationId, ERROR, ex.getMessage(), controlResponseProxy);
        }
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

        final String channel = strippedChannelBuilder(ChannelUri.parse(replayChannel))
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
            final String msg = "failed to create replay publication - " + ex;
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
            final String msg = "max concurrent recordings reached, can't record: " +
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
            final String msg = "cannot extend recording: " + originalRecordingSummary.recordingId +
                ": image joinPosition=" + image.joinPosition() +
                " not equal to recording stopPosition=" + originalRecordingSummary.stopPosition;

            controlSession.attemptErrorResponse(correlationId, msg, controlResponseProxy);
            throw new IllegalStateException(msg);
        }

        if (image.termBufferLength() != originalRecordingSummary.termBufferLength)
        {
            final String msg = "cannot extend recording: " + originalRecordingSummary.recordingId +
                ": image termBufferLength " + image.termBufferLength() +
                " not equal to recording termBufferLength=" + originalRecordingSummary.termBufferLength;

            controlSession.attemptErrorResponse(correlationId, msg, controlResponseProxy);
            throw new IllegalStateException(msg);
        }

        if (image.mtuLength() != originalRecordingSummary.mtuLength)
        {
            final String msg = "cannot extend recording: " + originalRecordingSummary.recordingId +
                ": image mtuLength " + image.mtuLength() +
                " not equal to recording mtuLength " + originalRecordingSummary.mtuLength;

            controlSession.attemptErrorResponse(correlationId, msg, controlResponseProxy);
            throw new IllegalStateException(msg);
        }
    }

    private static String makeKey(final int streamId, final String strippedChannel)
    {
        return streamId + ":" + strippedChannel;
    }

    private RecordingSummary validateFramePosition(
        final long correlationId, final ControlSession controlSession, final long recordingId, final long position)
    {
        if (!catalog.hasRecording(recordingId))
        {
            final String errorMessage = "unknown recording: " + recordingId;
            controlSession.sendResponse(correlationId, ERROR, errorMessage, controlResponseProxy);

            return null;
        }

        for (final ReplaySession replaySession : replaySessionByIdMap.values())
        {
            if (replaySession.recordingId() == recordingId)
            {
                final String errorMessage = "cannot truncate recording with active replay: " + recordingId;
                controlSession.sendResponse(correlationId, ERROR, errorMessage, controlResponseProxy);

                return null;
            }
        }

        catalog.recordingSummary(recordingId, recordingSummary);
        final long stopPosition = recordingSummary.stopPosition;
        final long startPosition = recordingSummary.startPosition;

        if (stopPosition == NULL_POSITION)
        {
            controlSession.sendResponse(
                correlationId, ERROR, "cannot truncate active recording", controlResponseProxy);

            return null;
        }

        if (position < startPosition || position > stopPosition || ((position & (FRAME_ALIGNMENT - 1)) != 0))
        {
            controlSession.sendResponse(correlationId, ERROR, "invalid position: " + position, controlResponseProxy);

            return null;
        }

        return recordingSummary;
    }
}
