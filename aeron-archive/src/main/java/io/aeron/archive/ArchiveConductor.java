/*
 * Copyright 2014-2019 Real Logic Ltd.
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
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.codecs.RecordingDescriptorDecoder;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.SemanticVersion;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.util.ArrayDeque;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.aeron.CommonContext.SPY_PREFIX;
import static io.aeron.CommonContext.UDP_MEDIA;
import static io.aeron.archive.Archive.segmentFileIndex;
import static io.aeron.archive.Archive.segmentFileName;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.client.ArchiveException.*;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.agrona.BufferUtil.allocateDirectAligned;
import static org.agrona.concurrent.status.CountersReader.METADATA_LENGTH;

abstract class ArchiveConductor extends SessionWorker<Session> implements AvailableImageHandler
{
    private static final EnumSet<StandardOpenOption> FILE_OPTIONS = EnumSet.of(READ, WRITE);
    private static final FileAttribute<?>[] NO_ATTRIBUTES = new FileAttribute[0];

    private final ArrayDeque<Runnable> taskQueue = new ArrayDeque<>();
    private final ChannelUriStringBuilder channelBuilder = new ChannelUriStringBuilder();
    private final Long2ObjectHashMap<ReplaySession> replaySessionByIdMap = new Long2ObjectHashMap<>();
    private final Long2ObjectHashMap<RecordingSession> recordingSessionByIdMap = new Long2ObjectHashMap<>();
    private final Object2ObjectHashMap<String, Subscription> recordingSubscriptionMap = new Object2ObjectHashMap<>();
    private final RecordingSummary recordingSummary = new RecordingSummary();
    private final UnsafeBuffer descriptorBuffer = new UnsafeBuffer();
    private final RecordingDescriptorDecoder recordingDescriptorDecoder = new RecordingDescriptorDecoder();
    private final ControlResponseProxy controlResponseProxy = new ControlResponseProxy();
    private final UnsafeBuffer tempBuffer = new UnsafeBuffer(new byte[METADATA_LENGTH]);
    private final UnsafeBuffer dataHeaderBuffer = new UnsafeBuffer(
        allocateDirectAligned(DataHeaderFlyweight.HEADER_LENGTH, 128));
    private final UnsafeBuffer replayBuffer = new UnsafeBuffer(
        allocateDirectAligned(Archive.Configuration.MAX_BLOCK_LENGTH, 128));

    private final Aeron aeron;
    private final AgentInvoker aeronAgentInvoker;
    private final AgentInvoker driverAgentInvoker;
    private final EpochClock epochClock;
    private final CachedEpochClock cachedEpochClock = new CachedEpochClock();
    private final File archiveDir;
    private final FileChannel archiveDirChannel;
    private final Subscription controlSubscription;
    private final Subscription localControlSubscription;
    private final long connectTimeoutMs;

    private final Catalog catalog;
    private final ArchiveMarkFile markFile;
    private final RecordingEventsProxy recordingEventsProxy;
    private final int maxConcurrentRecordings;
    private final int maxConcurrentReplays;
    private int replayId = 1;

    protected final Archive.Context ctx;
    protected SessionWorker<ReplaySession> replayer;
    protected SessionWorker<RecordingSession> recorder;

    private long nextControlSessionId = ThreadLocalRandom.current().nextInt();

    ArchiveConductor(final Archive.Context ctx)
    {
        super("archive-conductor", ctx.countedErrorHandler());

        this.ctx = ctx;

        aeron = ctx.aeron();
        aeronAgentInvoker = aeron.conductorAgentInvoker();
        driverAgentInvoker = ctx.mediaDriverAgentInvoker();
        epochClock = ctx.epochClock();
        archiveDir = ctx.archiveDir();
        archiveDirChannel = ctx.archiveDirChannel();
        maxConcurrentRecordings = ctx.maxConcurrentRecordings();
        maxConcurrentReplays = ctx.maxConcurrentReplays();
        connectTimeoutMs = TimeUnit.NANOSECONDS.toMillis(ctx.connectTimeoutNs());

        final ChannelUri controlChannelUri = ChannelUri.parse(ctx.controlChannel());
        controlChannelUri.put(CommonContext.SPARSE_PARAM_NAME, Boolean.toString(ctx.controlTermBufferSparse()));
        controlSubscription = aeron.addSubscription(controlChannelUri.toString(), ctx.controlStreamId(), this, null);

        localControlSubscription = aeron.addSubscription(
            ctx.localControlChannel(), ctx.localControlStreamId(), this, null);

        recordingEventsProxy = new RecordingEventsProxy(
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
            CloseHelper.close(recordingEventsProxy);
        }

        ctx.close();
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
        workCount += runTasks(taskQueue);

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
            final String msg = "max concurrent recordings reached " + maxConcurrentRecordings;
            controlSession.sendErrorResponse(correlationId, MAX_RECORDINGS, msg, controlResponseProxy);

            return;
        }

        try
        {
            final ChannelUri channelUri = ChannelUri.parse(originalChannel);
            final String strippedChannel = strippedChannelBuilder(channelUri).build();
            final String key = makeKey(streamId, channelUri);
            final Subscription oldSubscription = recordingSubscriptionMap.get(key);

            if (oldSubscription == null)
            {
                final String channel = channelUri.media().equals(UDP_MEDIA) && sourceLocation == SourceLocation.LOCAL ?
                    SPY_PREFIX + strippedChannel : strippedChannel;

                final AvailableImageHandler handler = (image) -> taskQueue.addLast(() -> startRecordingSession(
                    controlSession, correlationId, strippedChannel, originalChannel, image));

                final Subscription subscription = aeron.addSubscription(channel, streamId, handler, null);

                recordingSubscriptionMap.put(key, subscription);
                controlSession.sendOkResponse(correlationId, subscription.registrationId(), controlResponseProxy);
            }
            else
            {
                final String msg = "recording exists for streamId=" + streamId + " channel=" + originalChannel;
                controlSession.sendErrorResponse(correlationId, ACTIVE_SUBSCRIPTION, msg, controlResponseProxy);
            }
        }
        catch (final Exception ex)
        {
            errorHandler.onError(ex);
            controlSession.sendErrorResponse(correlationId, ex.getMessage(), controlResponseProxy);
        }
    }

    void stopRecording(
        final long correlationId, final ControlSession controlSession, final int streamId, final String channel)
    {
        try
        {
            final String key = makeKey(streamId, ChannelUri.parse(channel));
            final Subscription oldSubscription = recordingSubscriptionMap.remove(key);

            if (oldSubscription != null)
            {
                oldSubscription.close();
                controlSession.sendOkResponse(correlationId, controlResponseProxy);
            }
            else
            {
                final String msg = "no recording found for streamId=" + streamId + " channel=" + channel;
                controlSession.sendErrorResponse(correlationId, UNKNOWN_SUBSCRIPTION, msg, controlResponseProxy);
            }
        }
        catch (final Exception ex)
        {
            errorHandler.onError(ex);
            controlSession.sendErrorResponse(correlationId, ex.getMessage(), controlResponseProxy);
        }
    }

    void stopRecordingSubscription(
        final long correlationId, final ControlSession controlSession, final long subscriptionId)
    {
        final Iterator<Map.Entry<String, Subscription>> iter = recordingSubscriptionMap.entrySet().iterator();
        while (iter.hasNext())
        {
            final Map.Entry<String, Subscription> entry = iter.next();
            final Subscription subscription = entry.getValue();
            if (subscription.registrationId() == subscriptionId)
            {
                iter.remove();
                subscription.close();
                controlSession.sendOkResponse(correlationId, controlResponseProxy);

                return;
            }
        }

        final String msg = "no recording subscription found for " + subscriptionId;
        controlSession.sendErrorResponse(correlationId, UNKNOWN_SUBSCRIPTION, msg, controlResponseProxy);
    }

    void newListRecordingsSession(
        final long correlationId, final long fromId, final int count, final ControlSession controlSession)
    {
        if (controlSession.activeListRecordingsSession() != null)
        {
            final String msg = "active listing already in progress";
            controlSession.sendErrorResponse(correlationId, ACTIVE_LISTING, msg, controlResponseProxy);
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
        final byte[] channelFragment,
        final ControlSession controlSession)
    {
        if (controlSession.activeListRecordingsSession() != null)
        {
            final String msg = "active listing already in progress";
            controlSession.sendErrorResponse(correlationId, ACTIVE_LISTING, msg, controlResponseProxy);
        }
        else
        {
            final ListRecordingsForUriSession session = new ListRecordingsForUriSession(
                correlationId,
                fromRecordingId,
                count,
                channelFragment,
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

    void listRecording(final long correlationId, final ControlSession controlSession, final long recordingId)
    {
        if (controlSession.activeListRecordingsSession() != null)
        {
            final String msg = "active listing already in progress";
            controlSession.sendErrorResponse(correlationId, ACTIVE_LISTING, msg, controlResponseProxy);
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

    void findLastMatchingRecording(
        final long correlationId,
        final long minRecordingId,
        final int sessionId,
        final int streamId,
        final byte[] channelFragment,
        final ControlSession controlSession)
    {
        if (minRecordingId < 0 || minRecordingId >= catalog.countEntries())
        {
            final String msg = "min recording id outside valid range: " + minRecordingId;
            controlSession.sendErrorResponse(correlationId, UNKNOWN_RECORDING, msg, controlResponseProxy);
        }
        else
        {
            final long recordingId = catalog.findLast(minRecordingId, sessionId, streamId, channelFragment);
            controlSession.sendOkResponse(correlationId, recordingId, controlResponseProxy);
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
            final String msg = "max concurrent replays reached " + maxConcurrentReplays;
            controlSession.sendErrorResponse(correlationId, MAX_REPLAYS, msg, controlResponseProxy);

            return;
        }

        if (!catalog.hasRecording(recordingId))
        {
            final String msg = "unknown recording id " + recordingId;
            controlSession.sendErrorResponse(correlationId, UNKNOWN_RECORDING, msg, controlResponseProxy);

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

        final File segmentFile = segmentFile(controlSession, archiveDir, replayPosition, recordingId, correlationId);
        if (null == segmentFile)
        {
            return;
        }

        final ExclusivePublication replayPublication = newReplayPublication(
            correlationId, controlSession, replayChannel, replayStreamId, replayPosition, recordingSummary);

        final long replaySessionId = ((long)replayId++ << 32) | (replayPublication.sessionId() & 0xFFFF_FFFFL);
        final RecordingSession recordingSession = recordingSessionByIdMap.get(recordingId);
        final ReplaySession replaySession = new ReplaySession(
            replayPosition,
            length,
            replaySessionId,
            connectTimeoutMs,
            correlationId,
            controlSession,
            controlResponseProxy,
            replayBuffer,
            catalog,
            archiveDir,
            segmentFile,
            cachedEpochClock,
            replayPublication,
            recordingSummary,
            null == recordingSession ? null : recordingSession.recordingPosition());

        replaySessionByIdMap.put(replaySessionId, replaySession);
        replayer.addSession(replaySession);
    }

    void stopReplay(final long correlationId, final ControlSession controlSession, final long replaySessionId)
    {
        final ReplaySession replaySession = replaySessionByIdMap.get(replaySessionId);
        if (null == replaySession)
        {
            final String errorMessage = "replay session not known for " + replaySessionId;
            controlSession.sendErrorResponse(correlationId, UNKNOWN_REPLAY, errorMessage, controlResponseProxy);
        }
        else
        {
            replaySession.abort();
            controlSession.sendOkResponse(correlationId, controlResponseProxy);
        }
    }

    void extendRecording(
        final long correlationId,
        final ControlSession controlSession,
        final long recordingId,
        final int streamId,
        final String originalChannel,
        final SourceLocation sourceLocation)
    {
        if (recordingSessionByIdMap.size() >= maxConcurrentRecordings)
        {
            final String msg = "max concurrent recordings reached of " + maxConcurrentRecordings;
            controlSession.sendErrorResponse(correlationId, MAX_RECORDINGS, msg, controlResponseProxy);

            return;
        }

        if (!catalog.hasRecording(recordingId))
        {
            final String msg = "unknown recording id " + recordingId;
            controlSession.sendErrorResponse(correlationId, UNKNOWN_RECORDING, msg, controlResponseProxy);

            return;
        }

        if (recordingSessionByIdMap.containsKey(recordingId))
        {
            final String msg = "cannot extend active recording for " + recordingId;
            controlSession.sendErrorResponse(correlationId, ACTIVE_RECORDING, msg, controlResponseProxy);

            return;
        }

        try
        {
            final ChannelUri channelUri = ChannelUri.parse(originalChannel);
            final String strippedChannel = strippedChannelBuilder(channelUri).build();
            final String key = makeKey(streamId, channelUri);
            final Subscription oldSubscription = recordingSubscriptionMap.get(key);

            if (oldSubscription == null)
            {
                final String channel = originalChannel.contains("udp") && sourceLocation == SourceLocation.LOCAL ?
                    SPY_PREFIX + strippedChannel : strippedChannel;

                final AvailableImageHandler handler = (image) -> taskQueue.addLast(() -> extendRecordingSession(
                    controlSession, correlationId, recordingId, strippedChannel, originalChannel, image));

                final Subscription subscription = aeron.addSubscription(channel, streamId, handler, null);

                recordingSubscriptionMap.put(key, subscription);
                controlSession.sendOkResponse(correlationId, subscription.registrationId(), controlResponseProxy);
            }
            else
            {
                final String msg = "recording exists for streamId=" + streamId + " channel=" + originalChannel;
                controlSession.sendErrorResponse(correlationId, ACTIVE_SUBSCRIPTION, msg, controlResponseProxy);
            }
        }
        catch (final Exception ex)
        {
            errorHandler.onError(ex);
            controlSession.sendErrorResponse(correlationId, ex.getMessage(), controlResponseProxy);
        }
    }

    void getRecordingPosition(final long correlationId, final ControlSession controlSession, final long recordingId)
    {
        final RecordingSession recordingSession = recordingSessionByIdMap.get(recordingId);
        final long position = null == recordingSession ? NULL_POSITION : recordingSession.recordingPosition().get();

        controlSession.sendOkResponse(correlationId, position, controlResponseProxy);
    }

    void getStopPosition(final long correlationId, final ControlSession controlSession, final long recordingId)
    {
        if (catalog.hasRecording(recordingId))
        {
            controlSession.sendOkResponse(correlationId, catalog.stopPosition(recordingId), controlResponseProxy);
        }
        else
        {
            final String msg = "unknown recording " + recordingId;
            controlSession.sendErrorResponse(correlationId, UNKNOWN_RECORDING, msg, controlResponseProxy);
        }
    }

    void truncateRecording(
        final long correlationId, final ControlSession controlSession, final long recordingId, final long position)
    {
        final RecordingSummary summary = validateFramePosition(correlationId, controlSession, recordingId, position);
        if (null != summary)
        {
            final long startPosition = summary.startPosition;
            if (position < startPosition)
            {
                final String msg = "position " + position + " before start position " + startPosition;
                controlSession.sendErrorResponse(correlationId, GENERIC, msg, controlResponseProxy);

                return;
            }

            final long stopPosition = summary.stopPosition;
            if (stopPosition == position)
            {
                controlSession.sendOkResponse(correlationId, controlResponseProxy);

                return;
            }

            final int segmentLength = summary.segmentFileLength;
            final int segmentIndex = segmentFileIndex(startPosition, position, segmentLength);
            final File file = new File(archiveDir, segmentFileName(recordingId, segmentIndex));

            final int segmentOffset = (int)(position & (segmentLength - 1));
            final int termLength = summary.termBufferLength;
            final int termOffset = (int)(position & (termLength - 1));

            if (termOffset > 0)
            {
                try (FileChannel channel = FileChannel.open(file.toPath(), FILE_OPTIONS, NO_ATTRIBUTES))
                {
                    final int termCount = (int)(position >> LogBufferDescriptor.positionBitsToShift(termLength));
                    final int termId = summary.initialTermId + termCount;

                    if (ReplaySession.notHeaderAligned(
                        channel, dataHeaderBuffer, segmentOffset, termOffset, termId, summary.streamId))
                    {
                        final String msg = position + " position not aligned to data header";
                        controlSession.sendErrorResponse(correlationId, msg, controlResponseProxy);

                        return;
                    }

                    channel.truncate(segmentOffset);
                    dataHeaderBuffer.byteBuffer().put(0, (byte)0).limit(1).position(0);
                    channel.write(dataHeaderBuffer.byteBuffer(), segmentLength - 1);
                }
                catch (final IOException ex)
                {
                    controlSession.sendErrorResponse(correlationId, ex.getMessage(), controlResponseProxy);
                    LangUtil.rethrowUnchecked(ex);
                }
            }
            else if (!file.delete())
            {
                final String msg = "failed to delete " + file;
                controlSession.sendErrorResponse(correlationId, GENERIC, msg, controlResponseProxy);
                throw new ArchiveException(msg);
            }

            for (int i = segmentIndex + 1; (i * (long)segmentLength) <= stopPosition; i++)
            {
                final File f = new File(archiveDir, segmentFileName(recordingId, i));
                if (!f.delete())
                {
                    final String msg = "failed to delete " + file;
                    controlSession.sendErrorResponse(correlationId, GENERIC, msg, controlResponseProxy);
                    throw new ArchiveException(msg);
                }
            }

            catalog.recordingStopped(recordingId, position);
            controlSession.sendOkResponse(correlationId, controlResponseProxy);
        }
    }

    ControlSession newControlSession(
        final long correlationId,
        final int streamId,
        final int version,
        final String channel,
        final ControlSessionDemuxer demuxer)
    {
        final String controlChannel = strippedChannelBuilder(ChannelUri.parse(channel))
            .sparse(ctx.controlTermBufferSparse())
            .termLength(ctx.controlTermBufferLength())
            .mtu(ctx.controlMtuLength())
            .build();

        final ControlSession controlSession = new ControlSession(
            nextControlSessionId++,
            correlationId,
            connectTimeoutMs,
            demuxer,
            aeron.addExclusivePublication(controlChannel, streamId),
            this,
            cachedEpochClock,
            controlResponseProxy);
        addSession(controlSession);

        if (SemanticVersion.major(version) != AeronArchive.Configuration.MAJOR_VERSION)
        {
            controlSession.invalidVersion();
        }

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
        session.sendPendingError(controlResponseProxy);
        closeSession(session);
    }

    private int runTasks(final ArrayDeque<Runnable> taskQueue)
    {
        int workCount = 0;

        Runnable runnable;
        while (null != (runnable = taskQueue.pollFirst()))
        {
            runnable.run();
            workCount += 1;
        }

        return workCount;
    }

    private ChannelUriStringBuilder strippedChannelBuilder(final ChannelUri channelUri)
    {
        final String sessionIdStr = channelUri.get(CommonContext.SESSION_ID_PARAM_NAME);

        channelBuilder
            .clear()
            .media(channelUri.media())
            .endpoint(channelUri.get(CommonContext.ENDPOINT_PARAM_NAME))
            .networkInterface(channelUri.get(CommonContext.INTERFACE_PARAM_NAME))
            .controlEndpoint(channelUri.get(CommonContext.MDC_CONTROL_PARAM_NAME))
            .tags(channelUri.get(CommonContext.TAGS_PARAM_NAME))
            .alias(channelUri.get(CommonContext.ALIAS_PARAM_NAME));

        if (null != sessionIdStr)
        {
            if (ChannelUri.isTagged(sessionIdStr))
            {
                channelBuilder.isSessionIdTagged(true).sessionId((int)ChannelUri.getTag(sessionIdStr));
            }
            else
            {
                channelBuilder.sessionId(Integer.valueOf(sessionIdStr));
            }
        }

        return channelBuilder;
    }

    private static String makeKey(final int streamId, final ChannelUri channelUri)
    {
        final StringBuilder sb = new StringBuilder();

        sb.append(streamId).append(':').append(channelUri.media()).append('?');

        final String endpointStr = channelUri.get(CommonContext.ENDPOINT_PARAM_NAME);
        if (null != endpointStr)
        {
            sb.append(CommonContext.ENDPOINT_PARAM_NAME).append('=').append(endpointStr).append('|');
        }

        final String interfaceStr = channelUri.get(CommonContext.INTERFACE_PARAM_NAME);
        if (null != interfaceStr)
        {
            sb.append(CommonContext.INTERFACE_PARAM_NAME).append('=').append(interfaceStr).append('|');
        }

        final String controlStr = channelUri.get(CommonContext.MDC_CONTROL_PARAM_NAME);
        if (null != controlStr)
        {
            sb.append(CommonContext.MDC_CONTROL_PARAM_NAME).append('=').append(controlStr).append('|');
        }

        final String tagsStr = channelUri.get(CommonContext.TAGS_PARAM_NAME);
        if (null != tagsStr)
        {
            sb.append(CommonContext.TAGS_PARAM_NAME).append('=').append(tagsStr).append('|');
        }

        final String sessionIdStr = channelUri.get(CommonContext.SESSION_ID_PARAM_NAME);
        if (null != tagsStr)
        {
            sb.append(CommonContext.SESSION_ID_PARAM_NAME).append('=').append(sessionIdStr).append('|');
        }

        sb.setLength(sb.length() - 1);

        return sb.toString();
    }

    private void startRecordingSession(
        final ControlSession controlSession,
        final long correlationId,
        final String strippedChannel,
        final String originalChannel,
        final Image image)
    {
        validateMaxConcurrentRecordings(correlationId, controlSession, originalChannel, image);

        final int sessionId = image.sessionId();
        final int streamId = image.subscription().streamId();
        final String sourceIdentity = image.sourceIdentity();
        final int termBufferLength = image.termBufferLength();
        final int mtuLength = image.mtuLength();
        final int initialTermId = image.initialTermId();
        final long startPosition = image.joinPosition();
        final int segmentFileLength = Math.max(ctx.segmentFileLength(), termBufferLength);

        final long recordingId = catalog.addNewRecording(
            startPosition,
            cachedEpochClock.time(),
            initialTermId,
            segmentFileLength,
            termBufferLength,
            mtuLength,
            sessionId,
            streamId,
            strippedChannel,
            originalChannel,
            sourceIdentity);

        final Counter position = RecordingPos.allocate(
            aeron, tempBuffer, recordingId, sessionId, streamId, strippedChannel, image.sourceIdentity());
        position.setOrdered(startPosition);

        final RecordingSession session = new RecordingSession(
            recordingId,
            startPosition,
            segmentFileLength,
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
        final Image image)
    {
        if (recordingSessionByIdMap.containsKey(recordingId))
        {
            final String msg = "cannot extend active recording for " + recordingId;
            controlSession.attemptErrorResponse(correlationId, ACTIVE_RECORDING, msg, controlResponseProxy);
            throw new ArchiveException(msg);
        }

        validateMaxConcurrentRecordings(correlationId, controlSession, originalChannel, image);

        catalog.recordingSummary(recordingId, recordingSummary);
        validateImageForExtendRecording(correlationId, controlSession, image, recordingSummary);

        final Counter position = RecordingPos.allocate(
            aeron,
            tempBuffer,
            recordingId,
            image.sessionId(),
            image.subscription().streamId(),
            strippedChannel,
            image.sourceIdentity());

        position.setOrdered(image.joinPosition());

        final RecordingSession session = new RecordingSession(
            recordingId,
            recordingSummary.startPosition,
            recordingSummary.segmentFileLength,
            originalChannel,
            recordingEventsProxy,
            image,
            position,
            archiveDirChannel,
            ctx);

        recordingSessionByIdMap.put(recordingId, session);
        catalog.extendRecording(recordingId, controlSession.sessionId(), correlationId);
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
        final ChannelUri channelUri = ChannelUri.parse(replayChannel);
        final ChannelUriStringBuilder channelBuilder = strippedChannelBuilder(channelUri)
            .initialPosition(position, recording.initialTermId, recording.termBufferLength)
            .mtu(recording.mtuLength);

        final String lingerValue = channelUri.get(CommonContext.LINGER_PARAM_NAME);
        channelBuilder.linger(null != lingerValue ? Long.parseLong(lingerValue) : ctx.replayLingerTimeoutNs());

        try
        {
            return aeron.addExclusivePublication(channelBuilder.build(), replayStreamId);
        }
        catch (final Exception ex)
        {
            final String msg = "failed to create replay publication - " + ex;
            controlSession.sendErrorResponse(correlationId, msg, controlResponseProxy);
            throw ex;
        }
    }

    private void validateMaxConcurrentRecordings(
        final long correlationId,
        final ControlSession controlSession,
        final String originalChannel,
        final Image image)
    {
        if (recordingSessionByIdMap.size() >= maxConcurrentRecordings)
        {
            final String msg = "max concurrent recordings reached, cannot record " +
                image.subscription().streamId() + ":" + originalChannel;

            controlSession.attemptErrorResponse(correlationId, MAX_RECORDINGS, msg, controlResponseProxy);
            throw new ArchiveException(msg);
        }
    }

    private void validateImageForExtendRecording(
        final long correlationId,
        final ControlSession controlSession,
        final Image image,
        final RecordingSummary recordingSummary)
    {
        if (image.joinPosition() != recordingSummary.stopPosition)
        {
            final String msg = "cannot extend recording " + recordingSummary.recordingId +
                " image joinPosition " + image.joinPosition() +
                " not equal to recording stopPosition " + recordingSummary.stopPosition;

            controlSession.attemptErrorResponse(correlationId, INVALID_EXTENSION, msg, controlResponseProxy);
            throw new ArchiveException(msg);
        }

        if (image.termBufferLength() != recordingSummary.termBufferLength)
        {
            final String msg = "cannot extend recording " + recordingSummary.recordingId +
                " image termBufferLength " + image.termBufferLength() +
                " not equal to recording termBufferLength " + recordingSummary.termBufferLength;

            controlSession.attemptErrorResponse(correlationId, INVALID_EXTENSION, msg, controlResponseProxy);
            throw new ArchiveException(msg);
        }

        if (image.mtuLength() != recordingSummary.mtuLength)
        {
            final String msg = "cannot extend recording " + recordingSummary.recordingId +
                " image mtuLength " + image.mtuLength() +
                " not equal to recording mtuLength " + recordingSummary.mtuLength;

            controlSession.attemptErrorResponse(correlationId, INVALID_EXTENSION, msg, controlResponseProxy);
            throw new ArchiveException(msg);
        }
    }

    private RecordingSummary validateFramePosition(
        final long correlationId, final ControlSession controlSession, final long recordingId, final long position)
    {
        if (!catalog.hasRecording(recordingId))
        {
            final String msg = "unknown recording " + recordingId;
            controlSession.sendErrorResponse(correlationId, UNKNOWN_RECORDING, msg, controlResponseProxy);

            return null;
        }

        for (final ReplaySession replaySession : replaySessionByIdMap.values())
        {
            if (replaySession.recordingId() == recordingId)
            {
                final String msg = "cannot truncate recording with active replay " + recordingId;
                controlSession.sendErrorResponse(correlationId, ACTIVE_RECORDING, msg, controlResponseProxy);

                return null;
            }
        }

        catalog.recordingSummary(recordingId, recordingSummary);
        final long stopPosition = recordingSummary.stopPosition;
        final long startPosition = recordingSummary.startPosition;

        if (stopPosition == NULL_POSITION)
        {
            final String msg = "cannot truncate active recording";
            controlSession.sendErrorResponse(correlationId, ACTIVE_RECORDING, msg, controlResponseProxy);

            return null;
        }

        if (position < startPosition || position > stopPosition || ((position & (FRAME_ALIGNMENT - 1)) != 0))
        {
            controlSession.sendErrorResponse(correlationId, "invalid position " + position, controlResponseProxy);

            return null;
        }

        return recordingSummary;
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
            controlSession.sendErrorResponse(correlationId, msg, controlResponseProxy);

            return false;
        }

        final long startPosition = recordingSummary.startPosition;
        if (position - startPosition < 0)
        {
            final String msg = "requested replay start position " + position +
                ") is less than recording start position " + startPosition + " for recording " + recordingId;
            controlSession.sendErrorResponse(correlationId, msg, controlResponseProxy);

            return false;
        }

        final long stopPosition = recordingSummary.stopPosition;
        if (stopPosition != NULL_POSITION && position >= stopPosition)
        {
            final String msg = "requested replay start position " + position +
                " must be less than highest recorded position " + stopPosition + " for recording " + recordingId;
            controlSession.sendErrorResponse(correlationId, msg, controlResponseProxy);

            return false;
        }

        return true;
    }

    private File segmentFile(
        final ControlSession controlSession,
        final File archiveDir,
        final long position,
        final long recordingId,
        final long correlationId)
    {
        final long fromPosition = position == NULL_POSITION ? recordingSummary.startPosition : position;
        final int segmentFileIndex = segmentFileIndex(
            recordingSummary.startPosition, fromPosition, recordingSummary.segmentFileLength);
        final File segmentFile = new File(archiveDir, segmentFileName(recordingId, segmentFileIndex));

        if (!segmentFile.exists())
        {
            final String msg = "initial segment file does not exist for replay recording id " + recordingId;
            controlSession.sendErrorResponse(correlationId, msg, controlResponseProxy);

            return null;
        }

        return segmentFile;
    }
}
