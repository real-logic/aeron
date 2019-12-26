/*
 * Copyright 2014-2019 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
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
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.security.Authenticator;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.SemanticVersion;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.concurrent.*;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.util.ArrayDeque;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.CommonContext.SPY_PREFIX;
import static io.aeron.CommonContext.UDP_MEDIA;
import static io.aeron.archive.Archive.Configuration.MAX_BLOCK_LENGTH;
import static io.aeron.archive.Archive.segmentFileName;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.client.AeronArchive.segmentFileBasePosition;
import static io.aeron.archive.client.ArchiveException.*;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.agrona.BufferUtil.allocateDirectAligned;
import static org.agrona.concurrent.status.CountersReader.METADATA_LENGTH;

abstract class ArchiveConductor
    extends SessionWorker<Session>
    implements AvailableImageHandler, UnavailableCounterHandler
{
    private static final long MARK_FILE_UPDATE_INTERVAL_MS = TimeUnit.SECONDS.toMillis(1);
    private static final EnumSet<StandardOpenOption> FILE_OPTIONS = EnumSet.of(READ, WRITE);
    private static final FileAttribute<?>[] NO_ATTRIBUTES = new FileAttribute[0];

    private final RecordingSummary recordingSummary = new RecordingSummary();
    private final ControlRequestDecoders decoders = new ControlRequestDecoders();
    private final ArrayDeque<Runnable> taskQueue = new ArrayDeque<>();
    private final ChannelUriStringBuilder channelBuilder = new ChannelUriStringBuilder();
    private final Long2ObjectHashMap<ReplaySession> replaySessionByIdMap = new Long2ObjectHashMap<>();
    private final Long2ObjectHashMap<RecordingSession> recordingSessionByIdMap = new Long2ObjectHashMap<>();
    private final Long2ObjectHashMap<ReplicationSession> replicationSessionByIdMap = new Long2ObjectHashMap<>();
    private final Int2ObjectHashMap<Counter> counterByIdMap = new Int2ObjectHashMap<>();
    private final Object2ObjectHashMap<String, Subscription> recordingSubscriptionMap = new Object2ObjectHashMap<>();
    private final UnsafeBuffer descriptorBuffer = new UnsafeBuffer();
    private final RecordingDescriptorDecoder recordingDescriptorDecoder = new RecordingDescriptorDecoder();
    private final ControlResponseProxy controlResponseProxy = new ControlResponseProxy();
    private final UnsafeBuffer counterMetadataBuffer = new UnsafeBuffer(new byte[METADATA_LENGTH]);
    private final UnsafeBuffer dataBuffer = allocateBuffer();
    private final UnsafeBuffer replayBuffer = allocateBuffer();
    private final UnsafeBuffer recordingBuffer;

    private final Runnable aeronCloseHandler = this::abort;
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
    private final Authenticator authenticator;
    private final ControlSessionProxy controlSessionProxy;
    private final long connectTimeoutMs;
    private long timeOfLastMarkFileUpdateMs;
    private long nextSessionId = ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);
    private final int maxConcurrentRecordings;
    private final int maxConcurrentReplays;
    private int replayId = 1;
    private volatile boolean isAbort;

    protected final Archive.Context ctx;
    SessionWorker<ReplaySession> replayer;
    SessionWorker<RecordingSession> recorder;

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

        aeron.addUnavailableCounterHandler(this);
        aeron.addCloseHandler(aeronCloseHandler);

        final ChannelUri controlChannelUri = ChannelUri.parse(ctx.controlChannel());
        controlChannelUri.put(CommonContext.SPARSE_PARAM_NAME, Boolean.toString(ctx.controlTermBufferSparse()));
        controlSubscription = aeron.addSubscription(controlChannelUri.toString(), ctx.controlStreamId(), this, null);

        localControlSubscription = aeron.addSubscription(
            ctx.localControlChannel(), ctx.localControlStreamId(), this, null);

        recordingEventsProxy = new RecordingEventsProxy(ctx.recordingEventsEnabled() ?
            aeron.addExclusivePublication(ctx.recordingEventsChannel(), ctx.recordingEventsStreamId()) : null);

        catalog = ctx.catalog();
        markFile = ctx.archiveMarkFile();
        cachedEpochClock.update(epochClock.time());
        authenticator = ctx.authenticatorSupplier().get();
        controlSessionProxy = new ControlSessionProxy(controlResponseProxy);

        recordingBuffer = ctx.recordingCrcEnabled() ? allocateBuffer() : null;
    }

    private static UnsafeBuffer allocateBuffer()
    {
        return new UnsafeBuffer(allocateDirectAligned(MAX_BLOCK_LENGTH, BitUtil.CACHE_LINE_LENGTH));
    }

    public void onStart()
    {
        replayer = newReplayer();
        recorder = newRecorder();
    }

    public void onAvailableImage(final Image image)
    {
        addSession(new ControlSessionDemuxer(decoders, image, this));
    }

    public void onUnavailableCounter(
        final CountersReader countersReader, final long registrationId, final int counterId)
    {
        final Counter counter = counterByIdMap.remove(counterId);
        if (null != counter)
        {
            counter.close();

            for (final ReplaySession session : replaySessionByIdMap.values())
            {
                if (session.limitPosition() == counter)
                {
                    session.abort();
                }
            }
        }
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
        if (isAbort)
        {
            ctx.abortLatch().countDown();
        }
        else
        {
            aeron.removeCloseHandler(aeronCloseHandler);

            if (!ctx.ownsAeronClient())
            {
                aeron.removeUnavailableCounterHandler(this);

                for (final Subscription subscription : recordingSubscriptionMap.values())
                {
                    subscription.close();
                }

                CloseHelper.close(localControlSubscription);
                CloseHelper.close(controlSubscription);
                CloseHelper.close(recordingEventsProxy);
            }
        }

        ctx.close();
    }

    protected void abort()
    {
        try
        {
            replayer.abort();
            recorder.abort();
            isAbort = true;

            ctx.errorCounter().close();
            ctx.abortLatch().await(AgentRunner.RETRY_CLOSE_TIMEOUT_MS * 2L, TimeUnit.MILLISECONDS);
        }
        catch (final InterruptedException ignore)
        {
            Thread.currentThread().interrupt();
        }
    }

    protected int preWork()
    {
        int workCount = 0;

        if (isAbort)
        {
            throw new AgentTerminationException("unexpected Aeron close");
        }

        final long nowMs = epochClock.time();
        if (cachedEpochClock.time() != nowMs)
        {
            cachedEpochClock.update(nowMs);
            workCount += invokeAeronInvoker();

            if (nowMs >= (timeOfLastMarkFileUpdateMs + MARK_FILE_UPDATE_INTERVAL_MS))
            {
                markFile.updateActivityTimestamp(nowMs);
                timeOfLastMarkFileUpdateMs = nowMs;
            }
        }

        workCount += invokeDriverConductor();
        workCount += runTasks(taskQueue);

        return workCount;
    }

    final int invokeAeronInvoker()
    {
        int workCount = 0;

        if (null != aeronAgentInvoker)
        {
            workCount += aeronAgentInvoker.invoke();

            if (isAbort)
            {
                throw new AgentTerminationException("unexpected Aeron close");
            }
        }

        return workCount;
    }

    final int invokeDriverConductor()
    {
        return null != driverAgentInvoker ? driverAgentInvoker.invoke() : 0;
    }

    Catalog catalog()
    {
        return catalog;
    }

    ControlSession newControlSession(
        final long correlationId,
        final int streamId,
        final int version,
        final String channel,
        final byte[] encodedCredentials,
        final ControlSessionDemuxer demuxer)
    {
        final ChannelUri channelUri = ChannelUri.parse(channel);
        final String controlChannel = strippedChannelBuilder(channelUri)
            .ttl(channelUri)
            .sparse(ctx.controlTermBufferSparse())
            .termLength(ctx.controlTermBufferLength())
            .mtu(ctx.controlMtuLength())
            .build();

        String invalidVersionMessage = null;
        if (SemanticVersion.major(version) > AeronArchive.Configuration.PROTOCOL_MAJOR_VERSION)
        {
            invalidVersionMessage = "invalid client version " + SemanticVersion.toString(version) +
                ", archive is " + SemanticVersion.toString(AeronArchive.Configuration.PROTOCOL_SEMANTIC_VERSION);
        }

        final ControlSession controlSession = new ControlSession(
            SemanticVersion.major(version),
            nextSessionId++,
            correlationId,
            connectTimeoutMs,
            invalidVersionMessage,
            demuxer,
            aeron.addExclusivePublication(controlChannel, streamId),
            this,
            cachedEpochClock,
            controlResponseProxy,
            authenticator,
            controlSessionProxy);

        authenticator.onConnectRequest(controlSession.sessionId(), encodedCredentials, cachedEpochClock.time());

        addSession(controlSession);

        return controlSession;
    }

    void startRecording(
        final long correlationId,
        final int streamId,
        final SourceLocation sourceLocation,
        final String originalChannel,
        final ControlSession controlSession)
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
            final String key = makeKey(streamId, channelUri);
            final Subscription oldSubscription = recordingSubscriptionMap.get(key);

            if (oldSubscription == null)
            {
                final String strippedChannel = strippedChannelBuilder(channelUri).build();
                final String channel = sourceLocation == SourceLocation.LOCAL && channelUri.media().equals(UDP_MEDIA) ?
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
        final long correlationId, final int streamId, final String channel, final ControlSession controlSession)
    {
        try
        {
            final String key = makeKey(streamId, ChannelUri.parse(channel));
            final Subscription subscription = recordingSubscriptionMap.remove(key);

            if (subscription != null)
            {
                subscription.close();
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
        final long correlationId, final long subscriptionId, final ControlSession controlSession)
    {
        final Subscription subscription = removeRecordingSubscription(subscriptionId);
        if (null != subscription)
        {
            subscription.close();
            controlSession.sendOkResponse(correlationId, controlResponseProxy);
        }
        else
        {
            final String msg = "no recording subscription found for " + subscriptionId;
            controlSession.sendErrorResponse(correlationId, UNKNOWN_SUBSCRIPTION, msg, controlResponseProxy);
        }
    }

    Subscription removeRecordingSubscription(final long subscriptionId)
    {
        final Iterator<Subscription> iter = recordingSubscriptionMap.values().iterator();
        while (iter.hasNext())
        {
            final Subscription subscription = iter.next();
            if (subscription.registrationId() == subscriptionId)
            {
                iter.remove();
                return subscription;
            }
        }

        return null;
    }

    void newListRecordingsSession(
        final long correlationId, final long fromId, final int count, final ControlSession controlSession)
    {
        if (controlSession.hasActiveListing())
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
            controlSession.activeListing(session);
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
        if (controlSession.hasActiveListing())
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
            controlSession.activeListing(session);
        }
    }

    void listRecording(final long correlationId, final long recordingId, final ControlSession controlSession)
    {
        if (controlSession.hasActiveListing())
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
        final long recordingId,
        final long position,
        final long length,
        final int replayStreamId,
        final String replayChannel,
        final ControlSession controlSession)
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
            if (isInvalidReplayPosition(correlationId, controlSession, recordingId, position, recordingSummary))
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

        final long replaySessionId = ((long)(replayId++) << 32) | (replayPublication.sessionId() & 0xFFFF_FFFFL);
        final RecordingSession recordingSession = recordingSessionByIdMap.get(recordingId);
        final ReplaySession replaySession = new ReplaySession(
            replayPosition,
            length,
            replaySessionId,
            connectTimeoutMs,
            correlationId,
            ctx.replayCrcEnabled(),
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

    void startBoundedReplay(
        final long correlationId,
        final long recordingId,
        final long position,
        final long length,
        final int limitCounterId,
        final int replayStreamId,
        final String replayChannel,
        final ControlSession controlSession)
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
            if (isInvalidReplayPosition(correlationId, controlSession, recordingId, position, recordingSummary))
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

        final Counter limitCounter = getOrAddCounter(limitCounterId);

        final ExclusivePublication replayPublication = newReplayPublication(
            correlationId, controlSession, replayChannel, replayStreamId, replayPosition, recordingSummary);

        final long replaySessionId = ((long)(replayId++) << 32) | (replayPublication.sessionId() & 0xFFFF_FFFFL);
        final ReplaySession replaySession = new ReplaySession(
            replayPosition,
            length,
            replaySessionId,
            connectTimeoutMs,
            correlationId,
            ctx.replayCrcEnabled(),
            controlSession,
            controlResponseProxy,
            replayBuffer,
            catalog,
            archiveDir,
            segmentFile,
            cachedEpochClock,
            replayPublication,
            recordingSummary,
            limitCounter);

        replaySessionByIdMap.put(replaySessionId, replaySession);
        replayer.addSession(replaySession);
    }

    void stopReplay(final long correlationId, final long replaySessionId, final ControlSession controlSession)
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

    void stopAllReplays(final long correlationId, final long recordingId, final ControlSession controlSession)
    {
        for (final ReplaySession replaySession : replaySessionByIdMap.values())
        {
            if (NULL_VALUE == recordingId || replaySession.recordingId() == recordingId)
            {
                replaySession.abort();
            }
        }

        controlSession.sendOkResponse(correlationId, controlResponseProxy);
    }

    Subscription extendRecording(
        final long correlationId,
        final long recordingId,
        final int streamId,
        final SourceLocation sourceLocation,
        final String originalChannel,
        final ControlSession controlSession)
    {
        if (recordingSessionByIdMap.size() >= maxConcurrentRecordings)
        {
            final String msg = "max concurrent recordings reached of " + maxConcurrentRecordings;
            controlSession.sendErrorResponse(correlationId, MAX_RECORDINGS, msg, controlResponseProxy);
            return null;
        }

        if (!catalog.hasRecording(recordingId))
        {
            final String msg = "unknown recording id " + recordingId;
            controlSession.sendErrorResponse(correlationId, UNKNOWN_RECORDING, msg, controlResponseProxy);
            return null;
        }

        catalog.recordingSummary(recordingId, recordingSummary);
        if (streamId != recordingSummary.streamId)
        {
            final String msg = "cannot extend recording  " + recordingSummary.recordingId +
                " with streamId " + streamId + " != existing streamId " + recordingSummary.streamId;
            controlSession.sendErrorResponse(correlationId, UNKNOWN_RECORDING, msg, controlResponseProxy);
            return null;
        }

        if (recordingSessionByIdMap.containsKey(recordingId))
        {
            final String msg = "cannot extend active recording for " + recordingId;
            controlSession.sendErrorResponse(correlationId, ACTIVE_RECORDING, msg, controlResponseProxy);
            return null;
        }

        try
        {
            final ChannelUri channelUri = ChannelUri.parse(originalChannel);
            final String key = makeKey(streamId, channelUri);
            final Subscription oldSubscription = recordingSubscriptionMap.get(key);

            if (oldSubscription == null)
            {
                final String strippedChannel = strippedChannelBuilder(channelUri).build();
                final String channel = originalChannel.contains("udp") && sourceLocation == SourceLocation.LOCAL ?
                    SPY_PREFIX + strippedChannel : strippedChannel;

                final AvailableImageHandler handler = (image) -> taskQueue.addLast(() -> extendRecordingSession(
                    controlSession, correlationId, recordingId, strippedChannel, originalChannel, image));

                final Subscription subscription = aeron.addSubscription(channel, streamId, handler, null);

                recordingSubscriptionMap.put(key, subscription);
                controlSession.sendOkResponse(correlationId, subscription.registrationId(), controlResponseProxy);

                return subscription;
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

        return null;
    }

    void getStartPosition(final long correlationId, final long recordingId, final ControlSession controlSession)
    {
        if (hasRecording(recordingId, correlationId, controlSession))
        {
            controlSession.sendOkResponse(correlationId, catalog.startPosition(recordingId), controlResponseProxy);
        }
    }

    void getRecordingPosition(final long correlationId, final long recordingId, final ControlSession controlSession)
    {
        if (hasRecording(recordingId, correlationId, controlSession))
        {
            final RecordingSession recordingSession = recordingSessionByIdMap.get(recordingId);
            final long position = null == recordingSession ? NULL_POSITION : recordingSession.recordingPosition().get();

            controlSession.sendOkResponse(correlationId, position, controlResponseProxy);
        }
    }

    void getStopPosition(final long correlationId, final long recordingId, final ControlSession controlSession)
    {
        if (hasRecording(recordingId, correlationId, controlSession))
        {
            controlSession.sendOkResponse(correlationId, catalog.stopPosition(recordingId), controlResponseProxy);
        }
    }

    void truncateRecording(
        final long correlationId, final long recordingId, final long position, final ControlSession controlSession)
    {
        if (hasRecording(recordingId, correlationId, controlSession) &&
            isValidTruncate(correlationId, controlSession, recordingId, position))
        {
            final long stopPosition = recordingSummary.stopPosition;
            if (stopPosition == position)
            {
                controlSession.sendOkResponse(correlationId, controlResponseProxy);
                return;
            }

            final int segmentLength = recordingSummary.segmentFileLength;
            final int termLength = recordingSummary.termBufferLength;
            final long startPosition = recordingSummary.startPosition;
            final long segmentBasePosition = segmentFileBasePosition(
                startPosition, position, termLength, segmentLength);
            final int segmentOffset = (int)(position - segmentBasePosition);
            final File file = new File(archiveDir, segmentFileName(recordingId, segmentBasePosition));

            if (segmentOffset > 0)
            {
                if (!eraseRemainingSegment(
                    correlationId, controlSession, position, segmentLength, segmentOffset, termLength, file))
                {
                    return;
                }
            }
            else if (!file.delete())
            {
                final String msg = "failed to delete " + file;
                controlSession.sendErrorResponse(correlationId, msg, controlResponseProxy);
                throw new ArchiveException(msg);
            }

            catalog.stopPosition(recordingId, position);

            for (long p = segmentBasePosition + segmentLength; p <= stopPosition; p += segmentLength)
            {
                final File f = new File(archiveDir, segmentFileName(recordingId, p));
                if (!f.delete())
                {
                    final String msg = "failed to delete " + f;
                    controlSession.sendErrorResponse(correlationId, msg, controlResponseProxy);
                    throw new ArchiveException(msg);
                }
            }

            controlSession.sendOkResponse(correlationId, controlResponseProxy);
        }
    }

    void listRecordingSubscriptions(
        final long correlationId,
        final int pseudoIndex,
        final int subscriptionCount,
        final boolean applyStreamId,
        final int streamId,
        final String channelFragment,
        final ControlSession controlSession)
    {
        if (controlSession.hasActiveListing())
        {
            final String msg = "active listing already in progress";
            controlSession.sendErrorResponse(correlationId, ACTIVE_LISTING, msg, controlResponseProxy);
        }
        else if (pseudoIndex < 0 || pseudoIndex >= recordingSubscriptionMap.size() || subscriptionCount <= 0)
        {
            controlSession.sendSubscriptionUnknown(correlationId, controlResponseProxy);
        }
        else
        {
            final ListRecordingSubscriptionsSession session = new ListRecordingSubscriptionsSession(
                recordingSubscriptionMap,
                pseudoIndex,
                subscriptionCount,
                streamId,
                applyStreamId,
                channelFragment,
                correlationId,
                controlSession,
                controlResponseProxy);
            addSession(session);
            controlSession.activeListing(session);
        }
    }

    void closeRecordingSession(final RecordingSession session)
    {
        final long recordingId = session.sessionId();
        recordingSessionByIdMap.remove(recordingId);

        if (!isAbort)
        {
            catalog.recordingStopped(recordingId, session.recordedPosition(), epochClock.time());

            session.controlSession().attemptSignal(
                session.correlationId(),
                recordingId,
                session.image().subscription().registrationId(),
                session.recordedPosition(),
                RecordingSignal.STOP);

            closeSession(session);
        }
        else
        {
            session.abortClose();
        }
    }

    void closeReplaySession(final ReplaySession session)
    {
        replaySessionByIdMap.remove(session.sessionId());

        if (!isAbort)
        {
            session.sendPendingError(controlResponseProxy);
        }

        closeSession(session);
    }

    void replicate(
        final long correlationId,
        final long srcRecordingId,
        final long dstRecordingId,
        final long channelTagId,
        final long subscriptionTagId,
        final int srcControlStreamId,
        final String srcControlChannel,
        final String liveDestination,
        final ControlSession controlSession)
    {
        final boolean hasRecording = catalog.hasRecording(dstRecordingId);
        if (NULL_VALUE != dstRecordingId && !hasRecording)
        {
            final String msg = "unknown destination recording id " + dstRecordingId;
            controlSession.sendErrorResponse(correlationId, UNKNOWN_RECORDING, msg, controlResponseProxy);
            return;
        }

        if (hasRecording)
        {
            catalog.recordingSummary(dstRecordingId, recordingSummary);
        }

        final AeronArchive.Context remoteArchiveContext = ctx.archiveClientContext().clone()
            .controlRequestChannel(srcControlChannel)
            .controlRequestStreamId(srcControlStreamId);

        final long replicationId = aeron.nextCorrelationId();
        final ReplicationSession replicationSession = new ReplicationSession(
            correlationId,
            srcRecordingId,
            dstRecordingId,
            channelTagId,
            subscriptionTagId,
            replicationId,
            liveDestination,
            ctx.replicationChannel(),
            hasRecording ? recordingSummary : null,
            remoteArchiveContext,
            cachedEpochClock,
            catalog,
            controlResponseProxy,
            controlSession);

        replicationSessionByIdMap.put(replicationId, replicationSession);
        addSession(replicationSession);

        controlSession.sendOkResponse(correlationId, replicationId, controlResponseProxy);
    }

    void stopReplication(final long correlationId, final long replicationId, final ControlSession controlSession)
    {
        final ReplicationSession session = replicationSessionByIdMap.remove(replicationId);
        if (null == session)
        {
            final String msg = "unknown replication id " + replicationId;
            controlSession.sendErrorResponse(correlationId, replicationId, msg, controlResponseProxy);
        }
        else
        {
            session.abort();
            controlSession.sendOkResponse(correlationId, controlResponseProxy);
        }
    }

    void detachSegments(
        final long correlationId,
        final long recordingId,
        final long newStartPosition,
        final ControlSession controlSession)
    {
        if (hasRecording(recordingId, correlationId, controlSession) &&
            isValidDetach(correlationId, controlSession, recordingId, newStartPosition))
        {
            catalog.startPosition(recordingId, newStartPosition);
            controlSession.sendOkResponse(correlationId, controlResponseProxy);
        }
    }

    void deleteDetachedSegments(final long correlationId, final long recordingId, final ControlSession controlSession)
    {
        if (hasRecording(recordingId, correlationId, controlSession))
        {
            final long deletedSegmentCount = deleteDetachedSegments(recordingId);
            controlSession.sendOkResponse(correlationId, deletedSegmentCount, controlResponseProxy);
        }
    }

    void purgeSegments(
        final long correlationId,
        final long recordingId,
        final long newStartPosition,
        final ControlSession controlSession)
    {
        if (hasRecording(recordingId, correlationId, controlSession) &&
            isValidDetach(correlationId, controlSession, recordingId, newStartPosition))
        {
            catalog.startPosition(recordingId, newStartPosition);
            final long deletedSegmentCount = deleteDetachedSegments(recordingId);
            controlSession.sendOkResponse(correlationId, deletedSegmentCount, controlResponseProxy);
        }
    }

    void attachSegments(final long correlationId, final long recordingId, final ControlSession controlSession)
    {
        if (hasRecording(recordingId, correlationId, controlSession))
        {
            catalog.recordingSummary(recordingId, recordingSummary);
            final int segmentLength = recordingSummary.segmentFileLength;
            final int termLength = recordingSummary.termBufferLength;
            final int bitsToShift = LogBufferDescriptor.positionBitsToShift(termLength);
            final int streamId = recordingSummary.streamId;
            long position = recordingSummary.startPosition - segmentLength;
            long count = 0;

            while (position >= 0)
            {
                final File file = new File(archiveDir, segmentFileName(recordingId, position));
                if (!file.exists())
                {
                    break;
                }

                final long fileLength = file.length();
                if (fileLength != segmentLength)
                {
                    final String msg = "file length " + fileLength + " not equal to segment length " + segmentLength;
                    controlSession.sendErrorResponse(correlationId, msg, controlResponseProxy);
                    return;
                }

                try (FileChannel fileChannel = FileChannel.open(file.toPath(), FILE_OPTIONS, NO_ATTRIBUTES))
                {
                    final int termCount = (int)(position >> bitsToShift);
                    final int termId = recordingSummary.initialTermId + termCount;
                    final int termOffset = findTermOffsetForStart(
                        correlationId, controlSession, file, fileChannel, streamId, termId, termLength);

                    if (termOffset < 0)
                    {
                        return;
                    }
                    else if (0 == termOffset)
                    {
                        catalog.startPosition(recordingId, position);
                        count += 1;
                        position -= segmentLength;
                    }
                    else
                    {
                        catalog.startPosition(recordingId, position + termOffset);
                        count += 1;
                        break;
                    }
                }
                catch (final IOException ex)
                {
                    controlSession.sendErrorResponse(correlationId, ex.getMessage(), controlResponseProxy);
                    LangUtil.rethrowUnchecked(ex);
                }
            }

            controlSession.sendOkResponse(correlationId, count, controlResponseProxy);
        }
    }

    void migrateSegments(
        final long correlationId,
        final long srcRecordingId,
        final long dstRecordingId,
        final ControlSession controlSession)
    {
        if (hasRecording(srcRecordingId, correlationId, controlSession) &&
            hasRecording(dstRecordingId, correlationId, controlSession) &&
            isValidAttach(correlationId, controlSession, srcRecordingId, dstRecordingId))
        {
            long attachedSegmentCount = 0;
            long position = recordingSummary.stopPosition;
            final long startPosition = recordingSummary.startPosition;
            final int segmentLength = recordingSummary.segmentFileLength;
            final long segmentFileBasePosition = segmentFileBasePosition(
                startPosition, startPosition, recordingSummary.termBufferLength, segmentLength);

            while (position >= segmentFileBasePosition)
            {
                final File srcFile = new File(archiveDir, segmentFileName(srcRecordingId, position));
                if (position == recordingSummary.stopPosition)
                {
                    srcFile.delete();
                    position -= segmentLength;
                    continue;
                }
                else if (!srcFile.exists())
                {
                    break;
                }

                final String dstFile = segmentFileName(dstRecordingId, position);
                if (!srcFile.renameTo(new File(archiveDir, dstFile)))
                {
                    final String msg = "failed to rename " + srcFile + " to " + dstFile;
                    controlSession.sendErrorResponse(correlationId, msg, controlResponseProxy);
                    return;
                }

                attachedSegmentCount++;
                position -= segmentLength;
            }

            catalog.startPosition(dstRecordingId, startPosition);
            catalog.stopPosition(srcRecordingId, startPosition);
            controlSession.sendOkResponse(correlationId, attachedSegmentCount, controlResponseProxy);
        }
    }

    void removeReplicationSession(final ReplicationSession replicationSession)
    {
        replicationSessionByIdMap.remove(replicationSession.sessionId());
    }

    private long deleteDetachedSegments(final long recordingId)
    {
        catalog.recordingSummary(recordingId, recordingSummary);
        final int segmentFile = recordingSummary.segmentFileLength;
        long filenamePosition = recordingSummary.startPosition - segmentFile;
        long count = 0;

        while (filenamePosition >= 0)
        {
            final File f = new File(archiveDir, segmentFileName(recordingId, filenamePosition));
            if (!f.delete())
            {
                break;
            }

            count += 1;
            filenamePosition -= segmentFile;
        }

        return count;
    }

    private int findTermOffsetForStart(
        final long correlationId,
        final ControlSession controlSession,
        final File file,
        final FileChannel fileChannel,
        final int streamId,
        final int termId,
        final int termLength)
        throws IOException
    {
        int termOffset = 0;
        final ByteBuffer byteBuffer = dataBuffer.byteBuffer();
        byteBuffer.clear().limit(DataHeaderFlyweight.HEADER_LENGTH);

        if (DataHeaderFlyweight.HEADER_LENGTH != fileChannel.read(byteBuffer, 0))
        {
            final String msg = "failed to read segment file";
            controlSession.sendErrorResponse(correlationId, msg, controlResponseProxy);
            return termOffset;
        }

        final int fragmentLength = DataHeaderFlyweight.fragmentLength(dataBuffer, termOffset);
        if (fragmentLength <= 0)
        {
            boolean found = false;
            do
            {
                byteBuffer.clear().limit(Math.min(termLength - termOffset, MAX_BLOCK_LENGTH));
                final int bytesRead = fileChannel.read(byteBuffer, termOffset);
                if (-1 == bytesRead)
                {
                    final String msg = "read failed on " + file;
                    controlSession.sendErrorResponse(correlationId, msg, controlResponseProxy);
                    return NULL_VALUE;
                }

                final int limit = bytesRead - (bytesRead & (FRAME_ALIGNMENT - 1));
                int offset = 0;
                while (offset < limit)
                {
                    if (DataHeaderFlyweight.fragmentLength(dataBuffer, offset) > 0)
                    {
                        found = true;
                        break;
                    }

                    offset += FRAME_ALIGNMENT;
                }

                termOffset += offset;
            }
            while (termOffset < termLength && !found);
        }

        if (termOffset >= termLength)
        {
            final String msg = "fragment not found in first term of segment " + file;
            controlSession.sendErrorResponse(correlationId, msg, controlResponseProxy);
            return NULL_VALUE;
        }

        final int fileTermId = DataHeaderFlyweight.termId(dataBuffer, termOffset);
        if (fileTermId != termId)
        {
            final String msg = "term id does not match: actual=" + fileTermId + " expected=" + termId;
            controlSession.sendErrorResponse(correlationId, msg, controlResponseProxy);
            return NULL_VALUE;
        }

        final int fileStreamId = DataHeaderFlyweight.streamId(dataBuffer, termOffset);
        if (fileStreamId != streamId)
        {
            final String msg = "stream id does not match: actual=" + fileStreamId + " expected=" + streamId;
            controlSession.sendErrorResponse(correlationId, msg, controlResponseProxy);
            return NULL_VALUE;
        }

        return termOffset;
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
        channelBuilder
            .clear()
            .media(channelUri.media())
            .endpoint(channelUri)
            .networkInterface(channelUri)
            .controlEndpoint(channelUri)
            .controlMode(channelUri)
            .tags(channelUri)
            .rejoin(channelUri)
            .group(channelUri)
            .congestionControl(channelUri)
            .alias(channelUri);

        final String sessionIdStr = channelUri.get(CommonContext.SESSION_ID_PARAM_NAME);
        if (null != sessionIdStr)
        {
            if (ChannelUri.isTagged(sessionIdStr))
            {
                final long tag = ChannelUri.getTag(sessionIdStr);
                if (tag < Integer.MIN_VALUE || tag > Integer.MAX_VALUE)
                {
                    throw new IllegalArgumentException("invalid session id tag value: " + tag);
                }
                channelBuilder.isSessionIdTagged(true).sessionId((int)tag);
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

        final String sessionIdStr = channelUri.get(CommonContext.SESSION_ID_PARAM_NAME);
        if (null != sessionIdStr)
        {
            sb.append(CommonContext.SESSION_ID_PARAM_NAME).append('=').append(sessionIdStr).append('|');
        }

        final String tagsStr = channelUri.get(CommonContext.TAGS_PARAM_NAME);
        if (null != tagsStr)
        {
            sb.append(CommonContext.TAGS_PARAM_NAME).append('=').append(tagsStr).append('|');
        }

        sb.setLength(sb.length() - 1);

        return sb.toString();
    }

    private boolean hasRecording(final long recordingId, final long correlationId, final ControlSession session)
    {
        if (!catalog.hasRecording(recordingId))
        {
            final String msg = "unknown recording " + recordingId;
            session.sendErrorResponse(correlationId, UNKNOWN_RECORDING, msg, controlResponseProxy);
            return false;
        }

        return true;
    }

    private void startRecordingSession(
        final ControlSession controlSession,
        final long correlationId,
        final String strippedChannel,
        final String originalChannel,
        final Image image)
    {
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
            aeron, counterMetadataBuffer, recordingId, sessionId, streamId, strippedChannel, image.sourceIdentity());
        position.setOrdered(startPosition);

        final RecordingSession session = new RecordingSession(
            correlationId,
            recordingId,
            startPosition,
            segmentFileLength,
            originalChannel,
            recordingEventsProxy,
            image,
            position,
            archiveDirChannel,
            ctx,
            controlSession,
            recordingBuffer);

        recordingSessionByIdMap.put(recordingId, session);
        recorder.addSession(session);

        controlSession.attemptSignal(
            correlationId,
            recordingId,
            image.subscription().registrationId(),
            image.joinPosition(),
            RecordingSignal.START);
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

        catalog.recordingSummary(recordingId, recordingSummary);
        validateImageForExtendRecording(correlationId, controlSession, image, recordingSummary);

        final Counter position = RecordingPos.allocate(
            aeron,
            counterMetadataBuffer,
            recordingId,
            image.sessionId(),
            image.subscription().streamId(),
            strippedChannel,
            image.sourceIdentity());

        position.setOrdered(image.joinPosition());

        final RecordingSession session = new RecordingSession(
            correlationId,
            recordingId,
            recordingSummary.startPosition,
            recordingSummary.segmentFileLength,
            originalChannel,
            recordingEventsProxy,
            image,
            position,
            archiveDirChannel,
            ctx,
            controlSession,
            recordingBuffer);

        recordingSessionByIdMap.put(recordingId, session);
        catalog.extendRecording(recordingId, controlSession.sessionId(), correlationId, image.sessionId());
        recorder.addSession(session);

        controlSession.attemptSignal(
            correlationId,
            recordingId,
            image.subscription().registrationId(),
            image.joinPosition(),
            RecordingSignal.EXTEND);
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
            .ttl(channelUri)
            .eos(channelUri)
            .sparse(channelUri)
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

        if (image.initialTermId() != recordingSummary.initialTermId)
        {
            final String msg = "cannot extend recording " + recordingSummary.recordingId +
                " image initialTermId " + image.initialTermId() +
                " not equal to recording initialTermId " + recordingSummary.initialTermId;
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

    private boolean isValidTruncate(
        final long correlationId, final ControlSession controlSession, final long recordingId, final long position)
    {
        for (final ReplaySession replaySession : replaySessionByIdMap.values())
        {
            if (replaySession.recordingId() == recordingId)
            {
                final String msg = "cannot truncate recording with active replay " + recordingId;
                controlSession.sendErrorResponse(correlationId, ACTIVE_RECORDING, msg, controlResponseProxy);
                return false;
            }
        }

        catalog.recordingSummary(recordingId, recordingSummary);
        final long stopPosition = recordingSummary.stopPosition;
        final long startPosition = recordingSummary.startPosition;

        if (stopPosition == NULL_POSITION)
        {
            final String msg = "cannot truncate active recording";
            controlSession.sendErrorResponse(correlationId, ACTIVE_RECORDING, msg, controlResponseProxy);
            return false;
        }

        if (position < startPosition || position > stopPosition || ((position & (FRAME_ALIGNMENT - 1)) != 0))
        {
            final String msg = "invalid position " + position +
                ": start=" + recordingSummary.startPosition +
                " stop=" + recordingSummary.stopPosition +
                " alignment=" + FRAME_ALIGNMENT;
            controlSession.sendErrorResponse(correlationId, msg, controlResponseProxy);
            return false;
        }

        return true;
    }

    private boolean isInvalidReplayPosition(
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
            return true;
        }

        final long startPosition = recordingSummary.startPosition;
        if (position - startPosition < 0)
        {
            final String msg = "requested replay start position " + position +
                " is less than recording start position " + startPosition + " for recording " + recordingId;
            controlSession.sendErrorResponse(correlationId, msg, controlResponseProxy);
            return true;
        }

        final long stopPosition = recordingSummary.stopPosition;
        if (stopPosition != NULL_POSITION && position >= stopPosition)
        {
            final String msg = "requested replay start position " + position +
                " must be less than highest recorded position " + stopPosition + " for recording " + recordingId;
            controlSession.sendErrorResponse(correlationId, msg, controlResponseProxy);
            return true;
        }

        return false;
    }

    private boolean isValidDetach(
        final long correlationId, final ControlSession controlSession, final long recordingId, final long position)
    {
        catalog.recordingSummary(recordingId, recordingSummary);

        final int segmentLength = recordingSummary.segmentFileLength;
        final long startPosition = recordingSummary.startPosition;
        final int termLength = recordingSummary.termBufferLength;
        final long lowerBound =
            segmentFileBasePosition(startPosition, startPosition, termLength, segmentLength) + segmentLength;

        if (position != segmentFileBasePosition(startPosition, position, termLength, segmentLength))
        {
            final String msg = "invalid segment start: newStartPosition=" + position;
            controlSession.sendErrorResponse(correlationId, msg, controlResponseProxy);
            return false;
        }

        if (position < lowerBound)
        {
            final String msg = "invalid detach: newStartPosition=" + position + " lowerBound=" + lowerBound;
            controlSession.sendErrorResponse(correlationId, msg, controlResponseProxy);
            return false;
        }

        final long stopPosition = recordingSummary.stopPosition;
        long upperBound = NULL_VALUE == stopPosition ?
            recordingSessionByIdMap.get(recordingId).recordedPosition() : stopPosition;
        upperBound = segmentFileBasePosition(startPosition, upperBound, termLength, segmentLength);

        for (final ReplaySession replaySession : replaySessionByIdMap.values())
        {
            if (replaySession.recordingId() == recordingId)
            {
                upperBound = Math.min(upperBound, replaySession.segmentFileBasePosition());
            }
        }

        if (position > upperBound)
        {
            final String msg = "invalid detach: newStartPosition=" + position + " upperBound=" + upperBound;
            controlSession.sendErrorResponse(correlationId, msg, controlResponseProxy);
            return false;
        }

        return true;
    }

    private boolean isValidAttach(
        final long correlationId,
        final ControlSession controlSession,
        final long srcRecordingId,
        final long dstRecordingId)
    {
        catalog.recordingSummary(dstRecordingId, recordingSummary);

        final long dstStartPosition = recordingSummary.startPosition;
        final int dstSegmentFileLength = recordingSummary.segmentFileLength;
        final int dstTermBufferLength = recordingSummary.termBufferLength;
        final int dstInitialTermId = recordingSummary.initialTermId;
        final int dstStreamId = recordingSummary.streamId;
        final int dstMtuLength = recordingSummary.mtuLength;

        catalog.recordingSummary(srcRecordingId, recordingSummary);

        final long srcStopPosition = recordingSummary.stopPosition;
        if (NULL_POSITION == srcStopPosition)
        {
            final String msg = "source recording still active " + srcRecordingId;
            controlSession.sendErrorResponse(correlationId, msg, controlResponseProxy);
            return false;
        }

        if (dstStartPosition != srcStopPosition)
        {
            final String msg =
                "invalid migrate: srcStopPosition=" + srcStopPosition + " dstStartPosition=" + dstStartPosition;
            controlSession.sendErrorResponse(correlationId, msg, controlResponseProxy);
            return false;
        }

        final int srcSegmentFileLength = recordingSummary.segmentFileLength;
        if (dstSegmentFileLength != srcSegmentFileLength)
        {
            final String msg = "invalid migrate: srcSegmentFileLength=" + srcSegmentFileLength +
                " dstSegmentFileLength=" + dstSegmentFileLength;
            controlSession.sendErrorResponse(correlationId, msg, controlResponseProxy);
            return false;
        }

        final int srcTermBufferLength = recordingSummary.termBufferLength;
        if (dstTermBufferLength != srcTermBufferLength)
        {
            final String msg = "invalid migrate: srcTermBufferLength=" + srcTermBufferLength +
                " dstTermBufferLength=" + dstTermBufferLength;
            controlSession.sendErrorResponse(correlationId, msg, controlResponseProxy);
            return false;
        }

        final int srcInitialTermId = recordingSummary.initialTermId;
        if (dstInitialTermId != srcInitialTermId)
        {
            final String msg = "invalid migrate: srcInitialTermId=" + srcInitialTermId +
                " dstInitialTermId=" + dstInitialTermId;
            controlSession.sendErrorResponse(correlationId, msg, controlResponseProxy);
            return false;
        }

        final int srcStreamId = recordingSummary.streamId;
        if (dstStreamId != srcStreamId)
        {
            final String msg = "invalid migrate: srcStreamId=" + srcStreamId +
                " dstStreamId=" + dstStreamId;
            controlSession.sendErrorResponse(correlationId, msg, controlResponseProxy);
            return false;
        }

        final int srcMtuLength = recordingSummary.mtuLength;
        if (dstMtuLength != srcMtuLength)
        {
            final String msg = "invalid migrate: srcMtuLength=" + srcMtuLength +
                " dstMtuLength=" + dstMtuLength;
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
        final long segmentFileBasePosition = segmentFileBasePosition(
            recordingSummary.startPosition,
            fromPosition,
            recordingSummary.termBufferLength,
            recordingSummary.segmentFileLength);

        final File segmentFile = new File(archiveDir, segmentFileName(recordingId, segmentFileBasePosition));
        if (!segmentFile.exists())
        {
            final String msg = "initial segment file does not exist for replay recording id " + recordingId;
            controlSession.sendErrorResponse(correlationId, msg, controlResponseProxy);
            return null;
        }

        return segmentFile;
    }

    private boolean eraseRemainingSegment(
        final long correlationId,
        final ControlSession controlSession,
        final long position,
        final int segmentLength,
        final int segmentOffset,
        final int termLength,
        final File file)
    {
        try (FileChannel channel = FileChannel.open(file.toPath(), FILE_OPTIONS, NO_ATTRIBUTES))
        {
            final int termOffset = (int)(position & (termLength - 1));
            final int termCount = (int)(position >> LogBufferDescriptor.positionBitsToShift(termLength));
            final int termId = recordingSummary.initialTermId + termCount;

            if (ReplaySession.notHeaderAligned(
                channel, dataBuffer, segmentOffset, termOffset, termId, recordingSummary.streamId))
            {
                final String msg = position + " position not aligned to data header";
                controlSession.sendErrorResponse(correlationId, msg, controlResponseProxy);
                return false;
            }

            channel.truncate(segmentOffset);
            dataBuffer.byteBuffer().put(0, (byte)0).limit(1).position(0);
            channel.write(dataBuffer.byteBuffer(), segmentLength - 1);
        }
        catch (final IOException ex)
        {
            controlSession.sendErrorResponse(correlationId, ex.getMessage(), controlResponseProxy);
            LangUtil.rethrowUnchecked(ex);
        }

        return true;
    }

    private Counter getOrAddCounter(final int counterId)
    {
        Counter counter = counterByIdMap.get(counterId);

        if (null == counter)
        {
            counter = new Counter(aeron.countersReader(), NULL_VALUE, counterId);
            counterByIdMap.put(counterId, counter);
        }

        return counter;
    }
}
