/*
 * Copyright 2017 Real Logic Ltd.
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
package io.aeron.cluster.service;

import io.aeron.*;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.codecs.*;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.status.ReadableCounter;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.*;
import org.agrona.concurrent.status.CountersReader;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.CommonContext.SPY_PREFIX;

public class ClusteredServiceAgent implements ControlledFragmentHandler, Agent, Cluster
{
    /**
     * Length of the session header that will precede application protocol message.
     */
    public static final int SESSION_HEADER_LENGTH =
        MessageHeaderDecoder.ENCODED_LENGTH + SessionHeaderDecoder.BLOCK_LENGTH;

    private static final int SEND_ATTEMPTS = 3;
    private static final int FRAGMENT_LIMIT = 10;
    private static final int INITIAL_BUFFER_LENGTH = 4096;

    private final long serviceId;
    private long leadershipTermBeginPosition = 0;
    private long messageIndex;
    private long timestampMs;
    private final boolean shouldCloseResources;
    private final Aeron aeron;
    private final ClusteredService service;
    private final Subscription logSubscription;
    private final ExclusivePublication consensusModulePublication;
    private final ImageControlledFragmentAssembler fragmentAssembler = new ImageControlledFragmentAssembler(
        this, INITIAL_BUFFER_LENGTH, true);
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final SessionOpenEventDecoder openEventDecoder = new SessionOpenEventDecoder();
    private final SessionCloseEventDecoder closeEventDecoder = new SessionCloseEventDecoder();
    private final SessionHeaderDecoder sessionHeaderDecoder = new SessionHeaderDecoder();
    private final TimerEventDecoder timerEventDecoder = new TimerEventDecoder();
    private final ServiceActionRequestDecoder actionRequestDecoder = new ServiceActionRequestDecoder();
    private final BufferClaim bufferClaim = new BufferClaim();
    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final ScheduleTimerRequestEncoder scheduleTimerRequestEncoder = new ScheduleTimerRequestEncoder();
    private final CancelTimerRequestEncoder cancelTimerRequestEncoder = new CancelTimerRequestEncoder();
    private final ServiceActionAckEncoder serviceActionAckEncoder = new ServiceActionAckEncoder();
    private final ClientSessionEncoder clientSessionEncoder = new ClientSessionEncoder();

    private final Long2ObjectHashMap<ClientSession> sessionByIdMap = new Long2ObjectHashMap<>();
    private final IdleStrategy idleStrategy;

    private final RecordingLog recordingLog;
    private final AeronArchive.Context archiveCtx;
    private final ClusteredServiceContainer.Context ctx;

    private ReadableCounter consensusPosition;
    private Image logImage;
    private State state = State.INIT;

    public ClusteredServiceAgent(final ClusteredServiceContainer.Context ctx)
    {
        this.ctx = ctx;

        archiveCtx = ctx.archiveContext();
        serviceId = ctx.serviceId();
        aeron = ctx.aeron();
        shouldCloseResources = ctx.ownsAeronClient();
        service = ctx.clusteredService();
        recordingLog = ctx.recordingLog();
        idleStrategy = ctx.idleStrategy();

        String logChannel = ctx.logChannel();
        logChannel = logChannel.contains(IPC_CHANNEL) ? logChannel : SPY_PREFIX + logChannel;

        logSubscription = aeron.addSubscription(logChannel, ctx.logStreamId());

        consensusModulePublication = aeron.addExclusivePublication(
            ctx.consensusModuleChannel(), ctx.consensusModuleStreamId());
    }

    public void onStart()
    {
        service.onStart(this);

        final CountersReader counters = aeron.countersReader();
        final int counterId = findRecoveryCounterId(counters);

        checkForSnapshot(counters, counterId);
        checkForReplay(counters, counterId);

        consensusPosition = findConsensusPosition(counters, logSubscription);

        logImage = logSubscription.imageAtIndex(0);
        state = State.LEADING;
    }

    public void onClose()
    {
        state = State.CLOSED;

        if (shouldCloseResources)
        {
            CloseHelper.close(logSubscription);
            CloseHelper.close(consensusModulePublication);

            for (final ClientSession session : sessionByIdMap.values())
            {
                CloseHelper.close(session.responsePublication());
            }
        }
    }

    public int doWork()
    {
        int workCount = 0;

        workCount += logImage.boundedControlledPoll(fragmentAssembler, consensusPosition.get(), FRAGMENT_LIMIT);
        if (0 == workCount && logImage.isClosed())
        {
            throw new IllegalStateException("Image closed unexpectedly");
        }

        return workCount;
    }

    public String roleName()
    {
        return "clustered-service";
    }

    public ControlledFragmentHandler.Action onFragment(
        final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        final int templateId = messageHeaderDecoder.templateId();
        switch (templateId)
        {
            case SessionHeaderDecoder.TEMPLATE_ID:
            {
                sessionHeaderDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                timestampMs = sessionHeaderDecoder.timestamp();
                service.onSessionMessage(
                    sessionHeaderDecoder.clusterSessionId(),
                    sessionHeaderDecoder.correlationId(),
                    timestampMs,
                    buffer,
                    offset + SESSION_HEADER_LENGTH,
                    length - SESSION_HEADER_LENGTH,
                    header);

                break;
            }

            case TimerEventDecoder.TEMPLATE_ID:
            {
                timerEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                timestampMs = timerEventDecoder.timestamp();
                service.onTimerEvent(timerEventDecoder.correlationId(), timestampMs);
                break;
            }

            case SessionOpenEventDecoder.TEMPLATE_ID:
            {
                openEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final long sessionId = openEventDecoder.clusterSessionId();
                final ClientSession session = new ClientSession(
                    sessionId,
                    aeron.addExclusivePublication(
                        openEventDecoder.responseChannel(),
                        openEventDecoder.responseStreamId()),
                    this);

                timestampMs = openEventDecoder.timestamp();
                sessionByIdMap.put(sessionId, session);
                service.onSessionOpen(session, timestampMs);
                break;
            }

            case SessionCloseEventDecoder.TEMPLATE_ID:
            {
                closeEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final ClientSession session = sessionByIdMap.remove(closeEventDecoder.clusterSessionId());
                if (null != session)
                {
                    timestampMs = closeEventDecoder.timestamp();
                    session.responsePublication().close();
                    service.onSessionClose(session, timestampMs, closeEventDecoder.closeReason());
                }
                break;
            }

            case ServiceActionRequestDecoder.TEMPLATE_ID:
            {
                actionRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                timestampMs = actionRequestDecoder.timestamp();
                final long resultingPosition = leadershipTermBeginPosition + header.position();
                executeAction(actionRequestDecoder.action(), resultingPosition);
                break;
            }
        }

        ++messageIndex;

        return Action.CONTINUE;
    }

    public State state()
    {
        return state;
    }

    public Aeron aeron()
    {
        return aeron;
    }

    public ClientSession getClientSession(final long clusterSessionId)
    {
        return sessionByIdMap.get(clusterSessionId);
    }

    public long timeMs()
    {
        return timestampMs;
    }

    public void scheduleTimer(final long correlationId, final long deadlineMs)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + ScheduleTimerRequestEncoder.BLOCK_LENGTH;

        idleStrategy.reset();
        int attempts = SEND_ATTEMPTS;
        do
        {
            if (consensusModulePublication.tryClaim(length, bufferClaim) > 0)
            {
                scheduleTimerRequestEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .serviceId(serviceId)
                    .correlationId(correlationId)
                    .deadline(deadlineMs);

                bufferClaim.commit();

                return;
            }

            idleStrategy.idle();
        }
        while (--attempts > 0);

        throw new IllegalStateException("Failed to schedule timer");
    }

    public void cancelTimer(final long correlationId)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + CancelTimerRequestEncoder.BLOCK_LENGTH;

        idleStrategy.reset();
        int attempts = SEND_ATTEMPTS;
        do
        {
            if (consensusModulePublication.tryClaim(length, bufferClaim) > 0)
            {
                cancelTimerRequestEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .serviceId(serviceId)
                    .correlationId(correlationId);

                bufferClaim.commit();

                return;
            }

            idleStrategy.idle();
        }
        while (--attempts > 0);

        throw new IllegalStateException("Failed to schedule timer");
    }

    void addSession(final long clusterSessionId, final int responseStreamId, final String responseChannel)
    {
        sessionByIdMap.put(
            clusterSessionId,
            new ClientSession(
                clusterSessionId,
                aeron.addExclusivePublication(responseChannel, responseStreamId),
                ClusteredServiceAgent.this));
    }

    private void checkForSnapshot(final CountersReader counters, final int recoveryCounterId)
    {
        final long logPosition = RecoveryState.getLogPosition(counters, recoveryCounterId);

        if (logPosition > 0)
        {
            final RecordingLog.Entry snapshotEntry = recordingLog.getSnapshotByPosition(logPosition);
            if (null == snapshotEntry)
            {
                throw new IllegalStateException("No snapshot available for position: " + logPosition);
            }

            leadershipTermBeginPosition = logPosition;
            messageIndex = RecoveryState.getMessageIndex(counters, recoveryCounterId);
            timestampMs = RecoveryState.getTimestamp(counters, recoveryCounterId);

            snapshotEntry.confirmMatch(logPosition, messageIndex, timestampMs);
            loadSnapshot(snapshotEntry.recordingId);
        }

        sendAcknowledgment(ServiceAction.INIT, leadershipTermBeginPosition);
    }

    private void checkForReplay(final CountersReader counters, final int recoveryCounterId)
    {
        final long replayTermCount = RecoveryState.getReplayTermCount(counters, recoveryCounterId);
        if (0 == replayTermCount)
        {
            return;
        }

        try (Subscription subscription = aeron.addSubscription(ctx.replayChannel(), ctx.replayStreamId()))
        {
            final ImageControlledFragmentAssembler assembler = new ImageControlledFragmentAssembler(this);

            for (int i = 0; i < replayTermCount; i++)
            {
                final int counterId = findReplayConsensusCounterId(counters, i);
                final int sessionId = ConsensusPos.getSessionId(counters, counterId);
                leadershipTermBeginPosition = ConsensusPos.getBeginningLogPosition(counters, counterId);

                idleStrategy.reset();
                Image image;
                while ((image = subscription.imageBySessionId(sessionId)) == null)
                {
                    checkInterruptedStatus();
                    idleStrategy.idle();
                }

                final ReadableCounter limit = new ReadableCounter(counters, counterId);

                while (true)
                {
                    final int workCount = image.boundedControlledPoll(assembler, limit.get(), FRAGMENT_LIMIT);
                    if (workCount == 0)
                    {
                        if (image.isClosed())
                        {
                            if (!image.isEndOfStream())
                            {
                                throw new IllegalStateException("Unexpected close of replay");
                            }

                            break;
                        }

                        checkInterruptedStatus();
                    }

                    idleStrategy.idle(workCount);
                }

                sendAcknowledgment(ServiceAction.REPLAY, leadershipTermBeginPosition);
            }
        }
    }

    private int findRecoveryCounterId(final CountersReader counters)
    {
        int counterId = RecoveryState.findCounterId(counters);

        while (RecoveryState.NULL_COUNTER_ID == counterId)
        {
            checkInterruptedStatus();
            idleStrategy.idle();

            counterId = RecoveryState.findCounterId(counters);
        }

        return counterId;
    }

    private int findReplayConsensusCounterId(final CountersReader counters, final int replayStep)
    {
        int counterId = ConsensusPos.findCounterIdByReplayStep(counters, replayStep);

        while (RecoveryState.NULL_COUNTER_ID == counterId)
        {
            checkInterruptedStatus();
            idleStrategy.idle();

            counterId = ConsensusPos.findCounterIdByReplayStep(counters, replayStep);
        }

        return counterId;
    }

    private ReadableCounter findConsensusPosition(final CountersReader counters, final Subscription logSubscription)
    {
        idleStrategy.reset();
        while (!logSubscription.isConnected())
        {
            checkInterruptedStatus();
            idleStrategy.idle();
        }

        final int sessionId = logSubscription.imageAtIndex(0).sessionId();

        int counterId = ConsensusPos.findCounterIdBySession(counters, sessionId);
        while (ConsensusPos.NULL_COUNTER_ID == counterId)
        {
            checkInterruptedStatus();
            idleStrategy.idle();

            counterId = ConsensusPos.findCounterIdBySession(counters, sessionId);
        }

        return new ReadableCounter(counters, counterId);
    }

    private void loadSnapshot(final long recordingId)
    {
        try (AeronArchive archive = AeronArchive.connect(archiveCtx))
        {
            final RecordingExtent recordingExtent = new RecordingExtent();
            if (0 == archive.listRecording(recordingId, recordingExtent))
            {
                throw new IllegalStateException("Could not find recordingId: " + recordingId);
            }

            final String channel = ctx.replayChannel();
            final int streamId = ctx.replayStreamId();

            try (Subscription subscription = aeron.addSubscription(channel, streamId))
            {
                final long length = recordingExtent.stopPosition - recordingExtent.startPosition;
                final int sessionId = (int)archive.startReplay(recordingId, 0, length, channel, streamId);

                Image image;
                while ((image = subscription.imageBySessionId(sessionId)) == null)
                {
                    checkInterruptedStatus();
                    idleStrategy.idle();
                }

                loadState(image);
                service.onLoadSnapshot(image);
            }
        }
    }

    private void loadState(final Image image)
    {
        final SnapshotLoader snapshotLoader = new SnapshotLoader(image, this);
        while (snapshotLoader.inProgress())
        {
            final int fragments = snapshotLoader.poll();
            if (fragments == 0)
            {
                checkInterruptedStatus();

                if (image.isClosed() && snapshotLoader.inProgress())
                {
                    throw new IllegalStateException("Snapshot ended unexpectedly");
                }
            }

            idleStrategy.idle(fragments);
        }
    }

    private void onTakeSnapshot(final long position)
    {
        state = State.SNAPSHOTTING;
        final long recordingId;

        try (AeronArchive archive = AeronArchive.connect(archiveCtx))
        {
            archive.startRecording(ctx.snapshotChannel(), ctx.snapshotStreamId(), SourceLocation.LOCAL);

            try (Publication publication = aeron.addExclusivePublication(ctx.snapshotChannel(), ctx.snapshotStreamId()))
            {
                idleStrategy.reset();
                while (!publication.isConnected())
                {
                    checkInterruptedStatus();
                    idleStrategy.idle();
                }

                snapshotState(publication);
                service.onTakeSnapshot(publication);

                final CountersReader counters = aeron.countersReader();
                final int counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId());

                recordingId = RecordingPos.getRecordingId(counters, counterId);

                while (counters.getCounterValue(counterId) < publication.position())
                {
                    checkInterruptedStatus();
                    Thread.yield();
                }
            }
            finally
            {
                archive.stopRecording(ctx.snapshotChannel(), ctx.snapshotStreamId());
            }
        }
        finally
        {
            state = State.LEADING;
        }

        recordingLog.appendSnapshot(recordingId, position, messageIndex, timestampMs);
    }

    private void snapshotState(final Publication publication)
    {
        markSnapshot(publication, SnapshotMark.BEGIN);

        for (final ClientSession clientSession : sessionByIdMap.values())
        {
            final String responseChannel = clientSession.responsePublication().channel();
            final int responseStreamId = clientSession.responsePublication().streamId();
            final int length = MessageHeaderEncoder.ENCODED_LENGTH + ClientSessionEncoder.BLOCK_LENGTH +
                ClientSessionEncoder.responseChannelHeaderLength() + responseChannel.length();

            idleStrategy.reset();
            while (true)
            {
                final long result = publication.tryClaim(length, bufferClaim);
                if (result > 0)
                {
                    clientSessionEncoder
                        .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                        .clusterSessionId(clientSession.id())
                        .responseStreamId(responseStreamId)
                        .responseChannel(responseChannel);

                    bufferClaim.commit();
                    break;
                }

                checkResult(result);
                checkInterruptedStatus();
                idleStrategy.idle();
            }
        }

        markSnapshot(publication, SnapshotMark.END);
    }

    private void markSnapshot(final Publication publication, final SnapshotMark snapshotMark)
    {
        idleStrategy.reset();
        while (true)
        {
            final int length = MessageHeaderEncoder.ENCODED_LENGTH + SnapshotMarkerEncoder.BLOCK_LENGTH;
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                new SnapshotMarkerEncoder()
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .typeId(ClusteredServiceContainer.SNAPSHOT_TYPE_ID)
                    .index(0)
                    .mark(snapshotMark);

                bufferClaim.commit();
                break;
            }

            checkResult(result);
            checkInterruptedStatus();
            idleStrategy.idle();
        }
    }

    private void sendAcknowledgment(final ServiceAction action, final long logPosition)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + ServiceActionAckEncoder.BLOCK_LENGTH;

        idleStrategy.reset();
        int attempts = SEND_ATTEMPTS;
        do
        {
            if (consensusModulePublication.tryClaim(length, bufferClaim) > 0)
            {
                serviceActionAckEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .serviceId(serviceId)
                    .logPosition(logPosition)
                    .messageIndex(messageIndex)
                    .timestamp(timestampMs)
                    .action(action);

                bufferClaim.commit();

                return;
            }

            idleStrategy.idle();
        }
        while (--attempts > 0);

        throw new IllegalStateException("Failed to send ACK");
    }

    private void executeAction(final ServiceAction action, final long position)
    {
        if (State.RECOVERING == state)
        {
            return;
        }

        switch (action)
        {
            case SNAPSHOT:
                onTakeSnapshot(position);
                sendAcknowledgment(ServiceAction.SNAPSHOT, position);
                break;

            case SHUTDOWN:
                onTakeSnapshot(position);
                sendAcknowledgment(ServiceAction.SHUTDOWN, position);
                state = State.CLOSED;
                ctx.shutdownSignalBarrier().signal();
                break;

            case ABORT:
                sendAcknowledgment(ServiceAction.ABORT, position);
                state = State.CLOSED;
                ctx.shutdownSignalBarrier().signal();
                break;
        }
    }

    private static void checkInterruptedStatus()
    {
        if (Thread.currentThread().isInterrupted())
        {
            throw new AgentTerminationException("Unexpected interrupt during operation");
        }
    }

    private static void checkResult(final long result)
    {
        if (result == Publication.NOT_CONNECTED ||
            result == Publication.CLOSED ||
            result == Publication.MAX_POSITION_EXCEEDED)
        {
            throw new IllegalStateException("Unexpected publication state: " + result);
        }
    }
}
