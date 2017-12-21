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
import io.aeron.exceptions.TimeoutException;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.status.ReadableCounter;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.*;
import org.agrona.concurrent.status.CountersReader;

import java.util.concurrent.TimeUnit;

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
    private static final long TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);

    private final long serviceId;
    private long leadershipTermStartPosition = 0;
    private long messageIndex;
    private long timestampMs;
    private final boolean shouldCloseResources;
    private final EpochClock epochClock;
    private final Aeron aeron;
    private final ClusteredService service;
    private final Subscription logSubscription;
    private final ExclusivePublication consensusModulePublication;
    private final ControlledFragmentAssembler fragmentAssembler = new ControlledFragmentAssembler(
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


    private final Long2ObjectHashMap<ClientSession> sessionByIdMap = new Long2ObjectHashMap<>();
    private final IdleStrategy idleStrategy;

    private final RecordingIndex recordingIndex;
    private final AeronArchive.Context archiveCtx;
    private final ClusteredServiceContainer.Context ctx;

    private ReadableCounter recordingPosition;
    private Image logImage;

    public ClusteredServiceAgent(final ClusteredServiceContainer.Context ctx)
    {
        this.ctx = ctx;

        archiveCtx = ctx.archiveContext();
        serviceId = ctx.serviceId();
        epochClock = ctx.epochClock();
        aeron = ctx.aeron();
        shouldCloseResources = ctx.ownsAeronClient();
        service = ctx.clusteredService();
        recordingIndex = ctx.recordingIndex();
        idleStrategy = ctx.idleStrategy();

        String logChannel = ctx.logChannel();
        logChannel = logChannel.contains(IPC_CHANNEL) ? logChannel : SPY_PREFIX + logChannel;

        logSubscription = aeron.addSubscription(logChannel, ctx.logStreamId());

        consensusModulePublication = aeron.addExclusivePublication(
            ctx.consensusModuleChannel(), ctx.consensusModuleStreamId());
    }

    public void onStart()
    {
        final long recordingId = findRecordingPositionCounter();

        replayLogs();
        recordingIndex.appendLog(recordingId, leadershipTermStartPosition, messageIndex);

        logImage = logSubscription.imageAtIndex(0);

        acknowledge(ServiceAction.READY);
        service.onStart(this);
    }

    public void onClose()
    {
        if (shouldCloseResources)
        {
            CloseHelper.close(logSubscription);
            CloseHelper.close(consensusModulePublication);

            for (final ClientSession session : sessionByIdMap.values())
            {
                CloseHelper.close(session.responsePublication());
            }

            CloseHelper.close(recordingIndex);
        }
    }

    public int doWork()
    {
        int workCount = 0;

        if (null != logImage)
        {
            workCount += logImage.boundedControlledPoll(fragmentAssembler, recordingPosition.get(), FRAGMENT_LIMIT);
            if (0 == workCount && logImage.isClosed())
            {
                throw new IllegalStateException("Image closed unexpectedly");
            }
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

                sessionByIdMap.put(sessionId, session);
                timestampMs = openEventDecoder.timestamp();
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
                takeSnapshot(leadershipTermStartPosition + header.position());
                break;
            }
        }

        ++messageIndex;

        return Action.CONTINUE;
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

    public long leadershipTermStartPosition()
    {
        return leadershipTermStartPosition;
    }

    public long messageIndex()
    {
        return messageIndex;
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

    private long findRecordingPositionCounter()
    {
        final long deadlineNs = epochClock.time() + TIMEOUT_NS;

        idleStrategy.reset();
        while (!logSubscription.isConnected())
        {
            if (epochClock.time() > deadlineNs)
            {
                throw new TimeoutException("Failed to connect to cluster log");
            }

            checkInterruptedStatus();
            idleStrategy.idle();
        }

        final int sessionId = logSubscription.imageAtIndex(0).sessionId();
        final CountersReader countersReader = aeron.countersReader();

        int recordingCounterId = RecordingPos.findActiveRecordingCounterIdBySession(countersReader, sessionId);
        while (RecordingPos.NULL_COUNTER_ID == recordingCounterId)
        {
            if (epochClock.time() > deadlineNs)
            {
                throw new TimeoutException("Failed to find active recording position");
            }

            checkInterruptedStatus();
            idleStrategy.idle();

            recordingCounterId = RecordingPos.findActiveRecordingCounterIdBySession(countersReader, sessionId);
        }

        final long recordingId = RecordingPos.getActiveRecordingId(countersReader, recordingCounterId);

        recordingPosition = new ReadableCounter(countersReader, recordingCounterId);

        return recordingId;
    }

    private void replayLogs()
    {
        try (AeronArchive aeronArchive = AeronArchive.connect(archiveCtx))
        {
            recordingIndex.forEachFromLastSnapshot(
                (type, recordingId, logPosition, messageIndex) ->
                {
                    final RecordingInfo recordingInfo = new RecordingInfo();
                    if (0 == aeronArchive.listRecording(recordingId, recordingInfo))
                    {
                        throw new IllegalStateException("Could not find recordingId: " + recordingId);
                    }

                    leadershipTermStartPosition = logPosition;
                    this.messageIndex = messageIndex;

                    if (RecordingIndex.RECORDING_TYPE_SNAPSHOT == type)
                    {
                        loadSnapshot(aeronArchive, recordingInfo);
                    }
                    else if (RecordingIndex.RECORDING_TYPE_LOG == type)
                    {
                        replayRecordedLog(aeronArchive, recordingInfo);
                    }
                });
        }
    }

    private void replayRecordedLog(final AeronArchive archive, final RecordingInfo recordingInfo)
    {
        final long length = recordingInfo.stopPosition - recordingInfo.startPosition;

        try (Subscription replaySubscription = archive.replay(
            recordingInfo.recordingId,
            recordingInfo.startPosition,
            length,
            ctx.replayChannel(),
            ctx.replayStreamId()))
        {
            idleStrategy.reset();
            while (!replaySubscription.isConnected())
            {
                checkInterruptedStatus();
                idleStrategy.idle();
            }

            if (replaySubscription.imageCount() != 1)
            {
                throw new IllegalStateException("Only expected one replay");
            }

            final Image replayImage = replaySubscription.imageAtIndex(0);

            while (replayImage.position() < recordingInfo.stopPosition)
            {
                final int workCount = replayImage.controlledPoll(fragmentAssembler, FRAGMENT_LIMIT);
                if (workCount == 0)
                {
                    if (replayImage.isClosed())
                    {
                        throw new IllegalStateException("Unexpected close of replay");
                    }

                    checkInterruptedStatus();
                }

                idleStrategy.idle(workCount);
            }

            leadershipTermStartPosition += length;
        }
    }

    private void loadSnapshot(final AeronArchive aeronArchive, final RecordingInfo recordingInfo)
    {
        try (Subscription replaySubscription = aeronArchive.replay(
            recordingInfo.recordingId,
            recordingInfo.startPosition,
            recordingInfo.stopPosition - recordingInfo.startPosition,
            ctx.replayChannel(),
            ctx.replayStreamId()))
        {
            idleStrategy.reset();
            while (!replaySubscription.isConnected())
            {
                checkInterruptedStatus();
                idleStrategy.idle();
            }

            if (replaySubscription.imageCount() != 1)
            {
                throw new IllegalStateException("Only expected one replay");
            }

            service.onLoadSnapshot(replaySubscription.imageAtIndex(0));
        }
    }

    private void takeSnapshot(final long position)
    {
        final long recordingId;

        try (AeronArchive aeronArchive = AeronArchive.connect(archiveCtx))
        {
            aeronArchive.startRecording(ctx.snapshotChannel(), ctx.snapshotStreamId(), SourceLocation.LOCAL);

            try (Publication publication = aeron.addExclusivePublication(ctx.snapshotChannel(), ctx.snapshotStreamId()))
            {
                idleStrategy.reset();
                while (!publication.isConnected())
                {
                    checkInterruptedStatus();
                    idleStrategy.idle();
                }

                service.onTakeSnapshot(publication);

                final CountersReader countersReader = aeron.countersReader();
                final int recordingCounterId = RecordingPos.findActiveRecordingCounterIdBySession(
                    countersReader, publication.sessionId());

                recordingId = RecordingPos.getActiveRecordingId(countersReader, recordingCounterId);

                while (countersReader.getCounterValue(recordingCounterId) < publication.position())
                {
                    checkInterruptedStatus();
                    Thread.yield();
                }
            }
            finally
            {
                aeronArchive.stopRecording(ctx.snapshotChannel(), ctx.snapshotStreamId());
            }
        }

        recordingIndex.appendLog(recordingId, position, messageIndex);
        acknowledge(ServiceAction.SNAPSHOT);
    }

    private void acknowledge(final ServiceAction action)
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
                    .action(action);

                bufferClaim.commit();

                return;
            }

            idleStrategy.idle();
        }
        while (--attempts > 0);

        throw new IllegalStateException("Failed to send ACK");
    }

    private void checkInterruptedStatus()
    {
        if (Thread.currentThread().isInterrupted())
        {
            throw new AgentTerminationException("Unexpected interrupt during replay");
        }
    }
}
