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
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.RecordingInfo;
import io.aeron.cluster.codecs.*;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.status.ReadableCounter;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.*;
import org.agrona.concurrent.status.CountersReader;

public class ClusteredServiceAgent implements
    ControlledFragmentHandler, Agent, Cluster, AvailableImageHandler, UnavailableImageHandler
{
    /**
     * Length of the session header that will be precede application protocol message.
     */
    public static final int SESSION_HEADER_LENGTH =
        MessageHeaderDecoder.ENCODED_LENGTH + SessionHeaderDecoder.BLOCK_LENGTH;

    private static final int SEND_ATTEMPTS = 3;
    private static final int FRAGMENT_LIMIT = 10;
    private static final int INITIAL_BUFFER_LENGTH = 4096;

    private final long serviceId;
    private long timestampMs;
    private final boolean shouldCloseResources;
    private final Aeron aeron;
    private final ClusteredService service;
    private final Subscription logSubscription;
    private final ExclusivePublication consensusModulePublication;
    private final ControlledFragmentAssembler fragmentAssembler =
        new ControlledFragmentAssembler(this, INITIAL_BUFFER_LENGTH, true);
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final SessionOpenEventDecoder openEventDecoder = new SessionOpenEventDecoder();
    private final SessionCloseEventDecoder closeEventDecoder = new SessionCloseEventDecoder();
    private final SessionHeaderDecoder sessionHeaderDecoder = new SessionHeaderDecoder();
    private final TimerEventDecoder timerEventDecoder = new TimerEventDecoder();
    private final BufferClaim bufferClaim = new BufferClaim();
    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final ScheduleTimerRequestEncoder scheduleTimerRequestEncoder = new ScheduleTimerRequestEncoder();
    private final CancelTimerRequestEncoder cancelTimerRequestEncoder = new CancelTimerRequestEncoder();

    private final Long2ObjectHashMap<ClientSession> sessionByIdMap = new Long2ObjectHashMap<>();

    private final ClusterRecordingEventLog recordingEventLog;
    private final AeronArchive.Context archiveCtx;
    private final ClusteredServiceContainer.Context ctx;

    private ReadableCounter recordingPositionCounter;
    private volatile Image latestLogImage;

    public ClusteredServiceAgent(final ClusteredServiceContainer.Context ctx)
    {
        this.ctx = ctx;

        serviceId = ctx.serviceId();
        aeron = ctx.aeron();
        shouldCloseResources = ctx.ownsAeronClient();
        service = ctx.clusteredService();
        logSubscription = aeron.addSubscription(ctx.logChannel(), ctx.logStreamId(), this, this);
        consensusModulePublication = aeron.addExclusivePublication(ctx.timerChannel(), ctx.consensusModuleStreamId());
        recordingEventLog = ctx.clusterRecordingEventLog();
        archiveCtx = ctx.archiveContext();
    }

    public void onStart()
    {
        service.onStart(this);
        replayPreviousLogs();
        notifyReady();
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

            CloseHelper.close(recordingEventLog);
        }
    }

    public int doWork()
    {
        int workCount = 0;

        if (null != latestLogImage)
        {
            workCount += latestLogImage.boundedControlledPoll(
                fragmentAssembler, recordingPositionCounter.get(), FRAGMENT_LIMIT);
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
        }

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

    public void scheduleTimer(final long correlationId, final long deadlineMs)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + ScheduleTimerRequestEncoder.BLOCK_LENGTH;

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

            Thread.yield();
        }
        while (--attempts > 0);

        throw new IllegalStateException("Failed to schedule timer");
    }

    public void cancelTimer(final long correlationId)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + CancelTimerRequestEncoder.BLOCK_LENGTH;

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

            Thread.yield();
        }
        while (--attempts > 0);

        throw new IllegalStateException("Failed to schedule timer");
    }

    public void onAvailableImage(final Image image)
    {
        // TODO: make sessionId specific?
        latestLogImage = image;
    }

    public void onUnavailableImage(final Image image)
    {
        latestLogImage = null;
    }

    private void replayPreviousLogs()
    {
        try (AeronArchive aeronArchive = AeronArchive.connect(archiveCtx))
        {
            final IdleStrategy idleStrategy = new BackoffIdleStrategy(100, 100, 100, 1000);
            final CountersReader countersReader = aeron.countersReader();

            final Long2ObjectHashMap<RecordingInfo> recordingsMap = RecordingInfo.mapRecordings(
                aeronArchive, 0, 100, logSubscription.channel(), logSubscription.streamId());

            if (recordingsMap.size() == 0)
            {
                throw new IllegalStateException("could not find any log recordings");
            }

            final RecordingInfo latestRecordingInfo = RecordingInfo.findLatestRecording(recordingsMap);
            final int recordingPositionCounterId =
                RecordingPos.findActiveRecordingPositionCounterId(countersReader, latestRecordingInfo.recordingId);

            if (recordingPositionCounterId == RecordingPos.NULL_COUNTER_ID)
            {
                throw new IllegalStateException("could not find latest recording position counter Id");
            }

            recordingPositionCounter = new ReadableCounter(countersReader, recordingPositionCounterId);

            final MutableLong lastReplayedRecordingId = new MutableLong(-1);

            recordingEventLog.forEachFromLastSnapshot(
                (type, id, position, absolutePosition, messageIndex) ->
                {
                    final RecordingInfo recordingInfo = recordingsMap.get(id);
                    lastReplayedRecordingId.value = id;

                    if (ClusterRecordingEventLog.RECORDING_TYPE_SNAPSHOT == type)
                    {
                        // replay snapshot
                    }
                    else if (ClusterRecordingEventLog.RECORDING_TYPE_LOG == type)
                    {
                        replayRecording(idleStrategy, aeronArchive, recordingInfo);
                    }
                });

            while (!logSubscription.isConnected() && null == latestLogImage)
            {
                idleStrategy.idle();
            }

            if (lastReplayedRecordingId.value != latestRecordingInfo.recordingId)
            {
                recordingEventLog.appendLog(latestRecordingInfo.recordingId, latestLogImage.position(), -1, -1);
            }
        }
    }

    private void replayRecording(
        final IdleStrategy idleStrategy, final AeronArchive aeronArchive, final RecordingInfo recordingInfo)
    {
        if (null == recordingInfo)
        {
            throw new IllegalStateException("could not find log recording");
        }

        final long length = recordingInfo.stopPosition - recordingInfo.startPosition;
        if (length == 0)
        {
            return;
        }

        try (Subscription replaySubscription = aeronArchive.replay(
             recordingInfo.recordingId,
             recordingInfo.startPosition,
             length,
             ctx.logReplayChannel(),
             ctx.logReplayStreamId()))
        {
            while (!replaySubscription.isConnected())
            {
                idleStrategy.idle();
            }

            if (replaySubscription.imageCount() > 1)
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

                    if (Thread.currentThread().isInterrupted())
                    {
                        throw new AgentTerminationException("Unexpected interrupt during replay");
                    }
                }

                idleStrategy.idle(workCount);
            }
        }
    }

    private void notifyReady()
    {
        final ServiceReadyEncoder encoder = new ServiceReadyEncoder();
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + ServiceReadyEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS;
        do
        {
            if (consensusModulePublication.tryClaim(length, bufferClaim) > 0)
            {
                encoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .serviceId(serviceId);

                bufferClaim.commit();

                return;
            }

            Thread.yield();
        }
        while (--attempts > 0);

        throw new IllegalStateException("Failed to notify ready");
    }
}
