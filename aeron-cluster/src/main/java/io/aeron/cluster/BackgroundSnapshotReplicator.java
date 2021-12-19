/*
 * Copyright 2014-2021 Real Logic Limited.
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
package io.aeron.cluster;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.codecs.SnapshotRecordingsDecoder;

import java.lang.reflect.Member;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

class BackgroundSnapshotReplicator
{
    enum State
    {
        IDLE,
        AWAIT_QUERY,
        REPLICATE,
        COMPLETE,
        FAILED
    }

    private class ActiveReplication
    {
        private RecordingLog.Snapshot snapshot;
        private ClusterMember snapshotMember;
        private long correlationId;
        private long replicationId;
        private boolean isComplete = true;
        private long destinationRecordingId;
        private ArchiveException ex;

        private void reset(final RecordingLog.Snapshot snapshot, final ClusterMember snapshotMember)
        {
            this.snapshot = snapshot;
            this.snapshotMember = snapshotMember;
            this.correlationId = Aeron.NULL_VALUE;
            this.replicationId = Aeron.NULL_VALUE;
            this.destinationRecordingId = Aeron.NULL_VALUE;
            this.isComplete = null == snapshot;
        }

        private int doWork()
        {
            if (isComplete)
            {
                return 0;
            }

            if (null != ex)
            {
                if (Aeron.NULL_VALUE != replicationId)
                {
                    archive.stopReplication(replicationId);
                }

                return 0;
            }

            final int workDone;
            if (Aeron.NULL_VALUE == correlationId)
            {
                correlationId = startReplication();
                workDone = 1;
            }
            else
            {
                workDone = 0;
            }

            return workDone;
        }

        private long startReplication()
        {
            final String srcArchiveChannel = "aeron:udp?endpoint=" + snapshotMember.archiveEndpoint();

            final long correlationId = ctx.aeron().nextCorrelationId();
            final boolean successfulOffer = archive.archiveProxy().replicate(
                snapshot.recordingId,
                RecordingPos.NULL_RECORDING_ID,
                AeronArchive.NULL_POSITION,
                archive.context().controlRequestStreamId(),
                srcArchiveChannel,
                null,
                ctx.replicationChannel(),
                correlationId,
                archive.controlSessionId());

            return successfulOffer ? correlationId : Aeron.NULL_VALUE;
        }

        public boolean isComplete()
        {
            return isComplete;
        }

        public RecordingLog.Snapshot snapshot()
        {
            return snapshot;
        }

        public long destinationRecordingId()
        {
            return destinationRecordingId;
        }

        public void onArchiveControlError(final long correlationId, final ArchiveException ex)
        {
            if (correlationId == this.correlationId || correlationId == replicationId)
            {
                this.ex = ex;
                this.isComplete = true;
            }
        }

        public void onArchiveControlResponse(final long correlationId, final long replicationId)
        {
            if (correlationId == this.correlationId)
            {
                this.replicationId = replicationId;
            }
        }

        public void onRecordingSignal(
            final long correlationId,
            final long recordingId,
            final long position,
            final RecordingSignal signal)
        {
            if (correlationId == replicationId)
            {
                switch (signal)
                {
                    case EXTEND:
                        destinationRecordingId = recordingId;
                        break;

                    case STOP:
                        if (Aeron.NULL_VALUE == destinationRecordingId)
                        {
                            destinationRecordingId = recordingId;
                        }

                        isComplete = true;
                }
            }
        }

        private ArchiveException exception()
        {
            return ex;
        }
    }

    private AeronArchive archive;
    private final ConsensusModule.Context ctx;
    private final ConsensusPublisher consensusPublisher;
    private final long queryTimeoutMs;
    private final ClusterMember thisMember;
    private final ArrayList<RecordingLog.Snapshot> snapshotsToRetrieve = new ArrayList<>(4);
    private final ArrayList<RecordingLog.Snapshot> snapshotsRetrieved = new ArrayList<>(4);
    private int snapshotCursor = 0;
    private final ActiveReplication activeReplication = new ActiveReplication();

    private ClusterMember nextSnapshotMember = null;
    private long nextSnapshotLogPosition = Aeron.NULL_VALUE;

    private ClusterMember currentSnapshotMember = null;
    private long currentSnapshotLogPosition = Aeron.NULL_VALUE;

    private long queryCorrelationId = Aeron.NULL_VALUE;
    private long sendQueryDeadlineMs = 0;
    private State state = State.IDLE;

    BackgroundSnapshotReplicator(
        final ConsensusModule.Context ctx,
        final ConsensusPublisher consensusPublisher,
        final ClusterMember thisMember)
    {
        this.ctx = ctx;
        this.consensusPublisher = consensusPublisher;
        this.queryTimeoutMs = TimeUnit.NANOSECONDS.toMillis(ctx.dynamicJoinIntervalNs());
        this.thisMember = thisMember;
    }

    public void archive(final AeronArchive archive)
    {
        this.archive = archive;
    }

    void setLatestSnapshot(final ClusterMember snapshotMember, final long logPosition)
    {
        this.nextSnapshotMember = snapshotMember;
        this.nextSnapshotLogPosition = logPosition;
    }

    int doWork(final long nowMs)
    {
        switch (state)
        {
            case IDLE:
                if (null != nextSnapshotMember && Aeron.NULL_VALUE != nextSnapshotLogPosition)
                {
                    state = State.AWAIT_QUERY;
                    query(nowMs, thisMember);
                }
                break;

            case AWAIT_QUERY:
                return query(nowMs, thisMember);

            case REPLICATE:
                return replicate();

            case COMPLETE:
            case FAILED:
                snapshotsToRetrieve.clear();
                snapshotsRetrieved.clear();
                snapshotCursor = 0;
                currentSnapshotMember = null;
                currentSnapshotLogPosition = Aeron.NULL_VALUE;

                state = State.IDLE;
                break;

            default:
                break;
        }

        return 0;
    }

    private int query(final long nowMs, final ClusterMember thisMember)
    {
        int workDone = 0;

        if (sendQueryDeadlineMs <= nowMs)
        {
            if (null != nextSnapshotMember && currentSnapshotLogPosition < nextSnapshotLogPosition)
            {
                currentSnapshotMember = nextSnapshotMember;
                currentSnapshotLogPosition = nextSnapshotLogPosition;

                nextSnapshotMember = null;
                nextSnapshotLogPosition = Aeron.NULL_VALUE;
            }

            queryCorrelationId = ctx.aeron().nextCorrelationId();
            consensusPublisher.snapshotRecordingQuery(
                currentSnapshotMember.publication(),
                queryCorrelationId,
                thisMember.id());

            workDone++;
            sendQueryDeadlineMs = nowMs + queryTimeoutMs;
        }

        return workDone;
    }

    private int replicate()
    {
        if (activeReplication.isComplete())
        {
            if (null != activeReplication.exception())
            {
                ctx.errorLog().record(activeReplication.exception());
                state = State.FAILED;
                return 1;
            }

            final RecordingLog.Snapshot completeSnapshot = activeReplication.snapshot();
            if (null != completeSnapshot)
            {
                snapshotsRetrieved.add(new RecordingLog.Snapshot(
                    activeReplication.destinationRecordingId(),
                    completeSnapshot.leadershipTermId,
                    completeSnapshot.termBaseLogPosition,
                    completeSnapshot.logPosition,
                    completeSnapshot.timestamp,
                    completeSnapshot.serviceId));

                activeReplication.reset(null, null);
            }

            if (snapshotCursor < snapshotsToRetrieve.size())
            {
                final RecordingLog.Snapshot snapshot = snapshotsToRetrieve.get(snapshotCursor);
                activeReplication.reset(snapshot, currentSnapshotMember);
                snapshotCursor++;
            }
            else
            {
                state = State.COMPLETE;
                return 1;
            }
        }

        return activeReplication.doWork();
    }

    public void onSnapshotRecordings(
        final long correlationId,
        final SnapshotRecordingsDecoder decoder,
        final long nowMs)
    {
        if (State.AWAIT_QUERY == this.state &&
            this.queryCorrelationId == correlationId &&
            validateSnapshotRecordings(decoder, currentSnapshotLogPosition, ctx.serviceCount()))
        {
            decoder.sbeRewind();

            snapshotsToRetrieve.clear();

            final SnapshotRecordingsDecoder.SnapshotsDecoder snapshots = decoder.snapshots();
            while (snapshots.hasNext())
            {
                snapshots.next();

                if (currentSnapshotLogPosition == snapshots.logPosition())
                {
                    snapshotsToRetrieve.add(new RecordingLog.Snapshot(
                        snapshots.recordingId(),
                        snapshots.leadershipTermId(),
                        snapshots.termBaseLogPosition(),
                        snapshots.logPosition(),
                        snapshots.timestamp(),
                        snapshots.serviceId()));

                    state = State.REPLICATE;
                }
            }

            this.sendQueryDeadlineMs = nowMs + TimeUnit.NANOSECONDS.toMillis(ctx.leaderHeartbeatIntervalNs());
            this.queryCorrelationId = Aeron.NULL_VALUE;
        }
    }

    private boolean validateSnapshotRecordings(
        final SnapshotRecordingsDecoder decoder,
        final long logPosition,
        final int serviceCount)
    {
        final SnapshotRecordingsDecoder.SnapshotsDecoder snapshots = decoder.snapshots();
        int serviceSnapshotCount = 0;
        int consensusModuleSnapshotCount = 0;

        while (snapshots.hasNext())
        {
            snapshots.next();

            if (logPosition == snapshots.logPosition())
            {
                if (0 <= snapshots.serviceId())
                {
                    serviceSnapshotCount++;
                }
                else if (ConsensusModule.Configuration.SERVICE_ID == snapshots.serviceId())
                {
                    consensusModuleSnapshotCount++;
                }
            }
        }

        return serviceCount == serviceSnapshotCount && 1 == consensusModuleSnapshotCount;
    }

    boolean isComplete()
    {
        return State.COMPLETE == state;
    }

    List<RecordingLog.Snapshot> snapshotsRetrieved()
    {
        return snapshotsRetrieved;
    }

    void onArchiveControlError(final long correlationId, final ArchiveException ex)
    {
        activeReplication.onArchiveControlError(correlationId, ex);
    }

    void onArchiveControlResponse(final long correlationId, final long relevantId)
    {
        activeReplication.onArchiveControlResponse(correlationId, relevantId);
    }

    public void onRecordingSignal(
        final long correlationId,
        final long recordingId,
        final long position,
        final RecordingSignal signal)
    {
        activeReplication.onRecordingSignal(correlationId, recordingId, position, signal);
    }
}
