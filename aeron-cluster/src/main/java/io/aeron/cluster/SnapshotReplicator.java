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
import io.aeron.archive.client.ControlResponsePoller;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.SnapshotRecordingsDecoder;

import java.util.ArrayList;
import java.util.List;

class SnapshotReplicator
{
    enum State
    {
        IDLE,
        QUERY,
        REPLICATE,
        COMPLETE
    }

    private class ActiveReplication
    {
        private RecordingLog.Snapshot snapshot;
        private ClusterMember snapshotMember;
        private long requestCorrelationId;
        private long replicationCorrelationId;
        private boolean isComplete = true;
        private long replicatedRecordingId;

        private void reset(final RecordingLog.Snapshot snapshot, final ClusterMember snapshotMember)
        {
            this.snapshot = snapshot;
            this.snapshotMember = snapshotMember;
            this.requestCorrelationId = Aeron.NULL_VALUE;
            this.replicationCorrelationId = Aeron.NULL_VALUE;
            this.replicatedRecordingId = Aeron.NULL_VALUE;
            this.isComplete = null == snapshot;
        }

        private int doWork()
        {
            if (isComplete)
            {
                return 0;
            }

            final int workDone;
            if (Aeron.NULL_VALUE == requestCorrelationId)
            {
                requestCorrelationId = startReplication();
                workDone = 1;
            }
            else
            {
                workDone = replicating();
            }

            return workDone;
        }

        private long startReplication()
        {
            final String srcArchiveChannel = "aeron:udp?endpoint=" + snapshotMember.archiveEndpoint();

            final long correlationId = ctx.aeron().nextCorrelationId();
            final boolean successfulOffer = archive.archiveProxy().replicate(
                snapshot.recordingId,
                Aeron.NULL_VALUE,
                AeronArchive.NULL_POSITION,
                archive.context().controlRequestStreamId(),
                srcArchiveChannel,
                null,
                ctx.replicationChannel(),
                correlationId,
                archive.controlSessionId());

            return successfulOffer ? correlationId : Aeron.NULL_VALUE;
        }

        private int replicating()
        {
            int workDone = 0;

            final ControlResponsePoller controlResponsePoller = archive.controlResponsePoller();
            while (0 != controlResponsePoller.poll())
            {
                if (archive.controlSessionId() == controlResponsePoller.controlSessionId())
                {
                    if (requestCorrelationId == controlResponsePoller.correlationId())
                    {
                        final ControlResponseCode code = controlResponsePoller.code();
                        switch (code)
                        {
                            case OK:
                                replicationCorrelationId = controlResponsePoller.relevantId();
                                return 1;

                            case ERROR:
                                throw new ClusterException(controlResponsePoller.errorMessage());

                            case RECORDING_UNKNOWN:
                            case SUBSCRIPTION_UNKNOWN:
                            case NULL_VAL:
                                break;
                        }

                        workDone = 1;
                    }
                    else if (replicationCorrelationId == controlResponsePoller.correlationId() &&
                        null != controlResponsePoller.recordingSignal())
                    {
                        if (RecordingSignal.STOP == controlResponsePoller.recordingSignal())
                        {
                            replicatedRecordingId = controlResponsePoller.recordingId();
                            isComplete = true;
                        }

                        workDone = 1;
                    }
                }
            }

            return workDone;
        }

        public boolean isComplete()
        {
            return isComplete;
        }

        public RecordingLog.Snapshot snapshot()
        {
            return snapshot;
        }

        public long replicatedRecordingId()
        {
            return replicatedRecordingId;
        }
    }

    private final AeronArchive archive;
    private final ConsensusModule.Context ctx;
    private final ConsensusPublisher consensusPublisher;
    private final long queryTimeoutMs = 5_000;
    private final ArrayList<RecordingLog.Snapshot> snapshotsToRetrieve = new ArrayList<>(4);
    private final ArrayList<RecordingLog.Snapshot> snapshotsRetrieved = new ArrayList<>(4);
    private final ActiveReplication activeReplication = new ActiveReplication();

    private ClusterMember snapshotMember;
    private long logPosition = Aeron.NULL_VALUE;
    private long queryCorrelationId = Aeron.NULL_VALUE;
    private long sendQueryDeadlineMs = 0;
    private State state = State.IDLE;

    SnapshotReplicator(final ConsensusModule.Context ctx, final ConsensusPublisher consensusPublisher)
    {
        this.ctx = ctx;
        this.consensusPublisher = consensusPublisher;
        this.archive = AeronArchive.connect(ctx.archiveContext().clone());
    }

    void setLatestSnapshot(final ClusterMember snapshotMember, final long logPosition)
    {
        this.snapshotMember = snapshotMember;
        this.logPosition = logPosition;
        snapshotsToRetrieve.clear();
        snapshotsRetrieved.clear();
        state = State.QUERY;
    }

    int doWork(final long nowMs, final ClusterMember thisMember)
    {
        switch (state)
        {
            case QUERY:
                return query(nowMs, thisMember);

            case REPLICATE:
                return replicate(nowMs);

            case COMPLETE:
                snapshotsToRetrieve.clear();
                snapshotsRetrieved.clear();
                state = State.IDLE;
                break;

            case IDLE:
            default:
                break;
        }

        return 0;
    }

    private int query(final long nowMs, final ClusterMember thisMember)
    {
        int workDone = 0;

        if (null == snapshotMember || Aeron.NULL_VALUE == logPosition)
        {
            // TODO: Log warning?
            state = State.IDLE;
            return 0;
        }

        if (sendQueryDeadlineMs <= nowMs)
        {
            queryCorrelationId = ctx.aeron().nextCorrelationId();
            consensusPublisher.snapshotRecordingQuery(
                snapshotMember.publication(),
                queryCorrelationId,
                thisMember.id());

            workDone++;
            sendQueryDeadlineMs = nowMs + queryTimeoutMs;
        }

        return workDone;
    }

    private int replicate(final long nowMs)
    {
        if (activeReplication.isComplete())
        {
            final RecordingLog.Snapshot completeSnapshot = activeReplication.snapshot();
            if (null != completeSnapshot)
            {
                snapshotsRetrieved.add(new RecordingLog.Snapshot(
                    activeReplication.replicatedRecordingId(),
                    completeSnapshot.leadershipTermId,
                    completeSnapshot.termBaseLogPosition,
                    completeSnapshot.logPosition,
                    completeSnapshot.timestamp,
                    completeSnapshot.serviceId));
                activeReplication.reset(null, null);
            }

            if (!snapshotsToRetrieve.isEmpty())
            {
                final RecordingLog.Snapshot snapshot = snapshotsToRetrieve.remove(snapshotsToRetrieve.size() - 1);
                activeReplication.reset(snapshot, snapshotMember);
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
        if (this.queryCorrelationId == correlationId)
        {
            snapshotsToRetrieve.clear();

            final SnapshotRecordingsDecoder.SnapshotsDecoder snapshots = decoder.snapshots();
            while (snapshots.hasNext())
            {
                snapshots.next();

                if (logPosition == snapshots.logPosition())
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
            this.sendQueryDeadlineMs = nowMs + 100;
            this.queryCorrelationId = Aeron.NULL_VALUE;
        }
    }

    boolean isComplete()
    {
        return State.COMPLETE == state;
    }

    List<RecordingLog.Snapshot> snapshotsRetrieved()
    {
        return snapshotsRetrieved;
    }
}
