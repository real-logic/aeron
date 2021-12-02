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
import io.aeron.cluster.codecs.SnapshotRecordingsDecoder;

import java.util.ArrayList;

class SnapshotReplicator
{
    enum State
    {
        IDLE,
        QUERY,
        REPLICATE
    }

    private final ConsensusModule.Context ctx;
    private final ConsensusPublisher consensusPublisher;
    private final long queryTimeoutMs = 5_000;
    private final ArrayList<RecordingLog.Snapshot> snapshotsToRetrieve = new ArrayList<>(4);
    private final ArrayList<RecordingLog.Snapshot> snapshotsRetrieved = new ArrayList<>(4);

    private int memberId = Aeron.NULL_VALUE;
    private long logPosition = Aeron.NULL_VALUE;
    private long correlationId = Aeron.NULL_VALUE;
    private long sendQueryDeadlineMs = 0;
    private State state = State.IDLE;

    SnapshotReplicator(final ConsensusModule.Context ctx, final ConsensusPublisher consensusPublisher)
    {
        this.ctx = ctx;
        this.consensusPublisher = consensusPublisher;
    }

    void setLatestSnapshot(final int memberId, final long logPosition)
    {
        this.memberId = memberId;
        this.logPosition = logPosition;
        state = State.QUERY;
    }

    int doWork(final long nowMs, final ClusterMember[] activeMembers, final int thisMemberId)
    {
        switch (state)
        {
            case QUERY:
                return query(nowMs, activeMembers, thisMemberId);

            case REPLICATE:
                return replicate(nowMs, activeMembers, thisMemberId);

            case IDLE:
            default:
                break;
        }

        return 0;
    }

    private int query(final long nowMs, final ClusterMember[] activeMembers, final int thisMemberId)
    {
        int workDone = 0;

        if (Aeron.NULL_VALUE == memberId || Aeron.NULL_VALUE == logPosition)
        {
            // TODO: Log warning?
            return 0;
        }

        if (sendQueryDeadlineMs <= nowMs)
        {
            System.out.println("Querying for snapshots on: " + memberId + " from: " + thisMemberId);
            correlationId = ctx.aeron().nextCorrelationId();
            consensusPublisher.snapshotRecordingQuery(
                activeMembers[memberId].publication(),
                correlationId,
                thisMemberId);

            workDone++;
            sendQueryDeadlineMs = nowMs + queryTimeoutMs;
        }

        return workDone;
    }

    private int replicate(final long nowMs, final ClusterMember[] activeMembers, final int thisMemberId)
    {
        System.out.println("Replicating: " + snapshotsToRetrieve);
        state = State.IDLE;
        return 0;
    }

    public void onSnapshotRecordings(final long correlationId, final SnapshotRecordingsDecoder decoder, final long nowMs)
    {
        if (this.correlationId == correlationId)
        {
            System.out.println(decoder);
            snapshotsToRetrieve.clear();

            final SnapshotRecordingsDecoder.SnapshotsDecoder snapshots = decoder.snapshots();
            while (snapshots.hasNext())
            {
                snapshots.next();
                if (logPosition == snapshots.logPosition())
                {
                    System.out.println("Found log: " + snapshots);
                    this.logPosition = Aeron.NULL_VALUE;
                    this.memberId = Aeron.NULL_VALUE;

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
            this.correlationId = Aeron.NULL_VALUE;
        }
    }
}
