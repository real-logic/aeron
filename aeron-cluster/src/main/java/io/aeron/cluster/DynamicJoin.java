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

import io.aeron.*;
import io.aeron.archive.client.*;
import io.aeron.cluster.codecs.SnapshotRecordingsDecoder;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;

import java.util.ArrayList;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.CommonContext.ENDPOINT_PARAM_NAME;

class DynamicJoin implements AutoCloseable
{
    enum State
    {
        INIT,
        PASSIVE_FOLLOWER,
        SNAPSHOT_RETRIEVE,
        SNAPSHOT_LOAD,
        JOIN_CLUSTER,
        DONE
    }

    private final AeronArchive localArchive;
    private final ConsensusPublisher consensusPublisher;
    private final ConsensusModule.Context ctx;
    private final ConsensusModuleAgent consensusModuleAgent;
    private final String[] clusterConsensusEndpoints;
    private final String consensusEndpoints;
    private final String consensusEndpoint;
    private final ArrayList<RecordingLog.Snapshot> leaderSnapshots = new ArrayList<>();
    private final long intervalNs;

    private ExclusivePublication consensusPublication;
    private State state = State.INIT;
    private ClusterMember[] clusterMembers;
    private ClusterMember leaderMember;
    private SnapshotRetrieveMonitor snapshotRetrieveMonitor;
    private Counter recoveryStateCounter;
    private long timeOfLastActivityNs = 0;
    private long correlationId = NULL_VALUE;
    private int memberId = NULL_VALUE;
    private int clusterConsensusEndpointsCursor = NULL_VALUE;
    private int snapshotCursor = 0;

    DynamicJoin(
        final String consensusEndpoints,
        final AeronArchive localArchive,
        final ConsensusPublisher consensusPublisher,
        final ConsensusModule.Context ctx,
        final ConsensusModuleAgent consensusModuleAgent)
    {
        final ClusterMember thisMember = ClusterMember.parseEndpoints(-1, ctx.memberEndpoints());

        this.localArchive = localArchive;
        this.consensusPublisher = consensusPublisher;
        this.ctx = ctx;
        this.consensusModuleAgent = consensusModuleAgent;
        this.intervalNs = ctx.dynamicJoinIntervalNs();
        this.consensusEndpoints = ctx.memberEndpoints();
        this.consensusEndpoint = thisMember.consensusEndpoint();
        this.clusterConsensusEndpoints = consensusEndpoints.split(",");
    }

    public void close()
    {
        final ErrorHandler countedErrorHandler = ctx.countedErrorHandler();
        CloseHelper.closeAll(countedErrorHandler, consensusPublication);
    }

    ClusterMember[] clusterMembers()
    {
        return clusterMembers;
    }

    ClusterMember leader()
    {
        return leaderMember;
    }

    int memberId()
    {
        return memberId;
    }

    int doWork(final long nowNs)
    {
        int workCount = 0;

        switch (state)
        {
            case INIT:
                workCount += init(nowNs);
                break;

            case PASSIVE_FOLLOWER:
                workCount += passiveFollower(nowNs);
                break;

            case SNAPSHOT_RETRIEVE:
                workCount += snapshotRetrieve();
                break;

            case SNAPSHOT_LOAD:
                workCount += snapshotLoad(nowNs);
                break;

            case JOIN_CLUSTER:
                workCount += joinCluster();
                break;
        }

        return workCount;
    }

    void onClusterMembersChange(
        final long correlationId, final int leaderMemberId, final String activeMembers, final String passiveMembers)
    {
        if (State.INIT == state && correlationId == this.correlationId)
        {
            final ClusterMember[] passiveFollowers = ClusterMember.parse(passiveMembers);

            for (final ClusterMember follower : passiveFollowers)
            {
                if (consensusEndpoint.equals(follower.consensusEndpoint()))
                {
                    memberId = follower.id();
                    clusterMembers = ClusterMember.parse(activeMembers);
                    leaderMember = ClusterMember.findMember(clusterMembers, leaderMemberId);

                    if (null != leaderMember)
                    {
                        if (!leaderMember.consensusEndpoint().equals(
                            clusterConsensusEndpoints[clusterConsensusEndpointsCursor]))
                        {
                            CloseHelper.close(ctx.countedErrorHandler(), consensusPublication);

                            final ChannelUri consensusUri = ChannelUri.parse(ctx.consensusChannel());
                            consensusUri.put(ENDPOINT_PARAM_NAME, leaderMember.consensusEndpoint());
                            consensusPublication = ctx.aeron().addExclusivePublication(
                                consensusUri.toString(), ctx.consensusStreamId());
                        }

                        timeOfLastActivityNs = 0;
                        state(State.PASSIVE_FOLLOWER);
                    }

                    break;
                }
            }
        }
    }

    void onSnapshotRecordings(final long correlationId, final SnapshotRecordingsDecoder snapshotRecordingsDecoder)
    {
        if (State.PASSIVE_FOLLOWER == state && correlationId == this.correlationId)
        {
            final SnapshotRecordingsDecoder.SnapshotsDecoder snapshotsDecoder = snapshotRecordingsDecoder.snapshots();
            if (snapshotsDecoder.count() > 0)
            {
                for (final SnapshotRecordingsDecoder.SnapshotsDecoder snapshot : snapshotsDecoder)
                {
                    if (snapshot.serviceId() <= ctx.serviceCount())
                    {
                        leaderSnapshots.add(new RecordingLog.Snapshot(
                            snapshot.recordingId(),
                            snapshot.leadershipTermId(),
                            snapshot.termBaseLogPosition(),
                            snapshot.logPosition(),
                            snapshot.timestamp(),
                            snapshot.serviceId()));
                    }
                }
            }

            timeOfLastActivityNs = 0;
            snapshotCursor = 0;
            this.correlationId = NULL_VALUE;

            if (leaderSnapshots.isEmpty())
            {
                state(State.SNAPSHOT_LOAD);
            }
            else
            {
                state(State.SNAPSHOT_RETRIEVE);
            }
        }
    }

    private int init(final long nowNs)
    {
        if (nowNs > (timeOfLastActivityNs + intervalNs))
        {
            int cursor = ++clusterConsensusEndpointsCursor;
            if (cursor >= clusterConsensusEndpoints.length)
            {
                clusterConsensusEndpointsCursor = 0;
                cursor = 0;
            }

            CloseHelper.close(ctx.countedErrorHandler(), consensusPublication);
            final ChannelUri uri = ChannelUri.parse(ctx.consensusChannel());
            uri.put(ENDPOINT_PARAM_NAME, clusterConsensusEndpoints[cursor]);
            consensusPublication = ctx.aeron().addExclusivePublication(uri.toString(), ctx.consensusStreamId());
            correlationId = NULL_VALUE;
            timeOfLastActivityNs = nowNs;

            return 1;
        }
        else if (NULL_VALUE == correlationId && consensusPublication.isConnected())
        {
            final long correlationId = ctx.aeron().nextCorrelationId();

            if (consensusPublisher.addPassiveMember(consensusPublication, correlationId, consensusEndpoints))
            {
                timeOfLastActivityNs = nowNs;
                this.correlationId = correlationId;

                return 1;
            }
        }

        return 0;
    }

    private int passiveFollower(final long nowNs)
    {
        if (nowNs > (timeOfLastActivityNs + intervalNs))
        {
            correlationId = ctx.aeron().nextCorrelationId();

            if (consensusPublisher.snapshotRecordingQuery(consensusPublication, correlationId, memberId))
            {
                timeOfLastActivityNs = nowNs;
                return 1;
            }
        }

        return 0;
    }

    private int snapshotRetrieve()
    {
        int workCount = 0;

        if (null != snapshotRetrieveMonitor)
        {
            workCount += snapshotRetrieveMonitor.poll();
            if (snapshotRetrieveMonitor.isDone())
            {
                consensusModuleAgent.retrievedSnapshot(
                    snapshotRetrieveMonitor.recordingId(), leaderSnapshots.get(snapshotCursor));

                snapshotRetrieveMonitor = null;

                if (++snapshotCursor >= leaderSnapshots.size())
                {
                    state(State.SNAPSHOT_LOAD);
                    workCount++;
                }
            }
        }
        else if (localArchive.archiveProxy().replicate(
            leaderSnapshots.get(snapshotCursor).recordingId,
            NULL_VALUE,
            ctx.archiveContext().controlRequestStreamId(),
            "aeron:udp?term-length=64k|endpoint=" + leaderMember.archiveEndpoint(),
            null,
            ctx.aeron().nextCorrelationId(),
            localArchive.controlSessionId()))
        {
            snapshotRetrieveMonitor = new SnapshotRetrieveMonitor(localArchive);
            workCount++;
        }

        return workCount;
    }

    private int snapshotLoad(final long nowNs)
    {
        int workCount = 0;

        if (null == recoveryStateCounter)
        {
            recoveryStateCounter = consensusModuleAgent.loadSnapshotsForDynamicJoin();
            workCount++;
        }
        else if (consensusModuleAgent.pollForSnapshotLoadAck(recoveryStateCounter, nowNs))
        {
            CloseHelper.close(ctx.countedErrorHandler(), recoveryStateCounter);
            recoveryStateCounter = null;
            state(State.JOIN_CLUSTER);
            workCount++;
        }

        return workCount;
    }

    private int joinCluster()
    {
        int workCount = 0;
        final long leadershipTermId = leaderSnapshots.isEmpty() ? NULL_VALUE : leaderSnapshots.get(0).leadershipTermId;

        if (consensusPublisher.joinCluster(consensusPublication, leadershipTermId, memberId))
        {
            if (consensusModuleAgent.dynamicJoinComplete())
            {
                state(State.DONE);
                close();
                workCount++;
            }
        }

        return workCount;
    }

    private void state(final State newState)
    {
        //System.out.println("DynamicJoin: memberId=" + memberId + " " + state + " -> " + newState);
        state = newState;
        correlationId = NULL_VALUE;
    }
}
