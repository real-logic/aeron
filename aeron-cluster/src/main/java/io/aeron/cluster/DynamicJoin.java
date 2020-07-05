/*
 * Copyright 2014-2020 Real Logic Limited.
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
import io.aeron.archive.codecs.*;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.SnapshotRecordingsDecoder;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.collections.LongArrayList;
import org.agrona.concurrent.NoOpLock;

import java.util.ArrayList;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.CommonContext.ENDPOINT_PARAM_NAME;
import static io.aeron.archive.client.AeronArchive.NULL_LENGTH;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;

class DynamicJoin implements AutoCloseable
{
    enum State
    {
        INIT,
        PASSIVE_FOLLOWER,
        SNAPSHOT_LENGTH_RETRIEVE,
        SNAPSHOT_RETRIEVE,
        SNAPSHOT_LOAD,
        JOIN_CLUSTER,
        DONE
    }

    private final AeronArchive localArchive;
    private final ConsensusAdapter consensusAdapter;
    private final ConsensusPublisher consensusPublisher;
    private final ConsensusModule.Context ctx;
    private final ConsensusModuleAgent consensusModuleAgent;
    private final String[] clusterConsensusEndpoints;
    private final String consensusEndpoints;
    private final String consensusEndpoint;
    private final String catchupEndpoint;
    private final String archiveEndpoint;
    private final ArrayList<RecordingLog.Snapshot> leaderSnapshots = new ArrayList<>();
    private final LongArrayList snapshotLengths = new LongArrayList();
    private final long intervalNs;

    private ExclusivePublication consensusPublication;
    private State state = State.INIT;
    private ClusterMember[] clusterMembers;
    private ClusterMember leaderMember;
    private AeronArchive.AsyncConnect leaderArchiveAsyncConnect;
    private AeronArchive leaderArchive;
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
        final ConsensusAdapter consensusAdapter,
        final ConsensusPublisher consensusPublisher,
        final ConsensusModule.Context ctx,
        final ConsensusModuleAgent consensusModuleAgent)
    {
        final ClusterMember thisMember = ClusterMember.parseEndpoints(-1, ctx.memberEndpoints());

        this.localArchive = localArchive;
        this.consensusAdapter = consensusAdapter;
        this.consensusPublisher = consensusPublisher;
        this.ctx = ctx;
        this.consensusModuleAgent = consensusModuleAgent;
        this.intervalNs = ctx.dynamicJoinIntervalNs();
        this.consensusEndpoints = ctx.memberEndpoints();
        this.consensusEndpoint = thisMember.consensusEndpoint();
        this.catchupEndpoint = thisMember.catchupEndpoint();
        this.archiveEndpoint = thisMember.archiveEndpoint();
        this.clusterConsensusEndpoints = consensusEndpoints.split(",");
    }

    public void close()
    {
        final ErrorHandler countedErrorHandler = ctx.countedErrorHandler();
        CloseHelper.closeAll(countedErrorHandler, consensusPublication, leaderArchive, leaderArchiveAsyncConnect);
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
        workCount += consensusAdapter.poll();

        switch (state)
        {
            case INIT:
                workCount += init(nowNs);
                break;

            case PASSIVE_FOLLOWER:
                workCount += passiveFollower(nowNs);
                break;

            case SNAPSHOT_LENGTH_RETRIEVE:
                workCount += snapshotLengthRetrieve();
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
                final AeronArchive.Context leaderArchiveCtx = new AeronArchive.Context()
                    .aeron(ctx.aeron())
                    .lock(NoOpLock.INSTANCE)
                    .controlRequestChannel("aeron:udp?endpoint=" + leaderMember.archiveEndpoint())
                    .controlRequestStreamId(ctx.archiveContext().controlRequestStreamId())
                    .controlResponseChannel("aeron:udp?endpoint=" + archiveEndpoint)
                    .controlResponseStreamId(ctx.archiveContext().controlResponseStreamId());

                leaderArchiveAsyncConnect = AeronArchive.asyncConnect(leaderArchiveCtx);
                state(State.SNAPSHOT_LENGTH_RETRIEVE);
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

    private int snapshotLengthRetrieve()
    {
        int workCount = 0;

        if (null == leaderArchive)
        {
            leaderArchive = leaderArchiveAsyncConnect.poll();
            return null == leaderArchive ? 0 : 1;
        }

        if (NULL_VALUE == correlationId)
        {
            final long stopPositionCorrelationId = ctx.aeron().nextCorrelationId();
            final RecordingLog.Snapshot snapshot = leaderSnapshots.get(snapshotCursor);

            if (leaderArchive.archiveProxy().getStopPosition(
                snapshot.recordingId,
                stopPositionCorrelationId,
                leaderArchive.controlSessionId()))
            {
                correlationId = stopPositionCorrelationId;
                workCount++;
            }
        }
        else if (pollForResponse(leaderArchive, correlationId))
        {
            correlationId = NULL_VALUE;
            final long snapshotStopPosition = leaderArchive.controlResponsePoller().relevantId();

            if (NULL_POSITION == snapshotStopPosition)
            {
                throw new ClusterException("snapshot stopPosition is NULL_POSITION");
            }

            snapshotLengths.addLong(snapshotCursor, snapshotStopPosition);
            if (++snapshotCursor >= leaderSnapshots.size())
            {
                snapshotCursor = 0;
                state(State.SNAPSHOT_RETRIEVE);
            }

            workCount++;
        }

        return workCount;
    }

    private int snapshotRetrieve()
    {
        int workCount = 0;

        if (null == leaderArchive)
        {
            leaderArchive = leaderArchiveAsyncConnect.poll();
            return null == leaderArchive ? 0 : 1;
        }

        if (null != snapshotRetrieveMonitor)
        {
            workCount += snapshotRetrieveMonitor.poll();
            if (snapshotRetrieveMonitor.isDone())
            {
                consensusModuleAgent.retrievedSnapshot(
                    snapshotRetrieveMonitor.recordingId(), leaderSnapshots.get(snapshotCursor));

                snapshotRetrieveMonitor = null;
                correlationId = NULL_VALUE;

                if (++snapshotCursor >= leaderSnapshots.size())
                {
                    state(State.SNAPSHOT_LOAD);
                    workCount++;
                }
            }
        }
        else if (NULL_VALUE == correlationId)
        {
            final long replayId = ctx.aeron().nextCorrelationId();
            final RecordingLog.Snapshot snapshot = leaderSnapshots.get(snapshotCursor);
            final String catchupChannel = "aeron:udp?endpoint=" + catchupEndpoint;

            if (leaderArchive.archiveProxy().replay(
                snapshot.recordingId,
                0,
                NULL_LENGTH,
                catchupChannel,
                ctx.replayStreamId(),
                replayId,
                leaderArchive.controlSessionId()))
            {
                correlationId = replayId;
                workCount++;
            }
        }
        else if (pollForResponse(leaderArchive, correlationId))
        {
            final int replaySessionId = (int)leaderArchive.controlResponsePoller().relevantId();
            final String catchupChannel = "aeron:udp?endpoint=" + catchupEndpoint + "|session-id=" + replaySessionId;

            snapshotRetrieveMonitor = new SnapshotRetrieveMonitor(localArchive, snapshotLengths.get(snapshotCursor));

            localArchive.archiveProxy().startRecording(
                catchupChannel,
                ctx.replayStreamId(),
                SourceLocation.REMOTE,
                true,
                localArchive.context().aeron().nextCorrelationId(),
                localArchive.controlSessionId());

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
    }

    private static boolean pollForResponse(final AeronArchive archive, final long correlationId)
    {
        final ControlResponsePoller poller = archive.controlResponsePoller();

        if (poller.poll() > 0 && poller.isPollComplete())
        {
            if (poller.controlSessionId() == archive.controlSessionId() && poller.correlationId() == correlationId)
            {
                if (poller.code() == ControlResponseCode.ERROR)
                {
                    throw new ClusterException(
                        "archive response for correlationId=" + correlationId + ", error: " + poller.errorMessage());
                }

                return true;
            }
        }

        return false;
    }
}
