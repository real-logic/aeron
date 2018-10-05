/*
 * Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron.cluster;

import io.aeron.ChannelUri;
import io.aeron.ExclusivePublication;
import io.aeron.cluster.codecs.SnapshotRecordingsDecoder;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.CommonContext.ENDPOINT_PARAM_NAME;

public class DynamicJoin implements AutoCloseable
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

    private final ExclusivePublication clusterPublication;
    private final MemberStatusAdapter memberStatusAdapter;
    private final MemberStatusPublisher memberStatusPublisher;
    private final ConsensusModule.Context ctx;
    private final ConsensusModuleAgent consensusModuleAgent;
    private final String[] clusterMembersStatusEndpoints;
    private final String memberEndpoints;
    private final String memberStatusEndpoint;
    private final long intervalMs;

    private State state = State.INIT;
    private ClusterMember[] clusterMembers;
    private long timeOfLastActivity = 0;
    private long correlationId = NULL_VALUE;
    private int memberId = NULL_VALUE;
    private int highMemberId = NULL_VALUE;
    private int clusterMembersSattusEndpointsCursor = 0;

    public DynamicJoin(
        final String clusterMembersStatusEndpoints,
        final MemberStatusAdapter memberStatusAdapter,
        final MemberStatusPublisher memberStatusPublisher,
        final ConsensusModule.Context ctx,
        final ConsensusModuleAgent consensusModuleAgent)
    {
        this.memberStatusAdapter = memberStatusAdapter;
        this.memberStatusPublisher = memberStatusPublisher;
        this.ctx = ctx;
        this.consensusModuleAgent = consensusModuleAgent;
        this.intervalMs = TimeUnit.NANOSECONDS.toMillis(ctx.dynamicJoinIntervalNs());
        this.memberEndpoints = ctx.memberEndpoints();
        this.memberStatusEndpoint = ClusterMember.parseEndpoints(-1, ctx.memberEndpoints()).memberFacingEndpoint();
        this.clusterMembersStatusEndpoints = clusterMembersStatusEndpoints.split(",");

        final ChannelUri memberStatusUri = ChannelUri.parse(ctx.memberStatusChannel());
        memberStatusUri.put(
            ENDPOINT_PARAM_NAME, this.clusterMembersStatusEndpoints[clusterMembersSattusEndpointsCursor]);
        clusterPublication = ctx.aeron().addExclusivePublication(
            memberStatusUri.toString(), ctx.memberStatusStreamId());
    }

    public void close()
    {
        clusterPublication.close();
    }

    boolean isDone()
    {
        return State.DONE == state;
    }

    int doWork(final long nowMs)
    {
        int workCount = 0;
        workCount += memberStatusAdapter.poll();

        switch (state)
        {
            case INIT:
                workCount += init(nowMs);
                break;

            case PASSIVE_FOLLOWER:
                workCount += passiveFollower(nowMs);
                break;

            case SNAPSHOT_RETRIEVE:
                workCount += snapshotRetrieve(nowMs);
                break;

            case SNAPSHOT_LOAD:
                workCount += snapshotLoad(nowMs);
                break;

            case JOIN_CLUSTER:
                workCount += joinCluster(nowMs);
                break;
        }

        return workCount;
    }

    public void onClusterMembersChange(
        final long correlationId, final int leaderMemberId, final String activeMembers, final String passiveMembers)
    {
        if (State.INIT == state && correlationId == this.correlationId)
        {
            final ClusterMember[] passiveFollowers = ClusterMember.parse(passiveMembers);

            for (final ClusterMember follower : passiveFollowers)
            {
                if (memberStatusEndpoint.equals(follower.memberFacingEndpoint()))
                {
                    memberId = follower.id();

                    final ClusterMember[] clusterMembers = ClusterMember.parse(activeMembers);

                    // TODO: check that leader is in use. If not, close clusterPublication and use leader.

                    timeOfLastActivity = 0;
                    state = State.PASSIVE_FOLLOWER;
                    break;
                }
            }
        }
    }

    public void onSnapshotRecordings(
        final long correlationId, final SnapshotRecordingsDecoder snapshotRecordingsDecoder)
    {
        if (State.PASSIVE_FOLLOWER == state && correlationId == this.correlationId)
        {
            final SnapshotRecordingsDecoder.SnapshotsDecoder snapshotsDecoder = snapshotRecordingsDecoder.snapshots();
            final ArrayList<RecordingLog.Snapshot> snapshots = new ArrayList<>();

            if (snapshotsDecoder.count() > 0)
            {
                for (final SnapshotRecordingsDecoder.SnapshotsDecoder snapshot : snapshotsDecoder)
                {
                    if (snapshot.serviceId() <= ctx.serviceCount())
                    {
                        snapshots.add(new RecordingLog.Snapshot(
                            snapshot.recordingId(),
                            snapshot.leadershipTermId(),
                            snapshot.termBaseLogPosition(),
                            snapshot.logPosition(),
                            snapshot.timestamp(),
                            snapshot.serviceId()));
                    }
                }
            }

            clusterMembers = ClusterMember.parse(snapshotRecordingsDecoder.memberEndpoints());
            highMemberId = ClusterMember.highMemberId(clusterMembers);

            timeOfLastActivity = 0;
            state = State.SNAPSHOT_RETRIEVE;
        }
    }

    private int init(final long nowMs)
    {
        if (nowMs > (timeOfLastActivity + intervalMs))
        {
            correlationId = ctx.aeron().nextCorrelationId();

            if (memberStatusPublisher.addClusterMember(clusterPublication, correlationId, memberEndpoints))
            {
                timeOfLastActivity = nowMs;
                return 1;
            }
        }
        return 0;
    }

    private int passiveFollower(final long nowms)
    {
        if (nowms > (timeOfLastActivity + intervalMs))
        {
            correlationId = ctx.aeron().nextCorrelationId();

            if (memberStatusPublisher.snapshotRecordingQuery(clusterPublication, correlationId, memberId))
            {
                timeOfLastActivity = nowms;
                return 1;
            }
        }
        return 0;
    }

    private int snapshotRetrieve(final long nowMs)
    {
        // TODO: once have snapshots recordings info, then retrieve relevant snapshots, once done, go to load
        return 0;
    }

    private int snapshotLoad(final long nowMs)
    {
        // TODO: perform loading of snapshot and wait for done by the services, once done, go to join
        return 0;
    }

    private int joinCluster(final long nowMs)
    {
        // TODO: send join, create election, and go to done
        return 0;
    }
}
