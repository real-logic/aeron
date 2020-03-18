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
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ControlResponsePoller;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.SnapshotMarkerDecoder;
import io.aeron.cluster.codecs.SnapshotRecordingsDecoder;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.CountedErrorHandler;
import org.agrona.concurrent.status.CountersReader;

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
    private final MemberStatusAdapter memberStatusAdapter;
    private final MemberStatusPublisher memberStatusPublisher;
    private final ConsensusModule.Context ctx;
    private final ConsensusModuleAgent consensusModuleAgent;
    private final String[] clusterMemberStatusEndpoints;
    private final String memberEndpoints;
    private final String memberStatusEndpoint;
    private final String transferEndpoint;
    private final ArrayList<RecordingLog.Snapshot> leaderSnapshots = new ArrayList<>();
    private final Long2LongHashMap leaderSnapshotLengthMap = new Long2LongHashMap(NULL_LENGTH);
    private final long intervalNs;

    private ExclusivePublication memberStatusPublication;
    private State state = State.INIT;
    private ClusterMember[] clusterMembers;
    private ClusterMember leaderMember;
    private AeronArchive.AsyncConnect leaderArchiveAsyncConnect;
    private AeronArchive leaderArchive;
    private Subscription snapshotRetrieveSubscription;
    private Image snapshotRetrieveImage;
    private SnapshotReader snapshotReader;
    private Counter recoveryStateCounter;
    private long timeOfLastActivityNs = 0;
    private long correlationId = NULL_VALUE;
    private int memberId = NULL_VALUE;
    private int clusterMembersStatusEndpointsCursor = NULL_VALUE;
    private int snapshotCursor = 0;
    private int snapshotReplaySessionId = NULL_VALUE;

    DynamicJoin(
        final String clusterMemberStatusEndpoints,
        final AeronArchive localArchive,
        final MemberStatusAdapter memberStatusAdapter,
        final MemberStatusPublisher memberStatusPublisher,
        final ConsensusModule.Context ctx,
        final ConsensusModuleAgent consensusModuleAgent)
    {
        final ClusterMember thisMember = ClusterMember.parseEndpoints(-1, ctx.memberEndpoints());

        this.localArchive = localArchive;
        this.memberStatusAdapter = memberStatusAdapter;
        this.memberStatusPublisher = memberStatusPublisher;
        this.ctx = ctx;
        this.consensusModuleAgent = consensusModuleAgent;
        this.intervalNs = ctx.dynamicJoinIntervalNs();
        this.memberEndpoints = ctx.memberEndpoints();
        this.memberStatusEndpoint = thisMember.memberFacingEndpoint();
        this.transferEndpoint = thisMember.transferEndpoint();
        this.clusterMemberStatusEndpoints = clusterMemberStatusEndpoints.split(",");
    }

    public void close()
    {
        final CountedErrorHandler countedErrorHandler = ctx.countedErrorHandler();
        CloseHelper.close(countedErrorHandler, memberStatusPublication);
        CloseHelper.close(countedErrorHandler, snapshotRetrieveSubscription);
        CloseHelper.close(countedErrorHandler, leaderArchive);
        CloseHelper.close(countedErrorHandler, leaderArchiveAsyncConnect);
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
        workCount += memberStatusAdapter.poll();

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
                if (memberStatusEndpoint.equals(follower.memberFacingEndpoint()))
                {
                    memberId = follower.id();
                    clusterMembers = ClusterMember.parse(activeMembers);
                    leaderMember = ClusterMember.findMember(clusterMembers, leaderMemberId);

                    if (null != leaderMember)
                    {
                        if (!leaderMember.memberFacingEndpoint().equals(
                            clusterMemberStatusEndpoints[clusterMembersStatusEndpointsCursor]))
                        {
                            CloseHelper.close(ctx.countedErrorHandler(), memberStatusPublication);

                            final ChannelUri memberStatusUri = ChannelUri.parse(ctx.memberStatusChannel());
                            memberStatusUri.put(ENDPOINT_PARAM_NAME, leaderMember.memberFacingEndpoint());
                            memberStatusPublication = ctx.aeron().addExclusivePublication(
                                memberStatusUri.toString(), ctx.memberStatusStreamId());
                        }

                        timeOfLastActivityNs = 0;
                        state(State.PASSIVE_FOLLOWER);
                    }

                    break;
                }
            }
        }
    }

    void onSnapshotRecordings(
        final long correlationId, final SnapshotRecordingsDecoder snapshotRecordingsDecoder)
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
                final ChannelUri leaderArchiveUri = ChannelUri.parse(ctx.archiveContext().controlRequestChannel());
                leaderArchiveUri.put(ENDPOINT_PARAM_NAME, leaderMember.archiveEndpoint());

                final AeronArchive.Context leaderArchiveCtx = new AeronArchive.Context()
                    .aeron(ctx.aeron())
                    .controlRequestChannel(leaderArchiveUri.toString())
                    .controlRequestStreamId(ctx.archiveContext().controlRequestStreamId())
                    .controlResponseChannel(ctx.archiveContext().controlResponseChannel())
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
            int cursor = ++clusterMembersStatusEndpointsCursor;
            if (cursor >= clusterMemberStatusEndpoints.length)
            {
                clusterMembersStatusEndpointsCursor = 0;
                cursor = 0;
            }

            CloseHelper.close(ctx.countedErrorHandler(), memberStatusPublication);
            final ChannelUri uri = ChannelUri.parse(ctx.memberStatusChannel());
            uri.put(ENDPOINT_PARAM_NAME, clusterMemberStatusEndpoints[cursor]);
            memberStatusPublication = ctx.aeron().addExclusivePublication(uri.toString(), ctx.memberStatusStreamId());
            correlationId = NULL_VALUE;
            timeOfLastActivityNs = nowNs;

            return 1;
        }
        else if (NULL_VALUE == correlationId && memberStatusPublication.isConnected())
        {
            final long correlationId = ctx.aeron().nextCorrelationId();

            if (memberStatusPublisher.addPassiveMember(memberStatusPublication, correlationId, memberEndpoints))
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

            if (memberStatusPublisher.snapshotRecordingQuery(memberStatusPublication, correlationId, memberId))
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
            final long snapshotStopPosition = (int)leaderArchive.controlResponsePoller().relevantId();

            correlationId = NULL_VALUE;

            if (NULL_POSITION == snapshotStopPosition)
            {
                throw new ClusterException("snapshot stopPosition is NULL_POSITION");
            }

            leaderSnapshotLengthMap.put(snapshotCursor, snapshotStopPosition);
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

        if (null != snapshotReader)
        {
            if (snapshotReader.poll() == 0)
            {
                if (snapshotReader.isDone())
                {
                    consensusModuleAgent.retrievedSnapshot(
                        snapshotReader.recordingId(), leaderSnapshots.get(snapshotCursor));

                    CloseHelper.close(ctx.countedErrorHandler(), snapshotRetrieveSubscription);
                    snapshotRetrieveSubscription = null;
                    snapshotRetrieveImage = null;
                    snapshotReader = null;
                    correlationId = NULL_VALUE;
                    snapshotReplaySessionId = NULL_VALUE;

                    if (++snapshotCursor >= leaderSnapshots.size())
                    {
                        state(State.SNAPSHOT_LOAD);
                        workCount++;
                    }
                }
                else if (null != snapshotRetrieveImage && snapshotRetrieveImage.isClosed())
                {
                    throw new ClusterException("retrieval of snapshot image ended unexpectedly");
                }
            }
            else
            {
                workCount++;
            }
        }
        else if (null == snapshotRetrieveImage && null != snapshotRetrieveSubscription)
        {
            snapshotRetrieveImage = snapshotRetrieveSubscription.imageBySessionId(snapshotReplaySessionId);
            if (null != snapshotRetrieveImage)
            {
                snapshotReader = new SnapshotReader(
                    snapshotRetrieveImage, ctx.aeron().countersReader(), leaderSnapshotLengthMap.get(snapshotCursor));
                workCount++;
            }
        }
        else if (NULL_VALUE == correlationId)
        {
            final long replayId = ctx.aeron().nextCorrelationId();
            final RecordingLog.Snapshot snapshot = leaderSnapshots.get(snapshotCursor);
            final String transferChannel = "aeron:udp?endpoint=" + transferEndpoint;

            if (leaderArchive.archiveProxy().replay(
                snapshot.recordingId,
                0,
                NULL_LENGTH,
                transferChannel,
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
            snapshotReplaySessionId = (int)leaderArchive.controlResponsePoller().relevantId();
            final String replaySubscriptionChannel =
                "aeron:udp?endpoint=" + transferEndpoint + "|session-id=" + snapshotReplaySessionId;

            snapshotRetrieveSubscription = ctx.aeron().addSubscription(replaySubscriptionChannel, ctx.replayStreamId());
            localArchive.startRecording(replaySubscriptionChannel, ctx.replayStreamId(), SourceLocation.REMOTE, true);
            workCount++;
        }

        return workCount;
    }

    private int snapshotLoad(final long nowNs)
    {
        int workCount = 0;

        if (null == recoveryStateCounter)
        {
            recoveryStateCounter = consensusModuleAgent.loadSnapshotsFromDynamicJoin();
            workCount++;
        }
        else if (consensusModuleAgent.pollForEndOfSnapshotLoad(recoveryStateCounter, nowNs))
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

        if (memberStatusPublisher.joinCluster(memberStatusPublication, leadershipTermId, memberId))
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

    static class SnapshotReader implements ControlledFragmentHandler
    {
        private static final int FRAGMENT_LIMIT = 10;

        private final long endPosition;
        private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        private final SnapshotMarkerDecoder snapshotMarkerDecoder = new SnapshotMarkerDecoder();
        private final CountersReader countersReader;
        private final Image image;
        private long recordingId = RecordingPos.NULL_RECORDING_ID;
        private long recordingPosition = NULL_POSITION;
        private int counterId;
        private boolean inSnapshot = false;
        private boolean inHeader = false;

        SnapshotReader(final Image image, final CountersReader countersReader, final long endPosition)
        {
            this.countersReader = countersReader;
            this.image = image;
            this.counterId = RecordingPos.findCounterIdBySession(countersReader, image.sessionId());
            this.endPosition = endPosition;
        }

        boolean isDone()
        {
            return endPosition <= recordingPosition;
        }

        long recordingId()
        {
            return recordingId;
        }

        void pollRecordingPosition()
        {
            if (CountersReader.NULL_COUNTER_ID == counterId)
            {
                counterId = RecordingPos.findCounterIdBySession(countersReader, image.sessionId());
            }
            else if (RecordingPos.NULL_RECORDING_ID == recordingId)
            {
                recordingId = RecordingPos.getRecordingId(countersReader, counterId);
            }
            else
            {
                recordingPosition = countersReader.getCounterValue(counterId);
            }
        }

        int poll()
        {
            pollRecordingPosition();

            return image.controlledPoll(this, FRAGMENT_LIMIT);
        }

        public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            if (inHeader)
            {
                messageHeaderDecoder.wrap(buffer, offset);

                if (messageHeaderDecoder.templateId() == SnapshotMarkerDecoder.TEMPLATE_ID)
                {
                    snapshotMarkerDecoder.wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.blockLength(),
                        messageHeaderDecoder.version());

                    final long typeId = snapshotMarkerDecoder.typeId();
                    if (typeId != ConsensusModule.Configuration.SNAPSHOT_TYPE_ID &&
                        typeId != ClusteredServiceContainer.SNAPSHOT_TYPE_ID)
                    {
                        throw new ClusterException("unexpected snapshot type: " + typeId);
                    }

                    switch (snapshotMarkerDecoder.mark())
                    {
                        case BEGIN:
                            if (inSnapshot)
                            {
                                throw new ClusterException("already in snapshot");
                            }
                            inSnapshot = true;
                            inHeader = true;
                            return Action.CONTINUE;

                        case END:
                            if (!inSnapshot)
                            {
                                throw new ClusterException("missing begin snapshot");
                            }
                            inHeader = false;
                            return Action.CONTINUE;
                    }
                }
            }

            return ControlledFragmentHandler.Action.CONTINUE;
        }
    }
}
