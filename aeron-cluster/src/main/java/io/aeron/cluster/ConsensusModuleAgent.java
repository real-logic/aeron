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
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.client.RecordingSignalPoller;
import io.aeron.archive.codecs.*;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.*;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.service.*;
import io.aeron.exceptions.AeronException;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.security.Authenticator;
import io.aeron.status.ReadableCounter;
import org.agrona.*;
import org.agrona.collections.*;
import org.agrona.concurrent.*;
import org.agrona.concurrent.status.CountersReader;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.CommonContext.*;
import static io.aeron.archive.client.AeronArchive.NULL_LENGTH;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.client.ReplayMerge.LIVE_ADD_MAX_WINDOW;
import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static io.aeron.cluster.ClusterMember.quorumPosition;
import static io.aeron.cluster.ClusterSession.State.*;
import static io.aeron.cluster.ConsensusModule.Configuration.*;
import static io.aeron.cluster.client.AeronCluster.SESSION_HEADER_LENGTH;
import static io.aeron.cluster.service.ClusteredServiceContainer.Configuration.MARK_FILE_UPDATE_INTERVAL_NS;
import static io.aeron.exceptions.AeronException.Category.WARN;
import static java.lang.Math.min;
import static org.agrona.BitUtil.findNextPositivePowerOfTwo;

final class ConsensusModuleAgent implements Agent
{
    static final long SLOW_TICK_INTERVAL_NS = TimeUnit.MILLISECONDS.toNanos(10);
    private static final int SERVICE_MESSAGE_LIMIT = 20;

    private final long sessionTimeoutNs;
    private final long leaderHeartbeatIntervalNs;
    private final long leaderHeartbeatTimeoutNs;
    private long unavailableCounterHandlerRegistrationId;
    private long nextSessionId = 1;
    private long nextServiceSessionId = Long.MIN_VALUE + 1;
    private long logServiceSessionId = Long.MIN_VALUE;
    private long leadershipTermId = NULL_VALUE;
    private long replayLeadershipTermId = NULL_VALUE;
    private long expectedAckPosition = 0;
    private long serviceAckId = 0;
    private long terminationPosition = NULL_POSITION;
    private long notifiedCommitPosition = 0;
    private long lastAppendPosition = 0;
    private long timeOfLastLogUpdateNs = 0;
    private long timeOfLastAppendPositionNs = 0;
    private long slowTickDeadlineNs = 0;
    private long markFileUpdateDeadlineNs = 0;
    private int pendingServiceMessageHeadOffset = 0;
    private int uncommittedServiceMessages = 0;
    private int memberId;
    private int highMemberId;
    private int pendingMemberRemovals = 0;
    private long logPublicationChannelTag;
    private ReadableCounter appendPosition = null;
    private final Counter commitPosition;
    private ConsensusModule.State state = ConsensusModule.State.INIT;
    private Cluster.Role role = Cluster.Role.FOLLOWER;
    private ClusterMember[] activeMembers;
    private ClusterMember[] passiveMembers = ClusterMember.EMPTY_MEMBERS;
    private ClusterMember leaderMember;
    private ClusterMember thisMember;
    private long[] rankedPositions;
    private final long[] serviceClientIds;
    private final ArrayDeque<ServiceAck>[] serviceAckQueues;
    private final Counter clusterRoleCounter;
    private final ClusterMarkFile markFile;
    private final AgentInvoker aeronClientInvoker;
    private final ClusterClock clusterClock;
    private final TimeUnit clusterTimeUnit;
    private final Counter moduleState;
    private final Counter controlToggle;
    private final TimerService timerService;
    private final ConsensusModuleAdapter consensusModuleAdapter;
    private final ServiceProxy serviceProxy;
    private final IngressAdapter ingressAdapter;
    private final EgressPublisher egressPublisher;
    private final LogPublisher logPublisher;
    private final LogAdapter logAdapter;
    private final ConsensusAdapter consensusAdapter;
    private final ConsensusPublisher consensusPublisher = new ConsensusPublisher();
    private final Long2ObjectHashMap<ClusterSession> sessionByIdMap = new Long2ObjectHashMap<>();
    private final ArrayList<ClusterSession> pendingSessions = new ArrayList<>();
    private final ArrayList<ClusterSession> rejectedSessions = new ArrayList<>();
    private final ArrayList<ClusterSession> redirectSessions = new ArrayList<>();
    private final Int2ObjectHashMap<ClusterMember> clusterMemberByIdMap = new Int2ObjectHashMap<>();
    private final Long2LongCounterMap expiredTimerCountByCorrelationIdMap = new Long2LongCounterMap(0);
    private final ArrayDeque<ClusterSession> uncommittedClosedSessions = new ArrayDeque<>();
    private final LongArrayQueue uncommittedTimers = new LongArrayQueue(Long.MAX_VALUE);
    private final ExpandableRingBuffer pendingServiceMessages = new ExpandableRingBuffer();
    private final ExpandableRingBuffer.MessageConsumer serviceSessionMessageAppender =
        this::serviceSessionMessageAppender;
    private final ExpandableRingBuffer.MessageConsumer leaderServiceSessionMessageSweeper =
        this::leaderServiceSessionMessageSweeper;
    private final ExpandableRingBuffer.MessageConsumer followerServiceSessionMessageSweeper =
        this::followerServiceSessionMessageSweeper;
    private final Authenticator authenticator;
    private final ClusterSessionProxy sessionProxy;
    private final Aeron aeron;
    private final ConsensusModule.Context ctx;
    private final IdleStrategy idleStrategy;
    private final RecordingLog recordingLog;
    private final ArrayList<RecordingLog.Snapshot> dynamicJoinSnapshots = new ArrayList<>();
    private RecordingLog.RecoveryPlan recoveryPlan;
    private AeronArchive archive;
    private RecordingSignalPoller recordingSignalPoller;
    private Election election;
    private DynamicJoin dynamicJoin;
    private ClusterTermination clusterTermination;
    private long logSubscriptionId = NULL_VALUE;
    private long logRecordingId = NULL_VALUE;
    private long logRecordedPosition = NULL_POSITION;
    private String liveLogDestination;
    private String catchupLogDestination;
    private String ingressEndpoints;

    ConsensusModuleAgent(final ConsensusModule.Context ctx)
    {
        this.ctx = ctx;
        this.aeron = ctx.aeron();
        this.clusterClock = ctx.clusterClock();
        this.clusterTimeUnit = clusterClock.timeUnit();
        this.sessionTimeoutNs = ctx.sessionTimeoutNs();
        this.leaderHeartbeatIntervalNs = ctx.leaderHeartbeatIntervalNs();
        this.leaderHeartbeatTimeoutNs = ctx.leaderHeartbeatTimeoutNs();
        this.egressPublisher = ctx.egressPublisher();
        this.moduleState = ctx.moduleStateCounter();
        this.commitPosition = ctx.commitPositionCounter();
        this.controlToggle = ctx.controlToggleCounter();
        this.logPublisher = ctx.logPublisher();
        this.idleStrategy = ctx.idleStrategy();
        this.timerService = new TimerService(
            this,
            clusterTimeUnit,
            0,
            findNextPositivePowerOfTwo(clusterTimeUnit.convert(ctx.wheelTickResolutionNs(), TimeUnit.NANOSECONDS)),
            ctx.ticksPerWheel());
        this.activeMembers = ClusterMember.parse(ctx.clusterMembers());
        this.sessionProxy = new ClusterSessionProxy(egressPublisher);
        this.memberId = ctx.clusterMemberId();
        this.clusterRoleCounter = ctx.clusterNodeRoleCounter();
        this.markFile = ctx.clusterMarkFile();
        this.recordingLog = ctx.recordingLog();
        this.serviceClientIds = new long[ctx.serviceCount()];
        Arrays.fill(serviceClientIds, NULL_VALUE);
        this.serviceAckQueues = ServiceAck.newArray(ctx.serviceCount());
        this.highMemberId = ClusterMember.highMemberId(activeMembers);

        aeronClientInvoker = aeron.conductorAgentInvoker();
        aeronClientInvoker.invoke();

        rankedPositions = new long[ClusterMember.quorumThreshold(activeMembers.length)];
        role(Cluster.Role.FOLLOWER);

        ClusterMember.addClusterMemberIds(activeMembers, clusterMemberByIdMap);
        thisMember = ClusterMember.determineMember(activeMembers, ctx.clusterMemberId(), ctx.memberEndpoints());
        leaderMember = thisMember;

        final ChannelUri consensusUri = ChannelUri.parse(ctx.consensusChannel());
        if (!consensusUri.containsKey(ENDPOINT_PARAM_NAME))
        {
            consensusUri.put(ENDPOINT_PARAM_NAME, thisMember.consensusEndpoint());
        }

        consensusAdapter = new ConsensusAdapter(
            aeron.addSubscription(consensusUri.toString(), ctx.consensusStreamId()), this);

        ClusterMember.addConsensusPublications(activeMembers, thisMember, consensusUri, ctx.consensusStreamId(), aeron);

        ingressAdapter = new IngressAdapter(ctx.ingressFragmentLimit(), this);
        logAdapter = new LogAdapter(this, ctx.logFragmentLimit());

        consensusModuleAdapter = new ConsensusModuleAdapter(
            aeron.addSubscription(ctx.controlChannel(), ctx.consensusModuleStreamId()), this);
        serviceProxy = new ServiceProxy(aeron.addPublication(ctx.controlChannel(), ctx.serviceStreamId()));

        authenticator = ctx.authenticatorSupplier().get();
    }

    public void onClose()
    {
        if (!aeron.isClosed())
        {
            aeron.removeUnavailableCounterHandler(unavailableCounterHandlerRegistrationId);
            tryStopLogRecording();

            if (!ctx.ownsAeronClient())
            {
                logPublisher.disconnect(ctx.countedErrorHandler());
                logAdapter.disconnect(ctx.countedErrorHandler());

                final CountedErrorHandler errorHandler = ctx.countedErrorHandler();
                for (final ClusterSession session : sessionByIdMap.values())
                {
                    session.close(errorHandler);
                }

                CloseHelper.close(errorHandler, ingressAdapter);
                ClusterMember.closeConsensusPublications(errorHandler, activeMembers);
                CloseHelper.close(errorHandler, consensusAdapter);
                CloseHelper.close(errorHandler, serviceProxy);
                CloseHelper.close(errorHandler, consensusModuleAdapter);
                CloseHelper.close(errorHandler, archive);
            }

            state(ConsensusModule.State.CLOSED);
        }

        ctx.close();
    }

    public void onStart()
    {
        archive = AeronArchive.connect(ctx.archiveContext().clone());
        recordingSignalPoller = new RecordingSignalPoller(
            archive.controlSessionId(), archive.controlResponsePoller().subscription());

        if (null == (dynamicJoin = requiresDynamicJoin()))
        {
            final long lastTermRecordingId = recordingLog.findLastTermRecordingId();
            if (NULL_VALUE != lastTermRecordingId)
            {
                archive.tryStopRecordingByIdentity(lastTermRecordingId);
            }

            recoveryPlan = recordingLog.createRecoveryPlan(archive, ctx.serviceCount());
            if (null != recoveryPlan.log)
            {
                logRecordingId = recoveryPlan.log.recordingId;
            }

            try (Counter ignore = addRecoveryStateCounter(recoveryPlan))
            {
                if (!recoveryPlan.snapshots.isEmpty())
                {
                    loadSnapshot(recoveryPlan.snapshots.get(0), archive);
                }

                while (!ServiceAck.hasReached(expectedAckPosition, serviceAckId, serviceAckQueues))
                {
                    idle(consensusModuleAdapter.poll());
                }

                captureServiceClientIds();
                ++serviceAckId;
            }

            election = new Election(
                true,
                recoveryPlan.lastLeadershipTermId,
                commitPosition.getWeak(),
                recoveryPlan.appendedLogPosition,
                activeMembers,
                clusterMemberByIdMap,
                thisMember,
                consensusPublisher,
                ctx,
                this);

            state(ConsensusModule.State.ACTIVE);
        }

        unavailableCounterHandlerRegistrationId = aeron.addUnavailableCounterHandler(this::onUnavailableCounter);
    }

    public int doWork()
    {
        int workCount = 0;

        final long now = clusterClock.time();
        final long nowNs = clusterTimeUnit.toNanos(now);

        if (nowNs >= slowTickDeadlineNs)
        {
            slowTickDeadlineNs = nowNs + SLOW_TICK_INTERVAL_NS;
            workCount += slowTickWork(clusterTimeUnit.toMillis(now), nowNs);
        }

        workCount += consensusAdapter.poll();

        if (null != dynamicJoin)
        {
            workCount += dynamicJoin.doWork(nowNs);
        }
        else if (null != election)
        {
            workCount += election.doWork(nowNs);
        }
        else
        {
            workCount += consensusWork(now, nowNs);
        }

        return workCount;
    }

    public String roleName()
    {
        return "consensus-module_" + ctx.clusterId() + "_" + memberId;
    }

    void onSessionConnect(
        final long correlationId,
        final int responseStreamId,
        final int version,
        final String responseChannel,
        final byte[] encodedCredentials)
    {
        final long clusterSessionId = Cluster.Role.LEADER == role ? nextSessionId++ : NULL_VALUE;
        final ClusterSession session = new ClusterSession(clusterSessionId, responseStreamId, responseChannel);
        session.connect(aeron);

        final long now = clusterClock.time();
        session.lastActivityNs(clusterTimeUnit.toNanos(now), correlationId);

        if (Cluster.Role.LEADER != role)
        {
            redirectSessions.add(session);
        }
        else
        {
            if (AeronCluster.Configuration.PROTOCOL_MAJOR_VERSION != SemanticVersion.major(version))
            {
                final String detail = SESSION_INVALID_VERSION_MSG + " " + SemanticVersion.toString(version) +
                    ", cluster is " + SemanticVersion.toString(AeronCluster.Configuration.PROTOCOL_SEMANTIC_VERSION);
                session.reject(EventCode.ERROR, detail);
                rejectedSessions.add(session);
            }
            else if (pendingSessions.size() + sessionByIdMap.size() >= ctx.maxConcurrentSessions())
            {
                session.reject(EventCode.ERROR, SESSION_LIMIT_MSG);
                rejectedSessions.add(session);
            }
            else
            {
                authenticator.onConnectRequest(session.id(), encodedCredentials, clusterTimeUnit.toMillis(now));
                pendingSessions.add(session);
            }
        }
    }

    void onSessionClose(final long leadershipTermId, final long clusterSessionId)
    {
        if (leadershipTermId == this.leadershipTermId && Cluster.Role.LEADER == role)
        {
            final ClusterSession session = sessionByIdMap.get(clusterSessionId);
            if (null != session && session.state() == OPEN)
            {
                session.closing(CloseReason.CLIENT_ACTION);
                session.disconnect(ctx.countedErrorHandler());

                if (logPublisher.appendSessionClose(session, leadershipTermId, clusterClock.time()))
                {
                    session.closedLogPosition(logPublisher.position());
                    uncommittedClosedSessions.addLast(session);
                    sessionByIdMap.remove(clusterSessionId);
                    session.close(ctx.countedErrorHandler());
                }
            }
        }
    }

    ControlledFragmentAssembler.Action onIngressMessage(
        final long leadershipTermId,
        final long clusterSessionId,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        if (leadershipTermId == this.leadershipTermId && Cluster.Role.LEADER == role)
        {
            final ClusterSession session = sessionByIdMap.get(clusterSessionId);
            if (null != session && session.state() == OPEN)
            {
                final long now = clusterClock.time();
                if (logPublisher.appendMessage(leadershipTermId, clusterSessionId, now, buffer, offset, length) > 0)
                {
                    session.timeOfLastActivityNs(clusterTimeUnit.toNanos(now));
                    return ControlledFragmentHandler.Action.CONTINUE;
                }
                else
                {
                    return ControlledFragmentHandler.Action.ABORT;
                }
            }
        }

        return ControlledFragmentHandler.Action.CONTINUE;
    }

    void onSessionKeepAlive(final long leadershipTermId, final long clusterSessionId)
    {
        if (leadershipTermId == this.leadershipTermId && Cluster.Role.LEADER == role)
        {
            final ClusterSession session = sessionByIdMap.get(clusterSessionId);
            if (null != session && session.state() == OPEN)
            {
                session.timeOfLastActivityNs(clusterTimeUnit.toNanos(clusterClock.time()));
            }
        }
    }

    void onChallengeResponse(final long correlationId, final long clusterSessionId, final byte[] encodedCredentials)
    {
        if (Cluster.Role.LEADER == role)
        {
            for (int lastIndex = pendingSessions.size() - 1, i = lastIndex; i >= 0; i--)
            {
                final ClusterSession session = pendingSessions.get(i);

                if (session.id() == clusterSessionId && session.state() == CHALLENGED)
                {
                    final long now = clusterClock.time();
                    final long nowMs = clusterTimeUnit.toMillis(now);
                    session.lastActivityNs(clusterTimeUnit.toNanos(now), correlationId);
                    authenticator.onChallengeResponse(clusterSessionId, encodedCredentials, nowMs);
                    break;
                }
            }
        }
    }

    boolean onTimerEvent(final long correlationId)
    {
        final long appendPosition = logPublisher.appendTimer(correlationId, leadershipTermId, clusterClock.time());
        if (appendPosition > 0)
        {
            uncommittedTimers.offerLong(appendPosition);
            uncommittedTimers.offerLong(correlationId);
            return true;
        }

        return false;
    }

    void onCanvassPosition(final long logLeadershipTermId, final long logPosition, final int followerMemberId)
    {
        if (null != election)
        {
            election.onCanvassPosition(logLeadershipTermId, logPosition, followerMemberId);
        }
        else if (Cluster.Role.LEADER == role)
        {
            final ClusterMember follower = clusterMemberByIdMap.get(followerMemberId);
            if (null != follower && logLeadershipTermId <= leadershipTermId)
            {
                final RecordingLog.Entry termEntry = recordingLog.findTermEntry(
                    logLeadershipTermId < leadershipTermId ? logLeadershipTermId + 1 : logLeadershipTermId);
                if (null != termEntry)
                {
                    final long appendPosition = logPublisher.position();
                    consensusPublisher.newLeadershipTerm(
                        follower.publication(),
                        logLeadershipTermId,
                        logLeadershipTermId < leadershipTermId ? termEntry.termBaseLogPosition : appendPosition,
                        leadershipTermId,
                        appendPosition,
                        termEntry.timestamp,
                        memberId,
                        logPublisher.sessionId(),
                        false);
                }
            }
        }
    }

    void onRequestVote(
        final long logLeadershipTermId, final long logPosition, final long candidateTermId, final int candidateId)
    {
        if (null != election)
        {
            election.onRequestVote(logLeadershipTermId, logPosition, candidateTermId, candidateId);
        }
        else if (candidateTermId > leadershipTermId && null == dynamicJoin)
        {
            ctx.countedErrorHandler().onError(new ClusterException("unexpected vote request", WARN));
            enterElection();
        }
    }

    void onVote(
        final long candidateTermId,
        final long logLeadershipTermId,
        final long logPosition,
        final int candidateMemberId,
        final int followerMemberId,
        final boolean vote)
    {
        if (null != election)
        {
            election.onVote(
                candidateTermId, logLeadershipTermId, logPosition, candidateMemberId, followerMemberId, vote);
        }
    }

    void onNewLeadershipTerm(
        final long logLeadershipTermId,
        final long logTruncatePosition,
        final long leadershipTermId,
        final long logPosition,
        final long timestamp,
        final int leaderId,
        final int logSessionId,
        final boolean isStartup)
    {
        if (null != election)
        {
            election.onNewLeadershipTerm(
                logLeadershipTermId,
                logTruncatePosition,
                leadershipTermId,
                logPosition,
                timestamp,
                leaderId,
                logSessionId,
                isStartup);
        }
        else if (Cluster.Role.FOLLOWER == role &&
            leadershipTermId == this.leadershipTermId &&
            leaderId == leaderMember.id())
        {
            notifiedCommitPosition = Math.max(notifiedCommitPosition, logPosition);
            timeOfLastLogUpdateNs = clusterClock.timeNanos();
        }
        else if (leadershipTermId > this.leadershipTermId && null == dynamicJoin)
        {
            ctx.countedErrorHandler().onError(new ClusterException("unexpected new leadership term", WARN));
            enterElection();
        }
    }

    void onAppendPosition(final long leadershipTermId, final long logPosition, final int followerMemberId)
    {
        if (null != election)
        {
            election.onAppendPosition(leadershipTermId, logPosition, followerMemberId);
        }
        else if (leadershipTermId <= this.leadershipTermId && Cluster.Role.LEADER == role)
        {
            final ClusterMember follower = clusterMemberByIdMap.get(followerMemberId);
            if (null != follower)
            {
                follower
                    .logPosition(logPosition)
                    .timeOfLastAppendPositionNs(clusterClock.timeNanos());
                trackCatchupCompletion(follower, leadershipTermId);
            }
        }
    }

    void onCommitPosition(final long leadershipTermId, final long logPosition, final int leaderMemberId)
    {
        if (null != election)
        {
            election.onCommitPosition(leadershipTermId, logPosition, leaderMemberId);
        }
        else if (leadershipTermId == this.leadershipTermId &&
            leaderMemberId == leaderMember.id() &&
            Cluster.Role.FOLLOWER == role)
        {
            notifiedCommitPosition = logPosition;
            timeOfLastLogUpdateNs = clusterClock.timeNanos();
        }
        else if (leadershipTermId > this.leadershipTermId && null == dynamicJoin)
        {
            ctx.countedErrorHandler().onError(new ClusterException("unexpected commit position", WARN));
            enterElection();
        }
    }

    void onCatchupPosition(
        final long leadershipTermId, final long logPosition, final int followerMemberId, final String catchupEndpoint)
    {
        if (leadershipTermId <= this.leadershipTermId && Cluster.Role.LEADER == role)
        {
            final ClusterMember follower = clusterMemberByIdMap.get(followerMemberId);
            if (null != follower && follower.catchupReplaySessionId() == NULL_VALUE)
            {
                final String channel = new ChannelUriStringBuilder()
                    .media(CommonContext.UDP_MEDIA)
                    .endpoint(catchupEndpoint)
                    .sessionId(logPublisher.sessionId())
                    .linger(0L)
                    .eos(Boolean.FALSE)
                    .build();

                follower.catchupReplaySessionId(archive.startReplay(
                    logRecordingId, logPosition, Long.MAX_VALUE, channel, ctx.logStreamId()));
                follower.catchupReplayCorrelationId(archive.lastCorrelationId());
            }
        }
    }

    void onStopCatchup(final long leadershipTermId, final int followerMemberId)
    {
        if (leadershipTermId == this.replayLeadershipTermId && followerMemberId == memberId)
        {
            if (null != catchupLogDestination)
            {
                logAdapter.removeDestination(catchupLogDestination);
                catchupLogDestination = null;
            }
        }
    }

    void onAddPassiveMember(final long correlationId, final String memberEndpoints)
    {
        if (null == election && null == dynamicJoin)
        {
            if (Cluster.Role.LEADER == role)
            {
                if (ClusterMember.notDuplicateEndpoint(passiveMembers, memberEndpoints))
                {
                    final ClusterMember newMember = ClusterMember.parseEndpoints(++highMemberId, memberEndpoints);

                    newMember.correlationId(correlationId);
                    passiveMembers = ClusterMember.addMember(passiveMembers, newMember);
                    clusterMemberByIdMap.put(newMember.id(), newMember);

                    ClusterMember.addConsensusPublication(
                        newMember, ChannelUri.parse(ctx.consensusChannel()), ctx.consensusStreamId(), aeron);
                    logPublisher.addDestination(ctx.isLogMdc(), newMember.logEndpoint());
                }
            }
            else if (Cluster.Role.FOLLOWER == role)
            {
                consensusPublisher.addPassiveMember(leaderMember.publication(), correlationId, memberEndpoints);
            }
        }
    }

    void onClusterMembersChange(
        final long correlationId, final int leaderMemberId, final String activeMembers, final String passiveMembers)
    {
        if (null != dynamicJoin)
        {
            dynamicJoin.onClusterMembersChange(correlationId, leaderMemberId, activeMembers, passiveMembers);
        }
    }

    void onSnapshotRecordingQuery(final long correlationId, final int requestMemberId)
    {
        if (null == election && Cluster.Role.LEADER == role)
        {
            final ClusterMember requester = clusterMemberByIdMap.get(requestMemberId);
            if (null != requester)
            {
                consensusPublisher.snapshotRecording(
                    requester.publication(),
                    correlationId,
                    recoveryPlan,
                    ClusterMember.encodeAsString(activeMembers));
            }
        }
    }

    void onSnapshotRecordings(final long correlationId, final SnapshotRecordingsDecoder decoder)
    {
        if (null != dynamicJoin)
        {
            dynamicJoin.onSnapshotRecordings(correlationId, decoder);
        }
    }

    void onJoinCluster(final long leadershipTermId, final int memberId)
    {
        if (null == election && Cluster.Role.LEADER == role)
        {
            final ClusterMember member = clusterMemberByIdMap.get(memberId);
            final long snapshotLeadershipTermId = recoveryPlan.snapshots.isEmpty() ?
                NULL_VALUE : recoveryPlan.snapshots.get(0).leadershipTermId;

            if (null != member && !member.hasRequestedJoin() && leadershipTermId <= snapshotLeadershipTermId)
            {
                if (null == member.publication())
                {
                    final ChannelUri consensusUri = ChannelUri.parse(ctx.consensusChannel());
                    final int streamId = ctx.consensusStreamId();
                    ClusterMember.addConsensusPublication(member, consensusUri, streamId, aeron);
                    logPublisher.addDestination(ctx.isLogMdc(), member.logEndpoint());
                }

                member.hasRequestedJoin(true);
            }
        }
    }

    void onTerminationPosition(final long leadershipTermId, final long logPosition)
    {
        if (leadershipTermId == this.leadershipTermId && Cluster.Role.FOLLOWER == role)
        {
            terminationPosition = logPosition;
            timeOfLastLogUpdateNs = clusterClock.timeNanos();
        }
    }

    void onTerminationAck(final long leadershipTermId, final long logPosition, final int memberId)
    {
        if (leadershipTermId == this.leadershipTermId &&
            logPosition >= terminationPosition &&
            Cluster.Role.LEADER == role)
        {
            final ClusterMember member = clusterMemberByIdMap.get(memberId);
            if (null != member)
            {
                member.hasTerminated(true);

                if (clusterTermination.canTerminate(activeMembers, terminationPosition, clusterClock.timeNanos()))
                {
                    recordingLog.commitLogPosition(leadershipTermId, terminationPosition);
                    closeAndTerminate();
                }
            }
        }
    }

    void onBackupQuery(
        final long correlationId,
        final int responseStreamId,
        final int version,
        final String responseChannel,
        final byte[] encodedCredentials)
    {
        if (null == election && null == dynamicJoin)
        {
            if (Cluster.Role.LEADER != role)
            {
                consensusPublisher.backupQuery(
                    leaderMember.publication(),
                    correlationId,
                    responseStreamId,
                    version,
                    responseChannel,
                    encodedCredentials);
            }
            else if (state == ConsensusModule.State.ACTIVE || state == ConsensusModule.State.SUSPENDED)
            {
                final ClusterSession session = new ClusterSession(NULL_VALUE, responseStreamId, responseChannel);
                final long now = clusterClock.time();
                session.lastActivityNs(clusterTimeUnit.toNanos(now), correlationId);
                session.markAsBackupSession();
                session.connect(aeron);

                if (AeronCluster.Configuration.PROTOCOL_MAJOR_VERSION != SemanticVersion.major(version))
                {
                    final String detail = SESSION_INVALID_VERSION_MSG + " " + SemanticVersion.toString(version) +
                        ", cluster=" + SemanticVersion.toString(AeronCluster.Configuration.PROTOCOL_SEMANTIC_VERSION);
                    session.reject(EventCode.ERROR, detail);
                    rejectedSessions.add(session);
                }
                else if (pendingSessions.size() + sessionByIdMap.size() >= ctx.maxConcurrentSessions())
                {
                    session.reject(EventCode.ERROR, SESSION_LIMIT_MSG);
                    rejectedSessions.add(session);
                }
                else
                {
                    authenticator.onConnectRequest(session.id(), encodedCredentials, clusterTimeUnit.toMillis(now));
                    pendingSessions.add(session);
                }
            }
        }
    }

    void onRemoveMember(final int memberId, final boolean isPassive)
    {
        if (null == election && Cluster.Role.LEADER == role)
        {
            final ClusterMember member = clusterMemberByIdMap.get(memberId);
            if (null != member)
            {
                if (isPassive)
                {
                    passiveMembers = ClusterMember.removeMember(passiveMembers, memberId);
                    member.closePublication(ctx.countedErrorHandler());

                    logPublisher.removeDestination(ctx.isLogMdc(), member.logEndpoint());

                    clusterMemberByIdMap.remove(memberId);
                    clusterMemberByIdMap.compact();
                }
                else
                {
                    final long now = clusterClock.time();
                    final long position = logPublisher.appendMembershipChangeEvent(
                        leadershipTermId,
                        now,
                        this.memberId,
                        activeMembers.length,
                        ChangeType.QUIT,
                        memberId,
                        ClusterMember.encodeAsString(ClusterMember.removeMember(activeMembers, memberId)));

                    if (position > 0)
                    {
                        timeOfLastLogUpdateNs = clusterTimeUnit.toNanos(now) - leaderHeartbeatIntervalNs;
                        member.removalPosition(position);
                        pendingMemberRemovals++;
                    }
                }
            }
        }
    }

    void onClusterMembersQuery(final long correlationId, final boolean isExtendedRequest)
    {
        if (isExtendedRequest)
        {
            serviceProxy.clusterMembersExtendedResponse(
                correlationId, clusterClock.timeNanos(), leaderMember.id(), memberId, activeMembers, passiveMembers);
        }
        else
        {
            serviceProxy.clusterMembersResponse(
                correlationId,
                leaderMember.id(),
                ClusterMember.encodeAsString(activeMembers),
                ClusterMember.encodeAsString(passiveMembers));
        }
    }

    void state(final ConsensusModule.State newState)
    {
        if (newState != state)
        {
            stateChange(state, newState, memberId);
            state = newState;
            if (!moduleState.isClosed())
            {
                moduleState.set(newState.code());
            }
        }
    }

    ConsensusModule.State state()
    {
        return state;
    }

    void stateChange(final ConsensusModule.State oldState, final ConsensusModule.State newState, final int memberId)
    {
        //System.out.println("CM State memberId=" + memberId + " " + oldState + " -> " + newState);
    }

    void role(final Cluster.Role newRole)
    {
        if (newRole != role)
        {
            roleChange(role, newRole, memberId);
            role = newRole;
            if (!clusterRoleCounter.isClosed())
            {
                clusterRoleCounter.set(newRole.code());
            }
        }
    }

    void roleChange(final Cluster.Role oldRole, final Cluster.Role newRole, final int memberId)
    {
        //System.out.println("CM Role memberId=" + memberId + " " + oldRole + " -> " + newRole);
    }

    Cluster.Role role()
    {
        return role;
    }

    long prepareForNewLeadership(final long logPosition)
    {
        long appendPosition = 0;

        CloseHelper.close(ctx.countedErrorHandler(), ingressAdapter);
        ClusterControl.ToggleState.deactivate(controlToggle);

        if (RecordingPos.NULL_RECORDING_ID != logRecordingId)
        {
            if (null != catchupLogDestination)
            {
                logAdapter.removeDestination(catchupLogDestination);
                catchupLogDestination = null;
            }

            if (null != liveLogDestination)
            {
                logAdapter.removeDestination(liveLogDestination);
                liveLogDestination = null;
            }

            logAdapter.disconnect(ctx.countedErrorHandler());
            logPublisher.disconnect(ctx.countedErrorHandler());

            tryStopLogRecording();

            idleStrategy.reset();
            while (AeronArchive.NULL_POSITION == (appendPosition = archive.getStopPosition(logRecordingId)))
            {
                idle();
            }
            lastAppendPosition = appendPosition;

            recoveryPlan = recordingLog.createRecoveryPlan(archive, ctx.serviceCount());

            clearSessionsAfter(logPosition);
            for (final ClusterSession session : sessionByIdMap.values())
            {
                session.disconnect(ctx.countedErrorHandler());
            }

            commitPosition.setOrdered(logPosition);
            restoreUncommittedEntries(logPosition);
        }

        return appendPosition;
    }

    void onServiceCloseSession(final long clusterSessionId)
    {
        final ClusterSession session = sessionByIdMap.get(clusterSessionId);
        if (null != session)
        {
            session.closing(CloseReason.SERVICE_ACTION);

            if (Cluster.Role.LEADER == role &&
                logPublisher.appendSessionClose(session, leadershipTermId, clusterClock.time()))
            {
                final String msg = CloseReason.SERVICE_ACTION.name();
                egressPublisher.sendEvent(session, leadershipTermId, memberId, EventCode.CLOSED, msg);
                session.closedLogPosition(logPublisher.position());
                uncommittedClosedSessions.addLast(session);
                sessionByIdMap.remove(clusterSessionId);
                session.close(ctx.countedErrorHandler());
            }
        }
    }

    void onServiceMessage(final long leadershipTermId, final DirectBuffer buffer, final int offset, final int length)
    {
        if (leadershipTermId == this.leadershipTermId)
        {
            enqueueServiceSessionMessage((MutableDirectBuffer)buffer, offset, length, nextServiceSessionId++);
        }
    }

    void onScheduleTimer(final long correlationId, final long deadline)
    {
        if (expiredTimerCountByCorrelationIdMap.get(correlationId) == 0)
        {
            timerService.scheduleTimerForCorrelationId(correlationId, deadline);
        }
        else
        {
            expiredTimerCountByCorrelationIdMap.decrementAndGet(correlationId);
        }
    }

    void onCancelTimer(final long correlationId)
    {
        timerService.cancelTimerByCorrelationId(correlationId);
    }

    void onServiceAck(
        final long logPosition, final long timestamp, final long ackId, final long relevantId, final int serviceId)
    {
        captureServiceAck(logPosition, ackId, relevantId, serviceId);

        if (ServiceAck.hasReached(logPosition, serviceAckId, serviceAckQueues))
        {
            if (ConsensusModule.State.SNAPSHOT == state)
            {
                final ServiceAck[] serviceAcks = pollServiceAcks(logPosition, serviceId);
                ++serviceAckId;
                takeSnapshot(timestamp, logPosition, serviceAcks);

                if (null != clusterTermination)
                {
                    serviceProxy.terminationPosition(terminationPosition, ctx.countedErrorHandler());
                    clusterTermination.deadlineNs(clusterClock.timeNanos() + ctx.terminationTimeoutNs());
                    state(ConsensusModule.State.TERMINATING);
                }
                else
                {
                    state(ConsensusModule.State.ACTIVE);
                    if (Cluster.Role.LEADER == role)
                    {
                        ClusterControl.ToggleState.reset(controlToggle);
                    }
                }
            }
            else if (ConsensusModule.State.QUITTING == state)
            {
                closeAndTerminate();
            }
            else if (ConsensusModule.State.TERMINATING == state)
            {
                if (null == clusterTermination)
                {
                    consensusPublisher.terminationAck(
                        leaderMember.publication(), leadershipTermId, logPosition, memberId);
                    recordingLog.commitLogPosition(leadershipTermId, logPosition);
                    closeAndTerminate();
                }
                else
                {
                    clusterTermination.onServicesTerminated();
                    if (clusterTermination.canTerminate(
                        activeMembers, terminationPosition, clusterClock.timeNanos()))
                    {
                        recordingLog.commitLogPosition(leadershipTermId, logPosition);
                        closeAndTerminate();
                    }
                }
            }
        }
    }

    void onReplaySessionMessage(final long clusterSessionId, final long timestamp)
    {
        final ClusterSession clusterSession = sessionByIdMap.get(clusterSessionId);
        if (null == clusterSession)
        {
            logServiceSessionId = clusterSessionId;
            pendingServiceMessages.consume(followerServiceSessionMessageSweeper, Integer.MAX_VALUE);
        }
        else
        {
            clusterSession.timeOfLastActivityNs(clusterTimeUnit.toNanos(timestamp));
        }
    }

    void onReplayTimerEvent(final long correlationId)
    {
        if (!timerService.cancelTimerByCorrelationId(correlationId))
        {
            expiredTimerCountByCorrelationIdMap.getAndIncrement(correlationId);
        }
    }

    void onReplaySessionOpen(
        final long logPosition,
        final long correlationId,
        final long clusterSessionId,
        final long timestamp,
        final int responseStreamId,
        final String responseChannel)
    {
        final ClusterSession session = new ClusterSession(clusterSessionId, responseStreamId, responseChannel);
        session.open(logPosition);
        session.lastActivityNs(clusterTimeUnit.toNanos(timestamp), correlationId);

        sessionByIdMap.put(clusterSessionId, session);
        if (clusterSessionId >= nextSessionId)
        {
            nextSessionId = clusterSessionId + 1;
        }
    }

    void onReplaySessionClose(final long clusterSessionId, final CloseReason closeReason)
    {
        final ClusterSession clusterSession = sessionByIdMap.remove(clusterSessionId);
        if (null != clusterSession)
        {
            clusterSession.closing(closeReason);
            clusterSession.close(ctx.countedErrorHandler());
        }
    }

    void onReplayClusterAction(final long leadershipTermId, final ClusterAction action)
    {
        if (leadershipTermId == this.replayLeadershipTermId)
        {
            if (ClusterAction.SUSPEND == action)
            {
                state(ConsensusModule.State.SUSPENDED);
            }
            else if (ClusterAction.RESUME == action)
            {
                state(ConsensusModule.State.ACTIVE);
            }
            else if (ClusterAction.SNAPSHOT == action)
            {
                state(ConsensusModule.State.SNAPSHOT);
            }
        }
    }

    void onReplayNewLeadershipTermEvent(
        final long leadershipTermId,
        final long logPosition,
        final long timestamp,
        final long termBaseLogPosition,
        final TimeUnit timeUnit,
        final int appVersion)
    {
        if (timeUnit != clusterTimeUnit)
        {
            ctx.countedErrorHandler().onError(new ClusterException(
                "incompatible timestamp units: " + clusterTimeUnit + " log=" + timeUnit,
                AeronException.Category.FATAL));
            unexpectedTermination();
        }

        if (SemanticVersion.major(ctx.appVersion()) != SemanticVersion.major(appVersion))
        {
            ctx.countedErrorHandler().onError(new ClusterException(
                "incompatible version: " + SemanticVersion.toString(ctx.appVersion()) +
                " log=" + SemanticVersion.toString(appVersion),
                AeronException.Category.FATAL));
            unexpectedTermination();
        }

        leadershipTermId(leadershipTermId);

        if (null != election)
        {
            election.onReplayNewLeadershipTermEvent(
                logRecordingId, leadershipTermId, logPosition, timestamp, termBaseLogPosition);
        }
    }

    void onReplayMembershipChange(
        final long leadershipTermId,
        final long logPosition,
        final int leaderMemberId,
        final ChangeType changeType,
        final int memberId,
        final String clusterMembers)
    {
        if (leadershipTermId == this.replayLeadershipTermId)
        {
            if (ChangeType.JOIN == changeType)
            {
                final ClusterMember[] newMembers = ClusterMember.parse(clusterMembers);
                if (memberId == this.memberId)
                {
                    activeMembers = newMembers;
                    clusterMemberByIdMap.clear();
                    clusterMemberByIdMap.compact();
                    ClusterMember.addClusterMemberIds(newMembers, clusterMemberByIdMap);
                    thisMember = ClusterMember.findMember(activeMembers, memberId);
                    leaderMember = ClusterMember.findMember(activeMembers, leaderMemberId);

                    ClusterMember.addConsensusPublications(
                        newMembers,
                        thisMember,
                        ChannelUri.parse(ctx.consensusChannel()),
                        ctx.consensusStreamId(),
                        aeron);
                }
                else
                {
                    clusterMemberJoined(memberId, newMembers);
                }
            }
            else if (ChangeType.QUIT == changeType)
            {
                if (memberId == this.memberId)
                {
                    state(ConsensusModule.State.QUITTING);
                }
                else
                {
                    clusterMemberQuit(memberId);
                    if (leaderMemberId == memberId && null == election)
                    {
                        commitPosition.proposeMaxOrdered(logPosition);
                        enterElection();
                    }
                }
            }

            if (null != election)
            {
                election.onMembershipChange(activeMembers, changeType, memberId, logPosition);
            }
        }
    }

    void onLoadSession(
        final long clusterSessionId,
        final long correlationId,
        final long openedPosition,
        final long timeOfLastActivity,
        final CloseReason closeReason,
        final int responseStreamId,
        final String responseChannel)
    {
        sessionByIdMap.put(clusterSessionId, new ClusterSession(
            clusterSessionId,
            correlationId,
            openedPosition,
            timeOfLastActivity,
            responseStreamId,
            responseChannel,
            closeReason));

        if (clusterSessionId >= nextSessionId)
        {
            nextSessionId = clusterSessionId + 1;
        }
    }

    void onLoadPendingMessage(final DirectBuffer buffer, final int offset, final int length)
    {
        pendingServiceMessages.append(buffer, offset, length);
    }

    void onLoadConsensusModuleState(
        final long nextSessionId,
        final long nextServiceSessionId,
        final long logServiceSessionId,
        final int pendingMessageCapacity)
    {
        this.nextSessionId = nextSessionId;
        this.nextServiceSessionId = nextServiceSessionId;
        this.logServiceSessionId = logServiceSessionId;
        pendingServiceMessages.reset(pendingMessageCapacity);
    }

    void onLoadClusterMembers(final int memberId, final int highMemberId, final String members)
    {
        if (null == dynamicJoin && !ctx.clusterMembersIgnoreSnapshot())
        {
            if (NULL_VALUE == this.memberId)
            {
                this.memberId = memberId;
                ctx.clusterMarkFile().memberId(memberId);
            }

            if (ClusterMember.EMPTY_MEMBERS == activeMembers)
            {
                activeMembers = ClusterMember.parse(members);
                this.highMemberId = Math.max(ClusterMember.highMemberId(activeMembers), highMemberId);
                rankedPositions = new long[ClusterMember.quorumThreshold(activeMembers.length)];
                thisMember = clusterMemberByIdMap.get(memberId);

                final ChannelUri consensusUri = ChannelUri.parse(ctx.consensusChannel());
                consensusUri.put(ENDPOINT_PARAM_NAME, thisMember.consensusEndpoint());

                ClusterMember.addConsensusPublications(
                    activeMembers, thisMember, consensusUri, ctx.consensusStreamId(), aeron);
            }
        }
    }

    int addLogPublication()
    {
        final long logPublicationTag = aeron.nextCorrelationId();
        logPublicationChannelTag = aeron.nextCorrelationId();
        final ChannelUri channelUri = ChannelUri.parse(ctx.logChannel());

        channelUri.put(ALIAS_PARAM_NAME, "log");
        channelUri.put(TAGS_PARAM_NAME, logPublicationChannelTag + "," + logPublicationTag);

        if (channelUri.isUdp())
        {
            if (!channelUri.containsKey(FLOW_CONTROL_PARAM_NAME))
            {
                final long timeoutNs = (ctx.leaderHeartbeatTimeoutNs() >> 1) + (ctx.leaderHeartbeatTimeoutNs() >> 2);
                final long timeout = Math.max(TimeUnit.NANOSECONDS.toSeconds(timeoutNs), 3L);
                channelUri.put(FLOW_CONTROL_PARAM_NAME, "min,t:" + timeout + "s");
            }

            if (ctx.isLogMdc())
            {
                channelUri.put(MDC_CONTROL_MODE_PARAM_NAME, MDC_CONTROL_MODE_MANUAL);
            }

            channelUri.put(SPIES_SIMULATE_CONNECTION_PARAM_NAME, Boolean.toString(activeMembers.length == 1));
        }

        if (null != recoveryPlan.log)
        {
            channelUri.initialPosition(
                recoveryPlan.appendedLogPosition, recoveryPlan.log.initialTermId, recoveryPlan.log.termBufferLength);
            channelUri.put(MTU_LENGTH_PARAM_NAME, Integer.toString(recoveryPlan.log.mtuLength));
        }

        final String channel = channelUri.toString();
        final ExclusivePublication publication = aeron.addExclusivePublication(channel, ctx.logStreamId());

        if (ctx.isLogMdc())
        {
            for (final ClusterMember member : activeMembers)
            {
                if (member.id() != memberId)
                {
                    publication.asyncAddDestination("aeron:udp?endpoint=" + member.logEndpoint());
                }
            }

            for (final ClusterMember member : passiveMembers)
            {
                publication.asyncAddDestination("aeron:udp?endpoint=" + member.logEndpoint());
            }
        }

        logPublisher.publication(publication);

        return publication.sessionId();
    }

    void joinLogAsLeader(
        final long leadershipTermId, final long logPosition, final int logSessionId, final boolean isStartup)
    {
        final boolean isIpc = ctx.logChannel().startsWith(IPC_CHANNEL);
        final String channel = (isIpc ? "aeron:ipc" : "aeron:udp") +
            "?tags=" + logPublicationChannelTag + "|session-id=" + logSessionId + "|alias=log";

        leadershipTermId(leadershipTermId);
        startLogRecording(channel, ctx.logStreamId(), SourceLocation.LOCAL);
        createAppendPosition(logSessionId);

        awaitServicesReady(
            isIpc ? channel : SPY_PREFIX + channel,
            ctx.logStreamId(),
            logSessionId,
            leadershipTermId,
            logPosition,
            Long.MAX_VALUE,
            isStartup,
            Cluster.Role.LEADER);
    }

    void liveLogDestination(final String liveLogDestination)
    {
        this.liveLogDestination = liveLogDestination;
    }

    String liveLogDestination()
    {
        return liveLogDestination;
    }

    void catchupLogDestination(final String catchupLogDestination)
    {
        this.catchupLogDestination = catchupLogDestination;
    }

    String catchupLogDestination()
    {
        return catchupLogDestination;
    }

    void joinLogAsFollower(final Image image, final long leadershipTermId, final boolean isLeaderStartup)
    {
        appendDynamicJoinTermAndSnapshots();

        final Subscription logSubscription = image.subscription();
        final int streamId = logSubscription.streamId();
        final String channel = logSubscription.channel();

        startLogRecording(channel, streamId, SourceLocation.REMOTE);
        createAppendPosition(image.sessionId());

        logAdapter.image(image);
        lastAppendPosition = image.joinPosition();

        awaitServicesReady(
            channel,
            streamId,
            image.sessionId(),
            leadershipTermId,
            image.joinPosition(),
            Long.MAX_VALUE,
            isLeaderStartup,
            Cluster.Role.FOLLOWER);
    }

    void awaitServicesReady(
        final String logChannel,
        final int streamId,
        final int logSessionId,
        final long leadershipTermId,
        final long logPosition,
        final long maxLogPosition,
        final boolean isStartup,
        final Cluster.Role role)
    {
        serviceProxy.joinLog(
            leadershipTermId,
            logPosition,
            maxLogPosition,
            memberId,
            logSessionId,
            streamId,
            isStartup,
            role,
            logChannel);

        expectedAckPosition = logPosition;

        while (!ServiceAck.hasReached(logPosition, serviceAckId, serviceAckQueues))
        {
            idle(consensusModuleAdapter.poll());
        }

        ServiceAck.removeHead(serviceAckQueues);
        ++serviceAckId;
    }

    void leadershipTermId(final long leadershipTermId)
    {
        this.leadershipTermId = leadershipTermId;
        this.replayLeadershipTermId = leadershipTermId;
    }

    LogReplay newLogReplay(final long logPosition, final long appendPosition)
    {
        return new LogReplay(
            archive,
            recoveryPlan.log.recordingId,
            logPosition,
            appendPosition,
            recoveryPlan.log.leadershipTermId,
            logAdapter,
            ctx);
    }

    int replayLogPoll(final LogAdapter logAdapter, final long stopPosition)
    {
        int workCount = 0;

        if (ConsensusModule.State.ACTIVE == state || ConsensusModule.State.SUSPENDED == state)
        {
            final int fragments = logAdapter.poll(stopPosition);
            final long position = logAdapter.position();

            if (fragments > 0)
            {
                commitPosition.setOrdered(position);
            }
            else if (logAdapter.isImageClosed() && position < stopPosition)
            {
                throw new ClusterException("unexpected image close when replaying log: position=" + position);
            }

            workCount += fragments;
        }

        workCount += consensusModuleAdapter.poll();

        return workCount;
    }

    long logRecordingId()
    {
        return logRecordingId;
    }

    void truncateLogEntry(final long leadershipTermId, final long logPosition)
    {
        archive.stopAllReplays(logRecordingId);
        archive.truncateRecording(logRecordingId, logPosition);
        recordingLog.commitLogPosition(leadershipTermId, logPosition);
        logAdapter.disconnect(ctx.countedErrorHandler(), logPosition);
    }

    boolean electionComplete()
    {
        final long logPosition = election.logPosition();
        final long now = clusterClock.time();
        final long nowNs = clusterTimeUnit.toNanos(now);

        if (Cluster.Role.LEADER == role)
        {
            if (!logPublisher.isConnected() || !logPublisher.appendNewLeadershipTermEvent(
                leadershipTermId,
                now,
                logPosition,
                memberId,
                logPublisher.sessionId(),
                clusterTimeUnit,
                ctx.appVersion()))
            {
                return false;
            }

            timeOfLastLogUpdateNs = nowNs - leaderHeartbeatIntervalNs;
            timerService.currentTickTime(now);
            ClusterControl.ToggleState.activate(controlToggle);
            prepareSessionsForNewTerm(election.isLeaderStartup());
        }
        else
        {
            timeOfLastLogUpdateNs = nowNs;
            timeOfLastAppendPositionNs = nowNs;
        }

        recoveryPlan = recordingLog.createRecoveryPlan(archive, ctx.serviceCount());
        notifiedCommitPosition = logPosition;
        commitPosition.setOrdered(logPosition);
        pendingServiceMessages.consume(followerServiceSessionMessageSweeper, Integer.MAX_VALUE);
        updateMemberDetails(election.leader());
        election = null;

        connectIngress();

        return true;
    }

    boolean dynamicJoinComplete()
    {
        if (0 == activeMembers.length)
        {
            activeMembers = dynamicJoin.clusterMembers();
            ClusterMember.addClusterMemberIds(activeMembers, clusterMemberByIdMap);
            leaderMember = dynamicJoin.leader();

            ClusterMember.addConsensusPublications(
                activeMembers, thisMember, ChannelUri.parse(ctx.consensusChannel()), ctx.consensusStreamId(), aeron);
        }

        if (NULL_VALUE == memberId)
        {
            memberId = dynamicJoin.memberId();
            ctx.clusterMarkFile().memberId(memberId);
            thisMember.id(memberId);
        }

        dynamicJoin = null;

        election = new Election(
            false,
            leadershipTermId,
            commitPosition.getWeak(),
            recoveryPlan.appendedLogPosition,
            activeMembers,
            clusterMemberByIdMap,
            thisMember,
            consensusPublisher,
            ctx,
            this);

        return true;
    }

    void trackCatchupCompletion(final ClusterMember follower, final long leadershipTermId)
    {
        if (NULL_VALUE != follower.catchupReplaySessionId())
        {
            if (follower.logPosition() >= logPublisher.position())
            {
                if (NULL_VALUE != follower.catchupReplayCorrelationId())
                {
                    if (archive.archiveProxy().stopReplay(
                        follower.catchupReplaySessionId(), aeron.nextCorrelationId(), archive.controlSessionId()))
                    {
                        follower.catchupReplayCorrelationId(NULL_VALUE);
                    }
                }

                if (consensusPublisher.stopCatchup(follower.publication(), leadershipTermId, follower.id()))
                {
                    follower.catchupReplaySessionId(NULL_VALUE);
                }
            }
        }
    }

    void catchupInitiated(final long nowNs)
    {
        timeOfLastAppendPositionNs = nowNs;
    }

    int catchupPoll(final long limitPosition, final long nowNs)
    {
        int workCount = 0;

        if (ConsensusModule.State.ACTIVE == state || ConsensusModule.State.SUSPENDED == state)
        {
            final int fragments = logAdapter.poll(Math.min(appendPosition.get(), limitPosition));
            workCount += fragments;
            if (fragments == 0 && logAdapter.image().isClosed())
            {
                throw new ClusterException("unexpected close replaying log: position=" + logAdapter.image().position());
            }
        }

        final long appendPosition = logAdapter.position();
        if (appendPosition > lastAppendPosition || nowNs > (timeOfLastAppendPositionNs + leaderHeartbeatIntervalNs))
        {
            commitPosition.proposeMaxOrdered(appendPosition);
            final ExclusivePublication publication = election.leader().publication();
            if (consensusPublisher.appendPosition(publication, replayLeadershipTermId, appendPosition, memberId))
            {
                lastAppendPosition = appendPosition;
                timeOfLastAppendPositionNs = nowNs;
            }
        }

        if (nowNs > (timeOfLastAppendPositionNs + leaderHeartbeatTimeoutNs) && ConsensusModule.State.ACTIVE == state)
        {
            throw new ClusterException("no catchup progress", WARN);
        }

        workCount += consensusModuleAdapter.poll();

        return workCount;
    }

    boolean isCatchupNearLive(final long position)
    {
        final Image image = logAdapter.image();
        if (null != image)
        {
            final long localPosition = image.position();
            final long window = Math.min(image.termBufferLength() >> 2, LIVE_ADD_MAX_WINDOW);

            return localPosition >= (position - window);
        }

        return false;
    }

    void stopAllCatchups()
    {
        for (final ClusterMember member : activeMembers)
        {
            if (member.catchupReplaySessionId() != NULL_VALUE)
            {
                if (member.catchupReplayCorrelationId() != NULL_VALUE)
                {
                    try
                    {
                        archive.stopReplay(member.catchupReplaySessionId());
                    }
                    catch (final Exception ex)
                    {
                        ctx.countedErrorHandler().onError(new ClusterException("catchup already stopped", ex, WARN));
                    }
                }

                member.catchupReplaySessionId(NULL_VALUE);
                member.catchupReplayCorrelationId(NULL_VALUE);
            }
        }
    }

    void retrievedSnapshot(final long localRecordingId, final RecordingLog.Snapshot leaderSnapshot)
    {
        dynamicJoinSnapshots.add(new RecordingLog.Snapshot(
            localRecordingId,
            leaderSnapshot.leadershipTermId,
            leaderSnapshot.termBaseLogPosition,
            leaderSnapshot.logPosition,
            leaderSnapshot.timestamp,
            leaderSnapshot.serviceId));
    }

    Counter loadSnapshotsForDynamicJoin()
    {
        recoveryPlan = RecordingLog.createRecoveryPlan(dynamicJoinSnapshots);
        if (null != recoveryPlan.log)
        {
            logRecordingId = recoveryPlan.log.recordingId;
        }

        final Counter recoveryStateCounter = addRecoveryStateCounter(recoveryPlan);
        if (!recoveryPlan.snapshots.isEmpty())
        {
            loadSnapshot(recoveryPlan.snapshots.get(0), archive);
        }

        return recoveryStateCounter;
    }

    boolean pollForSnapshotLoadAck(final Counter recoveryStateCounter, final long nowNs)
    {
        consensusModuleAdapter.poll();

        if (ServiceAck.hasReached(expectedAckPosition, serviceAckId, serviceAckQueues))
        {
            captureServiceClientIds();
            ++serviceAckId;

            CloseHelper.close(ctx.countedErrorHandler(), recoveryStateCounter);
            state(ConsensusModule.State.ACTIVE);

            timeOfLastLogUpdateNs = nowNs;
            leadershipTermId(recoveryPlan.lastLeadershipTermId);

            return true;
        }

        return false;
    }

    private void startLogRecording(final String channel, final int streamId, final SourceLocation sourceLocation)
    {
        try
        {
            final long logRecordingId = recordingLog.findLastTermRecordingId();
            if (RecordingPos.NULL_RECORDING_ID == logRecordingId)
            {
                logSubscriptionId = archive.startRecording(channel, streamId, sourceLocation, true);
            }
            else
            {
                logSubscriptionId = archive.extendRecording(logRecordingId, channel, streamId, sourceLocation, true);
            }
        }
        catch (final ArchiveException ex)
        {
            if (ex.errorCode() == ArchiveException.STORAGE_SPACE)
            {
                ctx.countedErrorHandler().onError(ex);
                unexpectedTermination();
            }

            throw ex;
        }
    }

    private void prepareSessionsForNewTerm(final boolean isStartup)
    {
        if (isStartup)
        {
            for (final ClusterSession session : sessionByIdMap.values())
            {
                if (session.state() == OPEN)
                {
                    session.closing(CloseReason.TIMEOUT);
                }
            }
        }
        else
        {
            for (final ClusterSession session : sessionByIdMap.values())
            {
                if (session.state() == OPEN)
                {
                    session.connect(aeron);
                }
            }

            final long nowNs = clusterClock.timeNanos();
            for (final ClusterSession session : sessionByIdMap.values())
            {
                if (session.state() == OPEN)
                {
                    session.timeOfLastActivityNs(nowNs);
                    session.hasNewLeaderEventPending(true);
                }
            }
        }
    }

    private void updateMemberDetails(final ClusterMember newLeader)
    {
        leaderMember = newLeader;

        for (final ClusterMember clusterMember : activeMembers)
        {
            clusterMember.isLeader(clusterMember.id() == leaderMember.id());
        }

        ingressEndpoints = ClusterMember.ingressEndpoints(activeMembers);
    }

    private int slowTickWork(final long nowMs, final long nowNs)
    {
        int workCount = aeronClientInvoker.invoke();
        if (aeron.isClosed())
        {
            throw new AgentTerminationException("unexpected Aeron close");
        }
        else if (ConsensusModule.State.CLOSED == state)
        {
            unexpectedTermination();
        }

        if (null == dynamicJoin)
        {
            checkArchiveEvents(true);
        }

        if (nowNs >= markFileUpdateDeadlineNs)
        {
            markFile.updateActivityTimestamp(nowMs);
            markFileUpdateDeadlineNs = nowNs + MARK_FILE_UPDATE_INTERVAL_NS;
        }

        workCount += sendRedirects(redirectSessions, nowNs);
        workCount += sendRejections(rejectedSessions, nowNs);

        if (null == election)
        {
            if (Cluster.Role.LEADER == role)
            {
                workCount += checkControlToggle(nowNs);

                if (ConsensusModule.State.ACTIVE == state)
                {
                    workCount += processPendingSessions(pendingSessions, nowMs, nowNs);
                    workCount += checkSessions(sessionByIdMap, nowNs);
                    workCount += processPassiveMembers(passiveMembers);

                    if (!ClusterMember.hasActiveQuorum(activeMembers, nowNs, leaderHeartbeatTimeoutNs))
                    {
                        ctx.countedErrorHandler().onError(new ClusterException("inactive follower quorum", WARN));
                        enterElection();
                        workCount += 1;
                    }
                }
                else if (ConsensusModule.State.TERMINATING == state)
                {
                    if (clusterTermination.canTerminate(activeMembers, terminationPosition, nowNs))
                    {
                        recordingLog.commitLogPosition(leadershipTermId, terminationPosition);
                        closeAndTerminate();
                    }
                }
            }
            else if (ConsensusModule.State.ACTIVE == state || ConsensusModule.State.SUSPENDED == state)
            {
                if (nowNs >= (timeOfLastLogUpdateNs + leaderHeartbeatTimeoutNs) && NULL_POSITION == terminationPosition)
                {
                    ctx.countedErrorHandler().onError(new ClusterException("leader heartbeat timeout", WARN));
                    enterElection();
                    workCount += 1;
                }
            }
        }

        return workCount;
    }

    private int consensusWork(final long timestamp, final long nowNs)
    {
        int workCount = 0;

        if (Cluster.Role.LEADER == role)
        {
            if (ConsensusModule.State.ACTIVE == state)
            {
                workCount += timerService.poll(timestamp);
                workCount += pendingServiceMessages.forEach(
                    pendingServiceMessageHeadOffset, serviceSessionMessageAppender, SERVICE_MESSAGE_LIMIT);
                workCount += ingressAdapter.poll();
            }

            workCount += updateLeaderPosition(nowNs);
        }
        else
        {
            if (ConsensusModule.State.ACTIVE == state || ConsensusModule.State.SUSPENDED == state)
            {
                if (NULL_POSITION != terminationPosition && logAdapter.position() >= terminationPosition)
                {
                    serviceProxy.terminationPosition(terminationPosition, ctx.countedErrorHandler());
                    state(ConsensusModule.State.TERMINATING);
                }
                else
                {
                    final long limit = null != appendPosition ? appendPosition.get() : logRecordedPosition;
                    final int count = logAdapter.poll(min(notifiedCommitPosition, limit));
                    if (0 == count && logAdapter.isImageClosed())
                    {
                        ctx.countedErrorHandler().onError(new ClusterException("log disconnected from leader", WARN));
                        enterElection();
                        return 1;
                    }

                    commitPosition.proposeMaxOrdered(logAdapter.position());
                    workCount += ingressAdapter.poll();
                    workCount += count;
                }
            }

            workCount += updateFollowerPosition(nowNs);
        }

        workCount += consensusModuleAdapter.poll();

        return workCount;
    }

    private void checkArchiveEvents(final boolean isSlowTick)
    {
        if (null != archive)
        {
            final RecordingSignalPoller poller = this.recordingSignalPoller;
            if (!poller.subscription().isConnected())
            {
                ctx.countedErrorHandler().onError(new ClusterException("local archive is not connected", WARN));
                unexpectedTermination();
            }
            else if (poller.poll() > 0 && poller.isPollComplete())
            {
                if (poller.templateId() == ControlResponseDecoder.TEMPLATE_ID &&
                    poller.code() == ControlResponseCode.ERROR)
                {
                    for (final ClusterMember member : activeMembers)
                    {
                        if (member.catchupReplayCorrelationId() != NULL_VALUE &&
                            member.catchupReplayCorrelationId() == poller.correlationId())
                        {
                            member.catchupReplaySessionId(NULL_VALUE);
                            member.catchupReplayCorrelationId(NULL_VALUE);

                            ctx.countedErrorHandler().onError(new ClusterException(
                                "catchup replay failed - " + poller.errorMessage(), WARN));
                            return;
                        }
                    }

                    final ArchiveException ex = new ArchiveException(
                        poller.errorMessage(), (int)poller.relevantId(), poller.correlationId());

                    if (ex.errorCode() == ArchiveException.STORAGE_SPACE)
                    {
                        ctx.countedErrorHandler().onError(ex);
                        unexpectedTermination();
                    }

                    if (null != election && isSlowTick)
                    {
                        election.handleError(clusterClock.timeNanos(), ex);
                    }
                    else
                    {
                        throw ex;
                    }
                }
                else if (poller.templateId() == RecordingSignalEventDecoder.TEMPLATE_ID)
                {
                    if (poller.recordingSignal() == RecordingSignal.STOP && poller.recordingId() == logRecordingId)
                    {
                        logRecordedPosition = poller.recordingPosition();
                    }
                }
            }
        }
    }

    private int checkControlToggle(final long nowNs)
    {
        switch (ClusterControl.ToggleState.get(controlToggle))
        {
            case SUSPEND:
                if (ConsensusModule.State.ACTIVE == state && appendAction(ClusterAction.SUSPEND))
                {
                    state(ConsensusModule.State.SUSPENDED);
                }
                break;

            case RESUME:
                if (ConsensusModule.State.SUSPENDED == state && appendAction(ClusterAction.RESUME))
                {
                    state(ConsensusModule.State.ACTIVE);
                    ClusterControl.ToggleState.reset(controlToggle);
                }
                break;

            case SNAPSHOT:
                if (ConsensusModule.State.ACTIVE == state && appendAction(ClusterAction.SNAPSHOT))
                {
                    state(ConsensusModule.State.SNAPSHOT);
                }
                break;

            case SHUTDOWN:
                if (ConsensusModule.State.ACTIVE == state && appendAction(ClusterAction.SNAPSHOT))
                {
                    final CountedErrorHandler errorHandler = ctx.countedErrorHandler();
                    final long position = logPublisher.position();
                    clusterTermination = new ClusterTermination(nowNs + ctx.terminationTimeoutNs());
                    clusterTermination.terminationPosition(
                        errorHandler, consensusPublisher, activeMembers, thisMember, leadershipTermId, position);
                    terminationPosition = position;
                    state(ConsensusModule.State.SNAPSHOT);
                }
                break;

            case ABORT:
                if (ConsensusModule.State.ACTIVE == state)
                {
                    final CountedErrorHandler errorHandler = ctx.countedErrorHandler();
                    final long position = logPublisher.position();
                    clusterTermination = new ClusterTermination(nowNs + ctx.terminationTimeoutNs());
                    clusterTermination.terminationPosition(
                        errorHandler, consensusPublisher, activeMembers, thisMember, leadershipTermId, position);
                    terminationPosition = position;
                    serviceProxy.terminationPosition(terminationPosition, errorHandler);
                    state(ConsensusModule.State.TERMINATING);
                }
                break;

            default:
                return 0;
        }

        return 1;
    }

    private boolean appendAction(final ClusterAction action)
    {
        return logPublisher.appendClusterAction(leadershipTermId, clusterClock.time(), action);
    }

    private int processPendingSessions(
        final ArrayList<ClusterSession> pendingSessions, final long nowMs, final long nowNs)
    {
        int workCount = 0;

        for (int lastIndex = pendingSessions.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final ClusterSession session = pendingSessions.get(i);

            if (nowNs > (session.timeOfLastActivityNs() + sessionTimeoutNs))
            {
                ArrayListUtil.fastUnorderedRemove(pendingSessions, i, lastIndex--);
                session.close(ctx.countedErrorHandler());
                ctx.timedOutClientCounter().incrementOrdered();
                continue;
            }

            if (session.state() == INIT || session.state() == CONNECTED)
            {
                if (session.isResponsePublicationConnected())
                {
                    session.state(CONNECTED);
                    authenticator.onConnectedSession(sessionProxy.session(session), nowMs);
                }
            }

            if (session.state() == CHALLENGED)
            {
                if (session.isResponsePublicationConnected())
                {
                    authenticator.onChallengedSession(sessionProxy.session(session), nowMs);
                }
            }

            if (session.state() == AUTHENTICATED)
            {
                if (session.isBackupSession())
                {
                    final RecordingLog.Entry lastEntry = recordingLog.findLastTerm();
                    if (null != lastEntry && consensusPublisher.backupResponse(
                        session.responsePublication(),
                        session.correlationId(),
                        recoveryPlan.log.recordingId,
                        recoveryPlan.log.leadershipTermId,
                        recoveryPlan.log.termBaseLogPosition,
                        lastEntry.leadershipTermId,
                        lastEntry.termBaseLogPosition,
                        commitPosition.id(),
                        leaderMember.id(),
                        recoveryPlan,
                        ClusterMember.encodeAsString(activeMembers)))
                    {
                        ArrayListUtil.fastUnorderedRemove(pendingSessions, i, lastIndex--);
                        session.close(ctx.countedErrorHandler());
                        workCount += 1;
                    }
                }
                else if (appendSessionAndOpen(session, nowNs))
                {
                    ArrayListUtil.fastUnorderedRemove(pendingSessions, i, lastIndex--);
                    sessionByIdMap.put(session.id(), session);
                    workCount += 1;
                }
            }
            else if (session.state() == REJECTED)
            {
                ArrayListUtil.fastUnorderedRemove(pendingSessions, i, lastIndex--);
                rejectedSessions.add(session);
            }
        }

        return workCount;
    }

    private int sendRejections(final ArrayList<ClusterSession> rejectedSessions, final long nowNs)
    {
        int workCount = 0;

        for (int lastIndex = rejectedSessions.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final ClusterSession session = rejectedSessions.get(i);
            final String detail = session.responseDetail();
            final EventCode eventCode = session.eventCode();

            if (egressPublisher.sendEvent(session, leadershipTermId, leaderMember.id(), eventCode, detail) ||
                nowNs > (session.timeOfLastActivityNs() + sessionTimeoutNs))
            {
                ArrayListUtil.fastUnorderedRemove(rejectedSessions, i, lastIndex--);
                session.close(ctx.countedErrorHandler());
                workCount++;
            }
        }

        return workCount;
    }

    private int sendRedirects(final ArrayList<ClusterSession> redirectSessions, final long nowNs)
    {
        int workCount = 0;

        for (int lastIndex = redirectSessions.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final ClusterSession session = redirectSessions.get(i);
            final EventCode eventCode = EventCode.REDIRECT;
            final int leaderId = leaderMember.id();

            if (egressPublisher.sendEvent(session, leadershipTermId, leaderId, eventCode, ingressEndpoints) ||
                nowNs > (session.timeOfLastActivityNs() + sessionTimeoutNs))
            {
                ArrayListUtil.fastUnorderedRemove(redirectSessions, i, lastIndex--);
                session.close(ctx.countedErrorHandler());
                workCount++;
            }
        }

        return workCount;
    }

    private int processPassiveMembers(final ClusterMember[] passiveMembers)
    {
        int workCount = 0;

        for (final ClusterMember member : passiveMembers)
        {
            if (member.correlationId() != NULL_VALUE)
            {
                if (consensusPublisher.clusterMemberChange(
                    member.publication(),
                    member.correlationId(),
                    leaderMember.id(),
                    ClusterMember.encodeAsString(activeMembers),
                    ClusterMember.encodeAsString(passiveMembers)))
                {
                    member.correlationId(NULL_VALUE);
                    workCount++;
                }
            }
            else if (member.hasRequestedJoin() && member.logPosition() == logPublisher.position())
            {
                final ClusterMember[] newMembers = ClusterMember.addMember(activeMembers, member);
                final long now = clusterClock.time();

                if (logPublisher.appendMembershipChangeEvent(
                    leadershipTermId,
                    now,
                    leaderMember.id(),
                    newMembers.length,
                    ChangeType.JOIN,
                    member.id(),
                    ClusterMember.encodeAsString(newMembers)) > 0)
                {
                    timeOfLastLogUpdateNs = clusterTimeUnit.toNanos(now) - leaderHeartbeatIntervalNs;
                    this.passiveMembers = ClusterMember.removeMember(this.passiveMembers, member.id());
                    activeMembers = newMembers;
                    rankedPositions = new long[ClusterMember.quorumThreshold(activeMembers.length)];
                    member.hasRequestedJoin(false);

                    workCount++;
                    break;
                }
            }
        }

        return workCount;
    }

    private int checkSessions(final Long2ObjectHashMap<ClusterSession> sessionByIdMap, final long nowNs)
    {
        int workCount = 0;

        for (final Iterator<ClusterSession> i = sessionByIdMap.values().iterator(); i.hasNext(); )
        {
            final ClusterSession session = i.next();

            if (nowNs > (session.timeOfLastActivityNs() + sessionTimeoutNs))
            {
                if (session.state() == OPEN)
                {
                    session.closing(CloseReason.TIMEOUT);
                    if (logPublisher.appendSessionClose(session, leadershipTermId, clusterClock.time()))
                    {
                        final String msg = session.closeReason().name();
                        egressPublisher.sendEvent(session, leadershipTermId, memberId, EventCode.CLOSED, msg);
                        session.closedLogPosition(logPublisher.position());
                        uncommittedClosedSessions.addLast(session);
                        i.remove();
                        session.close(ctx.countedErrorHandler());
                        ctx.timedOutClientCounter().incrementOrdered();
                    }
                }
                else if (session.state() == CLOSING)
                {
                    if (logPublisher.appendSessionClose(session, leadershipTermId, clusterClock.time()))
                    {
                        final String msg = session.closeReason().name();
                        egressPublisher.sendEvent(session, leadershipTermId, memberId, EventCode.CLOSED, msg);
                        session.closedLogPosition(logPublisher.position());
                        uncommittedClosedSessions.addLast(session);
                        i.remove();
                        session.close(ctx.countedErrorHandler());

                        if (session.closeReason() == CloseReason.TIMEOUT)
                        {
                            ctx.timedOutClientCounter().incrementOrdered();
                        }
                    }
                }
                else
                {
                    i.remove();
                    session.close(ctx.countedErrorHandler());
                }

                workCount += 1;
            }
            else if (session.hasOpenEventPending())
            {
                workCount += sendSessionOpenEvent(session);
            }
            else if (session.hasNewLeaderEventPending())
            {
                workCount += sendNewLeaderEvent(session);
            }
        }

        return workCount;
    }

    private void captureServiceAck(final long logPosition, final long ackId, final long relevantId, final int serviceId)
    {
        if (0 == ackId && NULL_VALUE != serviceClientIds[serviceId])
        {
            throw new ClusterException(
                "initial ack already received from service: possible duplicate serviceId=" + serviceId);
        }

        serviceAckQueues[serviceId].offerLast(new ServiceAck(ackId, logPosition, relevantId));
    }

    private ServiceAck[] pollServiceAcks(final long logPosition, final int serviceId)
    {
        final ServiceAck[] serviceAcks = new ServiceAck[serviceAckQueues.length];
        for (int id = 0, length = serviceAckQueues.length; id < length; id++)
        {
            final ServiceAck serviceAck = serviceAckQueues[id].pollFirst();
            if (null == serviceAck || serviceAck.logPosition() != logPosition)
            {
                throw new ClusterException(
                    "invalid ack for serviceId=" + serviceId + " logPosition=" + logPosition + " " + serviceAck);
            }

            serviceAcks[id] = serviceAck;
        }

        return serviceAcks;
    }

    private int sendNewLeaderEvent(final ClusterSession session)
    {
        if (egressPublisher.newLeader(session, leadershipTermId, leaderMember.id(), ingressEndpoints))
        {
            session.hasNewLeaderEventPending(false);
            return 1;
        }

        return 0;
    }

    private int sendSessionOpenEvent(final ClusterSession session)
    {
        if (egressPublisher.sendEvent(session, leadershipTermId, memberId, EventCode.OK, ""))
        {
            session.hasOpenEventPending(false);
            return 1;
        }

        return 0;
    }

    private boolean appendSessionAndOpen(final ClusterSession session, final long nowNs)
    {
        final long resultingPosition = logPublisher.appendSessionOpen(session, leadershipTermId, clusterClock.time());
        if (resultingPosition > 0)
        {
            session.open(resultingPosition);
            session.timeOfLastActivityNs(nowNs);
            return true;
        }

        return false;
    }

    private void createAppendPosition(final int logSessionId)
    {
        final CountersReader counters = aeron.countersReader();
        final int counterId = awaitRecordingCounter(counters, logSessionId);
        final long registrationId = counters.getCounterRegistrationId(counterId);

        appendPosition = new ReadableCounter(counters, registrationId, counterId);
        if (NULL_VALUE == logRecordingId)
        {
            logRecordingId = RecordingPos.getRecordingId(counters, counterId);
            logRecordedPosition = NULL_POSITION;
        }
    }

    private void loadSnapshot(final RecordingLog.Snapshot snapshot, final AeronArchive archive)
    {
        final String channel = ctx.replayChannel();
        final int streamId = ctx.replayStreamId();
        final int sessionId = (int)archive.startReplay(snapshot.recordingId, 0, NULL_LENGTH, channel, streamId);
        final String replaySubscriptionChannel = ChannelUri.addSessionId(channel, sessionId);

        try (Subscription subscription = aeron.addSubscription(replaySubscriptionChannel, streamId))
        {
            final Image image = awaitImage(sessionId, subscription);
            final ConsensusModuleSnapshotLoader snapshotLoader = new ConsensusModuleSnapshotLoader(image, this);

            while (true)
            {
                final int fragments = snapshotLoader.poll();
                if (fragments == 0)
                {
                    if (snapshotLoader.isDone())
                    {
                        break;
                    }

                    if (image.isClosed())
                    {
                        throw new ClusterException("snapshot ended unexpectedly");
                    }
                }

                idle(fragments);
            }

            final int appVersion = snapshotLoader.appVersion();
            if (SemanticVersion.major(ctx.appVersion()) != SemanticVersion.major(appVersion))
            {
                throw new ClusterException(
                    "incompatible version: " + SemanticVersion.toString(ctx.appVersion()) +
                    " snapshot=" + SemanticVersion.toString(appVersion));
            }

            final TimeUnit timeUnit = snapshotLoader.timeUnit();
            if (timeUnit != clusterTimeUnit)
            {
                throw new ClusterException("incompatible time unit: " + clusterTimeUnit + " snapshot=" + timeUnit);
            }

            pendingServiceMessages.forEach(this::serviceSessionMessageReset, Integer.MAX_VALUE);
        }

        timerService.currentTickTime(clusterClock.time());
        leadershipTermId(snapshot.leadershipTermId);
        commitPosition.setOrdered(snapshot.logPosition);
        expectedAckPosition = snapshot.logPosition;
    }

    private Image awaitImage(final int sessionId, final Subscription subscription)
    {
        idleStrategy.reset();
        Image image;
        while ((image = subscription.imageBySessionId(sessionId)) == null)
        {
            idle();
        }

        return image;
    }

    private Counter addRecoveryStateCounter(final RecordingLog.RecoveryPlan plan)
    {
        final int snapshotsCount = plan.snapshots.size();

        if (snapshotsCount > 0)
        {
            final long[] serviceSnapshotRecordingIds = new long[snapshotsCount - 1];
            final RecordingLog.Snapshot snapshot = plan.snapshots.get(0);

            for (int i = 1; i < snapshotsCount; i++)
            {
                final RecordingLog.Snapshot serviceSnapshot = plan.snapshots.get(i);
                serviceSnapshotRecordingIds[serviceSnapshot.serviceId] = serviceSnapshot.recordingId;
            }

            return RecoveryState.allocate(
                aeron,
                snapshot.leadershipTermId,
                snapshot.logPosition,
                snapshot.timestamp,
                ctx.clusterId(),
                serviceSnapshotRecordingIds);
        }

        return RecoveryState.allocate(aeron, leadershipTermId, 0, 0, ctx.clusterId());
    }

    private DynamicJoin requiresDynamicJoin()
    {
        if (0 == activeMembers.length && null != ctx.clusterConsensusEndpoints())
        {
            return new DynamicJoin(ctx.clusterConsensusEndpoints(), archive, consensusPublisher, ctx, this);
        }

        return null;
    }

    private void captureServiceClientIds()
    {
        for (int i = 0, length = serviceClientIds.length; i < length; i++)
        {
            final ServiceAck serviceAck = serviceAckQueues[i].pollFirst();
            serviceClientIds[i] = Objects.requireNonNull(serviceAck).relevantId();
        }
    }

    private void handleMemberRemovals(final long commitPosition)
    {
        ClusterMember[] members = activeMembers;

        for (final ClusterMember member : activeMembers)
        {
            if (member.hasRequestedRemove() && member.removalPosition() <= commitPosition)
            {
                if (member.id() == memberId)
                {
                    state(ConsensusModule.State.QUITTING);
                }

                members = ClusterMember.removeMember(members, member.id());
                clusterMemberByIdMap.remove(member.id());
                clusterMemberByIdMap.compact();

                member.closePublication(ctx.countedErrorHandler());

                logPublisher.removeDestination(ctx.isLogMdc(), member.logEndpoint());
                pendingMemberRemovals--;
            }
        }

        activeMembers = members;
        rankedPositions = new long[ClusterMember.quorumThreshold(members.length)];
    }

    private int updateLeaderPosition(final long nowNs)
    {
        if (null != appendPosition)
        {
            final long position = appendPosition.get();
            thisMember.logPosition(position).timeOfLastAppendPositionNs(nowNs);
            final long commitPosition = min(quorumPosition(activeMembers, rankedPositions), position);

            if (commitPosition > this.commitPosition.getWeak() ||
                nowNs >= (timeOfLastLogUpdateNs + leaderHeartbeatIntervalNs))
            {
                for (final ClusterMember member : activeMembers)
                {
                    if (member.id() != memberId)
                    {
                        consensusPublisher.commitPosition(
                            member.publication(), leadershipTermId, commitPosition, memberId);
                    }
                }

                this.commitPosition.setOrdered(commitPosition);
                timeOfLastLogUpdateNs = nowNs;

                clearUncommittedEntriesTo(commitPosition);
                if (pendingMemberRemovals > 0)
                {
                    handleMemberRemovals(commitPosition);
                }

                return 1;
            }
        }

        return 0;
    }

    private int updateFollowerPosition(final long nowNs)
    {
        final long recordedPosition = null != appendPosition ? appendPosition.get() : logRecordedPosition;
        final long position = Math.max(recordedPosition, lastAppendPosition);

        if ((recordedPosition > lastAppendPosition ||
            nowNs >= (timeOfLastAppendPositionNs + leaderHeartbeatIntervalNs)) &&
            consensusPublisher.appendPosition(leaderMember.publication(), leadershipTermId, position, memberId))
        {
            lastAppendPosition = position;
            timeOfLastAppendPositionNs = nowNs;
            return 1;
        }

        return 0;
    }

    private void clearSessionsAfter(final long logPosition)
    {
        for (final Iterator<ClusterSession> i = sessionByIdMap.values().iterator(); i.hasNext(); )
        {
            final ClusterSession session = i.next();
            if (session.openedLogPosition() > logPosition)
            {
                i.remove();
                egressPublisher.sendEvent(session, leadershipTermId, memberId, EventCode.CLOSED, "election");
                session.close(ctx.countedErrorHandler());
            }
        }

        for (final ClusterSession session : pendingSessions)
        {
            egressPublisher.sendEvent(session, leadershipTermId, memberId, EventCode.CLOSED, "election");
            session.close(ctx.countedErrorHandler());
        }

        pendingSessions.clear();
    }

    private void clearUncommittedEntriesTo(final long commitPosition)
    {
        if (uncommittedServiceMessages > 0)
        {
            pendingServiceMessageHeadOffset -= pendingServiceMessages.consume(
                leaderServiceSessionMessageSweeper, Integer.MAX_VALUE);
        }

        while (uncommittedTimers.peekLong() <= commitPosition)
        {
            uncommittedTimers.pollLong();
            uncommittedTimers.pollLong();
        }

        while (true)
        {
            final ClusterSession clusterSession = uncommittedClosedSessions.peekFirst();
            if (null == clusterSession || clusterSession.closedLogPosition() > commitPosition)
            {
                break;
            }

            uncommittedClosedSessions.pollFirst();
        }
    }

    private void restoreUncommittedEntries(final long commitPosition)
    {
        for (final LongArrayQueue.LongIterator i = uncommittedTimers.iterator(); i.hasNext(); )
        {
            final long appendPosition = i.nextValue();
            final long correlationId = i.nextValue();

            if (appendPosition > commitPosition)
            {
                timerService.scheduleTimerForCorrelationId(correlationId, timerService.currentTickTime());
            }
        }
        uncommittedTimers.clear();

        pendingServiceMessages.consume(followerServiceSessionMessageSweeper, Integer.MAX_VALUE);
        pendingServiceMessageHeadOffset = 0;

        if (uncommittedServiceMessages > 0)
        {
            pendingServiceMessages.consume(leaderServiceSessionMessageSweeper, Integer.MAX_VALUE);
            pendingServiceMessages.forEach(this::serviceSessionMessageReset, Integer.MAX_VALUE);
            uncommittedServiceMessages = 0;
        }

        ClusterSession session;
        while (null != (session = uncommittedClosedSessions.pollFirst()))
        {
            if (session.closedLogPosition() > commitPosition)
            {
                session.closedLogPosition(NULL_POSITION);
                session.state(CLOSING);
                sessionByIdMap.put(session.id(), session);
            }
        }
    }

    private void enterElection()
    {
        role(Cluster.Role.FOLLOWER);

        election = new Election(
            false,
            leadershipTermId,
            commitPosition.getWeak(),
            null != appendPosition ? appendPosition.get() : recoveryPlan.appendedLogPosition,
            activeMembers,
            clusterMemberByIdMap,
            thisMember,
            consensusPublisher,
            ctx,
            this);
    }

    private void idle()
    {
        checkInterruptStatus();
        aeronClientInvoker.invoke();
        if (aeron.isClosed())
        {
            throw new AgentTerminationException("unexpected Aeron close");
        }

        idleStrategy.idle();
        checkArchiveEvents(false);
    }

    private void idle(final int workCount)
    {
        checkInterruptStatus();
        aeronClientInvoker.invoke();
        if (aeron.isClosed())
        {
            throw new AgentTerminationException("unexpected Aeron close");
        }

        idleStrategy.idle(workCount);

        if (0 == workCount)
        {
            checkArchiveEvents(false);
        }
    }

    private static void checkInterruptStatus()
    {
        if (Thread.currentThread().isInterrupted())
        {
            throw new AgentTerminationException("interrupted");
        }
    }

    private void takeSnapshot(final long timestamp, final long logPosition, final ServiceAck[] serviceAcks)
    {
        final long recordingId;
        try (ExclusivePublication publication = aeron.addExclusivePublication(
            ctx.snapshotChannel(), ctx.snapshotStreamId()))
        {
            final String channel = ChannelUri.addSessionId(ctx.snapshotChannel(), publication.sessionId());
            archive.startRecording(channel, ctx.snapshotStreamId(), LOCAL, true);
            final CountersReader counters = aeron.countersReader();
            final int counterId = awaitRecordingCounter(counters, publication.sessionId());
            recordingId = RecordingPos.getRecordingId(counters, counterId);

            snapshotState(publication, logPosition, replayLeadershipTermId);
            awaitRecordingComplete(recordingId, publication.position(), counters, counterId);
        }
        catch (final ArchiveException ex)
        {
            if (ex.errorCode() == ArchiveException.STORAGE_SPACE)
            {
                ctx.countedErrorHandler().onError(ex);
                unexpectedTermination();
            }

            throw ex;
        }

        final long termBaseLogPosition = recordingLog.getTermEntry(replayLeadershipTermId).termBaseLogPosition;

        for (int serviceId = serviceAcks.length - 1; serviceId >= 0; serviceId--)
        {
            final long snapshotId = serviceAcks[serviceId].relevantId();
            recordingLog.appendSnapshot(
                snapshotId, replayLeadershipTermId, termBaseLogPosition, logPosition, timestamp, serviceId);
        }

        recordingLog.appendSnapshot(
            recordingId, replayLeadershipTermId, termBaseLogPosition, logPosition, timestamp, SERVICE_ID);

        recordingLog.force(ctx.fileSyncLevel());
        recoveryPlan = recordingLog.createRecoveryPlan(archive, ctx.serviceCount());
        ctx.snapshotCounter().incrementOrdered();

        final long nowNs = clusterClock.timeNanos();
        for (final ClusterSession session : sessionByIdMap.values())
        {
            session.timeOfLastActivityNs(nowNs);
        }
    }

    private void awaitRecordingComplete(
        final long recordingId, final long position, final CountersReader counters, final int counterId)
    {
        idleStrategy.reset();
        while (counters.getCounterValue(counterId) < position)
        {
            idle();

            if (!RecordingPos.isActive(counters, counterId, recordingId))
            {
                throw new ClusterException("recording has stopped unexpectedly: " + recordingId);
            }
        }
    }

    private int awaitRecordingCounter(final CountersReader counters, final int sessionId)
    {
        idleStrategy.reset();
        int counterId = RecordingPos.findCounterIdBySession(counters, sessionId);
        while (CountersReader.NULL_COUNTER_ID == counterId)
        {
            idle();
            counterId = RecordingPos.findCounterIdBySession(counters, sessionId);
        }

        return counterId;
    }

    private void snapshotState(
        final ExclusivePublication publication, final long logPosition, final long leadershipTermId)
    {
        final ConsensusModuleSnapshotTaker snapshotTaker = new ConsensusModuleSnapshotTaker(
            publication, idleStrategy, aeronClientInvoker);

        snapshotTaker.markBegin(SNAPSHOT_TYPE_ID, logPosition, leadershipTermId, 0, clusterTimeUnit, ctx.appVersion());

        snapshotTaker.snapshotConsensusModuleState(
            nextSessionId, nextServiceSessionId, logServiceSessionId, pendingServiceMessages.size());
        snapshotTaker.snapshotClusterMembers(memberId, highMemberId, activeMembers);

        for (final ClusterSession session : sessionByIdMap.values())
        {
            if (session.state() == OPEN || session.state() == CLOSED)
            {
                snapshotTaker.snapshotSession(session);
            }
        }

        timerService.snapshot(snapshotTaker);
        snapshotTaker.snapshot(pendingServiceMessages);

        snapshotTaker.markEnd(SNAPSHOT_TYPE_ID, logPosition, leadershipTermId, 0, clusterTimeUnit, ctx.appVersion());
    }

    private void clusterMemberJoined(final int memberId, final ClusterMember[] newMembers)
    {
        highMemberId = Math.max(highMemberId, memberId);

        final ClusterMember eventMember = ClusterMember.findMember(newMembers, memberId);
        if (null != eventMember)
        {
            if (null == eventMember.publication())
            {
                ClusterMember.addConsensusPublication(
                    eventMember, ChannelUri.parse(ctx.consensusChannel()), ctx.consensusStreamId(), aeron);
            }

            activeMembers = ClusterMember.addMember(activeMembers, eventMember);
            clusterMemberByIdMap.put(memberId, eventMember);
            rankedPositions = new long[ClusterMember.quorumThreshold(activeMembers.length)];
        }
    }

    private void clusterMemberQuit(final int memberId)
    {
        activeMembers = ClusterMember.removeMember(activeMembers, memberId);
        clusterMemberByIdMap.remove(memberId);
        rankedPositions = new long[ClusterMember.quorumThreshold(activeMembers.length)];
    }

    private void onUnavailableIngressImage(final Image image)
    {
        ingressAdapter.freeSessionBuffer(image.sessionId());
    }

    private void enqueueServiceSessionMessage(
        final MutableDirectBuffer buffer, final int offset, final int length, final long clusterSessionId)
    {
        final int headerOffset = offset - SessionMessageHeaderDecoder.BLOCK_LENGTH;
        final int clusterSessionIdOffset = headerOffset + SessionMessageHeaderDecoder.clusterSessionIdEncodingOffset();
        final int timestampOffset = headerOffset + SessionMessageHeaderDecoder.timestampEncodingOffset();

        buffer.putLong(clusterSessionIdOffset, clusterSessionId, SessionMessageHeaderDecoder.BYTE_ORDER);
        buffer.putLong(timestampOffset, Long.MAX_VALUE, SessionMessageHeaderDecoder.BYTE_ORDER);
        if (!pendingServiceMessages.append(buffer, offset - SESSION_HEADER_LENGTH, length + SESSION_HEADER_LENGTH))
        {
            throw new ClusterException("pending service message buffer capacity: " + pendingServiceMessages.size());
        }
    }

    private boolean serviceSessionMessageAppender(
        final MutableDirectBuffer buffer, final int offset, final int length, final int headOffset)
    {
        final int headerOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH;
        final int clusterSessionIdOffset = headerOffset + SessionMessageHeaderDecoder.clusterSessionIdEncodingOffset();
        final int timestampOffset = headerOffset + SessionMessageHeaderDecoder.timestampEncodingOffset();
        final long clusterSessionId = buffer.getLong(clusterSessionIdOffset, SessionMessageHeaderDecoder.BYTE_ORDER);

        final long appendPosition = logPublisher.appendMessage(
            leadershipTermId,
            clusterSessionId,
            clusterClock.time(),
            buffer,
            offset + SESSION_HEADER_LENGTH,
            length - SESSION_HEADER_LENGTH);

        if (appendPosition > 0)
        {
            ++uncommittedServiceMessages;
            logServiceSessionId = clusterSessionId;
            pendingServiceMessageHeadOffset = headOffset;
            buffer.putLong(timestampOffset, appendPosition, SessionMessageHeaderEncoder.BYTE_ORDER);

            return true;
        }

        return false;
    }

    private boolean serviceSessionMessageReset(
        final MutableDirectBuffer buffer, final int offset, final int length, final int headOffset)
    {
        final int timestampOffset = offset +
            MessageHeaderDecoder.ENCODED_LENGTH + SessionMessageHeaderDecoder.timestampEncodingOffset();
        final long appendPosition = buffer.getLong(timestampOffset, SessionMessageHeaderDecoder.BYTE_ORDER);

        if (appendPosition < Long.MAX_VALUE)
        {
            buffer.putLong(timestampOffset, Long.MAX_VALUE, SessionMessageHeaderEncoder.BYTE_ORDER);
            return true;
        }

        return false;
    }

    private boolean leaderServiceSessionMessageSweeper(
        final MutableDirectBuffer buffer, final int offset, final int length, final int headOffset)
    {
        final int timestampOffset = offset +
            MessageHeaderDecoder.ENCODED_LENGTH + SessionMessageHeaderDecoder.timestampEncodingOffset();
        final long appendPosition = buffer.getLong(timestampOffset, SessionMessageHeaderDecoder.BYTE_ORDER);

        if (appendPosition <= commitPosition.getWeak())
        {
            --uncommittedServiceMessages;
            return true;
        }

        return false;
    }

    private boolean followerServiceSessionMessageSweeper(
        final MutableDirectBuffer buffer, final int offset, final int length, final int headOffset)
    {
        final int clusterSessionIdOffset = offset +
            MessageHeaderDecoder.ENCODED_LENGTH + SessionMessageHeaderDecoder.clusterSessionIdEncodingOffset();

        return buffer.getLong(clusterSessionIdOffset, SessionMessageHeaderDecoder.BYTE_ORDER) <= logServiceSessionId;
    }

    private void onUnavailableCounter(final CountersReader counters, final long registrationId, final int counterId)
    {
        if (ConsensusModule.State.TERMINATING != state && ConsensusModule.State.QUITTING != state)
        {
            for (final long clientId : serviceClientIds)
            {
                if (registrationId == clientId)
                {
                    ctx.countedErrorHandler().onError(new ClusterException(
                        "Aeron client in service closed unexpectedly", WARN));
                    state(ConsensusModule.State.CLOSED);
                    return;
                }
            }

            if (null != appendPosition && appendPosition.registrationId() == registrationId)
            {
                appendPosition = null;
                logSubscriptionId = NULL_VALUE;

                if (null != election)
                {
                    election.handleError(
                        clusterClock.timeNanos(), new ClusterException("log recording ended unexpectedly", WARN));
                }
                else if (NULL_POSITION == terminationPosition)
                {
                    ctx.countedErrorHandler().onError(new ClusterException("log recording ended unexpectedly", WARN));
                    enterElection();
                }
            }
        }
    }

    private void closeAndTerminate()
    {
        tryStopLogRecording();
        state(ConsensusModule.State.CLOSED);
        terminateAgent(new ClusterTerminationException());
    }

    private void unexpectedTermination()
    {
        aeron.removeUnavailableCounterHandler(unavailableCounterHandlerRegistrationId);
        serviceProxy.terminationPosition(0, ctx.countedErrorHandler());
        tryStopLogRecording();
        state(ConsensusModule.State.CLOSED);
        terminateAgent(new ClusterTerminationException());
    }

    private void terminateAgent(final AgentTerminationException ex)
    {
        try
        {
            ctx.terminationHook().run();
        }
        catch (final Throwable t)
        {
            ctx.countedErrorHandler().onError(t);
        }

        throw ex;
    }

    private void tryStopLogRecording()
    {
        appendPosition = null;

        if (NULL_VALUE != logSubscriptionId && archive.archiveProxy().publication().isConnected())
        {
            try
            {
                archive.tryStopRecording(logSubscriptionId);
            }
            catch (final Exception ex)
            {
                ctx.countedErrorHandler().onError(new ClusterException(ex, WARN));
            }

            logSubscriptionId = NULL_VALUE;
        }
    }

    private void appendDynamicJoinTermAndSnapshots()
    {
        if (!dynamicJoinSnapshots.isEmpty())
        {
            final RecordingLog.Snapshot lastSnapshot = dynamicJoinSnapshots.get(dynamicJoinSnapshots.size() - 1);

            recordingLog.appendTerm(
                logRecordingId,
                lastSnapshot.leadershipTermId,
                lastSnapshot.termBaseLogPosition,
                lastSnapshot.timestamp);

            for (int i = dynamicJoinSnapshots.size() - 1; i >= 0; i--)
            {
                final RecordingLog.Snapshot snapshot = dynamicJoinSnapshots.get(i);

                recordingLog.appendSnapshot(
                    snapshot.recordingId,
                    snapshot.leadershipTermId,
                    snapshot.termBaseLogPosition,
                    snapshot.logPosition,
                    snapshot.timestamp,
                    snapshot.serviceId);
            }

            dynamicJoinSnapshots.clear();
        }
    }

    private void connectIngress()
    {
        if (!ctx.ingressChannel().contains(ENDPOINT_PARAM_NAME))
        {
            final ChannelUri ingressUri = ChannelUri.parse(ctx.ingressChannel());
            ingressUri.put(ENDPOINT_PARAM_NAME, thisMember.ingressEndpoint());

            ingressAdapter.connect(aeron.addSubscription(
                ingressUri.toString(), ctx.ingressStreamId(), null, this::onUnavailableIngressImage));
        }
        else if (Cluster.Role.LEADER == role)
        {
            ingressAdapter.connect(aeron.addSubscription(
                ctx.ingressChannel(), ctx.ingressStreamId(), null, this::onUnavailableIngressImage));
        }
    }
}
