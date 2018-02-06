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
package io.aeron.cluster;

import io.aeron.*;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.codecs.*;
import io.aeron.cluster.service.*;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.status.ReadableCounter;
import org.agrona.*;
import org.agrona.collections.ArrayListUtil;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongArrayList;
import org.agrona.concurrent.*;
import org.agrona.concurrent.status.CountersReader;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.aeron.CommonContext.ENDPOINT_PARAM_NAME;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.cluster.ClusterSession.State.*;
import static io.aeron.cluster.ConsensusModule.Configuration.SESSION_TIMEOUT_MSG;
import static io.aeron.cluster.ConsensusModule.SNAPSHOT_TYPE_ID;

class SequencerAgent implements Agent
{
    private boolean isRecovering;
    private final int clusterMemberId;
    private int leaderMemberId;
    private int serviceAckCount = 0;
    private int logSessionId;
    private final long sessionTimeoutMs;
    private final long heartbeatIntervalMs;
    private final long heartbeatTimeoutMs;
    private long nextSessionId = 1;
    private long baseLogPosition = 0;
    private long leadershipTermId = -1;
    private long lastRecordingPosition = 0;
    private long timeOfLastLogUpdateMs = 0;
    private long followerCommitPosition = 0;
    private ReadableCounter logRecordingPosition;
    private Counter commitPosition;
    private ConsensusModule.State state = ConsensusModule.State.INIT;
    private Cluster.Role role;
    private ClusterMember[] clusterMembers;
    private long[] rankedPositions;
    private final Counter clusterRoleCounter;
    private final AgentInvoker aeronClientInvoker;
    private final EpochClock epochClock;
    private final CachedEpochClock cachedEpochClock;
    private final Counter moduleState;
    private final Counter controlToggle;
    private final TimerService timerService;
    private final ConsensusModuleAdapter consensusModuleAdapter;
    private final IngressAdapter ingressAdapter;
    private final EgressPublisher egressPublisher;
    private final LogAppender logAppender;
    private LogAdapter logAdapter;
    private final MemberStatusAdapter memberStatusAdapter;
    private final MemberStatusPublisher memberStatusPublisher = new MemberStatusPublisher();
    private final Long2ObjectHashMap<ClusterSession> sessionByIdMap = new Long2ObjectHashMap<>();
    private final ArrayList<ClusterSession> pendingSessions = new ArrayList<>();
    private final ArrayList<ClusterSession> rejectedSessions = new ArrayList<>();
    private final Authenticator authenticator;
    private final SessionProxy sessionProxy;
    private final Aeron aeron;
    private final ConsensusModule.Context ctx;
    private final MutableDirectBuffer tempBuffer;
    private final IdleStrategy idleStrategy;
    private final LongArrayList failedTimerCancellations = new LongArrayList();

    SequencerAgent(
        final ConsensusModule.Context ctx,
        final EgressPublisher egressPublisher,
        final LogAppender logAppender)
    {
        this.ctx = ctx;
        this.aeron = ctx.aeron();
        this.epochClock = ctx.epochClock();
        this.cachedEpochClock = ctx.cachedEpochClock();
        this.sessionTimeoutMs = TimeUnit.NANOSECONDS.toMillis(ctx.sessionTimeoutNs());
        this.heartbeatIntervalMs = TimeUnit.NANOSECONDS.toMillis(ctx.heartbeatIntervalNs());
        this.heartbeatTimeoutMs = TimeUnit.NANOSECONDS.toMillis(ctx.heartbeatTimeoutNs());
        this.egressPublisher = egressPublisher;
        this.moduleState = ctx.moduleStateCounter();
        this.controlToggle = ctx.controlToggleCounter();
        this.logAppender = logAppender;
        this.tempBuffer = ctx.tempBuffer();
        this.idleStrategy = ctx.idleStrategy();
        this.timerService = new TimerService(this);
        this.clusterMembers = ClusterMember.parse(ctx.clusterMembers());
        this.sessionProxy = new SessionProxy(egressPublisher);
        this.clusterMemberId = ctx.clusterMemberId();
        this.clusterRoleCounter = ctx.clusterNodeCounter();

        rankedPositions = new long[ClusterMember.quorumThreshold(clusterMembers.length)];
        role(Cluster.Role.FOLLOWER);

        final ChannelUri memberStatusUri = ChannelUri.parse(ctx.memberStatusChannel());
        memberStatusUri.put(ENDPOINT_PARAM_NAME, clusterMembers[clusterMemberId].memberFacingEndpoint());

        memberStatusAdapter = new MemberStatusAdapter(
            aeron.addSubscription(memberStatusUri.toString(), ctx.memberStatusStreamId()), this);

        final ChannelUri ingressUri = ChannelUri.parse(ctx.ingressChannel());
        if (!ingressUri.containsKey(ENDPOINT_PARAM_NAME))
        {
            ingressUri.put(ENDPOINT_PARAM_NAME, clusterMembers[clusterMemberId].clientFacingEndpoint());
        }

        ingressAdapter = new IngressAdapter(
            aeron.addSubscription(ingressUri.toString(), ctx.ingressStreamId()), this, ctx.invalidRequestCounter());

        consensusModuleAdapter = new ConsensusModuleAdapter(
            aeron.addSubscription(ctx.consensusModuleChannel(), ctx.consensusModuleStreamId()), this);

        authenticator = ctx.authenticatorSupplier().newAuthenticator(ctx);
        aeronClientInvoker = ctx.ownsAeronClient() ? ctx.aeron().conductorAgentInvoker() : null;
    }

    public void onClose()
    {
        if (!ctx.ownsAeronClient())
        {
            for (final ClusterSession session : sessionByIdMap.values())
            {
                session.close();
            }

            logAppender.disconnect();
            CloseHelper.close(memberStatusPublisher.publication());
            CloseHelper.close(memberStatusAdapter);

            CloseHelper.close(ingressAdapter);
            CloseHelper.close(consensusModuleAdapter);
        }
    }

    public void onStart()
    {
        try (AeronArchive archive = AeronArchive.connect(ctx.archiveContext()))
        {
            final RecordingLog.RecoveryPlan recoveryPlan = ctx.recordingLog().createRecoveryPlan(archive);

            serviceAckCount = 0;
            try (Counter ignore = addRecoveryStateCounter(recoveryPlan))
            {
                isRecovering = true;

                if (null != recoveryPlan.snapshotStep)
                {
                    recoverFromSnapshot(recoveryPlan.snapshotStep, archive);
                }

                waitForServiceAcks();

                if (recoveryPlan.termSteps.size() > 0)
                {
                    recoverFromLog(recoveryPlan.termSteps, archive);
                }

                isRecovering = false;
            }

            // TODO: handle suspended case
            state(ConsensusModule.State.ACTIVE);

            final long nowMs = epochClock.time();
            cachedEpochClock.update(nowMs);
            timeOfLastLogUpdateMs = nowMs;

            if (clusterMemberId == ctx.appointedLeaderId() || clusterMembers.length == 1)
            {
                leadershipTermId++;
                logSessionId = logAppender.connect(aeron, archive, ctx.logChannel(), ctx.logStreamId());
                becomeLeader(nowMs);
            }
            else
            {
                // TODO: record remote log
                logSessionId = connectLogAdapter(aeron, ctx.logChannel(), ctx.logStreamId());
                becomeFollower(nowMs, leaderMemberId);
            }

            final CountersReader counters = aeron.countersReader();
            logRecordingPosition = findLogRecording(logSessionId, counters);
            final long recordingId = RecordingPos.getRecordingId(counters, logRecordingPosition.counterId());

            commitPosition = CommitPos.allocate(
                aeron, tempBuffer, recordingId, baseLogPosition, leadershipTermId, logSessionId, -1);

            ctx.recordingLog().appendTerm(recordingId, leadershipTermId, baseLogPosition, nowMs, leaderMemberId);
        }
    }

    public int doWork()
    {
        int workCount = 0;

        boolean isSlowTickCycle = false;
        final long nowMs = epochClock.time();
        if (cachedEpochClock.time() != nowMs)
        {
            isSlowTickCycle = true;
            cachedEpochClock.update(nowMs);
        }

        switch (role)
        {
            case LEADER:
                if (ConsensusModule.State.ACTIVE == state)
                {
                    workCount += ingressAdapter.poll();
                }
                break;

            case FOLLOWER:
                if (ConsensusModule.State.ACTIVE == state || ConsensusModule.State.SUSPENDED == state)
                {
                    workCount += logAdapter.poll(followerCommitPosition);
                }
                break;
        }

        workCount += memberStatusAdapter.poll();
        workCount += updateMemberPosition(nowMs);

        if (isSlowTickCycle)
        {
            workCount += slowTickCycle(nowMs);
        }

        return workCount;
    }

    public String roleName()
    {
        return "sequencer";
    }

    public void onServiceActionAck(
        final long serviceId, final long logPosition, final long leadershipTermId, final ClusterAction action)
    {
        validateServiceAck(serviceId, logPosition, leadershipTermId, action);

        if (++serviceAckCount == ctx.serviceCount())
        {
            final long termPosition = logPosition - baseLogPosition;

            switch (action)
            {
                case SNAPSHOT:
                    ctx.snapshotCounter().incrementOrdered();
                    state(ConsensusModule.State.ACTIVE);
                    ClusterControl.ToggleState.reset(controlToggle);

                    final long nowNs = epochClock.time();
                    for (final ClusterSession session : sessionByIdMap.values())
                    {
                        session.timeOfLastActivityMs(nowNs);
                    }
                    break;

                case SHUTDOWN:
                    ctx.snapshotCounter().incrementOrdered();
                    ctx.recordingLog().commitLeadershipTermPosition(leadershipTermId, termPosition);
                    state(ConsensusModule.State.CLOSED);
                    ctx.terminationHook().run();
                    break;

                case ABORT:
                    ctx.recordingLog().commitLeadershipTermPosition(leadershipTermId, termPosition);
                    state(ConsensusModule.State.CLOSED);
                    ctx.terminationHook().run();
                    break;
            }
        }
        else if (serviceAckCount > ctx.serviceCount())
        {
            throw new IllegalStateException("Service count exceeded: " + serviceAckCount);
        }
    }

    public void onSessionConnect(
        final long correlationId,
        final int responseStreamId,
        final String responseChannel,
        final byte[] credentialData)
    {
        final long nowMs = cachedEpochClock.time();
        final long sessionId = nextSessionId++;
        final ClusterSession session = new ClusterSession(sessionId, responseStreamId, responseChannel);
        session.connect(aeron);
        session.lastActivity(nowMs, correlationId);

        if (pendingSessions.size() + sessionByIdMap.size() < ctx.maxConcurrentSessions())
        {
            authenticator.onConnectRequest(sessionId, credentialData, nowMs);
            pendingSessions.add(session);
        }
        else
        {
            rejectedSessions.add(session);
        }
    }

    public void onSessionClose(final long clusterSessionId)
    {
        final ClusterSession session = sessionByIdMap.get(clusterSessionId);
        if (null != session)
        {
            session.close();
            if (appendClosedSession(session, CloseReason.USER_ACTION, cachedEpochClock.time()))
            {
                sessionByIdMap.remove(clusterSessionId);
            }
        }
    }

    public ControlledFragmentAssembler.Action onSessionMessage(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final long clusterSessionId,
        final long correlationId)
    {
        final long nowMs = cachedEpochClock.time();
        final ClusterSession session = sessionByIdMap.get(clusterSessionId);
        if (null == session || (session.state() == TIMED_OUT || session.state() == CLOSED))
        {
            return ControlledFragmentHandler.Action.CONTINUE;
        }

        if (session.state() == OPEN && logAppender.appendMessage(buffer, offset, length, nowMs))
        {
            session.lastActivity(nowMs, correlationId);

            return ControlledFragmentHandler.Action.CONTINUE;
        }

        return ControlledFragmentHandler.Action.ABORT;
    }

    public void onKeepAlive(final long clusterSessionId)
    {
        final ClusterSession session = sessionByIdMap.get(clusterSessionId);
        if (null != session)
        {
            session.timeOfLastActivityMs(cachedEpochClock.time());
        }
    }

    public void onChallengeResponse(final long correlationId, final long clusterSessionId, final byte[] credentialData)
    {
        for (int lastIndex = pendingSessions.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final ClusterSession session = pendingSessions.get(i);

            if (session.id() == clusterSessionId && session.state() == CHALLENGED)
            {
                final long nowMs = cachedEpochClock.time();
                session.lastActivity(nowMs, correlationId);
                authenticator.onChallengeResponse(clusterSessionId, credentialData, nowMs);
                break;
            }
        }
    }

    public boolean onTimerEvent(final long correlationId, final long nowMs)
    {
        return logAppender.appendTimerEvent(correlationId, nowMs);
    }

    public void onScheduleTimer(final long correlationId, final long deadlineMs)
    {
        timerService.scheduleTimer(correlationId, deadlineMs);
    }

    public void onCancelTimer(final long correlationId)
    {
        timerService.cancelTimer(correlationId);
    }

    void state(final ConsensusModule.State state)
    {
        this.state = state;
        moduleState.set(state.code());
    }

    void role(final Cluster.Role role)
    {
        this.role = role;
        clusterRoleCounter.setOrdered(role.code());
    }

    void logRecordingPositionCounter(final ReadableCounter logRecordingPosition)
    {
        this.logRecordingPosition = logRecordingPosition;
    }

    void commitPositionCounter(final Counter commitPosition)
    {
        this.commitPosition = commitPosition;
    }

    @SuppressWarnings("unused")
    void onReplaySessionMessage(
        final long correlationId,
        final long clusterSessionId,
        final long timestamp,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        cachedEpochClock.update(timestamp);
        sessionByIdMap.get(clusterSessionId).lastActivity(timestamp, correlationId);
    }

    void onReplayTimerEvent(final long correlationId, final long timestamp)
    {
        cachedEpochClock.update(timestamp);

        if (!timerService.cancelTimer(correlationId))
        {
            failedTimerCancellations.addLong(correlationId);
        }
    }

    void onReplaySessionOpen(
        final long termPosition,
        final long correlationId,
        final long clusterSessionId,
        final long timestamp,
        final int responseStreamId,
        final String responseChannel)
    {
        cachedEpochClock.update(timestamp);

        addOpenSession(termPosition, clusterSessionId, correlationId, timestamp, responseStreamId, responseChannel);
    }

    void addOpenSession(
        final long openedTermPosition,
        final long clusterSessionId,
        final long correlationId,
        final long timestamp,
        final int responseStreamId,
        final String responseChannel)
    {
        final ClusterSession session = new ClusterSession(clusterSessionId, responseStreamId, responseChannel);
        session.open(openedTermPosition);
        session.lastActivity(timestamp, correlationId);

        sessionByIdMap.put(clusterSessionId, session);
    }

    @SuppressWarnings("unused")
    void onReplaySessionClose(
        final long correlationId,
        final long clusterSessionId,
        final long timestamp,
        final CloseReason closeReason)
    {
        cachedEpochClock.update(timestamp);
        sessionByIdMap.remove(clusterSessionId).close();
    }

    @SuppressWarnings("unused")
    void onReplayClusterAction(
        final long logPosition, final long leadershipTermId, final long timestamp, final ClusterAction action)
    {
        cachedEpochClock.update(timestamp);
        final long termPosition = logPosition - baseLogPosition;

        switch (action)
        {
            case SUSPEND:
                state(ConsensusModule.State.SUSPENDED);
                break;

            case RESUME:
                state(ConsensusModule.State.ACTIVE);
                break;

            case SNAPSHOT:
                if (!isRecovering)
                {
                    serviceAckCount = 0;
                    state(ConsensusModule.State.SNAPSHOT);
                    takeSnapshot(timestamp, termPosition);
                }
                break;

            case SHUTDOWN:
                if (!isRecovering)
                {
                    serviceAckCount = 0;
                    state(ConsensusModule.State.SHUTDOWN);
                    takeSnapshot(timestamp, termPosition);
                }
                break;

            case ABORT:
                if (!isRecovering)
                {
                    serviceAckCount = 0;
                    state(ConsensusModule.State.ABORT);
                }
                break;
        }
    }

    void onAppendedPosition(final long termPosition, final long leadershipTermId, final int memberId)
    {
        validateLeadershipTerm(leadershipTermId, "Append position not for current leadership term: expected=");

        clusterMembers[memberId].termPosition(termPosition);
    }

    void onCommitPosition(
        final long termPosition, final long leadershipTermId, final int leaderMemberId, final int logSessionId)
    {
        validateLeadershipTerm(leadershipTermId, "Quorum position not for current leadership term: expected=");

        if (leaderMemberId != this.leaderMemberId)
        {
            throw new IllegalStateException("Commit position not for current leader: expected=" +
                this.leaderMemberId + " received=" + leaderMemberId);
        }

        timeOfLastLogUpdateMs = cachedEpochClock.time();
        followerCommitPosition = termPosition;
    }

    void onAppliedPosition(final long termPosition, final long leadershipTermId, final int memberId)
    {
    }

    private int slowTickCycle(final long nowMs)
    {
        int workCount = 0;

        workCount += invokeAeronClient();
        workCount += consensusModuleAdapter.poll();

        if (Cluster.Role.LEADER == role)
        {
            workCount += checkControlToggle(nowMs);

            if (ConsensusModule.State.ACTIVE == state)
            {
                workCount += processPendingSessions(pendingSessions, nowMs);
                workCount += checkSessions(sessionByIdMap, nowMs);
                workCount += processRejectedSessions(rejectedSessions, nowMs);
                workCount += timerService.poll(nowMs);
            }
        }

        return workCount;
    }

    private int checkControlToggle(final long nowMs)
    {
        int workCount = 0;
        final ClusterControl.ToggleState toggleState = ClusterControl.ToggleState.get(controlToggle);

        switch (toggleState)
        {
            case SUSPEND:
                if (ConsensusModule.State.ACTIVE == state && appendAction(ClusterAction.SUSPEND, nowMs))
                {
                    state(ConsensusModule.State.SUSPENDED);
                    ClusterControl.ToggleState.reset(controlToggle);
                    workCount = 1;
                }
                break;

            case RESUME:
                if (ConsensusModule.State.SUSPENDED == state && appendAction(ClusterAction.RESUME, nowMs))
                {
                    state(ConsensusModule.State.ACTIVE);
                    ClusterControl.ToggleState.reset(controlToggle);
                    workCount = 1;
                }
                break;

            case SNAPSHOT:
                serviceAckCount = 0;
                if (ConsensusModule.State.ACTIVE == state && appendAction(ClusterAction.SNAPSHOT, nowMs))
                {
                    state(ConsensusModule.State.SNAPSHOT);
                    takeSnapshot(nowMs, logAppender.position());
                    workCount = 1;
                }
                break;

            case SHUTDOWN:
                serviceAckCount = 0;
                if (ConsensusModule.State.ACTIVE == state && appendAction(ClusterAction.SHUTDOWN, nowMs))
                {
                    state(ConsensusModule.State.SHUTDOWN);
                    takeSnapshot(nowMs, logAppender.position());
                    workCount = 1;
                }
                break;

            case ABORT:
                serviceAckCount = 0;
                if (ConsensusModule.State.ACTIVE == state && appendAction(ClusterAction.ABORT, nowMs))
                {
                    state(ConsensusModule.State.ABORT);
                    workCount = 1;
                }
                break;
        }

        return workCount;
    }

    private boolean appendAction(final ClusterAction action, final long nowMs)
    {
        final long position = baseLogPosition +
            logAppender.position() +
            MessageHeaderEncoder.ENCODED_LENGTH +
            ClusterActionRequestEncoder.BLOCK_LENGTH;

        return logAppender.appendClusterAction(action, leadershipTermId, position, nowMs);
    }

    private int processPendingSessions(final ArrayList<ClusterSession> pendingSessions, final long nowMs)
    {
        int workCount = 0;

        for (int lastIndex = pendingSessions.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final ClusterSession session = pendingSessions.get(i);

            if (session.state() == INIT || session.state() == CONNECTED)
            {
                if (session.responsePublication().isConnected())
                {
                    session.state(CONNECTED);
                    authenticator.onProcessConnectedSession(sessionProxy.session(session), nowMs);
                }
            }

            if (session.state() == CHALLENGED)
            {
                if (session.responsePublication().isConnected())
                {
                    authenticator.onProcessChallengedSession(sessionProxy.session(session), nowMs);
                }
            }

            if (session.state() == AUTHENTICATED)
            {
                ArrayListUtil.fastUnorderedRemove(pendingSessions, i, lastIndex);
                lastIndex--;

                session.timeOfLastActivityMs(nowMs);
                sessionByIdMap.put(session.id(), session);

                appendConnectedSession(session, nowMs);

                workCount += 1;
            }
            else if (session.state() == REJECTED)
            {
                ArrayListUtil.fastUnorderedRemove(pendingSessions, i, lastIndex);
                lastIndex--;

                rejectedSessions.add(session);
            }
            else if (nowMs > (session.timeOfLastActivityMs() + sessionTimeoutMs))
            {
                ArrayListUtil.fastUnorderedRemove(pendingSessions, i, lastIndex);
                lastIndex--;

                session.close();
            }
        }

        return workCount;
    }

    private int processRejectedSessions(final ArrayList<ClusterSession> rejectedSessions, final long nowMs)
    {
        int workCount = 0;

        for (int lastIndex = rejectedSessions.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final ClusterSession session = rejectedSessions.get(i);
            String detail = ConsensusModule.Configuration.SESSION_LIMIT_MSG;
            EventCode eventCode = EventCode.ERROR;

            if (session.state() == REJECTED)
            {
                detail = ConsensusModule.Configuration.SESSION_REJECTED_MSG;
                eventCode = EventCode.AUTHENTICATION_REJECTED;
            }

            if (egressPublisher.sendEvent(session, eventCode, detail) ||
                nowMs > (session.timeOfLastActivityMs() + sessionTimeoutMs))
            {
                ArrayListUtil.fastUnorderedRemove(rejectedSessions, i, lastIndex);
                lastIndex--;

                session.close();
                workCount++;
            }
        }

        return workCount;
    }

    private int checkSessions(final Long2ObjectHashMap<ClusterSession> sessionByIdMap, final long nowMs)
    {
        int workCount = 0;

        final Iterator<ClusterSession> iter = sessionByIdMap.values().iterator();
        while (iter.hasNext())
        {
            final ClusterSession session = iter.next();

            final ClusterSession.State state = session.state();
            if (nowMs > (session.timeOfLastActivityMs() + sessionTimeoutMs))
            {
                switch (state)
                {
                    case OPEN:
                        egressPublisher.sendEvent(session, EventCode.ERROR, SESSION_TIMEOUT_MSG);
                        if (appendClosedSession(session, CloseReason.TIMEOUT, nowMs))
                        {
                            iter.remove();
                            workCount += 1;
                        }
                        else
                        {
                            session.state(TIMED_OUT);
                        }
                        break;

                    case TIMED_OUT:
                    case CLOSED:
                        final CloseReason reason = state == TIMED_OUT ? CloseReason.TIMEOUT : CloseReason.USER_ACTION;
                        if (appendClosedSession(session, reason, nowMs))
                        {
                            iter.remove();
                            workCount += 1;
                        }
                        break;

                    default:
                        session.close();
                        iter.remove();
                }
            }
            else if (state == CONNECTED)
            {
                if (appendConnectedSession(session, nowMs))
                {
                    workCount += 1;
                }
            }
        }

        return workCount;
    }

    private boolean appendConnectedSession(final ClusterSession session, final long nowMs)
    {
        final long resultingPosition = logAppender.appendConnectedSession(session, nowMs);
        if (resultingPosition > 0)
        {
            session.open(resultingPosition);

            return true;
        }

        return false;
    }

    private boolean appendClosedSession(final ClusterSession session, final CloseReason closeReason, final long nowMs)
    {
        if (logAppender.appendClosedSession(session, closeReason, nowMs))
        {
            session.close();

            return true;
        }

        return false;
    }

    private void validateLeadershipTerm(final long leadershipTermId, final String msg)
    {
        if (leadershipTermId != this.leadershipTermId)
        {
            throw new IllegalStateException(msg + this.leadershipTermId + " received=" + leadershipTermId);
        }
    }

    private void becomeLeader(final long nowMs)
    {
        leaderMemberId = clusterMemberId;

        updateMemberDetails(leaderMemberId);
        role(Cluster.Role.LEADER);

        for (final ClusterSession session : sessionByIdMap.values())
        {
            session.connect(aeron);
            session.timeOfLastActivityMs(nowMs);
        }
    }

    private void becomeFollower(final long nowMs, final int leaderMemberId)
    {
        this.leaderMemberId = leaderMemberId;

        updateMemberDetails(leaderMemberId);
        role(Cluster.Role.FOLLOWER);
    }

    private void updateMemberDetails(final int leaderMemberId)
    {
        for (final ClusterMember clusterMember : clusterMembers)
        {
            clusterMember.isLeader(clusterMember.id() == leaderMemberId);
        }

        updateClusterMemberDetails(clusterMembers);
    }

    private void recoverFromSnapshot(final RecordingLog.ReplayStep snapshotStep, final AeronArchive archive)
    {
        final RecordingLog.Entry snapshot = snapshotStep.entry;

        cachedEpochClock.update(snapshot.timestamp);
        baseLogPosition = snapshot.logPosition;
        leadershipTermId = snapshot.leadershipTermId;

        final long recordingId = snapshot.recordingId;
        final RecordingExtent recordingExtent = new RecordingExtent();
        if (0 == archive.listRecording(recordingId, recordingExtent))
        {
            throw new IllegalStateException("Could not find recordingId: " + recordingId);
        }

        final String channel = ctx.replayChannel();
        final int streamId = ctx.replayStreamId();

        final long length = recordingExtent.stopPosition - recordingExtent.startPosition;
        final int sessionId = (int)archive.startReplay(recordingId, 0, length, channel, streamId);

        final String replaySubscriptionChannel = ChannelUri.addSessionId(channel, sessionId);
        try (Subscription subscription = aeron.addSubscription(replaySubscriptionChannel, streamId))
        {
            Image image;
            while ((image = subscription.imageBySessionId(sessionId)) == null)
            {
                idle();
            }

            final SnapshotLoader snapshotLoader = new SnapshotLoader(image, this);
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
                        throw new IllegalStateException("Snapshot ended unexpectedly");
                    }
                }

                idle(fragments);
            }
        }
    }

    private void recoverFromLog(final List<RecordingLog.ReplayStep> steps, final AeronArchive archive)
    {
        final String channel = ctx.replayChannel();
        final int streamId = ctx.replayStreamId();

        try (Subscription subscription = aeron.addSubscription(channel, streamId))
        {
            for (int i = 0, size = steps.size(); i < size; i++)
            {
                final RecordingLog.ReplayStep step = steps.get(0);
                final RecordingLog.Entry entry = step.entry;
                final long recordingId = entry.recordingId;
                final long startPosition = step.recordingStartPosition;
                final long stopPosition = step.recordingStopPosition;
                final long length = NULL_POSITION == stopPosition ? Long.MAX_VALUE : stopPosition - startPosition;
                final long logPosition = entry.logPosition;

                if (logPosition != baseLogPosition)
                {
                    throw new IllegalStateException("base position for log not as expected: expected " +
                        baseLogPosition + " actual " + logPosition);
                }
                leadershipTermId = entry.leadershipTermId;

                final int sessionId = (int)archive.startReplay(recordingId, startPosition, length, channel, streamId);

                idleStrategy.reset();
                Image image;
                while ((image = subscription.imageBySessionId(sessionId)) == null)
                {
                    idle();
                }

                serviceAckCount = 0;
                try (Counter counter = CommitPos.allocate(
                    aeron, tempBuffer, recordingId, logPosition, leadershipTermId, sessionId, i))
                {
                    counter.setOrdered(stopPosition);
                    replayTerm(image, stopPosition);
                    waitForServiceAcks();

                    baseLogPosition += image.position();

                    failedTimerCancellations.forEachOrderedLong(timerService::cancelTimer);
                    failedTimerCancellations.clear();
                }
            }

            failedTimerCancellations.trimToSize();
        }
    }

    private Counter addRecoveryStateCounter(final RecordingLog.RecoveryPlan plan)
    {
        final int termCount = plan.termSteps.size();
        final RecordingLog.ReplayStep snapshotStep = plan.snapshotStep;

        if (null != snapshotStep)
        {
            final RecordingLog.Entry snapshot = snapshotStep.entry;

            return RecoveryState.allocate(
                aeron, tempBuffer, snapshot.leadershipTermId, snapshot.termPosition, snapshot.timestamp, termCount);
        }

        return RecoveryState.allocate(aeron, tempBuffer, leadershipTermId, NULL_POSITION, 0, termCount);
    }

    private void waitForServiceAcks()
    {
        while (true)
        {
            final int fragmentsRead = consensusModuleAdapter.poll();
            if (serviceAckCount >= ctx.serviceCount())
            {
                break;
            }

            idle(fragmentsRead);
        }
    }

    private void validateServiceAck(
        final long serviceId, final long logPosition, final long leadershipTermId, final ClusterAction action)
    {
        final long currentLogPosition = baseLogPosition + logAppender.position();
        if (logPosition != currentLogPosition || leadershipTermId != this.leadershipTermId)
        {
            throw new IllegalStateException("Invalid log state:" +
                " serviceId=" + serviceId +
                ", logPosition=" + logPosition + " current is " + currentLogPosition +
                ", leadershipTermId=" + leadershipTermId + " current is " + this.leadershipTermId);
        }

        if (!state.isValid(action))
        {
            throw new IllegalStateException("Invalid action ack for state " + state + " action " + action);
        }
    }

    private void updateClusterMemberDetails(final ClusterMember[] members)
    {
        int leaderIndex = 0;
        for (int i = 0, length = members.length; i < length; i++)
        {
            if (members[i].isLeader())
            {
                leaderIndex = i;
                break;
            }
        }

        final StringBuilder builder = new StringBuilder(100);
        builder.append(members[leaderIndex].clientFacingEndpoint());

        for (int i = 0, length = members.length; i < length; i++)
        {
            if (i != leaderIndex)
            {
                builder.append(',').append(members[i].clientFacingEndpoint());
            }
        }

        sessionProxy.memberEndpointsDetail(builder.toString());
    }

    private ReadableCounter findLogRecording(final int sessionId, final CountersReader counters)
    {
        idleStrategy.reset();
        int recordingCounterId = RecordingPos.findCounterIdBySession(counters, sessionId);
        while (CountersReader.NULL_COUNTER_ID == recordingCounterId)
        {
            idle();
            recordingCounterId = RecordingPos.findCounterIdBySession(counters, sessionId);
        }

        return new ReadableCounter(counters, recordingCounterId);
    }

    private int updateMemberPosition(final long nowMs)
    {
        int workCount = 0;

        switch (role)
        {
            case LEADER:
            {
                clusterMembers[clusterMemberId].termPosition(logRecordingPosition.get());

                final long position = ClusterMember.quorumPosition(clusterMembers, rankedPositions);
                if (position > commitPosition.getWeak() || nowMs >= (timeOfLastLogUpdateMs + heartbeatIntervalMs))
                {
                    if (memberStatusPublisher.commitPosition(position, leadershipTermId, clusterMemberId, logSessionId))
                    {
                        commitPosition.setOrdered(position);
                        timeOfLastLogUpdateMs = nowMs;
                    }

                    workCount = 1;
                }
                break;
            }

            case FOLLOWER:
            {
                final long recordingPosition = logRecordingPosition.get();
                if (recordingPosition != lastRecordingPosition)
                {
                    if (memberStatusPublisher.appendedPosition(recordingPosition, leadershipTermId, clusterMemberId))
                    {
                        lastRecordingPosition = recordingPosition;
                    }

                    workCount = 1;
                }

                commitPosition.proposeMaxOrdered(logAdapter.position());

                if (nowMs >= (timeOfLastLogUpdateMs + heartbeatTimeoutMs))
                {
                    throw new AgentTerminationException("No heartbeat detected from cluster leader");
                }
                break;
            }
        }

        return workCount;
    }

    private void idle()
    {
        checkInterruptedStatus();
        idleStrategy.idle();
        invokeAeronClient();
    }

    private void idle(final int workCount)
    {
        checkInterruptedStatus();
        idleStrategy.idle(workCount);
        invokeAeronClient();
    }

    private static void checkInterruptedStatus()
    {
        if (Thread.currentThread().isInterrupted())
        {
            throw new RuntimeException("Unexpected interrupt");
        }
    }

    private int invokeAeronClient()
    {
        int workCount = 0;

        if (null != aeronClientInvoker)
        {
            workCount += aeronClientInvoker.invoke();
        }

        return workCount;
    }

    private void takeSnapshot(final long timestampMs, final long termPosition)
    {
        final long recordingId;
        final long logPosition = baseLogPosition + termPosition;
        final String channel = ctx.snapshotChannel();
        final int streamId = ctx.snapshotStreamId();

        try (AeronArchive archive = AeronArchive.connect(ctx.archiveContext());
            Publication publication = aeron.addExclusivePublication(channel, streamId))
        {
            final String recordingChannel = ChannelUri.addSessionId(channel, publication.sessionId());
            archive.startRecording(recordingChannel, streamId, SourceLocation.LOCAL);

            try
            {
                idleStrategy.reset();
                final CountersReader counters = aeron.countersReader();
                int counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId());
                while (CountersReader.NULL_COUNTER_ID == counterId)
                {
                    idle();
                    counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId());
                }

                recordingId = RecordingPos.getRecordingId(counters, counterId);
                snapshotState(publication, logPosition, leadershipTermId);

                do
                {
                    idle();

                    if (!RecordingPos.isActive(counters, counterId, recordingId))
                    {
                        throw new IllegalStateException("Recording has stopped unexpectedly: " + recordingId);
                    }
                }
                while (counters.getCounterValue(counterId) < publication.position());
            }
            finally
            {
                archive.stopRecording(recordingChannel, streamId);
            }
        }

        ctx.recordingLog().appendSnapshot(recordingId, leadershipTermId, baseLogPosition, termPosition, timestampMs);
    }

    private void snapshotState(final Publication publication, final long logPosition, final long leadershipTermId)
    {
        final ConsensusModuleSnapshotTaker snapshotTaker = new ConsensusModuleSnapshotTaker(
            publication, idleStrategy, aeronClientInvoker);

        snapshotTaker.markBegin(SNAPSHOT_TYPE_ID, logPosition, leadershipTermId, 0);

        for (final ClusterSession session : sessionByIdMap.values())
        {
            if (session.state() == OPEN)
            {
                snapshotTaker.snapshotSession(session);
            }
        }

        invokeAeronClient();

        timerService.snapshot(snapshotTaker);

        snapshotTaker.markEnd(SNAPSHOT_TYPE_ID, logPosition, leadershipTermId, 0);
    }

    private void replayTerm(final Image image, final long termLimit)
    {
        final LogAdapter logAdapter = new LogAdapter(image, this);

        while (true)
        {
            final int fragments = logAdapter.poll(termLimit);
            if (fragments == 0)
            {
                if (image.isClosed())
                {
                    if (!image.isEndOfStream())
                    {
                        throw new IllegalStateException("Unexpected close");
                    }

                    break;
                }
            }

            idle(fragments);
        }
    }

    private int connectLogAdapter(final Aeron aeron, final String logChannel, final int logStreamId)
    {
        return 0;
    }
}
