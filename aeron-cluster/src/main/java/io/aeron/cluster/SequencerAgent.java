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
import io.aeron.cluster.codecs.*;
import io.aeron.cluster.control.ClusterControl;
import io.aeron.cluster.service.RecordingLog;
import io.aeron.cluster.service.RecoveryState;
import io.aeron.logbuffer.ControlledFragmentHandler;
import org.agrona.*;
import org.agrona.collections.ArrayListUtil;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static io.aeron.cluster.ClusterSession.State.*;
import static io.aeron.cluster.ConsensusModule.Configuration.SESSION_TIMEOUT_MSG;
import static io.aeron.cluster.service.RecordingLog.ENTRY_TYPE_SNAPSHOT;

class SequencerAgent implements Agent
{
    private final long sessionTimeoutMs;
    private long nextSessionId = 1;
    private long leadershipTermBeginPosition = 0;
    private int serviceAckCount = 0;
    private final Aeron aeron;
    private final AgentInvoker aeronClientInvoker;
    private final EpochClock epochClock;
    private final CachedEpochClock cachedEpochClock;
    private final TimerService timerService;
    private final ConsensusModuleAdapter consensusModuleAdapter;
    private final IngressAdapter ingressAdapter;
    private final EgressPublisher egressPublisher;
    private final AeronArchive aeronArchive;
    private final LogAppender logAppender;
    private final Counter messageIndex;
    private final Counter moduleState;
    private final Counter controlToggle;
    private final ClusterSessionSupplier sessionSupplier;
    private final Long2ObjectHashMap<ClusterSession> sessionByIdMap = new Long2ObjectHashMap<>();
    private final ArrayList<ClusterSession> pendingSessions = new ArrayList<>();
    private final ArrayList<ClusterSession> rejectedSessions = new ArrayList<>();
    private final ConsensusModule.Context ctx;
    private final Authenticator authenticator;
    private final SessionProxy sessionProxy;
    private final MutableDirectBuffer tempBuffer;
    private ConsensusTracker consensusTracker;
    private ConsensusModule.State state = ConsensusModule.State.INIT;

    SequencerAgent(
        final ConsensusModule.Context ctx,
        final EgressPublisher egressPublisher,
        final AeronArchive aeronArchive,
        final LogAppender logAppender,
        final IngressAdapterSupplier ingressAdapterSupplier,
        final TimerServiceSupplier timerServiceSupplier,
        final ClusterSessionSupplier clusterSessionSupplier,
        final ConsensusModuleAdapterSupplier consensusModuleAdapterSupplier)
    {
        this.ctx = ctx;
        this.aeron = ctx.aeron();
        this.epochClock = ctx.epochClock();
        this.cachedEpochClock = ctx.cachedEpochClock();
        this.sessionTimeoutMs = ctx.sessionTimeoutNs() / 1000;
        this.egressPublisher = egressPublisher;
        this.messageIndex = ctx.messageIndex();
        this.moduleState = ctx.moduleState();
        this.controlToggle = ctx.controlToggle();
        this.aeronArchive = aeronArchive;
        this.logAppender = logAppender;
        this.sessionSupplier = clusterSessionSupplier;
        this.sessionProxy = new SessionProxy(egressPublisher);
        this.tempBuffer = ctx.tempBuffer();

        ingressAdapter = ingressAdapterSupplier.newIngressAdapter(this);
        timerService = timerServiceSupplier.newTimerService(this);
        consensusModuleAdapter = consensusModuleAdapterSupplier.newConsensusModuleAdapter(this);
        authenticator = ctx.authenticatorSupplier().newAuthenticator(this.ctx);
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

            CloseHelper.close(ingressAdapter);
            CloseHelper.close(consensusModuleAdapter);
        }
    }

    public void onStart()
    {
        final RecordingLog recordingLog = ctx.recordingLog();
        final List<RecordingLog.ReplayStep> recoverySteps = recordingLog.createRecoveryPlan(aeronArchive);

        try (Counter ignore = createRecoveryStateCounter(recoverySteps))
        {
            recoverFromSnapshot(recoverySteps);
            recoverFromLog(recoverySteps);
        }

        final long timestamp = epochClock.time();
        cachedEpochClock.update(timestamp);

        final long messageIndex = this.messageIndex.getWeak();
        final long position = leadershipTermBeginPosition;

        consensusTracker = new ConsensusTracker(
            aeron, ctx.tempBuffer(), position, messageIndex, logAppender.sessionId(), ctx.idleStrategy());

        recordingLog.appendTerm(consensusTracker.recordingId(), position, messageIndex, timestamp);
    }

    public int doWork()
    {
        int workCount = 0;

        final long nowMs = epochClock.time();
        cachedEpochClock.update(nowMs);

        if (null != aeronClientInvoker)
        {
            workCount += aeronClientInvoker.invoke();
        }

        if (null != consensusTracker)
        {
            consensusTracker.updatePosition();
        }

        workCount += checkControlToggle(nowMs);
        workCount += consensusModuleAdapter.poll();

        if (ConsensusModule.State.ACTIVE == state)
        {
            workCount += processPendingSessions(pendingSessions, nowMs);
            workCount += timerService.poll(nowMs);
            workCount += ingressAdapter.poll();
            workCount += checkSessions(sessionByIdMap, nowMs);
        }

        processRejectedSessions(rejectedSessions, nowMs);

        return workCount;
    }

    public String roleName()
    {
        return "sequencer";
    }

    public void onActionAck(
        final long serviceId, final long logPosition, final long messageIndex, final ServiceAction action)
    {
        final long currentLogPosition = leadershipTermBeginPosition + logAppender.position();
        final long currentMessageIndex = this.messageIndex.getWeak();
        if (logPosition != currentLogPosition || messageIndex != currentMessageIndex)
        {
            throw new IllegalStateException("Invalid log state:" +
                " serviceId=" + serviceId +
                ", logPosition=" + logPosition + " current log position is " + currentLogPosition +
                ", messageIndex=" + messageIndex + " current message index is " + currentMessageIndex);
        }

        if (!state.isValid(action))
        {
            throw new IllegalStateException("Invalid action ack for state " + state + " action " + action);
        }

        if (++serviceAckCount == ctx.serviceCount())
        {
            switch (action)
            {
                case INIT:
                    state(ConsensusModule.State.REPLAY);
                    serviceAckCount = 0;
                    break;

                case REPLAY:
                    state(ConsensusModule.State.ACTIVE);
                    serviceAckCount = 0;
                    break;

                case SNAPSHOT:
                    state(ConsensusModule.State.ACTIVE);
                    ClusterControl.ToggleState.reset(controlToggle);
                    break;

                case SHUTDOWN:
                    state(ConsensusModule.State.CLOSED);
                    ctx.shutdownSignalBarrier().signal();
                    break;

                case ABORT:
                    state(ConsensusModule.State.CLOSED);
                    ctx.shutdownSignalBarrier().signal();
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
        final ClusterSession session = sessionSupplier.newClusterSession(sessionId, responseStreamId, responseChannel);
        session.lastActivity(nowMs, correlationId);

        authenticator.onConnectRequest(sessionId, credentialData, nowMs);

        if (pendingSessions.size() + sessionByIdMap.size() < ctx.maxConcurrentSessions())
        {
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
            messageIndex.incrementOrdered();
            session.lastActivity(nowMs, correlationId);

            return ControlledFragmentHandler.Action.CONTINUE;
        }

        return ControlledFragmentHandler.Action.ABORT;
    }

    public void onKeepAlive(final long correlationId, final long clusterSessionId)
    {
        final ClusterSession session = sessionByIdMap.get(clusterSessionId);
        if (null != session)
        {
            session.lastActivity(cachedEpochClock.time(), correlationId);
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
        if (logAppender.appendTimerEvent(correlationId, nowMs))
        {
            messageIndex.incrementOrdered();

            return true;
        }

        return false;
    }

    public void onScheduleTimer(final long correlationId, final long deadlineMs)
    {
        timerService.scheduleTimer(correlationId, deadlineMs);
    }

    public void onCancelTimer(final long correlationId)
    {
        timerService.cancelTimer(correlationId);
    }

    private void recoverFromSnapshot(final List<RecordingLog.ReplayStep> recoverySteps)
    {
        final IdleStrategy idleStrategy = ctx.idleStrategy();
        waitForStateChange(ConsensusModule.State.REPLAY, idleStrategy);
    }

    private void recoverFromLog(final List<RecordingLog.ReplayStep> recoverySteps)
    {
        final IdleStrategy idleStrategy = ctx.idleStrategy();
        waitForStateChange(ConsensusModule.State.ACTIVE, idleStrategy);
    }

    private Counter createRecoveryStateCounter(final List<RecordingLog.ReplayStep> recoverySteps)
    {
        if (recoverySteps.isEmpty())
        {
            return RecoveryState.allocate(aeron, tempBuffer, 0, 0, 0, 0);
        }

        if (recoverySteps.get(0).entry.entryType == ENTRY_TYPE_SNAPSHOT)
        {
            final RecordingLog.Entry snapshot = recoverySteps.get(0).entry;
            final int replayTermCount = recoverySteps.size() - 1;
            cachedEpochClock.update(snapshot.timestamp);
            leadershipTermBeginPosition = snapshot.logPosition;
            messageIndex.setOrdered(snapshot.messageIndex);

            return RecoveryState.allocate(
                aeron, tempBuffer, snapshot.logPosition, snapshot.messageIndex, snapshot.timestamp, replayTermCount);
        }

        return RecoveryState.allocate(aeron, tempBuffer, 0, 0, 0, recoverySteps.size());
    }

    private void checkInterruptedStatus()
    {
        if (Thread.currentThread().isInterrupted())
        {
            throw new RuntimeException("Unexpected interrupt");
        }
    }

    private void waitForStateChange(final ConsensusModule.State expectedState, final IdleStrategy idleStrategy)
    {
        while (true)
        {
            final int fragmentsRead = consensusModuleAdapter.poll();
            if (expectedState == state)
            {
                break;
            }

            checkInterruptedStatus();
            idleStrategy.idle(fragmentsRead);

            if (null != aeronClientInvoker)
            {
                aeronClientInvoker.invoke();
            }
        }
    }

    private void loadSnapshot(final long recordingId)
    {
    }

    private void state(final ConsensusModule.State state)
    {
        this.state = state;
        moduleState.set(state.code());
    }

    private int checkControlToggle(final long nowMs)
    {
        int workCount = 0;
        final ClusterControl.ToggleState toggleState = ClusterControl.ToggleState.get(controlToggle);

        switch (toggleState)
        {
            case SUSPEND:
                if (ConsensusModule.State.ACTIVE == state)
                {
                    state(ConsensusModule.State.SUSPENDED);
                    ClusterControl.ToggleState.reset(controlToggle);
                    workCount = 1;
                }
                break;

            case RESUME:
                if (ConsensusModule.State.SUSPENDED == state)
                {
                    state(ConsensusModule.State.ACTIVE);
                    ClusterControl.ToggleState.reset(controlToggle);
                    workCount = 1;
                }
                break;

            case SNAPSHOT:
                if (ConsensusModule.State.ACTIVE == state && appendSnapshot(nowMs))
                {
                    state(ConsensusModule.State.SNAPSHOT);
                    serviceAckCount = 0;
                    workCount = 1;
                }
                break;

            case SHUTDOWN:
                if (ConsensusModule.State.ACTIVE == state && appendShutdown(nowMs))
                {
                    state(ConsensusModule.State.SHUTDOWN);
                    serviceAckCount = 0;
                    workCount = 1;
                }
                break;

            case ABORT:
                if (ConsensusModule.State.ACTIVE == state && appendAbort(nowMs))
                {
                    state(ConsensusModule.State.ABORT);
                    serviceAckCount = 0;
                    workCount = 1;
                }
                break;
        }

        return workCount;
    }

    private boolean appendSnapshot(final long nowMs)
    {
        final long position = resultingServiceActionPosition();

        if (logAppender.appendActionRequest(ServiceAction.SNAPSHOT, messageIndex.getWeak(), position, nowMs))
        {
            messageIndex.incrementOrdered();
            return true;
        }

        return false;
    }

    private boolean appendShutdown(final long nowMs)
    {
        final long position = resultingServiceActionPosition();

        if (logAppender.appendActionRequest(ServiceAction.SHUTDOWN, messageIndex.getWeak(), position, nowMs))
        {
            messageIndex.incrementOrdered();
            return true;
        }

        return false;
    }

    private boolean appendAbort(final long nowMs)
    {
        final long position = resultingServiceActionPosition();

        if (logAppender.appendActionRequest(ServiceAction.ABORT, messageIndex.getWeak(), position, nowMs))
        {
            messageIndex.incrementOrdered();
            return true;
        }

        return false;
    }

    private long resultingServiceActionPosition()
    {
        return leadershipTermBeginPosition +
            logAppender.position() +
            MessageHeaderEncoder.ENCODED_LENGTH +
            ServiceActionRequestEncoder.BLOCK_LENGTH;
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
                    sessionProxy.clusterSession(session);
                    authenticator.onProcessConnectedSession(sessionProxy, nowMs);
                }
            }

            if (session.state() == CHALLENGED)
            {
                if (session.responsePublication().isConnected())
                {
                    sessionProxy.clusterSession(session);
                    authenticator.onProcessChallengedSession(sessionProxy, nowMs);
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

    private void processRejectedSessions(final ArrayList<ClusterSession> rejectedSessions, final long nowMs)
    {
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
            }
        }
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
        if (logAppender.appendConnectedSession(session, nowMs))
        {
            messageIndex.incrementOrdered();
            session.state(ClusterSession.State.OPEN);

            return true;
        }

        return false;
    }

    private boolean appendClosedSession(final ClusterSession session, final CloseReason closeReason, final long nowMs)
    {
        if (logAppender.appendClosedSession(session, closeReason, nowMs))
        {
            messageIndex.incrementOrdered();
            session.close();

            return true;
        }

        return false;
    }
}
