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
import io.aeron.cluster.codecs.ClusterAction;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.security.DefaultAuthenticatorSupplier;
import io.aeron.status.ReadableCounter;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.concurrent.TimeUnit;

import static io.aeron.cluster.ClusterControl.ToggleState.*;
import static io.aeron.cluster.ConsensusModule.Configuration.*;
import static io.aeron.cluster.ConsensusModuleAgent.SLOW_TICK_INTERVAL_NS;
import static io.aeron.cluster.client.AeronCluster.Configuration.PROTOCOL_SEMANTIC_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;
import static java.lang.Boolean.TRUE;

public class ConsensusModuleAgentTest
{
    private static final long SLOW_TICK_INTERVAL_MS = TimeUnit.NANOSECONDS.toMillis(SLOW_TICK_INTERVAL_NS);
    private static final String RESPONSE_CHANNEL_ONE = "aeron:udp?endpoint=localhost:11111";
    private static final String RESPONSE_CHANNEL_TWO = "aeron:udp?endpoint=localhost:22222";

    private final EgressPublisher mockEgressPublisher = mock(EgressPublisher.class);
    private final LogPublisher mockLogPublisher = mock(LogPublisher.class);
    private final Aeron mockAeron = mock(Aeron.class);
    private final ConcurrentPublication mockResponsePublication = mock(ConcurrentPublication.class);
    private final Counter mockTimedOutClientCounter = mock(Counter.class);

    private final ConsensusModule.Context ctx = new ConsensusModule.Context()
        .errorHandler(Throwable::printStackTrace)
        .errorCounter(mock(AtomicCounter.class))
        .moduleStateCounter(mock(Counter.class))
        .commitPositionCounter(mock(Counter.class))
        .controlToggleCounter(mock(Counter.class))
        .clusterNodeCounter(mock(Counter.class))
        .timedOutClientCounter(mockTimedOutClientCounter)
        .idleStrategySupplier(NoOpIdleStrategy::new)
        .aeron(mockAeron)
        .clusterMemberId(0)
        .authenticatorSupplier(new DefaultAuthenticatorSupplier())
        .clusterMarkFile(mock(ClusterMarkFile.class))
        .archiveContext(new AeronArchive.Context())
        .logPublisher(mockLogPublisher)
        .egressPublisher(mockEgressPublisher);

    @BeforeEach
    public void before()
    {
        when(mockAeron.conductorAgentInvoker()).thenReturn(mock(AgentInvoker.class));
        when(mockEgressPublisher.sendEvent(any(), anyLong(), anyInt(), any(), any())).thenReturn(TRUE);
        when(mockLogPublisher.appendSessionClose(any(), anyLong(), anyLong())).thenReturn(TRUE);
        when(mockLogPublisher.appendSessionOpen(any(), anyLong(), anyLong())).thenReturn(128L);
        when(mockLogPublisher.appendClusterAction(anyLong(), anyLong(), any(ClusterAction.class)))
            .thenReturn(TRUE);
        when(mockAeron.addPublication(anyString(), anyInt())).thenReturn(mockResponsePublication);
        when(mockAeron.addSubscription(anyString(), anyInt())).thenReturn(mock(Subscription.class));
        when(mockAeron.addSubscription(anyString(), anyInt(), eq(null), any(UnavailableImageHandler.class)))
            .thenReturn(mock(Subscription.class));
        when(mockResponsePublication.isConnected()).thenReturn(TRUE);
    }

    @Test
    public void shouldLimitActiveSessions()
    {
        final TestClusterClock clock = new TestClusterClock(TimeUnit.MILLISECONDS);
        ctx.maxConcurrentSessions(1)
            .epochClock(clock)
            .clusterClock(clock);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);

        final long correlationIdOne = 1L;
        agent.state(ConsensusModule.State.ACTIVE);
        agent.role(Cluster.Role.LEADER);
        agent.appendedPositionCounter(mock(ReadableCounter.class));
        agent.onSessionConnect(correlationIdOne, 2, PROTOCOL_SEMANTIC_VERSION, RESPONSE_CHANNEL_ONE, new byte[0]);

        clock.update(17, TimeUnit.MILLISECONDS);
        agent.doWork();

        verify(mockLogPublisher).appendSessionOpen(any(ClusterSession.class), anyLong(), anyLong());

        final long correlationIdTwo = 2L;
        agent.onSessionConnect(correlationIdTwo, 3, PROTOCOL_SEMANTIC_VERSION, RESPONSE_CHANNEL_TWO, new byte[0]);
        clock.update(clock.time() + 10L, TimeUnit.MILLISECONDS);
        agent.doWork();

        verify(mockEgressPublisher).sendEvent(
            any(ClusterSession.class), anyLong(), anyInt(), eq(EventCode.ERROR), eq(SESSION_LIMIT_MSG));
    }

    @Test
    public void shouldCloseInactiveSession()
    {
        final TestClusterClock clock = new TestClusterClock(TimeUnit.MILLISECONDS);
        final long startMs = SLOW_TICK_INTERVAL_MS;
        clock.update(startMs, TimeUnit.MILLISECONDS);

        ctx.epochClock(clock)
            .clusterClock(clock);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);

        final long correlationId = 1L;
        agent.state(ConsensusModule.State.ACTIVE);
        agent.role(Cluster.Role.LEADER);
        agent.appendedPositionCounter(mock(ReadableCounter.class));
        agent.onSessionConnect(correlationId, 2, PROTOCOL_SEMANTIC_VERSION, RESPONSE_CHANNEL_ONE, new byte[0]);

        agent.doWork();

        verify(mockLogPublisher).appendSessionOpen(any(ClusterSession.class), anyLong(), eq(startMs));

        final long timeMs = startMs + TimeUnit.NANOSECONDS.toMillis(ConsensusModule.Configuration.sessionTimeoutNs());
        clock.update(timeMs, TimeUnit.MILLISECONDS);
        agent.doWork();

        final long timeoutMs = timeMs + SLOW_TICK_INTERVAL_MS;
        clock.update(timeoutMs, TimeUnit.MILLISECONDS);
        agent.doWork();

        verify(mockTimedOutClientCounter).incrementOrdered();
        verify(mockLogPublisher).appendSessionClose(any(ClusterSession.class), anyLong(), eq(timeoutMs));
        verify(mockEgressPublisher).sendEvent(
            any(ClusterSession.class), anyLong(), anyInt(), eq(EventCode.ERROR), eq(SESSION_TIMEOUT_MSG));
    }

    @Test
    public void shouldCloseTerminatedSession()
    {
        final TestClusterClock clock = new TestClusterClock(TimeUnit.MILLISECONDS);
        final long startMs = SLOW_TICK_INTERVAL_MS;
        clock.update(startMs, TimeUnit.MILLISECONDS);

        ctx.epochClock(clock)
            .clusterClock(clock);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);

        final long correlationId = 1L;
        agent.state(ConsensusModule.State.ACTIVE);
        agent.role(Cluster.Role.LEADER);
        agent.appendedPositionCounter(mock(ReadableCounter.class));
        agent.onSessionConnect(correlationId, 2, PROTOCOL_SEMANTIC_VERSION, RESPONSE_CHANNEL_ONE, new byte[0]);

        agent.doWork();

        final ArgumentCaptor<ClusterSession> sessionCaptor = ArgumentCaptor.forClass(ClusterSession.class);

        verify(mockLogPublisher).appendSessionOpen(sessionCaptor.capture(), anyLong(), eq(startMs));

        final long timeMs = startMs + SLOW_TICK_INTERVAL_MS;
        clock.update(timeMs, TimeUnit.MILLISECONDS);
        agent.doWork();

        agent.onServiceCloseSession(sessionCaptor.getValue().id());

        verify(mockLogPublisher).appendSessionClose(any(ClusterSession.class), anyLong(), eq(timeMs));
        verify(mockEgressPublisher).sendEvent(
            any(ClusterSession.class), anyLong(), anyInt(), eq(EventCode.ERROR), eq(SESSION_TERMINATED_MSG));
    }

    @Test
    public void shouldSuspendThenResume()
    {
        final TestClusterClock clock = new TestClusterClock(TimeUnit.MILLISECONDS);

        final MutableLong stateValue = new MutableLong();
        final Counter mockState = mock(Counter.class);
        when(mockState.get()).thenAnswer((invocation) -> stateValue.value);
        doAnswer(
            (invocation) ->
            {
                stateValue.value = invocation.getArgument(0);
                return null;
            })
            .when(mockState).set(anyLong());

        final MutableLong controlValue = new MutableLong(NEUTRAL.code());
        final Counter mockControlToggle = mock(Counter.class);
        when(mockControlToggle.get()).thenAnswer((invocation) -> controlValue.value);

        doAnswer(
            (invocation) ->
            {
                controlValue.value = invocation.getArgument(0);
                return null;
            })
            .when(mockControlToggle).set(anyLong());

        doAnswer(
            (invocation) ->
            {
                final long expected = invocation.getArgument(0);
                if (expected == controlValue.value)
                {
                    controlValue.value = invocation.getArgument(1);
                    return true;
                }
                return false;
            })
            .when(mockControlToggle).compareAndSet(anyLong(), anyLong());

        ctx.moduleStateCounter(mockState);
        ctx.controlToggleCounter(mockControlToggle);
        ctx.epochClock(clock).clusterClock(clock);

        final ConsensusModuleAgent agent = new ConsensusModuleAgent(ctx);
        agent.appendedPositionCounter(mock(ReadableCounter.class));

        assertEquals(ConsensusModule.State.INIT.code(), stateValue.get());

        agent.state(ConsensusModule.State.ACTIVE);
        agent.role(Cluster.Role.LEADER);
        assertEquals(ConsensusModule.State.ACTIVE.code(), stateValue.get());

        SUSPEND.toggle(mockControlToggle);
        clock.update(SLOW_TICK_INTERVAL_MS, TimeUnit.MILLISECONDS);
        agent.doWork();

        assertEquals(ConsensusModule.State.SUSPENDED.code(), stateValue.get());
        assertEquals(SUSPEND.code(), controlValue.get());

        RESUME.toggle(mockControlToggle);
        clock.update(SLOW_TICK_INTERVAL_MS * 2, TimeUnit.MILLISECONDS);
        agent.doWork();

        assertEquals(ConsensusModule.State.ACTIVE.code(), stateValue.get());
        assertEquals(NEUTRAL.code(), controlValue.get());

        final InOrder inOrder = Mockito.inOrder(mockLogPublisher);
        inOrder.verify(mockLogPublisher).appendClusterAction(anyLong(), anyLong(), eq(ClusterAction.SUSPEND));
        inOrder.verify(mockLogPublisher).appendClusterAction(anyLong(), anyLong(), eq(ClusterAction.RESUME));
    }
}
