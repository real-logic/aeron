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

import io.aeron.Aeron;
import io.aeron.Counter;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.codecs.EventCode;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.*;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static io.aeron.cluster.ClusterControl.ToggleState.*;
import static java.lang.Boolean.TRUE;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class SequencerAgentTest
{
    private static final String RESPONSE_CHANNEL_ONE = "responseChannelOne";
    private static final String RESPONSE_CHANNEL_TWO = "responseChannelTwo";

    private final EgressPublisher mockEgressPublisher = mock(EgressPublisher.class);
    private final LogAppender mockLogAppender = mock(LogAppender.class);
    private final Aeron mockAeron = mock(Aeron.class);
    private final ExclusivePublication mockResponsePublication = mock(ExclusivePublication.class);
    private final Subscription mockConsensusModuleSubscription = mock(Subscription.class);

    private final ConsensusModule.Context ctx = new ConsensusModule.Context()
        .errorCounter(mock(AtomicCounter.class))
        .errorHandler(Throwable::printStackTrace)
        .moduleState(mock(Counter.class))
        .controlToggle(mock(Counter.class))
        .idleStrategySupplier(NoOpIdleStrategy::new)
        .aeron(mockAeron)
        .epochClock(new SystemEpochClock())
        .cachedEpochClock(new CachedEpochClock())
        .authenticatorSupplier(new DefaultAuthenticatorSupplier());

    @Before
    public void before()
    {
        when(mockEgressPublisher.sendEvent(any(), any(), any())).thenReturn(TRUE);
        when(mockLogAppender.appendConnectedSession(any(), anyLong())).thenReturn(TRUE);
        when(mockAeron.addExclusivePublication(anyString(), anyInt())).thenReturn(mockResponsePublication);
        when(mockAeron.addSubscription(anyString(), anyInt())).thenReturn(mockConsensusModuleSubscription);
        when(mockResponsePublication.isConnected()).thenReturn(TRUE);
    }

    @Test
    public void shouldLimitActiveSessions()
    {
        ctx.maxConcurrentSessions(1);

        final SequencerAgent agent = newSequencerAgent();

        final long correlationIdOne = 1L;
        agent.state(ConsensusModule.State.ACTIVE);
        agent.onSessionConnect(correlationIdOne, 2, RESPONSE_CHANNEL_ONE, new byte[0]);
        agent.doWork();

        verify(mockLogAppender).appendConnectedSession(any(ClusterSession.class), anyLong());
        verify(mockEgressPublisher).sendEvent(any(ClusterSession.class), eq(EventCode.OK), eq(""));

        final long correlationIdTwo = 2L;
        agent.onSessionConnect(correlationIdTwo, 3, RESPONSE_CHANNEL_TWO, new byte[0]);
        agent.doWork();

        verify(mockEgressPublisher).sendEvent(
            any(ClusterSession.class), eq(EventCode.ERROR), eq(ConsensusModule.Configuration.SESSION_LIMIT_MSG));
    }

    @Test
    public void shouldCloseInactiveSession()
    {
        final CachedEpochClock clock = new CachedEpochClock();
        final long startMs = 7L;
        clock.update(startMs);

        ctx.epochClock(clock);

        final SequencerAgent agent = newSequencerAgent();

        final long correlationId = 1L;
        agent.state(ConsensusModule.State.ACTIVE);

        agent.onSessionConnect(correlationId, 2, RESPONSE_CHANNEL_ONE, new byte[0]);
        agent.doWork();

        verify(mockLogAppender).appendConnectedSession(any(ClusterSession.class), eq(startMs));

        final long timeMs = startMs + TimeUnit.NANOSECONDS.toMillis(ConsensusModule.Configuration.sessionTimeoutNs());
        clock.update(timeMs);
        agent.doWork();

        final long timeoutMs = timeMs + 1L;
        clock.update(timeoutMs);
        agent.doWork();

        verify(mockLogAppender).appendClosedSession(any(ClusterSession.class), eq(CloseReason.TIMEOUT), eq(timeoutMs));
        verify(mockEgressPublisher).sendEvent(
            any(ClusterSession.class), eq(EventCode.ERROR), eq(ConsensusModule.Configuration.SESSION_TIMEOUT_MSG));
    }

    @Test
    public void shouldSuspendThenResume()
    {
        final CachedEpochClock clock = new CachedEpochClock();

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

        ctx.moduleState(mockState);
        ctx.controlToggle(mockControlToggle);
        ctx.epochClock(clock);

        final SequencerAgent agent = newSequencerAgent();
        assertThat((int)stateValue.get(), is(ConsensusModule.State.INIT.code()));

        agent.state(ConsensusModule.State.ACTIVE);
        assertThat((int)stateValue.get(), is(ConsensusModule.State.ACTIVE.code()));

        controlValue.value = SUSPEND.code();
        clock.update(1);
        agent.doWork();

        assertThat((int)stateValue.get(), is(ConsensusModule.State.SUSPENDED.code()));
        assertThat((int)controlValue.get(), is(NEUTRAL.code()));

        controlValue.value = RESUME.code();
        clock.update(2);
        agent.doWork();

        assertThat((int)stateValue.get(), is(ConsensusModule.State.ACTIVE.code()));
        assertThat((int)controlValue.get(), is(NEUTRAL.code()));
    }

    private SequencerAgent newSequencerAgent()
    {
        return new SequencerAgent(ctx, mockEgressPublisher, mockLogAppender);
    }
}