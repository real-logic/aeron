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
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.codecs.EventCode;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.Before;
import org.junit.Test;

import static io.aeron.cluster.control.ClusterControl.Action.NEUTRAL;
import static io.aeron.cluster.control.ClusterControl.Action.RESUME;
import static io.aeron.cluster.control.ClusterControl.Action.SUSPEND;
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

    private final ConsensusModule.Context ctx = new ConsensusModule.Context()
        .errorCounter(mock(AtomicCounter.class))
        .errorHandler(Throwable::printStackTrace)
        .messageIndex(mock(Counter.class))
        .controlToggle(mock(Counter.class))
        .aeron(mock(Aeron.class))
        .epochClock(new SystemEpochClock())
        .cachedEpochClock(new CachedEpochClock());

    @Before
    public void before()
    {
        when(mockEgressPublisher.sendEvent(any(), any(), any())).thenReturn(Boolean.TRUE);
        when(mockLogAppender.appendConnectedSession(any(), anyLong())).thenReturn(Boolean.TRUE);
    }

    @Test
    public void shouldLimitActiveSessions()
    {
        ctx.maxConcurrentSessions(1);

        final SequencerAgent agent = newSequencerAgent();

        final long correlationIdOne = 1L;
        agent.onServiceReady(0L);
        agent.onSessionConnect(correlationIdOne, 2, RESPONSE_CHANNEL_ONE);
        agent.doWork();

        verify(mockLogAppender).appendConnectedSession(any(ClusterSession.class), anyLong());
        verify(mockEgressPublisher).sendEvent(any(ClusterSession.class), eq(EventCode.OK), eq(""));

        final long correlationIdTwo = 2L;
        agent.onSessionConnect(correlationIdTwo, 3, RESPONSE_CHANNEL_TWO);
        agent.doWork();

        verifyNoMoreInteractions(mockLogAppender);
        verify(mockEgressPublisher)
            .sendEvent(any(ClusterSession.class), eq(EventCode.ERROR), eq(SequencerAgent.SESSION_LIMIT_MSG));
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
        agent.onServiceReady(0L);

        agent.onSessionConnect(correlationId, 2, RESPONSE_CHANNEL_ONE);
        agent.doWork();

        verify(mockLogAppender).appendConnectedSession(any(ClusterSession.class), eq(startMs));

        final long timeMs = startMs + (ConsensusModule.Configuration.sessionTimeoutNs() / 1000);
        clock.update(timeMs);
        agent.doWork();

        verifyZeroInteractions(mockLogAppender);

        final long timeoutMs = timeMs + 1L;
        clock.update(timeoutMs);
        agent.doWork();

        verify(mockLogAppender).appendClosedSession(any(ClusterSession.class), eq(CloseReason.TIMEOUT), eq(timeoutMs));
        verify(mockEgressPublisher)
            .sendEvent(any(ClusterSession.class), eq(EventCode.ERROR), eq(SequencerAgent.SESSION_TIMEOUT_MSG));
    }

    @Test
    public void shouldTransitionStates()
    {
        final Counter mockControlToggle = mock(Counter.class);
        ctx.controlToggle(mockControlToggle);

        when(mockControlToggle.get()).thenReturn(NEUTRAL.code());

        final SequencerAgent agent = newSequencerAgent();
        assertThat(agent.state(), is(SequencerAgent.State.INIT));

        agent.onServiceReady(1L);
        assertThat(agent.state(), is(SequencerAgent.State.ACTIVE));

        when(mockControlToggle.get()).thenReturn(SUSPEND.code());
        agent.doWork();

        assertThat(agent.state(), is(SequencerAgent.State.SUSPENDED));
        verify(mockControlToggle).set(NEUTRAL.code());

        when(mockControlToggle.get()).thenReturn(RESUME.code());
        agent.doWork();

        assertThat(agent.state(), is(SequencerAgent.State.ACTIVE));
        verify(mockControlToggle, times(2)).set(NEUTRAL.code());
    }

    private SequencerAgent newSequencerAgent()
    {
        return new SequencerAgent(
            ctx,
            mockEgressPublisher,
            mockLogAppender,
            (sequencerAgent) -> mock(IngressAdapter.class),
            (sequencerAgent) -> mock(TimerService.class),
            (sessionId, responseStreamId, responseChannel) -> new ClusterSession(sessionId, null),
            (sequencerAgent) -> mock(ConsensusModuleAdapter.class));
    }
}