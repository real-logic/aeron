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
import io.aeron.Publication;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.cluster.codecs.ServiceAction;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.Before;
import org.junit.Test;

import static io.aeron.cluster.control.ClusterControl.ToggleState.*;
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
    private final Publication mockResponsePublication = mock(Publication.class);

    private final ConsensusModule.Context ctx = new ConsensusModule.Context()
        .errorCounter(mock(AtomicCounter.class))
        .errorHandler(Throwable::printStackTrace)
        .messageIndex(mock(Counter.class))
        .moduleState(mock(Counter.class))
        .controlToggle(mock(Counter.class))
        .aeron(mock(Aeron.class))
        .epochClock(new SystemEpochClock())
        .cachedEpochClock(new CachedEpochClock())
        .authenticatorSupplier(new DefaultAuthenticatorSupplier());

    @Before
    public void before()
    {
        when(mockEgressPublisher.sendEvent(any(), any(), any())).thenReturn(Boolean.TRUE);
        when(mockLogAppender.appendConnectedSession(any(), anyLong())).thenReturn(Boolean.TRUE);
        when(mockResponsePublication.isConnected()).thenReturn(true);
    }

    @Test
    public void shouldLimitActiveSessions()
    {
        ctx.maxConcurrentSessions(1);

        final SequencerAgent agent = newSequencerAgent();

        final long correlationIdOne = 1L;
        agent.onActionAck(0, 0, 0, ServiceAction.READY);
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
        agent.onActionAck(0, 0, 0, ServiceAction.READY);

        agent.onSessionConnect(correlationId, 2, RESPONSE_CHANNEL_ONE, new byte[0]);
        agent.doWork();

        verify(mockLogAppender).appendConnectedSession(any(ClusterSession.class), eq(startMs));

        final long timeMs = startMs + (ConsensusModule.Configuration.sessionTimeoutNs() / 1000);
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

        ctx.moduleState(mockState);

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

        ctx.controlToggle(mockControlToggle);

        final SequencerAgent agent = newSequencerAgent();
        assertThat((int)stateValue.get(), is(ConsensusModule.State.INIT.code()));

        agent.onActionAck(0, 0, 0, ServiceAction.READY);
        assertThat((int)stateValue.get(), is(ConsensusModule.State.ACTIVE.code()));

        controlValue.value = SUSPEND.code();
        agent.doWork();

        assertThat((int)stateValue.get(), is(ConsensusModule.State.SUSPENDED.code()));
        assertThat((int)controlValue.get(), is(NEUTRAL.code()));

        controlValue.value = RESUME.code();
        agent.doWork();

        assertThat((int)stateValue.get(), is(ConsensusModule.State.ACTIVE.code()));
        assertThat((int)controlValue.get(), is(NEUTRAL.code()));
    }

    @Test
    public void shouldAppendSnapshotRequest()
    {
        final MutableLong counterValue = new MutableLong(SNAPSHOT.code());
        final Counter mockControlToggle = mock(Counter.class);
        when(mockControlToggle.get()).thenReturn(counterValue.value);

        ctx.controlToggle(mockControlToggle);
        final SequencerAgent agent = newSequencerAgent();
        agent.onActionAck(0, 0, 0, ServiceAction.READY);

        when(mockLogAppender.appendActionRequest(eq(ServiceAction.SNAPSHOT), anyLong(), anyLong(), anyLong()))
            .thenReturn(Boolean.TRUE);

        agent.doWork();

        verify(mockLogAppender).appendActionRequest(eq(ServiceAction.SNAPSHOT), anyLong(), anyLong(), anyLong());
    }

    private SequencerAgent newSequencerAgent()
    {
        return new SequencerAgent(
            ctx,
            mockEgressPublisher,
            mockLogAppender,
            (sequencerAgent) -> mock(IngressAdapter.class),
            (sequencerAgent) -> mock(TimerService.class),
            (sessionId, responseStreamId, responseChannel) -> new ClusterSession(sessionId, mockResponsePublication),
            (sequencerAgent) -> mock(ConsensusModuleAdapter.class));
    }
}