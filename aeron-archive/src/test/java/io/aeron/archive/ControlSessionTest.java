package io.aeron.archive;

import io.aeron.Publication;
import org.agrona.concurrent.EpochClock;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class ControlSessionTest
{
    private final ControlSessionDemuxer mockDemuxer = mock(ControlSessionDemuxer.class);
    private final ArchiveConductor mockConductor = mock(ArchiveConductor.class);
    private final EpochClock mockEpochClock = mock(EpochClock.class);
    private final Publication mockControlPublication = mock(Publication.class);
    private final ControlResponseProxy mockProxy = mock(ControlResponseProxy.class);
    private ControlSession session;

    @Before
    public void before()
    {
        session = new ControlSession(
            1,
            2,
            mockDemuxer,
            mockControlPublication,
            mockConductor,
            mockEpochClock,
            mockProxy);
    }

    @Test
    public void shouldTimeoutIfConnectSentButPublicationNotConnected()
    {
        when(mockEpochClock.time()).thenReturn(0L);
        when(mockControlPublication.isClosed()).thenReturn(false);
        when(mockControlPublication.isConnected()).thenReturn(false);

        session.doWork();

        when(mockEpochClock.time()).thenReturn(ControlSession.TIMEOUT_MS + 1L);
        session.doWork();
        assertTrue(session.isDone());
    }

    @Test
    public void shouldTimeoutIfConnectSentButPublicationFailsToSend()
    {
        when(mockEpochClock.time()).thenReturn(0L);
        when(mockControlPublication.isClosed()).thenReturn(false);
        when(mockControlPublication.isConnected()).thenReturn(true);

        session.doWork();
        session.sendOkResponse(1L, mockProxy);
        session.doWork();

        when(mockEpochClock.time()).thenReturn(ControlSession.TIMEOUT_MS + 1L);
        session.doWork();
        assertTrue(session.isDone());
    }
}