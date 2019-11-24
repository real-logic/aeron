package io.aeron.archive;

import io.aeron.Publication;
import io.aeron.security.Authenticator;
import org.agrona.concurrent.CachedEpochClock;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class ControlSessionTest
{
    private static final long CONNECT_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(5);

    private final ControlSessionDemuxer mockDemuxer = mock(ControlSessionDemuxer.class);
    private final ArchiveConductor mockConductor = mock(ArchiveConductor.class);
    private final Publication mockControlPublication = mock(Publication.class);
    private final ControlResponseProxy mockProxy = mock(ControlResponseProxy.class);
    private final ControlSessionProxy mockSessionProxy = mock(ControlSessionProxy.class);
    private final Authenticator mockAuthenticator = mock(Authenticator.class);
    private final CachedEpochClock cachedEpochClock = new CachedEpochClock();
    private ControlSession session;

    @Before
    public void before()
    {
        session = new ControlSession(
            0,
            1,
            2,
            CONNECT_TIMEOUT_MS,
            null,
            mockDemuxer,
            mockControlPublication,
            mockConductor,
            cachedEpochClock,
            mockProxy,
            mockAuthenticator,
            mockSessionProxy);
    }

    @Test
    public void shouldTimeoutIfConnectSentButPublicationNotConnected()
    {
        when(mockControlPublication.isClosed()).thenReturn(false);
        when(mockControlPublication.isConnected()).thenReturn(false);

        session.doWork();

        cachedEpochClock.update(CONNECT_TIMEOUT_MS + 1L);
        session.doWork();
        assertTrue(session.isDone());
    }

    @Test
    public void shouldTimeoutIfConnectSentButPublicationFailsToSend()
    {
        when(mockControlPublication.isClosed()).thenReturn(false);
        when(mockControlPublication.isConnected()).thenReturn(true);

        session.doWork();
        session.sendOkResponse(1L, mockProxy);
        session.doWork();

        cachedEpochClock.update(CONNECT_TIMEOUT_MS + 1L);
        session.doWork();
        assertTrue(session.isDone());
    }
}