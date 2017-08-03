package io.aeron.archive;

import io.aeron.Image;
import io.aeron.Publication;
import org.agrona.concurrent.EpochClock;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.mockito.Mockito.*;

public class ControlSessionTest
{
    private final Image mockImage = mock(Image.class);
    private final ArchiveConductor mockConductor = mock(ArchiveConductor.class);
    private final EpochClock mockEpochClock = mock(EpochClock.class);
    private final Publication mockControlPublication = mock(Publication.class);
    private final ControlSessionProxy mockProxy = mock(ControlSessionProxy.class);
    private ControlSession session;

    @Before
    public void before() throws Exception
    {
        session = new ControlSession(mockImage, mockConductor, mockEpochClock, mockProxy);
    }

    @Test
    public void shouldSequenceListRecordingsProcessing() throws Exception
    {
        when(mockControlPublication.isClosed()).thenReturn(false);
        when(mockControlPublication.isConnected()).thenReturn(true);
        when(mockConductor.newControlPublication("mock", 42)).thenReturn(mockControlPublication);
        when(mockImage.poll(any(), anyInt())).then(
            invocation ->
            {
                session.onConnect("mock", 42);
                return null;
            });
        session.doWork();

        final ListRecordingsSession mockListRecordingSession1 = mock(ListRecordingsSession.class);
        when(mockConductor.newListRecordingsSession(1, 2, 3, session))
            .thenReturn(mockListRecordingSession1);

        session.onListRecordings(1, 2, 3);
        verify(mockConductor).newListRecordingsSession(1,  2, 3, session);
        verify(mockConductor).addSession(mockListRecordingSession1);

        final ListRecordingsSession mockListRecordingSession2 = mock(ListRecordingsSession.class);
        when(mockConductor.newListRecordingsSession(2,  3, 4, session))
            .thenReturn(mockListRecordingSession2);

        session.onListRecordings(2, 3, 4);
        verify(mockConductor).newListRecordingsSession(2,  3, 4, session);
        verify(mockConductor, never()).addSession(mockListRecordingSession2);

        session.onListRecordingSessionClosed(mockListRecordingSession1);
        verify(mockConductor).addSession(mockListRecordingSession2);
        assertTrue(!session.isDone());
    }

    @Test
    public void shouldTimeoutIfNoOnConnectEvent()
    {
        when(mockEpochClock.time()).thenReturn(0L);
        session.doWork();
        verify(mockEpochClock, times(2)).time();

        when(mockEpochClock.time()).thenReturn(ControlSession.TIMEOUT_MS + 1L);
        session.doWork();
        assertTrue(session.isDone());
    }

    @Test
    public void shouldTimeoutIfConnectSentButPublicationNotConnected()
    {
        when(mockEpochClock.time()).thenReturn(0L);
        when(mockControlPublication.isClosed()).thenReturn(false);
        when(mockControlPublication.isConnected()).thenReturn(false);
        when(mockConductor.newControlPublication("mock", 42)).thenReturn(mockControlPublication);
        when(mockImage.poll(any(), anyInt())).then(
            invocation ->
            {
                session.onConnect("mock", 42);
                return null;
            });
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
        when(mockConductor.newControlPublication("mock", 42)).thenReturn(mockControlPublication);
        when(mockImage.poll(any(), anyInt())).then(
            invocation ->
            {
                session.onConnect("mock", 42);
                return null;
            });
        session.doWork();
        session.sendOkResponse(1L, mockProxy);
        session.doWork();

        when(mockEpochClock.time()).thenReturn(ControlSession.TIMEOUT_MS + 1L);
        session.doWork();
        assertTrue(session.isDone());
    }
}