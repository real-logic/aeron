package io.aeron.archiver;

import io.aeron.Image;
import io.aeron.Publication;
import org.agrona.concurrent.EpochClock;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class ControlSessionTest
{
    private ControlSession session;
    private Image mockImage = mock(Image.class);
    private ArchiveConductor mockConductor = mock(ArchiveConductor.class);
    private EpochClock mockEpochClock = mock(EpochClock.class);
    private Publication mockControlPublication = mock(Publication.class);

    @Before
    public void before() throws Exception
    {
        session = new ControlSession(mockImage, mockConductor, mockEpochClock);
        when(mockControlPublication.isClosed()).thenReturn(false);
        when(mockControlPublication.isConnected()).thenReturn(true);
        when(mockConductor.newControlPublication("mock", 42)).thenReturn(mockControlPublication);
        session.onConnect("mock", 42);
        session.doWork();
    }

    @Test
    public void shouldSequenceListRecordingsProcessing() throws Exception
    {
        final ListRecordingsSession mockListRecordingSession1 = mock(ListRecordingsSession.class);
        when(mockConductor.newListRecordingsSession(1, mockControlPublication, 2, 3, session))
            .thenReturn(mockListRecordingSession1);

        session.onListRecordings(1, 2, 3);
        verify(mockConductor).newListRecordingsSession(1, mockControlPublication, 2, 3, session);
        verify(mockConductor).addSession(mockListRecordingSession1);

        final ListRecordingsSession mockListRecordingSession2 = mock(ListRecordingsSession.class);
        when(mockConductor.newListRecordingsSession(2, mockControlPublication, 3, 4, session))
            .thenReturn(mockListRecordingSession2);

        session.onListRecordings(2, 3, 4);
        verify(mockConductor).newListRecordingsSession(2, mockControlPublication, 3, 4, session);
        verify(mockConductor, never()).addSession(mockListRecordingSession2);

        session.onListRecordingSessionClosed(mockListRecordingSession1);
        verify(mockConductor).addSession(mockListRecordingSession2);
    }
}