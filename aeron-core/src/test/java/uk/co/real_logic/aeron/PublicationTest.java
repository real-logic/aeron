package uk.co.real_logic.aeron;

import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.aeron.conductor.ClientConductor;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender;
import uk.co.real_logic.aeron.util.status.PositionIndicator;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.co.real_logic.aeron.util.BufferRotationDescriptor.BUFFER_COUNT;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender.AppendStatus.SUCCESS;

public class PublicationTest
{
    public static final String DESTINATION = "udp://localhost:40124";
    public static final long CHANNEL_ID_1 = 2L;
    public static final long SESSION_ID_1 = 13L;
    public static final long TERM_ID_1 = 1L;
    public static final long TERM_ID_2 = 11L;
    public static final int PACKET_VALUE = 37;
    public static final int SEND_BUFFER_CAPACITY = 1024;

    private final ByteBuffer sendBuffer = ByteBuffer.allocate(SEND_BUFFER_CAPACITY);
    private final AtomicBuffer atomicSendBuffer = new AtomicBuffer(sendBuffer);

    private ClientConductor conductor;
    private Publication publication;
    private PositionIndicator indicator;
    private LogAppender[] appenders;

    @Before
    public void setup()
    {
        conductor = mock(ClientConductor.class);
        indicator = mock(PositionIndicator.class);

        appenders = new LogAppender[BUFFER_COUNT];
        for (int i = 0; i < BUFFER_COUNT; i++)
        {
            appenders[i] = mock(LogAppender.class);
            when(appenders[i].append(any(), anyInt(), anyInt())).thenReturn(SUCCESS);
        }

        publication = new Publication(
                conductor, DESTINATION, CHANNEL_ID_1,
                SESSION_ID_1, TERM_ID_1, appenders, indicator);
    }

    @Test
    public void canOfferAMessageUponConstruction() throws Exception
    {
        assertTrue(publication.offer(atomicSendBuffer));
    }

}
