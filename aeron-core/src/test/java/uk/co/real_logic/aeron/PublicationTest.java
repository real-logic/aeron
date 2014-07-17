package uk.co.real_logic.aeron;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import uk.co.real_logic.aeron.conductor.ClientConductor;
import uk.co.real_logic.aeron.conductor.LogInformation;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.status.PositionIndicator;

import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.inOrder;
import static uk.co.real_logic.aeron.util.TermHelper.BUFFER_COUNT;
import static uk.co.real_logic.aeron.util.TermHelper.termIdToBufferIndex;
import static uk.co.real_logic.aeron.util.concurrent.broadcast.RecordDescriptor.RECORD_ALIGNMENT;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender.AppendStatus.*;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.LOG_MIN_SIZE;

public class PublicationTest
{
    public static final String DESTINATION = "udp://localhost:40124";
    public static final long CHANNEL_ID_1 = 2L;
    public static final long SESSION_ID_1 = 13L;
    public static final long TERM_ID_1 = 1L;
    public static final int SEND_BUFFER_CAPACITY = 1024;

    private final ByteBuffer sendBuffer = ByteBuffer.allocate(SEND_BUFFER_CAPACITY);
    private final AtomicBuffer atomicSendBuffer = new AtomicBuffer(sendBuffer);
    private final DataHeaderFlyweight dataHeaderFlyweight = new DataHeaderFlyweight();

    private Publication publication;
    private PositionIndicator limit;
    private LogAppender[] appenders;
    private byte[][] headers;

    @Before
    public void setup()
    {
        final ClientConductor conductor = mock(ClientConductor.class);
        limit = mock(PositionIndicator.class);
        when(limit.position()).thenReturn(2L * SEND_BUFFER_CAPACITY);

        appenders = new LogAppender[BUFFER_COUNT];
        headers = new byte[BUFFER_COUNT][];
        for (int i = 0; i < BUFFER_COUNT; i++)
        {
            appenders[i] = mock(LogAppender.class);
            byte[] header = new byte[DataHeaderFlyweight.HEADER_LENGTH];
            headers[i] = header;
            when(appenders[i].append(any(), anyInt(), anyInt())).thenReturn(SUCCESS);
            when(appenders[i].defaultHeader()).thenReturn(header);
            when(appenders[i].capacity()).thenReturn(LOG_MIN_SIZE);
        }

        final LogInformation[] logInformation = new LogInformation[0];

        publication = new Publication(conductor, DESTINATION, CHANNEL_ID_1, SESSION_ID_1,
                                      TERM_ID_1, appenders, limit, logInformation);
    }

    @Test
    public void shouldOfferAMessageUponConstruction()
    {
        assertTrue(publication.offer(atomicSendBuffer));
    }

    @Test
    public void shouldFailToOfferAMessageWhenLimited()
    {
        when(limit.position()).thenReturn(0L);
        assertFalse(publication.offer(atomicSendBuffer));
    }

    @Test
    public void shouldFailToOfferWhenAppendFails()
    {
        when(appenders[termIdToBufferIndex(TERM_ID_1)].append(any(), anyInt(), anyInt())).thenReturn(FAILURE);
        assertFalse(publication.offer(atomicSendBuffer));
    }

    @Test
    public void shouldRotateWhenAppendTrips()
    {
        when(appenders[termIdToBufferIndex(TERM_ID_1)].append(any(), anyInt(), anyInt())).thenReturn(TRIPPED);
        when(appenders[termIdToBufferIndex(TERM_ID_1)].tailVolatile()).thenReturn(LOG_MIN_SIZE - RECORD_ALIGNMENT);
        when(limit.position()).thenReturn(Long.MAX_VALUE);

        assertTrue(publication.offer(atomicSendBuffer));

        final InOrder inOrder = inOrder(appenders[0], appenders[1], appenders[2]);
        inOrder.verify(appenders[termIdToBufferIndex(TERM_ID_1 + 1)]).status();
        inOrder.verify(appenders[termIdToBufferIndex(TERM_ID_1 + 2)]).statusOrdered(LogBufferDescriptor.NEEDS_CLEANING);

        // written data to the next record
        inOrder.verify(appenders[termIdToBufferIndex(TERM_ID_1 + 1)])
               .append(atomicSendBuffer, 0, atomicSendBuffer.capacity());

        // updated the term id in the header
        dataHeaderFlyweight.wrap(headers[termIdToBufferIndex(TERM_ID_1 + 1)]);
        assertThat(dataHeaderFlyweight.termId(), is(TERM_ID_1 + 1));
    }
}
