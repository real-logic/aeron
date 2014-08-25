package uk.co.real_logic.aeron;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.verification.VerificationMode;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogAppender;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor;
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.common.status.PositionIndicator;
import uk.co.real_logic.aeron.conductor.ClientConductor;
import uk.co.real_logic.aeron.conductor.ManagedBuffer;

import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.common.TermHelper.BUFFER_COUNT;
import static uk.co.real_logic.aeron.common.TermHelper.termIdToBufferIndex;
import static uk.co.real_logic.aeron.common.concurrent.broadcast.RecordDescriptor.RECORD_ALIGNMENT;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogAppender.AppendStatus.*;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.MIN_LOG_SIZE;

public class PublicationTest
{
    private static final String CHANNEL = "udp://localhost:40124";
    private static final int STREAM_ID_1 = 2;
    private static final int SESSION_ID_1 = 13;
    private static final int TERM_ID_1 = 1;
    private static final int SEND_BUFFER_CAPACITY = 1024;

    private final ByteBuffer sendBuffer = ByteBuffer.allocate(SEND_BUFFER_CAPACITY);
    private final AtomicBuffer atomicSendBuffer = new AtomicBuffer(sendBuffer);
    private final DataHeaderFlyweight dataHeaderFlyweight = new DataHeaderFlyweight();

    private Publication publication;
    private PositionIndicator limit;
    private LogAppender[] appenders;
    private byte[][] headers;
    private ManagedBuffer[] managedBuffers;

    @Before
    public void setUp()
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
            when(appenders[i].capacity()).thenReturn(MIN_LOG_SIZE);
        }

        managedBuffers = new ManagedBuffer[BUFFER_COUNT * 2];
        for (int i = 0; i < BUFFER_COUNT * 2; i++)
        {
            managedBuffers[i] = mock(ManagedBuffer.class);
        }

        publication = new Publication(
            conductor, CHANNEL, STREAM_ID_1, SESSION_ID_1, TERM_ID_1, appenders, limit, managedBuffers);
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
        when(appenders[termIdToBufferIndex(TERM_ID_1)].tailVolatile()).thenReturn(MIN_LOG_SIZE - RECORD_ALIGNMENT);
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

    @Test
    public void shouldUnmapBuffersWhenReleased() throws Exception
    {
        publication.release();
        verifyBuffersUnmapped(times(1));
    }

    @Test
    public void shouldNotUnmapBuffersBeforeLastRelease() throws Exception
    {
        publication.incRef();
        publication.release();
        verifyBuffersUnmapped(never());
    }

    @Test
    public void shouldUnmapBuffersWithMultipleReferences() throws Exception
    {
        publication.incRef();
        publication.release();

        publication.release();
        verifyBuffersUnmapped(times(1));
    }

    private void verifyBuffersUnmapped(final VerificationMode times) throws Exception
    {
        for (final ManagedBuffer buffer : managedBuffers)
        {
            verify(buffer, times).close();
        }
    }
}
