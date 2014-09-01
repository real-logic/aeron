package uk.co.real_logic.aeron;

import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.aeron.common.TermHelper;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogReader;
import uk.co.real_logic.aeron.common.status.PositionReporter;

import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.Connection.HEADER_LENGTH;
import static uk.co.real_logic.aeron.common.TermHelper.BUFFER_COUNT;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogReader.FrameHandler;

public class SubscriptionTest
{
    private static final String CHANNEL = "udp://localhost:40124";
    private static final int STREAM_ID_1 = 2;
    private static final int SESSION_ID_1 = 13;
    private static final int SESSION_ID_2 = 14;
    private static final int TERM_ID_1 = 1;
    private static final int ACTIVE_INDEX = TermHelper.termIdToBufferIndex(TERM_ID_1);
    private static final long SUBSCRIPTION_CORRELATION_ID = 100;
    private static final long CONNECTION_CORRELATION_ID = 101;
    private static final int READ_BUFFER_CAPACITY = 1024;
    public static final byte FLAGS = (byte) 0;
    public static final int FRAGMENT_COUNT_LIMIT = Integer.MAX_VALUE;

    private final ByteBuffer readBuffer = ByteBuffer.allocate(READ_BUFFER_CAPACITY);
    private final AtomicBuffer atomicReadBuffer = new AtomicBuffer(readBuffer);
    private final ClientConductor conductor = mock(ClientConductor.class);
    private final PositionReporter reporter = mock(PositionReporter.class);
    private final DataHandler dataHandler = mock(DataHandler.class);

    private Subscription subscription;
    private LogReader[] readers;
    private ManagedBuffer[] managedBuffers;

    @Before
    public void setUp()
    {
        readers = new LogReader[BUFFER_COUNT];
        for (int i = 0; i < BUFFER_COUNT; i++)
        {
            readers[i] = mock(LogReader.class);
            when(readers[i].isComplete()).thenReturn(false);
            when(readers[i].read(any(), anyInt())).thenReturn(0);
        }

        managedBuffers = new ManagedBuffer[BUFFER_COUNT * 2];
        for (int i = 0; i < BUFFER_COUNT * 2; i++)
        {
            managedBuffers[i] = mock(ManagedBuffer.class);
        }

        subscription = new Subscription(conductor, dataHandler, CHANNEL, STREAM_ID_1, SUBSCRIPTION_CORRELATION_ID);
    }

    @Test
    public void shouldReadNothingWithNoConnections()
    {
        assertThat(subscription.poll(1), is(0));
    }

    @Test
    public void shouldReadNothingWhenThereIsNoData()
    {
        onTermBuffersMapped(SESSION_ID_1);

        assertThat(subscription.poll(1), is(0));
    }

    @Test
    public void shouldReadData()
    {
        onTermBuffersMapped(SESSION_ID_1);

        when(readers[ACTIVE_INDEX].read(any(), anyInt())).then(
            (invocation) ->
            {
                FrameHandler handler = (FrameHandler) invocation.getArguments()[0];
                handler.onFrame(atomicReadBuffer, 0, READ_BUFFER_CAPACITY);
                return 1;
            });

        assertThat(subscription.poll(FRAGMENT_COUNT_LIMIT), is(1));
        verify(dataHandler).onData(atomicReadBuffer, HEADER_LENGTH, READ_BUFFER_CAPACITY - HEADER_LENGTH, SESSION_ID_1, FLAGS);
    }

    @Test
    public void shouldReadDataFromMultipleSources()
    {
        onTermBuffersMapped(SESSION_ID_1);
        onTermBuffersMapped(SESSION_ID_2);

        when(readers[ACTIVE_INDEX].read(any(), anyInt())).then(
            (invocation) ->
            {
                FrameHandler handler = (FrameHandler) invocation.getArguments()[0];
                handler.onFrame(atomicReadBuffer, 0, READ_BUFFER_CAPACITY);
                return 1;
            });

        assertThat(subscription.poll(FRAGMENT_COUNT_LIMIT), is(2));
    }

    private void onTermBuffersMapped(final int sessionId1)
    {
        subscription.onTermBuffersMapped(
            sessionId1, TERM_ID_1, 0, CONNECTION_CORRELATION_ID, readers, reporter, managedBuffers);
    }
}
