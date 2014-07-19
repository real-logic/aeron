package uk.co.real_logic.aeron;

import org.junit.Before;
import org.mockito.stubbing.Answer;
import uk.co.real_logic.aeron.conductor.BufferManager;
import uk.co.real_logic.aeron.conductor.ManagedBuffer;
import uk.co.real_logic.aeron.util.TermHelper;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;

import java.io.IOException;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class MockBufferUsage
{
    public static final int MAX_FRAME_LENGTH = 1024;
    public static final int LOG_BUFFER_SZ = LogBufferDescriptor.LOG_MIN_SIZE;

    public static final long SESSION_ID_1 = 13L;
    public static final long SESSION_ID_2 = 15L;

    protected AtomicBuffer[] logBuffersSession1 = new AtomicBuffer[TermHelper.BUFFER_COUNT];
    protected AtomicBuffer[] logBuffersSession2 = new AtomicBuffer[TermHelper.BUFFER_COUNT];
    protected AtomicBuffer[] stateBuffersSession1 = new AtomicBuffer[TermHelper.BUFFER_COUNT];
    protected AtomicBuffer[] stateBuffersSession2 = new AtomicBuffer[TermHelper.BUFFER_COUNT];
    protected LogAppender[] appendersSession1 = new LogAppender[TermHelper.BUFFER_COUNT];
    protected LogAppender[] appendersSession2 = new LogAppender[TermHelper.BUFFER_COUNT];
    protected BufferManager mockBufferUsage = mock(BufferManager.class);


    @Before
    public void setupBuffers() throws IOException
    {
        for (int i = 0; i < TermHelper.BUFFER_COUNT; i++)
        {
            logBuffersSession1[i] = new AtomicBuffer(new byte[LOG_BUFFER_SZ]);
            stateBuffersSession1[i] = new AtomicBuffer(new byte[LogBufferDescriptor.STATE_BUFFER_LENGTH]);
            logBuffersSession2[i] = new AtomicBuffer(new byte[LOG_BUFFER_SZ]);
            stateBuffersSession2[i] = new AtomicBuffer(new byte[LogBufferDescriptor.STATE_BUFFER_LENGTH]);

            when(mockBufferUsage.newBuffer(eq(SESSION_ID_1 + "-log-" + i), anyInt(), anyInt()))
                    .thenAnswer(answer(logBuffersSession1[i]));
            when(mockBufferUsage.newBuffer(eq(SESSION_ID_1 + "-state-" + i), anyInt(), anyInt()))
                    .thenAnswer(answer(stateBuffersSession1[i]));
            when(mockBufferUsage.newBuffer(eq(SESSION_ID_2 + "-log-" + i), anyInt(), anyInt()))
                    .thenAnswer(answer(logBuffersSession2[i]));
            when(mockBufferUsage.newBuffer(eq(SESSION_ID_2 + "-state-" + i), anyInt(), anyInt()))
                    .thenAnswer(answer(stateBuffersSession2[i]));

            appendersSession1[i] = new LogAppender(logBuffersSession1[i], stateBuffersSession1[i],
                    DataHeaderFlyweight.DEFAULT_HEADER_NULL_IDS, MAX_FRAME_LENGTH);
            appendersSession2[i] = new LogAppender(logBuffersSession2[i], stateBuffersSession2[i],
                    DataHeaderFlyweight.DEFAULT_HEADER_NULL_IDS, MAX_FRAME_LENGTH);
        }
    }

    public Answer<ManagedBuffer> answer(final AtomicBuffer buffer)
    {
        return (invocation) ->
        {
            final Object[] args = invocation.getArguments();
            return  new ManagedBuffer((String) args[0], (int) args[1], (int) args[2], buffer, mockBufferUsage);
        };
    }

    public void verifyBuffersReleased(final long sessionId)
    {
        for (int i = 0; i < TermHelper.BUFFER_COUNT; i++)
        {
            verify(mockBufferUsage).releaseBuffers(eq(sessionId + "-log-" + i), anyInt(), anyInt());
        }
    }

    public void verifyBuffersNotReleased(final long sessionId)
    {
        for (int i = 0; i < TermHelper.BUFFER_COUNT; i++)
        {
            verify(mockBufferUsage, never()).releaseBuffers(eq(sessionId + "-log-" + i), anyInt(), anyInt());
        }
    }

}
