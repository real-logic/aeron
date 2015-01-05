/*
 * Copyright 2014 Real Logic Ltd.
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

package uk.co.real_logic.aeron;

import org.junit.Before;
import org.mockito.stubbing.Answer;
import uk.co.real_logic.aeron.common.TermHelper;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogAppender;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor;
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;

import java.io.IOException;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockBufferUsage
{
    protected static final int MAX_FRAME_LENGTH = 1024;
    protected static final int LOG_BUFFER_SZ = LogBufferDescriptor.MIN_LOG_SIZE;

    protected static final int SESSION_ID_1 = 13;
    protected static final int SESSION_ID_2 = 15;

    protected UnsafeBuffer[] logBuffersSession1 = new UnsafeBuffer[TermHelper.BUFFER_COUNT];
    protected UnsafeBuffer[] logBuffersSession2 = new UnsafeBuffer[TermHelper.BUFFER_COUNT];
    protected UnsafeBuffer[] stateBuffersSession1 = new UnsafeBuffer[TermHelper.BUFFER_COUNT];
    protected UnsafeBuffer[] stateBuffersSession2 = new UnsafeBuffer[TermHelper.BUFFER_COUNT];
    protected LogAppender[] appendersSession1 = new LogAppender[TermHelper.BUFFER_COUNT];
    protected LogAppender[] appendersSession2 = new LogAppender[TermHelper.BUFFER_COUNT];
    protected BufferManager mockBufferUsage = mock(BufferManager.class);

    @Before
    public void setupBuffers() throws IOException
    {
        for (int i = 0; i < TermHelper.BUFFER_COUNT; i++)
        {
            logBuffersSession1[i] = new UnsafeBuffer(new byte[LOG_BUFFER_SZ]);
            stateBuffersSession1[i] = new UnsafeBuffer(new byte[LogBufferDescriptor.STATE_BUFFER_LENGTH]);
            logBuffersSession2[i] = new UnsafeBuffer(new byte[LOG_BUFFER_SZ]);
            stateBuffersSession2[i] = new UnsafeBuffer(new byte[LogBufferDescriptor.STATE_BUFFER_LENGTH]);

            when(mockBufferUsage.mapBuffer(eq(SESSION_ID_1 + "-log-" + i), anyInt(), anyInt()))
                .thenAnswer(answer(logBuffersSession1[i]));
            when(mockBufferUsage.mapBuffer(eq(SESSION_ID_1 + "-state-" + i), anyInt(), anyInt()))
                .thenAnswer(answer(stateBuffersSession1[i]));
            when(mockBufferUsage.mapBuffer(eq(SESSION_ID_2 + "-log-" + i), anyInt(), anyInt()))
                .thenAnswer(answer(logBuffersSession2[i]));
            when(mockBufferUsage.mapBuffer(eq(SESSION_ID_2 + "-state-" + i), anyInt(), anyInt()))
                .thenAnswer(answer(stateBuffersSession2[i]));

            appendersSession1[i] = new LogAppender(
                logBuffersSession1[i], stateBuffersSession1[i],
                DataHeaderFlyweight.createDefaultHeader(0, 0, 0), MAX_FRAME_LENGTH);
            appendersSession2[i] = new LogAppender(
                logBuffersSession2[i], stateBuffersSession2[i],
                DataHeaderFlyweight.createDefaultHeader(0, 0, 0), MAX_FRAME_LENGTH);
        }
    }

    public Answer<ManagedBuffer> answer(final UnsafeBuffer buffer)
    {
        return (invocation) ->
        {
            final ManagedBuffer mockBuffer = mock(ManagedBuffer.class);
            when(mockBuffer.buffer()).thenReturn(buffer);
            return mockBuffer;
        };
    }
}
