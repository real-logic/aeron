/*
 * Copyright 2014-2021 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.agent;

import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static io.aeron.agent.AgentTests.verifyLogHeader;
import static io.aeron.agent.ClusterEventCode.NEW_LEADERSHIP_TERM;
import static io.aeron.agent.ClusterEventCode.STATE_CHANGE;
import static io.aeron.agent.CommonEventEncoder.LOG_HEADER_LENGTH;
import static io.aeron.agent.CommonEventEncoder.STATE_SEPARATOR;
import static io.aeron.agent.EventConfiguration.MAX_EVENT_LENGTH;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.agrona.BitUtil.*;
import static org.agrona.concurrent.ringbuffer.RecordDescriptor.ALIGNMENT;
import static org.agrona.concurrent.ringbuffer.RecordDescriptor.encodedMsgOffset;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TAIL_POSITION_OFFSET;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ClusterEventLoggerTest
{
    private static final int CAPACITY = align(MAX_EVENT_LENGTH, CACHE_LINE_LENGTH);
    private final UnsafeBuffer logBuffer = new UnsafeBuffer(allocateDirect(CAPACITY + TRAILER_LENGTH));
    private final ClusterEventLogger logger = new ClusterEventLogger(new ManyToOneRingBuffer(logBuffer));

    @Test
    void logNewLeadershipTerm()
    {
        final int offset = align(22, ALIGNMENT);
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);
        final long logLeadershipTermId = 434;
        final long logTruncatePosition = 256;
        final long leadershipTermId = -500;
        final long logPosition = 43;
        final long timestamp = 2;
        final int leaderMemberId = 0;
        final int logSessionId = 3;
        final int captureLength = SIZE_OF_LONG * 5 + SIZE_OF_INT * 3;
        final boolean isStartup = true;

        logger.logNewLeadershipTerm(
            logLeadershipTermId,
            logTruncatePosition,
            leadershipTermId,
            logPosition,
            timestamp,
            leaderMemberId,
            logSessionId,
            isStartup);

        verifyLogHeader(logBuffer, offset, NEW_LEADERSHIP_TERM.toEventCodeId(), captureLength, captureLength);
        int relativeOffset = LOG_HEADER_LENGTH;
        assertEquals(logLeadershipTermId, logBuffer.getLong(encodedMsgOffset(offset + relativeOffset), LITTLE_ENDIAN));
        relativeOffset += SIZE_OF_LONG;
        assertEquals(logTruncatePosition, logBuffer.getLong(encodedMsgOffset(offset + relativeOffset), LITTLE_ENDIAN));
        relativeOffset += SIZE_OF_LONG;
        assertEquals(leadershipTermId, logBuffer.getLong(encodedMsgOffset(offset + relativeOffset), LITTLE_ENDIAN));
        relativeOffset += SIZE_OF_LONG;
        assertEquals(logPosition, logBuffer.getLong(encodedMsgOffset(offset + relativeOffset), LITTLE_ENDIAN));
        relativeOffset += SIZE_OF_LONG;
        assertEquals(timestamp, logBuffer.getLong(encodedMsgOffset(offset + relativeOffset), LITTLE_ENDIAN));
        relativeOffset += SIZE_OF_LONG;
        assertEquals(leaderMemberId, logBuffer.getInt(encodedMsgOffset(offset + relativeOffset), LITTLE_ENDIAN));
        relativeOffset += SIZE_OF_INT;
        assertEquals(logSessionId, logBuffer.getInt(encodedMsgOffset(offset + relativeOffset), LITTLE_ENDIAN));
        relativeOffset += SIZE_OF_INT;
        assertEquals(isStartup, 1 == logBuffer.getInt(encodedMsgOffset(offset + relativeOffset), LITTLE_ENDIAN));
    }

    @Test
    void logStateChange()
    {
        final int offset = ALIGNMENT * 11;
        logBuffer.putLong(CAPACITY + TAIL_POSITION_OFFSET, offset);
        final TimeUnit from = MINUTES;
        final TimeUnit to = SECONDS;
        final int memberId = 42;
        final String payload = from.name() + STATE_SEPARATOR + to.name();
        final int captureLength = SIZE_OF_INT * 2 + payload.length();

        logger.logStateChange(STATE_CHANGE, from, to, memberId);

        verifyLogHeader(logBuffer, offset, STATE_CHANGE.toEventCodeId(), captureLength, captureLength);
        assertEquals(memberId, logBuffer.getInt(encodedMsgOffset(offset + LOG_HEADER_LENGTH), LITTLE_ENDIAN));
        assertEquals(payload, logBuffer.getStringAscii(encodedMsgOffset(offset + LOG_HEADER_LENGTH + SIZE_OF_INT)));
    }
}
