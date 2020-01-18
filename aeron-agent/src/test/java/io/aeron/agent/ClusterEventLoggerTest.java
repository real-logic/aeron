/*
 * Copyright 2014-2020 Real Logic Limited.
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

import org.agrona.BitUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.concurrent.TimeUnit;

import static io.aeron.agent.ClusterEventCode.NEW_LEADERSHIP_TERM;
import static io.aeron.agent.ClusterEventCode.STATE_CHANGE;
import static io.aeron.agent.ClusterEventEncoder.SEPARATOR;
import static io.aeron.agent.ClusterEventLogger.toEventCodeId;
import static io.aeron.agent.Common.verifyLogHeader;
import static io.aeron.agent.CommonEventEncoder.LOG_HEADER_LENGTH;
import static io.aeron.agent.EventConfiguration.MAX_EVENT_LENGTH;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.concurrent.ringbuffer.RecordDescriptor.encodedMsgOffset;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ClusterEventLoggerTest
{
    private final UnsafeBuffer logBuffer = new UnsafeBuffer(
        allocateDirect(BitUtil.align(MAX_EVENT_LENGTH, 64) + TRAILER_LENGTH));
    private final ClusterEventLogger logger = new ClusterEventLogger(new ManyToOneRingBuffer(logBuffer));

    @ParameterizedTest
    @EnumSource(ClusterEventCode.class)
    void toEventCodeIdComputesEventId(final ClusterEventCode eventCode)
    {
        assertEquals(131072 + eventCode.id(), toEventCodeId(eventCode));
    }

    @Test
    void logNewLeadershipTerm()
    {
        final long logLeadershipTermId = 434;
        final long leadershipTermId = -500;
        final long logPosition = 43;
        final long timestamp = 2;
        final int leaderMemberId = 0;
        final int logSessionId = 3;
        final int captureLength = SIZE_OF_LONG * 4 + SIZE_OF_INT * 2;

        logger.logNewLeadershipTerm(
            logLeadershipTermId, leadershipTermId, logPosition, timestamp, leaderMemberId, logSessionId);

        verifyLogHeader(logBuffer, toEventCodeId(NEW_LEADERSHIP_TERM), captureLength, captureLength, captureLength);
        assertEquals(logLeadershipTermId, logBuffer.getLong(encodedMsgOffset(LOG_HEADER_LENGTH), LITTLE_ENDIAN));
        assertEquals(leadershipTermId,
            logBuffer.getLong(encodedMsgOffset(LOG_HEADER_LENGTH + SIZE_OF_LONG), LITTLE_ENDIAN));
        assertEquals(logPosition,
            logBuffer.getLong(encodedMsgOffset(LOG_HEADER_LENGTH + SIZE_OF_LONG * 2), LITTLE_ENDIAN));
        assertEquals(timestamp,
            logBuffer.getLong(encodedMsgOffset(LOG_HEADER_LENGTH + SIZE_OF_LONG * 3), LITTLE_ENDIAN));
        assertEquals(leaderMemberId,
            logBuffer.getInt(encodedMsgOffset(LOG_HEADER_LENGTH + SIZE_OF_LONG * 4), LITTLE_ENDIAN));
        assertEquals(logSessionId,
            logBuffer.getInt(encodedMsgOffset(LOG_HEADER_LENGTH + SIZE_OF_LONG * 4 + SIZE_OF_INT), LITTLE_ENDIAN));
    }

    @Test
    void logStateChange()
    {
        final TimeUnit from = MINUTES;
        final TimeUnit to = SECONDS;
        final String payload = from.name() + SEPARATOR + to.name();
        final int captureLength = SIZE_OF_INT * 2 + payload.length();

        logger.logStateChange(STATE_CHANGE, from, to, 42);

        verifyLogHeader(logBuffer, toEventCodeId(STATE_CHANGE), captureLength, captureLength, captureLength);
        assertEquals(42, logBuffer.getInt(encodedMsgOffset(LOG_HEADER_LENGTH), LITTLE_ENDIAN));
        assertEquals(payload, logBuffer.getStringAscii(encodedMsgOffset(LOG_HEADER_LENGTH + SIZE_OF_INT)));
    }
}
