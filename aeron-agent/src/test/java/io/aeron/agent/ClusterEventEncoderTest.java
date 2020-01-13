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

import io.aeron.cluster.Election;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import static io.aeron.agent.ClusterEventEncoder.SEPARATOR;
import static io.aeron.agent.CommonEventEncoder.LOG_HEADER_LENGTH;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.BufferUtil.allocateDirectAligned;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class ClusterEventEncoderTest
{
    private final UnsafeBuffer buffer = new UnsafeBuffer(allocateDirectAligned(256, 32));

    @Test
    void encodeStateChange()
    {
        final Election.State from = Election.State.CANDIDATE_BALLOT;
        final Election.State to = Election.State.CANVASS;
        final String payload = from.name() + SEPARATOR + to.name();
        final int captureLength = SIZE_OF_INT * 2 + payload.length();

        final int encodedLength = ClusterEventEncoder.encodeStateChange(buffer, from, to, 42);

        assertEquals(LOG_HEADER_LENGTH + captureLength, encodedLength);
        assertEquals(captureLength, buffer.getInt(0, LITTLE_ENDIAN));
        assertEquals(captureLength, buffer.getInt(SIZE_OF_INT, LITTLE_ENDIAN));
        assertNotEquals(0, buffer.getLong(SIZE_OF_INT * 2, LITTLE_ENDIAN));
        assertEquals(42, buffer.getInt(LOG_HEADER_LENGTH));
        assertEquals(payload, buffer.getStringAscii(LOG_HEADER_LENGTH + SIZE_OF_INT));
    }

    @Test
    void encodeNewLeadershipTerm()
    {
        final int logLeadershipTermId = 111;
        final int leadershipTermId = 222;
        final int logPosition = 1024;
        final int timestamp = 32423436;
        final int leaderMemberId = 42;
        final int logSessionId = 18;
        final int length = SIZE_OF_LONG * 4 + SIZE_OF_INT * 2;

        final int encodedLength = ClusterEventEncoder.encodeNewLeadershipTerm(
            buffer,
            logLeadershipTermId,
            leadershipTermId,
            logPosition,
            timestamp,
            leaderMemberId,
            logSessionId);

        assertEquals(LOG_HEADER_LENGTH + length, encodedLength);
        int offset = 0;
        assertEquals(length, buffer.getInt(offset, LITTLE_ENDIAN));
        offset += SIZE_OF_INT;
        assertEquals(length, buffer.getInt(offset, LITTLE_ENDIAN));
        offset += SIZE_OF_INT;
        assertNotEquals(0, buffer.getLong(offset, LITTLE_ENDIAN));
        offset += SIZE_OF_LONG;
        assertEquals(logLeadershipTermId, buffer.getLong(offset, LITTLE_ENDIAN));
        offset += SIZE_OF_LONG;
        assertEquals(leadershipTermId, buffer.getLong(offset, LITTLE_ENDIAN));
        offset += SIZE_OF_LONG;
        assertEquals(logPosition, buffer.getLong(offset, LITTLE_ENDIAN));
        offset += SIZE_OF_LONG;
        assertEquals(timestamp, buffer.getLong(offset, LITTLE_ENDIAN));
        offset += SIZE_OF_LONG;
        assertEquals(leaderMemberId, buffer.getInt(offset, LITTLE_ENDIAN));
        offset += SIZE_OF_INT;
        assertEquals(logSessionId, buffer.getInt(offset, LITTLE_ENDIAN));
    }
}