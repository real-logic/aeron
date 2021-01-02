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
import org.junit.jupiter.api.Test;

import static io.aeron.agent.ClusterEventCode.ELECTION_STATE_CHANGE;
import static io.aeron.agent.ClusterEventCode.NEW_LEADERSHIP_TERM;
import static io.aeron.agent.ClusterEventDissector.CONTEXT;
import static io.aeron.agent.CommonEventEncoder.LOG_HEADER_LENGTH;
import static io.aeron.agent.CommonEventEncoder.internalEncodeLogHeader;
import static io.aeron.agent.EventConfiguration.MAX_EVENT_LENGTH;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ClusterEventDissectorTest
{
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[MAX_EVENT_LENGTH]);
    private final StringBuilder builder = new StringBuilder();

    @Test
    void dissectNewLeadershipTerm()
    {
        internalEncodeLogHeader(buffer, 0, 8, 9, () -> 33_000_000_000L);
        buffer.putLong(LOG_HEADER_LENGTH, 1, LITTLE_ENDIAN);
        buffer.putLong(LOG_HEADER_LENGTH + SIZE_OF_LONG, 2, LITTLE_ENDIAN);
        buffer.putLong(LOG_HEADER_LENGTH + (SIZE_OF_LONG * 2), 3, LITTLE_ENDIAN);
        buffer.putLong(LOG_HEADER_LENGTH + (SIZE_OF_LONG * 3), 4, LITTLE_ENDIAN);
        buffer.putLong(LOG_HEADER_LENGTH + (SIZE_OF_LONG * 4), 5, LITTLE_ENDIAN);
        buffer.putInt(LOG_HEADER_LENGTH + (SIZE_OF_LONG * 5), 100, LITTLE_ENDIAN);
        buffer.putInt(LOG_HEADER_LENGTH + (SIZE_OF_LONG * 5) + SIZE_OF_INT, 200, LITTLE_ENDIAN);
        buffer.putInt(LOG_HEADER_LENGTH + (SIZE_OF_LONG * 5) + SIZE_OF_INT + SIZE_OF_INT, 1, LITTLE_ENDIAN);

        ClusterEventDissector.dissectNewLeadershipTerm(buffer, 0, builder);

        assertEquals("[33.0] " + CONTEXT + ": " + NEW_LEADERSHIP_TERM.name() + " [8/9]: logLeadershipTermId=1," +
            " logTruncatePosition=2, leadershipTermId=3, logPosition=4, timestamp=5, leaderMemberId=100," +
            " logSessionId=200, isStartup=true",
            builder.toString());
    }

    @Test
    void dissectStateChange()
    {
        internalEncodeLogHeader(buffer, 0, 100, 200, () -> -1_000_000_000);
        buffer.putInt(LOG_HEADER_LENGTH, 42, LITTLE_ENDIAN);
        buffer.putStringAscii(LOG_HEADER_LENGTH + SIZE_OF_INT, "a -> b");

        ClusterEventDissector.dissectStateChange(ELECTION_STATE_CHANGE, buffer, 0, builder);

        assertEquals("[-1.0] " + CONTEXT + ": " + ELECTION_STATE_CHANGE.name() + " [100/200]: memberId=42, a -> b",
            builder.toString());
    }
}