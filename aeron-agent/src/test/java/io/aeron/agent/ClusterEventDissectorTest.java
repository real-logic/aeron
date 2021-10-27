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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static io.aeron.agent.ClusterEventCode.ELECTION_STATE_CHANGE;
import static io.aeron.agent.ClusterEventCode.NEW_LEADERSHIP_TERM;
import static io.aeron.agent.ClusterEventDissector.CONTEXT;
import static io.aeron.agent.CommonEventEncoder.LOG_HEADER_LENGTH;
import static io.aeron.agent.CommonEventEncoder.internalEncodeLogHeader;
import static io.aeron.agent.EventConfiguration.MAX_EVENT_LENGTH;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;
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
        buffer.putLong(LOG_HEADER_LENGTH + (SIZE_OF_LONG * 3), 13, LITTLE_ENDIAN);
        buffer.putLong(LOG_HEADER_LENGTH + (SIZE_OF_LONG * 4), 23, LITTLE_ENDIAN);
        buffer.putLong(LOG_HEADER_LENGTH + (SIZE_OF_LONG * 5), 4, LITTLE_ENDIAN);
        buffer.putLong(LOG_HEADER_LENGTH + (SIZE_OF_LONG * 6), 5, LITTLE_ENDIAN);
        buffer.putLong(LOG_HEADER_LENGTH + (SIZE_OF_LONG * 7), 6, LITTLE_ENDIAN);
        buffer.putLong(LOG_HEADER_LENGTH + (SIZE_OF_LONG * 8), 7, LITTLE_ENDIAN);
        buffer.putInt(LOG_HEADER_LENGTH + (SIZE_OF_LONG * 9), 100, LITTLE_ENDIAN);
        buffer.putInt(LOG_HEADER_LENGTH + (SIZE_OF_LONG * 9) + SIZE_OF_INT, 200, LITTLE_ENDIAN);
        buffer.putInt(LOG_HEADER_LENGTH + (SIZE_OF_LONG * 9) + SIZE_OF_INT + SIZE_OF_INT, 1, LITTLE_ENDIAN);

        ClusterEventDissector.dissectNewLeadershipTerm(buffer, 0, builder);

        assertEquals("[33.0] " + CONTEXT + ": " + NEW_LEADERSHIP_TERM.name() + " [8/9]: logLeadershipTermId=1 " +
            "nextLeadershipTermId=2 nextTermBaseLogPosition=3 nextLogPosition=13 leadershipTermId=23 " +
            "termBaseLogPosition=4 logPosition=5 leaderRecordingId=6 " +
            "timestamp=7 leaderMemberId=100 logSessionId=200 isStartup=true",
            builder.toString());
    }

    @ParameterizedTest
    @EnumSource(value = ClusterEventCode.class, names = { "STATE_CHANGE", "ROLE_CHANGE" })
    void dissectStateChange(final ClusterEventCode code)
    {
        internalEncodeLogHeader(buffer, 0, 100, 200, () -> -1_000_000_000);
        buffer.putInt(LOG_HEADER_LENGTH, 42, LITTLE_ENDIAN);
        buffer.putStringAscii(LOG_HEADER_LENGTH + SIZE_OF_INT, "a -> b");

        ClusterEventDissector.dissectStateChange(code, buffer, 0, builder);

        assertEquals("[-1.0] " + CONTEXT + ": " + code.name() + " [100/200]: memberId=42 a -> b",
            builder.toString());
    }

    @Test
    void dissectElectionStateChange()
    {
        final int offset = 10;
        int writeIndex = offset;
        writeIndex += internalEncodeLogHeader(buffer, offset, 100, 200, () -> 5_000_000_000L);
        buffer.putInt(writeIndex, 86, LITTLE_ENDIAN);
        writeIndex += SIZE_OF_INT;
        buffer.putInt(writeIndex, 3, LITTLE_ENDIAN);
        writeIndex += SIZE_OF_INT;
        buffer.putLong(writeIndex, 101010, LITTLE_ENDIAN);
        writeIndex += SIZE_OF_LONG;
        buffer.putLong(writeIndex, 6, LITTLE_ENDIAN);
        writeIndex += SIZE_OF_LONG;
        buffer.putLong(writeIndex, 1024, LITTLE_ENDIAN);
        writeIndex += SIZE_OF_LONG;
        buffer.putLong(writeIndex, 2, LITTLE_ENDIAN);
        writeIndex += SIZE_OF_LONG;
        buffer.putLong(writeIndex, 1218, LITTLE_ENDIAN);
        writeIndex += SIZE_OF_LONG;
        buffer.putLong(writeIndex, 800, LITTLE_ENDIAN);
        writeIndex += SIZE_OF_LONG;
        buffer.putStringAscii(writeIndex, "old -> new");

        ClusterEventDissector.dissectElectionStateChange(buffer, offset, builder);

        assertEquals("[5.0] " + CONTEXT + ": " + ELECTION_STATE_CHANGE.name() + " [100/200]: memberId=86" +
            " old -> new leaderId=3 candidateTermId=101010 leadershipTermId=6 logPosition=1024 logLeadershipTermId=2" +
            " appendPosition=1218 catchupPosition=800",
            builder.toString());
    }
}
