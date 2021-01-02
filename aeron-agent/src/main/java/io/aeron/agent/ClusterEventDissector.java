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

import org.agrona.MutableDirectBuffer;

import static io.aeron.agent.ClusterEventCode.NEW_LEADERSHIP_TERM;
import static io.aeron.agent.CommonEventDissector.dissectLogHeader;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

final class ClusterEventDissector
{
    static final String CONTEXT = "CLUSTER";

    private ClusterEventDissector()
    {
    }

    static void dissectNewLeadershipTerm(
        final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, NEW_LEADERSHIP_TERM, buffer, absoluteOffset, builder);

        final long logLeadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long logTruncatePosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long leadershipTermId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long logPosition = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final long timestamp = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_LONG;

        final int leaderMemberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        final int logSessionId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        final boolean isStartup = 1 == buffer.getInt(absoluteOffset, LITTLE_ENDIAN);

        builder.append(": logLeadershipTermId=").append(logLeadershipTermId)
            .append(", logTruncatePosition=").append(logTruncatePosition)
            .append(", leadershipTermId=").append(leadershipTermId)
            .append(", logPosition=").append(logPosition)
            .append(", timestamp=").append(timestamp)
            .append(", leaderMemberId=").append(leaderMemberId)
            .append(", logSessionId=").append(logSessionId)
            .append(", isStartup=").append(isStartup);
    }

    static void dissectStateChange(
        final ClusterEventCode eventCode,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        int absoluteOffset = offset;
        absoluteOffset += dissectLogHeader(CONTEXT, eventCode, buffer, absoluteOffset, builder);

        final int memberId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
        absoluteOffset += SIZE_OF_INT;

        builder.append(": memberId=").append(memberId);
        builder.append(", ");
        buffer.getStringAscii(absoluteOffset, builder);
    }
}
