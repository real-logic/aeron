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

import org.agrona.concurrent.UnsafeBuffer;

import static io.aeron.agent.CommonEventEncoder.MAX_CAPTURE_LENGTH;
import static io.aeron.agent.CommonEventEncoder.encodeLogHeader;
import static java.lang.Math.min;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

final class ClusterEventEncoder
{
    static final String SEPARATOR = " -> ";

    private ClusterEventEncoder()
    {
    }

    static int encodeNewLeadershipTerm(
        final UnsafeBuffer encodingBuffer,
        final long logLeadershipTermId,
        final long leadershipTermId,
        final long logPosition,
        final long timestamp,
        final int leaderMemberId,
        final int logSessionId)
    {
        final int length = SIZE_OF_LONG * 4 + SIZE_OF_INT * 2;

        int offset = encodeLogHeader(encodingBuffer, length, length);

        encodingBuffer.putLong(offset, logLeadershipTermId, LITTLE_ENDIAN);
        offset += SIZE_OF_LONG;

        encodingBuffer.putLong(offset, leadershipTermId, LITTLE_ENDIAN);
        offset += SIZE_OF_LONG;

        encodingBuffer.putLong(offset, logPosition, LITTLE_ENDIAN);
        offset += SIZE_OF_LONG;

        encodingBuffer.putLong(offset, timestamp, LITTLE_ENDIAN);
        offset += SIZE_OF_LONG;

        encodingBuffer.putInt(offset, leaderMemberId, LITTLE_ENDIAN);
        offset += SIZE_OF_INT;

        encodingBuffer.putInt(offset, logSessionId, LITTLE_ENDIAN);
        offset += SIZE_OF_INT;

        return offset;
    }

    static <T extends Enum<T>> int encodeStateChange(
        final UnsafeBuffer encodingBuffer, final T from, final T to, final int memberId)
    {
        final int stringLength = from.name().length() + SEPARATOR.length() + to.name().length();
        final int length = stringLength + SIZE_OF_INT * 2;
        final int captureLength = min(length, MAX_CAPTURE_LENGTH);

        int offset = encodeLogHeader(encodingBuffer, captureLength, length);

        encodingBuffer.putInt(offset, memberId, LITTLE_ENDIAN);
        offset += SIZE_OF_INT;

        encodingBuffer.putInt(offset, stringLength, LITTLE_ENDIAN);
        offset += SIZE_OF_INT;

        offset += encodingBuffer.putStringWithoutLengthAscii(offset, from.name());
        offset += encodingBuffer.putStringWithoutLengthAscii(offset, SEPARATOR);
        offset += encodingBuffer.putStringWithoutLengthAscii(offset, to.name());

        return offset;
    }
}
