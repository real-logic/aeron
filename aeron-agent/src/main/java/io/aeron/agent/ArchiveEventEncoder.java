/*
 * Copyright 2019 Real Logic Ltd.
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

import static io.aeron.agent.CommonEventEncoder.STATE_SEPARATOR;
import static io.aeron.agent.CommonEventEncoder.encodeLogHeader;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_INT;

final class ArchiveEventEncoder
{
    private ArchiveEventEncoder()
    {
    }

    static <T extends Enum<T>> int encodeReplicationStateChange(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final T from,
        final T to,
        final int replaySessionId)
    {
        int relativeOffset = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putInt(offset + relativeOffset, replaySessionId, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        encodingBuffer.putInt(offset + relativeOffset, captureLength - (SIZE_OF_INT * 2), LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        relativeOffset += encodingBuffer.putStringWithoutLengthAscii(offset + relativeOffset, from.name());
        relativeOffset += encodingBuffer.putStringWithoutLengthAscii(offset + relativeOffset, STATE_SEPARATOR);
        relativeOffset += encodingBuffer.putStringWithoutLengthAscii(offset + relativeOffset, to.name());

        return relativeOffset;
    }
}
