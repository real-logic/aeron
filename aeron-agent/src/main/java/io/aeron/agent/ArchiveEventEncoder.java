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

import static io.aeron.agent.CommonEventEncoder.*;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

final class ArchiveEventEncoder
{
    private ArchiveEventEncoder()
    {
    }

    static <E extends Enum<E>> int encodeSessionStateChange(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final E from,
        final E to,
        final long id)
    {
        int relativeOffset = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putLong(offset + relativeOffset, id, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_LONG;

        encodingBuffer.putInt(offset + relativeOffset, captureLength - (SIZE_OF_LONG + SIZE_OF_INT), LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        relativeOffset += encodingBuffer.putStringWithoutLengthAscii(offset + relativeOffset, from.name());
        relativeOffset += encodingBuffer.putStringWithoutLengthAscii(offset + relativeOffset, STATE_SEPARATOR);
        relativeOffset += encodingBuffer.putStringWithoutLengthAscii(offset + relativeOffset, to.name());

        return relativeOffset;
    }

    static <E extends Enum<E>> int sessionStateChangeLength(final E from, final E to)
    {
        return stateTransitionStringLength(from, to) + SIZE_OF_LONG;
    }

    static void encodeReplaySessionError(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final long sessionId,
        final long recordingId,
        final String errorMessage)
    {
        int relativeOffset = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putLong(offset + relativeOffset, sessionId, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + relativeOffset, recordingId, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_LONG;

        encodeTrailingString(encodingBuffer, offset + relativeOffset, captureLength - (SIZE_OF_INT * 2), errorMessage);
    }

    static void encodeCatalogResize(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final long catalogLength,
        final long newCatalogLength)
    {
        int relativeOffset = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putLong(offset + relativeOffset, catalogLength, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + relativeOffset, newCatalogLength, LITTLE_ENDIAN);
    }

}
