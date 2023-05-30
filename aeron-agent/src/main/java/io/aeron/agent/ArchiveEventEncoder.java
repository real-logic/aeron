/*
 * Copyright 2014-2023 Real Logic Limited.
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
        final long id,
        final long position)
    {
        int encodedLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putLong(offset + encodedLength, id, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;
        encodingBuffer.putLong(offset + encodedLength, position, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        return encodeTrailingStateChange(encodingBuffer, offset, encodedLength, captureLength, from, to);
    }

    static <E extends Enum<E>> int sessionStateChangeLength(final E from, final E to)
    {
        return stateTransitionStringLength(from, to) + 2 * SIZE_OF_LONG;
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
        int encodedLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putLong(offset + encodedLength, sessionId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, recordingId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodeTrailingString(encodingBuffer, offset + encodedLength, captureLength - (SIZE_OF_INT * 2), errorMessage);
    }

    static void encodeCatalogResize(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final long catalogLength,
        final long newCatalogLength)
    {
        int encodedLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putLong(offset + encodedLength, catalogLength, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, newCatalogLength, LITTLE_ENDIAN);
    }
}
