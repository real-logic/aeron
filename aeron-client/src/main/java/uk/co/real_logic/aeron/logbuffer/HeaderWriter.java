/*
 * Copyright 2015 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.aeron.logbuffer;

import uk.co.real_logic.agrona.UnsafeAccess;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteOrder;

import static java.lang.Integer.reverseBytes;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static uk.co.real_logic.aeron.protocol.DataHeaderFlyweight.SESSION_ID_FIELD_OFFSET;
import static uk.co.real_logic.aeron.protocol.DataHeaderFlyweight.STREAM_ID_FIELD_OFFSET;
import static uk.co.real_logic.aeron.protocol.DataHeaderFlyweight.TERM_OFFSET_FIELD_OFFSET;
import static uk.co.real_logic.aeron.protocol.HeaderFlyweight.FRAME_LENGTH_FIELD_OFFSET;
import static uk.co.real_logic.aeron.protocol.HeaderFlyweight.VERSION_FIELD_OFFSET;

/**
 * Utility for applying a header to a message in a term buffer.
 *
 * This class is designed to be thread safe to be used across multiple producers and makes the header
 * visible in the correct order for consumers.
 */
public class HeaderWriter
{
    private final long versionFlagsType;
    private final long sessionId;
    private final long streamId;

    public HeaderWriter(final UnsafeBuffer defaultHeader)
    {
        if (ByteOrder.nativeOrder() == LITTLE_ENDIAN)
        {
            versionFlagsType = ((long)defaultHeader.getInt(VERSION_FIELD_OFFSET)) << 32;
            sessionId = ((long)defaultHeader.getInt(SESSION_ID_FIELD_OFFSET)) << 32;
            streamId = defaultHeader.getInt(STREAM_ID_FIELD_OFFSET) & 0xFFFF_FFFFL;
        }
        else
        {
            versionFlagsType = defaultHeader.getInt(VERSION_FIELD_OFFSET) & 0xFFFF_FFFFL;
            sessionId = defaultHeader.getInt(SESSION_ID_FIELD_OFFSET) & 0xFFFF_FFFFL;
            streamId = ((long)defaultHeader.getInt(STREAM_ID_FIELD_OFFSET)) << 32;
        }
    }

    /**
     * Write a header to the term buffer in {@link ByteOrder#LITTLE_ENDIAN} format using the minimum instructions.
     *
     * @param termBuffer to be written to.
     * @param offset     at which the header should be written.
     * @param length     of the fragment including the header.
     * @param termId     of the current term buffer.
     */
    public void write(final UnsafeBuffer termBuffer, final int offset, final int length, final int termId)
    {
        final long lengthVersionFlagsType;
        final long termOffsetSessionId;
        final long streamAndTermIds;

        if (ByteOrder.nativeOrder() == LITTLE_ENDIAN)
        {
            lengthVersionFlagsType = versionFlagsType | ((-length) & 0xFFFF_FFFFL);
            termOffsetSessionId = sessionId | offset;
            streamAndTermIds = streamId | (((long)termId) << 32);
        }
        else
        {
            lengthVersionFlagsType = versionFlagsType | ((((long)reverseBytes(-length))) << 32);
            termOffsetSessionId = sessionId | ((((long)reverseBytes(offset))) << 32);
            streamAndTermIds = streamId | (reverseBytes(termId) & 0xFFFF_FFFFL);
        }

        termBuffer.putLongOrdered(offset + FRAME_LENGTH_FIELD_OFFSET, lengthVersionFlagsType);
        UnsafeAccess.UNSAFE.storeFence();

        termBuffer.putLong(offset + TERM_OFFSET_FIELD_OFFSET, termOffsetSessionId);
        termBuffer.putLong(offset + STREAM_ID_FIELD_OFFSET, streamAndTermIds);
    }
}
