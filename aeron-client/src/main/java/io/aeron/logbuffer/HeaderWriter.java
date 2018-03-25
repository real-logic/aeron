/*
 * Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron.logbuffer;

import org.agrona.UnsafeAccess;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteOrder;

import static java.lang.Integer.reverseBytes;
import static io.aeron.protocol.DataHeaderFlyweight.SESSION_ID_FIELD_OFFSET;
import static io.aeron.protocol.DataHeaderFlyweight.STREAM_ID_FIELD_OFFSET;
import static io.aeron.protocol.DataHeaderFlyweight.TERM_OFFSET_FIELD_OFFSET;
import static io.aeron.protocol.HeaderFlyweight.FRAME_LENGTH_FIELD_OFFSET;
import static io.aeron.protocol.HeaderFlyweight.VERSION_FIELD_OFFSET;

/**
 * Utility for applying a header to a message in a term buffer.
 * <p>
 * This class is designed to be thread safe to be used across multiple producers and makes the header
 * visible in the correct order for consumers.
 */
public class HeaderWriter
{
    protected final long versionFlagsType;
    protected final long sessionId;
    protected final long streamId;

    protected HeaderWriter(final long versionFlagsType, final long sessionId, final long streamId)
    {
        this.versionFlagsType = versionFlagsType;
        this.sessionId = sessionId;
        this.streamId = streamId;
    }

    HeaderWriter(final UnsafeBuffer defaultHeader)
    {
        versionFlagsType = ((long)defaultHeader.getInt(VERSION_FIELD_OFFSET)) << 32;
        sessionId = ((long)defaultHeader.getInt(SESSION_ID_FIELD_OFFSET)) << 32;
        streamId = defaultHeader.getInt(STREAM_ID_FIELD_OFFSET) & 0xFFFF_FFFFL;
    }

    public static HeaderWriter newInstance(final UnsafeBuffer defaultHeader)
    {
        if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN)
        {
            return new HeaderWriter(defaultHeader);
        }
        else
        {
            return new NativeBigEndianHeaderWriter(defaultHeader);
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
        termBuffer.putLongOrdered(offset + FRAME_LENGTH_FIELD_OFFSET, versionFlagsType | ((-length) & 0xFFFF_FFFFL));
        UnsafeAccess.UNSAFE.storeFence();

        termBuffer.putLong(offset + TERM_OFFSET_FIELD_OFFSET, sessionId | offset);
        termBuffer.putLong(offset + STREAM_ID_FIELD_OFFSET, streamId | (((long)termId) << 32));
    }
}

class NativeBigEndianHeaderWriter extends HeaderWriter
{
    NativeBigEndianHeaderWriter(final UnsafeBuffer defaultHeader)
    {
        super(
            defaultHeader.getInt(VERSION_FIELD_OFFSET) & 0xFFFF_FFFFL,
            defaultHeader.getInt(SESSION_ID_FIELD_OFFSET) & 0xFFFF_FFFFL,
            ((long)defaultHeader.getInt(STREAM_ID_FIELD_OFFSET)) << 32);
    }

    public void write(final UnsafeBuffer termBuffer, final int offset, final int length, final int termId)
    {
        termBuffer.putLongOrdered(
            offset + FRAME_LENGTH_FIELD_OFFSET, versionFlagsType | ((((long)reverseBytes(-length))) << 32));
        UnsafeAccess.UNSAFE.storeFence();

        termBuffer.putLong(offset + TERM_OFFSET_FIELD_OFFSET, sessionId | ((((long)reverseBytes(offset))) << 32));
        termBuffer.putLong(offset + STREAM_ID_FIELD_OFFSET, streamId | (reverseBytes(termId) & 0xFFFF_FFFFL));
    }
}