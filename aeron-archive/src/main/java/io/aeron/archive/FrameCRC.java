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
package io.aeron.archive;

import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import static io.aeron.logbuffer.FrameDescriptor.*;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_DATA;
import static org.agrona.BitUtil.align;

/**
 * Compute CRC for every data frame ({@link io.aeron.protocol.HeaderFlyweight#HDR_TYPE_DATA}).
 */
final class FrameCRC
{
    private final CRC32 crc32 = new CRC32();

    @FunctionalInterface
    interface Consumer
    {
        /**
         * Invoked for every frame when a crc is computed.
         *
         * @param buffer      containing the block of message fragments.
         * @param frameOffset at which frame begins, including any headers.
         * @param frameLength of the frame in bytes, including any frame headers that is aligned up to
         *                    {@link io.aeron.logbuffer.FrameDescriptor#FRAME_ALIGNMENT}.
         * @param crc         computed CRC for the frame excluding header but including alignment bytes, i.e. computed
         *                    using bytes in the ranger from {@code offset + DataHeaderFlyweight.HEADER_LENGTH} until
         *                    {@code length}.
         */
        void consume(UnsafeBuffer buffer, int frameOffset, int frameLength, int crc);
    }

    /**
     * For each non-zero length data frame compute CRC32 checksum of the payload and invoke provided {@code consumer}.
     *
     * @param termBuffer containing the block of message fragments.
     * @param offset     at which frame begins, including any headers.
     * @param length     of the block in bytes, including any frame headers that is aligned up to
     *                   {@link io.aeron.logbuffer.FrameDescriptor#FRAME_ALIGNMENT}.
     * @param consumer   consumer for the computed CRC.
     */
    public void compute(final UnsafeBuffer termBuffer, final int offset, final int length, final Consumer consumer)
    {
        final ByteBuffer buffer = termBuffer.byteBuffer();
        final int position = buffer.position();
        final int limit = buffer.limit();
        int frameOffset = offset;
        while (frameOffset < length)
        {
            final int frameLength = frameLength(termBuffer, frameOffset);
            if (frameLength == 0)
            {
                break;
            }
            final int alignedLength = align(frameLength, FRAME_ALIGNMENT);
            final int frameType = frameType(termBuffer, frameOffset);
            if (HDR_TYPE_DATA == frameType)
            {
                buffer.limit(frameOffset + alignedLength); // end of the frame plus alignment
                buffer.position(frameOffset + HEADER_LENGTH); // skip the frame header
                crc32.reset();
                crc32.update(buffer);
                final int checksum = (int)crc32.getValue();
                consumer.consume(termBuffer, frameOffset, alignedLength, checksum);
            }
            frameOffset += alignedLength;
        }
        // Restore original limit and position
        buffer.limit(limit).position(position);
    }
}
