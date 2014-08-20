/*
 * Copyright 2014 Real Logic Ltd.
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
package uk.co.real_logic.aeron.common.concurrent.logbuffer;

import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;

import java.nio.ByteOrder;

import static uk.co.real_logic.aeron.common.BitUtil.align;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor.*;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.PADDING_FRAME_TYPE;

/**
 * Scans a log buffer reading MTU (Maximum Transmission Unit) ranges of messages
 * as they become available due to the tail progressing. This scanner makes the assumption that
 * the buffer is built in an append only fashion with no gaps.
 *
 * <b>Note:</b> An instance of this class is not threadsafe. Each thread must have its own instance.
 */
public class LogScanner extends LogBuffer
{
    /**
     * Handler for notifying an available chuck from latest offset.
     */
    @FunctionalInterface
    public interface AvailabilityHandler
    {
        void onAvailable(AtomicBuffer buffer, int offset, int length);
    }

    private final int alignedHeaderLength;

    private int offset = 0;

    /**
     * Construct a reader that iterates over a log buffer with associated state buffer. Messages are identified as
     * they become available up to the MTU limit.
     *
     * @param logBuffer containing the framed messages.
     * @param stateBuffer containing the state variables indicating the tail progress.
     * @param headerLength of frame before payload begins.
     */
    public LogScanner(final AtomicBuffer logBuffer, final AtomicBuffer stateBuffer, final int headerLength)
    {
        super(logBuffer, stateBuffer);

        checkHeaderLength(headerLength);

        alignedHeaderLength = align(headerLength, FRAME_ALIGNMENT);
    }

    /**
     * The offset at which the next frame begins.
     *
     * @return the offset at which the next frame begins.
     */
    public int offset()
    {
        return offset;
    }

    /**
     * Is the scanning of the log buffer complete?
     *
     * @return is the scanning of the log buffer complete?
     */
    public boolean isComplete()
    {
        return offset >= capacity();
    }

    /**
     * Number of available bytes remaining in the buffer to be scanned.
     *
     * @return the number of bytes remaining in the buffer to be scanned.
     */
    public int remaining()
    {
        return tailVolatile() - offset;
    }

    /**
     * Scan forward in the buffer for available frames limited by what will fit in mtuLength.
     *
     * @param handler called back if a frame is available.
     * @param mtuLength in bytes to scan.
     * @return number of bytes notified available
     */
    public int scanNext(final AvailabilityHandler handler, final int mtuLength)
    {
        int length = 0;

        if (!isComplete())
        {
            final int tail = tailVolatile();
            final int offset = this.offset;
            final AtomicBuffer logBuffer = logBuffer();

            if (tail > offset)
            {
                int padding = 0;

                do
                {
                    int alignedFrameLength = align(waitForFrameLength(logBuffer, offset + length), FRAME_ALIGNMENT);

                    if (PADDING_FRAME_TYPE == frameType(logBuffer, offset + length))
                    {
                        padding = alignedFrameLength - alignedHeaderLength;
                        alignedFrameLength = alignedHeaderLength;
                    }

                    length += alignedFrameLength;

                    if (length > mtuLength)
                    {
                        length -= alignedFrameLength;
                        padding = 0;
                        break;
                    }
                }
                while ((offset + length + padding) < tail);

                if (length > 0)
                {
                    this.offset += (length + padding);
                    handler.onAvailable(logBuffer, offset, length);
                }
            }
        }

        return length;
    }

    /**
     * Seek within a log buffer and get ready for the next scan.
     *
     * @param offset in the log buffer to seek to for next scan.
     * @throws IllegalStateException if the offset is beyond the tail.
     */
    public void seek(final int offset)
    {
        final int tail = tailVolatile();
        if (offset < 0 || offset > tail)
        {
            throw new IllegalStateException(String.format("Invalid offset %d: range is 0 - %d", offset, tail));
        }

        this.offset = offset;
    }

    private static int frameType(final AtomicBuffer logBuffer, final int frameOffset)
    {
        return logBuffer.getShort(typeOffset(frameOffset), ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
    }
}
