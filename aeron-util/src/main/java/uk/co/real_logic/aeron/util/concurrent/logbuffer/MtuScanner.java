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
package uk.co.real_logic.aeron.util.concurrent.logbuffer;

import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

import java.nio.ByteOrder;

import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.*;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.PADDING_FRAME_TYPE;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.checkLogBuffer;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.checkStateBuffer;

/**
 * Cursor that scans a log buffer reading MTU (Maximum Transmission Unit) ranges of messages
 * as they become available due to the tail progressing.
 *
 * <b>Note:</b> An instance of this class is not threadsafe. Each thread must have its own instance.
 */
public class MtuScanner
{
    private final AtomicBuffer logBuffer;
    private final AtomicBuffer stateBuffer;
    private final int mtuLength;
    private final int headerLength;
    private final int capacity;

    private int offset = 0;
    private int length = 0;
    private boolean isComplete = false;

    /**
     * Construct a reader that iterates over a log buffer with associated state buffer. Messages are identified as
     * they become available up to the MTU limit.
     *  @param logBuffer containing the framed messages.
     * @param stateBuffer containing the state variables indicating the tail progress.
     * @param mtuLength of the underlying transport.
     * @param headerLength of frame before payload begins.
     */
    public MtuScanner(final AtomicBuffer logBuffer,
                      final AtomicBuffer stateBuffer,
                      final int mtuLength,
                      final int headerLength)
    {
        checkLogBuffer(logBuffer);
        checkStateBuffer(stateBuffer);
        checkMaxFrameLength(mtuLength);
        checkHeaderLength(headerLength);

        this.logBuffer = logBuffer;
        this.stateBuffer = stateBuffer;
        this.mtuLength = mtuLength;
        this.headerLength = headerLength;
        this.capacity = logBuffer.capacity();
    }

    /**
     * The capacity of the underlying log buffer.
     *
     * @return the capacity of the underlying log buffer.
     */
    public int capacity()
    {
        return capacity;
    }

    /**
     * The length of the Maximum Transmission Unit for the transport.
     *
     * @return the length of the MTU for the transport.
     */
    public int mtuLength()
    {
        return mtuLength;
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
     * The length of the next range of frames that will be <= MTU.
     *
     * @return the length of the next range of frames that will be <= MTU.
     */
    public int length()
    {
        return length;
    }

    /**
     * Is the scanning of the log buffer complete?
     *
     * @return is the scanning of the log buffer complete?
     */
    public boolean isComplete()
    {
        return isComplete;
    }

    /**
     * Scan forward in the buffer for available frames limited by what will fit in the MTU.
     *
     * @return true if data is available otherwise false.
     */
    public boolean scan()
    {
        if (isComplete)
        {
            throw new IllegalStateException("Cannot scan beyond of the buffer");
        }

        boolean available = false;
        offset += length;
        length = 0;

        final int tail = getTailVolatile();
        if (tail > offset)
        {
            available = true;

            do
            {
                final int frameOffset = offset + length;
                final int frameLength = waitForFrameLengthVolatile(frameOffset);
                final int frameType = getFrameType(frameOffset);

                if (PADDING_FRAME_TYPE == frameType)
                {
                    length += headerLength;
                    isComplete = true;
                    break;
                }

                length += frameLength;
                if (length > mtuLength)
                {
                    length -= frameLength;
                    break;
                }
            }
            while (tail > (offset + length));
        }
        else if (tail == capacity)
        {
            isComplete = true;
        }

        return available;
    }

    /**
     * Seek within a log buffer and get ready for the next scan.
     *
     * @param offset in the log buffer to seek to for next scan.
     * @throws IllegalStateException if the offset is beyond the tail.
     */
    public void seek(final int offset)
    {
        isComplete = false;

        int tail = getTailVolatile();
        if (offset < 0 || offset > tail)
        {
            throw new IllegalStateException(String.format("Invalid offset %d: range is 0 - %d",
                                                          Integer.valueOf(offset), Integer.valueOf(tail)));
        }

        this.offset = offset;
        length = 0;
    }

    private int waitForFrameLengthVolatile(final int frameOffset)
    {
        int frameLength;
        do
        {
            frameLength = logBuffer.getIntVolatile(lengthOffset(frameOffset));
        }
        while (0 == frameLength);

        if (ByteOrder.nativeOrder() != ByteOrder.LITTLE_ENDIAN)
        {
            frameLength = Integer.reverseBytes(frameLength);
        }

        return frameLength;
    }

    private int getFrameType(final int frameOffset)
    {
        return logBuffer.getShort(typeOffset(frameOffset), ByteOrder.LITTLE_ENDIAN);
    }

    private int getTailVolatile()
    {
        return stateBuffer.getIntVolatile(LogBufferDescriptor.TAIL_COUNTER_OFFSET);
    }
}
