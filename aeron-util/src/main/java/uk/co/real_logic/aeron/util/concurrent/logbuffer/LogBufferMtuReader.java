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

import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.checkMaxFrameLength;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.lengthOffset;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.checkLogBuffer;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.checkStateBuffer;

/**
 * Cursor that scans a log buffer reading MTU (Maximum Transmission Unit) ranges of messages
 * as they become available due to the tail progressing.
 *
 * <b>Note:</b> An instance of this class is not threadsafe. Each thread must have its own instance.
 */
public class LogBufferMtuReader
{
    public enum Status
    {
        /** Reading of the buffer is now complete. */
        COMPLETE,

        /** No data is available at this time. */
        NO_DATA,

        /** Data is available and length has been set. */
        AVAILABLE,

    }

    private final AtomicBuffer logBuffer;
    private final AtomicBuffer stateBuffer;
    private final int mtuLength;
    private final int capacity;

    private int offset = 0;
    private int length = 0;
    private Status status = Status.NO_DATA;

    /**
     * Construct a reader that iterates over a log buffer with associated state buffer. Messages are identified as
     * they become available up to the MTU limit.
     *
     * @param logBuffer containing the framed messages.
     * @param stateBuffer containing the state variables indicating the tail progress.
     * @param mtuLength of the underlying transport.
     */
    public LogBufferMtuReader(final AtomicBuffer logBuffer, final AtomicBuffer stateBuffer, final int mtuLength)
    {
        checkLogBuffer(logBuffer);
        checkStateBuffer(stateBuffer);
        checkMaxFrameLength(mtuLength);

        this.logBuffer = logBuffer;
        this.stateBuffer = stateBuffer;
        this.mtuLength = mtuLength;
        this.capacity = logBuffer.capacity();
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
     * The {@link LogBufferMtuReader.Status} of the latest scan discovery.
     *
     * @return status of the latest scan discovery.
     */
    public Status status()
    {
        return status;
    }

    /**
     * Scan forward in the buffer for available frames limited by what will fit in the MTU.
     *
     * @return the {@link LogBufferMtuReader.Status} discovered by the scan.
     */
    public Status scan()
    {
        offset += length;
        length = 0;
        status = Status.NO_DATA;

        final int tail = getTailVolatile();
        if (tail > offset)
        {
            status = Status.AVAILABLE;
            do
            {
                final int frameLength = waitForFrameLengthVolatile(offset + length);
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
            status = Status.COMPLETE;
        }

        return status;
    }

    /**
     * Seek within a log buffer and get ready for the next scan.
     *
     * @param offset in the log buffer to seek to for next scan.
     * @throws IllegalStateException if the offset is beyond the tail.
     */
    public void seek(final int offset)
    {
        int tail = getTailVolatile();
        if (offset < 0 || offset > tail)
        {
            throw new IllegalStateException(String.format("Invalid offset %d: range is 0 - %d",
                                                          Integer.valueOf(offset), Integer.valueOf(tail)));
        }

        this.offset = offset;
        length = 0;
        status = Status.NO_DATA;
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

    private int getTailVolatile()
    {
        return stateBuffer.getIntVolatile(LogBufferDescriptor.TAIL_COUNTER_OFFSET);
    }
}
