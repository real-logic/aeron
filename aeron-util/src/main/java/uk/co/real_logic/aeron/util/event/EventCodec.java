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
package uk.co.real_logic.aeron.util.event;

import uk.co.real_logic.aeron.util.BitUtil;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.NakFlyweight;
import uk.co.real_logic.aeron.util.protocol.StatusMessageFlyweight;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Encoding/Decoding of event types
 */
public class EventCodec
{
    private final static ThreadLocal<HeaderFlyweight> headerFlyweight =
            ThreadLocal.withInitial(HeaderFlyweight::new);
    private final static ThreadLocal<DataHeaderFlyweight> dataHeader =
            ThreadLocal.withInitial(DataHeaderFlyweight::new);
    private final static ThreadLocal<StatusMessageFlyweight> smHeader =
            ThreadLocal.withInitial(StatusMessageFlyweight::new);
    private final static ThreadLocal<NakFlyweight> nakHeader =
            ThreadLocal.withInitial(NakFlyweight::new);

    private final static int HEADER_LENGTH = 16;

    public static int encode(final AtomicBuffer encodingBuffer, final AtomicBuffer buffer,
                             final int offset, final int bufferLength)
    {
        final int captureLength = determineCaptureLength(bufferLength);
        int relativeOffset = encodeLogHeader(encodingBuffer, captureLength, bufferLength);

        encodingBuffer.putBytes(relativeOffset, buffer, offset, captureLength);
        relativeOffset += captureLength;

        return relativeOffset;
    }

    public static int encode(final AtomicBuffer encodingBuffer, final ByteBuffer buffer,
                             final int bufferLength)
    {
        final int captureLength = determineCaptureLength(bufferLength);
        int relativeOffset = encodeLogHeader(encodingBuffer, captureLength, bufferLength);

        relativeOffset += encodingBuffer.putBytes(relativeOffset, buffer, captureLength);

        return relativeOffset;
    }

    public static int encode(final AtomicBuffer encodingBuffer, final byte[] buffer,
                             final int offset, final int bufferLength)
    {
        final int captureLength = determineCaptureLength(bufferLength);
        int relativeOffset = encodeLogHeader(encodingBuffer, captureLength, bufferLength);

        relativeOffset += encodingBuffer.putBytes(relativeOffset, buffer, offset, captureLength);

        return relativeOffset;
    }

    public static String decodeAsFrame(final EventCode code, final AtomicBuffer buffer,
                                       final int offset, final int length)
    {
        final String logHeader = formatLogHeader(code, buffer, offset);
        final HeaderFlyweight frame = headerFlyweight.get();
        String logBody;

        frame.wrap(buffer, offset + HEADER_LENGTH);
        switch (frame.headerType())
        {
            case HeaderFlyweight.HDR_TYPE_DATA:
                final DataHeaderFlyweight dataFrame = dataHeader.get();

                dataFrame.wrap(buffer, offset + HEADER_LENGTH);
                logBody = format(dataFrame);
                break;

            case HeaderFlyweight.HDR_TYPE_SM:
                final StatusMessageFlyweight smFrame = smHeader.get();

                smFrame.wrap(buffer, offset + HEADER_LENGTH);
                logBody = format(smFrame);
                break;

            case HeaderFlyweight.HDR_TYPE_NAK:
                final NakFlyweight nakFrame = nakHeader.get();

                nakFrame.wrap(buffer, offset + HEADER_LENGTH);
                logBody = format(nakFrame);
                break;

            default:
                logBody = "FRAME_UNKNOWN";
                break;
        }

        return String.format("%s:%s", logHeader, logBody);
    }

    private static int encodeLogHeader(final AtomicBuffer encodingBuffer, final int captureLength,
                                       final int bufferLength)
    {
        int relativeOffset = 0;
        /*
         * Stream of values:
         * - capture buffer length (int)
         * - total buffer length (int)
         * - timestamp (long)
         * - buffer (until end)
         */

        encodingBuffer.putInt(relativeOffset, captureLength, ByteOrder.LITTLE_ENDIAN);
        relativeOffset += BitUtil.SIZE_OF_INT;

        encodingBuffer.putInt(relativeOffset, bufferLength, ByteOrder.LITTLE_ENDIAN);
        relativeOffset += BitUtil.SIZE_OF_INT;

        encodingBuffer.putLong(relativeOffset, System.nanoTime(), ByteOrder.LITTLE_ENDIAN);
        relativeOffset += BitUtil.SIZE_OF_LONG;

        return relativeOffset;
    }

    private static int determineCaptureLength(final int bufferLength)
    {
        return Math.min(bufferLength, EventConfiguration.MAX_EVENT_LENGTH - HEADER_LENGTH);
    }

    private static String formatLogHeader(final EventCode code, final AtomicBuffer buffer, final int offset)
    {
        int relativeOffset = 0;

        final int captureLength = buffer.getInt(offset + relativeOffset, ByteOrder.LITTLE_ENDIAN);
        relativeOffset += BitUtil.SIZE_OF_INT;

        final int bufferLength = buffer.getInt(offset + relativeOffset, ByteOrder.LITTLE_ENDIAN);
        relativeOffset += BitUtil.SIZE_OF_INT;

        final long timestamp = buffer.getLong(offset + relativeOffset, ByteOrder.LITTLE_ENDIAN);

        return String.format("[%1$f] %2$s [%3$d/%4$d]", (double)timestamp / 1000000000.0, code.name(),
                captureLength, bufferLength);
    }

    private static String format(final DataHeaderFlyweight header)
    {
        return String.format("DATA %x len %d %x:%x:%x @%x", header.flags(), header.frameLength(),
                header.sessionId(), header.channelId(), header.termId(), header.termOffset());
    }

    private static String format(final StatusMessageFlyweight header)
    {
        return String.format("SM %x len %d %x:%x:%x @%x %d", header.flags(), header.frameLength(),
                header.sessionId(), header.channelId(), header.termId(), header.highestContiguousTermOffset(),
                header.receiverWindow());
    }

    private static String format(final NakFlyweight header)
    {
        return String.format("NAK %x len %d %x:%x:%x @%x %d", header.flags(), header.frameLength(),
                header.sessionId(), header.channelId(), header.termId(), header.termOffset(), header.length());
    }
}
