/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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

import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor.*;

/**
 * Rebuild a log buffer based on incoming frames that can be out-of-order.
 *
 * <b>Note:</b> Only one rebuilder should rebuild a log at any given time. This is not thread safe
 * by rebuilder instance or across rebuilder instances.
 */
public class LogRebuilder
{
    /**
     * Insert a packet of frames into the log at the appropriate offset as indicated by the term offset header.
     *
     * @param termBuffer into which the packet should be inserted.
     * @param termOffset offset in the term at which the packet should be inserted.
     * @param packet     containing a sequence of frames.
     * @param srcOffset  in the packet at which the frames begin.
     * @param length     of the sequence of frames in bytes.
     */
    public static void insert(
        final UnsafeBuffer termBuffer, final int termOffset, final UnsafeBuffer packet, final int srcOffset, final int length)
    {
        final int lengthOffset = lengthOffset(srcOffset);
        final int frameLength = packet.getInt(lengthOffset, LITTLE_ENDIAN);
        packet.putInt(lengthOffset, 0, LITTLE_ENDIAN);

        termBuffer.putBytes(termOffset, packet, srcOffset, length);
        frameLengthOrdered(termBuffer, termOffset, frameLength);
    }
}
