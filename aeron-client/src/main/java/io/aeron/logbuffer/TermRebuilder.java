/*
 * Copyright 2014 - 2016 Real Logic Ltd.
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

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static io.aeron.logbuffer.FrameDescriptor.frameLengthOrdered;

/**
 * Rebuild a term buffer based on incoming frames that can be out-of-order.
 */
public class TermRebuilder
{
    /**
     * Insert a packet of frames into the log at the appropriate termOffset as indicated by the term termOffset header.
     *
     * @param termBuffer into which the packet should be inserted.
     * @param termOffset in the term at which the packet should be inserted.
     * @param packet     containing a sequence of frames.
     * @param length     of the sequence of frames in bytes.
     */
    public static void insert(final UnsafeBuffer termBuffer, final int termOffset, final UnsafeBuffer packet, final int length)
    {
        final int firstFrameLength = packet.getInt(0, LITTLE_ENDIAN);
        packet.putInt(0, 0);
        UnsafeAccess.UNSAFE.storeFence();

        termBuffer.putBytes(termOffset, packet, 0, length);
        frameLengthOrdered(termBuffer, termOffset, firstFrameLength);
    }
}
