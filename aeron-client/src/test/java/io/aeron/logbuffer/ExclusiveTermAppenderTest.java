/*
 * Copyright 2014-2021 Real Logic Limited.
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
package io.aeron.logbuffer;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BufferUtil.allocateDirectAligned;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

class ExclusiveTermAppenderTest
{
    private final UnsafeBuffer metadataBuffer = new UnsafeBuffer(allocateDirectAligned(1024, 64));
    private final UnsafeBuffer termBuffer = mock(UnsafeBuffer.class);
    private final ExclusiveTermAppender termAppender = new ExclusiveTermAppender(termBuffer, metadataBuffer, 0);

    @Test
    void appendBlock()
    {
        final int termId = 43;
        final int termOffset = 128;
        final MutableDirectBuffer buffer = mock(MutableDirectBuffer.class);
        final int offset = 16;
        final int length = 1024;
        final int lengthOfFirstFrame = 148;
        when(buffer.getInt(offset, LITTLE_ENDIAN)).thenReturn(lengthOfFirstFrame);

        final int resultOffset = termAppender.appendBlock(termId, termOffset, buffer, offset, length);

        assertEquals(termOffset + length, resultOffset);
        final long rawTail = rawTail(metadataBuffer, 0);
        assertEquals(termId, termId(rawTail));
        assertEquals(termOffset + length, termOffset(rawTail));
        final InOrder inOrder = inOrder(termBuffer, buffer);
        inOrder.verify(buffer).getInt(offset, LITTLE_ENDIAN);
        inOrder.verify(buffer).putInt(offset, 0, LITTLE_ENDIAN);
        inOrder.verify(termBuffer).putBytes(termOffset, buffer, offset, length);
        inOrder.verify(termBuffer).putIntOrdered(termOffset, lengthOfFirstFrame);
        inOrder.verifyNoMoreInteractions();
    }
}