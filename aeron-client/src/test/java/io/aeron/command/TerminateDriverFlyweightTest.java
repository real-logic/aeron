/*
 * Copyright 2014-2025 Real Logic Limited.
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
package io.aeron.command;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static io.aeron.command.TerminateDriverFlyweight.TOKEN_BUFFER_OFFSET;
import static java.util.Arrays.fill;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TerminateDriverFlyweightTest
{
    @Test
    void tokenBuffer()
    {
        final int offset = 24;
        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocate(128));
        buffer.setMemory(0, offset, (byte)15);
        final TerminateDriverFlyweight flyweight = new TerminateDriverFlyweight();
        flyweight.wrap(buffer, offset);

        flyweight.tokenBuffer(newBuffer(16), 4, 8);

        assertEquals(8, flyweight.tokenBufferLength());
        assertEquals(TOKEN_BUFFER_OFFSET, flyweight.tokenBufferOffset());
        assertEquals(TOKEN_BUFFER_OFFSET + 8, flyweight.length());
    }

    private DirectBuffer newBuffer(final int length)
    {
        final byte[] bytes = new byte[length];
        fill(bytes, (byte)1);
        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocate(4 + length));
        buffer.putBytes(4, bytes);
        return buffer;
    }
}
