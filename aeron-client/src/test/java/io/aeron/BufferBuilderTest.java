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
package io.aeron;

import org.junit.Test;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static io.aeron.BufferBuilderUtil.MIN_ALLOCATED_CAPACITY;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;

public class BufferBuilderTest
{
    private final BufferBuilder bufferBuilder = new BufferBuilder();

    @Test
    public void shouldInitialiseToDefaultValues()
    {
        assertThat(bufferBuilder.capacity(), is(0));
        assertThat(bufferBuilder.buffer().capacity(), is(0));
        assertThat(bufferBuilder.limit(), is(0));
    }

    @Test
    public void shouldGrowDirectBuffer()
    {
        final BufferBuilder builder = new BufferBuilder(0, true);
        assertThat(builder.capacity(), is(0));
        assertThat(builder.buffer().capacity(), is(0));
        assertThat(builder.limit(), is(0));

        final int appendedLength = 10;
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[appendedLength]);
        builder.append(srcBuffer, 0, srcBuffer.capacity());

        assertThat(builder.capacity(), is(MIN_ALLOCATED_CAPACITY));
        assertThat(builder.buffer().capacity(), is(MIN_ALLOCATED_CAPACITY));
        assertThat(builder.limit(), is(appendedLength));
    }

    @Test
    public void shouldAppendNothingForZeroLength()
    {
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[MIN_ALLOCATED_CAPACITY]);

        bufferBuilder.append(srcBuffer, 0, 0);

        assertThat(bufferBuilder.limit(), is(0));
    }

    @Test
    public void shouldGrowToMultipleOfInitialCapacity()
    {
        final int srcCapacity = MIN_ALLOCATED_CAPACITY * 5;
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[srcCapacity]);

        bufferBuilder.append(srcBuffer, 0, srcBuffer.capacity());

        assertThat(bufferBuilder.limit(), is(srcCapacity));
        assertThat(bufferBuilder.capacity(), greaterThanOrEqualTo(srcCapacity));
    }

    @Test
    public void shouldAppendThenReset()
    {
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[MIN_ALLOCATED_CAPACITY]);

        bufferBuilder.append(srcBuffer, 0, srcBuffer.capacity());

        assertThat(bufferBuilder.limit(), is(srcBuffer.capacity()));

        bufferBuilder.reset();

        assertThat(bufferBuilder.limit(), is(0));
    }

    @Test
    public void shouldAppendOneBufferWithoutResizing()
    {
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[MIN_ALLOCATED_CAPACITY]);
        final byte[] bytes = "Hello World".getBytes(StandardCharsets.UTF_8);
        srcBuffer.putBytes(0, bytes, 0, bytes.length);

        bufferBuilder.append(srcBuffer, 0, bytes.length);

        final byte[] temp = new byte[bytes.length];
        bufferBuilder.buffer().getBytes(0, temp, 0, bytes.length);

        assertThat(bufferBuilder.limit(), is(bytes.length));
        assertThat(bufferBuilder.capacity(), is(MIN_ALLOCATED_CAPACITY));
        assertArrayEquals(temp, bytes);
    }

    @Test
    public void shouldAppendTwoBuffersWithoutResizing()
    {
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[MIN_ALLOCATED_CAPACITY]);
        final byte[] bytes = "1111111122222222".getBytes(StandardCharsets.UTF_8);
        srcBuffer.putBytes(0, bytes, 0, bytes.length);

        bufferBuilder.append(srcBuffer, 0, bytes.length / 2);
        bufferBuilder.append(srcBuffer, bytes.length / 2, bytes.length / 2);

        final byte[] temp = new byte[bytes.length];
        bufferBuilder.buffer().getBytes(0, temp, 0, bytes.length);

        assertThat(bufferBuilder.limit(), is(bytes.length));
        assertThat(bufferBuilder.capacity(), is(MIN_ALLOCATED_CAPACITY));
        assertArrayEquals(temp, bytes);
    }

    @Test
    public void shouldFillBufferWithoutResizing()
    {
        final int bufferLength = 128;
        final byte[] buffer = new byte[bufferLength];
        Arrays.fill(buffer, (byte)7);
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(buffer);

        final BufferBuilder bufferBuilder = new BufferBuilder(bufferLength);

        bufferBuilder.append(srcBuffer, 0, bufferLength);

        final byte[] temp = new byte[bufferLength];
        bufferBuilder.buffer().getBytes(0, temp, 0, bufferLength);

        assertThat(bufferBuilder.limit(), is(bufferLength));
        assertThat(bufferBuilder.capacity(), is(bufferLength));
        assertArrayEquals(temp, buffer);
    }

    @Test
    public void shouldResizeWhenBufferJustDoesNotFit()
    {
        final int bufferLength = 128;
        final byte[] buffer = new byte[bufferLength + 1];
        Arrays.fill(buffer, (byte)7);
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(buffer);

        final BufferBuilder bufferBuilder = new BufferBuilder(bufferLength);

        bufferBuilder.append(srcBuffer, 0, buffer.length);

        final byte[] temp = new byte[buffer.length];
        bufferBuilder.buffer().getBytes(0, temp, 0, buffer.length);

        assertThat(bufferBuilder.limit(), is(buffer.length));
        assertThat(bufferBuilder.capacity(), greaterThan(bufferLength));
        assertArrayEquals(temp, buffer);
    }

    @Test
    public void shouldAppendTwoBuffersAndResize()
    {
        final int bufferLength = 128;
        final byte[] buffer = new byte[bufferLength];
        final int firstLength = buffer.length / 4;
        final int secondLength = buffer.length / 2;
        Arrays.fill(buffer, 0, firstLength + secondLength, (byte)7);
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(buffer);

        final BufferBuilder bufferBuilder = new BufferBuilder(bufferLength / 2);

        bufferBuilder.append(srcBuffer, 0, firstLength);
        bufferBuilder.append(srcBuffer, firstLength, secondLength);

        final byte[] temp = new byte[buffer.length];
        bufferBuilder.buffer().getBytes(0, temp, 0, secondLength + firstLength);

        assertThat(bufferBuilder.limit(), is(firstLength + secondLength));
        assertThat(bufferBuilder.capacity(), greaterThanOrEqualTo(firstLength + secondLength));
        assertArrayEquals(temp, buffer);
    }

    @Test
    public void shouldCompactBufferToLowerLimit()
    {
        final int bufferLength = MIN_ALLOCATED_CAPACITY / 2;
        final byte[] buffer = new byte[bufferLength];
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(buffer);

        final BufferBuilder bufferBuilder = new BufferBuilder();

        final int bufferCount = 5;
        for (int i = 0; i < bufferCount; i++)
        {
            bufferBuilder.append(srcBuffer, 0, buffer.length);
        }

        final int expectedLimit = buffer.length * bufferCount;
        assertThat(bufferBuilder.limit(), is(expectedLimit));
        final int expandedCapacity = bufferBuilder.capacity();
        assertThat(expandedCapacity, greaterThan(expectedLimit));

        bufferBuilder.reset();

        bufferBuilder.append(srcBuffer, 0, buffer.length);
        bufferBuilder.append(srcBuffer, 0, buffer.length);
        bufferBuilder.append(srcBuffer, 0, buffer.length);

        bufferBuilder.compact();

        assertThat(bufferBuilder.limit(), is(buffer.length * 3));
        assertThat(bufferBuilder.capacity(), lessThan(expandedCapacity));
    }
}
