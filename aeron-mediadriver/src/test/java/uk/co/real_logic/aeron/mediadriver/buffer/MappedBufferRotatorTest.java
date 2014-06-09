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
package uk.co.real_logic.aeron.mediadriver.buffer;

import org.junit.ClassRule;
import org.junit.Test;
import uk.co.real_logic.aeron.mediadriver.TemplateFileResource;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static uk.co.real_logic.aeron.mediadriver.TemplateFileResource.BUFFER_SIZE;

public class MappedBufferRotatorTest
{
    @ClassRule
    public static TemplateFileResource template = new TemplateFileResource();

    @Test
    public void returnedBuffersAreAlwaysFresh() throws IOException
    {
        withRotatedBuffers(
            buffer ->
            {
                // check you get a clean buffer
                IntStream.range(0, BUFFER_SIZE)
                         .forEach(i -> assertThat(buffer.getByte(i), is((byte)0)));

                // dirty up the buffer
                buffer.putInt(1, 4);
                buffer.putInt(500, 4);
                buffer.putInt(996, 4);
            }
        );
    }

    @Test
    public void buffersAreReused() throws IOException
    {
        final Map<AtomicBuffer, Boolean> buffers = new IdentityHashMap<>();
        withRotatedBuffers(buffer -> buffers.put(buffer, Boolean.TRUE));

        assertThat(buffers.entrySet(), hasSize(6));
    }

    private void withRotatedBuffers(final Consumer<AtomicBuffer> handler) throws IOException
    {
        final MappedBufferRotator rotator =
            new MappedBufferRotator(template.directory(), template.file(), BUFFER_SIZE, template.file(), BUFFER_SIZE);
        final List<LogBuffers> buffers = rotator.buffers().collect(toList());

        for (int i = 0; i < 20; i++)
        {
            rotator.rotate();

            final LogBuffers buffer = buffers.get(i % buffers.size());
            handler.accept(buffer.logBuffer());
            handler.accept(buffer.stateBuffer());
        }
    }
}
