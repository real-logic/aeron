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
package uk.co.real_logic.aeron.mediadriver;

import org.junit.BeforeClass;
import org.junit.Test;
import uk.co.real_logic.aeron.util.IoUtil;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class MappedBufferRotatorTest
{
    public static final int BUFFER_SIZE = 1000;

    private static FileChannel templateFile;
    private static File directory;

    @BeforeClass
    public static void createFiles() throws IOException
    {
        directory = new File(System.getProperty("java.io.tmpdir"), "mapped-buffers");
        IoUtil.ensureDirectoryExists(directory, "mapped-buffers");
        directory.deleteOnExit();
        templateFile = IoUtil.createEmptyFile(new File(directory, "template"), BUFFER_SIZE);
    }

    @Test
    public void returnedBuffersAreAlwaysFresh() throws IOException
    {
        withRotatedBuffers(buffer ->
        {
           // check you get a clean buffer
           IntStream.range(0, BUFFER_SIZE)
                    .forEach(i -> assertThat(Byte.valueOf(buffer.get(i)), is(Byte.valueOf((byte)0))));

           // dirty up the buffer
           buffer.putInt(1, 4);
           buffer.putInt(500, 4);
           buffer.putInt(996, 4);
        });
    }

    @Test
    public void buffersAreReused() throws IOException
    {
        final Map<MappedByteBuffer, Boolean> buffers = new IdentityHashMap<>();
        withRotatedBuffers(buffer -> buffers.put(buffer, Boolean.TRUE));
        assertThat(buffers.entrySet(), hasSize(3));
    }

    private void withRotatedBuffers(final Consumer<MappedByteBuffer> handler) throws IOException
    {
        final MappedBufferRotator rotator = new MappedBufferRotator(templateFile, directory, BUFFER_SIZE);
        for (int iteration = 0; iteration < 20; iteration++)
        {
            final MappedByteBuffer buffer = rotator.rotate();
            handler.accept(buffer);
        }
    }
}
