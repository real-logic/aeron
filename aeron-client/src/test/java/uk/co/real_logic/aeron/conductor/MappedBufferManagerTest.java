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
package uk.co.real_logic.aeron.conductor;

import org.junit.*;
import uk.co.real_logic.aeron.common.IoUtil;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class MappedBufferManagerTest
{
    private static final String LOCATION = IoUtil.tmpDirName() + "/file_to_map";
    private static final String OTHER_LOCATION = LOCATION + "_other";

    private BufferManager usageStrategy;

    @Before
    public void setUp() throws IOException
    {
        createFile(LOCATION);
        createFile(OTHER_LOCATION);
        usageStrategy = new MappedBufferManager();
    }

    private void createFile(final String location) throws IOException
    {
        try (final FileChannel channel = FileChannel.open(new File(location).toPath(), CREATE, WRITE))
        {
            channel.write(ByteBuffer.allocate(10));
        }
    }

    @After
    public void tearDown() throws IOException
    {
        usageStrategy.close();
        removeFile(LOCATION);
        removeFile(OTHER_LOCATION);
    }

    private void removeFile(final String location) throws IOException
    {
        IoUtil.delete(new File(location), false);
    }

    @Test
    public void testInitiallyNoBuffersToRelease()
    {
        assertBuffersReleased(0, LOCATION);
    }

    @Test
    public void mappedBuffersShouldBeReleased() throws IOException
    {
        mapTermBuffers(LOCATION);

        assertBuffersReleased(1, LOCATION);
    }

    @Test
    public void mappedBuffersShouldOnlyBeReleasedOnce() throws IOException
    {
        mapTermBuffers(LOCATION);

        assertBuffersReleased(1, LOCATION);
        assertBuffersReleased(0, LOCATION);
    }

    @Test
    public void onlyRelevantMappedBuffersShouldBeReleased() throws IOException
    {
        mapTermBuffers(LOCATION);
        mapTermBuffers(OTHER_LOCATION);

        assertBuffersReleased(1, LOCATION);
    }

    private void assertBuffersReleased(final int count, final String location)
    {
        int buffersReleased = usageStrategy.releaseBuffers(location, 0, 10);
        assertThat(buffersReleased, is(count));
    }

    private void mapTermBuffers(final String location) throws IOException
    {
        usageStrategy.newBuffer(location, 0, 10);
    }
}