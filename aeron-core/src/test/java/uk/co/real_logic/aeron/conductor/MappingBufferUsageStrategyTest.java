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
import uk.co.real_logic.aeron.util.IoUtil;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static uk.co.real_logic.aeron.util.FileMappingConvention.Type.LOG;
import static uk.co.real_logic.aeron.util.FileMappingConvention.bufferSuffix;

public class MappingBufferUsageStrategyTest
{
    private static String LOCATION = IoUtil.tmpDirName() + "/file_to_map";
    private static String OTHER_LOCATION = LOCATION + "_other";

    private BufferUsageStrategy usageStrategy;

    @Before
    public void setUp() throws IOException
    {
        createFile(LOCATION);
        createFile(OTHER_LOCATION);
        usageStrategy = new MappingBufferUsageStrategy();
    }

    private void createFile(final String location) throws IOException
    {
        File file = new File(location);
        file.createNewFile();
    }

    @After
    public void tearDown()
    {
        usageStrategy.close();
        removeFile(LOCATION);
        removeFile(OTHER_LOCATION);
    }

    private void removeFile(final String location)
    {
        
    }

    @Test
    public void testInitiallyNoBuffersToRelease()
    {
        assertBuffersReleased(0, LOCATION);
    }

    @Test
    public void mappedBuffersShouldBeReleased() throws IOException
    {
        createTermBuffers(LOCATION);

        mapTermBuffers(LOCATION);

        assertBuffersReleased(3, LOCATION);
    }

    @Test
    public void mappedBuffersShouldOnlyBeReleasedOnce() throws IOException
    {
        createTermBuffers(LOCATION);

        mapTermBuffers(LOCATION);

        assertBuffersReleased(3, LOCATION);
        assertBuffersReleased(0, LOCATION);
    }

    @Test
    public void onlyRelevantMappedBuffersShouldBeReleased() throws IOException
    {
        createTermBuffers(LOCATION);
        createTermBuffers(OTHER_LOCATION);

        mapTermBuffers(LOCATION);
        mapTermBuffers(OTHER_LOCATION);


        assertBuffersReleased(3, LOCATION);
    }

    private void assertBuffersReleased(final int count, final String location)
    {
        int buffersReleased = usageStrategy.releaseBuffers(location);
        assertThat(buffersReleased, is(count));
    }

    private void mapTermBuffers(final String location) throws IOException
    {
        usageStrategy.newLogBuffer(location, 0);
    }

    private void createTermBuffers(final String location) throws IOException
    {
        new File(location).mkdir();
        System.out.println(location);
        File logFile = new File(location, bufferSuffix(0, LOG));
        System.out.println(logFile.getAbsolutePath());
        logFile.createNewFile();
    }
}