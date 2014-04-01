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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.aeron.util.IoUtil;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;


public class BasicBufferManagementStrategyTest
{

    private static final File DATA_DIR = new File(System.getProperty("java.io.tmpdir"), "DATA_DIR");

    @After
    public void cleanupFiles() throws IOException
    {
        IoUtil.delete(DATA_DIR, true);
    }

    @Before
    public void createDataDir()
    {
        IoUtil.ensureDirectoryExists(DATA_DIR, "data");
    }

    @Test
    public void mappedFilesAreCorrectSizeAndZeroed() throws Exception
    {
        final BasicBufferManagementStrategy strategy = new BasicBufferManagementStrategy(DATA_DIR.getAbsolutePath());
        final MappedByteBuffer term = strategy.mapTerm(DATA_DIR, "udp://localhost:4321", 1, 1, 1, 256);

        assertThat(term.capacity(), is(256));
        assertThat(term.get(0), is((byte) 0));
        assertThat(term.get(255), is((byte) 0));
    }

}
