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
package uk.co.real_logic.aeron.util;

import org.junit.rules.ExternalResource;
import uk.co.real_logic.aeron.util.status.StatusBufferCreator;

import java.io.File;
import java.io.IOException;

import static uk.co.real_logic.aeron.util.CommonConfiguration.COUNTERS_DIR_NAME;
import static uk.co.real_logic.aeron.util.IoUtil.ensureDirectoryExists;

/**
 * The file to be used
 */
public class CountersFileExternalResource extends ExternalResource
{
    private static final long BUFFER_SIZE = 1024L;
    private final File file = new File(COUNTERS_DIR_NAME);

    private StatusBufferCreator creator;

    protected void before() throws Throwable
    {
        ensureDirectoryExists(file, file.getName());
        creator = new StatusBufferCreator(BUFFER_SIZE, BUFFER_SIZE);
    }

    protected void after()
    {
        creator.close();
        creator = null;
        try
        {
            IoUtil.delete(file, true);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}
