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
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor;

import java.io.File;
import java.io.IOException;

import static uk.co.real_logic.aeron.util.FileMappingConvention.termLocation;
import static uk.co.real_logic.aeron.util.IoUtil.createEmptyFile;

public class SharedDirectory extends ExternalResource
{

    private File dataDir;
    private FileMappingConvention mapping;

    protected void before() throws Throwable
    {
        dataDir = new File(Directories.DATA_DIR);
        IoUtil.delete(dataDir, true);
        dataDir.mkdirs();
        mapping = new FileMappingConvention(dataDir.getAbsolutePath());
    }

    public void createSenderTermFile(final String destination,
                                     final long sessionId,
                                     final long channelId,
                                     final long termId) throws IOException
    {
        final File termLocation = termLocation(mapping.senderDir(), sessionId, channelId, termId, true, destination);
        IoUtil.delete(termLocation, true);
        createEmptyFile(termLocation, LogBufferDescriptor.LOG_MIN_SIZE);
    }

    public String dataDir()
    {
        return dataDir.getAbsolutePath();
    }
}
