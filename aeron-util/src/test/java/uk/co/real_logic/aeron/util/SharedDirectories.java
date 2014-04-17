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

import static uk.co.real_logic.aeron.util.FileMappingConvention.BUFFER_COUNT;
import static uk.co.real_logic.aeron.util.FileMappingConvention.Type;
import static uk.co.real_logic.aeron.util.FileMappingConvention.Type.LOG;
import static uk.co.real_logic.aeron.util.FileMappingConvention.Type.STATE;
import static uk.co.real_logic.aeron.util.FileMappingConvention.termLocation;
import static uk.co.real_logic.aeron.util.IoUtil.createEmptyFile;

public class SharedDirectories extends ExternalResource
{

    private File adminDir;
    private File dataDir;
    private FileMappingConvention mapping;

    protected void before() throws Throwable
    {
        dataDir = ensureDirectory(Directories.DATA_DIR);
        adminDir = ensureDirectory(Directories.ADMIN_DIR);
        mapping = new FileMappingConvention(dataDir.getAbsolutePath());
    }

    private File ensureDirectory(final String path) throws IOException
    {
        File dir = new File(path);
        if (dir.exists())
        {
            IoUtil.delete(dir, false);
        }
        IoUtil.ensureDirectoryExists(dir, "data dir");
        return dir;
    }

    public void createSenderTermFile(final String destination,
                                     final long sessionId,
                                     final long channelId,
                                     final long termId) throws IOException
    {
        for (int i = 0; i < BUFFER_COUNT; i++)
        {
            createTermFile(destination, sessionId, channelId, i, STATE);
            createTermFile(destination, sessionId, channelId, i, LOG);
        }
    }

    private void createTermFile(final String destination,
                                final long sessionId,
                                final long channelId,
                                final long termId,
                                final Type type) throws IOException
    {
        final File rootDir = mapping.senderDir();
        final File termLocation = termLocation(rootDir, sessionId, channelId, termId, true, destination, type);
        IoUtil.delete(termLocation, true);
        createEmptyFile(termLocation, LogBufferDescriptor.LOG_MIN_SIZE);
    }

    public String dataDir()
    {
        return dataDir.getAbsolutePath();
    }
}
