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

import org.junit.rules.ExternalResource;
import uk.co.real_logic.aeron.util.IoUtil;

import java.io.File;
import java.nio.channels.FileChannel;

public class TemplateFileResource extends ExternalResource
{

    public static final int BUFFER_SIZE = 1000;

    private FileChannel templateFile;
    private File directory;

    protected void before() throws Throwable
    {
        directory = new File(IoUtil.tmpDir(), "mapped-buffers");
        IoUtil.ensureDirectoryExists(directory, "mapped-buffers");
        directory.deleteOnExit();
        templateFile = IoUtil.createEmptyFile(new File(directory, "template"), BUFFER_SIZE);
    }

    public FileChannel file()
    {
        return templateFile;
    }

    public File directory()
    {
        return directory;
    }

}
