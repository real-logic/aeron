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

import java.io.File;
import java.io.IOException;

import static uk.co.real_logic.aeron.util.IoUtil.mapNewFile;

public class MediaDriverConductorMappedBuffers extends ConductorMappedBuffers
{
    public MediaDriverConductorMappedBuffers(final String adminDirName, final int bufferSize)
    {
        super(adminDirName);

        final File adminDir  = new File(adminDirName);
        IoUtil.checkDirectoryExists(adminDir, "adminDir");

        try
        {
            toMediaDriverBuffer = mapNewFile(toMediaDriverFile, MEDIA_DRIVER_FILE, bufferSize);
            toClientBuffer = mapNewFile(toClientFile, CLIENT_FILE, bufferSize);
        }
        catch (final IOException ex)
        {
            throw new RuntimeException(ex);
        }
    }
}
