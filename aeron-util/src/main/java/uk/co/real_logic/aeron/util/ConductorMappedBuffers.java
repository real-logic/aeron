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
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

/**
 * Interface for encapsulating the allocation of conductor communication buffers.
 *
 * Assumes one media driver per client instance.
 */
public abstract class ConductorMappedBuffers implements AutoCloseable
{
    protected static final String MEDIA_DRIVER_FILE = "media-driver";
    protected static final String CLIENT_FILE = "client";

    protected final File toMediaDriverFile;
    protected final File toClientFile;

    protected MappedByteBuffer toMediaDriverBuffer;
    protected MappedByteBuffer toClientBuffer;

    protected ConductorMappedBuffers(final String adminDirName)
    {
        toMediaDriverFile = new File(adminDirName, MEDIA_DRIVER_FILE);
        toClientFile = new File(adminDirName, CLIENT_FILE);
    }

    public ByteBuffer toMediaDriver()
    {
        return toMediaDriverBuffer;
    }

    public ByteBuffer toClient()
    {
        return toClientBuffer;
    }

    public void close()
    {
        IoUtil.unmap(toClientBuffer);
        IoUtil.unmap(toMediaDriverBuffer);
        toClientBuffer = null;
        toMediaDriverBuffer = null;
    }
}
