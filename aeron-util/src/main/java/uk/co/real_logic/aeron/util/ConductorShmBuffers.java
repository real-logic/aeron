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
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

import static uk.co.real_logic.aeron.util.IoUtil.mapExistingFile;
import static uk.co.real_logic.aeron.util.IoUtil.mapNewFile;

/**
 * Encapsulates the allocation and mapping inter-conductor communication {@link ByteBuffer}s.
 *
 * Assumes one media driver per client instance.
 */
public class ConductorShmBuffers implements AutoCloseable
{
    protected static final String TO_DRIVER_FILE = "to-driver";
    protected static final String TO_CLIENT_FILE = "to-client";

    private MappedByteBuffer toDriver;
    private MappedByteBuffer toClient;

    /**
     * Create and map the conductor buffers between the media driver and the client.
     *
     * @param adminDirName in which to create the buffers.
     * @param bufferSize to be used for the files.
     */
    public ConductorShmBuffers(final String adminDirName, final int bufferSize)
    {
        final File adminDir  = new File(adminDirName);
        IoUtil.checkDirectoryExists(new File(adminDirName), "adminDir");

        final File toDriverFile = new File(adminDir, TO_DRIVER_FILE);
        final File toClientFile = new File(adminDir, TO_CLIENT_FILE);

        try
        {
            toDriver = mapNewFile(toDriverFile, TO_DRIVER_FILE, bufferSize);
            toClient = mapNewFile(toClientFile, TO_CLIENT_FILE, bufferSize);
        }
        catch (final IOException ex)
        {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Map the conductor buffers over existing files between the media driver and the client.
     *
     * @param adminDirName in which to create the buffers.
     */
    public ConductorShmBuffers(final String adminDirName)
    {
        final File adminDir  = new File(adminDirName);
        IoUtil.checkDirectoryExists(new File(adminDirName), "adminDir");

        final File toMediaDriverFile = new File(adminDir, TO_DRIVER_FILE);
        final File toClientFile = new File(adminDir, TO_CLIENT_FILE);

        try
        {
            toDriver = mapExistingFile(toMediaDriverFile, TO_DRIVER_FILE);
            toClient = mapExistingFile(toClientFile, TO_CLIENT_FILE);
        }
        catch (final IOException ex)
        {
            throw new RuntimeException(ex);
        }
    }

    /**
     * The {@link ByteBuffer} via which messages are send from the client to the media driver.
     *
     * @return the {@link ByteBuffer} via which messages are send from the client to the media driver.
     */
    public ByteBuffer toDriver()
    {
        return toDriver;
    }

    /**
     * The {@link ByteBuffer} via which messages are send from the media driver to the client.
     *
     * @return the {@link ByteBuffer} via which messages are send from the media driver to the client.
     */
    public ByteBuffer toClient()
    {
        return toClient;
    }

    /**
     * Un-map buffers and allow for garbage collection.
     */
    public void close()
    {
        IoUtil.unmap(toClient);
        IoUtil.unmap(toDriver);
        toClient = null;
        toDriver = null;
    }
}
