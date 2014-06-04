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
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.ManyToOneRingBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;

import java.io.File;
import java.nio.ByteBuffer;

import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.BufferDescriptor.TRAILER_LENGTH;

public class ConductorBuffersExternalResource extends ExternalResource
{
    public static final int BUFFER_SIZE = 512 + TRAILER_LENGTH;

    private final String adminDirName;
    private final int bufferSize;

    private ConductorShmBuffers mediaShmBuffers;
    private ConductorShmBuffers clientShmBuffers;
    private ByteBuffer toMediaDriver;
    private ByteBuffer toClient;
    private File adminDir;

    public ConductorBuffersExternalResource()
    {
        this(BUFFER_SIZE);
    }

    public ConductorBuffersExternalResource(int bufferSize)
    {
        this(CommonConfiguration.ADMIN_DIR_PROP_DEFAULT, bufferSize);
    }

    public ConductorBuffersExternalResource(final String adminDirName)
    {
        this(adminDirName, BUFFER_SIZE);
    }

    private ConductorBuffersExternalResource(final String adminDirName, int bufferSize)
    {
        this.adminDirName = adminDirName;
        this.bufferSize = bufferSize;
    }

    protected void before() throws Exception
    {
        adminDir = new File(adminDirName);
        if (adminDir.exists())
        {
            IoUtil.delete(adminDir, false);
        }

        IoUtil.ensureDirectoryExists(adminDir, "conductor dir");
        mediaShmBuffers = new ConductorShmBuffers(adminDirName, bufferSize);
        clientShmBuffers = new ConductorShmBuffers(adminDirName);
        toMediaDriver = mediaShmBuffers.toDriver();
        toClient = mediaShmBuffers.toClient();
    }

    protected void after()
    {
        // Force un-mapping of byte clientShmBuffers to allow deletion
        mediaShmBuffers.close();
        clientShmBuffers.close();

        try
        {
            // do deletion here to make debugging easier if un-maps/closes are not done
            IoUtil.delete(adminDir, false);
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }

    public RingBuffer toMediaDriver()
    {
        return ringBuffer(toMediaDriver);
    }

    public RingBuffer toClient()
    {
        return ringBuffer(toClient);
    }

    private ManyToOneRingBuffer ringBuffer(final ByteBuffer buffer)
    {
        return new ManyToOneRingBuffer(new AtomicBuffer(buffer));
    }

    public ConductorShmBuffers clientShmBuffers()
    {
        return clientShmBuffers;
    }

    public ConductorShmBuffers mediaShmBuffers()
    {
        return mediaShmBuffers;
    }

    public String adminDirName()
    {
        return adminDirName;
    }

    public RingBuffer mappedToMediaDriver()
    {
        return suppress(() -> ringBuffer(toMediaDriver));
    }

    public RingBuffer mappedToClient()
    {
        return suppress(() -> ringBuffer(toClient));
    }

    private static interface ExceptionalSupplier<T>
    {
        public T supply() throws Exception;
    }

    private <T> T suppress(ExceptionalSupplier<T> supplier)
    {
        try
        {
            return supplier.supply();
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }
}
