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

import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_SIZE;

public class AdminBuffers extends ExternalResource
{

    public static final int BUFFER_SIZE = 512 + TRAILER_SIZE;

    private final AdminBufferStrategy creatingStrategy;
    private final String adminDir;
    private final AdminBufferStrategy mappingStrategy;

    private ByteBuffer toMediaDriver;
    private ByteBuffer toApi;

    public AdminBuffers()
    {
        this(BUFFER_SIZE);
    }

    public AdminBuffers(int bufferSize)
    {
        adminDir = System.getProperty("java.io.tmpdir") + "/admin";
        final File dir = new File(adminDir);
        dir.delete();
        dir.mkdir();
        creatingStrategy = new CreatingAdminBufferStrategy(adminDir, bufferSize);
        mappingStrategy = new MappingAdminBufferStrategy(adminDir);
    }

    protected void before() throws Exception
    {
        toMediaDriver = creatingStrategy.toMediaDriver();
        toApi = creatingStrategy.toApi();
    }

    public RingBuffer toMediaDriver()
    {
        return ringBuffer(toMediaDriver);
    }

    public RingBuffer toApi()
    {
        return ringBuffer(toApi);
    }

    private ManyToOneRingBuffer ringBuffer(final ByteBuffer buffer)
    {
        return new ManyToOneRingBuffer(new AtomicBuffer(buffer));
    }

    public AdminBufferStrategy strategy()
    {
        return creatingStrategy;
    }

    public String adminDir()
    {
        return adminDir;
    }

    public RingBuffer mappedToMediaDriver()
    {
        try
        {
            return ringBuffer(mappingStrategy.toMediaDriver());
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}
