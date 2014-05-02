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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

import static uk.co.real_logic.aeron.util.IoUtil.mapNewFile;

public class CreatingConductorBufferStrategy extends ConductorBufferStrategy
{

    private final int bufferSize;

    private MappedByteBuffer toMediaDriverBuffer;
    private MappedByteBuffer toApiBuffer;

    public CreatingConductorBufferStrategy(final String adminDir, final int bufferSize)
    {
        super(adminDir);
        this.bufferSize = bufferSize;
    }

    public ByteBuffer toMediaDriver() throws IOException
    {
        toMediaDriverBuffer = mapNewFile(toMediaDriver, MEDIA_DRIVER_FILE, bufferSize);
        return toMediaDriverBuffer;
    }

    public ByteBuffer toApi() throws IOException
    {
        toApiBuffer = mapNewFile(toApi, API_FILE, bufferSize);
        return toApiBuffer;
    }

    public void close()
    {
        IoUtil.unmap(toApiBuffer);
        IoUtil.unmap(toMediaDriverBuffer);
    }
}
