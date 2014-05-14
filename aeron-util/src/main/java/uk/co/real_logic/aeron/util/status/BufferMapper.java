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
package uk.co.real_logic.aeron.util.status;

import uk.co.real_logic.aeron.util.IoUtil;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;

import static uk.co.real_logic.aeron.util.CommonConfiguration.COUNTERS_DIR_NAME;

/**
 * Common Class
 */
public class BufferMapper
{
    private final MappedByteBuffer descriptor;
    private final MappedByteBuffer counter;

    private final AtomicBuffer descriptorBuffer;
    private final AtomicBuffer counterBuffer;

    static interface Mapper
    {
        MappedByteBuffer map(final File directory, final String file) throws IOException;
    }

    public BufferMapper(final Mapper descriptorMapper, final Mapper counterMapper)
    {
        final File directory = new File(COUNTERS_DIR_NAME);
        try
        {
            descriptor = descriptorMapper.map(new File(directory, "descriptor"), "descriptor");
            counter = counterMapper.map(new File(directory, "counter"), "counter");

            descriptorBuffer = new AtomicBuffer(descriptor);
            counterBuffer = new AtomicBuffer(counter);
        }
        catch (IOException e)
        {
            throw new IllegalStateException(e);
        }
    }

    public AtomicBuffer descriptorBuffer()
    {
        return descriptorBuffer;
    }

    public PositionIndicator indicator(final int offset) {
        return new BufferPositionIndicator(counterBuffer, offset);
    }

    public PositionReporter reporter(final int offset) {
        return new BufferPositionReporter(counterBuffer, offset);
    }

    public void close()
    {
        IoUtil.unmap(descriptor);
        IoUtil.unmap(counter);
    }
}
