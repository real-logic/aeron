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
package uk.co.real_logic.agrona.status;

import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.concurrent.CountersManager;

/**
 * Reports a position by recording it in an {@link uk.co.real_logic.agrona.concurrent.UnsafeBuffer}.
 */
public class BufferPositionReporter implements PositionReporter
{
    private final UnsafeBuffer buffer;
    private final int counterId;
    private final CountersManager countersManager;
    private final int offset;

    public BufferPositionReporter(final UnsafeBuffer buffer, final int counterId)
    {
        this(buffer, counterId, null);
    }

    public BufferPositionReporter(final UnsafeBuffer buffer, final int counterId, final CountersManager countersManager)
    {
        this.buffer = buffer;
        this.counterId = counterId;
        this.countersManager = countersManager;
        this.offset = CountersManager.counterOffset(counterId);
    }

    public void position(final long value)
    {
        buffer.putLongOrdered(offset, value);
    }

    public long position()
    {
        return buffer.getLongVolatile(offset);
    }

    public void close()
    {
        countersManager.free(counterId);
    }

    public int id()
    {
        return counterId;
    }
}
