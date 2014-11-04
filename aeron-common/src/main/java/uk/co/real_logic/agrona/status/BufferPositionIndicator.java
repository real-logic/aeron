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
 * Indicates the position of some entity that stores its value in an buffer. The buffer is managed by a {@link uk.co.real_logic.agrona.concurrent.CountersManager}.
 */
public class BufferPositionIndicator implements PositionIndicator
{
    private final UnsafeBuffer buffer;
    private final int counterId;
    private final CountersManager countersManager;
    private final int offset;

    /**
     * Map a position indicator over a buffer.
     *
     * @param buffer containing the counter.
     * @param counterId identifier of the counter.
     */
    public BufferPositionIndicator(final UnsafeBuffer buffer, final int counterId)
    {
        this(buffer, counterId, null);
    }

    /**
     * Map a position indicator over a buffer and this indicator owns the counter for reclamation.
     *
     * @param buffer containing the counter.
     * @param counterId identifier of the counter.
     * @param countersManager to be used for freeing the counter when this is closed.
     */
    public BufferPositionIndicator(final UnsafeBuffer buffer, final int counterId, final CountersManager countersManager)
    {
        this.buffer = buffer;
        this.counterId = counterId;
        this.countersManager = countersManager;
        this.offset = CountersManager.counterOffset(counterId);
    }

    public long position()
    {
        return buffer.getLongVolatile(offset);
    }

    public void close()
    {
        countersManager.free(counterId);
    }
}
