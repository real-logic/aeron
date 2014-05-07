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

import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

/**
 * .
 */
public class BufferPositionReporter implements PositionReporter
{

    private final AtomicBuffer buffer;

    public BufferPositionReporter(final AtomicBuffer buffer)
    {
        this.buffer = buffer;
    }

    public void position(final long value)
    {
        buffer.putLongOrdered(0, value);
    }
}
