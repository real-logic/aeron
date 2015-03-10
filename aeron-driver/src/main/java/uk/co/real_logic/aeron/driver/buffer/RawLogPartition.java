/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
package uk.co.real_logic.aeron.driver.buffer;

import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

/**
 * Encapsulates the pair of buffers used to hold a term log partition and associated state for publication/subscription
 */
public class RawLogPartition
{
    private final UnsafeBuffer termBuffer;
    private final UnsafeBuffer metaDataBuffer;

    public RawLogPartition(final UnsafeBuffer termBuffer, final UnsafeBuffer metaDataBuffer)
    {
        this.termBuffer = termBuffer;
        this.metaDataBuffer = metaDataBuffer;
    }

    /**
     * Get the buffer holding the recorded log of messages for a term.
     *
     * @return the buffer holding the recorded log of messages for a term.
     */
    public UnsafeBuffer termBuffer()
    {
        return termBuffer;
    }

    /**
     * Get the buffer holding the meta data about the recorded log of messages for a term.
     *
     * @return the buffer holding the state information about the recorded log of messages for a term.
     */
    public UnsafeBuffer metaDataBuffer()
    {
        return metaDataBuffer;
    }
}
