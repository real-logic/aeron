/*
 * Copyright 2014-2022 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.archive.client;

import io.aeron.Aeron;

/**
 * Fluent API for setting optional replay parameters. Not threadsafe.
 */
public class ReplayParams
{
    private int boundingLimitCounterId;
    private int maxFileIoLength;

    /**
     * Default, initialise all values to "null"
     */
    public ReplayParams()
    {
        reset();
    }

    /**
     * reset all value to "null", allows for an instance to be reused
     *
     * @return this for a fluent API
     */
    public ReplayParams reset()
    {
        boundingLimitCounterId = Aeron.NULL_VALUE;
        maxFileIoLength = Aeron.NULL_VALUE;
        return this;
    }

    /**
     * Sets the counter id to be used for bounding the replay. Setting this value will trigger the sending of a
     * bounded replay request instead of a normal replay.
     *
     * @param boundingLimitCounterId counter to use to bound the replay
     * @return this for a fluent API
     */
    public ReplayParams boundingLimitCounterId(final int boundingLimitCounterId)
    {
        this.boundingLimitCounterId = boundingLimitCounterId;
        return this;
    }

    /**
     * Gets the counterId specified for the bounding the replay. Returns {@link Aeron#NULL_VALUE} if unspecified.
     *
     * @return the counter id to bound the replay.
     */
    public int boundingLimitCounterId()
    {
        return this.boundingLimitCounterId;
    }

    /**
     * The maximum size of a file operation when reading from the archive to execute the replay. Will use the value
     * defined in the context otherwise. This can be used reduce the size of file IO operations to lower the
     * priority of some replays. Setting it to a value larger than the context value will have no affect.
     *
     * @param maxFileIoLength maximum length of a replay file operation
     * @return this for a fluent API
     */
    public ReplayParams maxFileIoLength(final int maxFileIoLength)
    {
        this.maxFileIoLength = maxFileIoLength;
        return this;
    }

    /**
     * Gets the maximum length for file IO operations in the replay. Defaults to {@link Aeron#NULL_VALUE} if not
     * set, which will trigger the use of the Archive.Context default.
     *
     * @return maximum file length for IO operations during replay.
     */
    public int maxFileIoLength()
    {
        return this.maxFileIoLength;
    }

    /**
     * Determines if the parameter setup has requested a bounded replay.
     *
     * @return true if the replay should be bounded, false otherwise.
     */
    public boolean isBounded()
    {
        return Aeron.NULL_VALUE != boundingLimitCounterId;
    }
}
