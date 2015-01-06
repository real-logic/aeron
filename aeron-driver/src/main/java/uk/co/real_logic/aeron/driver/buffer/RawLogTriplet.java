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
package uk.co.real_logic.aeron.driver.buffer;

import uk.co.real_logic.aeron.common.command.BuffersReadyFlyweight;

import java.util.stream.Stream;

/**
 * Represents the collection of term and associated state buffers for the connection between a publisher and subscriber
 * connection for the replicated log.
 */
public interface RawLogTriplet extends AutoCloseable
{
    /**
     * A {@link Stream} of the {@link RawLog} buffers.
     *
     * @return a {@link Stream} of the {@link RawLog} buffers.
     */
    Stream<? extends RawLog> stream();

    /**
     * An array of the {@link RawLog} buffers.
     *
     * @return an array of the {@link RawLog} buffers.
     */
    RawLog[] buffers();

    /**
     * Write the buffer location details to the flyweight.
     *
     * @param buffersReadyFlyweight to which the buffer locations will be written.
     */
    void writeBufferLocations(final BuffersReadyFlyweight buffersReadyFlyweight);

    void close();
}
