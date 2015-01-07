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
package uk.co.real_logic.aeron.common.command;

/**
 * Flyweight interface representing buffers to be used for the terms in a log.
 */
public interface BuffersReadyFlyweight
{
    /**
     * Get the offset at which the buffer for a given index starts.
     *
     * @param index of the buffer
     * @return offset at which the buffer for a given index starts.
     */
    long bufferOffset(int index);

    /**
     * Set the length of the buffer for a given index.
     *
     * @param index  of the buffer
     * @param offset of the buffer starting position
     * @return this for fluent API usage.
     */
    BuffersReadyFlyweight bufferOffset(int index, long offset);

    /**
     * Get the length of the buffer for a given index.
     *
     * @param index of the buffer
     * @return the length of the buffer for a given index
     */
    int bufferLength(int index);

    /**
     * Set the length of the buffer for a given index.
     *
     * @param index  of the buffer
     * @param length of the buffer
     * @return this for fluent API usage.
     */
    BuffersReadyFlyweight bufferLength(int index, int length);

    /**
     * Get the location in the filesystem of the buffer for a given index.
     *
     * @param index of the buffer
     * @return the location in the filesystem of the buffer for a given index.
     */
    String bufferLocation(int index);

    /**
     * Set the location in the filesystem of the buffer for a given index.
     *
     * @param index    of the buffer
     * @param location in the filesystem in which the buffer exists.
     * @return this for fluent API usage.
     */
    BuffersReadyFlyweight bufferLocation(int index, String location);
}
