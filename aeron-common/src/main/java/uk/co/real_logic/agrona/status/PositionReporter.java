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

/**
 * Reports on how far through a buffer some component has progressed..
 *
 * Threadsafe to write to.
 */
public interface PositionReporter extends AutoCloseable
{
    /**
     * Sets the current position of the component
     *
     * @param value the current position of the component.
     */
    void position(long value);

    /**
     * Get the value of the current position of the component.
     *
     * @return the value of the current position of the component.
     */
    long position();


    /**
     * Close down and free any underlying resources.
     */
    void close();

    /**
     * Identifier for this position counter.
     *
     * @return the identifier for this position.
     */
    int id();
}
