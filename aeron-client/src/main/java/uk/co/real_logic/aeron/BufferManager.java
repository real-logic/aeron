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
package uk.co.real_logic.aeron;

/**
 * Interface for encapsulating the strategy of mapping {@link ManagedBuffer}s at a giving file location.
 */
interface BufferManager
{
    /**
     * Map a buffer for a given region located in a file.
     *
     * @param fileName fully qualified to file containing the region to be mapped.
     * @param offset   within the file at which the mapping should begin.
     * @param length   of the region in the file to be mapped.
     * @return a {@link ManagedBuffer} representing the mapped buffer.
     */
    ManagedBuffer mapBuffer(String fileName, long offset, int length);
}
