/*
 * Copyright 2014-2025 Real Logic Limited.
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
package io.aeron.driver.buffer;

/**
 * Factory interface for creating the log buffers under publications and images.
 */
public interface LogFactory extends AutoCloseable
{
    /**
     * {@inheritDoc}
     */
    void close();

    /**
     * Create a new {@link RawLog} for a publication.
     *
     * @param correlationId    which is the original registration id for a publication.
     * @param termBufferLength length of the buffer for each term.
     * @param useSparseFiles   should the file be sparse so the pages are only allocated as required.
     * @return the newly created {@link RawLog}
     */
    RawLog newPublication(long correlationId, int termBufferLength, boolean useSparseFiles);

    /**
     * Create a new {@link RawLog} for an image of a publication.
     *
     * @param correlationId    assigned to uniquely identify an image on a driver.
     * @param termBufferLength length of the buffer for each term.
     * @param useSparseFiles   should the file be sparse so the pages are only allocated as required.
     * @return the newly created {@link RawLog}
     */
    RawLog newImage(long correlationId, int termBufferLength, boolean useSparseFiles);
}
