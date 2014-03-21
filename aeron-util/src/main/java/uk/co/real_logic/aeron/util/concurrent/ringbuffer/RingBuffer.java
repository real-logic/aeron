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
package uk.co.real_logic.aeron.util.concurrent.ringbuffer;

import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

/**
 * Ring-buffer for the concurrent exchanging of binary encoded events from producer to consumer in a FIFO manner.
 */
public interface RingBuffer
{
    /**
     * Get the capacity of the ring-buffer in bytes for exchange.
     *
     * @return the capacity of the ring-buffer in bytes for exchange.
     */
    int capacity();

    /**
     * Non blocking write of an event to an underlying ring-srcBuffer.
     *
     * @param eventTypeId type of the event encoding.
     * @param srcBuffer containing the encoded binary event.
     * @param srcIndex at which the encoded event begins.
     * @param length of the encoded event in bytes.
     * @return true if written to the ring-srcBuffer, or false if insufficient space exists.
     * @throws IllegalArgumentException if the length is greater than {@link RingBuffer#maxEventSize()}
     */
    boolean write(final int eventTypeId, final AtomicBuffer srcBuffer, final int srcIndex, final int length);

    /**
     * Read as many events as are available from the ring buffer.
     *
     * @param handler to be called for processing each event in turn.
     * @return the number of event that have been processed.
     */
    int read(final EventHandler handler);

    /**
     * Read as many events as are available from the ring buffer to up a supplied maximum.
     *
     * @param handler to be called for processing each event in turn.
     * @param maxEvents that will be read in a single invocation.
     * @return the number of event that have been processed.
     */
    int read(final EventHandler handler, final int maxEvents);

    /**
     * The maximum event size supported by the underlying ring buffer.
     *
     * @return the maximum event size supported by the underlying ring buffer.
     */
    int maxEventSize();

    /**
     * Get the next value that can be used for a correlation id on an event when a response needs to be correlated.
     * <p/>
     * This method should be thread safe.
     *
     * @return the next value in the correlation sequence.
     */
    long nextCorrelationId();
}
