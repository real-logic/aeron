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
package uk.co.real_logic.agrona.concurrent.ringbuffer;

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.agrona.concurrent.MessageHandler;

/**
 * Ring-buffer for the concurrent exchanging of binary encoded messages from producer to consumer in a FIFO manner.
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
     * Non-blocking write of an message to an underlying ring-buffer.
     *
     * @param msgTypeId type of the message encoding.
     * @param srcBuffer containing the encoded binary message.
     * @param srcIndex at which the encoded message begins.
     * @param length of the encoded message in bytes.
     * @return true if written to the ring-buffer, or false if insufficient space exists.
     * @throws IllegalArgumentException if the length is greater than {@link RingBuffer#maxMsgLength()}
     */
    boolean write(int msgTypeId, DirectBuffer srcBuffer, int srcIndex, int length);

    /**
     * Read as many messages as are available from the ring buffer.
     *
     * @param handler to be called for processing each message in turn.
     * @return the number of messages that have been processed.
     */
    int read(MessageHandler handler);

    /**
     * Read as many messages as are available from the ring buffer to up a supplied maximum.
     *
     * @param handler to be called for processing each message in turn.
     * @param messageCountLimit the number of messages will be read in a single invocation.
     * @return the number of messages that have been processed.
     */
    int read(MessageHandler handler, int messageCountLimit);

    /**
     * The maximum message length in bytes supported by the underlying ring buffer.
     *
     * @return the maximum message length in bytes supported by the underlying ring buffer.
     */
    int maxMsgLength();

    /**
     * Get the next value that can be used for a correlation id on an message when a response needs to be correlated.
     *
     * This method should be thread safe.
     *
     * @return the next value in the correlation sequence.
     */
    long nextCorrelationId();

    /**
     * Get the underlying buffer used by the RingBuffer for storage.
     *
     * @return the underlying buffer used by the RingBuffer for storage.
     */
    AtomicBuffer buffer();

    /**
     * Set the time of the last consumer heartbeat.
     *
     * @param time of the last consumer heartbeat.
     */
    void consumerHeartbeatTimeNs(long time);

    /**
     * The time of the last consumer heartbeat.
     *
     * @return the time of the last consumer heartbeat.
     */
    long consumerHeartbeatTimeNs();
}
