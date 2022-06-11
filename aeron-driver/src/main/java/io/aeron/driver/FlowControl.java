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
package io.aeron.driver;

import io.aeron.driver.media.UdpChannel;
import io.aeron.protocol.StatusMessageFlyweight;

import java.net.InetSocketAddress;

/**
 * Strategy for applying flow control to the {@link Sender} on each stream.
 */
public interface FlowControl
{
    /**
     * Update the sender flow control strategy based on a status message from the receiver.
     *
     * @param flyweight           over the status message received.
     * @param receiverAddress     of the receiver.
     * @param senderLimit         the current sender position limit.
     * @param initialTermId       for the term buffers.
     * @param positionBitsToShift in use for the length of each term buffer.
     * @param timeNs              current time (in nanoseconds).
     * @return the new position limit to be employed by the sender.
     */
    long onStatusMessage(
        StatusMessageFlyweight flyweight,
        InetSocketAddress receiverAddress,
        long senderLimit,
        int initialTermId,
        int positionBitsToShift,
        long timeNs);

    /**
     * Initialize the flow control strategy for a stream.
     *
     * @param context          to allow access to media driver configuration
     * @param udpChannel       for the stream.
     * @param initialTermId    at which the stream started.
     * @param termBufferLength to use as the length of each term buffer.
     */
    void initialize(MediaDriver.Context context, UdpChannel udpChannel, int initialTermId, int termBufferLength);

    /**
     * Perform any maintenance needed by the flow control strategy and return current sender limit position.
     *
     * @param timeNs         current time in nanoseconds.
     * @param senderLimit    for the current sender position.
     * @param senderPosition which has been sent.
     * @param isEos          is this end-of-stream for the sender.
     * @return the position limit to be employed by the sender.
     */
    long onIdle(long timeNs, long senderLimit, long senderPosition, boolean isEos);

    /**
     * Has the flow control strategy its required group of receivers to be considered connected? The
     * result of this feeds into the determination of if a publication is connected.
     *
     * @return true if the required group of receivers is connected, otherwise false.
     */
    boolean hasRequiredReceivers();
}
