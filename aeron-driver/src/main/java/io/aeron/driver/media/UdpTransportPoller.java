/*
 * Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron.driver.media;

import org.agrona.nio.TransportPoller;

import java.nio.channels.SelectionKey;

/**
 * Encapsulates the polling of a number of {@link UdpChannelTransport}s using whatever means provides the lowest latency.
 */
public abstract class UdpTransportPoller extends TransportPoller
{
    /**
     * Explicit event loop processing as a poll
     *
     * @return the number of frames processed.
     */
    public abstract int pollTransports();

    /**
     * Register channel for read.
     *
     * @param transport to associate with read
     * @return SelectionKey for registration for cancel
     */
    public abstract SelectionKey registerForRead(UdpChannelTransport transport);

    /**
     * Cancel previous registration.
     *
     * @param transport to cancel read for
     */
    public abstract void cancelRead(UdpChannelTransport transport);
}
