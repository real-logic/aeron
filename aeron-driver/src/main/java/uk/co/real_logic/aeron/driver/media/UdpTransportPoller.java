/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
package uk.co.real_logic.aeron.driver.media;

import uk.co.real_logic.agrona.LangUtil;
import uk.co.real_logic.agrona.collections.ArrayUtil;
import uk.co.real_logic.agrona.nio.TransportPoller;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;

/**
 * Encapsulates the polling of a number of {@link UdpChannelTransport}s using whatever means provides the lowest latency.
 */
public class UdpTransportPoller extends TransportPoller
{
    private UdpChannelTransport[] transports = new UdpChannelTransport[0];

    /**
     * Explicit event loop processing as a poll
     *
     * @return the number of frames processed.
     */
    public int pollTransports()
    {
        int bytesReceived = 0;
        try
        {
            final UdpChannelTransport[] transports = this.transports;
            final int numTransports = transports.length;
            if (numTransports <= ITERATION_THRESHOLD)
            {
                for (int i = numTransports - 1; i >= 0; i--)
                {
                    bytesReceived += transports[i].pollForData();
                }
            }
            else
            {
                selector.selectNow();

                final SelectionKey[] keys = selectedKeySet.keys();
                for (int i = selectedKeySet.size() - 1; i >= 0; i--)
                {
                    bytesReceived += ((UdpChannelTransport)keys[i].attachment()).pollForData();
                }

                selectedKeySet.reset();
            }
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return bytesReceived;
    }

    /**
     * Register channel for read.
     *
     * @param transport to associate with read
     * @return SelectionKey for registration for cancel
     */
    public SelectionKey registerForRead(final UdpChannelTransport transport)
    {
        SelectionKey key = null;
        try
        {
            transports = ArrayUtil.add(transports, transport);
            key = transport.receiveDatagramChannel().register(selector, SelectionKey.OP_READ, transport);
        }
        catch (final ClosedChannelException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return key;
    }

    /**
     * Cancel previous registration.
     *
     * @param transport to cancel read for
     */
    public void cancelRead(final UdpChannelTransport transport)
    {
        transports = ArrayUtil.remove(transports, transport);
    }

}
