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

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;

/**
 * Encapsulates the polling of a number of {@link UdpChannelTransport}s using whatever means provides the lowest latency.
 */
public class DataTransportPoller extends UdpTransportPoller
{
    private ReceiveChannelEndpoint[] transports = new ReceiveChannelEndpoint[0];

    public int pollTransports()
    {
        int bytesReceived = 0;
        try
        {
            if (transports.length <= ITERATION_THRESHOLD)
            {
                for (final ReceiveChannelEndpoint transport : transports)
                {
                    bytesReceived += transport.pollForData();
                }
            }
            else
            {
                selector.selectNow();

                final SelectionKey[] keys = selectedKeySet.keys();
                for (int i = 0, length = selectedKeySet.size(); i < length; i++)
                {
                    bytesReceived += ((ReceiveChannelEndpoint)keys[i].attachment()).pollForData();
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

    public SelectionKey registerForRead(final UdpChannelTransport transport)
    {
        return registerForRead((ReceiveChannelEndpoint)transport);
    }

    public SelectionKey registerForRead(final ReceiveChannelEndpoint transport)
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

    public void cancelRead(final UdpChannelTransport transport)
    {
        cancelRead((ReceiveChannelEndpoint)transport);
    }

    public void cancelRead(final ReceiveChannelEndpoint transport)
    {
        transports = ArrayUtil.remove(transports, transport);
    }
}
