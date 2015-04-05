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
package uk.co.real_logic.aeron.driver;

import uk.co.real_logic.agrona.LangUtil;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

/**
 * Encapsulates the polling of a number of {@link UdpChannelTransport}s using whatever means provides the lowest latency.
 */
public class TransportPoller implements AutoCloseable
{
    private static final int ITERATION_THRESHOLD = 5;
    private static final Field SELECTED_KEYS_FIELD;
    private static final Field PUBLIC_SELECTED_KEYS_FIELD;

    static
    {
        Field selectKeysField = null;
        Field publicSelectKeysField = null;

        try
        {
            final Class<?> clazz = Class.forName("sun.nio.ch.SelectorImpl", false, ClassLoader.getSystemClassLoader());

            if (clazz.isAssignableFrom(Selector.open().getClass()))
            {
                selectKeysField = clazz.getDeclaredField("selectedKeys");
                selectKeysField.setAccessible(true);

                publicSelectKeysField = clazz.getDeclaredField("publicSelectedKeys");
                publicSelectKeysField.setAccessible(true);
            }
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
        finally
        {
            SELECTED_KEYS_FIELD = selectKeysField;
            PUBLIC_SELECTED_KEYS_FIELD = publicSelectKeysField;
        }
    }

    private final Selector selector;
    private final NioSelectedKeySet selectedKeySet;
    private UdpChannelTransport[] transports = new UdpChannelTransport[0];

    /**
     * Construct a selector
     */
    public TransportPoller()
    {
        try
        {
            selector = Selector.open(); // yes, SelectorProvider, blah, blah
            selectedKeySet = new NioSelectedKeySet();

            SELECTED_KEYS_FIELD.set(selector, selectedKeySet);
            PUBLIC_SELECTED_KEYS_FIELD.set(selector, selectedKeySet);
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
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
            addTransport(transport);
            key = transport.datagramChannel().register(selector, SelectionKey.OP_READ, transport);
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
        removeTransport(transport);
    }

    /**
     * Close NioSelector down. Returns immediately.
     */
    public void close()
    {
        selector.wakeup();
        try
        {
            selector.close();
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

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
     * Explicit call to selectNow but without processing of selected keys.
     */
    public void selectNowWithoutProcessing()
    {
        try
        {
            selector.selectNow();
            selectedKeySet.reset();
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private void addTransport(final UdpChannelTransport transport)
    {
        final UdpChannelTransport[] oldTransports = transports;
        final int length = oldTransports.length;
        final UdpChannelTransport[] newTransports = new UdpChannelTransport[length + 1];

        System.arraycopy(oldTransports, 0, newTransports, 0, length);
        newTransports[length] = transport;

        transports = newTransports;
    }

    private void removeTransport(final UdpChannelTransport transport)
    {
        final UdpChannelTransport[] oldTransports = transports;
        final int length = oldTransports.length;
        final UdpChannelTransport[] newTransports = new UdpChannelTransport[length - 1];
        for (int i = 0, j = 0; i < length; i++)
        {
            if (oldTransports[i] != transport)
            {
                newTransports[j++] = oldTransports[i];
            }
        }

        transports = newTransports;
    }
}
