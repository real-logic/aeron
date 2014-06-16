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
package uk.co.real_logic.aeron.mediadriver;

import java.io.IOException;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Set;

/**
 * Encapsulation of NIO Selector logic for integration into Receiver Thread and Conductor Thread
 */
public class NioSelector implements AutoCloseable
{
    private Selector selector;

    public NioSelector()
    {
        try
        {
            this.selector = Selector.open(); // yes, SelectorProvider, blah, blah
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Register channel for read.
     *
     * @param channel to select for read
     * @param obj to associate with read
     * @return SelectionKey for registration for cancel
     * @throws Exception
     */
    public SelectionKey registerForRead(final SelectableChannel channel, final ReadHandler obj) throws Exception
    {
        return channel.register(selector, SelectionKey.OP_READ, obj);
    }

    /**
     * Cancel pending reads for selection key.
     *
     * @param key to cancel
     */
    public void cancelRead(final SelectionKey key)
    {
        key.cancel();
    }

    /**
     * Close NioSelector down. Returns immediately.
     */
    public void close() throws IOException
    {
        selector.wakeup();
        selector.close();
    }

    /**
     * Explicit event loop processing as poll
     */
    public boolean processKeys() throws Exception
    {
        selector.selectNow();

        return handleSelectedKeys();
    }

    public void wakeup()
    {
        // TODO: remove
    }

    /**
     * Explicit call to selectNow but with no processing of selected keys.
     *
     * @throws Exception
     */
    public void selectNowWithNoProcessing() throws Exception
    {
        selector.selectNow();
    }

    private boolean handleReadable(final SelectionKey key)
    {
        try
        {
            return ((ReadHandler)key.attachment()).onRead();
        }
        catch (final Exception ex)
        {
            ex.printStackTrace();
            return false;
        }
    }

    private boolean handleSelectedKeys() throws Exception
    {
        boolean hasDoneWork = false;
        final Set<SelectionKey> selectedKeys = selector.selectedKeys();

        if (!selectedKeys.isEmpty())
        {
            final Iterator<SelectionKey> iter = selectedKeys.iterator();
            while (iter.hasNext())
            {
                final SelectionKey key = iter.next();
                if (key.isReadable())
                {
                    hasDoneWork |= handleReadable(key);
                    iter.remove();  // TODO: should the key not be removed regardless?
                }
            }
        }

        return hasDoneWork;
    }
}
